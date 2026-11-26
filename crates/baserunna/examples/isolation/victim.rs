//! Victim workload generator using closed-loop KV and transaction clients plus periodic event queries.

use crate::http::{NodeApi, Owner};
use bytes::Bytes;
use reqwest::Method;
use serde::Serialize;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};

pub const CLIENT_COUNT: usize = 8;
const KEY_COUNT: usize = 100;
const EVENT_TABLE: &str = "vev";

/// Latency and failure summary for victim requests during a phase.
#[derive(Clone, Debug, Default, Serialize)]
pub struct VictimStats {
    pub requests: usize,
    pub failures: usize,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub event_queries: usize,
    pub event_query_p99_ms: f64,
    pub seconds: f64,
    /// Up to 10 sample failures formatted as `label: detail`.
    pub failure_samples: Vec<String>,
}

/// Populates victim KV keys and a small event table.
pub async fn prepare(api: &NodeApi, victim: &Owner) -> Result<(), String> {
    for key_index in 0..KEY_COUNT {
        api.send_expect(
            Method::PUT,
            &format!("/v1/dbs/{}/kv/k{key_index}", victim.db),
            &victim.token,
            vec![b'v'; 100],
            &[200, 201, 204],
        )
        .await?;
    }
    api.create_event_table(
        &victim.token,
        &victim.db,
        json!({"name": EVENT_TABLE, "columns": [{"name": "t", "type": "time"}, {"name": "u", "type": "string"},
            {"name": "e", "type": "string"}], "time_column": "t", "order_by": ["t"]}),
    )
    .await?;
    let rows: Vec<String> = (0..10_000)
        .map(|i| format!(r#"{{"t":{},"u":"u{}","e":"e{}"}}"#, 1_749_000_000_000i64 + i * 1000, i % 500, i % 8))
        .collect();
    api.bulk_insert(&victim.token, &victim.db, EVENT_TABLE, format!("[{}]", rows.join(","))).await
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() as f64 * quantile).ceil() as usize).clamp(1, sorted.len()) - 1]
}

struct RequestSample {
    label: &'static str,
    ok: bool,
    ms: f64,
    detail: String,
}

/// Runs victim traffic until `stop` is true and returns latency statistics.
///
/// `max_rps` caps the combined KV and transaction request rate to stay below the per-user rate
/// limit.
pub async fn run(api: Arc<NodeApi>, victim: Owner, stop: Arc<AtomicBool>, max_rps: f64) -> VictimStats {
    let started = Instant::now();
    let interval = Duration::from_secs_f64(CLIENT_COUNT as f64 / max_rps);
    let mut tasks = Vec::new();
    for client_index in 0..CLIENT_COUNT {
        let (api, victim, stop) = (api.clone(), victim.clone(), stop.clone());
        tasks.push(tokio::spawn(async move {
            let mut out = Vec::new();
            let mut step = client_index;
            while !stop.load(Relaxed) {
                let next = tokio::time::Instant::now() + interval;
                let key_index = (step * 7) % KEY_COUNT;
                let (label, method, path, body) = match step % 3 {
                    0 => ("kv get", Method::GET, format!("/v1/dbs/{}/kv/k{key_index}", victim.db), Bytes::new()),
                    1 => (
                        "kv put",
                        Method::PUT,
                        format!("/v1/dbs/{}/kv/k{key_index}", victim.db),
                        Bytes::from(vec![b'w'; 100]),
                    ),
                    _ => {
                        let ops = json!({"ops": [{"op": "get", "key": format!("k{key_index}")},
                            {"op": "put", "key": format!("k{}", (key_index + 1) % KEY_COUNT), "value": "txn value"}]});
                        ("txn", Method::POST, format!("/v1/dbs/{}/txn", victim.db), Bytes::from(ops.to_string()))
                    }
                };
                out.push(send_sample(&api, &victim, label, method, &path, body).await);
                step += CLIENT_COUNT;
                tokio::time::sleep_until(next).await;
            }
            out
        }));
    }
    let query_task = {
        let (api, victim, stop) = (api.clone(), victim.clone(), stop.clone());
        tokio::spawn(async move {
            let mut out = Vec::new();
            let query =
                json!({"from": 0, "to": 9_999_999_999_999i64, "group_by": ["e"], "aggregates": {"n": {"fn": "count"}}});
            while !stop.load(Relaxed) {
                let next = tokio::time::Instant::now() + Duration::from_secs(1);
                let path = format!("/v1/dbs/{}/events/{EVENT_TABLE}/rows/query", victim.db);
                out.push(
                    send_sample(&api, &victim, "event query", Method::POST, &path, Bytes::from(query.to_string()))
                        .await,
                );
                tokio::time::sleep_until(next).await;
            }
            out
        })
    };
    let mut samples = Vec::new();
    for task in tasks {
        samples.extend(task.await.expect("victim client"));
    }
    let queries = query_task.await.expect("victim event queries");
    let mut query_ms: Vec<f64> = queries.iter().map(|sample| sample.ms).collect();
    query_ms.sort_by(f64::total_cmp);
    samples.extend(queries);
    let mut ms: Vec<f64> = samples.iter().map(|sample| sample.ms).collect();
    ms.sort_by(f64::total_cmp);
    let failed: Vec<&RequestSample> = samples.iter().filter(|sample| !sample.ok).collect();
    VictimStats {
        requests: samples.len(),
        failures: failed.len(),
        p50_ms: percentile(&ms, 0.50),
        p99_ms: percentile(&ms, 0.99),
        max_ms: ms.last().copied().unwrap_or(0.0),
        event_queries: query_ms.len(),
        event_query_p99_ms: percentile(&query_ms, 0.99),
        seconds: started.elapsed().as_secs_f64(),
        failure_samples: failed.iter().take(10).map(|sample| format!("{}: {}", sample.label, sample.detail)).collect(),
    }
}

async fn send_sample(
    api: &NodeApi,
    victim: &Owner,
    label: &'static str,
    method: Method,
    path: &str,
    body: Bytes,
) -> RequestSample {
    let started = Instant::now();
    let result = api.send(method, path, &victim.token, body).await;
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    match result {
        Ok(reply) if (200..300).contains(&reply.status) => RequestSample { label, ok: true, ms, detail: String::new() },
        Ok(reply) => {
            RequestSample { label, ok: false, ms, detail: format!("status {}: {}", reply.status, reply.excerpt()) }
        }
        Err(err) => RequestSample { label, ok: false, ms, detail: err },
    }
}
