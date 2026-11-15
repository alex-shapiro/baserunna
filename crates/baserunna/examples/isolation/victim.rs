//! Victim load: closed-loop KV and transaction clients plus one small event query per second.

use crate::http::{Api, Owner};
use bytes::Bytes;
use reqwest::Method;
use serde::Serialize;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};

pub const CLIENTS: usize = 8;
const KEYS: usize = 100;
const EVENT_TABLE: &str = "vev";

/// Latency summary of the victim's requests in one phase.
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
    /// First few failures, as `label: status or error`.
    pub failure_samples: Vec<String>,
}

/// Creates the victim's keys and a small event table.
pub async fn prepare(api: &Api, v: &Owner) -> Result<(), String> {
    for k in 0..KEYS {
        api.expect(Method::PUT, &format!("/v1/dbs/{}/kv/k{k}", v.db), &v.token, vec![b'v'; 100], &[200, 201, 204]).await?;
    }
    api.create_event_table(
        &v.token,
        &v.db,
        json!({"name": EVENT_TABLE, "columns": [{"name": "t", "type": "time"}, {"name": "u", "type": "string"},
            {"name": "e", "type": "string"}], "time_column": "t", "order_by": ["t"]}),
    )
    .await?;
    let rows: Vec<String> = (0..10_000)
        .map(|i| format!(r#"{{"t":{},"u":"u{}","e":"e{}"}}"#, 1_749_000_000_000i64 + i * 1000, i % 500, i % 8))
        .collect();
    api.bulk_insert(&v.token, &v.db, EVENT_TABLE, format!("[{}]", rows.join(","))).await
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() as f64 * p).ceil() as usize).clamp(1, sorted.len()) - 1]
}

struct Sample {
    label: &'static str,
    ok: bool,
    ms: f64,
    detail: String,
}

/// Runs the victim until `stop` is set and returns its latency summary.
/// `max_rps` caps the total KV and transaction request rate so that the victim stays under its
/// own per-user rate limit.
pub async fn run(api: Arc<Api>, v: Owner, stop: Arc<AtomicBool>, max_rps: f64) -> VictimStats {
    let started = Instant::now();
    let interval = Duration::from_secs_f64(CLIENTS as f64 / max_rps);
    let mut tasks = Vec::new();
    for c in 0..CLIENTS {
        let (api, v, stop) = (api.clone(), v.clone(), stop.clone());
        tasks.push(tokio::spawn(async move {
            let mut out = Vec::new();
            let mut i = c;
            while !stop.load(Relaxed) {
                let next = tokio::time::Instant::now() + interval;
                let k = (i * 7) % KEYS;
                let (label, method, path, body) = match i % 3 {
                    0 => ("kv get", Method::GET, format!("/v1/dbs/{}/kv/k{k}", v.db), Bytes::new()),
                    1 => ("kv put", Method::PUT, format!("/v1/dbs/{}/kv/k{k}", v.db), Bytes::from(vec![b'w'; 100])),
                    _ => {
                        let ops = json!({"ops": [{"op": "get", "key": format!("k{k}")},
                            {"op": "put", "key": format!("k{}", (k + 1) % KEYS), "value": "txn value"}]});
                        ("txn", Method::POST, format!("/v1/dbs/{}/txn", v.db), Bytes::from(ops.to_string()))
                    }
                };
                out.push(sample(&api, &v, label, method, &path, body).await);
                i += CLIENTS;
                tokio::time::sleep_until(next).await;
            }
            out
        }));
    }
    let query_task = {
        let (api, v, stop) = (api.clone(), v.clone(), stop.clone());
        tokio::spawn(async move {
            let mut out = Vec::new();
            let q = json!({"from": 0, "to": 9_999_999_999_999i64, "group_by": ["e"], "aggregates": {"n": {"fn": "count"}}});
            while !stop.load(Relaxed) {
                let next = tokio::time::Instant::now() + Duration::from_secs(1);
                let path = format!("/v1/dbs/{}/events/{EVENT_TABLE}/rows/query", v.db);
                out.push(sample(&api, &v, "event query", Method::POST, &path, Bytes::from(q.to_string())).await);
                tokio::time::sleep_until(next).await;
            }
            out
        })
    };
    let mut samples = Vec::new();
    for t in tasks {
        samples.extend(t.await.expect("victim client"));
    }
    let queries = query_task.await.expect("victim event queries");
    let mut query_ms: Vec<f64> = queries.iter().map(|s| s.ms).collect();
    query_ms.sort_by(f64::total_cmp);
    samples.extend(queries);
    let mut ms: Vec<f64> = samples.iter().map(|s| s.ms).collect();
    ms.sort_by(f64::total_cmp);
    let failed: Vec<&Sample> = samples.iter().filter(|s| !s.ok).collect();
    VictimStats {
        requests: samples.len(),
        failures: failed.len(),
        p50_ms: percentile(&ms, 0.50),
        p99_ms: percentile(&ms, 0.99),
        max_ms: ms.last().copied().unwrap_or(0.0),
        event_queries: query_ms.len(),
        event_query_p99_ms: percentile(&query_ms, 0.99),
        seconds: started.elapsed().as_secs_f64(),
        failure_samples: failed.iter().take(10).map(|s| format!("{}: {}", s.label, s.detail)).collect(),
    }
}

async fn sample(api: &Api, v: &Owner, label: &'static str, method: Method, path: &str, body: Bytes) -> Sample {
    let started = Instant::now();
    let r = api.send(method, path, &v.token, body).await;
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    match r {
        Ok(r) if (200..300).contains(&r.status) => Sample { label, ok: true, ms, detail: String::new() },
        Ok(r) => Sample { label, ok: false, ms, detail: format!("status {}: {}", r.status, r.excerpt()) },
        Err(e) => Sample { label, ok: false, ms, detail: e },
    }
}
