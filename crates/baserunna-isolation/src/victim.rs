//! Victim workload generator using closed-loop KV and transaction clients plus periodic event queries.

use crate::http::{NodeApi, Owner, RequestRecord};
use crate::percentile;
use baserunna_api::{Body, Op, TxnRequest};
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

/// Runs victim traffic until `stop` is true and returns latency statistics.
///
/// `max_rps` limits the combined KV and transaction request rate so the victim stays below the
/// per-user rate limit.
pub async fn run(api: Arc<NodeApi>, victim: Owner, stop: Arc<AtomicBool>, max_rps: f64) -> VictimStats {
    let started = Instant::now();
    let interval = Duration::from_secs_f64(CLIENT_COUNT as f64 / max_rps);
    let mut tasks = Vec::new();
    for client_index in 0..CLIENT_COUNT {
        let (api, victim, stop) = (api.clone(), victim.clone(), stop.clone());
        tasks.push(tokio::spawn(async move {
            let mut samples = Vec::new();
            let mut step = client_index;
            while !stop.load(Relaxed) {
                let next_request_at = tokio::time::Instant::now() + interval;
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
                        let ops = vec![
                            Op::Get { key: format!("k{key_index}") },
                            Op::Put {
                                key: format!("k{}", (key_index + 1) % KEY_COUNT),
                                value: Some("txn value".into()),
                                blob_id: None,
                                ttl_ms: None,
                            },
                        ];
                        let body = TxnRequest { ops, ..Default::default() }.encode();
                        ("txn", Method::POST, format!("/v1/dbs/{}/txn", victim.db), body)
                    }
                };
                samples.push(api.send_timed(label, method, &path, &victim.token, body, &[200]).await);
                step += CLIENT_COUNT;
                tokio::time::sleep_until(next_request_at).await;
            }
            samples
        }));
    }
    let query_task = {
        let (api, victim, stop) = (api.clone(), victim.clone(), stop.clone());
        tokio::spawn(async move {
            let mut samples = Vec::new();
            let query =
                json!({"from": 0, "to": 9_999_999_999_999i64, "group_by": ["e"], "aggregates": {"n": {"fn": "count"}}});
            while !stop.load(Relaxed) {
                let next_request_at = tokio::time::Instant::now() + Duration::from_secs(1);
                let path = format!("/v1/dbs/{}/events/{EVENT_TABLE}/rows/query", victim.db);
                let body = Bytes::from(query.to_string());
                samples.push(api.send_timed("event query", Method::POST, &path, &victim.token, body, &[200]).await);
                tokio::time::sleep_until(next_request_at).await;
            }
            samples
        })
    };
    let mut samples = Vec::new();
    for task in tasks {
        samples.extend(task.await.expect("victim client"));
    }
    let query_samples = query_task.await.expect("victim event queries");
    let mut query_latencies_ms: Vec<f64> = query_samples.iter().map(|sample| sample.elapsed_ms).collect();
    query_latencies_ms.sort_by(f64::total_cmp);
    samples.extend(query_samples);
    let mut all_latencies_ms: Vec<f64> = samples.iter().map(|sample| sample.elapsed_ms).collect();
    all_latencies_ms.sort_by(f64::total_cmp);
    let failed_samples: Vec<&RequestRecord> = samples.iter().filter(|sample| !sample.expected).collect();
    VictimStats {
        requests: samples.len(),
        failures: failed_samples.len(),
        p50_ms: percentile(&all_latencies_ms, 0.50),
        p99_ms: percentile(&all_latencies_ms, 0.99),
        max_ms: all_latencies_ms.last().copied().unwrap_or(0.0),
        event_queries: query_latencies_ms.len(),
        event_query_p99_ms: percentile(&query_latencies_ms, 0.99),
        seconds: started.elapsed().as_secs_f64(),
        failure_samples: failed_samples
            .iter()
            .take(10)
            .map(|sample| format!("{}: {}", sample.label, sample.error.as_deref().unwrap_or_default()))
            .collect(),
    }
}

/// Populates victim KV keys and a small event table.
pub async fn prepare(api: &NodeApi, victim: &Owner) -> Result<(), String> {
    for key_index in 0..KEY_COUNT {
        api.put_value(&victim.token, &victim.db, &format!("k{key_index}"), vec![b'v'; 100]).await?;
    }
    api.create_event_table(&victim.token, &victim.db, EVENT_TABLE, "t", &["u", "e"]).await?;
    let rows: Vec<String> = (0..10_000)
        .map(|i| format!(r#"{{"t":{},"u":"u{}","e":"e{}"}}"#, 1_749_000_000_000i64 + i * 1000, i % 500, i % 8))
        .collect();
    api.bulk_insert(&victim.token, &victim.db, EVENT_TABLE, format!("[{}]", rows.join(","))).await
}

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
