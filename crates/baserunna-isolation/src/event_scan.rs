//! HTTP event scan benchmark.
//!
//! Inserts 1,000,000 events for 500,000 users across 10 days. Runs 7 queries in each round.

use crate::cgroup::NodeCgroup;
use crate::http::{NodeApi, Owner};
use crate::percentile;
use bytes::Bytes;
use reqwest::Method;
use serde_json::{Value as Json, json};
use std::time::Instant;

const DAY_MS: i64 = 86_400_000;
const QUERY_END_MS: i64 = 9_999_999_999_999;
const START_TIME_MS: i64 = 1_749_000_000_000;
const EVENT_ROW_COUNT: usize = 1_000_000;
const USER_COUNT: usize = 500_000;

/// Loads the event table and runs the query suite `rounds` times. Returns timing results as JSON.
///
/// Counts failed warm-up and benchmark queries in `failed_queries`. Omits failed queries from
/// medians.
pub async fn run(api: &NodeApi, owner: &Owner, cgroup: &NodeCgroup, rounds: usize) -> Result<Json, String> {
    let (token, db) = (&owner.token, &owner.db);
    api.create_event_table(token, db, "ev", "time", &["user", "event"]).await?;
    let setup_started = Instant::now();
    let time_span_ms = 10 * DAY_MS;
    for batch_start in (0..EVENT_ROW_COUNT).step_by(100_000) {
        let rows: Vec<String> = (batch_start..EVENT_ROW_COUNT.min(batch_start + 100_000))
            .map(|i| {
                let time = START_TIME_MS + (i as i64 * 7_919) % time_span_ms;
                format!(r#"{{"time": {time}, "user": "user-{:07}", "event": "e{}"}}"#, (i * 7_919) % USER_COUNT, i % 8)
            })
            .collect();
        api.bulk_insert(token, db, "ev", format!("[{}]", rows.join(","))).await?;
    }
    let table_info = api.wait_for_stable_parts(token, db, "ev").await?;
    let setup_s = setup_started.elapsed().as_secs_f64();
    let path = format!("/v1/dbs/{db}/events/ev/rows/query");
    let queries = benchmark_queries();
    let mut failures = Vec::new();
    for (name, query) in &queries {
        if let Err(err) = api.send_expect(Method::POST, &path, token, query.to_string(), &[200]).await {
            failures.push(format!("warm-up {name}: {err}"));
        }
    }
    let mut query_latencies_ms = vec![Vec::new(); queries.len()];
    let cpu_usec_before = cgroup.cpu_usage_usec();
    let bench_started = Instant::now();
    for _ in 0..rounds {
        for ((name, query), latencies_ms) in queries.iter().zip(&mut query_latencies_ms) {
            let body = Bytes::from(query.to_string());
            let record = api.send_timed(name, Method::POST, &path, token, body, &[200]).await;
            if record.expected {
                latencies_ms.push(record.elapsed_ms);
            } else {
                failures.push(format!("{name}: status {:?}: {}", record.status, record.error.unwrap_or_default()));
            }
        }
    }
    let wall_s = bench_started.elapsed().as_secs_f64();
    let cpu_usec = cgroup.cpu_usage_usec_since(cpu_usec_before);
    let per_query: Vec<Json> = queries
        .iter()
        .zip(&query_latencies_ms)
        .map(|((name, _), latencies_ms)| {
            json!({"query": name, "median_ms": median(latencies_ms.clone()), "successes": latencies_ms.len()})
        })
        .collect();
    Ok(json!({
        "rows": table_info["rows"], "parts": table_info["parts"].as_array().map_or(0, Vec::len),
        "stored_bytes": table_info["bytes"],
        "setup_s": setup_s, "rounds": rounds, "query_count": queries.len(),
        "median_query_ms": median(query_latencies_ms.concat()),
        "per_query": per_query,
        "bench_wall_s": wall_s,
        "node_cpu_usage_usec": cpu_usec,
        "failed_queries": failures.len(),
        "failure_samples": failures.iter().take(5).collect::<Vec<_>>(),
    }))
}

fn benchmark_queries() -> Vec<(&'static str, Json)> {
    let with_full_time_range = |mut query: Json| {
        query["from"] = json!(0);
        query["to"] = json!(QUERY_END_MS);
        query
    };
    vec![
        ("count", with_full_time_range(json!({"aggregates": {"n": {"fn": "count"}}}))),
        ("count by event", with_full_time_range(json!({"group_by": ["event"], "aggregates": {"n": {"fn": "count"}}}))),
        (
            "top users",
            with_full_time_range(json!({"group_by": ["user"], "aggregates": {"n": {"fn": "count"}},
                "order_by": [{"col": "n", "desc": true}], "limit": 100})),
        ),
        ("unique users", with_full_time_range(json!({"aggregates": {"u": {"fn": "uniq", "of": {"col": "user"}}}}))),
        (
            "filtered count",
            with_full_time_range(
                json!({"where": {"col": "event", "op": "eq", "value": "e3"}, "aggregates": {"n": {"fn": "count"}}}),
            ),
        ),
        (
            "filtered rows",
            with_full_time_range(json!({"where": {"col": "event", "op": "eq", "value": "e1"}, "limit": 1000})),
        ),
        ("latest rows", with_full_time_range(json!({"order_by": [{"col": "time", "desc": true}], "limit": 100}))),
    ]
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    percentile(&values, 0.5)
}
