//! HTTP event scan benchmark.
//!
//! Inserts 1,000,000 events for 500,000 users across 10 days, then runs seven queries per round.

use crate::cgroup::NodeCgroup;
use crate::http::{NodeApi, Owner};
use bytes::Bytes;
use reqwest::Method;
use serde_json::{Value as Json, json};
use std::time::{Duration, Instant};

const DAY_MS: i64 = 86_400_000;
const QUERY_END_MS: i64 = 9_999_999_999_999;
const START_TIME_MS: i64 = 1_749_000_000_000;
const EVENT_ROW_COUNT: usize = 1_000_000;
const USER_COUNT: usize = 500_000;

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
    if values.is_empty() { 0.0 } else { values[values.len() / 2] }
}

/// Loads the event table, runs `rounds` query rounds, and returns timing results.
///
/// Failed warm-up and benchmark queries increment `failed_queries` and are excluded from medians.
pub async fn run(api: &NodeApi, owner: &Owner, cgroup: &NodeCgroup, rounds: usize) -> Result<Json, String> {
    let (token, db) = (&owner.token, &owner.db);
    api.create_event_table(
        token,
        db,
        json!({"name": "ev", "columns": [{"name": "time", "type": "time"}, {"name": "user", "type": "string"},
            {"name": "event", "type": "string"}], "time_column": "time", "order_by": ["time"]}),
    )
    .await?;
    let started = Instant::now();
    let span = 10 * DAY_MS;
    for start in (0..EVENT_ROW_COUNT).step_by(100_000) {
        let rows: Vec<String> = (start..EVENT_ROW_COUNT.min(start + 100_000))
            .map(|i| {
                let time = START_TIME_MS + (i as i64 * 7_919) % span;
                format!(r#"{{"time": {time}, "user": "user-{:07}", "event": "e{}"}}"#, (i * 7_919) % USER_COUNT, i % 8)
            })
            .collect();
        api.bulk_insert(token, db, "ev", format!("[{}]", rows.join(","))).await?;
    }
    let mut parts = api.part_count(token, db, "ev").await?;
    let mut unchanged = 0;
    while unchanged < 5 && started.elapsed() < Duration::from_secs(60) {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now = api.part_count(token, db, "ev").await?;
        unchanged = if now == parts { unchanged + 1 } else { 0 };
        parts = now;
    }
    let info = api.table_info(token, db, "ev").await?;
    let setup_s = started.elapsed().as_secs_f64();
    let path = format!("/v1/dbs/{db}/events/ev/rows/query");
    let queries = benchmark_queries();
    let mut failures = Vec::new();
    for (name, query) in &queries {
        if let Err(err) = api.send_expect(Method::POST, &path, token, query.to_string(), &[200]).await {
            failures.push(format!("warm-up {name}: {err}"));
        }
    }
    let mut wall_ms = vec![Vec::new(); queries.len()];
    let cpu_before = cgroup.cpu_usage_usec();
    let bench_started = Instant::now();
    for _ in 0..rounds {
        for ((name, query), spent) in queries.iter().zip(&mut wall_ms) {
            let query_started = Instant::now();
            let result = api.send(Method::POST, &path, token, Bytes::from(query.to_string())).await;
            match result {
                Ok(reply) if reply.status == 200 => spent.push(query_started.elapsed().as_secs_f64() * 1000.0),
                Ok(reply) => failures.push(format!("{name}: status {}: {}", reply.status, reply.excerpt())),
                Err(err) => failures.push(format!("{name}: {err}")),
            }
        }
    }
    let wall_s = bench_started.elapsed().as_secs_f64();
    let cpu_usec = match (cpu_before, cgroup.cpu_usage_usec()) {
        (Some(before), Some(after)) => Some(after - before),
        _ => None,
    };
    let per_query: Vec<Json> = queries
        .iter()
        .zip(&wall_ms)
        .map(|((name, _), ms)| json!({"query": name, "median_ms": median(ms.clone()), "successes": ms.len()}))
        .collect();
    Ok(json!({
        "rows": info["rows"], "parts": info["parts"].as_array().map_or(0, Vec::len), "stored_bytes": info["bytes"],
        "setup_s": setup_s, "rounds": rounds, "queries": queries.len(),
        "median_query_ms": median(wall_ms.concat()),
        "per_query": per_query,
        "bench_wall_s": wall_s,
        "node_cpu_usage_usec": cpu_usec,
        "failed_queries": failures.len(),
        "failure_samples": failures.iter().take(5).collect::<Vec<_>>(),
    }))
}
