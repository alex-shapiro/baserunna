//! Event scan benchmark from `event_scan_cpu` (tests/event_query_limits.rs), run over HTTP:
//! 1,000,000 events from 500,000 users over 10 days, then rounds of 7 queries.

use crate::cgroup::NodeCgroup;
use crate::http::{Api, Owner};
use bytes::Bytes;
use reqwest::Method;
use serde_json::{Value as Json, json};
use std::time::{Duration, Instant};

const DAY_MS: i64 = 86_400_000;
const QUERY_TO_MS: i64 = 9_999_999_999_999;
const FROM_MS: i64 = 1_749_000_000_000;
const ROWS: usize = 1_000_000;
const USERS: usize = 500_000;

fn queries() -> Vec<(&'static str, Json)> {
    let all = |mut q: Json| {
        q["from"] = json!(0);
        q["to"] = json!(QUERY_TO_MS);
        q
    };
    vec![
        ("count", all(json!({"aggregates": {"n": {"fn": "count"}}}))),
        ("count by event", all(json!({"group_by": ["event"], "aggregates": {"n": {"fn": "count"}}}))),
        (
            "top users",
            all(json!({"group_by": ["user"], "aggregates": {"n": {"fn": "count"}},
                "order_by": [{"col": "n", "desc": true}], "limit": 100})),
        ),
        ("unique users", all(json!({"aggregates": {"u": {"fn": "uniq", "of": {"col": "user"}}}}))),
        ("filtered count", all(json!({"where": {"col": "event", "op": "eq", "value": "e3"}, "aggregates": {"n": {"fn": "count"}}}))),
        ("filtered rows", all(json!({"where": {"col": "event", "op": "eq", "value": "e1"}, "limit": 1000}))),
        ("latest rows", all(json!({"order_by": [{"col": "time", "desc": true}], "limit": 100}))),
    ]
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    if v.is_empty() { 0.0 } else { v[v.len() / 2] }
}

/// Loads the table, runs `rounds` rounds of the 7 queries, and returns the measurements.
pub async fn run(api: &Api, owner: &Owner, cg: &NodeCgroup, rounds: usize) -> Result<Json, String> {
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
    for start in (0..ROWS).step_by(100_000) {
        let rows: Vec<String> = (start..ROWS.min(start + 100_000))
            .map(|i| {
                let time = FROM_MS + (i as i64 * 7_919) % span;
                format!(r#"{{"time": {time}, "user": "user-{:07}", "event": "e{}"}}"#, (i * 7_919) % USERS, i % 8)
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
    let queries = queries();
    for (name, q) in &queries {
        api.expect(Method::POST, &path, token, q.to_string(), &[200]).await.map_err(|e| format!("{name}: {e}"))?;
    }
    let mut wall_ms = vec![Vec::new(); queries.len()];
    let cpu_before = cg.cpu_usage_usec();
    let bench_started = Instant::now();
    for _ in 0..rounds {
        for ((name, q), spent) in queries.iter().zip(&mut wall_ms) {
            let t = Instant::now();
            api.send(Method::POST, &path, token, Bytes::from(q.to_string()))
                .await
                .ok()
                .filter(|r| r.status == 200)
                .ok_or_else(|| format!("{name} failed during the benchmark"))?;
            spent.push(t.elapsed().as_secs_f64() * 1000.0);
        }
    }
    let wall_s = bench_started.elapsed().as_secs_f64();
    let cpu_usec = match (cpu_before, cg.cpu_usage_usec()) {
        (Some(a), Some(b)) => Some(b - a),
        _ => None,
    };
    let per_query: Vec<Json> = queries
        .iter()
        .zip(&wall_ms)
        .map(|((name, _), ms)| json!({"query": name, "median_ms": median(ms.clone())}))
        .collect();
    Ok(json!({
        "rows": info["rows"], "parts": info["parts"].as_array().map_or(0, Vec::len), "stored_bytes": info["bytes"],
        "setup_s": setup_s, "rounds": rounds, "queries": queries.len(),
        "median_query_ms": median(wall_ms.concat()),
        "per_query": per_query,
        "bench_wall_s": wall_s,
        "node_cpu_usage_usec": cpu_usec,
    }))
}
