//! Idle cost of many tenants: 1,000 owners each touch one database once, then all stay idle for
//! 30 s while the harness reads the node cgroup's memory, CPU and processes.

use crate::cgroup::{MemorySampler, NodeCgroup};
use crate::http::Api;
use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use reqwest::Method;
use serde_json::{Value as Json, json};
use std::time::{Duration, Instant};

const OWNERS: usize = 1000;
const SETUP_CONCURRENCY: usize = 32;
const IDLE: Duration = Duration::from_secs(30);
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const MIB: f64 = (1 << 20) as f64;

/// Creates the owners and their databases, touches each database, and measures the idle window.
pub async fn run(api: &Api, cg: &NodeCgroup) -> Result<Json, String> {
    let memory_before_setup = cg.memory_current();
    let started = Instant::now();
    let results: Vec<Result<(String, String), String>> = stream::iter(0..OWNERS)
        .map(|o| async move {
            let owner = api.create_owner(&format!("idle{o}@iso.test"), &format!("i{o}")).await?;
            let path = format!("/v1/dbs/{}/kv/k0", owner.db);
            api.expect(Method::PUT, &path, &owner.token, Bytes::from_static(b"value-00"), &[200]).await?;
            Ok((owner.token, path))
        })
        .buffer_unordered(SETUP_CONCURRENCY)
        .collect()
        .await;
    let owners = results.into_iter().collect::<Result<Vec<_>, String>>()?;
    let setup_s = started.elapsed().as_secs_f64();
    let memory_after_create = cg.memory_current();

    let touched = Instant::now();
    let touches: Vec<Result<(), String>> = stream::iter(&owners)
        .map(|(token, path)| async move {
            api.expect(Method::GET, path, token, Bytes::new(), &[200]).await?;
            api.expect(Method::PUT, path, token, Bytes::from_static(b"value-01"), &[200]).await.map(|_| ())
        })
        .buffer_unordered(SETUP_CONCURRENCY)
        .collect()
        .await;
    touches.into_iter().collect::<Result<(), String>>()?;
    let touch_s = touched.elapsed().as_secs_f64();

    let memory_after_setup = cg.memory_current();
    let census_start = cg.subtree_census();
    let cpu_start = cg.cpu_usage_usec();
    let sampler = MemorySampler::start_every(cg.clone(), SAMPLE_INTERVAL);
    let idle_started = Instant::now();
    tokio::time::sleep(IDLE).await;
    let idle_s = idle_started.elapsed().as_secs_f64();
    let cpu_end = cg.cpu_usage_usec();
    let memory = sampler.finish();
    let census_end = cg.subtree_census();
    let memory_end = cg.memory_current();
    let idle_cpu_usec = match (cpu_start, cpu_end) {
        (Some(a), Some(b)) => Some(b.saturating_sub(a)),
        _ => None,
    };
    let mib = |b: Option<u64>| b.map(|b| b as f64 / MIB);
    let summary = json!({
        "memory_before_setup_mib": mib(memory_before_setup),
        "memory_after_setup_mib": mib(memory_after_setup),
        "memory_idle_end_mib": mib(memory_end),
        "idle_cpu_usec_per_s": idle_cpu_usec.map(|c| c as f64 / idle_s),
        "processes_idle_start": census_start.processes,
        "processes_idle_end": census_end.processes,
    });
    eprintln!("idle_cost: {summary}");
    Ok(json!({
        "owners": OWNERS, "dbs": OWNERS, "keys_per_db": 1,
        "setup_s": setup_s, "touch_s": touch_s, "idle_s": idle_s,
        "summary": summary,
        "memory_before_setup_bytes": memory_before_setup,
        "memory_after_create_bytes": memory_after_create,
        "memory_after_setup_bytes": memory_after_setup,
        "memory_idle_end_bytes": memory_end,
        "idle_memory": memory,
        "idle_cpu_usage_usec": idle_cpu_usec,
        "subtree_idle_start": census_start,
        "subtree_idle_end": census_end,
    }))
}
