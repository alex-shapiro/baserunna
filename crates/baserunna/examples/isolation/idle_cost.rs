//! Idle resource cost benchmark for 1,000 inactive owners.
//!
//! Creates 1,000 owners, each with a database, and runs a read and write on each database.
//! Then measures cgroup memory, CPU, and process counts over a 30-second idle window.

use crate::cgroup::{MemorySampler, NodeCgroup};
use crate::http::NodeApi;
use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use reqwest::Method;
use serde_json::{Value as Json, json};
use std::time::{Duration, Instant};

const OWNER_COUNT: usize = 1000;
const SETUP_CONCURRENCY: usize = 32;
const IDLE_DURATION: Duration = Duration::from_secs(30);
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const MIB: f64 = (1 << 20) as f64;

/// Runs the idle resource benchmark for 1,000 inactive owners and returns cgroup measurements.
pub async fn run(api: &NodeApi, cgroup: &NodeCgroup) -> Result<Json, String> {
    let memory_before_setup_bytes = cgroup.memory_current_bytes();
    let started = Instant::now();
    let results: Vec<Result<(String, String), String>> = stream::iter(0..OWNER_COUNT)
        .map(|owner_index| async move {
            let owner = api.create_owner(&format!("idle{owner_index}@iso.test"), &format!("i{owner_index}")).await?;
            let path = format!("/v1/dbs/{}/kv/k0", owner.db);
            api.send_expect(Method::PUT, &path, &owner.token, Bytes::from_static(b"value-00"), &[200]).await?;
            Ok((owner.token, path))
        })
        .buffer_unordered(SETUP_CONCURRENCY)
        .collect()
        .await;
    let owners = results.into_iter().collect::<Result<Vec<_>, String>>()?;
    let setup_s = started.elapsed().as_secs_f64();
    let memory_after_create_bytes = cgroup.memory_current_bytes();

    let touch_started = Instant::now();
    let touches: Vec<Result<(), String>> = stream::iter(&owners)
        .map(|(token, path)| async move {
            api.send_expect(Method::GET, path, token, Bytes::new(), &[200]).await?;
            api.send_expect(Method::PUT, path, token, Bytes::from_static(b"value-01"), &[200]).await.map(|_| ())
        })
        .buffer_unordered(SETUP_CONCURRENCY)
        .collect()
        .await;
    touches.into_iter().collect::<Result<(), String>>()?;
    let touch_s = touch_started.elapsed().as_secs_f64();

    let memory_after_setup_bytes = cgroup.memory_current_bytes();
    let counts_start = cgroup.subtree_counts();
    let cpu_usec_start = cgroup.cpu_usage_usec();
    let sampler = MemorySampler::start_every(cgroup.clone(), SAMPLE_INTERVAL);
    let idle_started = Instant::now();
    tokio::time::sleep(IDLE_DURATION).await;
    let idle_s = idle_started.elapsed().as_secs_f64();
    let cpu_usec_end = cgroup.cpu_usage_usec();
    let memory = sampler.finish();
    let counts_end = cgroup.subtree_counts();
    let memory_end_bytes = cgroup.memory_current_bytes();
    let idle_cpu_usec = match (cpu_usec_start, cpu_usec_end) {
        (Some(before), Some(after)) => Some(after.saturating_sub(before)),
        _ => None,
    };
    let bytes_to_mib = |bytes: Option<u64>| bytes.map(|value| value as f64 / MIB);
    let summary = json!({
        "memory_before_setup_mib": bytes_to_mib(memory_before_setup_bytes),
        "memory_after_setup_mib": bytes_to_mib(memory_after_setup_bytes),
        "memory_idle_end_mib": bytes_to_mib(memory_end_bytes),
        "idle_cpu_usec_per_s": idle_cpu_usec.map(|cpu| cpu as f64 / idle_s),
        "processes_idle_start": counts_start.processes,
        "processes_idle_end": counts_end.processes,
    });
    eprintln!("idle_cost: {summary}");
    Ok(json!({
        "owners": OWNER_COUNT, "dbs": OWNER_COUNT, "keys_per_db": 1,
        "setup_s": setup_s, "touch_s": touch_s, "idle_s": idle_s,
        "summary": summary,
        "memory_before_setup_bytes": memory_before_setup_bytes,
        "memory_after_create_bytes": memory_after_create_bytes,
        "memory_after_setup_bytes": memory_after_setup_bytes,
        "memory_idle_end_bytes": memory_end_bytes,
        "idle_memory": memory,
        "idle_cpu_usage_usec": idle_cpu_usec,
        "subtree_idle_start": counts_start,
        "subtree_idle_end": counts_end,
    }))
}
