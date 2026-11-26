//! Idle resource cost benchmark for 1,000 inactive owners.
//!
//! Creates 1,000 owners, each with a database.
//! Runs a read and a write on each database.
//! Measures cgroup memory, CPU, and process counts over a 30-second idle window.

use crate::cgroup::{MemorySampler, NodeCgroup};
use crate::http::{NodeApi, Owner};
use baserunna_api::{GetValue, NoBody};
use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
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
    let setup_started = Instant::now();
    let owner_results: Vec<Result<Owner, String>> = stream::iter(0..OWNER_COUNT)
        .map(|owner_index| async move {
            let owner = api.create_owner(&format!("idle{owner_index}@iso.test"), &format!("i{owner_index}")).await?;
            api.put_value(&owner.token, &owner.db, "k0", Bytes::from_static(b"value-00")).await?;
            Ok(owner)
        })
        .buffer_unordered(SETUP_CONCURRENCY)
        .collect()
        .await;
    let owners = owner_results.into_iter().collect::<Result<Vec<_>, String>>()?;
    let setup_s = setup_started.elapsed().as_secs_f64();
    let memory_after_create_bytes = cgroup.memory_current_bytes();

    let touch_started = Instant::now();
    let touch_results: Vec<Result<(), String>> = stream::iter(&owners)
        .map(|owner| async move {
            let endpoint = GetValue { db: owner.db.clone(), key: Bytes::from_static(b"k0"), range: None };
            api.call(&endpoint, &owner.token, &NoBody {}).await?;
            api.put_value(&owner.token, &owner.db, "k0", Bytes::from_static(b"value-01")).await
        })
        .buffer_unordered(SETUP_CONCURRENCY)
        .collect()
        .await;
    touch_results.into_iter().collect::<Result<(), String>>()?;
    let touch_s = touch_started.elapsed().as_secs_f64();

    let memory_after_setup_bytes = cgroup.memory_current_bytes();
    let subtree_counts_start = cgroup.subtree_counts();
    let cpu_usec_start = cgroup.cpu_usage_usec();
    let sampler = MemorySampler::start_every(cgroup.clone(), SAMPLE_INTERVAL);
    let idle_started = Instant::now();
    tokio::time::sleep(IDLE_DURATION).await;
    let idle_s = idle_started.elapsed().as_secs_f64();
    let idle_cpu_usec = cgroup.cpu_usage_usec_since(cpu_usec_start);
    let memory = sampler.finish();
    let subtree_counts_end = cgroup.subtree_counts();
    let memory_end_bytes = cgroup.memory_current_bytes();
    let bytes_to_mib = |bytes: Option<u64>| bytes.map(|value| value as f64 / MIB);
    let summary = json!({
        "memory_before_setup_mib": bytes_to_mib(memory_before_setup_bytes),
        "memory_after_setup_mib": bytes_to_mib(memory_after_setup_bytes),
        "memory_idle_end_mib": bytes_to_mib(memory_end_bytes),
        "idle_cpu_usec_per_s": idle_cpu_usec.map(|cpu| cpu as f64 / idle_s),
        "processes_idle_start": subtree_counts_start.processes,
        "processes_idle_end": subtree_counts_end.processes,
    });
    Ok(json!({
        "owners": OWNER_COUNT, "dbs": OWNER_COUNT, "keys_per_db": 1,
        "setup_s": setup_s, "touch_s": touch_s, "idle_s": idle_s,
        "summary": summary,
        "memory_after_create_bytes": memory_after_create_bytes,
        "idle_memory": memory,
        "idle_cpu_usage_usec": idle_cpu_usec,
        "subtree_idle_start": subtree_counts_start,
        "subtree_idle_end": subtree_counts_end,
    }))
}
