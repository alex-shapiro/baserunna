//! Simulation workloads and checkers.

pub mod abandoned;
pub mod accounts;
pub mod api_tour;
pub mod auth;
pub mod bank;
pub mod blob_gc_race;
pub mod blobs;
pub mod console;
pub mod db_churn;
pub mod kv_model;
pub mod lifecycle;
pub mod limits;
pub mod orgs;
pub mod register;
pub mod schema_race;
pub mod steps;
pub mod streams;
pub mod tables;
pub mod ttl;
pub mod watch;

use super::Sim;
use std::sync::Arc;

/// Injects one random fault action after a random pause.
pub async fn chaos_step(sim: &Arc<Sim>) {
    sim.sleep(500 + sim.rand_below(4_000)).await;
    let n = sim.node_count();
    let up = sim.up_nodes();
    let extra = &sim.cfg.chaos;
    match sim.rand_below(16) {
        0 | 1 if up.len() > 1 || n == 1 => {
            let i = up[sim.rand_below(up.len() as u64) as usize];
            if n == 1 {
                sim.restart(i).await;
            } else {
                sim.crash(i);
            }
        }
        2 | 3 => {
            if let Some(i) = (0..n).find(|i| !up.contains(i)) {
                sim.start(i).await;
            }
        }
        4 if n > 1 => {
            let a = sim.rand_below(n as u64) as usize;
            let b = (a + 1 + sim.rand_below(n as u64 - 1) as usize) % n;
            sim.partition(a, b);
        }
        5 | 7 | 8 => sim.heal(),
        6 => {
            sim.sleep(sim.rand_below(20_000)).await;
        }
        9 if n > 1 && extra.one_way_partitions => {
            let a = sim.rand_below(n as u64) as usize;
            let b = (a + 1 + sim.rand_below(n as u64 - 1) as usize) % n;
            sim.partition_one_way(a, b);
        }
        10 if extra.store_isolation && !up.is_empty() => {
            sim.isolate_from_store(up[sim.rand_below(up.len() as u64) as usize]);
        }
        11 if extra.clock_step_max_ms > 0 && !up.is_empty() => {
            let i = up[sim.rand_below(up.len() as u64) as usize];
            let m = extra.clock_step_max_ms;
            sim.step_clock(i, sim.rand_below(2 * m + 1) as i64 - m as i64);
        }
        _ => {}
    }
}

/// Heals network partitions and restarts all crashed nodes.
pub async fn recover(sim: &Arc<Sim>) {
    sim.heal();
    for i in 0..sim.node_count() {
        if sim.node(i).is_none() {
            sim.start(i).await;
        }
    }
    sim.sleep(2 * sim.cfg.engine.lease_ms).await;
}

/// Verifies that the system database's metadata invariants hold.
pub async fn check_integrity(sim: &Arc<Sim>) -> Result<(), String> {
    let n = sim.up_nodes().first().copied().ok_or("no node up")?;
    let node = sim.node(n).ok_or("node gone")?.node.clone();
    let mut t = crate::node::retry_unavailable(|| node.sys_read_strong()).await.map_err(|e| e.to_string())?;
    crate::meta::check_integrity(&mut t).await.map_err(|e| e.to_string())
}

/// Verifies that all objects for the deleted databases `ids` are purged from the store.
pub async fn check_purged(sim: &Arc<Sim>, ids: &[String]) -> Result<(), String> {
    sim.sleep(super::purge_wait_ms(&sim.cfg)).await;
    let keys = sim.mem.keys();
    for id in ids {
        let prefix = format!("dbs/{id}/");
        if let Some(k) = keys.iter().find(|k| k.starts_with(&prefix)) {
            return Err(format!("deleted database {id} not purged: {k} remains"));
        }
    }
    Ok(())
}

/// Returns `Err(msg)` unless `cond` is true.
pub fn expect(cond: bool, msg: impl Into<String>) -> Result<(), String> {
    if cond { Ok(()) } else { Err(msg.into()) }
}

/// Maximum duration a node may act on revoked credentials or roles due to
/// auth cache TTL and snapshot staleness.
pub fn cache_lag(sim: &Sim) -> u64 {
    sim.cfg.node.auth_cache_ms + sim.cfg.node.sys_staleness_ms + 100
}

/// Waits until cached credentials and roles from before now have expired.
pub async fn cache_expiry(sim: &Arc<Sim>) {
    sim.sleep(cache_lag(sim)).await;
}

/// Spawns a background chaos loop; abort the returned handle to stop it.
pub fn spawn_chaos(sim: &Arc<Sim>) -> tokio::task::JoinHandle<()> {
    let sim = sim.clone();
    tokio::spawn(async move {
        loop {
            chaos_step(&sim).await;
        }
    })
}

/// Runs KV, bank, and register workloads concurrently under faults.
pub async fn chaos_mix(sim: Arc<Sim>, scale: usize) -> Result<(), String> {
    let chaos = spawn_chaos(&sim);
    let a = tokio::spawn(kv_model::run(sim.clone(), 2, 20 * scale, false));
    let b = tokio::spawn(bank::run(sim.clone(), 5, 3, 6 * scale, false));
    let c = tokio::spawn(register::run(sim.clone(), 2, 2, 10 * scale, 500, false));
    let mut errs = Vec::new();
    for h in [a, b, c] {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => errs.push(e),
            Err(e) => errs.push(format!("panicked: {e}")),
        }
    }
    chaos.abort();
    if let Some(e) = errs.into_iter().next() {
        return Err(e);
    }
    recover(&sim).await;
    Ok(())
}

pub type Runner = fn(Arc<Sim>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>>>>;

/// A named simulation scenario pairing a cluster configuration with a workload.
pub struct Scenario {
    pub name: &'static str,
    pub config: fn() -> super::SimConfig,
    pub run: Runner,
}

fn faulty3() -> super::SimConfig {
    super::SimConfig::faulty(3)
}

/// Applies small storage quotas to exercise quota limits in kv_model workloads.
fn with_quota(mut c: super::SimConfig) -> super::SimConfig {
    c.engine.db_max_bytes = 24 * 1024;
    c.engine.quota_sweep_interval_ms = 10_000;
    c
}

fn kv_basic() -> super::SimConfig {
    with_quota(super::SimConfig::basic())
}

fn kv_faulty3() -> super::SimConfig {
    with_quota(super::SimConfig::faulty(3))
}

fn kv_hostile3() -> super::SimConfig {
    with_quota(super::SimConfig::hostile(3))
}

fn faulty2() -> super::SimConfig {
    super::SimConfig::faulty(2)
}

fn faulty4() -> super::SimConfig {
    super::SimConfig::faulty(4)
}

/// `c` where writes often reply late, so a manifest write stays in flight
/// long after the store applied it, while other nodes can already read it.
fn late_replies(mut c: super::SimConfig) -> super::SimConfig {
    c.store.p_late_reply = 0.3;
    c.store.late_reply_ms = (50, 1_000);
    c
}

fn contended3() -> super::SimConfig {
    late_replies(super::SimConfig::faulty(3))
}

fn contended_hostile3() -> super::SimConfig {
    late_replies(super::SimConfig::hostile(3))
}

fn hostile3() -> super::SimConfig {
    super::SimConfig::hostile(3)
}

fn hostile4() -> super::SimConfig {
    super::SimConfig::hostile(4)
}

fn blob_gc_cfg() -> super::SimConfig {
    blob_gc_race::config(super::SimConfig::faulty(3))
}

/// Configures a hostile cluster with node clocks skewed by up to an hour.
/// Garbage collection uses the store's clock, so short grace periods must still hold.
fn blob_gc_hostile_cfg() -> super::SimConfig {
    let mut c = blob_gc_race::config(super::SimConfig::hostile(3));
    c.max_skew_ms = 3_600_000;
    c.chaos.clock_step_max_ms = 3_600_000;
    c
}

fn churn_cfg() -> super::SimConfig {
    db_churn::config(super::SimConfig::faulty(3))
}

fn churn_hostile_cfg() -> super::SimConfig {
    db_churn::config(super::SimConfig::hostile(3))
}

fn console_cfg() -> super::SimConfig {
    // Form posts are not idempotent, so run without fault injection across two nodes to exercise forwarding.
    let mut c = super::SimConfig::basic();
    c.nodes = 2;
    c
}

/// Returns all scenarios in the deterministic simulation suite.
pub fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario { name: "kv_single_node", config: kv_basic, run: |s| Box::pin(kv_model::run(s, 1, 120, false)) },
        Scenario { name: "kv_faulty_cluster", config: kv_faulty3, run: |s| Box::pin(kv_model::run(s, 2, 60, true)) },
        Scenario { name: "bank", config: faulty3, run: |s| Box::pin(bank::run(s, 6, 4, 12, true)) },
        Scenario { name: "register", config: faulty3, run: |s| Box::pin(register::run(s, 3, 3, 30, 500, true)) },
        Scenario {
            name: "register_contended",
            config: contended3,
            run: |s| Box::pin(register::run(s, 6, 6, 60, 20, true)),
        },
        Scenario { name: "tables", config: faulty3, run: |s| Box::pin(tables::run(s, 80, 3, true)) },
        Scenario { name: "blobs", config: faulty2, run: |s| Box::pin(blobs::run(s, 40, true)) },
        Scenario { name: "auth", config: faulty3, run: |s| Box::pin(auth::run(s)) },
        Scenario { name: "lifecycle", config: faulty3, run: |s| Box::pin(lifecycle::run(s, 4, true)) },
        Scenario { name: "console", config: console_cfg, run: |s| Box::pin(console::run(s)) },
        Scenario { name: "chaos", config: faulty4, run: |s| Box::pin(chaos_mix(s, 2)) },
        Scenario { name: "limits", config: limits::config, run: |s| Box::pin(limits::run(s)) },
        Scenario { name: "api_tour", config: faulty3, run: |s| Box::pin(api_tour::run(s)) },
        Scenario { name: "schema_race", config: faulty3, run: |s| Box::pin(schema_race::run(s, 3, 40, true)) },
        Scenario { name: "abandoned", config: faulty3, run: |s| Box::pin(abandoned::run(s, 4, 12, true)) },
        Scenario { name: "blob_gc_race", config: blob_gc_cfg, run: |s| Box::pin(blob_gc_race::run(s, 2, 3, 30, true)) },
        Scenario { name: "db_churn", config: churn_cfg, run: |s| Box::pin(db_churn::run(s, 4, 10, 60, true)) },
        Scenario { name: "ttl", config: faulty3, run: |s| Box::pin(ttl::run(s, 2, 60, true)) },
        Scenario { name: "watch", config: faulty3, run: |s| Box::pin(watch::run(s, 120, true)) },
        Scenario { name: "streams", config: faulty3, run: |s| Box::pin(streams::run(s, 60, true)) },
        Scenario { name: "orgs", config: faulty3, run: |s| Box::pin(orgs::run(s)) },
        Scenario { name: "accounts", config: faulty3, run: |s| Box::pin(accounts::run(s)) },
        Scenario { name: "kv_hostile", config: kv_hostile3, run: |s| Box::pin(kv_model::run(s, 2, 60, true)) },
        Scenario { name: "bank_hostile", config: hostile3, run: |s| Box::pin(bank::run(s, 6, 4, 12, true)) },
        Scenario {
            name: "register_hostile",
            config: hostile3,
            run: |s| Box::pin(register::run(s, 3, 3, 30, 500, true)),
        },
        Scenario {
            name: "register_contended_hostile",
            config: contended_hostile3,
            run: |s| Box::pin(register::run(s, 6, 6, 60, 20, true)),
        },
        Scenario { name: "tables_hostile", config: hostile3, run: |s| Box::pin(tables::run(s, 80, 3, true)) },
        Scenario { name: "blobs_hostile", config: hostile3, run: |s| Box::pin(blobs::run(s, 40, true)) },
        Scenario { name: "lifecycle_hostile", config: hostile3, run: |s| Box::pin(lifecycle::run(s, 4, true)) },
        Scenario { name: "schema_race_hostile", config: hostile3, run: |s| Box::pin(schema_race::run(s, 3, 40, true)) },
        Scenario { name: "abandoned_hostile", config: hostile3, run: |s| Box::pin(abandoned::run(s, 4, 12, true)) },
        Scenario {
            name: "blob_gc_race_hostile",
            config: blob_gc_hostile_cfg,
            run: |s| Box::pin(blob_gc_race::run(s, 2, 3, 30, true)),
        },
        Scenario {
            name: "db_churn_hostile",
            config: churn_hostile_cfg,
            run: |s| Box::pin(db_churn::run(s, 4, 10, 60, true)),
        },
        Scenario { name: "chaos_hostile", config: hostile4, run: |s| Box::pin(chaos_mix(s, 2)) },
        Scenario { name: "ttl_hostile", config: hostile3, run: |s| Box::pin(ttl::run(s, 2, 60, true)) },
        Scenario { name: "watch_hostile", config: hostile3, run: |s| Box::pin(watch::run(s, 120, true)) },
        Scenario { name: "streams_hostile", config: hostile3, run: |s| Box::pin(streams::run(s, 60, true)) },
        Scenario { name: "orgs_hostile", config: hostile3, run: |s| Box::pin(orgs::run(s)) },
        Scenario { name: "accounts_hostile", config: hostile3, run: |s| Box::pin(accounts::run(s)) },
    ]
}

pub fn scenario(name: &str) -> Option<Scenario> {
    scenarios().into_iter().find(|s| s.name == name)
}
