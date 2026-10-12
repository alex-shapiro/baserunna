//! Simulation workloads and checkers (SPEC §13.3).

pub mod abandoned;
pub mod api_tour;
pub mod auth;
pub mod bank;
pub mod blob_gc_race;
pub mod blobs;
pub mod console;
pub mod db_churn;
pub mod kv_model;
pub mod lifecycle;
pub mod register;
pub mod schema_race;
pub mod tables;

use super::Sim;
use std::sync::Arc;

/// One random fault action, after a random pause.
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
        // Partitions are transient: heal often.
        5 | 7 | 8 => sim.heal(),
        6 => {
            // A long pause: leases expire, caches go stale.
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

/// Undo faults: heal partitions and restart every crashed node.
pub async fn recover(sim: &Arc<Sim>) {
    sim.heal();
    for i in 0..sim.node_count() {
        if sim.node(i).is_none() {
            sim.start(i).await;
        }
    }
    sim.sleep(2 * sim.cfg.engine.lease_ms).await;
}

/// Every owner's database count in the system database matches its
/// databases (read directly through a node's engine).
pub async fn check_db_counts(sim: &Arc<Sim>) -> Result<(), String> {
    let n = sim.up_nodes().first().copied().ok_or("no node up")?;
    let node = sim.node(n).ok_or("node gone")?.node.clone();
    let mut t = crate::node::retry_unavailable(|| node.sys_read_strong()).await.map_err(|e| e.to_string())?;
    crate::meta::check_db_counts(&mut t).await.map_err(|e| e.to_string())
}

/// Spawn a chaos loop; abort the returned handle to stop it.
pub fn spawn_chaos(sim: &Arc<Sim>) -> tokio::task::JoinHandle<()> {
    let sim = sim.clone();
    tokio::spawn(async move {
        loop {
            chaos_step(&sim).await;
        }
    })
}

/// Everything at once on separate databases.
pub async fn chaos_mix(sim: Arc<Sim>, scale: usize) -> Result<(), String> {
    let chaos = spawn_chaos(&sim);
    let a = tokio::spawn(kv_model::run(sim.clone(), 2, 20 * scale, false));
    let b = tokio::spawn(bank::run(sim.clone(), 5, 3, 6 * scale, false));
    let c = tokio::spawn(register::run(sim.clone(), 2, 2, 10 * scale, false));
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

/// A named simulation scenario: a cluster configuration and a workload.
pub struct Scenario {
    pub name: &'static str,
    pub config: fn() -> super::SimConfig,
    pub run: Runner,
}

fn faulty3() -> super::SimConfig {
    super::SimConfig::faulty(3)
}

fn faulty2() -> super::SimConfig {
    super::SimConfig::faulty(2)
}

fn faulty4() -> super::SimConfig {
    super::SimConfig::faulty(4)
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

/// Hostile, with node clocks hours apart and jumping by up to an hour:
/// collection runs on the store's clock, so grace periods of a second or
/// two must hold regardless.
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
    // Fault handling is covered elsewhere; here form posts (not
    // idempotent) must be unambiguous. Two nodes exercise forwarding.
    let mut c = super::SimConfig::basic();
    c.nodes = 2;
    c
}

/// Every scenario of the DST suite (used by `tests/sim.rs` and the
/// `sim_tool` example).
pub fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "kv_single_node",
            config: super::SimConfig::basic,
            run: |s| Box::pin(kv_model::run(s, 1, 120, false)),
        },
        Scenario { name: "kv_faulty_cluster", config: faulty3, run: |s| Box::pin(kv_model::run(s, 2, 60, true)) },
        Scenario { name: "bank", config: faulty3, run: |s| Box::pin(bank::run(s, 6, 4, 12, true)) },
        Scenario { name: "register", config: faulty3, run: |s| Box::pin(register::run(s, 3, 3, 30, true)) },
        Scenario { name: "tables", config: faulty3, run: |s| Box::pin(tables::run(s, 80, 3, true)) },
        Scenario { name: "blobs", config: faulty2, run: |s| Box::pin(blobs::run(s, 40, true)) },
        Scenario { name: "auth", config: faulty3, run: |s| Box::pin(auth::run(s)) },
        Scenario { name: "lifecycle", config: faulty3, run: |s| Box::pin(lifecycle::run(s, 4, true)) },
        Scenario { name: "console", config: console_cfg, run: |s| Box::pin(console::run(s)) },
        Scenario { name: "chaos", config: faulty4, run: |s| Box::pin(chaos_mix(s, 2)) },
        Scenario { name: "api_tour", config: faulty3, run: |s| Box::pin(api_tour::run(s)) },
        Scenario { name: "schema_race", config: faulty3, run: |s| Box::pin(schema_race::run(s, 3, 40, true)) },
        Scenario { name: "abandoned", config: faulty3, run: |s| Box::pin(abandoned::run(s, 4, 12, true)) },
        Scenario { name: "blob_gc_race", config: blob_gc_cfg, run: |s| Box::pin(blob_gc_race::run(s, 2, 3, 30, true)) },
        Scenario { name: "db_churn", config: churn_cfg, run: |s| Box::pin(db_churn::run(s, 4, 10, 60, true)) },
        Scenario { name: "kv_hostile", config: hostile3, run: |s| Box::pin(kv_model::run(s, 2, 60, true)) },
        Scenario { name: "bank_hostile", config: hostile3, run: |s| Box::pin(bank::run(s, 6, 4, 12, true)) },
        Scenario { name: "register_hostile", config: hostile3, run: |s| Box::pin(register::run(s, 3, 3, 30, true)) },
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
    ]
}

pub fn scenario(name: &str) -> Option<Scenario> {
    scenarios().into_iter().find(|s| s.name == name)
}
