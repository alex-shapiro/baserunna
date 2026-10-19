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
pub mod history;
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

use super::swarm::{Knobs, Profile, Skew, world};
use super::{Sim, SimConfig};
use std::sync::Arc;

/// Injects one random fault action, chosen by the weights in
/// [`super::SimConfig::chaos`], after a random pause.
pub async fn chaos_step(sim: &Arc<Sim>) {
    let c = sim.cfg.chaos.clone();
    sim.sleep(100 + sim.rand_below(c.pause_ms)).await;
    let n = sim.node_count();
    let up = sim.up_nodes();
    let weights =
        [c.crash, c.start, c.partition, c.one_way_partition, c.store_isolation, c.clock_step, c.heal, c.quiet];
    let mut r = sim.fork_rng();
    let Some(action) = super::swarm::weighted(&mut r, &weights) else {
        return;
    };
    let pick_up = |r: &mut crate::util::rng::Rng| up[r.usize(up.len())];
    let pair = |r: &mut crate::util::rng::Rng| {
        let a = r.usize(n);
        (a, (a + 1 + r.usize(n - 1)) % n)
    };
    match action {
        0 if n == 1 => sim.restart(pick_up(&mut r)).await,
        0 if up.len() > 1 => sim.crash(pick_up(&mut r)),
        1 => {
            if let Some(i) = (0..n).find(|i| !up.contains(i)) {
                sim.start(i).await;
            }
        }
        2 if n > 1 => {
            let (a, b) = pair(&mut r);
            sim.partition(a, b);
        }
        3 if n > 1 => {
            let (a, b) = pair(&mut r);
            sim.partition_one_way(a, b);
        }
        4 if !up.is_empty() => sim.isolate_from_store(pick_up(&mut r)),
        5 if c.clock_step_max_ms > 0 && !up.is_empty() => {
            let m = c.clock_step_max_ms;
            sim.step_clock(pick_up(&mut r), r.below(2 * m + 1) as i64 - m as i64);
        }
        6 => sim.heal(),
        7 => sim.sleep(r.below(20_000)).await,
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

/// Spawns a background chaos loop, idle unless the world enables chaos;
/// abort the returned handle to stop it.
pub fn spawn_chaos(sim: &Arc<Sim>) -> tokio::task::JoinHandle<()> {
    let sim = sim.clone();
    tokio::spawn(async move {
        if !sim.cfg.chaos.enabled {
            return;
        }
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

/// A named simulation scenario: a workload and the worlds it runs in.
pub struct Scenario {
    pub name: &'static str,
    /// Draws the world for a run from its knobs.
    pub config: fn(&Knobs) -> SimConfig,
    pub run: Runner,
}

impl Scenario {
    /// The world of this scenario's run with `seed`.
    pub fn config_for(&self, seed: u64) -> SimConfig {
        let knobs = Arc::new(Knobs::new(seed));
        SimConfig { knobs: Some(knobs.clone()), ..(self.config)(&knobs) }
    }
}

fn faulty(nodes: (usize, usize)) -> Profile {
    Profile { nodes, ..Profile::FAULTY }
}

/// Applies a size quota to some worlds, exercising quota refusals.
fn maybe_quota(k: &Knobs, mut c: SimConfig) -> SimConfig {
    if k.chance("kv_model.quota", 0.5) {
        c.engine.db_max_bytes = k.log_range("kv_model.quota_bytes", 8 * 1024, 64 * 1024);
        c.engine.quota_sweep_interval_ms = 10_000;
    }
    c
}

/// Returns all scenarios in the deterministic simulation suite.
pub fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "kv_model",
            config: |k| maybe_quota(k, world(k, faulty((1, 4)))),
            run: |s| {
                let k = &s.knobs;
                let (clients, steps) = (k.range("kv_model.clients", 1, 3), k.range("kv_model.steps", 30, 120));
                Box::pin(kv_model::run(s.clone(), clients as usize, steps as usize, true))
            },
        },
        Scenario {
            name: "history",
            config: |k| world(k, faulty((1, 5))),
            run: |s| Box::pin(history::run(s.clone(), history::Shape::draw(&s.knobs), true)),
        },
        Scenario {
            name: "bank",
            config: |k| world(k, faulty((2, 5))),
            run: |s| {
                let k = &s.knobs;
                let accounts = k.range("bank.accounts", 2, 12) as usize;
                let clients = k.range("bank.clients", 1, 8) as usize;
                let transfers = k.range("bank.transfers", 4, 16) as usize;
                Box::pin(bank::run(s.clone(), accounts, clients, transfers, true))
            },
        },
        Scenario {
            name: "register",
            config: |k| world(k, faulty((2, 5))),
            run: |s| {
                let k = &s.knobs;
                let writers = k.range("register.writers", 1, 6) as usize;
                let readers = k.range("register.readers", 1, 6) as usize;
                let ops = k.range("register.ops", 20, 60) as usize;
                let pause = k.pick("register.max_pause_ms", &[20, 100, 500]);
                Box::pin(register::run(s.clone(), writers, readers, ops, pause, true))
            },
        },
        Scenario {
            name: "tables",
            config: |k| world(k, faulty((1, 4))),
            run: |s| {
                let k = &s.knobs;
                let (steps, contenders) = (k.range("tables.steps", 40, 100), k.range("tables.contenders", 1, 4));
                Box::pin(tables::run(s.clone(), steps as usize, contenders as usize, true))
            },
        },
        Scenario {
            name: "blobs",
            config: |k| world(k, faulty((1, 3))),
            run: |s| Box::pin(blobs::run(s.clone(), s.knobs.range("blobs.rounds", 20, 50) as usize, true)),
        },
        Scenario { name: "auth", config: |k| world(k, faulty((1, 3))), run: |s| Box::pin(auth::run(s)) },
        Scenario {
            name: "lifecycle",
            config: |k| world(k, faulty((1, 4))),
            run: |s| Box::pin(lifecycle::run(s.clone(), s.knobs.range("lifecycle.cycles", 2, 6) as usize, true)),
        },
        Scenario {
            // Form posts are not idempotent, so the console runs without faults.
            name: "console",
            config: |k| world(k, Profile { nodes: (1, 3), faults: false, skew: Skew::None }),
            run: |s| Box::pin(console::run(s)),
        },
        Scenario {
            name: "chaos",
            config: |k| world(k, faulty((2, 5))),
            run: |s| Box::pin(chaos_mix(s.clone(), s.knobs.range("chaos.scale", 1, 3) as usize)),
        },
        Scenario { name: "limits", config: limits::config, run: |s| Box::pin(limits::run(s)) },
        Scenario { name: "api_tour", config: |k| world(k, faulty((1, 3))), run: |s| Box::pin(api_tour::run(s)) },
        Scenario {
            name: "schema_race",
            config: |k| world(k, faulty((1, 4))),
            run: |s| {
                let k = &s.knobs;
                let (writers, steps) = (k.range("schema_race.writers", 1, 5), k.range("schema_race.steps", 20, 60));
                Box::pin(schema_race::run(s.clone(), writers as usize, steps as usize, true))
            },
        },
        Scenario {
            name: "abandoned",
            config: |k| world(k, faulty((2, 4))),
            run: |s| {
                let k = &s.knobs;
                let (clients, txns) = (k.range("abandoned.clients", 2, 6), k.range("abandoned.txns", 6, 16));
                Box::pin(abandoned::run(s.clone(), clients as usize, txns as usize, true))
            },
        },
        Scenario {
            // Garbage collection uses the store's clock, so short grace
            // periods must hold even with node clocks an hour apart.
            name: "blob_gc_race",
            config: |k| blob_gc_race::config(world(k, Profile { skew: Skew::Hour, ..faulty((1, 4)) })),
            run: |s| {
                let k = &s.knobs;
                let writers = k.range("blob_gc_race.writers", 1, 3) as usize;
                let readers = k.range("blob_gc_race.readers", 1, 4) as usize;
                let rounds = k.range("blob_gc_race.rounds", 15, 40) as usize;
                Box::pin(blob_gc_race::run(s.clone(), writers, readers, rounds, true))
            },
        },
        Scenario {
            name: "db_churn",
            config: |k| db_churn::config(world(k, faulty((1, 4)))),
            run: |s| {
                let k = &s.knobs;
                let clients = k.range("db_churn.clients", 1, 5) as usize;
                let dbs = k.range("db_churn.dbs", 2, 12) as usize;
                let steps = k.range("db_churn.steps", 30, 80) as usize;
                Box::pin(db_churn::run(s.clone(), clients, dbs, steps, true))
            },
        },
        Scenario {
            name: "ttl",
            config: |k| world(k, faulty((1, 4))),
            run: |s| {
                let k = &s.knobs;
                let (owners, steps) = (k.range("ttl.owners", 1, 3), k.range("ttl.steps", 30, 90));
                Box::pin(ttl::run(s.clone(), owners as usize, steps as usize, true))
            },
        },
        Scenario {
            name: "watch",
            config: |k| world(k, faulty((1, 4))),
            run: |s| Box::pin(watch::run(s.clone(), s.knobs.range("watch.steps", 60, 150) as usize, true)),
        },
        Scenario {
            name: "streams",
            config: |k| world(k, faulty((1, 4))),
            run: |s| Box::pin(streams::run(s.clone(), s.knobs.range("streams.steps", 30, 80) as usize, true)),
        },
        Scenario { name: "orgs", config: |k| world(k, faulty((1, 4))), run: |s| Box::pin(orgs::run(s)) },
        Scenario { name: "accounts", config: |k| world(k, faulty((1, 4))), run: |s| Box::pin(accounts::run(s)) },
    ]
}

pub fn scenario(name: &str) -> Option<Scenario> {
    scenarios().into_iter().find(|s| s.name == name)
}
