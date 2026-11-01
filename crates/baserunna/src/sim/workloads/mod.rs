//! Simulation workloads and checkers.

pub mod abandoned;
pub mod accounts;
pub mod api_tour;
pub mod auth;
pub mod bank;
pub mod blob_gc_race;
pub mod blobs;
pub mod client;
pub mod console;
pub mod db_churn;
pub mod events;
pub mod events_churn;
pub mod events_retention;
pub mod history;
pub mod kv_model;
pub mod lifecycle;
pub mod limits;
pub mod memory;
pub mod orgs;
pub mod register;
pub mod schema_race;
pub mod steps;
pub mod streams;
pub mod tables;
pub mod ttl;
pub mod watch;

use super::client::Client;
use super::swarm::{Knobs, Profile, Skew, world};
use super::{Sim, SimConfig};
use std::sync::Arc;

/// Injects a random fault action selected according to [`super::SimConfig::chaos`].
pub async fn chaos_step(sim: &Arc<Sim>) {
    let c = sim.cfg.chaos.clone();
    sim.sleep(100 + sim.rand_below(c.pause_ms)).await;
    let n = sim.node_count();
    let up = sim.up_nodes();
    let weights = [
        c.crash,
        c.start,
        c.partition,
        c.one_way_partition,
        c.store_isolation,
        c.clock_step,
        c.heal,
        c.quiet,
        c.pause_node,
        c.stop,
    ];
    let mut r = sim.fork_rng();
    let Some(action) = r.weighted(&weights.map(|w| w as f64)) else {
        return;
    };
    let pick_up = |r: &mut crate::util::rng::Rng| up[r.usize(up.len())];
    let pair = |r: &mut crate::util::rng::Rng| {
        let a = r.usize(n);
        (a, (a + 1 + r.usize(n - 1)) % n)
    };
    match action {
        0 if n == 1 && !up.is_empty() => sim.restart(pick_up(&mut r)).await,
        0 if up.len() > 1 => sim.crash(pick_up(&mut r)),
        1 => {
            if let Some(i) = (0..n).find(|i| !up.contains(i)) {
                let sim = sim.clone();
                tokio::spawn(async move { sim.start(i).await });
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
        8 if !up.is_empty() => {
            let ms = r.below(3 * sim.cfg.engine.read_lease_ms);
            sim.pause_node(pick_up(&mut r), ms);
        }
        9 if up.len() > 1 => {
            let (sim, i) = (sim.clone(), pick_up(&mut r));
            tokio::spawn(async move { sim.stop(i).await });
        }
        _ => {}
    }
}

/// Returns a client authenticated as the cluster administrator, creating the account on first call.
pub async fn admin(sim: &Arc<Sim>) -> Result<Client, String> {
    let mut g = sim.admin.lock().await;
    if let Some(t) = g.as_ref() {
        return Ok(Client::with_token(sim, "root", t));
    }
    let c = Client::signup(sim, "root", ADMIN_EMAIL, ADMIN_PASSWORD).await?;
    *g = c.token.clone();
    Ok(c)
}

pub const ADMIN_EMAIL: &str = "root@sim.test";
pub const ADMIN_PASSWORD: &str = "root-password";

/// Heals network partitions, resumes paused nodes, and restarts crashed nodes.
pub async fn recover(sim: &Arc<Sim>) {
    sim.heal();
    sim.resume_all();
    for i in 0..sim.node_count() {
        if sim.node(i).is_none() {
            sim.start(i).await;
        }
    }
    sim.sleep(2 * sim.cfg.engine.lease_ms).await;
}

/// Verifies that the system database's metadata invariants hold.
pub async fn check_integrity(sim: &Arc<Sim>) -> Result<(), String> {
    let n = sim.up_nodes().first().copied().ok_or("all nodes are down")?;
    let node = sim.node(n).ok_or("node is down")?.node.clone();
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

/// Spawns a background chaos loop when fault injection is enabled.
pub fn spawn_chaos(sim: &Arc<Sim>) -> tokio::task::JoinHandle<()> {
    let sim = sim.clone();
    tokio::spawn(async move {
        let c = sim.cfg.chaos.clone();
        if !c.enabled {
            return;
        }
        // Alternate fault storms with recovery periods so the cluster can make progress.
        loop {
            let until = sim.now_ms().saturating_add(c.storm_ms);
            while sim.now_ms() < until {
                chaos_step(&sim).await;
            }
            if c.calm_ms > 0 {
                recover(&sim).await;
                sim.sleep(sim.rand_below(c.calm_ms)).await;
            }
        }
    })
}

/// Tracks active chaos phases and the shared fault-injection task.
#[derive(Default)]
pub struct ChaosState {
    active: usize,
    task: Option<tokio::task::JoinHandle<()>>,
    ended: u64,
}

/// Guard for a workload's fault-injection phase.
pub struct ChaosPhase {
    sim: Arc<Sim>,
    open: bool,
}

/// Starts a chaos phase for the calling workload.
pub fn chaos_phase(sim: &Arc<Sim>) -> ChaosPhase {
    let mut st = sim.chaos.lock();
    st.active += 1;
    if st.task.is_none() {
        crate::buggify::set_quiet(false);
        st.task = Some(spawn_chaos(sim));
    }
    ChaosPhase { sim: sim.clone(), open: true }
}

impl ChaosPhase {
    fn leave(&mut self) -> Option<u64> {
        self.open = false;
        let mut st = self.sim.chaos.lock();
        st.active -= 1;
        if st.active > 0 {
            return None;
        }
        if let Some(t) = &st.task {
            t.abort();
        }
        crate::buggify::set_quiet(true);
        Some(st.ended)
    }

    /// Ends the chaos phase and waits until all active phases finish and the cluster recovers.
    pub async fn end(mut self) {
        let sim = self.sim.clone();
        let over = sim.chaos_over.notified();
        let mut over = std::pin::pin!(over);
        over.as_mut().enable();
        let seen = sim.chaos.lock().ended;
        if self.leave().is_some() {
            recover_and_release(sim.clone()).await;
            return;
        }
        loop {
            if sim.chaos.lock().ended > seen {
                return;
            }
            over.as_mut().await;
            over.set(sim.chaos_over.notified());
            over.as_mut().enable();
        }
    }
}

impl Drop for ChaosPhase {
    fn drop(&mut self) {
        if self.open && self.leave().is_some() {
            tokio::spawn(recover_and_release(self.sim.clone()));
        }
    }
}

async fn recover_and_release(sim: Arc<Sim>) {
    recover(&sim).await;
    sim.chaos.lock().ended += 1;
    sim.chaos_over.notify_waiters();
}

/// Ends `phase` if present and recovers the cluster.
pub async fn settle(sim: &Arc<Sim>, phase: Option<ChaosPhase>) {
    match phase {
        Some(p) => p.end().await,
        None => recover(sim).await,
    }
}

/// Workloads eligible for concurrent execution in [`mix`].
const MIX: &[(&str, &str)] = &[
    ("kv_model", "mix.kv_model"),
    ("history", "mix.history"),
    ("bank", "mix.bank"),
    ("register", "mix.register"),
    ("tables", "mix.tables"),
    ("blobs", "mix.blobs"),
    ("auth", "mix.auth"),
    ("lifecycle", "mix.lifecycle"),
    ("api_tour", "mix.api_tour"),
    ("schema_race", "mix.schema_race"),
    ("abandoned", "mix.abandoned"),
    ("ttl", "mix.ttl"),
    ("watch", "mix.watch"),
    ("streams", "mix.streams"),
    ("events", "mix.events"),
    ("orgs", "mix.orgs"),
    ("accounts", "mix.accounts"),
    ("client", "mix.client"),
];

/// Runs a random subset of at least two workloads concurrently on a shared cluster.
pub async fn mix(sim: Arc<Sim>) -> Result<(), String> {
    let k = &sim.knobs;
    let mut chosen: Vec<&str> = MIX.iter().filter(|(_, knob)| k.chance(knob, 0.3)).map(|(n, _)| *n).collect();
    for (n, _) in MIX.iter().cycle().skip(k.range("mix.first", 0, MIX.len() as u64 - 1) as usize) {
        if chosen.len() >= 2 {
            break;
        }
        if !chosen.contains(n) {
            chosen.push(n);
        }
    }
    sim.trace.event(&format!("mix of {chosen:?}"));
    admin(&sim).await?;
    let runs: Vec<_> =
        chosen.iter().map(|n| (*n, (scenario(n).expect("mixed workloads are scenarios").run)(sim.clone()))).collect();
    join_all(runs).await
}

type Run = std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>>>>;

/// Polls all futures to completion and returns the first error.
async fn join_all(mut runs: Vec<(&str, Run)>) -> Result<(), String> {
    let mut done = vec![false; runs.len()];
    let mut first: Option<String> = None;
    std::future::poll_fn(|cx| {
        for ((name, f), d) in runs.iter_mut().zip(done.iter_mut()) {
            if !*d && let std::task::Poll::Ready(r) = f.as_mut().poll(cx) {
                *d = true;
                if let Err(e) = r {
                    first.get_or_insert(format!("{name}: {e}"));
                }
            }
        }
        match done.iter().all(|d| *d) {
            true => std::task::Poll::Ready(()),
            false => std::task::Poll::Pending,
        }
    })
    .await;
    first.map_or(Ok(()), Err)
}

pub type Runner = fn(Arc<Sim>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>>>>;

/// Simulation scenario definition.
pub struct Scenario {
    pub name: &'static str,
    pub config: fn(&Knobs) -> SimConfig,
    pub run: Runner,
}

impl Scenario {
    pub fn config_for(&self, seed: u64) -> SimConfig {
        let knobs = Arc::new(Knobs::new(seed));
        SimConfig { knobs: Some(knobs.clone()), ..(self.config)(&knobs) }
    }
}

fn faulty(nodes: (usize, usize)) -> Profile {
    Profile { nodes, ..Profile::FAULTY }
}

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
            // Form submissions are not idempotent, so the console scenario runs without fault injection.
            name: "console",
            config: |k| world(k, Profile { nodes: (1, 3), faults: false, skew: Skew::None }),
            run: |s| Box::pin(console::run(s)),
        },
        Scenario { name: "mix", config: |k| world(k, faulty((2, 5))), run: |s| Box::pin(mix(s)) },
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
            // Garbage collection uses the object store clock and must tolerate large node clock skew.
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
        Scenario {
            name: "events",
            config: |k| {
                let mut c = world(k, faulty((1, 4)));
                c.engine.event_granule_rows = k.log_range("events.granule_rows", 2, 16) as usize;
                events::short_gc(k, &mut c);
                c.node.engine = c.engine.clone();
                c
            },
            run: |s| Box::pin(events::run(s.clone(), s.knobs.range("events.steps", 10, 40) as usize, true)),
        },
        Scenario { name: "orgs", config: |k| world(k, faulty((1, 4))), run: |s| Box::pin(orgs::run(s)) },
        Scenario { name: "memory", config: memory::config, run: |s| Box::pin(memory::run(s, true)) },
        Scenario { name: "accounts", config: |k| world(k, faulty((1, 4))), run: |s| Box::pin(accounts::run(s)) },
        Scenario {
            name: "client",
            config: |k| world(k, faulty((1, 4))),
            run: |s| {
                let k = &s.knobs;
                let size = client::Size {
                    counters: k.range("client.counters", 1, 3) as usize,
                    mutators: k.range("client.mutators", 1, 3) as usize,
                    ledgers: k.range("client.ledgers", 1, 3) as usize,
                    steps: k.range("client.steps", 20, 60) as usize,
                };
                Box::pin(client::run(s.clone(), size, true))
            },
        },
    ]
}

pub fn scenario(name: &str) -> Option<Scenario> {
    scenarios().into_iter().find(|s| s.name == name)
}
