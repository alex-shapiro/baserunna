//! Deterministic simulation of a Baserunna cluster (SPEC §13).
//!
//! Everything — every node, the object store, the network between nodes and
//! clients, and Google — runs on one current-thread tokio runtime with
//! paused (virtual) time, and all randomness flows from one seed. Running a
//! seed twice yields an identical trace hash.

pub mod client;
pub mod google;
pub mod openapi_check;
pub mod store;
pub mod workloads;

use crate::api;
use crate::cluster::{Transport, TransportError};
use crate::engine::manifest::Manifest;
use crate::engine::{EngineConfig, Value, segment};
use crate::env::Env;
use crate::meta::SignupPolicy;
use crate::node::{Node, NodeConfig};
use crate::store::{BoxFut, DynStore, MemStore};
use crate::util::rng::Rng;
use axum::body::Body;
use bytes::Bytes;
use http::{Request, Response};
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use store::{SimStore, StoreFaults};
use tokio::time::Instant;
use tower::ServiceExt;

/// Running hash of everything observable in a simulation.
pub struct Trace {
    state: Mutex<(u64, u64)>,
    log: Option<Mutex<Vec<String>>>,
}

impl Trace {
    pub fn new(keep_log: bool) -> Trace {
        Trace { state: Mutex::new((0xcbf2_9ce4_8422_2325, 0)), log: keep_log.then(|| Mutex::new(Vec::new())) }
    }

    pub fn event(&self, s: &str) {
        let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
        for b in s.bytes() {
            g.0 ^= b as u64;
            g.0 = g.0.wrapping_mul(0x0000_0100_0000_01B3);
        }
        g.0 ^= 0xff;
        g.0 = g.0.wrapping_mul(0x0000_0100_0000_01B3);
        g.1 += 1;
        if let Some(l) = &self.log {
            l.lock().unwrap_or_else(|e| e.into_inner()).push(s.to_string());
        }
    }

    pub fn hash(&self) -> u64 {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).0
    }

    pub fn events(&self) -> u64 {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).1
    }

    pub fn dump(&self) -> Vec<String> {
        self.log.as_ref().map(|l| l.lock().unwrap_or_else(|e| e.into_inner()).clone()).unwrap_or_default()
    }
}

#[derive(Clone, Debug)]
pub struct NetFaults {
    pub latency_min_ms: u64,
    pub latency_max_ms: u64,
    /// Probability that a response is lost after the request was processed.
    pub p_lose_response: f64,
}

impl NetFaults {
    pub fn none() -> NetFaults {
        NetFaults { latency_min_ms: 0, latency_max_ms: 2, p_lose_response: 0.0 }
    }

    pub fn moderate() -> NetFaults {
        NetFaults { latency_min_ms: 1, latency_max_ms: 10, p_lose_response: 0.01 }
    }
}

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub nodes: usize,
    pub store: StoreFaults,
    pub net: NetFaults,
    /// Maximum absolute wall-clock skew per node (must stay below the
    /// lease skew margin for lease reads to be linearizable).
    pub max_skew_ms: u64,
    pub engine: EngineConfig,
    pub node: NodeConfig,
    pub keep_log: bool,
    pub request_timeout_ms: u64,
}

impl SimConfig {
    /// Fast, fault-free single node.
    pub fn basic() -> SimConfig {
        let engine = EngineConfig {
            lease_ms: 3_000,
            clock_skew_margin_ms: 200,
            txn_max_lifetime_ms: 30_000,
            gc_grace_ms: 60_000,
            orphan_grace_ms: 400_000,
            blob_stage_ttl_ms: 300_000,
            sweep_interval_ms: 120_000,
            ..EngineConfig::small_for_tests()
        };
        SimConfig {
            nodes: 1,
            store: StoreFaults::none(),
            net: NetFaults::none(),
            max_skew_ms: 0,
            node: NodeConfig {
                name: "n".into(),
                pbkdf2_iterations: 10,
                heartbeat_ms: 1_000,
                auth_cache_ms: 1_000,
                sys_staleness_ms: 200,
                db_delete_grace_ms: 60_000,
                txn_idle_ms: 10_000,
                txn_max_ms: 30_000,
                db_idle_evict_ms: 20_000,
                json_value_max: 4096,
                signup: SignupPolicy::Open,
                engine: engine.clone(),
                ..NodeConfig::default()
            },
            engine,
            keep_log: std::env::var("SIM_LOG").is_ok(),
            request_timeout_ms: 30_000,
        }
    }

    /// Multi-node with store and network faults and clock skew.
    pub fn faulty(nodes: usize) -> SimConfig {
        SimConfig {
            nodes,
            store: StoreFaults::moderate(),
            net: NetFaults::moderate(),
            max_skew_ms: 150,
            ..SimConfig::basic()
        }
    }
}

pub struct SimNode {
    pub idx: usize,
    pub node: Arc<Node>,
    pub router: axum::Router,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetError {
    /// Never reached the node.
    Refused,
    /// May have been processed.
    Lost,
    Timeout,
}

#[derive(Debug, Clone)]
pub struct SimResponse {
    pub status: u16,
    pub headers: http::HeaderMap,
    pub body: Bytes,
}

pub struct Sim {
    pub seed: u64,
    pub cfg: SimConfig,
    rng: Mutex<Rng>,
    pub mem: Arc<MemStore>,
    pub store: Arc<SimStore>,
    pub trace: Arc<Trace>,
    pub google: Arc<google::FakeGoogle>,
    slots: Vec<Mutex<Option<Arc<SimNode>>>>,
    partitions: Mutex<BTreeSet<(usize, usize)>>,
    origin: Instant,
    base_ms: u64,
    starts: AtomicU64,
    pub crashes: AtomicU64,
    me: Weak<Sim>,
}

thread_local! {
    static PANIC: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

fn install_panic_hook() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            PANIC.with(|p| {
                if p.borrow().is_none() {
                    *p.borrow_mut() = Some(info.to_string());
                }
            });
            prev(info);
        }));
    });
}

fn take_panic() -> Option<String> {
    PANIC.with(|p| p.borrow_mut().take())
}

pub fn addr_of(i: usize) -> String {
    format!("sim://n{i}")
}

struct SimTransport {
    from: usize,
    sim: Weak<Sim>,
}

impl Transport for SimTransport {
    fn call(&self, addr: &str, req: Request<Body>) -> BoxFut<'static, Result<Response<Body>, TransportError>> {
        let sim = self.sim.upgrade();
        let from = self.from;
        let to = addr.strip_prefix("sim://n").and_then(|s| s.parse::<usize>().ok());
        Box::pin(async move {
            let undelivered = |m: &str| TransportError { message: m.to_string(), maybe_delivered: false };
            let (Some(sim), Some(to)) = (sim, to) else {
                return Err(undelivered("unknown peer"));
            };
            if sim.partitioned(from, to) {
                sim.net_delay().await;
                return Err(undelivered("partitioned"));
            }
            match sim.call_node(to, req).await {
                Ok(r) => {
                    let mut resp = Response::new(Body::from(r.body));
                    *resp.status_mut() =
                        http::StatusCode::from_u16(r.status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
                    *resp.headers_mut() = r.headers;
                    Ok(resp)
                }
                Err(NetError::Refused) => Err(undelivered("connection refused")),
                Err(e) => Err(TransportError { message: format!("{e:?}"), maybe_delivered: true }),
            }
        })
    }
}

impl Sim {
    pub async fn new(seed: u64, cfg: SimConfig) -> Arc<Sim> {
        install_panic_hook();
        let mut root = Rng::new(seed);
        let origin = Instant::now();
        let base_ms = 1_750_000_000_000 + root.below(1_000_000_000);
        let trace = Arc::new(Trace::new(cfg.keep_log));
        let clock_origin = origin;
        let mem = Arc::new(MemStore::new(Box::new(move || {
            base_ms + Instant::now().saturating_duration_since(clock_origin).as_millis() as u64
        })));
        let store = Arc::new(SimStore::new(mem.clone(), root.fork(), cfg.store.clone(), trace.clone()));
        let google = Arc::new(google::FakeGoogle::new(Box::new(move || {
            base_ms + Instant::now().saturating_duration_since(clock_origin).as_millis() as u64
        })));
        let n = cfg.nodes;
        let sim = Arc::new_cyclic(|me| Sim {
            seed,
            rng: Mutex::new(root.fork()),
            mem,
            store,
            trace,
            google,
            slots: (0..n).map(|_| Mutex::new(None)).collect(),
            partitions: Mutex::new(BTreeSet::new()),
            origin,
            base_ms,
            starts: AtomicU64::new(0),
            crashes: AtomicU64::new(0),
            cfg,
            me: me.clone(),
        });
        for i in 0..n {
            sim.start(i).await;
        }
        sim
    }

    pub fn rand_below(&self, n: u64) -> u64 {
        self.rng.lock().unwrap_or_else(|e| e.into_inner()).below(n.max(1))
    }

    pub fn chance(&self, p: f64) -> bool {
        self.rng.lock().unwrap_or_else(|e| e.into_inner()).chance(p)
    }

    pub fn fork_rng(&self) -> Rng {
        self.rng.lock().unwrap_or_else(|e| e.into_inner()).fork()
    }

    pub fn now_ms(&self) -> u64 {
        self.base_ms + Instant::now().saturating_duration_since(self.origin).as_millis() as u64
    }

    pub fn node_count(&self) -> usize {
        self.slots.len()
    }

    pub fn node(&self, i: usize) -> Option<Arc<SimNode>> {
        self.slots.get(i)?.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn up_nodes(&self) -> Vec<usize> {
        (0..self.slots.len()).filter(|i| self.node(*i).is_some()).collect()
    }

    /// Start (or restart) node `i`. Retries while the store is flaky.
    pub async fn start(&self, i: usize) {
        let n = self.starts.fetch_add(1, Ordering::SeqCst);
        let skew = if self.cfg.max_skew_ms == 0 {
            0
        } else {
            self.rand_below(2 * self.cfg.max_skew_ms + 1) as i64 - self.cfg.max_skew_ms as i64
        };
        let mut cfg = self.cfg.node.clone();
        cfg.name = format!("n{i}");
        cfg.addr = addr_of(i);
        cfg.public_url = "https://baserunna.test".into();
        cfg.engine = self.cfg.engine.clone();
        for attempt in 0..50u64 {
            let env = Arc::new(Env::sim(self.seed ^ (n * 7919 + attempt + 1), self.base_ms, self.origin, skew));
            if self.cfg.keep_log {
                let trace = self.trace.clone();
                let base = self.base_ms;
                let origin = self.origin;
                env.set_tracer(Arc::new(move |m: &str| {
                    let t = (base + Instant::now().saturating_duration_since(origin).as_millis() as u64) % 100_000;
                    trace.event(&format!("t={t} n{i}: {m}"));
                }));
            }
            let transport = Arc::new(SimTransport { from: i, sim: self.me.clone() });
            let store: DynStore = self.store.clone();
            let oidc: Arc<dyn crate::auth::OidcProvider> = self.google.clone();
            match Node::start(cfg.clone(), env.clone(), store, transport, Some(oidc)).await {
                Ok(node) => {
                    let router = api::router(node.clone());
                    self.trace.event(&format!("start n{i} as {}", node.id));
                    *self.slots[i].lock().unwrap_or_else(|e| e.into_inner()) =
                        Some(Arc::new(SimNode { idx: i, node, router }));
                    return;
                }
                Err(e) => {
                    env.shutdown();
                    self.trace.event(&format!("start n{i} failed: {e}"));
                    tokio::time::sleep(Duration::from_millis(200 + 100 * attempt)).await;
                }
            }
        }
        panic!("node {i} could not start");
    }

    /// Kill node `i`: abort all of its tasks and drop its memory.
    pub fn crash(&self, i: usize) {
        let n = self.slots[i].lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(n) = n {
            self.crashes.fetch_add(1, Ordering::SeqCst);
            self.trace.event(&format!("crash n{i} ({})", n.node.id));
            n.node.env.shutdown();
        }
    }

    pub async fn restart(&self, i: usize) {
        self.crash(i);
        self.start(i).await;
    }

    pub fn partition(&self, a: usize, b: usize) {
        let mut p = self.partitions.lock().unwrap_or_else(|e| e.into_inner());
        p.insert((a, b));
        p.insert((b, a));
        self.trace.event(&format!("partition {a}-{b}"));
    }

    pub fn heal(&self) {
        self.partitions.lock().unwrap_or_else(|e| e.into_inner()).clear();
        self.trace.event("heal");
    }

    fn partitioned(&self, a: usize, b: usize) -> bool {
        self.partitions.lock().unwrap_or_else(|e| e.into_inner()).contains(&(a, b))
    }

    async fn net_delay(&self) {
        let (lo, hi) = (self.cfg.net.latency_min_ms, self.cfg.net.latency_max_ms);
        let d = lo + self.rand_below(hi.saturating_sub(lo) + 1);
        tokio::time::sleep(Duration::from_millis(d)).await;
    }

    /// Deliver a request to node `i` through its full router. The handler
    /// runs as one of the node's tasks, so a crash aborts it.
    pub async fn call_node(&self, i: usize, req: Request<Body>) -> Result<SimResponse, NetError> {
        self.net_delay().await;
        let Some(n) = self.node(i) else {
            return Err(NetError::Refused);
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        let router = n.router.clone();
        n.node.env.spawn(async move {
            let resp = match router.oneshot(req).await {
                Ok(r) => r,
                Err(e) => match e {},
            };
            let (parts, body) = resp.into_parts();
            let body = match http_body_util::BodyExt::collect(body).await {
                Ok(b) => b.to_bytes(),
                Err(e) => {
                    // A failure while streaming the body is a broken
                    // connection from the client's point of view.
                    let _ = tx.send(Err(format!("body error: {e}")));
                    return;
                }
            };
            let _ = tx.send(Ok(SimResponse { status: parts.status.as_u16(), headers: parts.headers, body }));
        });
        drop(n);
        let timeout = Duration::from_millis(self.cfg.request_timeout_ms);
        let r = match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(r))) => Ok(r),
            Ok(Ok(Err(_))) | Ok(Err(_)) => Err(NetError::Lost),
            Err(_) => Err(NetError::Timeout),
        };
        self.net_delay().await;
        if r.is_ok() && self.chance(self.cfg.net.p_lose_response) {
            return Err(NetError::Lost);
        }
        r
    }

    pub fn shutdown_all(&self) {
        for i in 0..self.slots.len() {
            if let Some(n) = self.slots[i].lock().unwrap_or_else(|e| e.into_inner()).take() {
                n.node.env.shutdown();
            }
        }
    }

    /// Let simulated time pass.
    pub async fn sleep(&self, ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    /// Storage-level invariants at quiescence: every manifest decodes, every
    /// referenced segment exists and decodes in order, every referenced blob
    /// is complete.
    pub async fn check_storage(&self) -> Result<StorageReport, String> {
        let keys: BTreeSet<String> = self.mem.keys().into_iter().collect();
        let mut rep = StorageReport::default();
        let chunk = self.cfg.engine.blob_chunk_size as u64;
        for k in keys.iter().filter(|k| k.ends_with("/manifest")) {
            let db = k
                .strip_prefix("dbs/")
                .and_then(|s| s.strip_suffix("/manifest"))
                .ok_or_else(|| format!("odd manifest key {k}"))?;
            let g = self.mem.get_sync(k, &Default::default()).map_err(|e| e.to_string())?;
            let m = Manifest::decode(&g.data).map_err(|e| format!("{k}: {e}"))?;
            rep.databases += 1;
            if m.is_deleted() {
                continue;
            }
            let mut entries = m.tail.clone();
            for s in &m.segments {
                let sk = crate::engine::seg_key(db, &s.id);
                let d = self
                    .mem
                    .get_sync(&sk, &Default::default())
                    .map_err(|e| format!("db {db}: referenced segment {} missing: {e}", s.id))?;
                let es = segment::read_all(&d.data).map_err(|e| format!("segment {sk}: {e}"))?;
                if es.len() as u64 != s.entries {
                    return Err(format!("segment {sk}: entry count mismatch"));
                }
                rep.segments += 1;
                entries.extend(es);
            }
            // Only the newest entry per key is live; shadowed blob
            // references in older segments may legitimately be collected.
            let mut newest: std::collections::BTreeMap<Bytes, &crate::engine::Entry> =
                std::collections::BTreeMap::new();
            for e in &entries {
                if newest.get(&e.key).is_none_or(|x| e.seq > x.seq) {
                    newest.insert(e.key.clone(), e);
                }
            }
            for e in newest.values() {
                if let Value::Blob(b) = &e.value {
                    let n = b.size.div_ceil(chunk);
                    for c in 0..n {
                        let ck = crate::engine::blob_chunk_key(db, &b.id, c);
                        if !keys.contains(&ck) {
                            return Err(format!("db {db}: blob {} chunk {c} missing", b.id));
                        }
                    }
                    rep.blobs += 1;
                }
            }
        }
        rep.objects = keys.len();
        Ok(rep)
    }
}

#[derive(Debug, Default, Clone)]
pub struct StorageReport {
    pub databases: usize,
    pub segments: usize,
    pub blobs: usize,
    pub objects: usize,
}

#[derive(Debug)]
pub struct SimOutcome {
    pub seed: u64,
    pub result: Result<(), String>,
    pub trace_hash: u64,
    pub events: u64,
    pub virtual_ms: u64,
    pub store_ops: u64,
    pub injected_faults: u64,
    pub crashes: u64,
    pub storage: StorageReport,
    /// Last trace lines (when `keep_log`).
    pub trace_tail: Vec<String>,
}

/// Run one simulation to completion.
pub fn run<F, Fut>(seed: u64, cfg: SimConfig, f: F) -> SimOutcome
where
    F: FnOnce(Arc<Sim>) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let lvl = crate::log::level();
    if std::env::var("BASERUNNA_LOG").is_err() {
        crate::log::set_level(crate::log::ERROR);
    }
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().expect("runtime");
    let out = rt.block_on(async move {
        let start = Instant::now();
        let sim = Sim::new(seed, cfg).await;
        let mut result = f(sim.clone()).await;
        let mut storage = StorageReport::default();
        if result.is_ok() {
            // Quiesce: stop faults, let in-flight work settle.
            sim.store.set_faults_enabled(false);
            sim.sleep(5_000).await;
            match sim.check_storage().await {
                Ok(r) => storage = r,
                Err(e) => result = Err(format!("storage invariant: {e}")),
            }
        }
        if let Some(p) = take_panic() {
            result = Err(format!("panic in simulation: {p}"));
        }
        let virtual_ms = start.elapsed().as_millis() as u64;
        sim.shutdown_all();
        let dump = sim.trace.dump();
        let keep = if std::env::var("SIM_LOG").as_deref() == Ok("all") { dump.len() } else { 400 };
        let trace_tail = dump[dump.len().saturating_sub(keep)..].to_vec();
        SimOutcome {
            seed,
            result,
            trace_hash: sim.trace.hash(),
            events: sim.trace.events(),
            virtual_ms,
            store_ops: sim.store.stats.gets.load(Ordering::Relaxed)
                + sim.store.stats.puts.load(Ordering::Relaxed)
                + sim.store.stats.lists.load(Ordering::Relaxed)
                + sim.store.stats.deletes.load(Ordering::Relaxed),
            injected_faults: sim.store.stats.injected_failures.load(Ordering::Relaxed),
            crashes: sim.crashes.load(Ordering::Relaxed),
            storage,
            trace_tail,
        }
    });
    crate::log::set_level(lvl);
    out
}

/// Seeds to run: `SIM_SEED` (one), else `SIM_SEEDS` (count) starting from
/// `base`, else `default_count`.
pub fn seeds(base: u64, default_count: u64) -> Vec<u64> {
    if let Some(s) = crate::util::prop::env_u64("SIM_SEED") {
        return vec![s];
    }
    let n = crate::util::prop::env_u64("SIM_SEEDS").unwrap_or(default_count);
    (0..n).map(|i| base.wrapping_mul(1_000_003).wrapping_add(i)).collect()
}

/// Run `f` for every seed, panicking with a reproduction hint on failure.
pub fn run_seeds<F, Fut>(name: &str, base: u64, default_count: u64, cfg: impl Fn() -> SimConfig, f: F)
where
    F: Fn(Arc<Sim>) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    for seed in seeds(base, default_count) {
        let out = run(seed, cfg(), &f);
        if std::env::var("SIM_VERBOSE").is_ok() {
            eprintln!(
                "{name} seed={seed} ok={} events={} virtual={}s store_ops={} faults={} crashes={} storage={:?}",
                out.result.is_ok(),
                out.events,
                out.virtual_ms / 1000,
                out.store_ops,
                out.injected_faults,
                out.crashes,
                out.storage
            );
        }
        if let Err(e) = out.result {
            for l in &out.trace_tail {
                eprintln!("  | {l}");
            }
            panic!("simulation {name} failed: {e}\nreproduce with: SIM_SEED={seed} cargo test {name} -- --nocapture");
        }
    }
}
