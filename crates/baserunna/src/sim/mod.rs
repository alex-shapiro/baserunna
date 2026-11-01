//! Deterministic cluster simulation in virtual time.

pub mod amp;
pub mod campaign;
pub mod client;
pub mod cost;
pub mod explore;
pub mod google;
pub mod mail;
pub mod openapi_check;
mod storage_check;
pub mod store;
pub mod swarm;
pub mod txn_fuzz;
pub mod workloads;

pub use storage_check::StorageReport;

use crate::api;
use crate::cluster::{Transport, TransportError};
use crate::engine::EngineConfig;
use crate::engine::local::{LocalTier, MemLocal};
use crate::env::Env;
use crate::meta::SignupPolicy;
use crate::node::{Node, NodeConfig, Services};
use crate::store::{
    BoxFut, DynStore, GetOpts, GetResult, ListPage, MemStore, ObjectStore, PutMode, SResult, StoreError,
};
use crate::util::rng::Rng;
use axum::body::Body;
use bytes::Bytes;
use http::{Request, Response};
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use store::{SimStore, StoreFaults};
use tokio::time::Instant;
use tower::ServiceExt;

/// Running FNV-1a hash and optional log of simulation events.
pub struct Trace {
    state: Mutex<(u64, u64)>,
    log: Option<Mutex<Vec<String>>>,
}

impl Trace {
    pub fn new(keep_log: bool) -> Trace {
        Trace { state: Mutex::new((0xcbf2_9ce4_8422_2325, 0)), log: keep_log.then(|| Mutex::new(Vec::new())) }
    }

    pub fn event(&self, s: &str) {
        let mut g = self.state.lock();
        for b in s.bytes() {
            g.0 ^= b as u64;
            g.0 = g.0.wrapping_mul(0x0000_0100_0000_01B3);
        }
        g.0 ^= 0xff;
        g.0 = g.0.wrapping_mul(0x0000_0100_0000_01B3);
        g.1 += 1;
        if let Some(l) = &self.log {
            l.lock().push(s.to_string());
        }
    }

    pub fn hash(&self) -> u64 {
        self.state.lock().0
    }

    pub fn events(&self) -> u64 {
        self.state.lock().1
    }

    pub fn dump(&self) -> Vec<String> {
        self.log.as_ref().map(|l| l.lock().clone()).unwrap_or_default()
    }
}

#[derive(Clone, Debug)]
pub struct NetFaults {
    pub latency_min_ms: u64,
    pub latency_max_ms: u64,
    /// Probability that a response is lost after the request was processed.
    pub p_lose_response: f64,
    /// Probability that a peer-forwarded request is duplicated.
    pub p_forward_duplicate: f64,
    /// Probability that a peer-forwarded request is delayed by up to `forward_delay_max_ms`.
    pub p_forward_delay: f64,
    pub forward_delay_max_ms: u64,
}

impl NetFaults {
    pub fn none() -> NetFaults {
        NetFaults {
            latency_min_ms: 0,
            latency_max_ms: 2,
            p_lose_response: 0.0,
            p_forward_duplicate: 0.0,
            p_forward_delay: 0.0,
            forward_delay_max_ms: 0,
        }
    }
}

/// Weights and timing parameters for cluster fault injection.
#[derive(Clone, Debug, Default)]
pub struct ChaosFaults {
    pub enabled: bool,
    /// Maximum pause duration between fault actions.
    pub pause_ms: u64,
    /// Duration of an active fault storm.
    pub storm_ms: u64,
    /// Maximum recovery period after a storm, during which the cluster is healed.
    pub calm_ms: u64,
    pub crash: u64,
    /// Gracefully shuts down a node.
    pub stop: u64,
    pub start: u64,
    pub partition: u64,
    pub one_way_partition: u64,
    /// Isolates a node from the object store while keeping peer network links intact.
    pub store_isolation: u64,
    /// Steps a node's wall clock by up to `clock_step_max_ms` in either direction.
    pub clock_step: u64,
    pub heal: u64,
    pub quiet: u64,
    /// Freezes a node's execution for up to three lease durations.
    pub pause_node: u64,
    pub clock_step_max_ms: u64,
}

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub nodes: usize,
    pub store: StoreFaults,
    pub net: NetFaults,
    /// Maximum initial wall-clock skew per node.
    pub max_skew_ms: u64,
    pub chaos: ChaosFaults,
    pub engine: EngineConfig,
    pub node: NodeConfig,
    pub keep_log: bool,
    pub full_trace: bool,
    pub request_timeout_ms: u64,
    pub knobs: Option<Arc<swarm::Knobs>>,
    /// Fraction of buggify sites enabled in a simulation run.
    pub buggify: f64,
    pub crash_points: bool,
    /// Probability that a node task yields before being polled.
    pub reorder: f64,
    pub disk: DiskFaults,
    /// Sorted `(virtual_ms, salt)` branch points for deterministic replay and divergence.
    pub branches: Vec<(u64, u64)>,
}

impl SimConfig {
    /// Returns a fast, fault-free single-node configuration.
    pub fn basic() -> SimConfig {
        let engine = EngineConfig {
            lease_ms: 3_000,
            read_lease_ms: 6_000,
            lease_margin_ms: 200,
            txn_max_lifetime_ms: 30_000,
            gc_grace_ms: 60_000,
            orphan_grace_ms: 400_000,
            blob_stage_ttl_ms: 300_000,
            sweep_interval_ms: 120_000,
            clock_sync_interval_ms: 30_000,
            clock_sync_max_age_ms: 120_000,
            clock_sync_max_error_ms: 100,
            levels_inline_max: 1,
            version_sample_ms: 5_000,
            commit_retention_ms: 120_000,
            ..EngineConfig::small_for_tests()
        };
        SimConfig {
            nodes: 1,
            store: StoreFaults::none(),
            net: NetFaults::none(),
            max_skew_ms: 0,
            chaos: ChaosFaults::default(),
            node: NodeConfig {
                name: "n".into(),
                pbkdf2_iterations: 10,
                heartbeat_ms: 5_000,
                members_refresh_ms: 1_000,
                auth_cache_ms: 1_000,
                sys_staleness_ms: 200,
                db_delete_grace_ms: 60_000,
                txn_idle_ms: 10_000,
                txn_max_ms: 30_000,
                db_idle_evict_ms: 20_000,
                json_value_max: 4096,
                index_backfill_batch_rows: 8,
                signup: SignupPolicy::Open,
                limits: crate::limits::LimitConfig {
                    user: crate::limits::Rate::per_sec(400.0),
                    api_key: crate::limits::Rate::per_sec(200.0),
                    auth_per_client: crate::limits::Rate { per_sec: 20.0, burst: 40.0 },
                    signup_per_client: crate::limits::Rate::per_hour(3_600.0, 20.0),
                    mail_per_account: crate::limits::Rate::per_hour(600.0, 10.0),
                    account_free_failures: 5,
                    client_free_failures: 20,
                    failure_backoff_ms: 500,
                    failure_backoff_max_ms: 10_000,
                    hash_concurrency: 2,
                    hash_wait_ms: 5_000,
                    trusted_proxy_hops: 1,
                },
                engine: engine.clone(),
                // Below request_timeout_ms so timed-out client queries release their execution slot first.
                query_limits: crate::events::exec::Limits { max_ms: 20_000, ..Default::default() },
                ..NodeConfig::default()
            },
            engine,
            keep_log: std::env::var("SIM_LOG").is_ok(),
            full_trace: std::env::var("SIM_LOG").as_deref() == Ok("all"),
            request_timeout_ms: 30_000,
            knobs: None,
            buggify: 0.0,
            crash_points: false,
            reorder: 0.0,
            disk: DiskFaults { bytes: 64 * 1024, ..DiskFaults::default() },
            branches: Vec::new(),
        }
    }
}

pub struct SimNode {
    pub idx: usize,
    pub start: u64,
    pub node: Arc<Node>,
    pub router: axum::Router,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetError {
    /// Request did not reach the node.
    Refused,
    /// Request may have been processed before the response was lost.
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
    pub mailbox: Arc<mail::Mailbox>,
    slots: Vec<Mutex<Option<Arc<SimNode>>>>,
    /// Serializes startup for each node slot so concurrent starts do not spawn duplicate instances.
    starting: Vec<tokio::sync::Mutex<()>>,
    disks: Vec<Option<LocalTier>>,
    partitions: Mutex<BTreeSet<(usize, usize)>>,
    store_isolated: Mutex<BTreeSet<usize>>,
    origin: Instant,
    base_ms: u64,
    starts: AtomicU64,
    pub crashes: AtomicU64,
    violation: Mutex<Option<String>>,
    pub knobs: Arc<swarm::Knobs>,
    last_point_crash: Mutex<Option<Instant>>,
    pub(crate) chaos: Mutex<workloads::ChaosState>,
    pub(crate) chaos_over: tokio::sync::Notify,
    pub(crate) admin: tokio::sync::Mutex<Option<String>>,
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

/// Simulated local disk cache that persists across node restarts and injects read/write faults.
struct SimDisk {
    inner: MemLocal,
    rng: Mutex<Rng>,
    faults: DiskFaults,
    trace: Arc<Trace>,
}

/// Fault injection parameters for simulated local disks.
#[derive(Clone, Debug, Default)]
pub struct DiskFaults {
    /// Capacity of each node's local cache tier in bytes, or 0 to disable local caching.
    pub bytes: u64,
    /// Probability that a cached read misses as if the entry were evicted.
    pub p_lose: f64,
    /// Probability that a cache write is silently dropped.
    pub p_drop_write: f64,
    /// Probability that a cached read flips a random bit in the returned bytes.
    pub p_corrupt: f64,
}

impl crate::engine::local::LocalStore for SimDisk {
    fn get<'a>(&'a self, key: &'a str) -> BoxFut<'a, Option<Bytes>> {
        Box::pin(async move {
            let got = self.inner.get(key).await?;
            let mut r = self.rng.lock();
            if r.chance(self.faults.p_lose) {
                self.trace.event(&format!("disk lost {key}"));
                return None;
            }
            if !got.is_empty() && r.chance(self.faults.p_corrupt) {
                self.trace.event(&format!("disk corrupted {key}"));
                let mut b = got.to_vec();
                let i = r.usize(b.len());
                b[i] ^= 1 << r.below(8);
                return Some(Bytes::from(b));
            }
            Some(got)
        })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes) -> BoxFut<'a, ()> {
        Box::pin(async move {
            if self.rng.lock().chance(self.faults.p_drop_write) {
                return;
            }
            self.inner.put(key, data).await
        })
    }

    fn contains(&self, key: &str) -> bool {
        self.inner.contains(key)
    }

    fn stats(&self) -> crate::engine::local::LocalStats {
        self.inner.stats()
    }
}

/// Minimum virtual time between two crash-point crashes.
const POINT_CRASH_SPACING_MS: u64 = 30_000;

/// Maximum virtual time allowed for a simulation run before timing out.
const MAX_VIRTUAL_MS: u64 = 6 * 3_600_000;

/// Virtual time interval between global invariant checks.
const ORACLE_INTERVAL_MS: u64 = 20;

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
            let (parts, body) = req.into_parts();
            let body = match http_body_util::BodyExt::collect(body).await {
                Ok(b) => b.to_bytes(),
                Err(e) => return Err(undelivered(&format!("request body: {e}"))),
            };
            let copy = || {
                let mut r = Request::new(Body::from(body.clone()));
                *r.method_mut() = parts.method.clone();
                *r.uri_mut() = parts.uri.clone();
                *r.headers_mut() = parts.headers.clone();
                r
            };
            let net = &sim.cfg.net;
            let duplicated = sim.chance(net.p_forward_duplicate);
            if duplicated {
                let delay = sim.rand_below(net.forward_delay_max_ms + 1);
                sim.trace.event(&format!("forward n{from}->n{to} {} duplicated (+{delay}ms)", parts.uri.path()));
                let (sim2, dup) = (sim.clone(), copy());
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    let _ = sim2.call_node(to, dup).await;
                });
            }
            if sim.chance(net.p_forward_delay) {
                let delay = sim.rand_below(net.forward_delay_max_ms + 1);
                sim.trace.event(&format!("forward n{from}->n{to} {} delayed {delay}ms", parts.uri.path()));
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            let r = sim.call_node(to, copy()).await;
            if sim.partitioned(to, from) {
                return Err(TransportError {
                    message: "response lost (one-way partition)".into(),
                    maybe_delivered: true,
                });
            }
            match r {
                Ok(r) => {
                    let mut resp = Response::new(Body::from(r.body));
                    *resp.status_mut() =
                        http::StatusCode::from_u16(r.status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
                    *resp.headers_mut() = r.headers;
                    Ok(resp)
                }
                Err(NetError::Refused) if duplicated => {
                    Err(TransportError { message: "connection refused after duplicate".into(), maybe_delivered: true })
                }
                Err(NetError::Refused) => Err(undelivered("connection refused")),
                Err(e) => Err(TransportError { message: format!("{e:?}"), maybe_delivered: true }),
            }
        })
    }
}

struct NodeStore {
    idx: usize,
    sim: Weak<Sim>,
    inner: Arc<SimStore>,
}

impl NodeStore {
    async fn reachable(&self, op: &str, key: &str) -> SResult<()> {
        let Some(sim) = self.sim.upgrade() else { return Ok(()) };
        if !sim.store_isolated(self.idx) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100 + sim.rand_below(900))).await;
        sim.trace.event(&format!("n{} store {op} {key} -> unreachable", self.idx));
        Err(StoreError::Unavailable("object store unreachable".into()))
    }
}

impl ObjectStore for NodeStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        Box::pin(async move {
            self.reachable("get", key).await?;
            self.inner.get(key, opts).await
        })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move {
            self.reachable("put", key).await?;
            self.inner.put(key, data, mode).await
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        Box::pin(async move {
            self.reachable("delete", key).await?;
            self.inner.delete(key).await
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        limit: usize,
    ) -> BoxFut<'a, SResult<ListPage>> {
        Box::pin(async move {
            self.reachable("list", prefix).await?;
            self.inner.list(prefix, start_after, limit).await
        })
    }

    fn time_precision_ms(&self) -> u64 {
        self.inner.time_precision_ms()
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
        let disks = (0..n)
            .map(|_| {
                let d = cfg.disk.clone();
                let rng = Mutex::new(root.fork());
                let disk = SimDisk { inner: MemLocal::new(d.bytes), rng, faults: d, trace: trace.clone() };
                (cfg.disk.bytes > 0).then(|| LocalTier(Arc::new(disk)))
            })
            .collect();
        let sim = Arc::new_cyclic(|me| Sim {
            seed,
            rng: Mutex::new(root.fork()),
            mem,
            store,
            trace,
            google,
            mailbox: Arc::new(mail::Mailbox::default()),
            slots: (0..n).map(|_| Mutex::new(None)).collect(),
            starting: (0..n).map(|_| tokio::sync::Mutex::new(())).collect(),
            disks,
            partitions: Mutex::new(BTreeSet::new()),
            store_isolated: Mutex::new(BTreeSet::new()),
            origin,
            base_ms,
            starts: AtomicU64::new(0),
            crashes: AtomicU64::new(0),
            violation: Mutex::new(None),
            knobs: cfg.knobs.clone().unwrap_or_else(|| Arc::new(swarm::Knobs::new(seed))),
            last_point_crash: Mutex::new(None),
            chaos: Mutex::new(Default::default()),
            chaos_over: tokio::sync::Notify::new(),
            admin: tokio::sync::Mutex::new(None),
            cfg,
            me: me.clone(),
        });
        for i in 0..n {
            sim.start(i).await;
        }
        sim
    }

    pub fn rand_below(&self, n: u64) -> u64 {
        self.rng.lock().below(n.max(1))
    }

    pub fn chance(&self, p: f64) -> bool {
        self.rng.lock().chance(p)
    }

    pub fn fork_rng(&self) -> Rng {
        self.rng.lock().fork()
    }

    pub fn now_ms(&self) -> u64 {
        self.base_ms + Instant::now().saturating_duration_since(self.origin).as_millis() as u64
    }

    pub fn node_count(&self) -> usize {
        self.slots.len()
    }

    pub fn node(&self, i: usize) -> Option<Arc<SimNode>> {
        self.slots.get(i)?.lock().clone()
    }

    pub fn up_nodes(&self) -> Vec<usize> {
        (0..self.slots.len()).filter(|i| self.node(*i).is_some()).collect()
    }

    /// Starts node `i` if it is not running.
    pub async fn start(&self, i: usize) {
        let _starting = self.starting[i].lock().await;
        if self.node(i).is_some() {
            return;
        }
        self.store_isolated.lock().remove(&i);
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
        cfg.engine.local_tier = self.disks[i].clone();
        let mut failures = 0u64;
        for attempt in 0u64.. {
            let env = Arc::new(Env::sim(self.seed ^ (n * 7919 + attempt + 1), self.base_ms, self.origin, skew));
            let me = self.me.clone();
            env.set_crash_hook(Arc::new(move |why: &str| {
                me.upgrade().is_some_and(|sim| sim.crash_at_point(i, n, why))
            }));
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
            let store: DynStore = Arc::new(NodeStore { idx: i, sim: self.me.clone(), inner: self.store.clone() });
            let services = Services {
                transport,
                oidc: Some(self.google.clone() as Arc<dyn crate::auth::OidcProvider>),
                mailer: Some(self.mailbox.clone() as Arc<dyn crate::mail::Mailer>),
            };
            match Node::start(cfg.clone(), env.clone(), store, services).await {
                Ok(node) => {
                    let router = api::router(node.clone());
                    self.trace.event(&format!("start n{i} as {}", node.id));
                    *self.slots[i].lock() = Some(Arc::new(SimNode { idx: i, start: n, node, router }));
                    return;
                }
                Err(e) => {
                    env.shutdown();
                    self.trace.event(&format!("start n{i} failed: {e}"));
                    if !self.is_isolated(i) {
                        failures += 1;
                        if failures == 50 {
                            panic!("node {i} could not start");
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(200 + 100 * attempt.min(50))).await;
                }
            }
        }
    }

    /// Crashes node `i` at a crash point if it is still running instance `n`,
    /// and schedules a restart after a random delay.
    fn crash_at_point(self: &Arc<Self>, i: usize, n: u64, why: &str) -> bool {
        let current = self.slots[i].lock().as_ref().map(|s| s.start);
        if current != Some(n) {
            return false;
        }
        {
            let mut last = self.last_point_crash.lock();
            if last.is_some_and(|t| t.elapsed() < Duration::from_millis(POINT_CRASH_SPACING_MS)) {
                return false;
            }
            *last = Some(Instant::now());
        }
        self.trace.event(&format!("n{i}: {why}"));
        self.crash(i);
        let sim = self.clone();
        let delay = 100 + self.rand_below(5_000);
        tokio::spawn(async move {
            sim.sleep(delay).await;
            if sim.node(i).is_none() {
                sim.start(i).await;
            }
        });
        true
    }

    /// Gracefully shuts down node `i` and verifies that its released leases are cleared in object storage.
    pub async fn stop(&self, i: usize) {
        let _starting = self.starting[i].lock().await;
        let Some(n) = self.slots[i].lock().take() else { return };
        self.trace.event(&format!("stop n{i} ({})", n.node.id));
        let released = n.node.shutdown().await;
        for db in released {
            let Ok(g) = self.mem.get_sync(&crate::engine::manifest_key(&db), &GetOpts::default()) else { continue };
            let lease = crate::engine::manifest::Manifest::decode(&g.data).ok().and_then(|m| m.lease);
            if lease.is_some_and(|l| l.node == n.node.id) {
                let e = format!("n{i} released its lease on {db} as it stopped, but storage names it again");
                self.trace.event(&format!("oracle: {e}"));
                self.violation.lock().get_or_insert(e);
            }
        }
        self.trace.event(&format!("stopped n{i}"));
    }

    /// Aborts node `i`'s tasks and drops its in-memory state.
    pub fn crash(&self, i: usize) {
        let n = self.slots[i].lock().take();
        if let Some(n) = n {
            self.crashes.fetch_add(1, Ordering::SeqCst);
            self.trace.event(&format!("crash n{i} ({})", n.node.id));
            n.node.env.shutdown();
        }
    }

    /// Verifies cluster-wide lease invariants: at most one node holds an active
    /// lease per database, and any leased node without an in-flight manifest write
    /// matches the latest manifest in object storage.
    fn check_oracles(&self) -> Result<(), String> {
        let mut holders: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for i in self.up_nodes() {
            let Some(n) = self.node(i) else { continue };
            for db in n.node.engine.open_dbs() {
                let Some((write_seq, etag, in_flight)) = db.leased_view() else { continue };
                holders.entry(db.id.clone()).or_default().push(i);
                if in_flight {
                    continue;
                }
                let stored = self.mem.get_sync(&crate::engine::manifest_key(&db.id), &GetOpts::default());
                if let Ok(g) = stored
                    && g.etag != etag
                {
                    let ws = crate::engine::manifest::Manifest::decode(&g.data).map_or(0, |m| m.write_seq);
                    return Err(format!(
                        "n{i} holds lease on {} at write {write_seq}, but store has write {ws}",
                        db.id
                    ));
                }
            }
        }
        match holders.into_iter().find(|(_, ns)| ns.len() > 1) {
            Some((db, ns)) => Err(format!("multiple nodes {ns:?} hold an active lease on {db}")),
            None => Ok(()),
        }
    }

    fn spawn_oracles(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let sim = self.clone();
        tokio::spawn(async move {
            loop {
                sim.sleep(ORACLE_INTERVAL_MS).await;
                if let Err(e) = sim.check_oracles() {
                    sim.trace.event(&format!("oracle: {e}"));
                    sim.violation.lock().get_or_insert(e);
                    return;
                }
            }
        })
    }

    pub async fn restart(&self, i: usize) {
        self.crash(i);
        self.start(i).await;
    }

    pub fn partition(&self, a: usize, b: usize) {
        let mut p = self.partitions.lock();
        p.insert((a, b));
        p.insert((b, a));
        self.trace.event(&format!("partition {a}-{b}"));
    }

    /// Drops traffic from `from` to `to`, including responses to requests sent by `to`.
    pub fn partition_one_way(&self, from: usize, to: usize) {
        self.partitions.lock().insert((from, to));
        self.trace.event(&format!("partition {from}->{to}"));
    }

    pub fn isolate_from_store(&self, i: usize) {
        self.store_isolated.lock().insert(i);
        self.trace.event(&format!("isolate n{i} from store"));
    }

    pub fn heal(&self) {
        self.partitions.lock().clear();
        self.store_isolated.lock().clear();
        self.trace.event("heal");
    }

    fn partitioned(&self, from: usize, to: usize) -> bool {
        self.partitions.lock().contains(&(from, to))
    }

    fn is_isolated(&self, i: usize) -> bool {
        self.store_isolated(i) || self.partitions.lock().iter().any(|&(a, b)| a == i || b == i)
    }

    fn store_isolated(&self, i: usize) -> bool {
        self.store_isolated.lock().contains(&i)
    }

    /// Suspends all task execution on node `i` for `ms` milliseconds.
    pub fn pause_node(self: &Arc<Self>, i: usize, ms: u64) {
        let Some(n) = self.node(i) else { return };
        self.trace.event(&format!("pause n{i} for {ms}ms"));
        n.node.env.pause();
        let sim = self.clone();
        tokio::spawn(async move {
            sim.sleep(ms).await;
            n.node.env.resume();
            sim.trace.event(&format!("resume n{i}"));
        });
    }

    /// Resumes all paused nodes.
    pub fn resume_all(&self) {
        for i in self.up_nodes() {
            if let Some(n) = self.node(i) {
                n.node.env.resume();
            }
        }
    }

    pub fn step_clock(&self, i: usize, delta_ms: i64) {
        if let Some(n) = self.node(i) {
            n.node.env.step_clock(delta_ms);
            self.trace.event(&format!("clock n{i} {delta_ms:+}ms"));
        }
    }

    async fn net_delay(&self) {
        let (lo, hi) = (self.cfg.net.latency_min_ms, self.cfg.net.latency_max_ms);
        let d = lo + self.rand_below(hi.saturating_sub(lo) + 1);
        tokio::time::sleep(Duration::from_millis(d)).await;
    }

    /// Dispatches `req` to node `i` as a node task that is aborted if the node crashes.
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
            let n = self.slots[i].lock().take();
            if let Some(n) = n {
                n.node.env.shutdown();
            }
        }
    }

    pub async fn sleep(&self, ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
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
    pub probes: crate::probe::Hits,
    pub buggify: crate::buggify::Report,
    pub knobs: Vec<(&'static str, String)>,
    pub trace_tail: Vec<String>,
}

struct CatchPanic<F>(std::pin::Pin<Box<F>>);

impl<F: Future> Future for CatchPanic<F> {
    type Output = Result<F::Output, ()>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let inner = self.get_mut().0.as_mut();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(p) => p.map(Ok),
            Err(_) => std::task::Poll::Ready(Err(())),
        }
    }
}

pub fn run<F, Fut>(seed: u64, cfg: SimConfig, f: F) -> SimOutcome
where
    F: FnOnce(Arc<Sim>) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    if std::env::var("BASERUNNA_LOG").is_err() {
        crate::log::set_level(Some(crate::log::Level::Error));
    }
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().expect("runtime");
    let full_trace = cfg.full_trace;
    let b = crate::buggify::Config {
        p_active: cfg.buggify,
        faults: cfg.chaos.enabled,
        crashes: cfg.crash_points,
        p_reorder: cfg.reorder,
    };
    crate::buggify::enable(seed ^ 0xb066_1f1e, b);
    let branches = cfg.branches.clone();
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(async move {
            crate::probe::record();
            crate::probe::set_branch_time(branches.last().map_or(0, |(ms, _)| *ms));
            let start = Instant::now();
            crate::util::rng::branch(
                branches.iter().map(|(ms, salt)| (start + Duration::from_millis(*ms), *salt)).collect(),
            );
            let sim = Sim::new(seed, cfg).await;
            let mut oracles = sim.spawn_oracles();
            let mut workload = CatchPanic(Box::pin(f(sim.clone())));
            let mut oracles_or_deadline = Box::pin(async {
                let deadline = tokio::time::sleep(Duration::from_millis(MAX_VIRTUAL_MS));
                match crate::util::race(&mut oracles, &mut Box::pin(deadline)).await {
                    crate::util::Either::Left(_) => Ok(()),
                    crate::util::Either::Right(()) => {
                        Err(format!("workload timed out after {} s of virtual time", MAX_VIRTUAL_MS / 1000))
                    }
                }
            });
            let mut result = match crate::util::race(&mut workload, &mut oracles_or_deadline).await {
                crate::util::Either::Left(Ok(r)) => r,
                crate::util::Either::Left(Err(())) => Err("workload panicked".into()),
                crate::util::Either::Right(r) => r,
            };
            drop(oracles_or_deadline);
            drop(workload);
            oracles.abort();
            if let Some(v) = sim.violation.lock().take() {
                result = Err(format!("oracle: {v}"));
            }
            let mut storage = StorageReport::default();
            if result.is_ok() {
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
            let keep = if full_trace { dump.len() } else { 400 };
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
                probes: crate::probe::Hits::default(),
                buggify: Default::default(),
                knobs: sim.knobs.drawn(),
                trace_tail,
            }
        })
    }));
    crate::util::rng::branch(Vec::new());
    let (probes, buggify) = (crate::probe::finish(), crate::buggify::disable());
    let out = out.unwrap_or_else(|_| SimOutcome {
        seed,
        result: Err(format!("panic outside the workload: {}", take_panic().unwrap_or_default())),
        trace_hash: 0,
        events: 0,
        virtual_ms: 0,
        store_ops: 0,
        injected_faults: 0,
        crashes: 0,
        storage: StorageReport::default(),
        probes: Default::default(),
        buggify: Default::default(),
        knobs: Vec::new(),
        trace_tail: Vec::new(),
    });
    SimOutcome { probes, buggify, ..out }
}

/// Time required for two reaper passes to purge all deleted databases.
pub fn purge_wait_ms(cfg: &SimConfig) -> u64 {
    2 * cfg.node.db_delete_grace_ms + 3 * crate::node::REAP_INTERVAL_MS
}

/// Returns the seed list from `SIM_SEED`, `SIM_SEEDS`, or `default_count` starting from `base`.
pub fn seeds(base: u64, default_count: u64) -> Vec<u64> {
    if let Some(s) = crate::util::prop::env_u64("SIM_SEED") {
        return vec![s];
    }
    let n = crate::util::prop::env_u64("SIM_SEEDS").unwrap_or(default_count);
    (0..n).map(|i| base.wrapping_mul(1_000_003).wrapping_add(i)).collect()
}

/// Runs `scenario` with the seeds from [`seeds`], panicking with a
/// reproduction command on the first failure.
pub fn run_seeds(scenario: &workloads::Scenario, base: u64, default_count: u64) {
    let name = scenario.name;
    for seed in seeds(base, default_count) {
        let out = run(seed, scenario.config_for(seed), scenario.run);
        if std::env::var("SIM_VERBOSE").is_ok() {
            eprintln!(
                "{name} seed={seed} ok={} events={} virtual={}s store_ops={} faults={} crashes={} storage={:?} probes={}",
                out.result.is_ok(),
                out.events,
                out.virtual_ms / 1000,
                out.store_ops,
                out.injected_faults,
                out.crashes,
                out.storage,
                out.probes.hit().count()
            );
        }
        if let Err(e) = out.result {
            for l in &out.trace_tail {
                eprintln!("  | {l}");
            }
            let knobs: Vec<String> = out.knobs.iter().map(|(k, v)| format!("{k}={v}")).collect();
            panic!(
                "simulation {name} failed: {e}\nknobs: {}\nreproduce with: SIM_SEED={seed} cargo test sim_{name} -- --nocapture",
                knobs.join(" ")
            );
        }
    }
}
