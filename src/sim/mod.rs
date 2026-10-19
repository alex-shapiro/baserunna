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
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
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

/// The chaos actions [`workloads::chaos_step`] takes, by weight.
#[derive(Clone, Debug, Default)]
pub struct ChaosFaults {
    /// Whether workloads that inject chaos do so.
    pub enabled: bool,
    /// The longest pause between actions; each pause is drawn up to it.
    pub pause_ms: u64,
    /// How long a storm of actions lasts.
    pub storm_ms: u64,
    /// The longest calm after a storm, which begins by healing the cluster;
    /// 0 for none.
    pub calm_ms: u64,
    pub crash: u64,
    /// Start a crashed node.
    pub start: u64,
    pub partition: u64,
    /// Partitions that drop traffic in one direction only.
    pub one_way_partition: u64,
    /// Cut a node off from the object store while leaving peer traffic intact.
    pub store_isolation: u64,
    /// Step a node's wall clock by up to `clock_step_max_ms` either way.
    pub clock_step: u64,
    pub heal: u64,
    /// Pause longer, letting the cluster settle.
    pub quiet: u64,
    /// Pause a node's process for up to three lease durations.
    pub pause_node: u64,
    pub clock_step_max_ms: u64,
}

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub nodes: usize,
    pub store: StoreFaults,
    pub net: NetFaults,
    /// Maximum absolute wall-clock skew per node at start.
    pub max_skew_ms: u64,
    pub chaos: ChaosFaults,
    pub engine: EngineConfig,
    pub node: NodeConfig,
    pub keep_log: bool,
    pub full_trace: bool,
    pub request_timeout_ms: u64,
    /// The knobs this configuration was drawn from, if it was (see [`swarm`]).
    pub knobs: Option<Arc<swarm::Knobs>>,
    /// The share of buggify sites active in a run (see [`crate::buggify`]).
    pub buggify: f64,
    /// Whether crash points may crash nodes.
    pub crash_points: bool,
    /// How often a node's task yields before a poll, varying the order of
    /// tasks that are ready together.
    pub reorder: f64,
    pub disk: DiskFaults,
    /// Branch points of this run: `(virtual ms, salt)`, sorted (see
    /// [`crate::util::rng::branch`]). A run with branch points repeats the
    /// unbranched run up to the first, then explores another future.
    pub branches: Vec<(u64, u64)>,
}

impl SimConfig {
    /// Fast, fault-free single node.
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
            // Short enough to expire within a run, long enough to resolve ambiguous commits.
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
    /// Which start of the simulation this process was.
    pub start: u64,
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
    pub mailbox: Arc<mail::Mailbox>,
    slots: Vec<Mutex<Option<Arc<SimNode>>>>,
    /// Held while node `i` starts, so that concurrent starts yield one process.
    starting: Vec<tokio::sync::Mutex<()>>,
    disks: Vec<Option<LocalTier>>,
    partitions: Mutex<BTreeSet<(usize, usize)>>,
    store_isolated: Mutex<BTreeSet<usize>>,
    origin: Instant,
    base_ms: u64,
    starts: AtomicU64,
    pub crashes: AtomicU64,
    /// The first violation a global oracle found.
    violation: Mutex<Option<String>>,
    pub knobs: Arc<swarm::Knobs>,
    /// When a crash point last crashed a node.
    last_point_crash: Mutex<Option<Instant>>,
    /// Workloads in their chaos phase, and the chaos loop they share.
    pub(crate) chaos: Mutex<workloads::ChaosState>,
    /// Notified when the last chaos phase ends and the cluster has recovered.
    pub(crate) chaos_over: tokio::sync::Notify,
    /// The administrator's session token, once [`workloads::admin`] created it.
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

/// A node's local disk: its cache tier, which outlives the node's crashes,
/// and loses or corrupts entries at the rates of [`DiskFaults`].
struct SimDisk {
    inner: MemLocal,
    rng: Mutex<Rng>,
    faults: DiskFaults,
    trace: Arc<Trace>,
}

/// Faults of nodes' local disks.
#[derive(Clone, Debug, Default)]
pub struct DiskFaults {
    /// Bytes each node's local cache tier holds, or 0 for no tier.
    pub bytes: u64,
    /// Probability that a read finds its entry gone.
    pub p_lose: f64,
    /// Probability that a write never reaches the disk.
    pub p_drop_write: f64,
    /// Probability that a read returns its entry with one bit flipped.
    pub p_corrupt: f64,
}

impl crate::engine::local::LocalStore for SimDisk {
    fn get<'a>(&'a self, key: &'a str) -> BoxFut<'a, Option<Bytes>> {
        Box::pin(async move {
            let got = self.inner.get(key).await?;
            let mut r = self.rng.lock().unwrap_or_else(|e| e.into_inner());
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
            if self.rng.lock().unwrap_or_else(|e| e.into_inner()).chance(self.faults.p_drop_write) {
                return;
            }
            self.inner.put(key, data).await
        })
    }

    fn stats(&self) -> crate::engine::local::LocalStats {
        self.inner.stats()
    }
}

/// Least virtual time between two crashes at crash points.
const POINT_CRASH_SPACING_MS: u64 = 30_000;

/// Virtual time a run may take: longer means a workload made no progress.
const MAX_VIRTUAL_MS: u64 = 6 * 3_600_000;

/// How often the global oracles check the cluster, in virtual time.
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

    /// Starts node `i` unless it is up.
    pub async fn start(&self, i: usize) {
        let _starting = self.starting[i].lock().await;
        if self.node(i).is_some() {
            return;
        }
        self.store_isolated.lock().unwrap_or_else(|e| e.into_inner()).remove(&i);
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
        for attempt in 0..50u64 {
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
                    *self.slots[i].lock().unwrap_or_else(|e| e.into_inner()) =
                        Some(Arc::new(SimNode { idx: i, start: n, node, router }));
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

    /// Crashes node `i` at a crash point, if it is still the `n`th process
    /// started, and starts it again a little later.
    fn crash_at_point(self: &Arc<Self>, i: usize, n: u64, why: &str) -> bool {
        let current = self.slots[i].lock().unwrap_or_else(|e| e.into_inner()).as_ref().map(|s| s.start);
        if current != Some(n) {
            return false;
        }
        // Crashes at crash points are spaced out, so that a node that keeps
        // reaching one does not crash on every start.
        {
            let mut last = self.last_point_crash.lock().unwrap_or_else(|e| e.into_inner());
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

    /// Aborts node `i`'s tasks and drops its in-memory state.
    pub fn crash(&self, i: usize) {
        let n = self.slots[i].lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(n) = n {
            self.crashes.fetch_add(1, Ordering::SeqCst);
            self.trace.event(&format!("crash n{i} ({})", n.node.id));
            n.node.env.shutdown();
        }
    }

    /// Checks the global invariants no single node can see: at most one node
    /// relies on a lease on each database, and a node that relies on its
    /// lease, with no manifest write in flight, serves the manifest the store
    /// holds.
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
                        "n{i} relies on its lease on {} at write {write_seq}, but the store holds write {ws}",
                        db.id
                    ));
                }
            }
        }
        match holders.into_iter().find(|(_, ns)| ns.len() > 1) {
            Some((db, ns)) => Err(format!("nodes {ns:?} all rely on a lease on {db}")),
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
                    sim.violation.lock().unwrap_or_else(|e| e.into_inner()).get_or_insert(e);
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
        let mut p = self.partitions.lock().unwrap_or_else(|e| e.into_inner());
        p.insert((a, b));
        p.insert((b, a));
        self.trace.event(&format!("partition {a}-{b}"));
    }

    /// Drops traffic from `from` to `to`, including responses to requests sent by `to`.
    pub fn partition_one_way(&self, from: usize, to: usize) {
        self.partitions.lock().unwrap_or_else(|e| e.into_inner()).insert((from, to));
        self.trace.event(&format!("partition {from}->{to}"));
    }

    pub fn isolate_from_store(&self, i: usize) {
        self.store_isolated.lock().unwrap_or_else(|e| e.into_inner()).insert(i);
        self.trace.event(&format!("isolate n{i} from store"));
    }

    pub fn heal(&self) {
        self.partitions.lock().unwrap_or_else(|e| e.into_inner()).clear();
        self.store_isolated.lock().unwrap_or_else(|e| e.into_inner()).clear();
        self.trace.event("heal");
    }

    fn partitioned(&self, from: usize, to: usize) -> bool {
        self.partitions.lock().unwrap_or_else(|e| e.into_inner()).contains(&(from, to))
    }

    fn store_isolated(&self, i: usize) -> bool {
        self.store_isolated.lock().unwrap_or_else(|e| e.into_inner()).contains(&i)
    }

    /// Pauses node `i` for `ms`, as a stop-the-world pause or a frozen VM
    /// would: no task of it runs meanwhile.
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

    /// Resumes every paused node.
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
            let n = self.slots[i].lock().unwrap_or_else(|e| e.into_inner()).take();
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
    /// The knob values the run drew, by name.
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
    // Simulated nodes log only errors unless asked otherwise. The level is
    // process-wide, so it is not restored: campaigns run simulations on many
    // threads at once.
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
    let out = rt.block_on(async move {
        // Probes' first hits and branch points are in virtual time from here.
        crate::probe::record();
        let start = Instant::now();
        crate::util::rng::branch(
            branches.iter().map(|(ms, salt)| (start + Duration::from_millis(*ms), *salt)).collect(),
        );
        let sim = Sim::new(seed, cfg).await;
        // A violation ends the run at once: what follows it only obscures it.
        let mut oracles = sim.spawn_oracles();
        let mut workload = CatchPanic(Box::pin(f(sim.clone())));
        let mut oracles_or_deadline = Box::pin(async {
            let deadline = tokio::time::sleep(Duration::from_millis(MAX_VIRTUAL_MS));
            match crate::util::race(&mut oracles, &mut Box::pin(deadline)).await {
                crate::util::Either::Left(_) => Ok(()),
                crate::util::Either::Right(()) => {
                    Err(format!("the workload did not finish within {} s of virtual time", MAX_VIRTUAL_MS / 1000))
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
        if let Some(v) = sim.violation.lock().unwrap_or_else(|e| e.into_inner()).take() {
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
    });
    crate::util::rng::branch(Vec::new());
    SimOutcome { probes: crate::probe::finish(), buggify: crate::buggify::disable(), ..out }
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
