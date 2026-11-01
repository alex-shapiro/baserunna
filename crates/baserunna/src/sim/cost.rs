//! Object-store request accounting and monthly cost estimation.

use crate::api;
use crate::cluster::{Transport, TransportError};
use crate::engine::local::{LocalTier, MemLocal};
use crate::engine::{Consistency, EngineConfig};
use crate::env::Env;
use crate::meta::SignupPolicy;
use crate::node::{Node, NodeConfig};
use crate::store::{BoxFut, DynStore, GetOpts, GetResult, ListPage, MemStore, ObjectStore, PutMode, SResult};
use axum::body::Body;
use bytes::Bytes;
use http::{Request, Response};
use serde_json::{Value as Json, json};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;
use tower::ServiceExt;

/// S3 Standard us-east-1 list prices in USD.
pub mod prices {
    pub const PUT: f64 = 0.005 / 1000.0;
    pub const LIST: f64 = 0.005 / 1000.0;
    pub const GET: f64 = 0.0004 / 1000.0;
    pub const DELETE: f64 = 0.0;
    pub const STORAGE_GB_MONTH: f64 = 0.023;
}

/// Object-store request and byte counts.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Counts {
    pub gets: f64,
    pub puts: f64,
    pub lists: f64,
    pub deletes: f64,
    pub put_bytes: f64,
}

impl Counts {
    pub fn total(&self) -> f64 {
        self.gets + self.puts + self.lists + self.deletes
    }

    pub fn dollars(&self) -> f64 {
        self.gets * prices::GET + self.puts * prices::PUT + self.lists * prices::LIST + self.deletes * prices::DELETE
    }

    pub fn per(&self, n: f64) -> Counts {
        Counts {
            gets: self.gets / n,
            puts: self.puts / n,
            lists: self.lists / n,
            deletes: self.deletes / n,
            put_bytes: self.put_bytes / n,
        }
    }

    fn minus(&self, o: &Counts) -> Counts {
        Counts {
            gets: self.gets - o.gets,
            puts: self.puts - o.puts,
            lists: self.lists - o.lists,
            deletes: self.deletes - o.deletes,
            put_bytes: self.put_bytes - o.put_bytes,
        }
    }

    fn plus(&self, o: &Counts) -> Counts {
        Counts {
            gets: self.gets + o.gets,
            puts: self.puts + o.puts,
            lists: self.lists + o.lists,
            deletes: self.deletes + o.deletes,
            put_bytes: self.put_bytes + o.put_bytes,
        }
    }
}

/// Returns the database ID for a store key, `"sys"`, or `""` for keys outside `dbs/`.
pub fn scope_of(key: &str) -> &str {
    key.strip_prefix("dbs/").and_then(|r| r.split('/').next()).unwrap_or("")
}

/// Object store wrapper that counts requests per database and injects fixed latency.
pub struct MeteredStore {
    inner: DynStore,
    latency_ms: AtomicU64,
    counts: Mutex<BTreeMap<String, Counts>>,
}

impl MeteredStore {
    pub fn new(inner: DynStore, latency_ms: u64) -> MeteredStore {
        MeteredStore { inner, latency_ms: AtomicU64::new(latency_ms), counts: Mutex::new(BTreeMap::new()) }
    }

    pub fn set_latency_ms(&self, ms: u64) {
        self.latency_ms.store(ms, Ordering::Relaxed);
    }

    fn count(&self, key: &str, f: impl FnOnce(&mut Counts)) {
        let mut g = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        f(g.entry(scope_of(key).to_string()).or_default());
    }

    async fn delay(&self) {
        let ms = self.latency_ms.load(Ordering::Relaxed);
        if ms > 0 {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
    }

    pub fn scope(&self, scope: &str) -> Counts {
        self.counts.lock().unwrap_or_else(|e| e.into_inner()).get(scope).copied().unwrap_or_default()
    }

    pub fn total(&self) -> Counts {
        self.counts.lock().unwrap_or_else(|e| e.into_inner()).values().fold(Counts::default(), |a, c| a.plus(c))
    }
}

impl ObjectStore for MeteredStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        Box::pin(async move {
            self.count(key, |c| c.gets += 1.0);
            self.delay().await;
            self.inner.get(key, opts).await
        })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move {
            let n = data.len() as f64;
            self.count(key, |c| {
                c.puts += 1.0;
                c.put_bytes += n;
            });
            self.delay().await;
            self.inner.put(key, data, mode).await
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        Box::pin(async move {
            self.count(key, |c| c.deletes += 1.0);
            self.delay().await;
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
            self.count(prefix, |c| c.lists += 1.0);
            self.delay().await;
            self.inner.list(prefix, start_after, limit).await
        })
    }

    fn time_precision_ms(&self) -> u64 {
        self.inner.time_precision_ms()
    }
}

#[derive(Default)]
struct PeerNet {
    routers: BTreeMap<String, axum::Router>,
    calls: BTreeMap<String, u64>,
    stalled: BTreeSet<String>,
}

type Peers = Arc<Mutex<PeerNet>>;

struct PeerTransport {
    peers: Peers,
}

impl Transport for PeerTransport {
    fn call(&self, addr: &str, req: Request<Body>) -> BoxFut<'static, Result<Response<Body>, TransportError>> {
        let mut net = self.peers.lock().unwrap_or_else(|e| e.into_inner());
        *net.calls.entry(req.uri().path().to_string()).or_default() += 1;
        let stalled = net.stalled.contains(addr);
        let router = net.routers.get(addr).cloned();
        drop(net);
        Box::pin(async move {
            if stalled {
                std::future::pending::<()>().await;
            }
            let router =
                router.ok_or_else(|| TransportError { message: "unknown peer".into(), maybe_delivered: false })?;
            match router.oneshot(req).await {
                Ok(r) => Ok(r),
                Err(e) => match e {},
            }
        })
    }
}

pub struct Resp {
    pub status: u16,
    pub headers: http::HeaderMap,
    pub body: Bytes,
}

impl Resp {
    pub fn json(&self) -> Json {
        serde_json::from_slice(&self.body).unwrap_or(Json::Null)
    }
}

/// Production defaults with fast password hashing and unlimited databases/rates.
pub fn node_config(name: &str, addr: &str) -> NodeConfig {
    NodeConfig {
        name: name.into(),
        addr: addr.into(),
        public_url: addr.into(),
        pbkdf2_iterations: 10,
        signup: SignupPolicy::Open,
        max_dbs_per_owner: 1_000_000,
        limits: crate::limits::LimitConfig::unlimited(),
        engine: EngineConfig::default(),
        ..NodeConfig::default()
    }
}

fn peer_addr(i: usize) -> String {
    format!("cost://n{i}")
}

/// Simulated cluster backed by a [`MeteredStore`].
pub struct World {
    pub store: Arc<MeteredStore>,
    pub mem: Arc<MemStore>,
    nodes: Vec<Arc<Node>>,
    disks: Vec<LocalTier>,
    peers: Peers,
    origin: Instant,
    base_ms: u64,
    configure: fn(&mut NodeConfig),
}

impl World {
    pub async fn new(n: usize, latency_ms: u64) -> World {
        World::with_config(n, latency_ms, |_| {}).await
    }

    pub async fn with_config(n: usize, latency_ms: u64, configure: fn(&mut NodeConfig)) -> World {
        let origin = Instant::now();
        let base_ms = 1_750_000_000_000;
        let mem = Arc::new(MemStore::new(Box::new(move || {
            base_ms + Instant::now().saturating_duration_since(origin).as_millis() as u64
        })));
        let store = Arc::new(MeteredStore::new(mem.clone(), latency_ms));
        let mut w = World {
            store,
            mem,
            nodes: Vec::new(),
            disks: Vec::new(),
            peers: Arc::default(),
            origin,
            base_ms,
            configure,
        };
        for _ in 0..n {
            w.start_node().await;
        }
        w
    }

    pub async fn start_node(&mut self) -> usize {
        let i = self.nodes.len();
        self.disks.push(LocalTier(Arc::new(MemLocal::new(256 << 20))));
        let node = self.boot(i).await;
        self.nodes.push(node);
        i
    }

    /// Restarts node `i`, clearing memory while preserving its local disk tier.
    pub async fn restart_node(&mut self, i: usize) {
        self.nodes[i].env.shutdown();
        self.nodes[i] = self.boot(i).await;
    }

    async fn boot(&self, i: usize) -> Arc<Node> {
        let addr = peer_addr(i);
        let env = Arc::new(Env::sim(0xc057 + i as u64 + 1000 * self.nodes.len() as u64, self.base_ms, self.origin, 0));
        let store: DynStore = self.store.clone();
        let transport = Arc::new(PeerTransport { peers: self.peers.clone() });
        let mut cfg = node_config(&format!("n{i}"), &addr);
        cfg.engine.local_tier = Some(self.disks[i].clone());
        (self.configure)(&mut cfg);
        let services = crate::node::Services { transport, oidc: None, mailer: None };
        let node = Node::start(cfg, env, store, services).await.expect("node starts");
        let router = api::router(node.clone());
        self.peers.lock().unwrap_or_else(|e| e.into_inner()).routers.insert(addr, router);
        node
    }

    /// Returns the total number of peer requests sent to `path`.
    pub fn peer_calls(&self, path: &str) -> u64 {
        self.peers.lock().unwrap_or_else(|e| e.into_inner()).calls.get(path).copied().unwrap_or(0)
    }

    /// Configures whether peer requests to node `i` hang indefinitely.
    pub fn stall_peer(&self, i: usize, stalled: bool) {
        let addr = peer_addr(i);
        let mut net = self.peers.lock().unwrap_or_else(|e| e.into_inner());
        if stalled {
            net.stalled.insert(addr);
        } else {
            net.stalled.remove(&addr);
        }
    }

    pub fn node(&self, i: usize) -> &Arc<Node> {
        &self.nodes[i]
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub async fn call(&self, i: usize, method: &str, path: &str, token: &str, body: Bytes) -> Resp {
        call_router(&self.nodes[i].router().expect("router"), method, path, token, body).await
    }

    pub async fn json(&self, i: usize, method: &str, path: &str, token: &str, body: Json) -> Json {
        self.call(i, method, path, token, Bytes::from(body.to_string())).await.json()
    }

    pub async fn signup(&self, i: usize, email: &str) -> String {
        let r = self.json(i, "POST", "/v1/auth/signup", "", json!({"email": email, "password": "password-123"})).await;
        r["token"].as_str().expect("signup token").to_string()
    }

    pub async fn create_db(&self, i: usize, token: &str, name: &str) -> String {
        let r = self.json(i, "POST", "/v1/dbs", token, json!({"name": name})).await;
        r["id"].as_str().unwrap_or_else(|| panic!("create db: {r}")).to_string()
    }

    pub fn holder(&self, db_id: &str) -> Option<usize> {
        let h = self.nodes.iter().find_map(|n| n.engine.db(db_id).lease_holder())?;
        self.nodes.iter().position(|n| n.id == h)
    }

    pub fn stored_bytes(&self, db_id: &str) -> u64 {
        let p = format!("dbs/{db_id}/");
        self.mem.list_sync(&p, None, usize::MAX).objects.iter().map(|o| o.size).sum()
    }

    pub fn shutdown(&self) {
        for n in &self.nodes {
            n.env.shutdown();
        }
    }
}

pub async fn call_router(router: &axum::Router, method: &str, path: &str, token: &str, body: Bytes) -> Resp {
    let mut b = Request::builder().method(method).uri(path).header("content-type", "application/json");
    if !token.is_empty() {
        b = b.header("authorization", format!("Bearer {token}"));
    }
    let req = b.header("content-length", body.len().to_string()).body(Body::from(body)).expect("request");
    let resp = match router.clone().oneshot(req).await {
        Ok(r) => r,
        Err(e) => match e {},
    };
    let (parts, body) = resp.into_parts();
    let body = http_body_util::BodyExt::collect(body).await.map(|b| b.to_bytes()).unwrap_or_default();
    Resp { status: parts.status.as_u16(), headers: parts.headers, body }
}

pub fn run_paused<F: Future>(f: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().expect("runtime");
    rt.block_on(f)
}

pub const LATENCY_MS: u64 = 20;

/// Per-unit store request counts and latency for a benchmarked operation.
#[derive(Clone, Debug)]
pub struct Measurement {
    pub name: &'static str,
    pub unit: &'static str,
    pub per_unit: Counts,
    pub latency_ms: Option<f64>,
}

#[derive(Clone, Debug)]
pub struct Report {
    pub measurements: Vec<Measurement>,
    /// Ratio of stored bytes to live logical bytes after steady writes.
    pub space_amplification: f64,
    pub cold_open_segments: usize,
}

impl Report {
    pub fn get(&self, name: &str) -> &Measurement {
        self.measurements.iter().find(|m| m.name == name).unwrap_or_else(|| panic!("no measurement {name}"))
    }

    pub fn table(&self) -> String {
        let mut s = format!(
            "{:<26} {:<14} {:>9} {:>9} {:>7} {:>8} {:>11} {:>10}\n",
            "measurement", "per", "GET", "PUT", "LIST", "DELETE", "$ per 1M", "latency"
        );
        for m in &self.measurements {
            let c = &m.per_unit;
            s.push_str(&format!(
                "{:<26} {:<14} {:>9.3} {:>9.3} {:>7.3} {:>8.3} {:>11.4} {:>10}\n",
                m.name,
                m.unit,
                c.gets,
                c.puts,
                c.lists,
                c.deletes,
                c.dollars() * 1e6,
                m.latency_ms.map(|l| format!("{l:.0} ms")).unwrap_or_default(),
            ));
        }
        s.push_str(&format!(
            "space amplification after steady writes: {:.2}x; cold open read {} segments\n",
            self.space_amplification, self.cold_open_segments
        ));
        s
    }
}

struct Recorder<'a> {
    store: &'a MeteredStore,
    scope: String,
    before: Counts,
    started: Instant,
}

impl<'a> Recorder<'a> {
    fn start(store: &'a MeteredStore, scope: &str) -> Recorder<'a> {
        Recorder { store, scope: scope.to_string(), before: store.scope(scope), started: Instant::now() }
    }

    fn stop(self, name: &'static str, unit: &'static str, units: f64, timed: bool) -> Measurement {
        let d = self.store.scope(&self.scope).minus(&self.before);
        let ms = self.started.elapsed().as_millis() as f64;
        Measurement { name, unit, per_unit: d.per(units), latency_ms: timed.then(|| ms / units) }
    }
}

fn value(n: usize, seed: usize) -> Bytes {
    let mut r = crate::util::rng::Rng::new(seed as u64);
    Bytes::from((0..n).map(|_| b'a' + r.below(26) as u8).collect::<Vec<u8>>())
}

pub fn measure_all() -> Report {
    run_paused(async {
        let mut out = Vec::new();
        let (amp, segs) = {
            let mut w = World::new(1, LATENCY_MS).await;
            single_node(&w, &mut out).await;
            let amp = steady_writes(&w, &mut out).await;
            overhead(&w, &mut out, "node_overhead_1_node").await;
            let segs = multi_node(&mut w, &mut out).await;
            streams(&mut w, &mut out).await;
            events(&mut w, &mut out).await;
            w.shutdown();
            (amp, segs)
        };
        let w = World::new(3, LATENCY_MS).await;
        overhead(&w, &mut out, "node_overhead_3_nodes").await;
        w.shutdown();
        Report { measurements: out, space_amplification: amp, cold_open_segments: segs }
    })
}

async fn single_node(w: &World, out: &mut Vec<Measurement>) {
    let s = &*w.store;
    let t = w.signup(0, "cost@cost.test").await;
    let id = w.create_db(0, &t, "c").await;
    let kv = |k: &str| format!("/v1/dbs/c/kv/{k}");
    w.call(0, "PUT", &kv("warm"), &t, value(100, 0)).await;

    let r = Recorder::start(s, &id);
    for i in 0..20 {
        let resp = w.call(0, "PUT", &kv(&format!("k{i}")), &t, value(100, i)).await;
        assert_eq!(resp.status, 200);
    }
    out.push(r.stop("commit", "commit", 20.0, true));

    let r = Recorder::start(s, &id);
    for i in 0..100 {
        w.call(0, "GET", &kv(&format!("k{}", i % 20)), &t, Bytes::new()).await;
    }
    out.push(r.stop("strong_read_leased", "read", 100.0, true));

    let n = 64;
    let r = Recorder::start(s, &id);
    let router = w.node(0).router().expect("router");
    let hs: Vec<_> = (0..n)
        .map(|i| {
            let router = router.clone();
            let t = t.clone();
            tokio::spawn(async move {
                call_router(&router, "PUT", &format!("/v1/dbs/c/kv/g{i}"), &t, value(100, i)).await.status
            })
        })
        .collect();
    for h in hs {
        assert_eq!(h.await.expect("writer"), 200);
    }
    out.push(r.stop("group_commit_64", "commit", n as f64, false));

    let mib = 16usize;
    let data = value(mib << 20, 1);
    let r = Recorder::start(s, &id);
    assert_eq!(w.call(0, "PUT", &kv("blob"), &t, data).await.status, 200);
    out.push(r.stop("blob_upload", "MiB", mib as f64, false));
    let r = Recorder::start(s, &id);
    let g = w.call(0, "GET", &kv("blob"), &t, Bytes::new()).await;
    assert_eq!(g.body.len(), mib << 20);
    out.push(r.stop("blob_download", "MiB", mib as f64, false));

    w.call(0, "PUT", &kv("cold"), &t, value(100, 2)).await;
    tokio::time::sleep(Duration::from_secs(60)).await;
    let r = Recorder::start(s, &id);
    for _ in 0..60 {
        w.call(0, "GET", &kv("cold"), &t, Bytes::new()).await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
    let mut m = r.stop("strong_read_unleased", "read", 60.0, false);
    m.latency_ms = None;
    out.push(m);

    let r = Recorder::start(s, &id);
    for _ in 0..HOT_READS_PER_HOUR / 6 {
        w.call(0, "GET", &kv("cold"), &t, Bytes::new()).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut m = r.stop("hot_reads_10_per_s", "hour", 1.0, false);
    m.per_unit = m.per_unit.per(1.0 / 6.0);
    out.push(m);

    let idle = w.create_db(0, &t, "idle").await;
    w.call(0, "PUT", "/v1/dbs/idle/kv/x", &t, value(100, 3)).await;
    tokio::time::sleep(Duration::from_secs(60)).await;
    let r = Recorder::start(s, &idle);
    tokio::time::sleep(Duration::from_secs(3600)).await;
    out.push(r.stop("idle_db", "hour", 1.0, false));

    // Measure an idle change-feed watcher polling with 5-minute snapshot staleness.
    let feed = "/v1/dbs/idle/changes?after=0&limit=1";
    let cursor = w.json(0, "GET", feed, &t, Json::Null).await["next_cursor"].as_str().expect("cursor").to_string();
    let wait = format!(
        "/v1/dbs/idle/changes?after={cursor}&limit=1&wait_ms=50000&consistency=snapshot&max_staleness_ms=300000"
    );
    let r = Recorder::start(s, &idle);
    let until = Instant::now() + Duration::from_secs(3600);
    while Instant::now() < until {
        let resp = w.json(0, "GET", &wait, &t, Json::Null).await;
        assert_eq!(resp["changes"].as_array().map(|c| c.len()), Some(0));
    }
    out.push(r.stop("idle_watcher", "hour", 1.0, false));
}

/// Runs 1 commit/s over 1,000 keys for an hour and returns mean space
/// amplification over the second half.
async fn steady_writes(w: &World, out: &mut Vec<Measurement>) -> f64 {
    let t = w.signup(0, "steady@cost.test").await;
    let id = w.create_db(0, &t, "steady").await;
    let mut live: BTreeMap<usize, usize> = BTreeMap::new();
    let mut rng = crate::util::rng::Rng::new(7);
    let r = Recorder::start(&w.store, &id);
    let n = 3600;
    let (mut amp_sum, mut samples) = (0.0, 0);
    for i in 0..n {
        let k = rng.usize(1000);
        let resp = w.call(0, "PUT", &format!("/v1/dbs/steady/kv/w{k:04}"), &t, value(256, i)).await;
        assert_eq!(resp.status, 200);
        live.insert(k, 5 + 256);
        tokio::time::sleep(Duration::from_secs(1)).await;
        if i >= n / 2 && i % 10 == 0 {
            amp_sum += w.stored_bytes(&id) as f64 / live.values().sum::<usize>() as f64;
            samples += 1;
        }
    }
    out.push(r.stop("steady_write", "commit", n as f64, false));
    amp_sum / samples as f64
}

async fn overhead(w: &World, out: &mut Vec<Measurement>, name: &'static str) {
    tokio::time::sleep(Duration::from_secs(120)).await;
    let r = Recorder::start(&w.store, "sys");
    tokio::time::sleep(Duration::from_secs(3600)).await;
    out.push(r.stop(name, "node-hour", w.len() as f64, false));
}

async fn multi_node(w: &mut World, out: &mut Vec<Measurement>) -> usize {
    let t = w.signup(0, "multi@cost.test").await;
    let id = w.create_db(0, &t, "big").await;
    for i in 0..128 {
        let r = w.call(0, "PUT", &format!("/v1/dbs/big/kv/b{i:03}"), &t, value(16 << 10, i)).await;
        assert_eq!(r.status, 200);
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    let other = w.start_node().await;
    let key = crate::ops::user_key(b"b000").expect("key");
    let db = w.node(other).engine.db(&id);

    let r = Recorder::start(&w.store, &id);
    let v = db.read_version(Consistency::Strong).await.expect("cold open");
    let segs = v.m.segments.len();
    assert!(v.get(&key, v.read_time()).await.expect("read").is_some());
    out.push(r.stop("cold_open_point_read", "open", 1.0, true));

    let r = Recorder::start(&w.store, &id);
    let all = v.scan(&crate::engine::KeyRange::data(), false, usize::MAX, v.read_time()).await.expect("scan");
    assert_eq!(all.len(), 128);
    out.push(r.stop("cold_full_scan", "scan", 1.0, true));

    w.call(0, "PUT", "/v1/dbs/big/kv/b000", &t, value(16, 0)).await;
    assert_ne!(w.holder(&id), Some(other));
    let r = Recorder::start(&w.store, &id);
    for _ in 0..10 {
        let v = db.read_version(Consistency::Strong).await.expect("read");
        v.get(&key, v.read_time()).await.expect("get");
    }
    out.push(r.stop("strong_read_validated", "read", 10.0, true));

    let r = Recorder::start(&w.store, &id);
    for _ in 0..10 {
        let v = db.read_version(Consistency::Snapshot { max_staleness_ms: 5_000 }).await.expect("read");
        v.get(&key, v.read_time()).await.expect("get");
    }
    out.push(r.stop("snapshot_read", "read", 10.0, true));

    // Measure cross-node wakeup latency when a remote writer commits while a request waits on the home node.
    tokio::time::sleep(Duration::from_secs(30)).await;
    let live = w.node(0).live_members();
    let home_id = crate::cluster::rendezvous(&id, &live).expect("home").id.clone();
    let home = (0..w.len()).find(|&i| w.node(i).id == home_id).expect("home node");
    let writer = (home + 1) % w.len();
    let cursor = w.json(home, "GET", "/v1/dbs/big/changes?after=0&limit=1000", &t, Json::Null).await["next_cursor"]
        .as_str()
        .expect("cursor")
        .to_string();
    let wait = format!("/v1/dbs/big/changes?after={cursor}&wait_ms=50000&consistency=snapshot&max_staleness_ms=300000");
    let router = w.node(home).router().expect("router");
    let t2 = t.clone();
    let waiter = tokio::spawn(async move {
        let resp = call_router(&router, "GET", &wait, &t2, Bytes::new()).await;
        (Instant::now(), resp.json()["changes"].as_array().map(|c| c.len()).unwrap_or(0))
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    let wdb = w.node(writer).engine.db(&id);
    let mut txn = wdb.begin(crate::engine::Isolation::Serializable, Consistency::Strong).await.expect("begin");
    txn.put(crate::ops::user_key(b"woken").expect("key"), crate::engine::Value::Inline(value(8, 9))).expect("put");
    let r = Recorder::start(&w.store, &id);
    let committed = Instant::now();
    wdb.commit(txn.into_request()).await.expect("commit");
    let (woke, n) = waiter.await.expect("waiter");
    assert_eq!(n, 1, "the waiter saw the commit");
    let mut m = r.stop("watch_wakeup", "commit", 1.0, false);
    m.latency_ms = Some(woke.duration_since(committed).as_millis() as f64);
    out.push(m);

    w.restart_node(other).await;
    let db = w.node(other).engine.db(&id);
    let r = Recorder::start(&w.store, &id);
    let v = db.read_version(Consistency::Strong).await.expect("open after restart");
    assert!(v.get(&key, v.read_time()).await.expect("read").is_some());
    let all = v.scan(&crate::engine::KeyRange::data(), false, usize::MAX, v.read_time()).await.expect("scan");
    assert_eq!(all.len(), 129);
    out.push(r.stop("restart_open_and_scan", "open", 1.0, true));
    segs
}

/// Appends, tail reads, steady log ingestion, and a cold node reading a
/// sealed log (SPEC §5.11).
async fn streams(w: &mut World, out: &mut Vec<Measurement>) {
    let t = w.signup(0, "streams@cost.test").await;
    let id = w.create_db(0, &t, "logs").await;
    let s = &*w.store;
    let records = "/v1/dbs/logs/streams/app/records";
    w.json(0, "POST", "/v1/dbs/logs/streams", &t, json!({"name": "app"})).await;
    let append = |n: usize, i: usize| {
        let line = |j: usize| {
            format!("{{\"level\":\"info\",\"i\":{i},\"j\":{j},\"msg\":\"{}\"}}\n", value(150, i + j).escape_ascii())
        };
        Bytes::from((0..n).map(line).collect::<String>())
    };
    let router = w.node(0).router().expect("router");
    let ndjson = |body: Bytes| post_ndjson(&router, &t, records, body);
    assert_eq!(ndjson(append(1, 0)).await, 200);

    let r = Recorder::start(s, &id);
    for i in 0..20 {
        assert_eq!(ndjson(append(1, i)).await, 200);
    }
    out.push(r.stop("stream_append", "append", 20.0, true));

    let r = Recorder::start(s, &id);
    for _ in 0..100 {
        let got = w.json(0, "GET", &format!("{records}?tail=5"), &t, Json::Null).await;
        assert_eq!(got["records"].as_array().map(Vec::len), Some(5));
    }
    out.push(r.stop("stream_tail_read_leased", "read", 100.0, true));

    // Steady ingestion: a request of 100 log lines (about 20 KB) a second.
    let r = Recorder::start(s, &id);
    let requests = 1200;
    for i in 0..requests {
        assert_eq!(ndjson(append(100, i)).await, 200);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let mib = (requests * append(100, 0).len()) as f64 / (1 << 20) as f64;
    out.push(r.stop("log_ingest", "MiB", mib, false));
    tokio::time::sleep(Duration::from_secs(60)).await;

    let other = w.start_node().await;
    let db = w.node(other).engine.db(&id);
    let r = Recorder::start(&w.store, &id);
    let v = db.read_version(Consistency::Strong).await.expect("cold open");
    let mut t = crate::engine::Txn::new(
        crate::engine::Snapshot { time_ms: v.read_time(), version: v.clone() },
        crate::engine::Isolation::Serializable,
        0,
        usize::MAX,
    );
    let (c, st) = crate::stream::info(&mut t, "app").await.expect("info");
    let mut it = v.stream_iter(c.id, st.first_seq, st.next_seq, false);
    let mut bytes = 0;
    while let Some(rec) = it.next().await.expect("read") {
        bytes += rec.payload.len();
    }
    assert_eq!(st.records(), 21 + 100 * requests as u64);
    out.push(r.stop("cold_stream_read", "MiB", bytes as f64 / (1 << 20) as f64, true));
}

async fn events(w: &mut World, out: &mut Vec<Measurement>) {
    let t = w.signup(0, "events@cost.test").await;
    let id = w.create_db(0, &t, "analytics").await;
    let s = &*w.store;
    let table = json!({"name": "e", "columns": [{"name": "time", "type": "time"}, {"name": "event", "type": "string"},
        {"name": "user", "type": "string"}, {"name": "value", "type": "float"}, {"name": "props", "type": "json"}],
        "time_column": "time", "order_by": ["event", "time"]});
    assert_eq!(w.call(0, "POST", "/v1/dbs/analytics/events", &t, Bytes::from(table.to_string())).await.status, 201);
    let day = 1_700_000_000_000i64 / 86_400_000 * 86_400_000;
    let event = |i: usize| {
        json!({"time": day + (i as i64 * 7_919) % 86_400_000, "event": (["view", "click", "buy"][i % 3]),
            "user": format!("u{}", i % 997), "value": (i % 100) as f64 / 4.0,
            "props": {"page": format!("/p{}", i % 50), "ref": (["a", "b"][i % 2])}})
    };
    let txn = |i: usize| {
        let rows: Vec<Json> = (0..10).map(|j| event(i * 10 + j)).collect();
        Bytes::from(json!({"ops": [{"op": "insert_events", "table": "e", "rows": rows}]}).to_string())
    };
    assert_eq!(w.call(0, "POST", "/v1/dbs/analytics/txn", &t, txn(0)).await.status, 200);
    let r = Recorder::start(s, &id);
    for i in 1..21 {
        assert_eq!(w.call(0, "POST", "/v1/dbs/analytics/txn", &t, txn(i)).await.status, 200);
    }
    out.push(r.stop("event_txn_insert", "commit", 20.0, true));

    let router = w.node(0).router().expect("router");
    let r = Recorder::start(s, &id);
    let mut bytes = 0;
    for round in 0..16 {
        let mut handles = Vec::new();
        for k in 0..32 {
            let base = 1_000_000 + (round * 32 + k) * 500;
            let body: String = (0..500).map(|i| event(base + i).to_string() + "\n").collect();
            bytes += body.len();
            let router = router.clone();
            let t = t.clone();
            handles.push(tokio::spawn(async move {
                post_ndjson(&router, &t, "/v1/dbs/analytics/events/e/rows", Bytes::from(body)).await
            }));
        }
        for h in handles {
            assert_eq!(h.await.expect("insert"), 201);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    tokio::time::sleep(Duration::from_secs(60)).await;
    out.push(r.stop("event_bulk_ingest", "MiB", bytes as f64 / (1 << 20) as f64, false));

    let query = json!({"from": day, "to": day + 86_400_000, "group_by": ["event"],
        "aggregates": {"n": {"fn": "count"}, "total": {"fn": "sum", "of": "value"}}});
    let path = "/v1/dbs/analytics/events/e/rows/query";
    let first = w.json(0, "POST", path, &t, query.clone()).await;
    assert_eq!(first["rows"].as_array().map(Vec::len), Some(3), "{first}");
    let r = Recorder::start(s, &id);
    for _ in 0..10 {
        w.json(0, "POST", path, &t, query.clone()).await;
    }
    out.push(r.stop("event_query_warm", "query", 10.0, true));

    let other = w.start_node().await;
    let parts = first["stats"]["parts"].as_u64().unwrap_or(1).max(1);
    let r = Recorder::start(&w.store, &id);
    let cold = w.json(other, "POST", path, &t, query.clone()).await;
    assert_eq!(cold["rows"], first["rows"]);
    out.push(r.stop("event_query_cold", "part", parts as f64, true));

    let r = Recorder::start(&w.store, &id);
    tokio::time::sleep(Duration::from_secs(3_600)).await;
    out.push(r.stop("event_table_idle", "hour", 1.0, false));
}

async fn post_ndjson(router: &axum::Router, token: &str, path: &str, body: Bytes) -> u16 {
    let req = Request::post(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/x-ndjson")
        .body(Body::from(body))
        .expect("request");
    match router.clone().oneshot(req).await {
        Ok(r) => r.status().as_u16(),
        Err(e) => match e {},
    }
}

/// Workload parameters for monthly cost estimation.
#[derive(Clone, Debug)]
pub struct Profile {
    pub name: &'static str,
    pub reads_per_day: f64,
    pub writes_per_day: f64,
    pub active_hours: f64,
    pub data_bytes: f64,
    pub blob_bytes: f64,
    pub blob_upload_bytes_per_day: f64,
    pub blob_download_bytes_per_day: f64,
}

const MB: f64 = 1e6;
const GB: f64 = 1e9;

pub fn profiles() -> Vec<Profile> {
    vec![
        Profile {
            name: "idle",
            reads_per_day: 0.0,
            writes_per_day: 0.0,
            active_hours: 0.0,
            data_bytes: 5.0 * MB,
            blob_bytes: 0.0,
            blob_upload_bytes_per_day: 0.0,
            blob_download_bytes_per_day: 0.0,
        },
        Profile {
            name: "light",
            reads_per_day: 1_000.0,
            writes_per_day: 100.0,
            active_hours: 8.0,
            data_bytes: 1.0 * MB,
            blob_bytes: 0.0,
            blob_upload_bytes_per_day: 0.0,
            blob_download_bytes_per_day: 0.0,
        },
        Profile {
            name: "typical",
            reads_per_day: 100_000.0,
            writes_per_day: 10_000.0,
            active_hours: 12.0,
            data_bytes: 20.0 * MB,
            blob_bytes: 100.0 * MB,
            blob_upload_bytes_per_day: 10.0 * MB,
            blob_download_bytes_per_day: 50.0 * MB,
        },
        Profile {
            name: "heavy",
            reads_per_day: 5_000_000.0,
            writes_per_day: 500_000.0,
            active_hours: 24.0,
            data_bytes: 200.0 * MB,
            blob_bytes: 5.0 * GB,
            blob_upload_bytes_per_day: 500.0 * MB,
            blob_download_bytes_per_day: 5.0 * GB,
        },
    ]
}

/// Monthly USD cost breakdown for a database.
#[derive(Clone, Debug, Default)]
pub struct Bill {
    pub writes: f64,
    pub reads: f64,
    pub blobs: f64,
    pub storage: f64,
}

impl Bill {
    pub fn total(&self) -> f64 {
        self.writes + self.reads + self.blobs + self.storage
    }
}

pub const HOT_READS_PER_HOUR: u64 = 36_000;

/// Estimates the 30-day USD cost of `p` from benchmark report `r`.
pub fn monthly_cost(r: &Report, p: &Profile) -> Bill {
    let cfg = EngineConfig::default();
    let days = 30.0;
    let writes = p.writes_per_day * r.get("steady_write").per_unit.dollars() * days;
    let reads = if p.reads_per_day == 0.0 || p.active_hours == 0.0 {
        0.0
    } else {
        let lease_s = cfg.read_lease_ms as f64 / 1000.0;
        let periods = p.active_hours * 3600.0 / lease_s;
        let per_period = p.reads_per_day / periods;
        let period_cost = if per_period >= cfg.lease_read_threshold as f64 {
            let hot = r.get("hot_reads_10_per_s").per_unit;
            let per_hour = Counts { gets: 0.0, ..hot }.dollars();
            let per_read = hot.gets / HOT_READS_PER_HOUR as f64 * prices::GET;
            per_hour * lease_s / 3600.0 + per_period * per_read
        } else {
            per_period * r.get("strong_read_unleased").per_unit.dollars()
        };
        periods * period_cost * days
    };
    let mib = (1u64 << 20) as f64;
    let blobs = (p.blob_upload_bytes_per_day / mib * r.get("blob_upload").per_unit.dollars()
        + p.blob_download_bytes_per_day / mib * r.get("blob_download").per_unit.dollars())
        * days;
    let storage = (p.data_bytes * r.space_amplification + p.blob_bytes) / GB * prices::STORAGE_GB_MONTH;
    Bill { writes, reads, blobs, storage }
}

pub fn cost_table(r: &Report) -> String {
    let mut s = format!(
        "{:<9} {:>10} {:>10} {:>7} {:>9} {:>9} {:>9} {:>9} {:>10}\n",
        "profile", "reads/day", "writes/day", "data", "writes $", "reads $", "blobs $", "storage $", "total $/mo"
    );
    for p in profiles() {
        let b = monthly_cost(r, &p);
        s.push_str(&format!(
            "{:<9} {:>10} {:>10} {:>5.0}MB {:>9.4} {:>9.4} {:>9.4} {:>9.4} {:>10.4}\n",
            p.name,
            p.reads_per_day,
            p.writes_per_day,
            (p.data_bytes + p.blob_bytes) / MB,
            b.writes,
            b.reads,
            b.blobs,
            b.storage,
            b.total()
        ));
    }
    for name in ["node_overhead_1_node", "node_overhead_3_nodes"] {
        let m = r.get(name);
        s.push_str(&format!("{name}: ${:.2} per node-month\n", m.per_unit.dollars() * 24.0 * 30.0));
    }
    s
}
