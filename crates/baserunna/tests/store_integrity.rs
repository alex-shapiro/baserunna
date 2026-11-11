//! Verifies that a node detects corrupt, truncated, or substituted objects from the object store.
//!
//! Each test stores data through the HTTP API, configures [`TamperStore`] to return a damaged object,
//! restarts the node with cold caches, and reads the data back through the HTTP API. A test cell passes
//! if the read fails or returns the written data. A test cell fails if the read returns wrong data or if
//! a write commits the damaged content.

use axum::body::Body;
use baserunna::cluster::{Transport, TransportError};
use baserunna::engine::Consistency;
use baserunna::engine::local::{DiskCache, LocalTier};
use baserunna::engine::manifest::{Levels, Manifest};
use baserunna::engine::segment::{self, FOOTER_LEN, SegmentIndex};
use baserunna::env::Env;
use baserunna::log::{self, Level};
use baserunna::node::{Node, NodeConfig, Services};
use baserunna::sim::cost::{node_config, run_paused};
use baserunna::store::{BoxFut, GetOpts, GetResult, ListPage, MemStore, ObjectStore, PutMode, SResult, StoreError};
use baserunna::util::rng::Rng;
use baserunna_api::b64;
use bytes::Bytes;
use http::Request;
use parking_lot::Mutex;
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tower::ServiceExt;

// ---------------------------------------------------------------------------------------------
// Tampering object store
// ---------------------------------------------------------------------------------------------

/// Fault that [`TamperStore`] applies to `GET` requests for a target object.
#[derive(Clone, Debug)]
enum Fault {
    /// Inverts the lowest bit of the byte at this offset.
    FlipBit(usize),
    /// Inverts the bits in `mask` of the byte at offset `at`.
    FlipBits { at: usize, mask: u8 },
    /// Returns only the first bytes of the object.
    Truncate(usize),
    /// Returns other bytes in place of the object.
    Replace(Bytes),
    /// Serves a ranged `GET` that starts at `from` with the bytes that start at `from + by`.
    ShiftRange { from: u64, by: u64 },
}

impl Fault {
    /// Returns true if a `GET` for `range` on an object of `len` bytes returns damaged bytes.
    fn hits(&self, range: &Option<Range<u64>>, len: u64) -> bool {
        let r = range.clone().unwrap_or(0..len);
        match self {
            Fault::FlipBit(at) | Fault::FlipBits { at, .. } => r.contains(&(*at as u64)),
            Fault::Truncate(n) => r.end > *n as u64,
            Fault::Replace(_) => true,
            Fault::ShiftRange { from, .. } => r.start == *from,
        }
    }

    fn damage(&self, data: &Bytes) -> Bytes {
        match self {
            Fault::FlipBit(at) => Fault::FlipBits { at: *at, mask: 1 }.damage(data),
            Fault::FlipBits { at, mask } => {
                let mut d = data.to_vec();
                d[*at] ^= mask;
                Bytes::from(d)
            }
            Fault::Truncate(len) => data.slice(..*len),
            Fault::Replace(other) => other.clone(),
            Fault::ShiftRange { .. } => data.clone(),
        }
    }
}

/// Clamps `range` to `[0, size)` with the same rules as `MemStore`.
fn clamp_range(range: &Option<Range<u64>>, size: u64) -> SResult<Range<u64>> {
    match range {
        None => Ok(0..size),
        Some(r) if r.start <= r.end && (r.start < size || r.start == r.end && r.start <= size) => {
            Ok(r.start..r.end.min(size))
        }
        Some(_) => Err(StoreError::Fatal("invalid range".into())),
    }
}

/// In-memory object store that returns damaged copies of chosen objects and counts requests.
struct TamperStore {
    mem: Arc<MemStore>,
    faults: Mutex<BTreeMap<String, Fault>>,
    gets: Mutex<BTreeMap<String, u64>>,
    /// Number of `GET`s per key that returned damaged bytes.
    damaged_gets: Mutex<BTreeMap<String, u64>>,
    puts: Mutex<u64>,
    lists: Mutex<u64>,
}

impl TamperStore {
    fn new(mem: Arc<MemStore>) -> TamperStore {
        TamperStore {
            mem,
            faults: Mutex::default(),
            gets: Mutex::default(),
            damaged_gets: Mutex::default(),
            puts: Mutex::default(),
            lists: Mutex::default(),
        }
    }

    fn set_fault(&self, key: &str, fault: Fault) {
        self.faults.lock().insert(key.to_string(), fault);
    }

    fn clear_faults(&self) {
        self.faults.lock().clear();
    }

    fn gets_of(&self, key: &str) -> u64 {
        self.gets.lock().get(key).copied().unwrap_or(0)
    }

    fn damaged_gets_of(&self, key: &str) -> u64 {
        self.damaged_gets.lock().get(key).copied().unwrap_or(0)
    }

    fn gets_under(&self, prefix: &str) -> u64 {
        self.gets.lock().iter().filter(|(k, _)| k.starts_with(prefix)).map(|(_, n)| n).sum()
    }

    fn get_damaged(&self, key: &str, opts: &GetOpts, fault: Fault) -> SResult<GetResult> {
        if let Fault::ShiftRange { from, by } = fault {
            if fault.hits(&opts.range, 0) {
                *self.damaged_gets.lock().entry(key.to_string()).or_default() += 1;
            }
            let range = match &opts.range {
                Some(r) if r.start == from => Some(r.start + by..r.end + by),
                r => r.clone(),
            };
            return self.mem.get_sync(key, &GetOpts { range, ..opts.clone() });
        }
        let whole = self.mem.get_sync(key, &GetOpts { range: None, if_none_match: opts.if_none_match.clone() })?;
        if fault.hits(&opts.range, whole.data.len() as u64) {
            *self.damaged_gets.lock().entry(key.to_string()).or_default() += 1;
        }
        let data = fault.damage(&whole.data);
        let r = clamp_range(&opts.range, data.len() as u64)?;
        Ok(GetResult {
            data: data.slice(r.start as usize..r.end as usize),
            etag: whole.etag,
            size: data.len() as u64,
            last_modified_ms: whole.last_modified_ms,
        })
    }
}

impl ObjectStore for TamperStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        *self.gets.lock().entry(key.to_string()).or_default() += 1;
        let fault = self.faults.lock().get(key).cloned();
        let r = match fault {
            None => self.mem.get_sync(key, &opts),
            Some(f) => self.get_damaged(key, &opts, f),
        };
        Box::pin(async move { r })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        *self.puts.lock() += 1;
        self.mem.put(key, data, mode)
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        self.mem.delete(key)
    }

    fn list<'a>(&'a self, prefix: &'a str, after: Option<&'a str>, limit: usize) -> BoxFut<'a, SResult<ListPage>> {
        *self.lists.lock() += 1;
        self.mem.list(prefix, after, limit)
    }

    fn time_precision_ms(&self) -> u64 {
        self.mem.time_precision_ms()
    }
}

// ---------------------------------------------------------------------------------------------
// Single-node deployment
// ---------------------------------------------------------------------------------------------

/// Peer transport for a single-node deployment.
struct NoPeers;

impl Transport for NoPeers {
    fn call(&self, _: &str, _: Request<Body>) -> BoxFut<'static, Result<http::Response<Body>, TransportError>> {
        Box::pin(async { Err(TransportError { message: "no peers".into(), maybe_delivered: false }) })
    }
}

const BASE_MS: u64 = 1_750_000_000_000;

/// HTTP response with a body that can fail after the status line, like a streamed blob.
struct Reply {
    status: u16,
    headers: http::HeaderMap,
    body: Result<Bytes, String>,
}

impl Reply {
    fn json(&self) -> Json {
        self.body.as_ref().ok().and_then(|b| serde_json::from_slice(b).ok()).unwrap_or(Json::Null)
    }

    fn excerpt(&self) -> String {
        match &self.body {
            Ok(b) => String::from_utf8_lossy(&b[..b.len().min(160)]).into_owned(),
            Err(e) => format!("body stream failed: {e}"),
        }
    }

    fn failed(&self) -> bool {
        self.status >= 400 || self.body.is_err()
    }
}

/// Single node backed by [`TamperStore`] that can be restarted with cold memory caches.
struct Deployment {
    mem: Arc<MemStore>,
    store: Arc<TamperStore>,
    origin: Instant,
    configure: fn(&mut NodeConfig),
    local_tier: Option<LocalTier>,
    boots: u64,
    node: Option<Arc<Node>>,
}

impl Deployment {
    async fn start(configure: fn(&mut NodeConfig), local_tier: Option<LocalTier>) -> Deployment {
        let origin = Instant::now();
        let mem = Arc::new(MemStore::new(Box::new(move || {
            BASE_MS + Instant::now().saturating_duration_since(origin).as_millis() as u64
        })));
        let store = Arc::new(TamperStore::new(mem.clone()));
        let mut d = Deployment { mem, store, origin, configure, local_tier, boots: 0, node: None };
        d.boot().await.expect("node starts");
        d
    }

    async fn boot(&mut self) -> Result<(), String> {
        self.boots += 1;
        let env = Arc::new(Env::sim(0x5701 + self.boots, BASE_MS, self.origin, 0));
        let mut cfg = node_config("n0", "test://n0");
        cfg.engine.local_tier = self.local_tier.clone();
        (self.configure)(&mut cfg);
        let services = Services { transport: Arc::new(NoPeers), oidc: None, mailer: None };
        let node = Node::start(cfg, env, self.store.clone(), services).await.map_err(|e| e.to_string())?;
        let _ = baserunna::api::router(node.clone());
        self.node = Some(node);
        Ok(())
    }

    /// Stops the node and starts a new process on the same store, with empty memory caches.
    async fn restart(&mut self) -> Result<(), String> {
        if let Some(n) = self.node.take() {
            n.stop();
        }
        self.boot().await
    }

    fn node(&self) -> &Arc<Node> {
        self.node.as_ref().expect("node is running")
    }

    async fn call(&self, method: &str, path: &str, token: &str, headers: &[(&str, String)], body: Bytes) -> Reply {
        let Some(router) = self.node.as_ref().and_then(|n| n.router()) else {
            return Reply { status: 599, headers: http::HeaderMap::new(), body: Err("node is not running".into()) };
        };
        let mut b = Request::builder().method(method).uri(path);
        if !headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("content-type")) {
            b = b.header("content-type", "application/json");
        }
        if !token.is_empty() {
            b = b.header("authorization", format!("Bearer {token}"));
        }
        for (k, v) in headers {
            b = b.header(*k, v);
        }
        let req = b.header("content-length", body.len().to_string()).body(Body::from(body)).expect("request");
        let resp = match router.oneshot(req).await {
            Ok(r) => r,
            Err(e) => match e {},
        };
        let (parts, body) = resp.into_parts();
        let body = http_body_util::BodyExt::collect(body).await.map(|b| b.to_bytes()).map_err(|e| e.to_string());
        Reply { status: parts.status.as_u16(), headers: parts.headers, body }
    }

    async fn post(&self, path: &str, token: &str, body: Json) -> Reply {
        self.call("POST", path, token, &[], Bytes::from(body.to_string())).await
    }

    async fn txn(&self, token: &str, db: &str, body: Json) -> Reply {
        self.post(&format!("/v1/dbs/{db}/txn"), token, body).await
    }

    async fn signup(&self, email: &str) -> String {
        let r = self.post("/v1/auth/signup", "", json!({"email": email, "password": "password-123"})).await;
        r.json()["token"].as_str().unwrap_or_else(|| panic!("signup: {}", r.excerpt())).to_string()
    }

    async fn create_db(&self, token: &str, name: &str) -> String {
        let r = self.post("/v1/dbs", token, json!({"name": name})).await;
        r.json()["id"].as_str().unwrap_or_else(|| panic!("create db: {}", r.excerpt())).to_string()
    }

    fn object(&self, key: &str) -> Bytes {
        self.mem.get_sync(key, &GetOpts::default()).unwrap_or_else(|e| panic!("{key}: {e}")).data
    }

    fn objects(&self, prefix: &str) -> Vec<(String, u64)> {
        self.mem.list_sync(prefix, None, usize::MAX).objects.into_iter().map(|o| (o.key, o.size)).collect()
    }

    fn manifest(&self, db_id: &str) -> Manifest {
        let m = Manifest::decode(&self.object(&format!("dbs/{db_id}/manifest"))).expect("manifest decodes");
        match m.levels.clone() {
            Some(id) => {
                let l = Levels::decode(&self.object(&format!("dbs/{db_id}/seg/{id}.levels"))).expect("levels");
                let mut m = m;
                m.attach_levels(&l).expect("attach levels");
                m
            }
            None => m,
        }
    }

    fn stored_bytes(&self, db_id: &str) -> u64 {
        self.objects(&format!("dbs/{db_id}/")).iter().map(|(_, s)| s).sum()
    }
}

// ---------------------------------------------------------------------------------------------
// Matrix cells
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    /// The read failed with an error.
    Detected,
    /// The read returned the written data, for example through a fallback.
    Correct,
    /// The read succeeded with data that differs from the written data.
    SilentWrongData,
    /// A write committed the damaged content into the database.
    CorruptionCommitted,
    /// The read did not fetch the damaged bytes, so the cell tested nothing.
    NotRead,
}

impl Verdict {
    fn name(self) -> &'static str {
        match self {
            Verdict::Detected => "DETECTED",
            Verdict::Correct => "DETECTED (correct data)",
            Verdict::SilentWrongData => "SILENT WRONG DATA",
            Verdict::CorruptionCommitted => "CORRUPTION COMMITTED",
            Verdict::NotRead => "NOT READ (damaged bytes not fetched)",
        }
    }

    fn is_safe(self) -> bool {
        matches!(self, Verdict::Detected | Verdict::Correct | Verdict::NotRead)
    }
}

struct Cell {
    object: String,
    fault: String,
    read: String,
    verdict: Verdict,
    detail: String,
}

/// Collected cells of a matrix row group.
#[derive(Default)]
struct Matrix {
    cells: Vec<Cell>,
}

impl Matrix {
    fn add(&mut self, object: &str, fault: &str, read: &str, verdict: Verdict, detail: String) {
        self.cells.push(Cell { object: object.into(), fault: fault.into(), read: read.into(), verdict, detail });
    }

    fn table(&self) -> String {
        let mut s = String::from("| object | fault | read | verdict | detail |\n|---|---|---|---|---|\n");
        for c in &self.cells {
            let _ = writeln!(s, "| {} | {} | {} | {} | {} |", c.object, c.fault, c.read, c.verdict.name(), c.detail);
        }
        s
    }

    /// Asserts that every cell detected the fault or returned the written data.
    fn assert_safe(&self) {
        let unsafe_cells = self.cells.iter().filter(|c| !c.verdict.is_safe()).count();
        assert_eq!(unsafe_cells, 0, "{unsafe_cells} cells are not safe:\n{}", self.table());
    }
}

/// Highest-severity log records emitted while `capture` was active, as `level: message`.
fn logged(capture: &log::Capture) -> String {
    let records = capture.records();
    let worst = [Level::Error, Level::Warn, Level::Info, Level::Debug]
        .into_iter()
        .find_map(|l| records.iter().find(|r| r["level"] == l.name()).map(|r| (l, r)));
    match worst {
        Some((l, r)) => {
            let msg = r["msg"].as_str().unwrap_or_default();
            format!("log {}: {}", l.name(), &msg[..msg.len().min(90)])
        }
        None => "no log".into(),
    }
}

/// Restarts the node with `fault` applied to `key`, runs `read`, and classifies the outcome with `check`.
async fn probe(
    d: &mut Deployment,
    key: &str,
    fault: Fault,
    read: impl AsyncFnOnce(&Deployment) -> Reply,
    check: impl FnOnce(&Reply) -> Result<(), String>,
) -> (Verdict, String) {
    d.store.clear_faults();
    d.store.set_fault(key, fault);
    let restarted = d.restart().await;
    let gets_before = d.store.damaged_gets_of(key);
    let capture = log::capture(Level::Debug);
    let outcome = match restarted {
        Err(e) => (Verdict::Detected, format!("node start failed: {}", &e[..e.len().min(120)])),
        Ok(()) => {
            let r = read(d).await;
            let read_key = d.store.damaged_gets_of(key) > gets_before;
            match (r.failed(), check(&r)) {
                (true, _) => (Verdict::Detected, format!("{} {}", r.status, r.excerpt())),
                (false, Ok(())) if !read_key => (Verdict::NotRead, String::new()),
                (false, Ok(())) => (Verdict::Correct, String::new()),
                (false, Err(e)) => (Verdict::SilentWrongData, format!("{} {e}", r.status)),
            }
        }
    };
    let logs = logged(&capture);
    drop(capture);
    d.store.clear_faults();
    (outcome.0, format!("{}; {logs}", outcome.1.replace('|', "/").replace('\n', " ")))
}

// ---------------------------------------------------------------------------------------------
// Data helpers
// ---------------------------------------------------------------------------------------------

fn random_bytes(rng: &mut Rng, n: usize) -> Vec<u8> {
    (0..n).map(|_| rng.below(256) as u8).collect()
}

/// Writes `entries` with a transaction per 50 entries.
async fn put_all(d: &Deployment, token: &str, db: &str, entries: &[(String, Vec<u8>)]) {
    for batch in entries.chunks(50) {
        let ops: Vec<Json> = batch
            .iter()
            .map(|(k, v)| json!({"op": "put", "key": b64::encode(k.as_bytes()), "value": b64::encode(v)}))
            .collect();
        let r = d.txn(token, db, json!({"encoding": "base64", "ops": ops})).await;
        assert_eq!(r.status, 200, "put: {}", r.excerpt());
    }
}

async fn get_key(d: &Deployment, token: &str, db: &str, key: &str) -> Reply {
    d.txn(token, db, json!({"encoding": "base64", "ops": [{"op": "get", "key": b64::encode(key.as_bytes())}]})).await
}

fn check_value(r: &Reply, want: &[u8]) -> Result<(), String> {
    let got = &r.json()["results"][0];
    if got["found"] != true {
        return Err("key not found".into());
    }
    match got["value"].as_str().and_then(b64::decode) {
        Some(v) if v == want => Ok(()),
        Some(v) => Err(format!("value differs ({} bytes, want {})", v.len(), want.len())),
        None => Err(format!("no value: {got}")),
    }
}

async fn scan_prefix(d: &Deployment, token: &str, db: &str, prefix: &str) -> Reply {
    let op = json!({"op": "scan", "prefix": b64::encode(prefix.as_bytes()), "limit": 1000});
    d.txn(token, db, json!({"encoding": "base64", "ops": [op]})).await
}

fn check_scan(r: &Reply, want: &[(String, Vec<u8>)]) -> Result<(), String> {
    let items = r.json()["results"][0]["items"].as_array().cloned().unwrap_or_default();
    let got: Vec<(Vec<u8>, Vec<u8>)> = items
        .iter()
        .map(|i| {
            let k = i["key"].as_str().and_then(b64::decode).unwrap_or_default();
            let v = i["value"].as_str().and_then(b64::decode).unwrap_or_default();
            (k, v)
        })
        .collect();
    if got.len() != want.len() {
        return Err(format!("scan returned {} items, want {}", got.len(), want.len()));
    }
    for ((gk, gv), (wk, wv)) in got.iter().zip(want) {
        if gk != wk.as_bytes() || gv != wv {
            return Err(format!("item {} differs", String::from_utf8_lossy(gk)));
        }
    }
    Ok(())
}

/// Segment metadata of `db_id` from the stored manifest, as `(id, level, size, entries, first key)`.
fn segments(d: &Deployment, db_id: &str) -> Vec<(String, u8, u64, u64, Vec<u8>)> {
    d.manifest(db_id).segments.iter().map(|s| (s.id.clone(), s.level, s.size, s.entries, s.min_key.to_vec())).collect()
}

/// Byte offsets inside a stored segment that fall in each of its sections.
struct SegmentLayout {
    first_block: Range<u64>,
    middle_block: Range<u64>,
    bloom: Range<u64>,
    index: Range<u64>,
    footer: Range<u64>,
}

fn segment_layout(data: &Bytes) -> SegmentLayout {
    let idx = SegmentIndex::decode_whole(data).expect("segment decodes");
    let block = |i: usize| {
        let (_, off, len) = idx.blocks[i];
        off..off + len as u64
    };
    let f = &idx.footer;
    SegmentLayout {
        first_block: block(0),
        middle_block: block(idx.blocks.len() / 2),
        bloom: f.bloom_off..f.bloom_off + f.bloom_len as u64,
        index: f.index_off..f.index_off + f.index_len as u64,
        footer: data.len() as u64 - FOOTER_LEN as u64..data.len() as u64,
    }
}

fn mid(r: &Range<u64>) -> usize {
    ((r.start + r.end) / 2) as usize
}

// ---------------------------------------------------------------------------------------------
// Manifests
// ---------------------------------------------------------------------------------------------

/// Fault kinds (a) and (b) for an object of `len` bytes: bit flips at the first, middle, and last
/// byte, and truncations to all but the last byte and to half.
fn flips_and_truncations(len: usize) -> Vec<(String, Fault)> {
    vec![
        ("(a) flip byte 0".into(), Fault::FlipBit(0)),
        (format!("(a) flip byte {}", len / 2), Fault::FlipBit(len / 2)),
        (format!("(a) flip last byte {}", len - 1), Fault::FlipBit(len - 1)),
        (format!("(b) truncate to {}", len - 1), Fault::Truncate(len - 1)),
        (format!("(b) truncate to {}", len / 2), Fault::Truncate(len / 2)),
    ]
}

#[test]
fn damaged_manifests_are_detected() {
    run_paused(async {
        let mut d = Deployment::start(|_| {}, None).await;
        let token = d.signup("admin@integrity.test").await;
        let mut rng = Rng::new(1);
        let entries: Vec<(String, Vec<u8>)> = (0..3).map(|i| (format!("k{i}"), random_bytes(&mut rng, 100))).collect();
        let db = d.create_db(&token, "d").await;
        let other = d.create_db(&token, "e").await;
        put_all(&d, &token, "d", &entries).await;
        let other_entries: Vec<(String, Vec<u8>)> =
            (0..3).map(|i| (format!("k{i}"), random_bytes(&mut rng, 100))).collect();
        put_all(&d, &token, "e", &other_entries).await;
        let mut m = Matrix::default();
        let key = format!("dbs/{db}/manifest");
        let data = d.object(&key);
        let other_manifest = d.object(&format!("dbs/{other}/manifest"));
        let mut faults = flips_and_truncations(data.len());
        faults.push((
            format!("(c) other database's manifest ({} vs {} bytes)", other_manifest.len(), data.len()),
            Fault::Replace(other_manifest),
        ));
        for (name, fault) in faults {
            let (v, detail) = probe(
                &mut d,
                &key,
                fault,
                async |d| get_key(d, &token, "d", "k1").await,
                |r| check_value(r, &entries[1].1),
            )
            .await;
            m.add("database manifest", &name, "KV get", v, detail);
        }

        let key = "dbs/sys/manifest";
        let data = d.object(key);
        let user_manifest = d.object(&format!("dbs/{db}/manifest"));
        let mut faults = flips_and_truncations(data.len());
        faults.push(("(c) user database manifest".into(), Fault::Replace(user_manifest)));
        for (name, fault) in faults {
            let (v, detail) = probe(
                &mut d,
                key,
                fault,
                async |d| get_key(d, &token, "d", "k2").await,
                |r| check_value(r, &entries[2].1),
            )
            .await;
            m.add("system database manifest", &name, "node start + KV get", v, detail);
        }
        eprintln!("{}", m.table());
        m.assert_safe();
        d.restart().await.expect("node restarts without faults");
        check_scan(&scan_prefix(&d, &token, "d", "k").await, &entries).expect("data is intact after the faults");
    });
}

// ---------------------------------------------------------------------------------------------
// LSM segments and tables
// ---------------------------------------------------------------------------------------------

type Entries = Vec<(String, Vec<u8>)>;

/// Key-value data in two databases. Database `d` holds 3 level-0 segments of 300 entries with
/// prefixes `a`, `b`, and `c`, which have equal byte sizes. Database `big` holds 3,000 entries,
/// mostly in segments larger than `segment_whole_fetch_max` (1 MiB), which readers fetch with
/// ranged `GET`s.
struct KvData {
    token: String,
    db: String,
    big: String,
    groups: BTreeMap<&'static str, Entries>,
    big_entries: Entries,
}

impl KvData {
    async fn write(d: &Deployment) -> KvData {
        let token = d.signup("admin@integrity.test").await;
        let mut rng = Rng::new(2);
        let db = d.create_db(&token, "d").await;
        let mut groups = BTreeMap::new();
        for group in ["a", "b", "c"] {
            let entries: Entries = (0..300).map(|i| (format!("{group}{i:04}"), random_bytes(&mut rng, 1000))).collect();
            put_all(d, &token, "d", &entries).await;
            tokio::time::sleep(Duration::from_secs(5)).await;
            groups.insert(group, entries);
        }
        let big = d.create_db(&token, "big").await;
        let mut big_entries = Vec::new();
        for start in (0..3000).step_by(300) {
            let entries: Entries =
                (start..start + 300).map(|i| (format!("k{i:05}"), random_bytes(&mut rng, 1000))).collect();
            put_all(d, &token, "big", &entries).await;
            tokio::time::sleep(Duration::from_secs(5)).await;
            big_entries.extend(entries);
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
        KvData { token, db, big, groups, big_entries }
    }

    /// Store key and bytes of the segment of `db_id` whose first user key is `first_key`.
    fn segment(&self, d: &Deployment, db_id: &str, first_key: &str) -> (String, Bytes) {
        let segs = segments(d, db_id);
        let (id, ..) = segs
            .iter()
            .find(|s| s.4.get(1..) == Some(first_key.as_bytes()))
            .unwrap_or_else(|| panic!("no segment starts at {first_key}"));
        let key = format!("dbs/{db_id}/seg/{id}");
        let data = d.object(&key);
        (key, data)
    }
}

/// Fault kinds (a) and (b) for a segment: a bit flip in each section, and truncations.
fn segment_faults(data: &Bytes) -> Vec<(String, Fault)> {
    let l = segment_layout(data);
    let len = data.len();
    vec![
        ("(a) flip first data block".into(), Fault::FlipBit(mid(&l.first_block))),
        ("(a) flip middle data block".into(), Fault::FlipBit(mid(&l.middle_block))),
        ("(a) flip bloom filter".into(), Fault::FlipBit(mid(&l.bloom))),
        ("(a) flip block index".into(), Fault::FlipBit(mid(&l.index))),
        ("(a) flip footer".into(), Fault::FlipBit(mid(&l.footer))),
        ("(a) flip last byte".into(), Fault::FlipBit(len - 1)),
        (format!("(b) truncate to {}", len - 1), Fault::Truncate(len - 1)),
        (format!("(b) truncate to {}", len / 2), Fault::Truncate(len / 2)),
    ]
}

/// User key in the middle data block of a segment.
fn key_in_middle_block(data: &Bytes) -> String {
    let idx = SegmentIndex::decode_whole(data).expect("segment decodes");
    let (_, off, len) = idx.blocks[idx.blocks.len() / 2];
    let block = segment::decode_block(data.slice(off as usize..(off + len as u64) as usize)).expect("block");
    String::from_utf8(block[block.len() / 2].key[1..].to_vec()).expect("utf-8 key")
}

fn value_of<'a>(entries: &'a Entries, key: &str) -> &'a [u8] {
    &entries.iter().find(|(k, _)| k == key).unwrap_or_else(|| panic!("no entry {key}")).1
}

/// Runs a KV get of `key` and a scan of `prefix` against each fault applied to `object_key`.
async fn kv_cells(
    m: &mut Matrix,
    d: &mut Deployment,
    kv: &KvData,
    (object, object_key): (&str, &str),
    (db, key, prefix): (&str, &str, &str),
    entries: &Entries,
    faults: Vec<(String, Fault)>,
) {
    let want: Entries = entries.iter().filter(|(k, _)| k.starts_with(prefix)).cloned().collect();
    let value = value_of(entries, key).to_vec();
    for (name, fault) in faults {
        let (v, detail) = probe(
            d,
            object_key,
            fault.clone(),
            async |d| get_key(d, &kv.token, db, key).await,
            |r| check_value(r, &value),
        )
        .await;
        m.add(object, &name, "KV get", v, detail);
        let (v, detail) = probe(
            d,
            object_key,
            fault,
            async |d| scan_prefix(d, &kv.token, db, prefix).await,
            |r| check_scan(r, &want),
        )
        .await;
        m.add(object, &name, &format!("KV scan of {} keys", want.len()), v, detail);
    }
}

#[test]
fn damaged_segments_are_detected() {
    run_paused(async {
        let mut d = Deployment::start(|_| {}, None).await;
        let kv = KvData::write(&d).await;
        let mut m = Matrix::default();

        let (key, data) = kv.segment(&d, &kv.db, "b0000");
        let (_, other) = kv.segment(&d, &kv.big, "k02700");
        let mut faults = segment_faults(&data);
        faults.push((
            format!("(c) other level-0 segment ({} vs {} bytes)", other.len(), data.len()),
            Fault::Replace(other),
        ));
        let target = ("small segment (whole GET)", key.as_str());
        kv_cells(&mut m, &mut d, &kv, target, ("d", "b0150", "b0"), &kv.groups["b"], faults).await;

        let (key, data) = kv.segment(&d, &kv.big, "k00000");
        let (_, other) = kv.segment(&d, &kv.big, "k02700");
        let mut faults = segment_faults(&data);
        faults.push((format!("(c) level-0 segment ({} vs {} bytes)", other.len(), data.len()), Fault::Replace(other)));
        let target = ("large segment (ranged GETs)", key.as_str());
        let probe_key = key_in_middle_block(&data);
        kv_cells(&mut m, &mut d, &kv, target, ("big", &probe_key, "k00"), &kv.big_entries, faults).await;

        eprintln!("{}", m.table());
        m.assert_safe();
    });
}

#[test]
#[ignore = "TODO: Bind every stored object to its key"]
fn substituted_segment_of_equal_shape_is_detected() {
    run_paused(async {
        let mut d = Deployment::start(|_| {}, None).await;
        let kv = KvData::write(&d).await;
        let mut m = Matrix::default();
        let (key, data) = kv.segment(&d, &kv.db, "b0000");
        let (_, other) = kv.segment(&d, &kv.db, "c0000");
        assert_eq!(data.len(), other.len(), "the substitute segment must have the same size");
        let fault =
            vec![(format!("(c) segment c of equal size {} and 300 entries", other.len()), Fault::Replace(other))];
        let target = ("small segment (whole GET)", key.as_str());
        kv_cells(&mut m, &mut d, &kv, target, ("d", "b0150", "b0"), &kv.groups["b"], fault).await;

        let (key, data) = kv.segment(&d, &kv.big, "k00000");
        let (_, other) = kv.segment(&d, &kv.big, "k01200");
        assert_eq!(data.len(), other.len(), "the substitute segment must have the same size");
        let fault = vec![(
            format!("(c) segment k01200 of equal size {} and 1,200 entries", other.len()),
            Fault::Replace(other),
        )];
        let target = ("large segment (ranged GETs)", key.as_str());
        let probe_key = key_in_middle_block(&data);
        kv_cells(&mut m, &mut d, &kv, target, ("big", &probe_key, "k00"), &kv.big_entries, fault).await;

        // A fourth level-0 data segment starts a compaction that reads segment b from the store.
        let (key, _) = kv.segment(&d, &kv.db, "b0000");
        let (_, other) = kv.segment(&d, &kv.db, "c0000");
        d.store.set_fault(&key, Fault::Replace(other));
        d.restart().await.expect("node restarts");
        tokio::time::sleep(Duration::from_secs(10)).await;
        let mut rng = Rng::new(9);
        let more: Entries = (0..300).map(|i| (format!("d{i:04}"), random_bytes(&mut rng, 1000))).collect();
        put_all(&d, &kv.token, "d", &more).await;
        tokio::time::sleep(Duration::from_secs(60)).await;
        let compacted = !d.manifest(&kv.db).segments.iter().any(|s| key.ends_with(&s.id));
        d.store.clear_faults();
        d.restart().await.expect("node restarts");
        let r = scan_prefix(&d, &kv.token, "d", "b0").await;
        let (v, detail) = match (compacted, check_scan(&r, &kv.groups["b"])) {
            (_, Ok(())) => (Verdict::Correct, String::new()),
            (true, Err(e)) => (Verdict::CorruptionCommitted, format!("compaction replaced segment b; {e}")),
            (false, Err(e)) => (Verdict::SilentWrongData, e),
        };
        m.add(
            "small segment (whole GET)",
            "(c) segment c of equal size, then compaction",
            "KV scan after fault cleared",
            v,
            detail,
        );
        eprintln!("{}", m.table());
        m.assert_safe();
    });
}

#[test]
#[ignore = "TODO: Check ranged block reads against the block index"]
fn segment_block_served_from_wrong_offset_is_detected() {
    run_paused(async {
        let mut d = Deployment::start(|_| {}, None).await;
        let kv = KvData::write(&d).await;
        let mut m = Matrix::default();
        let (key, data) = kv.segment(&d, &kv.big, "k00000");
        let idx = SegmentIndex::decode_whole(&data).expect("segment decodes");
        let bi = idx.blocks.len() / 2;
        let ((_, off, len), (_, _, next_len)) = (idx.blocks[bi].clone(), idx.blocks[bi + 1].clone());
        assert_eq!(len, next_len, "blocks {bi} and {} must have equal length", bi + 1);
        let fault = vec![(
            format!("(d) range of block {bi} served from block {} ({len} bytes each)", bi + 1),
            Fault::ShiftRange { from: off, by: len as u64 },
        )];
        let target = ("large segment (ranged GETs)", key.as_str());
        let probe_key = key_in_middle_block(&data);
        kv_cells(&mut m, &mut d, &kv, target, ("big", &probe_key, "k00"), &kv.big_entries, fault).await;
        eprintln!("{}", m.table());
        m.assert_safe();
    });
}

/// Rows of table `items` in database `t`, stored in a level-0 segment.
async fn write_table(d: &Deployment, token: &str) -> (String, Vec<Json>) {
    let db = d.create_db(token, "t").await;
    let table = json!({"name": "items", "primary_key": ["id"], "columns": [
        {"name": "id", "type": "int", "nullable": false}, {"name": "label", "type": "string"}]});
    let r = d.post("/v1/dbs/t/tables", token, table).await;
    assert_eq!(r.status, 201, "create table: {}", r.excerpt());
    let mut rng = Rng::new(3);
    let rows: Vec<Json> = (0..300)
        .map(|i| {
            let label: String = (0..1000).map(|_| (b'a' + rng.below(26) as u8) as char).collect();
            json!({"id": i, "label": label})
        })
        .collect();
    for batch in rows.chunks(50) {
        let ops: Vec<Json> =
            batch.iter().map(|row| json!({"op": "insert_row", "table": "items", "row": row})).collect();
        let r = d.txn(token, "t", json!({"ops": ops})).await;
        assert_eq!(r.status, 200, "insert rows: {}", r.excerpt());
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    (db, rows)
}

#[test]
fn damaged_table_segment_is_detected() {
    run_paused(async {
        let mut d = Deployment::start(|_| {}, None).await;
        let token = d.signup("admin@integrity.test").await;
        let (db, rows) = write_table(&d, &token).await;
        let (seg, ..) = segments(&d, &db).into_iter().max_by_key(|s| s.2).expect("a segment");
        let key = format!("dbs/{db}/seg/{seg}");
        let data = d.object(&key);
        let mut m = Matrix::default();
        for (name, fault) in segment_faults(&data) {
            let (v, detail) = probe(
                &mut d,
                &key,
                fault,
                async |d| {
                    d.txn(&token, "t", json!({"ops": [{"op": "query_rows", "table": "items", "limit": 1000}]})).await
                },
                |r| match r.json()["results"][0]["rows"].as_array() {
                    Some(got) if *got == rows => Ok(()),
                    Some(got) => Err(format!("{} rows differ from the {} written", got.len(), rows.len())),
                    None => Err("no rows".into()),
                },
            )
            .await;
            m.add("table rows (segment)", &name, "query_rows", v, detail);
        }
        eprintln!("{}", m.table());
        m.assert_safe();
    });
}

// ---------------------------------------------------------------------------------------------
// Stream chunks
// ---------------------------------------------------------------------------------------------

/// Stream `log` in database `s` with 3 sealed chunks of 300 records of 1,000 bytes. Chunks 2 and 3
/// have equal byte sizes and record counts.
struct StreamData {
    token: String,
    /// `(seq, payload)` of every record, in order.
    records: Vec<(u64, Vec<u8>)>,
    /// Store key, bytes, first seq, and last seq of each sealed chunk, in stream order.
    chunks: Vec<(String, Bytes, u64, u64)>,
}

async fn read_stream(d: &Deployment, token: &str, from_seq: u64, limit: usize) -> Reply {
    let op = json!({"op": "read_stream", "stream": "log", "from_seq": from_seq, "limit": limit});
    d.txn(token, "s", json!({"encoding": "base64", "ops": [op]})).await
}

fn stream_records(r: &Reply) -> Vec<(u64, Vec<u8>)> {
    let records = r.json()["results"][0]["records"].as_array().cloned().unwrap_or_default();
    records
        .iter()
        .map(|rec| (rec["seq"].as_u64().unwrap_or(0), rec["value"].as_str().and_then(b64::decode).unwrap_or_default()))
        .collect()
}

fn check_records(r: &Reply, want: &[(u64, Vec<u8>)]) -> Result<(), String> {
    let got = stream_records(r);
    if got == want {
        return Ok(());
    }
    let seqs = |v: &[(u64, Vec<u8>)]| match (v.first(), v.last()) {
        (Some(a), Some(b)) => format!("seqs {}..={} ({} records)", a.0, b.0, v.len()),
        _ => "no records".into(),
    };
    Err(format!("{} instead of {}", seqs(&got), seqs(want)))
}

impl StreamData {
    async fn write(d: &Deployment) -> StreamData {
        let token = d.signup("admin@integrity.test").await;
        let db = d.create_db(&token, "s").await;
        let mut rng = Rng::new(4);
        for _ in 0..3 {
            for _ in 0..3 {
                let records: Vec<Json> =
                    (0..100).map(|_| json!({"value": b64::encode(&random_bytes(&mut rng, 1000))})).collect();
                let op = json!({"op": "append", "stream": "log", "records": records, "create": true});
                let r = d.txn(&token, "s", json!({"encoding": "base64", "ops": [op]})).await;
                assert_eq!(r.status, 200, "append: {}", r.excerpt());
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        let mut records = Vec::new();
        while records.len() < 900 {
            let from = records.last().map_or(0, |(s, _): &(u64, Vec<u8>)| s + 1);
            let page = stream_records(&read_stream(d, &token, from, 1000).await);
            assert!(!page.is_empty(), "stream read returned no records after {}", records.len());
            records.extend(page);
        }
        let mut chunks = Vec::new();
        for (key, _) in d.objects(&format!("dbs/{db}/seg/")) {
            let data = d.object(&key);
            let Ok(entries) = segment::read_all(&data) else { continue };
            let seqs: Vec<u64> = entries
                .iter()
                .filter_map(|e| baserunna::engine::stream::parse_record_key(&e.key))
                .map(|(_, s)| s)
                .collect();
            if seqs.len() == entries.len() && !seqs.is_empty() {
                chunks.push((key, data, seqs[0], *seqs.last().expect("seq")));
            }
        }
        chunks.sort_by_key(|c| c.2);
        assert_eq!(
            chunks.len(),
            3,
            "expected 3 sealed chunks: {:?}",
            chunks.iter().map(|c| (c.1.len(), c.2, c.3)).collect::<Vec<_>>()
        );
        StreamData { token, records, chunks }
    }

    fn window(&self, first: u64, last: u64) -> Vec<(u64, Vec<u8>)> {
        self.records.iter().filter(|(s, _)| (first..=last).contains(s)).cloned().collect()
    }
}

/// Reads the records of chunk 2 with each fault applied to chunk 2.
async fn stream_cells(m: &mut Matrix, d: &mut Deployment, s: &StreamData, faults: Vec<(String, Fault)>) {
    let (key, _, first, last) = s.chunks[1].clone();
    let want = s.window(first, last);
    for (name, fault) in faults {
        let (v, detail) = probe(
            d,
            &key,
            fault,
            async |d| read_stream(d, &s.token, first, want.len()).await,
            |r| check_records(r, &want),
        )
        .await;
        m.add("stream chunk", &name, &format!("read_stream of {} records", want.len()), v, detail);
    }
}

#[test]
fn damaged_stream_chunks_are_detected() {
    run_paused(async {
        let mut d = Deployment::start(|_| {}, None).await;
        let s = StreamData::write(&d).await;
        let mut m = Matrix::default();
        let (data, other) = (s.chunks[1].1.clone(), s.chunks[0].1.clone());
        let mut faults = segment_faults(&data);
        faults.retain(|(name, _)| !name.contains("bloom"));
        faults.push((
            format!("(c) chunk 1 of the same stream ({} vs {} bytes)", other.len(), data.len()),
            Fault::Replace(other),
        ));
        stream_cells(&mut m, &mut d, &s, faults).await;
        eprintln!("{}", m.table());
        m.assert_safe();
    });
}

#[test]
#[ignore = "TODO: Detect sequence gaps during stream reads"]
fn substituted_stream_chunk_of_equal_shape_is_detected() {
    run_paused(async {
        let mut d = Deployment::start(|_| {}, None).await;
        let s = StreamData::write(&d).await;
        let mut m = Matrix::default();
        let (data, other) = (s.chunks[1].1.clone(), s.chunks[2].1.clone());
        assert_eq!(data.len(), other.len(), "chunks 2 and 3 must have equal size");
        let fault = vec![(
            format!("(c) chunk 3 of the same stream, equal size {} and 300 records", other.len()),
            Fault::Replace(other),
        )];
        stream_cells(&mut m, &mut d, &s, fault).await;
        eprintln!("{}", m.table());
        m.assert_safe();
    });
}

// ---------------------------------------------------------------------------------------------
// Blob chunks and staged blob metadata
// ---------------------------------------------------------------------------------------------

/// Blobs in database `b`: `blob1` and `blob3` hold 100,000 bytes, `blob2` holds 120,000 bytes.
struct BlobData {
    token: String,
    db: String,
    values: BTreeMap<&'static str, Vec<u8>>,
    ids: BTreeMap<&'static str, String>,
}

impl BlobData {
    async fn write(d: &Deployment) -> BlobData {
        let token = d.signup("admin@integrity.test").await;
        let db = d.create_db(&token, "b").await;
        let mut rng = Rng::new(5);
        let (mut values, mut ids) = (BTreeMap::new(), BTreeMap::new());
        for (name, len) in [("blob1", 100_000), ("blob2", 120_000), ("blob3", 100_000)] {
            let v = random_bytes(&mut rng, len);
            let r = d.call("PUT", &format!("/v1/dbs/b/kv/{name}"), &token, &[], Bytes::from(v.clone())).await;
            assert_eq!(r.status, 200, "put blob: {}", r.excerpt());
            let got = get_key(d, &token, "b", name).await.json();
            let id = got["results"][0]["blob"]["blob_id"].as_str().unwrap_or_else(|| panic!("not a blob: {got}"));
            ids.insert(name, id.to_string());
            values.insert(name, v);
        }
        BlobData { token, db, values, ids }
    }

    fn chunk_key(&self, name: &str) -> String {
        format!("dbs/{}/blob/{}/00000000", self.db, self.ids[name])
    }
}

fn check_body(r: &Reply, want: &[u8]) -> Result<(), String> {
    match &r.body {
        Ok(b) if b.as_ref() == want => Ok(()),
        Ok(b) => {
            let diff = b.iter().zip(want).position(|(x, y)| x != y).map_or("length".into(), |i| format!("byte {i}"));
            Err(format!("{} bytes differ from the {} written, first at {diff}", b.len(), want.len()))
        }
        Err(e) => Err(e.clone()),
    }
}

/// Reads `blob1` in full, by range, and through the staged-blob route, with each fault applied to its chunk.
async fn blob_cells(m: &mut Matrix, d: &mut Deployment, b: &BlobData, faults: Vec<(String, Fault)>) {
    let key = b.chunk_key("blob1");
    let want = b.values["blob1"].clone();
    let by_id = format!("/v1/dbs/b/blobs/{}", b.ids["blob1"]);
    for (name, fault) in faults {
        let (v, detail) = probe(
            d,
            &key,
            fault.clone(),
            async |d| d.call("GET", "/v1/dbs/b/kv/blob1", &b.token, &[], Bytes::new()).await,
            |r| check_body(r, &want),
        )
        .await;
        m.add("blob chunk", &name, "KV get, full", v, detail);
        let (v, detail) = probe(
            d,
            &key,
            fault.clone(),
            async |d| {
                let range = [("range", "bytes=40000-59999".to_string())];
                d.call("GET", "/v1/dbs/b/kv/blob1", &b.token, &range, Bytes::new()).await
            },
            |r| check_body(r, &want[40_000..60_000]),
        )
        .await;
        m.add("blob chunk", &name, "KV get, bytes 40000-59999", v, detail);
        let (v, detail) = probe(
            d,
            &key,
            fault,
            async |d| d.call("GET", &by_id, &b.token, &[], Bytes::new()).await,
            |r| check_body(r, &want),
        )
        .await;
        m.add("blob chunk", &name, "GET /blobs/{id}, full", v, detail);
    }
}

#[test]
#[ignore = "TODO: Verify blob chunks on read"]
fn damaged_blob_chunks_are_detected() {
    run_paused(async {
        let mut d = Deployment::start(|_| {}, None).await;
        let b = BlobData::write(&d).await;
        let mut m = Matrix::default();
        let mut faults = flips_and_truncations(100_000);
        let (larger, equal) = (d.object(&b.chunk_key("blob2")), d.object(&b.chunk_key("blob3")));
        faults.push(("(c) chunk of a 120,000-byte blob".into(), Fault::Replace(larger)));
        faults.push(("(c) chunk of another 100,000-byte blob".into(), Fault::Replace(equal)));
        faults.push(("(d) range served 1 byte late".into(), Fault::ShiftRange { from: 40_000, by: 1 }));
        blob_cells(&mut m, &mut d, &b, faults).await;
        eprintln!("{}", m.table());
        m.assert_safe();
    });
}

/// Byte offsets of the fields in an encoded `BlobRef` descriptor.
struct BlobMetaLayout {
    id: usize,
    size: usize,
    sha256: usize,
    content_type: usize,
}

fn blob_meta_layout(meta: &[u8]) -> BlobMetaLayout {
    let id_len = meta[0] as usize;
    let size = 1 + id_len;
    let size_len = meta[size..].iter().position(|b| b & 0x80 == 0).expect("size varint") + 1;
    let sha256 = size + size_len;
    BlobMetaLayout { id: 1 + id_len / 2, size, sha256, content_type: sha256 + 32 + 1 }
}

/// The `BlobRef` that the database stores for `key`, read from the engine, as `BlobMeta` JSON.
async fn bound_blob(d: &Deployment, db_id: &str, key: &str) -> Json {
    let v = d.node().engine.db(db_id).read_version(Consistency::Strong).await.expect("read version");
    let k = baserunna::ops::user_key(key.as_bytes()).expect("key");
    match v.get(&k, v.read_time()).await.expect("get").map(|e| e.value) {
        Some(baserunna::engine::Value::Blob(r)) => json!({"blob_id": r.id, "size": r.size,
            "sha256": r.sha256.iter().map(|b| format!("{b:02x}")).collect::<String>(), "content_type": r.content_type}),
        other => json!({"not_a_blob": format!("{other:?}")}),
    }
}

#[test]
#[ignore = "TODO: Add a checksum to blob metadata objects"]
fn damaged_staged_blob_metadata_is_detected() {
    run_paused(async {
        let d = Deployment::start(|_| {}, None).await;
        let token = d.signup("admin@integrity.test").await;
        let db = d.create_db(&token, "m").await;
        let mut rng = Rng::new(6);
        let mut m = Matrix::default();
        let stage = async |rng: &mut Rng| {
            let data = random_bytes(rng, 100_000);
            let headers = [("content-type", "application/x-test".to_string())];
            let r = d.call("POST", "/v1/dbs/m/blobs", &token, &headers, Bytes::from(data.clone())).await;
            assert_eq!(r.status, 201, "stage blob: {}", r.excerpt());
            (r.json(), data)
        };
        let (other, _) = stage(&mut rng).await;
        let other_meta = d.object(&format!("dbs/{db}/blob/{}/meta", other["blob_id"].as_str().expect("id")));
        type MetaFault = Box<dyn Fn(&BlobMetaLayout, usize) -> Fault>;
        let faults: Vec<(&str, MetaFault)> = vec![
            ("(a) flip blob id", Box::new(|l, _| Fault::FlipBit(l.id))),
            ("(a) flip size bit 0 (larger)", Box::new(|l, _| Fault::FlipBit(l.size))),
            ("(a) flip size bit 5 (smaller)", Box::new(|l, _| Fault::FlipBits { at: l.size, mask: 0x20 })),
            ("(a) flip sha256", Box::new(|l, _| Fault::FlipBit(l.sha256 + 10))),
            ("(a) flip content type", Box::new(|l, _| Fault::FlipBit(l.content_type + 3))),
            ("(b) truncate by 1 byte", Box::new(|_, len| Fault::Truncate(len - 1))),
            ("(b) truncate to half", Box::new(|_, len| Fault::Truncate(len / 2))),
            ("(c) other staged blob's metadata", Box::new(move |_, _| Fault::Replace(other_meta.clone()))),
        ];
        for (i, (name, fault)) in faults.into_iter().enumerate() {
            let (staged, data) = stage(&mut rng).await;
            let id = staged["blob_id"].as_str().expect("blob id").to_string();
            let meta_key = format!("dbs/{db}/blob/{id}/meta");
            let meta = d.object(&meta_key);
            let fault = fault(&blob_meta_layout(&meta), meta.len());

            d.store.set_fault(&meta_key, fault.clone());
            let capture = log::capture(Level::Debug);
            let r = d.call("GET", &format!("/v1/dbs/m/blobs/{id}"), &token, &[], Bytes::new()).await;
            let served_type = r.headers.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
            let served = check_body(&r, &data).and_then(|()| match served_type == "application/x-test" {
                true => Ok(()),
                false => Err(format!("content type {served_type:?}")),
            });
            let (v, detail) = match (r.failed(), served) {
                (true, _) => (Verdict::Detected, format!("{} {}", r.status, r.excerpt())),
                (false, Ok(())) => (Verdict::Correct, String::new()),
                (false, Err(e)) => (Verdict::SilentWrongData, format!("{} {e}", r.status)),
            };
            m.add("staged blob metadata", name, "GET /blobs/{id}", v, format!("{detail}; {}", logged(&capture)));

            let key = format!("bound{i}");
            let bind = json!({"ops": [{"op": "put", "key": key, "blob_id": id}]});
            let r = d.txn(&token, "m", bind).await;
            let logs = logged(&capture);
            drop(capture);
            d.store.clear_faults();
            let (v, detail) = if r.failed() {
                (Verdict::Detected, format!("{} {}", r.status, r.excerpt()))
            } else {
                let got = bound_blob(&d, &db, &key).await;
                let read = d.call("GET", &format!("/v1/dbs/m/kv/{key}"), &token, &[], Bytes::new()).await;
                let fields = ["blob_id", "size", "sha256", "content_type"];
                let changed: Vec<String> = fields
                    .iter()
                    .filter(|f| got[**f] != staged[**f])
                    .map(|f| format!("{f} {} -> {}", staged[*f], got[*f]))
                    .collect();
                match (changed.is_empty(), check_body(&read, &data)) {
                    (true, Ok(())) => (Verdict::Correct, String::new()),
                    (_, body) => (
                        Verdict::CorruptionCommitted,
                        format!(
                            "bound {}; later KV get: {}",
                            changed.join(", "),
                            body.err().unwrap_or("intact".into())
                        ),
                    ),
                }
            };
            m.add("staged blob metadata", name, "commit put with blob_id", v, format!("{detail}; {logs}"));
        }
        eprintln!("{}", m.table());
        m.assert_safe();
    });
}

// ---------------------------------------------------------------------------------------------
// Event parts
// ---------------------------------------------------------------------------------------------

const DAY_MS: i64 = 86_400_000;
const EVENTS_FROM_MS: i64 = 1_740_000_000_000 / DAY_MS * DAY_MS;

/// Event table `ev` in database `ev` with a part per day. Days 0 and 1 hold identical rows except
/// for the time; day 2 holds more rows; day 3 holds 100,000 rows with random tags in a part larger
/// than `segment_whole_fetch_max`, which readers fetch with ranged `GET`s.
struct EventData {
    token: String,
    db: String,
    /// Part ID of each day.
    parts: BTreeMap<i64, String>,
}

fn day_query(day: i64) -> Json {
    let from = EVENTS_FROM_MS + day * DAY_MS;
    json!({"from": from, "to": from + DAY_MS, "group_by": ["user"], "order_by": [{"col": "user"}], "limit": 1000,
        "aggregates": {"n": {"fn": "count"}, "s": {"fn": "sum", "of": "v"}, "t": {"fn": "uniq_exact", "of": "tag"}}})
}

async fn query_day(d: &Deployment, token: &str, day: i64) -> Reply {
    d.post("/v1/dbs/ev/events/ev/rows/query", token, day_query(day)).await
}

impl EventData {
    async fn write(d: &Deployment) -> EventData {
        let token = d.signup("admin@integrity.test").await;
        let db = d.create_db(&token, "ev").await;
        let table = json!({"name": "ev", "columns": [{"name": "time", "type": "time"}, {"name": "user", "type": "string"},
            {"name": "v", "type": "int"}, {"name": "tag", "type": "string"}], "time_column": "time", "order_by": ["time"]});
        let r = d.post("/v1/dbs/ev/events", &token, table).await;
        assert_eq!(r.status, 201, "create event table: {}", r.excerpt());
        let mut rng = Rng::new(7);
        for (day, rows) in [(0i64, 1000usize), (1, 1000), (2, 1200), (3, 100_000)] {
            let start = EVENTS_FROM_MS + day * DAY_MS;
            let body: Vec<Json> = (0..rows)
                .map(|i| {
                    let tag: String = match day {
                        3 => (0..24).map(|_| (b'a' + rng.below(26) as u8) as char).collect(),
                        _ => format!("t{}", i % 7),
                    };
                    json!({"time": start + i as i64 * 800, "user": format!("u{:03}", i % 50), "v": i, "tag": tag})
                })
                .collect();
            let r = d.post("/v1/dbs/ev/events/ev/rows", &token, Json::Array(body)).await;
            assert_eq!(r.status, 201, "bulk insert: {}", r.excerpt());
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
        let mut parts = BTreeMap::new();
        for r in part_refs(d, &db).await {
            let day = (r.min_time - EVENTS_FROM_MS).div_euclid(DAY_MS);
            assert!(parts.insert(day, r.id.clone()).is_none(), "day {day} has more than 1 part");
        }
        assert_eq!(parts.len(), 4, "expected a single part per day: {parts:?}");
        EventData { token, db, parts }
    }

    fn part_key(&self, day: i64) -> String {
        format!("dbs/{}/part/{}/0000", self.db, self.parts[&day])
    }
}

async fn part_refs(d: &Deployment, db_id: &str) -> Vec<baserunna::engine::events::PartRef> {
    let v = d.node().engine.db(db_id).read_version(Consistency::Strong).await.expect("read version");
    let range = baserunna::engine::KeyRange::prefix(&[baserunna::engine::events::PARTS]);
    let entries = v.scan(&range, false, usize::MAX, v.read_time()).await.expect("scan part references");
    entries.iter().filter_map(baserunna::engine::events::ref_of).collect()
}

/// Bit flips in the first byte, the column data, index, and metadata of column `v`, and the last
/// byte, and truncations.
async fn part_faults(d: &Deployment, e: &EventData, day: i64) -> Vec<(String, Fault)> {
    let r = part_refs(d, &e.db).await.into_iter().find(|r| r.id == e.parts[&day]).expect("part reference");
    let data = d.object(&e.part_key(day));
    assert_eq!(data.len() as u64, r.size, "a part must be stored in a single chunk");
    let meta = baserunna::events::part::PartMeta::decode(
        &data.slice(r.meta_off as usize..(r.meta_off + r.meta_len) as usize),
        r.rows,
    )
    .expect("part metadata");
    let col = |n: usize| {
        meta.cols.iter().find(|c| c.cref == baserunna::events::part::ColRef::User(n as u32)).expect("column")
    };
    let len = data.len();
    let mut faults = vec![("(a) flip byte 0".to_string(), Fault::FlipBit(0))];
    for (name, n) in [("user", 1), ("v", 2), ("tag", 3)] {
        let c = col(n);
        faults.push((format!("(a) flip column {name} data"), Fault::FlipBit((c.data_off + c.data_len / 2) as usize)));
        faults
            .push((format!("(a) flip column {name} index"), Fault::FlipBit((c.index_off + c.index_len / 2) as usize)));
    }
    faults.push(("(a) flip part metadata".into(), Fault::FlipBit((r.meta_off + r.meta_len / 2) as usize)));
    faults.push((format!("(a) flip last byte {}", len - 1), Fault::FlipBit(len - 1)));
    faults.push((format!("(b) truncate to {}", len - 1), Fault::Truncate(len - 1)));
    faults.push((format!("(b) truncate to {}", len / 2), Fault::Truncate(len / 2)));
    faults
}

async fn event_cells(
    m: &mut Matrix,
    d: &mut Deployment,
    e: &EventData,
    object: &str,
    day: i64,
    faults: Vec<(String, Fault)>,
) {
    let want = query_day(d, &e.token, day).await;
    assert_eq!(want.status, 200, "baseline query: {}", want.excerpt());
    let want_rows = want.json()["rows"].clone();
    let key = e.part_key(day);
    for (name, fault) in faults {
        let (v, detail) = probe(
            d,
            &key,
            fault,
            async |d| query_day(d, &e.token, day).await,
            |r| {
                let got = &r.json()["rows"];
                match *got == want_rows {
                    true => Ok(()),
                    false => Err(format!("rows differ: {}", &got.to_string()[..got.to_string().len().min(100)])),
                }
            },
        )
        .await;
        m.add(object, &name, "event query by user", v, detail);
    }
}

#[test]
fn damaged_event_parts_are_detected() {
    run_paused(async {
        let mut d = Deployment::start(|_| {}, None).await;
        let e = EventData::write(&d).await;
        let baseline = query_day(&d, &e.token, 0).await.json();
        let rows = baseline["rows"].as_array().cloned().unwrap_or_default();
        let total: i64 = rows.iter().map(|r| r[1].as_i64().unwrap_or(0)).sum();
        assert_eq!((rows.len(), total), (50, 1000), "baseline query: {baseline}");
        let mut m = Matrix::default();
        let mut faults = part_faults(&d, &e, 0).await;
        let size = d.object(&e.part_key(0)).len();
        for day in [2, 3] {
            let other = d.object(&e.part_key(day));
            faults.push((format!("(c) part of day {day} ({} vs {size} bytes)", other.len()), Fault::Replace(other)));
        }
        event_cells(&mut m, &mut d, &e, "event part (whole GET)", 0, faults).await;
        let mut faults = part_faults(&d, &e, 3).await;
        let other = d.object(&e.part_key(2));
        let size = d.object(&e.part_key(3)).len();
        faults.push((format!("(c) part of day 2 ({} vs {size} bytes)", other.len()), Fault::Replace(other)));
        event_cells(&mut m, &mut d, &e, "event part (ranged GETs)", 3, faults).await;
        eprintln!("{}", m.table());
        m.assert_safe();
    });
}

#[test]
#[ignore = "TODO: Bind every stored object to its key"]
fn substituted_event_part_of_equal_shape_is_detected() {
    run_paused(async {
        let mut d = Deployment::start(|_| {}, None).await;
        let e = EventData::write(&d).await;
        let mut m = Matrix::default();
        let (data, other) = (d.object(&e.part_key(0)), d.object(&e.part_key(1)));
        assert_eq!(data.len(), other.len(), "parts of days 0 and 1 must have equal size");
        let fault =
            vec![(format!("(c) part of day 1, equal size {} and 1,000 rows", other.len()), Fault::Replace(other))];
        event_cells(&mut m, &mut d, &e, "event part (whole GET)", 0, fault).await;
        eprintln!("{}", m.table());
        m.assert_safe();
    });
}

// ---------------------------------------------------------------------------------------------
// Levels objects
// ---------------------------------------------------------------------------------------------

#[test]
fn damaged_levels_objects_are_detected() {
    run_paused(async {
        // Set `levels_inline_max` to 0 so that deep segment lists are stored in a levels object.
        let mut d = Deployment::start(|c| c.engine.levels_inline_max = 0, None).await;
        let kv = KvData::write(&d).await;
        let id = Manifest::decode(&d.object(&format!("dbs/{}/manifest", kv.big)))
            .expect("manifest")
            .levels
            .expect("database big stores its deep segments in a levels object");
        let key = format!("dbs/{}/seg/{id}.levels", kv.big);
        let data = d.object(&key);
        let older: Vec<String> = d
            .objects(&format!("dbs/{}/seg/", kv.big))
            .into_iter()
            .map(|(k, _)| k)
            .filter(|k| k.ends_with(".levels") && *k != key)
            .collect();
        let mut faults = flips_and_truncations(data.len());
        if let Some(other) = older.first() {
            let other = d.object(other);
            faults.push((
                format!("(c) older levels object ({} vs {} bytes)", other.len(), data.len()),
                Fault::Replace(other),
            ));
        }
        let mut m = Matrix::default();
        let value = value_of(&kv.big_entries, "k00150").to_vec();
        for (name, fault) in faults {
            let (v, detail) = probe(
                &mut d,
                &key,
                fault,
                async |d| get_key(d, &kv.token, "big", "k00150").await,
                |r| check_value(r, &value),
            )
            .await;
            m.add("levels object", &name, "KV get", v, detail);
        }
        eprintln!("{}", m.table());
        m.assert_safe();
    });
}

// ---------------------------------------------------------------------------------------------
// Local disk cache tier
// ---------------------------------------------------------------------------------------------

/// Directory for disk cache files of a test, emptied before use.
fn cache_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join("baserunna-store-integrity").join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("cache directory");
    dir
}

/// File name of a disk cache entry, as `DiskCache` derives it from the entry key.
fn cache_file_name(key: &str) -> String {
    key.bytes().map(|b| if b.is_ascii_alphanumeric() { (b as char).to_string() } else { format!("_{b:02x}") }).collect()
}

#[test]
fn damaged_disk_cache_files_fall_back_to_the_store() {
    run_paused(async {
        let dir = cache_dir("fallback");
        let open =
            |dir: &std::path::Path| Some(LocalTier(Arc::new(DiskCache::open(dir, 1 << 30).expect("disk cache"))));
        let mut d = Deployment::start(|_| {}, open(&dir)).await;
        let kv = KvData::write(&d).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        let (small_key, _) = kv.segment(&d, &kv.db, "b0000");
        let (large_key, large) = kv.segment(&d, &kv.big, "k00000");
        let large_idx = SegmentIndex::decode_whole(&large).expect("segment");
        let probe_key = key_in_middle_block(&large);
        let block = large_idx.blocks.len() / 2;
        let files = [
            ("small segment file #all", small_key.clone(), format!("{small_key}#all"), "b0150".to_string()),
            ("large segment file #meta", large_key.clone(), format!("{large_key}#meta"), probe_key.clone()),
            ("large segment file #b", large_key.clone(), format!("{large_key}#b{block}"), probe_key),
        ];
        let mut m = Matrix::default();
        for (object, store_key, entry, key) in files {
            let path = dir.join(cache_file_name(&entry));
            let original = std::fs::read(&path).unwrap_or_else(|e| panic!("{entry} is not cached: {e}"));
            let mut flipped = original.clone();
            flipped[original.len() / 2] ^= 1;
            let truncated = original[..original.len() - 1].to_vec();
            let (db, entries) = if store_key == small_key { ("d", &kv.groups["b"]) } else { ("big", &kv.big_entries) };
            let value = value_of(entries, &key).to_vec();
            for (fault, bytes) in [("(a) flip middle byte", flipped), ("(b) truncate by 1 byte", truncated)] {
                if let Some(n) = d.node.take() {
                    n.stop();
                }
                std::fs::write(&path, &bytes).expect("damage cache file");
                d.local_tier = open(&dir);
                d.boot().await.expect("node restarts");
                let store_gets = d.store.gets_of(&store_key);
                let capture = log::capture(Level::Debug);
                let r = get_key(&d, &kv.token, db, &key).await;
                let fetched = d.store.gets_of(&store_key) - store_gets;
                let (v, detail) = match (r.failed(), check_value(&r, &value)) {
                    (true, _) => (Verdict::Detected, format!("{} {}", r.status, r.excerpt())),
                    (false, Ok(())) if fetched > 0 => {
                        (Verdict::Correct, format!("{fetched} store GETs of the segment"))
                    }
                    (false, Ok(())) => (Verdict::NotRead, "no store GET; served from cache".into()),
                    (false, Err(e)) => (Verdict::SilentWrongData, e),
                };
                m.add(object, fault, "KV get", v, format!("{detail}; {}", logged(&capture)));
                drop(capture);
                // The read refills the cache file on a blocking thread, which does not advance with
                // the paused clock. Wait for the refill so that it cannot overwrite the next damage.
                if let Some(n) = d.node.take() {
                    n.stop();
                }
                for _ in 0..200 {
                    if std::fs::read(&path).is_ok_and(|b| b == original) {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                std::fs::write(&path, &original).expect("restore cache file");
            }
        }
        eprintln!("{}", m.table());
        m.assert_safe();
        let _ = std::fs::remove_dir_all(&dir);
    });
}

// ---------------------------------------------------------------------------------------------
// Event deletion masks
// ---------------------------------------------------------------------------------------------

/// Inserts 10,000 rows each on days 4 and 5, deletes 2,400 rows from each day, and returns the
/// external mask object key for each day.
async fn write_masks(d: &Deployment, e: &EventData) -> BTreeMap<i64, String> {
    for day in [4i64, 5] {
        let start = EVENTS_FROM_MS + day * DAY_MS;
        let body: Vec<Json> = (0..10_000)
            .map(|i| json!({"time": start + i as i64 * 800, "user": format!("u{:03}", i % 50), "v": i, "tag": "t"}))
            .collect();
        let r = d.post("/v1/dbs/ev/events/ev/rows", &e.token, Json::Array(body)).await;
        assert_eq!(r.status, 201, "bulk insert: {}", r.excerpt());
        let first = if day == 4 { 1 } else { 3 };
        let users: Vec<String> = (0..12).map(|i| format!("u{:03}", first + 4 * i)).collect();
        let del = json!({"from": start, "to": start + DAY_MS, "where": {"col": "user", "op": "in", "value": users}});
        let r = d.post("/v1/dbs/ev/events/ev/rows/delete", &e.token, del).await;
        assert_eq!(r.status, 200, "delete events: {}", r.excerpt());
    }
    tokio::time::sleep(Duration::from_secs(60)).await;
    let mut masks = BTreeMap::new();
    for r in part_refs(d, &e.db).await {
        let day = (r.min_time - EVENTS_FROM_MS).div_euclid(DAY_MS);
        if let Some(baserunna::engine::events::Mask::Object { id, .. }) = &r.mask {
            assert_eq!(r.deleted, 2_400, "day {day} must have 2,400 deleted rows");
            masks.insert(day, format!("dbs/{}/part/{id}/0000", e.db));
        }
    }
    assert_eq!(masks.keys().copied().collect::<Vec<_>>(), vec![4, 5], "days 4 and 5 must have mask objects");
    masks
}

async fn mask_cells(m: &mut Matrix, d: &mut Deployment, e: &EventData, key: &str, faults: Vec<(String, Fault)>) {
    let want = query_day(d, &e.token, 4).await.json()["rows"].clone();
    let users = want.as_array().map_or(0, Vec::len);
    assert_eq!(users, 38, "the masked query must return the 38 remaining users: {want}");
    for (name, fault) in faults {
        let (v, detail) = probe(
            d,
            key,
            fault,
            async |d| query_day(d, &e.token, 4).await,
            |r| {
                let got = &r.json()["rows"];
                match *got == want {
                    true => Ok(()),
                    false => Err(format!("rows differ: {}", &got.to_string()[..got.to_string().len().min(100)])),
                }
            },
        )
        .await;
        m.add("event deletion mask", &name, "event query by user", v, detail);
    }
}

#[test]
fn damaged_event_masks_are_detected() {
    run_paused(async {
        let mut d = Deployment::start(|_| {}, None).await;
        let e = EventData::write(&d).await;
        let masks = write_masks(&d, &e).await;
        let data = d.object(&masks[&4]);
        let mut faults = flips_and_truncations(data.len());
        let other = d.object(&format!("dbs/{}/part/{}/0000", e.db, e.parts[&0]));
        faults.push(("(c) event part in place of the mask".into(), Fault::Replace(other)));
        let mut m = Matrix::default();
        mask_cells(&mut m, &mut d, &e, &masks[&4], faults).await;
        eprintln!("{}", m.table());
        m.assert_safe();
    });
}

#[test]
#[ignore = "TODO: Bind every stored object to its key"]
fn substituted_event_mask_of_equal_count_is_detected() {
    run_paused(async {
        let mut d = Deployment::start(|_| {}, None).await;
        let e = EventData::write(&d).await;
        let masks = write_masks(&d, &e).await;
        let other = d.object(&masks[&5]);
        let fault = vec![(
            format!("(c) mask of day 5 ({} vs {} bytes, 2,400 other rows)", other.len(), d.object(&masks[&4]).len()),
            Fault::Replace(other),
        )];
        let mut m = Matrix::default();
        mask_cells(&mut m, &mut d, &e, &masks[&4], fault).await;
        eprintln!("{}", m.table());
        m.assert_safe();
    });
}

// ---------------------------------------------------------------------------------------------
// Maintenance on a persistently corrupt segment
// ---------------------------------------------------------------------------------------------

/// Restores the production rate limits and database quota that `cost::node_config` disables.
fn production_limits(c: &mut NodeConfig) {
    let d = NodeConfig::default();
    c.limits = d.limits;
    c.engine.db_max_bytes = d.engine.db_max_bytes;
}

/// Store and engine state of database `w` after a number of commits.
struct MaintenanceSample {
    commits: u64,
    level0: usize,
    segments: usize,
    manifest_bytes: usize,
    stored_bytes: u64,
    corrupt_gets: u64,
    db_gets: u64,
    db_lists: u64,
    db_puts_total: u64,
}

/// Records of `capture` at `info` level or above (the default level) that are not request logs,
/// as `level: message` with a count.
fn default_level_records(capture: &log::Capture) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    for r in capture.records() {
        let level = r["level"].as_str().unwrap_or_default();
        let msg = r["msg"].as_str().unwrap_or_default();
        if matches!(level, "error" | "warn" | "info") && msg != "request" && msg != "commit" {
            *out.entry(format!("{level}: {}", &msg[..msg.len().min(100)])).or_default() += 1;
        }
    }
    out
}

/// Debug-level records of `capture`, with digits removed so repeated messages group together.
fn debug_records(capture: &log::Capture) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    for r in capture.records().iter().filter(|r| r["level"] == "debug") {
        let msg: String = r["msg"].as_str().unwrap_or_default().chars().filter(|c| !c.is_ascii_hexdigit()).collect();
        *out.entry(msg[..msg.len().min(100)].to_string()).or_default() += 1;
    }
    out
}

#[test]
#[ignore = "TODO: Surface persistent maintenance failures in metrics and error logs"]
fn persistent_segment_corruption_is_reported_and_backed_off() {
    run_paused(async {
        let mut d = Deployment::start(production_limits, None).await;
        let token = d.signup("admin@integrity.test").await;
        let db = d.create_db(&token, "w").await;
        let mut rng = Rng::new(8);
        let damaged: Entries = (0..300).map(|i| (format!("a{i:04}"), random_bytes(&mut rng, 1000))).collect();
        put_all(&d, &token, "w", &damaged).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        let (seg, ..) = segments(&d, &db)
            .into_iter()
            .find(|s| s.4.get(1..) == Some(b"a0000".as_slice()))
            .expect("segment of the a keys");
        let corrupt_key = format!("dbs/{db}/seg/{seg}");
        let data = d.object(&corrupt_key);
        let layout = segment_layout(&data);
        d.store.set_fault(&corrupt_key, Fault::FlipBit(mid(&layout.middle_block)));
        d.restart().await.expect("node restarts");
        tokio::time::sleep(Duration::from_secs(10)).await;

        let capture = log::capture(Level::Debug);
        let prefix = format!("dbs/{db}/");
        let lists_before = *d.store.lists.lock();
        let puts_before = *d.store.puts.lock();
        let sample = |d: &Deployment, commits: u64| {
            let m = d.manifest(&db);
            MaintenanceSample {
                commits,
                level0: m.segments.iter().filter(|s| s.level == 0).count(),
                segments: m.segments.len(),
                manifest_bytes: d.object(&format!("dbs/{db}/manifest")).len(),
                stored_bytes: d.stored_bytes(&db),
                corrupt_gets: d.store.gets_of(&corrupt_key),
                db_gets: d.store.gets_under(&prefix),
                db_lists: *d.store.lists.lock() - lists_before,
                db_puts_total: *d.store.puts.lock() - puts_before,
            }
        };
        let mut samples = vec![sample(&d, 0)];
        let mut statuses: BTreeMap<u16, u64> = BTreeMap::new();
        let mut live: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        // Phase 1: 1,000 commits at 1 per second that overwrite 50 keys.
        // Phase 2: 1 hour without commits so a sweep is due, followed by 200 commits.
        // Phase 3: a node restart with cold caches, followed by 100 commits.
        for i in 1..=1300u64 {
            if i == 1001 {
                tokio::time::sleep(Duration::from_secs(3_700)).await;
                samples.push(sample(&d, 1000));
            }
            if i == 1201 {
                d.restart().await.expect("node restarts");
                tokio::time::sleep(Duration::from_secs(10)).await;
                samples.push(sample(&d, 1200));
            }
            let (key, value) = (format!("w{:02}", i % 50), random_bytes(&mut rng, 8 * 1024));
            let r = d.call("PUT", &format!("/v1/dbs/w/kv/{key}"), &token, &[], Bytes::from(value.clone())).await;
            *statuses.entry(r.status).or_default() += 1;
            if r.status == 200 {
                live.insert(key, value);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            if [125, 250, 500, 1000, 1100, 1200, 1250, 1300].contains(&i) {
                samples.push(sample(&d, i));
            }
        }
        let maintenance_logs = default_level_records(&capture);
        let debug_logs = debug_records(&capture);
        drop(capture);

        eprintln!("commit statuses: {statuses:?}");
        eprintln!(
            "| commits | level-0 segments | segments | manifest bytes | stored bytes | GETs of corrupt segment \
             (per commit in window) | db GETs (per commit in window) | LISTs (all) | PUTs (all) |"
        );
        for w in samples.windows(2) {
            let (a, b) = (&w[0], &w[1]);
            let n = (b.commits - a.commits).max(1) as f64;
            eprintln!(
                "| {} | {} | {} | {} | {} | {} ({:.2}) | {} ({:.2}) | {} | {} |",
                b.commits,
                b.level0,
                b.segments,
                b.manifest_bytes,
                b.stored_bytes,
                b.corrupt_gets,
                (b.corrupt_gets - a.corrupt_gets) as f64 / n,
                b.db_gets,
                (b.db_gets - a.db_gets) as f64 / n,
                b.db_lists,
                b.db_puts_total
            );
        }
        eprintln!("default-level log records other than request and commit logs: {maintenance_logs:?}");
        eprintln!("debug-level records: {debug_logs:?}");
        let metrics = d.call("GET", "/v1/admin/metrics", &token, &[], Bytes::new()).await.json();
        let store_metrics: BTreeMap<&String, &Json> =
            metrics["store"].as_object().map(|o| o.iter().collect()).unwrap_or_default();
        for (k, v) in &store_metrics {
            eprintln!("metric store.{k}: count {} errors {}", v["count"], v["errors"]);
        }
        eprintln!("metric keys: {:?}", metrics.as_object().map(|o| o.keys().collect::<Vec<_>>()));

        let good = live.iter().next().expect("a written key");
        let good_read = get_key(&d, &token, "w", good.0).await;
        let bad_read = get_key(&d, &token, "w", "a0150").await;
        eprintln!(
            "read of an undamaged key: {} ({:?}); read of a key in the damaged block: {} {}",
            good_read.status,
            check_value(&good_read, good.1),
            bad_read.status,
            bad_read.excerpt()
        );

        let last = samples.len() - 1;
        let late = (samples[last].corrupt_gets - samples[last - 1].corrupt_gets) as f64
            / (samples[last].commits - samples[last - 1].commits) as f64;
        let signaled = maintenance_logs.keys().any(|k| k.starts_with("error") || k.starts_with("warn"));
        assert!(
            signaled && late < 0.1,
            "maintenance must report the failure at warn or error level and back off: default-level records \
             {maintenance_logs:?}; {late:.2} GETs of the corrupt segment per commit over the last window"
        );
    });
}
