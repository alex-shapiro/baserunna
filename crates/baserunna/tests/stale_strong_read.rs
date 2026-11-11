//! Verifies that strong reads never return older data after a client observes newer data.
//! This file tests an object store that applies a manifest `PUT` after returning an error.
//!
//! Each test commits `k = old` and an event row. It then commits `k = new` and a second event row on
//! the lease holder while [`LateManifestPutStore`] fails every manifest `PUT` of that commit. The
//! commit returns `503` with `outcome: "unknown"`. The store then applies the first failed `PUT`,
//! which still matches the manifest ETag. Each test reads through a client-visible path until
//! every node returns the new data. The test fails if a response shows the old data after an
//! earlier response showed the new data.

use axum::body::Body;
use baserunna::cluster::{self, Transport, TransportError};
use baserunna::env::Env;
use baserunna::node::{Node, Services};
use baserunna::sim::cost::{Resp, call_router, node_config, run_paused};
use baserunna::store::{BoxFut, GetOpts, GetResult, ListPage, MemStore, ObjectStore, PutMode, SResult, StoreError};
use bytes::Bytes;
use http::{Request, Response};
use parking_lot::Mutex;
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tower::ServiceExt;

const BASE_MS: u64 = 1_750_000_000_000;
const QUERY_TO_MS: i64 = 9_999_999_999_999;
/// Interval between two rounds of client requests after the late `PUT` is applied.
const ROUND_MS: u64 = 100;
/// Maximum test-clock time for all nodes to return the new data.
const SETTLE_MS: u64 = 30_000;

/// Object store wrapper that fails the next manifest `PUT`s of a database and buffers them.
/// [`LateManifestPutStore::apply_late_puts`] applies those writes with their original preconditions.
struct LateManifestPutStore {
    mem: Arc<MemStore>,
    /// Manifest key and remaining failed `PUT` count.
    failing: Mutex<Option<(String, u32)>>,
    late_puts: Mutex<Vec<(String, Bytes, PutMode)>>,
}

impl LateManifestPutStore {
    fn fail_manifest_puts(&self, db_id: &str, n: u32) {
        *self.failing.lock() = Some((baserunna::engine::manifest_key(db_id), n));
    }

    fn stop_failing(&self) {
        *self.failing.lock() = None;
    }

    fn late_put_count(&self) -> usize {
        self.late_puts.lock().len()
    }

    fn apply_late_puts(&self) -> Vec<SResult<String>> {
        let late = std::mem::take(&mut *self.late_puts.lock());
        late.into_iter().map(|(key, data, mode)| self.mem.put_sync(&key, data, &mode)).collect()
    }

    fn takes_failure(&self, key: &str) -> bool {
        let mut failing = self.failing.lock();
        match failing.as_mut() {
            Some((k, n)) if k == key && *n > 0 => {
                *n -= 1;
                true
            }
            _ => false,
        }
    }
}

impl ObjectStore for LateManifestPutStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        self.mem.get(key, opts)
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move {
            if self.takes_failure(key) {
                self.late_puts.lock().push((key.to_string(), data, mode));
                return Err(StoreError::Unavailable("connection reset".into()));
            }
            self.mem.put_sync(key, data, &mode)
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        self.mem.delete(key)
    }

    fn list<'a>(&'a self, prefix: &'a str, after: Option<&'a str>, limit: usize) -> BoxFut<'a, SResult<ListPage>> {
        self.mem.list(prefix, after, limit)
    }

    fn time_precision_ms(&self) -> u64 {
        self.mem.time_precision_ms()
    }
}

/// Peer network between the nodes of a [`Cluster`].
#[derive(Default)]
struct PeerNet {
    routers: BTreeMap<String, axum::Router>,
    /// Count of forwarded client requests per address that fail before delivery.
    failed_forwards: BTreeMap<String, u32>,
}

struct PeerTransport(Arc<Mutex<PeerNet>>);

impl Transport for PeerTransport {
    fn call(&self, addr: &str, req: Request<Body>) -> BoxFut<'static, Result<Response<Body>, TransportError>> {
        let mut net = self.0.lock();
        let failed = req.uri().path().starts_with("/v1/dbs/")
            && net.failed_forwards.get_mut(addr).is_some_and(|n| {
                let fail = *n > 0;
                *n = n.saturating_sub(1);
                fail
            });
        let router = net.routers.get(addr).cloned();
        drop(net);
        Box::pin(async move {
            let undelivered = |m: &str| TransportError { message: m.into(), maybe_delivered: false };
            if failed {
                return Err(undelivered("connection reset"));
            }
            let router = router.ok_or_else(|| undelivered("unknown peer"))?;
            match router.oneshot(req).await {
                Ok(r) => Ok(r),
                Err(e) => match e {},
            }
        })
    }
}

fn peer_addr(i: usize) -> String {
    format!("peer://n{i}")
}

/// Nodes with production defaults that share a [`LateManifestPutStore`].
struct Cluster {
    store: Arc<LateManifestPutStore>,
    net: Arc<Mutex<PeerNet>>,
    origin: Instant,
    nodes: Vec<Arc<Node>>,
}

impl Cluster {
    async fn start(n: usize) -> Cluster {
        let origin = Instant::now();
        let mem = Arc::new(MemStore::new(Box::new(move || {
            BASE_MS + Instant::now().saturating_duration_since(origin).as_millis() as u64
        })));
        let store = Arc::new(LateManifestPutStore { mem, failing: Mutex::default(), late_puts: Mutex::default() });
        let mut c = Cluster { store, net: Arc::default(), origin, nodes: Vec::new() };
        for _ in 0..n {
            c.add_node().await;
        }
        c
    }

    async fn add_node(&mut self) -> usize {
        let i = self.nodes.len();
        let env = Arc::new(Env::sim(0x57a1e + i as u64, BASE_MS, self.origin, 0));
        let services = Services { transport: Arc::new(PeerTransport(self.net.clone())), oidc: None, mailer: None };
        let node = Node::start(node_config(&format!("n{i}"), &peer_addr(i)), env, self.store.clone(), services)
            .await
            .expect("node starts");
        let router = baserunna::api::router(node.clone());
        self.net.lock().routers.insert(peer_addr(i), router);
        self.nodes.push(node);
        i
    }

    async fn call(&self, i: usize, method: &str, path: &str, token: &str, body: Json) -> Resp {
        let body = if body.is_null() { Bytes::new() } else { Bytes::from(body.to_string()) };
        call_router(&self.nodes[i].router().expect("router"), method, path, token, body).await
    }

    fn lease_holder(&self, db_id: &str) -> usize {
        let holder = self.nodes.iter().find_map(|n| n.engine.db(db_id).lease_holder()).expect("a lease holder");
        self.nodes.iter().position(|n| n.id == holder).expect("the lease holder is a node")
    }

    /// Returns the rendezvous home node of `db_id` in the membership view of node `i`.
    fn home_seen_by(&self, i: usize, db_id: &str) -> usize {
        let live = self.nodes[i].live_members();
        let home = cluster::rendezvous(db_id, &live).expect("a live node").id.clone();
        self.nodes.iter().position(|n| n.id == home).expect("the home is a node")
    }

    fn fail_forwards_to(&self, i: usize, n: u32) {
        self.net.lock().failed_forwards.insert(peer_addr(i), n);
    }

    fn shutdown(&self) {
        for n in &self.nodes {
            n.stop();
        }
    }
}

fn excerpt(r: &Resp) -> String {
    String::from_utf8_lossy(&r.body[..r.body.len().min(300)]).into_owned()
}

/// Relationship between the lease holder and the rendezvous home node of the database.
#[derive(Clone, Copy, Debug)]
enum Home {
    /// The lease holder is the home node.
    LeaseHolder,
    /// A newly joined node is the home node while the first node holds the lease.
    JoinedNode,
}

/// Client request that returns either the old or the new data.
#[derive(Clone, Copy, Debug)]
enum Probe {
    /// `GET /kv/k` with strong consistency.
    StrongGet(usize),
    /// Strong change-feed long poll from the cursor after `k = old`.
    ChangeFeed(usize),
    /// `GET /commits/key:<idempotency key of the late commit>`.
    CommitStatus(usize),
    /// Strong event query that counts the rows of table `ev`.
    StrongEventCount(usize),
}

/// Response recorded from a client probe.
#[derive(Debug)]
struct ProbeResponse {
    /// Test-clock milliseconds after the store applies the late `PUT`.
    at_ms: i64,
    probe: Probe,
    status: u16,
    shows_new: bool,
    detail: String,
}

/// State of a database after `k = new` fails with an unknown outcome.
struct LateCommit {
    c: Cluster,
    token: String,
    db: String,
    db_id: String,
    holder: usize,
    other: usize,
    /// Change-feed cursor after the commit of `k = old`.
    cursor: String,
    commit_finished_at: Instant,
    applied_at: Option<Instant>,
    responses: Vec<ProbeResponse>,
}

const IDEMPOTENCY_KEY: &str = "late-commit";

fn event_table() -> Json {
    json!({"name": "ev", "columns": [{"name": "time", "type": "time"}, {"name": "user", "type": "string"}],
        "time_column": "time", "order_by": ["time"]})
}

fn commit_body(value: &str, user: &str, idempotency_key: Option<&str>) -> Json {
    let mut b = json!({"ops": [
        {"op": "put", "key": "k", "value": value},
        {"op": "insert_events", "table": "ev", "rows": [{"time": 1_749_000_000_000i64, "user": user}]},
    ]});
    if let Some(k) = idempotency_key {
        b["idempotency_key"] = json!(k);
    }
    b
}

async fn create_db_with_old_data(c: &Cluster, token: &str, name: &str) -> String {
    let r = c.call(0, "POST", "/v1/dbs", token, json!({"name": name})).await;
    assert_eq!(r.status, 201, "create db: {}", excerpt(&r));
    let id = r.json()["id"].as_str().expect("db id").to_string();
    let r = c.call(0, "POST", &format!("/v1/dbs/{name}/events"), token, event_table()).await;
    assert_eq!(r.status, 201, "create event table: {}", excerpt(&r));
    let r = c.call(0, "POST", &format!("/v1/dbs/{name}/txn"), token, commit_body("old", "u-old", None)).await;
    assert_eq!(r.status, 200, "commit old: {}", excerpt(&r));
    id
}

/// Builds a two-node cluster and commits `k = new` on the lease holder with an unknown outcome.
async fn late_commit(home: Home) -> LateCommit {
    let (c, token, db, db_id) = match home {
        Home::LeaseHolder => {
            let c = Cluster::start(2).await;
            // Wait until both nodes list each other and agree on the home node.
            tokio::time::sleep(Duration::from_millis(c.nodes[0].cfg.members_refresh_ms + 1_000)).await;
            let token = signup(&c).await;
            let db_id = create_db_with_old_data(&c, &token, "d").await;
            (c, token, "d".to_string(), db_id)
        }
        Home::JoinedNode => {
            let mut c = Cluster::start(1).await;
            let token = signup(&c).await;
            let mut ids = Vec::new();
            for i in 0..8 {
                ids.push((format!("d{i}"), create_db_with_old_data(&c, &token, &format!("d{i}")).await));
            }
            let joined = c.add_node().await;
            let (db, db_id) = ids
                .into_iter()
                .find(|(_, id)| c.home_seen_by(joined, id) == joined)
                .expect("setup: the joined node is the home node of a database");
            // Renew the lease on the first node.
            let renew = json!({"ops": [{"op": "put", "key": "k", "value": "old"}]});
            let r = c.call(0, "POST", &format!("/v1/dbs/{db}/txn"), &token, renew).await;
            assert_eq!(r.status, 200, "{}", excerpt(&r));
            (c, token, db, db_id)
        }
    };
    let holder = c.lease_holder(&db_id);
    let other = 1 - holder;
    let home_seen_by_other = c.home_seen_by(other, &db_id);
    match home {
        Home::LeaseHolder => assert_eq!(home_seen_by_other, holder, "setup: the lease holder is the home node"),
        Home::JoinedNode => assert_eq!(home_seen_by_other, other, "setup: the joined node is the home node"),
    }
    let feed = c.call(holder, "GET", &format!("/v1/dbs/{db}/changes?after=0"), &token, Json::Null).await;
    let cursor = feed.json()["next_cursor"].as_str().expect("cursor").to_string();
    let retries = c.nodes[holder].cfg.engine.commit_retry_max;
    c.store.fail_manifest_puts(&db_id, retries);
    let path = format!("/v1/dbs/{db}/txn");
    let r = c.call(holder, "POST", &path, &token, commit_body("new", "u-new", Some(IDEMPOTENCY_KEY))).await;
    c.store.stop_failing();
    assert_eq!(r.status, 503, "setup: the commit fails: {}", excerpt(&r));
    assert_eq!(r.json()["error"]["details"]["outcome"], "unknown", "setup: the outcome is unknown: {}", excerpt(&r));
    eprintln!("{home:?}: holder n{holder}, other n{other}; {} manifest PUTs failed", c.store.late_put_count());
    LateCommit {
        c,
        token,
        db,
        db_id,
        holder,
        other,
        cursor,
        commit_finished_at: Instant::now(),
        applied_at: None,
        responses: Vec::new(),
    }
}

async fn signup(c: &Cluster) -> String {
    let r =
        c.call(0, "POST", "/v1/auth/signup", "", json!({"email": "a@stale.test", "password": "password-123"})).await;
    r.json()["token"].as_str().unwrap_or_else(|| panic!("signup: {}", excerpt(&r))).to_string()
}

impl LateCommit {
    /// Applies the buffered manifest `PUT`s. The first `PUT` matches the stored manifest ETag.
    fn apply_late_put(&mut self) {
        let applied = self.c.store.apply_late_puts();
        assert!(applied.first().is_some_and(Result::is_ok), "setup: the first late PUT is applied: {applied:?}");
        self.applied_at = Some(Instant::now());
        eprintln!(
            "late PUT applied {} ms after the 503; later late PUTs: {:?}",
            self.commit_finished_at.elapsed().as_millis(),
            applied[1..].iter().map(|r| r.as_ref().map(drop).map_err(ToString::to_string)).collect::<Vec<_>>()
        );
    }

    fn at_ms(&self) -> i64 {
        match self.applied_at {
            Some(t) => t.elapsed().as_millis() as i64,
            None => -(self.commit_finished_at.elapsed().as_millis() as i64),
        }
    }

    async fn ask(&mut self, probe: Probe) -> bool {
        let (db, t) = (&self.db, &self.token);
        let (r, shows_new, detail) = match probe {
            Probe::StrongGet(i) => {
                let r = self.c.call(i, "GET", &format!("/v1/dbs/{db}/kv/k"), t, Json::Null).await;
                let v = String::from_utf8_lossy(&r.body).into_owned();
                (r, v == "new", v)
            }
            Probe::ChangeFeed(i) => {
                let path = format!("/v1/dbs/{db}/changes?after={}&wait_ms=2000", self.cursor);
                let r = self.c.call(i, "GET", &path, t, Json::Null).await;
                let changes = r.json()["changes"].clone();
                let new = changes.as_array().is_some_and(|cs| cs.iter().any(|ch| ch["value"] == "new"));
                (r, new, changes.to_string())
            }
            Probe::CommitStatus(i) => {
                let path = format!("/v1/dbs/{db}/commits/key:{IDEMPOTENCY_KEY}");
                let r = self.c.call(i, "GET", &path, t, Json::Null).await;
                let status = r.json()["status"].as_str().unwrap_or_default().to_string();
                (r, status == "committed", status)
            }
            Probe::StrongEventCount(i) => {
                let q = json!({"from": 0, "to": QUERY_TO_MS, "aggregates": {"n": {"fn": "count"}},
                    "consistency": "strong"});
                let r = self.c.call(i, "POST", &format!("/v1/dbs/{db}/events/ev/rows/query"), t, q).await;
                let j = r.json();
                let row = &j["rows"][0];
                let n = row.get(0).or_else(|| row.get("n")).and_then(Json::as_u64);
                (r, n == Some(2), format!("count {n:?} at version {}", j["version"]))
            }
        };
        let response = ProbeResponse { at_ms: self.at_ms(), probe, status: r.status, shows_new, detail };
        self.responses.push(response);
        shows_new
    }

    /// Runs `round` every [`ROUND_MS`] until all probes in a round return the new data.
    async fn ask_until_new(&mut self, round: &[Probe]) {
        let start = Instant::now();
        loop {
            let mut all_new = true;
            for p in round {
                all_new &= self.ask(*p).await;
            }
            if all_new {
                return;
            }
            assert!(start.elapsed() < Duration::from_millis(SETTLE_MS), "the nodes never returned the new data");
            tokio::time::sleep(Duration::from_millis(ROUND_MS)).await;
        }
    }

    /// Prints all responses and asserts that none returned old data after an earlier response returned new data.
    fn assert_no_response_goes_back(&self, path: &str) {
        let first_new = self.responses.iter().position(|a| a.shows_new);
        let mut went_back = Vec::new();
        for (i, a) in self.responses.iter().enumerate() {
            let mark = match (a.shows_new, first_new) {
                (false, Some(f)) if i > f => {
                    went_back.push(a);
                    "OLD AFTER NEW"
                }
                (true, _) => "new",
                (false, _) => "old",
            };
            eprintln!("  t={:>6} ms {:<22} {} {:<13} {}", a.at_ms, format!("{:?}", a.probe), a.status, mark, a.detail);
        }
        let holder = self.c.nodes[self.holder].engine.db(&self.db_id).lease_holder();
        eprintln!("{path}: holder n{}, other n{}; lease holder now {holder:?}", self.holder, self.other);
        if let (Some(f), Some(last)) = (first_new, went_back.last()) {
            let first = &self.responses[f];
            panic!(
                "{path}: {} responses showed the old data after {:?} showed the new data at t={} ms; \
                 the last at t={} ms ({:?}), so the client saw old data for {} ms after new data",
                went_back.len(),
                first.probe,
                first.at_ms,
                last.at_ms,
                last.probe,
                last.at_ms - first.at_ms
            );
        }
    }
}

/// Runs the late commit for `home`, applies the late `PUT`, and polls `round` until all probes return new data.
fn check_path(path: &str, home: Home, round: fn(&LateCommit) -> Vec<Probe>, failed_forwards: u32) {
    run_paused(async {
        let mut lc = late_commit(home).await;
        let probes = round(&lc);
        lc.ask(Probe::StrongGet(lc.holder)).await;
        lc.apply_late_put();
        lc.c.fail_forwards_to(lc.holder, failed_forwards);
        lc.ask_until_new(&probes).await;
        lc.c.shutdown();
        lc.assert_no_response_goes_back(path);
    });
}

// (i) Strong `GET` on the other node, followed by a strong `GET` on the lease holder.

#[test]
fn strong_gets_never_go_back_if_the_lease_holder_is_home() {
    check_path(
        "(i) home = holder",
        Home::LeaseHolder,
        |lc| vec![Probe::StrongGet(lc.other), Probe::StrongGet(lc.holder)],
        0,
    );
}

#[test]
fn strong_gets_never_go_back_after_a_node_joins() {
    check_path(
        "(i) home = joined node",
        Home::JoinedNode,
        |lc| vec![Probe::StrongGet(lc.other), Probe::StrongGet(lc.holder)],
        0,
    );
}

// (ii) Strong change-feed long poll on the home node, followed by a strong `GET`.

#[test]
fn change_feed_then_strong_get_never_go_back_if_the_lease_holder_is_home() {
    check_path(
        "(ii) home = holder",
        Home::LeaseHolder,
        |lc| vec![Probe::ChangeFeed(lc.other), Probe::StrongGet(lc.holder), Probe::StrongGet(lc.other)],
        0,
    );
}

#[test]
fn change_feed_then_strong_get_never_go_back_after_a_node_joins() {
    check_path(
        "(ii) home = joined node",
        Home::JoinedNode,
        |lc| vec![Probe::ChangeFeed(lc.other), Probe::StrongGet(lc.holder), Probe::StrongGet(lc.other)],
        0,
    );
}

// (iii) Commit status check, followed by a strong `GET`.

#[test]
fn writer_reads_its_write_after_commit_status_is_committed() {
    check_path(
        "(iii) commit status",
        Home::LeaseHolder,
        |lc| vec![Probe::CommitStatus(lc.other), Probe::StrongGet(lc.holder), Probe::StrongGet(lc.other)],
        0,
    );
}

// (iv) Strong event queries, which the receiving node serves locally.

#[test]
fn strong_event_queries_never_go_back() {
    check_path(
        "(iv) strong event query",
        Home::LeaseHolder,
        |lc| vec![Probe::StrongEventCount(lc.other), Probe::StrongEventCount(lc.holder)],
        0,
    );
}

// (v) Strong `GET` served locally by the other node after a forward to the lease holder fails.

#[test]
fn strong_gets_never_go_back_after_a_failed_forward() {
    check_path(
        "(v) failed forward",
        Home::LeaseHolder,
        |lc| vec![Probe::StrongGet(lc.other), Probe::StrongGet(lc.holder)],
        1,
    );
}
