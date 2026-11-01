//! End-to-end tests of `baserunna-client` against an in-process node, including responses lost
//! before or after the server applied a commit.

use baserunna::env::Env;
use baserunna::node::{Node, NodeConfig, Services};
use baserunna::store::MemStore;
use baserunna_api::{self as api, ColumnType, Endpoint as _, NewColumn, NewTable, Outcome};
use baserunna_client::{BoxFuture, Client, Db, Error, Expect, Op, OpResult, Route, Row, Transport, TransportError};
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tower::ServiceExt;

/// How [`Router`] fails the next request to `fail_route`.
#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    /// Drops the request without sending it, reporting an unknown outcome.
    Drop,
    /// Sends the request and drops the response, reporting an unknown outcome.
    LoseResponse,
    /// Sends the request and never returns its response.
    Hang,
    /// Sends the request and returns its response after 500 ms.
    Slow,
}

/// Transport that sends requests to an in-process router and can fail one route.
struct Router {
    app: axum::Router,
    fail_route: &'static str,
    fault: Mutex<(Fault, usize)>,
    dropped: Mutex<Vec<http::Request<Bytes>>>,
    sent: AtomicUsize,
}

impl Router {
    fn fail(&self, route: &'static str, fault: Fault, times: usize) -> &Self {
        assert_eq!(route, self.fail_route);
        *self.fault.lock() = (fault, times);
        self
    }

    async fn call(app: axum::Router, req: http::Request<Bytes>) -> http::Response<Bytes> {
        let resp = app.oneshot(req.map(axum::body::Body::from)).await.unwrap();
        let (parts, body) = resp.into_parts();
        http::Response::from_parts(parts, axum::body::to_bytes(body, usize::MAX).await.unwrap())
    }
}

impl Transport for Router {
    fn send(&self, req: http::Request<Bytes>) -> BoxFuture<'_, Result<http::Response<Bytes>, TransportError>> {
        Box::pin(async move {
            self.sent.fetch_add(1, Ordering::Relaxed);
            let fault = {
                let mut f = self.fault.lock();
                match req.extensions().get::<Route>() {
                    Some(Route(r)) if *r == self.fail_route && f.1 > 0 => {
                        f.1 -= 1;
                        f.0
                    }
                    _ => Fault::None,
                }
            };
            match fault {
                Fault::None => Ok(Router::call(self.app.clone(), req).await),
                Fault::Drop => {
                    self.dropped.lock().push(req);
                    Err(TransportError::Unknown("connection reset".into()))
                }
                Fault::LoseResponse => {
                    Router::call(self.app.clone(), req).await;
                    Err(TransportError::Unknown("response lost".into()))
                }
                Fault::Hang => {
                    Router::call(self.app.clone(), req).await;
                    std::future::pending().await
                }
                Fault::Slow => {
                    let resp = Router::call(self.app.clone(), req).await;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    Ok(resp)
                }
            }
        })
    }
}

async fn node(fail_route: &'static str) -> (Db, Arc<Router>) {
    node_with(fail_route, |_| {}).await
}

async fn node_with(fail_route: &'static str, configure: impl FnOnce(&mut NodeConfig)) -> (Db, Arc<Router>) {
    baserunna::log::set_level(Some(baserunna::log::Level::Warn));
    let mut cfg = NodeConfig {
        pbkdf2_iterations: 1000,
        limits: baserunna::limits::LimitConfig::unlimited(),
        ..NodeConfig::default()
    };
    configure(&mut cfg);
    let store = Arc::new(MemStore::with_system_clock());
    let node = Node::start(cfg, Arc::new(Env::real()), store, Services::local()).await.unwrap();
    let router = Arc::new(Router {
        app: baserunna::api::router(node),
        fail_route,
        fault: Mutex::new((Fault::None, 0)),
        dropped: Mutex::default(),
        sent: AtomicUsize::new(0),
    });
    let client = Client::new(router.clone());
    let session = client.sign_up("admin@client.test", "password-123").await.unwrap();
    let db = client.with_token(&session.token).db("app");
    db.ensure().await.unwrap();
    db.ensure().await.unwrap();
    db.create_table(&table()).await.unwrap();
    (db, router)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Item {
    owner: String,
    id: i64,
    label: Option<String>,
}

impl Row for Item {
    const TABLE: &'static str = "items";
}

fn table() -> NewTable {
    NewTable {
        name: Item::TABLE.into(),
        columns: vec![
            NewColumn::new("owner", ColumnType::String),
            NewColumn::new("id", ColumnType::Int),
            NewColumn::nullable("label", ColumnType::String),
        ],
        primary_key: vec!["owner".into(), "id".into()],
        indexes: vec![],
        if_not_exists: true,
    }
}

fn item(owner: &str, id: i64) -> Item {
    Item { owner: owner.into(), id, label: None }
}

/// Reads every page of `op` in one-shot reads.
async fn pages(db: &Db, mut op: Op) -> Vec<OpResult> {
    let mut out = Vec::new();
    loop {
        let r = db.read_one(op.clone()).await.unwrap();
        let next = op.next_page(&r);
        out.push(r);
        match next {
            Some(n) => op = n,
            None => return out,
        }
    }
}

#[tokio::test]
async fn tables_rows_streams_and_keys_round_trip_through_the_server() {
    let (db, _) = node("").await;
    let again = db.create_table(&table()).await.unwrap();
    assert_eq!(again.table.primary_key, ["owner", "id"], "creating an existing table returns its schema");
    let page = db.list_tables(api::ListTablesQuery::default()).await.unwrap();
    assert_eq!(page.tables.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(), ["items"]);
    assert_eq!(db.get_table("items").await.unwrap(), again.table);

    let rows: Vec<Op> = (0..2500).map(|i| Op::insert(&item(if i % 2 == 0 { "a" } else { "b" }, i))).collect();
    db.write(&rows).await.unwrap();
    let a: Vec<Item> =
        pages(&db, Op::query::<Item>("a")).await.into_iter().flat_map(|r| r.rows::<Item>().unwrap()).collect();
    assert_eq!(a.len(), 1250, "a prefix query pages through every matching row");
    assert!(a.iter().all(|i| i.owner == "a") && a.windows(2).all(|w| w[0].id < w[1].id));
    let all = pages(&db, Op::query_all::<Item>()).await;
    assert_eq!((all.len(), all.into_iter().map(|r| r.rows::<Item>().unwrap().len()).sum::<usize>()), (3, 2500));

    let set = serde_json::json!({"label": "x"});
    let ops = [Op::update::<Item>(("a", 0), &set), Op::update::<Item>(("a", 1), &set), Op::delete_rows::<Item>("b")];
    let out = db.txn(&ops, None).await.unwrap().results.unwrap();
    assert_eq!(out, [OpResult::Found(true), OpResult::Found(false), OpResult::Count(1250)]);
    let got = db.read_one(Op::get_row::<Item>(("a", 0))).await.unwrap().row::<Item>().unwrap();
    assert_eq!(got, Some(Item { label: Some("x".into()), ..item("a", 0) }));
    let e = db.write(&[Op::insert(&item("a", 0))]).await.unwrap_err();
    assert_eq!(e.code(), Some(api::ErrorCode::AlreadyExists), "{e}");

    let events: Vec<u64> = (0..2100).collect();
    let appended =
        db.write(&[Op::append("log", &events[..1500]), Op::append("log", &events[1500..])]).await.unwrap().unwrap();
    assert_eq!(appended.appends, [0, 1500]);
    let read: Vec<u64> =
        pages(&db, Op::read_stream("log", 0)).await.into_iter().flat_map(|r| r.records().unwrap()).collect();
    assert_eq!(read, events);

    let keys: Vec<Op> = (0..1500).map(|i| Op::put(format!("k/{i:04}"), vec![0xff, i as u8])).collect();
    db.write(&keys).await.unwrap();
    let scanned = pages(&db, Op::scan("k/")).await;
    let entries: Vec<_> = scanned.into_iter().flat_map(|r| r.scan_page().unwrap().entries).collect();
    assert_eq!(entries.len(), 1500);
    assert_eq!(entries[1].value.data().unwrap().as_ref(), [0xff, 1], "binary values round trip");
    assert_eq!(db.read_one(Op::increment("n", 5)).await.unwrap(), OpResult::Increment(5));
}

#[tokio::test]
async fn a_transaction_reads_its_own_writes_and_commits_its_queued_writes() {
    let (db, _) = node("").await;
    let mut txn = db.begin();
    txn.run(&[Op::put("k", "v"), Op::insert(&item("a", 1))]).await.unwrap();
    let own = txn.run(&[Op::get("k"), Op::get_row::<Item>(("a", 1))]).await.unwrap();
    let OpResult::Get(Some(e)) = &own[0] else { panic!("{own:?}") };
    assert_eq!((e.version, e.value.data().map(|d| d.as_ref())), (None, Some(&b"v"[..])), "an own write has no version");
    txn.write([Op::append("log", &["one"])]);
    let done = txn.commit().await.unwrap().unwrap();
    assert_eq!(done.appends, [0]);
    let read = db.read(&[Op::get_row::<Item>(("a", 1)), Op::read_stream("log", 0)]).await.unwrap();
    assert_eq!(read[0].clone().row::<Item>().unwrap(), Some(item("a", 1)));
    assert_eq!(read[1].clone().records::<String>().unwrap(), ["one"]);
}

#[tokio::test]
async fn a_commit_whose_response_is_lost_resolves_as_committed() {
    let (db, router) = node(api::CommitTxn::ROUTE).await;
    let mut txn = db.begin();
    txn.run_one(Op::get("n")).await.unwrap();
    txn.write([Op::increment("n", 1)]);
    router.fail(api::CommitTxn::ROUTE, Fault::LoseResponse, 1);
    let done = txn.commit().await.unwrap().unwrap();
    let n = db.read_one(Op::get("n")).await.unwrap().entry().unwrap().unwrap();
    assert_eq!((n.version, n.value.data().unwrap().as_ref()), (Some(done.version), &b"1"[..]));
}

#[tokio::test]
async fn a_commit_that_never_arrived_is_fenced_so_a_late_copy_cannot_apply() {
    let (db, router) = node(api::CommitTxn::ROUTE).await;
    let mut txn = db.begin();
    txn.run_one(Op::get("n")).await.unwrap();
    txn.write([Op::increment("n", 1)]);
    router.fail(api::CommitTxn::ROUTE, Fault::Drop, 1);
    let e = txn.commit().await.unwrap_err();
    assert!(matches!(&e, Error::NotApplied(_)) && e.outcome() == Outcome::NotApplied, "{e:?}");
    let late = router.dropped.lock().pop().unwrap();
    let resp = Router::call(router.app.clone(), late).await;
    assert_eq!(resp.status(), 409, "{:?}", resp.body());
    assert_eq!(db.read_one(Op::get("n")).await.unwrap().entry().unwrap(), None, "the fenced commit never applies");
}

#[tokio::test]
async fn a_write_applies_once_when_every_response_is_lost() {
    let (db, router) = node(api::Txn::ROUTE).await;
    router.fail(api::Txn::ROUTE, Fault::LoseResponse, 4);
    let done = db.write(&[Op::increment("n", 1), Op::append("log", &["x"])]).await.unwrap().unwrap();
    assert_eq!(router.fault.lock().1, 0, "every attempt lost its response");
    let read = db.read(&[Op::get("n"), Op::read_stream("log", 0)]).await.unwrap();
    let n = read[0].clone().entry().unwrap().unwrap();
    assert_eq!((n.version, n.value.data().unwrap().as_ref()), (Some(done.version), &b"1"[..]));
    assert_eq!(read[1].clone().records::<String>().unwrap(), ["x"]);

    router.fail(api::Txn::ROUTE, Fault::Drop, 4);
    let e = db.write(&[Op::increment("n", 1)]).await.unwrap_err();
    assert!(matches!(e, Error::NotApplied(_)), "{e:?}");
    let n = db.read_one(Op::get("n")).await.unwrap().entry().unwrap().unwrap();
    assert_eq!(n.value.data().unwrap().as_ref(), b"1", "a write that never arrived is not applied");
}

#[tokio::test]
async fn concurrent_read_modify_writes_serialize_through_retries() {
    let (db, router) = node("").await;
    db.write(&[Op::insert(&Item { label: Some("0".into()), ..item("c", 0) })]).await.unwrap();
    let tasks: Vec<_> = (0..16)
        .map(|_| {
            let db = db.clone();
            tokio::spawn(async move {
                let mut retry = db.retry();
                loop {
                    let mut txn = db.begin();
                    let r = async {
                        let c = txn.run_one(Op::get_row::<Item>(("c", 0))).await?.row::<Item>()?.unwrap();
                        let n: u32 = c.label.as_deref().unwrap().parse().unwrap();
                        txn.write([Op::upsert(&Item { label: Some((n + 1).to_string()), ..c })]);
                        Ok::<_, Error>(())
                    }
                    .await;
                    if retry.finish(txn, r).await.unwrap().is_some() {
                        return;
                    }
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    let c = db.read_one(Op::get_row::<Item>(("c", 0))).await.unwrap().row::<Item>().unwrap().unwrap();
    assert_eq!(c.label.as_deref(), Some("16"), "every increment applied exactly once");
    assert!(router.sent.load(Ordering::Relaxed) >= 16 * 2);
}

#[tokio::test]
async fn a_dropped_transaction_is_aborted_and_frees_its_open_transaction_slot() {
    let (db, router) = node_with("", |c| c.txns_per_user = 2).await;
    let client = db.client().with_retry_policy(baserunna_client::RetryPolicy { attempts: 1, ..Default::default() });
    let db = client.db(db.name());
    for i in 0..5 {
        let mut txn = db.begin();
        let begun = txn.run_one(Op::get("k")).await;
        assert!(begun.is_ok(), "transaction {i} begins after earlier ones were dropped: {begun:?}");
        let sent = router.sent.load(Ordering::Relaxed);
        drop(txn);
        let aborted = async {
            while router.sent.load(Ordering::Relaxed) == sent {
                tokio::task::yield_now().await;
            }
        };
        let _ = tokio::time::timeout(std::time::Duration::from_millis(500), aborted).await;
    }
}

#[tokio::test]
async fn a_cancelled_request_keeps_its_transaction_from_committing() {
    let (db, router) = node(api::TxnOps::ROUTE).await;
    let mut txn = db.begin();
    txn.run_one(Op::get("k")).await.unwrap();
    router.fail(api::TxnOps::ROUTE, Fault::Hang, 1);
    let cancelled = tokio::time::timeout(Duration::from_millis(200), txn.run(&[Op::put("k", "cancelled")])).await;
    assert!(cancelled.is_err(), "the request was cancelled");
    let committed = txn.commit().await;
    assert!(committed.is_err(), "a transaction with a cancelled request commits: {committed:?}");
    assert_eq!(db.read_one(Op::get("k")).await.unwrap().entry().unwrap(), None);
}

#[tokio::test]
async fn a_write_without_write_operations_checks_its_preconditions_and_commits_nothing() {
    let (db, _) = node("").await;
    assert_eq!(db.write(&[]).await.unwrap(), None);
    let absent = Op::Check { key: "k".into(), expect: Expect::Absent };
    assert_eq!(db.write(std::slice::from_ref(&absent)).await.unwrap(), None);
    db.write(&[Op::put("k", "v")]).await.unwrap();
    let e = db.write(&[absent]).await.unwrap_err();
    assert_eq!(e.code(), Some(api::ErrorCode::PreconditionFailed), "{e}");
}

#[tokio::test]
async fn a_write_that_changes_nothing_commits_nothing() {
    let (db, _) = node("").await;
    let set = serde_json::json!({"label": "x"});
    for op in [
        Op::delete_rows::<Item>("nobody"),
        Op::update::<Item>(("nobody", 1), &set),
        Op::delete_row::<Item>(("nobody", 1)),
    ] {
        let r = db.write(std::slice::from_ref(&op)).await;
        assert_eq!(r, Ok(None), "{op:?}");
        let mut txn = db.begin();
        txn.write([op]);
        assert_eq!(txn.commit().await, Ok(None));
    }
}

#[tokio::test]
async fn a_begin_whose_caller_stops_waiting_is_aborted() {
    let (db, router) = node_with(api::BeginTxn::ROUTE, |c| c.txns_per_user = 1).await;
    let client = db.client().with_retry_policy(baserunna_client::RetryPolicy { attempts: 1, ..Default::default() });
    let db = client.db(db.name());
    router.fail(api::BeginTxn::ROUTE, Fault::Slow, 1);
    let mut txn = db.begin();
    let begun = tokio::time::timeout(Duration::from_millis(200), txn.run_one(Op::get("k"))).await;
    assert!(begun.is_err(), "the caller stopped waiting: {begun:?}");
    drop(txn);
    let sent = router.sent.load(Ordering::Relaxed);
    let aborted = async {
        while router.sent.load(Ordering::Relaxed) == sent {
            tokio::task::yield_now().await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), aborted).await.expect("the begun transaction is aborted");
    let mut txn = db.begin();
    let begun = txn.run_one(Op::get("k")).await;
    assert!(begun.is_ok(), "a new transaction begins: {begun:?}");
    txn.abort().await;
}

#[tokio::test]
async fn a_begin_retried_after_memory_pressure_eases_succeeds() {
    let (db, _) = node_with("", |c| c.engine.memory_bytes = 16 << 20).await;
    let big = |tag: &str| -> Vec<Op> { (0..120).map(|i| Op::put(format!("{tag}{i}"), vec![b'v'; 20_000])).collect() };
    let once = db.client().with_retry_policy(baserunna_client::RetryPolicy { attempts: 1, ..Default::default() });
    let mut fillers = Vec::new();
    loop {
        let mut t = once.db(db.name()).begin();
        if t.run(&big(&format!("f{}-", fillers.len()))).await.is_err() {
            break;
        }
        fillers.push(t);
    }
    let freer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        for t in fillers {
            t.abort().await;
        }
    });
    let mut txn = db.begin();
    let r = txn.run(&big("late-")).await;
    freer.await.unwrap();
    assert!(r.is_ok(), "a begin retried after its Retry-After, once memory was freed: {r:?}");
    txn.abort().await;
}

#[tokio::test]
async fn a_begun_transaction_that_writes_nothing_commits_nothing() {
    let (db, _) = node("").await;
    for writes in [vec![], vec![Op::delete_row::<Item>(("nobody", 1))]] {
        let mut txn = db.begin();
        txn.run_one(Op::get("k")).await.unwrap();
        txn.write(writes);
        assert_eq!(txn.commit().await, Ok(None), "like Db::write and a transaction that never began");
    }
}

#[tokio::test]
async fn a_table_with_a_column_type_the_server_does_not_know_is_rejected() {
    let (db, _) = node("").await;
    let mut t = table();
    t.name = "later".into();
    t.columns[0].ty = api::ColumnType::Unknown;
    let e = db.create_table(&t).await.unwrap_err();
    assert_eq!(e.code(), Some(api::ErrorCode::InvalidArgument), "{e}");
}

#[tokio::test]
async fn a_begin_whose_caller_leaves_after_the_response_arrives_is_aborted() {
    use std::future::Future;
    use std::task::Poll;
    let (db, _) = node_with("", |c| c.txns_per_user = 1).await;
    let client = db.client().with_retry_policy(baserunna_client::RetryPolicy { attempts: 1, ..Default::default() });
    let db = client.db(db.name());
    let mut txn = db.begin();
    {
        let mut run = std::pin::pin!(txn.run_one(Op::get("k")));
        let pending = std::future::poll_fn(|cx| Poll::Ready(run.as_mut().poll(cx).is_pending())).await;
        assert!(pending, "the begin is in flight");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    drop(txn);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut txn = db.begin();
    let begun = txn.run_one(Op::get("k")).await;
    assert!(begun.is_ok(), "a new transaction begins: {begun:?}");
    txn.abort().await;
}

#[tokio::test]
async fn a_cancelled_abort_still_frees_the_open_transaction_slot() {
    let (db, router) = node_with(api::AbortTxn::ROUTE, |c| c.txns_per_user = 1).await;
    let mut txn = db.begin();
    txn.run_one(Op::get("k")).await.unwrap();
    router.fail(api::AbortTxn::ROUTE, Fault::Drop, 1);
    let r = tokio::time::timeout(Duration::from_millis(0), txn.abort()).await;
    assert!(r.is_err(), "the abort was cancelled");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut txn = db.begin();
    let begun = txn.run_one(Op::get("k")).await;
    assert!(begun.is_ok(), "a new transaction begins: {begun:?}");
    txn.abort().await;
}

#[tokio::test]
async fn a_prefix_of_none_matches_only_rows_whose_key_is_null() {
    let (db, _) = node("").await;
    db.write(&[Op::insert(&item("a", 1)), Op::insert(&item("b", 2))]).await.unwrap();
    let owner: Option<&str> = None;
    let none = db.read_one(Op::query::<Item>(owner)).await.unwrap().rows::<Item>().unwrap();
    assert!(none.is_empty(), "a query for a missing owner read {none:?}");
    db.write(&[Op::delete_rows::<Item>(owner)]).await.unwrap();
    let left = db.read_one(Op::query::<Item>("a")).await.unwrap().rows::<Item>().unwrap();
    assert_eq!(left.len(), 1, "deleting the rows of a missing owner deleted other rows");
}

#[tokio::test]
async fn an_empty_prefix_selects_no_rows() {
    let (db, _) = node("").await;
    db.write(&[Op::insert(&item("a", 1)), Op::insert(&item("b", 2))]).await.unwrap();
    let prefix: Vec<String> = Vec::new();
    let read = db.read_one(Op::query::<Item>(&prefix)).await.unwrap().rows::<Item>().unwrap();
    db.write(&[Op::delete_rows::<Item>(&prefix)]).await.unwrap();
    let left = db.read_one(Op::query_all::<Item>()).await.unwrap().rows::<Item>().unwrap();
    assert_eq!((read.len(), left.len()), (0, 2), "an empty prefix read {read:?} and left {left:?}");
}
