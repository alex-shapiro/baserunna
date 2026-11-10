//! End-to-end tests for `baserunna-client` against an in-process node.
//! Covers dropped requests, lost responses, slow responses, and concurrent transactions.

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

/// Fault injected by [`Router`] on requests to `fail_route`.
#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    /// Drops the request before it reaches the server and returns an unknown transport error.
    Drop,
    /// Executes the request on the server and drops the response.
    LoseResponse,
    /// Executes the request on the server and hangs without returning.
    Hang,
    /// Executes the request on the server and delays the response by 500 ms.
    Slow,
}

/// In-process transport that injects faults on a target route.
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

/// Returns a handle to `db` that sends each request at most once without retrying.
fn without_retries(db: &Db) -> Db {
    db.client().with_retry_policy(baserunna_client::RetryPolicy { attempts: 1, ..Default::default() }).db(db.name())
}

/// Waits up to `timeout` for `router` to receive a request beyond `sent`.
async fn wait_for_next_request(router: &Router, sent: usize, timeout: Duration) -> bool {
    let next = async {
        while router.sent.load(Ordering::Relaxed) == sent {
            tokio::task::yield_now().await;
        }
    };
    tokio::time::timeout(timeout, next).await.is_ok()
}

/// Reads every page of `op` using one-shot reads.
async fn read_all_pages(db: &Db, mut op: Op) -> Vec<OpResult> {
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
async fn tables_rows_streams_and_keys_are_written_and_read_back() {
    let (db, _) = node("").await;
    let again = db.create_table(&table()).await.unwrap();
    assert_eq!(again.table.primary_key, ["owner", "id"], "creating an existing table returns its schema");
    let page = db.list_tables(api::ListTablesQuery::default()).await.unwrap();
    assert_eq!(page.tables.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(), ["items"]);
    assert_eq!(db.get_table("items").await.unwrap(), again.table);

    let rows: Vec<Op> = (0..2500).map(|i| Op::insert(&item(if i % 2 == 0 { "a" } else { "b" }, i))).collect();
    db.write(&rows).await.unwrap();
    let a: Vec<Item> =
        read_all_pages(&db, Op::query::<Item>("a")).await.into_iter().flat_map(|r| r.rows::<Item>().unwrap()).collect();
    assert_eq!(a.len(), 1250, "a prefix query returns every matching row across pages");
    assert!(a.iter().all(|i| i.owner == "a") && a.windows(2).all(|w| w[0].id < w[1].id));
    let all = read_all_pages(&db, Op::query_all::<Item>()).await;
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
        read_all_pages(&db, Op::read_stream("log", 0)).await.into_iter().flat_map(|r| r.records().unwrap()).collect();
    assert_eq!(read, events);

    let keys: Vec<Op> = (0..1500).map(|i| Op::put(format!("k/{i:04}"), vec![0xff, i as u8])).collect();
    db.write(&keys).await.unwrap();
    let scanned = read_all_pages(&db, Op::scan("k/")).await;
    let entries: Vec<_> = scanned.into_iter().flat_map(|r| r.scan_page().unwrap().entries).collect();
    assert_eq!(entries.len(), 1500);
    assert_eq!(entries[1].value.data().unwrap().as_ref(), [0xff, 1], "binary values are preserved");
    assert_eq!(db.read_one(Op::increment("n", 5)).await.unwrap(), OpResult::Increment(5));
}

#[tokio::test]
async fn a_transaction_reads_its_own_writes_and_commits_its_queued_writes() {
    let (db, _) = node("").await;
    let mut txn = db.begin();
    txn.run(&[Op::put("k", "v"), Op::insert(&item("a", 1))]).await.unwrap();
    let own = txn.run(&[Op::get("k"), Op::get_row::<Item>(("a", 1))]).await.unwrap();
    let OpResult::Get(Some(e)) = &own[0] else { panic!("{own:?}") };
    assert_eq!(
        (e.version, e.value.data().map(|d| d.as_ref())),
        (None, Some(&b"v"[..])),
        "uncommitted write does not have a version"
    );
    txn.write([Op::append("log", &["one"])]);
    let done = txn.commit().await.unwrap().unwrap();
    assert_eq!(done.appends, [0]);
    let read = db.read(&[Op::get_row::<Item>(("a", 1)), Op::read_stream("log", 0)]).await.unwrap();
    assert_eq!(read[0].clone().row::<Item>().unwrap(), Some(item("a", 1)));
    assert_eq!(read[1].clone().records::<String>().unwrap(), ["one"]);
}

#[tokio::test]
async fn a_commit_whose_response_is_lost_is_resolved_as_committed() {
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
async fn a_commit_that_never_arrived_is_fenced_so_a_late_copy_is_rejected() {
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
    assert_eq!(db.read_one(Op::get("n")).await.unwrap().entry().unwrap(), None, "the fenced commit is rejected");
}

#[tokio::test]
async fn a_write_is_applied_once_if_every_response_is_lost() {
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
async fn concurrent_read_modify_writes_are_serialized_through_retries() {
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
    assert_eq!(c.label.as_deref(), Some("16"), "every increment was applied exactly once");
    assert!(router.sent.load(Ordering::Relaxed) >= 16 * 2);
}

#[tokio::test]
async fn a_dropped_transaction_is_aborted_and_frees_its_open_transaction_slot() {
    let (db, router) = node_with("", |c| c.txns_per_user = 2).await;
    let db = without_retries(&db);
    for i in 0..5 {
        let mut txn = db.begin();
        let begun = txn.run_one(Op::get("k")).await;
        assert!(begun.is_ok(), "transaction {i} begins after earlier ones were dropped: {begun:?}");
        let sent = router.sent.load(Ordering::Relaxed);
        drop(txn);
        wait_for_next_request(&router, sent, Duration::from_millis(500)).await;
    }
}

#[tokio::test]
async fn a_canceled_request_keeps_its_transaction_from_committing() {
    let (db, router) = node(api::TxnOps::ROUTE).await;
    let mut txn = db.begin();
    txn.run_one(Op::get("k")).await.unwrap();
    router.fail(api::TxnOps::ROUTE, Fault::Hang, 1);
    let canceled = tokio::time::timeout(Duration::from_millis(200), txn.run(&[Op::put("k", "canceled")])).await;
    assert!(canceled.is_err(), "the request was canceled");
    let committed = txn.commit().await;
    assert!(committed.is_err(), "a transaction with a canceled request commits: {committed:?}");
    assert_eq!(db.read_one(Op::get("k")).await.unwrap().entry().unwrap(), None);
}

#[tokio::test]
async fn a_write_with_only_checks_validates_preconditions_without_committing() {
    let (db, _) = node("").await;
    assert_eq!(db.write(&[]).await.unwrap(), None);
    let absent = Op::Check { key: "k".into(), expect: Expect::Absent };
    assert_eq!(db.write(std::slice::from_ref(&absent)).await.unwrap(), None);
    db.write(&[Op::put("k", "v")]).await.unwrap();
    let e = db.write(&[absent]).await.unwrap_err();
    assert_eq!(e.code(), Some(api::ErrorCode::PreconditionFailed), "{e}");
}

#[tokio::test]
async fn a_write_that_does_not_modify_rows_does_not_commit() {
    let (db, _) = node("").await;
    let set = serde_json::json!({"label": "x"});
    for op in [
        Op::delete_rows::<Item>("nobody"),
        Op::update::<Item>(("nobody", 1), &set),
        Op::delete_row::<Item>(("nobody", 1)),
    ] {
        assert_eq!(db.write(std::slice::from_ref(&op)).await, Ok(None), "{op:?}");
        for begun in [false, true] {
            let mut txn = db.begin();
            if begun {
                txn.run_one(Op::get("k")).await.unwrap();
            }
            txn.write([op.clone()]);
            assert_eq!(txn.commit().await, Ok(None), "{op:?} in a transaction that began: {begun}");
        }
    }
    let mut txn = db.begin();
    txn.run_one(Op::get("k")).await.unwrap();
    assert_eq!(txn.commit().await, Ok(None), "a begun transaction without writes");
}

#[tokio::test]
async fn a_begin_whose_caller_stops_waiting_is_aborted() {
    use std::future::Future;
    use std::task::Poll;
    // Test dropping the future both before the response arrives (`slow = true`) and after the
    // response arrives (`slow = false`).
    for slow in [true, false] {
        let (db, router) = node_with(api::BeginTxn::ROUTE, |c| c.txns_per_user = 1).await;
        let db = without_retries(&db);
        if slow {
            router.fail(api::BeginTxn::ROUTE, Fault::Slow, 1);
        }
        let mut txn = db.begin();
        {
            let mut run = std::pin::pin!(txn.run_one(Op::get("k")));
            let pending = std::future::poll_fn(|cx| Poll::Ready(run.as_mut().poll(cx).is_pending())).await;
            assert!(pending, "the begin is in flight");
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        let sent = router.sent.load(Ordering::Relaxed);
        drop(txn);
        assert!(wait_for_next_request(&router, sent, Duration::from_secs(5)).await, "the begun transaction is aborted");
        let mut txn = db.begin();
        let begun = txn.run_one(Op::get("k")).await;
        assert!(begun.is_ok(), "a new transaction begins after a slow begin ({slow}): {begun:?}");
        txn.abort().await;
    }
}

#[tokio::test]
async fn a_begin_retried_after_memory_is_freed_succeeds() {
    let (db, _) = node_with("", |c| c.engine.memory_bytes = 16 << 20).await;
    let big = |tag: &str| -> Vec<Op> { (0..120).map(|i| Op::put(format!("{tag}{i}"), vec![b'v'; 20_000])).collect() };
    let mut fillers = Vec::new();
    loop {
        let mut t = without_retries(&db).begin();
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
async fn a_table_with_an_unknown_column_type_is_rejected_even_if_it_exists() {
    let (db, _) = node("").await;
    for name in ["later", &table().name] {
        let mut t = table();
        t.name = name.into();
        t.columns[0].ty = api::ColumnType::Unknown;
        let e = db.create_table(&t).await.unwrap_err();
        assert_eq!(e.code(), Some(api::ErrorCode::InvalidArgument), "{name}: {e}");
    }
}

#[tokio::test]
async fn a_canceled_abort_still_frees_the_open_transaction_slot() {
    let (db, router) = node_with(api::AbortTxn::ROUTE, |c| c.txns_per_user = 1).await;
    let mut txn = db.begin();
    txn.run_one(Op::get("k")).await.unwrap();
    router.fail(api::AbortTxn::ROUTE, Fault::Drop, 1);
    let r = tokio::time::timeout(Duration::from_millis(0), txn.abort()).await;
    assert!(r.is_err(), "the abort was canceled");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut txn = db.begin();
    let begun = txn.run_one(Op::get("k")).await;
    assert!(begun.is_ok(), "a new transaction begins: {begun:?}");
    txn.abort().await;
}

#[tokio::test]
async fn an_empty_or_null_key_prefix_does_not_match_any_rows() {
    let (db, _) = node("").await;
    db.write(&[Op::insert(&item("a", 1)), Op::insert(&item("b", 2))]).await.unwrap();
    let (none, empty): (Option<&str>, Vec<String>) = (None, Vec::new());
    for (query, delete) in [
        (Op::query::<Item>(none), Op::delete_rows::<Item>(none)),
        (Op::query::<Item>(&empty), Op::delete_rows::<Item>(&empty)),
    ] {
        let read = db.read_one(query).await.unwrap().rows::<Item>().unwrap();
        db.write(&[delete]).await.unwrap();
        let left = db.read_one(Op::query_all::<Item>()).await.unwrap().rows::<Item>().unwrap();
        assert_eq!((read.len(), left.len()), (0, 2), "the prefix read {read:?} and left {left:?}");
    }
}

#[tokio::test]
async fn queued_writes_are_executed_before_later_operations() {
    let (db, _) = node("").await;
    let mut txn = db.begin();
    txn.write([Op::put("k", "first")]);
    let seen = txn.run_one(Op::get("k")).await.unwrap().entry().unwrap().map(|e| e.value);
    txn.run(&[Op::put("k", "second")]).await.unwrap();
    txn.commit().await.unwrap();
    let k = db.read_one(Op::get("k")).await.unwrap().entry().unwrap().unwrap();
    assert_eq!(seen.as_ref().and_then(|v| v.data()).map(|d| d.as_ref()), Some(&b"first"[..]));
    assert_eq!(k.value.data().unwrap().as_ref(), b"second");
}

#[tokio::test]
async fn reverse_stream_reads_bounded_by_time_return_every_record_across_pages() {
    let (db, _) = node("").await;
    let events: Vec<u64> = (0..5).collect();
    db.write(&[Op::append("log", &events)]).await.unwrap();
    let op = Op::ReadStream(api::ReadStream {
        stream: "log".into(),
        until_time: Some(u64::MAX / 2),
        reverse: true,
        limit: Some(2),
        ..api::ReadStream::default()
    });
    let read: Vec<u64> = read_all_pages(&db, op).await.into_iter().flat_map(|r| r.records::<u64>().unwrap()).collect();
    assert_eq!(read, [4, 3, 2, 1, 0]);
}

#[tokio::test]
async fn a_failed_operation_reports_its_index_in_the_batch() {
    let (db, _) = node("").await;
    db.write(&[Op::put("k", "v")]).await.unwrap();
    let mut txn = db.begin();
    txn.run_one(Op::get("k")).await.unwrap();
    txn.write([Op::put("x", "1")]);
    let e = txn.run(&[Op::get("x"), Op::Check { key: "k".into(), expect: Expect::Absent }]).await.unwrap_err();
    let Error::Api { error, .. } = &e else { panic!("{e:?}") };
    assert_eq!(error.details.as_ref().and_then(|d| d.op_index), Some(1), "{e}");
}

#[tokio::test]
async fn a_write_whose_first_attempt_was_dropped_reports_the_server_rejection() {
    let (db, router) = node(api::Txn::ROUTE).await;
    db.write(&[Op::insert(&item("a", 1))]).await.unwrap();
    router.fail(api::Txn::ROUTE, Fault::Drop, 1);
    let e = db.write(&[Op::insert(&item("a", 1))]).await.unwrap_err();
    assert_eq!(e.code(), Some(api::ErrorCode::AlreadyExists), "{e:?}");
    router.fail(api::Txn::ROUTE, Fault::Drop, 1);
    let mut retry = db.retry();
    let mut passes = 0;
    let r = loop {
        passes += 1;
        let txn = db.begin();
        let w = db.write(&[Op::insert(&item("a", 1))]).await;
        if let Some(r) = retry.finish(txn, w).await.transpose() {
            break r;
        }
    };
    assert_eq!(passes, 1, "a rejected write is not retried: {r:?}");
}

#[tokio::test]
async fn a_failed_check_whose_response_is_lost_reports_precondition_failed() {
    let (db, router) = node(api::Txn::ROUTE).await;
    db.write(&[Op::put("k", "v")]).await.unwrap();
    router.fail(api::Txn::ROUTE, Fault::LoseResponse, 1);
    let cas = [Op::Check { key: "k".into(), expect: Expect::Absent }, Op::put("k", "w")];
    let e = db.write(&cas).await.unwrap_err();
    assert_eq!(e.code(), Some(api::ErrorCode::PreconditionFailed), "{e}");
    let k = db.read_one(Op::get("k")).await.unwrap().entry().unwrap().unwrap();
    assert_eq!(k.value.data().unwrap().as_ref(), b"v", "the rejected write was not applied");
}

#[tokio::test]
async fn a_retried_read_reports_the_servers_answer() {
    let (db, router) = node(api::GetTable::ROUTE).await;
    router.fail(api::GetTable::ROUTE, Fault::LoseResponse, 1);
    let e = db.get_table("missing").await.unwrap_err();
    assert_eq!(e.code(), Some(api::ErrorCode::NotFound), "{e:?}");
}

#[tokio::test]
async fn raw_values_and_blobs_return_their_version_type_range_and_expiry() {
    let (db, _) = node("").await;
    let opts = |expect| api::PutOptions { content_type: Some("text/plain".into()), ttl_ms: Some(60_000), expect };
    let w = db.put_value("k", "hello world", opts(Some(Expect::Absent))).await.unwrap();
    let v = db.get_value("k", Some(api::ByteRange::Span { start: 0, end: 4 })).await.unwrap().unwrap();
    assert_eq!((v.content.data.as_ref(), v.version), (&b"hello"[..], w.version));
    assert_eq!(v.content.content_range.as_deref(), Some("bytes 0-4/11"));
    assert!(v.expires_at.is_some(), "{v:?}");
    let e = db.put_value("k", "again", opts(Some(Expect::Absent))).await.unwrap_err();
    assert_eq!(e.code(), Some(api::ErrorCode::PreconditionFailed), "{e}");
    db.put_value("k", "again", opts(Some(Expect::Version(w.version)))).await.unwrap();
    assert_eq!(db.get_value("missing", None).await.unwrap(), None);
    let e = db.client().db("nonexistent").get_value("k", None).await.unwrap_err();
    assert_eq!(e.code(), Some(api::ErrorCode::NotFound), "a missing database is not a missing key: {e}");

    let blob = db.stage_blob(Bytes::from_static(b"\x89PNG-data"), "image/png").await.unwrap();
    let tail = db.read_blob(&blob.blob_id, Some(api::ByteRange::Last(4))).await.unwrap();
    assert_eq!((tail.data.as_ref(), tail.content_type.as_deref()), (&b"data"[..], Some("image/png")));
}

#[tokio::test]
async fn stream_endpoints_append_wait_read_and_query_records() {
    let (db, _) = node("").await;
    let stream = api::NewStream { name: "log".into(), retention_ms: Some(60_000), max_bytes: None };
    assert_eq!(db.create_stream(&stream).await.unwrap().stream.retention_ms, Some(60_000));
    let record = |text: &str, from: &str| api::NewRecord {
        json: Some(serde_json::json!({"text": text})),
        headers: [("from".to_string(), from.to_string())].into(),
        ..api::NewRecord::default()
    };
    let append = api::AppendRequest { records: vec![record("a", "ann")], expect_seq: Some(0), ..Default::default() };
    assert_eq!(db.append_records("log", &append).await.unwrap().next_seq, 1);
    let e = db.append_records("log", &append).await.unwrap_err();
    assert_eq!(e.code(), Some(api::ErrorCode::PreconditionFailed), "{e}");
    let lines = db.append_lines("log", Bytes::from_static(b"{\"text\":\"b\"}\n{\"text\":\"c\"}\n")).await.unwrap();
    assert_eq!((lines.first_seq, lines.count), (1, 2));

    let waiter = {
        let db = db.clone();
        let query = api::ReadRecordsQuery { from_seq: Some(3), wait_ms: Some(10_000), ..Default::default() };
        tokio::spawn(async move { db.read_records("log", query).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    db.append_records("log", &api::AppendRequest { records: vec![record("d", "bob")], ..Default::default() })
        .await
        .unwrap();
    let woken = waiter.await.unwrap().unwrap();
    assert_eq!(woken.records.iter().map(|r| r.seq).collect::<Vec<_>>(), [3]);

    let last = api::ReadRecordsQuery { tail: Some(2), reverse: true, ..Default::default() };
    let page = db.read_records("log", last).await.unwrap();
    assert_eq!(page.records.iter().map(|r| r.seq).collect::<Vec<_>>(), [3, 2]);
    let filter = Some(serde_json::json!({"header": "from", "op": "eq", "value": "ann"}));
    let anns = db.query_records("log", &api::RecordQuery { filter, ..Default::default() }).await.unwrap();
    assert_eq!(anns.records.iter().map(|r| r.seq).collect::<Vec<_>>(), [0]);

    db.write(&[Op::set_consumer("log", "reader", 2), Op::trim_stream("log", 1)]).await.unwrap();
    let info = db.stream_info("log").await.unwrap();
    assert_eq!((info.stream.state.first_seq, info.consumers[0].lag), (1, 2), "{info:?}");
    let listed = db.list_streams(api::ListStreamsQuery { prefix: Some("lo".into()), ..Default::default() }).await;
    assert_eq!(listed.unwrap().streams.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(), ["log"]);
}

#[tokio::test]
async fn databases_api_keys_and_schema_changes_are_applied_and_read_back() {
    let (db, _) = node("").await;
    let info = db.info().await.unwrap();
    assert_eq!((info.name.as_str(), info.role), ("app", api::Role::Owner));
    let dbs = db.client().list_dbs(api::ListDbsQuery::default()).await.unwrap();
    assert_eq!(dbs.databases.iter().map(|d| d.name.as_str()).collect::<Vec<_>>(), ["app"]);

    let new_key = api::NewApiKey { name: "reader".into(), db: Some("app".into()), read_only: true, rate_limit: None };
    let created = db.client().create_api_key(&new_key).await.unwrap();
    let reader = db.client().with_token(&created.key).db("app");
    reader.read_one(Op::get("k")).await.unwrap();
    let e = reader.write(&[Op::put("k", "v")]).await.unwrap_err();
    assert_eq!(e.code(), Some(api::ErrorCode::PermissionDenied), "{e}");
    let keys = db.client().list_api_keys(api::PageQuery::default()).await.unwrap();
    assert_eq!(keys.api_keys, std::slice::from_ref(&created.api_key));
    db.client().delete_api_key(&created.api_key.id).await.unwrap();
    assert!(reader.read_one(Op::get("k")).await.is_err(), "a deleted key is rejected");

    db.write(&[Op::insert(&item("a", 1)), Op::insert(&item("a", 2))]).await.unwrap();
    let label = NewColumn { default: Some(serde_json::json!("new")), ..NewColumn::nullable("tag", ColumnType::String) };
    db.add_column(Item::TABLE, &label).await.unwrap();
    let index = api::NewIndex { name: "by_id".into(), columns: vec!["id".into()], unique: false };
    assert!(db.add_index(Item::TABLE, &index).await.unwrap().table.indexes.iter().any(|i| i.name == "by_id"));
    let high = api::Selection {
        index: Some("by_id".into()),
        range: Some(api::RangeSpec { gte: Some(serde_json::json!(2)), ..Default::default() }),
        ..Default::default()
    };
    let out = db.txn(&[Op::update_where::<Item>(high.clone(), &serde_json::json!({"label": "x"}))], Some("k1")).await;
    assert_eq!(out.unwrap().results.unwrap(), [OpResult::Count(1)]);
    let rows = db.read_one(Op::query_where::<Item>(high)).await.unwrap().rows::<Item>().unwrap();
    assert_eq!(rows, [Item { label: Some("x".into()), ..item("a", 2) }]);
    db.delete().await.unwrap();
    assert!(db.info().await.is_err(), "the database is gone");
}

#[tokio::test]
async fn a_raw_put_is_applied_once_if_every_response_is_lost() {
    let (db, router) = node(api::PutValue::ROUTE).await;
    let expect = |e| api::PutOptions { expect: Some(e), ..Default::default() };
    let first = db.put_value("k", "1", expect(Expect::Absent)).await.unwrap();
    router.fail(api::PutValue::ROUTE, Fault::LoseResponse, 4);
    let second = db.put_value("k", "2", expect(Expect::Version(first.version))).await.unwrap();
    assert_eq!(router.fault.lock().1, 0, "every attempt lost its response");
    let v = db.get_value("k", None).await.unwrap().unwrap();
    assert_eq!((v.content.data.as_ref(), v.version), (&b"2"[..], second.version), "a retry that reapplied would fail");
}
