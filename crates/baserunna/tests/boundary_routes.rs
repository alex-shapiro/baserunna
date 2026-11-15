//! Route tests of the worker boundary: the node process runs behind a store that fails every
//! access to a tenant database, and its workers run in process over in-memory pipes.
//!
//! - `routes_run_with_the_guard_on`: each route succeeds, and the node process
//!   makes no access to tenant objects.
//! - `routes_go_through_the_boundary`: each route opens exactly one request stream to a worker.
//!
//! The tour covers the routes in [`tour`]; it is not generated from the router.

use baserunna::env::Env;
use baserunna::node::{Node, Services};
use baserunna::sim::cost::{call_router, node_config};
use baserunna::store::{DynStore, MemStore, TenantDenyingStore};
use baserunna::worker::WorkerPool;
use bytes::Bytes;
use serde_json::json;
use std::sync::Arc;

/// One request of the tour and what it did.
struct Visit {
    route: &'static str,
    status: u16,
    denied: u64,
    opened: u64,
}

/// Starts a node whose store denies tenant objects, with in-process workers on the plain store.
async fn guarded_node() -> (Arc<Node>, Arc<TenantDenyingStore>, Arc<WorkerPool>) {
    let mem: DynStore = Arc::new(MemStore::new(Box::new(|| {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
    })));
    let guard = Arc::new(TenantDenyingStore::new(mem.clone()));
    let cfg = node_config("n0", "http://n0.invalid");
    let services = || Services { transport: Arc::new(baserunna::cluster::HttpTransport::new()), oidc: None, mailer: None };
    let node = Node::start(cfg.clone(), Arc::new(Env::real()), guard.clone(), services()).await.expect("node starts");
    let id = node.id.clone();
    let pool = Arc::new(WorkerPool::in_process(Box::new(move || {
        Node::new(cfg.clone(), Arc::new(Env::real()), mem.clone(), services(), Some(id.clone())).expect("worker node")
    })));
    pool.set_node_id(node.id.clone());
    let (runner, weak) = (pool.clone(), Arc::downgrade(&node));
    let _ = node.meta_runner.set(Box::new(move |cmd| {
        let (runner, weak) = (runner.clone(), weak.clone());
        Box::pin(async move {
            let answer = runner.run_meta(cmd).await;
            if let Some(n) = weak.upgrade() {
                n.engine.forget(baserunna::meta::SYS_DB);
            }
            answer
        })
    }));
    node.engine.forget(baserunna::meta::SYS_DB);
    let _ = node.engine.shared.workers.set(pool.clone());
    (node, guard, pool)
}

/// Sends one request of each route in the tour and records what each one did.
async fn tour() -> Vec<Visit> {
    let (node, guard, pool) = guarded_node().await;
    let router = baserunna::api::router(node.clone());
    node.set_router(router.clone());
    let mut visits = Vec::new();
    let mut token = String::new();
    let steps: Vec<(&'static str, &str, String, serde_json::Value)> = vec![
        ("POST /v1/auth/signup", "POST", "/v1/auth/signup".into(), json!({"email": "a@x.io", "password": "password-123"})),
        ("POST /v1/dbs", "POST", "/v1/dbs".into(), json!({"name": "a"})),
        ("GET /v1/dbs/{db}", "GET", "/v1/dbs/a".into(), json!(null)),
        ("PUT /v1/dbs/{db}/kv/{key}", "PUT", "/v1/dbs/a/kv/k".into(), json!("v")),
        ("GET /v1/dbs/{db}/kv/{key}", "GET", "/v1/dbs/a/kv/k".into(), json!(null)),
        ("POST /v1/dbs/{db}/txn", "POST", "/v1/dbs/a/txn".into(), json!({"ops": [{"op": "get", "key": "k"}]})),
        ("DELETE /v1/dbs/{db}/kv/{key}", "DELETE", "/v1/dbs/a/kv/k".into(), json!(null)),
    ];
    for (route, method, path, body) in steps {
        let (denied, opened) = (guard.denied(), pool.opened_streams());
        let body = if body.is_null() { Bytes::new() } else { Bytes::from(body.to_string()) };
        let resp = call_router(&router, method, &path, &token, body).await;
        if route == "POST /v1/auth/signup" {
            token = resp.json()["token"].as_str().unwrap_or_default().to_string();
        }
        visits.push(Visit {
            route,
            status: resp.status,
            denied: guard.denied() - denied,
            opened: pool.opened_streams() - opened,
        });
    }
    visits
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routes_run_with_the_guard_on() {
    let bad: Vec<String> = tour()
        .await
        .into_iter()
        .filter(|v| v.status >= 400 || v.denied > 0)
        .map(|v| format!("{}: status {}, {} denied store accesses", v.route, v.status, v.denied))
        .collect();
    assert!(bad.is_empty(), "routes that failed with the guard on:\n{}", bad.join("\n"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routes_go_through_the_boundary() {
    let bad: Vec<String> = tour()
        .await
        .into_iter()
        .filter(|v| v.opened != 1)
        .map(|v| format!("{}: {} request streams to a worker", v.route, v.opened))
        .collect();
    assert!(bad.is_empty(), "routes that did not cross the boundary once:\n{}", bad.join("\n"));
}
