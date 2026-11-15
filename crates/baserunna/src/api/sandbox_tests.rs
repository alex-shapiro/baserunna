use crate::sandbox::{NodeSandbox, OWNER_MEMORY_BOUND, OwnerResolver, Sandbox};
use crate::sim::cost::{World, call_router, run_paused};
use bytes::Bytes;
use serde_json::{Value as Json, json};
use std::sync::Arc;

/// Sets a sandbox with `bound` bytes per owner on node 0 of `w`.
fn set_sandbox(w: &World, bound: usize) -> Arc<NodeSandbox> {
    let sandbox = Sandbox::new(&crate::sandbox::tests::guest_wasm(), bound).unwrap();
    let owner_of: OwnerResolver = Arc::new(|_db: String| Box::pin(async { Ok(None) }));
    let runner = Arc::new(NodeSandbox { sandbox, shared: w.node(0).engine.shared.clone(), owner_of });
    w.node(0).engine.set_sandbox(runner.clone());
    runner
}

async fn txn(w: &World, token: &str, body: &Json) -> (u16, Json) {
    let router = w.node(0).router().unwrap();
    let r = call_router(&router, "POST", "/v1/dbs/d/txn", token, Bytes::from(body.to_string())).await;
    let mut out: Json = serde_json::from_slice(&r.body).unwrap_or_else(|_| json!(String::from_utf8_lossy(&r.body)));
    if let Some(o) = out.as_object_mut() {
        o.remove("commit_id");
    }
    (r.status, out)
}

async fn world_with_db(email: &str) -> (World, String) {
    let w = World::new(1, 0).await;
    let token = w.signup(0, email).await;
    w.create_db(0, &token, "d").await;
    (w, token)
}

#[test]
fn txn_ops_in_sandbox_match_native() {
    run_paused(async {
        let (native, native_token) = world_with_db("native@api.test").await;
        let (guest, guest_token) = world_with_db("guest@api.test").await;
        let runner = set_sandbox(&guest, OWNER_MEMORY_BOUND);
        let puts: Vec<Json> = (0..20).map(|i| json!({"op": "put", "key": format!("k{i:02}"), "value": format!("v{i}")})).collect();
        let txns = [
            json!({"ops": puts}),
            json!({"ops": [{"op": "scan", "prefix": "k", "limit": 100}, {"op": "get", "key": "k03"}, {"op": "get", "key": "none"}]}),
            json!({"ops": [{"op": "put", "key": "k05", "value": "changed"}, {"op": "get", "key": "k05"}, {"op": "delete", "key": "k06"}]}),
            json!({"ops": [{"op": "scan", "prefix": "k", "limit": 3, "reverse": true}, {"op": "get", "key": "k06"}]}),
            json!({"ops": [{"op": "check", "key": "k01", "value": "wrong"}, {"op": "put", "key": "k07", "value": "lost"}]}),
            json!({"ops": [{"op": "get", "key": "k07"}, {"op": "check", "key": "k01", "value": "v1"}]}),
            json!({"ops": [{"op": "bogus"}]}),
        ];
        for body in &txns {
            let want = txn(&native, &native_token, body).await;
            let got = txn(&guest, &guest_token, body).await;
            assert_eq!(got, want, "transaction {body}");
        }
        assert!(runner.sandbox.last_instance_peak() > 0, "the ops ran in an instance");
    });
}

#[test]
fn txn_scan_output_over_owner_bound_is_refused() {
    run_paused(async {
        let (w, token) = world_with_db("big@api.test").await;
        let value = "x".repeat(32 << 10);
        for first in (0..256).step_by(64) {
            let puts: Vec<Json> =
                (first..first + 64).map(|i| json!({"op": "put", "key": format!("s{i:04}"), "value": value})).collect();
            let (s, r) = txn(&w, &token, &json!({"ops": puts})).await;
            assert_eq!(s, 200, "{r}");
        }
        let runner = set_sandbox(&w, 128 << 20);
        let scan = json!({"op": "scan", "prefix": "s", "limit": 1000});
        let (s, r) = txn(&w, &token, &json!({"ops": [scan]})).await;
        eprintln!("one scan: status {s}, peak instance memory {} KiB", runner.sandbox.last_instance_peak() >> 10);
        assert_eq!(s, 200, "one 8 MiB scan fits: {r}");
        assert_eq!(r["results"][0]["items"].as_array().map(Vec::len), Some(256));
        let (s, r) = txn(&w, &token, &json!({"ops": vec![scan; 8]})).await;
        assert!(s == 413 || s == 503, "8 scans of 8 MiB exceed a 128 MiB bound: {s} {r}");
        runner.sandbox.evict_idle_instances();
        assert_eq!(runner.sandbox.all_owners_memory_used(), 0);
    });
}

#[test]
fn host_reads_of_a_sandboxed_database_fail() {
    run_paused(async {
        let (w, token) = world_with_db("deny@api.test").await;
        let (s, r) = txn(&w, &token, &json!({"ops": [{"op": "put", "key": "k", "value": "v"}]})).await;
        assert_eq!(s, 200, "{r}");
        let info = call_router(&w.node(0).router().unwrap(), "GET", "/v1/dbs/d", &token, Bytes::new()).await;
        let info: Json = serde_json::from_slice(&info.body).unwrap();
        let db_id = info["id"].as_str().expect("database id").to_string();
        let db = w.node(0).engine.db(&db_id);
        let read = || async {
            let mut t = db.begin(crate::engine::Isolation::Serializable, crate::engine::Consistency::Strong).await?;
            t.get(b"\x01k").await
        };
        assert!(read().await.is_ok(), "reads outside a request scope pass");
        let denied = crate::engine::host_reads::sandboxed_request(db_id.clone(), read()).await;
        assert!(matches!(denied, Err(crate::error::Error::Internal(_))), "{denied:?}");
        let other = crate::engine::host_reads::sandboxed_request("other".into(), read()).await;
        assert!(other.is_ok(), "the scope denies only its own database");
    });
}
