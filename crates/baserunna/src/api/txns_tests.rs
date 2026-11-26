//! Tests for interactive transactions through the HTTP API.

use crate::sim::cost::{Resp, World};
use baserunna_storage::engine::{Consistency, Isolation, Value};
use baserunna_util::error::Error;
use baserunna_util::run_paused;
use bytes::Bytes;
use serde_json::{Value as Json, json};
use std::time::Duration;

async fn begin(w: &World, token: &str, db: &str) -> Resp {
    w.call(0, "POST", &format!("/v1/dbs/{db}/transactions"), token, Bytes::from("{}")).await
}

async fn ops(w: &World, token: &str, db: &str, id: &str, ops: Json) -> Json {
    w.json(0, "POST", &format!("/v1/dbs/{db}/transactions/{id}/ops"), token, json!({"ops": ops})).await
}

fn get_op() -> Json {
    json!([{"op": "get", "key": "k0"}])
}

fn big_puts(tag: &str) -> Json {
    (0..120).map(|i| json!({"op": "put", "key": format!("{tag}{i}"), "value": "v".repeat(20_000)})).collect()
}

fn growth_slack(w: &World) -> usize {
    w.node(0).engine.shared.memory.limit() / 1024 + 32 * 1024
}

async fn fill_memory(w: &World, token: &str, db: &str) -> (Vec<String>, Option<String>) {
    let mut open = Vec::new();
    loop {
        let r = begin(w, token, db).await;
        if r.status != 201 {
            assert_eq!((r.status, &r.json()["error"]["retryable"]), (503, &json!(true)), "{:?}", r.json());
            return (open, None);
        }
        let id = r.json()["txn_id"].as_str().unwrap().to_string();
        let r = ops(w, token, db, &id, big_puts("k")).await;
        if !r["error"].is_null() {
            assert_eq!(
                (&r["error"]["code"], &r["error"]["retryable"]),
                (&json!("txn_not_found"), &json!(true)),
                "rejected ops request did not return txn_not_found: {r}"
            );
            assert!(r["error"]["message"].as_str().unwrap().contains("memory budget"), "{r}");
            return (open, Some(id));
        }
        open.push(id);
        assert!(open.len() < 20, "transactions were never rejected");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[test]
fn interactive_transactions_are_capped_per_user() {
    run_paused(async {
        let w = World::with_config(1, 1, |c| c.txns_per_user = 3).await;
        let (alice, bob) = (w.signup(0, "alice@x.io").await, w.signup(0, "bob@x.io").await);
        w.create_db(0, &alice, "a").await;
        w.create_db(0, &bob, "b").await;
        let mut ids = Vec::new();
        for _ in 0..3 {
            let r = begin(&w, &alice, "a").await;
            assert_eq!(r.status, 201);
            ids.push(r.json()["txn_id"].as_str().unwrap().to_string());
        }
        let r = begin(&w, &alice, "a").await;
        assert_eq!((r.status, r.json()["error"]["code"].as_str()), (429, Some("rate_limited")), "{:?}", r.json());
        assert_eq!(begin(&w, &bob, "b").await.status, 201, "user quota affected another user");
        let r = w.call(0, "DELETE", &format!("/v1/dbs/a/transactions/{}", ids[0]), &alice, Bytes::new()).await;
        assert_eq!(r.status, 200);
        assert_eq!(begin(&w, &alice, "a").await.status, 201, "an aborted transaction still counted");
    });
}

#[test]
fn retried_interactive_commit_returns_unknown_outcome() {
    run_paused(async {
        let world = World::new(1, 1).await;
        let token = world.signup(0, "a@x.io").await;
        world.create_db(0, &token, "a").await;
        let begun = begin(&world, &token, "a").await.json();
        let (txn_id, commit_id) = (begun["txn_id"].as_str().unwrap(), begun["commit_id"].as_str().unwrap());
        ops(&world, &token, "a", txn_id, json!([{"op": "put", "key": "k", "value": "v"}])).await;
        let commit = format!("/v1/dbs/a/transactions/{txn_id}/commit");
        assert_eq!(world.call(0, "POST", &commit, &token, Bytes::from("{}")).await.status, 200);
        // A duplicate commit request after a lost response receives 503 with an unknown outcome.
        let again = world.call(0, "POST", &commit, &token, Bytes::from("{}")).await.json();
        assert_eq!(again["error"]["code"], "unavailable", "{again}");
        assert_eq!(again["error"]["details"]["outcome"], "unknown", "{again}");
        assert!(again["error"]["message"].as_str().unwrap().contains(commit_id), "{again}");
    });
}

#[test]
fn failed_interactive_commit_returns_txn_not_found_on_retry() {
    run_paused(async {
        let world = World::new(1, 1).await;
        let token = world.signup(0, "a@x.io").await;
        world.create_db(0, &token, "a").await;
        let txn_id = begin(&world, &token, "a").await.json()["txn_id"].as_str().unwrap().to_string();
        let commit = format!("/v1/dbs/a/transactions/{txn_id}/commit");
        let bad = Bytes::from(r#"{"ops": [{"op": "check", "key": "missing", "exists": true}]}"#);
        assert_eq!(world.call(0, "POST", &commit, &token, bad).await.status, 412);
        let again = world.call(0, "POST", &commit, &token, Bytes::from("{}")).await.json();
        assert_eq!(again["error"]["code"], "txn_not_found", "{again}");
    });
}

#[test]
fn expired_transaction_returns_abort_reason() {
    run_paused(async {
        let w = World::new(1, 1).await;
        let token = w.signup(0, "a@x.io").await;
        w.create_db(0, &token, "a").await;
        let id = begin(&w, &token, "a").await.json()["txn_id"].as_str().unwrap().to_string();
        tokio::time::sleep(Duration::from_millis(w.node(0).cfg.txn_idle_ms)).await;
        w.node(0).txns.expire();
        let r = ops(&w, &token, "a", &id, get_op()).await;
        assert_eq!(r["error"]["code"], "txn_not_found", "{r}");
        assert!(r["error"]["message"].as_str().unwrap().contains("idle timeout"), "{r}");
    });
}

async fn commit(w: &World, token: &str, db: &str, id: &str) -> Resp {
    w.call(0, "POST", &format!("/v1/dbs/{db}/transactions/{id}/commit"), token, Bytes::from("{}")).await
}

#[test]
fn cancelled_ops_request_aborts_transaction() {
    run_paused(async {
        let w = World::new(1, 1).await;
        let token = w.signup(0, "a@x.io").await;
        w.create_db(0, &token, "a").await;
        let big = "b".repeat(w.node(0).cfg.engine.value_inline_max + 1);
        let r = w
            .json(0, "POST", "/v1/dbs/a/txn", &token, json!({"ops": [{"op": "put", "key": "blob", "value": big}]}))
            .await;
        assert!(r["error"].is_null(), "{r}");
        let id = begin(&w, &token, "a").await.json()["txn_id"].as_str().unwrap().to_string();
        w.store.set_latency_ms(1_000);
        let request =
            ops(&w, &token, "a", &id, json!([{"op": "put", "key": "x", "value": "1"}, {"op": "get", "key": "blob"}]));
        let cancelled = tokio::time::timeout(Duration::from_millis(100), request).await;
        assert!(cancelled.is_err(), "the request finished before it was cancelled: {cancelled:?}");
        w.store.set_latency_ms(1);
        let r = ops(&w, &token, "a", &id, json!([{"op": "get", "key": "x"}])).await;
        assert_eq!(r["error"]["code"], "txn_not_found", "a partly applied request left its transaction open: {r}");
        assert!(r["error"]["message"].as_str().unwrap().contains("cancelled"), "{r}");
    });
}

#[test]
fn transactions_are_bounded_by_memory_budget_and_can_finish() {
    run_paused(async {
        let w = World::with_config(1, 1, |c| c.engine.memory_bytes = 16 << 20).await;
        let token = w.signup(0, "a@x.io").await;
        w.create_db(0, &token, "a").await;
        let (open, rejected) = fill_memory(&w, &token, "a").await;
        let memory = &w.node(0).engine.shared.memory;
        assert!(memory.used() <= memory.limit() + growth_slack(&w), "{} of {}", memory.used(), memory.limit());
        let rejected = rejected.expect("growing past the budget did not fail");
        let r = commit(&w, &token, "a", &rejected).await;
        assert_eq!(r.json()["error"]["code"], "txn_not_found", "{:?}", r.json());
        let r = commit(&w, &token, "a", &open[0]).await;
        assert_eq!(r.status, 200, "committing a transaction under memory pressure: {:?}", r.json());
        for id in &open[1..] {
            let r = w.call(0, "DELETE", &format!("/v1/dbs/a/transactions/{id}"), &token, Bytes::new()).await;
            assert_eq!(r.status, 200, "aborting a transaction under memory pressure: {:?}", r.json());
        }
        let r = begin(&w, &token, "a").await;
        assert_eq!(r.status, 201, "released memory was not reused");
    });
}

#[test]
fn concurrent_transaction_growth_is_bounded_by_memory_budget() {
    run_paused(async {
        let w = World::with_config(1, 1, |c| c.engine.memory_bytes = 16 << 20).await;
        let token = w.signup(0, "a@x.io").await;
        let db_id = w.create_db(0, &token, "a").await;
        let node = w.node(0);
        let db = node.engine.db(&db_id);
        db.open(true).await.unwrap();
        let mut open = Vec::new();
        for i in 0..20 {
            let t = db.begin(Isolation::Snapshot, Consistency::Strong).await.unwrap();
            open.push(Some(node.txns.open(format!("t{i}"), "a", &db_id, t, Default::default()).unwrap()));
        }
        let (memory, mut peak, mut rejected) = (&node.engine.shared.memory, 0, 0);
        for k in 0..120 {
            for g in open.iter_mut() {
                let Some(t) = g.as_mut() else { continue };
                let put = t.txn().put(Bytes::from(format!("k{k}")), Value::Inline(Bytes::from(vec![b'v'; 20_000])));
                if let Err(e) = put {
                    assert!(matches!(e, Error::Unavailable { .. }), "{e:?}");
                    (*g, rejected) = (None, rejected + 1);
                }
                peak = peak.max(memory.used());
            }
        }
        assert!(
            (1..=16).contains(&rejected),
            "{rejected} of 20 transactions of 2.4 MB were rejected in a 16 MiB budget"
        );
        let bound = memory.limit() + 20 * growth_slack(&w);
        assert!(peak <= bound, "interleaved growth peaked at {peak} of {}", memory.limit());
    });
}

#[test]
fn user_at_memory_budget_cannot_starve_other_users() {
    run_paused(async {
        let w = World::with_config(1, 1, |c| c.engine.memory_bytes = 16 << 20).await;
        let (alice, bob) = (w.signup(0, "alice@x.io").await, w.signup(0, "bob@x.io").await);
        w.create_db(0, &alice, "a").await;
        w.create_db(0, &bob, "b").await;
        let (alices, _) = fill_memory(&w, &alice, "a").await;
        let memory = &w.node(0).engine.shared.memory;
        let r = begin(&w, &bob, "b").await;
        assert_eq!(r.status, 201, "a user with 0 open transactions was rejected: {:?}", r.json());
        let bobs = r.json()["txn_id"].as_str().unwrap().to_string();
        let mut grown = 0;
        loop {
            let r = ops(&w, &bob, "b", &bobs, big_puts(&format!("b{grown}-"))).await;
            if !r["error"].is_null() {
                assert_eq!(r["error"]["code"], "txn_not_found", "{r}");
                break;
            }
            grown += 1;
            let (used, limit) = (memory.used(), memory.limit());
            assert!(used <= limit + growth_slack(&w), "bob's growth left {used} of {limit} bytes used");
            assert!(grown < 10, "bob was never rejected");
        }
        assert!(grown >= 3, "bob grew to {grown} write batches, less than half of the budget");
        let r = ops(&w, &alice, "a", &alices[0], get_op()).await;
        assert_eq!((&r["error"]["code"], &r["error"]["retryable"]), (&json!("txn_not_found"), &json!(true)), "{r}");
        assert!(r["error"]["message"].as_str().unwrap().contains("free node memory"), "{r}");
        let r = ops(&w, &alice, "a", &alices[alices.len() - 1], get_op()).await;
        assert!(r["error"].is_null(), "the most recently used transaction was evicted: {r}");
    });
}

#[test]
fn transactions_pinning_stale_versions_count_against_their_user() {
    run_paused(async {
        let w = World::with_config(1, 1, |c| c.engine.memory_bytes = 16 << 20).await;
        let (alice, bob) = (w.signup(0, "alice@x.io").await, w.signup(0, "bob@x.io").await);
        w.create_db(0, &alice, "a").await;
        w.create_db(0, &bob, "b").await;
        let fill: Vec<_> =
            (0..110).map(|i| json!({"op": "put", "key": format!("f{i}"), "value": "t".repeat(2_000)})).collect();
        let r = w.json(0, "POST", "/v1/dbs/a/txn", &alice, json!({"ops": fill})).await;
        assert!(r["error"].is_null(), "{r}");
        let mut pins = 0;
        loop {
            let r = begin(&w, &alice, "a").await;
            if r.status != 201 {
                assert_eq!(r.status, 503, "{:?}", r.json());
                break;
            }
            pins += 1;
            assert!(pins < 100, "transactions pinning versions were never rejected");
            let put = json!({"ops": [{"op": "put", "key": format!("p{pins}"), "value": "x"}]});
            let r = w.json(0, "POST", "/v1/dbs/a/txn", &alice, put).await;
            assert!(r["error"].is_null(), "{r}");
        }
        let memory = &w.node(0).engine.shared.memory;
        let r = begin(&w, &bob, "b").await;
        assert_eq!(r.status, 201, "bob was rejected while alice's transactions pinned {pins} versions: {:?}", r.json());
        assert!(!memory.is_exceeded(), "admitting bob left {} of {} bytes used", memory.used(), memory.limit());
    });
}
