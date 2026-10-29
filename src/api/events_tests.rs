use crate::sim::cost::{World, call_router, run_paused};
use bytes::Bytes;
use serde_json::{Value as Json, json};
use tower::ServiceExt;

const TABLE: &str = r#"{"name": "ev", "columns": [{"name": "time", "type": "time", "default": "now"},
    {"name": "event", "type": "string"}, {"name": "user", "type": "string"}],
    "time_column": "time", "order_by": ["event", "time"]}"#;

async fn send(
    w: &World,
    i: usize,
    t: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, Json) {
    let router = w.node(i).router().unwrap();
    let mut b = http::Request::builder().method(method).uri(path);
    if !t.is_empty() {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    let resp = router.oneshot(b.body(axum::body::Body::from(body.to_string())).unwrap()).await.unwrap();
    let status = resp.status().as_u16();
    let body = http_body_util::BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap_or_else(|_| json!(String::from_utf8_lossy(&body))))
}

async fn setup(nodes: usize) -> (World, String) {
    let w = World::new(nodes, 0).await;
    let t = w.signup(0, "ev@api.test").await;
    w.create_db(0, &t, "d").await;
    let r = call_router(&w.node(0).router().unwrap(), "POST", "/v1/dbs/d/events", &t, Bytes::from(TABLE)).await;
    assert_eq!(r.status, 201, "{}", String::from_utf8_lossy(&r.body));
    (w, t)
}

async fn count(w: &World, i: usize, t: &str) -> Json {
    let q = json!({"from": 0, "to": i64::MAX / 8, "aggregates": {"n": {"fn": "count"}}, "consistency": "strong"});
    let (s, r) = send(w, i, t, "POST", "/v1/dbs/d/events/ev/query", &[], &q.to_string()).await;
    assert_eq!(s, 200, "{r}");
    r["rows"][0][0].clone()
}

#[test]
fn replayed_bulk_insert_reports_the_original_count() {
    run_paused(async {
        let (w, t) = setup(1).await;
        let ct = [("content-type", "application/json"), ("idempotency-key", "k1")];
        let (s1, a) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &ct, r#"[{"event": "a"}]"#).await;
        assert_eq!(s1, 201, "{a}");
        let (s2, b) =
            send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &ct, r#"[{"event": "a"}, {"event": "b"}]"#).await;
        assert_eq!(s2, 200, "{b}");
        assert_eq!(b["replayed"], json!(true));
        assert_eq!(count(&w, 0, &t).await, json!(1));
        assert_eq!(b["count"], a["count"], "a replay answers with what the original request inserted: {a} vs {b}");
        w.shutdown();
    });
}

#[test]
fn bulk_insert_refuses_a_key_used_by_a_transaction() {
    run_paused(async {
        let (w, t) = setup(1).await;
        let txn = json!({"ops": [{"op": "put", "key": "x", "value": "y"}], "idempotency_key": "shared"});
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/txn", &[], &txn.to_string()).await;
        assert_eq!(s, 200, "{r}");
        let h = [("content-type", "application/json"), ("idempotency-key", "shared")];
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &h, r#"[{"event": "a"}]"#).await;
        let n = count(&w, 0, &t).await;
        assert!(s == 400 || n == json!(1), "the insert answered {s} {r} but stored {n} events: its rows were dropped");
        w.shutdown();
    });
}

async fn api_key(w: &World, t: &str, body: Json) -> String {
    let (s, r) = send(w, 0, t, "POST", "/v1/api-keys", &[], &body.to_string()).await;
    assert_eq!(s, 201, "{r}");
    r["key"].as_str().unwrap().to_string()
}

#[test]
fn read_only_keys_and_strangers_cannot_write_events() {
    run_paused(async {
        let (w, t) = setup(1).await;
        let ro = api_key(&w, &t, json!({"name": "ro", "read_only": true})).await;
        let j = [("content-type", "application/json")];
        let rows = r#"[{"event": "a"}]"#;
        assert_eq!(send(&w, 0, &ro, "POST", "/v1/dbs/d/events/ev/rows", &j, rows).await.0, 403);
        let nd = [("content-type", "application/x-ndjson")];
        assert_eq!(send(&w, 0, &ro, "POST", "/v1/dbs/d/events/ev/rows", &nd, "{\"event\":\"a\"}\n").await.0, 403);
        let del = json!({"where": {"col": "event", "op": "eq", "value": "a"}}).to_string();
        assert_eq!(send(&w, 0, &ro, "POST", "/v1/dbs/d/events/ev/delete", &j, &del).await.0, 403);
        assert_eq!(send(&w, 0, &ro, "PATCH", "/v1/dbs/d/events/ev", &j, r#"{"retention_ms": 1}"#).await.0, 403);
        assert_eq!(send(&w, 0, &ro, "DELETE", "/v1/dbs/d/events/ev", &[], "").await.0, 403);
        let col = r#"{"name":"x","type":"int"}"#;
        assert_eq!(send(&w, 0, &ro, "POST", "/v1/dbs/d/events/ev/columns", &j, col).await.0, 403);
        assert_eq!(send(&w, 0, &ro, "DELETE", "/v1/dbs/d/events/ev/columns/user", &[], "").await.0, 403);
        let si = r#"{"column":"user","type":"bloom"}"#;
        assert_eq!(send(&w, 0, &ro, "POST", "/v1/dbs/d/events/ev/skip-indexes", &j, si).await.0, 403);
        assert_eq!(send(&w, 0, &ro, "POST", "/v1/dbs/d/events", &j, TABLE).await.0, 403);
        let txn = json!({"ops": [{"op": "insert_events", "table": "ev", "rows": [{"event": "a"}]}]}).to_string();
        assert_eq!(send(&w, 0, &ro, "POST", "/v1/dbs/d/txn", &j, &txn).await.0, 403);
        let txn = json!({"ops": [{"op": "delete_events", "table": "ev",
            "where": {"col": "event", "op": "eq", "value": "a"}}]})
        .to_string();
        assert_eq!(send(&w, 0, &ro, "POST", "/v1/dbs/d/txn", &j, &txn).await.0, 403);
        let (s, r) = send(&w, 0, &ro, "POST", "/v1/dbs/d/events/ev/query", &j, r#"{"from":0,"to":1}"#).await;
        assert_eq!(s, 200, "{r}");
        let other = w.signup(0, "other@api.test").await;
        w.create_db(0, &t, "e").await;
        let scoped = api_key(&w, &t, json!({"name": "e", "db": "e"})).await;
        let (_, info) = send(&w, 0, &t, "GET", "/v1/dbs/d", &[], "").await;
        let id = info["id"].as_str().unwrap().to_string();
        for tok in [&other, &scoped] {
            for path in ["/v1/dbs/d/events/ev/rows".to_string(), format!("/v1/dbs/id:{id}/events/ev/rows")] {
                let (s, r) = send(&w, 0, tok, "POST", &path, &j, rows).await;
                assert!(s == 404 || s == 403, "{path}: {s} {r}");
                let qp = path.replace("/rows", "/query");
                let (s, r) = send(&w, 0, tok, "POST", &qp, &j, r#"{"from":0,"to":1}"#).await;
                assert!(s == 404 || s == 403, "{qp}: {s} {r}");
            }
        }
        let body = json!({"db": id, "bind": ""}).to_string();
        let (s, _) = send(&w, 0, &t, "POST", "/internal/v1/events/bind", &j, &body).await;
        assert_eq!(s, 403);
        assert_eq!(count(&w, 0, &t).await, json!(0));
        w.shutdown();
    });
}

#[test]
fn bulk_insert_bodies() {
    run_paused(async {
        let (w, t) = setup(1).await;
        let p = "/v1/dbs/d/events/ev/rows";
        let nd = [("content-type", "application/x-ndjson")];
        let (s, r) = send(&w, 0, &t, "POST", p, &nd, "{\"event\":\"a\"}\n{bad\n{\"event\":\"b\"}\n").await;
        assert_eq!((s, &r["error"]["code"]), (400, &json!("invalid_argument")), "{r}");
        assert_eq!(send(&w, 0, &t, "POST", p, &nd, "").await.0, 400);
        assert_eq!(send(&w, 0, &t, "POST", p, &[("content-type", "application/json")], "").await.0, 400);
        assert_eq!(send(&w, 0, &t, "POST", p, &[("content-type", "text/plain")], "{}").await.0, 400);
        let form = [("content-type", "application/x-www-form-urlencoded")];
        assert_eq!(send(&w, 0, &t, "POST", p, &form, "[{}]").await.0, 400);
        let cs = [("content-type", "application/json; charset=utf-8")];
        let (s, r) = send(&w, 0, &t, "POST", p, &cs, r#"{"rows":[{"event":"a"}]}"#).await;
        assert_eq!(s, 201, "{r}");
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/nope/rows", &[], r#"[{"event":"a"}]"#).await;
        assert_eq!((s, &r["error"]["code"]), (404, &json!("not_found")), "{r}");
        let big = format!(r#"[{{"event": "{}"}}]"#, "x".repeat(2 << 20));
        let (s, r) = send(&w, 0, &t, "POST", p, &[], &big).await;
        assert_eq!((s, &r["error"]["code"]), (413, &json!("too_large")), "{r}");
        let huge = format!("[{}]", vec![r#"{"event":"a"}"#; 3_000_000].join(","));
        let (s, r) = send(&w, 0, &t, "POST", p, &[], &huge).await;
        assert_eq!((s, &r["error"]["code"]), (413, &json!("too_large")), "{r}");
        assert_eq!(count(&w, 0, &t).await, json!(1));
        w.shutdown();
    });
}

#[test]
fn concurrent_retries_with_one_key_insert_once() {
    run_paused(async {
        let (w, t) = setup(3).await;
        let h = [("idempotency-key", "same")];
        let p = "/v1/dbs/d/events/ev/rows";
        let body = r#"[{"event":"a"}]"#;
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        count(&w, 1, &t).await;
        let (a, b, c) = tokio::join!(
            send(&w, 0, &t, "POST", p, &h, body),
            send(&w, 0, &t, "POST", p, &h, body),
            send(&w, 1, &t, "POST", p, &h, body)
        );
        println!("{a:?} {b:?} {c:?}");
        assert_eq!(count(&w, 0, &t).await, json!(1));
        let ok = [&a, &b, &c].iter().filter(|r| r.0 == 201 || r.0 == 200).count();
        assert_eq!(ok, 3, "every retry of one insert succeeds: {a:?} {b:?} {c:?}");
        w.shutdown();
    });
}

#[test]
fn bulk_insert_survives_a_lease_move_while_buffered() {
    run_paused(async {
        let (w, t) = setup(3).await;
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        let (_, info) = send(&w, 0, &t, "GET", "/v1/dbs/d", &[], "").await;
        let id = info["id"].as_str().unwrap().to_string();
        for i in 0..3 {
            count(&w, i, &t).await;
        }
        let h = w.holder(&id).expect("a lease holder");
        let n = (h + 1) % 3;
        let m = (h + 2) % 3;
        let insert = send(&w, n, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], r#"[{"event":"a"}]"#);
        let mover = async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            assert!(w.node(h).engine.db(&id).release_lease().await.unwrap());
            let md = w.node(m).engine.db(&id);
            md.refresh(tokio::time::Instant::now(), false).await.unwrap();
            md.acquire_lease().await.unwrap();
        };
        let ((s, r), ()) = tokio::join!(insert, mover);
        assert_eq!(w.holder(&id), Some(m));
        println!("holder {h} -> {m}, insert on {n}: {s} {r}");
        assert_eq!(s, 201, "an insert buffered while the lease moved fails: {r}");
        w.shutdown();
    });
}

#[test]
fn event_responses_match_the_openapi_document() {
    run_paused(async {
        let (w, t) = setup(1).await;
        let router = w.node(0).router().unwrap();
        let calls: Vec<(&str, &str, Json)> = vec![
            ("POST", "/v1/dbs/d/events/ev/rows", json!([{"event": "a", "user": "u", "time": 1_700_000_000_000i64}])),
            ("POST", "/v1/dbs/d/events/ev/rows", json!({"rows": [{"event": "b"}], "idempotency_key": "x"})),
            ("POST", "/v1/dbs/d/events/ev/rows", json!({"rows": [{"event": "b"}], "idempotency_key": "x"})),
            ("POST", "/v1/dbs/d/events/ev/rows", json!([{"nope": 1}])),
            ("POST", "/v1/dbs/d/events/ev/columns", json!({"name": "n", "type": "int", "default": 3})),
            ("POST", "/v1/dbs/d/events/ev/columns", json!({"name": "p", "type": "json"})),
            ("POST", "/v1/dbs/d/events/ev/skip-indexes", json!({"column": "user", "type": "bloom"})),
            ("PATCH", "/v1/dbs/d/events/ev", json!({"retention_ms": 86_400_000})),
            ("PATCH", "/v1/dbs/d/events/ev", json!({"retention_ms": null})),
            ("GET", "/v1/dbs/d/events/ev", Json::Null),
            ("GET", "/v1/dbs/d/events", Json::Null),
            ("POST", "/v1/dbs/d/events/ev/query", json!({"from": 0, "to": "2100-01-01"})),
            ("POST", "/v1/dbs/d/events/ev/query", json!({"from": 0, "to": "2100-01-01", "limit": 1})),
            (
                "POST",
                "/v1/dbs/d/events/ev/query",
                json!({"from": 0, "to": "2100-01-01", "group_by": ["event"],
                "aggregates": {"n": {"fn": "count"}, "u": {"fn": "uniq", "col": "user"},
                "q": {"fn": "quantiles", "col": "n", "levels": [0.5]}}}),
            ),
            (
                "POST",
                "/v1/dbs/d/txn",
                json!({"ops": [{"op": "insert_events", "table": "ev", "rows": [{"event": "c"}]},
                {"op": "query_events", "table": "ev", "from": 0, "to": "2100-01-01",
                 "aggregates": {"n": {"fn": "count"}}}]}),
            ),
            ("POST", "/v1/dbs/d/events/ev/delete", json!({"where": {"col": "event", "op": "eq", "value": "a"}})),
            (
                "POST",
                "/v1/dbs/d/txn",
                json!({"ops": [{"op": "delete_events", "table": "ev",
                "where": {"col": "event", "op": "eq", "value": "b"}}]}),
            ),
            ("DELETE", "/v1/dbs/d/events/ev/skip-indexes/user", Json::Null),
            ("DELETE", "/v1/dbs/d/events/ev/columns/n", Json::Null),
            (
                "POST",
                "/v1/dbs/d/events",
                json!({"name": "ev", "columns": [{"name": "time", "type": "time"}],
                "time_column": "time", "order_by": ["time"], "if_not_exists": true}),
            ),
            ("DELETE", "/v1/dbs/d/events/ev", Json::Null),
            ("GET", "/v1/dbs/d/events/ev", Json::Null),
        ];
        let mut bad = Vec::new();
        for (m, p, b) in calls {
            let body = if b.is_null() { Bytes::new() } else { Bytes::from(b.to_string()) };
            let r = call_router(&router, m, p, &t, body).await;
            let sr = crate::sim::SimResponse { status: r.status, headers: r.headers.clone(), body: r.body.clone() };
            if let Err(e) = crate::sim::openapi_check::check_response(m, p, &sr) {
                bad.push(format!("{m} {p} {}: {e}\n  {}", r.status, String::from_utf8_lossy(&r.body)));
            }
        }
        assert!(bad.is_empty(), "{}", bad.join("\n"));
        w.shutdown();
    });
}

#[test]
fn query_consistency_options() {
    run_paused(async {
        let (w, t) = setup(1).await;
        for (q, want) in [
            (r#"{"from":0,"to":1,"max_staleness_ms":"soon"}"#, 400),
            (r#"{"from":0,"to":1,"consistency":"eventual"}"#, 400),
            (r#"{"from":0,"to":1,"consistency":7}"#, 400),
            (r#"[1]"#, 400),
            (r#"{"from":0,"to":1,"max_staleness_ms":0}"#, 200),
            (r#"{"from":0,"to":1,"consistency":"strong"}"#, 200),
        ] {
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/query", &[], q).await;
            assert_eq!(s, want, "{q}: {r}");
        }
        w.shutdown();
    });
}

#[test]
fn console_default_queries_run_for_any_sort_key() {
    run_paused(async {
        let w = World::new(1, 0).await;
        let t = w.signup(0, "c@api.test").await;
        w.create_db(0, &t, "d").await;
        for (name, time, key) in [("a", "day", "day"), ("b", "time", "count"), ("c", "time", "day"), ("e", "t", "n")] {
            let def = json!({"name": name, "columns": [{"name": time, "type": "time"}, {"name": "n", "type": "int"},
                {"name": "day", "type": "string"}, {"name": "count", "type": "int"}],
                "time_column": time, "order_by": [key]});
            let def = if time == "day" {
                json!({"name": name, "columns": [{"name": "day", "type": "time"}, {"name": "n", "type": "int"}],
                    "time_column": "day", "order_by": [key]})
            } else {
                def
            };
            let (s, table) = send(&w, 0, &t, "POST", "/v1/dbs/d/events", &[], &def.to_string()).await;
            assert_eq!(s, 201, "{table}");
            let q = crate::console::events::default_query(&table);
            let (s, r) = send(&w, 0, &t, "POST", &format!("/v1/dbs/d/events/{name}/query"), &[], &q).await;
            assert_eq!(s, 200, "the console's default query for {def} fails: {r}");
        }
        w.shutdown();
    });
}

#[test]
fn inserts_on_one_node_are_seen_by_strong_queries_on_others() {
    run_paused(async {
        let (w, t) = setup(3).await;
        for i in 0..3 {
            let (s, r) = send(&w, i, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], r#"[{"event":"a"}]"#).await;
            assert_eq!(s, 201, "node {i}: {r}");
            for j in 0..3 {
                assert_eq!(count(&w, j, &t).await, json!(i + 1), "node {j} after an insert on {i}");
            }
        }
        w.shutdown();
    });
}

/// A meta command reaches the system database after its lease moves.
#[test]
fn meta_commands_survive_a_system_lease_move() {
    run_paused(async {
        let (w, t) = setup(3).await;
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        let sys = crate::meta::SYS_DB;
        let h = w.holder(sys).expect("a system lease holder");
        let (n, m) = ((h + 1) % 3, (h + 2) % 3);
        w.node(n).engine.db(sys).refresh(tokio::time::Instant::now(), false).await.unwrap();
        assert_eq!(w.node(n).engine.db(sys).lease_holder().as_deref(), Some(w.node(h).id.as_str()));
        assert!(w.node(h).engine.db(sys).release_lease().await.unwrap());
        let md = w.node(m).engine.db(sys);
        md.refresh(tokio::time::Instant::now(), false).await.unwrap();
        md.acquire_lease().await.unwrap();
        let (s, r) = send(&w, n, &t, "POST", "/v1/dbs", &[], r#"{"name": "d2"}"#).await;
        assert!(s == 200 || s == 201, "creating a database on node {n} fails: {s} {r}");
        w.shutdown();
    });
}

/// `"consistency": null` reads as an absent field, as `/txn` reads it.
#[test]
fn null_consistency_reads_as_absent() {
    run_paused(async {
        let (w, t) = setup(1).await;
        let txn = r#"{"ops": [{"op": "get", "key": "x"}], "consistency": null, "max_staleness_ms": null}"#;
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/txn", &[], txn).await;
        assert_eq!(s, 200, "/txn: {r}");
        let q = r#"{"from": 0, "to": 1, "consistency": null, "max_staleness_ms": null}"#;
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/query", &[], q).await;
        assert_eq!(s, 200, "event query: {r}");
        w.shutdown();
    });
}

/// A transaction that inserts events cannot also query or delete them,
/// since their positions are assigned at commit; it fails without writing.
#[test]
fn a_transaction_cannot_read_events_it_inserts() {
    run_paused(async {
        let (w, t) = setup(1).await;
        let insert = json!({"op": "insert_events", "table": "ev", "rows": [{"event": "a", "time": 1000}]});
        for second in [
            json!({"op": "query_events", "table": "ev", "from": 0, "to": "2100-01-01", "aggregates": {"n": {"fn": "count"}}}),
            json!({"op": "delete_events", "table": "ev", "where": {"col": "event", "op": "eq", "value": "a"}}),
        ] {
            let txn = json!({"ops": [insert, second]});
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/txn", &[], &txn.to_string()).await;
            assert_eq!(s, 400, "{r}");
            assert!(r["error"]["message"].as_str().unwrap_or("").contains("cannot also query or delete"), "{r}");
        }
        assert_eq!(count(&w, 0, &t).await, json!(0));
        w.shutdown();
    });
}

/// An event query may run up to its own time limit, longer than the request
/// deadline of other requests.
#[test]
fn slow_event_queries_run_past_the_request_deadline() {
    run_paused(async {
        let (w, t) = setup(2).await;
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        let (s, r) =
            send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], r#"[{"event": "a", "time": 1000}]"#).await;
        assert_eq!(s, 201, "{r}");
        let commit_timeout = w.node(0).cfg.engine.commit_timeout_ms;
        let cold = 1 - w.holder(&db_id(&w, &t).await).expect("a lease holder");
        w.store.set_latency_ms(commit_timeout + 2_000);
        let started = tokio::time::Instant::now();
        let q = r#"{"from": 0, "to": 2000, "consistency": "strong", "aggregates": {"n": {"fn": "count"}}}"#;
        let (s, r) = send(&w, cold, &t, "POST", "/v1/dbs/d/events/ev/query", &[], q).await;
        let took = started.elapsed().as_millis() as u64;
        assert!(took > commit_timeout, "the query took {took} ms: {s} {r}");
        assert_eq!((s, r["rows"].clone()), (200, serde_json::json!([[1]])), "after {took} ms: {r}");
        w.shutdown();
    });
}

async fn db_id(w: &World, t: &str) -> String {
    let (_, info) = send(w, 0, t, "GET", "/v1/dbs/d", &[], "").await;
    info["id"].as_str().expect("a database id").to_string()
}

mod requests {
    use super::*;
    use std::time::Duration;

    async fn setup_with(nodes: usize, latency_ms: u64, configure: fn(&mut crate::node::NodeConfig)) -> (World, String) {
        let w = World::with_config(nodes, latency_ms, configure).await;
        let t = w.signup(0, "ev@api.test").await;
        w.create_db(0, &t, "d").await;
        let r = call_router(&w.node(0).router().unwrap(), "POST", "/v1/dbs/d/events", &t, Bytes::from(TABLE)).await;
        assert_eq!(r.status, 201, "{}", String::from_utf8_lossy(&r.body));
        (w, t)
    }

    /// The request that fills a node's insert buffer may be cancelled while
    /// the buffer flushes without failing the requests buffered with it.
    #[test]
    fn a_cancelled_insert_does_not_fail_inserts_buffered_with_it() {
        run_paused(async {
            let (w, t) = setup_with(1, 20, |c| {
                c.event_insert_buffer_bytes = 4_000;
                c.event_insert_buffer_ms = 2_000;
            })
            .await;
            let p = "/v1/dbs/d/events/ev/rows";
            let a_body = format!(r#"[{{"event": "a", "user": "{}"}}]"#, "a".repeat(2_500));
            let b_body = format!(r#"[{{"event": "b", "user": "{}"}}]"#, "b".repeat(2_500));
            let a = send(&w, 0, &t, "POST", p, &[], &a_body);
            let b = async {
                while w.node(0).event_buffers().pending() == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                let fut = send(&w, 0, &t, "POST", p, &[], &b_body);
                tokio::pin!(fut);
                while w.node(0).event_buffers().pending() > 0 {
                    if let Some(r) = futures_util::FutureExt::now_or_never(&mut fut) {
                        panic!("the second insert finished before it was cancelled: {r:?}");
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                // The second client disconnects while its request flushes the buffer.
            };
            let ((s, r), ()) = tokio::join!(a, b);
            assert_eq!(s, 201, "an insert failed because another client's request was cancelled: {r}");
            w.shutdown();
        });
    }

    /// A client resolves its in-flight insert's key (SPEC §6.2) while the
    /// insert waits in a non-holder node's buffer: the forwarded bind is
    /// refused as aborted, and the client gets the same 409 as on the holder.
    #[test]
    fn a_forwarded_bind_keeps_its_aborted_error() {
        run_paused(async {
            let (w, t) = setup(2).await;
            tokio::time::sleep(Duration::from_secs(10)).await;
            let id = db_id(&w, &t).await;
            for i in 0..2 {
                count(&w, i, &t).await;
            }
            let h = w.holder(&id).expect("a lease holder");
            let n = 1 - h;
            let insert =
                send(&w, n, &t, "POST", "/v1/dbs/d/events/ev/rows", &[("idempotency-key", "k")], r#"[{"event": "a"}]"#);
            let resolve = async {
                while w.node(n).event_buffers().pending() == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                let (s, r) = send(&w, h, &t, "GET", "/v1/dbs/d/commits/key:k", &[], "").await;
                assert_eq!((s, r["status"].clone()), (200, json!("not_committed")), "{r}");
            };
            let ((s, r), ()) = tokio::join!(insert, resolve);
            assert_eq!((s, r["error"]["code"].clone()), (409, json!("aborted")), "{r}");
            w.shutdown();
        });
    }

    /// Events past retention are not returned, including rows inserted by
    /// transactions that stay unsealed below `event_seal_bytes`.
    #[test]
    fn queries_skip_unsealed_events_past_retention() {
        run_paused(async {
            let (w, t) = setup(1).await;
            let (s, r) =
                send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], r#"[{"event": "bulk", "time": 1000}]"#).await;
            assert_eq!(s, 201, "{r}");
            let txn =
                json!({"ops": [{"op": "insert_events", "table": "ev", "rows": [{"event": "txn", "time": 2000}]}]});
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/txn", &[], &txn.to_string()).await;
            assert_eq!(s, 200, "{r}");
            let (s, r) = send(&w, 0, &t, "PATCH", "/v1/dbs/d/events/ev", &[], r#"{"retention_ms": 3600000}"#).await;
            assert_eq!(s, 200, "{r}");
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_secs(30)).await;
                let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], r#"[{"event": "fresh"}]"#).await;
                assert_eq!(s, 201, "{r}");
            }
            tokio::time::sleep(Duration::from_secs(30)).await;
            let q = json!({"from": 0, "to": 10_000, "columns": ["event"], "consistency": "strong"});
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/query", &[], &q.to_string()).await;
            assert_eq!(s, 200, "{r}");
            assert_eq!(r["rows"], json!([]), "events older than retention_ms are still returned: {r}");
            w.shutdown();
        });
    }

    /// An added column defaulting to `"now"` has no value for earlier events.
    #[test]
    fn an_added_column_defaulting_to_now_is_nullable() {
        run_paused(async {
            let (w, t) = setup(1).await;
            let (s, r) =
                send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], r#"[{"event": "a", "time": 1000}]"#).await;
            assert_eq!(s, 201, "{r}");
            let col = r#"{"name": "seen", "type": "time", "nullable": false, "default": "now"}"#;
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/columns", &[], col).await;
            assert_eq!(s, 400, "a non-nullable column was added that reads null for earlier events: {r}");
            let col = r#"{"name": "seen", "type": "time", "default": "now"}"#;
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/columns", &[], col).await;
            assert_eq!(s, 200, "{r}");
            let q = r#"{"from": 0, "to": 2000, "columns": ["seen"], "consistency": "strong"}"#;
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/query", &[], q).await;
            assert_eq!((s, r["rows"].clone()), (200, json!([[null]])), "{r}");
            w.shutdown();
        });
    }

    /// A delete whose range is reversed is refused, as a query's is.
    #[test]
    fn deletes_refuse_reversed_ranges_like_queries() {
        run_paused(async {
            let (w, t) = setup(1).await;
            let q = r#"{"from": 2000, "to": 1000, "aggregates": {"n": {"fn": "count"}}}"#;
            let (sq, rq) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/query", &[], q).await;
            let d = r#"{"from": 2000, "to": 1000, "where": {"col": "event", "op": "eq", "value": "a"}}"#;
            let (sd, rd) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/delete", &[], d).await;
            assert_eq!(sq, sd, "query {sq} {rq} vs delete {sd} {rd}");
            w.shutdown();
        });
    }

    /// A null `idempotency_key` means none, as for stream appends and `/txn`.
    #[test]
    fn a_null_idempotency_key_in_the_body_is_absent() {
        run_paused(async {
            let (w, t) = setup(1).await;
            let b = r#"{"rows": [{"event": "a"}], "idempotency_key": null}"#;
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], b).await;
            assert_eq!(s, 201, "{r}");
            w.shutdown();
        });
    }

    /// A request naming one key in its header and another in its body is
    /// refused instead of being answered for the header's key.
    #[test]
    fn header_and_body_idempotency_keys_must_agree() {
        run_paused(async {
            let (w, t) = setup(1).await;
            let p = "/v1/dbs/d/events/ev/rows";
            let (s, r) = send(&w, 0, &t, "POST", p, &[("idempotency-key", "h")], r#"[{"event": "a"}]"#).await;
            assert_eq!(s, 201, "{r}");
            let b = r#"{"rows": [{"event": "b"}], "idempotency_key": "other"}"#;
            let (s, r) = send(&w, 0, &t, "POST", p, &[("idempotency-key", "h")], b).await;
            assert_eq!((s, r["error"]["message"].as_str().is_some_and(|m| m.contains("differ"))), (400, true), "{r}");
            let p = "/v1/dbs/d/streams/s/records?create=true";
            let b = r#"{"records": [{"value": "x"}], "idempotency_key": "other"}"#;
            let (s, r) = send(&w, 0, &t, "POST", p, &[("idempotency-key", "s")], b).await;
            assert_eq!((s, r["error"]["message"].as_str().is_some_and(|m| m.contains("differ"))), (400, true), "{r}");
            w.shutdown();
        });
    }
}
