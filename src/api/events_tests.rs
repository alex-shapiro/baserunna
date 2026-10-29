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
