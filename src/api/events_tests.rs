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
    let (s, r) = send(w, i, t, "POST", "/v1/dbs/d/events/ev/rows/query", &[], &q.to_string()).await;
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

/// JSON integers that `serde_json` would round to a float are refused on
/// every insert path rather than stored rounded; floats that large are kept.
#[test]
fn integers_beyond_64_bits_are_refused_not_rounded() {
    run_paused(async {
        let (w, t) = setup(1).await;
        let (s, r) =
            send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/columns", &[], r#"{"name": "p", "type": "json"}"#).await;
        assert_eq!(s, 200, "{r}");
        let ndjson = [("content-type", "application/x-ndjson")];
        for n in ["9223372036854775808", "18446744073709551616", "-9223372036854775809"] {
            let event = format!(r#"{{"event": "a", "time": 1000, "p": {{"id": {n}}}}}"#);
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], &format!("[{event}]")).await;
            assert_eq!(s, 400, "bulk {n}: {r}");
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &ndjson, &event).await;
            assert_eq!(s, 400, "NDJSON {n}: {r}");
            let txn = format!(r#"{{"ops": [{{"op": "insert_events", "table": "ev", "rows": [{event}]}}]}}"#);
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/txn", &[], &txn).await;
            assert_eq!(s, 400, "txn {n}: {r}");
        }
        assert_eq!(count(&w, 0, &t).await, json!(0));
        let (s, r) =
            send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], r#"[{"time": 1000, "p": {"x": 1e19}}]"#).await;
        assert_eq!(s, 201, "{r}");
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
        assert_eq!(send(&w, 0, &ro, "POST", "/v1/dbs/d/events/ev/rows/delete", &j, &del).await.0, 403);
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
        let (s, r) = send(&w, 0, &ro, "POST", "/v1/dbs/d/events/ev/rows/query", &j, r#"{"from":0,"to":1}"#).await;
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
                let qp = format!("{path}/query");
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
            ("POST", "/v1/dbs/d/events/ev/rows/query", json!({"from": 0, "to": "2100-01-01"})),
            ("POST", "/v1/dbs/d/events/ev/rows/query", json!({"from": 0, "to": "2100-01-01", "limit": 1})),
            (
                "POST",
                "/v1/dbs/d/events/ev/rows/query",
                json!({"from": 0, "to": "2100-01-01", "group_by": ["user"],
                "aggregates": {"n": {"fn": "count"}, "u": {"fn": "uniq", "of": "event"},
                "q": {"fn": "quantiles", "of": "time", "q": [0.5, 0.9]},
                "f": {"fn": "window_funnel", "window_ms": 3_600_000, "mode": "strict_increase", "steps": [
                    {"col": "event", "op": "eq", "value": "a"}, {"col": "event", "op": "eq", "value": "b"}]},
                "r": {"fn": "retention", "conditions": [
                    {"col": "event", "op": "eq", "value": "a"}, {"col": "event", "op": "eq", "value": "b"}]}},
                "then": {"group_by": ["f"], "aggregates": {"users": {"fn": "count"}}}}),
            ),
            (
                "POST",
                "/v1/dbs/d/txn",
                json!({"ops": [{"op": "insert_events", "table": "ev", "rows": [{"event": "c"}]},
                {"op": "query_events", "table": "ev", "from": 0, "to": "2100-01-01",
                 "aggregates": {"n": {"fn": "count"}}}]}),
            ),
            ("POST", "/v1/dbs/d/events/ev/rows/delete", json!({"where": {"col": "event", "op": "eq", "value": "a"}})),
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
            if p.ends_with("/rows/query") && r.status != 200 {
                bad.push(format!("{m} {p} {b}: {} {}", r.status, String::from_utf8_lossy(&r.body)));
            }
        }
        assert!(bad.is_empty(), "{}", bad.join("\n"));
        w.shutdown();
    });
}

/// The funnel per plan that SPEC §4.8 shows as an example answers as it says.
#[test]
fn the_spec_example_query_answers() {
    run_paused(async {
        let (w, t) = setup(1).await;
        let users = json!({"name": "users", "columns": [{"name": "user", "type": "string"},
            {"name": "plan", "type": "string"}], "primary_key": ["user"]});
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/tables", &[], &users.to_string()).await;
        assert_eq!(s, 201, "{r}");
        for (u, plan) in [("u1", "pro"), ("u2", "free"), ("u3", "pro")] {
            let row = json!({"row": {"user": u, "plan": plan}}).to_string();
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/tables/users/rows", &[], &row).await;
            assert!(s == 200 || s == 201, "{r}");
        }
        let at = |d: &str| format!("2026-10-0{d}T12:00:00Z");
        let rows = json!([
            {"event": "view", "user": "u1", "time": at("2")}, {"event": "buy", "user": "u1", "time": at("2")},
            {"event": "view", "user": "u2", "time": at("2")}, {"event": "buy", "user": "u2", "time": at("5")},
            {"event": "view", "user": "u3", "time": at("3")},
        ]);
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], &rows.to_string()).await;
        assert_eq!(s, 201, "{r}");
        let q = r#"{"from": "2026-10-01", "to": "2026-11-01", "lookups": {"plan": {"table":
  "users", "key": "user", "value": "plan"}}, "group_by": ["user", {"lookup":
  "plan", "as": "plan"}], "aggregates": {"steps": {"fn": "window_funnel",
  "window_ms": 86400000, "steps": [{"col": "event", "op": "eq", "value":
  "view"}, {"col": "event", "op": "eq", "value": "buy"}]}}, "then":
  {"group_by": ["plan", "steps"], "aggregates": {"users": {"fn": "count"}}}, "consistency": "strong"}"#;
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows/query", &[], q).await;
        assert_eq!(s, 200, "{r}");
        assert_eq!(r["columns"], json!(["plan", "steps", "users"]), "{r}");
        assert_eq!(r["rows"], json!([["free", 1, 1], ["pro", 1, 1], ["pro", 2, 1]]), "{r}");
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
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows/query", &[], q).await;
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
            let (s, r) = send(&w, 0, &t, "POST", &format!("/v1/dbs/d/events/{name}/rows/query"), &[], &q).await;
            assert_eq!(s, 200, "the console's default query for {def} fails: {r}");
        }
        w.shutdown();
    });
}

/// The console's table page runs its default query on every view, so it
/// reads only the newest month of parts, however old the table is.
#[test]
fn console_default_queries_read_the_newest_month() {
    run_paused(async {
        let (w, t) = setup(1).await;
        let now = w.node(0).now_ms() as i64;
        let day = 86_400_000;
        let rows = json!([{"event": "a", "time": now - 400 * day}, {"event": "b", "time": now - 2 * day}]);
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], &rows.to_string()).await;
        assert_eq!(s, 201, "{r}");
        let (_, table) = send(&w, 0, &t, "GET", "/v1/dbs/d/events/ev", &[], "").await;
        assert_eq!(table["parts"].as_array().map(Vec::len), Some(2), "{table}");
        let q = crate::console::events::default_query(&table);
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows/query", &[], &q).await;
        assert_eq!((s, r["stats"]["parts"].clone()), (200, json!(1)), "{q}: {r}");
        w.shutdown();
    });
}

/// The console shows times before 1970, which tables accept, as dates:
/// partition starts and the times in query answers alike.
#[test]
fn console_shows_times_before_1970() {
    run_paused(async {
        let (w, t) = setup(1).await;
        let rows = r#"[{"event": "a", "time": "1960-06-15T12:00:00Z"}]"#;
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], rows).await;
        assert_eq!(s, 201, "{r}");
        let req = http::Request::builder()
            .uri("/console/dbs/d/events/ev")
            .header("cookie", format!("{}={t}", crate::api::SESSION_COOKIE))
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = w.node(0).router().unwrap().oneshot(req).await.unwrap();
        let html = http_body_util::BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
        let html = String::from_utf8_lossy(&html);
        let at = |h: &str| html.find(h).unwrap_or_else(|| panic!("no {h} in {html}"));
        let (answer, partitions) = (&html[..at("<h2>Partitions</h2>")], &html[at("<h2>Partitions</h2>")..]);
        assert!(partitions.contains("<td>1960-06-15 00:00:00</td>"), "{partitions}");
        assert!(answer.contains("<td>1960-06-15 00:00:00</td><td>a</td>"), "{answer}");
        w.shutdown();
    });
}

/// A time that is not an RFC 3339 timestamp with an offset or whole
/// milliseconds is refused for its format, not as out of range.
#[test]
fn unparseable_times_are_refused_for_their_format() {
    run_paused(async {
        let (w, t) = setup(1).await;
        for time in [json!("2026-10-01T12:00:00"), json!("2026-10-01T12:00:00.123456"), json!(1_790_000_000_000.5)] {
            let rows = json!([{"event": "a", "time": time}]);
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], &rows.to_string()).await;
            assert_eq!(s, 400, "{r}");
            let m = r["error"]["message"].as_str().unwrap_or_default();
            assert!(!m.contains("years 0000 to 9999"), "{time} is refused as out of range: {m}");
        }
        w.shutdown();
    });
}

/// A query's pacing counts the expressions it computes per row, so one
/// computing many copies of a large default per row still stops near its
/// time limit.
#[test]
fn queries_of_many_expressions_stop_near_their_time_limit() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
    rt.block_on(async {
        let w = World::with_config(1, 0, |c| c.query_limits.max_ms = 500).await;
        let t = w.signup(0, "ev@api.test").await;
        w.create_db(0, &t, "d").await;
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events", &[], TABLE).await;
        assert_eq!(s, 201, "{r}");
        let rows: Vec<Json> = (0..40_000).map(|i| json!({"event": "a", "time": 1000 + i})).collect();
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], &json!(rows).to_string()).await;
        assert_eq!(s, 201, "{r}");
        let pad = "x".repeat(crate::events::schema::DEFAULT_MAX_BYTES - 2);
        let col = json!({"name": "pad", "type": "string", "default": pad});
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/columns", &[], &col.to_string()).await;
        assert_eq!(s, 200, "{r}");
        let group_by: Vec<Json> = (0..64).map(|i| json!({"col": "pad", "as": format!("p{i}")})).collect();
        let q = json!({"from": 0, "to": 1_000_000, "group_by": group_by, "consistency": "strong"}).to_string();
        let started = tokio::time::Instant::now();
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows/query", &[], &q).await;
        let elapsed = started.elapsed();
        assert!(elapsed < std::time::Duration::from_millis(1500), "a 500 ms query ran {elapsed:?}: {s} {r}");
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
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows/query", &[], q).await;
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
        let (s, r) = send(&w, cold, &t, "POST", "/v1/dbs/d/events/ev/rows/query", &[], q).await;
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

#[test]
fn event_table_listings_are_paginated() {
    run_paused(async {
        let (w, t) = setup(1).await;
        let def = json!({"name": "ev2", "columns": [{"name": "time", "type": "time"}], "time_column": "time", "order_by": ["time"]});
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events", &[], &def.to_string()).await;
        assert_eq!(s, 201, "{r}");
        let (s, r) = send(&w, 0, &t, "GET", "/v1/dbs/d/events?limit=1", &[], "").await;
        assert_eq!(s, 200, "{r}");
        assert_eq!(r["tables"].as_array().map(Vec::len), Some(1), "limit=1 lists: {r}");
        assert!(r["next_cursor"].is_string(), "no next_cursor: {r}");
        w.shutdown();
    });
}

fn row_table(name: &str) -> String {
    json!({"name": name, "primary_key": ["id"], "columns": [{"name": "id", "type": "int", "nullable": false}]})
        .to_string()
}

#[test]
fn row_table_listings_are_paginated() {
    run_paused(async {
        let (w, t) = setup(1).await;
        for name in ["a", "b"] {
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/tables", &[], &row_table(name)).await;
            assert_eq!(s, 201, "{r}");
        }
        let (s, r) = send(&w, 0, &t, "GET", "/v1/dbs/d/tables?limit=1", &[], "").await;
        assert_eq!(s, 200, "{r}");
        assert_eq!(r["tables"].as_array().map(Vec::len), Some(1), "limit=1 lists: {r}");
        assert!(r["next_cursor"].is_string(), "no next_cursor: {r}");
        w.shutdown();
    });
}

/// Every catalog listing pages through all its entries exactly once, in
/// name order, filters by prefix, and refuses page sizes outside 1..=1000.
#[test]
fn catalog_listings_page_through_each_entry_once() {
    run_paused(async {
        let (w, t) = setup(1).await;
        let names: Vec<String> = (0..7).map(|i| format!("p{i}")).collect();
        for n in &names {
            let def = json!({"name": n, "columns": [{"name": "time", "type": "time"}], "time_column": "time", "order_by": ["time"]});
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events", &[], &def.to_string()).await;
            assert_eq!(s, 201, "{r}");
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/tables", &[], &row_table(n)).await;
            assert_eq!(s, 201, "{r}");
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/streams", &[], &json!({"name": n}).to_string()).await;
            assert_eq!(s, 201, "{r}");
        }
        for (path, field) in [("events", "tables"), ("tables", "tables"), ("streams", "streams")] {
            let (mut seen, mut pages, mut cursor) = (Vec::new(), 0, None::<String>);
            loop {
                let after = cursor.as_ref().map(|c| format!("&cursor={c}")).unwrap_or_default();
                let uri = format!("/v1/dbs/d/{path}?prefix=p&limit=3{after}");
                let (s, r) = send(&w, 0, &t, "GET", &uri, &[], "").await;
                assert_eq!(s, 200, "{uri}: {r}");
                pages += 1;
                seen.extend(r[field].as_array().unwrap().iter().map(|x| x["name"].as_str().unwrap().to_string()));
                match r["next_cursor"].as_str() {
                    Some(c) => cursor = Some(c.to_string()),
                    None => break,
                }
            }
            assert_eq!((seen, pages), (names.clone(), 3), "{path}");
            let (s, r) = send(&w, 0, &t, "GET", &format!("/v1/dbs/d/{path}?prefix=p"), &[], "").await;
            assert_eq!((s, r[field].as_array().map(Vec::len)), (200, Some(7)), "{path} by default: {r}");
            for limit in [0, 1001] {
                let (s, r) = send(&w, 0, &t, "GET", &format!("/v1/dbs/d/{path}?limit={limit}"), &[], "").await;
                assert_eq!(s, 400, "{path} limit={limit}: {r}");
            }
            let (s, r) = send(&w, 0, &t, "GET", &format!("/v1/dbs/d/{path}?cursor=%25"), &[], "").await;
            assert_eq!(s, 400, "{path} with a bad cursor: {r}");
        }
        w.shutdown();
    });
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
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows/query", &[], &q.to_string()).await;
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
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows/query", &[], q).await;
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
            let (sq, rq) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows/query", &[], q).await;
            let d = r#"{"from": 2000, "to": 1000, "where": {"col": "event", "op": "eq", "value": "a"}}"#;
            let (sd, rd) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows/delete", &[], d).await;
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

/// Bulk inserts and queries are served by the node they reach; every other
/// event route goes to the lease holder.
#[test]
fn event_rows_routes_are_served_where_they_arrive() {
    let post = http::Method::POST;
    for (method, path, local, query) in [
        (&post, "/v1/dbs/d/events/t/rows", true, false),
        (&post, "/v1/dbs/d/events/t/rows/", true, false),
        (&post, "/v1/dbs/d/events/t/rows/query", true, true),
        (&post, "/v1/dbs/d/events/t/rows/delete", false, false),
        (&post, "/v1/dbs/d/events/t/rows/query/x", false, false),
        (&post, "/v1/dbs/d/tables/t/rows/query", false, false),
        (&post, "/v1/dbs/d/events/rows/query", false, false),
        (&http::Method::GET, "/v1/dbs/d/events/t/rows/query", false, false),
    ] {
        let got = (super::events::served_locally(method, path), super::events::is_query(method, path));
        assert_eq!(got, (local, query), "{method} {path}");
    }
}
mod forwarding {
    use super::*;
    use std::time::Duration;

    async fn setup_cfg(nodes: usize, configure: fn(&mut crate::node::NodeConfig)) -> (World, String) {
        let w = World::with_config(nodes, 0, configure).await;
        let t = w.signup(0, "ev@api.test").await;
        w.create_db(0, &t, "d").await;
        let r = call_router(&w.node(0).router().unwrap(), "POST", "/v1/dbs/d/events", &t, Bytes::from(TABLE)).await;
        assert_eq!(r.status, 201, "{}", String::from_utf8_lossy(&r.body));
        (w, t)
    }

    /// A bulk insert accepted by the lease holder is accepted by every node:
    /// a forwarded bind is not bounded by the client JSON body limit.
    #[test]
    fn forwarded_binds_are_not_bounded_by_the_json_body_limit() {
        run_paused(async {
            let (w, t) = setup_cfg(2, |c| c.max_json_body = 256 << 10).await;
            tokio::time::sleep(Duration::from_secs(10)).await;
            let id = db_id(&w, &t).await;
            for i in 0..2 {
                count(&w, i, &t).await;
            }
            let h = w.holder(&id).expect("a lease holder");
            let n = 1 - h;
            let day = 86_400_000i64;
            let rows: Vec<Json> = (0..4000).map(|i| json!({"event": "a", "time": i * day})).collect();
            let body = Json::Array(rows).to_string();
            assert!(body.len() < 256 << 10, "{}", body.len());
            let p = "/v1/dbs/d/events/ev/rows";
            let (sh, rh) = send(&w, h, &t, "POST", p, &[], &body).await;
            let (sn, rn) = send(&w, n, &t, "POST", p, &[], &body).await;
            assert_eq!((sh, sn), (201, 201), "on the holder: {sh} {rh}\non the other node: {sn} {rn}");
            w.shutdown();
        });
    }

    /// The console's table page runs its default query; a table holding an
    /// event after 2100 still gets an answer.
    #[test]
    fn console_default_queries_cover_far_future_events() {
        run_paused(async {
            let (w, t) = setup(1).await;
            let rows =
                json!([{"event": "a", "time": "2026-01-01T00:00:00Z"}, {"event": "b", "time": "2200-01-01T00:00:00Z"}]);
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], &rows.to_string()).await;
            assert_eq!(s, 201, "{r}");
            let (_, table) = send(&w, 0, &t, "GET", "/v1/dbs/d/events/ev", &[], "").await;
            let q = crate::console::events::default_query(&table);
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows/query", &[], &q).await;
            assert_eq!(s, 200, "the console's default query fails: {q}: {r}");
            w.shutdown();
        });
    }

    /// The console's default query counts an event in the last second of
    /// year 9999, which the table accepts.
    #[test]
    fn console_default_queries_count_the_latest_storable_event() {
        run_paused(async {
            let (w, t) = setup(1).await;
            let rows = json!([{"event": "b", "time": "9999-12-31T23:59:59.500Z"}]);
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], &rows.to_string()).await;
            assert_eq!(s, 201, "{r}");
            let (_, table) = send(&w, 0, &t, "GET", "/v1/dbs/d/events/ev", &[], "").await;
            let q = crate::console::events::default_query(&table);
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows/query", &[], &q).await;
            assert_eq!(s, 200, "{q}: {r}");
            let n: i64 = r["rows"].as_array().unwrap().iter().map(|row| row[2].as_i64().unwrap_or(0)).sum();
            assert_eq!(n, 1, "the default query {q} answered {r}");
            w.shutdown();
        });
    }
}

/// A transaction that reuses a bulk insert's idempotency key is refused,
/// as a bulk insert reusing a transaction's key is, instead of answering
/// `replayed: true` without applying its ops.
#[test]
fn transactions_refuse_a_key_used_by_a_bulk_insert() {
    run_paused(async {
        let (w, t) = setup(1).await;
        let h = [("content-type", "application/json"), ("idempotency-key", "shared")];
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &h, r#"[{"event": "a"}]"#).await;
        assert_eq!(s, 201, "{r}");
        let txn = json!({"ops": [{"op": "put", "key": "x", "value": "y"}], "idempotency_key": "shared"});
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/txn", &[], &txn.to_string()).await;
        let (g, _) = send(&w, 0, &t, "GET", "/v1/dbs/d/kv/x", &[], "").await;
        assert!(s == 400 || g == 200, "the transaction answered {s} {r} but its put was not applied (GET x: {g})");
        w.shutdown();
    });
}

mod console_pages {
    use super::*;

    async fn web(w: &World, t: &str, m: &str, p: &str, form: &[(&str, &str)]) -> (u16, String) {
        let body: Vec<String> = form
            .iter()
            .map(|(k, v)| {
                let enc: String = v
                    .bytes()
                    .map(|b| if b.is_ascii_alphanumeric() { (b as char).to_string() } else { format!("%{b:02X}") })
                    .collect();
                format!("{k}={enc}")
            })
            .collect();
        let req = http::Request::builder()
            .method(m)
            .uri(p)
            .header("cookie", format!("{}={t}", crate::api::SESSION_COOKIE))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(axum::body::Body::from(body.join("&")))
            .unwrap();
        let resp = w.node(0).router().unwrap().oneshot(req).await.unwrap();
        let status = resp.status().as_u16();
        let b = http_body_util::BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&b).to_string())
    }

    fn csrf_of(html: &str) -> String {
        let i = html.find("name=\"csrf\" value=\"").expect("a csrf token") + "name=\"csrf\" value=\"".len();
        html[i..i + html[i..].find('"').unwrap()].to_string()
    }

    /// The query runner shows time buckets as times, including those a
    /// `then` stage groups by.
    #[test]
    fn query_runner_shows_second_stage_buckets_as_times() {
        run_paused(async {
            let (w, t) = setup(1).await;
            let rows = r#"[{"event": "a", "time": "2023-11-14T22:13:20Z"}]"#;
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], rows).await;
            assert_eq!(s, 201, "{r}");
            let (_, page) = web(&w, &t, "GET", "/console/dbs/d/events/ev", &[]).await;
            let q = r#"{"from": 0, "to": "2100-01-01",
                "group_by": [{"bucket": "time", "interval": "1h", "as": "hour"}], "aggregates": {"n": {"fn": "count"}},
                "then": {"group_by": [{"bucket": "hour", "interval": "1d", "as": "day"}], "aggregates": {"hours": {"fn": "count"}}}}"#;
            let (s, html) =
                web(&w, &t, "POST", "/console/dbs/d/events/ev/query", &[("csrf", &csrf_of(&page)), ("query", q)]).await;
            assert_eq!(s, 200);
            let answer = &html[html.find("<th>day</th>").expect("an answer table")..];
            let answer = &answer[..answer.find("</table>").unwrap()];
            assert!(answer.contains("<td>2023-11-14 00:00:00</td>"), "the day bucket is shown as a number: {answer}");
            w.shutdown();
        });
    }

    /// A retention shorter than an hour is not shown as zero.
    #[test]
    fn table_page_shows_sub_hour_retention() {
        run_paused(async {
            let (w, t) = setup(1).await;
            let (s, r) = send(&w, 0, &t, "PATCH", "/v1/dbs/d/events/ev", &[], r#"{"retention_ms": 1800000}"#).await;
            assert_eq!(s, 200, "{r}");
            let (_, page) = web(&w, &t, "GET", "/console/dbs/d/events/ev", &[]).await;
            let stats = &page[page.find("<span>Retention</span>").expect("a retention stat")..];
            let stats = &stats[..stats.find("</div>").unwrap()];
            assert!(!stats.contains(">0 h"), "30 minutes of retention reads as zero: {stats}");
            w.shutdown();
        });
    }
}

/// A large bulk insert of tiny events is written as a few parts, not one
/// sequential store round trip per batch of rows decoded.
#[test]
fn a_large_bulk_insert_is_not_split_into_sequential_part_writes() {
    run_paused(async {
        let w = World::new(1, 20).await;
        let t = w.signup(0, "ev@api.test").await;
        w.create_db(0, &t, "d").await;
        let r = call_router(&w.node(0).router().unwrap(), "POST", "/v1/dbs/d/events", &t, Bytes::from(TABLE)).await;
        assert_eq!(r.status, 201);
        let nd = vec![r#"{"event":"a"}"#; 2_200_000].join("\n");
        let start = tokio::time::Instant::now();
        let ct = [("content-type", "application/x-ndjson")];
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &ct, &nd).await;
        assert_eq!(s, 201, "{r}");
        let took = start.elapsed();
        assert!(took < std::time::Duration::from_millis(500), "a 31 MB insert took {took:?} at 20 ms per store call");
        w.shutdown();
    });
}

/// A body naming `rows` or `idempotency_key` twice is refused rather than
/// read one way or the other.
#[test]
fn bulk_insert_bodies_name_each_field_once() {
    run_paused(async {
        let (w, t) = setup(1).await;
        for b in [
            r#"{"rows": [{"event": "a"}], "rows": [{"event": "b"}]}"#,
            r#"{"idempotency_key": "a", "rows": [{"event": "a"}], "idempotency_key": "b"}"#,
        ] {
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], b).await;
            assert_eq!(s, 400, "{b}: {r}");
        }
        assert_eq!(count(&w, 0, &t).await, json!(0));
        w.shutdown();
    });
}

/// A deleted event leaves object storage within the purge window plus GC
/// bounds, even when its part is too little deleted to be rewritten for
/// space and its partition is never merged again.
#[test]
fn deleted_events_are_eventually_purged_from_storage() {
    run_paused(async {
        let (w, t) = setup(1).await;
        let rows = r#"[{"event": "a", "user": "u1", "time": 1000}, {"event": "a", "user": "u2", "time": 2000},
            {"event": "a", "user": "pii-erase-me", "time": 3000}]"#;
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], rows).await;
        assert_eq!(s, 201, "{r}");
        let del = r#"{"where": {"col": "user", "op": "eq", "value": "pii-erase-me"}}"#;
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows/delete", &[], del).await;
        assert_eq!((s, r["deleted"].clone()), (200, json!(1)), "{r}");
        for i in 0..30 {
            tokio::time::sleep(std::time::Duration::from_secs(3 * 3600)).await;
            let put = json!({"ops": [{"op": "put", "key": format!("k{i}"), "value": "v"}]});
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/txn", &[], &put.to_string()).await;
            assert_eq!(s, 200, "{r}");
        }
        let (s, d) = send(&w, 0, &t, "GET", "/v1/dbs/d/events/ev", &[], "").await;
        assert_eq!((s, d["rows"].clone()), (200, json!(2)), "{d}");
        let holding: Vec<String> = w
            .mem
            .keys()
            .into_iter()
            .filter(|k| {
                let data = w.mem.get_sync(k, &Default::default()).unwrap().data;
                data.windows(12).any(|x| x == b"pii-erase-me")
            })
            .collect();
        assert!(holding.is_empty(), "3.75 days after the delete, {holding:?} still hold the deleted event: {d}");
        w.shutdown();
    });
}

/// A page by time reads only the parts that can hold its rows: neither the
/// parts before its cursor nor those after its last row.
#[test]
fn row_pages_read_only_parts_that_can_hold_them() {
    run_paused(async {
        let (w, t) = setup(1).await;
        let day = 86_400_000i64;
        for d in 0..4 {
            let rows: Vec<_> = (0..5).map(|i| json!({"event": "e", "time": d * day + i})).collect();
            let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows", &[], &json!(rows).to_string()).await;
            assert_eq!(s, 201, "{r}");
        }
        let q = json!({"from": 0, "to": 4 * day, "limit": 5, "consistency": "strong"});
        let (s, first) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows/query", &[], &q.to_string()).await;
        assert_eq!(s, 200, "{first}");
        let mut q2 = q.clone();
        q2["cursor"] = first["next_cursor"].clone();
        let (s, second) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows/query", &[], &q2.to_string()).await;
        assert_eq!(s, 200, "{second}");
        assert_eq!(second["rows"][0][0], json!(day), "{second}");
        assert_eq!(
            (first["stats"]["parts_read"].clone(), second["stats"]["parts_read"].clone()),
            (json!(1), json!(1)),
            "pages of 5 rows, each within one day's part, read: {} and {}",
            first["stats"],
            second["stats"]
        );
        let latest = json!({"from": 0, "to": 4 * day, "limit": 3, "order_by": [{"col": "time", "desc": true}],
            "consistency": "strong"});
        let (s, r) = send(&w, 0, &t, "POST", "/v1/dbs/d/events/ev/rows/query", &[], &latest.to_string()).await;
        assert_eq!(s, 200, "{r}");
        assert_eq!((&r["rows"][0][0], &r["stats"]["parts_read"]), (&json!(3 * day + 4), &json!(1)), "{r}");
        w.shutdown();
    });
}
