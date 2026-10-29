//! HTTP API fuzzing inside the simulator: random and mutated requests must
//! never produce a 500 or a panic, and responses on documented routes must
//! conform to the OpenAPI document. Scale with `FUZZ_ITERS`.

use baserunna::sim::client::Client;
use baserunna::sim::{SimConfig, openapi_check, run};
use baserunna::util::rng::Rng;
use serde_json::{Value, json};

fn rand_string(r: &mut Rng) -> String {
    let pool = [
        "",
        "a",
        "x/y",
        "../..",
        "%00",
        "%FF",
        "é",
        "\u{0}",
        "id:",
        "{}",
        "k".repeat(5000).as_str(),
        "🦀",
        " ",
        "null",
    ]
    .map(|s| s.to_string());
    if r.chance(0.5) {
        pool[r.usize(pool.len())].clone()
    } else {
        (0..r.usize(12)).map(|_| (b'a' + r.below(26) as u8) as char).collect()
    }
}

fn rand_json(r: &mut Rng, depth: u32) -> Value {
    match r.below(if depth > 3 { 6 } else { 9 }) {
        0 => Value::Null,
        1 => json!(r.chance(0.5)),
        2 => json!(r.next_u64() as i64),
        3 => json!(r.unit() * 1e6 - 5e5),
        4 => json!(rand_string(r)),
        5 => json!(i64::MIN),
        6 => Value::Array((0..r.usize(4)).map(|_| rand_json(r, depth + 1)).collect()),
        _ => {
            let keys = [
                "op",
                "ops",
                "key",
                "value",
                "table",
                "row",
                "rows",
                "pk",
                "set",
                "name",
                "columns",
                "type",
                "primary_key",
                "indexes",
                "limit",
                "cursor",
                "prefix",
                "start",
                "end",
                "eq",
                "range",
                "filter",
                "index",
                "encoding",
                "isolation",
                "idempotency_key",
                "blob_id",
                "by",
                "exists",
                "version",
                "email",
                "password",
                "db",
                "read_only",
                "upsert",
                "reverse",
                "col",
                "and",
                "or",
                "not",
                "ttl_ms",
                "where",
                "after",
                "wait_ms",
                "org",
                "role",
                "token",
                "current_password",
                "new_password",
                "from",
                "to",
                "group_by",
                "aggregates",
                "fn",
                "of",
                "path",
                "lookup",
                "bucket",
                "interval",
                "then",
                "order_by",
                "time_column",
                "partition",
            ];
            let mut m = serde_json::Map::new();
            for _ in 0..r.usize(5) {
                m.insert(keys[r.usize(keys.len())].to_string(), rand_json(r, depth + 1));
            }
            Value::Object(m)
        }
    }
}

/// Valid request bodies that mutation starts from.
fn seeds() -> Vec<(&'static str, &'static str, Value)> {
    vec![
        (
            "POST",
            "/v1/dbs/fz/txn",
            json!({"ops": [{"op": "put", "key": "a", "value": "1"}, {"op": "get", "key": "a"},
            {"op": "scan", "prefix": "", "limit": 3}, {"op": "increment", "key": "n", "by": 2},
            {"op": "check", "key": "a", "exists": true}, {"op": "delete_range", "prefix": "zz"},
            {"op": "copy", "from": "a", "to": "b", "ttl_ms": 1000}]}),
        ),
        ("POST", "/v1/dbs/fz/txn", json!({"ops": [{"op": "copy", "from": "missing", "to": "c"}]})),
        (
            "POST",
            "/v1/dbs/fz/txn",
            json!({"ops": [{"op": "query_rows", "table": "t", "index": "by_name", "eq": ["x"],
            "range": {"gte": 1}, "filter": {"col": "n", "op": "gt", "value": 1}, "limit": 5}], "encoding": "base64"}),
        ),
        (
            "POST",
            "/v1/dbs/fz/tables",
            json!({"name": "t2", "columns": [{"name": "id", "type": "int", "nullable": false}],
            "primary_key": ["id"], "indexes": [{"name": "i", "columns": ["id"], "unique": true}]}),
        ),
        ("POST", "/v1/dbs/fz/tables/t/rows", json!({"rows": [{"id": 1, "name": "x", "n": 2}], "upsert": true})),
        ("POST", "/v1/dbs/fz/tables/t/rows/query", json!({"index": "by_name", "eq": ["x"], "limit": 2})),
        ("POST", "/v1/dbs/fz/tables/t/rows/update", json!({"pk": 1, "set": {"n": 3}, "ttl_ms": 60_000})),
        (
            "POST",
            "/v1/dbs/fz/tables/t/rows/update",
            json!({"where": {"index": "by_name", "eq": ["x"], "filter": {"col": "n", "op": "gt", "value": 1}},
            "set": {"n": 4}, "limit": 2}),
        ),
        ("POST", "/v1/dbs/fz/tables/t/rows/delete", json!({"where": {"range": {"gte": 5}}, "limit": 1})),
        (
            "POST",
            "/v1/dbs/fz/txn",
            json!({"ops": [{"op": "put", "key": "e", "value": "1", "ttl_ms": 1000},
            {"op": "upsert_row", "table": "t", "row": {"id": 9, "name": "z"}, "ttl_ms": 5000},
            {"op": "update_rows", "table": "t", "where": {"index": "by_name", "range": {"gte": "a"}}, "set": {"n": 1}},
            {"op": "delete_rows", "table": "t", "where": {"filter": {"not": {"col": "n", "op": "exists"}}}, "limit": 3}]}),
        ),
        ("POST", "/v1/dbs/fz/tables/t/rows/get", json!({"pk": [1]})),
        ("POST", "/v1/dbs/fz/transactions", json!({"isolation": "snapshot"})),
        ("POST", "/v1/dbs", json!({"name": "other"})),
        ("POST", "/v1/api-keys", json!({"name": "k", "db": "fz", "read_only": true})),
        ("POST", "/v1/auth/login", json!({"email": "f@z.io", "password": "password-fz"})),
        ("POST", "/v1/dbs/fz/tables/t/columns", json!({"name": "c", "type": "json"})),
        ("POST", "/v1/orgs", json!({"name": "fz-other"})),
        ("POST", "/v1/orgs/fz-org/members", json!({"email": "nobody@z.io", "role": "writer"})),
        ("POST", "/v1/orgs/fz-org/members/nobody", json!({"role": "admin"})),
        ("POST", "/v1/dbs", json!({"name": "d2", "org": "fz-org"})),
        ("POST", "/v1/dbs/fz-org.d/txn", json!({"ops": [{"op": "put", "key": "a", "value": "1"}]})),
        ("POST", "/v1/me/password", json!({"current_password": "not-the-password", "new_password": "password-new"})),
        ("POST", "/v1/auth/password-reset", json!({"email": "f@z.io"})),
        ("POST", "/v1/auth/password-reset/confirm", json!({"token": "AAAA", "password": "password-new"})),
        ("POST", "/v1/auth/verify-email", json!({"token": "cmVzZXQ6dTE6eA"})),
        ("POST", "/v1/dbs/fz/streams", json!({"name": "s2", "retention_ms": 60_000, "max_bytes": 10_000})),
        (
            "POST",
            "/v1/dbs/fz/streams/s/records",
            json!({"records": [{"json": {"lvl": "error", "n": 3}, "headers": {"h": "v"}}, {"value": "x"}],
            "expect_seq": 0, "create": true, "idempotency_key": "k", "encoding": "utf8"}),
        ),
        (
            "POST",
            "/v1/dbs/fz/streams/s/records/query",
            json!({"from_seq": 0, "until_seq": 9, "reverse": true, "limit": 5, "scan_limit": 10, "max_bytes": 100,
            "filter": {"and": [{"field": "lvl", "op": "eq", "value": "error"}, {"not": {"header": "h", "op": "exists"}},
                {"op": "contains", "value": "x"}]}}),
        ),
        ("POST", "/v1/dbs/fz/streams/s/trim", json!({"before_seq": 1})),
        ("PUT", "/v1/dbs/fz/streams/s/consumers/c", json!({"seq": 2})),
        ("PATCH", "/v1/dbs/fz/streams/s", json!({"retention_ms": null, "max_bytes": 5})),
        (
            "POST",
            "/v1/dbs/fz/events",
            json!({"name": "e2", "columns": [{"name": "t", "type": "time"}, {"name": "j", "type": "json"}],
            "time_column": "t", "order_by": ["t"], "partition": "hour", "retention_ms": 60_000}),
        ),
        (
            "POST",
            "/v1/dbs/fz/events/e/rows",
            json!({"rows": [{"kind": "a", "n": 1, "props": {"x": [1], "y": {"z": "w"}}}, {"time": "2025-01-01T00:00:00Z", "kind": "b"}],
            "idempotency_key": "ev"}),
        ),
        (
            "POST",
            "/v1/dbs/fz/events/e/rows/query",
            json!({"from": 0, "to": "2100-01-01", "where": {"or": [{"col": "kind", "op": "in", "value": ["a"]},
                {"col": "props", "path": ["y", "z"], "op": "prefix", "value": "w"}]},
            "lookups": {"p": {"table": "people", "key": {"col": "kind"}, "value": "who", "default": {"col": "kind"}}},
            "group_by": [{"bucket": {"col": "time"}, "interval": "1d", "offset": "+02:00"}, {"lookup": "p"}],
            "aggregates": {"n": {"fn": "count"}, "u": {"fn": "uniq", "of": "kind"}, "q": {"fn": "quantiles", "of": "n", "q": [0.5]},
                "f": {"fn": "window_funnel", "window_ms": 1000, "steps": [{"col": "kind", "op": "eq", "value": "a"}], "mode": "strict_order"},
                "r": {"fn": "retention", "conditions": [{"col": "n", "op": "exists"}]}},
            "then": {"group_by": ["p"], "aggregates": {"s": {"fn": "sum", "of": "r"}}}, "order_by": [{"col": "s", "desc": true}], "limit": 3}),
        ),
        (
            "POST",
            "/v1/dbs/fz/events/e/rows/query",
            json!({"from": 0, "to": 9_000_000_000_000i64, "columns": ["time", "kind", "props", {"col": "props", "path": ["x"], "as": "x"}],
            "order_by": [{"col": "kind", "desc": true}], "limit": 2, "cursor": null, "consistency": "strong"}),
        ),
        ("POST", "/v1/dbs/fz/events/e/rows/delete", json!({"from": 0, "where": {"col": "n", "op": "ge", "value": 5}})),
        ("POST", "/v1/dbs/fz/events/e/columns", json!({"name": "extra", "type": "float", "default": 1.5})),
        ("POST", "/v1/dbs/fz/events/e/skip-indexes", json!({"column": "kind", "type": "bloom"})),
        ("PATCH", "/v1/dbs/fz/events/e", json!({"retention_ms": 3_600_000})),
        (
            "POST",
            "/v1/dbs/fz/txn",
            json!({"ops": [{"op": "insert_events", "table": "e", "rows": [{"kind": "c", "n": 3}]},
            {"op": "query_events", "table": "e", "from": 0, "to": 9_000_000_000_000i64, "aggregates": {"n": {"fn": "count"}}},
            {"op": "delete_events", "table": "e", "where": {"col": "kind", "op": "eq", "value": "zz"}}]}),
        ),
        (
            "POST",
            "/v1/dbs/fz/txn",
            json!({"ops": [{"op": "append", "stream": "s", "records": [{"value": "y"}], "expect_seq": 1},
            {"op": "read_stream", "stream": "s", "tail": 3, "filter": {"field": "n", "op": "gt", "value": 1}},
            {"op": "stream_info", "stream": "s"}, {"op": "trim_stream", "stream": "s", "before_seq": 1},
            {"op": "set_consumer", "stream": "s", "consumer": "c", "seq": 1},
            {"op": "get_consumer", "stream": "s", "consumer": "c"}]}),
        ),
    ]
}

fn mutate(r: &mut Rng, v: &Value) -> Value {
    let mut v = v.clone();
    for _ in 0..1 + r.usize(3) {
        mutate_once(r, &mut v, 0);
    }
    v
}

fn mutate_once(r: &mut Rng, v: &mut Value, depth: u32) {
    match v {
        Value::Object(m) if !m.is_empty() && r.chance(0.7) && depth < 6 => {
            let keys: Vec<String> = m.keys().cloned().collect();
            let k = keys[r.usize(keys.len())].clone();
            match r.below(4) {
                0 => {
                    m.remove(&k);
                }
                1 => {
                    m.insert(k, rand_json(r, 2));
                }
                _ => {
                    if let Some(x) = m.get_mut(&k) {
                        mutate_once(r, x, depth + 1);
                    }
                }
            }
        }
        Value::Array(a) if !a.is_empty() && r.chance(0.7) && depth < 6 => {
            let i = r.usize(a.len());
            if r.chance(0.3) {
                a.remove(i);
            } else {
                mutate_once(r, &mut a[i], depth + 1);
            }
        }
        _ => *v = rand_json(r, 2),
    }
}

#[test]
fn fuzz_http_api() {
    let iters = baserunna::util::prop::env_u64("FUZZ_ITERS").unwrap_or(1500) as usize;
    let seed = baserunna::util::prop::env_u64("FUZZ_SEED").unwrap_or(77);
    let out = run(seed, SimConfig::basic(), move |sim| async move {
        let c = Client::signup(&sim, "fuzzer", "f@z.io", "password-fz").await?;
        c.create_db("fz").await?;
        c.call_retry(
            "POST",
            "/v1/dbs/fz/tables",
            Some(json!({"name": "t", "primary_key": ["id"],
            "columns": [{"name": "id", "type": "int", "nullable": false}, {"name": "name", "type": "string"},
                        {"name": "n", "type": "int"}, {"name": "b", "type": "blob"}],
            "indexes": [{"name": "by_name", "columns": ["name"]}]})),
        )
        .await;
        c.call_retry("POST", "/v1/dbs/fz/streams", Some(json!({"name": "s"}))).await;
        c.call_retry(
            "POST",
            "/v1/dbs/fz/events",
            Some(json!({"name": "e", "time_column": "time", "order_by": ["kind", "time"],
            "columns": [{"name": "time", "type": "time", "default": "now"}, {"name": "kind", "type": "string"},
                        {"name": "n", "type": "int"}, {"name": "props", "type": "json"}],
            "skip_indexes": [{"column": "n", "type": "bloom"}]})),
        )
        .await;
        c.call_retry(
            "POST",
            "/v1/dbs/fz/tables",
            Some(json!({"name": "people", "primary_key": ["id"],
            "columns": [{"name": "id", "type": "string", "nullable": false}, {"name": "who", "type": "string"}]})),
        )
        .await;
        baserunna::sim::workloads::steps::create_org(&c, "fz-org").await?;
        c.create_db_owned(Some("fz-org"), "d").await?;
        let token = c.token.clone().unwrap_or_default();
        let mut r = Rng::new(seed);
        let mut hist: std::collections::BTreeMap<u16, usize> = std::collections::BTreeMap::new();
        let seeds = seeds();
        let paths = [
            "/v1/dbs/fz/kv/",
            "/v1/dbs/fz/kv",
            "/v1/dbs/fz/changes?after=",
            "/v1/dbs/fz/tables/t/changes?wait_ms=1&after=",
            "/v1/dbs/fz/txn",
            "/v1/dbs/fz/tables/t/rows/query",
            "/v1/dbs/fz/transactions/",
            "/v1/dbs/fz/commits/",
            "/v1/dbs/fz/blobs/",
            "/v1/dbs/",
            "/v1/api-keys/",
            "/v1/admin/users/",
            "/v1/me",
            "/v1/me/sessions/",
            "/v1/orgs/",
            "/v1/orgs/fz-org/members/",
            "/v1/dbs/fz-org.",
            "/v1/dbs?org=",
            "/v1/auth/google/callback?state=",
            "/v1/dbs/fz/tables/",
            "/v1/dbs/fz/streams/",
            "/v1/dbs/fz/streams/s/records?from_seq=",
            "/v1/dbs/fz/streams/s/records?format=sse&wait_ms=20&tail=",
            "/v1/dbs/fz/streams/s/records?format=ndjson&reverse=true&until_seq=",
            "/v1/dbs/fz/streams/s/consumers/",
            "/console/dbs/fz",
            "/console/dbs/fz/streams/",
            "/v1/dbs/fz/events/",
            "/v1/dbs/fz/events/e/",
            "/console/dbs/fz/events/",
            "/internal/v1/meta",
            "/internal/v1/events/bind",
        ];
        for i in 0..iters {
            let (method, path, body) = if r.chance(0.6) {
                let (m, p, b) = &seeds[r.usize(seeds.len())];
                let b = if r.chance(0.35) { b.clone() } else { mutate(&mut r, b) };
                (m.to_string(), p.to_string(), b.to_string())
            } else {
                let m = ["GET", "POST", "PUT", "DELETE", "PATCH"][r.usize(5)].to_string();
                let p = format!(
                    "{}{}",
                    paths[r.usize(paths.len())],
                    rand_string(&mut r)
                        .bytes()
                        .map(|b| if b.is_ascii_alphanumeric() { (b as char).to_string() } else { format!("%{b:02X}") })
                        .collect::<String>()
                );
                let body = if r.chance(0.3) {
                    let n = r.usize(40);
                    String::from_utf8_lossy(&r.bytes(n)).into_owned()
                } else {
                    rand_json(&mut r, 0).to_string()
                };
                (m, p, body)
            };
            let mut b = http::Request::builder().method(method.as_str()).uri(path.as_str());
            if r.chance(0.9) {
                b = b.header("authorization", format!("Bearer {token}"));
            }
            if r.chance(0.2) {
                b = b.header("range", ["bytes=0-0", "bytes=-1", "bytes=9-1", "bytes=x", "bytes=0-1,2-3"][r.usize(5)]);
            }
            if r.chance(0.1) {
                b = b.header("if-match", "\"abc\"");
            }
            if r.chance(0.15) {
                let ct = ["application/x-ndjson", "text/plain", "application/json", "image/png"][r.usize(4)];
                b = b.header("content-type", ct);
            }
            if r.chance(0.05) {
                b = b.header("last-event-id", ["0", "x", "18446744073709551615"][r.usize(3)]);
            }
            let Ok(req) = b.body(axum::body::Body::from(body.clone())) else { continue };
            let resp = sim.call_node(0, req).await.map_err(|e| format!("network error {e:?}"))?;
            *hist.entry(resp.status).or_default() += 1;
            if resp.status >= 500 && resp.status != 503 {
                return Err(format!(
                    "iteration {i}: {method} {path} -> {}\nbody: {body}\nresponse: {}",
                    resp.status,
                    String::from_utf8_lossy(&resp.body)
                ));
            }
            if openapi_check::match_path(openapi_check::doc(), &path).is_some()
                && openapi_check::doc()["paths"][openapi_check::match_path(openapi_check::doc(), &path).unwrap().0]
                    .get(method.to_ascii_lowercase())
                    .is_some()
            {
                openapi_check::check_response(&method, &path, &resp).map_err(|e| {
                    format!(
                        "iteration {i}: {method} {path}: {e}\nbody: {body}\nresponse: {}",
                        String::from_utf8_lossy(&resp.body)
                    )
                })?;
            }
        }
        if std::env::var("FUZZ_VERBOSE").is_ok() {
            eprintln!("status histogram: {hist:?}");
        }
        // Ensure enough requests pass validation to exercise handlers.
        let ok = hist.iter().filter(|(s, _)| **s < 300).map(|(_, n)| n).sum::<usize>();
        if ok * 10 < iters {
            return Err(format!("too few successful fuzz requests: {hist:?}"));
        }
        let r = c.call_retry("POST", "/v1/dbs/fz/txn", Some(json!({"ops": [{"op": "get", "key": "a"}]}))).await;
        if r.status != 200 {
            return Err(format!("service broken after fuzzing: {}", r.json));
        }
        Ok(())
    });
    assert!(out.result.is_ok(), "{:?}", out.result);
}

/// Structure-aware fuzzing of the transaction-op language against an exact
/// model. Scale with `FUZZ_ITERS`; `FUZZ_VERBOSE` prints response counts per
/// route and status, and `FUZZ_DEBUG` includes error messages.
#[test]
fn fuzz_txn_ops_against_model() {
    let iters = baserunna::util::prop::env_u64("FUZZ_ITERS").unwrap_or(400);
    let base = baserunna::util::prop::env_u64("FUZZ_SEED").unwrap_or(1);
    for i in 0..iters {
        let seed = base.wrapping_add(i);
        let mut r = Rng::new(seed);
        let n = r.usize(600);
        let data = r.bytes(n);
        if let Err(e) = baserunna::sim::txn_fuzz::run_input(&data) {
            panic!("{e}\nreproduce with: FUZZ_SEED={seed} FUZZ_ITERS=1 cargo test --test fuzz fuzz_txn_ops");
        }
    }
    if std::env::var("FUZZ_VERBOSE").is_ok() {
        for (k, n) in baserunna::sim::txn_fuzz::stats() {
            eprintln!("{n:>7} {k}");
        }
    }
}
