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
            {"op": "check", "key": "a", "exists": true}, {"op": "delete_range", "prefix": "zz"}]}),
        ),
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
        ("POST", "/v1/dbs/fz/tables/t/rows/update", json!({"pk": 1, "set": {"n": 3}})),
        ("POST", "/v1/dbs/fz/tables/t/rows/get", json!({"pk": [1]})),
        ("POST", "/v1/dbs/fz/transactions", json!({"isolation": "snapshot"})),
        ("POST", "/v1/dbs", json!({"name": "other"})),
        ("POST", "/v1/api-keys", json!({"name": "k", "db": "fz", "read_only": true})),
        ("POST", "/v1/auth/login", json!({"email": "f@z.io", "password": "password-fz"})),
        ("POST", "/v1/dbs/fz/tables/t/columns", json!({"name": "c", "type": "json"})),
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
        let token = c.token.clone().unwrap_or_default();
        let mut r = Rng::new(seed);
        let mut hist: std::collections::BTreeMap<u16, usize> = std::collections::BTreeMap::new();
        let seeds = seeds();
        let paths = [
            "/v1/dbs/fz/kv/",
            "/v1/dbs/fz/kv",
            "/v1/dbs/fz/txn",
            "/v1/dbs/fz/tables/t/rows/query",
            "/v1/dbs/fz/transactions/",
            "/v1/dbs/fz/commits/",
            "/v1/dbs/fz/blobs/",
            "/v1/dbs/",
            "/v1/api-keys/",
            "/v1/admin/users/",
            "/v1/me",
            "/v1/auth/google/callback?state=",
            "/v1/dbs/fz/tables/",
            "/console/dbs/fz",
            "/internal/v1/meta",
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
        // Enough requests must get past validation to exercise handlers.
        let ok = hist.iter().filter(|(s, _)| **s < 300).map(|(_, n)| n).sum::<usize>();
        if ok * 10 < iters {
            return Err(format!("too few successful fuzz requests: {hist:?}"));
        }
        // The service still works afterwards.
        let r = c.call_retry("POST", "/v1/dbs/fz/txn", Some(json!({"ops": [{"op": "get", "key": "a"}]}))).await;
        if r.status != 200 {
            return Err(format!("service broken after fuzzing: {}", r.json));
        }
        Ok(())
    });
    assert!(out.result.is_ok(), "{:?}", out.result);
}
