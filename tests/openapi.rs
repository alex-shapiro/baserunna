//! The OpenAPI document is well formed, every reference can be resolved, and the
//! documented routes are exactly the router's public routes.

use baserunna::sim::SimResponse;
use baserunna::sim::openapi_check::{check_response, doc, match_path, validate};
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;

fn refs(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Object(m) => {
            for (k, x) in m {
                if k == "$ref"
                    && let Some(s) = x.as_str()
                {
                    out.push(s.to_string());
                }
                refs(x, out);
            }
        }
        Value::Array(a) => a.iter().for_each(|x| refs(x, out)),
        _ => {}
    }
}

/// `openapi.json` at the repository root is the published copy of the
/// document; regenerate with `cargo run --example gen_openapi`.
#[test]
fn checked_in_document_is_current() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("openapi.json");
    let on_disk = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        on_disk == baserunna::api::openapi::document_json(),
        "openapi.json is stale: run `cargo run --example gen_openapi`"
    );
}

#[test]
fn document_is_well_formed() {
    let d = doc();
    assert_eq!(d["openapi"], "3.1.0");
    let mut rs = Vec::new();
    refs(d, &mut rs);
    assert!(!rs.is_empty());
    for r in rs {
        let ptr = r.trim_start_matches('#');
        assert!(d.pointer(ptr).is_some(), "dangling $ref {r}");
    }
    for (p, item) in d["paths"].as_object().unwrap() {
        for (m, op) in item.as_object().unwrap() {
            assert!(op["responses"].is_object(), "{m} {p} has no responses");
            assert!(op["summary"].is_string(), "{m} {p} has no summary");
        }
    }
}

/// Public routes registered in the router, from the source.
fn router_routes() -> BTreeSet<(String, String)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut out = BTreeSet::new();
    let mut files: Vec<_> = std::fs::read_dir(root.join("src/api")).unwrap().map(|e| e.unwrap().path()).collect();
    files.sort();
    for f in files.iter().filter(|f| f.extension().is_some_and(|x| x == "rs")) {
        let text = std::fs::read_to_string(f).unwrap();
        let text = text.split("\n#[cfg(test)]").next().unwrap();
        let consts: HashMap<&str, &str> = text
            .lines()
            .filter_map(|l| l.split_once("const ")?.1.split_once(": &str = \""))
            .map(|(name, v)| (name, v.trim_end_matches("\";")))
            .collect();
        for chunk in text.split(".route(").skip(1) {
            let (arg, rest) = chunk.split_once(',').unwrap_or((chunk, ""));
            let arg = arg.trim();
            let Some(path) =
                arg.strip_prefix('"').map(|a| a.trim_end_matches('"')).or_else(|| consts.get(arg).copied())
            else {
                continue;
            };
            let path = path.replace("{*key}", "{key}");
            if path.starts_with("/internal") {
                continue;
            }
            let rest: String = rest.chars().take_while(|c| *c != '\n').collect();
            for m in ["get", "post", "put", "patch", "delete"] {
                if rest.contains(&format!("{m}(")) {
                    out.insert((path.clone(), m.to_string()));
                }
            }
        }
    }
    out
}

#[test]
fn documented_routes_match_router() {
    let d = doc();
    let mut documented = BTreeSet::new();
    for (p, item) in d["paths"].as_object().unwrap() {
        for m in item.as_object().unwrap().keys() {
            documented.insert((p.clone(), m.clone()));
        }
    }
    let routed = router_routes();
    assert!(routed.len() >= 40, "route parser found only {} routes", routed.len());
    let missing: Vec<_> = routed.difference(&documented).collect();
    let extra: Vec<_> = documented.difference(&routed).collect();
    assert!(missing.is_empty(), "routes not documented: {missing:?}");
    assert!(extra.is_empty(), "documented routes that do not exist: {extra:?}");
}

fn resp(status: u16, body: Value) -> SimResponse {
    let mut h = http::HeaderMap::new();
    h.insert("content-type", "application/json".parse().unwrap());
    SimResponse { status, headers: h, body: bytes::Bytes::from(body.to_string()) }
}

#[test]
fn validator_catches_drift() {
    let db = |id: Value, extra: Option<(&str, Value)>| {
        let mut d = json!({"id": id, "name": "b", "org": null, "created_at": 1});
        if let Some((k, v)) = extra {
            d[k] = v;
        }
        resp(201, d)
    };
    assert!(check_response("POST", "/v1/dbs", &db(json!("a"), None)).is_ok());
    assert!(check_response("POST", "/v1/dbs", &db(json!(1), None)).is_err(), "a mistyped field");
    assert!(check_response("POST", "/v1/dbs", &db(json!("a"), Some(("x", json!(1))))).is_err(), "an extra field");
    let err = json!({"error": {"code": "not_found", "message": "m", "retryable": false}});
    assert!(check_response("GET", "/v1/dbs/x/kv/a%2Fb", &resp(404, err.clone())).is_ok());
    let bad = json!({"error": {"code": "nope", "message": "m", "retryable": false}});
    assert!(check_response("GET", "/v1/dbs/x", &resp(404, bad)).is_err());
    assert!(check_response("GET", "/v1/nope", &resp(404, err)).is_err());
    assert_eq!(match_path(doc(), "/v1/dbs/a/tables/t/rows/get").unwrap().0, "/v1/dbs/{db}/tables/{t}/rows/get");
    assert!(validate(doc(), &json!({"type": "integer"}), &json!(1.5), "$").is_err());
}

/// Every op kind, with every field the server accepts, validates against the
/// documented `Op` schema, and fields the server refuses do not.
#[test]
fn op_schema_matches_the_ops_the_server_accepts() {
    let f = json!({"col": "a", "op": "eq", "value": 1});
    let accepted = [
        json!({"op": "get", "key": "k"}),
        json!({"op": "scan", "prefix": "p", "start": "a", "end": "z", "limit": 10, "reverse": true,
            "keys_only": true, "cursor": "c"}),
        json!({"op": "put", "key": "k", "value": "v", "ttl_ms": 1}),
        json!({"op": "delete", "key": "k"}),
        json!({"op": "copy", "from": "a", "to": "b", "ttl_ms": 1}),
        json!({"op": "delete_range", "prefix": "p"}),
        json!({"op": "check", "key": "k", "exists": true, "version": 1, "value": "v"}),
        json!({"op": "increment", "key": "k", "by": 2, "ttl_ms": 1}),
        json!({"op": "get_row", "table": "t", "pk": 1}),
        json!({"op": "insert_row", "table": "t", "row": {"a": 1}, "ttl_ms": 1}),
        json!({"op": "upsert_row", "table": "t", "row": {"a": 1}}),
        json!({"op": "update_row", "table": "t", "pk": 1, "set": {"a": 2}}),
        json!({"op": "delete_row", "table": "t", "pk": 1}),
        json!({"op": "update_rows", "table": "t", "where": {"filter": f}, "set": {"a": 2}, "limit": 5}),
        json!({"op": "delete_rows", "table": "t", "where": {}, "limit": 5}),
        json!({"op": "query_rows", "table": "t", "index": "i", "eq": [1], "range": {"gt": 1}, "filter": f,
            "limit": 10, "reverse": true, "cursor": "c"}),
        json!({"op": "append", "stream": "s", "records": [{"value": "x"}], "expect_seq": 1, "create": true}),
        json!({"op": "read_stream", "stream": "s", "from_seq": 1, "until_seq": 9, "reverse": false, "limit": 5,
            "max_bytes": 100, "filter": {"field": "a", "op": "eq", "value": 1}, "scan_limit": 10}),
        json!({"op": "stream_info", "stream": "s"}),
        json!({"op": "trim_stream", "stream": "s", "before_seq": 3}),
        json!({"op": "get_consumer", "stream": "s", "consumer": "c"}),
        json!({"op": "set_consumer", "stream": "s", "consumer": "c", "seq": 1}),
        json!({"op": "insert_events", "table": "e", "rows": [{"time": 1}]}),
        json!({"op": "query_events", "table": "e", "from": 0, "to": "2026-01-01T00:00:00Z", "where": f,
            "group_by": ["a"], "aggregates": {"n": {"fn": "count"}}, "order_by": ["n"], "limit": 5}),
        json!({"op": "query_events", "table": "e", "from": 0, "to": 1, "columns": ["a"], "cursor": "c"}),
        json!({"op": "delete_events", "table": "e", "from": 0, "to": 1, "where": f}),
    ];
    let refused = [
        json!({"op": "query_rows", "table": "t", "consistency": "strong"}),
        json!({"op": "read_stream", "stream": "s", "encoding": "base64"}),
        json!({"op": "query_events", "table": "e", "from": 0, "to": 1, "consistency": "strong"}),
        json!({"op": "query_events", "table": "e", "from": 0, "to": 1, "nope": 1}),
    ];
    let schema = json!({"$ref": "#/components/schemas/Op"});
    for op in accepted {
        serde_json::from_value::<baserunna::ops::Op>(op.clone()).unwrap_or_else(|e| panic!("server refuses {op}: {e}"));
        validate(doc(), &schema, &op, "$").unwrap_or_else(|e| panic!("schema refuses {op}: {e}"));
    }
    for op in refused {
        assert!(serde_json::from_value::<baserunna::ops::Op>(op.clone()).is_err(), "server accepts {op}");
        assert!(validate(doc(), &schema, &op, "$").is_err(), "schema accepts {op}");
    }
}
