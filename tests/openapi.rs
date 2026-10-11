//! OpenAPI document checks: well-formed, every reference resolves, and the
//! documented routes are exactly the router's public routes. (Every
//! simulated response is additionally validated against the document by the
//! simulation client.)

use baserunna::sim::SimResponse;
use baserunna::sim::openapi_check::{check_response, doc, match_path, validate};
use serde_json::{Value, json};
use std::collections::BTreeSet;
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
    for f in ["src/api/mod.rs", "src/api/account.rs", "src/api/data.rs"] {
        let text = std::fs::read_to_string(root.join(f)).unwrap();
        for chunk in text.split(".route(\"").skip(1) {
            let path = chunk.split('"').next().unwrap().replace("{*key}", "{key}");
            if path.starts_with("/internal") {
                continue;
            }
            let rest: String = chunk.split('"').nth(1).unwrap_or("").chars().take_while(|c| *c != '\n').collect();
            for m in ["get", "post", "put", "delete"] {
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
    assert!(check_response("POST", "/v1/dbs", &resp(201, json!({"id": "a", "name": "b", "created_at": 1}))).is_ok());
    assert!(check_response("POST", "/v1/dbs", &resp(201, json!({"id": 1, "name": "b", "created_at": 1}))).is_err());
    assert!(
        check_response("POST", "/v1/dbs", &resp(201, json!({"id": "a", "name": "b", "created_at": 1, "x": 1})))
            .is_err()
    );
    let err = json!({"error": {"code": "not_found", "message": "m", "retryable": false}});
    assert!(check_response("GET", "/v1/dbs/x/kv/a%2Fb", &resp(404, err.clone())).is_ok());
    let bad = json!({"error": {"code": "nope", "message": "m", "retryable": false}});
    assert!(check_response("GET", "/v1/dbs/x", &resp(404, bad)).is_err());
    assert!(check_response("GET", "/v1/nope", &resp(404, err)).is_err());
    assert_eq!(match_path(doc(), "/v1/dbs/a/tables/t/rows/get").unwrap().0, "/v1/dbs/{db}/tables/{t}/rows/get");
    assert!(validate(doc(), &json!({"type": "integer"}), &json!(1.5), "$").is_err());
}
