//! OpenAPI response validator and route coverage tracker.

use super::SimResponse;
use serde_json::Value as Json;

pub fn doc() -> &'static Json {
    crate::api::openapi::document()
}

fn resolve<'a>(root: &'a Json, s: &'a Json) -> &'a Json {
    let mut cur = s;
    for _ in 0..16 {
        match cur.get("$ref").and_then(|r| r.as_str()) {
            Some(r) => {
                let ptr = r.trim_start_matches('#');
                match root.pointer(ptr) {
                    Some(t) => cur = t,
                    None => return cur,
                }
            }
            None => return cur,
        }
    }
    cur
}

fn type_ok(t: &str, v: &Json) -> bool {
    match t {
        "null" => v.is_null(),
        "boolean" => v.is_boolean(),
        "integer" => v.is_i64() || v.is_u64(),
        "number" => v.is_number(),
        "string" => v.is_string(),
        "array" => v.is_array(),
        "object" => v.is_object(),
        _ => true,
    }
}

pub fn validate(root: &Json, schema: &Json, v: &Json, path: &str) -> Result<(), String> {
    let s = resolve(root, schema);
    if s.as_bool() == Some(true) || s.as_object().is_some_and(|o| o.is_empty()) {
        return Ok(());
    }
    if let Some(t) = s.get("type") {
        let ok = match t {
            Json::String(t) => type_ok(t, v),
            Json::Array(ts) => ts.iter().filter_map(|t| t.as_str()).any(|t| type_ok(t, v)),
            _ => true,
        };
        if !ok {
            return Err(format!("{path}: expected type {t}, got {v}"));
        }
    }
    if let Some(e) = s.get("enum").and_then(|e| e.as_array())
        && !e.contains(v)
    {
        return Err(format!("{path}: {v} not in enum {e:?}"));
    }
    if let Some(alts) = s.get("oneOf").or_else(|| s.get("anyOf")).and_then(|a| a.as_array()) {
        let errs: Vec<String> = alts.iter().filter_map(|a| validate(root, a, v, path).err()).collect();
        if errs.len() == alts.len() {
            return Err(format!("{path}: matches no alternative: {errs:?}"));
        }
    }
    if let Some(o) = v.as_object() {
        if let Some(req) = s.get("required").and_then(|r| r.as_array()) {
            for r in req.iter().filter_map(|r| r.as_str()) {
                if !o.contains_key(r) {
                    return Err(format!("{path}: missing required property {r:?}"));
                }
            }
        }
        let props = s.get("properties").and_then(|p| p.as_object());
        for (k, val) in o {
            match props.and_then(|p| p.get(k)) {
                Some(ps) => validate(root, ps, val, &format!("{path}.{k}"))?,
                None => match s.get("additionalProperties") {
                    Some(Json::Bool(false)) => return Err(format!("{path}: unexpected property {k:?}")),
                    Some(ap) if ap.is_object() => validate(root, ap, val, &format!("{path}.{k}"))?,
                    _ => {}
                },
            }
        }
    }
    if let (Some(items), Some(arr)) = (s.get("items"), v.as_array()) {
        for (i, it) in arr.iter().enumerate() {
            validate(root, items, it, &format!("{path}[{i}]"))?;
        }
    }
    Ok(())
}

static COVERAGE: std::sync::Mutex<std::collections::BTreeSet<(String, String)>> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());

pub fn record_coverage(method: &str, path: &str) {
    if let Some((tpl, item)) = match_path(doc(), path) {
        let m = method.to_ascii_lowercase();
        if item.get(&m).is_some() {
            COVERAGE.lock().unwrap_or_else(|e| e.into_inner()).insert((tpl.to_string(), m));
        }
    }
}

pub fn coverage() -> std::collections::BTreeSet<(String, String)> {
    COVERAGE.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

/// Returns all `(path_template, method)` pairs in the OpenAPI document.
pub fn documented_operations() -> std::collections::BTreeSet<(String, String)> {
    let mut out = std::collections::BTreeSet::new();
    for (p, item) in doc()["paths"].as_object().into_iter().flatten() {
        for m in item.as_object().into_iter().flatten().map(|(m, _)| m) {
            out.insert((p.clone(), m.clone()));
        }
    }
    out
}

pub fn match_path<'a>(root: &'a Json, path: &str) -> Option<(&'a str, &'a Json)> {
    let path = path.split('?').next().unwrap_or(path);
    let segs: Vec<&str> = path.trim_end_matches('/').split('/').collect();
    let paths = root.get("paths")?.as_object()?;
    let mut best: Option<(&str, &Json, usize)> = None;
    for (tpl, item) in paths {
        let tsegs: Vec<&str> = tpl.split('/').collect();
        let wildcard = tsegs.last().is_some_and(|l| l.starts_with("{*") || l.ends_with("+}"));
        let ok = if wildcard {
            segs.len() >= tsegs.len()
                && tsegs[..tsegs.len() - 1]
                    .iter()
                    .zip(&segs)
                    .all(|(t, s)| (t.starts_with('{') && !s.is_empty()) || t == s)
        } else {
            segs.len() == tsegs.len()
                && tsegs.iter().zip(&segs).all(|(t, s)| (t.starts_with('{') && !s.is_empty()) || t == s)
        };
        if ok {
            let literal = tsegs.iter().filter(|t| !t.starts_with('{')).count();
            if best.is_none_or(|b| literal > b.2) {
                best = Some((tpl.as_str(), item, literal));
            }
        }
    }
    best.map(|(t, i, _)| (t, i))
}

/// Validates a `/v1/` JSON response against the OpenAPI schema for its route, method, and status.
pub fn check_response(method: &str, path: &str, resp: &SimResponse) -> Result<(), String> {
    let p = path.split('?').next().unwrap_or(path);
    if !p.starts_with("/v1/") {
        return Ok(());
    }
    let root = doc();
    if root.get("paths").and_then(|p| p.as_object()).is_none_or(|p| p.is_empty()) {
        return Ok(());
    }
    let (tpl, item) = match_path(root, p).ok_or_else(|| format!("path {p} not documented"))?;
    let op =
        item.get(method.to_ascii_lowercase()).ok_or_else(|| format!("method {method} not documented for {tpl}"))?;
    let status = resp.status.to_string();
    let responses = op.get("responses").ok_or("no responses")?;
    let r = responses
        .get(&status)
        .or_else(|| responses.get("default"))
        .ok_or_else(|| format!("status {status} not documented for {method} {tpl}"))?;
    let r = resolve(root, r);
    let ct = resp.headers.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("");
    if !ct.starts_with("application/json") {
        return Ok(());
    }
    let Some(schema) = r.pointer("/content/application~1json/schema") else {
        return Err(format!("{method} {tpl} {status}: JSON body not documented"));
    };
    let v: Json = serde_json::from_slice(&resp.body).map_err(|e| format!("invalid JSON: {e}"))?;
    validate(root, schema, &v, "$")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn validator_basics() {
        let root = json!({"components": {"schemas": {"X": {"type": "object", "required": ["a"],
            "properties": {"a": {"type": "integer"}, "b": {"type": ["string", "null"]}},
            "additionalProperties": false}}}});
        let s = json!({"$ref": "#/components/schemas/X"});
        assert!(validate(&root, &s, &json!({"a": 1}), "$").is_ok());
        assert!(validate(&root, &s, &json!({"a": 1, "b": null}), "$").is_ok());
        assert!(validate(&root, &s, &json!({"a": "x"}), "$").is_err());
        assert!(validate(&root, &s, &json!({}), "$").is_err());
        assert!(validate(&root, &s, &json!({"a": 1, "c": 2}), "$").is_err());
        let one = json!({"oneOf": [{"type": "string"}, {"type": "integer"}]});
        assert!(validate(&root, &one, &json!(3), "$").is_ok());
        assert!(validate(&root, &one, &json!([]), "$").is_err());
    }
}
