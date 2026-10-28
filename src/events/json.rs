//! JSON columns as paths (SPEC §5.12): objects are flattened into leaf
//! paths when written to a part and rebuilt when read.
//!
//! A leaf is a scalar, an array or an empty object. Nulls inside objects
//! are dropped when a row is inserted, so a path is either present with a
//! non-null leaf or absent, in unsealed rows and parts alike.

use serde_json::{Map, Value as Json};

/// Shared buckets per JSON column for paths that are not their own columns.
pub const BUCKETS: u8 = 32;

/// Prepares an inserted value: removes null members of objects
/// (recursively; array elements are kept), stores `-0.0` as `0.0` so that it
/// equals zero in every comparison, and rejects integers beyond the signed
/// 64-bit range, which queries could not compare exactly.
pub fn normalize(v: &mut Json) -> Result<(), String> {
    match v {
        Json::Object(o) => {
            o.retain(|_, x| !x.is_null());
            o.values_mut().try_for_each(normalize)
        }
        Json::Array(a) => a.iter_mut().try_for_each(normalize),
        Json::Number(n) if n.is_u64() && n.as_i64().is_none() => {
            Err(format!("{n} exceeds the signed 64-bit integer range; send it as a string"))
        }
        Json::Number(n) if n.as_f64() == Some(0.0) && n.is_f64() => {
            *v = Json::from(0.0);
            Ok(())
        }
        _ => Ok(()),
    }
}

/// The canonical text of a path: a JSON array of its segments.
pub fn path_key(segments: &[String]) -> String {
    serde_json::to_string(segments).unwrap_or_default()
}

pub fn path_segments(key: &str) -> Option<Vec<String>> {
    serde_json::from_str(key).ok()
}

/// Leaf paths of `o` as canonical keys, with their values.
pub fn flatten(o: &Map<String, Json>) -> Vec<(String, &Json)> {
    fn walk<'a>(o: &'a Map<String, Json>, prefix: &mut Vec<String>, out: &mut Vec<(String, &'a Json)>) {
        for (k, v) in o {
            prefix.push(k.clone());
            match v {
                Json::Null => {}
                Json::Object(m) if !m.is_empty() => walk(m, prefix, out),
                _ => out.push((path_key(prefix), v)),
            }
            prefix.pop();
        }
    }
    let mut out = Vec::new();
    walk(o, &mut Vec::new(), &mut out);
    out
}

/// The leaf at `path` in `o`, if present.
pub fn leaf<'a>(o: &'a Json, path: &[String]) -> Option<&'a Json> {
    let mut cur = o;
    for seg in path {
        cur = cur.as_object()?.get(seg)?;
    }
    match cur {
        Json::Null => None,
        Json::Object(m) if !m.is_empty() => None,
        v => Some(v),
    }
}

/// Rebuilds an object from leaf paths.
pub fn unflatten(leaves: impl IntoIterator<Item = (Vec<String>, Json)>) -> Map<String, Json> {
    fn insert(m: &mut Map<String, Json>, path: &[String], v: Json) {
        match path {
            [] => {}
            [last] => {
                m.insert(last.clone(), v);
            }
            [first, rest @ ..] => {
                if let Json::Object(inner) = m.entry(first.clone()).or_insert_with(|| Json::Object(Map::new())) {
                    insert(inner, rest, v);
                }
            }
        }
    }
    let mut root = Map::new();
    for (path, v) in leaves {
        insert(&mut root, &path, v);
    }
    root
}

/// The shared bucket of a path that is not a column of its own.
pub fn bucket(key: &str) -> u8 {
    (crate::util::bloom::key_hash(key.as_bytes()) % BUCKETS as u64) as u8
}

/// How the values of one path are stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum JType {
    Int,
    Float,
    Str,
    Bool,
    /// Mixed or non-scalar values, stored as JSON text.
    Dyn,
}

impl JType {
    pub fn of(v: &Json) -> JType {
        match v {
            Json::Number(n) if n.is_i64() => JType::Int,
            Json::Number(n) if n.is_f64() => JType::Float,
            Json::String(_) => JType::Str,
            Json::Bool(_) => JType::Bool,
            _ => JType::Dyn,
        }
    }

    /// The type of a path holding values of both types.
    pub fn join(self, other: JType) -> JType {
        if self == other { self } else { JType::Dyn }
    }

    pub fn tag(self) -> u8 {
        self as u8
    }

    pub fn from_tag(t: u8) -> Option<JType> {
        [JType::Int, JType::Float, JType::Str, JType::Bool, JType::Dyn].get(t as usize).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn flatten_then_unflatten_roundtrips() {
        let mut v = json!({"a": {"b": 1, "c": {"d": [1, null]}, "e": {}}, "f": null, "g": "x", "h.i": true});
        normalize(&mut v).unwrap();
        let o = v.as_object().unwrap();
        let leaves = flatten(o);
        let keys: Vec<&str> = leaves.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec![r#"["a","b"]"#, r#"["a","c","d"]"#, r#"["a","e"]"#, r#"["g"]"#, r#"["h.i"]"#]);
        let back = unflatten(leaves.iter().map(|(k, v)| (path_segments(k).unwrap(), (*v).clone())));
        assert_eq!(Json::Object(back), v);
        assert_eq!(leaf(&v, &["a".into(), "b".into()]), Some(&json!(1)));
        assert_eq!(leaf(&v, &["a".into()]), None, "inner objects are not leaves");
        assert_eq!(leaf(&v, &["a".into(), "e".into()]), Some(&json!({})));
        assert_eq!(leaf(&v, &["f".into()]), None);
        assert!(bucket(r#"["a"]"#) < BUCKETS);
    }

    #[test]
    fn normalizing_zeroes_negative_zero_and_rejects_huge_integers() {
        let mut v = json!({"z": -0.0, "a": [-0.0, {"n": null}], "i": -3});
        normalize(&mut v).unwrap();
        assert_eq!(v, json!({"z": 0.0, "a": [0.0, {}], "i": -3}));
        assert!(v["z"].as_f64().unwrap().is_sign_positive());
        assert!(normalize(&mut json!({"id": u64::MAX})).is_err());
        assert!(normalize(&mut json!({"ids": [1, i64::MAX as u64 + 1]})).is_err());
        assert!(normalize(&mut json!({"id": i64::MAX})).is_ok());
    }

    #[test]
    fn types_join_to_dynamic() {
        assert_eq!(JType::of(&json!(1)), JType::Int);
        assert_eq!(JType::of(&json!(1.5)), JType::Float);
        assert_eq!(JType::of(&json!(u64::MAX)), JType::Dyn);
        assert_eq!(JType::of(&json!([1])), JType::Dyn);
        assert_eq!(JType::Int.join(JType::Float), JType::Dyn);
        for t in [JType::Int, JType::Float, JType::Str, JType::Bool, JType::Dyn] {
            assert_eq!(JType::from_tag(t.tag()), Some(t));
        }
    }
}
