//! Stream record filters (SPEC §4.6, §4.7): the filter language over a
//! record's JSON body fields (`field`, a dotted path), its headers
//! (`header`) and, naming neither, its body text, which only `contains`
//! tests.

use super::Body;
use crate::error::{Error, Result};
use crate::filter::{self, Target, Test};
use serde::Deserialize;
use serde_json::{Map, Value as Json};
use std::cell::OnceCell;
use std::cmp::Ordering;

const FIELD_MAX: usize = 256;
const HEADER_MAX: usize = 128;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(transparent)]
pub struct Filter(filter::Filter<Operand>);

/// What a record comparison names.
#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    /// A dotted path into a JSON body; numeric segments index arrays.
    Field(String),
    Header(String),
    Text,
}

impl filter::Operand for Operand {
    fn parse(keys: &mut Map<String, Json>) -> Result<Operand> {
        let mut name = |k: &str, max: usize| match keys.remove(k) {
            None => Ok(None),
            Some(Json::String(s)) if (1..=max).contains(&s.len()) => Ok(Some(s)),
            Some(_) => Err(Error::invalid(format!("{k} is a string of 1-{max} bytes"))),
        };
        match (name("field", FIELD_MAX)?, name("header", HEADER_MAX)?) {
            (Some(_), Some(_)) => Err(Error::invalid("a comparison names a field or a header, not both")),
            (Some(f), None) => Ok(Operand::Field(f)),
            (None, Some(h)) => Ok(Operand::Header(h)),
            (None, None) => Ok(Operand::Text),
        }
    }

    fn check(&self, test: &Test<Json>) -> Result<()> {
        match (self, test) {
            (Operand::Text, Test::Contains(Json::String(_))) | (Operand::Field(_) | Operand::Header(_), _) => Ok(()),
            (Operand::Text, _) => Err(Error::invalid(
                "a comparison of the body text, naming no field or header, is contains with a string",
            )),
        }
    }
}

impl Filter {
    pub fn matches(&self, body: &Body) -> bool {
        let doc = OnceCell::new();
        self.0.eval(&mut |o: &Operand| match o {
            Operand::Field(f) => {
                let doc = doc.get_or_init(|| body.json_value());
                doc.as_ref().and_then(|d| lookup(d, f)).filter(|v| !v.is_null()).map(Value::Json)
            }
            Operand::Header(h) => body.header(h).map(Value::Str),
            Operand::Text => Some(Value::Text(&body.data)),
        })
    }
}

/// The value at a dotted path: object keys, and array indexes.
fn lookup<'a>(doc: &'a Json, path: &str) -> Option<&'a Json> {
    path.split('.').try_fold(doc, |cur, seg| match cur {
        Json::Object(o) => o.get(seg),
        Json::Array(a) => seg.parse::<usize>().ok().and_then(|i| a.get(i)),
        _ => None,
    })
}

enum Value<'a> {
    Json(&'a Json),
    Str(&'a str),
    Text(&'a [u8]),
}

impl Target<Json> for Value<'_> {
    fn equals(&self, arg: &Json) -> bool {
        match self {
            Value::Json(j) => Target::equals(j, arg),
            Value::Str(s) => Target::equals(s, arg),
            Value::Text(_) => false,
        }
    }

    fn order(&self, arg: &Json) -> Option<Ordering> {
        match self {
            Value::Json(j) => Target::order(j, arg),
            Value::Str(s) => Target::order(s, arg),
            Value::Text(_) => None,
        }
    }

    fn starts_with(&self, arg: &Json) -> bool {
        match self {
            Value::Json(j) => Target::starts_with(j, arg),
            Value::Str(s) => Target::starts_with(s, arg),
            Value::Text(_) => false,
        }
    }

    fn contains(&self, arg: &Json) -> bool {
        match self {
            Value::Json(j) => Target::contains(j, arg),
            Value::Str(s) => Target::contains(s, arg),
            Value::Text(t) => arg.as_str().is_some_and(|p| filter::contains_bytes(t, p.as_bytes())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

    fn json_body(v: Json, headers: &[(&str, &str)]) -> Body {
        let headers = headers.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        Body { json: true, headers, data: Bytes::from(v.to_string()) }
    }

    fn f(v: Json) -> Filter {
        serde_json::from_value(v.clone()).unwrap_or_else(|e| panic!("{v}: {e}"))
    }

    #[test]
    fn comparisons() {
        let b = json_body(
            json!({"level": "error", "n": 3, "http": {"status": 503}, "tags": ["a", "b"], "msg": "upstream timeout",
                "gone": null}),
            &[("svc", "api")],
        );
        for (yes, filter) in [
            (true, json!({"field": "level", "op": "eq", "value": "error"})),
            (true, json!({"field": "n", "op": "eq", "value": 3.0})),
            (true, json!({"field": "http.status", "op": "ge", "value": 500})),
            (false, json!({"field": "http.status", "op": "lt", "value": 500})),
            (true, json!({"field": "tags.1", "op": "eq", "value": "b"})),
            (true, json!({"field": "tags", "op": "contains", "value": "a"})),
            (true, json!({"field": "msg", "op": "contains", "value": "time"})),
            (true, json!({"field": "level", "op": "in", "value": ["warn", "error"]})),
            (true, json!({"field": "level", "op": "prefix", "value": "err"})),
            (false, json!({"field": "missing", "op": "ne", "value": 1})),
            (true, json!({"not": {"field": "missing", "op": "eq", "value": 1}})),
            (false, json!({"field": "gone", "op": "exists"})),
            (false, json!({"field": "gone", "op": "ne", "value": 1})),
            (false, json!({"field": "level", "op": "gt", "value": 5})),
            (true, json!({"field": "http", "op": "exists"})),
            (true, json!({"header": "svc", "op": "eq", "value": "api"})),
            (true, json!({"header": "svc", "op": "lt", "value": "b"})),
            (false, json!({"header": "other", "op": "exists"})),
            (false, json!({"header": "other", "op": "ne", "value": "api"})),
            (true, json!({"op": "contains", "value": "upstream"})),
            (
                true,
                json!({"and": [{"field": "n", "op": "lt", "value": 4}, {"not": {"field": "n", "op": "lt", "value": 3}}]}),
            ),
            (
                false,
                json!({"or": [{"field": "n", "op": "gt", "value": 9}, {"header": "svc", "op": "ne", "value": "api"}]}),
            ),
        ] {
            assert_eq!(f(filter.clone()).matches(&b), yes, "{filter}");
        }
        let text = Body { json: false, headers: vec![], data: Bytes::from_static(b"plain level=error") };
        assert!(f(json!({"op": "contains", "value": "level=err"})).matches(&text));
        assert!(!f(json!({"field": "level", "op": "exists"})).matches(&text), "text bodies have no fields");
    }

    #[test]
    fn malformed_filters_are_refused() {
        let a = json!({"field": "a", "op": "exists"});
        let b = json!({"field": "b", "op": "exists"});
        for bad in [
            json!({"and": [a], "or": [b]}),
            json!({"not": a, "field": "b", "op": "exists"}),
            json!({"and": [], "bogus": 1}),
            json!({"field": "a", "header": "b", "op": "eq", "value": 1}),
            json!({"field": "a", "col": "b", "op": "eq", "value": 1}),
            json!({"op": "eq", "value": "x"}),
            json!({"op": "exists"}),
            json!({"field": "", "op": "eq", "value": 1}),
            json!({"field": 1, "op": "eq", "value": 1}),
            json!({"header": "x".repeat(HEADER_MAX + 1), "op": "exists"}),
            json!({"op": "contains", "value": 1}),
            json!({"field": "a", "op": "between", "value": 1}),
        ] {
            assert!(serde_json::from_value::<Filter>(bad.clone()).is_err(), "{bad}");
        }
    }
}
