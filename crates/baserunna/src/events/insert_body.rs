//! Parsing of event insert bodies: a JSON array, `{"rows": [...]}`, or NDJSON.

use crate::error::{Error, Result};
use serde_json::Value as Json;

const BODY_SHAPE: &str = "the body is {\"rows\": [...]}, an array of events, or NDJSON";

/// Streams events from a JSON or NDJSON insert body into `f` without buffering
/// the parsed array, and returns the body's `idempotency_key` if present.
pub fn each_event(content_type: &str, data: &[u8], f: &mut dyn FnMut(Json) -> Result<()>) -> Result<Option<String>> {
    crate::util::json::reject_inexact_integers(data).map_err(Error::invalid)?;
    match content_type {
        "" | "application/json" => {
            let mut failed = None;
            let mut de = serde_json::Deserializer::from_slice(data);
            let parsed = serde::de::DeserializeSeed::deserialize(Body { f, failed: &mut failed }, &mut de)
                .and_then(|key| de.end().map(|()| key));
            match (failed, parsed) {
                (Some(e), _) => Err(e),
                (None, Ok(key)) => Ok(key),
                (None, Err(e)) => Err(Error::invalid(format!("invalid JSON body: {e}"))),
            }
        }
        "application/x-ndjson" | "application/jsonl" | "application/json-lines" => {
            let text = std::str::from_utf8(data).map_err(|_| Error::invalid("NDJSON must be UTF-8"))?;
            let text = text.strip_prefix('\u{feff}').unwrap_or(text);
            for (i, l) in text.lines().enumerate().filter(|(_, l)| !l.trim().is_empty()) {
                f(serde_json::from_str(l).map_err(|e| Error::invalid(format!("line {}: {e}", i + 1)))?)?;
            }
            Ok(None)
        }
        other => Err(Error::invalid(format!(
            "unsupported content type {other:?}: use application/json or application/x-ndjson"
        ))),
    }
}

struct Events<'a> {
    f: &'a mut dyn FnMut(Json) -> Result<()>,
    failed: &'a mut Option<Error>,
}

impl<'de> serde::de::DeserializeSeed<'de> for Events<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> std::result::Result<(), D::Error> {
        d.deserialize_seq(self)
    }
}

impl<'de> serde::de::Visitor<'de> for Events<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("an array of events")
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<(), A::Error> {
        while let Some(v) = seq.next_element::<Json>()? {
            if let Err(e) = (self.f)(v) {
                *self.failed = Some(e);
                return Err(serde::de::Error::custom("rejected"));
            }
        }
        Ok(())
    }
}

struct Body<'a> {
    f: &'a mut dyn FnMut(Json) -> Result<()>,
    failed: &'a mut Option<Error>,
}

impl Body<'_> {
    fn serde_err<E: serde::de::Error>(self, e: Error) -> E {
        *self.failed = Some(e);
        E::custom("rejected")
    }
}

impl<'de> serde::de::DeserializeSeed<'de> for Body<'_> {
    type Value = Option<String>;
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> std::result::Result<Option<String>, D::Error> {
        d.deserialize_any(self)
    }
}

impl<'de> serde::de::Visitor<'de> for Body<'_> {
    type Value = Option<String>;
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(BODY_SHAPE)
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, seq: A) -> std::result::Result<Option<String>, A::Error> {
        serde::de::Visitor::visit_seq(Events { f: self.f, failed: self.failed }, seq).map(|()| None)
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> std::result::Result<Option<String>, A::Error> {
        let (mut key, mut rows, mut keyed) = (None, false, false);
        while let Some(k) = map.next_key::<String>()? {
            if (k == "rows" && rows) || (k == "idempotency_key" && keyed) {
                return Err(self.serde_err(Error::invalid(format!("{k:?} appears twice"))));
            }
            match k.as_str() {
                "rows" => {
                    map.next_value_seed(Events { f: &mut *self.f, failed: &mut *self.failed })?;
                    rows = true;
                }
                "idempotency_key" => {
                    keyed = true;
                    match map.next_value::<Json>()? {
                        Json::Null => key = None,
                        Json::String(k) => key = Some(k),
                        _ => return Err(self.serde_err(Error::invalid("idempotency_key is a string"))),
                    }
                }
                other => return Err(self.serde_err(Error::invalid(format!("unknown field {other:?}")))),
            }
        }
        if !rows {
            return Err(self.serde_err(Error::invalid(BODY_SHAPE)));
        }
        Ok(key)
    }
}

