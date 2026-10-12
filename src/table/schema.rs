//! Table schemas, typed values (datums) and row encoding.

use super::tuple::Key;
use crate::engine::BlobRef;
use crate::error::{Error, Result};
use crate::util::b64;
use crate::util::codec::{DResult, DecodeError, Reader, Writer};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColType {
    String,
    Int,
    Float,
    Bool,
    Bytes,
    Json,
    Blob,
}

impl ColType {
    pub fn indexable(self) -> bool {
        !matches!(self, ColType::Json | ColType::Blob)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub id: u32,
    pub name: String,
    #[serde(rename = "type")]
    pub ty: ColType,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Json>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDef {
    pub id: u32,
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableSchema {
    pub id: u32,
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<IndexDef>,
    pub next_column_id: u32,
    pub next_index_id: u32,
}

/// A typed column value.
#[derive(Clone, Debug, PartialEq)]
pub enum Datum {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    Json(Json),
    Blob(BlobRef),
}

impl Datum {
    pub fn to_key(&self) -> Option<Key> {
        Some(match self {
            Datum::Null => Key::Null,
            Datum::Bool(b) => Key::Bool(*b),
            Datum::Int(i) => Key::Int(*i),
            Datum::Float(f) => Key::Float(*f),
            Datum::Str(s) => Key::Str(s.clone()),
            Datum::Bytes(b) => Key::Bytes(b.clone()),
            Datum::Json(_) | Datum::Blob(_) => return None,
        })
    }

    pub fn from_key(k: Key) -> Datum {
        match k {
            Key::Null => Datum::Null,
            Key::Bool(b) => Datum::Bool(b),
            Key::Int(i) => Datum::Int(i),
            Key::Float(f) => Datum::Float(f),
            Key::Str(s) => Datum::Str(s),
            Key::Bytes(b) => Datum::Bytes(b),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    pub fn to_json(&self) -> Json {
        match self {
            Datum::Null => Json::Null,
            Datum::Bool(b) => json!(b),
            Datum::Int(i) => json!(i),
            Datum::Float(f) => json!(f),
            Datum::Str(s) => json!(s),
            Datum::Bytes(b) => json!(b64::encode(b)),
            Datum::Json(j) => j.clone(),
            Datum::Blob(r) => blob_json(r),
        }
    }
}

pub fn blob_json(r: &BlobRef) -> Json {
    json!({
        "blob_id": r.id,
        "size": r.size,
        "sha256": b64::hex(&r.sha256),
        "content_type": r.content_type,
    })
}

/// Convert a JSON value to a datum of `ty`. Blob columns accept a staged
/// blob id (a string); the caller resolves it.
pub fn datum_from_json(ty: ColType, v: &Json, col: &str) -> Result<Datum> {
    if v.is_null() {
        return Ok(Datum::Null);
    }
    let bad = || Error::invalid(format!("column {col:?} expects {ty:?}, got {v}"));
    Ok(match ty {
        ColType::String => Datum::Str(v.as_str().ok_or_else(bad)?.to_string()),
        ColType::Int => Datum::Int(v.as_i64().ok_or_else(bad)?),
        ColType::Float => {
            let f = v.as_f64().ok_or_else(bad)?;
            if !f.is_finite() {
                return Err(bad());
            }
            Datum::Float(f)
        }
        ColType::Bool => Datum::Bool(v.as_bool().ok_or_else(bad)?),
        ColType::Bytes => Datum::Bytes(b64::decode(v.as_str().ok_or_else(bad)?).ok_or_else(bad)?),
        ColType::Json => Datum::Json(v.clone()),
        ColType::Blob => {
            // Resolved later by the caller; keep the id as a string marker.
            Datum::Str(v.as_str().ok_or_else(bad)?.to_string())
        }
    })
}

/// Coerce a JSON value for comparing against a column (filters, query
/// bounds). Ints are accepted for float columns.
pub fn key_from_json(ty: ColType, v: &Json, col: &str) -> Result<Key> {
    if !ty.indexable() {
        return Err(Error::invalid(format!("column {col:?} is not indexable")));
    }
    datum_from_json(ty, v, col)?.to_key().ok_or_else(|| Error::invalid("not indexable"))
}

pub fn valid_ident(s: &str) -> bool {
    let mut it = s.chars();
    matches!(it.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && s.len() <= 64
        && it.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

impl TableSchema {
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn index(&self, name: &str) -> Option<&IndexDef> {
        self.indexes.iter().find(|i| i.name == name)
    }

    pub fn validate(&self) -> Result<()> {
        if !valid_ident(&self.name) {
            return Err(Error::invalid(format!("invalid table name {:?}", self.name)));
        }
        if self.columns.is_empty() || self.columns.len() > 256 {
            return Err(Error::invalid("a table needs 1..=256 columns"));
        }
        let mut seen = std::collections::BTreeSet::new();
        for c in &self.columns {
            if !valid_ident(&c.name) {
                return Err(Error::invalid(format!("invalid column name {:?}", c.name)));
            }
            if !seen.insert(c.name.as_str()) {
                return Err(Error::invalid(format!("duplicate column {:?}", c.name)));
            }
            if let Some(d) = &c.default {
                if c.ty == ColType::Blob {
                    return Err(Error::invalid("blob columns cannot have defaults"));
                }
                datum_from_json(c.ty, d, &c.name)?;
            }
        }
        if self.primary_key.is_empty() || self.primary_key.len() > 8 {
            return Err(Error::invalid("primary key needs 1..=8 columns"));
        }
        let mut pk_seen = std::collections::BTreeSet::new();
        for p in &self.primary_key {
            let c = self.column(p).ok_or_else(|| Error::invalid(format!("primary key column {p:?} does not exist")))?;
            if !c.ty.indexable() {
                return Err(Error::invalid(format!("primary key column {p:?} is not indexable")));
            }
            if c.nullable {
                return Err(Error::invalid(format!("primary key column {p:?} must not be nullable")));
            }
            if !pk_seen.insert(p) {
                return Err(Error::invalid("duplicate primary key column"));
            }
        }
        let mut idx_names = std::collections::BTreeSet::new();
        for i in &self.indexes {
            self.validate_index(i)?;
            if !idx_names.insert(i.name.as_str()) {
                return Err(Error::invalid(format!("duplicate index {:?}", i.name)));
            }
        }
        Ok(())
    }

    pub fn validate_index(&self, i: &IndexDef) -> Result<()> {
        if !valid_ident(&i.name) {
            return Err(Error::invalid(format!("invalid index name {:?}", i.name)));
        }
        if i.columns.is_empty() || i.columns.len() > 8 {
            return Err(Error::invalid("an index needs 1..=8 columns"));
        }
        let mut seen = std::collections::BTreeSet::new();
        for c in &i.columns {
            let col = self.column(c).ok_or_else(|| Error::invalid(format!("index column {c:?} does not exist")))?;
            if !col.ty.indexable() {
                return Err(Error::invalid(format!("column {c:?} is not indexable")));
            }
            if !seen.insert(c) {
                return Err(Error::invalid("duplicate index column"));
            }
        }
        Ok(())
    }

    pub fn to_json(&self) -> Json {
        json!({
            "name": self.name,
            "columns": self.columns.iter().map(|c| {
                let mut o = json!({"name": c.name, "type": c.ty, "nullable": c.nullable});
                if let Some(d) = &c.default { o["default"] = d.clone(); }
                o
            }).collect::<Vec<_>>(),
            "primary_key": self.primary_key,
            "indexes": self.indexes.iter().map(|i| json!({"name": i.name, "columns": i.columns, "unique": i.unique})).collect::<Vec<_>>(),
        })
    }
}

/// Stored row: column id -> datum (blob columns live in side entries and
/// are not part of the encoded row).
pub type Row = BTreeMap<u32, Datum>;

const D_BOOL: u8 = 1;
const D_INT: u8 = 2;
const D_FLOAT: u8 = 3;
const D_STR: u8 = 4;
const D_BYTES: u8 = 5;
const D_JSON: u8 = 6;

pub fn encode_row(row: &Row) -> Vec<u8> {
    let mut w = Writer::new();
    let stored: Vec<(&u32, &Datum)> = row.iter().filter(|(_, d)| !matches!(d, Datum::Null | Datum::Blob(_))).collect();
    w.u8(1); // format
    w.varint(stored.len() as u64);
    for (id, d) in stored {
        w.varint(*id as u64);
        match d {
            Datum::Bool(b) => {
                w.u8(D_BOOL);
                w.bool(*b);
            }
            Datum::Int(i) => {
                w.u8(D_INT);
                w.u64(*i as u64);
            }
            Datum::Float(f) => {
                w.u8(D_FLOAT);
                w.u64(f.to_bits());
            }
            Datum::Str(s) => {
                w.u8(D_STR);
                w.str(s);
            }
            Datum::Bytes(b) => {
                w.u8(D_BYTES);
                w.bytes(b);
            }
            Datum::Json(j) => {
                w.u8(D_JSON);
                w.str(&j.to_string());
            }
            Datum::Null | Datum::Blob(_) => unreachable!("filtered above"),
        }
    }
    w.into_inner()
}

pub fn decode_row(data: &[u8]) -> DResult<Row> {
    let mut r = Reader::new(data);
    if r.u8()? != 1 {
        return Err(DecodeError("unknown row format"));
    }
    let n = r.len(data.len())?;
    let mut row = Row::new();
    for _ in 0..n {
        let id = u32::try_from(r.varint()?).map_err(|_| DecodeError("bad column id"))?;
        let d = match r.u8()? {
            D_BOOL => Datum::Bool(r.bool()?),
            D_INT => Datum::Int(r.u64()? as i64),
            D_FLOAT => Datum::Float(f64::from_bits(r.u64()?)),
            D_STR => Datum::Str(r.string()?),
            D_BYTES => Datum::Bytes(r.bytes()?.to_vec()),
            D_JSON => Datum::Json(serde_json::from_str(r.str()?).map_err(|_| DecodeError("bad json"))?),
            _ => return Err(DecodeError("bad datum tag")),
        };
        row.insert(id, d);
    }
    if !r.is_empty() {
        return Err(DecodeError("row trailing data"));
    }
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::rng::Rng;

    #[test]
    fn row_roundtrip() {
        let mut row = Row::new();
        row.insert(1, Datum::Int(-5));
        row.insert(2, Datum::Str("héllo".into()));
        row.insert(3, Datum::Float(1.5));
        row.insert(4, Datum::Bytes(vec![0, 1, 255]));
        row.insert(5, Datum::Json(json!({"a": [1, 2]})));
        row.insert(6, Datum::Bool(true));
        row.insert(7, Datum::Null);
        let enc = encode_row(&row);
        let mut back = decode_row(&enc).unwrap();
        back.insert(7, Datum::Null);
        assert_eq!(back, row);
    }

    #[test]
    fn decode_row_garbage() {
        let mut r = Rng::new(8);
        for _ in 0..20000 {
            let n = r.usize(30);
            let mut d = r.bytes(n);
            if !d.is_empty() && r.chance(0.5) {
                d[0] = 1;
            }
            let _ = decode_row(&d);
        }
    }

    #[test]
    fn schema_validation() {
        let mut s = TableSchema {
            id: 1,
            name: "users".into(),
            columns: vec![
                Column { id: 1, name: "id".into(), ty: ColType::Int, nullable: false, default: None },
                Column { id: 2, name: "email".into(), ty: ColType::String, nullable: true, default: None },
                Column { id: 3, name: "meta".into(), ty: ColType::Json, nullable: true, default: None },
            ],
            primary_key: vec!["id".into()],
            indexes: vec![IndexDef { id: 1, name: "by_email".into(), columns: vec!["email".into()], unique: true }],
            next_column_id: 4,
            next_index_id: 2,
        };
        s.validate().unwrap();
        s.indexes.push(IndexDef { id: 2, name: "bad".into(), columns: vec!["meta".into()], unique: false });
        assert!(s.validate().is_err());
        s.indexes.pop();
        s.primary_key = vec!["email".into()];
        assert!(s.validate().is_err());
        assert!(valid_ident("_a1"));
        assert!(!valid_ident("1a"));
        assert!(!valid_ident("a-b"));
    }
}
