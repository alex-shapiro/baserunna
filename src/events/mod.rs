//! Event tables (SPEC §4.8): append-only, columnar collections for
//! analytical queries, committed by the same transactions as keys, rows
//! and streams.
//!
//! Rows inserted by transactions start as unsealed rows in the LSM tree and
//! are sealed into immutable columnar parts in the background; bulk inserts
//! write parts directly. Parts are merged by a size-tiered policy within
//! time partitions, and queries read both from one snapshot.

pub mod agg;
pub mod build;
pub mod bulk;
pub mod exec;
pub mod json;
pub mod maint;
pub mod part;
pub mod query;
pub mod schema;
pub mod sketch;
pub mod vector;

#[cfg(test)]
mod tests;

use crate::engine::events::{self as ev, EventOp, PartRef, TableState};
use crate::engine::{KeyRange, ReadValue, Txn, Value};
use crate::error::{Error, Result};
use crate::table::SPACE_CATALOG;
use crate::table::schema::{Datum, Row, encode_row};
use bytes::Bytes;
use schema::{EvColumn, EvType, EventSchema, Granularity, SkipIndex};
use serde::Deserialize;
use serde_json::Value as Json;

fn catalog_key(name: &str) -> Bytes {
    let mut k = vec![SPACE_CATALOG];
    k.extend_from_slice(b"e/");
    k.extend_from_slice(name.as_bytes());
    Bytes::from(k)
}

fn decode_schema(it: &ReadValue) -> Result<EventSchema> {
    match &it.value {
        Value::Inline(b) => serde_json::from_slice(b).map_err(|e| Error::internal(format!("bad event schema: {e}"))),
        _ => Err(Error::internal("bad event schema value")),
    }
}

fn put_schema(txn: &mut Txn, s: &EventSchema) -> Result<()> {
    let data = serde_json::to_vec(s).map_err(|e| Error::internal(e.to_string()))?;
    txn.put(catalog_key(&s.name), Value::Inline(Bytes::from(data)))
}

/// Lists the event tables, by name.
pub async fn list(txn: &mut Txn) -> Result<Vec<EventSchema>> {
    let mut prefix = vec![SPACE_CATALOG];
    prefix.extend_from_slice(b"e/");
    let items = txn.scan(&KeyRange::prefix(&prefix), false, 10_000).await?;
    items.iter().map(decode_schema).collect()
}

/// Reads the schema of `name`, pinning it so that a concurrent schema change
/// or drop makes the transaction fail under every isolation level.
pub async fn get_schema(txn: &mut Txn, name: &str) -> Result<EventSchema> {
    match txn.get_pinned(&catalog_key(name)).await? {
        Some(it) => decode_schema(&it),
        None => Err(Error::not_found(format!("event table {name:?} not found"))),
    }
}

/// A column as a request declares it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: EvType,
    #[serde(default = "yes")]
    pub nullable: bool,
    #[serde(default)]
    pub default: Option<Json>,
}

fn yes() -> bool {
    true
}

fn day() -> Granularity {
    Granularity::Day
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateEvents {
    pub name: String,
    pub columns: Vec<ColumnSpec>,
    pub time_column: String,
    pub order_by: Vec<String>,
    #[serde(default = "day")]
    pub partition: Granularity,
    #[serde(default)]
    pub skip_indexes: Vec<SkipIndex>,
    #[serde(default)]
    pub retention_ms: Option<u64>,
    #[serde(default)]
    pub if_not_exists: bool,
}

pub async fn create(txn: &mut Txn, def: &CreateEvents) -> Result<EventSchema> {
    if let Some(existing) = txn.get(&catalog_key(&def.name)).await? {
        if def.if_not_exists {
            return decode_schema(&existing);
        }
        return Err(Error::AlreadyExists(format!("event table {:?} already exists", def.name)));
    }
    let mut s = EventSchema {
        id: 0,
        name: def.name.clone(),
        columns: Vec::new(),
        time_column: def.time_column.clone(),
        order_by: def.order_by.clone(),
        partition: def.partition,
        skip_indexes: def.skip_indexes.clone(),
        retention_ms: def.retention_ms,
        next_column_id: 1,
    };
    for c in &def.columns {
        let nullable = c.nullable && c.name != def.time_column;
        s.columns.push(EvColumn {
            id: s.next_column_id,
            name: c.name.clone(),
            ty: c.ty,
            nullable,
            default: c.default.clone(),
        });
        s.next_column_id += 1;
    }
    s.validate()?;
    s.id = crate::table::allocate_table_id(txn).await?;
    put_schema(txn, &s)?;
    txn.put(ev::state_key(s.id), Value::Inline(TableState::default().encode()))?;
    Ok(s)
}

/// Every part reference of table `id`, read into the transaction's read set.
pub async fn refs(txn: &mut Txn, id: u32) -> Result<Vec<PartRef>> {
    let items = txn.scan(&ev::refs(id), false, usize::MAX >> 1).await?;
    items.iter().map(|it| ref_of(it)).collect()
}

pub fn ref_of(it: &ReadValue) -> Result<PartRef> {
    match &it.value {
        Value::Inline(b) => PartRef::decode(&it.key, b).map_err(|_| Error::internal("corrupt part reference")),
        _ => Err(Error::internal("corrupt part reference")),
    }
}

/// Drops table `name`, its unsealed rows and its parts.
pub async fn drop(txn: &mut Txn, name: &str) -> Result<()> {
    let s = get_schema(txn, name).await?;
    for r in refs(txn, s.id).await? {
        txn.delete(r.key())?;
        txn.event_op(EventOp::Ref { old: Some(r), new: None })?;
    }
    txn.delete_range(ev::rows(s.id))?;
    txn.delete(ev::state_key(s.id))?;
    txn.delete(catalog_key(name))
}

pub async fn add_column(txn: &mut Txn, table: &str, c: &ColumnSpec) -> Result<EventSchema> {
    let mut s = get_schema(txn, table).await?;
    if s.column(&c.name).is_some() {
        return Err(Error::AlreadyExists(format!("column {:?} already exists", c.name)));
    }
    if !c.nullable && c.default.is_none() {
        return Err(Error::invalid("a new non-nullable column needs a default"));
    }
    s.columns.push(EvColumn {
        id: s.next_column_id,
        name: c.name.clone(),
        ty: c.ty,
        nullable: c.nullable,
        default: c.default.clone(),
    });
    s.next_column_id += 1;
    s.validate()?;
    put_schema(txn, &s)?;
    Ok(s)
}

pub async fn drop_column(txn: &mut Txn, table: &str, name: &str) -> Result<EventSchema> {
    let mut s = get_schema(txn, table).await?;
    if s.column(name).is_none() {
        return Err(Error::not_found(format!("column {name:?} not found")));
    }
    if s.time_column == name || s.order_by.iter().any(|o| o == name) {
        return Err(Error::invalid("the time column and sort key columns cannot be dropped"));
    }
    if s.skip_indexes.iter().any(|i| i.column == name) {
        return Err(Error::invalid(format!("column {name:?} has a skip index; drop it first")));
    }
    s.columns.retain(|c| c.name != name);
    s.validate()?;
    put_schema(txn, &s)?;
    Ok(s)
}

pub async fn add_skip_index(txn: &mut Txn, table: &str, i: &SkipIndex) -> Result<EventSchema> {
    let mut s = get_schema(txn, table).await?;
    if s.skip_indexes.iter().any(|x| x.column == i.column) {
        return Err(Error::AlreadyExists(format!("column {:?} already has a skip index", i.column)));
    }
    s.skip_indexes.push(i.clone());
    s.validate()?;
    put_schema(txn, &s)?;
    Ok(s)
}

pub async fn drop_skip_index(txn: &mut Txn, table: &str, column: &str) -> Result<EventSchema> {
    let mut s = get_schema(txn, table).await?;
    if !s.skip_indexes.iter().any(|x| x.column == column) {
        return Err(Error::not_found(format!("no skip index on {column:?}")));
    }
    s.skip_indexes.retain(|x| x.column != column);
    put_schema(txn, &s)?;
    Ok(s)
}

/// Sets or clears the table's retention.
pub async fn set_retention(txn: &mut Txn, table: &str, retention_ms: Option<u64>) -> Result<EventSchema> {
    let mut s = get_schema(txn, table).await?;
    s.retention_ms = retention_ms;
    s.validate()?;
    put_schema(txn, &s)?;
    Ok(s)
}

/// Validates JSON rows against `s`, normalizing JSON objects.
pub fn validate_rows(s: &EventSchema, rows: &[Json], now_ms: i64) -> Result<Vec<Row>> {
    rows.iter()
        .enumerate()
        .map(|(i, v)| {
            let mut row = s.row(v, now_ms).map_err(|e| match e {
                Error::InvalidArgument(m) => Error::invalid(format!("event {i}: {m}")),
                o => o,
            })?;
            for d in row.values_mut() {
                match d {
                    Datum::Json(j) => json::normalize(j).map_err(|m| Error::invalid(format!("event {i}: {m}")))?,
                    Datum::Float(f) if *f == 0.0 => *f = 0.0,
                    _ => {}
                }
            }
            if encode_row(&row).len() > schema::ROW_MAX_BYTES {
                return Err(Error::TooLarge(format!("event {i} exceeds {} bytes", schema::ROW_MAX_BYTES)));
            }
            Ok(row)
        })
        .collect()
}

/// Inserts JSON rows into table `name` at commit, returning how many.
pub async fn insert(txn: &mut Txn, name: &str, rows: &[Json], now_ms: i64) -> Result<usize> {
    let s = get_schema(txn, name).await?;
    let rows = validate_rows(&s, rows, now_ms)?;
    let n = rows.len();
    txn.insert_events(s.id, rows.iter().map(|r| Bytes::from(encode_row(r))).collect())?;
    Ok(n)
}

/// The JSON description of a table and its storage.
pub fn table_json(s: &EventSchema, st: &TableState, refs: &[PartRef]) -> Json {
    let mut out = s.to_json();
    let mut parts: Vec<Json> = Vec::new();
    let mut partitions: std::collections::BTreeMap<i64, (u64, u64, u64)> = Default::default();
    for r in refs {
        let b = ev::partition_bucket(r.partition);
        let e = partitions.entry(b).or_default();
        e.0 += 1;
        e.1 += r.live_rows();
        e.2 += r.stored_bytes();
        parts.push(serde_json::json!({
            "id": r.id, "partition": s.bucket_span(b).0, "rows": r.rows, "deleted": r.deleted,
            "bytes": r.stored_bytes(), "raw_bytes": r.raw_size, "level": r.level,
            "min_time": r.min_time, "max_time": r.max_time,
        }));
    }
    out["partitions"] = Json::Array(
        partitions
            .iter()
            .map(|(b, (n, rows, bytes))| {
                let (start, end) = s.bucket_span(*b);
                serde_json::json!({"start": start, "end": end, "parts": n, "rows": rows, "bytes": bytes})
            })
            .collect(),
    );
    out["parts"] = Json::Array(parts);
    out["unsealed_rows"] = Json::from(st.unsealed_rows);
    out["unsealed_bytes"] = Json::from(st.unsealed_bytes);
    out["rows"] = Json::from(st.unsealed_rows + refs.iter().map(PartRef::live_rows).sum::<u64>());
    out["bytes"] = Json::from(refs.iter().map(PartRef::stored_bytes).sum::<u64>());
    out
}

/// Reads table state at the transaction's snapshot without tracking it.
pub async fn state(txn: &mut Txn, id: u32) -> Result<TableState> {
    match txn.untracked_snapshot().get_entry(&ev::state_key(id)).await? {
        Some(e) if !e.value.is_tombstone() => {
            Ok(ev::state_of(&e).ok_or_else(|| Error::internal("corrupt event state"))?.1)
        }
        _ => Ok(TableState::default()),
    }
}

/// The bounds of a delete: milliseconds or RFC 3339 timestamps, all time by default.
pub fn time_bounds(from: &Option<Json>, to: &Option<Json>) -> Result<(i64, i64)> {
    let bound = |v: &Option<Json>, k: &str, dflt: i64| -> Result<i64> {
        match v {
            None | Some(Json::Null) => Ok(dflt),
            Some(Json::String(s)) => schema::parse_rfc3339(s)
                .ok_or_else(|| Error::invalid(format!("{k} is milliseconds or an RFC 3339 timestamp"))),
            Some(v) => {
                v.as_i64().ok_or_else(|| Error::invalid(format!("{k} is milliseconds or an RFC 3339 timestamp")))
            }
        }
    };
    Ok((bound(from, "from", i64::MIN / 4)?, bound(to, "to", i64::MAX / 4)?))
}
