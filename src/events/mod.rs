//! Event tables (SPEC §4.8): append-only, columnar collections for
//! analytical queries, committed by the same transactions as keys, rows,
//! and streams.
//!
//! Transactions insert unsealed rows into the LSM tree, and background
//! maintenance seals those rows into immutable columnar parts. Bulk inserts
//! write parts directly. Parts are merged by a size-tiered policy within
//! time partitions, and queries read both unsealed rows and parts from a
//! single snapshot.

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
mod difftests;
#[cfg(test)]
mod reference;
#[cfg(test)]
mod tests;

use crate::engine::events::{self as ev, EventOp, PartRef, TableState};
use crate::engine::{ReadValue, Txn, Value};
use crate::error::{Error, Result};
use crate::table::SPACE_CATALOG;
use crate::table::schema::{Datum, encode_row};
use bytes::Bytes;
use schema::{EvColumn, EvType, EventSchema, Granularity, SkipIndex};
use serde::Deserialize;
use serde_json::Value as Json;

/// Key prefix for event table schemas, indexed by table name.
pub(crate) const SCHEMAS: &[u8] = &[SPACE_CATALOG, b'e', b'/'];

fn catalog_key(name: &str) -> Bytes {
    Bytes::from([SCHEMAS, name.as_bytes()].concat())
}

/// Key prefix for event table names, indexed by table ID.
pub(crate) const NAMES: &[u8] = &[SPACE_CATALOG, b'e', b'#'];

fn name_key(id: u32) -> Bytes {
    Bytes::from([NAMES, &id.to_be_bytes()].concat())
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

/// Returns a page of event tables whose names start with `prefix` after
/// `cursor`, and the cursor for the next page.
pub async fn list(
    txn: &mut Txn,
    prefix: &str,
    limit: usize,
    cursor: Option<&str>,
) -> Result<(Vec<EventSchema>, Option<String>)> {
    let (page, next) = crate::catalog::names_page(txn, SCHEMAS, prefix, limit, cursor).await?;
    Ok((page.iter().map(|(_, it)| decode_schema(it)).collect::<Result<_>>()?, next))
}

/// Reads the schema of `name` and pins the catalog entry so that a concurrent
/// schema change or table drop causes the transaction to fail at every
/// isolation level.
pub async fn get_schema(txn: &mut Txn, name: &str) -> Result<EventSchema> {
    match txn.get_pinned(&catalog_key(name)).await? {
        Some(it) => decode_schema(&it),
        None => Err(Error::not_found(format!("event table {name:?} not found"))),
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: EvType,
    #[serde(default = "default_true")]
    pub nullable: bool,
    #[serde(default)]
    pub default: Option<Json>,
}

fn default_true() -> bool {
    true
}

fn default_day() -> Granularity {
    Granularity::Day
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateEvents {
    pub name: String,
    pub columns: Vec<ColumnSpec>,
    pub time_column: String,
    pub order_by: Vec<String>,
    #[serde(default = "default_day")]
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
    txn.put(name_key(s.id), Value::Inline(Bytes::from(s.name.clone())))?;
    txn.event_op(EventOp::Create { table: s.id })?;
    Ok(s)
}

/// Reads all part references for table `id` into the transaction's read set.
pub async fn refs(txn: &mut Txn, id: u32) -> Result<Vec<PartRef>> {
    let items = txn.scan(&ev::refs(id), false, usize::MAX >> 1).await?;
    items.iter().map(ref_of).collect()
}

pub fn ref_of(it: &ReadValue) -> Result<PartRef> {
    match &it.value {
        Value::Inline(b) => PartRef::decode(&it.key, b).map_err(|_| Error::internal("corrupt part reference")),
        _ => Err(Error::internal("corrupt part reference")),
    }
}

/// Drops table `name`, its unsealed rows, and its part references.
///
/// This function does not read part references, so concurrent part bindings,
/// seals, or merges do not conflict with it. Unreferenced part objects are
/// removed by the orphan sweep (SPEC §5.8).
pub async fn drop(txn: &mut Txn, name: &str) -> Result<()> {
    let s = get_schema(txn, name).await?;
    txn.delete_range(ev::refs(s.id))?;
    txn.delete_range(ev::rows(s.id))?;
    txn.event_op(EventOp::Drop { table: s.id })?;
    txn.delete(name_key(s.id))?;
    txn.delete(catalog_key(name))
}

pub async fn add_column(txn: &mut Txn, table: &str, c: &ColumnSpec) -> Result<EventSchema> {
    let mut s = get_schema(txn, table).await?;
    if s.column(&c.name).is_some() {
        return Err(Error::AlreadyExists(format!("column {:?} already exists", c.name)));
    }
    if !c.nullable && c.default.is_none() {
        return Err(Error::invalid("a new non-nullable column requires a default"));
    }
    if !c.nullable && c.ty == EvType::Time && c.default.as_ref().and_then(Json::as_str) == Some(schema::NOW) {
        return Err(Error::invalid(
            "a new column defaulting to \"now\" must be nullable: earlier events are read as null",
        ));
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
        return Err(Error::not_found(format!("skip index on {column:?} not found")));
    }
    s.skip_indexes.retain(|x| x.column != column);
    put_schema(txn, &s)?;
    Ok(s)
}

pub async fn set_retention(txn: &mut Txn, table: &str, retention_ms: Option<u64>) -> Result<EventSchema> {
    let mut s = get_schema(txn, table).await?;
    s.retention_ms = retention_ms;
    s.validate()?;
    put_schema(txn, &s)?;
    Ok(s)
}

/// Validated and encoded rows stored in a single buffer.
#[derive(Clone, Debug, Default)]
pub struct ValidRows {
    data: Vec<u8>,
    /// Partition key and end offset in `data` for each row.
    rows: Vec<(u32, u32)>,
    limit: Option<usize>,
}

impl ValidRows {
    /// Creates a buffer that rejects additional rows if the total encoded size
    /// exceeds `limit` bytes.
    pub fn up_to(limit: usize) -> ValidRows {
        ValidRows { limit: Some(limit), ..ValidRows::default() }
    }

    /// Validates event `v`, normalizes its JSON objects, and appends its
    /// encoded representation.
    pub fn push(&mut self, s: &EventSchema, v: &Json, now_ms: i64) -> Result<()> {
        let i = self.rows.len();
        let mut row = s.row(v, now_ms).map_err(|e| match e {
            Error::InvalidArgument(m) => Error::invalid(format!("event {i}: {m}")),
            o => o,
        })?;
        for d in row.values_mut() {
            if let Datum::Json(j) = d {
                json::normalize(j).map_err(|m| Error::invalid(format!("event {i}: {m}")))?;
            }
        }
        let encoded = encode_row(&row);
        if encoded.len() > schema::ROW_MAX_BYTES {
            return Err(Error::TooLarge(format!("event {i} exceeds {} bytes", schema::ROW_MAX_BYTES)));
        }
        if build::stored_len(&row) > schema::ROW_STORED_MAX_BYTES {
            return Err(Error::TooLarge(format!(
                "event {i}'s JSON is flattened to more than {} bytes of paths and values",
                schema::ROW_STORED_MAX_BYTES
            )));
        }
        if let Some(limit) = self.limit
            && self.data.len() + encoded.len() > limit
        {
            return Err(Error::TooLarge(format!(
                "the events take more than {limit} bytes once encoded, column defaults included"
            )));
        }
        self.data.extend_from_slice(&encoded);
        let end = u32::try_from(self.data.len()).map_err(|_| Error::TooLarge("too many events".into()))?;
        self.rows.push((ev::partition_key(s.bucket(s.time_of(&row))), end));
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Total memory used by the encoded data and row offsets, in bytes.
    pub fn bytes(&self) -> usize {
        self.data.len() + 12 * self.rows.len()
    }

    /// Returns the partition key and encoded bytes of row `i`.
    pub fn get(&self, i: usize) -> (u32, &[u8]) {
        let start = if i == 0 { 0 } else { self.rows[i - 1].1 as usize };
        let (p, end) = self.rows[i];
        (p, &self.data[start..end as usize])
    }

    pub fn iter(&self) -> impl Iterator<Item = (u32, &[u8])> {
        (0..self.rows.len()).map(|i| self.get(i))
    }
}

/// Validates and encodes `rows` against schema `s`.
pub fn validate_rows(s: &EventSchema, rows: &[Json], now_ms: i64) -> Result<ValidRows> {
    let mut out = ValidRows::default();
    for v in rows {
        out.push(s, v, now_ms)?;
    }
    Ok(out)
}

/// Buffers `rows` for insertion into table `name` at commit and returns the
/// number of inserted rows.
pub async fn insert(txn: &mut Txn, name: &str, rows: &[Json], now_ms: i64) -> Result<usize> {
    if rows.len() > ev::INSERT_MAX_ROWS {
        return Err(Error::TooLarge(format!("a transaction inserts at most {} events", ev::INSERT_MAX_ROWS)));
    }
    let s = get_schema(txn, name).await?;
    let mut valid = ValidRows::up_to(txn.write_budget());
    for v in rows {
        valid.push(&s, v, now_ms)?;
    }
    let rows = valid;
    txn.insert_events(s.id, rows.iter().map(|(_, r)| Bytes::copy_from_slice(r)).collect())?;
    Ok(rows.len())
}

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

/// Reads table state from the transaction's snapshot without adding the state
/// key to the conflict read set.
pub async fn state(txn: &mut Txn, id: u32) -> Result<TableState> {
    match txn.untracked_snapshot().get_entry(&ev::state_key(id)).await? {
        Some(e) if !e.value.is_tombstone() => {
            Ok(ev::state_of(&e).ok_or_else(|| Error::internal("corrupt event state"))?.1)
        }
        _ => Ok(TableState::default()),
    }
}

/// Parses inclusive delete bounds from millisecond integers or RFC 3339
/// timestamps, defaulting to the full supported time range.
pub fn time_bounds(from: &Option<Json>, to: &Option<Json>) -> Result<(i64, i64)> {
    let bound = |v: &Option<Json>, k: &str, dflt: i64| -> Result<i64> {
        match v {
            None | Some(Json::Null) => Ok(dflt),
            Some(Json::String(s)) => schema::parse_rfc3339(s)
                .ok_or_else(|| Error::invalid(format!("{k} must be milliseconds or an RFC 3339 timestamp"))),
            Some(v) => {
                v.as_i64().ok_or_else(|| Error::invalid(format!("{k} must be milliseconds or an RFC 3339 timestamp")))
            }
        }
    };
    let (from, to) = (bound(from, "from", i64::MIN / 4)?, bound(to, "to", i64::MAX / 4)?);
    if from > to {
        return Err(Error::invalid("from is after to"));
    }
    Ok((from, to))
}
