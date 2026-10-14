//! Typed tables with primary keys and secondary indexes, layered on the KV
//! engine inside the caller's transaction (so every table operation is
//! serializable with everything else in the transaction). See SPEC §4.4.

pub mod schema;
pub mod tuple;

use crate::engine::{
    BlobRef, Change, ChangePos, Consistency, Db, Isolation, KeyRange, ReadValue, Seq, Ttl, Txn, Value, blob,
};
use crate::error::{Error, Result};
use crate::util::b64;
use bytes::Bytes;
use schema::{ColType, Column, Datum, IndexDef, IndexState, Row, TableSchema, datum_from_json, key_from_json};
use serde::Deserialize;
use serde_json::{Map, Value as Json};
use std::collections::BTreeSet;
use std::ops::Bound;
use std::sync::Arc;
use tuple::Key;

pub const SPACE_CATALOG: u8 = 0x00;
pub const SPACE_KV: u8 = 0x01;
pub const SPACE_ROWS: u8 = 0x02;
pub const SPACE_INDEX: u8 = 0x03;
pub const SPACE_ROW_BLOBS: u8 = 0x04;

const MAX_EXAMINED_PER_QUERY: usize = 10_000;

fn catalog_key(name: &str) -> Bytes {
    let mut k = vec![SPACE_CATALOG];
    k.extend_from_slice(b"t/");
    k.extend_from_slice(name.as_bytes());
    Bytes::from(k)
}

/// The progress of a building index: the key of the last row backfilled
/// (empty before the first batch). Kept out of the schema, which every row
/// write pins, so that backfill batches do not conflict with row writes.
fn backfill_key(tid: u32, iid: u32) -> Bytes {
    let mut k = vec![SPACE_CATALOG];
    k.extend_from_slice(b"b/");
    k.extend_from_slice(&tid.to_be_bytes());
    k.extend_from_slice(&iid.to_be_bytes());
    Bytes::from(k)
}

fn next_id_key() -> Bytes {
    let mut k = vec![SPACE_CATALOG];
    k.extend_from_slice(b"next_table_id");
    Bytes::from(k)
}

fn table_prefix(space: u8, tid: u32) -> Vec<u8> {
    let mut k = vec![space];
    k.extend_from_slice(&tid.to_be_bytes());
    k
}

fn index_prefix(tid: u32, iid: u32) -> Vec<u8> {
    let mut k = table_prefix(SPACE_INDEX, tid);
    k.extend_from_slice(&iid.to_be_bytes());
    k
}

fn row_key(tid: u32, pk: &[Key]) -> Bytes {
    let mut k = table_prefix(SPACE_ROWS, tid);
    k.extend_from_slice(&tuple::encode(pk));
    Bytes::from(k)
}

fn blob_side_prefix(tid: u32, pk: &[Key]) -> Vec<u8> {
    let mut k = table_prefix(SPACE_ROW_BLOBS, tid);
    k.extend_from_slice(&tuple::encode(pk));
    k
}

fn blob_side_key(tid: u32, pk: &[Key], col: u32) -> Bytes {
    let mut k = blob_side_prefix(tid, pk);
    k.extend_from_slice(&col.to_be_bytes());
    Bytes::from(k)
}

// ---------------------------------------------------------------- catalog

pub async fn list_tables(txn: &mut Txn) -> Result<Vec<TableSchema>> {
    let mut prefix = vec![SPACE_CATALOG];
    prefix.extend_from_slice(b"t/");
    let items = txn.scan(&KeyRange::prefix(&prefix), false, 10_000).await?;
    items.iter().map(decode_schema).collect()
}

fn decode_schema(it: &ReadValue) -> Result<TableSchema> {
    match &it.value {
        Value::Inline(b) => serde_json::from_slice(b).map_err(|e| Error::internal(format!("bad schema: {e}"))),
        _ => Err(Error::internal("bad schema value")),
    }
}

/// A table's schema. Pinned: every write derived from it must commit
/// against the schema it was derived from, whatever the isolation level.
pub async fn get_schema(txn: &mut Txn, name: &str) -> Result<TableSchema> {
    match txn.get_pinned(&catalog_key(name)).await? {
        Some(it) => decode_schema(&it),
        None => Err(Error::not_found(format!("table {name:?} not found"))),
    }
}

fn put_schema(txn: &mut Txn, s: &TableSchema) -> Result<()> {
    let data = serde_json::to_vec(s).map_err(|e| Error::internal(e.to_string()))?;
    txn.put(catalog_key(&s.name), Value::Inline(Bytes::from(data)))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: ColType,
    #[serde(default = "default_true")]
    pub nullable: bool,
    #[serde(default)]
    pub default: Option<Json>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexSpec {
    pub name: String,
    pub columns: Vec<String>,
    #[serde(default)]
    pub unique: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<ColumnSpec>,
    pub primary_key: Vec<String>,
    #[serde(default)]
    pub indexes: Vec<IndexSpec>,
    #[serde(default)]
    pub if_not_exists: bool,
}

pub async fn create_table(txn: &mut Txn, def: &CreateTable) -> Result<TableSchema> {
    if let Some(existing) = txn.get(&catalog_key(&def.name)).await? {
        if def.if_not_exists {
            return decode_schema(&existing);
        }
        return Err(Error::AlreadyExists(format!("table {:?} already exists", def.name)));
    }
    let next = match txn.get(&next_id_key()).await? {
        Some(ReadValue { value: Value::Inline(b), .. }) => std::str::from_utf8(&b)
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .ok_or_else(|| Error::internal("bad table id counter"))?,
        _ => 1,
    };
    let mut s = TableSchema {
        id: next,
        name: def.name.clone(),
        columns: Vec::new(),
        primary_key: def.primary_key.clone(),
        indexes: Vec::new(),
        next_column_id: 1,
        next_index_id: 1,
    };
    for c in &def.columns {
        s.columns.push(Column {
            id: s.next_column_id,
            name: c.name.clone(),
            ty: c.ty,
            nullable: c.nullable && !def.primary_key.contains(&c.name),
            default: c.default.clone(),
        });
        s.next_column_id += 1;
    }
    for i in &def.indexes {
        s.indexes.push(IndexDef {
            id: s.next_index_id,
            name: i.name.clone(),
            columns: i.columns.clone(),
            unique: i.unique,
            state: IndexState::Ready,
        });
        s.next_index_id += 1;
    }
    s.validate()?;
    txn.put(next_id_key(), Value::Inline(Bytes::from((next + 1).to_string())))?;
    put_schema(txn, &s)?;
    Ok(s)
}

pub async fn drop_table(txn: &mut Txn, name: &str) -> Result<()> {
    let s = get_schema(txn, name).await?;
    for space in [SPACE_ROWS, SPACE_INDEX, SPACE_ROW_BLOBS] {
        txn.delete_range(KeyRange::prefix(&table_prefix(space, s.id)))?;
    }
    for i in s.indexes.iter().filter(|i| i.state == IndexState::Building) {
        txn.delete(backfill_key(s.id, i.id))?;
    }
    txn.delete(catalog_key(name))
}

pub async fn add_column(txn: &mut Txn, table: &str, c: &ColumnSpec) -> Result<TableSchema> {
    let mut s = get_schema(txn, table).await?;
    if s.column(&c.name).is_some() {
        return Err(Error::AlreadyExists(format!("column {:?} already exists", c.name)));
    }
    if !c.nullable && c.default.is_none() {
        return Err(Error::invalid("a new non-nullable column needs a default"));
    }
    s.columns.push(Column {
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

pub async fn drop_column(txn: &mut Txn, table: &str, name: &str) -> Result<TableSchema> {
    let mut s = get_schema(txn, table).await?;
    let col = s.column(name).cloned().ok_or_else(|| Error::not_found(format!("column {name:?} not found")))?;
    if s.primary_key.iter().any(|p| p == name) {
        return Err(Error::invalid("cannot drop a primary key column"));
    }
    if let Some(i) = s.indexes.iter().find(|i| i.columns.iter().any(|c| c == name)) {
        return Err(Error::invalid(format!("column is used by index {:?}; drop it first", i.name)));
    }
    if col.ty == ColType::Blob {
        let mut pages = Pages::new(KeyRange::prefix(&table_prefix(SPACE_ROW_BLOBS, s.id)));
        while let Some(page) = pages.next(txn).await? {
            for it in page.iter().filter(|it| it.key.ends_with(&col.id.to_be_bytes())) {
                txn.delete(it.key.clone())?;
            }
        }
    }
    s.columns.retain(|c| c.name != name);
    s.validate()?;
    put_schema(txn, &s)?;
    Ok(s)
}

/// Batch sizes of an index backfill (see [`add_index`]).
#[derive(Clone, Copy, Debug)]
pub struct Backfill {
    /// Rows read per batch transaction.
    pub batch_rows: usize,
    /// Index bytes written per batch transaction (a batch stops early once
    /// it has written this much).
    pub batch_bytes: usize,
}

impl Default for Backfill {
    fn default() -> Backfill {
        Backfill { batch_rows: 1000, batch_bytes: 1 << 20 }
    }
}

/// Conflicts a backfill transaction retries before giving up.
const BACKFILL_RETRIES: u32 = 50;

/// Run `$body` (with `$t: &mut Txn`) in a serializable transaction on
/// `$db`, retrying conflicts. Evaluates to `(T, Seq)`; returns errors from
/// the enclosing function.
macro_rules! retrying_txn {
    ($db:expr, |$t:ident| $body:expr) => {{
        let mut attempt_ = 0u32;
        loop {
            let mut txn_ = $db.begin(Isolation::Serializable, Consistency::Strong).await?;
            let out_ = {
                let $t: &mut Txn = &mut txn_;
                $body
            }?;
            match $db.commit(txn_.into_request()).await {
                Ok(v) => break (out_, v),
                Err(Error::Conflict(_)) if attempt_ < BACKFILL_RETRIES => {
                    attempt_ += 1;
                    let d = 5 * (1u64 << attempt_.min(6)) + $db.shared().env.random_below(10);
                    tokio::time::sleep(std::time::Duration::from_millis(d)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }};
}

/// Add an index to a table without blocking writes to it.
///
/// The index is first added in the `building` state, in which every row
/// write maintains it but queries cannot use it yet. It is then backfilled
/// from the table's rows in batches of separate transactions, the last of
/// which marks it ready. Returns the schema and the version of that last
/// transaction.
///
/// If the build is interrupted (a crash, a lost connection), the index
/// stays `building`; calling `add_index` again with the same definition
/// resumes it. A unique index whose rows turn out to hold duplicate values
/// is removed again, and the call fails with `AlreadyExists`.
pub async fn add_index(db: &Arc<Db>, table: &str, spec: &IndexSpec, opts: Backfill) -> Result<(TableSchema, Seq)> {
    let (idx, _) = retrying_txn!(db, |t| begin_index(t, table, spec).await);
    match backfill(db, table, &idx, opts).await {
        Err(Error::AlreadyExists(m)) => {
            retrying_txn!(db, |t| abandon_index(t, table, &idx).await);
            Err(Error::AlreadyExists(m))
        }
        other => other,
    }
}

/// Add `spec` to the table as a building index, or find the building
/// index with the same definition to resume.
async fn begin_index(txn: &mut Txn, table: &str, spec: &IndexSpec) -> Result<IndexDef> {
    let mut s = get_schema(txn, table).await?;
    if let Some(i) = s.index(&spec.name) {
        if i.state == IndexState::Building && i.columns == spec.columns && i.unique == spec.unique {
            return Ok(i.clone());
        }
        return Err(Error::AlreadyExists(format!("index {:?} already exists", spec.name)));
    }
    let idx = IndexDef {
        id: s.next_index_id,
        name: spec.name.clone(),
        columns: spec.columns.clone(),
        unique: spec.unique,
        state: IndexState::Building,
    };
    s.validate_index(&idx)?;
    s.next_index_id += 1;
    s.indexes.push(idx.clone());
    put_schema(txn, &s)?;
    txn.put(backfill_key(s.id, idx.id), Value::Inline(Bytes::new()))?;
    Ok(idx)
}

async fn backfill(db: &Arc<Db>, table: &str, idx: &IndexDef, opts: Backfill) -> Result<(TableSchema, Seq)> {
    loop {
        if let (Some(s), v) = retrying_txn!(db, |t| backfill_batch(t, table, idx, opts).await) {
            return Ok((s, v));
        }
    }
}

/// Backfill the next batch of rows into `idx`. Returns the schema once the
/// index is ready.
async fn backfill_batch(txn: &mut Txn, table: &str, idx: &IndexDef, opts: Backfill) -> Result<Option<TableSchema>> {
    let mut s = get_schema(txn, table).await?;
    let state = s.indexes.iter().find(|i| i.id == idx.id).map(|i| i.state);
    match state {
        None => return Err(Error::not_found(format!("index {:?} was dropped while it was being built", idx.name))),
        Some(IndexState::Ready) => return Ok(Some(s)),
        Some(IndexState::Building) => {}
    }
    let ck = backfill_key(s.id, idx.id);
    let cursor = match txn.get(&ck).await? {
        Some(ReadValue { value: Value::Inline(c), .. }) => c,
        _ => return Err(Error::internal(format!("index {:?} has no backfill cursor", idx.name))),
    };
    let prefix = table_prefix(SPACE_ROWS, s.id);
    let mut range = KeyRange::prefix(&prefix);
    if !cursor.is_empty() {
        range.start = Bound::Excluded(cursor);
    }
    let batch = opts.batch_rows.max(1);
    let page = txn.scan(&range, false, batch).await?;
    let mut last = None;
    let (mut bytes, mut written) = (0, 0);
    for it in &page {
        let (pk, row) = decode_stored_row(&s, &prefix, it)?;
        let (k, v) = index_entry(&s, idx, &row, &pk)?;
        if idx.unique
            && !v.is_empty()
            && let Some(existing) = txn.get(&k).await?
            && !matches!(&existing.value, Value::Inline(b) if *b == v)
        {
            return Err(Error::AlreadyExists(format!("duplicate values for unique index {:?}", idx.name)));
        }
        bytes += k.len() + v.len();
        txn.put_expiring(k, Value::Inline(v), it.ttl)?;
        last = Some(it.key.clone());
        written += 1;
        if bytes >= opts.batch_bytes {
            break;
        }
    }
    // A short page read to its end leaves no rows behind it.
    let done = written == page.len() && page.len() < batch;
    if !done {
        let last = last.ok_or_else(|| Error::internal("empty backfill batch"))?;
        txn.put(ck, Value::Inline(last))?;
        return Ok(None);
    }
    txn.delete(ck)?;
    for i in s.indexes.iter_mut().filter(|i| i.id == idx.id) {
        i.state = IndexState::Ready;
    }
    put_schema(txn, &s)?;
    Ok(Some(s))
}

/// Remove a building index and everything written for it.
async fn abandon_index(txn: &mut Txn, table: &str, idx: &IndexDef) -> Result<()> {
    let mut s = get_schema(txn, table).await?;
    if s.indexes.iter().any(|i| i.id == idx.id && i.state == IndexState::Building) {
        txn.delete_range(KeyRange::prefix(&index_prefix(s.id, idx.id)))?;
        txn.delete(backfill_key(s.id, idx.id))?;
        s.indexes.retain(|i| i.id != idx.id);
        put_schema(txn, &s)?;
    }
    Ok(())
}

pub async fn drop_index(txn: &mut Txn, table: &str, name: &str) -> Result<TableSchema> {
    let mut s = get_schema(txn, table).await?;
    let idx = s.index(name).cloned().ok_or_else(|| Error::not_found(format!("index {name:?} not found")))?;
    txn.delete_range(KeyRange::prefix(&index_prefix(s.id, idx.id)))?;
    if idx.state == IndexState::Building {
        txn.delete(backfill_key(s.id, idx.id))?;
    }
    s.indexes.retain(|i| i.name != name);
    put_schema(txn, &s)?;
    Ok(s)
}

// ---------------------------------------------------------------- rows

/// The key of a row from [`row_from_json`], which has already rejected
/// missing and null key columns.
fn pk_keys_of_row(s: &TableSchema, row: &Row) -> Result<Vec<Key>> {
    s.primary_key
        .iter()
        .map(|p| {
            s.column(p)
                .and_then(|c| row.get(&c.id))
                .and_then(Datum::to_key)
                .ok_or_else(|| Error::internal(format!("row has no key value for {p:?}")))
        })
        .collect()
}

/// Parse a primary key given as a scalar (single-column keys) or an array.
pub fn parse_pk(s: &TableSchema, pk: &Json) -> Result<Vec<Key>> {
    let vals = match pk {
        Json::Array(a) => a.as_slice(),
        scalar => std::slice::from_ref(scalar),
    };
    if vals.len() != s.primary_key.len() {
        return Err(Error::invalid(format!("primary key has {} columns, got {}", s.primary_key.len(), vals.len())));
    }
    s.primary_key
        .iter()
        .zip(vals)
        .map(|(p, v)| {
            let c = s.column(p).ok_or_else(|| Error::internal("pk column missing"))?;
            if v.is_null() {
                return Err(Error::invalid("primary key values cannot be null"));
            }
            key_from_json(c.ty, v, p)
        })
        .collect()
}

fn index_entry(s: &TableSchema, idx: &IndexDef, row: &Row, pk: &[Key]) -> Result<(Bytes, Bytes)> {
    let mut vals = Vec::with_capacity(idx.columns.len());
    for c in &idx.columns {
        let col = s.column(c).ok_or_else(|| Error::internal("index column missing"))?;
        let d = row.get(&col.id).cloned().unwrap_or(Datum::Null);
        vals.push(d.to_key().ok_or_else(|| Error::internal("index value not indexable"))?);
    }
    let has_null = vals.contains(&Key::Null);
    let mut k = index_prefix(s.id, idx.id);
    k.extend_from_slice(&tuple::encode(&vals));
    if idx.unique && !has_null {
        Ok((Bytes::from(k), Bytes::from(tuple::encode(pk))))
    } else {
        k.extend_from_slice(&tuple::encode(pk));
        Ok((Bytes::from(k), Bytes::new()))
    }
}

/// Resolves staged blob ids for blob columns.
pub struct BlobResolver<'a> {
    pub db: &'a Arc<Db>,
}

impl BlobResolver<'_> {
    async fn resolve(&self, id: &str) -> Result<BlobRef> {
        blob::load_staged(self.db.shared(), &self.db.id, id).await
    }
}

/// Build a full row from JSON (applying defaults), resolving blob ids.
async fn row_from_json(s: &TableSchema, obj: &Json, blobs: &BlobResolver<'_>, base: Option<&Row>) -> Result<Row> {
    let obj = obj.as_object().ok_or_else(|| Error::invalid("row must be a JSON object"))?;
    for k in obj.keys() {
        if s.column(k).is_none() {
            return Err(Error::invalid(format!("unknown column {k:?}")));
        }
    }
    let mut row = Row::new();
    for c in &s.columns {
        let d = match obj.get(&c.name) {
            Some(v) => {
                let d = datum_from_json(c.ty, v, &c.name)?;
                match (c.ty, d) {
                    (ColType::Blob, Datum::Str(id)) => Datum::Blob(blobs.resolve(&id).await?),
                    (_, d) => d,
                }
            }
            None => match base.and_then(|b| b.get(&c.id)) {
                Some(d) => d.clone(),
                None => match &c.default {
                    Some(dv) => datum_from_json(c.ty, dv, &c.name)?,
                    None => Datum::Null,
                },
            },
        };
        if d.is_null() && !c.nullable {
            return Err(Error::invalid(format!("column {:?} must not be null", c.name)));
        }
        row.insert(c.id, d);
    }
    Ok(row)
}

/// Fill in defaults (or null) for columns missing from a stored row.
fn fill_defaults(s: &TableSchema, row: &mut Row) -> Result<()> {
    for c in &s.columns {
        if !row.contains_key(&c.id) && c.ty != ColType::Blob {
            let d = match &c.default {
                Some(dv) => datum_from_json(c.ty, dv, &c.name)?,
                None => Datum::Null,
            };
            row.insert(c.id, d);
        }
    }
    Ok(())
}

/// A stored row (with its blob columns) and when it expires.
type Stored = (Row, Option<Ttl>);

/// Read a stored row (with blob columns) by primary key.
async fn load_row(txn: &mut Txn, s: &TableSchema, pk: &[Key]) -> Result<Option<Stored>> {
    let Some(it) = txn.get(&row_key(s.id, pk)).await? else {
        return Ok(None);
    };
    let Value::Inline(data) = &it.value else {
        return Err(Error::internal("row stored out of line"));
    };
    // Values of dropped columns stay until the row is rewritten; readers
    // only look up the schema's columns.
    let mut row = schema::decode_row(data)?;
    fill_defaults(s, &mut row)?;
    load_blob_columns(txn, s, pk, &mut row).await?;
    Ok(Some((row, it.ttl)))
}

async fn load_blob_columns(txn: &mut Txn, s: &TableSchema, pk: &[Key], row: &mut Row) -> Result<()> {
    for c in &s.columns {
        if c.ty == ColType::Blob {
            let d = match txn.get(&blob_side_key(s.id, pk, c.id)).await? {
                Some(ReadValue { value: Value::Blob(b), .. }) => Datum::Blob(b),
                _ => Datum::Null,
            };
            row.insert(c.id, d);
        }
    }
    Ok(())
}

/// Write a row and its blob columns, all to expire as `ttl` says, over the
/// stored row `old` (if any).
fn write_row(
    txn: &mut Txn,
    s: &TableSchema,
    pk: &[Key],
    row: &Row,
    ttl: Option<Ttl>,
    old: Option<&Stored>,
) -> Result<()> {
    txn.put_expiring(row_key(s.id, pk), Value::Inline(Bytes::from(schema::encode_row(row))), ttl)?;
    for c in &s.columns {
        if c.ty != ColType::Blob {
            continue;
        }
        let new = row.get(&c.id);
        let prev = old.and_then(|(o, _)| o.get(&c.id));
        if new == prev && old.is_some_and(|(_, t)| *t == ttl) {
            continue;
        }
        match new {
            Some(Datum::Blob(b)) => txn.put_expiring(blob_side_key(s.id, pk, c.id), Value::Blob(b.clone()), ttl)?,
            _ => txn.delete(blob_side_key(s.id, pk, c.id))?,
        }
    }
    Ok(())
}

/// Move a row's index entries from those of `old` to those of `new`. An
/// index entry expires with its row.
async fn update_indexes(
    txn: &mut Txn,
    s: &TableSchema,
    pk: &[Key],
    old: Option<(&Row, Option<Ttl>)>,
    new: Option<(&Row, Option<Ttl>)>,
) -> Result<()> {
    for idx in &s.indexes {
        let entry = |r: Option<(&Row, Option<Ttl>)>| -> Result<Option<(Bytes, Bytes, Option<Ttl>)>> {
            r.map(|(r, e)| index_entry(s, idx, r, pk).map(|(k, v)| (k, v, e))).transpose()
        };
        let (o, n) = (entry(old)?, entry(new)?);
        if o == n {
            continue;
        }
        if let Some((ok, _, _)) = &o {
            txn.delete(ok.clone())?;
        }
        if let Some((nk, nv, ne)) = n {
            if idx.unique
                && !nv.is_empty()
                && let Some(existing) = txn.get(&nk).await?
            {
                let same_row = matches!(&existing.value, Value::Inline(b) if *b == nv);
                if !same_row {
                    return Err(Error::AlreadyExists(format!("unique index {:?} already has this value", idx.name)));
                }
            }
            txn.put_expiring(nk, Value::Inline(nv), ne)?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InsertMode {
    Insert,
    Upsert,
}

/// What a row update does to the row's expiry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expiry {
    Keep,
    Set(Option<Ttl>),
}

impl Expiry {
    /// The time to live of a row that had `old`.
    fn of(self, old: Option<Ttl>) -> Option<Ttl> {
        match self {
            Expiry::Keep => old,
            Expiry::Set(t) => t,
        }
    }
}

/// Insert (or with `Upsert`, replace) a row.
pub async fn insert(txn: &mut Txn, blobs: &BlobResolver<'_>, table: &str, obj: &Json, mode: InsertMode) -> Result<()> {
    insert_expiring(txn, blobs, table, obj, mode, None).await
}

/// [`insert`] a row that expires as `ttl` says.
pub async fn insert_expiring(
    txn: &mut Txn,
    blobs: &BlobResolver<'_>,
    table: &str,
    obj: &Json,
    mode: InsertMode,
    ttl: Option<Ttl>,
) -> Result<()> {
    let s = get_schema(txn, table).await?;
    let row = row_from_json(&s, obj, blobs, None).await?;
    let pk = pk_keys_of_row(&s, &row)?;
    let old = load_row(txn, &s, &pk).await?;
    if old.is_some() && mode == InsertMode::Insert {
        return Err(Error::AlreadyExists(format!("row with this primary key already exists in {table:?}")));
    }
    update_indexes(txn, &s, &pk, old.as_ref().map(|(r, t)| (r, *t)), Some((&row, ttl))).await?;
    write_row(txn, &s, &pk, &row, ttl, old.as_ref())
}

/// Set some columns of a row, if it exists.
pub async fn update(txn: &mut Txn, blobs: &BlobResolver<'_>, table: &str, pk: &Json, set: &Json) -> Result<bool> {
    update_expiring(txn, blobs, table, pk, set, Expiry::Keep).await
}

/// [`update`], also changing when the row expires.
pub async fn update_expiring(
    txn: &mut Txn,
    blobs: &BlobResolver<'_>,
    table: &str,
    pk: &Json,
    set: &Json,
    expiry: Expiry,
) -> Result<bool> {
    let s = get_schema(txn, table).await?;
    let pk = parse_pk(&s, pk)?;
    let Some(old) = load_row(txn, &s, &pk).await? else {
        return Ok(false);
    };
    if let Some(obj) = set.as_object() {
        for p in &s.primary_key {
            if obj.contains_key(p) {
                return Err(Error::invalid("primary key columns cannot be updated"));
            }
        }
    }
    update_stored(txn, blobs, &s, &pk, old, set, expiry).await?;
    Ok(true)
}

async fn update_stored(
    txn: &mut Txn,
    blobs: &BlobResolver<'_>,
    s: &TableSchema,
    pk: &[Key],
    old: Stored,
    set: &Json,
    expiry: Expiry,
) -> Result<()> {
    let row = row_from_json(s, set, blobs, Some(&old.0)).await?;
    let ttl = expiry.of(old.1);
    update_indexes(txn, s, pk, Some((&old.0, old.1)), Some((&row, ttl))).await?;
    write_row(txn, s, pk, &row, ttl, Some(&old))
}

pub async fn delete(txn: &mut Txn, table: &str, pk: &Json) -> Result<bool> {
    let s = get_schema(txn, table).await?;
    let pk = parse_pk(&s, pk)?;
    let Some(old) = load_row(txn, &s, &pk).await? else {
        return Ok(false);
    };
    delete_stored(txn, &s, &pk, &old).await?;
    Ok(true)
}

async fn delete_stored(txn: &mut Txn, s: &TableSchema, pk: &[Key], old: &Stored) -> Result<()> {
    update_indexes(txn, s, pk, Some((&old.0, old.1)), None).await?;
    txn.delete(row_key(s.id, pk))?;
    if s.columns.iter().any(|c| c.ty == ColType::Blob) {
        txn.delete_range(KeyRange::prefix(&blob_side_prefix(s.id, pk)))?;
    }
    Ok(())
}

/// Update every row `sel` selects (at most `limit`), with `set` applied as
/// by [`update`]. Returns how many rows were updated. The transaction's
/// write limit bounds how many rows one call may change.
pub async fn update_where(
    txn: &mut Txn,
    blobs: &BlobResolver<'_>,
    table: &str,
    sel: &Selection,
    set: &Json,
    expiry: Expiry,
    limit: Option<usize>,
) -> Result<usize> {
    let s = get_schema(txn, table).await?;
    check_set(&s, set)?;
    let rows = select(txn, &s, sel, limit).await?;
    for (pk, old) in &rows {
        update_stored(txn, blobs, &s, pk, old.clone(), set, expiry).await?;
    }
    Ok(rows.len())
}

/// Delete every row `sel` selects (at most `limit`). Returns how many.
pub async fn delete_where(txn: &mut Txn, table: &str, sel: &Selection, limit: Option<usize>) -> Result<usize> {
    let s = get_schema(txn, table).await?;
    let rows = select(txn, &s, sel, limit).await?;
    for (pk, old) in &rows {
        delete_stored(txn, &s, pk, old).await?;
    }
    Ok(rows.len())
}

/// `set` must be an object of columns other than the primary key's, each
/// with a value of the column's type.
fn check_set(s: &TableSchema, set: &Json) -> Result<()> {
    let obj = set.as_object().ok_or_else(|| Error::invalid("set must be a JSON object"))?;
    for (k, v) in obj {
        let c = s.column(k).ok_or_else(|| Error::invalid(format!("unknown column {k:?}")))?;
        if s.primary_key.contains(k) {
            return Err(Error::invalid("primary key columns cannot be updated"));
        }
        if v.is_null() && !c.nullable {
            return Err(Error::invalid(format!("column {k:?} must not be null")));
        }
        datum_from_json(c.ty, v, k)?;
    }
    Ok(())
}

/// The rows `sel` selects, in its access path's order, at most `limit`.
/// All are read before any is written, so writes cannot move a row into
/// the part of an index still to be read.
async fn select(
    txn: &mut Txn,
    s: &TableSchema,
    sel: &Selection,
    limit: Option<usize>,
) -> Result<Vec<(Vec<Key>, Stored)>> {
    let path = access_path(s, sel)?;
    let mut out = Vec::new();
    let Some(range) = path.range.clone() else { return Ok(out) };
    if limit == Some(0) {
        return Ok(out);
    }
    // Every selected row costs a write of at least its key.
    let mut budget = txn.write_budget();
    let mut pages = Pages::new(range);
    while let Some(page) = pages.next(txn).await? {
        for it in &page {
            let (pk, row) = path.row(txn, s, it).await?;
            let Some(row) = row else { continue };
            if sel.filter.as_ref().is_some_and(|f| !eval_filter(s, f, &row.0)) {
                continue;
            }
            let cost = row_key(s.id, &pk).len() + 8;
            budget = budget.checked_sub(cost).ok_or_else(|| {
                Error::TooLarge("the selected rows exceed the transaction's write limit; use limit".into())
            })?;
            out.push((pk, row));
            if limit.is_some_and(|l| out.len() >= l) {
                return Ok(out);
            }
        }
    }
    Ok(out)
}

pub fn row_to_json(s: &TableSchema, row: &Row) -> Json {
    let mut m = Map::new();
    for c in &s.columns {
        m.insert(c.name.clone(), row.get(&c.id).map(|d| d.to_json()).unwrap_or(Json::Null));
    }
    Json::Object(m)
}

/// A row by primary key.
pub async fn get(txn: &mut Txn, table: &str, pk: &Json) -> Result<Option<Json>> {
    Ok(get_expiring(txn, table, pk).await?.map(|(r, _)| r))
}

/// A row by primary key, and when it expires.
pub async fn get_expiring(txn: &mut Txn, table: &str, pk: &Json) -> Result<Option<(Json, Option<u64>)>> {
    let s = get_schema(txn, table).await?;
    let pk = parse_pk(&s, pk)?;
    Ok(load_row(txn, &s, &pk).await?.map(|(r, t)| {
        (
            row_to_json(&s, &r),
            t.and_then(|t| match t {
                Ttl::Until(e) => Some(e),
                Ttl::For(_) => None,
            }),
        )
    }))
}

/// Reads a key range a page at a time, so that memory stays bounded however
/// large the range.
struct Pages {
    rest: Option<KeyRange>,
}

impl Pages {
    const SIZE: usize = 1000;

    fn new(range: KeyRange) -> Pages {
        Pages { rest: Some(range) }
    }

    async fn next(&mut self, txn: &mut Txn) -> Result<Option<Vec<ReadValue>>> {
        let Some(range) = self.rest.take() else {
            return Ok(None);
        };
        let page = txn.scan(&range, false, Self::SIZE).await?;
        if let Some(last) = page.last()
            && page.len() == Self::SIZE
        {
            self.rest = Some(KeyRange::new(Bound::Excluded(last.key.clone()), range.end));
        }
        Ok(Some(page))
    }
}

/// A row as stored under `prefix` (the table's row space), with defaults
/// applied and without its blob columns.
fn decode_stored_row(s: &TableSchema, prefix: &[u8], it: &ReadValue) -> Result<(Vec<Key>, Row)> {
    let pk = tuple::decode_all(&it.key[prefix.len()..])?;
    let Value::Inline(d) = &it.value else {
        return Err(Error::internal("row stored out of line"));
    };
    let mut row = schema::decode_row(d)?;
    fill_defaults(s, &mut row)?;
    Ok((pk, row))
}

/// Every row of a table by row key, with defaults applied, and its expiry.
async fn all_rows(txn: &mut Txn, s: &TableSchema) -> Result<Vec<(Bytes, Vec<Key>, Row, Option<u64>)>> {
    let prefix = table_prefix(SPACE_ROWS, s.id);
    let mut out = Vec::new();
    let mut pages = Pages::new(KeyRange::prefix(&prefix));
    while let Some(page) = pages.next(txn).await? {
        for it in &page {
            let (pk, row) = decode_stored_row(s, &prefix, it)?;
            out.push((it.key.clone(), pk, row, it.expires()));
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------- queries

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RangeSpec {
    pub gt: Option<Json>,
    pub gte: Option<Json>,
    pub lt: Option<Json>,
    pub lte: Option<Json>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Filter {
    And {
        and: Vec<Filter>,
    },
    Or {
        or: Vec<Filter>,
    },
    Not {
        not: Box<Filter>,
    },
    Cmp {
        col: String,
        op: CmpOp,
        #[serde(default)]
        value: Json,
    },
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    Prefix,
    IsNull,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Query {
    #[serde(default)]
    pub index: Option<String>,
    #[serde(default)]
    pub eq: Vec<Json>,
    #[serde(default)]
    pub range: Option<RangeSpec>,
    #[serde(default)]
    pub filter: Option<Filter>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub reverse: bool,
    #[serde(default)]
    pub cursor: Option<String>,
}

pub struct QueryResult {
    pub rows: Vec<Json>,
    pub next_cursor: Option<String>,
}

fn validate_filter(s: &TableSchema, f: &Filter, depth: usize) -> Result<()> {
    if depth > 16 {
        return Err(Error::invalid("filter nested too deeply"));
    }
    match f {
        Filter::And { and: v } | Filter::Or { or: v } => {
            for x in v {
                validate_filter(s, x, depth + 1)?;
            }
            Ok(())
        }
        Filter::Not { not } => validate_filter(s, not, depth + 1),
        Filter::Cmp { col, op, value } => {
            let c = s.column(col).ok_or_else(|| Error::invalid(format!("unknown column {col:?}")))?;
            match op {
                CmpOp::IsNull => Ok(()),
                CmpOp::In => {
                    let arr = value.as_array().ok_or_else(|| Error::invalid("'in' needs an array"))?;
                    if arr.len() > 1000 {
                        return Err(Error::invalid("'in' list too long"));
                    }
                    for v in arr {
                        cmp_value(c, v)?;
                    }
                    Ok(())
                }
                CmpOp::Prefix => {
                    if !matches!(c.ty, ColType::String | ColType::Bytes) {
                        return Err(Error::invalid("'prefix' needs a string or bytes column"));
                    }
                    cmp_value(c, value).map(|_| ())
                }
                _ => cmp_value(c, value).map(|_| ()),
            }
        }
    }
}

fn cmp_value(c: &Column, v: &Json) -> Result<Datum> {
    match c.ty {
        ColType::Json => Ok(Datum::Json(v.clone())),
        ColType::Blob => Err(Error::invalid("blob columns cannot be filtered (use is_null)")),
        ty => datum_from_json(ty, v, &c.name),
    }
}

fn datum_cmp(a: &Datum, b: &Datum) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Datum::Json(x), Datum::Json(y)) => (x == y).then_some(std::cmp::Ordering::Equal),
        _ => {
            let (ka, kb) = (a.to_key()?, b.to_key()?);
            if std::mem::discriminant(&ka) != std::mem::discriminant(&kb) {
                return None;
            }
            Some(ka.cmp_key(&kb))
        }
    }
}

fn eval_filter(s: &TableSchema, f: &Filter, row: &Row) -> bool {
    use std::cmp::Ordering::*;
    match f {
        Filter::And { and } => and.iter().all(|x| eval_filter(s, x, row)),
        Filter::Or { or } => or.iter().any(|x| eval_filter(s, x, row)),
        Filter::Not { not } => !eval_filter(s, not, row),
        Filter::Cmp { col, op, value } => {
            let Some(c) = s.column(col) else { return false };
            let d = row.get(&c.id).cloned().unwrap_or(Datum::Null);
            if *op == CmpOp::IsNull {
                return d.is_null();
            }
            if d.is_null() {
                return false;
            }
            match op {
                CmpOp::In => value.as_array().is_some_and(|arr| {
                    arr.iter().filter_map(|v| cmp_value(c, v).ok()).any(|v| datum_cmp(&d, &v) == Some(Equal))
                }),
                CmpOp::Prefix => match (&d, cmp_value(c, value)) {
                    (Datum::Str(x), Ok(Datum::Str(p))) => x.starts_with(&p),
                    (Datum::Bytes(x), Ok(Datum::Bytes(p))) => x.starts_with(&p),
                    _ => false,
                },
                _ => {
                    let Ok(v) = cmp_value(c, value) else { return false };
                    let o = datum_cmp(&d, &v);
                    match op {
                        CmpOp::Eq => o == Some(Equal),
                        CmpOp::Ne => o.is_some_and(|o| o != Equal),
                        CmpOp::Lt => o == Some(Less),
                        CmpOp::Le => matches!(o, Some(Less | Equal)),
                        CmpOp::Gt => o == Some(Greater),
                        CmpOp::Ge => matches!(o, Some(Greater | Equal)),
                        _ => false,
                    }
                }
            }
        }
    }
}

/// Which rows to read or change: an access path (the primary key, or an
/// index), equality on a prefix of its columns with an optional range on
/// the next one, and a residual filter.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Selection {
    #[serde(default)]
    pub index: Option<String>,
    #[serde(default)]
    pub eq: Vec<Json>,
    #[serde(default)]
    pub range: Option<RangeSpec>,
    #[serde(default)]
    pub filter: Option<Filter>,
}

/// The stored keys a selection reads.
struct AccessPath {
    /// Prefix of the path's keys: the table's rows or one index.
    base: Vec<u8>,
    index: Option<IndexDef>,
    /// `None` if the selection is empty.
    range: Option<KeyRange>,
}

impl AccessPath {
    /// The row a key of this path stands for, unless it has gone.
    async fn row(&self, txn: &mut Txn, s: &TableSchema, it: &ReadValue) -> Result<(Vec<Key>, Option<Stored>)> {
        let rest = &it.key[self.base.len()..];
        let pk = match &self.index {
            None => tuple::decode_all(rest)?,
            Some(idx) => {
                let (_, used) = tuple::decode_n(rest, idx.columns.len())?;
                let pk_bytes: &[u8] = match &it.value {
                    Value::Inline(v) if !v.is_empty() => v,
                    _ => &rest[used..],
                };
                tuple::decode_all(pk_bytes)?
            }
        };
        let row = load_row(txn, s, &pk).await?;
        Ok((pk, row))
    }
}

fn access_path(s: &TableSchema, sel: &Selection) -> Result<AccessPath> {
    if let Some(f) = &sel.filter {
        validate_filter(s, f, 0)?;
    }
    let (base, cols, index): (Vec<u8>, Vec<String>, Option<IndexDef>) = match &sel.index {
        None => (table_prefix(SPACE_ROWS, s.id), s.primary_key.clone(), None),
        Some(name) => {
            let idx = s.index(name).cloned().ok_or_else(|| Error::not_found(format!("index {name:?} not found")))?;
            if idx.state == IndexState::Building {
                return Err(Error::Unavailable {
                    message: format!("index {name:?} is still being built"),
                    retry_after_ms: Some(1000),
                    outcome_unknown: false,
                });
            }
            (index_prefix(s.id, idx.id), idx.columns.clone(), Some(idx))
        }
    };
    if sel.eq.len() > cols.len() {
        return Err(Error::invalid("more equality values than key columns"));
    }
    let mut prefix = base.clone();
    for (v, c) in sel.eq.iter().zip(&cols) {
        let col = s.column(c).ok_or_else(|| Error::internal("column missing"))?;
        let k = if v.is_null() { Key::Null } else { key_from_json(col.ty, v, c)? };
        tuple::encode_one(&mut prefix, &k);
    }
    let pfx_end = crate::util::prefix_end(&prefix).map(Bytes::from);
    let mut start = Bound::Included(Bytes::from(prefix.clone()));
    let mut end = pfx_end.clone().map(Bound::Excluded).unwrap_or(Bound::Unbounded);
    if let Some(r) = &sel.range {
        let c = cols
            .get(sel.eq.len())
            .ok_or_else(|| Error::invalid("range needs a key column after the equality values"))?;
        let col = s.column(c).ok_or_else(|| Error::internal("column missing"))?;
        let enc = |v: &Json| -> Result<Vec<u8>> {
            let mut p = prefix.clone();
            tuple::encode_one(&mut p, &key_from_json(col.ty, v, c)?);
            Ok(p)
        };
        if r.gt.is_some() && r.gte.is_some() || r.lt.is_some() && r.lte.is_some() {
            return Err(Error::invalid("use only one lower and one upper bound"));
        }
        if let Some(v) = &r.gte {
            start = Bound::Included(Bytes::from(enc(v)?));
        }
        if let Some(v) = &r.gt {
            start = match crate::util::prefix_end(&enc(v)?) {
                Some(x) => Bound::Included(Bytes::from(x)),
                None => return Ok(AccessPath { base, index, range: None }),
            };
        }
        if let Some(v) = &r.lt {
            end = Bound::Excluded(Bytes::from(enc(v)?));
        }
        if let Some(v) = &r.lte {
            end = crate::util::prefix_end(&enc(v)?).map(|x| Bound::Excluded(Bytes::from(x))).unwrap_or(end);
        }
    }
    Ok(AccessPath { base, index, range: Some(KeyRange::new(start, end)) })
}

pub async fn query(txn: &mut Txn, table: &str, q: &Query) -> Result<QueryResult> {
    let s = get_schema(txn, table).await?;
    let limit = q.limit.unwrap_or(100);
    if limit == 0 || limit > 1000 {
        return Err(Error::invalid("limit must be in 1..=1000"));
    }
    let sel = Selection { index: q.index.clone(), eq: q.eq.clone(), range: q.range.clone(), filter: q.filter.clone() };
    let path = access_path(&s, &sel)?;
    let Some(mut range) = path.range.clone() else {
        return Ok(QueryResult { rows: vec![], next_cursor: None });
    };
    if let Some(c) = &q.cursor {
        let ck = Bytes::from(b64::decode_url(c).ok_or_else(|| Error::invalid("bad cursor"))?);
        if !range.contains(&ck) {
            return Err(Error::invalid("cursor does not belong to this query"));
        }
        range = match q.reverse {
            true => KeyRange::new(range.start, Bound::Excluded(ck)),
            false => KeyRange::new(Bound::Excluded(ck), range.end),
        };
    }
    let mut rows = Vec::new();
    let mut examined = 0usize;
    let page_size = (limit * 2).clamp(16, 1000);
    loop {
        let page = txn.scan(&range, q.reverse, page_size).await?;
        let full = page.len() == page_size;
        for (pos, it) in page.iter().enumerate() {
            examined += 1;
            if let (_, Some((row, _))) = path.row(txn, &s, it).await?
                && q.filter.as_ref().is_none_or(|f| eval_filter(&s, f, &row))
            {
                rows.push(row_to_json(&s, &row));
            }
            let stop = rows.len() >= limit || examined >= MAX_EXAMINED_PER_QUERY;
            if stop {
                let more = full || pos + 1 < page.len();
                let next_cursor = more.then(|| b64::encode_url(&it.key));
                return Ok(QueryResult { rows, next_cursor });
            }
        }
        if !full {
            return Ok(QueryResult { rows, next_cursor: None });
        }
        let last = page.last().map(|l| l.key.clone()).unwrap_or_default();
        range = if q.reverse {
            KeyRange::new(range.start.clone(), Bound::Excluded(last))
        } else {
            KeyRange::new(Bound::Excluded(last), range.end.clone())
        };
    }
}

/// A page of a table's row changes (see [`row_changes`]).
pub struct RowChanges {
    pub changes: Vec<Json>,
    pub more: bool,
    /// Where the next page starts.
    pub next: ChangePos,
}

/// The changes to `table`'s rows after `after`, at the transaction's
/// snapshot: for each row changed since, its state then, `{"version",
/// "pk", "row", "expires_at"?}` or `{"version", "pk", "deleted": true}`.
pub async fn row_changes(txn: &mut Txn, table: &str, after: &ChangePos, limit: usize) -> Result<RowChanges> {
    let s = get_schema(txn, table).await?;
    let prefix = table_prefix(SPACE_ROWS, s.id);
    let snapshot = txn.snapshot.clone();
    let page = snapshot.changes(&KeyRange::prefix(&prefix), after, limit).await?;
    let mut changes = Vec::with_capacity(page.changes.len());
    for c in &page.changes {
        let (version, e) = match c {
            Change::Entry(e) => (e.seq, e.clone().at(txn.read_time)),
            // Rows are only ever deleted one by one while their table exists.
            Change::DeleteRange(t) => {
                changes.push(serde_json::json!({"version": t.seq, "deleted_range": true}));
                continue;
            }
        };
        let pk = tuple::decode_all(&e.key[prefix.len()..])?;
        let pk_json = Json::Array(pk.iter().cloned().map(|k| Datum::from_key(k).to_json()).collect());
        if e.value.is_tombstone() {
            changes.push(serde_json::json!({"version": version, "pk": pk_json, "deleted": true}));
            continue;
        }
        let rv =
            ReadValue { key: e.key.clone(), value: e.value.clone(), seq: Some(e.seq), ttl: e.expires.map(Ttl::Until) };
        let (_, mut row) = decode_stored_row(&s, &prefix, &rv)?;
        load_blob_columns(txn, &s, &pk, &mut row).await?;
        let mut j = serde_json::json!({"version": version, "pk": pk_json, "row": row_to_json(&s, &row)});
        if let Some(x) = e.expires {
            j["expires_at"] = serde_json::json!(x);
        }
        changes.push(j);
    }
    let next = match (page.more, page.changes.last()) {
        (true, Some(c)) => c.pos(),
        _ => after.clone().max(ChangePos::after_version(snapshot.version())),
    };
    Ok(RowChanges { changes, more: page.more, next })
}

/// Recompute every index of a table from its rows and compare with the
/// stored index entries (consistency checker used by tests and the DST).
/// A building index must hold the entries of every row its backfill has
/// passed, and no entry that matches no row.
pub async fn check_indexes(txn: &mut Txn, table: &str) -> Result<()> {
    use std::collections::BTreeMap;
    let s = get_schema(txn, table).await?;
    let rows = all_rows(txn, &s).await?;
    for idx in &s.indexes {
        let stored = txn.scan(&KeyRange::prefix(&index_prefix(s.id, idx.id)), false, usize::MAX).await?;
        let got: BTreeMap<Bytes, (Bytes, Option<u64>)> = stored
            .into_iter()
            .map(|it| {
                let expires = it.expires();
                match it.value {
                    Value::Inline(v) => (it.key, (v, expires)),
                    _ => (it.key, (Bytes::new(), expires)),
                }
            })
            .collect();
        if idx.state == IndexState::Building {
            let cursor = match txn.get(&backfill_key(s.id, idx.id)).await? {
                Some(ReadValue { value: Value::Inline(c), .. }) => c,
                _ => return Err(Error::internal(format!("building index {} has no backfill cursor", idx.name))),
            };
            let mut all = BTreeSet::new();
            for (key, pk, row, expires) in &rows {
                let (k, v) = index_entry(&s, idx, row, pk)?;
                let entry = (k, (v, *expires));
                if key <= &cursor && got.get(&entry.0) != Some(&entry.1) {
                    return Err(Error::internal(format!(
                        "building index {}.{} misses a backfilled row",
                        table, idx.name
                    )));
                }
                all.insert(entry);
            }
            if let Some((k, _)) = got.iter().find(|(k, v)| !all.contains(&((*k).clone(), (*v).clone()))) {
                return Err(Error::internal(format!(
                    "building index {}.{} has a stale entry {}",
                    table,
                    idx.name,
                    b64::hex(k)
                )));
            }
            continue;
        }
        let mut want = BTreeMap::new();
        for (_, pk, row, expires) in &rows {
            let (k, v) = index_entry(&s, idx, row, pk)?;
            if want.insert(k, (v, *expires)).is_some() {
                return Err(Error::internal(format!("unique index {} violated", idx.name)));
            }
        }
        if got != want {
            return Err(Error::internal(format!(
                "index {}.{} inconsistent: {} stored vs {} expected",
                table,
                idx.name,
                got.len(),
                want.len()
            )));
        }
    }
    Ok(())
}

/// Every row and index entry must belong to a live table and index, and
/// every row blob to a live row and blob column: dropping any of them must
/// leave nothing behind, even when racing writes.
pub async fn check_no_orphans(txn: &mut Txn) -> Result<()> {
    let tables = list_tables(txn).await?;
    let live: BTreeSet<u32> = tables.iter().map(|s| s.id).collect();
    let live_idx: BTreeSet<(u32, u32)> = tables.iter().flat_map(|s| s.indexes.iter().map(|i| (s.id, i.id))).collect();
    let blob_cols: BTreeSet<(u32, u32)> =
        tables.iter().flat_map(|s| s.columns.iter().filter(|c| c.ty == ColType::Blob).map(|c| (s.id, c.id))).collect();
    let building: BTreeSet<Bytes> = tables
        .iter()
        .flat_map(|s| s.indexes.iter().filter(|i| i.state == IndexState::Building).map(|i| backfill_key(s.id, i.id)))
        .collect();
    for it in txn.scan(&KeyRange::prefix(&[SPACE_CATALOG, b'b', b'/']), false, usize::MAX).await? {
        if !building.contains(&it.key) {
            return Err(Error::internal(format!("orphaned backfill cursor {}", b64::hex(&it.key))));
        }
    }
    let all = KeyRange::half_open(Bytes::from_static(&[SPACE_ROWS]), Some(Bytes::from_static(&[SPACE_ROW_BLOBS + 1])));
    let id_at = |k: &[u8], at: usize| k.get(at..at + 4).map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]));
    // Rows sort before their blobs (SPACE_ROWS < SPACE_ROW_BLOBS).
    let mut rows = BTreeSet::new();
    for it in txn.scan(&all, false, usize::MAX).await? {
        let k = &it.key;
        let orphan = match (k[0], id_at(k, 1)) {
            (SPACE_INDEX, Some(t)) => id_at(k, 5).is_none_or(|i| !live_idx.contains(&(t, i))),
            (SPACE_ROW_BLOBS, Some(t)) => {
                // The key is the row's key (in the row-blob space) and the column id.
                let (row, col) = k.split_at(k.len() - 4);
                let mut row = row.to_vec();
                row[0] = SPACE_ROWS;
                !rows.contains(&row) || id_at(col, 0).is_none_or(|c| !blob_cols.contains(&(t, c)))
            }
            (SPACE_ROWS, Some(t)) => {
                rows.insert(k.to_vec());
                !live.contains(&t)
            }
            _ => true,
        };
        if orphan {
            return Err(Error::internal(format!("orphaned entry {} of a dropped table or index", b64::hex(k))));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
