//! The JSON transaction-op language shared by one-shot transactions,
//! interactive transactions and the console. See SPEC §6.3.

use crate::engine::{BlobRef, Db, KeyRange, ReadValue, Txn, Value, blob};
use crate::error::{Error, Result};
use crate::table::{self, BlobResolver, InsertMode, Query, SPACE_KV, schema::blob_json};
use crate::util::b64;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value as Json, json};
use std::ops::Bound;
use std::sync::Arc;

pub const MAX_USER_KEY: usize = 4096;
pub const MAX_SCAN_LIMIT: usize = 1000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Encoding {
    #[default]
    Utf8,
    Base64,
}

impl Encoding {
    pub fn decode(&self, s: &str, what: &str) -> Result<Vec<u8>> {
        match self {
            Encoding::Utf8 => Ok(s.as_bytes().to_vec()),
            Encoding::Base64 => b64::decode(s).ok_or_else(|| Error::invalid(format!("{what} is not valid base64"))),
        }
    }

    /// Encode bytes for JSON output. In utf8 mode, non-UTF-8 data is
    /// reported with `None` (callers then include a base64 field).
    pub fn encode(&self, b: &[u8]) -> Json {
        match self {
            Encoding::Utf8 => match std::str::from_utf8(b) {
                Ok(s) => json!(s),
                Err(_) => Json::Null,
            },
            Encoding::Base64 => json!(b64::encode(b)),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Op {
    Get {
        key: String,
    },
    Scan {
        #[serde(default)]
        prefix: Option<String>,
        #[serde(default)]
        start: Option<String>,
        #[serde(default)]
        end: Option<String>,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        reverse: bool,
        #[serde(default)]
        keys_only: bool,
        #[serde(default)]
        cursor: Option<String>,
    },
    Put {
        key: String,
        #[serde(default)]
        value: Option<String>,
        #[serde(default)]
        blob_id: Option<String>,
    },
    Delete {
        key: String,
    },
    DeleteRange {
        #[serde(default)]
        prefix: Option<String>,
        #[serde(default)]
        start: Option<String>,
        #[serde(default)]
        end: Option<String>,
    },
    Check {
        key: String,
        #[serde(default)]
        exists: Option<bool>,
        #[serde(default)]
        version: Option<u64>,
        #[serde(default)]
        value: Option<String>,
    },
    Increment {
        key: String,
        #[serde(default = "one")]
        by: i64,
    },
    GetRow {
        table: String,
        pk: Json,
    },
    InsertRow {
        table: String,
        row: Json,
    },
    UpsertRow {
        table: String,
        row: Json,
    },
    UpdateRow {
        table: String,
        pk: Json,
        set: Json,
    },
    DeleteRow {
        table: String,
        pk: Json,
    },
    QueryRows {
        table: String,
        #[serde(default)]
        index: Option<String>,
        #[serde(default)]
        eq: Vec<Json>,
        #[serde(default)]
        range: Option<crate::table::RangeSpec>,
        #[serde(default)]
        filter: Option<crate::table::Filter>,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        reverse: bool,
        #[serde(default)]
        cursor: Option<String>,
    },
}

fn one() -> i64 {
    1
}

impl Op {
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            Op::Put { .. }
                | Op::Delete { .. }
                | Op::DeleteRange { .. }
                | Op::Increment { .. }
                | Op::InsertRow { .. }
                | Op::UpsertRow { .. }
                | Op::UpdateRow { .. }
                | Op::DeleteRow { .. }
        )
    }
}

/// Context for executing ops.
pub struct OpCtx<'a> {
    pub db: &'a Arc<Db>,
    pub encoding: Encoding,
    pub read_only: bool,
    /// Blob values up to this size are returned inline in JSON.
    pub json_value_max: u64,
}

pub fn user_key(k: &[u8]) -> Result<Bytes> {
    if k.is_empty() {
        return Err(Error::invalid("keys must not be empty"));
    }
    if k.len() > MAX_USER_KEY {
        return Err(Error::invalid(format!("keys are limited to {MAX_USER_KEY} bytes")));
    }
    let mut v = Vec::with_capacity(k.len() + 1);
    v.push(SPACE_KV);
    v.extend_from_slice(k);
    Ok(Bytes::from(v))
}

/// User-key range in the internal KV space.
pub fn user_range(enc: Encoding, prefix: Option<&str>, start: Option<&str>, end: Option<&str>) -> Result<KeyRange> {
    let space = [SPACE_KV];
    let space_end = Bytes::from_static(&[SPACE_KV + 1]);
    let mk = |b: &[u8]| -> Bytes {
        let mut v = Vec::with_capacity(b.len() + 1);
        v.push(SPACE_KV);
        v.extend_from_slice(b);
        Bytes::from(v)
    };
    if prefix.is_some() && (start.is_some() || end.is_some()) {
        return Err(Error::invalid("use either prefix or start/end"));
    }
    if let Some(p) = prefix {
        let p = enc.decode(p, "prefix")?;
        let full = mk(&p);
        let e = crate::util::prefix_end(&full).map(Bytes::from).unwrap_or(space_end);
        return Ok(KeyRange::half_open(full, Some(e)));
    }
    let s = match start {
        Some(s) => Bound::Included(mk(&enc.decode(s, "start")?)),
        None => Bound::Included(Bytes::copy_from_slice(&space)),
    };
    let e = match end {
        Some(e) => Bound::Excluded(mk(&enc.decode(e, "end")?)),
        None => Bound::Excluded(space_end),
    };
    Ok(KeyRange::new(s, e))
}

/// Render a value for a JSON result.
pub async fn value_json(ctx: &OpCtx<'_>, v: &Value, out: &mut serde_json::Map<String, Json>) -> Result<()> {
    match v {
        Value::Inline(b) => put_bytes(ctx.encoding, "value", b, out),
        Value::Blob(r) => {
            out.insert("blob".into(), blob_json(r));
            if r.size <= ctx.json_value_max {
                let data = blob::read_range(ctx.db.shared(), &ctx.db.id, r, 0..r.size).await?;
                put_bytes(ctx.encoding, "value", &data, out);
            }
        }
        Value::Tombstone => {}
    }
    Ok(())
}

fn put_bytes(enc: Encoding, field: &str, b: &[u8], out: &mut serde_json::Map<String, Json>) {
    match enc.encode(b) {
        Json::Null => {
            out.insert(format!("{field}_b64"), json!(b64::encode(b)));
        }
        j => {
            out.insert(field.into(), j);
        }
    }
}

async fn read_json(ctx: &OpCtx<'_>, r: Option<ReadValue>) -> Result<Json> {
    let mut m = serde_json::Map::new();
    match r {
        None => {
            m.insert("found".into(), json!(false));
        }
        Some(rv) => {
            m.insert("found".into(), json!(true));
            value_json(ctx, &rv.value, &mut m).await?;
            m.insert("version".into(), json!(rv.seq));
        }
    }
    Ok(Json::Object(m))
}

/// Store bytes as a value, moving large values out of band.
pub async fn make_value(db: &Arc<Db>, data: &[u8], content_type: &str) -> Result<Value> {
    if data.len() > db.shared().cfg.value_inline_max {
        Ok(Value::Blob(blob::write_blob(db.shared(), &db.id, data, content_type).await?))
    } else {
        Ok(Value::Inline(Bytes::copy_from_slice(data)))
    }
}

pub async fn value_bytes(db: &Db, v: &Value) -> Result<Bytes> {
    match v {
        Value::Inline(b) => Ok(b.clone()),
        Value::Blob(r) => blob::read_range(db.shared(), &db.id, r, 0..r.size).await,
        Value::Tombstone => Err(Error::internal("tombstone has no value")),
    }
}

/// Execute ops in order inside `txn`. Returns one result per op. Any error
/// aborts the request (the caller discards the transaction).
pub async fn exec_ops(ctx: &OpCtx<'_>, txn: &mut Txn, ops: &[Op]) -> Result<Vec<Json>> {
    if ops.len() > 10_000 {
        return Err(Error::invalid("too many ops (max 10000)"));
    }
    let mut results = Vec::with_capacity(ops.len());
    for (i, op) in ops.iter().enumerate() {
        if ctx.read_only && op.is_write() {
            return Err(Error::PermissionDenied("this credential is read-only".into()));
        }
        let r = exec_one(ctx, txn, op).await.map_err(|e| match e {
            Error::PreconditionFailed { message, .. } => Error::PreconditionFailed { message, op_index: Some(i) },
            other => other,
        })?;
        results.push(r);
    }
    Ok(results)
}

async fn exec_one(ctx: &OpCtx<'_>, txn: &mut Txn, op: &Op) -> Result<Json> {
    let enc = ctx.encoding;
    let blobs = BlobResolver { db: ctx.db };
    match op {
        Op::Get { key } => {
            let k = user_key(&enc.decode(key, "key")?)?;
            let r = txn.get(&k).await?;
            read_json(ctx, r).await
        }
        Op::Scan { prefix, start, end, limit, reverse, keys_only, cursor } => {
            let limit = limit.unwrap_or(100);
            if limit == 0 || limit > MAX_SCAN_LIMIT {
                return Err(Error::invalid(format!("limit must be in 1..={MAX_SCAN_LIMIT}")));
            }
            let mut range = user_range(enc, prefix.as_deref(), start.as_deref(), end.as_deref())?;
            if let Some(c) = cursor {
                let ck = user_key(&b64::decode_url(c).ok_or_else(|| Error::invalid("bad cursor"))?)?;
                if !range.contains(&ck) {
                    return Err(Error::invalid("cursor outside of the scanned range"));
                }
                range = if *reverse {
                    KeyRange::new(range.start, Bound::Excluded(ck))
                } else {
                    KeyRange::new(Bound::Excluded(ck), range.end)
                };
            }
            let items = txn.scan(&range, *reverse, limit + 1).await?;
            let more = items.len() > limit;
            let mut out = Vec::with_capacity(limit.min(items.len()));
            for it in items.iter().take(limit) {
                let mut m = serde_json::Map::new();
                put_bytes(enc, "key", &it.key[1..], &mut m);
                if !keys_only {
                    value_json(ctx, &it.value, &mut m).await?;
                } else if let Value::Blob(r) = &it.value {
                    m.insert("blob".into(), blob_json(r));
                }
                m.insert("version".into(), json!(it.seq));
                out.push(Json::Object(m));
            }
            let next = if more { items.get(limit - 1).map(|it| json!(b64::encode_url(&it.key[1..]))) } else { None };
            Ok(json!({"items": out, "next_cursor": next}))
        }
        Op::Put { key, value, blob_id } => {
            let k = user_key(&enc.decode(key, "key")?)?;
            let v = match (value, blob_id) {
                (Some(v), None) => make_value(ctx.db, &enc.decode(v, "value")?, "application/octet-stream").await?,
                (None, Some(id)) => Value::Blob(blob::load_staged(ctx.db.shared(), &ctx.db.id, id).await?),
                _ => return Err(Error::invalid("put needs exactly one of value or blob_id")),
            };
            txn.put(k, v)?;
            Ok(json!({}))
        }
        Op::Delete { key } => {
            let k = user_key(&enc.decode(key, "key")?)?;
            txn.delete(k)?;
            Ok(json!({}))
        }
        Op::DeleteRange { prefix, start, end } => {
            if prefix.is_none() && start.is_none() && end.is_none() {
                return Err(Error::invalid(
                    "delete_range needs a prefix or bounds (use prefix \"\" to clear everything)",
                ));
            }
            let r = user_range(enc, prefix.as_deref(), start.as_deref(), end.as_deref())?;
            txn.delete_range(r)?;
            Ok(json!({}))
        }
        Op::Check { key, exists, version, value } => {
            let k = user_key(&enc.decode(key, "key")?)?;
            let r = txn.get(&k).await?;
            let fail = |m: String| Error::PreconditionFailed { message: m, op_index: None };
            if exists.is_none() && version.is_none() && value.is_none() {
                return Err(Error::invalid("check needs exists, version or value"));
            }
            if let Some(e) = exists
                && r.is_some() != *e
            {
                return Err(fail(format!("key {} {}", key, if *e { "does not exist" } else { "exists" })));
            }
            if let Some(want) = version {
                let got = r.as_ref().and_then(|x| x.seq);
                if got != Some(*want) {
                    let got = got.map(|g| g.to_string()).unwrap_or_else(|| "none (missing or uncommitted)".into());
                    return Err(fail(format!("key {key} has version {got}, expected {want}")));
                }
            }
            if let Some(want) = value {
                let want = enc.decode(want, "value")?;
                let got = match &r {
                    Some(rv) => Some(value_bytes(ctx.db, &rv.value).await?),
                    None => None,
                };
                if got.as_deref() != Some(&want[..]) {
                    return Err(fail(format!("key {key} does not have the expected value")));
                }
            }
            Ok(json!({}))
        }
        Op::Increment { key, by } => {
            let k = user_key(&enc.decode(key, "key")?)?;
            let cur = match txn.get(&k).await? {
                None => 0i64,
                Some(rv) => {
                    let b = value_bytes(ctx.db, &rv.value).await?;
                    std::str::from_utf8(&b)
                        .ok()
                        .and_then(|s| s.trim().parse::<i64>().ok())
                        .ok_or_else(|| Error::invalid(format!("value of {key} is not a decimal integer")))?
                }
            };
            let n = cur.checked_add(*by).ok_or_else(|| Error::invalid("increment overflows i64"))?;
            txn.put(k, Value::Inline(Bytes::from(n.to_string())))?;
            Ok(json!({"value": n}))
        }
        Op::GetRow { table, pk } => {
            let row = table::get(txn, table, pk).await?;
            Ok(match row {
                Some(r) => json!({"found": true, "row": r}),
                None => json!({"found": false}),
            })
        }
        Op::InsertRow { table, row } => {
            table::insert(txn, &blobs, table, row, InsertMode::Insert).await?;
            Ok(json!({}))
        }
        Op::UpsertRow { table, row } => {
            table::insert(txn, &blobs, table, row, InsertMode::Upsert).await?;
            Ok(json!({}))
        }
        Op::UpdateRow { table, pk, set } => {
            let found = table::update(txn, &blobs, table, pk, set).await?;
            Ok(json!({"found": found}))
        }
        Op::DeleteRow { table, pk } => {
            let found = table::delete(txn, table, pk).await?;
            Ok(json!({"found": found}))
        }
        Op::QueryRows { table, index, eq, range, filter, limit, reverse, cursor } => {
            let query = Query {
                index: index.clone(),
                eq: eq.clone(),
                range: range.clone(),
                filter: filter.clone(),
                limit: *limit,
                reverse: *reverse,
                cursor: cursor.clone(),
            };
            let r = table::query(txn, table, &query).await?;
            Ok(json!({"rows": r.rows, "next_cursor": r.next_cursor}))
        }
    }
}

/// Blob ref to JSON (re-export for API handlers).
pub fn blob_meta(r: &BlobRef) -> Json {
    blob_json(r)
}
