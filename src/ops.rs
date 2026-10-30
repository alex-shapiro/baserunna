//! JSON transaction operations and execution.

use crate::engine::{Appended, BlobRef, Db, KeyRange, ReadValue, Ttl, Txn, Value, blob};
use crate::error::{Error, Result};
use crate::stream::{self, AppendTo, NewRecord, ReadSpec};
use crate::table::{self, BlobResolver, Expiry, InsertMode, Query, SPACE_KV, Selection, schema::blob_json};
use crate::util::b64;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value as Json, json};
use std::ops::Bound;
use std::sync::Arc;

pub const MAX_USER_KEY: usize = 4096;
pub const MAX_SCAN_LIMIT: usize = 1000;
pub const MAX_TXN_OPS: usize = 10_000;
pub const MAX_TTL_MS: u64 = 10 * 365 * 24 * 3_600_000;

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

    /// Encodes `b` as JSON, returning `Json::Null` for non-UTF-8 bytes in `Utf8` mode.
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
        #[serde(default)]
        ttl_ms: Option<u64>,
    },
    Delete {
        key: String,
    },
    Copy {
        from: String,
        to: String,
        #[serde(default)]
        ttl_ms: Option<u64>,
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
        #[serde(default)]
        ttl_ms: Option<u64>,
    },
    GetRow {
        table: String,
        pk: Json,
    },
    InsertRow {
        table: String,
        row: Json,
        #[serde(default)]
        ttl_ms: Option<u64>,
    },
    UpsertRow {
        table: String,
        row: Json,
        #[serde(default)]
        ttl_ms: Option<u64>,
    },
    UpdateRow {
        table: String,
        pk: Json,
        set: Json,
        #[serde(default)]
        ttl_ms: Option<u64>,
    },
    DeleteRow {
        table: String,
        pk: Json,
    },
    UpdateRows {
        table: String,
        #[serde(rename = "where")]
        selection: Selection,
        set: Json,
        #[serde(default)]
        ttl_ms: Option<u64>,
        #[serde(default)]
        limit: Option<usize>,
    },
    DeleteRows {
        table: String,
        #[serde(rename = "where")]
        selection: Selection,
        #[serde(default)]
        limit: Option<usize>,
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
    Append {
        stream: String,
        records: Vec<NewRecord>,
        #[serde(default)]
        expect_seq: Option<u64>,
        #[serde(default)]
        create: bool,
    },
    ReadStream(StreamRead),
    StreamInfo {
        stream: String,
    },
    TrimStream {
        stream: String,
        #[serde(default)]
        before_seq: Option<u64>,
        #[serde(default)]
        before_time: Option<u64>,
    },
    GetConsumer {
        stream: String,
        consumer: String,
    },
    SetConsumer {
        stream: String,
        consumer: String,
        seq: u64,
    },
    InsertEvents {
        table: String,
        rows: Vec<Json>,
    },
    QueryEvents(EventsQuery),
    DeleteEvents {
        table: String,
        #[serde(default)]
        from: Option<Json>,
        #[serde(default)]
        to: Option<Json>,
        #[serde(rename = "where")]
        filter: crate::events::query::Filter,
    },
}

/// Payload of a `query_events` transaction operation.
#[derive(Debug, Clone)]
pub struct EventsQuery {
    pub table: String,
    pub query: crate::events::query::Query,
}

impl<'de> Deserialize<'de> for EventsQuery {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<EventsQuery, D::Error> {
        use serde::de::Error as _;
        let mut fields = serde_json::Map::deserialize(d)?;
        let table = fields.remove("table").ok_or_else(|| D::Error::missing_field("table"))?;
        Ok(EventsQuery {
            table: serde_json::from_value(table).map_err(D::Error::custom)?,
            query: crate::events::query::Query::parse(Json::Object(fields))
                .map_err(|e| D::Error::custom(e.message()))?,
        })
    }
}

/// A `read_stream` op: a stream and a [`ReadSpec`] beside it.
#[derive(Debug, Clone)]
pub struct StreamRead {
    pub stream: String,
    pub spec: ReadSpec,
}

impl<'de> Deserialize<'de> for StreamRead {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<StreamRead, D::Error> {
        use serde::de::Error as _;
        let mut fields = serde_json::Map::deserialize(d)?;
        let stream = fields.remove("stream").ok_or_else(|| D::Error::missing_field("stream"))?;
        Ok(StreamRead {
            stream: serde_json::from_value(stream).map_err(D::Error::custom)?,
            spec: serde_json::from_value(Json::Object(fields)).map_err(D::Error::custom)?,
        })
    }
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
                | Op::Copy { .. }
                | Op::DeleteRange { .. }
                | Op::Increment { .. }
                | Op::InsertRow { .. }
                | Op::UpsertRow { .. }
                | Op::UpdateRow { .. }
                | Op::DeleteRow { .. }
                | Op::UpdateRows { .. }
                | Op::DeleteRows { .. }
                | Op::Append { .. }
                | Op::TrimStream { .. }
                | Op::SetConsumer { .. }
                | Op::InsertEvents { .. }
                | Op::DeleteEvents { .. }
        )
    }
}

/// Validates `ttl_ms` and converts it to a [`Ttl`].
pub fn expiry(ttl_ms: Option<u64>) -> Result<Option<Ttl>> {
    match ttl_ms {
        None => Ok(None),
        Some(t) if (1..=MAX_TTL_MS).contains(&t) => Ok(Some(Ttl::For(t))),
        Some(_) => Err(Error::invalid(format!("ttl_ms must be in 1..={MAX_TTL_MS}"))),
    }
}

fn batch_limit(limit: Option<usize>) -> Result<Option<usize>> {
    match limit {
        Some(0) => Err(Error::invalid("limit must be at least 1")),
        l => Ok(l),
    }
}

/// Context for executing ops.
pub struct OpCtx<'a> {
    pub db: &'a Arc<Db>,
    pub encoding: Encoding,
    pub access: crate::meta::Access,
    /// Blob values up to this size are returned inline in JSON.
    pub json_value_max: u64,
    /// Resource limits for `query_events` operations.
    pub query_limits: crate::events::exec::Limits,
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

/// Builds an internal [`KeyRange`] in the user KV keyspace.
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

/// Populates `out` with the JSON representation of `v`.
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

/// Inserts `b` into `out` under `field`, falling back to `{field}_b64` for non-UTF-8 bytes.
pub fn put_key(enc: Encoding, field: &str, b: &[u8], out: &mut serde_json::Map<String, Json>) {
    put_bytes(enc, field, b, out)
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
            put_expiry(rv.expires(), &mut m);
        }
    }
    Ok(Json::Object(m))
}

/// Inserts `expires_at` into `out` when `expires` is `Some`.
pub fn put_expiry(expires: Option<u64>, out: &mut serde_json::Map<String, Json>) {
    if let Some(e) = expires {
        out.insert("expires_at".into(), json!(e));
    }
}

/// Stores `data` inline or as an out-of-band blob if it exceeds `value_inline_max`.
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

/// Executes `ops` sequentially inside `txn`, returning one JSON result per operation.
pub async fn exec_ops(ctx: &OpCtx<'_>, txn: &mut Txn, ops: &[Op]) -> Result<Vec<Json>> {
    if ops.len() > MAX_TXN_OPS {
        return Err(Error::invalid(format!("too many ops (max {MAX_TXN_OPS})")));
    }
    let mut results = Vec::with_capacity(ops.len());
    for (i, op) in ops.iter().enumerate() {
        if op.is_write() {
            ctx.access.require_write()?;
        }
        let r = exec_one(ctx, txn, op, i).await.map_err(|e| match e {
            Error::PreconditionFailed { message, .. } => Error::PreconditionFailed { message, op_index: Some(i) },
            other => other,
        })?;
        results.push(r);
    }
    Ok(results)
}

async fn exec_one(ctx: &OpCtx<'_>, txn: &mut Txn, op: &Op, index: usize) -> Result<Json> {
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
                put_expiry(it.expires(), &mut m);
                out.push(Json::Object(m));
            }
            let next = if more { items.get(limit - 1).map(|it| json!(b64::encode_url(&it.key[1..]))) } else { None };
            Ok(json!({"items": out, "next_cursor": next}))
        }
        Op::Put { key, value, blob_id, ttl_ms } => {
            let k = user_key(&enc.decode(key, "key")?)?;
            let ttl = expiry(*ttl_ms)?;
            let v = match (value, blob_id) {
                (Some(v), None) => make_value(ctx.db, &enc.decode(v, "value")?, "application/octet-stream").await?,
                (None, Some(id)) => Value::Blob(blob::load_staged(ctx.db.shared(), &ctx.db.id, id).await?),
                _ => return Err(Error::invalid("put needs exactly one of value or blob_id")),
            };
            txn.put_expiring(k, v, ttl)?;
            Ok(json!({}))
        }
        Op::Delete { key } => {
            let k = user_key(&enc.decode(key, "key")?)?;
            txn.delete(k)?;
            Ok(json!({}))
        }
        Op::Copy { from, to, ttl_ms } => {
            let f = user_key(&enc.decode(from, "from")?)?;
            let t = user_key(&enc.decode(to, "to")?)?;
            if !txn.copy(&f, t, expiry(*ttl_ms)?).await? {
                return Err(Error::PreconditionFailed {
                    message: format!("key {from} does not exist"),
                    op_index: None,
                });
            }
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
        Op::Increment { key, by, ttl_ms } => {
            let k = user_key(&enc.decode(key, "key")?)?;
            let set = expiry(*ttl_ms)?;
            let (cur, kept) = match txn.get(&k).await? {
                None => (0i64, None),
                Some(rv) => {
                    let b = value_bytes(ctx.db, &rv.value).await?;
                    let n = std::str::from_utf8(&b)
                        .ok()
                        .and_then(|s| s.trim().parse::<i64>().ok())
                        .ok_or_else(|| Error::invalid(format!("value of {key} is not a decimal integer")))?;
                    (n, rv.ttl)
                }
            };
            let n = cur.checked_add(*by).ok_or_else(|| Error::invalid("increment overflows i64"))?;
            txn.put_expiring(k, Value::Inline(Bytes::from(n.to_string())), set.or(kept))?;
            Ok(json!({"value": n}))
        }
        Op::GetRow { table, pk } => {
            let row = table::get_expiring(txn, table, pk).await?;
            Ok(match row {
                Some((r, expires)) => {
                    let mut m = serde_json::Map::new();
                    m.insert("found".into(), json!(true));
                    m.insert("row".into(), r);
                    put_expiry(expires, &mut m);
                    Json::Object(m)
                }
                None => json!({"found": false}),
            })
        }
        Op::InsertRow { table, row, ttl_ms } => {
            table::insert_expiring(txn, &blobs, table, row, InsertMode::Insert, expiry(*ttl_ms)?).await?;
            Ok(json!({}))
        }
        Op::UpsertRow { table, row, ttl_ms } => {
            table::insert_expiring(txn, &blobs, table, row, InsertMode::Upsert, expiry(*ttl_ms)?).await?;
            Ok(json!({}))
        }
        Op::UpdateRow { table, pk, set, ttl_ms } => {
            let e = row_expiry(*ttl_ms)?;
            let found = table::update_expiring(txn, &blobs, table, pk, set, e).await?;
            Ok(json!({"found": found}))
        }
        Op::DeleteRow { table, pk } => {
            let found = table::delete(txn, table, pk).await?;
            Ok(json!({"found": found}))
        }
        Op::UpdateRows { table, selection, set, ttl_ms, limit } => {
            let e = row_expiry(*ttl_ms)?;
            let n = table::update_where(txn, &blobs, table, selection, set, e, batch_limit(*limit)?).await?;
            Ok(json!({"count": n}))
        }
        Op::DeleteRows { table, selection, limit } => {
            let n = table::delete_where(txn, table, selection, batch_limit(*limit)?).await?;
            Ok(json!({"count": n}))
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
        Op::Append { stream: name, records, expect_seq, create } => {
            let bodies = records.iter().cloned().map(|r| r.into_body(enc)).collect::<Result<Vec<_>>>()?;
            let shared = ctx.db.shared();
            let to = AppendTo {
                name,
                create: create.then(|| (shared.env.random_u64(), shared.time_ms())),
                expect: *expect_seq,
                op_index: Some(index),
                record_max: shared.cfg.stream_record_max,
            };
            stream::append(txn, &to, &bodies).await?;
            Ok(json!({"count": bodies.len()}))
        }
        Op::ReadStream(StreamRead { stream: name, spec }) => {
            let out = stream::read(txn, name, spec).await?;
            let records: Vec<Json> = out.records.iter().map(|(r, b)| stream::record_json(r, b, enc)).collect();
            Ok(json!({"records": records, "next_seq": out.next_seq, "done": out.done}))
        }
        Op::StreamInfo { stream: name } => {
            let (_, st) = stream::info(txn, name).await?;
            Ok(Json::Object(stream::state_json(&st)))
        }
        Op::TrimStream { stream: name, before_seq, before_time } => {
            stream::trim(txn, name, *before_seq, *before_time).await?;
            Ok(json!({}))
        }
        Op::GetConsumer { stream: name, consumer } => Ok(match stream::consumer(txn, name, consumer).await? {
            Some(c) => json!({"found": true, "seq": c.seq}),
            None => json!({"found": false}),
        }),
        Op::SetConsumer { stream: name, consumer, seq } => {
            stream::set_consumer(txn, name, consumer, *seq, ctx.db.shared().time_ms()).await?;
            Ok(json!({}))
        }
        Op::InsertEvents { table, rows } => {
            let n = crate::events::insert(txn, table, rows, ctx.db.shared().time_ms() as i64).await?;
            Ok(json!({"count": n}))
        }
        Op::QueryEvents(EventsQuery { table, query }) => {
            let s = crate::events::get_schema(txn, table).await?;
            let out = crate::events::exec::run(ctx.db.shared(), &ctx.db.id, txn, &s, query, &ctx.query_limits).await?;
            Ok(json!({"columns": out.columns, "rows": out.rows, "next_cursor": out.next_cursor, "stats": out.stats}))
        }
        Op::DeleteEvents { table, from, to, filter } => {
            let (from, to) = crate::events::time_bounds(from, to)?;
            let n = crate::events::maint::delete(ctx.db, txn, table, from, to, filter).await?;
            Ok(json!({"deleted": n}))
        }
    }
}

/// Adds the first seq each append received to the results of its op.
pub fn fill_appends(ops: &[Op], results: &mut [Json], appends: &[Appended]) {
    let appended = ops.iter().zip(results.iter_mut()).filter(|(o, _)| matches!(o, Op::Append { .. }));
    for ((_, r), a) in appended.zip(appends) {
        r["first_seq"] = json!(a.first_seq);
    }
}

/// Converts an optional `ttl_ms` on an update into an [`Expiry`] policy.
pub fn row_expiry(ttl_ms: Option<u64>) -> Result<Expiry> {
    Ok(match expiry(ttl_ms)? {
        Some(t) => Expiry::Set(Some(t)),
        None => Expiry::Keep,
    })
}

/// Formats `r` as a JSON blob metadata object.
pub fn blob_meta(r: &BlobRef) -> Json {
    blob_json(r)
}
