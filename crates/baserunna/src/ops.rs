//! JSON transaction operations and execution.

use crate::engine::{Appended, BlobRef, Db, KeyRange, ReadValue, Ttl, Txn, Value, blob};
use crate::error::{Error, Result};
use crate::stream::{self, AppendTo, ReadSpec};
use crate::table::{self, BlobResolver, Expiry, InsertMode, Query, SPACE_KV, Selection};
use baserunna_api::{self as api, OpResult, b64};
use bytes::Bytes;
use serde_json::{Value as Json, json};
use std::ops::Bound;
use std::sync::Arc;

pub const MAX_USER_KEY: usize = 4096;
pub const MAX_SCAN_LIMIT: usize = api::PAGE_MAX;
pub const MAX_TXN_OPS: usize = 10_000;
pub const MAX_TTL_MS: u64 = 10 * 365 * 24 * 3_600_000;

pub trait EncodingExt {
    fn decode(&self, s: &str, what: &str) -> Result<Vec<u8>>;

    /// Encodes `b` as JSON, returning `Json::Null` for non-UTF-8 bytes in `Utf8` mode.
    fn encode(&self, b: &[u8]) -> Json;

    /// Encodes `b` into `(field, field_b64)`, setting only the second element if `b` cannot be
    /// represented in `self`.
    fn text(&self, b: &[u8]) -> (Option<String>, Option<String>);
}

impl EncodingExt for api::Encoding {
    fn decode(&self, s: &str, what: &str) -> Result<Vec<u8>> {
        match self {
            api::Encoding::Utf8 => Ok(s.as_bytes().to_vec()),
            api::Encoding::Base64 => {
                b64::decode(s).ok_or_else(|| Error::invalid(format!("{what} is not valid base64")))
            }
        }
    }

    fn encode(&self, b: &[u8]) -> Json {
        match self.text(b) {
            (Some(t), _) => json!(t),
            (None, _) => Json::Null,
        }
    }

    fn text(&self, b: &[u8]) -> (Option<String>, Option<String>) {
        match self {
            api::Encoding::Utf8 => match std::str::from_utf8(b) {
                Ok(s) => (Some(s.to_string()), None),
                Err(_) => (None, Some(b64::encode(b))),
            },
            api::Encoding::Base64 => (Some(b64::encode(b)), None),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Op {
    Get {
        key: String,
    },
    Scan {
        prefix: Option<String>,
        start: Option<String>,
        end: Option<String>,
        limit: Option<usize>,
        reverse: bool,
        keys_only: bool,
        cursor: Option<String>,
    },
    Put {
        key: String,
        value: Option<String>,
        blob_id: Option<String>,
        ttl_ms: Option<u64>,
    },
    Delete {
        key: String,
    },
    Copy {
        from: String,
        to: String,
        ttl_ms: Option<u64>,
    },
    DeleteRange {
        prefix: Option<String>,
        start: Option<String>,
        end: Option<String>,
    },
    Check {
        key: String,
        exists: Option<bool>,
        version: Option<u64>,
        value: Option<String>,
    },
    Increment {
        key: String,
        by: i64,
        ttl_ms: Option<u64>,
    },
    GetRow {
        table: String,
        pk: Json,
    },
    InsertRow {
        table: String,
        row: Json,
        ttl_ms: Option<u64>,
    },
    UpsertRow {
        table: String,
        row: Json,
        ttl_ms: Option<u64>,
    },
    UpdateRow {
        table: String,
        pk: Json,
        set: Json,
        ttl_ms: Option<u64>,
    },
    DeleteRow {
        table: String,
        pk: Json,
    },
    UpdateRows {
        table: String,
        selection: Selection,
        set: Json,
        ttl_ms: Option<u64>,
        limit: Option<usize>,
    },
    DeleteRows {
        table: String,
        selection: Selection,
        limit: Option<usize>,
    },
    QueryRows {
        table: String,
        index: Option<String>,
        eq: Vec<Json>,
        range: Option<api::RangeSpec>,
        filter: Option<crate::table::Filter>,
        limit: Option<usize>,
        reverse: bool,
        cursor: Option<String>,
    },
    Append {
        stream: String,
        records: Vec<api::NewRecord>,
        expect_seq: Option<u64>,
        create: bool,
    },
    ReadStream(StreamRead),
    StreamInfo {
        stream: String,
    },
    TrimStream {
        stream: String,
        before_seq: Option<u64>,
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
        from: Option<Json>,
        to: Option<Json>,
        filter: crate::events::query::Filter,
    },
}

/// Payload of a `query_events` transaction operation.
#[derive(Debug, Clone)]
pub struct EventsQuery {
    pub table: String,
    pub query: crate::events::query::Query,
}

#[derive(Debug, Clone)]
pub struct StreamRead {
    pub stream: String,
    pub spec: ReadSpec,
}

fn parse<T: serde::de::DeserializeOwned>(v: Json) -> Result<T> {
    serde_json::from_value(v).map_err(|e| Error::invalid(format!("invalid JSON body: {e}")))
}

impl TryFrom<api::Op> for Op {
    type Error = Error;

    fn try_from(o: api::Op) -> Result<Op> {
        use api::Op as A;
        Ok(match o {
            A::Get { key } => Op::Get { key },
            A::Scan { prefix, start, end, limit, reverse, keys_only, cursor } => {
                Op::Scan { prefix, start, end, limit, reverse, keys_only, cursor }
            }
            A::Put { key, value, blob_id, ttl_ms } => Op::Put { key, value, blob_id, ttl_ms },
            A::Delete { key } => Op::Delete { key },
            A::Copy { from, to, ttl_ms } => Op::Copy { from, to, ttl_ms },
            A::DeleteRange { prefix, start, end } => Op::DeleteRange { prefix, start, end },
            A::Check { key, exists, version, value } => Op::Check { key, exists, version, value },
            A::Increment { key, by, ttl_ms } => Op::Increment { key, by, ttl_ms },
            A::GetRow { table, pk } => Op::GetRow { table, pk },
            A::InsertRow { table, row, ttl_ms } => Op::InsertRow { table, row, ttl_ms },
            A::UpsertRow { table, row, ttl_ms } => Op::UpsertRow { table, row, ttl_ms },
            A::UpdateRow { table, pk, set, ttl_ms } => Op::UpdateRow { table, pk, set, ttl_ms },
            A::DeleteRow { table, pk } => Op::DeleteRow { table, pk },
            A::UpdateRows { table, selection, set, ttl_ms, limit } => {
                Op::UpdateRows { table, selection: Selection::try_from(selection)?, set, ttl_ms, limit }
            }
            A::DeleteRows { table, selection, limit } => {
                Op::DeleteRows { table, selection: Selection::try_from(selection)?, limit }
            }
            A::QueryRows { table, index, eq, range, filter, limit, reverse, cursor } => Op::QueryRows {
                table,
                index,
                eq,
                range,
                filter: filter.map(parse).transpose()?,
                limit,
                reverse,
                cursor,
            },
            A::Append { stream, records, expect_seq, create } => Op::Append { stream, records, expect_seq, create },
            A::ReadStream(r) => {
                let (stream, spec) = ReadSpec::split(r)?;
                Op::ReadStream(StreamRead { stream, spec })
            }
            A::StreamInfo { stream } => Op::StreamInfo { stream },
            A::TrimStream { stream, before_seq, before_time } => Op::TrimStream { stream, before_seq, before_time },
            A::GetConsumer { stream, consumer } => Op::GetConsumer { stream, consumer },
            A::SetConsumer { stream, consumer, seq } => Op::SetConsumer { stream, consumer, seq },
            A::InsertEvents { table, rows } => Op::InsertEvents { table, rows },
            A::QueryEvents(q) => Op::QueryEvents(EventsQuery {
                table: q.table,
                query: crate::events::query::Query::parse(Json::Object(q.query))
                    .map_err(|e| Error::invalid(format!("invalid JSON body: {}", e.message())))?,
            }),
            A::DeleteEvents { table, from, to, filter } => Op::DeleteEvents { table, from, to, filter: parse(filter)? },
        })
    }
}

pub fn parse_ops(ops: Vec<api::Op>) -> Result<Vec<Op>> {
    ops.into_iter().map(Op::try_from).collect()
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
    pub encoding: api::Encoding,
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
pub fn user_range(
    enc: api::Encoding,
    prefix: Option<&str>,
    start: Option<&str>,
    end: Option<&str>,
) -> Result<KeyRange> {
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

pub struct ValueFields {
    pub value: Option<String>,
    pub value_b64: Option<String>,
    pub blob: Option<api::BlobMeta>,
}

/// Encodes `v` for a response, omitting inline values and blob data if `keys_only` is true or if a
/// blob exceeds `json_value_max`.
pub async fn value_fields(ctx: &OpCtx<'_>, v: &Value, keys_only: bool) -> Result<ValueFields> {
    let mut out = ValueFields { value: None, value_b64: None, blob: None };
    let data = match v {
        Value::Inline(_) | Value::Tombstone if keys_only => None,
        Value::Inline(b) => Some(b.clone()),
        Value::Blob(r) => {
            out.blob = Some(blob_meta(r));
            match r.size <= ctx.json_value_max && !keys_only {
                true => Some(blob::read_range(ctx.db.shared(), &ctx.db.id, r, 0..r.size).await?),
                false => None,
            }
        }
        Value::Tombstone => None,
    };
    if let Some(d) = data {
        (out.value, out.value_b64) = ctx.encoding.text(&d);
    }
    Ok(out)
}

async fn get_result(ctx: &OpCtx<'_>, r: Option<ReadValue>) -> Result<api::GetResult> {
    let Some(rv) = r else {
        return Ok(api::GetResult { found: false, ..api::GetResult::default() });
    };
    let v = value_fields(ctx, &rv.value, false).await?;
    Ok(api::GetResult {
        found: true,
        value: v.value,
        value_b64: v.value_b64,
        blob: v.blob,
        version: Some(rv.seq),
        expires_at: rv.expires(),
    })
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
pub async fn exec_ops(ctx: &OpCtx<'_>, txn: &mut Txn, ops: &[Op]) -> Result<Vec<OpResult>> {
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

async fn exec_one(ctx: &OpCtx<'_>, txn: &mut Txn, op: &Op, index: usize) -> Result<OpResult> {
    let enc = ctx.encoding;
    let blobs = BlobResolver { db: ctx.db };
    match op {
        Op::Get { key } => {
            let k = user_key(&enc.decode(key, "key")?)?;
            let r = txn.get(&k).await?;
            Ok(OpResult::Get(get_result(ctx, r).await?))
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
                let (key, key_b64) = enc.text(&it.key[1..]);
                let v = value_fields(ctx, &it.value, *keys_only).await?;
                out.push(api::ScanItem {
                    key,
                    key_b64,
                    value: v.value,
                    value_b64: v.value_b64,
                    blob: v.blob,
                    version: it.seq,
                    expires_at: it.expires(),
                });
            }
            let next = if more { items.get(limit - 1).map(|it| b64::encode_url(&it.key[1..])) } else { None };
            Ok(OpResult::Scan(api::ScanResult { items: out, next_cursor: next }))
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
            Ok(done())
        }
        Op::Delete { key } => {
            let k = user_key(&enc.decode(key, "key")?)?;
            txn.delete(k)?;
            Ok(done())
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
            Ok(done())
        }
        Op::DeleteRange { prefix, start, end } => {
            if prefix.is_none() && start.is_none() && end.is_none() {
                return Err(Error::invalid(
                    "delete_range needs a prefix or bounds (use prefix \"\" to clear everything)",
                ));
            }
            let r = user_range(enc, prefix.as_deref(), start.as_deref(), end.as_deref())?;
            txn.delete_range(r)?;
            Ok(done())
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
            Ok(done())
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
            Ok(OpResult::Increment(api::IncrementResult { value: n }))
        }
        Op::GetRow { table, pk } => {
            let row = table::get_expiring(txn, table, pk).await?;
            Ok(OpResult::GetRow(match row {
                Some((r, expires_at)) => api::RowResult { found: true, row: Some(r), expires_at },
                None => api::RowResult::default(),
            }))
        }
        Op::InsertRow { table, row, ttl_ms } => {
            table::insert_expiring(txn, &blobs, table, row, InsertMode::Insert, expiry(*ttl_ms)?).await?;
            Ok(done())
        }
        Op::UpsertRow { table, row, ttl_ms } => {
            table::insert_expiring(txn, &blobs, table, row, InsertMode::Upsert, expiry(*ttl_ms)?).await?;
            Ok(done())
        }
        Op::UpdateRow { table, pk, set, ttl_ms } => {
            let e = row_expiry(*ttl_ms)?;
            let found = table::update_expiring(txn, &blobs, table, pk, set, e).await?;
            Ok(OpResult::Found(api::FoundResult { found }))
        }
        Op::DeleteRow { table, pk } => {
            let found = table::delete(txn, table, pk).await?;
            Ok(OpResult::Found(api::FoundResult { found }))
        }
        Op::UpdateRows { table, selection, set, ttl_ms, limit } => {
            let e = row_expiry(*ttl_ms)?;
            let n = table::update_where(txn, &blobs, table, selection, set, e, batch_limit(*limit)?).await?;
            Ok(OpResult::Count(api::CountResult { count: n as u64 }))
        }
        Op::DeleteRows { table, selection, limit } => {
            let n = table::delete_where(txn, table, selection, batch_limit(*limit)?).await?;
            Ok(OpResult::Count(api::CountResult { count: n as u64 }))
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
            Ok(OpResult::QueryRows(api::RowsResult { rows: r.rows, next_cursor: r.next_cursor }))
        }
        Op::Append { stream: name, records, expect_seq, create } => {
            let bodies = records.iter().cloned().map(|r| stream::record_body(r, enc)).collect::<Result<Vec<_>>>()?;
            let shared = ctx.db.shared();
            let to = AppendTo {
                name,
                create: create.then(|| (shared.env.random_u64(), shared.time_ms())),
                expect: *expect_seq,
                op_index: Some(index),
                record_max: shared.cfg.stream_record_max,
            };
            stream::append(txn, &to, &bodies).await?;
            Ok(OpResult::Append(api::AppendResult { count: bodies.len() as u64, first_seq: None }))
        }
        Op::ReadStream(StreamRead { stream: name, spec }) => {
            let out = stream::read(txn, name, spec).await?;
            let records = out.records.iter().map(|(r, b)| stream::record(r, b, enc)).collect();
            Ok(OpResult::ReadStream(api::RecordsResult { records, next_seq: out.next_seq, done: out.done }))
        }
        Op::StreamInfo { stream: name } => {
            let (_, st) = stream::info(txn, name).await?;
            Ok(OpResult::StreamInfo(stream::public_state(&st)))
        }
        Op::TrimStream { stream: name, before_seq, before_time } => {
            stream::trim(txn, name, *before_seq, *before_time).await?;
            Ok(done())
        }
        Op::GetConsumer { stream: name, consumer } => {
            let c = stream::consumer(txn, name, consumer).await?;
            Ok(OpResult::GetConsumer(api::ConsumerResult { found: c.is_some(), seq: c.map(|c| c.seq) }))
        }
        Op::SetConsumer { stream: name, consumer, seq } => {
            stream::set_consumer(txn, name, consumer, *seq, ctx.db.shared().time_ms()).await?;
            Ok(done())
        }
        Op::InsertEvents { table, rows } => {
            let n = crate::events::insert(txn, table, rows, ctx.db.shared().time_ms() as i64).await?;
            Ok(OpResult::Count(api::CountResult { count: n as u64 }))
        }
        Op::QueryEvents(EventsQuery { table, query }) => {
            let s = crate::events::get_schema(txn, table).await?;
            let out = crate::events::exec::run(ctx.db.shared(), &ctx.db.id, txn, &s, query, &ctx.query_limits).await?;
            Ok(OpResult::QueryEvents(
                json!({"columns": out.columns, "rows": out.rows, "next_cursor": out.next_cursor, "stats": out.stats}),
            ))
        }
        Op::DeleteEvents { table, from, to, filter } => {
            let (from, to) = crate::events::time_bounds(from, to)?;
            let n = crate::events::maint::delete(ctx.db, txn, table, from, to, filter).await?;
            Ok(OpResult::DeleteEvents(api::DeletedResult { deleted: n as u64 }))
        }
    }
}

/// Adds the first seq each append received to the results of its op.
pub fn fill_appends(ops: &[Op], results: &mut [OpResult], appends: &[Appended]) {
    let appended = ops.iter().zip(results.iter_mut()).filter(|(o, _)| matches!(o, Op::Append { .. }));
    for ((_, r), a) in appended.zip(appends) {
        if let OpResult::Append(r) = r {
            r.first_seq = Some(a.first_seq);
        }
    }
}

/// Converts an optional `ttl_ms` on an update into an [`Expiry`] policy.
pub fn row_expiry(ttl_ms: Option<u64>) -> Result<Expiry> {
    Ok(match expiry(ttl_ms)? {
        Some(t) => Expiry::Set(Some(t)),
        None => Expiry::Keep,
    })
}

pub fn blob_meta(r: &BlobRef) -> api::BlobMeta {
    api::BlobMeta {
        blob_id: r.id.clone(),
        size: r.size,
        sha256: b64::hex(&r.sha256),
        content_type: r.content_type.clone(),
    }
}

fn done() -> OpResult {
    OpResult::Done(api::Empty {})
}
