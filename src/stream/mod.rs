//! Streams (SPEC §4.6): named, append-only logs of records, layered on the
//! engine's stream records (§5.11). This layer keeps each stream's catalog
//! entry and consumers, and gives records their bodies: JSON or bytes, with
//! headers.

pub mod filter;

use crate::engine::stream::{self as es, StreamIter};
use crate::engine::{KeyRange, Record, Retention, Seek, StreamOp, StreamState, Txn, Value};
use crate::error::{Error, Result};
use crate::ops::Encoding;
use crate::util::b64;
use crate::util::codec::{DResult, DecodeError, Reader, Writer};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json, json};
use std::collections::BTreeMap;

pub use filter::Filter;

pub const SPACE_CATALOG: u8 = 0x05;
pub const SPACE_CONSUMERS: u8 = 0x08;
pub const NAME_MAX: usize = 128;
pub const READ_LIMIT_MAX: usize = 1000;
pub const READ_BYTES_DEFAULT: usize = 4 << 20;
pub const READ_BYTES_MAX: usize = 16 << 20;
pub const SCAN_LIMIT_DEFAULT: usize = 10_000;
pub const SCAN_LIMIT_MAX: usize = 100_000;
const HEADERS_MAX: usize = 64;
const HEADER_NAME_MAX: usize = 128;
const HEADER_VALUE_MAX: usize = 4096;
/// The longest retention: ten years.
pub const RETENTION_MAX_MS: u64 = crate::ops::MAX_TTL_MS;

/// Stream and consumer names: 1-128 characters of `[A-Za-z0-9_.:-]`.
pub fn valid_name(s: &str) -> bool {
    (1..=NAME_MAX).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
}

fn check_name(s: &str, what: &str) -> Result<()> {
    match valid_name(s) {
        true => Ok(()),
        false => Err(Error::invalid(format!("{what} names are 1-{NAME_MAX} characters of [A-Za-z0-9_.:-]"))),
    }
}

fn catalog_key(name: &str) -> Bytes {
    let mut k = vec![SPACE_CATALOG];
    k.extend_from_slice(name.as_bytes());
    Bytes::from(k)
}

fn consumer_key(id: u64, name: &str) -> Bytes {
    let mut k = vec![SPACE_CONSUMERS];
    k.extend_from_slice(&id.to_be_bytes());
    k.extend_from_slice(name.as_bytes());
    Bytes::from(k)
}

fn consumers(id: u64) -> KeyRange {
    KeyRange::prefix(&consumer_key(id, ""))
}

/// Decodes a catalog entry or consumer.
fn decode<T: DeserializeOwned>(v: &Value, what: &str) -> Result<T> {
    match v {
        Value::Inline(b) => serde_json::from_slice(b).map_err(|_| Error::internal(format!("corrupt {what}"))),
        _ => Err(Error::internal(format!("corrupt {what}"))),
    }
}

/// The keys `space ++ name` of names starting with `prefix`, after the name
/// a listing cursor holds; `None` if no name is.
fn names_after(space: &[u8], prefix: &str, cursor: Option<&str>) -> Result<Option<KeyRange>> {
    let key = |name: &str| Bytes::from([space, name.as_bytes()].concat());
    let range = KeyRange::prefix(&key(prefix));
    let Some(c) = cursor else { return Ok(Some(range)) };
    let after =
        b64::decode_url(c).and_then(|b| String::from_utf8(b).ok()).ok_or_else(|| Error::invalid("bad cursor"))?;
    let after = key(&after);
    Ok(match range.contains(&after) {
        true => Some(KeyRange::new(std::ops::Bound::Excluded(after), range.end)),
        false if after < key(prefix) => Some(range),
        false => None,
    })
}

/// The cursor that resumes a listing after `name`, if the page was full.
fn next_cursor(more: bool, name: Option<&String>) -> Option<String> {
    name.filter(|_| more).map(|n| b64::encode_url(n.as_bytes()))
}

/// A stream's catalog entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Catalog {
    pub id: u64,
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
}

impl Catalog {
    pub fn retention(&self) -> Retention {
        Retention { retention_ms: self.retention_ms, max_bytes: self.max_bytes }
    }
}

/// Checks retention settings a client asked for.
pub fn check_retention(r: &Retention) -> Result<()> {
    if r.retention_ms.is_some_and(|ms| !(1..=RETENTION_MAX_MS).contains(&ms)) {
        return Err(Error::invalid(format!("retention_ms must be in 1..={RETENTION_MAX_MS}")));
    }
    if r.max_bytes == Some(0) {
        return Err(Error::invalid("max_bytes must be at least 1"));
    }
    Ok(())
}

/// A record's body: JSON or bytes, with headers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Body {
    /// `data` is JSON text.
    pub json: bool,
    /// Sorted by name, names unique.
    pub headers: Vec<(String, String)>,
    pub data: Bytes,
}

const BODY_JSON: u8 = 1;

impl Body {
    pub fn text(data: impl Into<Bytes>) -> Body {
        Body { json: false, headers: Vec::new(), data: data.into() }
    }

    pub fn json(v: &Json) -> Body {
        Body { json: true, headers: Vec::new(), data: Bytes::from(v.to_string()) }
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.binary_search_by(|(k, _)| k.as_str().cmp(name)).ok().map(|i| self.headers[i].1.as_str())
    }

    /// The parsed body, if it is JSON.
    pub fn json_value(&self) -> Option<Json> {
        self.json.then(|| serde_json::from_slice(&self.data).ok()).flatten()
    }

    pub fn encode(&self) -> Bytes {
        let mut w = Writer::with_capacity(self.data.len() + 8);
        w.u8(if self.json { BODY_JSON } else { 0 });
        w.varint(self.headers.len() as u64);
        for (k, v) in &self.headers {
            w.str(k);
            w.str(v);
        }
        w.raw(&self.data);
        Bytes::from(w.into_inner())
    }

    pub fn decode(b: &Bytes) -> DResult<Body> {
        let mut r = Reader::new(b);
        let json = match r.u8()? {
            0 => false,
            BODY_JSON => true,
            _ => return Err(DecodeError("bad record body flags")),
        };
        let n = r.len(HEADERS_MAX)?;
        let mut headers: Vec<(String, String)> = Vec::with_capacity(n);
        for _ in 0..n {
            let (k, v) = (r.string()?, r.string()?);
            if headers.last().is_some_and(|(p, _)| *p >= k) {
                return Err(DecodeError("record headers not sorted"));
            }
            headers.push((k, v));
        }
        Ok(Body { json, headers, data: b.slice(b.len() - r.remaining()..) })
    }
}

/// A record as a client writes it: exactly one of `value` and `json`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewRecord {
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default, deserialize_with = "present")]
    pub json: Option<Json>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

/// Deserializes a field that may hold JSON `null` as `Some`.
fn present<'de, D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Option<Json>, D::Error> {
    Json::deserialize(d).map(Some)
}

impl NewRecord {
    pub fn into_body(self, enc: Encoding) -> Result<Body> {
        let mut b = match (self.value, self.json) {
            (Some(v), None) => Body::text(enc.decode(&v, "value")?),
            (None, Some(j)) => Body::json(&j),
            _ => return Err(Error::invalid("a record has exactly one of value and json")),
        };
        if self.headers.len() > HEADERS_MAX {
            return Err(Error::invalid(format!("a record has at most {HEADERS_MAX} headers")));
        }
        for (k, v) in &self.headers {
            if k.is_empty() || k.len() > HEADER_NAME_MAX || v.len() > HEADER_VALUE_MAX {
                return Err(Error::invalid(format!(
                    "header names are 1-{HEADER_NAME_MAX} bytes and values at most {HEADER_VALUE_MAX}"
                )));
            }
        }
        b.headers = self.headers.into_iter().collect();
        Ok(b)
    }
}

/// Splits a body of lines into records: each line JSON with `json`, text
/// otherwise. Empty lines and a leading byte order mark are skipped.
pub fn parse_lines(data: &Bytes, json: bool) -> Result<Vec<Body>> {
    let data = &data.slice(if data.starts_with(b"\xEF\xBB\xBF") { 3 } else { 0 }..);
    let mut out = Vec::new();
    let mut start = 0;
    for (i, line) in data.split(|b| *b == b'\n').enumerate() {
        let at = start;
        start += line.len() + 1;
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.iter().all(|b| b.is_ascii_whitespace()) {
            continue;
        }
        let slice = data.slice(at..at + line.len());
        if json {
            if serde_json::from_slice::<serde::de::IgnoredAny>(line).is_err() {
                return Err(Error::invalid(format!("line {} is not JSON", i + 1)));
            }
            out.push(Body { json: true, headers: Vec::new(), data: slice });
        } else {
            out.push(Body::text(slice));
        }
    }
    Ok(out)
}

/// A record as reads return it.
pub fn record_json(r: &Record, b: &Body, enc: Encoding) -> Json {
    let mut m = Map::new();
    m.insert("seq".into(), json!(r.seq));
    m.insert("time".into(), json!(r.time_ms));
    if !b.headers.is_empty() {
        let h: Map<String, Json> = b.headers.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
        m.insert("headers".into(), Json::Object(h));
    }
    match b.json_value() {
        Some(v) => {
            m.insert("json".into(), v);
        }
        None => match enc.encode(&b.data) {
            Json::Null => {
                m.insert("value_b64".into(), json!(b64::encode(&b.data)));
            }
            v => {
                m.insert("value".into(), v);
            }
        },
    }
    Json::Object(m)
}

/// Reads `name`'s catalog entry, pinned so that the transaction conflicts
/// with the stream's deletion or reconfiguration.
pub async fn lookup(txn: &mut Txn, name: &str) -> Result<Option<Catalog>> {
    check_name(name, "stream")?;
    txn.get_pinned(&catalog_key(name)).await?.map(|rv| decode(&rv.value, "stream catalog")).transpose()
}

pub async fn open(txn: &mut Txn, name: &str) -> Result<Catalog> {
    lookup(txn, name).await?.ok_or_else(|| Error::not_found(format!("stream {name:?} not found")))
}

fn put_catalog(txn: &mut Txn, name: &str, c: &Catalog) -> Result<()> {
    let data = serde_json::to_vec(c).map_err(|e| Error::internal(e.to_string()))?;
    txn.put(catalog_key(name), Value::Inline(Bytes::from(data)))
}

/// Creates stream `name` with the fresh id `id` at time `now`.
pub async fn create(txn: &mut Txn, name: &str, retention: Retention, id: u64, now: u64) -> Result<Catalog> {
    check_retention(&retention)?;
    if lookup(txn, name).await?.is_some() {
        return Err(Error::AlreadyExists(format!("stream {name:?} already exists")));
    }
    if txn.stream_state(id).await?.is_some() {
        return Err(Error::Conflict("stream id in use".into()));
    }
    let c = Catalog { id, created_at: now, retention_ms: retention.retention_ms, max_bytes: retention.max_bytes };
    put_catalog(txn, name, &c)?;
    txn.stream_op(StreamOp::Create { id, retention })?;
    Ok(c)
}

pub async fn configure(txn: &mut Txn, name: &str, retention: Retention) -> Result<Catalog> {
    check_retention(&retention)?;
    let c = Catalog { retention_ms: retention.retention_ms, max_bytes: retention.max_bytes, ..open(txn, name).await? };
    put_catalog(txn, name, &c)?;
    txn.stream_op(StreamOp::Configure { id: c.id, retention })?;
    Ok(c)
}

/// Deletes stream `name` with its records and consumers.
pub async fn delete(txn: &mut Txn, name: &str) -> Result<()> {
    let c = open(txn, name).await?;
    txn.delete(catalog_key(name))?;
    txn.delete_range(es::records(c.id))?;
    txn.delete(es::state_key(c.id))?;
    txn.delete_range(consumers(c.id))
}

/// How an append finds its stream.
pub struct AppendTo<'a> {
    pub name: &'a str,
    /// Create the stream, with default retention, if it does not exist.
    pub create: Option<(u64, u64)>,
    pub expect: Option<u64>,
    pub op_index: Option<usize>,
    pub record_max: usize,
}

/// Appends `bodies` to a stream at commit.
pub async fn append(txn: &mut Txn, to: &AppendTo<'_>, bodies: &[Body]) -> Result<Catalog> {
    let payloads: Vec<Bytes> = bodies.iter().map(Body::encode).collect();
    if payloads.iter().any(|p| p.len() > to.record_max) {
        return Err(Error::TooLarge(format!("records are limited to {} bytes", to.record_max)));
    }
    let c = match (lookup(txn, to.name).await?, to.create) {
        (Some(c), _) => c,
        (None, Some((id, now))) => create(txn, to.name, Retention::default(), id, now).await?,
        (None, None) => return Err(Error::not_found(format!("stream {:?} not found", to.name))),
    };
    txn.append(c.id, payloads, to.expect, to.op_index)?;
    Ok(c)
}

/// Trims stream `name`'s records before `before_seq`, or before the first
/// record at or after `before_time`.
pub async fn trim(txn: &mut Txn, name: &str, before_seq: Option<u64>, before_time: Option<u64>) -> Result<Catalog> {
    let c = open(txn, name).await?;
    let before = match (before_seq, before_time) {
        (Some(s), None) => s,
        (None, Some(t)) => {
            let st = state(txn, &c, false).await?;
            let before = txn.snapshot().stream_seek(c.id, &st, Seek::Time(t)).await?;
            // If all existing records are earlier than `t` (`before == st.next_seq`), leave the
            // read range unbounded above so that a concurrent append with a timestamp `< t` conflicts.
            txn.note_stream_read(c.id, st.first_seq, (before < st.next_seq).then_some(before + 1))?;
            before
        }
        _ => return Err(Error::invalid("trim needs exactly one of before_seq and before_time")),
    };
    txn.stream_op(StreamOp::Trim { id: c.id, before, retention: None })?;
    Ok(c)
}

/// The stream's state as the transaction sees it: as of its snapshot, less
/// the records it trims itself. A stream the transaction created has no
/// state yet: it is empty. `recorded` puts the state in the read set, so
/// that the transaction conflicts with every append and trim after its
/// snapshot.
async fn state(txn: &mut Txn, c: &Catalog, recorded: bool) -> Result<StreamState> {
    let st = match recorded {
        true => txn.stream_state(c.id).await?,
        false => txn.snapshot().stream_state(c.id).await?,
    };
    let mut st = st.unwrap_or(StreamState::new(c.retention()));
    let target = txn.pending_trim(c.id).unwrap_or(0).clamp(st.first_seq, st.next_seq);
    if target > st.first_seq {
        (st.first_pos, st.first_time) = match target == st.next_seq {
            true => (st.next_pos, st.last_time),
            false => {
                let r = txn.snapshot().stream_record(c.id, target).await?;
                r.map(|r| (r.pos, r.time_ms)).ok_or_else(|| Error::internal("stream record missing"))?
            }
        };
        st.first_seq = target;
    }
    Ok(st)
}

/// The stream's state, recorded in the read set: the transaction conflicts
/// with every append and trim after its snapshot.
pub async fn info(txn: &mut Txn, name: &str) -> Result<(Catalog, StreamState)> {
    let c = open(txn, name).await?;
    let st = state(txn, &c, true).await?;
    Ok((c, st))
}

pub fn state_json(st: &StreamState) -> Map<String, Json> {
    let mut m = Map::new();
    m.insert("first_seq".into(), json!(st.first_seq));
    m.insert("next_seq".into(), json!(st.next_seq));
    m.insert("bytes".into(), json!(st.bytes()));
    if st.records() > 0 {
        m.insert("first_time".into(), json!(st.first_time));
        m.insert("last_time".into(), json!(st.last_time));
    }
    m
}

/// A stream as listings and info describe it.
pub fn stream_json(name: &str, c: &Catalog, st: &StreamState) -> Json {
    let mut m = state_json(st);
    m.insert("name".into(), json!(name));
    m.insert("created_at".into(), json!(c.created_at));
    m.insert("retention_ms".into(), json!(c.retention_ms));
    m.insert("max_bytes".into(), json!(c.max_bytes));
    Json::Object(m)
}

/// A page of streams whose names start with `prefix`, after `cursor`.
pub async fn list(
    txn: &mut Txn,
    prefix: &str,
    limit: usize,
    cursor: Option<&str>,
) -> Result<(Vec<(String, Catalog, StreamState)>, Option<String>)> {
    let Some(range) = names_after(&[SPACE_CATALOG], prefix, cursor)? else { return Ok((Vec::new(), None)) };
    let items = txn.scan(&range, false, limit + 1).await?;
    let mut out = Vec::with_capacity(limit.min(items.len()));
    for it in items.iter().take(limit) {
        let name = String::from_utf8_lossy(&it.key[1..]).into_owned();
        let c: Catalog = decode(&it.value, "stream catalog")?;
        let st = state(txn, &c, false).await?;
        out.push((name, c, st));
    }
    let next = next_cursor(items.len() > limit, out.last().map(|(n, _, _)| n));
    Ok((out, next))
}

/// What a read returns: where it starts and ends, and which records.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadSpec {
    #[serde(default)]
    pub from_seq: Option<u64>,
    #[serde(default)]
    pub from_time: Option<u64>,
    /// Start at the last `tail` records.
    #[serde(default)]
    pub tail: Option<u64>,
    /// Start at this consumer's cursor.
    #[serde(default)]
    pub consumer: Option<String>,
    #[serde(default)]
    pub until_seq: Option<u64>,
    #[serde(default)]
    pub until_time: Option<u64>,
    /// Newest first, from the end of the range.
    #[serde(default)]
    pub reverse: bool,
    #[serde(default)]
    pub limit: Option<usize>,
    /// Stop once the records returned hold this many body bytes.
    #[serde(default)]
    pub max_bytes: Option<usize>,
    #[serde(default)]
    pub filter: Option<Filter>,
    /// Examine at most this many records.
    #[serde(default)]
    pub scan_limit: Option<usize>,
}

impl ReadSpec {
    pub fn validate(&self) -> Result<()> {
        let starts = [self.from_seq.is_some(), self.from_time.is_some(), self.tail.is_some(), self.consumer.is_some()];
        if starts.iter().filter(|s| **s).count() > 1 {
            return Err(Error::invalid("give at most one of from_seq, from_time, tail and consumer"));
        }
        if self.until_seq.is_some() && self.until_time.is_some() {
            return Err(Error::invalid("give at most one of until_seq and until_time"));
        }
        if self.limit.is_some_and(|l| !(1..=READ_LIMIT_MAX).contains(&l)) {
            return Err(Error::invalid(format!("limit must be in 1..={READ_LIMIT_MAX}")));
        }
        if self.max_bytes.is_some_and(|b| !(1..=READ_BYTES_MAX).contains(&b)) {
            return Err(Error::invalid(format!("max_bytes must be in 1..={READ_BYTES_MAX}")));
        }
        if self.scan_limit.is_some_and(|l| !(1..=SCAN_LIMIT_MAX).contains(&l)) {
            return Err(Error::invalid(format!("scan_limit must be in 1..={SCAN_LIMIT_MAX}")));
        }
        if let Some(c) = &self.consumer {
            check_name(c, "consumer")?;
        }
        Ok(())
    }
}

/// The result of a read.
#[derive(Clone, Debug)]
pub struct ReadOut {
    pub records: Vec<(Record, Body)>,
    /// Where the next page starts: the next `from_seq` of a forward read,
    /// the next `until_seq` of a reverse one.
    pub next_seq: u64,
    /// The read reached the end of its range.
    pub done: bool,
    pub first_seq: u64,
    /// The stream's next seq.
    pub tail_seq: u64,
    /// A forward read reached the end of the stream, and its range goes on
    /// past it: records appended next would extend it.
    pub open: bool,
}

/// Reads stream `name`. In a transaction, the read conflicts with appends
/// only if it reached the end of the stream, and with trims of the records
/// it covered (SPEC §6.1).
pub async fn read(txn: &mut Txn, name: &str, spec: &ReadSpec) -> Result<ReadOut> {
    spec.validate()?;
    let c = open(txn, name).await?;
    let v = txn.snapshot().clone();
    let mut st = state(txn, &c, false).await?;
    let lo = match (spec.from_seq, spec.from_time, spec.tail, &spec.consumer) {
        (Some(s), ..) => s,
        (_, Some(t), ..) => v.stream_seek(c.id, &st, Seek::Time(t)).await?,
        (_, _, Some(n), _) => {
            st = state(txn, &c, true).await?;
            st.next_seq.saturating_sub(n)
        }
        (_, _, _, Some(cn)) => get_consumer(txn, &c, cn).await?.map_or(0, |x| x.seq),
        _ => 0,
    };
    let until = match (spec.until_seq, spec.until_time) {
        (Some(s), _) => Some(s),
        // Later appends never carry earlier times, but when every record is
        // before the bound, they may fall within it too.
        (_, Some(t)) => Some(v.stream_seek(c.id, &st, Seek::Time(t)).await?).filter(|s| *s < st.next_seq),
        _ => None,
    };
    let lo = lo.max(st.first_seq);
    let hi = until.unwrap_or(u64::MAX).min(st.next_seq);
    let limit = spec.limit.unwrap_or(100);
    let max_bytes = spec.max_bytes.unwrap_or(READ_BYTES_DEFAULT);
    let scan_limit = spec.scan_limit.unwrap_or(SCAN_LIMIT_DEFAULT);
    let mut it: StreamIter = v.stream_iter(c.id, lo, hi.max(lo), spec.reverse);
    let (mut records, mut bytes, mut scanned, mut last) = (Vec::new(), 0, 0, None);
    let exhausted = loop {
        if records.len() >= limit || bytes >= max_bytes || scanned >= scan_limit {
            break false;
        }
        let Some(r) = it.next().await? else { break true };
        scanned += 1;
        last = Some(r.seq);
        let b = Body::decode(&r.payload).map_err(|_| Error::internal("corrupt record body"))?;
        if spec.filter.as_ref().is_none_or(|f| f.matches(&b)) {
            bytes += r.payload.len();
            records.push((r, b));
        }
    };
    let next_seq = match (spec.reverse, exhausted, last) {
        (false, false, Some(l)) => l + 1,
        (false, _, _) => hi.max(lo),
        (true, false, Some(l)) => l,
        (true, _, _) => lo.min(hi),
    };
    match spec.reverse {
        false => txn.note_stream_read(c.id, lo, if exhausted { until } else { Some(next_seq) })?,
        true => txn.note_stream_read(c.id, if exhausted { lo } else { next_seq }, until)?,
    }
    let open = !spec.reverse && exhausted && until.is_none_or(|u| u > st.next_seq);
    Ok(ReadOut { records, next_seq, done: exhausted, first_seq: st.first_seq, tail_seq: st.next_seq, open })
}

/// A consumer's cursor: the seq it reads from next.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Consumer {
    pub seq: u64,
    pub updated_at: u64,
}

async fn get_consumer(txn: &mut Txn, c: &Catalog, name: &str) -> Result<Option<Consumer>> {
    check_name(name, "consumer")?;
    txn.get(&consumer_key(c.id, name)).await?.map(|rv| decode(&rv.value, "consumer")).transpose()
}

pub async fn consumer(txn: &mut Txn, stream: &str, name: &str) -> Result<Option<Consumer>> {
    let c = open(txn, stream).await?;
    get_consumer(txn, &c, name).await
}

/// Moves a consumer's cursor to `seq`, which must not be past the stream's
/// next seq.
pub async fn set_consumer(txn: &mut Txn, stream: &str, name: &str, seq: u64, now: u64) -> Result<()> {
    check_name(name, "consumer")?;
    let c = open(txn, stream).await?;
    // Sequence numbers are monotonic, so a cursor valid at the snapshot remains
    // valid without adding the stream state to the read set (which would conflict
    // with concurrent appends). If the cursor is ahead of the snapshot, re-read
    // and record the state so an optimistic transaction revalidates against any
    // in-flight append.
    let mut next = state(txn, &c, false).await?.next_seq;
    if seq > next {
        next = state(txn, &c, true).await?.next_seq;
    }
    if seq > next {
        return Err(Error::invalid(format!("seq {seq} is past the stream's next seq {next}")));
    }
    let data = serde_json::to_vec(&Consumer { seq, updated_at: now }).map_err(|e| Error::internal(e.to_string()))?;
    txn.put(consumer_key(c.id, name), Value::Inline(Bytes::from(data)))
}

/// Deletes a consumer, reporting whether it existed.
pub async fn delete_consumer(txn: &mut Txn, stream: &str, name: &str) -> Result<bool> {
    let c = open(txn, stream).await?;
    let found = get_consumer(txn, &c, name).await?.is_some();
    txn.delete(consumer_key(c.id, name))?;
    Ok(found)
}

/// A page of a stream's consumers, by name, after `cursor`, with the
/// cursor of the next page.
pub async fn list_consumers(
    txn: &mut Txn,
    stream: &str,
    limit: usize,
    cursor: Option<&str>,
) -> Result<(Vec<(String, Consumer)>, Option<String>)> {
    let c = open(txn, stream).await?;
    let Some(range) = names_after(&consumer_key(c.id, ""), "", cursor)? else { return Ok((Vec::new(), None)) };
    let items = txn.scan(&range, false, limit + 1).await?;
    let mut out = Vec::with_capacity(limit.min(items.len()));
    for it in items.iter().take(limit) {
        out.push((String::from_utf8_lossy(&it.key[9..]).into_owned(), decode(&it.value, "consumer")?));
    }
    let next = next_cursor(items.len() > limit, out.last().map(|(n, _)| n));
    Ok((out, next))
}

/// Checks that every stream's catalog entry, state, records and consumers
/// agree: seqs dense from the first to the next, positions adding up, times
/// never decreasing, and nothing left of deleted streams.
pub async fn check_integrity(txn: &mut Txn) -> Result<()> {
    let fail = |m: String| Err(Error::internal(m));
    let mut ids: BTreeMap<u64, String> = BTreeMap::new();
    for it in txn.scan(&KeyRange::prefix(&[SPACE_CATALOG]), false, usize::MAX).await? {
        let name = String::from_utf8_lossy(&it.key[1..]).into_owned();
        let c: Catalog = decode(&it.value, "stream catalog")?;
        if ids.insert(c.id, name.clone()).is_some() {
            return fail(format!("stream {name} shares its id"));
        }
    }
    let v = txn.snapshot().clone();
    let mut it = v.iter(
        &KeyRange::half_open(Bytes::from_static(&[es::RECORDS]), Some(Bytes::from_static(&[es::STATE + 1]))),
        false,
        false,
        None,
    );
    let mut entries: BTreeMap<u64, u64> = BTreeMap::new();
    let mut states = 0;
    while let Some(e) = it.next().await? {
        if let Some((id, _)) = es::parse_record_key(&e.key) {
            *entries.entry(id).or_default() += 1;
        } else if let Some(id) = es::parse_state_key(&e.key) {
            states += 1;
            if !ids.contains_key(&id) {
                return fail(format!("state of deleted stream {id}"));
            }
        } else {
            return fail(format!("stray stream entry {}", b64::hex(&e.key)));
        }
    }
    if let Some(id) = entries.keys().find(|id| !ids.contains_key(id)) {
        return fail(format!("records of deleted stream {id}"));
    }
    if states != ids.len() {
        return fail(format!("{} streams but {states} states", ids.len()));
    }
    for it in txn.scan(&KeyRange::prefix(&[SPACE_CONSUMERS]), false, usize::MAX).await? {
        let id = u64::from_be_bytes(it.key[1..9].try_into().map_err(|_| Error::internal("bad consumer key"))?);
        if !ids.contains_key(&id) {
            return fail(format!("consumer of deleted stream {id}"));
        }
    }
    for (id, name) in &ids {
        let st = v.stream_state(*id).await?.ok_or_else(|| Error::internal(format!("{name} has no state")))?;
        let mut it = v.stream_iter(*id, st.first_seq, st.next_seq, false);
        let (mut seq, mut pos, mut time) = (st.first_seq, st.first_pos, st.first_time);
        while let Some(r) = it.next().await? {
            if r.seq != seq || r.pos != pos || r.time_ms < time || (seq == st.first_seq && r.time_ms != st.first_time) {
                return fail(format!("{name}: record {} at {} {} after {seq} {pos} {time}", r.seq, r.pos, r.time_ms));
            }
            Body::decode(&r.payload).map_err(|_| Error::internal(format!("{name}: bad body at {}", r.seq)))?;
            (seq, pos, time) = (seq + 1, pos + r.payload.len() as u64, r.time_ms);
        }
        if seq != st.next_seq || pos != st.next_pos || (st.records() > 0 && time != st.last_time) {
            return fail(format!("{name}: records end at {seq} {pos} {time}, state {st:?}"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
