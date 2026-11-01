//! Structure-aware HTTP fuzzing of the transaction API against a reference model.

use super::{Sim, SimConfig, SimResponse, openapi_check};
use crate::engine::KeyRange;
use crate::ops::Encoding;
use crate::table::RangeSpec;
use crate::table::tuple::Key as TKey;
use crate::util::{b64, sha256};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Map, Value as Json, json};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::Arc;

/// Draws choices from fuzzer bytes; once exhausted every draw is 0.
pub struct Choices<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Choices<'a> {
    pub fn new(data: &'a [u8]) -> Choices<'a> {
        Choices { data, pos: 0 }
    }

    pub fn exhausted(&self) -> bool {
        self.pos >= self.data.len()
    }

    fn byte(&mut self) -> u8 {
        let b = self.data.get(self.pos).copied().unwrap_or(0);
        self.pos += 1;
        b
    }

    fn below(&mut self, n: usize) -> usize {
        match n {
            0 | 1 => 0,
            2..=256 => self.byte() as usize % n,
            _ => (self.byte() as usize | (self.byte() as usize) << 8) % n,
        }
    }

    fn chance(&mut self, n: u8) -> bool {
        self.byte() > 255 - n
    }

    fn pick<T: Clone>(&mut self, xs: &[T]) -> T {
        xs[self.below(xs.len())].clone()
    }
}

const ANY: &str = "\u{1}any";
const ANY_NUMBER: &str = "\u{1}number";

fn matches(want: &Json, got: &Json) -> bool {
    match (want, got) {
        (Json::String(w), Json::String(_)) if w == ANY => true,
        (Json::String(w), Json::Number(_)) if w == ANY_NUMBER => true,
        (Json::Object(a), Json::Object(b)) => {
            a.len() == b.len() && a.iter().all(|(k, v)| b.get(k).is_some_and(|g| matches(v, g)))
        }
        (Json::Array(a), Json::Array(b)) => a.len() == b.len() && a.iter().zip(b).all(|(x, y)| matches(x, y)),
        _ => want == got,
    }
}

#[derive(Clone, Debug, PartialEq)]
struct BlobMeta {
    /// `None` for implicitly created blobs whose ID is unknown.
    id: Option<String>,
    content_type: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
enum Expiry {
    #[default]
    Never,
    Pending,
    At,
}

impl Expiry {
    fn committed(self) -> Expiry {
        match self {
            Expiry::Pending => Expiry::At,
            e => e,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct MVal {
    data: Vec<u8>,
    blob: Option<BlobMeta>,
    expiry: Expiry,
}

#[derive(Clone, Debug, PartialEq)]
struct MRow {
    name: Option<String>,
    grp: Option<i64>,
    doc: Option<Json>,
    expiry: Expiry,
}

#[derive(Clone, Debug, PartialEq)]
struct MRecord {
    data: Vec<u8>,
    json: Option<Json>,
    headers: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default)]
struct MStream {
    records: Vec<MRecord>,
    first: u64,
    consumers: BTreeMap<String, u64>,
}

impl MStream {
    fn next(&self) -> u64 {
        self.records.len() as u64
    }
}

#[derive(Clone, Default)]
struct Snapshot {
    kv: BTreeMap<Vec<u8>, (MVal, u64)>,
    rows: BTreeMap<i64, MRow>,
    streams: BTreeMap<String, MStream>,
}

/// Commit versions of stream modifications used for conflict detection.
#[derive(Clone, Debug, Default)]
struct StreamLog {
    catalog: u64,
    state: u64,
    /// `(version, first_seq, count)` for each append.
    appends: Vec<(u64, u64, u64)>,
    /// `(version, first_seq)` for each trim.
    trims: Vec<(u64, u64)>,
    consumers: BTreeMap<String, u64>,
}

#[derive(Clone, Debug)]
enum StreamRead {
    Catalog(String),
    State(String),
    Range(String, u64, Option<u64>),
    Consumer(String, String),
}

#[derive(Clone, Debug)]
enum StreamWrite {
    Create(String),
    Append { stream: String, records: Vec<MRecord>, expect: Option<u64>, op_index: usize },
    Trim(String, u64),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct NewRecord {
    #[serde(default)]
    value: Option<String>,
    #[serde(default, deserialize_with = "some_json")]
    json: Option<Json>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
}

fn some_json<'de, D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Option<Json>, D::Error> {
    Json::deserialize(d).map(Some)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Iso {
    #[default]
    Serializable,
    Snapshot,
}

/// Independent copy of `ops::Op` so default or classification bugs are caught.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Op {
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
        range: Option<RangeSpec>,
        #[serde(default)]
        filter: Option<Json>,
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
    ReadStream {
        stream: String,
        #[serde(default)]
        from_seq: Option<u64>,
        #[serde(default)]
        from_time: Option<u64>,
        #[serde(default)]
        tail: Option<u64>,
        #[serde(default)]
        consumer: Option<String>,
        #[serde(default)]
        until_seq: Option<u64>,
        #[serde(default)]
        until_time: Option<u64>,
        #[serde(default)]
        reverse: bool,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        max_bytes: Option<usize>,
        #[serde(default)]
        filter: Option<Json>,
        #[serde(default)]
        scan_limit: Option<usize>,
    },
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
}

fn one() -> i64 {
    1
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Selection {
    #[serde(default)]
    index: Option<String>,
    #[serde(default)]
    eq: Vec<Json>,
    #[serde(default)]
    range: Option<RangeSpec>,
    #[serde(default)]
    filter: Option<Json>,
}

const TTL_BEYOND_RUN_MS: u64 = 1_000_000_000;

fn ttl(ttl_ms: Option<u64>) -> MResult<Expiry> {
    match ttl_ms {
        None => Ok(Expiry::Never),
        Some(t) if (TTL_BEYOND_RUN_MS..=crate::ops::MAX_TTL_MS).contains(&t) => Ok(Expiry::Pending),
        Some(t) if (1..=crate::ops::MAX_TTL_MS).contains(&t) => Err(Fail::Unmodeled),
        Some(_) => Err(Fail::Invalid),
    }
}

impl Op {
    fn is_write(&self) -> bool {
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
        )
    }

    /// Returns `true` if the operation contains an invalid filter expression.
    fn unparseable(&self) -> bool {
        match self {
            Op::ReadStream { filter: Some(f), .. } => !filter_parses(f, stream_operand),
            Op::QueryRows { filter: Some(f), .. }
            | Op::UpdateRows { selection: Selection { filter: Some(f), .. }, .. }
            | Op::DeleteRows { selection: Selection { filter: Some(f), .. }, .. } => !filter_parses(f, table_operand),
            _ => false,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OneShotBody {
    ops: Vec<Op>,
    #[serde(default)]
    encoding: Encoding,
    #[serde(default)]
    #[allow(dead_code)]
    isolation: Iso,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    consistency: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    max_staleness_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BeginBody {
    #[serde(default)]
    isolation: Iso,
    #[serde(default)]
    encoding: Encoding,
    #[serde(default)]
    ops: Vec<Op>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct CommitBody {
    #[serde(default)]
    ops: Vec<Op>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OpsBody {
    ops: Vec<Op>,
}

fn parse_body<T: serde::de::DeserializeOwned>(b: &[u8]) -> Option<T> {
    let b: &[u8] = if b.iter().all(|c| c.is_ascii_whitespace()) { b"{}" } else { b };
    serde_json::from_slice(b).ok()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fail {
    Invalid,
    NotFound,
    Exists,
    Precondition,
    /// The idempotency key was resolved as not applied by an earlier failed precondition.
    Aborted,
    TooLarge,
    /// Valid request with an in-run TTL whose exact expiry is not modeled.
    Unmodeled,
}

impl Fail {
    fn status(self) -> (u16, &'static str) {
        match self {
            Fail::Invalid => (400, "invalid_argument"),
            Fail::NotFound => (404, "not_found"),
            Fail::Exists => (409, "already_exists"),
            Fail::Precondition => (412, "precondition_failed"),
            Fail::Aborted => (409, "aborted"),
            Fail::TooLarge => (413, "too_large"),
            Fail::Unmodeled => (200, ""),
        }
    }
}

type MResult<T> = std::result::Result<T, Fail>;

#[derive(Clone, Debug)]
enum ReadItem {
    Key(Vec<u8>),
    Range(KeyRange),
}

#[derive(Clone)]
struct TxnView {
    base: Snapshot,
    rv: u64,
    writes: BTreeMap<Vec<u8>, Option<MVal>>,
    ranges: Vec<KeyRange>,
    row_writes: BTreeMap<i64, Option<MRow>>,
    reads: Vec<ReadItem>,
    table_ops: bool,
    stream_reads: Vec<StreamRead>,
    stream_keys: Vec<StreamRead>,
    stream_writes: Vec<StreamWrite>,
    consumer_writes: BTreeMap<(String, String), u64>,
    /// Keys read by `copy`, validated under every isolation level.
    pinned: Vec<Vec<u8>>,
    op_index: usize,
}

impl TxnView {
    fn new(base: Snapshot, rv: u64) -> TxnView {
        TxnView {
            base,
            rv,
            writes: BTreeMap::new(),
            ranges: Vec::new(),
            row_writes: BTreeMap::new(),
            reads: Vec::new(),
            table_ops: false,
            stream_reads: Vec::new(),
            stream_keys: Vec::new(),
            stream_writes: Vec::new(),
            consumer_writes: BTreeMap::new(),
            pinned: Vec::new(),
            op_index: 0,
        }
    }

    fn created(&self, name: &str) -> bool {
        self.stream_writes.iter().any(|w| matches!(w, StreamWrite::Create(n) if n == name))
    }

    fn stream(&self, name: &str) -> Option<MStream> {
        let mut st = match self.base.streams.get(name) {
            Some(s) => s.clone(),
            None if self.created(name) => MStream::default(),
            None => return None,
        };
        for w in &self.stream_writes {
            if let StreamWrite::Trim(n, before) = w
                && n == name
            {
                st.first = (*before).clamp(st.first, st.next());
            }
        }
        Some(st)
    }

    fn masked(&self, k: &[u8]) -> bool {
        self.ranges.iter().any(|r| r.contains(k))
    }

    fn get(&mut self, k: &[u8]) -> Option<(MVal, Option<u64>)> {
        if let Some(w) = self.writes.get(k) {
            return w.clone().map(|v| (v, None));
        }
        self.reads.push(ReadItem::Key(k.to_vec()));
        if self.masked(k) {
            return None;
        }
        self.base.kv.get(k).map(|(v, s)| (v.clone(), Some(*s)))
    }

    fn view(&self, r: &KeyRange) -> Vec<(Vec<u8>, MVal, Option<u64>)> {
        let mut m: BTreeMap<Vec<u8>, (MVal, Option<u64>)> = BTreeMap::new();
        for (k, (v, s)) in &self.base.kv {
            if r.contains(k) && !self.writes.contains_key(k) && !self.masked(k) {
                m.insert(k.clone(), (v.clone(), Some(*s)));
            }
        }
        for (k, w) in &self.writes {
            if let (true, Some(v)) = (r.contains(k), w) {
                m.insert(k.clone(), (v.clone(), None));
            }
        }
        m.into_iter().map(|(k, (v, s))| (k, v, s)).collect()
    }

    fn scan(&mut self, r: &KeyRange, reverse: bool, n: usize) -> Vec<(Vec<u8>, MVal, Option<u64>)> {
        if r.is_empty() {
            return Vec::new();
        }
        let mut all = self.view(r);
        if reverse {
            all.reverse();
        }
        if all.len() >= n {
            let last = Bytes::from(all[n - 1].0.clone());
            let covered = if reverse {
                KeyRange::new(Bound::Included(last), r.end.clone())
            } else {
                KeyRange::new(r.start.clone(), Bound::Included(last))
            };
            self.reads.push(ReadItem::Range(covered));
            all.truncate(n);
        } else {
            self.reads.push(ReadItem::Range(r.clone()));
        }
        all
    }

    fn put(&mut self, k: Vec<u8>, v: MVal) {
        self.writes.insert(k, Some(v));
    }

    fn delete_range(&mut self, r: KeyRange) {
        if r.is_empty() {
            return;
        }
        self.writes.retain(|k, _| !r.contains(k));
        self.ranges.push(r);
    }

    fn row(&self, pk: i64) -> Option<MRow> {
        match self.row_writes.get(&pk) {
            Some(w) => w.clone(),
            None => self.base.rows.get(&pk).cloned(),
        }
    }

    fn rows(&self) -> BTreeMap<i64, MRow> {
        let mut m = self.base.rows.clone();
        for (pk, w) in &self.row_writes {
            match w {
                Some(r) => m.insert(*pk, r.clone()),
                None => m.remove(pk),
            };
        }
        m
    }

    fn is_write(&self) -> bool {
        !self.writes.is_empty()
            || !self.ranges.is_empty()
            || !self.row_writes.is_empty()
            || !self.stream_writes.is_empty()
            || !self.consumer_writes.is_empty()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Ct {
    Int,
    Str,
    Json,
}

fn col_type(c: &str) -> Option<Ct> {
    match c {
        "id" | "grp" => Some(Ct::Int),
        "name" => Some(Ct::Str),
        "doc" => Some(Ct::Json),
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Datum {
    Null,
    Int(i64),
    Str(String),
    Json(Json),
}

impl Datum {
    fn key(&self) -> Option<TKey> {
        match self {
            Datum::Null => Some(TKey::Null),
            Datum::Int(i) => Some(TKey::Int(*i)),
            Datum::Str(s) => Some(TKey::Str(s.clone())),
            Datum::Json(_) => None,
        }
    }
}

fn datum_from_json(ct: Ct, v: &Json) -> MResult<Datum> {
    if v.is_null() {
        return Ok(Datum::Null);
    }
    match ct {
        Ct::Int => v.as_i64().map(Datum::Int).ok_or(Fail::Invalid),
        Ct::Str => v.as_str().map(|s| Datum::Str(s.to_string())).ok_or(Fail::Invalid),
        Ct::Json => Ok(Datum::Json(v.clone())),
    }
}

fn key_from_json(ct: Ct, v: &Json) -> MResult<TKey> {
    if ct == Ct::Json {
        return Err(Fail::Invalid);
    }
    datum_from_json(ct, v)?.key().ok_or(Fail::Invalid)
}

fn row_value(pk: i64, r: &MRow, col: &str) -> Option<Json> {
    match col {
        "id" => Some(json!(pk)),
        "name" => r.name.clone().map(Json::String),
        "grp" => r.grp.map(|g| json!(g)),
        _ => r.doc.clone(),
    }
}

fn row_json(pk: i64, r: &MRow) -> Json {
    json!({"id": pk, "name": r.name, "grp": r.grp, "doc": r.doc.clone().unwrap_or(Json::Null)})
}

/// Returns `true` if a parsed filter is valid for the table schema.
fn filter_binds(v: &Json) -> bool {
    match filter_shape(v) {
        FilterShape::And(fs) | FilterShape::Or(fs) => fs.iter().all(filter_binds),
        FilterShape::Not(f) => filter_binds(f),
        FilterShape::Cmp(o) => {
            let Some(ct) = o.get("col").and_then(Json::as_str).and_then(col_type) else { return false };
            let fits = |v: &Json| match ct {
                Ct::Int => v.is_i64(),
                Ct::Str => v.is_string(),
                Ct::Json => true,
            };
            match (o["op"].as_str(), o.get("value")) {
                (Some("exists"), _) => true,
                (Some("prefix" | "contains"), _) if ct == Ct::Int => false,
                (Some("in"), Some(Json::Array(a))) => a.iter().all(fits),
                (_, Some(v)) => fits(v),
                _ => false,
            }
        }
    }
}

fn row_matches(f: &Json, pk: i64, r: &MRow) -> bool {
    filter_eval(f, &|o: &Map<String, Json>| {
        let col = o["col"].as_str().unwrap_or("");
        cmp_holds(o, row_value(pk, r, col).as_ref())
    })
}

fn cmp_vec(a: &[TKey], b: &[TKey]) -> Ordering {
    for (x, y) in a.iter().zip(b) {
        match x.cmp_key(y) {
            Ordering::Equal => {}
            o => return o,
        }
    }
    a.len().cmp(&b.len())
}

/// Returns a row's sort key in `index`, omitting the primary key for non-null unique entries.
fn entry_vec(index: Option<&str>, pk: i64, r: &MRow) -> Vec<TKey> {
    match index {
        Some("by_name") => match &r.name {
            Some(n) => vec![TKey::Str(n.clone())],
            None => vec![TKey::Null, TKey::Int(pk)],
        },
        Some("by_grp") => vec![r.grp.map(TKey::Int).unwrap_or(TKey::Null), TKey::Int(pk)],
        _ => vec![TKey::Int(pk)],
    }
}

fn valid_stream_name(s: &str) -> bool {
    (1..=128).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
}

fn varint_len(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

fn record_size(r: &MRecord) -> usize {
    let headers: usize = r
        .headers
        .iter()
        .map(|(k, v)| varint_len(k.len() as u64) + k.len() + varint_len(v.len() as u64) + v.len())
        .sum();
    1 + varint_len(r.headers.len() as u64) + headers + r.data.len()
}

fn new_record(enc: Encoding, r: &NewRecord) -> MResult<MRecord> {
    let (data, json) = match (&r.value, &r.json) {
        (Some(v), None) => (decode(enc, v)?, None),
        (None, Some(j)) => (j.to_string().into_bytes(), Some(j.clone())),
        _ => return Err(Fail::Invalid),
    };
    if r.headers.len() > 64 || r.headers.iter().any(|(k, v)| k.is_empty() || k.len() > 128 || v.len() > 4096) {
        return Err(Fail::Invalid);
    }
    Ok(MRecord { data, json, headers: r.headers.clone() })
}

fn stream_record_json(enc: Encoding, seq: u64, r: &MRecord) -> Json {
    let mut m = Map::new();
    m.insert("seq".into(), json!(seq));
    m.insert("time".into(), json!(ANY_NUMBER));
    if !r.headers.is_empty() {
        m.insert("headers".into(), json!(r.headers));
    }
    match &r.json {
        Some(j) => {
            m.insert("json".into(), j.clone());
        }
        None => put_bytes(enc, "value", &r.data, &mut m),
    }
    Json::Object(m)
}

const FILTER_OPS: [&str; 10] = ["eq", "ne", "lt", "le", "gt", "ge", "in", "prefix", "contains", "exists"];

enum FilterShape<'a> {
    And(&'a [Json]),
    Or(&'a [Json]),
    Not(&'a Json),
    Cmp(&'a Map<String, Json>),
}

fn filter_shape(v: &Json) -> FilterShape<'_> {
    let o = v.as_object().expect("a parsed filter is an object");
    match (o.get("and"), o.get("or"), o.get("not")) {
        (Some(Json::Array(a)), ..) => FilterShape::And(a),
        (_, Some(Json::Array(a)), _) => FilterShape::Or(a),
        (.., Some(n)) => FilterShape::Not(n),
        _ => FilterShape::Cmp(o),
    }
}

type OperandOk = fn(&Map<String, Json>, &str, Option<&Json>) -> bool;

fn table_operand(o: &Map<String, Json>, _: &str, _: Option<&Json>) -> bool {
    o.get("col").is_some_and(Json::is_string) && o.keys().all(|k| ["col", "op", "value"].contains(&k.as_str()))
}

fn stream_operand(o: &Map<String, Json>, op: &str, value: Option<&Json>) -> bool {
    let name = |k: &str, max: usize| o.get(k).map(|v| v.as_str().is_some_and(|s| (1..=max).contains(&s.len())));
    let known = o.keys().all(|k| ["field", "header", "op", "value"].contains(&k.as_str()));
    known
        && match (name("field", 256), name("header", 128)) {
            (Some(false), _) | (_, Some(false)) | (Some(_), Some(_)) => false,
            (None, None) => op == "contains" && value.is_some_and(Json::is_string),
            _ => true,
        }
}

fn filter_parses(v: &Json, operand_ok: OperandOk) -> bool {
    fn node(v: &Json, ok: OperandOk, depth: usize, nodes: &mut usize) -> bool {
        *nodes += 1;
        let Some(o) = v.as_object().filter(|_| depth <= 16 && *nodes <= 256) else { return false };
        if ["and", "or", "not"].iter().any(|k| o.contains_key(*k)) {
            return o.len() == 1
                && match (o.get("and").or(o.get("or")), o.get("not")) {
                    (Some(Json::Array(fs)), _) => fs.iter().all(|f| node(f, ok, depth + 1, nodes)),
                    (_, Some(f)) => node(f, ok, depth + 1, nodes),
                    _ => false,
                };
        }
        let Some(op) = o.get("op").and_then(Json::as_str).filter(|op| FILTER_OPS.contains(op)) else { return false };
        let value = o.get("value");
        let value_ok = match (op, value) {
            ("exists", v) => v.is_none(),
            (_, None | Some(Json::Null)) => false,
            ("in", Some(v)) => v.as_array().is_some_and(|a| a.len() <= 1000 && a.iter().all(|x| !x.is_null())),
            ("prefix", Some(v)) => v.is_string(),
            ("lt" | "le" | "gt" | "ge", Some(v)) => v.is_number() || v.is_string() || v.is_boolean(),
            _ => true,
        };
        value_ok && ok(o, op, value)
    }
    node(v, operand_ok, 0, &mut 0)
}

fn filter_eval(v: &Json, cmp: &dyn Fn(&Map<String, Json>) -> bool) -> bool {
    match filter_shape(v) {
        FilterShape::And(fs) => fs.iter().all(|f| filter_eval(f, cmp)),
        FilterShape::Or(fs) => fs.iter().any(|f| filter_eval(f, cmp)),
        FilterShape::Not(f) => !filter_eval(f, cmp),
        FilterShape::Cmp(o) => cmp(o),
    }
}

fn numbers_cmp(a: &serde_json::Number, b: &serde_json::Number) -> Option<Ordering> {
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return Some(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_u64(), b.as_u64()) {
        return Some(x.cmp(&y));
    }
    a.as_f64()?.partial_cmp(&b.as_f64()?)
}

fn same(a: &Json, b: &Json) -> bool {
    match (a, b) {
        (Json::Number(x), Json::Number(y)) => numbers_cmp(x, y) == Some(Ordering::Equal),
        (Json::Array(x), Json::Array(y)) => x.len() == y.len() && x.iter().zip(y).all(|(a, b)| same(a, b)),
        (Json::Object(x), Json::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, a)| y.get(k).is_some_and(|b| same(a, b)))
        }
        _ => a == b,
    }
}

fn cmp_holds(o: &Map<String, Json>, target: Option<&Json>) -> bool {
    let Some(t) = target.filter(|t| !t.is_null()) else { return false };
    let value = o.get("value").unwrap_or(&Json::Null);
    let order = || match (t, value) {
        (Json::Number(x), Json::Number(y)) => numbers_cmp(x, y),
        (Json::String(x), Json::String(y)) => Some(x.cmp(y)),
        (Json::Bool(x), Json::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    };
    match o["op"].as_str().unwrap_or("") {
        "eq" => same(t, value),
        "ne" => !same(t, value),
        "lt" => order() == Some(Ordering::Less),
        "le" => matches!(order(), Some(Ordering::Less | Ordering::Equal)),
        "gt" => order() == Some(Ordering::Greater),
        "ge" => matches!(order(), Some(Ordering::Greater | Ordering::Equal)),
        "in" => value.as_array().is_some_and(|a| a.iter().any(|x| same(t, x))),
        "prefix" => matches!((t, value), (Json::String(x), Json::String(p)) if x.starts_with(p.as_str())),
        "contains" => match (t, value) {
            (Json::String(x), Json::String(p)) => x.contains(p.as_str()),
            (Json::Array(a), x) => a.iter().any(|e| same(e, x)),
            _ => false,
        },
        _ => true,
    }
}

fn record_matches(f: &Json, r: &MRecord) -> bool {
    filter_eval(f, &|o: &Map<String, Json>| match (o.get("field"), o.get("header")) {
        (Some(f), _) => {
            let found = r.json.as_ref().and_then(|d| {
                f.as_str().unwrap_or("").split('.').try_fold(d, |cur, seg| match cur {
                    Json::Object(o) => o.get(seg),
                    Json::Array(a) => seg.parse::<usize>().ok().and_then(|i| a.get(i)),
                    _ => None,
                })
            });
            cmp_holds(o, found)
        }
        (_, Some(h)) => cmp_holds(o, r.headers.get(h.as_str().unwrap_or("")).map(|v| json!(v)).as_ref()),
        _ => {
            let needle = o["value"].as_str().unwrap_or("").as_bytes();
            needle.is_empty() || r.data.windows(needle.len()).any(|w| w == needle)
        }
    })
}

struct Out {
    json: Json,
    cursor: Option<(Option<String>, Vec<TKey>)>,
}

impl From<Json> for Out {
    fn from(json: Json) -> Out {
        Out { json, cursor: None }
    }
}

struct QueryArgs<'q> {
    index: &'q Option<String>,
    eq: &'q [Json],
    range: &'q Option<RangeSpec>,
    filter: &'q Option<Json>,
    limit: Option<usize>,
    reverse: bool,
    cursor: &'q Option<String>,
}

struct World {
    snap: Snapshot,
    version: u64,
    last_write: BTreeMap<Vec<u8>, u64>,
    idem: BTreeMap<String, (u64, Vec<u64>)>,
    /// Idempotency keys resolved as not applied by a failed precondition.
    aborted: BTreeSet<String>,
    staged: BTreeMap<String, (Vec<u8>, String)>,
    query_cursors: BTreeMap<String, (Option<String>, Vec<TKey>)>,
    inline_max: usize,
    json_max: u64,
    record_max: usize,
    stream_log: BTreeMap<String, StreamLog>,
}

fn put_bytes(enc: Encoding, field: &str, b: &[u8], m: &mut Map<String, Json>) {
    match enc {
        Encoding::Base64 => {
            m.insert(field.into(), json!(b64::encode(b)));
        }
        Encoding::Utf8 => match std::str::from_utf8(b) {
            Ok(s) => {
                m.insert(field.into(), json!(s));
            }
            Err(_) => {
                m.insert(format!("{field}_b64"), json!(b64::encode(b)));
            }
        },
    }
}

fn blob_json(v: &MVal, b: &BlobMeta) -> Json {
    json!({
        "blob_id": b.id.clone().unwrap_or_else(|| ANY.into()),
        "size": v.data.len(),
        "sha256": b64::hex(&sha256(&v.data)),
        "content_type": b.content_type,
    })
}

fn decode(enc: Encoding, s: &str) -> MResult<Vec<u8>> {
    match enc {
        Encoding::Utf8 => Ok(s.as_bytes().to_vec()),
        Encoding::Base64 => b64::decode(s).ok_or(Fail::Invalid),
    }
}

fn user_key(k: Vec<u8>) -> MResult<Vec<u8>> {
    if k.is_empty() || k.len() > crate::ops::MAX_USER_KEY { Err(Fail::Invalid) } else { Ok(k) }
}

fn user_range(
    enc: Encoding,
    prefix: &Option<String>,
    start: &Option<String>,
    end: &Option<String>,
) -> MResult<KeyRange> {
    if prefix.is_some() && (start.is_some() || end.is_some()) {
        return Err(Fail::Invalid);
    }
    if let Some(p) = prefix {
        let p = decode(enc, p)?;
        let e = crate::util::prefix_end(&p).map(|e| Bound::Excluded(Bytes::from(e))).unwrap_or(Bound::Unbounded);
        return Ok(KeyRange::new(Bound::Included(Bytes::from(p)), e));
    }
    let s = match start {
        Some(s) => Bytes::from(decode(enc, s)?),
        None => Bytes::new(),
    };
    let e = match end {
        Some(e) => Bound::Excluded(Bytes::from(decode(enc, e)?)),
        None => Bound::Unbounded,
    };
    Ok(KeyRange::new(Bound::Included(s), e))
}

impl World {
    fn value_json(&self, enc: Encoding, v: &MVal, m: &mut Map<String, Json>) {
        match &v.blob {
            None => put_bytes(enc, "value", &v.data, m),
            Some(b) => {
                m.insert("blob".into(), blob_json(v, b));
                if v.data.len() as u64 <= self.json_max {
                    put_bytes(enc, "value", &v.data, m);
                }
            }
        }
    }

    fn read_json(&self, enc: Encoding, r: Option<(MVal, Option<u64>)>) -> Json {
        let mut m = Map::new();
        match r {
            None => {
                m.insert("found".into(), json!(false));
            }
            Some((v, s)) => {
                m.insert("found".into(), json!(true));
                self.value_json(enc, &v, &mut m);
                m.insert("version".into(), json!(s));
                if v.expiry == Expiry::At {
                    m.insert("expires_at".into(), json!(ANY_NUMBER));
                }
            }
        }
        Json::Object(m)
    }

    fn make_value(&self, data: Vec<u8>) -> MVal {
        let blob = (data.len() > self.inline_max)
            .then(|| BlobMeta { id: None, content_type: "application/octet-stream".into() });
        MVal { data, blob, expiry: Expiry::Never }
    }

    fn scan_items(&self, enc: Encoding, items: &[(Vec<u8>, MVal, Option<u64>)], keys_only: bool) -> Vec<Json> {
        items
            .iter()
            .map(|(k, v, s)| {
                let mut m = Map::new();
                put_bytes(enc, "key", k, &mut m);
                if !keys_only {
                    self.value_json(enc, v, &mut m);
                } else if let Some(b) = &v.blob {
                    m.insert("blob".into(), blob_json(v, b));
                }
                m.insert("version".into(), json!(s));
                if v.expiry == Expiry::At {
                    m.insert("expires_at".into(), json!(ANY_NUMBER));
                }
                Json::Object(m)
            })
            .collect()
    }

    fn exec(&self, t: &mut TxnView, enc: Encoding, op: &Op) -> MResult<Out> {
        match op {
            Op::Get { key } => {
                let k = user_key(decode(enc, key)?)?;
                let r = t.get(&k);
                Ok(self.read_json(enc, r).into())
            }
            Op::Scan { prefix, start, end, limit, reverse, keys_only, cursor } => {
                let limit = limit.unwrap_or(100);
                if limit == 0 || limit > crate::ops::MAX_SCAN_LIMIT {
                    return Err(Fail::Invalid);
                }
                let mut range = user_range(enc, prefix, start, end)?;
                if let Some(c) = cursor {
                    let ck = user_key(b64::decode_url(c).ok_or(Fail::Invalid)?)?;
                    if !range.contains(&ck) {
                        return Err(Fail::Invalid);
                    }
                    let ck = Bound::Excluded(Bytes::from(ck));
                    range = if *reverse { KeyRange::new(range.start, ck) } else { KeyRange::new(ck, range.end) };
                }
                let items = t.scan(&range, *reverse, limit + 1);
                let next = (items.len() > limit).then(|| b64::encode_url(&items[limit - 1].0));
                let shown = &items[..items.len().min(limit)];
                Ok(json!({"items": self.scan_items(enc, shown, *keys_only), "next_cursor": next}).into())
            }
            Op::Put { key, value, blob_id, ttl_ms } => {
                let k = user_key(decode(enc, key)?)?;
                let expiry = ttl(*ttl_ms)?;
                let v = match (value, blob_id) {
                    (Some(v), None) => self.make_value(decode(enc, v)?),
                    (None, Some(id)) => {
                        if !crate::engine::types::valid_object_id(id) {
                            return Err(Fail::Invalid);
                        }
                        let (data, ct) = self.staged.get(id).ok_or(Fail::NotFound)?;
                        MVal {
                            data: data.clone(),
                            blob: Some(BlobMeta { id: Some(id.clone()), content_type: ct.clone() }),
                            expiry: Expiry::Never,
                        }
                    }
                    _ => return Err(Fail::Invalid),
                };
                t.put(k, MVal { expiry, ..v });
                Ok(json!({}).into())
            }
            Op::Delete { key } => {
                let k = user_key(decode(enc, key)?)?;
                t.writes.insert(k, None);
                Ok(json!({}).into())
            }
            Op::Copy { from, to, ttl_ms } => {
                let f = user_key(decode(enc, from)?)?;
                let to = user_key(decode(enc, to)?)?;
                let expiry = ttl(*ttl_ms)?;
                let r = t.get(&f);
                t.pinned.push(f);
                let (v, _) = r.ok_or(Fail::Precondition)?;
                t.put(to, MVal { expiry, ..v });
                Ok(json!({}).into())
            }
            Op::DeleteRange { prefix, start, end } => {
                if prefix.is_none() && start.is_none() && end.is_none() {
                    return Err(Fail::Invalid);
                }
                let r = user_range(enc, prefix, start, end)?;
                t.delete_range(r);
                Ok(json!({}).into())
            }
            Op::Check { key, exists, version, value } => {
                let k = user_key(decode(enc, key)?)?;
                let r = t.get(&k);
                if exists.is_none() && version.is_none() && value.is_none() {
                    return Err(Fail::Invalid);
                }
                if exists.is_some_and(|e| r.is_some() != e) {
                    return Err(Fail::Precondition);
                }
                if let Some(want) = version
                    && r.as_ref().and_then(|x| x.1) != Some(*want)
                {
                    return Err(Fail::Precondition);
                }
                if let Some(want) = value {
                    let want = decode(enc, want)?;
                    if r.as_ref().map(|x| &x.0.data) != Some(&want) {
                        return Err(Fail::Precondition);
                    }
                }
                Ok(json!({}).into())
            }
            Op::Increment { key, by, ttl_ms } => {
                let k = user_key(decode(enc, key)?)?;
                let set = ttl(*ttl_ms)?;
                let (cur, kept) = match t.get(&k) {
                    None => (0, Expiry::Never),
                    Some((v, _)) => (
                        std::str::from_utf8(&v.data)
                            .ok()
                            .and_then(|s| s.trim().parse::<i64>().ok())
                            .ok_or(Fail::Invalid)?,
                        v.expiry,
                    ),
                };
                let n = cur.checked_add(*by).ok_or(Fail::Invalid)?;
                let expiry = if set == Expiry::Never { kept } else { set };
                t.put(k, MVal { data: n.to_string().into_bytes(), blob: None, expiry });
                Ok(json!({"value": n}).into())
            }
            Op::GetRow { table, pk } => {
                schema(t, table)?;
                let pk = parse_pk(pk)?;
                Ok(match t.row(pk) {
                    Some(r) if r.expiry == Expiry::At => {
                        json!({"found": true, "row": row_json(pk, &r), "expires_at": ANY_NUMBER})
                    }
                    Some(r) => json!({"found": true, "row": row_json(pk, &r)}),
                    None => json!({"found": false}),
                }
                .into())
            }
            Op::InsertRow { table, row, ttl_ms } | Op::UpsertRow { table, row, ttl_ms } => {
                let expiry = ttl(*ttl_ms)?;
                schema(t, table)?;
                let (pk, new) = row_from_json(row, None)?;
                if matches!(op, Op::InsertRow { .. }) && t.row(pk).is_some() {
                    return Err(Fail::Exists);
                }
                check_unique(t, pk, &new)?;
                t.row_writes.insert(pk, Some(MRow { expiry, ..new }));
                Ok(json!({}).into())
            }
            Op::UpdateRow { table, pk, set, ttl_ms } => {
                let expiry = ttl(*ttl_ms)?;
                schema(t, table)?;
                let pk = parse_pk(pk)?;
                let Some(old) = t.row(pk) else { return Ok(json!({"found": false}).into()) };
                if set.as_object().is_some_and(|o| o.contains_key("id")) {
                    return Err(Fail::Invalid);
                }
                let (_, new) = row_from_json(set, Some((pk, &old)))?;
                check_unique(t, pk, &new)?;
                let expiry = if expiry == Expiry::Never { old.expiry } else { expiry };
                t.row_writes.insert(pk, Some(MRow { expiry, ..new }));
                Ok(json!({"found": true}).into())
            }
            Op::UpdateRows { table, selection, set, ttl_ms, limit } => {
                let expiry = ttl(*ttl_ms)?;
                if *limit == Some(0) {
                    return Err(Fail::Invalid);
                }
                schema(t, table)?;
                check_set(set)?;
                let pks = self.select(t, selection, *limit)?;
                for pk in &pks {
                    let old = t.row(*pk).ok_or(Fail::NotFound)?;
                    let (_, new) = row_from_json(set, Some((*pk, &old)))?;
                    check_unique(t, *pk, &new)?;
                    let expiry = if expiry == Expiry::Never { old.expiry } else { expiry };
                    t.row_writes.insert(*pk, Some(MRow { expiry, ..new }));
                }
                Ok(json!({"count": pks.len()}).into())
            }
            Op::DeleteRows { table, selection, limit } => {
                if *limit == Some(0) {
                    return Err(Fail::Invalid);
                }
                schema(t, table)?;
                let pks = self.select(t, selection, *limit)?;
                for pk in &pks {
                    t.row_writes.insert(*pk, None);
                }
                Ok(json!({"count": pks.len()}).into())
            }
            Op::DeleteRow { table, pk } => {
                schema(t, table)?;
                let pk = parse_pk(pk)?;
                if t.row(pk).is_none() {
                    return Ok(json!({"found": false}).into());
                }
                t.row_writes.insert(pk, None);
                Ok(json!({"found": true}).into())
            }
            Op::QueryRows { table, index, eq, range, filter, limit, reverse, cursor } => {
                schema(t, table)?;
                let q = QueryArgs { index, eq, range, filter, limit: *limit, reverse: *reverse, cursor };
                let (rows, stop) = self.query(t, &q)?;
                let next = stop.as_ref().map(|_| ANY);
                Ok(Out { json: json!({"rows": rows, "next_cursor": next}), cursor: stop.map(|v| (index.clone(), v)) })
            }
            Op::Append { stream, records, expect_seq, create } => {
                let records = records.iter().map(|r| new_record(enc, r)).collect::<MResult<Vec<_>>>()?;
                let sizes: Vec<usize> = records.iter().map(record_size).collect();
                if sizes.iter().any(|n| *n > self.record_max) {
                    return Err(Fail::TooLarge);
                }
                if !valid_stream_name(stream) {
                    return Err(Fail::Invalid);
                }
                t.stream_keys.push(StreamRead::Catalog(stream.clone()));
                if t.stream(stream).is_none() {
                    if !create {
                        return Err(Fail::NotFound);
                    }
                    t.stream_writes.push(StreamWrite::Create(stream.clone()));
                }
                let n = records.len();
                t.stream_writes.push(StreamWrite::Append {
                    stream: stream.clone(),
                    records,
                    expect: *expect_seq,
                    op_index: t.op_index,
                });
                Ok(json!({"count": n}).into())
            }
            Op::ReadStream {
                stream,
                from_seq,
                from_time,
                tail,
                consumer,
                until_seq,
                until_time,
                reverse,
                limit,
                max_bytes,
                filter,
                scan_limit,
            } => {
                let starts = [from_seq.is_some(), from_time.is_some(), tail.is_some(), consumer.is_some()];
                if starts.iter().filter(|x| **x).count() > 1
                    || until_seq.is_some() && until_time.is_some()
                    || limit.is_some_and(|l| !(1..=1000).contains(&l))
                    || max_bytes.is_some_and(|b| !(1..=16 << 20).contains(&b))
                    || scan_limit.is_some_and(|l| !(1..=100_000).contains(&l))
                    || consumer.as_ref().is_some_and(|c| !valid_stream_name(c))
                    || !valid_stream_name(stream)
                {
                    return Err(Fail::Invalid);
                }
                t.stream_keys.push(StreamRead::Catalog(stream.clone()));
                let st = t.stream(stream).ok_or(Fail::NotFound)?;
                if from_time.is_some() || until_time.is_some() {
                    return Err(Fail::Unmodeled);
                }
                let lo = match (from_seq, tail, consumer) {
                    (Some(s), ..) => *s,
                    (_, Some(n), _) => {
                        t.stream_reads.push(StreamRead::State(stream.clone()));
                        st.next().saturating_sub(*n)
                    }
                    (_, _, Some(c)) => {
                        t.stream_reads.push(StreamRead::Consumer(stream.clone(), c.clone()));
                        let own = t.consumer_writes.get(&(stream.clone(), c.clone()));
                        own.or(st.consumers.get(c)).copied().unwrap_or(0)
                    }
                    _ => 0,
                }
                .max(st.first);
                let hi = until_seq.unwrap_or(u64::MAX).min(st.next());
                let mut seqs: Vec<u64> = (lo..hi.max(lo)).collect();
                if *reverse {
                    seqs.reverse();
                }
                let (limit, scan) = (limit.unwrap_or(100), scan_limit.unwrap_or(10_000));
                let max_bytes = max_bytes.unwrap_or(4 << 20);
                let (mut out, mut bytes, mut scanned, mut last) = (Vec::new(), 0, 0, None);
                let mut it = seqs.into_iter();
                let exhausted = loop {
                    if out.len() >= limit || bytes >= max_bytes || scanned >= scan {
                        break false;
                    }
                    let Some(seq) = it.next() else { break true };
                    scanned += 1;
                    last = Some(seq);
                    let r = &st.records[seq as usize];
                    if filter.as_ref().is_none_or(|f| record_matches(f, r)) {
                        bytes += record_size(r);
                        out.push(stream_record_json(enc, seq, r));
                    }
                };
                let next_seq = match (*reverse, exhausted, last) {
                    (false, false, Some(l)) => l + 1,
                    (false, _, _) => hi.max(lo),
                    (true, false, Some(l)) => l,
                    (true, _, _) => lo.min(hi),
                };
                let (rlo, rhi) = match (*reverse, exhausted) {
                    (false, true) => (lo, *until_seq),
                    (false, false) => (lo, Some(next_seq)),
                    (true, true) => (lo, *until_seq),
                    (true, false) => (next_seq, *until_seq),
                };
                t.stream_reads.push(StreamRead::Range(stream.clone(), rlo, rhi));
                Ok(json!({"records": out, "next_seq": next_seq, "done": exhausted}).into())
            }
            Op::StreamInfo { stream } => {
                if !valid_stream_name(stream) {
                    return Err(Fail::Invalid);
                }
                t.stream_keys.push(StreamRead::Catalog(stream.clone()));
                let st = t.stream(stream).ok_or(Fail::NotFound)?;
                t.stream_reads.push(StreamRead::State(stream.clone()));
                let mut m = json!({"first_seq": st.first, "next_seq": st.next(), "bytes": ANY_NUMBER});
                if st.next() > st.first {
                    m["first_time"] = json!(ANY_NUMBER);
                    m["last_time"] = json!(ANY_NUMBER);
                }
                Ok(m.into())
            }
            Op::TrimStream { stream, before_seq, before_time } => {
                if !valid_stream_name(stream) {
                    return Err(Fail::Invalid);
                }
                t.stream_keys.push(StreamRead::Catalog(stream.clone()));
                t.stream(stream).ok_or(Fail::NotFound)?;
                let before = match (before_seq, before_time) {
                    (Some(b), None) => *b,
                    (None, Some(_)) => return Err(Fail::Unmodeled),
                    _ => return Err(Fail::Invalid),
                };
                t.stream_writes.push(StreamWrite::Trim(stream.clone(), before));
                Ok(json!({}).into())
            }
            Op::GetConsumer { stream, consumer } => {
                if !valid_stream_name(stream) {
                    return Err(Fail::Invalid);
                }
                t.stream_keys.push(StreamRead::Catalog(stream.clone()));
                let st = t.stream(stream).ok_or(Fail::NotFound)?;
                if !valid_stream_name(consumer) {
                    return Err(Fail::Invalid);
                }
                t.stream_reads.push(StreamRead::Consumer(stream.clone(), consumer.clone()));
                let own = t.consumer_writes.get(&(stream.clone(), consumer.clone()));
                Ok(match own.or(st.consumers.get(consumer)) {
                    Some(seq) => json!({"found": true, "seq": seq}),
                    None => json!({"found": false}),
                }
                .into())
            }
            Op::SetConsumer { stream, consumer, seq } => {
                if !valid_stream_name(consumer) || !valid_stream_name(stream) {
                    return Err(Fail::Invalid);
                }
                t.stream_keys.push(StreamRead::Catalog(stream.clone()));
                if *seq > t.stream(stream).ok_or(Fail::NotFound)?.next() {
                    return Err(Fail::Invalid);
                }
                t.consumer_writes.insert((stream.clone(), consumer.clone()), *seq);
                Ok(json!({}).into())
            }
        }
    }

    /// Returns the starting sequence number for each append in `t` if committed
    /// now, or the index of the first append whose `expect_seq` precondition fails.
    fn stream_outcome(&self, t: &TxnView) -> std::result::Result<Vec<u64>, usize> {
        let mut next: BTreeMap<&str, u64> = BTreeMap::new();
        let mut firsts = Vec::new();
        for w in &t.stream_writes {
            if let StreamWrite::Append { stream, records, expect, op_index } = w {
                let n = next.entry(stream).or_insert_with(|| self.snap.streams.get(stream).map_or(0, |s| s.next()));
                if expect.is_some_and(|x| x != *n) {
                    return Err(*op_index);
                }
                firsts.push(*n);
                *n += records.len() as u64;
            }
        }
        Ok(firsts)
    }

    fn stream_changed(&self, r: &StreamRead, rv: u64) -> bool {
        let empty = StreamLog::default();
        let name = match r {
            StreamRead::Catalog(n) | StreamRead::State(n) | StreamRead::Range(n, ..) | StreamRead::Consumer(n, _) => n,
        };
        let log = self.stream_log.get(name).unwrap_or(&empty);
        match r {
            StreamRead::Catalog(_) => log.catalog > rv,
            StreamRead::State(_) => log.state > rv,
            StreamRead::Range(_, lo, hi) => {
                let below_hi = |s: u64| hi.is_none_or(|h| s < h);
                log.appends.iter().any(|(v, f, c)| *v > rv && *c > 0 && f + c > *lo && below_hi(*f))
                    || log.trims.iter().any(|(v, target)| *v > rv && *target > *lo && below_hi(*lo))
            }
            StreamRead::Consumer(_, c) => log.consumers.get(c).is_some_and(|v| *v > rv),
        }
    }

    fn plan(
        &self,
        t: &TxnView,
        sel: (&Option<String>, &[Json], &Option<RangeSpec>, &Option<Json>),
        cursor: &Option<String>,
        reverse: bool,
    ) -> MResult<Vec<(Vec<TKey>, i64)>> {
        let (index, eq, range, filter) = sel;
        if filter.as_ref().is_some_and(|f| !filter_binds(f)) {
            return Err(Fail::Invalid);
        }
        let ct = match index.as_deref() {
            None => Ct::Int,
            Some("by_name") => Ct::Str,
            Some("by_grp") => Ct::Int,
            Some(_) => return Err(Fail::NotFound),
        };
        if eq.len() > 1 {
            return Err(Fail::Invalid);
        }
        let eqk = eq.first().map(|v| if v.is_null() { Ok(TKey::Null) } else { key_from_json(ct, v) }).transpose()?;
        let mut lo: Option<(TKey, bool)> = None;
        let mut hi: Option<(TKey, bool)> = None;
        if let Some(r) = range {
            if !eq.is_empty() {
                return Err(Fail::Invalid);
            }
            if r.gt.is_some() && r.gte.is_some() || r.lt.is_some() && r.lte.is_some() {
                return Err(Fail::Invalid);
            }
            if let Some(v) = &r.gte {
                lo = Some((key_from_json(ct, v)?, true));
            }
            if let Some(v) = &r.gt {
                lo = Some((key_from_json(ct, v)?, false));
            }
            if let Some(v) = &r.lt {
                hi = Some((key_from_json(ct, v)?, false));
            }
            if let Some(v) = &r.lte {
                hi = Some((key_from_json(ct, v)?, true));
            }
        }
        let in_region = |v: &[TKey]| {
            let e = &v[0];
            eqk.as_ref().is_none_or(|k| e.cmp_key(k) == Ordering::Equal)
                && lo.as_ref().is_none_or(|(k, incl)| match e.cmp_key(k) {
                    Ordering::Greater => true,
                    Ordering::Equal => *incl,
                    Ordering::Less => false,
                })
                && hi.as_ref().is_none_or(|(k, incl)| match e.cmp_key(k) {
                    Ordering::Less => true,
                    Ordering::Equal => *incl,
                    Ordering::Greater => false,
                })
        };
        let cursor = match cursor {
            None => None,
            Some(c) => match self.query_cursors.get(c) {
                Some((ix, v)) if ix == index && in_region(v) => Some(v.clone()),
                // Strings from rand_string never base64-decode to a valid row or index prefix.
                _ => return Err(Fail::Invalid),
            },
        };
        let rows = t.rows();
        let mut entries: Vec<(Vec<TKey>, i64)> = rows
            .iter()
            .map(|(pk, r)| (entry_vec(index.as_deref(), *pk, r), *pk))
            .filter(|(v, _)| in_region(v))
            .filter(|(v, _)| match &cursor {
                None => true,
                Some(c) if reverse => cmp_vec(v, c) == Ordering::Less,
                Some(c) => cmp_vec(v, c) == Ordering::Greater,
            })
            .collect();
        entries.sort_by(|a, b| cmp_vec(&a.0, &b.0));
        if reverse {
            entries.reverse();
        }
        Ok(entries)
    }

    fn query(&self, t: &TxnView, q: &QueryArgs) -> MResult<(Vec<Json>, Option<Vec<TKey>>)> {
        let limit = q.limit.unwrap_or(100);
        if limit == 0 || limit > 1000 {
            return Err(Fail::Invalid);
        }
        let entries = self.plan(t, (q.index, q.eq, q.range, q.filter), q.cursor, q.reverse)?;
        let rows = t.rows();
        let page = (limit * 2).clamp(16, 1000);
        let mut out = Vec::new();
        for chunk in entries.chunks(page) {
            for (pos, (v, pk)) in chunk.iter().enumerate() {
                let r = &rows[pk];
                if q.filter.as_ref().is_none_or(|f| row_matches(f, *pk, r)) {
                    out.push(row_json(*pk, r));
                }
                if out.len() >= limit {
                    let more = chunk.len() == page || pos + 1 < chunk.len();
                    return Ok((out, more.then(|| v.clone())));
                }
            }
            if chunk.len() < page {
                break;
            }
        }
        Ok((out, None))
    }

    fn select(&self, t: &TxnView, sel: &Selection, limit: Option<usize>) -> MResult<Vec<i64>> {
        let entries = self.plan(t, (&sel.index, &sel.eq, &sel.range, &sel.filter), &None, false)?;
        let rows = t.rows();
        Ok(entries
            .iter()
            .filter(|(_, pk)| sel.filter.as_ref().is_none_or(|f| row_matches(f, *pk, &rows[pk])))
            .map(|(_, pk)| *pk)
            .take(limit.unwrap_or(usize::MAX))
            .collect())
    }

    fn conflicts(&self, t: &TxnView, iso: Iso) -> Option<bool> {
        if self.version == t.rv {
            return Some(false);
        }
        if t.table_ops {
            return None;
        }
        let items: Vec<ReadItem> = match iso {
            Iso::Serializable => t.reads.clone(),
            Iso::Snapshot => {
                let mut v: Vec<ReadItem> = t.writes.keys().cloned().map(ReadItem::Key).collect();
                v.extend(t.ranges.iter().cloned().map(ReadItem::Range));
                v
            }
        };
        let changed = |k: &[u8]| self.last_write.get(k).is_some_and(|v| *v > t.rv);
        let kv = items.iter().any(|it| match it {
            ReadItem::Key(k) => changed(k),
            ReadItem::Range(r) => self.last_write.iter().any(|(k, v)| *v > t.rv && r.contains(k)),
        });
        let pinned = t.stream_keys.iter().any(|r| self.stream_changed(r, t.rv)) || t.pinned.iter().any(|k| changed(k));
        let streams = match iso {
            Iso::Serializable => t.stream_reads.iter().any(|r| self.stream_changed(r, t.rv)),
            Iso::Snapshot => t
                .consumer_writes
                .keys()
                .any(|(s, c)| self.stream_changed(&StreamRead::Consumer(s.clone(), c.clone()), t.rv)),
        };
        Some(kv || pinned || streams)
    }

    fn abort(&mut self, key: Option<&str>) {
        if let Some(k) = key
            && self.aborted.insert(k.to_string())
        {
            self.version += 1;
        }
    }

    fn apply(&mut self, t: TxnView, version: u64) -> Result<(), String> {
        for r in &t.ranges {
            let doomed: Vec<Vec<u8>> =
                self.snap.kv.keys().filter(|k| r.contains(k) && !t.writes.contains_key(*k)).cloned().collect();
            for k in doomed {
                self.snap.kv.remove(&k);
                self.last_write.insert(k, version);
            }
        }
        for (k, w) in t.writes {
            self.last_write.insert(k.clone(), version);
            match w {
                Some(v) => self.snap.kv.insert(k, (MVal { expiry: v.expiry.committed(), ..v }, version)),
                None => self.snap.kv.remove(&k),
            };
        }
        for (pk, w) in t.row_writes {
            match w {
                Some(r) => self.snap.rows.insert(pk, MRow { expiry: r.expiry.committed(), ..r }),
                None => self.snap.rows.remove(&pk),
            };
        }
        for w in t.stream_writes {
            let name = match &w {
                StreamWrite::Create(n) | StreamWrite::Append { stream: n, .. } | StreamWrite::Trim(n, _) => n.clone(),
            };
            let log = self.stream_log.entry(name.clone()).or_default();
            log.state = version;
            let st = self.snap.streams.entry(name).or_default();
            match w {
                StreamWrite::Create(_) => log.catalog = version,
                StreamWrite::Append { records, .. } => {
                    log.appends.push((version, st.next(), records.len() as u64));
                    st.records.extend(records);
                }
                StreamWrite::Trim(_, before) => {
                    let target = before.clamp(st.first, st.next());
                    if target > st.first {
                        st.first = target;
                        log.trims.push((version, target));
                    }
                }
            }
        }
        for ((name, c), seq) in t.consumer_writes {
            self.stream_log.entry(name.clone()).or_default().consumers.insert(c.clone(), version);
            self.snap.streams.entry(name).or_default().consumers.insert(c, seq);
        }
        self.version = version;
        let mut names = std::collections::BTreeSet::new();
        for r in self.snap.rows.values() {
            if let Some(n) = &r.name
                && !names.insert(n.clone())
            {
                return Err(format!("committed state violates the unique index: two rows named {n:?}"));
            }
        }
        Ok(())
    }
}

fn schema(t: &mut TxnView, table: &str) -> MResult<()> {
    t.table_ops = true;
    if table == "t" { Ok(()) } else { Err(Fail::NotFound) }
}

fn parse_pk(pk: &Json) -> MResult<i64> {
    let vals: Vec<Json> = match pk {
        Json::Array(a) => a.clone(),
        other => vec![other.clone()],
    };
    if vals.len() != 1 || vals[0].is_null() {
        return Err(Fail::Invalid);
    }
    vals[0].as_i64().ok_or(Fail::Invalid)
}

fn row_from_json(obj: &Json, base: Option<(i64, &MRow)>) -> MResult<(i64, MRow)> {
    let o = obj.as_object().ok_or(Fail::Invalid)?;
    if o.keys().any(|k| col_type(k).is_none()) {
        return Err(Fail::Invalid);
    }
    let field = |c: &str, ct: Ct| -> MResult<Option<Datum>> { o.get(c).map(|v| datum_from_json(ct, v)).transpose() };
    let id = match field("id", Ct::Int)? {
        Some(Datum::Int(i)) => Some(i),
        Some(_) => None,
        None => base.map(|b| b.0),
    };
    let id = id.ok_or(Fail::Invalid)?;
    let name = match field("name", Ct::Str)? {
        Some(Datum::Str(s)) => Some(s),
        Some(_) => None,
        None => base.and_then(|b| b.1.name.clone()),
    };
    let grp = match field("grp", Ct::Int)? {
        Some(Datum::Int(i)) => Some(i),
        Some(_) => None,
        None => base.and_then(|b| b.1.grp),
    };
    let doc = match field("doc", Ct::Json)? {
        Some(Datum::Json(j)) => Some(j),
        Some(_) => None,
        None => base.and_then(|b| b.1.doc.clone()),
    };
    Ok((id, MRow { name, grp, doc, expiry: Expiry::Never }))
}

fn check_set(set: &Json) -> MResult<()> {
    let o = set.as_object().ok_or(Fail::Invalid)?;
    for (k, v) in o {
        match col_type(k) {
            None => return Err(Fail::Invalid),
            Some(_) if k == "id" => return Err(Fail::Invalid),
            Some(ct) => datum_from_json(ct, v).map(|_| ())?,
        }
    }
    Ok(())
}

fn check_unique(t: &TxnView, pk: i64, new: &MRow) -> MResult<()> {
    match &new.name {
        Some(n) if t.rows().iter().any(|(p, r)| *p != pk && r.name.as_ref() == Some(n)) => Err(Fail::Exists),
        _ => Ok(()),
    }
}

const NAMES: &[&str] = &["ann", "bob", "cy", "é", ""];

/// Picks a test string that never base64-decodes to a valid row or index key prefix.
fn rand_string(u: &mut Choices) -> String {
    const POOL: &[&str] = &["", "a", "t", "u", "id", "name", "grp", "doc", "by_name", "by_grp", "x/y", "é", "null"];
    match u.below(4) {
        0 => "k".repeat(4097),
        1 => (0..u.below(10)).map(|_| (b'a' + u.below(26) as u8) as char).collect(),
        _ => u.pick(POOL).to_string(),
    }
}

fn rand_json(u: &mut Choices, depth: u32) -> Json {
    match u.below(if depth > 2 { 7 } else { 9 }) {
        0 => Json::Null,
        1 => json!(u.chance(128)),
        2 => json!(u.below(10) as i64 - 3),
        3 => u.pick(&[json!(i64::MAX), json!(i64::MIN), json!(u64::MAX), json!(1.5), json!(-0.0), json!(1e300)]),
        4 | 5 => json!(rand_string(u)),
        6 => json!([1, "x"]),
        7 => Json::Array((0..u.below(3)).map(|_| rand_json(u, depth + 1)).collect()),
        _ => {
            const KEYS: &[&str] = &[
                "op", "key", "value", "table", "row", "pk", "set", "limit", "cursor", "prefix", "start", "end", "eq",
                "range", "filter", "index", "col", "and", "or", "not", "gt", "lte", "by", "exists", "version", "id",
                "name", "grp", "doc", "bogus",
            ];
            let mut m = Map::new();
            for _ in 0..u.below(4) {
                m.insert(u.pick(KEYS).to_string(), rand_json(u, depth + 1));
            }
            Json::Object(m)
        }
    }
}

fn mutate(u: &mut Choices, v: &mut Json, depth: u32) {
    match v {
        Json::Object(m) if !m.is_empty() && depth < 5 && u.chance(200) => {
            let keys: Vec<String> = m.keys().cloned().collect();
            let k = u.pick(&keys);
            match u.below(5) {
                0 => {
                    m.remove(&k);
                }
                1 => {
                    m.insert(k, rand_json(u, 1));
                }
                2 => {
                    m.insert("bogus".into(), rand_json(u, 1));
                }
                _ => {
                    if let Some(x) = m.get_mut(&k) {
                        mutate(u, x, depth + 1);
                    }
                }
            }
        }
        Json::Array(a) if !a.is_empty() && depth < 5 && u.chance(200) => {
            let i = u.below(a.len());
            if u.chance(64) {
                a.remove(i);
            } else {
                mutate(u, &mut a[i], depth + 1);
            }
        }
        _ => *v = rand_json(u, 1),
    }
}

fn enc_str(enc: Encoding, b: &[u8]) -> String {
    match enc {
        Encoding::Utf8 => String::from_utf8_lossy(b).into_owned(),
        Encoding::Base64 => b64::encode(b),
    }
}

struct Gen<'a, 'w> {
    u: Choices<'a>,
    w: &'w World,
    scan_cursors: &'w [String],
}

impl Gen<'_, '_> {
    fn key_bytes(&mut self, enc: Encoding) -> Vec<u8> {
        let utf8: &[&[u8]] = &[b"a", b"b", b"c", b"n", b"a/x", b"a/y", b"b/x", "é".as_bytes()];
        let binary: &[&[u8]] = &[b"\xff", b"\x00\x01", b"a\x80"];
        match self.u.below(100) {
            97 => Vec::new(),
            98 => vec![b'k'; 4096],
            99 => vec![b'k'; 4097],
            87..=96 if enc == Encoding::Base64 => self.u.pick(binary).to_vec(),
            _ => self.u.pick(utf8).to_vec(),
        }
    }

    fn key(&mut self, enc: Encoding) -> String {
        if enc == Encoding::Base64 && self.u.chance(3) {
            return "!!".into();
        }
        let k = self.key_bytes(enc);
        enc_str(enc, &k)
    }

    fn value(&mut self, enc: Encoding) -> String {
        let v: Vec<u8> = match self.u.below(14) {
            0 => Vec::new(),
            1 => b"5".to_vec(),
            2 => b"-3".to_vec(),
            3 => b" 12 ".to_vec(),
            4 => b"9223372036854775807".to_vec(),
            5 => vec![b'x'; 600],
            6 => vec![b'y'; 5000],
            7 if enc == Encoding::Base64 => vec![0xff, 0xfe, 0x00],
            8 if enc == Encoding::Base64 => return "!!".into(),
            _ => format!("v{}", self.u.below(5)).into_bytes(),
        };
        enc_str(enc, &v)
    }

    fn prefix(&mut self, enc: Encoding) -> String {
        let p: Vec<u8> = match self.u.below(8) {
            0 => Vec::new(),
            1 => b"a/".to_vec(),
            2 => b"z".to_vec(),
            3 if enc == Encoding::Base64 => vec![0xff],
            _ => self.key_bytes(enc),
        };
        enc_str(enc, &p)
    }

    fn limit(&mut self) -> Option<Json> {
        match self.u.below(24) {
            0..=3 => None,
            4 => Some(json!(0)),
            5 => Some(json!(1000)),
            6 => Some(json!(1001)),
            _ => Some(json!(1 + self.u.below(5))),
        }
    }

    fn scan(&mut self, enc: Encoding) -> Json {
        let mut o = json!({"op": "scan"});
        match self.u.below(5) {
            0 => {}
            1 | 2 => o["prefix"] = json!(self.prefix(enc)),
            3 => {
                if self.u.chance(200) {
                    o["start"] = json!(self.key(enc));
                }
                if self.u.chance(200) {
                    o["end"] = json!(self.key(enc));
                }
            }
            _ => {
                o["prefix"] = json!(self.prefix(enc));
                o["start"] = json!(self.key(enc));
            }
        }
        if let Some(l) = self.limit() {
            o["limit"] = l;
        }
        if self.u.chance(80) {
            o["reverse"] = json!(true);
        }
        if self.u.chance(50) {
            o["keys_only"] = json!(true);
        }
        if self.u.chance(80) {
            o["cursor"] = match self.u.below(4) {
                0 => json!(rand_string(&mut self.u)),
                _ if self.scan_cursors.is_empty() => json!(b64::encode_url(b"b")),
                _ => json!(self.u.pick(self.scan_cursors)),
            };
        }
        o
    }

    fn check(&mut self, enc: Encoding) -> Json {
        let kb = self.key_bytes(enc);
        let cur = self.w.snap.kv.get(&kb).cloned();
        let mut o = json!({"op": "check", "key": enc_str(enc, &kb)});
        match self.u.below(10) {
            9 => {}
            1 | 5 => o["exists"] = json!(if self.u.chance(200) { cur.is_some() } else { cur.is_none() }),
            2 | 6 => o["version"] = json!(cur.as_ref().map(|c| c.1).unwrap_or(0) + self.u.below(2) as u64),
            3 | 7 => {
                o["value"] = match &cur {
                    Some((v, _)) if self.u.chance(200) => json!(enc_str(enc, &v.data)),
                    _ => json!(self.value(enc)),
                }
            }
            _ => {
                o["exists"] = json!(true);
                o["version"] = json!(cur.as_ref().map(|c| c.1).unwrap_or(0));
            }
        }
        o
    }

    fn pk(&mut self) -> Json {
        match self.u.below(40) {
            35 => json!("x"),
            36 => Json::Null,
            37 => json!([1, 2]),
            38 => json!(1.5),
            39 => json!([]),
            5..=8 => json!([self.u.below(6) as i64 + 1]),
            _ => json!(self.u.below(6) as i64 + 1),
        }
    }

    fn row_fields(&mut self, with_id: bool) -> Json {
        let mut m = Map::new();
        if with_id && self.u.below(16) != 0 {
            m.insert("id".into(), self.pk());
        }
        if self.u.below(4) != 0 {
            m.insert("name".into(), if self.u.chance(40) { Json::Null } else { json!(self.u.pick(NAMES)) });
        }
        if self.u.below(4) != 0 {
            m.insert("grp".into(), if self.u.chance(40) { Json::Null } else { json!(self.u.below(3) as i64) });
        }
        if self.u.chance(80) {
            m.insert(
                "doc".into(),
                self.u.pick(&[json!({"k": true}), json!([1, "y"]), json!("s"), json!(7), Json::Null]),
            );
        }
        if self.u.chance(4) {
            m.insert(if self.u.chance(128) { "bogus" } else { "id" }.into(), json!(1));
        }
        Json::Object(m)
    }

    fn table(&mut self) -> &'static str {
        if self.u.chance(3) { "u" } else { "t" }
    }

    fn filter(&mut self, depth: u32) -> Json {
        match self.u.below(if depth > 1 { 1 } else { 6 }) {
            0 => {
                let col = if self.u.chance(8) { "nope" } else { self.u.pick(&["name", "grp", "id", "doc"]) };
                let op = self.u.pick(&FILTER_OPS);
                let value = if self.u.chance(24) {
                    Some(self.u.pick(&[Json::Null, json!("bob"), json!(1), json!([0, 1, "bob", null])]))
                } else if op == "exists" {
                    None
                } else if op == "in" {
                    Some(Json::Array((0..self.u.below(4)).map(|_| self.cmp_value(col)).collect()))
                } else {
                    Some(self.cmp_value(col))
                };
                let mut f = json!({"col": col, "op": op});
                if let Some(v) = value {
                    f["value"] = v;
                }
                f
            }
            1 => json!({"and": [self.filter(depth + 1), self.filter(depth + 1)]}),
            2 => json!({"or": [self.filter(depth + 1), self.filter(depth + 1)]}),
            3 if self.u.chance(16) => {
                let a = self.filter(depth + 1);
                self.u.pick(&[
                    json!({"and": [a], "or": [a]}),
                    json!({"not": a, "col": "name", "op": "exists"}),
                    json!({"and": [], "bogus": 1}),
                    json!({"col": "name", "op": "exists", "value": 1}),
                ])
            }
            _ => json!({"not": self.filter(depth + 1)}),
        }
    }

    fn cmp_value(&mut self, col: &str) -> Json {
        match col {
            "name" => json!(self.u.pick(&["ann", "bob", "cy", "é", "", "a", "an", "bo"])),
            "doc" => self.u.pick(&[json!({"k": true}), json!("s"), json!(7), json!(7.0), json!("y"), json!(1)]),
            _ => json!(self.u.below(5) as i64 - 1),
        }
    }

    fn ttl(&mut self, o: &mut Json) {
        if self.u.chance(80) {
            o["ttl_ms"] = self.u.pick(&[
                json!(3_600_000_000u64),
                json!(3_600_000_000u64),
                json!(0),
                json!(crate::ops::MAX_TTL_MS + 1),
            ]);
        }
    }

    fn selection(&mut self, o: &mut Json) {
        let index = self.u.pick(&[None, None, Some("by_name"), Some("by_grp"), Some("nope")]);
        let mut w = json!({});
        if let Some(i) = index {
            w["index"] = json!(i);
        }
        let bound = |g: &mut Self| -> Json {
            match (index, g.u.below(8)) {
                (_, 0) => Json::Null,
                (Some("by_name"), _) => json!(g.u.pick(NAMES)),
                _ => json!(g.u.below(7) as i64),
            }
        };
        match self.u.below(5) {
            0 => w["eq"] = json!([bound(self)]),
            1 | 2 => w["range"] = json!({"gte": bound(self)}),
            _ => {}
        }
        if self.u.chance(128) {
            w["filter"] = self.filter(0);
        }
        o["where"] = w;
        if self.u.chance(64) {
            o["limit"] = json!(self.u.below(4));
        }
    }

    fn query(&mut self) -> Json {
        let index = self.u.pick(&[None, None, None, Some("by_name"), Some("by_grp"), Some("by_grp"), Some("nope")]);
        let mut o = json!({"op": "query_rows", "table": self.table()});
        if let Some(i) = index {
            o["index"] = json!(i);
        }
        let bound = |g: &mut Self| -> Json {
            match (index, g.u.below(8)) {
                (_, 0) => Json::Null,
                (_, 1) => json!("x"),
                (Some("by_name"), _) => json!(g.u.pick(NAMES)),
                _ => json!(g.u.below(7) as i64),
            }
        };
        match self.u.below(6) {
            0 => o["eq"] = json!([bound(self)]),
            1 => o["eq"] = json!([bound(self), 1]),
            2 | 3 => {
                let mut r = Map::new();
                for b in ["gt", "gte", "lt", "lte"] {
                    if self.u.chance(90) {
                        r.insert(b.into(), bound(self));
                    }
                }
                o["range"] = Json::Object(r);
            }
            _ => {}
        }
        if self.u.chance(192) {
            o["filter"] = self.filter(0);
        }
        if let Some(l) = self.limit() {
            o["limit"] = l;
        }
        if self.u.chance(80) {
            o["reverse"] = json!(true);
        }
        if self.u.chance(48) {
            let known: Vec<&String> =
                self.w.query_cursors.iter().filter(|(_, (ix, _))| ix.as_deref() == index).map(|(c, _)| c).collect();
            o["cursor"] = match (known.is_empty(), self.u.below(4)) {
                (false, 1..) => json!(known[self.u.below(known.len())]),
                _ => json!(rand_string(&mut self.u)),
            };
        }
        o
    }

    fn stream(&mut self) -> &'static str {
        self.u.pick(&["s1", "s2", "s1", "s2", "s3", "bad/name"])
    }

    fn new_record(&mut self, enc: Encoding) -> Json {
        let mut r = match self.u.below(16) {
            0..=6 => json!({"value": self.value(enc)}),
            7..=12 => json!({"json": self.u.pick(&[json!({"a": 1, "k": "v"}), json!({"a": 2}), json!("x"),
                json!([1, "a"]), Json::Null, json!({"a": 1.5, "n": {"x": true}}), json!({"a": null, "k": "w"})])}),
            13 => json!({"value": "v", "json": 1}),
            14 => json!({}),
            _ => json!({"value": enc_str(enc, &vec![b'z'; 9_000])}),
        };
        if self.u.chance(60) {
            r["headers"] = json!({"h": self.u.pick(&["v", "w", ""])});
        }
        r
    }

    fn stream_filter(&mut self) -> Json {
        match self.u.below(12) {
            0 => {
                json!({"field": "a", "op": self.u.pick(&["eq", "ne", "lt", "ge", "in"]), "value": self.u.pick(&[json!(1), json!(1.5), json!([1, 2]), json!("x")])})
            }
            1 => json!({"field": "n.x", "op": "exists"}),
            2 => json!({"header": "h", "op": self.u.pick(&["eq", "ne"]), "value": self.u.pick(&["v", "w"])}),
            3 => json!({"op": "contains", "value": self.u.pick(&["v", "a\"", "1"])}),
            4 => json!({"not": {"field": "k", "op": "prefix", "value": "v"}}),
            5 => json!({"and": [{"field": "a", "op": "gt", "value": 0}, {"header": "h", "op": "ne", "value": "w"}]}),
            6 => json!({"field": "a", "header": "h", "op": "eq", "value": 1}),
            7 => json!({"op": "eq", "value": 1}),
            8 => json!({"and": [{"field": "a", "op": "exists"}], "or": [{"header": "h", "op": "exists"}]}),
            9 => json!({"not": {"field": "a", "op": "exists"}, "field": "b", "op": "exists"}),
            10 => json!({"and": [], "bogus": 1}),
            _ => json!({"bogus": 1}),
        }
    }

    fn stream_op(&mut self, enc: Encoding) -> Json {
        let stream = self.stream();
        let next = self.w.snap.streams.get(stream).map_or(0, |s| s.next());
        match self.u.below(8) {
            0..=2 => {
                let n = self.u.below(4).max(if self.u.chance(240) { 1 } else { 0 });
                let records: Vec<Json> = (0..n).map(|_| self.new_record(enc)).collect();
                let mut o = json!({"op": "append", "stream": stream, "records": records});
                if self.u.chance(60) {
                    o["expect_seq"] = json!(next + self.u.pick(&[0, 0, 0, 1]));
                }
                if self.u.chance(40) {
                    o["create"] = json!(true);
                }
                o
            }
            3 | 4 => {
                let mut o = json!({"op": "read_stream", "stream": stream});
                match self.u.below(8) {
                    0 | 1 => o["from_seq"] = json!(self.u.below(8)),
                    2 => o["tail"] = json!(self.u.below(5)),
                    3 => o["consumer"] = json!(self.u.pick(&["c1", "c2"])),
                    4 if self.u.chance(16) => {
                        o["from_seq"] = json!(1);
                        o["tail"] = json!(1);
                    }
                    _ => {}
                }
                if self.u.chance(50) {
                    o["until_seq"] = json!(self.u.below(8));
                }
                if self.u.chance(80) {
                    o["reverse"] = json!(true);
                }
                if let Some(l) = self.limit() {
                    o["limit"] = l;
                }
                if self.u.chance(40) {
                    o["max_bytes"] = json!(self.u.pick(&[0, 1, 40, 200]));
                }
                if self.u.chance(60) {
                    o["filter"] = self.stream_filter();
                }
                if self.u.chance(40) {
                    o["scan_limit"] = json!(self.u.pick(&[0, 1, 3, 100_001]));
                }
                o
            }
            5 => json!({"op": "stream_info", "stream": stream}),
            6 => {
                let mut o = json!({"op": "trim_stream", "stream": stream});
                match self.u.below(8) {
                    0 => {}
                    1 => {
                        o["before_seq"] = json!(1);
                        o["before_time"] = json!(1);
                    }
                    _ => o["before_seq"] = json!(next.saturating_sub(self.u.below(4) as u64)),
                }
                o
            }
            _ => {
                let consumer = self.u.pick(&["c1", "c2", "bad/c"]);
                match self.u.chance(128) {
                    true => json!({"op": "get_consumer", "stream": stream, "consumer": consumer}),
                    false => {
                        json!({"op": "set_consumer", "stream": stream, "consumer": consumer, "seq": self.u.below(6)})
                    }
                }
            }
        }
    }

    fn op(&mut self, enc: Encoding) -> Json {
        let mut o = match self.u.below(24) {
            0 | 1 => json!({"op": "get", "key": self.key(enc)}),
            2 => self.scan(enc),
            3 | 4 => {
                let mut o = json!({"op": "put", "key": self.key(enc)});
                match self.u.below(24) {
                    23 => {}
                    22 => {
                        o["value"] = json!(self.value(enc));
                        o["blob_id"] = json!("0".repeat(32));
                    }
                    18..=21 => {
                        o["blob_id"] = match self.w.staged.keys().next() {
                            Some(_) if self.u.chance(200) => {
                                let ids: Vec<&String> = self.w.staged.keys().collect();
                                json!(ids[self.u.below(ids.len())])
                            }
                            _ => json!(self.u.pick(&["0".repeat(32), "zz".into()])),
                        }
                    }
                    _ => o["value"] = json!(self.value(enc)),
                }
                self.ttl(&mut o);
                o
            }
            5 if self.u.chance(128) => {
                let mut o = json!({"op": "copy", "from": self.key(enc), "to": self.key(enc)});
                self.ttl(&mut o);
                o
            }
            5 => json!({"op": "delete", "key": self.key(enc)}),
            6 => {
                let mut o = json!({"op": "delete_range"});
                match self.u.below(8) {
                    7 => {}
                    6 => {
                        o["start"] = json!(self.key(enc));
                        o["end"] = json!(self.key(enc));
                    }
                    _ => o["prefix"] = json!(self.prefix(enc)),
                }
                o
            }
            7 => self.check(enc),
            8 => {
                let mut o = json!({"op": "increment", "key": self.key(enc)});
                match self.u.below(8) {
                    0 => {}
                    1 => o["by"] = json!(i64::MAX),
                    2 => o["by"] = json!(i64::MIN),
                    _ => o["by"] = json!(self.u.below(9) as i64 - 4),
                }
                self.ttl(&mut o);
                o
            }
            9 => json!({"op": "get_row", "table": self.table(), "pk": self.pk()}),
            10 | 11 => {
                let op = if self.u.chance(128) { "insert_row" } else { "upsert_row" };
                let mut o = json!({"op": op, "table": self.table(), "row": self.row_fields(true)});
                self.ttl(&mut o);
                o
            }
            12 => {
                let mut o =
                    json!({"op": "update_row", "table": self.table(), "pk": self.pk(), "set": self.row_fields(false)});
                self.ttl(&mut o);
                o
            }
            13 => json!({"op": "delete_row", "table": self.table(), "pk": self.pk()}),
            14 => {
                let mut o = json!({"op": "update_rows", "table": self.table(), "set": self.row_fields(false)});
                self.selection(&mut o);
                self.ttl(&mut o);
                o
            }
            15 => {
                let mut o = json!({"op": "delete_rows", "table": self.table()});
                self.selection(&mut o);
                o
            }
            19..=23 => self.stream_op(enc),
            _ => self.query(),
        };
        if self.u.chance(6) {
            mutate(&mut self.u, &mut o, 0);
        }
        o
    }

    fn ops(&mut self, enc: Encoding) -> Vec<Json> {
        (0..self.u.below(6)).map(|_| self.op(enc)).collect()
    }
}

struct OpenTxn {
    id: String,
    view: TxnView,
    iso: Iso,
    enc: Encoding,
    alive: bool,
}

enum Expect {
    Ok(u16, Json),
    Err(Fail, Option<usize>),
    Any4xx,
    Unchecked,
}

struct Session<'a> {
    sim: Arc<Sim>,
    token: String,
    db: String,
    u: Choices<'a>,
    w: World,
    txns: Vec<OpenTxn>,
    scan_cursors: Vec<String>,
    step: usize,
    lost: bool,
    hist: RefCell<BTreeMap<String, u64>>,
}

struct Reply {
    status: u16,
    json: Json,
}

fn trunc(s: &str) -> String {
    if s.len() > 2000 { format!("{}…", &s[..s.floor_char_boundary(2000)]) } else { s.to_string() }
}

impl<'a> Session<'a> {
    fn with_gen<T>(&mut self, f: impl FnOnce(&mut Gen<'a, '_>) -> T) -> T {
        let mut g =
            Gen { u: Choices { data: self.u.data, pos: self.u.pos }, w: &self.w, scan_cursors: &self.scan_cursors };
        let out = f(&mut g);
        let pos = g.u.pos;
        self.u.pos = pos;
        out
    }

    async fn call(&self, method: &str, path: &str, headers: &[(&str, &str)], body: Vec<u8>) -> Result<Reply, String> {
        let mut b = http::Request::builder()
            .method(method)
            .uri(path)
            .header("authorization", format!("Bearer {}", self.token))
            .header("content-length", body.len().to_string());
        for (k, v) in headers {
            b = b.header(*k, *v);
        }
        let req = b.body(axum::body::Body::from(body.clone())).map_err(|e| e.to_string())?;
        let ctx =
            || format!("step {}: {method} {path}\nrequest: {}", self.step, trunc(&String::from_utf8_lossy(&body)));
        let resp: SimResponse = self
            .sim
            .call_node(0, req)
            .await
            .map_err(|e| format!("{}\nno response ({e:?}): did the handler panic?", ctx()))?;
        let text = String::from_utf8_lossy(&resp.body).into_owned();
        if resp.status >= 500 {
            return Err(format!("{}\nserver error {}: {}", ctx(), resp.status, trunc(&text)));
        }
        openapi_check::check_response(method, path, &resp)
            .map_err(|e| format!("{}\nOpenAPI violation: {e}\nresponse: {}", ctx(), trunc(&text)))?;
        let kind = openapi_check::match_path(openapi_check::doc(), path).map(|m| m.0).unwrap_or(path);
        *self.hist.borrow_mut().entry(format!("{method} {kind} {}", resp.status)).or_default() += 1;
        let json: Json = serde_json::from_slice(&resp.body).unwrap_or(Json::Null);
        if std::env::var("FUZZ_DEBUG").is_ok() && resp.status >= 400 {
            let m = json["error"]["message"].as_str().unwrap_or("").chars().take(40).collect::<String>();
            *self.hist.borrow_mut().entry(format!("  {} {m}", resp.status)).or_default() += 1;
        }
        Ok(Reply { status: resp.status, json })
    }

    async fn expect(&self, method: &str, path: &str, body: Vec<u8>, want: &Expect) -> Result<Reply, String> {
        let r = self.call(method, path, &[("content-type", "application/json")], body.clone()).await?;
        let ok = match want {
            Expect::Ok(st, j) => r.status == *st && matches(j, &r.json),
            Expect::Err(f, idx) => {
                let (st, code) = f.status();
                r.status == st
                    && r.json["error"]["code"] == code
                    && idx.is_none_or(|i| r.json["error"]["details"]["op_index"] == i)
            }
            Expect::Any4xx => (400..500).contains(&r.status),
            Expect::Unchecked => r.status < 500,
        };
        if !ok {
            let want = match want {
                Expect::Ok(st, j) => format!("{st} {j}"),
                Expect::Err(f, i) => format!("{:?} (op {i:?})", f.status()),
                Expect::Any4xx => "any 4xx".into(),
                Expect::Unchecked => "anything but a 5xx".into(),
            };
            return Err(format!(
                "step {}: {method} {path}\nrequest: {}\nexpected: {}\ngot: {} {}",
                self.step,
                trunc(&String::from_utf8_lossy(&body)),
                trunc(&want),
                r.status,
                trunc(&r.json.to_string())
            ));
        }
        Ok(r)
    }

    fn path(&self, rest: &str) -> String {
        format!("/v1/dbs/{}{rest}", self.db)
    }

    fn body_bytes(&mut self, body: &Json) -> Vec<u8> {
        let s = body.to_string();
        match self.u.below(128) {
            127 => s.as_bytes()[..s.floor_char_boundary(s.len() / 2)].to_vec(),
            126 => b"   ".to_vec(),
            125 => vec![0xff, b'{'],
            _ => s.into_bytes(),
        }
    }

    fn learn_cursors(&mut self, outs: &[Out], results: &Json) {
        for (i, o) in outs.iter().enumerate() {
            let next = results[i]["next_cursor"].as_str().map(|s| s.to_string());
            match (&o.cursor, next) {
                (Some(c), Some(s)) => {
                    self.w.query_cursors.insert(s, c.clone());
                }
                (None, Some(s)) if results[i].get("items").is_some() && !self.scan_cursors.contains(&s) => {
                    self.scan_cursors.push(s);
                }
                _ => {}
            }
        }
    }

    async fn one_shot(&mut self) -> Result<(), String> {
        let enc = if self.u.chance(80) { Encoding::Base64 } else { Encoding::Utf8 };
        let ops = self.with_gen(|g| g.ops(enc));
        let mut body = json!({"ops": ops});
        if enc == Encoding::Base64 || self.u.chance(20) {
            body["encoding"] = json!(if enc == Encoding::Base64 { "base64" } else { "utf8" });
        }
        if self.u.chance(50) {
            body["isolation"] = json!(if self.u.chance(128) { "snapshot" } else { "serializable" });
        }
        if self.u.chance(90) {
            body["idempotency_key"] = json!(if self.u.chance(16) { "" } else { self.u.pick(&["k1", "k2", "k3"]) });
        }
        if self.u.chance(40) {
            body["consistency"] =
                json!(if self.u.chance(32) { "eventual" } else { self.u.pick(&["strong", "snapshot"]) });
            if self.u.chance(128) {
                body["max_staleness_ms"] = json!(self.u.pick(&[json!(0), json!(5000), json!(-1)]));
            }
        }
        if self.u.chance(10) {
            mutate(&mut self.u, &mut body, 0);
        }
        self.submit_one_shot(&body).await
    }

    async fn submit_one_shot(&mut self, body: &Json) -> Result<(), String> {
        let bytes = self.body_bytes(body);
        let path = self.path("/txn");
        let Some(req) = parse_body::<OneShotBody>(&bytes).filter(|r| !r.ops.iter().any(Op::unparseable)) else {
            self.expect("POST", &path, bytes, &Expect::Any4xx).await?;
            return Ok(());
        };
        let writes = req.ops.iter().any(|o| o.is_write());
        let snapshot = match req.consistency.as_deref() {
            None | Some("strong") => false,
            Some("snapshot") => true,
            Some(_) => {
                self.expect("POST", &path, bytes, &Expect::Err(Fail::Invalid, None)).await?;
                return Ok(());
            }
        };
        if writes && snapshot {
            self.expect("POST", &path, bytes, &Expect::Err(Fail::Invalid, None)).await?;
            return Ok(());
        }
        // A transaction without writes ignores its idempotency key.
        let key = req.idempotency_key.as_deref().filter(|_| writes);
        if let Some(k) = key {
            if k.is_empty() || k.len() > 200 {
                self.expect("POST", &path, bytes, &Expect::Err(Fail::Invalid, None)).await?;
                return Ok(());
            }
            if let Some((v, appends)) = self.w.idem.get(k) {
                let want = json!({"version": v, "committed": true, "replayed": true, "commit_id": ANY,
                    "results": null, "appends": appends});
                self.expect("POST", &path, bytes, &Expect::Ok(200, want)).await?;
                return Ok(());
            }
            if self.w.aborted.contains(k) {
                self.expect("POST", &path, bytes, &Expect::Err(Fail::Aborted, None)).await?;
                return Ok(());
            }
        }
        let mut t = TxnView::new(self.w.snap.clone(), self.w.version);
        let mut outs = Vec::new();
        for (i, op) in req.ops.iter().enumerate() {
            t.op_index = i;
            match self.w.exec(&mut t, req.encoding, op) {
                Ok(o) => outs.push(o),
                Err(Fail::Unmodeled) => {
                    self.lost = true;
                    self.expect("POST", &path, bytes, &Expect::Unchecked).await?;
                    return Ok(());
                }
                Err(f) => {
                    let idx = (f == Fail::Precondition).then_some(i);
                    self.expect("POST", &path, bytes, &Expect::Err(f, idx)).await?;
                    if f == Fail::Precondition {
                        self.w.abort(key);
                    }
                    return Ok(());
                }
            }
        }
        let mut results: Vec<Json> = outs.iter().map(|o| o.json.clone()).collect();
        let r = if !t.is_write() {
            let want = json!({"version": self.w.version, "committed": false, "results": results});
            self.expect("POST", &path, bytes, &Expect::Ok(200, want)).await?
        } else {
            let firsts = match self.w.stream_outcome(&t) {
                Ok(f) => f,
                Err(i) => {
                    self.expect("POST", &path, bytes, &Expect::Err(Fail::Precondition, Some(i))).await?;
                    self.w.abort(key);
                    return Ok(());
                }
            };
            let appended = req.ops.iter().zip(results.iter_mut()).filter(|(o, _)| matches!(o, Op::Append { .. }));
            for ((_, r), f) in appended.zip(&firsts) {
                r["first_seq"] = json!(f);
            }
            let v = self.w.version + 1;
            let want =
                json!({"version": v, "committed": true, "commit_id": ANY, "results": results, "appends": firsts});
            let r = self.expect("POST", &path, bytes, &Expect::Ok(200, want)).await?;
            self.w.apply(t, v).map_err(|e| format!("step {}: {e}", self.step))?;
            if let Some(k) = req.idempotency_key {
                self.w.idem.insert(k, (v, firsts));
            }
            r
        };
        self.learn_cursors(&outs, &r.json["results"]);
        Ok(())
    }

    /// Executes `ops` against the model `t`. Returns `Ok(None)` if an operation failed and the
    /// HTTP error response was already verified.
    async fn exec_ops(
        &mut self,
        t: &mut TxnView,
        enc: Encoding,
        ops: &[Op],
        path: &str,
        bytes: &[u8],
    ) -> Result<Option<Vec<Out>>, String> {
        let mut outs = Vec::new();
        for (i, op) in ops.iter().enumerate() {
            t.op_index = i;
            match self.w.exec(t, enc, op) {
                Ok(o) => outs.push(o),
                Err(Fail::Unmodeled) => {
                    self.lost = true;
                    self.expect("POST", path, bytes.to_vec(), &Expect::Unchecked).await?;
                    return Ok(None);
                }
                Err(f) => {
                    let idx = (f == Fail::Precondition).then_some(i);
                    self.expect("POST", path, bytes.to_vec(), &Expect::Err(f, idx)).await?;
                    return Ok(None);
                }
            }
        }
        Ok(Some(outs))
    }

    async fn begin(&mut self) -> Result<(), String> {
        let mut body = json!({});
        if self.u.chance(100) {
            body["isolation"] = json!(if self.u.chance(128) { "snapshot" } else { "serializable" });
        }
        let enc = if self.u.chance(80) {
            body["encoding"] = json!("base64");
            Encoding::Base64
        } else {
            Encoding::Utf8
        };
        if self.u.chance(3) {
            body["ops"] = json!(self.with_gen(|g| g.ops(enc)));
        }
        if self.u.chance(10) {
            mutate(&mut self.u, &mut body, 0);
        }
        let bytes = self.body_bytes(&body);
        let path = self.path("/transactions");
        let Some(b) = parse_body::<BeginBody>(&bytes).filter(|b| !b.ops.iter().any(Op::unparseable)) else {
            self.expect("POST", &path, bytes, &Expect::Any4xx).await?;
            return Ok(());
        };
        let mut view = TxnView::new(self.w.snap.clone(), self.w.version);
        let Some(outs) = self.exec_ops(&mut view, b.encoding, &b.ops, &path, &bytes).await? else {
            return Ok(());
        };
        let results: Vec<Json> = outs.iter().map(|o| o.json.clone()).collect();
        let want = json!({"txn_id": ANY, "commit_id": ANY, "read_version": self.w.version,
            "expires_at": ANY_NUMBER, "idle_timeout_ms": ANY_NUMBER, "results": results});
        let r = self.expect("POST", &path, bytes, &Expect::Ok(201, want)).await?;
        self.learn_cursors(&outs, &r.json["results"]);
        self.txns.push(OpenTxn {
            id: r.json["txn_id"].as_str().unwrap_or_default().to_string(),
            view,
            iso: b.isolation,
            enc: b.encoding,
            alive: true,
        });
        Ok(())
    }

    fn pick_txn(&mut self) -> Option<usize> {
        if self.txns.is_empty() || self.u.chance(4) {
            return None;
        }
        let alive: Vec<usize> = (0..self.txns.len()).filter(|i| self.txns[*i].alive).collect();
        if alive.is_empty() || self.u.chance(24) {
            return Some(self.u.below(self.txns.len()));
        }
        Some(alive[self.u.below(alive.len())])
    }

    async fn txn_ops(&mut self) -> Result<(), String> {
        let ti = self.pick_txn();
        let enc = ti.map(|i| self.txns[i].enc).unwrap_or_default();
        let ops = self.with_gen(|g| g.ops(enc));
        let mut body = json!({"ops": ops});
        if self.u.chance(10) {
            mutate(&mut self.u, &mut body, 0);
        }
        let bytes = self.body_bytes(&body);
        let id = ti.map(|i| self.txns[i].id.clone()).unwrap_or_else(|| "0123456789abcdef0123456789abcdef".into());
        let path = self.path(&format!("/transactions/{id}/ops"));
        let Some(req) = parse_body::<OpsBody>(&bytes).filter(|r| !r.ops.iter().any(Op::unparseable)) else {
            self.expect("POST", &path, bytes, &Expect::Any4xx).await?;
            return Ok(());
        };
        let Some(ti) = ti.filter(|i| self.txns[*i].alive) else {
            self.call_txn_not_found("POST", &path, bytes).await?;
            return Ok(());
        };
        let mut t = self.txns[ti].view.clone();
        let mut outs = Vec::new();
        for (i, op) in req.ops.iter().enumerate() {
            t.op_index = i;
            match self.w.exec(&mut t, enc, op) {
                Ok(o) => outs.push(o),
                Err(Fail::Unmodeled) => {
                    self.lost = true;
                    self.expect("POST", &path, bytes, &Expect::Unchecked).await?;
                    return Ok(());
                }
                Err(f) => {
                    self.txns[ti].alive = false;
                    let idx = (f == Fail::Precondition).then_some(i);
                    self.expect("POST", &path, bytes, &Expect::Err(f, idx)).await?;
                    return Ok(());
                }
            }
        }
        let results: Vec<Json> = outs.iter().map(|o| o.json.clone()).collect();
        let r = self.expect("POST", &path, bytes, &Expect::Ok(200, json!({"results": results}))).await?;
        self.learn_cursors(&outs, &r.json["results"]);
        self.txns[ti].view = t;
        Ok(())
    }

    async fn call_txn_not_found(&self, method: &str, path: &str, body: Vec<u8>) -> Result<(), String> {
        let r = self.call(method, path, &[("content-type", "application/json")], body).await?;
        if r.status != 404 || r.json["error"]["code"] != "txn_not_found" {
            return Err(format!(
                "step {}: {method} {path}: expected 404 txn_not_found, got {} {}",
                self.step, r.status, r.json
            ));
        }
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), String> {
        let Some(ti) = self.pick_txn() else { return Ok(()) };
        let path = self.path(&format!("/transactions/{}/commit", self.txns[ti].id));
        let enc = self.txns[ti].enc;
        let mut body = json!({});
        if self.u.chance(3) {
            body["ops"] = json!(self.with_gen(|g| g.ops(enc)));
        }
        if self.u.chance(10) {
            mutate(&mut self.u, &mut body, 0);
        }
        let bytes = self.body_bytes(&body);
        let parsed =
            if bytes.trim_ascii() == b"null" { Some(CommitBody::default()) } else { parse_body::<CommitBody>(&bytes) };
        let Some(req) = parsed.filter(|b| !b.ops.iter().any(Op::unparseable)) else {
            self.expect("POST", &path, bytes, &Expect::Any4xx).await?;
            return Ok(());
        };
        if !self.txns[ti].alive {
            return self.call_txn_not_found("POST", &path, bytes).await;
        }
        self.txns[ti].alive = false;
        let mut t = self.txns[ti].view.clone();
        let iso = self.txns[ti].iso;
        let Some(outs) = self.exec_ops(&mut t, enc, &req.ops, &path, &bytes).await? else {
            return Ok(());
        };
        let mut results: Vec<Json> = outs.iter().map(|o| o.json.clone()).collect();
        if !t.is_write() {
            let want = json!({"version": t.rv, "commit_id": ANY, "appends": [], "results": results});
            let r = self.expect("POST", &path, bytes, &Expect::Ok(200, want)).await?;
            self.learn_cursors(&outs, &r.json["results"]);
            return Ok(());
        }
        let v = self.w.version + 1;
        let outcome = self.w.stream_outcome(&t);
        let firsts = outcome.clone().unwrap_or_default();
        let own = req.ops.iter().filter(|o| matches!(o, Op::Append { .. })).count();
        let appended = req.ops.iter().zip(results.iter_mut()).filter(|(o, _)| matches!(o, Op::Append { .. }));
        for ((_, r), f) in appended.zip(&firsts[firsts.len().saturating_sub(own)..]) {
            r["first_seq"] = json!(f);
        }
        let committed = json!({"version": v, "commit_id": ANY, "appends": firsts, "results": results});
        match self.w.conflicts(&t, iso) {
            Some(false) => {
                if let Err(i) = outcome {
                    let want = Expect::Err(Fail::Precondition, Some(i));
                    self.expect("POST", &path, bytes, &want).await?;
                    return Ok(());
                }
                let r = self.expect("POST", &path, bytes, &Expect::Ok(200, committed)).await?;
                self.learn_cursors(&outs, &r.json["results"]);
            }
            Some(true) => {
                let r = self.call("POST", &path, &[], bytes).await?;
                if r.status != 409 || r.json["error"]["code"] != "conflict" {
                    return Err(format!("step {}: commit should conflict, got {} {}", self.step, r.status, r.json));
                }
                return Ok(());
            }
            None => {
                let r = self.call("POST", &path, &[], bytes).await?;
                if r.status == 409 && r.json["error"]["code"] == "conflict" {
                    return Ok(());
                }
                if let Err(i) = outcome {
                    if r.status != 412 || r.json["error"]["details"]["op_index"] != i {
                        return Err(format!(
                            "step {}: commit: expected 412 at op {i}, got {} {}",
                            self.step, r.status, r.json
                        ));
                    }
                    return Ok(());
                }
                if r.status != 200 || !matches(&committed, &r.json) {
                    return Err(format!(
                        "step {}: commit: expected {committed} or a conflict, got {} {}",
                        self.step, r.status, r.json
                    ));
                }
            }
        }
        self.w.apply(t, v).map_err(|e| format!("step {}: {e}", self.step))
    }

    async fn abort(&mut self) -> Result<(), String> {
        let Some(ti) = self.pick_txn() else { return Ok(()) };
        let path = self.path(&format!("/transactions/{}", self.txns[ti].id));
        if !self.txns[ti].alive {
            return self.call_txn_not_found("DELETE", &path, Vec::new()).await;
        }
        self.txns[ti].alive = false;
        self.expect("DELETE", &path, Vec::new(), &Expect::Ok(200, json!({}))).await?;
        Ok(())
    }

    async fn stage_blob(&mut self) -> Result<(), String> {
        let n = self.u.pick(&[0usize, 3, 700, 1500]);
        let data: Vec<u8> = (0..n).map(|i| (i * 7 + self.u.below(3)) as u8).collect();
        let ct = if self.u.chance(128) { Some("image/png") } else { None };
        let headers: Vec<(&str, &str)> = ct.iter().map(|c| ("content-type", *c)).collect();
        let r = self.call("POST", &self.path("/blobs"), &headers, data.clone()).await?;
        let ct = ct.unwrap_or("application/octet-stream").to_string();
        let want = json!({"blob_id": ANY, "size": n, "sha256": b64::hex(&sha256(&data)), "content_type": ct});
        if r.status != 201 || !matches(&want, &r.json) {
            return Err(format!("step {}: stage blob: expected 201 {want}, got {} {}", self.step, r.status, r.json));
        }
        let id = r.json["blob_id"].as_str().unwrap_or_default().to_string();
        self.w.staged.insert(id, (data, ct));
        Ok(())
    }

    async fn full_check(&mut self) -> Result<(), String> {
        let mut t = TxnView::new(self.w.snap.clone(), self.w.version);
        let items = t.scan(&KeyRange::all(), false, usize::MAX);
        let want = json!({"items": self.w.scan_items(Encoding::Base64, &items, false), "next_cursor": null,
            "version": self.w.version});
        let path = self.path("/kv?encoding=base64&limit=1000");
        let r = self.call("GET", &path, &[], Vec::new()).await?;
        if r.status != 200 || !matches(&want, &r.json) {
            return Err(format!("step {}: full KV check:\nwant {want}\n got {} {}", self.step, r.status, r.json));
        }
        for (name, st) in &self.w.snap.streams {
            let records: Vec<Json> = (st.first..st.next())
                .map(|seq| stream_record_json(Encoding::Base64, seq, &st.records[seq as usize]))
                .collect();
            let want = json!({"records": records, "next_seq": st.next(), "first_seq": st.first, "tail_seq": st.next(),
                "done": true, "version": self.w.version});
            let path = self.path(&format!("/streams/{name}/records?limit=1000&encoding=base64"));
            let r = self.call("GET", &path, &[], Vec::new()).await?;
            if r.status != 200 || !matches(&want, &r.json) {
                return Err(format!(
                    "step {}: full {name} check:\nwant {want}\n got {} {}",
                    self.step, r.status, r.json
                ));
            }
        }
        let rows: Vec<Json> = self.w.snap.rows.iter().map(|(pk, r)| row_json(*pk, r)).collect();
        let want = json!({"rows": rows, "next_cursor": null, "version": self.w.version});
        let path = self.path("/tables/t/rows/query");
        let r = self.call("POST", &path, &[("content-type", "application/json")], b"{\"limit\":1000}".to_vec()).await?;
        if r.status != 200 || !matches(&want, &r.json) {
            return Err(format!("step {}: full row check:\nwant {want}\n got {} {}", self.step, r.status, r.json));
        }
        Ok(())
    }

    async fn setup(&mut self) -> Result<(), String> {
        let r = self.call("POST", "/v1/dbs", &[], json!({"name": self.db}).to_string().into_bytes()).await?;
        if r.status != 201 {
            return Err(format!("create database: {} {}", r.status, r.json));
        }
        let table = json!({"name": "t", "primary_key": ["id"], "columns": [
            {"name": "id", "type": "int", "nullable": false},
            {"name": "name", "type": "string"},
            {"name": "grp", "type": "int"},
            {"name": "doc", "type": "json"}
        ], "indexes": [
            {"name": "by_name", "columns": ["name"], "unique": true},
            {"name": "by_grp", "columns": ["grp"]}
        ]});
        let r = self.call("POST", &self.path("/tables"), &[], table.to_string().into_bytes()).await?;
        if r.status != 201 {
            return Err(format!("create table: {} {}", r.status, r.json));
        }
        self.w.version = r.json["version"].as_u64().ok_or("create table: no version")?;
        for name in ["s1", "s2"] {
            let r =
                self.call("POST", &self.path("/streams"), &[], json!({"name": name}).to_string().into_bytes()).await?;
            if r.status != 201 {
                return Err(format!("create stream: {} {}", r.status, r.json));
            }
            self.w.version = r.json["version"].as_u64().ok_or("create stream: no version")?;
            self.w.snap.streams.insert(name.into(), MStream::default());
            let log = self.w.stream_log.entry(name.into()).or_default();
            (log.catalog, log.state) = (self.w.version, self.w.version);
        }
        self.seed_rows().await
    }

    async fn seed_rows(&mut self) -> Result<(), String> {
        let ops: Vec<Json> = (0..self.u.below(NAMES.len() + 1) as i64)
            .map(|id| {
                let name = if self.u.chance(60) { Json::Null } else { json!(NAMES[id as usize]) };
                let grp = if self.u.chance(60) { Json::Null } else { json!(self.u.below(3) as i64) };
                let doc = self.u.pick(&[json!({"k": true}), json!("s"), json!(7), Json::Null]);
                json!({"op": "insert_row", "table": "t", "row": {"id": id, "name": name, "grp": grp, "doc": doc}})
            })
            .collect();
        if ops.is_empty() {
            return Ok(());
        }
        self.submit_one_shot(&json!({"ops": ops})).await
    }

    async fn run(&mut self) -> Result<(), String> {
        self.setup().await?;
        while !self.u.exhausted() && self.step < 48 && !self.lost {
            self.step += 1;
            let alive = self.txns.iter().filter(|t| t.alive).count();
            match self.u.below(20) {
                0..=9 => self.one_shot().await?,
                10 if alive < 3 => self.begin().await?,
                11..=16 if alive == 0 && self.u.below(8) != 0 => self.begin().await?,
                11..=13 => self.txn_ops().await?,
                14 | 15 => self.commit().await?,
                16 => self.abort().await?,
                17 => self.stage_blob().await?,
                _ => self.full_check().await?,
            }
        }
        if !self.lost {
            self.full_check().await?;
        }
        for t in self.txns.iter().filter(|t| t.alive) {
            self.call("DELETE", &self.path(&format!("/transactions/{}", t.id)), &[], Vec::new()).await?;
        }
        let r = self.call("DELETE", &self.path(""), &[], Vec::new()).await?;
        if r.status != 200 {
            return Err(format!("delete database: {} {}", r.status, r.json));
        }
        Ok(())
    }
}

/// Single-node fuzz harness reused across inputs.
pub struct Harness {
    rt: tokio::runtime::Runtime,
    sim: Arc<Sim>,
    token: String,
    inputs: u64,
    hist: BTreeMap<String, u64>,
}

impl Harness {
    pub fn new(seed: u64) -> Harness {
        let rt =
            tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().expect("runtime");
        let (sim, token) = rt.block_on(async {
            let sim = Sim::new(seed, SimConfig::basic()).await;
            let body = json!({"email": "fuzz@txn.test", "password": "password-fz"}).to_string();
            let req = http::Request::post("/v1/auth/signup")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .expect("request");
            let r = sim.call_node(0, req).await.expect("signup response");
            let j: Json = serde_json::from_slice(&r.body).expect("signup JSON");
            let token = j["token"].as_str().expect("session token").to_string();
            (sim, token)
        });
        Harness { rt, sim, token, inputs: 0, hist: BTreeMap::new() }
    }

    pub fn run(&mut self, data: &[u8]) -> Result<(), String> {
        self.inputs += 1;
        let cfg = &self.sim.cfg;
        let w = World {
            snap: Snapshot::default(),
            version: 0,
            last_write: BTreeMap::new(),
            idem: BTreeMap::new(),
            aborted: BTreeSet::new(),
            staged: BTreeMap::new(),
            query_cursors: BTreeMap::new(),
            inline_max: cfg.engine.value_inline_max,
            json_max: cfg.node.json_value_max,
            record_max: cfg.engine.stream_record_max,
            stream_log: BTreeMap::new(),
        };
        let mut s = Session {
            sim: self.sim.clone(),
            token: self.token.clone(),
            db: format!("fz{}", self.inputs),
            u: Choices::new(data),
            w,
            txns: Vec::new(),
            scan_cursors: Vec::new(),
            step: 0,
            lost: false,
            hist: RefCell::new(BTreeMap::new()),
        };
        let r = self.rt.block_on(s.run());
        if self.inputs.is_multiple_of(256) {
            self.rt.block_on(self.sim.sleep(super::purge_wait_ms(cfg)));
        }
        for (k, n) in s.hist.into_inner() {
            *self.hist.entry(k).or_default() += n;
        }
        r
    }

    pub fn stats(&self) -> &BTreeMap<String, u64> {
        &self.hist
    }
}

thread_local! {
    static HARNESS: RefCell<Option<Harness>> = const { RefCell::new(None) };
}

pub fn run_input(data: &[u8]) -> Result<(), String> {
    HARNESS.with(|h| h.borrow_mut().get_or_insert_with(|| Harness::new(1)).run(data))
}

pub fn stats() -> BTreeMap<String, u64> {
    HARNESS.with(|h| h.borrow().as_ref().map(|h| h.stats().clone()).unwrap_or_default())
}
