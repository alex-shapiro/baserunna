//! Streams in the engine (SPEC §5.11): the keys and encodings of stream
//! records and state, the committer's appends and trims, sealing runs of
//! records into chunks, and reads by seq, time and position.
//!
//! A stream's records are entries `RECORDS ++ id ++ seq`. Each value is a
//! record, or a reference to a sealed chunk keyed by the chunk's last seq.
//! A chunk is a segment holding one run of consecutive records, written
//! once and never rewritten. The engine owns each stream's state entry
//! (`STATE ++ id`); the layer above owns everything else about a stream.

use super::Shared;
use super::manifest::SegmentMeta;
use super::range_delete::RangeTombstone;
use super::segment::SegmentBuilder;
use super::types::{Entry, Seq, Value, valid_object_id};
use super::version::{IterSpec, KeyRange, MergeIter, ReadMode, Version};
use crate::error::{Error, Result};
use crate::util::codec::{DResult, DecodeError, Reader, Writer};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Key space of stream records and chunk references.
pub const RECORDS: u8 = 0x06;
/// Key space of the engine's per-stream state.
pub const STATE: u8 = 0x07;
/// Most records one transaction may append.
pub const APPEND_MAX_RECORDS: usize = 100_000;

pub fn record_key(id: u64, seq: u64) -> Bytes {
    let mut k = Vec::with_capacity(17);
    k.push(RECORDS);
    k.extend_from_slice(&id.to_be_bytes());
    k.extend_from_slice(&seq.to_be_bytes());
    Bytes::from(k)
}

/// The stream and seq of a record key.
pub fn parse_record_key(k: &[u8]) -> Option<(u64, u64)> {
    if k.len() != 17 || k[0] != RECORDS {
        return None;
    }
    let id = u64::from_be_bytes(k[1..9].try_into().ok()?);
    let seq = u64::from_be_bytes(k[9..17].try_into().ok()?);
    Some((id, seq))
}

fn prefix(space: u8, id: u64) -> Vec<u8> {
    let mut p = Vec::with_capacity(9);
    p.push(space);
    p.extend_from_slice(&id.to_be_bytes());
    p
}

/// Every record key of stream `id`.
pub fn records(id: u64) -> KeyRange {
    KeyRange::prefix(&prefix(RECORDS, id))
}

/// Record keys of stream `id` with seqs in `[lo, hi)`, or from `lo` on.
pub fn records_between(id: u64, lo: u64, hi: Option<u64>) -> KeyRange {
    match hi {
        Some(hi) => KeyRange::half_open(record_key(id, lo), Some(record_key(id, hi))),
        None => KeyRange::new(std::ops::Bound::Included(record_key(id, lo)), records(id).end),
    }
}

pub fn state_key(id: u64) -> Bytes {
    Bytes::from(prefix(STATE, id))
}

pub fn parse_state_key(k: &[u8]) -> Option<u64> {
    match k.len() == 9 && k[0] == STATE {
        true => Some(u64::from_be_bytes(k[1..9].try_into().ok()?)),
        false => None,
    }
}

const TAG_RECORD: u8 = 0;
const TAG_CHUNK: u8 = 1;
const STATE_FORMAT: u8 = 1;

/// A stream record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub seq: u64,
    /// The version time of the commit that appended it (SPEC §4.6).
    pub time_ms: u64,
    /// Payload bytes the stream held before this record.
    pub pos: u64,
    /// The bytes the layer above stores; their length is the record's size.
    pub payload: Bytes,
}

impl Record {
    fn encode_value(time_ms: u64, pos: u64, payload: &[u8]) -> Bytes {
        let mut w = Writer::with_capacity(payload.len() + 16);
        w.u8(TAG_RECORD);
        w.varint(time_ms);
        w.varint(pos);
        w.raw(payload);
        Bytes::from(w.into_inner())
    }
}

/// A sealed chunk, as its reference in the LSM describes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkRef {
    /// The chunk's segment id; the chunk is stored at `seg/{id}`.
    pub id: String,
    /// Stored size of the chunk object.
    pub size: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    pub first_pos: u64,
    /// Payload bytes of its records.
    pub bytes: u64,
    pub min_time: u64,
    pub max_time: u64,
}

impl ChunkRef {
    fn encode_value(&self) -> Bytes {
        let mut w = Writer::with_capacity(96);
        w.u8(TAG_CHUNK);
        w.str(&self.id);
        w.varint(self.size);
        w.varint(self.first_seq);
        w.varint(self.first_pos);
        w.varint(self.bytes);
        w.varint(self.min_time);
        w.varint(self.max_time);
        Bytes::from(w.into_inner())
    }

    /// The chunk as a segment of stream `stream`'s records, for reading.
    pub fn meta(&self, stream: u64) -> SegmentMeta {
        SegmentMeta {
            id: self.id.clone(),
            level: 0,
            size: self.size,
            entries: self.last_seq - self.first_seq + 1,
            tombstones: 0,
            min_key: record_key(stream, self.first_seq),
            max_key: record_key(stream, self.last_seq),
            min_seq: 0,
            max_seq: 0,
            deletes_applied: 0,
            expiring: 0,
            expires_max: 0,
        }
    }
}

/// What a record key holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stored {
    Record(Record),
    Chunk(ChunkRef),
}

impl Stored {
    /// Decodes the value of the record key with seq `seq`.
    pub fn decode(seq: u64, value: &Bytes) -> DResult<Stored> {
        let mut r = Reader::new(value);
        match r.u8()? {
            TAG_RECORD => {
                let time_ms = r.varint()?;
                let pos = r.varint()?;
                let payload = value.slice(value.len() - r.remaining()..);
                Ok(Stored::Record(Record { seq, time_ms, pos, payload }))
            }
            TAG_CHUNK => {
                let c = ChunkRef {
                    id: r.string()?,
                    size: r.varint()?,
                    first_seq: r.varint()?,
                    last_seq: seq,
                    first_pos: r.varint()?,
                    bytes: r.varint()?,
                    min_time: r.varint()?,
                    max_time: r.varint()?,
                };
                if !r.is_empty() {
                    return Err(DecodeError("chunk reference trailing data"));
                }
                if !valid_object_id(&c.id) || c.first_seq > c.last_seq || c.min_time > c.max_time {
                    return Err(DecodeError("bad chunk reference"));
                }
                c.first_pos.checked_add(c.bytes).ok_or(DecodeError("bad chunk reference"))?;
                Ok(Stored::Chunk(c))
            }
            _ => Err(DecodeError("bad stream value tag")),
        }
    }

    pub fn first_seq(&self) -> u64 {
        match self {
            Stored::Record(r) => r.seq,
            Stored::Chunk(c) => c.first_seq,
        }
    }
}

/// The stream and contents of a live entry in the records space, if `e` is one.
pub fn stored(e: &Entry) -> Option<(u64, Stored)> {
    let (id, seq) = parse_record_key(&e.key)?;
    match &e.value {
        Value::Inline(v) => Stored::decode(seq, v).ok().map(|s| (id, s)),
        _ => None,
    }
}

fn corrupt() -> Error {
    Error::internal("corrupt stream entry")
}

fn decode_entry(e: &Entry) -> Result<(u64, Stored)> {
    stored(e).ok_or_else(corrupt)
}

/// How long a stream keeps its records.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Retention {
    /// Records older than this may be trimmed.
    pub retention_ms: Option<u64>,
    /// The oldest records beyond this many bytes may be trimmed.
    pub max_bytes: Option<u64>,
}

impl Retention {
    /// The limits past which a trim is due: a sixteenth beyond these, so
    /// that each trim removes a share of the stream rather than the record
    /// or two each append pushes past them. Trims go back to these.
    pub fn due_at(&self) -> Retention {
        let slack = |limit: u64| limit.saturating_add(limit / 16);
        Retention { retention_ms: self.retention_ms.map(slack), max_bytes: self.max_bytes.map(slack) }
    }
}

/// Where a stream begins and ends; maintained by the committer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamState {
    pub next_seq: u64,
    pub next_pos: u64,
    /// The oldest seq not trimmed.
    pub first_seq: u64,
    pub first_pos: u64,
    /// The time of the record at `first_seq`, if there is one.
    pub first_time: u64,
    /// The time of the newest record.
    pub last_time: u64,
    pub retention: Retention,
}

impl StreamState {
    pub fn new(retention: Retention) -> StreamState {
        StreamState { next_seq: 0, next_pos: 0, first_seq: 0, first_pos: 0, first_time: 0, last_time: 0, retention }
    }

    /// Payload bytes of the records not trimmed.
    pub fn bytes(&self) -> u64 {
        self.next_pos - self.first_pos
    }

    pub fn records(&self) -> u64 {
        self.next_seq - self.first_seq
    }

    /// Whether retention calls for a trim at store time `now` (see
    /// [`Retention::due_at`]).
    pub fn trim_due(&self, now: u64) -> bool {
        let due = self.retention.due_at();
        let old = due.retention_ms.is_some_and(|r| self.records() > 0 && self.first_time.saturating_add(r) < now);
        old || due.max_bytes.is_some_and(|m| self.bytes() > m)
    }

    pub fn encode(&self) -> Bytes {
        let mut w = Writer::with_capacity(64);
        w.u8(STATE_FORMAT);
        for v in [self.next_seq, self.next_pos, self.first_seq, self.first_pos, self.first_time, self.last_time] {
            w.varint(v);
        }
        for o in [self.retention.retention_ms, self.retention.max_bytes] {
            match o {
                None => w.u8(0),
                Some(v) => {
                    w.u8(1);
                    w.varint(v);
                }
            }
        }
        Bytes::from(w.into_inner())
    }

    pub fn decode(b: &[u8]) -> DResult<StreamState> {
        let mut r = Reader::new(b);
        if r.u8()? != STATE_FORMAT {
            return Err(DecodeError("unsupported stream state format"));
        }
        let mut v = [0u64; 6];
        for x in &mut v {
            *x = r.varint()?;
        }
        let mut opt = || -> DResult<Option<u64>> {
            match r.u8()? {
                0 => Ok(None),
                1 => Ok(Some(r.varint()?)),
                _ => Err(DecodeError("bad stream retention")),
            }
        };
        let retention = Retention { retention_ms: opt()?, max_bytes: opt()? };
        if !r.is_empty() {
            return Err(DecodeError("stream state trailing data"));
        }
        let [next_seq, next_pos, first_seq, first_pos, first_time, last_time] = v;
        if first_seq > next_seq || first_pos > next_pos || first_time > last_time {
            return Err(DecodeError("inconsistent stream state"));
        }
        Ok(StreamState { next_seq, next_pos, first_seq, first_pos, first_time, last_time, retention })
    }
}

/// The stream and its state, if `e` is a live state entry.
pub fn state_of(e: &Entry) -> Option<(u64, StreamState)> {
    let id = parse_state_key(&e.key)?;
    match &e.value {
        Value::Inline(v) => StreamState::decode(v).ok().map(|s| (id, s)),
        _ => None,
    }
}

/// A change to a stream that only the committer can make, because it
/// depends on the stream's state at commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamOp {
    Create {
        id: u64,
        retention: Retention,
    },
    Configure {
        id: u64,
        retention: Retention,
    },
    /// Appends records with these payloads at the stream's next seqs; with
    /// `expect`, only if the next seq is that.
    Append {
        id: u64,
        payloads: Vec<Bytes>,
        expect: Option<u64>,
        /// Reported with a failed `expect`.
        op_index: Option<usize>,
    },
    /// Trims the records before `before`, or all of them. A trim for
    /// retention names the retention it was computed for, and does nothing
    /// if the stream's retention has changed since.
    Trim {
        id: u64,
        before: u64,
        retention: Option<Retention>,
    },
}

impl StreamOp {
    pub fn id(&self) -> u64 {
        match self {
            StreamOp::Create { id, .. }
            | StreamOp::Configure { id, .. }
            | StreamOp::Append { id, .. }
            | StreamOp::Trim { id, .. } => *id,
        }
    }

    /// Approximate bytes an append adds to the database.
    pub fn appended_bytes(&self) -> u64 {
        match self {
            StreamOp::Append { payloads, .. } => payloads.iter().map(|p| p.len() as u64 + 32).sum(),
            _ => 0,
        }
    }
}

/// The records one append of a commit received.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Appended {
    pub first_seq: u64,
    pub count: u64,
}

/// What one transaction's stream ops do, planned against the state before
/// it so that a transaction that fails changes nothing.
#[derive(Debug, Default)]
pub(crate) struct Effects {
    /// New records and states.
    pub entries: Vec<Entry>,
    /// Ranges trims delete, each with a range tombstone.
    pub trims: Vec<KeyRange>,
    /// The records each append received.
    pub appends: Vec<Appended>,
    /// Everything written, to validate later transactions of the batch.
    pub touched: Vec<KeyRange>,
    pub states: BTreeMap<u64, StreamState>,
}

/// Committed state as the committer sees it partway through a batch: the
/// base version, the batch's tail so far and its range deletions.
pub(crate) struct Pending<'a> {
    pub base: &'a Version,
    pub tail: &'a BTreeMap<Bytes, Entry>,
    pub batch_deletes: Vec<RangeTombstone>,
}

impl Pending<'_> {
    async fn entry(&self, k: &Bytes) -> Result<Option<Entry>> {
        if let Some(e) = self.tail.get(k) {
            return Ok(Some(e.clone()));
        }
        if self.batch_deletes.iter().any(|t| t.contains_key(k)) {
            return Ok(None);
        }
        self.base.get_entry(k).await
    }

    pub(crate) async fn state(&self, id: u64) -> Result<Option<StreamState>> {
        match self.entry(&state_key(id)).await? {
            None => Ok(None),
            Some(e) if e.value.is_tombstone() => Ok(None),
            Some(e) => Ok(Some(state_of(&e).ok_or_else(corrupt)?.1)),
        }
    }

    /// The record at `seq` of stream `id`, which must exist.
    async fn record(&self, id: u64, seq: u64) -> Result<Record> {
        let k = record_key(id, seq);
        if let Some(e) = self.tail.get(&k)
            && let (_, Stored::Record(r)) = decode_entry(e)?
        {
            return Ok(r);
        }
        self.base.stream_record(id, seq).await?.ok_or_else(|| Error::internal("stream record missing"))
    }
}

/// Plans `ops` to commit at `version`, store time `time`.
pub(crate) async fn plan(ops: &[StreamOp], at: &Pending<'_>, version: Seq, time: u64) -> Result<Effects> {
    let mut states: BTreeMap<u64, Option<StreamState>> = BTreeMap::new();
    let mut appended: BTreeMap<Bytes, Entry> = BTreeMap::new();
    let mut fx = Effects::default();
    for op in ops {
        let id = op.id();
        let cur = match states.get(&id) {
            Some(s) => *s,
            None => at.state(id).await?,
        };
        let missing = || Error::not_found("stream not found");
        let next = match op {
            StreamOp::Create { retention, .. } => match cur {
                Some(_) => return Err(Error::AlreadyExists("stream id in use".into())),
                None => StreamState::new(*retention),
            },
            StreamOp::Configure { retention, .. } => StreamState { retention: *retention, ..cur.ok_or_else(missing)? },
            StreamOp::Append { payloads, expect, op_index, .. } => {
                let mut st = cur.ok_or_else(missing)?;
                if let Some(x) = expect
                    && *x != st.next_seq
                {
                    return Err(Error::PreconditionFailed {
                        message: format!("the stream's next seq is {}, not {x}", st.next_seq),
                        op_index: *op_index,
                    });
                }
                fx.appends.push(Appended { first_seq: st.next_seq, count: payloads.len() as u64 });
                if payloads.is_empty() {
                    st
                } else {
                    if st.records() == 0 {
                        st.first_time = time;
                    }
                    let start = st.next_seq;
                    for p in payloads {
                        let k = record_key(id, st.next_seq);
                        let v = Record::encode_value(time, st.next_pos, p);
                        appended.insert(k.clone(), Entry::new(k, version, Value::Inline(v)));
                        st.next_seq += 1;
                        st.next_pos += p.len() as u64;
                    }
                    st.last_time = time;
                    fx.touched.push(records_between(id, start, Some(st.next_seq)));
                    st
                }
            }
            StreamOp::Trim { before, retention, .. } => {
                let mut st = cur.ok_or_else(missing)?;
                let target = (*before).clamp(st.first_seq, st.next_seq);
                if target > st.first_seq && retention.is_none_or(|r| r == st.retention) {
                    let (pos, t) = match target == st.next_seq {
                        true => (st.next_pos, st.last_time),
                        false => match appended.get(&record_key(id, target)) {
                            Some(e) => match decode_entry(e)? {
                                (_, Stored::Record(r)) => (r.pos, r.time_ms),
                                _ => return Err(corrupt()),
                            },
                            None => {
                                let r = at.record(id, target).await?;
                                (r.pos, r.time_ms)
                            }
                        },
                    };
                    (st.first_seq, st.first_pos, st.first_time) = (target, pos, t);
                    let r = records_between(id, 0, Some(target));
                    appended.retain(|k, _| !r.contains(k));
                    fx.touched.push(r.clone());
                    fx.trims.push(r);
                }
                st
            }
        };
        states.insert(id, Some(next));
    }
    for (id, st) in states {
        let Some(st) = st else { continue };
        let k = state_key(id);
        fx.touched.push(KeyRange::new(std::ops::Bound::Included(k.clone()), std::ops::Bound::Included(k.clone())));
        fx.entries.push(Entry::new(k, version, Value::Inline(st.encode())));
        fx.states.insert(id, st);
    }
    fx.entries.extend(appended.into_values());
    Ok(fx)
}

/// Seals runs of consecutive records of one stream into chunks as entries
/// stream past in key order, writing everything else through unchanged.
/// Runs of at least `min_bytes` are sealed, cut at `max_bytes`.
pub(crate) struct Sealer {
    min_bytes: usize,
    max_bytes: usize,
    run: Vec<(Entry, Record)>,
    run_bytes: usize,
    run_id: u64,
    /// Chunks written, and their stored bytes.
    pub chunks: u64,
    pub sealed_bytes: u64,
}

impl Sealer {
    pub fn new(min_bytes: usize, max_bytes: usize) -> Sealer {
        Sealer {
            min_bytes: min_bytes.max(1),
            max_bytes: max_bytes.max(min_bytes.max(1)),
            run: Vec::new(),
            run_bytes: 0,
            run_id: 0,
            chunks: 0,
            sealed_bytes: 0,
        }
    }

    /// Takes the next entry in key order, adding what to write to `out`.
    pub async fn push(&mut self, shared: &Shared, db: &str, e: Entry, out: &mut Vec<Entry>) -> Result<()> {
        let record = match stored(&e) {
            Some((id, Stored::Record(r))) => Some((id, r)),
            _ => None,
        };
        let Some((id, r)) = record else {
            self.finish(shared, db, out).await?;
            out.push(e);
            return Ok(());
        };
        let continues = self.run.last().is_some_and(|(_, last)| self.run_id == id && last.seq + 1 == r.seq);
        if !continues {
            self.finish(shared, db, out).await?;
            self.run_id = id;
        }
        self.run_bytes += e.size();
        self.run.push((e, r));
        if self.run_bytes >= self.max_bytes {
            self.seal(shared, db, out).await?;
        }
        Ok(())
    }

    /// Ends the current run: sealed if long enough, written through if not.
    pub async fn finish(&mut self, shared: &Shared, db: &str, out: &mut Vec<Entry>) -> Result<()> {
        if self.run_bytes >= self.min_bytes {
            return self.seal(shared, db, out).await;
        }
        out.extend(self.run.drain(..).map(|(e, _)| e));
        self.run_bytes = 0;
        Ok(())
    }

    async fn seal(&mut self, shared: &Shared, db: &str, out: &mut Vec<Entry>) -> Result<()> {
        let run = std::mem::take(&mut self.run);
        self.run_bytes = 0;
        let (Some((first_e, first)), Some((last_e, last))) = (run.first(), run.last()) else { return Ok(()) };
        let cfg = &shared.cfg;
        let mut b = SegmentBuilder::new(cfg.block_target, cfg.compression).without_bloom();
        for (e, _) in &run {
            b.add(e);
        }
        let (data, _) = b.finish();
        let id = shared.env.random_hex(16);
        let key = super::seg_key(db, &id);
        shared.retry.run(|| shared.store.put(&key, data.clone(), crate::store::PutMode::Overwrite)).await?;
        shared.remember_segment(db, &id, &data, false)?;
        let c = ChunkRef {
            id,
            size: data.len() as u64,
            first_seq: first.seq,
            last_seq: last.seq,
            first_pos: first.pos,
            bytes: run.iter().map(|(_, r)| r.payload.len() as u64).sum(),
            min_time: first.time_ms,
            max_time: last.time_ms,
        };
        self.chunks += 1;
        self.sealed_bytes += c.size;
        let version = run.iter().map(|(e, _)| e.seq).max().unwrap_or(first_e.seq);
        out.push(Entry::new(last_e.key.clone(), version, Value::Inline(c.encode_value())));
        Ok(())
    }
}

/// What a binary search over a stream's seqs looks for: the first record
/// at or after a time, or at or after a position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Seek {
    Time(u64),
    Pos(u64),
}

impl Seek {
    fn holds(self, r: &Record) -> bool {
        match self {
            Seek::Time(t) => r.time_ms >= t,
            Seek::Pos(p) => r.pos >= p,
        }
    }

    /// Whether the search target holds for every record of `c`, for none,
    /// or for some (`None`).
    fn classify(self, c: &ChunkRef) -> Option<bool> {
        match self {
            Seek::Time(t) if c.min_time >= t => Some(true),
            Seek::Time(t) if c.max_time < t => Some(false),
            Seek::Pos(p) if c.first_pos >= p => Some(true),
            Seek::Pos(p) if c.first_pos + c.bytes < p => Some(false),
            _ => None,
        }
    }
}

/// Iterates a stream's records with seqs in `[lo, hi)`, in either direction.
pub struct StreamIter {
    shared: Arc<Shared>,
    db: String,
    id: u64,
    lo: u64,
    hi: u64,
    reverse: bool,
    /// Finds the chunk that holds the seq before `hi`, for a reverse read.
    probe: Option<MergeIter>,
    lsm: MergeIter,
    chunk: Option<MergeIter>,
    done: bool,
}

impl StreamIter {
    fn open_chunk(&mut self, c: &ChunkRef) {
        let (lo, hi) = (self.lo.max(c.first_seq), self.hi.min(c.last_seq.saturating_add(1)));
        if lo >= hi {
            return;
        }
        let spec = IterSpec {
            range: records_between(self.id, lo, Some(hi)),
            reverse: self.reverse,
            include_tombstones: false,
            mode: ReadMode::Cached,
            deletes: None,
            expire_at: None,
        };
        self.chunk = Some(MergeIter::new(self.shared.clone(), &self.db, &[], vec![vec![c.meta(self.id)]], spec));
    }

    pub async fn next(&mut self) -> Result<Option<Record>> {
        loop {
            if let Some(c) = &mut self.chunk {
                match c.next().await? {
                    Some(e) => match decode_entry(&e)? {
                        (id, Stored::Record(r)) if id == self.id => return Ok(Some(r)),
                        _ => return Err(corrupt()),
                    },
                    None => self.chunk = None,
                }
            }
            if self.done {
                return Ok(None);
            }
            if let Some(mut p) = self.probe.take() {
                if let Some(e) = p.next().await?
                    && let (_, Stored::Chunk(c)) = decode_entry(&e)?
                    && c.first_seq < self.hi
                {
                    self.open_chunk(&c);
                }
                continue;
            }
            let Some(e) = self.lsm.next().await? else {
                self.done = true;
                continue;
            };
            match decode_entry(&e)?.1 {
                Stored::Record(r) if !self.reverse && r.seq >= self.hi => self.done = true,
                Stored::Record(r) => return Ok(Some(r)),
                Stored::Chunk(c) if !self.reverse && c.first_seq >= self.hi => self.done = true,
                Stored::Chunk(c) => self.open_chunk(&c),
            }
        }
    }
}

impl Version {
    fn stream_lsm(&self, range: KeyRange, reverse: bool) -> MergeIter {
        let spec = IterSpec {
            range,
            reverse,
            include_tombstones: false,
            mode: ReadMode::Cached,
            deletes: Some(self.deletes()),
            expire_at: None,
        };
        MergeIter::new(self.shared.clone(), &self.m.db_id, &self.m.tail, self.runs(|_| true), spec)
    }

    /// The state of stream `id`.
    pub async fn stream_state(&self, id: u64) -> Result<Option<StreamState>> {
        match self.get_entry(&state_key(id)).await? {
            Some(e) if !e.value.is_tombstone() => Ok(Some(state_of(&e).ok_or_else(corrupt)?.1)),
            _ => Ok(None),
        }
    }

    /// Records of stream `id` with seqs in `[lo, hi)`: oldest first, or
    /// newest first with `reverse`.
    pub fn stream_iter(&self, id: u64, lo: u64, hi: u64, reverse: bool) -> StreamIter {
        let (lsm, probe) = match reverse {
            false => (self.stream_lsm(records_between(id, lo, None), false), None),
            true => (
                self.stream_lsm(records_between(id, lo, Some(hi)), true),
                Some(self.stream_lsm(records_between(id, hi, None), false)),
            ),
        };
        StreamIter {
            shared: self.shared.clone(),
            db: self.m.db_id.clone(),
            id,
            lo,
            hi,
            reverse,
            probe,
            lsm,
            chunk: None,
            done: lo >= hi,
        }
    }

    /// What holds seq `seq` of stream `id`: its record, or its chunk.
    async fn stream_holder(&self, id: u64, seq: u64) -> Result<Option<Stored>> {
        let mut it = self.stream_lsm(records_between(id, seq, None), false);
        match it.next().await? {
            None => Ok(None),
            Some(e) => match decode_entry(&e)?.1 {
                Stored::Record(r) if r.seq != seq => Ok(None),
                s => Ok(Some(s)),
            },
        }
    }

    /// The record at `seq` of stream `id`, if stored.
    pub async fn stream_record(&self, id: u64, seq: u64) -> Result<Option<Record>> {
        match self.stream_holder(id, seq).await? {
            Some(Stored::Record(r)) => Ok(Some(r)),
            Some(Stored::Chunk(c)) if c.first_seq <= seq => self.stream_iter(id, seq, seq + 1, false).next().await,
            _ => Ok(None),
        }
    }

    /// The first seq of the stream whose record satisfies `seek`, or its
    /// next seq if none does. Times and positions never decrease along a
    /// stream, so this is a binary search; a chunk settles a probe from its
    /// range unless the answer lies inside it.
    pub async fn stream_seek(&self, id: u64, st: &StreamState, seek: Seek) -> Result<u64> {
        let (mut lo, mut hi) = (st.first_seq, st.next_seq);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.stream_holder(id, mid).await? {
                Some(Stored::Record(r)) if seek.holds(&r) => hi = mid,
                Some(Stored::Record(_)) => lo = mid + 1,
                Some(Stored::Chunk(c)) if c.first_seq <= mid => match seek.classify(&c) {
                    Some(true) => hi = hi.min(c.first_seq).max(lo),
                    Some(false) => lo = c.last_seq.saturating_add(1).min(hi),
                    None => {
                        let end = hi.min(c.last_seq.saturating_add(1));
                        let mut it = self.stream_iter(id, lo.max(c.first_seq), end, false);
                        let mut end = end;
                        while let Some(r) = it.next().await? {
                            if seek.holds(&r) {
                                end = r.seq;
                                break;
                            }
                        }
                        // Records past the chunk all hold, and those before it none.
                        return Ok(end);
                    }
                },
                _ => return Err(Error::internal(format!("stream record {mid} missing"))),
            }
        }
        Ok(lo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::rng::Rng;

    fn chunk(first: u64, last: u64) -> ChunkRef {
        ChunkRef {
            id: "a".repeat(32),
            size: 100,
            first_seq: first,
            last_seq: last,
            first_pos: 7,
            bytes: 40,
            min_time: 10,
            max_time: 20,
        }
    }

    #[test]
    fn keys_order_by_stream_then_seq() {
        assert!(record_key(1, u64::MAX) < record_key(2, 0));
        assert!(record_key(1, 255) < record_key(1, 256));
        assert_eq!(parse_record_key(&record_key(9, 42)), Some((9, 42)));
        assert_eq!(parse_record_key(&record_key(9, 42)[..16]), None);
        assert!(records(3).contains(&record_key(3, 0)) && records(3).contains(&record_key(3, u64::MAX)));
        assert!(!records(3).contains(&record_key(4, 0)) && !records(3).contains(&state_key(3)));
        assert!(records(u64::MAX).contains(&record_key(u64::MAX, 5)));
        let r = records_between(3, 5, Some(8));
        assert!(!r.contains(&record_key(3, 4)) && r.contains(&record_key(3, 5)) && !r.contains(&record_key(3, 8)));
        assert_eq!(parse_state_key(&state_key(77)), Some(77));
    }

    #[test]
    fn values_roundtrip() {
        let v = Record::encode_value(123, 456, b"payload");
        let want = Record { seq: 3, time_ms: 123, pos: 456, payload: Bytes::from_static(b"payload") };
        assert_eq!(Stored::decode(3, &v).unwrap(), Stored::Record(want));
        let c = chunk(4, 9);
        assert_eq!(Stored::decode(9, &c.encode_value()).unwrap(), Stored::Chunk(c.clone()));
        assert!(Stored::decode(3, &c.encode_value()).is_err(), "a chunk's last seq precedes its first");
        let st = StreamState {
            next_seq: 10,
            next_pos: 500,
            first_seq: 4,
            first_pos: 100,
            first_time: 5,
            last_time: 9,
            retention: Retention { retention_ms: Some(1_000), max_bytes: None },
        };
        assert_eq!(StreamState::decode(&st.encode()).unwrap(), st);
        assert_eq!((st.records(), st.bytes()), (6, 400));
    }

    #[test]
    fn retention_is_due_once_exceeded_by_a_sixteenth() {
        let mut st = StreamState::new(Retention { retention_ms: Some(160), max_bytes: Some(160) });
        assert!(!st.trim_due(1_000_000), "an empty stream");
        (st.next_seq, st.next_pos, st.first_time, st.last_time) = (3, 170, 1_000, 2_000);
        assert!(!st.trim_due(1_170) && st.trim_due(1_171));
        st.next_pos = 171;
        assert!(st.trim_due(0));
    }

    #[test]
    fn chunks_settle_probes_from_their_ranges() {
        let c = chunk(4, 9);
        assert_eq!(Seek::Time(10).classify(&c), Some(true));
        assert_eq!(Seek::Time(21).classify(&c), Some(false));
        assert_eq!(Seek::Time(15).classify(&c), None);
        assert_eq!(Seek::Pos(7).classify(&c), Some(true));
        assert_eq!(Seek::Pos(48).classify(&c), Some(false));
        assert_eq!(Seek::Pos(47).classify(&c), None, "the last record may start at the chunk's end");
    }

    #[test]
    fn fuzz_decoders_never_panic() {
        let st = StreamState::new(Retention { retention_ms: Some(5), max_bytes: Some(6) }).encode();
        let goods = [Record::encode_value(1, 2, b"xyz"), chunk(1, 5).encode_value(), st];
        let mut r = Rng::new(17);
        for i in 0..20_000 {
            let mut d = goods[i % goods.len()].to_vec();
            if i % 4 == 0 {
                let n = r.usize(40);
                d = r.bytes(n);
            }
            for _ in 0..r.usize(3) {
                if !d.is_empty() {
                    let p = r.usize(d.len());
                    d[p] = r.next_u32() as u8;
                }
            }
            d.truncate(r.usize(d.len() + 1));
            let b = Bytes::from(d);
            let _ = Stored::decode(r.next_u64() % 10, &b);
            let _ = StreamState::decode(&b);
        }
    }
}
