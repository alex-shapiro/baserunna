//! Event table storage in the LSM tree (SPEC §4.8, §5.12): unsealed rows,
//! part references and per-table state, and how the committer applies the
//! event ops of a transaction or a bulk bind.
//!
//! Every change to which rows exist is a commit that changes these entries,
//! so at every version each row lives in exactly one place: as an unsealed
//! row, or in a part named by a reference.

use super::manifest::ObjKind;
use super::stream::Pending;
use super::types::{Entry, Seq, Value, blob_id_time, valid_object_id};
use super::version::KeyRange;
use crate::error::{Error, Result};
use crate::util::codec::{DResult, DecodeError, Reader, Writer};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::ops::Bound;

/// Key prefix of unsealed rows: `UNSEALED ++ table ++ version ++ ordinal`.
pub const UNSEALED: u8 = 0x09;
/// Key prefix of part references: `PARTS ++ table ++ partition ++ part_id`.
pub const PARTS: u8 = 0x0A;
/// Key prefix of table state: `TABLE_STATE ++ table`.
pub const TABLE_STATE: u8 = 0x0B;
/// Most rows one transaction inserts into event tables.
pub const INSERT_MAX_ROWS: usize = 100_000;
/// Masks larger than this are stored as objects rather than in references.
pub const MASK_INLINE_MAX: usize = 4096;

fn table_prefix(space: u8, table: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(32);
    k.push(space);
    k.extend_from_slice(&table.to_be_bytes());
    k
}

/// A row's position: the version of the commit that inserted it and its
/// index among that table's rows in the commit. Positions are unique and
/// increase in commit order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Pos {
    pub version: u64,
    pub ordinal: u32,
}

pub fn row_key(table: u32, p: Pos) -> Bytes {
    let mut k = table_prefix(UNSEALED, table);
    k.extend_from_slice(&p.version.to_be_bytes());
    k.extend_from_slice(&p.ordinal.to_be_bytes());
    Bytes::from(k)
}

pub fn parse_row_key(k: &[u8]) -> Option<(u32, Pos)> {
    if k.len() != 17 || k[0] != UNSEALED {
        return None;
    }
    let table = u32::from_be_bytes(k[1..5].try_into().ok()?);
    let version = u64::from_be_bytes(k[5..13].try_into().ok()?);
    let ordinal = u32::from_be_bytes(k[13..17].try_into().ok()?);
    Some((table, Pos { version, ordinal }))
}

/// Every unsealed row of `table`.
pub fn rows(table: u32) -> KeyRange {
    KeyRange::prefix(&table_prefix(UNSEALED, table))
}

/// The unsealed rows of `table` at or before `through`.
pub fn rows_through(table: u32, through: Pos) -> KeyRange {
    KeyRange::new(Bound::Included(Bytes::from(table_prefix(UNSEALED, table))), Bound::Included(row_key(table, through)))
}

/// Partitions are stored offset by 2^31 so that keys order like the signed buckets.
pub fn partition_key(bucket: i64) -> u32 {
    (bucket.clamp(i32::MIN as i64, i32::MAX as i64) as i32 as u32) ^ 0x8000_0000
}

pub fn partition_bucket(key: u32) -> i64 {
    (key ^ 0x8000_0000) as i32 as i64
}

pub fn ref_key(table: u32, partition: u32, part_id: &str) -> Bytes {
    let mut k = table_prefix(PARTS, table);
    k.extend_from_slice(&partition.to_be_bytes());
    k.extend_from_slice(part_id.as_bytes());
    Bytes::from(k)
}

pub fn parse_ref_key(k: &[u8]) -> Option<(u32, u32, &str)> {
    if k.len() != 41 || k[0] != PARTS {
        return None;
    }
    let table = u32::from_be_bytes(k[1..5].try_into().ok()?);
    let partition = u32::from_be_bytes(k[5..9].try_into().ok()?);
    let id = std::str::from_utf8(&k[9..]).ok().filter(|id| valid_object_id(id))?;
    Some((table, partition, id))
}

/// The references of every partition of `table`.
pub fn refs(table: u32) -> KeyRange {
    KeyRange::prefix(&table_prefix(PARTS, table))
}

/// The references of partitions `lo..=hi` of `table`.
pub fn refs_between(table: u32, lo: u32, hi: u32) -> KeyRange {
    let mut s = table_prefix(PARTS, table);
    s.extend_from_slice(&lo.to_be_bytes());
    let mut e = table_prefix(PARTS, table);
    match hi.checked_add(1) {
        Some(h) => {
            e.extend_from_slice(&h.to_be_bytes());
            KeyRange::half_open(Bytes::from(s), Some(Bytes::from(e)))
        }
        None => KeyRange::new(Bound::Included(Bytes::from(s)), refs(table).end),
    }
}

pub fn state_key(table: u32) -> Bytes {
    Bytes::from(table_prefix(TABLE_STATE, table))
}

pub fn parse_state_key(k: &[u8]) -> Option<u32> {
    match k.len() == 5 && k[0] == TABLE_STATE {
        true => Some(u32::from_be_bytes(k[1..5].try_into().ok()?)),
        false => None,
    }
}

/// Committer-maintained counters of a table's unsealed rows. Transactions
/// never read it into their read sets, so inserts stay blind writes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableState {
    pub unsealed_rows: u64,
    /// Key and value bytes of the unsealed rows.
    pub unsealed_bytes: u64,
    /// Every row at or before this position has been sealed or deleted.
    pub sealed_through: Option<Pos>,
}

const STATE_FORMAT: u8 = 1;

impl TableState {
    pub fn encode(&self) -> Bytes {
        let mut w = Writer::with_capacity(32);
        w.u8(STATE_FORMAT);
        w.varint(self.unsealed_rows);
        w.varint(self.unsealed_bytes);
        match self.sealed_through {
            None => w.u8(0),
            Some(p) => {
                w.u8(1);
                w.varint(p.version);
                w.varint(p.ordinal as u64);
            }
        }
        Bytes::from(w.into_inner())
    }

    pub fn decode(b: &[u8]) -> DResult<TableState> {
        let mut r = Reader::new(b);
        if r.u8()? != STATE_FORMAT {
            return Err(DecodeError("unsupported event table state format"));
        }
        let unsealed_rows = r.varint()?;
        let unsealed_bytes = r.varint()?;
        let sealed_through = match r.u8()? {
            0 => None,
            1 => Some(Pos {
                version: r.varint()?,
                ordinal: u32::try_from(r.varint()?).map_err(|_| DecodeError("bad ordinal"))?,
            }),
            _ => return Err(DecodeError("bad sealed position tag")),
        };
        if !r.is_empty() {
            return Err(DecodeError("event table state trailing data"));
        }
        Ok(TableState { unsealed_rows, unsealed_bytes, sealed_through })
    }
}

pub fn state_of(e: &Entry) -> Option<(u32, TableState)> {
    let id = parse_state_key(&e.key)?;
    match &e.value {
        Value::Inline(v) => TableState::decode(v).ok().map(|s| (id, s)),
        _ => None,
    }
}

/// Deleted row offsets of a part as sorted, disjoint, non-adjacent runs
/// `(start, len)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Runs(pub Vec<(u32, u32)>);

impl Runs {
    /// Runs covering the sorted, distinct `offsets`.
    pub fn from_sorted(offsets: impl IntoIterator<Item = u32>) -> Runs {
        let mut v: Vec<(u32, u32)> = Vec::new();
        for o in offsets {
            match v.last_mut() {
                Some((s, n)) if *s + *n == o => *n += 1,
                Some((s, n)) if o < *s + *n => {}
                _ => v.push((o, 1)),
            }
        }
        Runs(v)
    }

    pub fn count(&self) -> u64 {
        self.0.iter().map(|(_, n)| *n as u64).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn contains(&self, o: u32) -> bool {
        let i = self.0.partition_point(|(s, _)| *s <= o);
        i > 0 && o < self.0[i - 1].0 + self.0[i - 1].1
    }

    /// Whether every row in `lo..hi` is covered.
    pub fn covers(&self, lo: u32, hi: u32) -> bool {
        if lo >= hi {
            return true;
        }
        let i = self.0.partition_point(|(s, _)| *s <= lo);
        i > 0 && hi <= self.0[i - 1].0 + self.0[i - 1].1
    }

    pub fn union(&self, other: &Runs) -> Runs {
        let mut all: Vec<(u32, u32)> = self.0.iter().chain(other.0.iter()).copied().collect();
        all.sort_unstable();
        let mut v: Vec<(u32, u32)> = Vec::with_capacity(all.len());
        for (s, n) in all {
            match v.last_mut() {
                Some((ls, ln)) if s <= *ls + *ln => *ln = (*ln).max(s + n - *ls),
                _ => v.push((s, n)),
            }
        }
        Runs(v)
    }

    pub fn encode(&self, w: &mut Writer) {
        w.varint(self.0.len() as u64);
        let mut prev = 0u64;
        for (s, n) in &self.0 {
            w.varint(*s as u64 - prev);
            w.varint(*n as u64);
            prev = *s as u64 + *n as u64;
        }
    }

    pub fn decode(r: &mut Reader<'_>) -> DResult<Runs> {
        let n = r.len(r.remaining())?;
        let mut v = Vec::with_capacity(n.min(1 << 16));
        let mut prev = 0u64;
        for i in 0..n {
            let gap = r.varint()?;
            let len = r.varint()?;
            if len == 0 || (i > 0 && gap == 0) {
                return Err(DecodeError("bad mask run"));
            }
            let s = prev.checked_add(gap).ok_or(DecodeError("bad mask run"))?;
            let e = s.checked_add(len).filter(|e| *e <= u32::MAX as u64).ok_or(DecodeError("bad mask run"))?;
            v.push((s as u32, len as u32));
            prev = e;
        }
        Ok(Runs(v))
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut w = Writer::new();
        self.encode(&mut w);
        Bytes::from(w.into_inner())
    }
}

/// Where a part's delete mask is stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mask {
    Inline(Runs),
    /// An object at `part/{id}/0` holding the encoded runs and a CRC32C.
    Object {
        id: String,
        size: u64,
    },
}

/// Everything a query needs to decide whether to open a part, and what the
/// committer needs to account for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartRef {
    /// 32 hex characters: a 48-bit creation time and 80 random bits.
    pub id: String,
    pub table: u32,
    pub partition: u32,
    /// The part's byte stream is stored in objects of this many bytes.
    pub chunk_bytes: u64,
    /// Stored bytes of the part.
    pub size: u64,
    /// Uncompressed bytes of its rows, which decide its merge tier.
    pub raw_size: u64,
    pub rows: u64,
    /// The merges that produced the part: 0 for staged and sealed parts.
    pub level: u8,
    /// Byte range of the part's metadata in its stream.
    pub meta_off: u64,
    pub meta_len: u64,
    pub min_time: i64,
    pub max_time: i64,
    /// Tuple-encoded sort keys of the first and last rows.
    pub min_key: Bytes,
    pub max_key: Bytes,
    /// The version of every row, for parts whose rows were bound by one
    /// commit after the part was written; `None` when the part stores it.
    pub version: Option<Seq>,
    pub min_version: Seq,
    pub max_version: Seq,
    /// Rows covered by the mask.
    pub deleted: u64,
    pub mask: Option<Mask>,
}

const REF_FORMAT: u8 = 1;

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

impl PartRef {
    pub fn key(&self) -> Bytes {
        ref_key(self.table, self.partition, &self.id)
    }

    /// Rows not covered by the mask.
    pub fn live_rows(&self) -> u64 {
        self.rows - self.deleted
    }

    /// Bytes of objects the reference names: the part and its mask object.
    pub fn stored_bytes(&self) -> u64 {
        self.size
            + match &self.mask {
                Some(Mask::Object { size, .. }) => *size,
                _ => 0,
            }
    }

    pub fn mask_object(&self) -> Option<&str> {
        match &self.mask {
            Some(Mask::Object { id, .. }) => Some(id),
            _ => None,
        }
    }

    pub fn chunks(&self) -> u64 {
        self.size.div_ceil(self.chunk_bytes.max(1))
    }

    pub fn encode(&self) -> Bytes {
        let mut w = Writer::with_capacity(160 + self.min_key.len() + self.max_key.len());
        w.u8(REF_FORMAT);
        w.varint(self.chunk_bytes);
        w.varint(self.size);
        w.varint(self.raw_size);
        w.varint(self.rows);
        w.u8(self.level);
        w.varint(self.meta_off);
        w.varint(self.meta_len);
        w.varint(zigzag(self.min_time));
        w.varint(zigzag(self.max_time));
        w.bytes(&self.min_key);
        w.bytes(&self.max_key);
        match self.version {
            None => w.u8(0),
            Some(v) => {
                w.u8(1);
                w.varint(v);
            }
        }
        w.varint(self.min_version);
        w.varint(self.max_version);
        w.varint(self.deleted);
        match &self.mask {
            None => w.u8(0),
            Some(Mask::Inline(r)) => {
                w.u8(1);
                r.encode(&mut w);
            }
            Some(Mask::Object { id, size }) => {
                w.u8(2);
                w.str(id);
                w.varint(*size);
            }
        }
        Bytes::from(w.into_inner())
    }

    pub fn decode(key: &[u8], b: &[u8]) -> DResult<PartRef> {
        let (table, partition, id) = parse_ref_key(key).ok_or(DecodeError("bad part reference key"))?;
        let mut r = Reader::new(b);
        if r.u8()? != REF_FORMAT {
            return Err(DecodeError("unsupported part reference format"));
        }
        let mut p = PartRef {
            id: id.to_string(),
            table,
            partition,
            chunk_bytes: r.varint()?,
            size: r.varint()?,
            raw_size: r.varint()?,
            rows: r.varint()?,
            level: r.u8()?,
            meta_off: r.varint()?,
            meta_len: r.varint()?,
            min_time: unzigzag(r.varint()?),
            max_time: unzigzag(r.varint()?),
            min_key: Bytes::copy_from_slice(r.bytes()?),
            max_key: Bytes::copy_from_slice(r.bytes()?),
            version: None,
            min_version: 0,
            max_version: 0,
            deleted: 0,
            mask: None,
        };
        p.version = match r.u8()? {
            0 => None,
            1 => Some(r.varint()?),
            _ => return Err(DecodeError("bad part version tag")),
        };
        p.min_version = r.varint()?;
        p.max_version = r.varint()?;
        p.deleted = r.varint()?;
        p.mask = match r.u8()? {
            0 => None,
            1 => Some(Mask::Inline(Runs::decode(&mut r)?)),
            2 => {
                let id = r.string()?;
                if !valid_object_id(&id) {
                    return Err(DecodeError("bad mask id"));
                }
                Some(Mask::Object { id, size: r.varint()? })
            }
            _ => return Err(DecodeError("bad mask tag")),
        };
        if !r.is_empty() {
            return Err(DecodeError("part reference trailing data"));
        }
        let ok = p.chunk_bytes > 0
            && p.meta_off.checked_add(p.meta_len).is_some_and(|e| e <= p.size)
            && p.min_time <= p.max_time
            && p.min_version <= p.max_version
            && p.deleted < p.rows.max(1)
            && p.rows > 0
            && p.level <= 64;
        if !ok {
            return Err(DecodeError("inconsistent part reference"));
        }
        if let Some(Mask::Inline(runs)) = &p.mask
            && (runs.count() != p.deleted || runs.0.last().is_some_and(|(s, n)| (*s + *n) as u64 > p.rows))
        {
            return Err(DecodeError("mask does not match its part"));
        }
        Ok(p)
    }
}

pub fn ref_of(e: &Entry) -> Option<PartRef> {
    match &e.value {
        Value::Inline(v) => PartRef::decode(&e.key, v).ok(),
        _ => None,
    }
}

/// Encodes a mask object.
pub fn encode_mask_object(r: &Runs) -> Bytes {
    let mut w = Writer::new();
    w.raw(b"BRMK");
    r.encode(&mut w);
    let crc = crate::util::crc32c::crc32c(&w.buf);
    w.u32(crc);
    Bytes::from(w.into_inner())
}

pub fn decode_mask_object(b: &[u8]) -> DResult<Runs> {
    if b.len() < 8 || &b[..4] != b"BRMK" {
        return Err(DecodeError("bad mask object"));
    }
    let (body, c) = b.split_at(b.len() - 4);
    if crate::util::crc32c::crc32c(body) != u32::from_le_bytes([c[0], c[1], c[2], c[3]]) {
        return Err(DecodeError("mask checksum mismatch"));
    }
    let mut r = Reader::new(&body[4..]);
    let runs = Runs::decode(&mut r)?;
    if !r.is_empty() {
        return Err(DecodeError("mask trailing data"));
    }
    Ok(runs)
}

/// Changes to event tables that the committer applies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventOp {
    /// Inserts encoded rows into `table`, giving them positions at commit.
    Insert { table: u32, rows: Vec<Bytes> },
    /// Accounts for a reference added, replaced or removed by a put in the
    /// same write set. `old` is the reference the transaction read, which
    /// validation guarantees is still current: its objects that `new` does
    /// not name become obsolete.
    Ref { old: Option<PartRef>, new: Option<PartRef> },
    /// Unsealed rows removed by a seal or a delete in the same write set.
    Forget { table: u32, rows: u64, bytes: u64, sealed_through: Option<Pos> },
}

impl EventOp {
    pub fn inserted_bytes(&self) -> u64 {
        match self {
            EventOp::Insert { rows, .. } => rows.iter().map(|r| r.len() as u64 + 32).sum(),
            _ => 0,
        }
    }

    /// Objects first named by this op, which must be bindable.
    pub fn new_objects(&self) -> Vec<&str> {
        let EventOp::Ref { old, new: Some(n) } = self else { return Vec::new() };
        let mut v = Vec::new();
        if old.as_ref().is_none_or(|o| o.id != n.id) {
            v.push(n.id.as_str());
        }
        if let Some(m) = n.mask_object()
            && old.as_ref().and_then(|o| o.mask_object()) != Some(m)
        {
            v.push(m);
        }
        v
    }
}

/// Whether an object staged at the time in its id can still be bound at
/// store time `now`.
pub fn stage_fresh(id: &str, now: u64, stage_ttl_ms: u64) -> bool {
    blob_id_time(id).is_some_and(|t| t + stage_ttl_ms >= now)
}

/// What a write set's event ops do to the committed state.
#[derive(Debug, Default)]
pub(crate) struct Effects {
    pub entries: Vec<Entry>,
    pub touched: Vec<KeyRange>,
    /// Objects no longer referenced.
    pub obsolete: Vec<(ObjKind, String)>,
    pub part_bytes_added: u64,
    pub part_bytes_removed: u64,
    /// Tables whose unsealed rows or references changed.
    pub tables: BTreeMap<u32, TableState>,
}

async fn state(at: &Pending<'_>, table: u32) -> Result<Option<TableState>> {
    match at.entry(&state_key(table)).await? {
        None => Ok(None),
        Some(e) if e.value.is_tombstone() => Ok(None),
        Some(e) => Ok(Some(state_of(&e).ok_or_else(|| Error::internal("corrupt event table state"))?.1)),
    }
}

fn point(k: &Bytes) -> KeyRange {
    KeyRange::new(Bound::Included(k.clone()), Bound::Included(k.clone()))
}

/// Plans `ops` committed at `version`.
pub(crate) async fn plan(ops: &[EventOp], at: &Pending<'_>, version: Seq) -> Result<Effects> {
    let mut fx = Effects::default();
    let mut states: BTreeMap<u32, TableState> = BTreeMap::new();
    let mut ordinals: BTreeMap<u32, u32> = BTreeMap::new();
    for op in ops {
        match op {
            EventOp::Insert { table, rows } => {
                let mut st = match states.get(table) {
                    Some(s) => *s,
                    None => state(at, *table).await?.ok_or_else(|| Error::not_found("event table not found"))?,
                };
                let next = ordinals.entry(*table).or_insert(0);
                let first = Pos { version, ordinal: *next };
                for r in rows {
                    let k = row_key(*table, Pos { version, ordinal: *next });
                    st.unsealed_rows += 1;
                    st.unsealed_bytes += (k.len() + r.len()) as u64;
                    fx.entries.push(Entry::new(k, version, Value::Inline(r.clone())));
                    *next = next.checked_add(1).ok_or_else(|| Error::TooLarge("too many rows".into()))?;
                }
                if !rows.is_empty() {
                    let last = Pos { version, ordinal: *next - 1 };
                    fx.touched.push(KeyRange::new(
                        Bound::Included(row_key(*table, first)),
                        Bound::Included(row_key(*table, last)),
                    ));
                }
                states.insert(*table, st);
            }
            EventOp::Forget { table, rows, bytes, sealed_through } => {
                let mut st = match states.get(table) {
                    Some(s) => *s,
                    None => state(at, *table).await?.ok_or_else(|| Error::not_found("event table not found"))?,
                };
                st.unsealed_rows = st.unsealed_rows.saturating_sub(*rows);
                st.unsealed_bytes = st.unsealed_bytes.saturating_sub(*bytes);
                if let Some(p) = sealed_through {
                    st.sealed_through = st.sealed_through.max(Some(*p));
                }
                states.insert(*table, st);
            }
            EventOp::Ref { old, new } => {
                if let Some(o) = old {
                    fx.part_bytes_removed += o.stored_bytes();
                    if new.as_ref().is_none_or(|n| n.id != o.id) {
                        fx.obsolete.push((ObjKind::Part, o.id.clone()));
                    }
                    if let Some(m) = o.mask_object()
                        && new.as_ref().and_then(|n| n.mask_object()) != Some(m)
                    {
                        fx.obsolete.push((ObjKind::Part, m.to_string()));
                    }
                }
                if let Some(n) = new {
                    fx.part_bytes_added += n.stored_bytes();
                }
                let table = new.as_ref().or(old.as_ref()).map(|r| r.table).unwrap_or(0);
                if !states.contains_key(&table)
                    && let Some(st) = state(at, table).await?
                {
                    states.insert(table, st);
                }
            }
        }
    }
    for (table, st) in states {
        let k = state_key(table);
        fx.touched.push(point(&k));
        fx.entries.push(Entry::new(k, version, Value::Inline(st.encode())));
        fx.tables.insert(table, st);
    }
    Ok(fx)
}

/// One request of a bulk bind: its own transaction and commit record.
#[derive(Clone, Debug)]
pub struct BindSlot {
    pub commit_id: u64,
    /// After this instant the committer rejects the slot without applying it.
    pub deadline: Option<tokio::time::Instant>,
}

/// A part staged for a bulk bind, and which of its rows each slot wrote.
#[derive(Clone, Debug)]
pub struct StagedPart {
    /// `version` and `mask` are set by the committer.
    pub part: PartRef,
    /// For each slot of the bind, the row offsets it wrote in this part.
    pub slot_rows: Vec<Runs>,
}

/// Bulk inserted requests whose rows were staged together as parts, bound
/// by one commit: accepted slots share its version, and the rows of
/// rejected slots are masked.
#[derive(Clone, Debug)]
pub struct Bind {
    pub table: u32,
    pub slots: Vec<BindSlot>,
    pub parts: Vec<StagedPart>,
}

impl Bind {
    /// Encodes the bind for forwarding to the lease holder, with deadlines as
    /// remaining milliseconds.
    pub fn encode(&self) -> Bytes {
        let now = tokio::time::Instant::now();
        let mut w = Writer::new();
        w.u8(1);
        w.varint(self.table as u64);
        w.varint(self.slots.len() as u64);
        for s in &self.slots {
            w.u64(s.commit_id);
            match s.deadline {
                None => w.u8(0),
                Some(d) => {
                    w.u8(1);
                    w.varint(d.saturating_duration_since(now).as_millis() as u64);
                }
            }
        }
        w.varint(self.parts.len() as u64);
        for p in &self.parts {
            w.bytes(&p.part.key());
            w.bytes(&p.part.encode());
            for r in &p.slot_rows {
                r.encode(&mut w);
            }
        }
        Bytes::from(w.into_inner())
    }

    pub fn decode(b: &[u8]) -> DResult<Bind> {
        let now = tokio::time::Instant::now();
        let mut r = Reader::new(b);
        if r.u8()? != 1 {
            return Err(DecodeError("unsupported bind format"));
        }
        let table = u32::try_from(r.varint()?).map_err(|_| DecodeError("bad table"))?;
        let n = r.len(r.remaining())?;
        let mut slots = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            let commit_id = r.u64()?;
            let deadline = match r.u8()? {
                0 => None,
                1 => Some(now + std::time::Duration::from_millis(r.varint()?.min(3_600_000))),
                _ => return Err(DecodeError("bad deadline tag")),
            };
            slots.push(BindSlot { commit_id, deadline });
        }
        let m = r.len(r.remaining())?;
        let mut parts = Vec::with_capacity(m.min(4096));
        for _ in 0..m {
            let key = r.bytes()?;
            let part = PartRef::decode(key, r.bytes()?)?;
            if part.table != table {
                return Err(DecodeError("part of another table"));
            }
            let mut slot_rows = Vec::with_capacity(n);
            for _ in 0..n {
                slot_rows.push(Runs::decode(&mut r)?);
            }
            parts.push(StagedPart { part, slot_rows });
        }
        if !r.is_empty() {
            return Err(DecodeError("bind trailing data"));
        }
        Ok(Bind { table, slots, parts })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::rng::Rng;

    pub(crate) fn sample_ref() -> PartRef {
        PartRef {
            id: "0123456789abcdef0123456789abcdef".into(),
            table: 7,
            partition: partition_key(20_000),
            chunk_bytes: 1 << 20,
            size: 5000,
            raw_size: 20_000,
            rows: 100,
            level: 2,
            meta_off: 4000,
            meta_len: 1000,
            min_time: -5,
            max_time: 1_700_000_000_000,
            min_key: Bytes::from_static(b"a"),
            max_key: Bytes::from_static(b"z"),
            version: Some(9),
            min_version: 9,
            max_version: 9,
            deleted: 3,
            mask: Some(Mask::Inline(Runs(vec![(1, 2), (50, 1)]))),
        }
    }

    #[test]
    fn keys_order_and_parse() {
        let a = row_key(1, Pos { version: 5, ordinal: u32::MAX });
        let b = row_key(1, Pos { version: 6, ordinal: 0 });
        assert!(a < b && b < row_key(2, Pos::default()));
        assert_eq!(parse_row_key(&b), Some((1, Pos { version: 6, ordinal: 0 })));
        assert!(rows(1).contains(&a) && !rows(1).contains(&row_key(2, Pos::default())));
        let through = rows_through(1, Pos { version: 5, ordinal: 3 });
        assert!(through.contains(&row_key(1, Pos { version: 5, ordinal: 3 })));
        assert!(!through.contains(&row_key(1, Pos { version: 5, ordinal: 4 })));
        for (x, y) in [(-3i64, -2i64), (-1, 0), (0, 1), (i32::MAX as i64 - 1, i32::MAX as i64)] {
            assert!(partition_key(x) < partition_key(y));
            assert_eq!(partition_bucket(partition_key(x)), x);
        }
        let id = "0123456789abcdef0123456789abcdef";
        let k = ref_key(3, 9, id);
        assert_eq!(parse_ref_key(&k), Some((3, 9, id)));
        assert!(refs_between(3, 9, 9).contains(&k) && !refs_between(3, 10, 12).contains(&k));
        assert!(refs_between(3, 0, u32::MAX).contains(&k) && refs(3).contains(&k));
        assert_eq!(parse_state_key(&state_key(42)), Some(42));
    }

    #[test]
    fn refs_and_states_roundtrip() {
        let mut p = sample_ref();
        assert_eq!(PartRef::decode(&p.key(), &p.encode()).unwrap(), p);
        p.mask = Some(Mask::Object { id: "f".repeat(32), size: 77 });
        p.version = None;
        assert_eq!(PartRef::decode(&p.key(), &p.encode()).unwrap(), p);
        assert_eq!(p.stored_bytes(), 5077);
        p.mask = Some(Mask::Inline(Runs(vec![(1, 2)])));
        assert!(PartRef::decode(&p.key(), &p.encode()).is_err(), "mask count must match");
        let st =
            TableState { unsealed_rows: 4, unsealed_bytes: 99, sealed_through: Some(Pos { version: 3, ordinal: 1 }) };
        assert_eq!(TableState::decode(&st.encode()).unwrap(), st);
        let runs = Runs(vec![(0, 3), (9, 1)]);
        assert_eq!(decode_mask_object(&encode_mask_object(&runs)).unwrap(), runs);
    }

    #[test]
    fn runs_union_and_membership() {
        let mut r = Rng::new(5);
        for _ in 0..300 {
            let n = 200;
            let a: std::collections::BTreeSet<u32> = (0..r.usize(60)).map(|_| r.usize(n) as u32).collect();
            let b: std::collections::BTreeSet<u32> = (0..r.usize(60)).map(|_| r.usize(n) as u32).collect();
            let (ra, rb) = (Runs::from_sorted(a.iter().copied()), Runs::from_sorted(b.iter().copied()));
            let u = ra.union(&rb);
            for o in 0..n as u32 {
                assert_eq!(u.contains(o), a.contains(&o) || b.contains(&o));
                assert_eq!(ra.contains(o), a.contains(&o));
            }
            assert_eq!(u.count() as usize, a.union(&b).count());
            assert_eq!(Runs::decode(&mut Reader::new(&u.to_bytes())).unwrap(), u);
            assert!(u.0.windows(2).all(|w| w[0].0 + w[0].1 < w[1].0), "runs are disjoint and apart");
            let lo = r.usize(n) as u32;
            let hi = lo + r.usize(10) as u32;
            assert_eq!(u.covers(lo, hi), (lo..hi).all(|o| u.contains(o)));
        }
    }

    #[test]
    fn binds_roundtrip() {
        let mut part = sample_ref();
        part.version = None;
        part.mask = None;
        part.deleted = 0;
        let b = Bind {
            table: 7,
            slots: vec![BindSlot { commit_id: 1, deadline: None }, BindSlot { commit_id: 2, deadline: None }],
            parts: vec![StagedPart { part: part.clone(), slot_rows: vec![Runs(vec![(0, 50)]), Runs(vec![(50, 50)])] }],
        };
        let d = Bind::decode(&b.encode()).unwrap();
        assert_eq!((d.table, d.slots.len(), d.parts[0].part.clone()), (7, 2, part));
        assert_eq!(d.parts[0].slot_rows, b.parts[0].slot_rows);
    }

    #[test]
    fn fuzz_decoders_never_panic() {
        let p = sample_ref();
        let goods = [p.encode(), TableState::default().encode(), encode_mask_object(&Runs(vec![(3, 4)]))];
        let key = p.key();
        let mut r = Rng::new(23);
        for i in 0..20_000 {
            let mut d = goods[i % goods.len()].to_vec();
            for _ in 0..r.usize(3) {
                if !d.is_empty() {
                    let at = r.usize(d.len());
                    d[at] = r.next_u32() as u8;
                }
            }
            d.truncate(r.usize(d.len() + 1));
            let _ = PartRef::decode(&key, &d);
            let _ = TableState::decode(&d);
            let _ = decode_mask_object(&d);
            let _ = Bind::decode(&d);
            let _ = Runs::decode(&mut Reader::new(&d));
        }
    }
}
