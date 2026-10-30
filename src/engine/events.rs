//! Event table entries in the LSM tree and committer operations (SPEC §4.8, §5.13).

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
/// Maximum rows inserted into event tables by a single transaction.
pub const INSERT_MAX_ROWS: usize = 100_000;

fn table_prefix(space: u8, table: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(32);
    k.push(space);
    k.extend_from_slice(&table.to_be_bytes());
    k
}

/// Unique row position ordered by commit `version` and `ordinal` within the table's rows in that commit.
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

pub fn rows(table: u32) -> KeyRange {
    KeyRange::prefix(&table_prefix(UNSEALED, table))
}

pub fn rows_through(table: u32, through: Pos) -> KeyRange {
    KeyRange::new(Bound::Included(Bytes::from(table_prefix(UNSEALED, table))), Bound::Included(row_key(table, through)))
}

/// Encodes a signed partition `bucket` with a `0x8000_0000` offset so that unsigned keys preserve signed order.
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

pub fn refs(table: u32) -> KeyRange {
    KeyRange::prefix(&table_prefix(PARTS, table))
}

/// Returns the key range of part references for `table` in partitions `lo..=hi`.
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

/// Committer-maintained counters for an event table.
/// Transactions do not add `TableState` to their read sets, so event inserts and binds do not conflict on it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableState {
    pub unsealed_rows: u64,
    pub unsealed_bytes: u64,
    /// Total stored bytes of referenced parts and mask objects for this table.
    pub part_bytes: u64,
}

const STATE_FORMAT: u8 = 2;

impl TableState {
    pub fn encode(&self) -> Bytes {
        let mut w = Writer::with_capacity(32);
        w.u8(STATE_FORMAT);
        w.varint(self.unsealed_rows);
        w.varint(self.unsealed_bytes);
        w.varint(self.part_bytes);
        Bytes::from(w.into_inner())
    }

    pub fn decode(b: &[u8]) -> DResult<TableState> {
        let mut r = Reader::new(b);
        if r.u8()? != STATE_FORMAT {
            return Err(DecodeError("unsupported event table state format"));
        }
        let unsealed_rows = r.varint()?;
        let unsealed_bytes = r.varint()?;
        let part_bytes = r.varint()?;
        if !r.is_empty() {
            return Err(DecodeError("event table state trailing data"));
        }
        Ok(TableState { unsealed_rows, unsealed_bytes, part_bytes })
    }
}

pub fn state_of(e: &Entry) -> Option<(u32, TableState)> {
    let id = parse_state_key(&e.key)?;
    match &e.value {
        Value::Inline(v) => TableState::decode(v).ok().map(|s| (id, s)),
        _ => None,
    }
}

/// Sorted, disjoint, non-adjacent `(start, len)` runs of row offsets in a part.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Runs(pub Vec<(u32, u32)>);

impl Runs {
    /// Builds runs from sorted row `offsets`.
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

    /// Returns true if all row offsets in `lo..hi` are contained in `self`.
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

/// Deletion mask for a part, stored inline in [`PartRef`] or as a separate object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mask {
    Inline(Runs),
    /// Mask object at `part/{id}/0000` containing encoded runs and a CRC32C checksum.
    Object {
        id: String,
        size: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartRef {
    pub id: String,
    pub table: u32,
    pub partition: u32,
    pub chunk_bytes: u64,
    pub size: u64,
    /// Uncompressed row bytes, used to assign the part to a merge tier.
    pub raw_size: u64,
    pub rows: u64,
    /// Merge depth: 0 for staged and sealed parts.
    pub level: u8,
    pub meta_off: u64,
    pub meta_len: u64,
    pub min_time: i64,
    pub max_time: i64,
    /// Creation timestamp of the newest input part, or of this part if unmerged.
    pub arrived_ms: u64,
    /// Commit version shared by all rows in a newly bound staged part, or `None` if stored in the part's version column.
    pub version: Option<Seq>,
    pub min_pos: Pos,
    pub max_pos: Pos,
    pub deleted: u64,
    /// Timestamp of the first deletion in this part, or 0 if unmasked.
    pub masked_ms: u64,
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

    pub fn live_rows(&self) -> u64 {
        self.rows - self.deleted
    }

    /// Total stored bytes of the part and its external mask object, if any.
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
        let mut w = Writer::with_capacity(160);
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
        w.varint(self.arrived_ms);
        match self.version {
            None => w.u8(0),
            Some(v) => {
                w.u8(1);
                w.varint(v);
            }
        }
        for p in [self.min_pos, self.max_pos] {
            w.varint(p.version);
            w.varint(u64::from(p.ordinal));
        }
        w.varint(self.deleted);
        w.varint(self.masked_ms);
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
            arrived_ms: r.varint()?,
            version: None,
            min_pos: Pos::default(),
            max_pos: Pos::default(),
            deleted: 0,
            masked_ms: 0,
            mask: None,
        };
        p.version = match r.u8()? {
            0 => None,
            1 => Some(r.varint()?),
            _ => return Err(DecodeError("bad part version tag")),
        };
        for p in [&mut p.min_pos, &mut p.max_pos] {
            p.version = r.varint()?;
            p.ordinal = u32::try_from(r.varint()?).map_err(|_| DecodeError("bad part position"))?;
        }
        p.deleted = r.varint()?;
        p.masked_ms = r.varint()?;
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
            && p.min_pos <= p.max_pos
            && p.deleted < p.rows.max(1)
            && p.rows > 0;
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventOp {
    /// Inserts encoded rows into `table` and assigns their positions at commit.
    Insert {
        table: u32,
        rows: Vec<Bytes>,
    },
    /// Updates stored byte counts and marks replaced objects obsolete for a [`PartRef`] change in the same write set.
    Ref(Box<RefChange>),
    /// Subtracts unsealed row and byte counts removed by a seal or delete in the same write set.
    Forget {
        table: u32,
        rows: u64,
        bytes: u64,
    },
    Create {
        table: u32,
    },
    /// Deletes the [`TableState`] for `table` and subtracts its `part_bytes` from the database total.
    Drop {
        table: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefChange {
    pub old: Option<PartRef>,
    pub new: Option<PartRef>,
}

impl EventOp {
    /// Records a [`PartRef`] transition from `old` to `new`.
    /// Objects referenced by `old` that are not referenced by `new` are marked obsolete at commit.
    pub fn part_ref(old: Option<PartRef>, new: Option<PartRef>) -> EventOp {
        EventOp::Ref(Box::new(RefChange { old, new }))
    }

    pub fn inserted_bytes(&self) -> u64 {
        match self {
            EventOp::Insert { rows, .. } => rows.iter().map(|r| r.len() as u64 + 32).sum(),
            _ => 0,
        }
    }

    /// Returns newly referenced part and mask object IDs that must pass stage TTL validation.
    pub fn new_objects(&self) -> Vec<&str> {
        let EventOp::Ref(c) = self else { return Vec::new() };
        let RefChange { old, new: Some(n) } = c.as_ref() else { return Vec::new() };
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

/// Returns true if an object with creation timestamp encoded in `id` is within `stage_ttl_ms` at `now`.
pub fn stage_fresh(id: &str, now: u64, stage_ttl_ms: u64) -> bool {
    blob_id_time(id).is_some_and(|t| t + stage_ttl_ms >= now)
}

#[derive(Debug, Default)]
pub(crate) struct Effects {
    pub entries: Vec<Entry>,
    pub touched: Vec<KeyRange>,
    pub obsolete: Vec<(ObjKind, String)>,
    pub part_bytes_added: u64,
    pub part_bytes_removed: u64,
    pub tables: BTreeMap<u32, TableState>,
}

pub(crate) async fn state(at: &Pending<'_>, table: u32) -> Result<Option<TableState>> {
    match at.entry(&state_key(table)).await? {
        None => Ok(None),
        Some(e) if e.value.is_tombstone() => Ok(None),
        Some(e) => Ok(Some(state_of(&e).ok_or_else(|| Error::internal("corrupt event table state"))?.1)),
    }
}

pub(crate) async fn plan(ops: &[EventOp], at: &Pending<'_>, version: Seq) -> Result<Effects> {
    let mut fx = Effects::default();
    let mut states: BTreeMap<u32, (Option<TableState>, bool)> = BTreeMap::new();
    let mut ordinals: BTreeMap<u32, u32> = BTreeMap::new();
    let not_found = || Error::not_found("event table not found");
    for op in ops {
        let table = match op {
            EventOp::Insert { table, .. }
            | EventOp::Forget { table, .. }
            | EventOp::Create { table }
            | EventOp::Drop { table } => *table,
            EventOp::Ref(c) => c.new.as_ref().or(c.old.as_ref()).map(|r| r.table).unwrap_or(0),
        };
        let (st, _) = match states.entry(table) {
            std::collections::btree_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::btree_map::Entry::Vacant(e) => {
                let st = state(at, table).await?;
                e.insert((st, st.is_some()))
            }
        };
        match op {
            EventOp::Insert { rows, .. } => {
                let st = st.as_mut().ok_or_else(not_found)?;
                let next = ordinals.entry(table).or_insert(0);
                let first = Pos { version, ordinal: *next };
                for r in rows {
                    let k = row_key(table, Pos { version, ordinal: *next });
                    st.unsealed_rows += 1;
                    st.unsealed_bytes += (k.len() + r.len()) as u64;
                    fx.entries.push(Entry::new(k, version, Value::Inline(r.clone())));
                    *next = next.checked_add(1).ok_or_else(|| Error::TooLarge("too many rows".into()))?;
                }
                if !rows.is_empty() {
                    let last = Pos { version, ordinal: *next - 1 };
                    fx.touched.push(KeyRange::new(
                        Bound::Included(row_key(table, first)),
                        Bound::Included(row_key(table, last)),
                    ));
                }
            }
            EventOp::Forget { rows, bytes, .. } => {
                let st = st.as_mut().ok_or_else(not_found)?;
                st.unsealed_rows = st.unsealed_rows.saturating_sub(*rows);
                st.unsealed_bytes = st.unsealed_bytes.saturating_sub(*bytes);
            }
            EventOp::Create { .. } => *st = Some(TableState::default()),
            EventOp::Drop { .. } => {
                fx.part_bytes_removed += st.take().map_or(0, |s| s.part_bytes);
                let rows = self::rows(table);
                fx.entries.retain(|e| !rows.contains(&e.key));
            }
            EventOp::Ref(c) => {
                let RefChange { old, new } = c.as_ref();
                let st = st.as_mut().ok_or_else(not_found)?;
                if let Some(o) = old {
                    st.part_bytes = st.part_bytes.saturating_sub(o.stored_bytes());
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
                    st.part_bytes += n.stored_bytes();
                    fx.part_bytes_added += n.stored_bytes();
                }
            }
        }
    }
    for (table, (st, existed)) in states {
        let k = state_key(table);
        match st {
            Some(st) => {
                fx.entries.push(Entry::new(k.clone(), version, Value::Inline(st.encode())));
                fx.tables.insert(table, st);
            }
            None if existed => fx.entries.push(Entry::tombstone(k.clone(), version)),
            None => continue,
        }
        fx.touched.push(KeyRange::point(&k));
    }
    Ok(fx)
}

/// Single request slot in a [`Bind`] batch.
#[derive(Clone, Debug)]
pub struct BindSlot {
    pub commit_id: u64,
    /// Instant after which the committer rejects this slot without applying it.
    pub deadline: Option<tokio::time::Instant>,
}

#[derive(Clone, Debug)]
pub struct StagedPart {
    /// `version` and `mask` are populated by the committer.
    pub part: PartRef,
    /// Row offsets in `part` written by each slot in [`Bind::slots`].
    pub slot_rows: Vec<Runs>,
}

/// Batch of bulk-insert requests staged into shared parts and committed together.
/// Accepted slots share a single commit version, and rows belonging to rejected slots are masked.
#[derive(Clone, Debug)]
pub struct Bind {
    pub table: u32,
    pub slots: Vec<BindSlot>,
    pub parts: Vec<StagedPart>,
}

impl Bind {
    /// Encodes the bind batch for forwarding to the lease holder, converting deadlines to remaining milliseconds.
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
            arrived_ms: 1_700_000_000_123,
            version: Some(9),
            min_pos: Pos { version: 9, ordinal: 2 },
            max_pos: Pos { version: 9, ordinal: 300 },
            deleted: 3,
            masked_ms: 1_700_000_100_000,
            mask: Some(Mask::Inline(Runs(vec![(1, 2), (50, 1)]))),
        }
    }

    #[test]
    fn key_encoding_and_ranges() {
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
    fn part_ref_and_table_state_codec() {
        let mut p = sample_ref();
        assert_eq!(PartRef::decode(&p.key(), &p.encode()).unwrap(), p);
        p.mask = Some(Mask::Object { id: "f".repeat(32), size: 77 });
        p.version = None;
        assert_eq!(PartRef::decode(&p.key(), &p.encode()).unwrap(), p);
        assert_eq!(p.stored_bytes(), 5077);
        p.mask = Some(Mask::Inline(Runs(vec![(1, 2)])));
        assert!(PartRef::decode(&p.key(), &p.encode()).is_err());
        let st = TableState { unsealed_rows: 4, unsealed_bytes: 99, part_bytes: 5077 };
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
            assert!(u.0.windows(2).all(|w| w[0].0 + w[0].1 < w[1].0));
            let lo = r.usize(n) as u32;
            let hi = lo + r.usize(10) as u32;
            assert_eq!(u.covers(lo, hi), (lo..hi).all(|o| u.contains(o)));
        }
    }

    #[test]
    fn bind_codec() {
        let mut part = sample_ref();
        part.version = None;
        part.mask = None;
        part.deleted = 0;
        part.masked_ms = 0;
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
    fn fuzz_decoders() {
        let p = sample_ref();
        let inputs = [p.encode(), TableState::default().encode(), encode_mask_object(&Runs(vec![(3, 4)]))];
        let key = p.key();
        let mut r = Rng::new(23);
        for i in 0..20_000 {
            let mut d = inputs[i % inputs.len()].to_vec();
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
