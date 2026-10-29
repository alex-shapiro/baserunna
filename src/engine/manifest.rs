//! The per-database manifest: the root of all committed state.
//!
//! It is the only mutable object of a database and is only ever written with
//! a conditional PUT. Small writes are kept inline in `tail`, which makes the
//! manifest the write-ahead log as well. When a database has many segments below
//! level 0, their metadata moves to an immutable levels object updated only
//! during compaction, avoiding rewriting them on regular commits.

use super::range_delete::RangeTombstone;
use super::types::{Entry, Seq, is_commit_key, valid_object_id};
use crate::util::bloom::key_hash;
use crate::util::codec::{DResult, DecodeError, Reader, Writer};
use crate::util::crc32c::crc32c;
use bytes::Bytes;

const MAGIC: &[u8; 4] = b"BRMF";

/// The database was deleted: every read and commit fails with not-found.
pub const FLAG_DELETED: u64 = 1;
/// The database has had an event table, so `part/` may hold objects for the
/// orphan sweep to collect, even once every table is dropped.
pub const FLAG_EVENT_TABLES: u64 = 2;
const FORMAT: u16 = 8;
const FORMAT_V7: u16 = 7;
const FORMAT_V6: u16 = 6;
const FORMAT_V5: u16 = 5;
const ZSTD_LEVEL: i32 = 1;
const MAX_BODY: u64 = 64 << 20;
const LEVELS_MAGIC: &[u8; 4] = b"BRLV";
const LEVELS_FORMAT: u16 = 2;

/// Deepest segment level. Level 0 holds flushed tails, newest first, which
/// may overlap; each level from 1 to `MAX_LEVEL` is one sorted run of
/// segments with disjoint key ranges.
pub const MAX_LEVEL: u8 = 6;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub node: String,
    /// The holder's wall-clock expiry: identifies a renewal and hints at
    /// routing, but is never compared with another clock.
    pub expires_ms: u64,
    /// Lease duration in milliseconds, waited out plus a margin by other nodes
    /// from when they first observe this renewal.
    pub ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentMeta {
    /// 32-character hex identifier.
    pub id: String,
    /// Level in the LSM tree, updated in the manifest on compaction moves.
    pub level: u8,
    pub size: u64,
    pub entries: u64,
    pub tombstones: u64,
    pub min_key: Bytes,
    pub max_key: Bytes,
    pub min_seq: Seq,
    pub max_seq: Seq,
    /// Sequence number through which all committed range deletions have been applied to this segment.
    pub deletes_applied: Seq,
    /// Number of entries with an expiry, and the latest expiry among them.
    pub expiring: u64,
    pub expires_max: u64,
}

impl SegmentMeta {
    fn encode(&self, w: &mut Writer) {
        w.str(&self.id);
        w.u8(self.level);
        w.varint(self.size);
        w.varint(self.entries);
        w.varint(self.tombstones);
        w.bytes(&self.min_key);
        w.bytes(&self.max_key);
        w.varint(self.min_seq);
        w.varint(self.max_seq);
        w.varint(self.deletes_applied);
        w.varint(self.expiring);
        w.varint(self.expires_max);
    }

    fn decode(r: &mut Reader<'_>) -> DResult<SegmentMeta> {
        let s = SegmentMeta {
            id: r.string()?,
            level: r.u8()?,
            size: r.varint()?,
            entries: r.varint()?,
            tombstones: r.varint()?,
            min_key: Bytes::copy_from_slice(r.bytes()?),
            max_key: Bytes::copy_from_slice(r.bytes()?),
            min_seq: r.varint()?,
            max_seq: r.varint()?,
            deletes_applied: r.varint()?,
            expiring: r.varint()?,
            expires_max: r.varint()?,
        };
        if !valid_object_id(&s.id) {
            return Err(DecodeError("bad segment id"));
        }
        Ok(s)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObjKind {
    Segment = 0,
    Blob = 1,
    /// External levels object; see [`Manifest::levels`].
    Levels = 2,
    /// An event table part or mask: every object under `part/{id}/`.
    Part = 3,
}

/// Tracks referenced blob bytes for size quotas, updated incrementally on commit and reconciled by sweeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct BlobBytes {
    /// Total bytes of all blobs bound by commits since inception.
    pub bound: u64,
    /// Total bytes of distinct referenced blobs discovered during the last sweep.
    pub swept: u64,
    /// Value of `bound` at the time of the last sweep.
    pub swept_at: u64,
}

impl BlobBytes {
    /// Estimated total bytes of referenced blobs, combining the last sweep total with subsequent bindings.
    pub fn total(&self) -> u64 {
        self.swept + self.bound.saturating_sub(self.swept_at)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Obsolete {
    pub kind: ObjKind,
    pub id: String,
    pub since_ms: u64,
}

const PAGE_KEY_MASK: u64 = 31;
const PAGE_MAX_BYTES: usize = 16 * 1024;

/// Caches compressed zstd frames for manifest tail pages keyed by their uncompressed SHA-256 digest.
#[derive(Default)]
pub struct PageCache {
    frames: std::collections::BTreeMap<[u8; 32], Vec<u8>>,
}

impl PageCache {
    pub fn heap_bytes(&self) -> usize {
        self.frames.values().map(|f| super::memory::alloc_bytes(f.len()) + 32 + 3 * super::memory::ALLOC_OVERHEAD).sum()
    }

    fn compress(&mut self, prefix: &[u8], tail: &[Vec<u8>], suffix: &[u8]) -> Option<Vec<u8>> {
        let compress = |b: &[u8]| crate::util::zstd::compress(b, ZSTD_LEVEL).ok();
        let mut out = compress(prefix)?;
        let mut frames = std::collections::BTreeMap::new();
        for page in tail {
            let digest = crate::util::sha256(page);
            let frame = match self.frames.remove(&digest) {
                Some(frame) => frame,
                None => compress(page)?,
            };
            out.extend_from_slice(&frame);
            frames.insert(digest, frame);
        }
        out.extend_from_slice(&compress(suffix)?);
        self.frames = frames;
        Some(out)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Manifest {
    pub db_id: String,
    pub version: Seq,
    /// Incremented on every manifest write, ordering manifests that share a data version.
    pub write_seq: u64,
    /// Non-decreasing store time in milliseconds of the latest manifest write,
    /// used by readers to evaluate expiries (SPEC §6.1).
    pub time_ms: u64,
    pub created_ms: u64,
    /// Bit flags; see [`FLAG_DELETED`].
    pub flags: u64,
    pub lease: Option<Lease>,
    /// Ordered by level. Level 0 is newest first; deeper levels are in key
    /// order. For any key, a shallower segment holds a newer entry than a
    /// deeper one.
    pub segments: Vec<SegmentMeta>,
    /// ID of the external levels object holding segments below L0, if not stored inline.
    pub levels: Option<String>,
    /// Incremented by every compaction install so concurrent compactions cannot interleave.
    pub compaction_seq: u64,
    /// Sorted by key; newest entry per key. Does not contain entries deleted by range tombstones.
    pub tail: Vec<Entry>,
    /// Active range tombstones in commit order.
    pub range_deletes: Vec<RangeTombstone>,
    /// Sequence number at or below which point and range tombstones may have been purged.
    pub tombstone_horizon: Seq,
    /// `(version, time_ms)` samples, oldest first.
    pub version_times: Vec<(Seq, u64)>,
    pub obsolete: Vec<Obsolete>,
    pub last_sweep_ms: u64,
    pub blob_bytes: BlobBytes,
    /// Bytes of sealed stream chunks, accounted like blob bytes.
    pub chunk_bytes: BlobBytes,
    /// Incremented by every tail flush install so stale background flushes are rejected.
    pub flush_seq: u64,
    /// Exact bytes of the event table parts and masks that references name
    /// (SPEC §5.10), maintained by every commit that changes a reference.
    pub part_bytes: u64,
}

impl Manifest {
    /// Creates an empty manifest with `store_ms` as the initial version time, or 0 if unknown.
    pub fn new(db_id: &str, now_ms: u64, store_ms: Option<u64>) -> Manifest {
        Manifest {
            db_id: db_id.to_string(),
            created_ms: now_ms,
            time_ms: store_ms.unwrap_or(0),
            last_sweep_ms: now_ms,
            version_times: vec![(0, now_ms)],
            ..Default::default()
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.flags & FLAG_DELETED != 0
    }

    pub fn heap_bytes(&self) -> usize {
        use super::memory::alloc_bytes;
        use std::mem::size_of;
        let tail: usize = self.tail.iter().map(|e| alloc_bytes(e.key.len()) + e.value.heap_bytes()).sum();
        let segments: usize = self
            .segments
            .iter()
            .map(|s| alloc_bytes(s.id.len()) + alloc_bytes(s.min_key.len()) + alloc_bytes(s.max_key.len()))
            .sum();
        let deletes: usize = self
            .range_deletes
            .iter()
            .map(|r| alloc_bytes(r.start.len()) + r.end.as_ref().map_or(0, |e| alloc_bytes(e.len())))
            .sum();
        let obsolete: usize = self.obsolete.iter().map(|o| alloc_bytes(o.id.len())).sum();
        size_of::<Manifest>()
            + alloc_bytes(self.db_id.len())
            + self.levels.as_ref().map_or(0, |l| alloc_bytes(l.len()))
            + self.lease.as_ref().map_or(0, |l| alloc_bytes(l.node.len()))
            + alloc_bytes(self.tail.capacity() * size_of::<Entry>())
            + tail
            + alloc_bytes(self.segments.capacity() * size_of::<SegmentMeta>())
            + segments
            + alloc_bytes(self.range_deletes.capacity() * size_of::<RangeTombstone>())
            + deletes
            + alloc_bytes(self.version_times.capacity() * size_of::<(Seq, u64)>())
            + alloc_bytes(self.obsolete.capacity() * size_of::<Obsolete>())
            + obsolete
    }

    pub fn tail_bytes(&self) -> usize {
        self.tail.iter().map(|e| e.size()).sum()
    }

    /// Total stored bytes tracked against quotas: segments, tail,
    /// referenced blobs and sealed stream chunks, excluding internal commit
    /// records.
    pub fn stored_bytes(&self) -> u64 {
        let segments: u64 = self.segments.iter().filter(|s| !is_commit_key(&s.min_key)).map(|s| s.size).sum();
        let tail: usize = self.tail.iter().filter(|e| !is_commit_key(&e.key)).map(|e| e.size()).sum();
        segments + tail as u64 + self.blob_bytes.total() + self.chunk_bytes.total() + self.part_bytes
    }

    pub fn encode(&self) -> Bytes {
        self.encode_with(&mut PageCache::default())
    }

    /// Encodes the manifest, reusing cached compressed tail pages from `pages`.
    pub fn encode_with(&self, pages: &mut PageCache) -> Bytes {
        let (prefix, tail, suffix) = self.encode_parts();
        let len = prefix.len() + tail.iter().map(Vec::len).sum::<usize>() + suffix.len();
        let mut w = Writer::with_capacity(16 + len);
        w.raw(MAGIC);
        w.u16(FORMAT);
        let compressed = pages.compress(&prefix, &tail, &suffix).filter(|c| c.len() + 10 < len - len / 8);
        match compressed {
            Some(c) => {
                w.u8(1);
                w.varint(len as u64);
                w.raw(&c);
            }
            None => {
                w.u8(0);
                w.raw(&prefix);
                tail.iter().for_each(|p| w.raw(p));
                w.raw(&suffix);
            }
        }
        let crc = crc32c(&w.buf);
        w.u32(crc);
        Bytes::from(w.into_inner())
    }

    #[cfg(test)]
    pub(super) fn encode_body(&self) -> Vec<u8> {
        let (prefix, tail, suffix) = self.encode_parts();
        [vec![prefix], tail, vec![suffix]].concat().concat()
    }

    fn encode_parts(&self) -> (Vec<u8>, Vec<Vec<u8>>, Vec<u8>) {
        let mut w = Writer::with_capacity(1024);
        w.str(&self.db_id);
        w.u64(self.version);
        w.u64(self.write_seq);
        w.varint(self.time_ms);
        w.u64(self.created_ms);
        w.varint(self.flags);
        match &self.lease {
            None => w.u8(0),
            Some(l) => {
                w.u8(1);
                w.str(&l.node);
                w.u64(l.expires_ms);
                w.varint(l.ms);
            }
        }
        let inline = match &self.levels {
            Some(_) => self.level(0),
            None => &self.segments[..],
        };
        w.varint(inline.len() as u64);
        for s in inline {
            s.encode(&mut w);
        }
        match &self.levels {
            None => w.u8(0),
            Some(id) => {
                w.u8(1);
                w.str(id);
            }
        }
        w.varint(self.compaction_seq);
        w.varint(self.tail.len() as u64);
        let prefix = w.into_inner();
        let mut pages = Vec::new();
        let page_capacity = 2 * PAGE_MAX_BYTES;
        let mut page = Writer::with_capacity(page_capacity);
        for e in &self.tail {
            e.encode(&mut page);
            if key_hash(&e.key) & PAGE_KEY_MASK == 0 || page.len() >= PAGE_MAX_BYTES {
                pages.push(std::mem::replace(&mut page, Writer::with_capacity(page_capacity)).into_inner());
            }
        }
        if !page.is_empty() {
            pages.push(page.into_inner());
        }
        let mut w = Writer::with_capacity(256);
        w.varint(self.range_deletes.len() as u64);
        for t in &self.range_deletes {
            t.encode(&mut w);
        }
        w.varint(self.tombstone_horizon);
        w.varint(self.version_times.len() as u64);
        for (v, t) in &self.version_times {
            w.varint(*v);
            w.varint(*t);
        }
        w.varint(self.obsolete.len() as u64);
        for o in &self.obsolete {
            w.u8(o.kind as u8);
            w.str(&o.id);
            w.varint(o.since_ms);
        }
        w.varint(self.last_sweep_ms);
        for b in [&self.blob_bytes, &self.chunk_bytes] {
            w.varint(b.bound);
            w.varint(b.swept);
            w.varint(b.swept_at);
        }
        w.varint(self.flush_seq);
        w.varint(self.part_bytes);
        (prefix, pages, w.into_inner())
    }

    pub fn decode(data: &Bytes) -> DResult<Manifest> {
        if data.len() < 10 {
            return Err(DecodeError("manifest too short"));
        }
        let (body, c) = data.split_at(data.len() - 4);
        if crc32c(body) != u32::from_le_bytes([c[0], c[1], c[2], c[3]]) {
            return Err(DecodeError("manifest checksum mismatch"));
        }
        let src = data.slice(0..body.len());
        let mut r = Reader::new(&src);
        if r.take(4)? != MAGIC {
            return Err(DecodeError("bad manifest magic"));
        }
        let format = r.u16()?;
        let src = match (format, r.u8()) {
            (FORMAT_V5, _) => src.slice(6..),
            (FORMAT | FORMAT_V7 | FORMAT_V6, Ok(0)) => src.slice(7..),
            (FORMAT | FORMAT_V7 | FORMAT_V6, Ok(1)) => {
                let n = r.varint()?;
                if n > MAX_BODY {
                    return Err(DecodeError("manifest body too large"));
                }
                let rest = r.take(r.remaining())?;
                let inflated = crate::util::zstd::decompress(rest, n as usize)
                    .map_err(|_| DecodeError("bad compressed manifest"))?;
                if inflated.len() as u64 != n {
                    return Err(DecodeError("manifest body length mismatch"));
                }
                Bytes::from(inflated)
            }
            _ => return Err(DecodeError("unsupported manifest format")),
        };
        let mut r = Reader::new(&src);
        let db_id = r.string()?;
        let version = r.u64()?;
        let write_seq = r.u64()?;
        let time_ms = r.varint()?;
        let created_ms = r.u64()?;
        let flags = r.varint()?;
        let lease = match r.u8()? {
            0 => None,
            1 => Some(Lease { node: r.string()?, expires_ms: r.u64()?, ms: r.varint()? }),
            _ => return Err(DecodeError("bad lease tag")),
        };
        let max = r.remaining();
        let n = r.len(max)?;
        let mut segments = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            segments.push(SegmentMeta::decode(&mut r)?);
        }
        let levels = match r.u8()? {
            0 => None,
            1 => {
                let id = r.string()?;
                if !valid_object_id(&id) {
                    return Err(DecodeError("bad levels id"));
                }
                if segments.iter().any(|s| s.level > 0) {
                    return Err(DecodeError("deep segments inline and in a levels object"));
                }
                Some(id)
            }
            _ => return Err(DecodeError("bad levels tag")),
        };
        let compaction_seq = r.varint()?;
        let n = r.len(r.remaining())?;
        let mut tail: Vec<Entry> = Vec::with_capacity(n.min(65536));
        for _ in 0..n {
            let e = Entry::decode(&mut r, &src)?;
            if let Some(p) = tail.last()
                && e.key <= p.key
            {
                return Err(DecodeError("tail not sorted"));
            }
            tail.push(e);
        }
        let n = r.len(r.remaining())?;
        let mut range_deletes = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            range_deletes.push(RangeTombstone::decode(&mut r)?);
        }
        let tombstone_horizon = r.varint()?;
        let n = r.len(r.remaining())?;
        let mut version_times = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            version_times.push((r.varint()?, r.varint()?));
        }
        let n = r.len(r.remaining())?;
        let mut obsolete = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            let kind = match r.u8()? {
                0 => ObjKind::Segment,
                1 => ObjKind::Blob,
                2 => ObjKind::Levels,
                3 => ObjKind::Part,
                _ => return Err(DecodeError("bad obsolete kind")),
            };
            let id = r.string()?;
            if !valid_object_id(&id) {
                return Err(DecodeError("bad obsolete id"));
            }
            obsolete.push(Obsolete { kind, id, since_ms: r.varint()? });
        }
        let last_sweep_ms = r.varint()?;
        let blob_bytes = BlobBytes { bound: r.varint()?, swept: r.varint()?, swept_at: r.varint()? };
        let chunk_bytes = BlobBytes { bound: r.varint()?, swept: r.varint()?, swept_at: r.varint()? };
        let flush_seq = if format >= FORMAT_V7 { r.varint()? } else { 0 };
        let part_bytes = if format >= FORMAT { r.varint()? } else { 0 };
        if !r.is_empty() {
            return Err(DecodeError("manifest trailing data"));
        }
        let m = Manifest {
            db_id,
            version,
            write_seq,
            time_ms,
            created_ms,
            flags,
            lease,
            segments,
            levels,
            compaction_seq,
            tail,
            range_deletes,
            tombstone_horizon,
            version_times,
            obsolete,
            last_sweep_ms,
            blob_bytes,
            chunk_bytes,
            flush_seq,
            part_bytes,
        };
        m.check_levels().map_err(DecodeError)?;
        Ok(m)
    }

    /// The segments below level 0.
    pub fn deep_segments(&self) -> &[SegmentMeta] {
        &self.segments[self.level(0).len()..]
    }

    /// Attaches segments from an external levels object to a decoded manifest.
    pub fn attach_levels(&mut self, levels: &Levels) -> DResult<()> {
        if self.levels.as_deref() != Some(levels.id.as_str()) || levels.db_id != self.db_id {
            return Err(DecodeError("levels object belongs to another manifest"));
        }
        if !self.deep_segments().is_empty() {
            return Err(DecodeError("levels already attached"));
        }
        self.segments.extend(levels.segments.iter().cloned());
        self.check_levels().map_err(DecodeError)
    }

    /// The segments of `level`.
    pub fn level(&self, level: u8) -> &[SegmentMeta] {
        let start = self.segments.partition_point(|s| s.level < level);
        let end = self.segments.partition_point(|s| s.level <= level);
        &self.segments[start..end]
    }

    pub fn level_bytes(&self, level: u8) -> u64 {
        self.level(level).iter().map(|s| s.size).sum()
    }

    /// Sorts segments by level, preserving recency order in level 0 and sorting
    /// deeper levels by key.
    pub fn sort_segments(&mut self) {
        self.segments.sort_by(|a, b| {
            a.level
                .cmp(&b.level)
                .then_with(|| if a.level == 0 { std::cmp::Ordering::Equal } else { a.min_key.cmp(&b.min_key) })
        });
    }

    /// Levels are in order, within range, and disjoint below level 0.
    pub fn check_levels(&self) -> Result<(), &'static str> {
        for w in self.segments.windows(2) {
            let (a, b) = (&w[0], &w[1]);
            if a.level > b.level {
                return Err("segments out of level order");
            }
            if a.level == b.level && a.level > 0 && a.max_key >= b.min_key {
                return Err("overlapping segments within a level");
            }
        }
        if self.segments.iter().any(|s| s.level > MAX_LEVEL || s.min_key > s.max_key) {
            return Err("bad segment level or key range");
        }
        Ok(())
    }

    /// Newest sampled version at least `age_ms` old at `now_ms`, or 0 if none.
    pub fn version_at_least_age(&self, now_ms: u64, age_ms: u64) -> Seq {
        let cutoff = now_ms.saturating_sub(age_ms);
        self.version_times.iter().filter(|(_, t)| *t <= cutoff).map(|(v, _)| *v).max().unwrap_or(0)
    }
}

/// Immutable levels object storing segment metadata below level 0.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Levels {
    pub db_id: String,
    pub id: String,
    pub segments: Vec<SegmentMeta>,
}

impl Levels {
    pub fn encode(&self) -> Bytes {
        let mut w = Writer::with_capacity(64 + 100 * self.segments.len());
        w.raw(LEVELS_MAGIC);
        w.u16(LEVELS_FORMAT);
        w.str(&self.db_id);
        w.str(&self.id);
        w.varint(self.segments.len() as u64);
        for s in &self.segments {
            s.encode(&mut w);
        }
        let crc = crc32c(&w.buf);
        w.u32(crc);
        Bytes::from(w.into_inner())
    }

    pub fn decode(data: &[u8]) -> DResult<Levels> {
        if data.len() < 10 {
            return Err(DecodeError("levels object too short"));
        }
        let (body, c) = data.split_at(data.len() - 4);
        if crc32c(body) != u32::from_le_bytes([c[0], c[1], c[2], c[3]]) {
            return Err(DecodeError("levels checksum mismatch"));
        }
        let mut r = Reader::new(body);
        if r.take(4)? != LEVELS_MAGIC {
            return Err(DecodeError("bad levels magic"));
        }
        if r.u16()? != LEVELS_FORMAT {
            return Err(DecodeError("unsupported levels format"));
        }
        let db_id = r.string()?;
        let id = r.string()?;
        let n = r.len(r.remaining())?;
        let mut segments = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            let s = SegmentMeta::decode(&mut r)?;
            if s.level == 0 {
                return Err(DecodeError("level 0 segment in a levels object"));
            }
            segments.push(s);
        }
        if !r.is_empty() {
            return Err(DecodeError("levels trailing data"));
        }
        Ok(Levels { db_id, id, segments })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::types::{BlobRef, Value};
    use crate::util::rng::Rng;

    fn sample() -> Manifest {
        let mut m = Manifest::new("db1", 123, Some(456));
        m.version = 42;
        m.write_seq = 50;
        m.flush_seq = 300;
        m.time_ms = 1_700_000_000_000;
        m.flags = FLAG_DELETED;
        m.lease = Some(Lease { node: "n1".into(), expires_ms: 999, ms: 5_000 });
        m.compaction_seq = 9;
        m.segments.push(SegmentMeta {
            id: "0".repeat(32),
            level: 0,
            size: 100,
            entries: 3,
            tombstones: 1,
            min_key: Bytes::from_static(b"a"),
            max_key: Bytes::from_static(b"z"),
            min_seq: 1,
            max_seq: 40,
            deletes_applied: 30,
            expiring: 2,
            expires_max: 1_700_000_000_500,
        });
        m.tail.push(Entry::new(Bytes::from_static(b"k1"), 41, Value::Inline(Bytes::from_static(b"v"))));
        m.tail.push(Entry::tombstone(Bytes::from_static(b"k2"), 42));
        m.tail.push(Entry {
            expires: Some(1_700_000_001_000),
            ..Entry::new(
                Bytes::from_static(b"k3"),
                42,
                Value::Blob(BlobRef { id: "a".repeat(32), size: 5, sha256: [1; 32], content_type: "x/y".into() }),
            )
        });
        m.range_deletes = vec![
            RangeTombstone { start: Bytes::from_static(b"m"), end: Some(Bytes::from_static(b"n")), seq: 38 },
            RangeTombstone { start: Bytes::new(), end: None, seq: 39 },
        ];
        m.tombstone_horizon = 7;
        m.blob_bytes = BlobBytes { bound: 500, swept: 200, swept_at: 400 };
        m.chunk_bytes = BlobBytes { bound: 70, swept: 60, swept_at: 50 };
        m.version_times = vec![(0, 100), (40, 200)];
        m.obsolete = vec![
            Obsolete { kind: ObjKind::Blob, id: "b".repeat(32), since_ms: 5 },
            Obsolete { kind: ObjKind::Levels, id: "c".repeat(32), since_ms: 6 },
        ];
        m
    }

    #[test]
    fn roundtrip() {
        let m = sample();
        let enc = m.encode();
        assert_eq!(Manifest::decode(&enc).unwrap(), m);
        assert_eq!(m.version_at_least_age(250, 100), 0);
        assert_eq!(m.version_at_least_age(400, 100), 40);
        assert_eq!(m.blob_bytes.total(), 300, "swept, plus bound since");
    }

    #[test]
    fn compressible_tails_are_stored_compressed() {
        let mut m = sample();
        m.tail = (0..200u64)
            .map(|i| {
                let value =
                    format!("lesson {i}: a vector space over a field is a set with two operations. ").repeat(10);
                Entry::new(Bytes::from(format!("k{i:04}")), i, Value::Inline(Bytes::from(value)))
            })
            .collect();
        let enc = m.encode();
        assert_eq!(enc[6], 1, "zstd codec");
        assert!(enc.len() * 4 < m.encode_body().len(), "{} of {} bytes", enc.len(), m.encode_body().len());
        assert_eq!(Manifest::decode(&enc).unwrap(), m);
    }

    #[test]
    fn tail_pages_are_compressed_once() {
        let mut r = Rng::new(4);
        let mut m = sample();
        m.tail = (0..2000u64)
            .map(|i| Entry::new(Bytes::from(format!("k{i:05}")), i, Value::Inline(Bytes::from(r.text(250)))))
            .collect();
        let mut pages = PageCache::default();
        let first = m.encode_with(&mut pages);
        assert_eq!(Manifest::decode(&first).unwrap(), m);
        let whole = crate::util::zstd::compress(&m.encode_body(), ZSTD_LEVEL).unwrap();
        assert!(first.len() < whole.len() * 5 / 4, "paged {} bytes, whole {} bytes", first.len(), whole.len());
        let frames: Vec<Vec<u8>> = pages.frames.values().cloned().collect();
        assert!(frames.len() > 20, "{} pages", frames.len());
        for (i, edit) in [7usize, 1234, 1999].into_iter().enumerate() {
            m.tail[edit].value = Value::Inline(Bytes::from(format!("edit {i}")));
            let key = Bytes::from(format!("k{edit:05}+"));
            let at = m.tail.partition_point(|e| e.key < key);
            m.tail.insert(at, Entry::new(key, 9_000 + i as u64, Value::Tombstone));
            m.write_seq += 1;
            let enc = m.encode_with(&mut pages);
            assert_eq!(Manifest::decode(&enc).unwrap(), m);
            assert_eq!(enc, m.encode(), "reused frames encode the same as fresh ones");
        }
        let kept = frames.iter().filter(|f| pages.frames.values().any(|g| g == *f)).count();
        assert!(kept + 6 >= frames.len(), "{kept} of {} pages kept their frames", frames.len());
    }

    fn encode_legacy_format(m: &Manifest, format: u16) -> Bytes {
        let flush_seq = if format == FORMAT_V7 { m.flush_seq } else { 0 };
        let body = Manifest { flush_seq, part_bytes: 0, ..m.clone() }.encode_body();
        let mut w = Writer::with_capacity(1024);
        w.raw(MAGIC);
        w.u16(format);
        if format != FORMAT_V5 {
            w.u8(0);
        }
        let trailing = if format == FORMAT_V7 { 1 } else { 2 };
        w.raw(&body[..body.len() - trailing]);
        let crc = crc32c(&w.buf);
        w.u32(crc);
        Bytes::from(w.into_inner())
    }

    #[test]
    fn decodes_legacy_manifest_formats() {
        let m = Manifest { flush_seq: 0, part_bytes: 0, ..sample() };
        for format in [FORMAT_V5, FORMAT_V6, FORMAT_V7] {
            assert_eq!(Manifest::decode(&encode_legacy_format(&m, format)).unwrap(), m, "format {format}");
        }
    }

    #[test]
    fn deeper_levels_in_an_object() {
        let mut m = sample();
        m.segments.extend([seg('c', 3, "a", "c"), seg('d', 5, "a", "z")]);
        m.sort_segments();
        let inline = m.encode();
        m.levels = Some("e".repeat(32));
        let out_of_line = m.encode();
        assert!(out_of_line.len() < inline.len());
        let mut d = Manifest::decode(&out_of_line).unwrap();
        assert_eq!(d.segments.len(), 1, "level 0 only, until attached");
        let levels = Levels { db_id: m.db_id.clone(), id: "e".repeat(32), segments: m.deep_segments().to_vec() };
        let enc = levels.encode();
        assert_eq!(Levels::decode(&enc).unwrap(), levels);
        let other = Levels { id: "f".repeat(32), ..levels.clone() };
        assert!(d.clone().attach_levels(&other).is_err(), "another manifest's object");
        d.attach_levels(&levels).unwrap();
        assert_eq!(d, m);
        assert!(d.attach_levels(&levels).is_err(), "attached twice");
        let mut bad = levels.clone();
        bad.segments.push(seg('g', 0, "a", "b"));
        assert!(Levels::decode(&bad.encode()).is_err(), "level 0 belongs inline");
        for n in 0..enc.len() {
            assert!(Levels::decode(&enc[..n]).is_err());
        }
    }

    fn seg(id: char, level: u8, min: &str, max: &str) -> SegmentMeta {
        SegmentMeta {
            id: id.to_string().repeat(32),
            level,
            size: 10,
            entries: 1,
            tombstones: 0,
            min_key: Bytes::copy_from_slice(min.as_bytes()),
            max_key: Bytes::copy_from_slice(max.as_bytes()),
            min_seq: 0,
            max_seq: 0,
            deletes_applied: 0,
            expiring: 0,
            expires_max: 0,
        }
    }

    #[test]
    fn levels() {
        let mut m = Manifest::new("d", 0, None);
        m.segments = vec![seg('a', 0, "a", "z"), seg('b', 0, "b", "c"), seg('c', 2, "m", "p"), seg('d', 2, "a", "c")];
        assert!(m.check_levels().is_err(), "level 2 out of key order");
        m.sort_segments();
        assert!(m.check_levels().is_ok());
        let ids = |l: &[SegmentMeta]| l.iter().map(|s| &s.id[..1]).collect::<String>();
        assert_eq!((ids(m.level(0)), ids(m.level(1)), ids(m.level(2))), ("ab".into(), "".into(), "dc".into()));
        assert_eq!(m.level_bytes(2), 20);
        assert!(Manifest::decode(&m.encode()).is_ok());
        let mut overlapping = m.clone();
        overlapping.segments.push(seg('e', 2, "p", "q"));
        assert!(overlapping.check_levels().is_err(), "keys shared by neighbors in a level");
        assert!(Manifest::decode(&overlapping.encode()).is_err());
        let mut deep = m.clone();
        deep.segments.push(seg('f', MAX_LEVEL + 1, "a", "b"));
        assert!(deep.check_levels().is_err());
        let mut inverted = m;
        inverted.segments[0] = seg('a', 0, "z", "a");
        assert!(inverted.check_levels().is_err());
    }

    #[test]
    fn fuzz_decode_never_panics() {
        let mut out_of_line = sample();
        out_of_line.levels = Some("e".repeat(32));
        let levels = Levels {
            db_id: "db1".into(),
            id: "e".repeat(32),
            segments: vec![seg('c', 3, "a", "c"), seg('d', 5, "d", "z")],
        };
        let goods = [sample().encode().to_vec(), out_of_line.encode().to_vec(), levels.encode().to_vec()];
        let mut r = Rng::new(5);
        for i in 0..8000 {
            let mut d = if i % 3 == 0 {
                let n = r.usize(200);
                r.bytes(n)
            } else {
                goods[i % goods.len()].clone()
            };
            if !d.is_empty() {
                for _ in 0..1 + r.usize(3) {
                    let p = r.usize(d.len());
                    d[p] = r.next_u32() as u8;
                }
            }
            if i % 5 == 0 {
                d.truncate(r.usize(d.len() + 1));
            }
            // Re-checksum some mutated inputs so decoding goes deeper.
            if i % 2 == 1 && d.len() > 4 {
                let n = d.len() - 4;
                let c = crc32c(&d[..n]).to_le_bytes();
                d[n..].copy_from_slice(&c);
            }
            if let Ok(l) = Levels::decode(&d) {
                let _ = out_of_line.clone().attach_levels(&l);
            }
            let _ = Manifest::decode(&Bytes::from(d));
        }
    }
}
