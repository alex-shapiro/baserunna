//! Immutable database snapshots and read iterators.

use super::cache::{CacheKey, CacheVal};
use super::manifest::{Levels, MAX_LEVEL, Manifest, SegmentMeta};
use super::range_delete::{RangeDeletes, RangeTombstone};
use super::segment::{self, Block, FOOTER_LEN, Footer, SegmentIndex};
use super::types::{COMMIT_SPACE, Entry, Seq};
use super::{Shared, levels_key, manifest_key, seg_key};
use crate::error::{Error, Result};
use crate::store::{GetOpts, GetResult, StoreError};
use bytes::Bytes;
use std::collections::{BinaryHeap, VecDeque};
use std::ops::Bound;
use std::sync::Arc;

/// An immutable, consistent view of a database at `m.version`.
pub struct Version {
    pub m: Manifest,
    pub etag: String,
    /// Object-store `last_modified` timestamp (in ms) when this manifest was loaded from storage.
    pub stored_ms: Option<u64>,
    pub(crate) shared: Arc<Shared>,
    /// `m.range_deletes`, indexed for lookups.
    deletes: Arc<RangeDeletes>,
    /// True if any stored value has an expiration timestamp.
    pub(crate) expiring: bool,
    charge: super::memory::Charge,
}

/// Owned key range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRange {
    pub start: Bound<Bytes>,
    pub end: Bound<Bytes>,
}

impl KeyRange {
    pub fn all() -> KeyRange {
        KeyRange { start: Bound::Unbounded, end: Bound::Unbounded }
    }

    /// Key range covering all user data keys below `COMMIT_SPACE`.
    pub fn data() -> KeyRange {
        KeyRange { start: Bound::Unbounded, end: Bound::Excluded(Bytes::from_static(&[COMMIT_SPACE])) }
    }

    pub fn new(start: Bound<Bytes>, end: Bound<Bytes>) -> KeyRange {
        KeyRange { start, end }
    }

    /// `[start, end)`.
    pub fn half_open(start: Bytes, end: Option<Bytes>) -> KeyRange {
        KeyRange { start: Bound::Included(start), end: end.map(Bound::Excluded).unwrap_or(Bound::Unbounded) }
    }

    /// The range holding only `k`.
    pub fn point(k: &Bytes) -> KeyRange {
        KeyRange::new(Bound::Included(k.clone()), Bound::Included(k.clone()))
    }

    pub fn prefix(p: &[u8]) -> KeyRange {
        KeyRange::half_open(Bytes::copy_from_slice(p), crate::util::prefix_end(p).map(Bytes::from))
    }

    pub fn heap_bytes(&self) -> usize {
        let b = |b: &Bound<Bytes>| match b {
            Bound::Included(k) | Bound::Excluded(k) => super::memory::alloc_bytes(k.len()),
            Bound::Unbounded => 0,
        };
        b(&self.start) + b(&self.end)
    }

    pub fn contains(&self, k: &[u8]) -> bool {
        let lo = match &self.start {
            Bound::Included(s) => k >= s.as_ref(),
            Bound::Excluded(s) => k > s.as_ref(),
            Bound::Unbounded => true,
        };
        let hi = match &self.end {
            Bound::Included(e) => k <= e.as_ref(),
            Bound::Excluded(e) => k < e.as_ref(),
            Bound::Unbounded => true,
        };
        lo && hi
    }

    /// Returns true if `k` is below `self.end`.
    pub fn before_end(&self, k: &[u8]) -> bool {
        match &self.end {
            Bound::Included(e) => k <= e.as_ref(),
            Bound::Excluded(e) => k < e.as_ref(),
            Bound::Unbounded => true,
        }
    }

    pub fn after_start(&self, k: &[u8]) -> bool {
        match &self.start {
            Bound::Included(s) => k >= s.as_ref(),
            Bound::Excluded(s) => k > s.as_ref(),
            Bound::Unbounded => true,
        }
    }

    pub fn is_empty(&self) -> bool {
        match (&self.start, &self.end) {
            (Bound::Included(s), Bound::Excluded(e)) => s >= e,
            (Bound::Included(s), Bound::Included(e)) => s > e,
            (Bound::Excluded(s), Bound::Excluded(e)) => s >= e,
            (Bound::Excluded(s), Bound::Included(e)) => s >= e,
            _ => false,
        }
    }

    pub fn overlaps_keys(&self, min: &[u8], max: &[u8]) -> bool {
        self.after_start(max) && self.before_end(min)
    }

    /// Returns the intersection of `self` and `other`.
    pub fn intersect(&self, other: &KeyRange) -> KeyRange {
        use std::cmp::Ordering::*;
        let start = match (&self.start, &other.start) {
            (Bound::Unbounded, b) | (b, Bound::Unbounded) => b.clone(),
            (Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y)) => match x.cmp(y) {
                Less => other.start.clone(),
                Greater => self.start.clone(),
                Equal if matches!(self.start, Bound::Excluded(_)) => self.start.clone(),
                Equal => other.start.clone(),
            },
        };
        let end = match (&self.end, &other.end) {
            (Bound::Unbounded, b) | (b, Bound::Unbounded) => b.clone(),
            (Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y)) => match x.cmp(y) {
                Less => self.end.clone(),
                Greater => other.end.clone(),
                Equal if matches!(self.end, Bound::Excluded(_)) => self.end.clone(),
                Equal => other.end.clone(),
            },
        };
        KeyRange { start, end }
    }

    pub fn intersects(&self, other: &KeyRange) -> bool {
        let a_before_b = match (&self.end, &other.start) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
            (Bound::Excluded(e), Bound::Included(s)) => e <= s,
            (Bound::Excluded(e), Bound::Excluded(s)) => e <= s,
            (Bound::Included(e), Bound::Excluded(s)) => e <= s,
            (Bound::Included(e), Bound::Included(s)) => e < s,
        };
        let b_before_a = match (&other.end, &self.start) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
            (Bound::Excluded(e), Bound::Included(s)) => e <= s,
            (Bound::Excluded(e), Bound::Excluded(s)) => e <= s,
            (Bound::Included(e), Bound::Excluded(s)) => e <= s,
            (Bound::Included(e), Bound::Included(s)) => e < s,
        };
        !(a_before_b || b_before_a || self.is_empty() || other.is_empty())
    }
}

impl Shared {
    /// Decodes a manifest and attaches deep-level segment metadata from `current` or from the
    /// external levels object. Returns `Err(LevelsMissing)` if the external levels object was
    /// deleted by garbage collection.
    pub(crate) async fn load_manifest(
        &self,
        db: &str,
        data: &Bytes,
        current: Option<&Version>,
    ) -> Result<std::result::Result<Manifest, LevelsMissing>> {
        let mut m = Manifest::decode(data)?;
        if m.db_id != db {
            return Err(Error::internal("manifest belongs to another database"));
        }
        let Some(id) = m.levels.clone() else { return Ok(Ok(m)) };
        if let Some(c) = current.filter(|c| c.m.levels.as_deref() == Some(id.as_str())) {
            m.segments.extend_from_slice(c.m.deep_segments());
            m.check_levels().map_err(Error::internal)?;
            return Ok(Ok(m));
        }
        let Some(l) = self.levels(db, &id).await? else { return Ok(Err(LevelsMissing(id))) };
        m.attach_levels(&l)?;
        Ok(Ok(m))
    }

    async fn levels(&self, db: &str, id: &str) -> Result<Option<Arc<Levels>>> {
        let key = levels_key(db, id);
        let ck = CacheKey::levels(db, id);
        if let Some(CacheVal::Levels(l)) = self.cache.get(&ck) {
            return Ok(Some(l));
        }
        let tier = self.cfg.local_tier.as_ref();
        let local = match tier {
            Some(t) => t.0.get(&key).await.and_then(|d| Levels::decode(&d).ok()),
            None => None,
        };
        let l = match local {
            Some(l) => l,
            None => {
                let g = match self.retry.run(|| self.store.get(&key, GetOpts::default())).await {
                    Err(StoreError::NotFound) => return Ok(None),
                    g => g?,
                };
                let l = Levels::decode(&g.data)?;
                if let Some(t) = tier.cloned() {
                    let k = key.clone();
                    self.env.spawn(async move { t.0.put(&k, g.data).await });
                }
                l
            }
        };
        if l.db_id != db || l.id != id {
            return Err(Error::internal("levels object belongs to another database"));
        }
        let l = Arc::new(l);
        self.cache.insert(ck, CacheVal::Levels(l.clone()));
        Ok(Some(l))
    }

    /// Writes a new levels object under a random ID and populates local caches.
    pub(crate) async fn write_levels(&self, db: &str, segments: Vec<SegmentMeta>) -> Result<String> {
        let id = self.env.random_hex(16);
        let l = Levels { db_id: db.to_string(), id: id.clone(), segments };
        let data = l.encode();
        let key = levels_key(db, &id);
        self.retry.run(|| self.store.put(&key, data.clone(), crate::store::PutMode::Overwrite)).await?;
        crate::probe::hit(crate::probe::Probe::LevelsObjectWritten);
        self.cache.insert(CacheKey::levels(db, &id), CacheVal::Levels(Arc::new(l)));
        if let Some(t) = self.cfg.local_tier.clone() {
            self.env.spawn(async move { t.0.put(&key, data).await });
        }
        Ok(id)
    }

    fn local_key(db: &str, seg: &str, part: &str) -> String {
        format!("{}#{part}", seg_key(db, seg))
    }

    async fn local_get(&self, db: &str, seg: &str, part: &str) -> Option<Bytes> {
        self.cfg.local_tier.as_ref()?.0.get(&Self::local_key(db, seg, part)).await
    }

    fn local_contains(&self, db: &str, seg: &str, part: &str) -> bool {
        self.cfg.local_tier.as_ref().is_some_and(|t| t.0.contains(&Self::local_key(db, seg, part)))
    }

    fn local_put(&self, db: &str, seg: &str, part: &str, data: Bytes) {
        if let Some(t) = self.cfg.local_tier.clone() {
            let key = Self::local_key(db, seg, part);
            self.env.spawn(async move { t.0.put(&key, data).await });
        }
    }

    fn cache_whole(&self, db: &str, seg: &str, data: &Bytes) -> Result<(Arc<SegmentIndex>, Vec<Block>)> {
        let idx = Arc::new(SegmentIndex::decode_whole(data)?);
        let blocks = segment::decode_blocks(&idx, 0, data)?;
        for (bi, b) in blocks.iter().enumerate() {
            self.cache.insert(CacheKey::block(db, seg, bi), CacheVal::Block(b.clone()));
        }
        self.cache.insert(CacheKey::index(db, seg), CacheVal::Index(idx.clone()));
        Ok((idx, blocks))
    }

    /// Populates the local tier and in-memory index cache for a newly written segment, and caches
    /// decoded blocks in memory if `hot` is true.
    pub(crate) fn remember_segment(&self, db: &str, seg: &str, data: &Bytes, hot: bool) -> Result<()> {
        let idx = if hot {
            self.cache_whole(db, seg, data)?.0
        } else {
            let idx = Arc::new(SegmentIndex::decode_whole(data)?);
            self.cache.insert(CacheKey::index(db, seg), CacheVal::Index(idx.clone()));
            idx
        };
        if idx.size <= self.whole_fetch_max() {
            self.local_put(db, seg, "all", data.clone());
        } else {
            self.local_put(db, seg, "meta", data.slice(idx.footer.bloom_off as usize..));
            for (bi, (_, off, len)) in idx.blocks.iter().enumerate() {
                self.local_put(db, seg, &format!("b{bi}"), data.slice(*off as usize..(*off + *len as u64) as usize));
            }
        }
        Ok(())
    }

    fn is_small_segment(&self, seg: &SegmentMeta) -> bool {
        seg.size <= self.whole_fetch_max()
    }

    fn whole_fetch_max(&self) -> u64 {
        self.cfg.segment_whole_fetch_max.max(FOOTER_LEN as u64)
    }

    #[cfg(test)]
    pub(crate) async fn seg_index(&self, db: &str, seg: &SegmentMeta) -> Result<Arc<SegmentIndex>> {
        Ok(self.open_segment(db, seg, ReadMode::Cached).await?.0)
    }

    /// Loads the index for `seg`. If a small segment is fetched in full, also returns its decoded blocks.
    async fn open_segment(
        &self,
        db: &str,
        seg: &SegmentMeta,
        mode: ReadMode,
    ) -> Result<(Arc<SegmentIndex>, Option<Vec<Block>>)> {
        let ck = CacheKey::index(db, &seg.id);
        let cached = match mode {
            ReadMode::Cached => self.cache.get(&ck),
            ReadMode::Bulk => self.cache.peek(&ck),
        };
        if let Some(CacheVal::Index(i)) = cached {
            return Ok((i, None));
        }
        if self.is_small_segment(seg) {
            if mode == ReadMode::Bulk {
                return self.read_whole_segment(db, seg).await.map(|(i, b)| (i, Some(b)));
            }
            if let Some(d) = self.local_get(db, &seg.id, "all").await
                && let Ok((idx, blocks)) = self.cache_whole(db, &seg.id, &d)
            {
                return Ok((idx, Some(blocks)));
            }
            let g = self.seg_get(db, &seg.id, GetOpts::default()).await?;
            let (idx, blocks) = self.cache_whole(db, &seg.id, &g.data)?;
            self.local_put(db, &seg.id, "all", g.data);
            return Ok((idx, Some(blocks)));
        }
        let idx = Arc::new(self.load_index(db, seg, mode == ReadMode::Cached).await?);
        if mode == ReadMode::Cached {
            self.cache.insert(ck, CacheVal::Index(idx.clone()));
        }
        Ok((idx, None))
    }

    /// Loads the bloom filter and block index of a large segment from the local tier or object storage.
    async fn load_index(&self, db: &str, seg: &SegmentMeta, keep: bool) -> Result<SegmentIndex> {
        let size = seg.size;
        let parse = |meta: &Bytes| -> Result<SegmentIndex> {
            let n = meta.len();
            if n < FOOTER_LEN {
                return Err(Error::internal("short segment metadata"));
            }
            let footer = Footer::decode(&meta[n - FOOTER_LEN..], size)?;
            let start = size - n as u64;
            if footer.bloom_off != start {
                return Err(Error::internal("segment metadata misaligned"));
            }
            let bloom = &meta[..footer.bloom_len as usize];
            let index = &meta
                [(footer.index_off - start) as usize..(footer.index_off - start) as usize + footer.index_len as usize];
            Ok(SegmentIndex::decode(footer, index, bloom, size)?)
        };
        if let Some(meta) = self.local_get(db, &seg.id, "meta").await
            && let Ok(idx) = parse(&meta)
        {
            return Ok(idx);
        }
        let g = self
            .seg_get(db, &seg.id, GetOpts { range: Some(size - FOOTER_LEN as u64..size), ..Default::default() })
            .await?;
        let footer = Footer::decode(&g.data, size)?;
        let start = footer.bloom_off;
        let g = self.seg_get(db, &seg.id, GetOpts { range: Some(start..size), ..Default::default() }).await?;
        if g.data.len() as u64 != size - start {
            return Err(Error::internal("short segment read"));
        }
        let idx = parse(&g.data)?;
        if keep {
            self.local_put(db, &seg.id, "meta", g.data);
        }
        Ok(idx)
    }

    async fn read_whole_segment(&self, db: &str, seg: &SegmentMeta) -> Result<(Arc<SegmentIndex>, Vec<Block>)> {
        let decode = |d: &Bytes| -> Result<(Arc<SegmentIndex>, Vec<Block>)> {
            let idx = Arc::new(SegmentIndex::decode_whole(d)?);
            let blocks = segment::decode_blocks(&idx, 0, d)?;
            Ok((idx, blocks))
        };
        if let Some(d) = self.local_get(db, &seg.id, "all").await
            && let Ok(opened) = decode(&d)
        {
            return Ok(opened);
        }
        decode(&self.seg_get(db, &seg.id, GetOpts::default()).await?.data)
    }

    /// Reads from segment `seg` of `db`. Returns a retryable error if garbage collection removed
    /// the segment after the reading version was superseded, or an internal error if the current
    /// manifest still references `seg`.
    async fn seg_get(&self, db: &str, seg: &str, opts: GetOpts) -> Result<GetResult> {
        let key = seg_key(db, seg);
        match self.retry.run(|| self.store.get(&key, opts.clone())).await {
            Err(StoreError::NotFound) => Err(self.missing_segment_error(db, seg).await),
            r => Ok(r?),
        }
    }

    async fn missing_segment_error(&self, db: &str, seg: &str) -> Error {
        let key = manifest_key(db);
        let m = match self.retry.run(|| self.store.get(&key, GetOpts::default())).await {
            Ok(g) => self.load_manifest(db, &g.data, None).await,
            Err(StoreError::NotFound) => return Error::not_found("database not found"),
            Err(e) => return e.into(),
        };
        match m {
            Ok(Ok(m)) if m.segments.iter().any(|s| s.id == seg) => Error::internal(format!("segment {seg} is missing")),
            Ok(_) => {
                crate::probe::hit(crate::probe::Probe::SegmentCollectedUnderRead);
                Error::unavailable("garbage collection removed a segment this read needed; retry")
            }
            Err(e) => e,
        }
    }

    fn cached_block(&self, db: &str, seg: &str, bi: usize) -> Option<Block> {
        match self.cache.get(&CacheKey::block(db, seg, bi)) {
            Some(CacheVal::Block(b)) => Some(b),
            _ => None,
        }
    }

    fn peek_block(&self, db: &str, seg: &str, bi: usize) -> Option<Block> {
        match self.cache.peek(&CacheKey::block(db, seg, bi)) {
            Some(CacheVal::Block(b)) => Some(b),
            _ => None,
        }
    }

    fn is_block_cached(&self, db: &str, seg: &SegmentMeta, bi: usize) -> bool {
        let part = if self.is_small_segment(seg) { "all".to_string() } else { format!("b{bi}") };
        self.cache.contains(&CacheKey::block(db, &seg.id, bi)) || self.local_contains(db, &seg.id, &part)
    }

    fn local_put_block(&self, db: &str, seg: &SegmentMeta, bi: usize, data: Bytes) {
        if !self.is_small_segment(seg) {
            self.local_put(db, &seg.id, &format!("b{bi}"), data);
        }
    }

    pub(crate) async fn seg_block(&self, db: &str, seg: &SegmentMeta, idx: &SegmentIndex, bi: usize) -> Result<Block> {
        if let Some(b) = self.cached_block(db, &seg.id, bi) {
            return Ok(b);
        }
        match self.local_block(db, seg, bi, true).await {
            Some(b) => Ok(b),
            None => self.fetch_block(db, seg, idx, bi).await,
        }
    }

    async fn local_block(&self, db: &str, seg: &SegmentMeta, bi: usize, keep: bool) -> Option<Block> {
        if self.is_small_segment(seg) {
            let data = self.local_get(db, &seg.id, "all").await?;
            if keep {
                return self.cache_whole(db, &seg.id, &data).ok()?.1.into_iter().nth(bi);
            }
            let idx = SegmentIndex::decode_whole(&data).ok()?;
            let (_, off, len) = *idx.blocks.get(bi)?;
            return segment::decode_block(data.slice(off as usize..(off + len as u64) as usize)).ok();
        }
        let b = segment::decode_block(self.local_get(db, &seg.id, &format!("b{bi}")).await?).ok()?;
        if keep {
            self.cache.insert(CacheKey::block(db, &seg.id, bi), CacheVal::Block(b.clone()));
        }
        Some(b)
    }

    async fn fetch_block(&self, db: &str, seg: &SegmentMeta, idx: &SegmentIndex, bi: usize) -> Result<Block> {
        let (_, off, len) = idx.blocks[bi];
        let g = self.seg_get(db, &seg.id, GetOpts { range: Some(off..off + len as u64), ..Default::default() }).await?;
        if g.data.len() != len as usize {
            return Err(Error::internal("short block read"));
        }
        let b = segment::decode_block(g.data.clone())?;
        self.cache.insert(CacheKey::block(db, &seg.id, bi), CacheVal::Block(b.clone()));
        self.local_put_block(db, seg, bi, g.data);
        Ok(b)
    }

    /// Fetches a contiguous range of blocks from `seg` in a single request.
    async fn seg_blocks_bulk(
        &self,
        db: &str,
        seg: &SegmentMeta,
        idx: &SegmentIndex,
        blocks: std::ops::Range<usize>,
        cache: bool,
    ) -> Result<Vec<Block>> {
        let span = idx.span(blocks.clone());
        let g = self.seg_get(db, &seg.id, GetOpts { range: Some(span.clone()), ..Default::default() }).await?;
        if g.data.len() as u64 != span.end - span.start {
            return Err(Error::internal("short segment read"));
        }
        let mut decoded = segment::decode_blocks(idx, blocks.start, &g.data)?;
        decoded.truncate(blocks.len());
        if decoded.len() != blocks.len() {
            return Err(Error::internal("short segment read"));
        }
        if cache {
            for (b, bi) in decoded.iter().zip(blocks) {
                self.cache.insert(CacheKey::block(db, &seg.id, bi), CacheVal::Block(b.clone()));
                let (_, off, len) = idx.blocks[bi];
                let at = (off - span.start) as usize;
                self.local_put_block(db, seg, bi, g.data.slice(at..at + len as usize));
            }
        }
        Ok(decoded)
    }
}

pub(crate) struct LevelsMissing(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReadMode {
    /// Populates caches and prefetches blocks after two single-block fetches in a forward scan.
    Cached,
    /// Uses existing cache entries without inserting new ones, and fetches uncached blocks in bulk.
    Bulk,
}

/// Iterates entries across a sorted run of segments (either a single L0 segment
/// or the non-overlapping segments of a deeper level).
struct RunCursor {
    segs: VecDeque<SegmentMeta>,
    seg: Option<(SegmentMeta, Arc<SegmentIndex>)>,
    /// Decoded blocks if the current segment was read in full.
    decoded_blocks: Option<Vec<Block>>,
    blocks: VecDeque<usize>,
    ready: VecDeque<Block>,
    fetched: u32,
    cur: Option<Block>,
    pos: usize,
}

enum Cursor {
    Tail { entries: Vec<Entry>, pos: usize },
    Run(Box<RunCursor>),
}

/// Configuration for [`MergeIter`].
pub(crate) struct IterSpec {
    pub range: KeyRange,
    pub reverse: bool,
    pub include_tombstones: bool,
    pub mode: ReadMode,
    /// Range tombstones used to filter out covered entries.
    pub deletes: Option<Arc<RangeDeletes>>,
    /// Store timestamp at which expired entries are converted to tombstones.
    pub expire_at: Option<u64>,
}

/// Lazily merges entries across a database's sources in key order, yielding the
/// newest entry for each key.
pub struct MergeIter {
    shared: Arc<Shared>,
    db: String,
    range: KeyRange,
    reverse: bool,
    include_tombstones: bool,
    mode: ReadMode,
    deletes: Option<Arc<RangeDeletes>>,
    expire_at: Option<u64>,
    cursors: Vec<Cursor>,
    heads: Option<Vec<Option<Entry>>>,
}

impl MergeIter {
    pub(crate) fn new(
        shared: Arc<Shared>,
        db: &str,
        tail: &[Entry],
        runs: Vec<Vec<SegmentMeta>>,
        spec: IterSpec,
    ) -> MergeIter {
        let IterSpec { range, reverse, include_tombstones, mode, deletes, expire_at } = spec;
        let deletes = deletes.filter(|d| !d.is_empty());
        debug_assert!(!(reverse && mode == ReadMode::Bulk));
        let mut cursors = Vec::with_capacity(runs.len() + 1);
        let mut entries: Vec<Entry> = tail.iter().filter(|e| range.contains(&e.key)).cloned().collect();
        if reverse {
            entries.reverse();
        }
        cursors.push(Cursor::Tail { entries, pos: 0 });
        for mut run in runs {
            run.retain(|s| range.overlaps_keys(&s.min_key, &s.max_key));
            if reverse {
                run.reverse();
            }
            if !run.is_empty() {
                cursors.push(Cursor::Run(Box::new(RunCursor {
                    segs: run.into(),
                    seg: None,
                    decoded_blocks: None,
                    blocks: VecDeque::new(),
                    ready: VecDeque::new(),
                    fetched: 0,
                    cur: None,
                    pos: 0,
                })));
            }
        }
        MergeIter {
            shared,
            db: db.to_string(),
            range,
            reverse,
            include_tombstones,
            mode,
            deletes,
            expire_at,
            cursors,
            heads: None,
        }
    }

    async fn peek(&self, c: &mut Cursor) -> Result<Option<Entry>> {
        let r = match c {
            Cursor::Tail { entries, pos } => return Ok(entries.get(*pos).cloned()),
            Cursor::Run(r) => r,
        };
        let (range, reverse) = (&self.range, self.reverse);
        loop {
            if let Some(b) = &r.cur {
                while r.pos < b.len() {
                    let e = if reverse { &b[b.len() - 1 - r.pos] } else { &b[r.pos] };
                    if range.contains(&e.key) {
                        return Ok(Some(e.clone()));
                    }
                    let past = if reverse { !range.after_start(&e.key) } else { !range.before_end(&e.key) };
                    if past {
                        r.segs.clear();
                        r.blocks.clear();
                        r.ready.clear();
                        r.cur = None;
                        return Ok(None);
                    }
                    r.pos += 1;
                }
                r.cur = None;
            }
            if let Some(b) = r.ready.pop_front() {
                r.cur = Some(b);
                r.pos = 0;
                continue;
            }
            if let Some(bi) = r.blocks.pop_front() {
                match self.mode {
                    ReadMode::Cached => self.load_cached(r, bi).await?,
                    ReadMode::Bulk => self.load_bulk(r, bi).await?,
                }
                r.pos = 0;
                continue;
            }
            let Some(meta) = r.segs.pop_front() else {
                r.seg = None;
                return Ok(None);
            };
            let (idx, decoded_blocks) = self.shared.open_segment(&self.db, &meta, self.mode).await?;
            let first = match &range.start {
                Bound::Included(s) | Bound::Excluded(s) => idx.block_for(s),
                Bound::Unbounded => Some(0),
            };
            let mut blocks = VecDeque::new();
            if let Some(first) = first {
                for bi in first..idx.blocks.len() {
                    blocks.push_back(bi);
                    if !range.before_end(&idx.blocks[bi].0) {
                        break;
                    }
                }
            }
            if reverse {
                blocks = blocks.into_iter().rev().collect();
            }
            r.blocks = blocks;
            r.fetched = 0;
            r.seg = Some((meta, idx));
            r.decoded_blocks = decoded_blocks;
        }
    }

    async fn load_cached(&self, r: &mut RunCursor, bi: usize) -> Result<()> {
        let (meta, idx) = r.seg.as_ref().ok_or_else(|| Error::internal("run cursor without a segment"))?;
        if let Some(b) = r.decoded_blocks.as_ref().and_then(|w| w.get(bi)) {
            r.cur = Some(b.clone());
        } else if let Some(b) = self.shared.cached_block(&self.db, &meta.id, bi) {
            r.cur = Some(b);
        } else if let Some(b) = self.shared.local_block(&self.db, meta, bi, true).await {
            r.cur = Some(b);
        } else if self.reverse || r.fetched < 2 {
            r.fetched += 1;
            r.cur = Some(self.shared.fetch_block(&self.db, meta, idx, bi).await?);
        } else {
            self.read_ahead(r, bi, true).await?;
        }
        Ok(())
    }

    async fn load_bulk(&self, r: &mut RunCursor, bi: usize) -> Result<()> {
        let (meta, _) = r.seg.as_ref().ok_or_else(|| Error::internal("run cursor without a segment"))?;
        if let Some(b) = r.decoded_blocks.as_ref().and_then(|w| w.get(bi)) {
            r.cur = Some(b.clone());
        } else if let Some(b) = self.shared.peek_block(&self.db, &meta.id, bi) {
            r.cur = Some(b);
        } else if self.shared.is_small_segment(meta) {
            let blocks = self.shared.read_whole_segment(&self.db, meta).await?.1;
            r.cur = Some(blocks.get(bi).cloned().ok_or(Error::internal("short segment"))?);
            r.decoded_blocks = Some(blocks);
        } else if let Some(b) = self.shared.local_block(&self.db, meta, bi, false).await {
            r.cur = Some(b);
        } else {
            self.read_ahead(r, bi, false).await?;
        }
        Ok(())
    }

    /// Fetches block `bi` and consecutive uncached blocks up to `bulk_read_bytes` in a single request.
    async fn read_ahead(&self, r: &mut RunCursor, bi: usize, cache: bool) -> Result<()> {
        let (meta, idx) = r.seg.as_ref().ok_or_else(|| Error::internal("run cursor without a segment"))?;
        let mut end = bi + 1;
        while let Some(&next) = r.blocks.get(end - bi - 1) {
            let span = idx.span(bi..next + 1);
            if next != end
                || span.end - span.start > self.shared.cfg.bulk_read_bytes
                || self.shared.is_block_cached(&self.db, meta, next)
            {
                break;
            }
            end += 1;
        }
        let mut got = self.shared.seg_blocks_bulk(&self.db, meta, idx, bi..end, cache).await?.into_iter();
        r.blocks.drain(..end - bi - 1);
        r.cur = got.next();
        r.ready.extend(got);
        Ok(())
    }

    fn advance(c: &mut Cursor) {
        match c {
            Cursor::Tail { pos, .. } => *pos += 1,
            Cursor::Run(r) => r.pos += 1,
        }
    }

    /// Returns the newest entry for the next key, or `None` at the end.
    pub async fn next(&mut self) -> Result<Option<Entry>> {
        let mut cursors = std::mem::take(&mut self.cursors);
        let r = self.next_from(&mut cursors).await;
        self.cursors = cursors;
        r
    }

    async fn next_from(&mut self, cursors: &mut [Cursor]) -> Result<Option<Entry>> {
        let mut heads = match self.heads.take() {
            Some(h) => h,
            None => {
                let mut h = Vec::with_capacity(cursors.len());
                for c in cursors.iter_mut() {
                    h.push(self.peek(c).await?);
                }
                h
            }
        };
        let out = loop {
            let mut best: Option<&Entry> = None;
            for e in heads.iter().flatten() {
                let better = match best {
                    None => true,
                    Some(b) => {
                        let ord = if self.reverse { b.key.cmp(&e.key) } else { e.key.cmp(&b.key) };
                        ord.is_lt() || (ord.is_eq() && e.seq > b.seq)
                    }
                };
                if better {
                    best = Some(e);
                }
            }
            let Some(winner) = best.cloned() else { break None };
            for (i, c) in cursors.iter_mut().enumerate() {
                if heads[i].as_ref().is_some_and(|e| e.key == winner.key) {
                    Self::advance(c);
                    heads[i] = self.peek(c).await?;
                }
            }
            if self.deletes.as_ref().is_some_and(|d| d.seq_at(&winner.key) > winner.seq) {
                continue;
            }
            let winner = match self.expire_at {
                Some(t) => winner.at(t),
                None => winner,
            };
            if self.include_tombstones || !winner.value.is_tombstone() {
                break Some(winner);
            }
        };
        self.heads = Some(heads);
        Ok(out)
    }
}

impl Version {
    pub fn new(m: Manifest, etag: String, shared: Arc<Shared>) -> Version {
        let deletes = Arc::new(RangeDeletes::new(&m.range_deletes));
        let expiring = m.tail.iter().any(|e| e.expires.is_some()) || m.segments.iter().any(|s| s.expiring > 0);
        let bytes = std::mem::size_of::<Version>()
            + m.heap_bytes()
            + super::memory::alloc_bytes(etag.len())
            + deletes.heap_bytes();
        let charge = match shared.is_system(&m.db_id) {
            true => super::memory::Charge::default(),
            false => super::memory::Charge::new(&shared.memory, bytes),
        };
        Version { m, etag, stored_ms: None, shared, deletes, expiring, charge }
    }

    pub fn version(&self) -> Seq {
        self.m.version
    }

    pub fn memory_bytes(&self) -> usize {
        self.charge.bytes()
    }

    /// Returns the newest entry for `key`, including tombstones. If covered by
    /// a range tombstone, returns a tombstone with that sequence number.
    pub async fn get_entry(&self, key: &[u8]) -> Result<Option<Entry>> {
        let found = self.stored_entry(key).await?;
        let deleted_at = self.deletes.seq_at(key);
        Ok(match found {
            Some(e) if e.seq >= deleted_at => Some(e),
            _ if deleted_at > 0 => Some(Entry::tombstone(Bytes::copy_from_slice(key), deleted_at)),
            none => none,
        })
    }

    /// Returns the newest stored entry for `key`, ignoring range tombstones.
    async fn stored_entry(&self, key: &[u8]) -> Result<Option<Entry>> {
        if let Ok(i) = self.m.tail.binary_search_by(|e| e.key.as_ref().cmp(key)) {
            return Ok(Some(self.m.tail[i].clone()));
        }
        for seg in self.candidates(key) {
            let (idx, whole) = self.shared.open_segment(&self.m.db_id, seg, ReadMode::Cached).await?;
            if !idx.bloom.may_contain(key) {
                continue;
            }
            let Some(bi) = idx.block_for(key) else { continue };
            let b = match whole.and_then(|mut w| (bi < w.len()).then(|| w.swap_remove(bi))) {
                Some(b) => b,
                None => self.shared.seg_block(&self.m.db_id, seg, &idx, bi).await?,
            };
            if let Ok(i) = b.binary_search_by(|e| e.key.as_ref().cmp(key)) {
                return Ok(Some(b[i].clone()));
            }
        }
        Ok(None)
    }

    /// Returns segments whose key range covers `key`, ordered from newest to oldest.
    fn candidates<'a>(&'a self, key: &'a [u8]) -> impl Iterator<Item = &'a SegmentMeta> + 'a {
        let covers = move |s: &&SegmentMeta| key >= s.min_key.as_ref() && key <= s.max_key.as_ref();
        let l0 = self.m.level(0).iter().filter(covers);
        let deeper = (1..=MAX_LEVEL).filter_map(move |l| {
            let segs = self.m.level(l);
            let i = segs.partition_point(|s| s.max_key.as_ref() < key);
            segs.get(i).filter(covers)
        });
        l0.chain(deeper)
    }

    /// Returns the number of segments whose key range covers `key`.
    pub fn segments_for_key(&self, key: &[u8]) -> usize {
        self.candidates(key).count()
    }

    /// Returns the entry for `key` if it is neither deleted nor expired at store time `at`.
    pub async fn get(&self, key: &[u8], at: u64) -> Result<Option<Entry>> {
        Ok(self.get_entry(key).await?.filter(|e| !e.is_dead(at)))
    }

    /// Returns the store timestamp for reads against this version (`max(m.time_ms, store_now_ms)`).
    pub fn read_time(&self) -> u64 {
        self.m.time_ms.max(self.shared.store_now_ms().unwrap_or(0))
    }

    /// Returns the indexed range tombstones for this version.
    pub(crate) fn deletes(&self) -> Arc<RangeDeletes> {
        self.deletes.clone()
    }

    /// Returns the sorted runs for this version: each matching L0 segment as a separate run,
    /// followed by a run per non-empty deeper level.
    pub(crate) fn runs(&self, keep: impl Fn(&SegmentMeta) -> bool) -> Vec<Vec<SegmentMeta>> {
        let mut runs: Vec<Vec<SegmentMeta>> =
            self.m.level(0).iter().filter(|s| keep(s)).map(|s| vec![s.clone()]).collect();
        for l in 1..=MAX_LEVEL {
            let run: Vec<SegmentMeta> = self.m.level(l).iter().filter(|s| keep(s)).cloned().collect();
            if !run.is_empty() {
                runs.push(run);
            }
        }
        runs
    }

    /// Returns an iterator over the newest entry for each key in `range`. Keys covered
    /// by a range tombstone are skipped even when `include_tombstones` is true.
    /// When `at` is set, entries expired at `at` are converted to tombstones.
    pub fn iter(&self, range: &KeyRange, reverse: bool, include_tombstones: bool, at: Option<u64>) -> MergeIter {
        let spec = IterSpec {
            range: range.clone(),
            reverse,
            include_tombstones,
            mode: ReadMode::Cached,
            deletes: Some(self.deletes.clone()),
            expire_at: at,
        };
        MergeIter::new(self.shared.clone(), &self.m.db_id, &self.m.tail, self.runs(|_| true), spec)
    }

    /// Iterates all stored entries in bulk mode, bypassing the caches.
    pub(crate) fn iter_bulk(&self, include_tombstones: bool) -> MergeIter {
        let spec = IterSpec {
            range: KeyRange::all(),
            reverse: false,
            include_tombstones,
            mode: ReadMode::Bulk,
            deletes: Some(self.deletes.clone()),
            expire_at: None,
        };
        MergeIter::new(self.shared.clone(), &self.m.db_id, &self.m.tail, self.runs(|_| true), spec)
    }

    /// Returns up to `limit` live entries in `range` at store time `at`.
    pub async fn scan(&self, range: &KeyRange, reverse: bool, limit: usize, at: u64) -> Result<Vec<Entry>> {
        let mut out = Vec::new();
        if limit == 0 || range.is_empty() {
            return Ok(out);
        }
        let mut it = self.iter(range, reverse, false, Some(at));
        while let Some(e) = it.next().await? {
            out.push(e);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// Returns whether any entry or range tombstone in `range` has `seq > after`.
    pub async fn changed_since(&self, range: &KeyRange, after: Seq) -> Result<bool> {
        if range.is_empty() {
            return Ok(false);
        }
        if self.deletes.changed_since(range, after) {
            return Ok(true);
        }
        if self.m.tail.iter().any(|e| e.seq > after && range.contains(&e.key)) {
            return Ok(true);
        }
        for run in self.runs(|s| s.max_seq > after) {
            let spec = IterSpec {
                range: range.clone(),
                reverse: false,
                include_tombstones: true,
                mode: ReadMode::Cached,
                deletes: None,
                expire_at: None,
            };
            let mut it = MergeIter::new(self.shared.clone(), &self.m.db_id, &[], vec![run], spec);
            while let Some(e) = it.next().await? {
                if e.seq > after {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Returns the newest stored entry for every user key.
    pub async fn all_entries(&self, include_tombstones: bool) -> Result<Vec<Entry>> {
        let mut out = Vec::new();
        let mut it = self.iter(&KeyRange::data(), false, include_tombstones, None);
        while let Some(e) = it.next().await? {
            out.push(e);
        }
        Ok(out)
    }

    /// Point version of [`Version::changed_since`].
    pub async fn key_changed_since(&self, key: &[u8], after: Seq) -> Result<bool> {
        Ok(self.get_entry(key).await?.is_some_and(|e| e.seq > after))
    }

    /// Returns up to `limit` changes in `range` after `after` in feed order:
    /// the newest entry for each modified key and any range tombstones clipped
    /// to `range`. Fails with `CursorExpired` if tombstones required by `after`
    /// have been purged.
    pub async fn changes(&self, range: &KeyRange, after: &ChangeCursor, limit: usize) -> Result<ChangePage> {
        let floor = if after.is_start() { self.m.version } else { after.floor };
        if after.pos < ChangePos::after_version(self.m.tombstone_horizon) && floor < self.m.tombstone_horizon {
            return Err(Error::CursorExpired(format!(
                "deletions up to version {} may have been purged; resynchronize",
                self.m.tombstone_horizon
            )));
        }
        let mut best: BinaryHeap<Change> = BinaryHeap::new();
        let mut offer = |c: Change| {
            if c.pos() > after.pos {
                best.push(c);
                if best.len() > limit + 1 {
                    best.pop();
                }
            }
        };
        // Merge overlapping range tombstones from the same commit after clipping to `range`
        // so that each change has a distinct position.
        let mut clipped: Vec<RangeTombstone> = self
            .m
            .range_deletes
            .iter()
            .filter_map(|t| RangeTombstone::new(&t.range().intersect(range), t.seq))
            .collect();
        clipped.sort_by(|a, b| (a.seq, &a.start).cmp(&(b.seq, &b.start)));
        let mut merged: Vec<RangeTombstone> = Vec::with_capacity(clipped.len());
        for t in clipped {
            match merged.last_mut() {
                Some(p) if p.seq == t.seq && p.end.as_ref().is_none_or(|e| *e >= t.start) => {
                    p.end = match (&p.end, &t.end) {
                        (Some(a), Some(b)) => Some(a.max(b).clone()),
                        _ => None,
                    };
                }
                _ => merged.push(t),
            }
        }
        for t in merged {
            offer(Change::DeleteRange(t));
        }
        let spec = IterSpec {
            range: range.clone(),
            reverse: false,
            include_tombstones: true,
            mode: ReadMode::Cached,
            deletes: Some(self.deletes.clone()),
            expire_at: None,
        };
        let runs = self.runs(|s| s.max_seq >= after.pos.seq);
        let mut it = MergeIter::new(self.shared.clone(), &self.m.db_id, &self.m.tail, runs, spec);
        while let Some(e) = it.next().await? {
            if e.seq >= after.pos.seq {
                offer(Change::Entry(e));
            }
        }
        let mut changes = best.into_sorted_vec();
        let more = changes.len() > limit;
        changes.truncate(limit);
        let next = match changes.last() {
            Some(c) if more => ChangeCursor { pos: c.pos(), floor: floor.min(self.m.version) },
            _ if after.pos > ChangePos::after_version(self.m.version) => after.clone(),
            _ => ChangeCursor::after_version(self.m.version),
        };
        Ok(ChangePage { changes, more, next })
    }
}

/// Database version paired with the store timestamp used to evaluate TTL expiration.
#[derive(Clone)]
pub struct Snapshot {
    pub version: Arc<Version>,
    /// Store timestamp for the read (`>= version.m.time_ms`).
    pub time_ms: u64,
}

impl Snapshot {
    pub fn version(&self) -> Seq {
        self.version.m.version
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        self.version.get(key, self.time_ms).await
    }

    pub async fn scan(&self, range: &KeyRange, reverse: bool, limit: usize) -> Result<Vec<Entry>> {
        self.version.scan(range, reverse, limit, self.time_ms).await
    }
}

/// Cursor position in a change feed, ordered by `(seq, kind, key)`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChangePos {
    pub seq: Seq,
    /// `0` for range deletions, `1` for key entries, `2` for the end of `seq`.
    pub kind: u8,
    pub key: Bytes,
}

impl ChangePos {
    pub const RANGE: u8 = 0;
    pub const KEY: u8 = 1;
    pub const END: u8 = 2;

    pub fn after_version(seq: Seq) -> ChangePos {
        ChangePos { seq, kind: ChangePos::END, key: Bytes::new() }
    }
}

/// Resume cursor for a change feed.
///
/// A cursor is valid only if all tombstones that could delete keys held by the
/// caller are still retained (`> tombstone_horizon`). An incremental caller
/// (`floor == 0`) requires all tombstones after `pos`. A caller performing a
/// full state transfer from version 0 holds only keys returned by earlier pages
/// of that transfer, so it requires only tombstones newer than `floor`, the
/// minimum snapshot version across all pages read in the transfer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeCursor {
    pub pos: ChangePos,
    /// Minimum snapshot version read during a full state transfer from version 0, or `0`.
    pub floor: Seq,
}

impl ChangeCursor {
    pub fn after_version(seq: Seq) -> ChangeCursor {
        ChangeCursor { pos: ChangePos::after_version(seq), floor: 0 }
    }

    fn is_start(&self) -> bool {
        self.pos == ChangePos::after_version(0)
    }

    /// Encodes the cursor as an opaque URL-safe string; see [`ChangeCursor::decode`].
    pub fn encode(&self) -> String {
        let p = &self.pos;
        match (p.kind, self.floor) {
            (ChangePos::END, _) => p.seq.to_string(),
            (k, 0) => format!("{}.{k}.{}", p.seq, crate::util::b64::encode_url(&p.key)),
            (k, f) => format!("{}.{k}.{}.{f}", p.seq, crate::util::b64::encode_url(&p.key)),
        }
    }

    pub fn decode(s: &str) -> Option<ChangeCursor> {
        let mut parts = s.split('.');
        let seq = parts.next()?.parse().ok()?;
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (None, ..) => Some(ChangeCursor::after_version(seq)),
            (Some(k), Some(key), floor, None) => {
                let kind = k.parse().ok().filter(|k| *k < ChangePos::END)?;
                let floor = match floor {
                    None => 0,
                    Some(f) => f.parse().ok().filter(|f| *f > 0)?,
                };
                let key = Bytes::from(crate::util::b64::decode_url(key)?);
                Some(ChangeCursor { pos: ChangePos { seq, kind, key }, floor })
            }
            _ => None,
        }
    }
}

/// Change entry returned by [`Version::changes`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    /// Newest entry for a key (value or tombstone).
    Entry(Entry),
    /// Range deletion covering entries older than `seq`.
    DeleteRange(RangeTombstone),
}

impl Change {
    pub fn seq(&self) -> Seq {
        match self {
            Change::Entry(e) => e.seq,
            Change::DeleteRange(t) => t.seq,
        }
    }

    pub fn pos(&self) -> ChangePos {
        match self {
            Change::Entry(e) => ChangePos { seq: e.seq, kind: ChangePos::KEY, key: e.key.clone() },
            Change::DeleteRange(t) => ChangePos { seq: t.seq, kind: ChangePos::RANGE, key: t.start.clone() },
        }
    }
}

impl PartialOrd for Change {
    fn partial_cmp(&self, o: &Change) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}

impl Ord for Change {
    fn cmp(&self, o: &Change) -> std::cmp::Ordering {
        self.pos().cmp(&o.pos())
    }
}

/// Page of changes returned by [`Version::changes`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangePage {
    pub changes: Vec<Change>,
    /// True if additional changes follow this page.
    pub more: bool,
    /// Cursor for the next page.
    pub next: ChangeCursor,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn range_logic() {
        let r = KeyRange::half_open(b("b"), Some(b("d")));
        assert!(!r.contains(b"a"));
        assert!(r.contains(b"b"));
        assert!(r.contains(b"c\xff"));
        assert!(!r.contains(b"d"));
        assert!(r.overlaps_keys(b"a", b"b"));
        assert!(!r.overlaps_keys(b"d", b"e"));
        assert!(!r.overlaps_keys(b"0", b"a"));
        assert!(r.intersects(&KeyRange::half_open(b("c"), Some(b("z")))));
        assert!(!r.intersects(&KeyRange::half_open(b("d"), Some(b("z")))));
        assert!(r.intersects(&KeyRange::new(Bound::Unbounded, Bound::Included(b("b")))));
        assert!(!r.intersects(&KeyRange::new(Bound::Unbounded, Bound::Excluded(b("b")))));
        assert!(KeyRange::half_open(b("b"), Some(b("b"))).is_empty());
        assert!(KeyRange::prefix(b"ab").contains(b"ab\x00"));
        assert!(!KeyRange::prefix(b"ab").contains(b"ac"));
    }

    #[test]
    fn intersections_contain_exactly_the_keys_of_both() {
        let keys: Vec<Bytes> = ["", "a", "b", "b\0", "c", "d"].iter().map(|k| b(k)).collect();
        let bounds = |k: &Bytes| [Bound::Included(k.clone()), Bound::Excluded(k.clone()), Bound::Unbounded];
        for s1 in keys.iter().flat_map(bounds) {
            for e1 in keys.iter().flat_map(bounds) {
                let r1 = KeyRange::new(s1.clone(), e1);
                for s2 in keys.iter().flat_map(bounds) {
                    for e2 in keys.iter().flat_map(bounds) {
                        let r2 = KeyRange::new(s2.clone(), e2);
                        let both = r1.intersect(&r2);
                        for k in &keys {
                            assert_eq!(both.contains(k), r1.contains(k) && r2.contains(k), "{r1:?} ∩ {r2:?} at {k:?}");
                        }
                    }
                }
            }
        }
    }

    /// Decoding a cursor never panics, and decoded cursors survive a round trip.
    #[test]
    fn fuzz_change_cursors() {
        let mut r = crate::util::rng::Rng::new(9);
        let pieces =
            ["", ".", "0", "1", "2", "3", "18446744073709551615", "18446744073709551616", "-1", "AA", "_-", "=", "é"];
        for _ in 0..20_000 {
            let s: String = (0..r.usize(6)).map(|_| pieces[r.usize(pieces.len())]).collect();
            if let Some(c) = ChangeCursor::decode(&s) {
                assert_eq!(ChangeCursor::decode(&c.encode()), Some(c), "{s:?}");
            }
        }
    }
}
