//! Immutable snapshots of a database (a decoded manifest) and the read path:
//! point lookups, merged range scans, change feeds and conflict checks.

use super::cache::{CacheKey, CacheVal};
use super::manifest::{Levels, MAX_LEVEL, Manifest, SegmentMeta};
use super::range_delete::{RangeDeletes, RangeTombstone};
use super::segment::{self, Block, FOOTER_LEN, Footer, SegmentIndex};
use super::types::{COMMIT_SPACE, Entry, Seq};
use super::{Shared, levels_key, seg_key};
use crate::error::{Error, Result};
use crate::store::GetOpts;
use bytes::Bytes;
use std::collections::{BinaryHeap, VecDeque};
use std::ops::Bound;
use std::sync::Arc;

/// An immutable, consistent view of a database at `m.version`.
pub struct Version {
    pub m: Manifest,
    pub etag: String,
    /// The store's last-modified time of the manifest, if it was read from
    /// the store rather than written by this node.
    pub stored_ms: Option<u64>,
    pub(crate) shared: Arc<Shared>,
    /// `m.range_deletes`, indexed for lookups.
    deletes: Arc<RangeDeletes>,
    /// Whether any stored value expires, so that what reads see depends on
    /// the time they read at.
    pub(crate) expiring: bool,
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

    /// Every key below the engine's commit records: all the data the
    /// layers above store.
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

    pub fn prefix(p: &[u8]) -> KeyRange {
        KeyRange::half_open(Bytes::copy_from_slice(p), crate::util::prefix_end(p).map(Bytes::from))
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

    /// True if the key is below the range's end (i.e. a forward scan
    /// starting at or before it has not passed the end).
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
        self.after_start_or_eq_max(max) && self.before_end(min)
    }

    fn after_start_or_eq_max(&self, max: &[u8]) -> bool {
        // Some key <= max is >= start.
        self.after_start(max)
    }

    /// The keys in both ranges.
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
        // Ranges intersect unless one ends before the other starts.
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
    /// Decodes a stored manifest, fetching deeper levels from its levels
    /// object if not already present in `current`.
    pub(crate) async fn load_manifest(&self, db: &str, data: &Bytes, current: Option<&Version>) -> Result<Manifest> {
        let mut m = Manifest::decode(data)?;
        if m.db_id != db {
            return Err(Error::internal("manifest belongs to another database"));
        }
        let Some(id) = m.levels.clone() else { return Ok(m) };
        if let Some(c) = current.filter(|c| c.m.levels.as_deref() == Some(id.as_str())) {
            m.segments.extend_from_slice(c.m.deep_segments());
            m.check_levels().map_err(Error::internal)?;
            return Ok(m);
        }
        m.attach_levels(&*self.levels(db, &id).await?)?;
        Ok(m)
    }

    async fn levels(&self, db: &str, id: &str) -> Result<Arc<Levels>> {
        let key = levels_key(db, id);
        let ck = CacheKey { seg_path: Arc::from(key.as_str()), block: 0 };
        if let Some(CacheVal::Levels(l)) = self.cache.get(&ck) {
            return Ok(l);
        }
        let tier = self.cfg.local_tier.as_ref();
        let local = match tier {
            Some(t) => t.0.get(&key).await.and_then(|d| Levels::decode(&d).ok()),
            None => None,
        };
        let l = match local {
            Some(l) => l,
            None => {
                let g = self.retry.run(|| self.store.get(&key, GetOpts::default())).await?;
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
        Ok(l)
    }

    /// Writes a new levels object under a random ID and populates local caches.
    pub(crate) async fn write_levels(&self, db: &str, segments: Vec<SegmentMeta>) -> Result<String> {
        let id = self.env.random_hex(16);
        let l = Levels { db_id: db.to_string(), id: id.clone(), segments };
        let data = l.encode();
        let key = levels_key(db, &id);
        self.retry.run(|| self.store.put(&key, data.clone(), crate::store::PutMode::Overwrite)).await?;
        self.counters.add(&super::Activity { levels_objects_written: 1, ..Default::default() });
        self.cache.insert(CacheKey { seg_path: Arc::from(key.as_str()), block: 0 }, CacheVal::Levels(Arc::new(l)));
        if let Some(t) = self.cfg.local_tier.clone() {
            self.env.spawn(async move { t.0.put(&key, data).await });
        }
        Ok(id)
    }

    fn cache_key(&self, db: &str, seg: &str, block: u32) -> CacheKey {
        CacheKey { seg_path: Arc::from(seg_key(db, seg).as_str()), block }
    }

    fn local_key(db: &str, seg: &str, part: &str) -> String {
        format!("{}#{part}", seg_key(db, seg))
    }

    async fn local_get(&self, db: &str, seg: &str, part: &str) -> Option<Bytes> {
        self.cfg.local_tier.as_ref()?.0.get(&Self::local_key(db, seg, part)).await
    }

    fn local_put(&self, db: &str, seg: &str, part: &str, data: Bytes) {
        if let Some(t) = self.cfg.local_tier.clone() {
            let key = Self::local_key(db, seg, part);
            self.env.spawn(async move { t.0.put(&key, data).await });
        }
    }

    /// Index a whole segment's bytes and cache its index and blocks.
    fn cache_whole(&self, db: &str, seg: &str, data: &Bytes) -> Result<Arc<SegmentIndex>> {
        let idx = Arc::new(SegmentIndex::decode_whole(data)?);
        for (bi, b) in segment::decode_blocks(&idx, 0, data)?.into_iter().enumerate() {
            self.cache.insert(self.cache_key(db, seg, bi as u32), CacheVal::Block(b));
        }
        self.cache.insert(self.cache_key(db, seg, u32::MAX), CacheVal::Index(idx.clone()));
        Ok(idx)
    }

    /// Caches a newly written segment in the local tier and its index in memory,
    /// also caching its decoded blocks in memory when `hot` is true.
    pub(crate) fn remember_segment(&self, db: &str, seg: &str, data: &Bytes, hot: bool) -> Result<()> {
        let idx = if hot {
            self.cache_whole(db, seg, data)?
        } else {
            let idx = Arc::new(SegmentIndex::decode_whole(data)?);
            self.cache.insert(self.cache_key(db, seg, u32::MAX), CacheVal::Index(idx.clone()));
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

    fn whole_fetch_max(&self) -> u64 {
        self.cfg.segment_whole_fetch_max.max(FOOTER_LEN as u64)
    }

    /// Loads or returns the cached index of a segment.
    pub(crate) async fn seg_index(&self, db: &str, seg: &SegmentMeta) -> Result<Arc<SegmentIndex>> {
        let ck = self.cache_key(db, &seg.id, u32::MAX);
        if let Some(CacheVal::Index(i)) = self.cache.get(&ck) {
            return Ok(i);
        }
        let key = seg_key(db, &seg.id);
        if seg.size <= self.whole_fetch_max() {
            if let Some(d) = self.local_get(db, &seg.id, "all").await
                && let Ok(idx) = self.cache_whole(db, &seg.id, &d)
            {
                return Ok(idx);
            }
            let g = self.retry.run(|| self.store.get(&key, GetOpts::default())).await?;
            let idx = self.cache_whole(db, &seg.id, &g.data)?;
            self.local_put(db, &seg.id, "all", g.data);
            return Ok(idx);
        }
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
            let idx = Arc::new(idx);
            self.cache.insert(ck, CacheVal::Index(idx.clone()));
            return Ok(idx);
        }
        let g = self
            .retry
            .run(|| self.store.get(&key, GetOpts { range: Some(size - FOOTER_LEN as u64..size), ..Default::default() }))
            .await?;
        let footer = Footer::decode(&g.data, size)?;
        let start = footer.bloom_off;
        let g =
            self.retry.run(|| self.store.get(&key, GetOpts { range: Some(start..size), ..Default::default() })).await?;
        if g.data.len() as u64 != size - start {
            return Err(Error::internal("short segment read"));
        }
        let idx = Arc::new(parse(&g.data)?);
        self.cache.insert(ck, CacheVal::Index(idx.clone()));
        self.local_put(db, &seg.id, "meta", g.data);
        Ok(idx)
    }

    fn cached_block(&self, db: &str, seg: &str, bi: usize) -> Option<Block> {
        match self.cache.get(&self.cache_key(db, seg, bi as u32)) {
            Some(CacheVal::Block(b)) => Some(b),
            _ => None,
        }
    }

    pub(crate) async fn seg_block(&self, db: &str, seg: &SegmentMeta, idx: &SegmentIndex, bi: usize) -> Result<Block> {
        match self.cached_block(db, &seg.id, bi) {
            Some(b) => Ok(b),
            None => self.load_block(db, seg, idx, bi).await,
        }
    }

    async fn load_block(&self, db: &str, seg: &SegmentMeta, idx: &SegmentIndex, bi: usize) -> Result<Block> {
        match self.local_block(db, &seg.id, bi).await {
            Some(b) => Ok(b),
            None => self.fetch_block(db, seg, idx, bi).await,
        }
    }

    async fn local_block(&self, db: &str, seg: &str, bi: usize) -> Option<Block> {
        let b = segment::decode_block(self.local_get(db, seg, &format!("b{bi}")).await?).ok()?;
        self.cache.insert(self.cache_key(db, seg, bi as u32), CacheVal::Block(b.clone()));
        Some(b)
    }

    async fn fetch_block(&self, db: &str, seg: &SegmentMeta, idx: &SegmentIndex, bi: usize) -> Result<Block> {
        let ck = self.cache_key(db, &seg.id, bi as u32);
        let part = format!("b{bi}");
        let (_, off, len) = idx.blocks[bi];
        let key = seg_key(db, &seg.id);
        let g = self
            .retry
            .run(|| self.store.get(&key, GetOpts { range: Some(off..off + len as u64), ..Default::default() }))
            .await?;
        if g.data.len() != len as usize {
            return Err(Error::internal("short block read"));
        }
        let b = segment::decode_block(g.data.clone())?;
        self.cache.insert(ck, CacheVal::Block(b.clone()));
        self.local_put(db, &seg.id, &part, g.data);
        Ok(b)
    }

    /// Fetches consecutive blocks starting at `blocks.start` up to `blocks.end`
    /// within `bulk_read_bytes` in a single request, optionally caching them.
    async fn seg_blocks_bulk(
        &self,
        db: &str,
        seg: &SegmentMeta,
        idx: &SegmentIndex,
        blocks: std::ops::Range<usize>,
        cache: bool,
    ) -> Result<Vec<Block>> {
        let first = blocks.start;
        let mut end = first + 1;
        while end < blocks.end.min(idx.blocks.len()) {
            let span = idx.span(first..end + 1);
            if span.end - span.start > self.cfg.bulk_read_bytes {
                break;
            }
            end += 1;
        }
        let span = idx.span(first..end);
        let key = seg_key(db, &seg.id);
        let g = self
            .retry
            .run(|| self.store.get(&key, GetOpts { range: Some(span.clone()), ..Default::default() }))
            .await?;
        if g.data.len() as u64 != span.end - span.start {
            return Err(Error::internal("short segment read"));
        }
        let decoded = segment::decode_blocks(idx, first, &g.data)?;
        if decoded.len() != end - first {
            return Err(Error::internal("short segment read"));
        }
        if cache {
            for (i, b) in decoded.iter().enumerate() {
                let bi = first + i;
                self.cache.insert(self.cache_key(db, &seg.id, bi as u32), CacheVal::Block(b.clone()));
                let (_, off, len) = idx.blocks[bi];
                let at = (off - span.start) as usize;
                self.local_put(db, &seg.id, &format!("b{bi}"), g.data.slice(at..at + len as usize));
            }
        }
        Ok(decoded)
    }
}

/// How an iterator loads segment blocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReadMode {
    /// Block by block through the caches.
    Cached,
    /// Consecutive blocks per request, bypassing the caches. Forward iteration only.
    Bulk,
}

/// Iterates the entries of a sorted run of segments: one level 0 segment,
/// or the segments of a deeper level, in iteration order.
struct RunCursor {
    segs: VecDeque<SegmentMeta>,
    seg: Option<(SegmentMeta, Arc<SegmentIndex>)>,
    /// Blocks of the current segment still to visit, in iteration order.
    blocks: VecDeque<usize>,
    /// Blocks already fetched ahead.
    ready: VecDeque<Block>,
    /// Blocks of the current segment fetched one at a time so far.
    fetched: u32,
    cur: Option<Block>,
    pos: usize,
}

enum Cursor {
    Tail { entries: Vec<Entry>, pos: usize },
    Run(Box<RunCursor>),
}

/// What a [`MergeIter`] iterates and how.
pub(crate) struct IterSpec {
    pub range: KeyRange,
    pub reverse: bool,
    pub include_tombstones: bool,
    pub mode: ReadMode,
    /// Range tombstones to filter out covered entries.
    pub deletes: Option<Arc<RangeDeletes>>,
    /// Entries that have expired at this store time read as tombstones.
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
                    // Outside the range: skip if before it, stop if past it.
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
                let (meta, idx) = r.seg.as_ref().ok_or_else(|| Error::internal("run cursor without a segment"))?;
                // A forward scan that keeps fetching blocks of a segment from
                // the store reads ahead, up to the last block it needs.
                let bulk = match self.mode {
                    ReadMode::Bulk => Some(false),
                    ReadMode::Cached => {
                        let near = match self.shared.cached_block(&self.db, &meta.id, bi) {
                            Some(b) => Some(b),
                            None => self.shared.local_block(&self.db, &meta.id, bi).await,
                        };
                        match near {
                            Some(b) => {
                                r.cur = Some(b);
                                None
                            }
                            None if !reverse && r.fetched >= 2 => Some(true),
                            None => {
                                r.fetched += 1;
                                r.cur = Some(self.shared.fetch_block(&self.db, meta, idx, bi).await?);
                                None
                            }
                        }
                    }
                };
                if let Some(cache) = bulk {
                    let last = r.blocks.back().copied().unwrap_or(bi);
                    let got = self.shared.seg_blocks_bulk(&self.db, meta, idx, bi..last + 1, cache).await?;
                    for _ in 1..got.len() {
                        r.blocks.pop_front();
                    }
                    r.ready.extend(got);
                }
                r.pos = 0;
                continue;
            }
            let Some(meta) = r.segs.pop_front() else {
                r.seg = None;
                return Ok(None);
            };
            let idx = self.shared.seg_index(&self.db, &meta).await?;
            let first = match &range.start {
                Bound::Included(s) | Bound::Excluded(s) => idx.block_for(s),
                Bound::Unbounded => Some(0),
            };
            let mut blocks = VecDeque::new();
            if let Some(first) = first {
                for bi in first..idx.blocks.len() {
                    blocks.push_back(bi);
                    // Stop once a block's last key reaches past the end.
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
        }
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
        Version { m, etag, stored_ms: None, shared, deletes, expiring }
    }

    pub fn version(&self) -> Seq {
        self.m.version
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
            let idx = self.shared.seg_index(&self.m.db_id, seg).await?;
            if !idx.bloom.may_contain(key) {
                continue;
            }
            let Some(bi) = idx.block_for(key) else { continue };
            let b = self.shared.seg_block(&self.m.db_id, seg, &idx, bi).await?;
            if let Ok(i) = b.binary_search_by(|e| e.key.as_ref().cmp(key)) {
                return Ok(Some(b[i].clone()));
            }
        }
        Ok(None)
    }

    /// Segments that may hold `key`, newest first: level 0 segments, then
    /// at most one per deeper level.
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

    /// Number of segments whose key range covers `key`.
    pub fn segments_for_key(&self, key: &[u8]) -> usize {
        self.candidates(key).count()
    }

    /// The entry for `key` if it is live at store time `at`: neither
    /// deleted nor expired.
    pub async fn get(&self, key: &[u8], at: u64) -> Result<Option<Entry>> {
        Ok(self.get_entry(key).await?.filter(|e| !e.is_dead(at)))
    }

    /// Earliest store time reads of this version may use: the version's own
    /// time or the store's current clock, whichever is later.
    pub fn read_time(&self) -> u64 {
        self.m.time_ms.max(self.shared.store_now_ms().unwrap_or(0))
    }

    /// This version's range tombstones, indexed for lookups.
    pub(crate) fn deletes(&self) -> Arc<RangeDeletes> {
        self.deletes.clone()
    }

    /// The sorted runs of this version: each level 0 segment alone, then
    /// each deeper level.
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

    /// Returns the newest entry of each key in `range`, in order. Keys covered
    /// by a range tombstone are skipped, even with `include_tombstones`.
    /// With `at`, entries expired by then read as tombstones.
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

    /// At most `limit` entries of `range` live at store time `at`, in order.
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
    /// to `range`. Fails with `CursorExpired` if tombstones after `after` may
    /// have been purged.
    pub async fn changes(&self, range: &KeyRange, after: &ChangePos, limit: usize) -> Result<ChangePage> {
        if !after.retained_by(self.m.tombstone_horizon) {
            return Err(Error::CursorExpired(format!(
                "deletions up to version {} may have been purged; resynchronize",
                self.m.tombstone_horizon
            )));
        }
        let mut best: BinaryHeap<Change> = BinaryHeap::new();
        let mut offer = |c: Change| {
            if c.pos() > *after {
                best.push(c);
                if best.len() > limit + 1 {
                    best.pop();
                }
            }
        };
        // Tombstones of one commit may overlap once clipped: report their
        // union, so that no two changes share a position.
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
        let runs = self.runs(|s| s.max_seq >= after.seq);
        let mut it = MergeIter::new(self.shared.clone(), &self.m.db_id, &self.m.tail, runs, spec);
        while let Some(e) = it.next().await? {
            if e.seq >= after.seq {
                offer(Change::Entry(e));
            }
        }
        let mut changes = best.into_sorted_vec();
        let more = changes.len() > limit;
        changes.truncate(limit);
        Ok(ChangePage { changes, more })
    }
}

/// A version read at a moment: what has expired by `time_ms` reads as
/// deleted.
#[derive(Clone)]
pub struct Snapshot {
    pub version: Arc<Version>,
    /// Store time of the read, never before the version's own time.
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

/// A position in a change feed. Changes are ordered by version, and within
/// one by kind (range deletions apply first) and key, so a position after
/// every change of version `seq` is `ChangePos::after_version(seq)`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChangePos {
    pub seq: Seq,
    /// 0 for range deletions, 1 for keys, 2 for the end of the version.
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

    /// Whether every deletion after this position is still stored, given
    /// that those up to `horizon` may have been purged.
    pub fn retained_by(&self, horizon: Seq) -> bool {
        self.seq > horizon || (self.seq == horizon && self.kind == ChangePos::END)
    }

    /// Encodes the cursor as an opaque URL-safe string; see [`ChangePos::decode`].
    pub fn encode(&self) -> String {
        match self.kind {
            ChangePos::END => self.seq.to_string(),
            k => format!("{}.{k}.{}", self.seq, crate::util::b64::encode_url(&self.key)),
        }
    }

    pub fn decode(s: &str) -> Option<ChangePos> {
        let mut parts = s.splitn(3, '.');
        let seq = parts.next()?.parse().ok()?;
        match (parts.next(), parts.next()) {
            (None, _) => Some(ChangePos::after_version(seq)),
            (Some(k), Some(key)) => {
                let kind = k.parse().ok().filter(|k| *k < ChangePos::END)?;
                Some(ChangePos { seq, kind, key: Bytes::from(crate::util::b64::decode_url(key)?) })
            }
            _ => None,
        }
    }
}

/// One change reported by [`Version::changes`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    /// A key's newest entry: its value, or a tombstone if it was deleted.
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

/// A page of a change feed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangePage {
    pub changes: Vec<Change>,
    /// More changes follow the last one.
    pub more: bool,
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

    /// Cursors decode without panicking, and decoded ones round-trip.
    #[test]
    fn fuzz_change_cursors() {
        let mut r = crate::util::rng::Rng::new(9);
        let pieces =
            ["", ".", "0", "1", "2", "3", "18446744073709551615", "18446744073709551616", "-1", "AA", "_-", "=", "é"];
        for _ in 0..20_000 {
            let s: String = (0..r.usize(6)).map(|_| pieces[r.usize(pieces.len())]).collect();
            if let Some(p) = ChangePos::decode(&s) {
                assert_eq!(ChangePos::decode(&p.encode()), Some(p), "{s:?}");
            }
        }
    }
}
