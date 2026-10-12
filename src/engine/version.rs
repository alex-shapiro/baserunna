//! Immutable snapshots of a database (a decoded manifest) and the read path:
//! point lookups, merged range scans and conflict checks.

use super::cache::{CacheKey, CacheVal};
use super::manifest::{MAX_LEVEL, Manifest, SegmentMeta};
use super::segment::{self, Block, FOOTER_LEN, Footer, SegmentIndex};
use super::types::{Entry, Seq};
use super::{Shared, seg_key};
use crate::error::{Error, Result};
use crate::store::GetOpts;
use bytes::Bytes;
use std::collections::VecDeque;
use std::ops::Bound;
use std::sync::Arc;

/// An immutable, consistent view of a database at `m.version`.
pub struct Version {
    pub m: Manifest,
    pub etag: String,
    pub(crate) shared: Arc<Shared>,
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

    /// Cache a segment this node just wrote, so it is never fetched back:
    /// its index in memory (and, if `hot`, its blocks), everything in the
    /// local tier.
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

    /// Load (or fetch from cache) the index of a segment.
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

    /// A block that is not in the memory cache: from the local tier, or
    /// else the store.
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

    /// A block from the store, kept in both caches.
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

    /// Blocks `blocks.start..` up to `blocks.end`, read with one request of
    /// at most `bulk_read_bytes` (but at least one block). With `cache`, they
    /// are kept like blocks read one by one; compaction and sweeps read
    /// each block once and do not keep them.
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
    /// Block by block through the caches (queries).
    Cached,
    /// Consecutive blocks per request, bypassing the caches (compaction,
    /// sweeps). Forward iteration only.
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
}

/// A merged iteration over a database's sources (or a subset of them, for
/// compaction): the newest entry of each key, in key order. Blocks load
/// lazily, so memory is bounded by one block per source (one bulk read
/// per source in bulk mode).
pub struct MergeIter {
    shared: Arc<Shared>,
    db: String,
    range: KeyRange,
    reverse: bool,
    include_tombstones: bool,
    mode: ReadMode,
    cursors: Vec<Cursor>,
    heads: Option<Vec<Option<Entry>>>,
}

impl MergeIter {
    /// Iterate `tail` and the `runs` (each a sorted run of segments).
    pub(crate) fn new(
        shared: Arc<Shared>,
        db: &str,
        tail: &[Entry],
        runs: Vec<Vec<SegmentMeta>>,
        spec: IterSpec,
    ) -> MergeIter {
        let IterSpec { range, reverse, include_tombstones, mode } = spec;
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
        MergeIter { shared, db: db.to_string(), range, reverse, include_tombstones, mode, cursors, heads: None }
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

    /// The next entry (the newest of its key), or `None` at the end.
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
            if self.include_tombstones || !winner.value.is_tombstone() {
                break Some(winner);
            }
        };
        self.heads = Some(heads);
        Ok(out)
    }
}

impl Version {
    pub fn version(&self) -> Seq {
        self.m.version
    }

    /// Newest entry for `key`, including tombstones.
    pub async fn get_entry(&self, key: &[u8]) -> Result<Option<Entry>> {
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

    /// Segments whose key range covers `key`: the bloom filters a point
    /// lookup that misses the tail may have to check.
    pub fn segments_for_key(&self, key: &[u8]) -> usize {
        self.candidates(key).count()
    }

    /// Live (non-tombstone) entry for `key`.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        Ok(self.get_entry(key).await?.filter(|e| !e.value.is_tombstone()))
    }

    /// The sorted runs of this version: each level 0 segment alone, then
    /// each deeper level.
    fn runs(&self, keep: impl Fn(&SegmentMeta) -> bool) -> Vec<Vec<SegmentMeta>> {
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

    /// The newest entry of each key in `range`, in order.
    pub fn iter(&self, range: &KeyRange, reverse: bool, include_tombstones: bool) -> MergeIter {
        let spec = IterSpec { range: range.clone(), reverse, include_tombstones, mode: ReadMode::Cached };
        MergeIter::new(self.shared.clone(), &self.m.db_id, &self.m.tail, self.runs(|_| true), spec)
    }

    /// Every entry, read in bulk without filling the caches (sweeps).
    pub(crate) fn iter_bulk(&self, include_tombstones: bool) -> MergeIter {
        let spec = IterSpec { range: KeyRange::all(), reverse: false, include_tombstones, mode: ReadMode::Bulk };
        MergeIter::new(self.shared.clone(), &self.m.db_id, &self.m.tail, self.runs(|_| true), spec)
    }

    /// Merged scan. Returns at most `limit` entries; tombstones are skipped
    /// unless `include_tombstones`.
    pub async fn scan(
        &self,
        range: &KeyRange,
        reverse: bool,
        limit: usize,
        include_tombstones: bool,
    ) -> Result<Vec<Entry>> {
        let mut out = Vec::new();
        if limit == 0 || range.is_empty() {
            return Ok(out);
        }
        let mut it = self.iter(range, reverse, include_tombstones);
        while let Some(e) = it.next().await? {
            out.push(e);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// True if any entry (including tombstones and shadowed versions) in
    /// `range` has `seq > after`.
    pub async fn changed_since(&self, range: &KeyRange, after: Seq) -> Result<bool> {
        if range.is_empty() {
            return Ok(false);
        }
        if self.m.tail.iter().any(|e| e.seq > after && range.contains(&e.key)) {
            return Ok(true);
        }
        // Each source alone: a newer entry anywhere is a change.
        for run in self.runs(|s| s.max_seq > after) {
            let spec =
                IterSpec { range: range.clone(), reverse: false, include_tombstones: true, mode: ReadMode::Cached };
            let mut it = MergeIter::new(self.shared.clone(), &self.m.db_id, &[], vec![run], spec);
            while let Some(e) = it.next().await? {
                if e.seq > after {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Every newest entry (tests and checks).
    pub async fn all_entries(&self, include_tombstones: bool) -> Result<Vec<Entry>> {
        self.scan(&KeyRange::all(), false, usize::MAX, include_tombstones).await
    }

    /// Point version of [`Version::changed_since`].
    pub async fn key_changed_since(&self, key: &[u8], after: Seq) -> Result<bool> {
        Ok(self.get_entry(key).await?.is_some_and(|e| e.seq > after))
    }
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
}
