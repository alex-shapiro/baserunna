//! Immutable snapshots of a database (a decoded manifest) and the read path:
//! point lookups, merged range scans and conflict checks.

use super::cache::{CacheKey, CacheVal};
use super::manifest::{Manifest, SegmentMeta};
use super::segment::{self, Block, FOOTER_LEN, Footer, SegmentIndex};
use super::types::{Entry, Seq};
use super::{Shared, seg_key};
use crate::error::{Error, Result};
use crate::store::GetOpts;
use bytes::Bytes;
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

    /// Load (or fetch from cache) the index of a segment.
    pub(crate) async fn seg_index(&self, db: &str, seg: &SegmentMeta) -> Result<Arc<SegmentIndex>> {
        let ck = self.cache_key(db, &seg.id, u32::MAX);
        if let Some(CacheVal::Index(i)) = self.cache.get(&ck) {
            return Ok(i);
        }
        let key = seg_key(db, &seg.id);
        if seg.size <= self.cfg.segment_whole_fetch_max.max(FOOTER_LEN as u64) {
            let g = self.retry.run(|| self.store.get(&key, GetOpts::default())).await?;
            let idx = Arc::new(SegmentIndex::decode_whole(&g.data)?);
            for (bi, (_, off, len)) in idx.blocks.iter().enumerate() {
                let raw = g.data.slice(*off as usize..(*off + *len as u64) as usize);
                let b = segment::decode_block(raw)?;
                self.cache.insert(self.cache_key(db, &seg.id, bi as u32), CacheVal::Block(b));
            }
            self.cache.insert(ck, CacheVal::Index(idx.clone()));
            return Ok(idx);
        }
        let size = seg.size;
        let g = self
            .retry
            .run(|| self.store.get(&key, GetOpts { range: Some(size - FOOTER_LEN as u64..size), ..Default::default() }))
            .await?;
        let footer = Footer::decode(&g.data, size)?;
        let start = footer.bloom_off;
        let end = footer.index_off + footer.index_len as u64;
        let g =
            self.retry.run(|| self.store.get(&key, GetOpts { range: Some(start..end), ..Default::default() })).await?;
        if g.data.len() as u64 != end - start {
            return Err(Error::internal("short segment read"));
        }
        let bloom = &g.data[..footer.bloom_len as usize];
        let index = &g.data[(footer.index_off - start) as usize..];
        let idx = Arc::new(SegmentIndex::decode(footer, index, bloom, size)?);
        self.cache.insert(ck, CacheVal::Index(idx.clone()));
        Ok(idx)
    }

    pub(crate) async fn seg_block(&self, db: &str, seg: &SegmentMeta, idx: &SegmentIndex, bi: usize) -> Result<Block> {
        let ck = self.cache_key(db, &seg.id, bi as u32);
        if let Some(CacheVal::Block(b)) = self.cache.get(&ck) {
            return Ok(b);
        }
        let (_, off, len) = idx.blocks[bi];
        let key = seg_key(db, &seg.id);
        let g = self
            .retry
            .run(|| self.store.get(&key, GetOpts { range: Some(off..off + len as u64), ..Default::default() }))
            .await?;
        if g.data.len() != len as usize {
            return Err(Error::internal("short block read"));
        }
        let b = segment::decode_block(g.data)?;
        self.cache.insert(ck, CacheVal::Block(b.clone()));
        Ok(b)
    }
}

enum Cursor {
    Tail {
        entries: Vec<Entry>,
        pos: usize,
    },
    Seg {
        meta: SegmentMeta,
        idx: Arc<SegmentIndex>,
        /// Block indices left to visit, in iteration order (reversed for
        /// reverse scans), consumed from the back.
        todo: Vec<usize>,
        cur: Option<Block>,
        pos: usize,
    },
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
        for seg in &self.m.segments {
            if key < seg.min_key.as_ref() || key > seg.max_key.as_ref() {
                continue;
            }
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

    /// Live (non-tombstone) entry for `key`.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        Ok(self.get_entry(key).await?.filter(|e| !e.value.is_tombstone()))
    }

    /// Cursors over `range`, leaving out segments with no entry newer than
    /// `newer_than`.
    async fn cursors(&self, range: &KeyRange, reverse: bool, newer_than: Option<Seq>) -> Result<Vec<Cursor>> {
        let mut cursors = Vec::with_capacity(1 + self.m.segments.len());
        let tail: Vec<Entry> = self.m.tail.iter().filter(|e| range.contains(&e.key)).cloned().collect();
        cursors.push(Cursor::Tail { entries: tail, pos: 0 });
        for seg in &self.m.segments {
            if newer_than.is_some_and(|s| seg.max_seq <= s) {
                continue;
            }
            if !range.overlaps_keys(&seg.min_key, &seg.max_key) {
                continue;
            }
            let idx = self.shared.seg_index(&self.m.db_id, seg).await?;
            let first = match &range.start {
                Bound::Included(s) | Bound::Excluded(s) => match idx.block_for(s) {
                    Some(b) => b,
                    None => continue,
                },
                Bound::Unbounded => 0,
            };
            let mut blocks = Vec::new();
            for bi in first..idx.blocks.len() {
                blocks.push(bi);
                // Stop once a block's last key reaches past the end.
                if !range.before_end(&idx.blocks[bi].0) {
                    break;
                }
            }
            // `todo` is consumed from the back.
            if !reverse {
                blocks.reverse();
            }
            cursors.push(Cursor::Seg { meta: seg.clone(), idx, todo: blocks, cur: None, pos: 0 });
        }
        if reverse {
            for c in &mut cursors {
                if let Cursor::Tail { entries, .. } = c {
                    entries.reverse();
                }
            }
        }
        Ok(cursors)
    }

    async fn peek(&self, c: &mut Cursor, range: &KeyRange, reverse: bool) -> Result<Option<Entry>> {
        match c {
            Cursor::Tail { entries, pos } => Ok(entries.get(*pos).cloned()),
            Cursor::Seg { meta, idx, todo, cur, pos } => loop {
                if let Some(b) = cur {
                    while *pos < b.len() {
                        let e = if reverse { &b[b.len() - 1 - *pos] } else { &b[*pos] };
                        if range.contains(&e.key) {
                            return Ok(Some(e.clone()));
                        }
                        // Outside the range: skip if before it, stop if past it.
                        let past = if reverse { !range.after_start(&e.key) } else { !range.before_end(&e.key) };
                        if past {
                            todo.clear();
                            *cur = None;
                            return Ok(None);
                        }
                        *pos += 1;
                    }
                }
                match todo.pop() {
                    None => {
                        *cur = None;
                        return Ok(None);
                    }
                    Some(bi) => {
                        let b = self.shared.seg_block(&self.m.db_id, meta, idx, bi).await?;
                        *cur = Some(b);
                        *pos = 0;
                    }
                }
            },
        }
    }

    fn advance(c: &mut Cursor) {
        match c {
            Cursor::Tail { pos, .. } | Cursor::Seg { pos, .. } => *pos += 1,
        }
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
        let mut cursors = self.cursors(range, reverse, None).await?;
        let mut heads: Vec<Option<Entry>> = Vec::with_capacity(cursors.len());
        for c in cursors.iter_mut() {
            heads.push(self.peek(c, range, reverse).await?);
        }
        loop {
            let mut best: Option<usize> = None;
            for (i, h) in heads.iter().enumerate() {
                let Some(e) = h else { continue };
                let better = match best.and_then(|b| heads[b].as_ref()) {
                    None => true,
                    Some(cur) => {
                        if reverse {
                            e.key > cur.key
                        } else {
                            e.key < cur.key
                        }
                    }
                };
                if better {
                    best = Some(i);
                }
            }
            let Some(winner) = best.and_then(|b| heads[b].clone()) else { break };
            for i in 0..cursors.len() {
                if heads[i].as_ref().is_some_and(|e| e.key == winner.key) {
                    Self::advance(&mut cursors[i]);
                    heads[i] = self.peek(&mut cursors[i], range, reverse).await?;
                }
            }
            if include_tombstones || !winner.value.is_tombstone() {
                out.push(winner);
                if out.len() >= limit {
                    break;
                }
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
        let mut cursors = self.cursors(range, false, Some(after)).await?;
        for c in cursors.iter_mut() {
            loop {
                match self.peek(c, range, false).await? {
                    None => break,
                    Some(e) => {
                        if e.seq > after {
                            return Ok(true);
                        }
                        Self::advance(c);
                    }
                }
            }
        }
        Ok(false)
    }

    /// Point version of [`Version::changed_since`].
    pub async fn key_changed_since(&self, key: &[u8], after: Seq) -> Result<bool> {
        Ok(self.get_entry(key).await?.is_some_and(|e| e.seq > after))
    }

    /// Every live entry (used for GC reference scans and checks).
    pub async fn all_entries(&self, include_tombstones: bool) -> Result<Vec<Entry>> {
        self.scan(&KeyRange::all(), false, usize::MAX, include_tombstones).await
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
