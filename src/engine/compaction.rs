//! Size-tiered compaction policy and merge.

use super::Shared;
use super::manifest::{Manifest, SegmentMeta};
use super::segment::{SegmentBuilder, SegmentStats};
use super::types::{Entry, Seq};
use super::version::{KeyRange, Version};
use crate::error::Result;
use bytes::Bytes;
use std::ops::Range;
use std::sync::Arc;

/// Choose a contiguous run of segments (newest first) to merge.
pub fn pick_run(segs: &[SegmentMeta], fanout: usize, max_segments: usize) -> Option<Range<usize>> {
    if segs.len() < 2 {
        return None;
    }
    let mut acc = segs[0].size.max(1);
    let mut end = 1;
    while end < segs.len() && segs[end].size <= acc.saturating_mul(2) {
        acc += segs[end].size;
        end += 1;
    }
    if end >= fanout.max(2) {
        return Some(0..end);
    }
    if segs.len() > max_segments {
        let k = (segs.len() - max_segments + 1).max(2);
        return Some(0..k);
    }
    None
}

pub struct MergeOutput {
    pub data: Option<(Bytes, SegmentStats)>,
    /// Highest seq of a purged tombstone (0 if none).
    pub purged_max_seq: Seq,
}

/// Merge `run` (newest first). Tombstones with `seq <= purge_seq` are
/// dropped when `bottom` (the run includes the oldest segment).
pub async fn merge(
    shared: &Arc<Shared>,
    m: &Manifest,
    run: &[SegmentMeta],
    bottom: bool,
    purge_seq: Seq,
) -> Result<MergeOutput> {
    let view = Version {
        m: Manifest { db_id: m.db_id.clone(), segments: run.to_vec(), ..Default::default() },
        etag: String::new(),
        shared: shared.clone(),
    };
    let entries: Vec<Entry> = view.scan(&KeyRange::all(), false, usize::MAX, true).await?;
    let mut b = SegmentBuilder::new(shared.cfg.block_target);
    let mut purged_max_seq = 0;
    for e in &entries {
        if bottom && e.value.is_tombstone() && e.seq <= purge_seq {
            purged_max_seq = purged_max_seq.max(e.seq);
            continue;
        }
        b.add(e);
    }
    let data = if b.is_empty() { None } else { Some(b.finish()) };
    Ok(MergeOutput { data, purged_max_seq })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(size: u64) -> SegmentMeta {
        SegmentMeta {
            id: "0".repeat(32),
            size,
            entries: 1,
            tombstones: 0,
            min_key: Bytes::new(),
            max_key: Bytes::new(),
            min_seq: 0,
            max_seq: 0,
        }
    }

    #[test]
    fn policy() {
        assert_eq!(pick_run(&[seg(10)], 4, 12), None);
        assert_eq!(pick_run(&[seg(10), seg(10), seg(10)], 4, 12), None);
        assert_eq!(pick_run(&[seg(10), seg(10), seg(10), seg(10)], 4, 12), Some(0..4));
        // A big old segment is not pulled in by small new ones.
        assert_eq!(pick_run(&[seg(10), seg(10), seg(10), seg(1000)], 4, 12), None);
        assert_eq!(pick_run(&[seg(10), seg(10), seg(10), seg(10), seg(1000)], 4, 12), Some(0..4));
        assert_eq!(pick_run(&[seg(300), seg(300), seg(300), seg(1000)], 4, 12), Some(0..4));
        assert_eq!(pick_run(&[seg(10), seg(10)], 2, 12), Some(0..2));
        // Segments too different in size to tier are merged only beyond
        // `max_segments`.
        let many: Vec<_> = (0..14).map(|i| seg(1u64 << (3 * i))).collect();
        assert_eq!(pick_run(&many[..12], 4, 12), None);
        assert_eq!(pick_run(&many[..13], 4, 12), Some(0..2));
        assert_eq!(pick_run(&many, 4, 12), Some(0..3));
    }

    #[test]
    fn logarithmic_segment_count() {
        // Simulate repeated flushes of equal size with the policy applied.
        let mut segs: Vec<SegmentMeta> = Vec::new();
        for _ in 0..1000 {
            segs.insert(0, seg(100));
            while let Some(r) = pick_run(&segs, 4, 12) {
                let total: u64 = segs[r.clone()].iter().map(|s| s.size).sum();
                segs.splice(r, [seg(total)]);
            }
            assert!(segs.len() <= 12, "too many segments: {}", segs.len());
        }
        let total: u64 = segs.iter().map(|s| s.size).sum();
        assert_eq!(total, 100_000);
    }
}
