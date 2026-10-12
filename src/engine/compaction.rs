//! Leveled compaction with dynamic level sizes (SPEC §5.6).
//!
//! Level 0 holds flushed tails. Levels 1 to [`MAX_LEVEL`] are sorted runs,
//! each about `level_size_ratio` times smaller than the next, sized from the
//! deepest level down so that it holds most of the data. Level 0 compacts
//! into the shallowest level in use (the base level), and a level over its
//! target pushes one segment into the next. Merges stream their inputs and
//! write output segments of bounded size, so memory stays bounded however
//! large the database.

use super::Shared;
use super::config::EngineConfig;
use super::manifest::{MAX_LEVEL, Manifest, SegmentMeta};
use super::segment::SegmentBuilder;
use super::types::Seq;
use super::version::{IterSpec, KeyRange, MergeIter, ReadMode};
use super::{seg_key, segment};
use crate::error::Result;
use crate::store::PutMode;
use std::sync::Arc;

/// A compaction chosen by [`pick`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plan {
    /// Segments merged (and replaced) by the compaction.
    pub inputs: Vec<SegmentMeta>,
    pub output_level: u8,
    /// The single input overlaps nothing in the output level, so it moves
    /// there without being rewritten.
    pub trivial: bool,
}

/// The base level (where level 0 compacts into) and each level's target
/// size. Levels above the base have target 0: they should be empty.
pub fn level_targets(m: &Manifest, cfg: &EngineConfig) -> (u8, [u64; MAX_LEVEL as usize + 1]) {
    let mut t = [0u64; MAX_LEVEL as usize + 1];
    let ratio = cfg.level_size_ratio.max(2);
    t[MAX_LEVEL as usize] = m.level_bytes(MAX_LEVEL).max(cfg.level_base_bytes);
    for l in (1..MAX_LEVEL as usize).rev() {
        t[l] = t[l + 1] / ratio;
    }
    let base = (1..=MAX_LEVEL).find(|l| t[*l as usize] >= cfg.level_base_bytes).unwrap_or(MAX_LEVEL);
    for x in t.iter_mut().take(base as usize) {
        *x = 0;
    }
    (base, t)
}

/// The segments of `level` whose key ranges intersect `[min, max]`.
fn overlapping<'a>(m: &'a Manifest, level: u8, min: &[u8], max: &[u8]) -> &'a [SegmentMeta] {
    let segs = m.level(level);
    let start = segs.partition_point(|s| s.max_key.as_ref() < min);
    let end = segs.partition_point(|s| s.min_key.as_ref() <= max);
    &segs[start..end.max(start)]
}

/// The most urgent compaction, if any level needs one.
pub fn pick(m: &Manifest, cfg: &EngineConfig) -> Option<Plan> {
    let (base, targets) = level_targets(m, cfg);
    let mut best: Option<(f64, u8)> = None;
    let l0 = m.level(0);
    if l0.len() >= cfg.l0_compaction_trigger.max(1) {
        best = Some((l0.len() as f64 / cfg.l0_compaction_trigger.max(1) as f64, 0));
    }
    for l in 1..MAX_LEVEL {
        let size = m.level_bytes(l);
        if size == 0 {
            continue;
        }
        let score = if l < base { f64::INFINITY } else { size as f64 / targets[l as usize] as f64 };
        if score >= 1.0 && best.is_none_or(|(b, _)| score > b) {
            best = Some((score, l));
        }
    }
    let (_, level) = best?;
    if level == 0 {
        // The oldest level 0 segments, so the ones left stay newer.
        let take = l0.len().min(cfg.l0_compaction_max_inputs.max(1));
        let mut inputs: Vec<SegmentMeta> = l0[l0.len() - take..].to_vec();
        let min = inputs.iter().map(|s| s.min_key.clone()).min().unwrap_or_default();
        let max = inputs.iter().map(|s| s.max_key.clone()).max().unwrap_or_default();
        inputs.extend_from_slice(overlapping(m, base, &min, &max));
        return Some(Plan { inputs, output_level: base, trivial: false });
    }
    // The segment that overlaps least of the next level, relative to its
    // size: the cheapest to push down.
    let next = level + 1;
    let cost = |s: &SegmentMeta| {
        let o: u64 = overlapping(m, next, &s.min_key, &s.max_key).iter().map(|x| x.size).sum();
        o as f64 / s.size.max(1) as f64
    };
    let seg = m.level(level).iter().min_by(|a, b| cost(a).total_cmp(&cost(b)))?;
    let below = overlapping(m, next, &seg.min_key, &seg.max_key);
    let mut inputs = vec![seg.clone()];
    inputs.extend_from_slice(below);
    Some(Plan { trivial: below.is_empty(), inputs, output_level: next })
}

pub struct Output {
    pub segments: Vec<SegmentMeta>,
    /// Highest seq of a purged tombstone (0 if none).
    pub purged_max_seq: Seq,
}

/// True if a segment of a level deeper than `level` covers `key`, so a
/// tombstone for it still shadows something.
fn covered_below(m: &Manifest, level: u8, key: &[u8]) -> bool {
    (level + 1..=MAX_LEVEL).any(|l| !overlapping(m, l, key, key).is_empty())
}

/// Merge the inputs of `plan` (not a trivial move) into new segments of the
/// output level, written to the store as they fill. Tombstones with
/// `seq <= purge_seq` are dropped where no deeper level could hold an older
/// entry for their key.
pub async fn run(shared: &Arc<Shared>, m: &Manifest, plan: &Plan, purge_seq: Seq) -> Result<Output> {
    let cfg = &shared.cfg;
    let runs = sorted_runs(&plan.inputs);
    let spec = IterSpec { range: KeyRange::all(), reverse: false, include_tombstones: true, mode: ReadMode::Bulk };
    let mut it = MergeIter::new(shared.clone(), &m.db_id, &[], runs, spec);
    let mut out = Output { segments: Vec::new(), purged_max_seq: 0 };
    let mut b = SegmentBuilder::new(cfg.block_target, cfg.compression);
    while let Some(e) = it.next().await? {
        if e.value.is_tombstone() && e.seq <= purge_seq && !covered_below(m, plan.output_level, &e.key) {
            out.purged_max_seq = out.purged_max_seq.max(e.seq);
            continue;
        }
        if b.size_estimate() >= cfg.segment_target_bytes {
            let full = std::mem::replace(&mut b, SegmentBuilder::new(cfg.block_target, cfg.compression));
            out.segments.push(write(shared, &m.db_id, full, plan.output_level).await?);
        }
        b.add(&e);
    }
    if !b.is_empty() {
        out.segments.push(write(shared, &m.db_id, b, plan.output_level).await?);
    }
    Ok(out)
}

/// The sorted runs of `inputs` (in manifest order), each read by one
/// cursor: a level 0 segment alone, or a deeper level's segments together,
/// so a merge holds one read per level rather than one per segment.
fn sorted_runs(inputs: &[SegmentMeta]) -> Vec<Vec<SegmentMeta>> {
    let mut runs: Vec<Vec<SegmentMeta>> = Vec::new();
    for s in inputs {
        match runs.last_mut() {
            Some(r) if s.level > 0 && r[0].level == s.level => r.push(s.clone()),
            _ => runs.push(vec![s.clone()]),
        }
    }
    runs
}

/// Finish and store a new segment (unconditional PUT under a fresh id).
pub(crate) async fn write(shared: &Shared, db: &str, b: SegmentBuilder, level: u8) -> Result<SegmentMeta> {
    let (data, stats) = b.finish();
    let id = shared.env.random_hex(16);
    let key = seg_key(db, &id);
    shared.retry.run(|| shared.store.put(&key, data.clone(), PutMode::Overwrite)).await?;
    shared.remember_segment(db, &id, &data, level == 0)?;
    Ok(seg_meta(id, level, &stats))
}

pub(crate) fn seg_meta(id: String, level: u8, s: &segment::SegmentStats) -> SegmentMeta {
    SegmentMeta {
        id,
        level,
        size: s.size,
        entries: s.entries,
        tombstones: s.tombstones,
        min_key: s.min_key.clone(),
        max_key: s.max_key.clone(),
        min_seq: s.min_seq,
        max_seq: s.max_seq,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn seg(id: usize, level: u8, size: u64, min: &str, max: &str) -> SegmentMeta {
        SegmentMeta {
            id: format!("{id:032x}"),
            level,
            size,
            entries: 1,
            tombstones: 0,
            min_key: Bytes::copy_from_slice(min.as_bytes()),
            max_key: Bytes::copy_from_slice(max.as_bytes()),
            min_seq: 0,
            max_seq: 0,
        }
    }

    fn cfg() -> EngineConfig {
        EngineConfig {
            l0_compaction_trigger: 4,
            l0_compaction_max_inputs: 3,
            level_size_ratio: 10,
            level_base_bytes: 100,
            ..EngineConfig::default()
        }
    }

    fn manifest(segs: Vec<SegmentMeta>) -> Manifest {
        let mut m = Manifest::new("d", 0);
        m.segments = segs;
        m.sort_segments();
        m.check_levels().unwrap();
        m
    }

    fn ids(p: &Plan) -> Vec<usize> {
        p.inputs.iter().map(|s| usize::from_str_radix(&s.id, 16).unwrap()).collect()
    }

    #[test]
    fn levels_are_sized_from_the_bottom() {
        let c = cfg();
        let (base, t) = level_targets(&manifest(vec![]), &c);
        assert_eq!((base, t[MAX_LEVEL as usize]), (MAX_LEVEL, 100), "a new database compacts into the last level");
        let (base, t) = level_targets(&manifest(vec![seg(1, MAX_LEVEL, 50_000, "a", "z")]), &c);
        assert_eq!(base, MAX_LEVEL - 2);
        assert_eq!(&t[MAX_LEVEL as usize - 2..], &[500, 5_000, 50_000]);
        assert!(t[..MAX_LEVEL as usize - 2].iter().all(|x| *x == 0), "levels above the base are unused");
    }

    #[test]
    fn level_zero_compacts_its_oldest_segments_into_the_base() {
        let c = cfg();
        let l0 = |n: usize| (0..n).map(|i| seg(i, 0, 10, "c", "d")).collect::<Vec<_>>();
        assert_eq!(pick(&manifest(l0(3)), &c), None);
        let mut segs = l0(5);
        segs.extend([
            seg(10, MAX_LEVEL, 50, "a", "b"),
            seg(11, MAX_LEVEL, 50, "c", "c"),
            seg(12, MAX_LEVEL, 50, "e", "f"),
        ]);
        let p = pick(&manifest(segs), &c).unwrap();
        assert_eq!((p.output_level, p.trivial), (MAX_LEVEL, false));
        assert_eq!(ids(&p), [2, 3, 4, 11], "the three oldest, and what they overlap below");
    }

    #[test]
    fn full_levels_push_their_cheapest_segment_down() {
        let c = cfg();
        let base = MAX_LEVEL - 1;
        let segs = vec![
            seg(1, base, 400, "a", "c"),
            seg(2, base, 400, "m", "n"),
            seg(3, base, 400, "x", "y"),
            seg(10, MAX_LEVEL, 3_000, "a", "b"),
            seg(11, MAX_LEVEL, 3_000, "c", "l"),
            seg(12, MAX_LEVEL, 3_000, "w", "z"),
        ];
        // Level `base` targets 900 bytes and holds 1200.
        let p = pick(&manifest(segs.clone()), &c).unwrap();
        assert_eq!((ids(&p), p.output_level, p.trivial), (vec![2], MAX_LEVEL, true), "m..n overlaps nothing");
        let mut segs = segs;
        segs[1] = seg(2, base, 400, "d", "e");
        segs[3] = seg(10, MAX_LEVEL, 30, "a", "b");
        let p = pick(&manifest(segs), &c).unwrap();
        assert_eq!((ids(&p), p.trivial), (vec![2, 11], false), "the least overlap relative to size");
        // A small segment over a large overlap costs more per byte moved
        // than a large one over a moderate overlap.
        let segs = vec![
            seg(1, base, 100, "a", "b"),
            seg(2, base, 1_000, "m", "n"),
            seg(10, MAX_LEVEL, 1_000, "a", "b"),
            seg(11, MAX_LEVEL, 3_000, "m", "n"),
        ];
        assert_eq!(ids(&pick(&manifest(segs), &c).unwrap()), [2, 11]);
    }

    /// The level furthest over its target compacts first.
    #[test]
    fn the_most_urgent_level_goes_first() {
        let c = cfg();
        let base = MAX_LEVEL - 1;
        // Level `base` targets 900 bytes; 1350 is 1.5 times that.
        let mut segs = vec![seg(1, base, 1_350, "a", "c"), seg(10, MAX_LEVEL, 9_000, "a", "z")];
        segs.extend((0..8).map(|i| seg(20 + i, 0, 10, "a", "b")));
        assert_eq!(pick(&manifest(segs.clone()), &c).unwrap().output_level, base, "level 0, at twice its trigger");
        segs.truncate(2 + 5);
        assert_eq!(pick(&manifest(segs), &c).unwrap().output_level, MAX_LEVEL, "level 0, at 1.25 times its trigger");
        // With 90 kB at the bottom, the levels above target 9 kB and 900 B.
        let deep = |l4: u64, l5: u64| {
            let segs = vec![seg(1, 4, l4, "a", "b"), seg(2, 5, l5, "a", "b"), seg(3, MAX_LEVEL, 90_000, "a", "z")];
            pick(&manifest(segs), &c).unwrap().output_level
        };
        assert_eq!(deep(2_700, 13_500), 5, "3x over beats 1.5x over");
        assert_eq!(deep(1_350, 27_000), MAX_LEVEL, "3x over beats 1.5x over");
        assert_eq!(deep(1_800, 18_000), 5, "a tie goes to the shallower level");
    }

    #[test]
    fn each_level_is_read_as_one_run() {
        let inputs = [
            seg(1, 0, 10, "a", "b"),
            seg(2, 0, 10, "a", "b"),
            seg(3, 4, 10, "a", "b"),
            seg(4, 4, 10, "c", "d"),
            seg(5, 5, 10, "a", "b"),
            seg(6, 5, 10, "c", "d"),
        ];
        let shape: Vec<usize> = sorted_runs(&inputs).iter().map(|r| r.len()).collect();
        assert_eq!(shape, [1, 1, 2, 2]);
    }

    #[test]
    fn data_above_the_base_level_moves_down() {
        let c = cfg();
        let p = pick(&manifest(vec![seg(1, 2, 10, "a", "b"), seg(2, MAX_LEVEL, 100, "a", "z")]), &c).unwrap();
        assert_eq!((ids(&p), p.output_level), (vec![1], 3));
    }

    #[test]
    fn tombstones_stay_while_deeper_levels_cover_their_key() {
        let m = manifest(vec![seg(1, 3, 10, "c", "f"), seg(2, MAX_LEVEL, 10, "m", "p")]);
        assert!(covered_below(&m, 2, b"d") && covered_below(&m, 2, b"n"));
        assert!(!covered_below(&m, 3, b"d"), "only deeper levels count");
        assert!(!covered_below(&m, 2, b"h") && !covered_below(&m, 2, b"z"));
        assert!(!covered_below(&m, MAX_LEVEL, b"n"));
    }
}
