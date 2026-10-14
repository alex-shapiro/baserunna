//! Leveled compaction with dynamic level sizes (SPEC §5.6).
//!
//! Level 0 holds flushed tails. Levels 1 to [`MAX_LEVEL`] are sorted runs,
//! each about `level_size_ratio` times smaller than the next, sized from the
//! deepest level down so that it holds most of the data. Level 0 compacts
//! into the shallowest level in use, and a level over its target pushes one
//! segment into the next. Merges stream their inputs and write output
//! segments of bounded size.

use super::Shared;
use super::config::EngineConfig;
use super::manifest::{MAX_LEVEL, Manifest, SegmentMeta};
use super::range_delete::RangeDeletes;
use super::segment::SegmentBuilder;
use super::types::{Entry, Seq, is_commit_key};
use super::version::{IterSpec, KeyRange, MergeIter, ReadMode};
use super::{seg_key, segment};
use crate::error::Result;
use crate::store::PutMode;
use std::sync::Arc;

/// A compaction chosen by [`pick`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plan {
    /// Segments replaced by the compaction.
    pub inputs: Vec<SegmentMeta>,
    pub kind: Kind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Merge overlapping segments from one level into the next target level.
    Merge { level: u8 },
    /// Move a segment directly into `level` without rewriting because it has no overlaps.
    Move { level: u8 },
    /// Drop segments whose entries are entirely deleted by range tombstones.
    Drop,
    /// Rewrite segments in-place on their current level to purge range-deleted entries.
    Rewrite,
    /// Drop segments of commit records past their retention.
    DropExpired,
    /// Rewrite segments in place whose entries have mostly expired,
    /// keeping only tombstones of them.
    Expire,
}

/// Maximum number of segments replaced in a single rewrite compaction.
const REWRITE_MAX_INPUTS: usize = 8;

/// Returns the base level and each level's target size in bytes. Levels above
/// the base have target 0.
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

/// Selects the highest-priority compaction plan, prioritizing cleanup of range-deleted segments.
pub fn pick(m: &Manifest, cfg: &EngineConfig) -> Option<Plan> {
    let (base, targets) = level_targets(m, cfg);
    let dirty = |s: &&SegmentMeta| m.range_deletes.iter().any(|t| t.dirties(s));
    let covered: Vec<SegmentMeta> =
        m.segments.iter().filter(|s| m.range_deletes.iter().any(|t| t.covers(s))).cloned().collect();
    if !covered.is_empty() {
        return Some(Plan { inputs: covered, kind: Kind::Drop });
    }
    let records_horizon = records_horizon(m, cfg);
    let lapsed: Vec<SegmentMeta> =
        m.segments.iter().filter(|s| holds_records(s) && s.max_seq <= records_horizon).cloned().collect();
    if !lapsed.is_empty() {
        return Some(Plan { inputs: lapsed, kind: Kind::DropExpired });
    }
    // Drain levels above the base level downward before merging L0.
    let above_base = (1..base).any(|l| !m.level(l).is_empty());
    // Data and commit records are flushed to separate level 0 segments and
    // compacted independently.
    let (records_l0, data_l0): (Vec<&SegmentMeta>, Vec<&SegmentMeta>) =
        m.level(0).iter().partition(|s| holds_records(s));
    let max_inputs = cfg.l0_compaction_max_inputs.max(1);
    if !above_base && let Some(newest) = data_l0.iter().position(&dirty) {
        return Some(l0_merge(m, base, &data_l0, (data_l0.len() - newest).min(max_inputs)));
    }
    let rewrite: Vec<SegmentMeta> = m.deep_segments().iter().filter(dirty).take(REWRITE_MAX_INPUTS).cloned().collect();
    if !rewrite.is_empty() {
        return Some(Plan { inputs: rewrite, kind: Kind::Rewrite });
    }
    let expire: Vec<SegmentMeta> =
        m.deep_segments().iter().filter(|s| mostly_expired(s, m.time_ms)).take(REWRITE_MAX_INPUTS).cloned().collect();
    if !expire.is_empty() {
        return Some(Plan { inputs: expire, kind: Kind::Expire });
    }
    let mut best: Option<(f64, u8)> = None;
    let l0 = if data_l0.len() >= records_l0.len() { &data_l0 } else { &records_l0 };
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
        return Some(l0_merge(m, base, l0, l0.len().min(max_inputs)));
    }
    let next = level + 1;
    let cost = |s: &SegmentMeta| {
        let o: u64 = overlapping(m, next, &s.min_key, &s.max_key).iter().map(|x| x.size).sum();
        o as f64 / s.size.max(1) as f64
    };
    let seg = m.level(level).iter().min_by(|a, b| cost(a).total_cmp(&cost(b)))?;
    let below = overlapping(m, next, &seg.min_key, &seg.max_key);
    let mut inputs = vec![seg.clone()];
    inputs.extend_from_slice(below);
    let kind = if below.is_empty() { Kind::Move { level: next } } else { Kind::Merge { level: next } };
    Some(Plan { inputs, kind })
}

/// Whether `s` holds only commit records.
pub(crate) fn holds_records(s: &SegmentMeta) -> bool {
    is_commit_key(&s.min_key)
}

/// Commit records up to this version are past their retention, by the
/// manifest's time.
pub(crate) fn records_horizon(m: &Manifest, cfg: &EngineConfig) -> Seq {
    m.version_at_least_age(m.time_ms, cfg.commit_retention_ms)
}

/// A segment at least half of whose entries have expired by `now`.
fn mostly_expired(s: &SegmentMeta, now: u64) -> bool {
    s.expiring > 0 && s.expires_max <= now && 2 * s.expiring >= s.entries
}

/// Plans a merge of the oldest `take` segments from `l0` with overlapping base-level segments.
fn l0_merge(m: &Manifest, base: u8, l0: &[&SegmentMeta], take: usize) -> Plan {
    let mut inputs: Vec<SegmentMeta> = l0[l0.len() - take.min(l0.len())..].iter().map(|s| (*s).clone()).collect();
    let min = inputs.iter().map(|s| s.min_key.clone()).min().unwrap_or_default();
    let max = inputs.iter().map(|s| s.max_key.clone()).max().unwrap_or_default();
    inputs.extend_from_slice(overlapping(m, base, &min, &max));
    Plan { inputs, kind: Kind::Merge { level: base } }
}

pub struct Output {
    pub segments: Vec<SegmentMeta>,
    /// Highest seq of a purged tombstone, or 0 if none.
    pub purged_max_seq: Seq,
    /// Entries dropped or reduced to tombstones because they had expired.
    pub expired: u64,
}

/// True if a segment of a level deeper than `level` covers `key`, so a
/// tombstone for it still shadows something.
fn covered_below(m: &Manifest, level: u8, key: &[u8]) -> bool {
    (level + 1..=MAX_LEVEL).any(|l| !overlapping(m, l, key, key).is_empty())
}

/// Executes a compaction plan, writing output segments and purging obsolete entries.
pub async fn run(shared: &Arc<Shared>, m: &Manifest, plan: &Plan, purge_seq: Seq) -> Result<Output> {
    let deletes = Arc::new(RangeDeletes::new(&m.range_deletes));
    let mut out = Output { segments: Vec::new(), purged_max_seq: 0, expired: 0 };
    match plan.kind {
        Kind::Merge { level } => {
            merge(shared, m, sorted_runs(&plan.inputs), level, &deletes, purge_seq, &mut out).await?
        }
        Kind::Rewrite | Kind::Expire => {
            for s in &plan.inputs {
                merge(shared, m, vec![vec![s.clone()]], s.level, &deletes, purge_seq, &mut out).await?;
            }
        }
        Kind::Move { .. } | Kind::Drop | Kind::DropExpired => {}
    }
    Ok(out)
}

async fn merge(
    shared: &Arc<Shared>,
    m: &Manifest,
    runs: Vec<Vec<SegmentMeta>>,
    level: u8,
    deletes: &Arc<RangeDeletes>,
    purge_seq: Seq,
    out: &mut Output,
) -> Result<()> {
    let cfg = &shared.cfg;
    let spec = IterSpec {
        range: KeyRange::all(),
        reverse: false,
        include_tombstones: true,
        mode: ReadMode::Bulk,
        deletes: Some(deletes.clone()),
        expire_at: None,
    };
    let mut it = MergeIter::new(shared.clone(), &m.db_id, &[], runs, spec);
    let mut b = SegmentBuilder::new(cfg.block_target, cfg.compression);
    let mut records = false;
    let records_horizon = records_horizon(m, cfg);
    while let Some(e) = it.next().await? {
        if is_commit_key(&e.key) && e.seq <= records_horizon {
            out.expired += 1;
            continue;
        }
        // Every reader of the manifest the output joins reads at its time
        // or later, when these entries have expired.
        let e = match e.expired(m.time_ms) {
            false => e,
            true => {
                out.expired += 1;
                Entry::tombstone(e.key, e.seq)
            }
        };
        if e.value.is_tombstone() && e.seq <= purge_seq && !covered_below(m, level, &e.key) {
            out.purged_max_seq = out.purged_max_seq.max(e.seq);
            continue;
        }
        // Cut where commit records begin, so that no segment holds both.
        if b.size_estimate() >= cfg.segment_target_bytes || (is_commit_key(&e.key) && !b.is_empty() && !records) {
            let full = std::mem::replace(&mut b, SegmentBuilder::new(cfg.block_target, cfg.compression));
            out.segments.push(write(shared, &m.db_id, full, level, m.version).await?);
        }
        records = is_commit_key(&e.key);
        b.add(&e);
    }
    if !b.is_empty() {
        out.segments.push(write(shared, &m.db_id, b, level, m.version).await?);
    }
    Ok(())
}

/// Groups `inputs` into sorted runs: each level 0 segment is its own run,
/// and deeper segments on the same level share a run.
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

/// The segments below level 0 once `inputs` are replaced by `outputs`.
pub fn deep_after(m: &Manifest, inputs: &[String], outputs: &[SegmentMeta]) -> Vec<SegmentMeta> {
    let mut deep: Vec<SegmentMeta> =
        m.deep_segments().iter().filter(|s| !inputs.contains(&s.id)).chain(outputs).cloned().collect();
    deep.sort_by(|a, b| a.level.cmp(&b.level).then_with(|| a.min_key.cmp(&b.min_key)));
    deep
}

/// Finishes and stores a new segment, recording the sequence number through
/// which range deletions were applied.
pub(crate) async fn write(
    shared: &Shared,
    db: &str,
    b: SegmentBuilder,
    level: u8,
    deletes_applied: Seq,
) -> Result<SegmentMeta> {
    let (data, stats) = b.finish();
    let id = shared.env.random_hex(16);
    let key = seg_key(db, &id);
    shared.retry.run(|| shared.store.put(&key, data.clone(), PutMode::Overwrite)).await?;
    shared.remember_segment(db, &id, &data, level == 0)?;
    Ok(seg_meta(id, level, &stats, deletes_applied))
}

pub(crate) fn seg_meta(id: String, level: u8, s: &segment::SegmentStats, deletes_applied: Seq) -> SegmentMeta {
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
        deletes_applied,
        expiring: s.expiring,
        expires_max: s.expires_max,
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
            deletes_applied: 0,
            expiring: 0,
            expires_max: 0,
        }
    }

    fn target(p: &Plan) -> (u8, bool) {
        match p.kind {
            Kind::Merge { level } => (level, false),
            Kind::Move { level } => (level, true),
            k => panic!("{k:?}"),
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
        let mut m = Manifest::new("d", 0, None);
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
        assert_eq!(target(&p), (MAX_LEVEL, false));
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
        let p = pick(&manifest(segs.clone()), &c).unwrap();
        assert_eq!((ids(&p), target(&p)), (vec![2], (MAX_LEVEL, true)), "m..n overlaps nothing");
        let mut segs = segs;
        segs[1] = seg(2, base, 400, "d", "e");
        segs[3] = seg(10, MAX_LEVEL, 30, "a", "b");
        let p = pick(&manifest(segs), &c).unwrap();
        assert_eq!((ids(&p), target(&p).1), (vec![2, 11], false), "the least overlap relative to size");
        let segs = vec![
            seg(1, base, 100, "a", "b"),
            seg(2, base, 1_000, "m", "n"),
            seg(10, MAX_LEVEL, 1_000, "a", "b"),
            seg(11, MAX_LEVEL, 3_000, "m", "n"),
        ];
        assert_eq!(ids(&pick(&manifest(segs), &c).unwrap()), [2, 11]);
    }

    #[test]
    fn the_most_urgent_level_goes_first() {
        let c = cfg();
        let base = MAX_LEVEL - 1;
        let mut segs = vec![seg(1, base, 1_350, "a", "c"), seg(10, MAX_LEVEL, 9_000, "a", "z")];
        segs.extend((0..8).map(|i| seg(20 + i, 0, 10, "a", "b")));
        assert_eq!(target(&pick(&manifest(segs.clone()), &c).unwrap()).0, base, "level 0, at twice its trigger");
        segs.truncate(2 + 5);
        assert_eq!(target(&pick(&manifest(segs), &c).unwrap()).0, MAX_LEVEL, "level 0, at 1.25 times its trigger");
        let deep = |l4: u64, l5: u64| {
            let segs = vec![seg(1, 4, l4, "a", "b"), seg(2, 5, l5, "a", "b"), seg(3, MAX_LEVEL, 90_000, "a", "z")];
            target(&pick(&manifest(segs), &c).unwrap()).0
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
        assert_eq!((ids(&p), target(&p).0), (vec![1], 3));
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
