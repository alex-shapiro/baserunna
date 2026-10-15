//! Verifies that deterministic simulation scenarios exercise all documented
//! API operations and core storage engine execution paths.

use baserunna::engine::Activity;
use baserunna::sim::openapi_check::{coverage, documented_operations};
use baserunna::sim::run;
use baserunna::sim::workloads::scenarios;

#[test]
fn simulation_exercises_every_documented_operation_and_engine_path() {
    let mut engine = Activity::default();
    for s in scenarios() {
        let out = run(1, (s.config)(), s.run);
        assert!(out.result.is_ok(), "scenario {} failed: {:?}", s.name, out.result);
        engine.add(&out.engine);
    }
    let covered = coverage();
    let missing: Vec<_> = documented_operations().difference(&covered).cloned().collect();
    assert!(missing.is_empty(), "documented operations never exercised by the simulator: {missing:?}");
    let Activity {
        range_tombstones_written,
        range_tombstones_retired,
        segments_dropped,
        segments_rewritten,
        levels_objects_written,
        quota_refusals,
        expired_entries,
        segments_expired,
        chunks_sealed_at_flush,
        chunks_sealed_in_compaction,
        retention_trims,
    } = engine;
    for (path, n) in [
        ("range tombstones written", range_tombstones_written),
        ("range tombstones retired", range_tombstones_retired),
        ("segments dropped by range tombstones", segments_dropped),
        ("segments rewritten for range tombstones", segments_rewritten),
        ("levels objects written", levels_objects_written),
        ("commits refused for the quota", quota_refusals),
        ("expired entries dropped or reduced to tombstones", expired_entries),
        ("segments rewritten or dropped for expired entries", segments_expired),
        ("stream chunks sealed at flush", chunks_sealed_at_flush),
        ("stream chunks sealed in compaction", chunks_sealed_in_compaction),
        ("streams trimmed for retention", retention_trims),
    ] {
        assert!(n > 0, "the simulator never exercised: {path} ({engine:?})");
    }
}
