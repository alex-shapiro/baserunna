//! Every documented API operation must be exercised by the deterministic
//! simulation suite (one seed of every scenario).

use baserunna::sim::openapi_check::{coverage, documented_operations};
use baserunna::sim::run;
use baserunna::sim::workloads::scenarios;

#[test]
fn simulation_exercises_every_documented_operation() {
    for s in scenarios() {
        let out = run(1, (s.config)(), s.run);
        assert!(out.result.is_ok(), "scenario {} failed: {:?}", s.name, out.result);
    }
    let covered = coverage();
    let missing: Vec<_> = documented_operations().difference(&covered).cloned().collect();
    assert!(missing.is_empty(), "documented operations never exercised by the simulator: {missing:?}");
}
