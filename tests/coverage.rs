//! Verifies that a short campaign exercises all documented API operations
//! and all non-rare probes (SPEC §13.4).

use baserunna::sim::campaign::{campaign, default_threads};
use baserunna::sim::openapi_check::{coverage, documented_operations};
use baserunna::sim::workloads::scenarios;

#[test]
fn simulation_exercises_every_documented_operation_and_probe() {
    let all = scenarios();
    let report = campaign(&all, 1..9, default_threads(), None, |_, _| {});
    if let Some((job, error)) = report.failures.first() {
        panic!("{} failed: {error}", job.args(&all));
    }
    let covered = coverage();
    let missing: Vec<_> = documented_operations().difference(&covered).cloned().collect();
    assert!(missing.is_empty(), "documented operations were not exercised: {missing:?}");
    let unreached: Vec<String> = report
        .unreached()
        .iter()
        .filter(|p| !baserunna::probe::Probe::RARE.contains(p))
        .map(|p| format!("{} ({})", p.name(), p.doc()))
        .collect();
    assert!(unreached.is_empty(), "unreached probes:\n{}\n{}", unreached.join("\n"), report.coverage_table());
}
