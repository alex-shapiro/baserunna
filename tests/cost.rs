//! Budgets for object-store requests per operation, typical monthly database
//! cost, and compaction amplification. The measurements are deterministic, so
//! any failure reflects a real change.
//!
//! Run `cargo run --release --example cost_model` and `--example amplification`
//! to print the current numbers.

use baserunna::engine::EngineConfig;
use baserunna::engine::segment::Compression;
use baserunna::sim::amp::{self, measure_all as measure_amplification, measure_streams};
use baserunna::sim::cost::{Counts, measure_all, monthly_cost, profiles};

/// Upper bounds per unit: `(measurement, GET, PUT, LIST, DELETE)`.
const BUDGETS: &[(&str, f64, f64, f64, f64)] = &[
    ("commit", 0.0, 1.0, 0.0, 0.0),
    ("strong_read_leased", 0.0, 0.0, 0.0, 0.0),
    ("group_commit_64", 0.0, 0.05, 0.0, 0.0),
    ("blob_upload", 0.0, 0.25, 0.0, 0.0),
    ("blob_download", 0.125, 0.0, 0.0, 0.0),
    ("strong_read_unleased", 1.0, 0.0, 0.0, 0.0),
    // The window includes the first hourly orphan sweep's LISTs.
    ("hot_reads_10_per_s", 60.0, 300.0, 12.0, 0.0),
    ("idle_db", 0.0, 0.0, 0.0, 0.0),
    // One manifest validation per staleness window; waiting requests read nothing.
    ("idle_watcher", 13.0, 0.0, 0.0, 0.0),
    // The writer's commit, and the home node's refresh when notified of it.
    ("watch_wakeup", 2.0, 1.0, 0.0, 0.0),
    ("steady_write", 0.01, 1.01, 0.002, 0.002),
    ("node_overhead_1_node", 122.0, 31.0, 2.0, 0.0),
    ("node_overhead_3_nodes", 145.0, 31.0, 1.0, 0.0),
    ("cold_open_point_read", 2.0, 0.0, 0.0, 0.0),
    ("cold_full_scan", 2.0, 0.0, 0.0, 0.0),
    ("strong_read_validated", 1.0, 0.0, 0.0, 0.0),
    ("snapshot_read", 0.0, 0.0, 0.0, 0.0),
    ("restart_open_and_scan", 1.0, 0.0, 0.0, 0.0),
    ("stream_append", 0.0, 1.0, 0.0, 0.0),
    ("stream_tail_read_leased", 0.0, 0.0, 0.0, 0.0),
    ("log_ingest", 0.5, 62.0, 0.1, 0.1),
    ("cold_stream_read", 5.5, 0.0, 0.0, 0.0),
];

/// Monthly budget in USD for the `typical` profile.
const TYPICAL_MONTHLY_BUDGET: f64 = 4.5;

/// Stored bytes per live logical byte after steady overwrites.
const SPACE_AMPLIFICATION_BUDGET: f64 = 1.3;

#[test]
fn request_costs_stay_within_budget() {
    let r = measure_all();
    println!("{}", r.table());
    let mut failures = Vec::new();
    for &(name, gets, puts, lists, deletes) in BUDGETS {
        let c = r.get(name).per_unit;
        let budget = Counts { gets, puts, lists, deletes, put_bytes: 0.0 };
        for (what, got, max) in [
            ("GET", c.gets, budget.gets),
            ("PUT", c.puts, budget.puts),
            ("LIST", c.lists, budget.lists),
            ("DELETE", c.deletes, budget.deletes),
        ] {
            if got > max + 1e-9 {
                failures.push(format!("{name}: {got:.3} {what} per {} exceeds budget {max}", r.get(name).unit));
            }
        }
    }
    let names: Vec<&str> = BUDGETS.iter().map(|b| b.0).collect();
    for m in &r.measurements {
        if !names.contains(&m.name) {
            failures.push(format!("{}: missing budget row", m.name));
        }
    }
    if r.space_amplification > SPACE_AMPLIFICATION_BUDGET {
        failures.push(format!(
            "space amplification {:.2} exceeds the budget of {SPACE_AMPLIFICATION_BUDGET}",
            r.space_amplification
        ));
    }
    let typical = profiles().into_iter().find(|p| p.name == "typical").expect("typical profile");
    let bill = monthly_cost(&r, &typical);
    if bill.total() > TYPICAL_MONTHLY_BUDGET {
        failures.push(format!(
            "typical database costs ${:.2}/month, over the budget of ${TYPICAL_MONTHLY_BUDGET}: {bill:?}",
            bill.total()
        ));
    }
    assert!(
        failures.is_empty(),
        "cost regressions (if intentional, update the budgets in tests/cost.rs and SPEC §13.5):\n  {}",
        failures.join("\n  ")
    );
}

/// Upper bounds per workload on write, settled space, and read amplification
/// of the compaction policy without compression.
const AMPLIFICATION_BUDGETS: &[(&str, f64, f64, f64)] = &[
    ("uniform_update", 3.5, 1.4, 3.0),
    ("skewed_update", 3.0, 1.3, 2.5),
    ("append", 2.2, 1.1, 1.0),
    ("queue", 3.0, 2.2, 3.0),
    ("mixed", 2.5, 1.4, 3.0),
];

#[test]
fn amplification_stays_within_budget() {
    let cfg = EngineConfig { compression: Compression::None, ..EngineConfig::default() };
    let rows = measure_amplification(0.25, &cfg);
    println!("{}", amp::table(&rows));
    let mut failures = Vec::new();
    for a in &rows {
        let &(_, write, space, read) =
            AMPLIFICATION_BUDGETS.iter().find(|b| b.0 == a.workload.name()).expect("a budget per workload");
        if a.write_amp > write || a.space_amp_settled > space || a.read_amp > read {
            failures.push(format!(
                "{}: write {:.2}x (budget {write}), settled space {:.2}x (budget {space}), read {:.2} (budget {read})",
                a.workload.name(),
                a.write_amp,
                a.space_amp_settled,
                a.read_amp
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "amplification regressions (if intentional, update the budgets in tests/cost.rs and SPEC §5.6):\n  {}",
        failures.join("\n  ")
    );
}

/// Upper bounds per stream workload on write amplification without
/// compression, and lower bounds on the share of records sealed (SPEC §5.11).
const STREAM_BUDGETS: &[(&str, f64, f64)] = &[("log", 1.3, 0.85), ("feeds", 3.0, 0.5), ("chat", 3.0, 0.0)];

#[test]
fn stream_amplification_stays_within_budget() {
    let cfg = EngineConfig { compression: Compression::None, ..EngineConfig::default() };
    let rows = measure_streams(0.25, &cfg);
    println!("{}", amp::stream_table(&rows));
    let mut failures = Vec::new();
    for a in &rows {
        let &(_, write, sealed) = STREAM_BUDGETS.iter().find(|b| b.0 == a.workload.name()).expect("a budget");
        if a.write_amp > write || a.sealed < sealed {
            failures.push(format!(
                "{}: write {:.2}x (budget {write}), {:.0}% sealed (at least {:.0}%)",
                a.workload.name(),
                a.write_amp,
                100.0 * a.sealed,
                100.0 * sealed
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "stream amplification regressions (if intentional, update the budgets in tests/cost.rs and SPEC §5.11):\n  {}",
        failures.join("\n  ")
    );
}
