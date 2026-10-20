//! Simulation runner for multi-seed campaigns, guided exploration, and trace debugging.
//!
//! ```text
//! cargo run --release --example sim_tool -- list
//! cargo run --release --example sim_tool -- campaign <scenario|all> <seeds> [first_seed]   # SIM_MINUTES, SIM_COVERAGE=1
//! cargo run --release --example sim_tool -- explore <scenario|all> [first_seed]          # SIM_RUNS, SIM_MINUTES, SIM_COMPARE=1
//! cargo run --release --example sim_tool -- trace <scenario> <seed> [branches]           # full trace
//! cargo run --release --example sim_tool -- determinism <scenario> <seed> [branches]
//! ```
//!
//! Campaigns and explorations use all available CPU cores unless `SIM_THREADS` is set,
//! and stop starting new runs after `SIM_MINUTES`. When `SIM_REQUIRE_PROBES` is set,
//! a campaign exits with a non-zero status if any probe was not reached.

use baserunna::probe::Probe;
use baserunna::sim::campaign::{Job, RARE, Report, campaign, default_threads, parse_branches};
use baserunna::sim::explore::{Budget, explore};
use baserunna::sim::workloads::{Scenario, scenario, scenarios};
use baserunna::sim::{SimOutcome, run};
use baserunna::util::prop::env_u64;
use std::time::{Duration, Instant};

/// Default run limit when neither `SIM_RUNS` nor `SIM_MINUTES` is set.
const DEFAULT_EXPLORE_RUNS: u64 = 2_000;

fn run_one(name: &str, seed: u64, branches: &str, keep_log: bool) -> SimOutcome {
    let s = scenario(name).unwrap_or_else(|| panic!("unknown scenario {name}"));
    let mut cfg = s.config_for(seed);
    cfg.branches = parse_branches(branches).unwrap_or_else(|| panic!("bad branch points {branches}"));
    cfg.keep_log = keep_log;
    cfg.full_trace = keep_log;
    run(seed, cfg, s.run)
}

/// Resolves a scenario name or `"all"` to the matching list of scenarios.
fn chosen(name: &str) -> Vec<Scenario> {
    let chosen: Vec<Scenario> = scenarios().into_iter().filter(|s| name == "all" || s.name == name).collect();
    assert!(!chosen.is_empty(), "unknown scenario {name}");
    chosen
}

fn threads() -> usize {
    env_u64("SIM_THREADS").map_or_else(default_threads, |t| t as usize)
}

fn deadline(start: Instant) -> Option<Instant> {
    env_u64("SIM_MINUTES").map(|m| start + Duration::from_secs(60 * m))
}

/// Prints a failed run and the command to reproduce it.
fn report_failure(scenarios: &[Scenario]) -> impl Fn(&Job, &SimOutcome) + Sync + '_ {
    move |job, out| {
        if let Err(e) = &out.result {
            let args = job.args(scenarios);
            println!("FAIL {args}: {e}\n  reproduce: cargo run --release --example sim_tool -- trace {args}");
        }
    }
}

/// Prints failure counts grouped by normalized error message (with numbers and IDs masked).
fn print_distinct(report: &Report) {
    let mut by: std::collections::BTreeMap<String, usize> = Default::default();
    for (_, e) in &report.failures {
        let first = e.lines().next().unwrap_or("");
        let shape: String = first
            .split_inclusive(|c: char| !c.is_ascii_alphanumeric())
            .map(|w| match w.contains(|c: char| c.is_ascii_digit()) {
                true => format!("#{}", w.trim_start_matches(|c: char| c.is_ascii_alphanumeric())),
                false => w.to_string(),
            })
            .collect::<String>()
            .chars()
            .take(160)
            .collect();
        *by.entry(shape).or_default() += 1;
    }
    let mut v: Vec<(usize, String)> = by.into_iter().map(|(k, n)| (n, k)).collect();
    v.sort_by_key(|(n, _)| std::cmp::Reverse(*n));
    if !v.is_empty() {
        println!("{} distinct failures:", v.len());
        for (n, f) in v {
            println!("  {n:>4} x {f}");
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let arg = |i: usize| args.get(i).cloned().unwrap_or_default();
    match arg(0).as_str() {
        "list" => {
            for s in scenarios() {
                println!("{}", s.name);
            }
        }
        "campaign" => {
            let chosen = chosen(&arg(1));
            let n: u64 = arg(2).parse().unwrap_or(100);
            let first: u64 = arg(3).parse().unwrap_or(1);
            let t = Instant::now();
            let report = campaign(&chosen, first..first + n, threads(), deadline(t), report_failure(&chosen));
            if std::env::var("SIM_COVERAGE").is_ok() {
                print!("{}", report.coverage_table());
            }
            let unreached: Vec<&str> = report.unreached().iter().map(|p| p.name()).collect();
            println!(
                "{} runs in {:?} wall, {}s simulated, {} failed, {} probes never reached{}",
                report.runs,
                t.elapsed(),
                report.virtual_ms / 1000,
                report.failures.len(),
                unreached.len(),
                if unreached.len() <= 10 { format!(": {}", unreached.join(" ")) } else { String::new() }
            );
            print_distinct(&report);
            let missed = std::env::var("SIM_REQUIRE_PROBES").is_ok_and(|v| !v.is_empty()) && !unreached.is_empty();
            std::process::exit(if report.failures.is_empty() && !missed { 0 } else { 1 });
        }
        "explore" => {
            let chosen = chosen(&arg(1));
            let first: u64 = arg(2).parse().unwrap_or(1);
            let t = Instant::now();
            let runs = match (env_u64("SIM_RUNS"), deadline(t)) {
                (Some(r), _) => r,
                (None, Some(_)) => u64::MAX,
                (None, None) => DEFAULT_EXPLORE_RUNS,
            };
            let budget = Budget { deadline: deadline(t), threads: threads(), ..Budget::runs(runs) };
            let ex = explore(&chosen, first, &budget, report_failure(&chosen));
            let r = &ex.report;
            println!(
                "explored {} runs ({} branched) in {:?}: {} features, {} probes, corpus {}, {} failed",
                r.runs,
                r.branched,
                t.elapsed(),
                r.features.len(),
                r.probes().len(),
                ex.corpus,
                r.failures.len()
            );
            print_distinct(r);
            if std::env::var("SIM_COMPARE").is_ok() {
                let plain = Budget { fresh: 1.0, threads: threads(), ..Budget::runs(r.runs) };
                let base = explore(&chosen, first, &plain, |_, _| {}).report;
                println!(
                    "a campaign of {} runs: {} features, {} probes, {} failed",
                    base.runs,
                    base.features.len(),
                    base.probes().len(),
                    base.failures.len()
                );
                let only: Vec<String> =
                    r.features.keys().filter(|f| !base.features.contains_key(f)).map(|f| f.to_string()).collect();
                println!("features only exploration reached: {}", only.join(" "));
                let n = |r: &Report, p: Probe| r.runs_hitting.get(p as usize).copied().unwrap_or(0);
                let rare: Vec<String> = Probe::ALL
                    .iter()
                    .filter(|p| base.share(**p) < RARE)
                    .map(|p| format!("{} {}/{}", p.name(), n(r, *p), n(&base, *p)))
                    .collect();
                println!("runs reaching rare probes (exploration/campaign): {}", rare.join(", "));
            }
            std::process::exit(if r.failures.is_empty() { 0 } else { 1 });
        }
        "trace" => {
            let seed = arg(2).parse().unwrap_or(1);
            let o = run_one(&arg(1), seed, &arg(3), true);
            for l in &o.trace_tail {
                println!("{l}");
            }
            let knobs: Vec<String> = o.knobs.iter().map(|(k, v)| format!("{k}={v}")).collect();
            println!("knobs: {}", knobs.join(" "));
            let fired: Vec<String> =
                o.buggify.sites.iter().filter(|(_, s)| s.fired > 0).map(|(l, s)| format!("{l}x{}", s.fired)).collect();
            println!("buggify sites fired: {}", fired.join(" "));
            println!("{:?}", o.result);
        }
        "determinism" => {
            let seed = arg(2).parse().unwrap_or(1);
            let a = run_one(&arg(1), seed, &arg(3), true).trace_tail;
            let b = run_one(&arg(1), seed, &arg(3), true).trace_tail;
            match a.iter().zip(&b).position(|(x, y)| x != y) {
                None if a.len() == b.len() => println!("deterministic ({} events)", a.len()),
                None => println!("traces differ in length: {} vs {}", a.len(), b.len()),
                Some(i) => {
                    for l in &a[i.saturating_sub(5)..i] {
                        println!("  {l}");
                    }
                    println!("A {}\nB {}", a[i], b[i]);
                }
            }
        }
        _ => eprintln!(
            "usage: sim_tool list | campaign <scenario|all> <seeds> [first] | explore <scenario|all> [first] | trace <scenario> <seed> [branches] | determinism <scenario> <seed> [branches]"
        ),
    }
}
