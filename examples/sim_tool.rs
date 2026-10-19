//! Simulation tool for long campaigns and debugging.
//!
//! ```text
//! cargo run --release --example sim_tool -- list
//! cargo run --release --example sim_tool -- campaign <scenario|all> <seeds> [first_seed]   # SIM_THREADS, SIM_MINUTES, SIM_COVERAGE=1
//! cargo run --release --example sim_tool -- explore <scenario|all> [first_seed]          # SIM_RUNS, SIM_MINUTES, SIM_COMPARE=1
//! cargo run --release --example sim_tool -- trace <scenario> <seed> [branches]      # full trace
//! cargo run --release --example sim_tool -- determinism <scenario> <seed>
//! ```

use baserunna::sim::campaign::{campaign, default_threads};
use baserunna::sim::explore::{Budget, explore, parse_branches};
use baserunna::sim::workloads::{scenario, scenarios};
use baserunna::sim::{SimOutcome, run};

fn run_one(name: &str, seed: u64, branches: &str, keep_log: bool) -> (SimOutcome, Vec<String>) {
    let s = scenario(name).unwrap_or_else(|| panic!("unknown scenario {name}"));
    let mut cfg = s.config_for(seed);
    cfg.branches = parse_branches(branches).unwrap_or_else(|| panic!("bad branch points {branches}"));
    cfg.keep_log = keep_log;
    cfg.full_trace = keep_log;
    let out = run(seed, cfg, s.run);
    let log = out.trace_tail.clone();
    (out, log)
}

/// Groups failures by their message with digits blanked, so that
/// repeats of one failure count once.
fn distinct_failures<'a>(errors: impl Iterator<Item = &'a str>) -> Vec<(usize, String)> {
    let mut by: std::collections::BTreeMap<String, usize> = Default::default();
    for e in errors {
        let first = e.lines().next().unwrap_or("");
        let shape: String = first.chars().map(|c| if c.is_ascii_digit() { '#' } else { c }).collect();
        *by.entry(shape.chars().take(160).collect()).or_default() += 1;
    }
    let mut v: Vec<(usize, String)> = by.into_iter().map(|(k, n)| (n, k)).collect();
    v.sort_by(|a, b| b.0.cmp(&a.0));
    v
}

fn print_distinct(failures: Vec<(usize, String)>) {
    if !failures.is_empty() {
        println!("{} distinct failures:", failures.len());
        for (n, f) in failures {
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
            let all = scenarios();
            let chosen: Vec<_> = all.into_iter().filter(|s| arg(1) == "all" || s.name == arg(1)).collect();
            if chosen.is_empty() {
                panic!("unknown scenario {}", arg(1));
            }
            let n: u64 = arg(2).parse().unwrap_or(100);
            let first: u64 = arg(3).parse().unwrap_or(1);
            let threads =
                std::env::var("SIM_THREADS").ok().and_then(|t| t.parse().ok()).unwrap_or_else(default_threads);
            let seeds: Vec<u64> = (first..first + n).collect();
            let t = std::time::Instant::now();
            // SIM_MINUTES bounds a campaign's wall time instead of its seeds.
            let deadline = std::env::var("SIM_MINUTES")
                .ok()
                .and_then(|m| m.parse::<u64>().ok())
                .map(|m| t + std::time::Duration::from_secs(60 * m));
            let report = campaign(&chosen, &seeds, threads, deadline, |name, out| {
                if let Err(e) = &out.result {
                    println!("FAIL {name} seed={}: {e}", out.seed);
                    println!("  reproduce: cargo run --release --example sim_tool -- trace {name} {}", out.seed);
                }
            });
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
            print_distinct(distinct_failures(report.failures.iter().map(|f| f.error.as_str())));
            // SIM_REQUIRE_PROBES makes a campaign that missed a probe fail.
            let missed = std::env::var("SIM_REQUIRE_PROBES").is_ok_and(|v| !v.is_empty()) && !unreached.is_empty();
            std::process::exit(if report.failures.is_empty() && !missed { 0 } else { 1 });
        }
        "explore" => {
            let all = scenarios();
            let chosen: Vec<_> = all.into_iter().filter(|s| arg(1) == "all" || s.name == arg(1)).collect();
            if chosen.is_empty() {
                panic!("unknown scenario {}", arg(1));
            }
            let first: u64 = arg(2).parse().unwrap_or(1);
            let runs: u64 = std::env::var("SIM_RUNS").ok().and_then(|r| r.parse().ok()).unwrap_or(u64::MAX);
            let t = std::time::Instant::now();
            let mut budget = Budget::runs(runs);
            budget.threads =
                std::env::var("SIM_THREADS").ok().and_then(|t| t.parse().ok()).unwrap_or_else(default_threads);
            budget.deadline = std::env::var("SIM_MINUTES")
                .ok()
                .and_then(|m| m.parse::<u64>().ok())
                .map(|m| t + std::time::Duration::from_secs(60 * m));
            let report = explore(&chosen, first, &budget, |job, out| {
                if let Err(e) = &out.result {
                    println!("FAIL {}: {e}", job.args(&chosen));
                    println!("  reproduce: cargo run --release --example sim_tool -- trace {}", job.args(&chosen));
                }
            });
            println!(
                "explored {} runs ({} branched) in {:?}: {} features, {} probes, corpus {}, {} failed",
                report.runs,
                report.branched,
                t.elapsed(),
                report.features.len(),
                report.probes().len(),
                report.corpus,
                report.failures.len()
            );
            print_distinct(distinct_failures(report.failures.iter().map(|(_, e)| e.as_str())));
            if std::env::var("SIM_COMPARE").is_ok() {
                // The same number of runs as a plain campaign, from the same seeds.
                let mut plain = Budget::runs(report.runs);
                plain.threads = budget.threads;
                plain.fresh = 1.0;
                let base = explore(&chosen, first, &plain, |_, _| {});
                println!(
                    "a campaign of {} runs: {} features, {} probes, {} failed",
                    base.runs,
                    base.features.len(),
                    base.probes().len(),
                    base.failures.len()
                );
                let only: Vec<String> = report
                    .features
                    .keys()
                    .filter(|f| !base.features.contains_key(f))
                    .map(|(p, b)| format!("{}#{b}", p.name()))
                    .collect();
                println!("features only exploration reached: {}", only.join(" "));
                // How often each method put a run into the situations a plain
                // campaign rarely reaches.
                let rare: Vec<String> = baserunna::probe::Probe::ALL
                    .iter()
                    .filter(|p| base.share(**p) < 0.05)
                    .map(|p| {
                        let n =
                            |r: &baserunna::sim::explore::Report| r.runs_hitting.get(*p as usize).copied().unwrap_or(0);
                        format!("{} {}/{}", p.name(), n(&report), n(&base))
                    })
                    .collect();
                println!("runs reaching rare probes (exploration/campaign): {}", rare.join(", "));
            }
            std::process::exit(if report.failures.is_empty() { 0 } else { 1 });
        }
        "trace" => {
            let seed = arg(2).parse().unwrap_or(1);
            let (o, log) = run_one(&arg(1), seed, &arg(3), true);
            for l in log {
                println!("{l}");
            }
            let knobs: Vec<String> = o.knobs.iter().map(|(k, v)| format!("{k}={v}")).collect();
            println!("knobs: {}", knobs.join(" "));
            println!("{:?}", o.result);
        }
        "determinism" => {
            let seed = arg(2).parse().unwrap_or(1);
            let (_, a) = run_one(&arg(1), seed, &arg(3), true);
            let (_, b) = run_one(&arg(1), seed, &arg(3), true);
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
