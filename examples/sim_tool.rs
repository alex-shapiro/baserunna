//! Simulation tool for long campaigns and debugging.
//!
//! ```text
//! cargo run --release --example sim_tool -- list
//! cargo run --release --example sim_tool -- campaign <scenario|all> <seeds> [first_seed]   # SIM_THREADS, SIM_MINUTES, SIM_COVERAGE=1
//! cargo run --release --example sim_tool -- trace <scenario> <seed>      # full trace
//! cargo run --release --example sim_tool -- determinism <scenario> <seed>
//! ```

use baserunna::sim::campaign::{campaign, default_threads};
use baserunna::sim::workloads::{scenario, scenarios};
use baserunna::sim::{SimOutcome, run};

fn run_one(name: &str, seed: u64, keep_log: bool) -> (SimOutcome, Vec<String>) {
    let s = scenario(name).unwrap_or_else(|| panic!("unknown scenario {name}"));
    let mut cfg = s.config_for(seed);
    cfg.keep_log = keep_log;
    cfg.full_trace = keep_log;
    let out = run(seed, cfg, s.run);
    let log = out.trace_tail.clone();
    (out, log)
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
            std::process::exit(if report.failures.is_empty() { 0 } else { 1 });
        }
        "trace" => {
            let seed = arg(2).parse().unwrap_or(1);
            let (o, log) = run_one(&arg(1), seed, true);
            for l in log {
                println!("{l}");
            }
            let knobs: Vec<String> = o.knobs.iter().map(|(k, v)| format!("{k}={v}")).collect();
            println!("knobs: {}", knobs.join(" "));
            println!("{:?}", o.result);
        }
        "determinism" => {
            let seed = arg(2).parse().unwrap_or(1);
            let (_, a) = run_one(&arg(1), seed, true);
            let (_, b) = run_one(&arg(1), seed, true);
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
            "usage: sim_tool list | campaign <scenario|all> <seeds> [first] | trace <scenario> <seed> | determinism <scenario> <seed>"
        ),
    }
}
