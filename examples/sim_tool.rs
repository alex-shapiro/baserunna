//! Simulation tool for long campaigns and debugging.
//!
//! ```text
//! cargo run --release --example sim_tool -- list
//! cargo run --release --example sim_tool -- campaign <scenario|all> <seeds> [first_seed]
//! cargo run --release --example sim_tool -- trace <scenario> <seed>      # full trace
//! cargo run --release --example sim_tool -- determinism <scenario> <seed>
//! ```

use baserunna::sim::workloads::{scenario, scenarios};
use baserunna::sim::{SimOutcome, run};

fn run_one(name: &str, seed: u64, keep_log: bool) -> (SimOutcome, Vec<String>) {
    let s = scenario(name).unwrap_or_else(|| panic!("unknown scenario {name}"));
    let mut cfg = (s.config)();
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
            let names: Vec<String> =
                if arg(1) == "all" { scenarios().iter().map(|s| s.name.to_string()).collect() } else { vec![arg(1)] };
            let n: u64 = arg(2).parse().unwrap_or(100);
            let first: u64 = arg(3).parse().unwrap_or(1);
            let mut failures = 0;
            for name in names {
                let t = std::time::Instant::now();
                let mut virt = 0;
                for seed in first..first + n {
                    let (o, _) = run_one(&name, seed, false);
                    virt += o.virtual_ms;
                    if let Err(e) = o.result {
                        failures += 1;
                        println!("FAIL {name} seed={seed}: {e}");
                    }
                }
                println!("{name}: {n} seeds in {:?} wall, {}s simulated", t.elapsed(), virt / 1000);
            }
            std::process::exit(if failures > 0 { 1 } else { 0 });
        }
        "trace" => {
            let seed = arg(2).parse().unwrap_or(1);
            let (o, log) = run_one(&arg(1), seed, true);
            for l in log {
                println!("{l}");
            }
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
