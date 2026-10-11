//! Deterministic simulation tests (SPEC §13). Each scenario runs a few seeds
//! by default; scale up with `SIM_SEEDS=500 cargo test --release --test sim`,
//! reproduce with `SIM_SEED=<seed> cargo test --test sim <name>`, and debug
//! with `cargo run --release --example sim_tool -- trace <scenario> <seed>`.

use baserunna::sim::workloads::{chaos_mix, scenario};
use baserunna::sim::{SimConfig, run, run_seeds};

fn run_scenario(name: &str, base: u64, seeds: u64) {
    let s = scenario(name).expect("scenario exists");
    run_seeds(name, base, seeds, s.config, s.run);
}

#[test]
fn sim_kv_model_single_node() {
    run_scenario("kv_single_node", 1, 2);
}

#[test]
fn sim_kv_model_faulty_cluster() {
    run_scenario("kv_faulty_cluster", 2, 2);
}

#[test]
fn sim_bank_serializability() {
    run_scenario("bank", 3, 2);
}

#[test]
fn sim_register_linearizability() {
    run_scenario("register", 4, 2);
}

#[test]
fn sim_tables_and_indexes() {
    run_scenario("tables", 5, 2);
}

#[test]
fn sim_blobs_and_gc() {
    run_scenario("blobs", 6, 2);
}

#[test]
fn sim_auth() {
    run_scenario("auth", 7, 2);
}

#[test]
fn sim_lifecycle() {
    run_scenario("lifecycle", 8, 2);
}

#[test]
fn sim_console() {
    run_scenario("console", 10, 2);
}

#[test]
fn sim_chaos_mix() {
    run_scenario("chaos", 9, 1);
}

#[test]
fn sim_is_deterministic() {
    let seed = baserunna::util::prop::env_u64("SIM_SEED").unwrap_or(4242);
    let a = run(seed, SimConfig::faulty(3), |sim| chaos_mix(sim, 1));
    let b = run(seed, SimConfig::faulty(3), |sim| chaos_mix(sim, 1));
    assert!(a.result.is_ok(), "{:?}", a.result);
    assert_eq!((a.trace_hash, a.events), (b.trace_hash, b.events), "same seed, different trace");
    let c = run(seed + 1, SimConfig::faulty(3), |sim| chaos_mix(sim, 1));
    assert_ne!(a.trace_hash, c.trace_hash, "different seeds should diverge");
}

#[test]
fn sim_api_tour() {
    run_scenario("api_tour", 11, 2);
}
