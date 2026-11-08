//! Checks list-append histories in EDN with the checks of the simulator's `elle` workload: lost,
//! duplicated, and aborted appends, stale reads, and dependency cycles under strict
//! serializability.
//!
//! Run: `cargo run --release --example elle_check -- runs/r1/history.edn [...]`

fn main() {
    let mut failed = false;
    for path in std::env::args().skip(1) {
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        match baserunna::sim::workloads::elle::check_edn(&text) {
            Ok(n) => println!("{path}: valid ({n} events)"),
            Err(e) => {
                failed = true;
                println!("{path}: INVALID: {e}");
            }
        }
    }
    std::process::exit(i32::from(failed));
}
