//! Verifies EDN list-append histories against strict serializability using the `elle` simulation
//! checker.
//!
//! Run: `cargo run --release --example elle_check -- runs/r1/history.edn [...]`

fn main() {
    let mut failed = false;
    for path in std::env::args().skip(1) {
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        match baserunna::sim::workloads::elle::check_edn(&text) {
            Ok(event_count) => println!("{path}: valid ({event_count} events)"),
            Err(err) => {
                failed = true;
                println!("{path}: INVALID: {err}");
            }
        }
    }
    std::process::exit(i32::from(failed));
}
