#![no_main]
//! Structure-aware HTTP fuzzing of the transaction-op language against an
//! exact model of the database.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Err(e) = baserunna::sim::txn_fuzz::run_input(data) {
        panic!("{e}");
    }
});
