#![no_main]
//! The JSON op language and query/filter parsers must never panic.
use baserunna::ops::Op;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<Vec<Op>>(data);
    let _ = serde_json::from_slice::<baserunna::table::Query>(data);
    let _ = serde_json::from_slice::<baserunna::table::CreateTable>(data);
    let _ = serde_json::from_slice::<baserunna::meta::MetaCmd>(data);
});
