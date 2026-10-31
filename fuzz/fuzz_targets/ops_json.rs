#![no_main]
//! The JSON op language and query/filter parsers must never panic.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(ops) = serde_json::from_slice::<Vec<baserunna_api::Op>>(data) {
        let _ = baserunna::ops::parse_ops(ops);
    }
    let _ = serde_json::from_slice::<baserunna::table::Query>(data);
    let _ = serde_json::from_slice::<baserunna::table::CreateTable>(data);
    let _ = serde_json::from_slice::<baserunna::meta::MetaCmd>(data);
});
