#![no_main]
//! Manifest decoding must never panic; valid manifests must round-trip.
use baserunna::engine::manifest::Manifest;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(m) = Manifest::decode(&bytes::Bytes::copy_from_slice(data)) {
        let again = Manifest::decode(&m.encode()).expect("re-encoded manifest decodes");
        assert_eq!(again, m);
    }
});
