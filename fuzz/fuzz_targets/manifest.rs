#![no_main]
//! Manifest and levels object decoding must never panic; valid inputs must
//! round-trip.
use baserunna::engine::manifest::{Levels, Manifest};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(m) = Manifest::decode(&bytes::Bytes::copy_from_slice(data)) {
        let again = Manifest::decode(&m.encode()).expect("re-encoded manifest decodes");
        assert_eq!(again, m);
    }
    if let Ok(l) = Levels::decode(data) {
        assert_eq!(Levels::decode(&l.encode()).expect("re-encoded levels object decodes"), l);
    }
});
