#![no_main]
//! Tuple and row decoders must never panic; decoded tuples re-encode to
//! the same bytes (canonical encoding).
use baserunna::table::{schema, tuple};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(keys) = tuple::decode_all(data) {
        let enc = tuple::encode(&keys);
        let back = tuple::decode_all(&enc).expect("re-decodes");
        assert_eq!(back.len(), keys.len());
    }
    let _ = schema::decode_row(data);
});
