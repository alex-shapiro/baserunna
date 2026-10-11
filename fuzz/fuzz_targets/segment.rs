#![no_main]
//! Segment and block decoding must never panic.
use baserunna::engine::segment;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let b = bytes::Bytes::copy_from_slice(data);
    let _ = segment::read_all(&b);
    let _ = segment::decode_block(b.clone());
    if let Ok(entries) = segment::read_all(&b) {
        // Anything that decodes must have strictly increasing keys.
        assert!(entries.windows(2).all(|w| w[0].key < w[1].key));
    }
});
