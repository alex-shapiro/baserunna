#![no_main]
//! Decoding a change feed cursor must never panic, and decoded cursors
//! must survive a round trip.
use baserunna::engine::ChangeCursor;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data)
        && let Some(c) = ChangeCursor::decode(s)
    {
        assert_eq!(ChangeCursor::decode(&c.encode()), Some(c));
    }
});
