#![no_main]
//! Decoding a change feed cursor must never panic, and decoded cursors
//! must survive a round trip.
use baserunna::engine::ChangePos;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data)
        && let Some(p) = ChangePos::decode(s)
    {
        assert_eq!(ChangePos::decode(&p.encode()), Some(p));
    }
});
