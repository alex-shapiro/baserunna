#![no_main]
//! Change feed cursors must decode without panicking, and decoded cursors
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
