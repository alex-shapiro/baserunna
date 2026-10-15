#![no_main]
//! Stream record values, chunk references, stream state, commit records,
//! record bodies and filters must decode or parse without panicking, and
//! what decodes must survive a round trip.
use baserunna::engine::stream::{StreamState, Stored};
use baserunna::engine::types::decode_commit_record;
use baserunna::stream::{Body, Filter};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let b = bytes::Bytes::copy_from_slice(data);
    let seq = data.first().copied().unwrap_or(0) as u64;
    let _ = Stored::decode(seq, &b);
    let _ = decode_commit_record(data);
    if let Ok(st) = StreamState::decode(data) {
        assert_eq!(StreamState::decode(&st.encode()), Ok(st));
    }
    if let Ok(body) = Body::decode(&b) {
        assert_eq!(Body::decode(&body.encode()), Ok(body.clone()));
        if let Ok(f) = serde_json::from_slice::<Filter>(data) {
            let _ = f.matches(&body);
        }
    }
    if let Ok(f) = serde_json::from_slice::<Filter>(data) {
        let _ = f.matches(&Body { json: true, headers: vec![("h".into(), "v".into())], data: b });
    }
});
