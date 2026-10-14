#![no_main]
//! Fuzzes database reference parsing and signed email token verification.
//! Parsed references must format back to their input, and XOR-mutated tokens
//! must either fail verification or open with the exact signed user and
//! fingerprint.
use baserunna::auth::{TokenPurpose, open_token, sign_token};
use baserunna::meta::DbRef;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        match DbRef::parse(s) {
            Some(DbRef::Personal(n)) => assert_eq!(DbRef::format(None, &n), s),
            Some(DbRef::Org { org, name }) => assert_eq!(DbRef::format(Some(&org), &name), s),
            Some(DbRef::Id(id)) => assert_eq!(format!("id:{id}"), s),
            None => {}
        }
    }
    let token = sign_token(b"fuzz-key", TokenPurpose::Reset, "user-1", "0123456789abcdef", u64::MAX);
    let mut altered = token.into_bytes();
    let n = altered.len();
    for (i, b) in data.iter().enumerate() {
        altered[i % n] ^= b;
    }
    let altered = String::from_utf8_lossy(&altered);
    if let Some(opened) = open_token(b"fuzz-key", TokenPurpose::Reset, &altered, 0) {
        assert_eq!(opened, ("user-1".to_string(), "0123456789abcdef".to_string()), "{altered:?}");
    }
    assert!(open_token(b"fuzz-key", TokenPurpose::Verify, &altered, 0).is_none(), "a reset token verified email");
});
