#![no_main]
//! The console's multipart/form-data parser must never panic, and whatever
//! it parses must encode and parse back to the same fields.
use baserunna::console::multipart;
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

const CT: &str = "multipart/form-data; boundary=fz";

fuzz_target!(|data: &[u8]| {
    let _ = multipart::boundary(&String::from_utf8_lossy(data));
    if let Ok(parts) = multipart::parse(CT, &Bytes::copy_from_slice(data)) {
        let boundary = "fuzz-boundary-0f3a9c";
        let small = |p: &multipart::Part| p.name.len() + p.filename.as_ref().map_or(0, |f| f.len()) < 2048;
        let clear = |p: &multipart::Part| !p.data.windows(boundary.len()).any(|w| w == boundary.as_bytes());
        if parts.iter().all(|p| small(p) && clear(p)) {
            assert_eq!(
                multipart::parse(
                    &format!("multipart/form-data; boundary={boundary}"),
                    &Bytes::from(multipart::encode(boundary, &parts))
                ),
                Ok(parts)
            );
        }
    }
});
