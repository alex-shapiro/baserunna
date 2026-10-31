pub use baserunna_api::b64;
pub mod bloom;
pub mod codec;
pub mod crc32c;
pub mod json;
pub mod prop;
pub mod rng;
mod ttl;
pub mod zstd;

pub use ttl::TtlCache;

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, DefaultHasher};

/// `HashMap` with a deterministic hasher.
pub type DetHashMap<K, V> = HashMap<K, V, BuildHasherDefault<DefaultHasher>>;

pub enum Either<A, B> {
    Left(A),
    Right(B),
}

/// Polls `a` and `b` until the first future completes, polling `a` before `b` on each wakeup.
pub async fn race<A: Future + Unpin, B: Future + Unpin>(a: &mut A, b: &mut B) -> Either<A::Output, B::Output> {
    use std::task::Poll;
    std::future::poll_fn(|cx| {
        if let Poll::Ready(x) = std::pin::Pin::new(&mut *a).poll(cx) {
            return Poll::Ready(Either::Left(x));
        }
        std::pin::Pin::new(&mut *b).poll(cx).map(Either::Right)
    })
    .await
}

pub fn sha256(data: &[u8]) -> [u8; 32] {
    let d = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, data);
    let mut out = [0u8; 32];
    out.copy_from_slice(d.as_ref());
    out
}

pub struct Sha256(aws_lc_rs::digest::Context);

impl Default for Sha256 {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha256 {
    pub fn new() -> Sha256 {
        Sha256(aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256))
    }
    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }
    pub fn finish(self) -> [u8; 32] {
        let d = self.0.finish();
        let mut out = [0u8; 32];
        out.copy_from_slice(d.as_ref());
        out
    }
}

pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let k = aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, key);
    let tag = aws_lc_rs::hmac::sign(&k, data);
    let mut out = [0u8; 32];
    out.copy_from_slice(tag.as_ref());
    out
}

/// Constant-time byte slice equality.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Smallest byte string strictly greater than all strings with `prefix`, or `None` if `prefix` is all `0xFF`.
pub fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut p = prefix.to_vec();
    while let Some(last) = p.pop() {
        if last != 0xFF {
            p.push(last + 1);
            return Some(p);
        }
    }
    None
}

/// Immediate lexicographical successor of `key`.
pub fn key_successor(key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(key.len() + 1);
    k.extend_from_slice(key);
    k.push(0);
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_end_works() {
        assert_eq!(prefix_end(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_end(b"a\xff"), Some(b"b".to_vec()));
        assert_eq!(prefix_end(b"\xff\xff"), None);
        assert_eq!(prefix_end(b""), None);
    }

    #[test]
    fn sha_vectors() {
        assert_eq!(b64::hex(&sha256(b"abc")), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
        let mut s = Sha256::new();
        s.update(b"a");
        s.update(b"bc");
        assert_eq!(s.finish(), sha256(b"abc"));
    }

    #[test]
    fn ct_eq_works() {
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"ab"));
    }
}
