//! Base64 and hex encoding.

const STD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn encode_with(data: &[u8], alpha: &[u8; 64], pad: bool) -> String {
    let mut out = Vec::with_capacity(data.len().div_ceil(3) * 4);
    let sym = |n: u32, shift: u32| alpha[(n >> shift) as usize & 63];
    let (chunks, rest) = data.as_chunks::<3>();
    for c in chunks {
        let n = (u32::from(c[0]) << 16) | (u32::from(c[1]) << 8) | u32::from(c[2]);
        out.extend_from_slice(&[sym(n, 18), sym(n, 12), sym(n, 6), sym(n, 0)]);
    }
    match *rest {
        [a] => {
            let n = u32::from(a) << 16;
            out.extend_from_slice(&[sym(n, 18), sym(n, 12)]);
            if pad {
                out.extend_from_slice(b"==");
            }
        }
        [a, b] => {
            let n = (u32::from(a) << 16) | (u32::from(b) << 8);
            out.extend_from_slice(&[sym(n, 18), sym(n, 12), sym(n, 6)]);
            if pad {
                out.push(b'=');
            }
        }
        _ => {}
    }
    String::from_utf8(out).expect("base64 is ASCII")
}

/// Maps each byte to its 6-bit value in an alphabet, or to `INVALID`.
const fn table(alpha: &[u8; 64]) -> [u8; 256] {
    let mut t = [INVALID; 256];
    let mut i = 0;
    while i < 64 {
        t[alpha[i] as usize] = i as u8;
        i += 1;
    }
    t
}

const INVALID: u8 = 0xff;
const STD_TABLE: [u8; 256] = table(STD);
const URL_TABLE: [u8; 256] = table(URL);

fn decode_with(s: &str, url: bool) -> Option<Vec<u8>> {
    let table = if url { &URL_TABLE } else { &STD_TABLE };
    let bytes = s.as_bytes();
    let trimmed = {
        let mut end = bytes.len();
        let mut pads = 0;
        while end > 0 && bytes[end - 1] == b'=' && pads < 2 {
            end -= 1;
            pads += 1;
        }
        if pads > 0 && !bytes.len().is_multiple_of(4) {
            return None;
        }
        &bytes[..end]
    };
    if trimmed.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(trimmed.len() * 3 / 4);
    let (chunks, rest) = trimmed.as_chunks::<4>();
    for c in chunks {
        let (a, b, c, d) = (table[c[0] as usize], table[c[1] as usize], table[c[2] as usize], table[c[3] as usize]);
        // Values are below 64, so the union is `INVALID` only if a byte is invalid.
        if (a | b | c | d) == INVALID {
            return None;
        }
        let n = (u32::from(a) << 18) | (u32::from(b) << 12) | (u32::from(c) << 6) | u32::from(d);
        out.extend_from_slice(&[(n >> 16) as u8, (n >> 8) as u8, n as u8]);
    }
    if !rest.is_empty() {
        let mut n = 0u32;
        for (i, &c) in rest.iter().enumerate() {
            let v = table[c as usize];
            if v == INVALID {
                return None;
            }
            n |= u32::from(v) << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        // Non-canonical encodings set bits after the last byte.
        if rest.len() == 2 && n & 0xFFFF != 0 {
            return None;
        }
        if rest.len() == 3 {
            out.push((n >> 8) as u8);
            if n & 0xFF != 0 {
                return None;
            }
        }
    }
    Some(out)
}

/// Standard alphabet with padding.
pub fn encode(data: &[u8]) -> String {
    encode_with(data, STD, true)
}

/// Standard alphabet; padding optional.
pub fn decode(s: &str) -> Option<Vec<u8>> {
    decode_with(s, false)
}

/// URL-safe alphabet without padding.
pub fn encode_url(data: &[u8]) -> String {
    encode_with(data, URL, false)
}

/// URL-safe alphabet; padding optional.
pub fn decode_url(s: &str) -> Option<Vec<u8>> {
    decode_with(s, true)
}

pub fn hex(data: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(data.len() * 2);
    for b in data {
        s.push(H[(b >> 4) as usize] as char);
        s.push(H[(b & 15) as usize] as char);
    }
    s
}

pub fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let d = |c: u8| -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    };
    s.as_bytes().chunks(2).map(|p| Some((d(p[0])? << 4) | d(p[1])?)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SplitMix64 generator for property tests.
    struct Rng(u64);

    impl Rng {
        fn new(seed: u64) -> Rng {
            Rng(seed)
        }

        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }

        fn usize(&mut self, n: usize) -> usize {
            (self.next_u64() % n as u64) as usize
        }

        fn range(&mut self, lo: u64, hi: u64) -> u64 {
            lo + self.next_u64() % (hi - lo)
        }

        fn bytes(&mut self, n: usize) -> Vec<u8> {
            (0..n).map(|_| self.next_u64() as u8).collect()
        }
    }

    #[test]
    fn rfc4648_vectors() {
        let v = [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ];
        for (plain, enc) in v {
            assert_eq!(encode(plain.as_bytes()), enc);
            assert_eq!(decode(enc).unwrap(), plain.as_bytes());
            assert_eq!(decode(enc.trim_end_matches('=')).unwrap(), plain.as_bytes());
        }
    }

    #[test]
    fn roundtrip_random() {
        let mut r = Rng::new(5);
        for _ in 0..2000 {
            let n = r.usize(64);
            let d = r.bytes(n);
            assert_eq!(decode(&encode(&d)).unwrap(), d);
            assert_eq!(decode_url(&encode_url(&d)).unwrap(), d);
            assert_eq!(unhex(&hex(&d)).unwrap(), d);
        }
    }

    #[test]
    fn rejects_garbage() {
        assert!(decode("Zg=").is_none());
        assert!(decode("Z").is_none());
        assert!(decode("Zh==").is_none());
        assert!(decode("ab$d").is_none());
        assert!(decode_url("ab+d").is_none());
        assert!(unhex("abc").is_none());
        assert!(unhex("zz").is_none());
        let mut r = Rng::new(11);
        for _ in 0..5000 {
            let n = r.usize(12);
            let s: String = (0..n).map(|_| (r.range(32, 127) as u8) as char).collect();
            let _ = decode(&s);
            let _ = decode_url(&s);
        }
    }

    #[test]
    fn decoding_accepts_only_canonical_encodings() {
        let mut r = Rng::new(3);
        let alphabet = b"AQgw/+9=";
        for _ in 0..20_000 {
            let n = 4 * r.usize(4) + usize::from(r.usize(8) == 0);
            let s: String = (0..n).map(|_| alphabet[r.usize(alphabet.len())] as char).collect();
            if let Some(d) = decode(&s) {
                assert_eq!(encode(&d).trim_end_matches('='), s.trim_end_matches('='), "{s:?} is not canonical");
            }
        }
    }
}
