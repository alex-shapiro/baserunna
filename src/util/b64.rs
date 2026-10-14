//! Base64 and hex encoding.

const STD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn encode_with(data: &[u8], alpha: &[u8; 64], pad: bool) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(alpha[(n >> 18) as usize & 63] as char);
        out.push(alpha[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(alpha[(n >> 6) as usize & 63] as char);
        } else if pad {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(alpha[n as usize & 63] as char);
        } else if pad {
            out.push('=');
        }
    }
    out
}

fn val(c: u8, url: bool) -> Option<u32> {
    Some(match c {
        b'A'..=b'Z' => c - b'A',
        b'a'..=b'z' => c - b'a' + 26,
        b'0'..=b'9' => c - b'0' + 52,
        b'+' if !url => 62,
        b'/' if !url => 63,
        b'-' if url => 62,
        b'_' if url => 63,
        _ => return None,
    } as u32)
}

fn decode_with(s: &str, url: bool) -> Option<Vec<u8>> {
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
    for chunk in trimmed.chunks(4) {
        let mut n = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            n |= val(c, url)? << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        } else if n & 0xFFFF != 0 {
            return None; // non-canonical trailing bits
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        } else if chunk.len() == 3 && n & 0xFF != 0 {
            return None;
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
    use crate::util::rng::Rng;

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
}
