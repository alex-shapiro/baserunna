//! `multipart/form-data` (RFC 7578) parser and encoder for console file uploads.

use bytes::Bytes;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Part {
    pub name: String,
    pub filename: Option<String>,
    pub content_type: Option<String>,
    pub data: Bytes,
}

const MAX_PARTS: usize = 64;
const MAX_HEADERS: usize = 8 * 1024;

/// Extracts the boundary parameter from a `multipart/form-data` `Content-Type` header.
pub fn boundary(content_type: &str) -> Option<String> {
    let mut it = content_type.split(';');
    if !it.next()?.trim().eq_ignore_ascii_case("multipart/form-data") {
        return None;
    }
    let (_, v) = it.filter_map(|p| p.split_once('=')).find(|(k, _)| k.trim().eq_ignore_ascii_case("boundary"))?;
    let v = v.trim();
    let v = v.strip_prefix('"').and_then(|v| v.strip_suffix('"')).unwrap_or(v);
    let ok = (1..=70).contains(&v.len()) && v.bytes().all(|b| b.is_ascii_graphic() || b == b' ');
    ok.then(|| v.to_string())
}

fn find(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if from > hay.len() || needle.is_empty() {
        return None;
    }
    hay[from..].windows(needle.len()).position(|w| w == needle).map(|i| i + from)
}

/// Parses a `multipart/form-data` body, returning parts whose `data` slices into `body`.
pub fn parse(content_type: &str, body: &Bytes) -> Result<Vec<Part>, String> {
    let b = boundary(content_type).ok_or("not multipart/form-data with a boundary")?;
    let delim = format!("--{b}").into_bytes();
    let mut close = b"\r\n".to_vec();
    close.extend_from_slice(&delim);
    // A preamble may precede the first delimiter.
    let mut at =
        if body.starts_with(&delim) { delim.len() } else { find(body, &close, 0).ok_or("no boundary")? + close.len() };
    let mut parts = Vec::new();
    loop {
        let rest = &body[at..];
        if rest.starts_with(b"--") {
            return Ok(parts);
        }
        if !rest.starts_with(b"\r\n") {
            return Err("malformed boundary line".into());
        }
        if parts.len() == MAX_PARTS {
            return Err("too many parts".into());
        }
        let start = at + 2;
        let head_end = find(body, b"\r\n\r\n", start).ok_or("unterminated part headers")?;
        if head_end - start > MAX_HEADERS {
            return Err("part headers too long".into());
        }
        let head = std::str::from_utf8(&body[start..head_end]).map_err(|_| "part headers are not UTF-8")?;
        let data_start = head_end + 4;
        let data_end = find(body, &close, data_start).ok_or("unterminated part")?;
        parts.push(part(head, body.slice(data_start..data_end))?);
        at = data_end + close.len();
    }
}

fn part(head: &str, data: Bytes) -> Result<Part, String> {
    let (mut name, mut filename, mut content_type) = (None, None, None);
    for line in head.split("\r\n") {
        let (k, v) = line.split_once(':').ok_or("malformed part header")?;
        match k.trim().to_ascii_lowercase().as_str() {
            "content-disposition" => {
                let (kind, params) = v.split_once(';').unwrap_or((v, ""));
                if !kind.trim().eq_ignore_ascii_case("form-data") {
                    return Err("parts must be form-data".into());
                }
                for (k, v) in params_of(params)? {
                    match k.as_str() {
                        "name" => name = Some(v),
                        "filename" => filename = Some(v),
                        _ => {}
                    }
                }
            }
            "content-type" => content_type = Some(v.trim().to_string()),
            _ => {}
        }
    }
    Ok(Part { name: name.ok_or("a part has no name")?, filename, content_type, data })
}

/// Parses semicolon-separated `k=v` or `k="v"` header parameters.
fn params_of(s: &str) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    let mut c = s.chars().peekable();
    loop {
        while c.peek().is_some_and(|ch| *ch == ';' || ch.is_whitespace()) {
            c.next();
        }
        if c.peek().is_none() {
            return Ok(out);
        }
        let mut key = String::new();
        loop {
            match c.next().ok_or("a parameter has no value")? {
                '=' => break,
                ';' => return Err("a parameter has no value".into()),
                ch => key.push(ch),
            }
        }
        let mut val = String::new();
        if c.peek() == Some(&'"') {
            c.next();
            loop {
                match c.next().ok_or("unterminated quoted string")? {
                    '"' => break,
                    '\\' => val.push(c.next().ok_or("unterminated quoted string")?),
                    ch => val.push(ch),
                }
            }
        } else {
            while let Some(ch) = c.peek().copied().filter(|ch| *ch != ';') {
                val.push(ch);
                c.next();
            }
            val = val.trim().to_string();
        }
        out.push((key.trim().to_ascii_lowercase(), val));
    }
}

/// Encodes `parts` as a `multipart/form-data` body using `boundary`.
pub fn encode(boundary: &str, parts: &[Part]) -> Vec<u8> {
    let q = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
    let mut out = Vec::new();
    for p in parts {
        out.extend_from_slice(
            format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{}\"", q(&p.name)).as_bytes(),
        );
        if let Some(f) = &p.filename {
            out.extend_from_slice(format!("; filename=\"{}\"", q(f)).as_bytes());
        }
        if let Some(ct) = &p.content_type {
            out.extend_from_slice(format!("\r\nContent-Type: {ct}").as_bytes());
        }
        out.extend_from_slice(b"\r\n\r\n");
        out.extend_from_slice(&p.data);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::prop;
    use crate::util::rng::Rng;

    const CT: &str = "multipart/form-data; boundary=----b0undary";

    fn parse_bytes(ct: &str, body: &[u8]) -> Result<Vec<Part>, String> {
        parse(ct, &Bytes::copy_from_slice(body))
    }

    #[test]
    fn browser_bodies() {
        let body = b"preamble\r\n------b0undary\r\nContent-Disposition: form-data; name=\"csrf\"\r\n\r\ntok\r\n\
------b0undary\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a \\\"b\\\".bin\"\r\n\
Content-Type: image/png\r\n\r\n\x00\r\n--x\r\n------b0undary--\r\n";
        let p = parse_bytes(CT, body).unwrap();
        assert_eq!(p.len(), 2);
        assert_eq!((p[0].name.as_str(), &p[0].data[..]), ("csrf", &b"tok"[..]));
        assert_eq!(p[1].filename.as_deref(), Some("a \"b\".bin"));
        assert_eq!(p[1].content_type.as_deref(), Some("image/png"));
        assert_eq!(&p[1].data[..], b"\x00\r\n--x");
        assert_eq!(boundary("Multipart/Form-Data; charset=x; boundary=\"q r\""), Some("q r".into()));
        assert_eq!(boundary("multipart/form-data; flag; boundary=b"), Some("b".into()), "valueless params skip");
        for bad in ["text/plain; boundary=x", "multipart/form-data", "multipart/form-data; boundary="] {
            assert_eq!(boundary(bad), None, "{bad}");
        }
        for bad in [
            &b""[..],
            b"------b0undary",
            b"------b0undaryX",
            b"------b0undary\r\nContent-Disposition: form-data\r\n\r\nx\r\n------b0undary--",
            b"------b0undary\r\nContent-Disposition: attachment; name=\"a\"\r\n\r\nx\r\n------b0undary--",
            b"------b0undary\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\nno end",
            b"------b0undary\r\nbroken header\r\n\r\nx\r\n------b0undary--",
            b"------b0undary\r\nContent-Disposition: form-data; name=\"a\r\n\r\nx\r\n------b0undary--",
            b"------b0undary\r\nContent-Disposition: form-data; name\r\n\r\nx\r\n------b0undary--",
            b"------b0undary\r\nContent-Disposition: form-data; name; filename=\"f\"\r\n\r\nx\r\n------b0undary--",
        ] {
            assert!(parse_bytes(CT, bad).is_err(), "{:?}", String::from_utf8_lossy(bad));
        }
    }

    #[test]
    fn parts_share_the_body() {
        let body = Bytes::from(encode(
            "b",
            &[Part { name: "f".into(), filename: None, content_type: None, data: Bytes::from_static(b"data") }],
        ));
        let p = parse("multipart/form-data; boundary=b", &body).unwrap();
        let start = p[0].data.as_ptr() as usize - body.as_ptr() as usize;
        assert!(start < body.len(), "the part is a slice of the body, not a copy");
    }

    fn gen_parts(r: &mut Rng) -> Vec<Part> {
        let text =
            |r: &mut Rng| String::from_utf8_lossy(&prop::small_key(r, 8)).replace('\0', "").replace(['\r', '\n'], "");
        (0..r.usize(5))
            .map(|_| Part {
                name: text(r),
                filename: r.chance(0.5).then(|| text(r)),
                content_type: r.chance(0.5).then(|| "application/x-test".to_string()),
                data: {
                    let mut d = prop::small_key(r, 40);
                    if r.chance(0.3) {
                        d.extend_from_slice(b"\r\n--b0und");
                    }
                    Bytes::from(d)
                },
            })
            .collect()
    }

    #[test]
    fn round_trip_and_robustness() {
        prop::check(
            "multipart round trip",
            500,
            |r| (gen_parts(r), r.next_u64()),
            prop::no_shrink,
            |(parts, seed)| {
                let body = encode("----b0undary", parts);
                let got = parse_bytes(CT, &body)?;
                if got != *parts {
                    return Err(format!("{got:?} != {parts:?}"));
                }
                let mut r = Rng::new(*seed);
                for _ in 0..20 {
                    let mut m = body.clone();
                    for _ in 0..1 + r.usize(4) {
                        match r.below(3) {
                            0 if !m.is_empty() => {
                                let i = r.usize(m.len());
                                m[i] = r.next_u32() as u8;
                            }
                            1 if !m.is_empty() => {
                                m.truncate(r.usize(m.len()));
                            }
                            _ => {
                                let i = r.usize(m.len() + 1);
                                m.insert(i, b"\r\n-\"\\;="[r.usize(7)]);
                            }
                        }
                    }
                    let _ = parse_bytes(CT, &m);
                }
                Ok(())
            },
        );
    }
}
