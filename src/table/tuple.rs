//! Order-preserving, self-delimiting tuple encoding for primary keys and
//! index entries. `encode(a) < encode(b)` (bytewise) iff `a < b`
//! element-wise, and the encoding of a tuple is a prefix of the encoding of
//! any tuple that extends it.
//!
//! Type order: null < false < true < int < float < string < bytes.

use crate::util::codec::{DResult, DecodeError};
use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq)]
pub enum Key {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
}

const T_NULL: u8 = 0x01;
const T_FALSE: u8 = 0x02;
const T_TRUE: u8 = 0x03;
const T_INT: u8 = 0x04;
const T_FLOAT: u8 = 0x05;
const T_STR: u8 = 0x06;
const T_BYTES: u8 = 0x07;

impl Key {
    fn rank(&self) -> u8 {
        match self {
            Key::Null => 0,
            Key::Bool(false) => 1,
            Key::Bool(true) => 2,
            Key::Int(_) => 3,
            Key::Float(_) => 4,
            Key::Str(_) => 5,
            Key::Bytes(_) => 6,
        }
    }

    /// Total order matching the encoding.
    pub fn cmp_key(&self, other: &Key) -> Ordering {
        match (self, other) {
            (Key::Int(a), Key::Int(b)) => a.cmp(b),
            (Key::Float(a), Key::Float(b)) => float_bits(*a).cmp(&float_bits(*b)),
            (Key::Str(a), Key::Str(b)) => a.as_bytes().cmp(b.as_bytes()),
            (Key::Bytes(a), Key::Bytes(b)) => a.cmp(b),
            _ => self.rank().cmp(&other.rank()),
        }
    }
}

fn float_bits(f: f64) -> u64 {
    let f = if f == 0.0 { 0.0 } else { f }; // -0.0 == 0.0
    let b = f.to_bits();
    if b >> 63 == 1 { !b } else { b ^ (1 << 63) }
}

fn float_from_bits(b: u64) -> f64 {
    let raw = if b >> 63 == 1 { b ^ (1 << 63) } else { !b };
    f64::from_bits(raw)
}

fn escape(out: &mut Vec<u8>, data: &[u8]) {
    for &b in data {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.push(0x00);
    out.push(0x01);
}

pub fn encode_one(out: &mut Vec<u8>, k: &Key) {
    match k {
        Key::Null => out.push(T_NULL),
        Key::Bool(false) => out.push(T_FALSE),
        Key::Bool(true) => out.push(T_TRUE),
        Key::Int(i) => {
            out.push(T_INT);
            out.extend_from_slice(&((*i as u64) ^ (1 << 63)).to_be_bytes());
        }
        Key::Float(f) => {
            out.push(T_FLOAT);
            out.extend_from_slice(&float_bits(*f).to_be_bytes());
        }
        Key::Str(s) => {
            out.push(T_STR);
            escape(out, s.as_bytes());
        }
        Key::Bytes(b) => {
            out.push(T_BYTES);
            escape(out, b);
        }
    }
}

pub fn encode(keys: &[Key]) -> Vec<u8> {
    let mut out = Vec::new();
    for k in keys {
        encode_one(&mut out, k);
    }
    out
}

fn unescape(data: &[u8], pos: &mut usize) -> DResult<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let b = *data.get(*pos).ok_or(DecodeError("unterminated string"))?;
        *pos += 1;
        if b == 0 {
            let n = *data.get(*pos).ok_or(DecodeError("unterminated string"))?;
            *pos += 1;
            match n {
                0xFF => out.push(0),
                0x01 => return Ok(out),
                _ => return Err(DecodeError("bad escape")),
            }
        } else {
            out.push(b);
        }
    }
}

/// Decode one element starting at `*pos`.
pub fn decode_one(data: &[u8], pos: &mut usize) -> DResult<Key> {
    let t = *data.get(*pos).ok_or(DecodeError("empty tuple element"))?;
    *pos += 1;
    let fixed8 = |pos: &mut usize| -> DResult<u64> {
        let s = data.get(*pos..*pos + 8).ok_or(DecodeError("truncated number"))?;
        *pos += 8;
        let mut a = [0u8; 8];
        a.copy_from_slice(s);
        Ok(u64::from_be_bytes(a))
    };
    Ok(match t {
        T_NULL => Key::Null,
        T_FALSE => Key::Bool(false),
        T_TRUE => Key::Bool(true),
        T_INT => Key::Int((fixed8(pos)? ^ (1 << 63)) as i64),
        T_FLOAT => {
            let f = float_from_bits(fixed8(pos)?);
            if f.is_nan() {
                return Err(DecodeError("NaN in tuple"));
            }
            Key::Float(f)
        }
        T_STR => Key::Str(String::from_utf8(unescape(data, pos)?).map_err(|_| DecodeError("bad utf8"))?),
        T_BYTES => Key::Bytes(unescape(data, pos)?),
        _ => return Err(DecodeError("bad tuple type")),
    })
}

/// Decode exactly `n` elements; returns them and the number of bytes used.
pub fn decode_n(data: &[u8], n: usize) -> DResult<(Vec<Key>, usize)> {
    let mut pos = 0;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(decode_one(data, &mut pos)?);
    }
    Ok((out, pos))
}

/// Decode a whole buffer.
pub fn decode_all(data: &[u8]) -> DResult<Vec<Key>> {
    let mut pos = 0;
    let mut out = Vec::new();
    while pos < data.len() {
        out.push(decode_one(data, &mut pos)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::prop;
    use crate::util::rng::Rng;

    fn gen_key(r: &mut Rng) -> Key {
        match r.below(7) {
            0 => Key::Null,
            1 => Key::Bool(r.chance(0.5)),
            2 => Key::Int(match r.below(4) {
                0 => i64::MIN,
                1 => i64::MAX,
                2 => r.range(0, 10) as i64 - 5,
                _ => r.next_u64() as i64,
            }),
            3 => Key::Float(match r.below(5) {
                0 => -0.0,
                1 => f64::INFINITY,
                2 => f64::NEG_INFINITY,
                3 => r.range(0, 10) as f64 - 5.5,
                _ => match f64::from_bits(r.next_u64()) {
                    f if f.is_nan() => 0.5,
                    f => f.clamp(-1e300, 1e300),
                },
            }),
            4 => Key::Str(String::from_utf8_lossy(&prop::small_key(r, 5)).into_owned()),
            _ => Key::Bytes(prop::small_key(r, 5)),
        }
    }

    fn gen_tuple(r: &mut Rng) -> Vec<Key> {
        (0..r.usize(4)).map(|_| gen_key(r)).collect()
    }

    /// The intended order, independent of the implementation: by type (in
    /// the module's documented order), then numerically or bytewise.
    fn oracle(a: &Key, b: &Key) -> Ordering {
        let rank = |k: &Key| match k {
            Key::Null => 0,
            Key::Bool(false) => 1,
            Key::Bool(true) => 2,
            Key::Int(_) => 3,
            Key::Float(_) => 4,
            Key::Str(_) => 5,
            Key::Bytes(_) => 6,
        };
        match (a, b) {
            (Key::Int(x), Key::Int(y)) => x.cmp(y),
            (Key::Float(x), Key::Float(y)) => x.partial_cmp(y).expect("keys are never NaN"),
            (Key::Str(x), Key::Str(y)) => x.as_bytes().cmp(y.as_bytes()),
            (Key::Bytes(x), Key::Bytes(y)) => x.cmp(y),
            _ => rank(a).cmp(&rank(b)),
        }
    }

    fn cmp_tuples(a: &[Key], b: &[Key]) -> Ordering {
        for (x, y) in a.iter().zip(b) {
            let c = oracle(x, y);
            if c != Ordering::Equal {
                return c;
            }
        }
        a.len().cmp(&b.len())
    }

    #[test]
    fn prop_roundtrip_and_order() {
        prop::check(
            "tuple-order",
            3000,
            |r| (gen_tuple(r), gen_tuple(r)),
            prop::no_shrink,
            |(a, b)| {
                let ea = encode(a);
                let eb = encode(b);
                let da = decode_all(&ea).map_err(|e| e.to_string())?;
                if da != *a {
                    return Err(format!("roundtrip {a:?} -> {da:?}"));
                }
                if ea.cmp(&eb) != cmp_tuples(a, b) {
                    return Err(format!("order mismatch {a:?} vs {b:?}"));
                }
                for (x, y) in a.iter().zip(b) {
                    if x.cmp_key(y) != oracle(x, y) {
                        return Err(format!("cmp_key {x:?} vs {y:?}"));
                    }
                }
                // Prefix property.
                if !a.is_empty() {
                    let p = encode(&a[..a.len() - 1]);
                    if !ea.starts_with(&p) {
                        return Err("prefix property".into());
                    }
                }
                Ok(())
            },
        );
    }

    #[test]
    fn decode_garbage_never_panics() {
        let mut r = Rng::new(3);
        for _ in 0..20000 {
            let n = r.usize(24);
            let d = r.bytes(n);
            let _ = decode_all(&d);
            let _ = decode_n(&d, 3);
        }
    }
}
