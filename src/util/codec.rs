//! Binary encoding and decoding primitives.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError(pub &'static str);

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "decode error: {}", self.0)
    }
}

impl std::error::Error for DecodeError {}

pub type DResult<T> = Result<T, DecodeError>;

#[derive(Default, Debug, Clone)]
pub struct Writer {
    pub buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Writer {
        Writer { buf: Vec::new() }
    }

    pub fn with_capacity(n: usize) -> Writer {
        Writer { buf: Vec::with_capacity(n) }
    }

    pub fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    pub fn u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn u128(&mut self, v: u128) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }

    pub fn bytes(&mut self, b: &[u8]) {
        self.varint(b.len() as u64);
        self.buf.extend_from_slice(b);
    }

    pub fn str(&mut self, s: &str) {
        self.bytes(s.as_bytes());
    }

    pub fn raw(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }

    pub fn bool(&mut self, b: bool) {
        self.u8(b as u8);
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }
}

#[derive(Debug, Clone)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Reader<'a> {
        Reader { buf, pos: 0 }
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    pub fn take(&mut self, n: usize) -> DResult<&'a [u8]> {
        if n > self.remaining() {
            return Err(DecodeError("truncated"));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    pub fn u8(&mut self) -> DResult<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn bool(&mut self) -> DResult<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(DecodeError("bad bool")),
        }
    }

    pub fn u16(&mut self) -> DResult<u16> {
        let b = self.take(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    pub fn u32(&mut self) -> DResult<u32> {
        let b = self.take(4)?;
        let mut a = [0u8; 4];
        a.copy_from_slice(b);
        Ok(u32::from_le_bytes(a))
    }

    pub fn u64(&mut self) -> DResult<u64> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_le_bytes(a))
    }

    pub fn u128(&mut self) -> DResult<u128> {
        let b = self.take(16)?;
        let mut a = [0u8; 16];
        a.copy_from_slice(b);
        Ok(u128::from_le_bytes(a))
    }

    pub fn varint(&mut self) -> DResult<u64> {
        let mut v: u64 = 0;
        let mut shift = 0u32;
        loop {
            let b = self.u8()?;
            if shift == 63 && b > 1 {
                return Err(DecodeError("varint overflow"));
            }
            v |= ((b & 0x7F) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
            shift += 7;
            if shift > 63 {
                return Err(DecodeError("varint too long"));
            }
        }
    }

    /// Read a varint length no greater than `max`.
    pub fn len(&mut self, max: usize) -> DResult<usize> {
        let v = self.varint()?;
        if v > max as u64 {
            return Err(DecodeError("length too large"));
        }
        Ok(v as usize)
    }

    pub fn bytes(&mut self) -> DResult<&'a [u8]> {
        let n = self.len(self.remaining())?;
        self.take(n)
    }

    pub fn str(&mut self) -> DResult<&'a str> {
        std::str::from_utf8(self.bytes()?).map_err(|_| DecodeError("bad utf8"))
    }

    pub fn string(&mut self) -> DResult<String> {
        Ok(self.str()?.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::rng::Rng;

    #[test]
    fn varint_roundtrip() {
        let mut r = Rng::new(1);
        let mut vals = vec![0u64, 1, 127, 128, 255, 16383, 16384, u64::MAX, u64::MAX - 1];
        for _ in 0..2000 {
            let bits = r.below(64) as u32;
            vals.push(r.next_u64() >> bits);
        }
        let mut w = Writer::new();
        for v in &vals {
            w.varint(*v);
        }
        let buf = w.into_inner();
        let mut rd = Reader::new(&buf);
        for v in &vals {
            assert_eq!(rd.varint().unwrap(), *v);
        }
        assert!(rd.is_empty());
    }

    #[test]
    fn varint_rejects_overflow() {
        let bad = [0xFFu8; 11];
        assert!(Reader::new(&bad).varint().is_err());
        let bad2 = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02];
        assert!(Reader::new(&bad2).varint().is_err());
    }

    #[test]
    fn reader_never_panics_on_garbage() {
        let mut r = Rng::new(3);
        for _ in 0..5000 {
            let n = r.usize(20);
            let data = r.bytes(n);
            let mut rd = Reader::new(&data);
            for _ in 0..8 {
                let _ = match r.below(6) {
                    0 => rd.varint().map(|_| ()),
                    1 => rd.bytes().map(|_| ()),
                    2 => rd.u64().map(|_| ()),
                    3 => rd.str().map(|_| ()),
                    4 => rd.bool().map(|_| ()),
                    _ => rd.u128().map(|_| ()),
                };
            }
        }
    }
}
