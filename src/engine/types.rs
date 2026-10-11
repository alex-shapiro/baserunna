//! Core engine types: entries, values and blob references.

use crate::util::codec::{DResult, DecodeError, Reader, Writer};
use bytes::Bytes;

/// A database version / commit sequence number.
pub type Seq = u64;

pub const MAX_KEY_LEN: usize = 4096 + 64; // user keys + internal prefixes

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobRef {
    /// 32 lowercase hex chars: 48-bit creation ms + 80 random bits.
    pub id: String,
    pub size: u64,
    pub sha256: [u8; 32],
    pub content_type: String,
}

impl BlobRef {
    pub fn encode(&self, w: &mut Writer) {
        w.str(&self.id);
        w.varint(self.size);
        w.raw(&self.sha256);
        w.str(&self.content_type);
    }

    pub fn decode(r: &mut Reader<'_>) -> DResult<BlobRef> {
        let id = r.string()?;
        if !valid_blob_id(&id) {
            return Err(DecodeError("bad blob id"));
        }
        let size = r.varint()?;
        let mut sha256 = [0u8; 32];
        sha256.copy_from_slice(r.take(32)?);
        let content_type = r.string()?;
        Ok(BlobRef { id, size, sha256, content_type })
    }

    /// Creation time encoded in the id.
    pub fn created_ms(&self) -> u64 {
        blob_id_time(&self.id).unwrap_or(0)
    }
}

pub fn valid_blob_id(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub fn blob_id_time(id: &str) -> Option<u64> {
    if !valid_blob_id(id) {
        return None;
    }
    u64::from_str_radix(&id[..12], 16).ok()
}

pub fn make_blob_id(now_ms: u64, random: &[u8]) -> String {
    let mut s = format!("{:012x}", now_ms & 0xFFFF_FFFF_FFFF);
    s.push_str(&crate::util::b64::hex(&random[..10]));
    s
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Inline(Bytes),
    Blob(BlobRef),
    Tombstone,
}

impl Value {
    pub fn is_tombstone(&self) -> bool {
        matches!(self, Value::Tombstone)
    }

    /// Approximate encoded size.
    pub fn size(&self) -> usize {
        match self {
            Value::Inline(b) => b.len() + 4,
            Value::Blob(r) => r.id.len() + r.content_type.len() + 48,
            Value::Tombstone => 1,
        }
    }

    pub fn encode(&self, w: &mut Writer) {
        match self {
            Value::Inline(b) => {
                w.u8(0);
                w.bytes(b);
            }
            Value::Blob(r) => {
                w.u8(1);
                r.encode(w);
            }
            Value::Tombstone => w.u8(2),
        }
    }

    /// `src` must be the buffer the reader was created over; inline values
    /// then share it without copying.
    pub fn decode(r: &mut Reader<'_>, src: &Bytes) -> DResult<Value> {
        match r.u8()? {
            0 => {
                let b = r.bytes()?;
                Ok(Value::Inline(share(src, r.pos(), b)))
            }
            1 => Ok(Value::Blob(BlobRef::decode(r)?)),
            2 => Ok(Value::Tombstone),
            _ => Err(DecodeError("bad value kind")),
        }
    }
}

/// `b` was just read from a reader over `src` that is now at `end`; return
/// it as a zero-copy slice of `src` when possible.
fn share(src: &Bytes, end: usize, b: &[u8]) -> Bytes {
    let off = end.wrapping_sub(b.len());
    if end <= src.len() && off <= end && std::ptr::eq(src[off..end].as_ptr(), b.as_ptr()) {
        src.slice(off..end)
    } else {
        Bytes::copy_from_slice(b)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub key: Bytes,
    pub seq: Seq,
    pub value: Value,
}

impl Entry {
    pub fn size(&self) -> usize {
        self.key.len() + self.value.size() + 10
    }

    pub fn encode(&self, w: &mut Writer) {
        w.bytes(&self.key);
        w.varint(self.seq);
        self.value.encode(w);
    }

    pub fn decode(r: &mut Reader<'_>, src: &Bytes) -> DResult<Entry> {
        let k = r.bytes()?;
        if k.len() > MAX_KEY_LEN {
            return Err(DecodeError("key too long"));
        }
        let key = share(src, r.pos(), k);
        let seq = r.varint()?;
        let value = Value::decode(r, src)?;
        Ok(Entry { key, seq, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_ids() {
        let id = make_blob_id(1_700_000_000_123, &[7u8; 10]);
        assert!(valid_blob_id(&id));
        assert_eq!(blob_id_time(&id), Some(1_700_000_000_123));
        assert!(!valid_blob_id("xyz"));
    }

    #[test]
    fn entry_roundtrip() {
        let es = vec![
            Entry { key: Bytes::from_static(b"a"), seq: 5, value: Value::Inline(Bytes::from_static(b"hello")) },
            Entry { key: Bytes::from_static(b"b"), seq: 1 << 40, value: Value::Tombstone },
            Entry {
                key: Bytes::from_static(b"c"),
                seq: 0,
                value: Value::Blob(BlobRef {
                    id: make_blob_id(1, &[1; 10]),
                    size: 10,
                    sha256: [3; 32],
                    content_type: "image/png".into(),
                }),
            },
        ];
        let mut w = Writer::new();
        for e in &es {
            e.encode(&mut w);
        }
        let src = Bytes::from(w.into_inner());
        let mut r = Reader::new(&src);
        for e in &es {
            assert_eq!(&Entry::decode(&mut r, &src).unwrap(), e);
        }
        assert!(r.is_empty());
    }
}
