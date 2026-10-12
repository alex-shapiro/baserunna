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
        if !valid_object_id(&id) {
            return Err(DecodeError("bad blob id"));
        }
        let size = r.varint()?;
        let mut sha256 = [0u8; 32];
        sha256.copy_from_slice(r.take(32)?);
        let content_type = r.string()?;
        Ok(BlobRef { id, size, sha256, content_type })
    }
}

/// Object ids (segments, blobs) are 32 lowercase hex characters.
pub fn valid_object_id(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub fn blob_id_time(id: &str) -> Option<u64> {
    if !valid_object_id(id) {
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

    /// `src` must be the buffer the reader was created over: inline values
    /// share it without copying.
    pub fn decode(r: &mut Reader<'_>, src: &Bytes) -> DResult<Value> {
        match r.u8()? {
            0 => Ok(Value::Inline(src.slice_ref(r.bytes()?))),
            1 => Ok(Value::Blob(BlobRef::decode(r)?)),
            2 => Ok(Value::Tombstone),
            _ => Err(DecodeError("bad value kind")),
        }
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

    /// `src` must be the buffer the reader was created over (see
    /// [`Value::decode`]).
    pub fn decode(r: &mut Reader<'_>, src: &Bytes) -> DResult<Entry> {
        let k = r.bytes()?;
        if k.len() > MAX_KEY_LEN {
            return Err(DecodeError("key too long"));
        }
        let key = src.slice_ref(k);
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
        assert!(valid_object_id(&id));
        assert_eq!(blob_id_time(&id), Some(1_700_000_000_123));
        assert!(!valid_object_id("xyz"));
        assert!(!valid_object_id(&id[..31]), "short");
        assert!(!valid_object_id(&id.to_uppercase()), "uppercase");
        assert!(!valid_object_id(&format!("{}g", &id[..31])), "not hex");
    }

    /// The size estimate that write limits and the cache account with
    /// covers the encoding, with little slack.
    #[test]
    fn value_size_bounds_the_encoding() {
        let blob = |size, ct: &str| {
            Value::Blob(BlobRef { id: make_blob_id(1, &[1; 10]), size, sha256: [3; 32], content_type: ct.into() })
        };
        for v in [
            Value::Tombstone,
            Value::Inline(Bytes::new()),
            Value::Inline(Bytes::from(vec![1; 100])),
            Value::Inline(Bytes::from(vec![1; 100_000])),
            blob(1, ""),
            blob(1, "image/png"),
            blob(u64::MAX, "application/vnd.openxmlformats-officedocument"),
        ] {
            let mut w = Writer::new();
            v.encode(&mut w);
            let n = w.into_inner().len();
            assert!((n..=n + 16).contains(&v.size()), "{v:?}: size {} for {n} encoded bytes", v.size());
        }
    }

    #[test]
    fn decoding_bounds_key_length() {
        for (len, ok) in [(4096 + 64, true), (4096 + 65, false)] {
            let mut w = Writer::new();
            Entry { key: Bytes::from(vec![b'k'; len]), seq: 1, value: Value::Tombstone }.encode(&mut w);
            let src = Bytes::from(w.into_inner());
            assert_eq!(Entry::decode(&mut Reader::new(&src), &src).is_ok(), ok, "key of {len} bytes");
        }
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
        let within = |b: &Bytes| src.as_ptr_range().contains(&b.as_ptr());
        for e in &es {
            let d = Entry::decode(&mut r, &src).unwrap();
            assert_eq!(&d, e);
            assert!(within(&d.key), "keys are decoded without copying");
            if let Value::Inline(v) = &d.value {
                assert!(within(v), "values are decoded without copying");
            }
        }
        assert!(r.is_empty());
    }
}
