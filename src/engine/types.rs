//! Core engine types: entries, values, commit records, and blob references.

use super::stream::Appended;
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

/// Key prefix for commit records (`COMMIT_SPACE ++ commit_id_be`).
pub const COMMIT_SPACE: u8 = 0xFF;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitRecord {
    Applied { time_ms: u64, appends: Vec<Appended> },
    Aborted,
}

pub const ABORT_RECORD: Bytes = Bytes::new();

pub fn encode_commit_record(time_ms: u64, appends: &[Appended]) -> Bytes {
    let mut w = Writer::with_capacity(8 + 8 * appends.len());
    w.varint(time_ms);
    for a in appends {
        w.varint(a.first_seq);
        w.varint(a.count);
    }
    Bytes::from(w.into_inner())
}

pub fn decode_commit_record(b: &[u8]) -> DResult<CommitRecord> {
    if b.is_empty() {
        return Ok(CommitRecord::Aborted);
    }
    let mut r = Reader::new(b);
    let time_ms = r.varint()?;
    let mut appends = Vec::new();
    while !r.is_empty() {
        appends.push(Appended { first_seq: r.varint()?, count: r.varint()? });
    }
    Ok(CommitRecord::Applied { time_ms, appends })
}

pub fn commit_key(id: u64) -> Bytes {
    let mut k = Vec::with_capacity(9);
    k.push(COMMIT_SPACE);
    k.extend_from_slice(&id.to_be_bytes());
    Bytes::from(k)
}

pub fn is_commit_key(k: &[u8]) -> bool {
    k.first() == Some(&COMMIT_SPACE)
}

/// Returns true if `id` is 32 lowercase ASCII hex characters.
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

    pub fn heap_bytes(&self) -> usize {
        use super::memory::alloc_bytes;
        match self {
            Value::Inline(b) => alloc_bytes(b.len()),
            Value::Blob(r) => alloc_bytes(r.id.len()) + alloc_bytes(r.content_type.len()),
            Value::Tombstone => 0,
        }
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

    /// Decodes a value from `r`, slicing inline bytes directly from `src`.
    pub fn decode(r: &mut Reader<'_>, src: &Bytes) -> DResult<Value> {
        let kind = r.u8()?;
        Value::decode_kind(r, src, kind)
    }

    fn decode_kind(r: &mut Reader<'_>, src: &Bytes, kind: u8) -> DResult<Value> {
        match kind {
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
    /// Store expiration timestamp in milliseconds.
    pub expires: Option<u64>,
}

/// Bit flag set on the encoded value kind byte if an expiry timestamp follows.
const EXPIRES_FLAG: u8 = 0x10;

impl Entry {
    pub fn new(key: Bytes, seq: Seq, value: Value) -> Entry {
        Entry { key, seq, value, expires: None }
    }

    pub fn tombstone(key: Bytes, seq: Seq) -> Entry {
        Entry::new(key, seq, Value::Tombstone)
    }

    pub fn size(&self) -> usize {
        self.key.len() + self.value.size() + 10 + if self.expires.is_some() { 8 } else { 0 }
    }

    pub fn is_dead(&self, at: u64) -> bool {
        self.value.is_tombstone() || self.expired(at)
    }

    pub fn expired(&self, at: u64) -> bool {
        self.expires.is_some_and(|e| e <= at)
    }

    pub fn at(self, at: u64) -> Entry {
        match self.expired(at) {
            true => Entry::tombstone(self.key, self.seq),
            false => self,
        }
    }

    pub fn encode(&self, w: &mut Writer) {
        w.bytes(&self.key);
        w.varint(self.seq);
        let start = w.len();
        self.value.encode(w);
        if let Some(e) = self.expires {
            w.buf[start] |= EXPIRES_FLAG;
            w.varint(e);
        }
    }

    /// Decodes an entry from `r`, zero-copying slices from `src`.
    pub fn decode(r: &mut Reader<'_>, src: &Bytes) -> DResult<Entry> {
        let k = r.bytes()?;
        if k.len() > MAX_KEY_LEN {
            return Err(DecodeError("key too long"));
        }
        let key = src.slice_ref(k);
        let seq = r.varint()?;
        let kind = r.u8()?;
        let value = Value::decode_kind(r, src, kind & !EXPIRES_FLAG)?;
        let expires = match kind & EXPIRES_FLAG {
            0 => None,
            _ if value.is_tombstone() => return Err(DecodeError("expiring tombstone")),
            _ => Some(r.varint()?),
        };
        Ok(Entry { key, seq, value, expires })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_records_roundtrip() {
        let appends = [Appended { first_seq: 0, count: 3 }, Appended { first_seq: u64::MAX, count: 0 }];
        let applied = |time_ms, appends: &[Appended]| CommitRecord::Applied { time_ms, appends: appends.to_vec() };
        assert_eq!(decode_commit_record(&encode_commit_record(17, &appends)).unwrap(), applied(17, &appends));
        assert_eq!(decode_commit_record(&encode_commit_record(5, &[])).unwrap(), applied(5, &[]));
        assert_eq!(decode_commit_record(&ABORT_RECORD).unwrap(), CommitRecord::Aborted);
        assert!(decode_commit_record(&[0x80]).is_err());
    }

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
            Entry::tombstone(Bytes::from(vec![b'k'; len]), 1).encode(&mut w);
            let src = Bytes::from(w.into_inner());
            assert_eq!(Entry::decode(&mut Reader::new(&src), &src).is_ok(), ok, "key of {len} bytes");
        }
    }

    #[test]
    fn entry_roundtrip() {
        let es = vec![
            Entry::new(Bytes::from_static(b"a"), 5, Value::Inline(Bytes::from_static(b"hello"))),
            Entry::tombstone(Bytes::from_static(b"b"), 1 << 40),
            Entry::new(
                Bytes::from_static(b"c"),
                0,
                Value::Blob(BlobRef {
                    id: make_blob_id(1, &[1; 10]),
                    size: 10,
                    sha256: [3; 32],
                    content_type: "image/png".into(),
                }),
            ),
            Entry {
                expires: Some(1_700_000_000_000),
                ..Entry::new(Bytes::from_static(b"d"), 7, Value::Inline(Bytes::new()))
            },
            Entry {
                expires: Some(0),
                ..Entry::new(
                    Bytes::from_static(b"e"),
                    8,
                    Value::Blob(BlobRef {
                        id: make_blob_id(2, &[2; 10]),
                        size: 1,
                        sha256: [4; 32],
                        content_type: "".into(),
                    }),
                )
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
            if let Value::Inline(v) = &d.value
                && !v.is_empty()
            {
                assert!(within(v), "values are decoded without copying");
            }
        }
        assert!(r.is_empty());
    }

    #[test]
    fn expiry() {
        let e = Entry { expires: Some(100), ..Entry::new(Bytes::from_static(b"k"), 3, Value::Inline(Bytes::new())) };
        assert!(!e.is_dead(99) && e.is_dead(100) && e.is_dead(101));
        assert_eq!(e.clone().at(99), e);
        assert_eq!(e.clone().at(100), Entry::tombstone(Bytes::from_static(b"k"), 3));
        assert!(Entry::tombstone(Bytes::new(), 1).is_dead(0));
        let mut w = Writer::new();
        Entry::tombstone(Bytes::from_static(b"k"), 3).encode(&mut w);
        let mut buf = w.into_inner();
        // The value kind byte follows the key and the seq.
        buf[3] |= EXPIRES_FLAG;
        buf.push(5);
        let src = Bytes::from(buf);
        assert!(Entry::decode(&mut Reader::new(&src), &src).is_err(), "an expiring tombstone is corrupt");
        assert!(e.size() >= Entry::new(e.key.clone(), 3, e.value.clone()).size() + 8, "expiries are accounted");
        assert!(is_commit_key(&commit_key(7)) && !is_commit_key(b"\x01k") && commit_key(1) < commit_key(256));
    }
}
