//! The per-database manifest: the root of all committed state.
//!
//! It is the only mutable object of a database and is only ever written with
//! a conditional PUT. Small writes are kept inline in `tail`, which makes the
//! manifest the write-ahead log as well.

use super::types::{Entry, Seq};
use crate::util::codec::{DResult, DecodeError, Reader, Writer};
use crate::util::crc32c::crc32c;
use bytes::Bytes;

const MAGIC: &[u8; 4] = b"BRMF";

/// The database was deleted: every read and commit fails with not-found.
pub const FLAG_DELETED: u64 = 1;
const FORMAT: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub node: String,
    pub expires_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentMeta {
    /// 32 hex chars, unique.
    pub id: String,
    pub size: u64,
    pub entries: u64,
    pub tombstones: u64,
    pub min_key: Bytes,
    pub max_key: Bytes,
    pub min_seq: Seq,
    pub max_seq: Seq,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObjKind {
    Segment = 0,
    Blob = 1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Obsolete {
    pub kind: ObjKind,
    pub id: String,
    pub since_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Manifest {
    pub db_id: String,
    pub version: Seq,
    /// Incremented on every manifest write (including lease-only writes);
    /// orders manifests that share a data version.
    pub write_seq: u64,
    pub created_ms: u64,
    /// Bit flags; see [`FLAG_DELETED`].
    pub flags: u64,
    pub lease: Option<Lease>,
    /// Newest first.
    pub segments: Vec<SegmentMeta>,
    /// Sorted by key; newest entry per key.
    pub tail: Vec<Entry>,
    pub tombstone_horizon: Seq,
    /// Oldest first: (commit id, version).
    pub recent_commits: Vec<(u128, Seq)>,
    /// Oldest first: (version, wall ms) samples.
    pub version_times: Vec<(Seq, u64)>,
    pub obsolete: Vec<Obsolete>,
    pub last_sweep_ms: u64,
}

impl Manifest {
    pub fn new(db_id: &str, now_ms: u64) -> Manifest {
        Manifest {
            db_id: db_id.to_string(),
            created_ms: now_ms,
            last_sweep_ms: now_ms,
            version_times: vec![(0, now_ms)],
            ..Default::default()
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.flags & FLAG_DELETED != 0
    }

    pub fn tail_bytes(&self) -> usize {
        self.tail.iter().map(|e| e.size()).sum()
    }

    pub fn encode(&self) -> Bytes {
        let mut w = Writer::with_capacity(256 + self.tail_bytes());
        w.raw(MAGIC);
        w.u16(FORMAT);
        w.str(&self.db_id);
        w.u64(self.version);
        w.u64(self.write_seq);
        w.u64(self.created_ms);
        w.varint(self.flags);
        match &self.lease {
            None => w.u8(0),
            Some(l) => {
                w.u8(1);
                w.str(&l.node);
                w.u64(l.expires_ms);
            }
        }
        w.varint(self.segments.len() as u64);
        for s in &self.segments {
            w.str(&s.id);
            w.varint(s.size);
            w.varint(s.entries);
            w.varint(s.tombstones);
            w.bytes(&s.min_key);
            w.bytes(&s.max_key);
            w.varint(s.min_seq);
            w.varint(s.max_seq);
        }
        w.varint(self.tail.len() as u64);
        for e in &self.tail {
            e.encode(&mut w);
        }
        w.varint(self.tombstone_horizon);
        w.varint(self.recent_commits.len() as u64);
        for (id, v) in &self.recent_commits {
            w.u128(*id);
            w.varint(*v);
        }
        w.varint(self.version_times.len() as u64);
        for (v, t) in &self.version_times {
            w.varint(*v);
            w.varint(*t);
        }
        w.varint(self.obsolete.len() as u64);
        for o in &self.obsolete {
            w.u8(o.kind as u8);
            w.str(&o.id);
            w.varint(o.since_ms);
        }
        w.varint(self.last_sweep_ms);
        let crc = crc32c(&w.buf);
        w.u32(crc);
        Bytes::from(w.into_inner())
    }

    pub fn decode(data: &Bytes) -> DResult<Manifest> {
        if data.len() < 10 {
            return Err(DecodeError("manifest too short"));
        }
        let (body, c) = data.split_at(data.len() - 4);
        if crc32c(body) != u32::from_le_bytes([c[0], c[1], c[2], c[3]]) {
            return Err(DecodeError("manifest checksum mismatch"));
        }
        let src = data.slice(0..body.len());
        let mut r = Reader::new(&src);
        if r.take(4)? != MAGIC {
            return Err(DecodeError("bad manifest magic"));
        }
        if r.u16()? != FORMAT {
            return Err(DecodeError("unsupported manifest format"));
        }
        let db_id = r.string()?;
        let version = r.u64()?;
        let write_seq = r.u64()?;
        let created_ms = r.u64()?;
        let flags = r.varint()?;
        let lease = match r.u8()? {
            0 => None,
            1 => Some(Lease { node: r.string()?, expires_ms: r.u64()? }),
            _ => return Err(DecodeError("bad lease tag")),
        };
        let max = r.remaining();
        let n = r.len(max)?;
        let mut segments = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            let s = SegmentMeta {
                id: r.string()?,
                size: r.varint()?,
                entries: r.varint()?,
                tombstones: r.varint()?,
                min_key: Bytes::copy_from_slice(r.bytes()?),
                max_key: Bytes::copy_from_slice(r.bytes()?),
                min_seq: r.varint()?,
                max_seq: r.varint()?,
            };
            if !valid_object_id(&s.id) {
                return Err(DecodeError("bad segment id"));
            }
            segments.push(s);
        }
        let n = r.len(r.remaining())?;
        let mut tail: Vec<Entry> = Vec::with_capacity(n.min(65536));
        for _ in 0..n {
            let e = Entry::decode(&mut r, &src)?;
            if let Some(p) = tail.last()
                && e.key <= p.key
            {
                return Err(DecodeError("tail not sorted"));
            }
            tail.push(e);
        }
        let tombstone_horizon = r.varint()?;
        let n = r.len(r.remaining())?;
        let mut recent_commits = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            recent_commits.push((r.u128()?, r.varint()?));
        }
        let n = r.len(r.remaining())?;
        let mut version_times = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            version_times.push((r.varint()?, r.varint()?));
        }
        let n = r.len(r.remaining())?;
        let mut obsolete = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            let kind = match r.u8()? {
                0 => ObjKind::Segment,
                1 => ObjKind::Blob,
                _ => return Err(DecodeError("bad obsolete kind")),
            };
            let id = r.string()?;
            if !valid_object_id(&id) {
                return Err(DecodeError("bad obsolete id"));
            }
            obsolete.push(Obsolete { kind, id, since_ms: r.varint()? });
        }
        let last_sweep_ms = r.varint()?;
        if !r.is_empty() {
            return Err(DecodeError("manifest trailing data"));
        }
        Ok(Manifest {
            db_id,
            version,
            write_seq,
            created_ms,
            flags,
            lease,
            segments,
            tail,
            tombstone_horizon,
            recent_commits,
            version_times,
            obsolete,
            last_sweep_ms,
        })
    }

    /// Newest sampled version that is at least `age_ms` old at `now_ms`
    /// (0 if none).
    pub fn version_at_least_age(&self, now_ms: u64, age_ms: u64) -> Seq {
        let cutoff = now_ms.saturating_sub(age_ms);
        self.version_times.iter().filter(|(_, t)| *t <= cutoff).map(|(v, _)| *v).max().unwrap_or(0)
    }

    pub fn commit_version(&self, id: u128) -> Option<Seq> {
        self.recent_commits.iter().rev().find(|(c, _)| *c == id).map(|(_, v)| *v)
    }
}

/// Object ids (segments, blobs) are 32 lowercase hex characters.
pub fn valid_object_id(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::types::{BlobRef, Value};
    use crate::util::rng::Rng;

    fn sample() -> Manifest {
        let mut m = Manifest::new("db1", 123);
        m.version = 42;
        m.write_seq = 50;
        m.flags = FLAG_DELETED;
        m.lease = Some(Lease { node: "n1".into(), expires_ms: 999 });
        m.segments.push(SegmentMeta {
            id: "0".repeat(32),
            size: 100,
            entries: 3,
            tombstones: 1,
            min_key: Bytes::from_static(b"a"),
            max_key: Bytes::from_static(b"z"),
            min_seq: 1,
            max_seq: 40,
        });
        m.tail.push(Entry { key: Bytes::from_static(b"k1"), seq: 41, value: Value::Inline(Bytes::from_static(b"v")) });
        m.tail.push(Entry { key: Bytes::from_static(b"k2"), seq: 42, value: Value::Tombstone });
        m.tail.push(Entry {
            key: Bytes::from_static(b"k3"),
            seq: 42,
            value: Value::Blob(BlobRef { id: "a".repeat(32), size: 5, sha256: [1; 32], content_type: "x/y".into() }),
        });
        m.tombstone_horizon = 7;
        m.recent_commits = vec![(1, 41), (u128::MAX, 42)];
        m.version_times = vec![(0, 100), (40, 200)];
        m.obsolete = vec![Obsolete { kind: ObjKind::Blob, id: "b".repeat(32), since_ms: 5 }];
        m
    }

    #[test]
    fn roundtrip() {
        let m = sample();
        let enc = m.encode();
        assert_eq!(Manifest::decode(&enc).unwrap(), m);
        assert_eq!(m.commit_version(u128::MAX), Some(42));
        assert_eq!(m.version_at_least_age(250, 100), 0);
        assert_eq!(m.version_at_least_age(400, 100), 40);
    }

    #[test]
    fn fuzz_decode_never_panics() {
        let good = sample().encode().to_vec();
        let mut r = Rng::new(5);
        for i in 0..5000 {
            let mut d = if i % 3 == 0 {
                let n = r.usize(200);
                r.bytes(n)
            } else {
                good.clone()
            };
            if !d.is_empty() {
                for _ in 0..1 + r.usize(3) {
                    let p = r.usize(d.len());
                    d[p] = r.next_u32() as u8;
                }
            }
            if i % 5 == 0 {
                d.truncate(r.usize(d.len() + 1));
            }
            // Re-checksum some mutated inputs so decoding goes deeper.
            if i % 2 == 1 && d.len() > 4 {
                let n = d.len() - 4;
                let c = crc32c(&d[..n]).to_le_bytes();
                d[n..].copy_from_slice(&c);
            }
            let _ = Manifest::decode(&Bytes::from(d));
        }
    }
}
