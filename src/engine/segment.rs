//! Immutable sorted segment files (SSTs).
//!
//! ```text
//! [data block]* [bloom] [index] [footer: 64 bytes]
//! block  = entry* ++ count:u32 ++ crc32c:u32
//! bloom  = Bloom ++ crc32c:u32
//! index  = count:varint ++ (last_key, offset:u64, len:u32)* ++ crc32c:u32
//! footer = "BRSG" format:u16 0:u16 index_off:u64 index_len:u32
//!          bloom_off:u64 bloom_len:u32 entries:u64 min_seq:u64 max_seq:u64
//!          pad:u32 crc32c:u32
//! ```
//! This module is pure (no I/O); see `version.rs` for loading.

use super::types::{Entry, Seq, Value};
use crate::util::bloom::Bloom;
use crate::util::codec::{DResult, DecodeError, Reader, Writer};
use crate::util::crc32c::crc32c;
use bytes::Bytes;
use std::sync::Arc;

pub const FOOTER_LEN: usize = 64;
const MAGIC: &[u8; 4] = b"BRSG";
const FORMAT: u16 = 1;
const BLOOM_BITS_PER_KEY: usize = 10;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentStats {
    pub size: u64,
    pub entries: u64,
    pub tombstones: u64,
    pub min_key: Bytes,
    pub max_key: Bytes,
    pub min_seq: Seq,
    pub max_seq: Seq,
}

pub struct SegmentBuilder {
    out: Vec<u8>,
    block: Writer,
    block_entries: u32,
    block_target: usize,
    index: Vec<(Bytes, u64, u32)>,
    key_hashes: Vec<Bytes>,
    entries: u64,
    tombstones: u64,
    min_seq: Seq,
    max_seq: Seq,
    first_key: Option<Bytes>,
    last_key: Option<Bytes>,
}

impl SegmentBuilder {
    pub fn new(block_target: usize) -> SegmentBuilder {
        SegmentBuilder {
            out: Vec::new(),
            block: Writer::new(),
            block_entries: 0,
            block_target: block_target.max(256),
            index: Vec::new(),
            key_hashes: Vec::new(),
            entries: 0,
            tombstones: 0,
            min_seq: Seq::MAX,
            max_seq: 0,
            first_key: None,
            last_key: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }

    /// Approximate size so far.
    pub fn size(&self) -> usize {
        self.out.len() + self.block.len()
    }

    /// Keys must be added in strictly increasing order.
    pub fn add(&mut self, e: &Entry) {
        if let Some(last) = &self.last_key {
            assert!(e.key > *last, "segment keys must be strictly increasing");
        }
        e.encode(&mut self.block);
        self.block_entries += 1;
        self.entries += 1;
        if e.value.is_tombstone() {
            self.tombstones += 1;
        }
        self.min_seq = self.min_seq.min(e.seq);
        self.max_seq = self.max_seq.max(e.seq);
        if self.first_key.is_none() {
            self.first_key = Some(e.key.clone());
        }
        self.last_key = Some(e.key.clone());
        self.key_hashes.push(e.key.clone());
        if self.block.len() >= self.block_target {
            self.finish_block();
        }
    }

    fn finish_block(&mut self) {
        if self.block_entries == 0 {
            return;
        }
        let mut b = std::mem::take(&mut self.block);
        b.u32(self.block_entries);
        let crc = crc32c(&b.buf);
        b.u32(crc);
        let off = self.out.len() as u64;
        let len = b.buf.len() as u32;
        self.out.extend_from_slice(&b.buf);
        self.index.push((self.last_key.clone().unwrap_or_default(), off, len));
        self.block_entries = 0;
    }

    pub fn finish(mut self) -> (Bytes, SegmentStats) {
        self.finish_block();
        let mut bloom = Bloom::new(self.key_hashes.len(), BLOOM_BITS_PER_KEY);
        for k in &self.key_hashes {
            bloom.insert(k);
        }
        let bloom_off = self.out.len() as u64;
        let mut bw = Writer::new();
        bloom.encode(&mut bw);
        let crc = crc32c(&bw.buf);
        bw.u32(crc);
        let bloom_len = bw.buf.len() as u32;
        self.out.extend_from_slice(&bw.buf);

        let index_off = self.out.len() as u64;
        let mut iw = Writer::new();
        iw.varint(self.index.len() as u64);
        for (k, off, len) in &self.index {
            iw.bytes(k);
            iw.u64(*off);
            iw.u32(*len);
        }
        let crc = crc32c(&iw.buf);
        iw.u32(crc);
        let index_len = iw.buf.len() as u32;
        self.out.extend_from_slice(&iw.buf);

        let mut f = Writer::with_capacity(FOOTER_LEN);
        f.raw(MAGIC);
        f.u16(FORMAT);
        f.u16(0);
        f.u64(index_off);
        f.u32(index_len);
        f.u64(bloom_off);
        f.u32(bloom_len);
        f.u64(self.entries);
        let min_seq = if self.entries == 0 { 0 } else { self.min_seq };
        f.u64(min_seq);
        f.u64(self.max_seq);
        f.u32(0);
        let crc = crc32c(&f.buf);
        f.u32(crc);
        debug_assert_eq!(f.buf.len(), FOOTER_LEN);
        self.out.extend_from_slice(&f.buf);

        let size = self.out.len() as u64;
        let stats = SegmentStats {
            size,
            entries: self.entries,
            tombstones: self.tombstones,
            min_key: self.first_key.unwrap_or_default(),
            max_key: self.last_key.unwrap_or_default(),
            min_seq,
            max_seq: self.max_seq,
        };
        (Bytes::from(self.out), stats)
    }
}

/// Build a segment from sorted, unique entries.
pub fn build(entries: &[Entry], block_target: usize) -> (Bytes, SegmentStats) {
    let mut b = SegmentBuilder::new(block_target);
    for e in entries {
        b.add(e);
    }
    b.finish()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Footer {
    pub index_off: u64,
    pub index_len: u32,
    pub bloom_off: u64,
    pub bloom_len: u32,
    pub entries: u64,
    pub min_seq: Seq,
    pub max_seq: Seq,
}

impl Footer {
    pub fn decode(b: &[u8], file_size: u64) -> DResult<Footer> {
        if b.len() != FOOTER_LEN {
            return Err(DecodeError("bad footer length"));
        }
        let crc = u32::from_le_bytes([b[60], b[61], b[62], b[63]]);
        if crc32c(&b[..60]) != crc {
            return Err(DecodeError("footer checksum mismatch"));
        }
        let mut r = Reader::new(b);
        if r.take(4)? != MAGIC {
            return Err(DecodeError("bad segment magic"));
        }
        if r.u16()? != FORMAT {
            return Err(DecodeError("unsupported segment format"));
        }
        r.u16()?;
        let f = Footer {
            index_off: r.u64()?,
            index_len: r.u32()?,
            bloom_off: r.u64()?,
            bloom_len: r.u32()?,
            entries: r.u64()?,
            min_seq: r.u64()?,
            max_seq: r.u64()?,
        };
        let data_end = file_size.checked_sub(FOOTER_LEN as u64).ok_or(DecodeError("segment too small"))?;
        let ie = f.index_off.checked_add(f.index_len as u64).ok_or(DecodeError("bad index range"))?;
        let be = f.bloom_off.checked_add(f.bloom_len as u64).ok_or(DecodeError("bad bloom range"))?;
        if ie > data_end || be > f.index_off {
            return Err(DecodeError("segment section out of range"));
        }
        Ok(f)
    }
}

/// Parsed, cacheable metadata of a segment: block index + bloom filter.
#[derive(Debug)]
pub struct SegmentIndex {
    pub blocks: Vec<(Bytes, u64, u32)>,
    pub bloom: Bloom,
    pub footer: Footer,
    pub size: u64,
}

fn check_crc(section: &[u8]) -> DResult<&[u8]> {
    if section.len() < 4 {
        return Err(DecodeError("section too short"));
    }
    let (body, c) = section.split_at(section.len() - 4);
    let crc = u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
    if crc32c(body) != crc {
        return Err(DecodeError("checksum mismatch"));
    }
    Ok(body)
}

impl SegmentIndex {
    pub fn decode(footer: Footer, index: &[u8], bloom: &[u8], size: u64) -> DResult<SegmentIndex> {
        let ib = check_crc(index)?;
        let src = Bytes::copy_from_slice(ib);
        let mut r = Reader::new(&src);
        let n = r.len(src.len())?;
        let mut blocks = Vec::with_capacity(n);
        let mut prev_end = 0u64;
        for _ in 0..n {
            let k = Bytes::copy_from_slice(r.bytes()?);
            let off = r.u64()?;
            let len = r.u32()?;
            if off != prev_end || len < 8 {
                return Err(DecodeError("bad block layout"));
            }
            prev_end = off + len as u64;
            if let Some((pk, _, _)) = blocks.last()
                && k <= *pk
            {
                return Err(DecodeError("index keys not increasing"));
            }
            blocks.push((k, off, len));
        }
        if !r.is_empty() || prev_end != footer.bloom_off {
            return Err(DecodeError("index trailing data"));
        }
        let bb = check_crc(bloom)?;
        let bloom = Bloom::decode(&mut Reader::new(bb))?;
        Ok(SegmentIndex { blocks, bloom, footer, size })
    }

    /// Parse a fully-loaded segment.
    pub fn decode_whole(data: &[u8]) -> DResult<SegmentIndex> {
        if data.len() < FOOTER_LEN {
            return Err(DecodeError("segment too small"));
        }
        let size = data.len() as u64;
        let footer = Footer::decode(&data[data.len() - FOOTER_LEN..], size)?;
        let index = &data[footer.index_off as usize..(footer.index_off + footer.index_len as u64) as usize];
        let bloom = &data[footer.bloom_off as usize..(footer.bloom_off + footer.bloom_len as u64) as usize];
        SegmentIndex::decode(footer, index, bloom, size)
    }

    /// Index of the first block whose last key is >= `key`.
    pub fn block_for(&self, key: &[u8]) -> Option<usize> {
        let i = self.blocks.partition_point(|(last, _, _)| last.as_ref() < key);
        if i < self.blocks.len() { Some(i) } else { None }
    }

    pub fn mem_size(&self) -> usize {
        self.blocks.iter().map(|(k, _, _)| k.len() + 32).sum::<usize>() + self.bloom.size_bytes() + 128
    }
}

pub type Block = Arc<Vec<Entry>>;

/// Decode and verify one block (including its trailing count and crc).
pub fn decode_block(raw: Bytes) -> DResult<Block> {
    let body = check_crc(&raw)?;
    if body.len() < 4 {
        return Err(DecodeError("block too short"));
    }
    let n = u32::from_le_bytes([body[body.len() - 4], body[body.len() - 3], body[body.len() - 2], body[body.len() - 1]])
        as usize;
    let entries_bytes = raw.slice(0..body.len() - 4);
    let mut r = Reader::new(&entries_bytes);
    let mut out = Vec::with_capacity(n.min(entries_bytes.len()));
    for _ in 0..n {
        let e = Entry::decode(&mut r, &entries_bytes)?;
        if let Some(p) = out.last() {
            let p: &Entry = p;
            if e.key <= p.key {
                return Err(DecodeError("block keys not increasing"));
            }
        }
        out.push(e);
    }
    if !r.is_empty() {
        return Err(DecodeError("block trailing data"));
    }
    Ok(Arc::new(out))
}

/// Read every entry of a fully-loaded segment (tests, compaction).
pub fn read_all(data: &Bytes) -> DResult<Vec<Entry>> {
    let idx = SegmentIndex::decode_whole(data)?;
    let mut out = Vec::with_capacity(idx.footer.entries as usize);
    for (_, off, len) in &idx.blocks {
        let b = decode_block(data.slice(*off as usize..(*off + *len as u64) as usize))?;
        out.extend(b.iter().cloned());
    }
    Ok(out)
}

/// Helper used by tests and fuzzers: is the value a tombstone.
pub fn live(e: &Entry) -> bool {
    !matches!(e.value, Value::Tombstone)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::prop;
    use crate::util::rng::Rng;

    fn gen_entries(r: &mut Rng) -> Vec<Entry> {
        let n = r.usize(400);
        let mut keys: Vec<Vec<u8>> = (0..n).map(|_| prop::small_key(r, 12)).collect();
        keys.sort();
        keys.dedup();
        keys.into_iter()
            .map(|k| Entry {
                key: Bytes::from(k),
                seq: r.below(1000),
                value: if r.chance(0.2) {
                    Value::Tombstone
                } else {
                    let len = r.usize(200);
                    Value::Inline(Bytes::from(r.bytes(len)))
                },
            })
            .collect()
    }

    #[test]
    fn prop_roundtrip() {
        prop::check(
            "segment-roundtrip",
            200,
            |r| (gen_entries(r), 256 + r.usize(4096)),
            |(v, bt)| prop::shrink_vec(v).into_iter().map(|v| (v, *bt)).collect(),
            |(entries, bt)| {
                let (data, stats) = build(entries, *bt);
                let back = read_all(&data).map_err(|e| e.to_string())?;
                if &back != entries {
                    return Err("entries differ".into());
                }
                if stats.entries != entries.len() as u64 || stats.size != data.len() as u64 {
                    return Err("bad stats".into());
                }
                let idx = SegmentIndex::decode_whole(&data).map_err(|e| e.to_string())?;
                for e in entries {
                    if !idx.bloom.may_contain(&e.key) {
                        return Err("bloom false negative".into());
                    }
                    let bi = idx.block_for(&e.key).ok_or("no block")?;
                    let (_, off, len) = &idx.blocks[bi];
                    let b = decode_block(data.slice(*off as usize..(*off + *len as u64) as usize))
                        .map_err(|e| e.to_string())?;
                    if !b.iter().any(|x| x == e) {
                        return Err("entry not in its block".into());
                    }
                }
                Ok(())
            },
        );
    }

    #[test]
    fn fuzz_decoders_never_panic() {
        let mut r = Rng::new(99);
        let (good, _) = build(&gen_entries(&mut Rng::new(1)), 512);
        for i in 0..3000 {
            let mut d = if i % 2 == 0 {
                good.to_vec()
            } else {
                let n = r.usize(300);
                r.bytes(n)
            };
            if !d.is_empty() {
                for _ in 0..1 + r.usize(4) {
                    let p = r.usize(d.len());
                    d[p] = r.next_u32() as u8;
                }
            }
            let d = Bytes::from(d);
            let _ = read_all(&d);
            let _ = decode_block(d.clone());
            if d.len() >= FOOTER_LEN {
                let _ = Footer::decode(&d[d.len() - FOOTER_LEN..], d.len() as u64);
            }
        }
    }

    #[test]
    fn empty_segment() {
        let (data, stats) = build(&[], 1024);
        assert_eq!(stats.entries, 0);
        assert!(read_all(&data).unwrap().is_empty());
    }
}
