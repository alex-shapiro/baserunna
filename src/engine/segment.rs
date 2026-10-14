//! Immutable sorted segment files (SSTs).
//!
//! ```text
//! [data block]* [bloom] [index] [footer: 64 bytes]
//! block  = codec:u8 ++ payload ++ crc32c:u32        (crc covers codec and payload)
//!          payload = entries ++ count:u32            (codec 0: raw)
//!          payload = raw_len:varint ++ zstd(entries ++ count:u32)   (codec 1)
//! bloom  = Bloom ++ crc32c:u32
//! index  = count:varint ++ (last_key, offset:u64, len:u32)* ++ crc32c:u32
//! footer = "BRSG" format:u16 0:u16 index_off:u64 index_len:u32
//!          bloom_off:u64 bloom_len:u32 entries:u64 min_seq:u64 max_seq:u64
//!          pad:u32 crc32c:u32
//! ```
//! Blocks are compressed independently, so a point read fetches and
//! decompresses one block. This module is pure (no I/O); see `version.rs`
//! for loading.

use super::types::{Entry, Seq};
use crate::util::bloom::{Bloom, key_hash};
use crate::util::codec::{DResult, DecodeError, Reader, Writer};
use crate::util::crc32c::crc32c;
use bytes::Bytes;
use std::sync::Arc;

pub const FOOTER_LEN: usize = 64;
const MAGIC: &[u8; 4] = b"BRSG";
const FORMAT: u16 = 2;
const BLOOM_BITS_PER_KEY: usize = 10;
const CODEC_RAW: u8 = 0;
const CODEC_ZSTD: u8 = 1;
/// Decompressed blocks larger than this are rejected as corrupt.
const MAX_BLOCK_RAW: u64 = 64 << 20;
/// Smallest valid block: codec, entry count and checksum.
const MIN_BLOCK_LEN: u32 = 9;

/// How data blocks are compressed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    None,
    /// zstd at the given level. A block is stored raw when compression
    /// saves less than an eighth of it.
    Zstd(i32),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentStats {
    pub size: u64,
    pub entries: u64,
    pub tombstones: u64,
    pub min_key: Bytes,
    pub max_key: Bytes,
    pub min_seq: Seq,
    pub max_seq: Seq,
    /// Entries with an expiry, and the latest expiry (0 if none).
    pub expiring: u64,
    pub expires_max: u64,
}

/// Builds a segment in memory from entries added in key order.
pub struct SegmentBuilder {
    out: Vec<u8>,
    block: Writer,
    block_entries: u32,
    block_target: usize,
    compression: Compression,
    index: Vec<(Bytes, u64, u32)>,
    hashes: Vec<u64>,
    entries: u64,
    tombstones: u64,
    min_seq: Seq,
    max_seq: Seq,
    expiring: u64,
    expires_max: u64,
    first_key: Option<Bytes>,
    last_key: Vec<u8>,
}

impl SegmentBuilder {
    pub fn new(block_target: usize, compression: Compression) -> SegmentBuilder {
        SegmentBuilder {
            out: Vec::new(),
            block: Writer::new(),
            block_entries: 0,
            block_target: block_target.max(256),
            compression,
            index: Vec::new(),
            hashes: Vec::new(),
            entries: 0,
            tombstones: 0,
            min_seq: Seq::MAX,
            max_seq: 0,
            expiring: 0,
            expires_max: 0,
            first_key: None,
            last_key: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }

    /// Bytes the segment would have if finished now: exact without
    /// compression, an upper bound with it.
    pub fn size_estimate(&self) -> usize {
        let varint = |v: usize| (usize::BITS - (v | 1).leading_zeros()).div_ceil(7) as usize;
        // An index entry: the key and its length, offset and length.
        let entry = |k: usize| varint(k) + k + 12;
        let (blocks, pending) = match self.block_entries {
            0 => (self.index.len(), 0),
            _ => (self.index.len() + 1, self.block.len() + 9 + entry(self.last_key.len())),
        };
        let bits = (self.hashes.len() * BLOOM_BITS_PER_KEY).max(64).div_ceil(8);
        let bloom = 1 + varint(bits) + bits + 4;
        let index = varint(blocks) + self.index.iter().map(|(k, _, _)| entry(k.len())).sum::<usize>() + 4;
        self.out.len() + pending + bloom + index + FOOTER_LEN
    }

    /// Keys must be added in strictly increasing order. Keys and values
    /// are copied, so the entry's buffers are not retained.
    pub fn add(&mut self, e: &Entry) {
        if self.entries > 0 {
            assert!(e.key[..] > self.last_key[..], "segment keys must be strictly increasing");
        }
        e.encode(&mut self.block);
        self.block_entries += 1;
        self.entries += 1;
        if e.value.is_tombstone() {
            self.tombstones += 1;
        }
        self.min_seq = self.min_seq.min(e.seq);
        self.max_seq = self.max_seq.max(e.seq);
        if let Some(x) = e.expires {
            self.expiring += 1;
            self.expires_max = self.expires_max.max(x);
        }
        if self.first_key.is_none() {
            self.first_key = Some(Bytes::copy_from_slice(&e.key));
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(&e.key);
        self.hashes.push(key_hash(&e.key));
        if self.block.len() >= self.block_target {
            self.finish_block();
        }
    }

    fn finish_block(&mut self) {
        if self.block_entries == 0 {
            return;
        }
        let mut raw = std::mem::take(&mut self.block);
        raw.u32(self.block_entries);
        let off = self.out.len();
        let compressed = match self.compression {
            Compression::Zstd(level) if raw.len() >= 64 => {
                zstd::bulk::compress(&raw.buf, level).ok().filter(|c| c.len() + 10 < raw.len() - raw.len() / 8)
            }
            _ => None,
        };
        match compressed {
            Some(c) => {
                self.out.push(CODEC_ZSTD);
                let mut len = Writer::with_capacity(10);
                len.varint(raw.len() as u64);
                self.out.extend_from_slice(&len.buf);
                self.out.extend_from_slice(&c);
            }
            None => {
                self.out.push(CODEC_RAW);
                self.out.extend_from_slice(&raw.buf);
            }
        }
        let crc = crc32c(&self.out[off..]);
        self.out.extend_from_slice(&crc.to_le_bytes());
        let len = (self.out.len() - off) as u32;
        self.index.push((Bytes::copy_from_slice(&self.last_key), off as u64, len));
        self.block_entries = 0;
    }

    pub fn finish(mut self) -> (Bytes, SegmentStats) {
        self.finish_block();
        let mut bloom = Bloom::new(self.hashes.len(), BLOOM_BITS_PER_KEY);
        for h in &self.hashes {
            bloom.insert_hash(*h);
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
            max_key: if self.entries == 0 { Bytes::new() } else { Bytes::from(self.last_key) },
            min_seq,
            max_seq: self.max_seq,
            expiring: self.expiring,
            expires_max: self.expires_max,
        };
        (Bytes::from(self.out), stats)
    }
}

/// Build a segment from sorted, unique entries.
pub fn build(entries: &[Entry], block_target: usize, compression: Compression) -> (Bytes, SegmentStats) {
    let mut b = SegmentBuilder::new(block_target, compression);
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
            if off != prev_end || len < MIN_BLOCK_LEN {
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

    /// The byte range of blocks `first..end`.
    pub fn span(&self, blocks: std::ops::Range<usize>) -> std::ops::Range<u64> {
        let start = self.blocks[blocks.start].1;
        let (_, off, len) = self.blocks[blocks.end - 1];
        start..off + len as u64
    }

    pub fn mem_size(&self) -> usize {
        self.blocks.iter().map(|(k, _, _)| k.len() + 32).sum::<usize>() + self.bloom.size_bytes() + 128
    }
}

pub type Block = Arc<Vec<Entry>>;

/// Decode and verify one stored block.
pub fn decode_block(raw: Bytes) -> DResult<Block> {
    let body = check_crc(&raw)?;
    let (&codec, _) = body.split_first().ok_or(DecodeError("block too short"))?;
    let payload = raw.slice(1..body.len());
    let data = match codec {
        CODEC_RAW => payload,
        CODEC_ZSTD => {
            let mut r = Reader::new(&payload);
            let n = r.varint()?;
            if n > MAX_BLOCK_RAW {
                return Err(DecodeError("block too large"));
            }
            let c = &payload[payload.len() - r.remaining()..];
            let d = zstd::bulk::decompress(c, n as usize).map_err(|_| DecodeError("bad compressed block"))?;
            if d.len() as u64 != n {
                return Err(DecodeError("compressed block length mismatch"));
            }
            Bytes::from(d)
        }
        _ => return Err(DecodeError("unknown block codec")),
    };
    if data.len() < 4 {
        return Err(DecodeError("block too short"));
    }
    let tail = &data[data.len() - 4..];
    let n = u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]) as usize;
    let entries_bytes = data.slice(0..data.len() - 4);
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

/// Decode the blocks `first..` of a segment from `data`, starting at block
/// `first`'s offset.
pub fn decode_blocks(idx: &SegmentIndex, first: usize, data: &Bytes) -> DResult<Vec<Block>> {
    let base = idx.blocks.get(first).ok_or(DecodeError("block out of range"))?.1;
    let mut out = Vec::new();
    for (_, off, len) in &idx.blocks[first..] {
        let (s, e) = ((off - base) as usize, (off - base + *len as u64) as usize);
        if e > data.len() {
            break;
        }
        out.push(decode_block(data.slice(s..e))?);
    }
    Ok(out)
}

/// Decodes all entries from a complete segment buffer.
pub fn read_all(data: &Bytes) -> DResult<Vec<Entry>> {
    let idx = SegmentIndex::decode_whole(data)?;
    let mut out = Vec::with_capacity(idx.footer.entries as usize);
    for (_, off, len) in &idx.blocks {
        let b = decode_block(data.slice(*off as usize..(*off + *len as u64) as usize))?;
        out.extend(b.iter().cloned());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::types::Value;
    use crate::util::prop;
    use crate::util::rng::Rng;

    fn gen_entries(r: &mut Rng) -> Vec<Entry> {
        let n = r.usize(400);
        let mut keys: Vec<Vec<u8>> = (0..n).map(|_| prop::small_key(r, 12)).collect();
        keys.sort();
        keys.dedup();
        keys.into_iter()
            .map(|k| {
                let e = Entry::new(
                    Bytes::from(k),
                    r.below(1000),
                    if r.chance(0.2) {
                        Value::Tombstone
                    } else {
                        let len = r.usize(200);
                        Value::Inline(Bytes::from(r.bytes(len)))
                    },
                );
                match !e.value.is_tombstone() && r.chance(0.3) {
                    true => Entry { expires: Some(r.below(1 << 45)), ..e },
                    false => e,
                }
            })
            .collect()
    }

    #[test]
    fn rejects_inconsistent_layouts_with_valid_checksums() {
        let entries: Vec<Entry> = (0..60u64)
            .map(|i| Entry::new(Bytes::from(format!("k{i:04}")), i, Value::Inline(Bytes::from(vec![7u8; 40]))))
            .collect();
        let (data, _) = build(&entries, 256, Compression::None);
        assert_eq!(read_all(&data).unwrap(), entries);
        let fo = data.len() - FOOTER_LEN;
        // (offset, width) of index_off, index_len, bloom_off, bloom_len.
        for (off, width) in [(8usize, 8usize), (16, 4), (20, 8), (28, 4)] {
            for delta in [-1i64, 1, 1 << 20] {
                let mut d = data.to_vec();
                let f = &mut d[fo..];
                let mut v = [0u8; 8];
                v[..width].copy_from_slice(&f[off..off + width]);
                let n = (u64::from_le_bytes(v) as i64).wrapping_add(delta) as u64;
                f[off..off + width].copy_from_slice(&n.to_le_bytes()[..width]);
                let crc = crc32c(&f[..60]);
                f[60..].copy_from_slice(&crc.to_le_bytes());
                let d = Bytes::from(d);
                assert!(read_all(&d).is_err(), "footer field at {off} shifted by {delta} was accepted");
                assert!(SegmentIndex::decode_whole(&d).is_err());
            }
        }
    }

    /// Replaces `data`'s block index with `blocks` followed by `trailing` bytes,
    /// recomputing checksums.
    fn with_index(data: &Bytes, blocks: &[(Bytes, u64, u32)], trailing: &[u8]) -> Bytes {
        let fo = data.len() - FOOTER_LEN;
        let footer = Footer::decode(&data[fo..], data.len() as u64).unwrap();
        let mut iw = Writer::new();
        iw.varint(blocks.len() as u64);
        for (k, off, len) in blocks {
            iw.bytes(k);
            iw.u64(*off);
            iw.u32(*len);
        }
        iw.raw(trailing);
        let crc = crc32c(&iw.buf);
        iw.u32(crc);
        let mut f = data[fo..].to_vec();
        f[16..20].copy_from_slice(&(iw.buf.len() as u32).to_le_bytes());
        let crc = crc32c(&f[..60]);
        f[60..].copy_from_slice(&crc.to_le_bytes());
        let mut out = data[..footer.index_off as usize].to_vec();
        out.extend_from_slice(&iw.buf);
        out.extend_from_slice(&f);
        Bytes::from(out)
    }

    #[test]
    fn rejects_block_indexes_that_do_not_tile_the_data() {
        let entries: Vec<Entry> = (0..60u64)
            .map(|i| Entry::new(Bytes::from(format!("k{i:04}")), i, Value::Inline(Bytes::from(vec![7u8; 40]))))
            .collect();
        let (data, _) = build(&entries, 256, Compression::None);
        let blocks = SegmentIndex::decode_whole(&data).unwrap().blocks;
        assert!(blocks.len() >= 3);
        assert!(SegmentIndex::decode_whole(&with_index(&data, &blocks, &[])).is_ok(), "setup");
        let mut gap = blocks.clone();
        gap[1].1 += 1;
        gap[1].2 -= 1;
        let mut tiny = blocks.clone();
        tiny[0].2 = MIN_BLOCK_LEN - 1;
        tiny[1].1 = (MIN_BLOCK_LEN - 1) as u64;
        tiny[1].2 += blocks[0].2 - (MIN_BLOCK_LEN - 1);
        for (what, bad, trailing) in
            [("gap", gap, &[][..]), ("tiny block", tiny, &[][..]), ("trailing", blocks, &[0][..])]
        {
            assert!(SegmentIndex::decode_whole(&with_index(&data, &bad, trailing)).is_err(), "{what} was accepted");
        }
    }

    fn sealed(body: &[u8]) -> Bytes {
        let mut raw = body.to_vec();
        raw.extend_from_slice(&crc32c(body).to_le_bytes());
        Bytes::from(raw)
    }

    #[test]
    fn blocks_too_short_for_their_count_are_rejected() {
        for body in [&b""[..], b"\x00", b"\x00\x01", b"\x00\x01\x02\x03"] {
            assert!(decode_block(sealed(body)).is_err(), "{body:?}");
        }
    }

    #[test]
    fn rejects_bad_compressed_blocks() {
        let mut raw = Writer::new();
        Entry::new(Bytes::from_static(b"k"), 1, Value::Inline(Bytes::from(vec![b'x'; 300]))).encode(&mut raw);
        raw.u32(1);
        let z = zstd::bulk::compress(&raw.buf, 3).unwrap();
        let block = |codec: u8, n: u64, payload: &[u8]| {
            let mut w = Writer::new();
            w.u8(codec);
            w.varint(n);
            w.raw(payload);
            sealed(&w.buf)
        };
        assert_eq!(decode_block(block(CODEC_ZSTD, raw.len() as u64, &z)).unwrap().len(), 1, "setup");
        for (what, b) in [
            ("short length", block(CODEC_ZSTD, raw.len() as u64 - 1, &z)),
            ("long length", block(CODEC_ZSTD, raw.len() as u64 + 1, &z)),
            ("over the limit", block(CODEC_ZSTD, MAX_BLOCK_RAW + 1, &z)),
            ("a terabyte", block(CODEC_ZSTD, 1 << 40, &z)),
            ("garbage", block(CODEC_ZSTD, raw.len() as u64, &z[..z.len() / 2])),
            ("unknown codec", block(7, raw.len() as u64, &z)),
        ] {
            assert!(decode_block(b).is_err(), "{what} was accepted");
        }
    }

    #[test]
    fn compression_pays_or_is_skipped() {
        let mut r = Rng::new(3);
        let entries = |random: bool, r: &mut Rng| -> Vec<Entry> {
            (0..500u64)
                .map(|i| {
                    let v = if random {
                        r.bytes(100)
                    } else {
                        format!("{{\"id\":{i},\"name\":\"user number {i}\",\"active\":true}}").into_bytes()
                    };
                    Entry::new(Bytes::from(format!("row/{i:06}")), i, Value::Inline(Bytes::from(v)))
                })
                .collect()
        };
        for random in [false, true] {
            let es = entries(random, &mut r);
            let (plain, _) = build(&es, 4096, Compression::None);
            let (packed, _) = build(&es, 4096, Compression::Zstd(3));
            assert_eq!(read_all(&packed).unwrap(), es);
            if random {
                assert_eq!(packed.len(), plain.len(), "incompressible blocks are stored as they are");
            } else {
                assert!(packed.len() * 3 < plain.len(), "{} vs {} bytes", packed.len(), plain.len());
            }
        }
    }

    #[test]
    fn prop_roundtrip() {
        prop::check(
            "segment-roundtrip",
            200,
            |r| {
                let c = if r.chance(0.5) { Compression::None } else { Compression::Zstd(1 + r.below(5) as i32) };
                (gen_entries(r), 256 + r.usize(4096), c)
            },
            |(v, bt, c)| prop::shrink_vec(v).into_iter().map(|v| (v, *bt, *c)).collect(),
            |(entries, bt, c)| {
                let (data, stats) = build(entries, *bt, *c);
                let back = read_all(&data).map_err(|e| e.to_string())?;
                if &back != entries {
                    return Err("entries differ".into());
                }
                let want = SegmentStats {
                    size: data.len() as u64,
                    entries: entries.len() as u64,
                    tombstones: entries.iter().filter(|e| e.value.is_tombstone()).count() as u64,
                    min_key: entries.first().map(|e| e.key.clone()).unwrap_or_default(),
                    max_key: entries.last().map(|e| e.key.clone()).unwrap_or_default(),
                    min_seq: entries.iter().map(|e| e.seq).min().unwrap_or(0),
                    max_seq: entries.iter().map(|e| e.seq).max().unwrap_or(0),
                    expiring: entries.iter().filter(|e| e.expires.is_some()).count() as u64,
                    expires_max: entries.iter().filter_map(|e| e.expires).max().unwrap_or(0),
                };
                if stats != want {
                    return Err(format!("stats {stats:?}, want {want:?}"));
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
                if let Some(last) = entries.last() {
                    let past = [last.key.as_ref(), b"\xff"].concat();
                    if idx.block_for(&past).is_some() {
                        return Err("a block was found for a key past the last one".into());
                    }
                }
                Ok(())
            },
        );
    }

    #[test]
    fn size_estimates_track_the_finished_size() {
        let mut r = Rng::new(8);
        for _ in 0..50 {
            let entries = gen_entries(&mut r);
            let block_target = 256 + r.usize(2048);
            for cut in [entries.len() / 3, entries.len()] {
                let mut b = SegmentBuilder::new(block_target, Compression::None);
                for e in &entries[..cut] {
                    b.add(e);
                }
                let estimate = b.size_estimate();
                let (data, _) = b.finish();
                let diff = estimate as i64 - data.len() as i64;
                assert_eq!(diff, 0, "estimated {estimate} bytes for {}", data.len());
            }
        }
    }

    #[test]
    #[should_panic(expected = "strictly increasing")]
    fn keys_out_of_order_are_refused() {
        let e = |k: &'static [u8]| Entry::tombstone(Bytes::from_static(k), 1);
        let mut b = SegmentBuilder::new(256, Compression::None);
        b.add(&e(b"b"));
        b.add(&e(b"c"));
        b.add(&e(b"a"));
    }

    #[test]
    fn fuzz_decoders_never_panic() {
        let mut r = Rng::new(99);
        let (plain, _) = build(&gen_entries(&mut Rng::new(1)), 512, Compression::None);
        let (packed, _) = build(&gen_entries(&mut Rng::new(1)), 512, Compression::Zstd(3));
        for i in 0..3000 {
            let mut d = if i % 3 < 2 {
                if i % 3 == 0 { plain.to_vec() } else { packed.to_vec() }
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
        let (data, stats) = build(&[], 1024, Compression::Zstd(3));
        assert_eq!(stats.entries, 0);
        assert!(read_all(&data).unwrap().is_empty());
    }
}
