//! Columnar event part format (SPEC §5.13). An event part is an immutable,
//! sorted, column-major byte stream stored across fixed-size chunk objects
//! under `part/{id}/`.
//!
//! ```text
//! stream   = [column]* [metadata]
//! column   = [block]* [column index]
//! block    = framed (segment codec + CRC32C) values of a granule run
//! index    = framed dictionary, block locations, per-granule statistics, blooms
//! metadata = framed "BRPT", granule sizes, sort key, primary index, column
//!            directory, shared JSON path directory
//! ```
//!
//! Queries prune granules using the primary index and per-granule statistics,
//! then fetch only the column blocks covering matching granules. A [`PartRef`]
//! stores the byte offset and length of the part metadata footer so opening a
//! part requires a single ranged `GET`, and column indexes are fetched only
//! for referenced columns.

use super::json::JType;
use super::vector::{NULL_CODE, Phys, Strs, Vector};
use crate::engine::Shared;
use crate::engine::cache::{CacheKey, CacheVal};
use crate::engine::events::{Mask, PartRef, Runs, decode_mask_object};
use crate::engine::segment::{Compression, frame, unframe};
use crate::error::{Error, Result};
use crate::store::{GetOpts, PutMode, StoreError};
use crate::util::bloom::Bloom;
use crate::util::codec::{DResult, DecodeError, Reader, Writer};
use crate::util::{Either, race};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

const MAGIC: &[u8; 4] = b"BRPT";
const FORMAT: u8 = 1;
/// Maximum byte length of string min/max prefixes in granule statistics.
pub const STAT_PREFIX: usize = 32;
/// Number of granules covered by each bloom filter entry.
pub const BLOOM_GRANULES: u32 = 4;
const BLOOM_BITS: usize = 10;
const BLOOM_MAX_BYTES: usize = 8 << 20;
/// Maximum number of distinct entries in a part-level string dictionary.
pub const DICT_MAX: usize = 65_536;
/// Maximum number of rows in a single block or granule.
pub const BLOCK_ROWS_MAX: usize = 1 << 22;
/// Maximum number of granules in a part.
pub const GRANULES_MAX: usize = 1 << 22;
/// Maximum number of physical columns in a part.
pub const PART_COLUMNS_MAX: usize = 1 << 16;
/// Maximum number of shared JSON paths listed in a part directory.
pub const SHARED_PATHS_MAX: usize = 1 << 20;
/// Maximum byte length of a primary-index sort key; longer sort keys are omitted.
pub const PK_KEY_MAX: usize = 4096;
/// Maximum total string byte length in a part-level dictionary.
pub const DICT_MAX_BYTES: usize = 1 << 20;
const COALESCE_BYTES: u64 = 1 << 20;
const FRAMED_MAX: u64 = crate::engine::segment::MAX_BLOCK_RAW + 16;

/// Identifier for a physical column stored in a part.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ColRef {
    /// User-defined column by ID; for a JSON column, stores a boolean presence flag.
    User(u32),
    /// Commit version of each row.
    Version,
    /// Row ordinal within its commit version.
    Ordinal,
    /// Uncompressed byte length ([`super::build::stored_len`]) of each row.
    RowBytes,
    /// Dedicated subcolumn for a JSON leaf path.
    Path { col: u32, key: String },
    /// Shared JSON bucket column storing serialized JSON objects for paths hashing to `bucket`.
    Shared { col: u32, bucket: u8 },
}

impl ColRef {
    fn encode(&self, w: &mut Writer) {
        match self {
            ColRef::User(id) => {
                w.u8(0);
                w.varint(*id as u64);
            }
            ColRef::Version => w.u8(1),
            ColRef::Ordinal => w.u8(2),
            ColRef::RowBytes => w.u8(5),
            ColRef::Path { col, key } => {
                w.u8(3);
                w.varint(*col as u64);
                w.str(key);
            }
            ColRef::Shared { col, bucket } => {
                w.u8(4);
                w.varint(*col as u64);
                w.u8(*bucket);
            }
        }
    }

    fn decode(r: &mut Reader<'_>) -> DResult<ColRef> {
        let id = |r: &mut Reader<'_>| u32::try_from(r.varint()?).map_err(|_| DecodeError("bad column id"));
        Ok(match r.u8()? {
            0 => ColRef::User(id(r)?),
            1 => ColRef::Version,
            2 => ColRef::Ordinal,
            3 => ColRef::Path { col: id(r)?, key: r.string()? },
            4 => ColRef::Shared { col: id(r)?, bucket: r.u8()? },
            5 => ColRef::RowBytes,
            _ => return Err(DecodeError("bad column ref")),
        })
    }
}

/// Metadata entry for a physical column in [`PartMeta`].
#[derive(Clone, Debug, PartialEq)]
pub struct ColMeta {
    pub cref: ColRef,
    pub phys: Phys,
    /// True if string values contain serialized JSON text.
    pub json: bool,
    pub index_off: u64,
    pub index_len: u64,
    pub data_off: u64,
    pub data_len: u64,
    pub nonnull: u64,
}

/// Directory entry for a JSON leaf path stored in shared buckets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedPath {
    pub key: String,
    pub count: u64,
    pub ty: JType,
}

/// Footer metadata locating granules, primary keys, and columns in a part.
#[derive(Clone, Debug, PartialEq)]
pub struct PartMeta {
    pub rows: u64,
    /// Row count of each granule.
    pub granules: Vec<u32>,
    /// Starting row offset of each granule, followed by the total row count.
    pub starts: Vec<u64>,
    pub sort_ids: Vec<u32>,
    /// Tuple-encoded sort key of the first row of each granule and the last row of the part (empty if longer than [`PK_KEY_MAX`]).
    pub pk: Vec<Bytes>,
    pub cols: Vec<ColMeta>,
    /// Shared bucket paths for each JSON column.
    pub shared: BTreeMap<u32, Vec<SharedPath>>,
    /// JSON column IDs whose shared buckets contain paths omitted from `shared`.
    pub partial: BTreeSet<u32>,
}

impl PartMeta {
    /// Approximate heap memory usage in bytes.
    pub fn mem_size(&self) -> usize {
        let cols: usize = self
            .cols
            .iter()
            .map(|c| {
                96 + match &c.cref {
                    ColRef::Path { key, .. } => key.len(),
                    _ => 0,
                }
            })
            .sum();
        let shared: usize = self.shared.values().flatten().map(|p| p.key.len() + 48).sum();
        cols + shared + self.pk.iter().map(|k| k.len() + 16).sum::<usize>() + self.granules.len() * 12
    }

    /// Returns true if path `key` of JSON column `col` may reside in a shared bucket.
    pub fn may_share(&self, col: u32, key: &str) -> bool {
        self.partial.contains(&col) || self.shared.get(&col).is_some_and(|ps| ps.iter().any(|p| p.key == key))
    }

    pub fn find(&self, c: &ColRef) -> Option<usize> {
        self.cols.iter().position(|m| m.cref == *c)
    }

    pub fn rows_of(&self, g: Range<usize>) -> Range<u64> {
        self.starts[g.start]..self.starts[g.end]
    }

    pub fn granule_count(&self) -> usize {
        self.granules.len()
    }

    /// Returns `(col_idx, path_key)` for all dedicated path subcolumns of JSON column `col`.
    pub fn paths(&self, col: u32) -> impl Iterator<Item = (usize, &str)> {
        self.cols.iter().enumerate().filter_map(move |(i, m)| match &m.cref {
            ColRef::Path { col: c, key } if *c == col => Some((i, key.as_str())),
            _ => None,
        })
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let listed: usize = self.shared.values().map(Vec::len).sum();
        if self.granules.len() > GRANULES_MAX
            || [self.cols.len(), self.shared.len(), self.partial.len()].iter().any(|n| *n > PART_COLUMNS_MAX)
            || listed > SHARED_PATHS_MAX
        {
            return Err(Error::internal("part metadata exceeds granule, column, or path limits"));
        }
        let mut w = Writer::new();
        w.raw(MAGIC);
        w.u8(FORMAT);
        w.varint(self.rows);
        w.varint(self.granules.len() as u64);
        for g in &self.granules {
            w.varint(*g as u64);
        }
        w.varint(self.sort_ids.len() as u64);
        for s in &self.sort_ids {
            w.varint(*s as u64);
        }
        for k in &self.pk {
            w.bytes(k);
        }
        w.varint(self.cols.len() as u64);
        for c in &self.cols {
            c.cref.encode(&mut w);
            w.u8(c.phys as u8);
            w.bool(c.json);
            for v in [c.index_off, c.index_len, c.data_off, c.data_len, c.nonnull] {
                w.varint(v);
            }
        }
        w.varint(self.shared.len() as u64);
        for (col, paths) in &self.shared {
            w.varint(*col as u64);
            w.varint(paths.len() as u64);
            for p in paths {
                w.str(&p.key);
                w.varint(p.count);
                w.u8(p.ty.tag());
            }
        }
        w.varint(self.partial.len() as u64);
        for col in &self.partial {
            w.varint(*col as u64);
        }
        framed(&w.buf, Compression::Zstd(1), "part metadata")
    }

    /// Decodes part metadata and verifies that granule row counts sum to `rows`.
    pub fn decode(b: &Bytes, rows: u64) -> DResult<PartMeta> {
        let raw = unframe(b)?;
        let mut r = Reader::new(&raw);
        if r.take(4)? != MAGIC || r.u8()? != FORMAT {
            return Err(DecodeError("bad part metadata"));
        }
        if r.varint()? != rows || rows == 0 {
            return Err(DecodeError("part metadata does not match the part's rows"));
        }
        // Each granule requires at least 2 bytes (size varint + primary key length varint).
        let n = r.len(GRANULES_MAX.min(usize::try_from(rows).unwrap_or(usize::MAX)).min(r.remaining() / 2))?;
        let mut granules = Vec::with_capacity(n);
        let mut starts = Vec::with_capacity(n + 1);
        let mut at = 0u64;
        for _ in 0..n {
            let g = u32::try_from(r.varint()?).map_err(|_| DecodeError("bad granule"))?;
            if g == 0 || g as usize > BLOCK_ROWS_MAX {
                return Err(DecodeError("bad granule"));
            }
            starts.push(at);
            at += g as u64;
            granules.push(g);
        }
        starts.push(at);
        if at != rows {
            return Err(DecodeError("granules do not cover the part"));
        }
        let ns = r.len(8)?;
        let mut sort_ids = Vec::with_capacity(ns);
        for _ in 0..ns {
            sort_ids.push(u32::try_from(r.varint()?).map_err(|_| DecodeError("bad sort id"))?);
        }
        let mut pk = Vec::with_capacity(n + 1);
        for _ in 0..=n {
            let k = r.bytes()?;
            if k.len() > PK_KEY_MAX {
                return Err(DecodeError("primary index key too long"));
            }
            pk.push(Bytes::copy_from_slice(k));
        }
        let nc = r.len(PART_COLUMNS_MAX)?;
        let mut cols = Vec::with_capacity(nc);
        for _ in 0..nc {
            let cref = ColRef::decode(&mut r)?;
            let phys = Phys::from_tag(r.u8()?).ok_or(DecodeError("bad column type"))?;
            let json = r.bool()?;
            let mut v = [0u64; 5];
            for x in &mut v {
                *x = r.varint()?;
            }
            let [index_off, index_len, data_off, data_len, nonnull] = v;
            let in_range = index_off.checked_add(index_len).is_some() && data_off.checked_add(data_len).is_some();
            if !in_range || nonnull > rows {
                return Err(DecodeError("column out of range"));
            }
            cols.push(ColMeta { cref, phys, json, index_off, index_len, data_off, data_len, nonnull });
        }
        let nj = r.len(PART_COLUMNS_MAX)?;
        let mut shared = BTreeMap::new();
        let mut listed = 0;
        for _ in 0..nj {
            let col = u32::try_from(r.varint()?).map_err(|_| DecodeError("bad column id"))?;
            let np = r.len(SHARED_PATHS_MAX - listed)?;
            listed += np;
            let mut paths = Vec::with_capacity(np.min(4096));
            for _ in 0..np {
                let key = r.string()?;
                let count = r.varint()?;
                let ty = JType::from_tag(r.u8()?).ok_or(DecodeError("bad path type"))?;
                paths.push(SharedPath { key, count, ty });
            }
            shared.insert(col, paths);
        }
        let np = r.len(PART_COLUMNS_MAX)?;
        let mut partial = BTreeSet::new();
        for _ in 0..np {
            partial.insert(u32::try_from(r.varint()?).map_err(|_| DecodeError("bad column id"))?);
        }
        if !r.is_empty() {
            return Err(DecodeError("part metadata trailing data"));
        }
        Ok(PartMeta { rows, granules, starts, sort_ids, pk, cols, shared, partial })
    }
}

/// Minimum or maximum value of a column within a granule.
#[derive(Clone, Debug, PartialEq)]
pub enum Stat {
    Int(i64),
    Float(f64),
    Bool(bool),
    /// UTF-8 prefix truncated to at most [`STAT_PREFIX`] bytes.
    Str(String),
}

/// Per-granule column statistics.
#[derive(Clone, Debug, PartialEq)]
pub struct GranStat {
    pub nulls: u32,
    pub min: Option<Stat>,
    pub max: Option<Stat>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockLoc {
    pub off: u64,
    pub len: u64,
    pub first_granule: u32,
}

/// Per-column index containing dictionary entries, block locations, granule statistics, and bloom filters.
#[derive(Clone, Debug, PartialEq)]
pub struct ColIndex {
    pub dict: Option<Arc<Vec<String>>>,
    pub blocks: Vec<BlockLoc>,
    pub stats: Vec<GranStat>,
    /// Bloom filters covering groups of [`BLOOM_GRANULES`] granules.
    pub blooms: Vec<Bloom>,
}

impl ColIndex {
    pub fn block_of(&self, granule: usize) -> usize {
        self.blocks.partition_point(|b| b.first_granule as usize <= granule).saturating_sub(1)
    }

    fn granules_of(&self, b: usize, granules: usize) -> Range<usize> {
        self.blocks[b].first_granule as usize..self.blocks.get(b + 1).map_or(granules, |n| n.first_granule as usize)
    }

    /// Returns the number of rows in block `b`.
    pub fn rows_of(&self, b: usize, meta: &PartMeta) -> u64 {
        let rows = meta.rows_of(self.granules_of(b, meta.granule_count()));
        rows.end - rows.start
    }

    fn mem_size(&self) -> usize {
        self.dict.as_ref().map_or(0, |d| d.iter().map(|s| s.len() + 24).sum())
            + self.blocks.len() * 24
            + self.stats.len() * 80
            + self.blooms.iter().map(Bloom::size_bytes).sum::<usize>()
    }

    fn encode(&self, phys: Phys) -> Result<Vec<u8>> {
        let mut w = Writer::new();
        match &self.dict {
            None => w.u8(0),
            Some(d) => {
                w.u8(1);
                w.varint(d.len() as u64);
                for s in d.iter() {
                    w.str(s);
                }
            }
        }
        w.varint(self.blocks.len() as u64);
        for b in &self.blocks {
            w.varint(b.off);
            w.varint(b.len);
            w.varint(b.first_granule as u64);
        }
        w.varint(self.stats.len() as u64);
        for s in &self.stats {
            w.varint(s.nulls as u64);
            match (&s.min, &s.max) {
                (Some(lo), Some(hi)) => {
                    w.u8(1);
                    put_stat(&mut w, phys, lo);
                    put_stat(&mut w, phys, hi);
                }
                _ => w.u8(0),
            }
        }
        w.varint(self.blooms.len() as u64);
        for b in &self.blooms {
            b.encode(&mut w);
        }
        framed(&w.buf, Compression::Zstd(1), "a column index")
    }

    /// Decodes the index for `col` and validates that its blocks cover all granules in order.
    pub fn decode(b: &Bytes, meta: &PartMeta, col: &ColMeta) -> DResult<ColIndex> {
        let (phys, granules) = (col.phys, meta.granule_count());
        let raw = unframe(b)?;
        let mut r = Reader::new(&raw);
        let dict = match r.u8()? {
            0 => None,
            1 => {
                let n = r.len(DICT_MAX)?;
                let mut d = Vec::with_capacity(n);
                let mut bytes = 0;
                for _ in 0..n {
                    let s = r.string()?;
                    bytes += s.len();
                    if bytes > DICT_MAX_BYTES {
                        return Err(DecodeError("dictionary too large"));
                    }
                    d.push(s);
                }
                Some(Arc::new(d))
            }
            _ => return Err(DecodeError("bad dictionary tag")),
        };
        if dict.is_some() && phys != Phys::Str {
            return Err(DecodeError("dictionary on a non-string column"));
        }
        let nb = r.len(granules)?;
        if nb == 0 {
            return Err(DecodeError("a column index without blocks"));
        }
        let data_end = col.data_off.checked_add(col.data_len).ok_or(DecodeError("column out of range"))?;
        let mut blocks: Vec<BlockLoc> = Vec::with_capacity(nb);
        for _ in 0..nb {
            let b = BlockLoc {
                off: r.varint()?,
                len: r.varint()?,
                first_granule: u32::try_from(r.varint()?).map_err(|_| DecodeError("bad block"))?,
            };
            let in_order = match blocks.last() {
                None => b.first_granule == 0 && b.off >= col.data_off,
                Some(p) => b.first_granule > p.first_granule && b.off >= p.off + p.len,
            };
            if !in_order {
                return Err(DecodeError("blocks out of order"));
            }
            if b.first_granule as usize >= granules
                || b.len > FRAMED_MAX
                || b.off.checked_add(b.len).is_none_or(|end| end > data_end)
            {
                return Err(DecodeError("block outside its column"));
            }
            blocks.push(b);
        }
        let mut idx = ColIndex { dict, blocks, stats: Vec::new(), blooms: Vec::new() };
        if (0..nb).any(|b| idx.rows_of(b, meta) > BLOCK_ROWS_MAX as u64) {
            return Err(DecodeError("block too large"));
        }
        if r.varint()? != granules as u64 {
            return Err(DecodeError("column statistics do not match the part's granules"));
        }
        idx.stats.reserve_exact(granules);
        for g in 0..granules {
            let nulls = u32::try_from(r.varint()?).map_err(|_| DecodeError("bad null count"))?;
            if nulls > meta.granules[g] {
                return Err(DecodeError("bad null count"));
            }
            let (min, max) = match r.u8()? {
                0 => (None, None),
                1 => (Some(get_stat(&mut r, phys)?), Some(get_stat(&mut r, phys)?)),
                _ => return Err(DecodeError("bad stat tag")),
            };
            idx.stats.push(GranStat { nulls, min, max });
        }
        let nl = r.varint()?;
        if nl != 0 && nl != granules.div_ceil(BLOOM_GRANULES as usize) as u64 {
            return Err(DecodeError("column blooms do not match the part's granules"));
        }
        idx.blooms.reserve_exact(nl as usize);
        let mut bloom_bytes = 0;
        for _ in 0..nl {
            let b = Bloom::decode(&mut r)?;
            bloom_bytes += b.size_bytes();
            if bloom_bytes > BLOOM_MAX_BYTES {
                return Err(DecodeError("column blooms too large"));
            }
            idx.blooms.push(b);
        }
        if !r.is_empty() {
            return Err(DecodeError("column index trailing data"));
        }
        Ok(idx)
    }
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

fn put_stat(w: &mut Writer, phys: Phys, s: &Stat) {
    match (phys, s) {
        (Phys::Int, Stat::Int(v)) => w.varint(zigzag(*v)),
        (Phys::Float, Stat::Float(f)) => w.u64(f.to_bits()),
        (Phys::Bool, Stat::Bool(b)) => w.bool(*b),
        (Phys::Str, Stat::Str(s)) => w.str(s),
        _ => w.u8(0),
    }
}

fn get_stat(r: &mut Reader<'_>, phys: Phys) -> DResult<Stat> {
    Ok(match phys {
        Phys::Int => Stat::Int(unzigzag(r.varint()?)),
        Phys::Float => Stat::Float(f64::from_bits(r.u64()?)),
        Phys::Bool => Stat::Bool(r.bool()?),
        Phys::Str => {
            let s = r.string()?;
            if s.len() > STAT_PREFIX {
                return Err(DecodeError("string statistic too long"));
            }
            Stat::Str(s)
        }
    })
}

/// Returns the longest UTF-8 prefix of `s` within [`STAT_PREFIX`] bytes.
pub fn stat_prefix(s: &str) -> &str {
    let mut end = s.len().min(STAT_PREFIX);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

pub fn bloom_hash_int(v: i64) -> u64 {
    crate::util::bloom::key_hash(&[&b"i"[..], &v.to_le_bytes()].concat())
}

pub fn bloom_hash_float(v: f64) -> u64 {
    let v = if v == 0.0 { 0.0 } else { v };
    crate::util::bloom::key_hash(&[&b"f"[..], &v.to_bits().to_le_bytes()].concat())
}

pub fn bloom_hash_bool(v: bool) -> u64 {
    crate::util::bloom::key_hash(&[b'b', v as u8])
}

pub fn bloom_hash_str(v: &str) -> u64 {
    crate::util::bloom::key_hash(&[&b"s"[..], v.as_bytes()].concat())
}

const NULLS_NONE: u8 = 0;
const NULLS_BITMAP: u8 = 1;
const INT_PLAIN: u8 = 0;
const INT_DELTA: u8 = 1;
const INT_CONST: u8 = 2;
const STR_DICT: u8 = 0;
const STR_PLAIN: u8 = 1;

fn put_bitmap(w: &mut Writer, n: usize, bit: impl Fn(usize) -> bool) {
    let mut b = vec![0u8; n.div_ceil(8)];
    for i in 0..n {
        if bit(i) {
            b[i / 8] |= 1 << (i % 8);
        }
    }
    w.raw(&b);
}

fn get_bitmap(r: &mut Reader<'_>, n: usize) -> DResult<Vec<bool>> {
    let b = r.take(n.div_ceil(8))?;
    Ok((0..n).map(|i| b[i / 8] & (1 << (i % 8)) != 0).collect())
}

fn encode_block(v: &Vector, dict: Option<&BTreeMap<&str, u32>>) -> Result<Vec<u8>> {
    let n = v.len();
    let mut w = Writer::with_capacity(n * 4 + 16);
    w.varint(n as u64);
    if v.nulls() == 0 {
        w.u8(NULLS_NONE);
    } else {
        w.u8(NULLS_BITMAP);
        put_bitmap(&mut w, n, |i| v.is_null(i));
    }
    match v {
        Vector::Int(xs) => {
            let vals: Vec<i64> = xs.iter().map(|x| x.unwrap_or(0)).collect();
            if vals.windows(2).all(|p| p[0] == p[1]) {
                w.u8(INT_CONST);
                w.varint(zigzag(vals.first().copied().unwrap_or(0)));
            } else {
                let mut d = Writer::with_capacity(n * 2);
                let mut prev = 0i64;
                for x in &vals {
                    d.varint(zigzag(x.wrapping_sub(prev)));
                    prev = *x;
                }
                if d.len() < 8 * n {
                    w.u8(INT_DELTA);
                    w.raw(&d.buf);
                } else {
                    w.u8(INT_PLAIN);
                    for x in &vals {
                        w.u64(*x as u64);
                    }
                }
            }
        }
        Vector::Float(xs) => {
            for x in xs {
                w.u64(x.unwrap_or(0.0).to_bits());
            }
        }
        Vector::Bool(xs) => put_bitmap(&mut w, n, |i| xs[i] == Some(true)),
        Vector::Str(s) => match dict {
            Some(d) => {
                w.u8(STR_DICT);
                let width: u8 = match d.len() {
                    0..=256 => 1,
                    257..=65_536 => 2,
                    _ => 4,
                };
                w.u8(width);
                let map: Vec<u32> = s
                    .dict
                    .iter()
                    .map(|x| {
                        d.get(x.as_str())
                            .copied()
                            .ok_or_else(|| Error::internal("string missing from column dictionary"))
                    })
                    .collect::<Result<_>>()?;
                for c in &s.codes {
                    let code = if *c == NULL_CODE { 0 } else { map[*c as usize] };
                    match width {
                        1 => w.u8(code as u8),
                        2 => w.u16(code as u16),
                        _ => w.u32(code),
                    }
                }
            }
            None => {
                w.u8(STR_PLAIN);
                for i in 0..n {
                    if let Some(x) = s.get(i) {
                        w.str(x);
                    }
                }
            }
        },
    }
    Ok(w.into_inner())
}

/// Decodes a column block of `rows` rows using optional part-level dictionary `dict`.
pub fn decode_block(raw: &[u8], phys: Phys, dict: Option<&Arc<Vec<String>>>, rows: u64) -> DResult<Vector> {
    let mut r = Reader::new(raw);
    let n = r.len(BLOCK_ROWS_MAX)?;
    if n as u64 != rows {
        return Err(DecodeError("block does not hold its granules' rows"));
    }
    let nulls = match r.u8()? {
        NULLS_NONE => None,
        NULLS_BITMAP => Some(get_bitmap(&mut r, n)?),
        _ => return Err(DecodeError("bad null tag")),
    };
    let null = |i: usize| nulls.as_ref().is_some_and(|b| b[i]);
    let v = match phys {
        Phys::Int => {
            let vals: Vec<i64> = match r.u8()? {
                INT_CONST => vec![unzigzag(r.varint()?); n],
                INT_DELTA => {
                    let mut out = Vec::with_capacity(n);
                    let mut prev = 0i64;
                    for _ in 0..n {
                        prev = prev.wrapping_add(unzigzag(r.varint()?));
                        out.push(prev);
                    }
                    out
                }
                INT_PLAIN => (0..n).map(|_| r.u64().map(|x| x as i64)).collect::<DResult<_>>()?,
                _ => return Err(DecodeError("bad int encoding")),
            };
            Vector::Int(vals.into_iter().enumerate().map(|(i, x)| (!null(i)).then_some(x)).collect())
        }
        Phys::Float => Vector::Float(
            (0..n).map(|i| r.u64().map(|b| (!null(i)).then_some(f64::from_bits(b)))).collect::<DResult<_>>()?,
        ),
        Phys::Bool => {
            let b = get_bitmap(&mut r, n)?;
            Vector::Bool(b.into_iter().enumerate().map(|(i, x)| (!null(i)).then_some(x)).collect())
        }
        Phys::Str => match r.u8()? {
            STR_DICT => {
                let dict = dict.ok_or(DecodeError("dictionary codes without a dictionary"))?;
                let width = r.u8()?;
                let mut codes = Vec::with_capacity(n);
                for i in 0..n {
                    let c = match width {
                        1 => r.u8()? as u32,
                        2 => r.u16()? as u32,
                        4 => r.u32()?,
                        _ => return Err(DecodeError("bad code width")),
                    };
                    if null(i) {
                        codes.push(NULL_CODE);
                    } else if (c as usize) < dict.len() {
                        codes.push(c);
                    } else {
                        return Err(DecodeError("code outside the dictionary"));
                    }
                }
                Vector::Str(Strs { dict: dict.clone(), codes })
            }
            STR_PLAIN => {
                let mut vals: Vec<Option<&str>> = Vec::with_capacity(n);
                for i in 0..n {
                    vals.push(if null(i) { None } else { Some(r.str()?) });
                }
                Vector::Str(Strs::from_values(vals))
            }
            _ => return Err(DecodeError("bad string encoding")),
        },
    };
    if !r.is_empty() {
        return Err(DecodeError("block trailing data"));
    }
    Ok(v)
}

/// Returns the approximate uncompressed byte size of `v`.
pub fn vector_bytes(v: &Vector) -> usize {
    match v {
        Vector::Int(x) => x.len() * 8,
        Vector::Float(x) => x.len() * 8,
        Vector::Bool(x) => x.len().div_ceil(8),
        Vector::Str(s) => (0..s.codes.len()).map(|i| s.get(i).map_or(1, |x| x.len() + 1)).sum(),
    }
}

fn granule_stat(v: &Vector) -> GranStat {
    let nulls = v.nulls() as u32;
    let (min, max) = match v {
        Vector::Int(x) => {
            let it = || x.iter().flatten().copied();
            (it().min().map(Stat::Int), it().max().map(Stat::Int))
        }
        Vector::Float(x) => {
            let it = || x.iter().flatten().copied();
            (it().reduce(f64::min).map(Stat::Float), it().reduce(f64::max).map(Stat::Float))
        }
        Vector::Bool(x) => {
            let it = || x.iter().flatten().copied();
            (it().min().map(Stat::Bool), it().max().map(Stat::Bool))
        }
        Vector::Str(s) => {
            let it = || (0..s.codes.len()).filter_map(|i| s.get(i));
            let lo = it().min().map(|m| Stat::Str(stat_prefix(m).to_string()));
            let hi = it().max().map(|m| Stat::Str(stat_prefix(m).to_string()));
            (lo, hi)
        }
    };
    GranStat { nulls, min, max }
}

fn bloom_hashes(v: &Vector, out: &mut Vec<u64>) {
    match v {
        Vector::Int(x) => out.extend(x.iter().flatten().map(|x| bloom_hash_int(*x))),
        Vector::Float(x) => out.extend(x.iter().flatten().map(|x| bloom_hash_float(*x))),
        Vector::Bool(x) => out.extend(x.iter().flatten().map(|x| bloom_hash_bool(*x))),
        Vector::Str(s) => out.extend((0..s.codes.len()).filter_map(|i| s.get(i)).map(bloom_hash_str)),
    }
}

/// Column storage configuration for a part being written.
#[derive(Clone, Debug)]
pub struct ColPlan {
    pub cref: ColRef,
    pub phys: Phys,
    pub json: bool,
    /// Optional part-level dictionary for a string column.
    pub dict: Option<Arc<Vec<String>>>,
    pub bloom: bool,
}

/// Column-oriented row stream for constructing a part.
pub trait Source {
    fn rows(&self) -> u64;
    /// Uncompressed byte size of row `i`, used to determine granule boundaries.
    fn row_bytes(&self, i: u64) -> u64;
    /// Tuple-encoded sort key of row `i`.
    fn key_at(&self, i: u64) -> Bytes;
    fn plans(&self) -> &[ColPlan];
    fn shared_paths(&self) -> BTreeMap<u32, Vec<SharedPath>>;
    /// JSON column IDs whose shared paths in [`Source::shared_paths`] are truncated.
    fn partial_paths(&self) -> BTreeSet<u32>;
    /// Returns values for column `plan` across `rows`.
    fn column(&mut self, plan: usize, rows: Range<u64>) -> impl Future<Output = Result<Vector>> + Send;
}

/// Buffers a part byte stream and uploads fixed-size chunk objects to storage.
pub struct Sink<'a> {
    shared: &'a Shared,
    db: &'a str,
    pub id: String,
    chunk_bytes: u64,
    buf: Vec<u8>,
    written: u64,
    whole: Option<Vec<u8>>,
}

impl<'a> Sink<'a> {
    pub fn new(shared: &'a Shared, db: &'a str, id: String) -> Sink<'a> {
        Sink {
            shared,
            db,
            id,
            chunk_bytes: shared.cfg.event_chunk_bytes.max(1) as u64,
            buf: Vec::new(),
            written: 0,
            whole: Some(Vec::new()),
        }
    }

    pub fn pos(&self) -> u64 {
        self.written + self.buf.len() as u64
    }

    fn write(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
        if let Some(w) = &mut self.whole {
            if w.len() + b.len() > self.shared.cfg.segment_whole_fetch_max as usize {
                self.whole = None;
            } else {
                w.extend_from_slice(b);
            }
        }
    }

    async fn put_chunk(&mut self, data: Bytes) -> Result<()> {
        let key = crate::engine::part_chunk_key(self.db, &self.id, self.written / self.chunk_bytes);
        self.shared.retry.run(|| self.shared.store.put(&key, data.clone(), PutMode::Overwrite)).await?;
        self.written += data.len() as u64;
        Ok(())
    }

    async fn flush(&mut self, all: bool) -> Result<()> {
        while self.buf.len() as u64 >= self.chunk_bytes || (all && !self.buf.is_empty()) {
            let n = (self.chunk_bytes as usize).min(self.buf.len());
            let rest = self.buf.split_off(n);
            let chunk = Bytes::from(std::mem::replace(&mut self.buf, rest));
            self.put_chunk(chunk).await?;
        }
        Ok(())
    }
}

fn framed(raw: &[u8], c: Compression, what: &str) -> Result<Vec<u8>> {
    if raw.len() as u64 > crate::engine::segment::MAX_BLOCK_RAW {
        return Err(Error::internal(format!("{what} of {} bytes exceeds maximum block size", raw.len())));
    }
    let mut out = Vec::with_capacity(raw.len() / 2 + 16);
    frame(raw, c, &mut out);
    Ok(out)
}

/// Layout summary returned after writing a part.
#[derive(Clone, Debug)]
pub struct Written {
    pub size: u64,
    pub meta_off: u64,
    pub meta_len: u64,
    pub raw_size: u64,
    pub rows: u64,
    pub chunk_bytes: u64,
}

/// Partitions rows into granules of at most `max_rows` rows or `max_bytes` bytes.
pub fn granules(src: &impl Source, max_rows: usize, max_bytes: usize) -> Vec<u32> {
    let mut out = Vec::new();
    let (mut n, mut bytes) = (0u32, 0u64);
    for i in 0..src.rows() {
        n += 1;
        bytes += src.row_bytes(i);
        if n as usize >= max_rows.max(1) || bytes >= max_bytes as u64 {
            out.push(n);
            (n, bytes) = (0, 0);
        }
    }
    if n > 0 {
        out.push(n);
    }
    out
}

/// Writes all columns and metadata from `src` into `sink` using compression `level`.
pub async fn write(sink: &mut Sink<'_>, src: &mut impl Source, level: i32, sort_ids: Vec<u32>) -> Result<Written> {
    let cfg = &sink.shared.cfg;
    let rows = src.rows();
    if rows == 0 {
        return Err(Error::internal("event part cannot be empty"));
    }
    let granules = granules(src, cfg.event_granule_rows, cfg.event_granule_bytes);
    let mut starts = vec![0u64];
    for g in &granules {
        starts.push(starts.last().copied().unwrap_or(0) + *g as u64);
    }
    let indexed = |k: Bytes| if k.len() > PK_KEY_MAX { Bytes::new() } else { k };
    let mut pk: Vec<Bytes> = starts[..granules.len()].iter().map(|s| indexed(src.key_at(*s))).collect();
    pk.push(indexed(src.key_at(rows - 1)));
    let raw_size: u64 = (0..rows).map(|i| src.row_bytes(i)).sum();
    let block_bytes = cfg.event_block_bytes.max(1);
    let plans: Vec<ColPlan> = src.plans().to_vec();
    let mut cols = Vec::with_capacity(plans.len());
    for (pi, plan) in plans.iter().enumerate() {
        let data_off = sink.pos();
        let dict_map: Option<BTreeMap<&str, u32>> =
            plan.dict.as_ref().map(|d| d.iter().enumerate().map(|(i, s)| (s.as_str(), i as u32)).collect());
        let mut blocks = Vec::new();
        let mut stats = Vec::with_capacity(granules.len());
        let mut blooms = Vec::new();
        let mut hashes = Vec::new();
        let mut nonnull = 0u64;
        let mut bloom_bytes = 0;
        let mut pending: Vec<Vector> = Vec::new();
        let (mut pending_bytes, mut pending_rows) = (0usize, 0usize);
        let mut first = 0u32;
        for (g, gr) in granules.iter().enumerate() {
            let v = src.column(pi, starts[g]..starts[g] + *gr as u64).await?;
            if v.len() != *gr as usize || v.phys() != plan.phys {
                return Err(Error::internal("part source returned mismatched column length or type"));
            }
            stats.push(granule_stat(&v));
            nonnull += (v.len() - v.nulls()) as u64;
            if plan.bloom && bloom_bytes <= BLOOM_MAX_BYTES {
                bloom_hashes(&v, &mut hashes);
                if (g as u32 + 1).is_multiple_of(BLOOM_GRANULES) || g + 1 == granules.len() {
                    hashes.sort_unstable();
                    hashes.dedup();
                    let mut b = Bloom::new(hashes.len(), BLOOM_BITS);
                    hashes.iter().for_each(|h| b.insert_hash(*h));
                    bloom_bytes += b.size_bytes();
                    blooms.push(b);
                    hashes.clear();
                }
            }
            pending_bytes += vector_bytes(&v);
            pending_rows += v.len();
            pending.push(v);
            let next_rows = granules.get(g + 1).map_or(0, |n| *n as usize);
            if pending_bytes >= block_bytes || pending_rows + next_rows > BLOCK_ROWS_MAX || g + 1 == granules.len() {
                let block = Vector::concat(std::mem::take(&mut pending), plan.phys);
                let raw = encode_block(&block, dict_map.as_ref())?;
                let off = sink.pos();
                let framed = framed(&raw, Compression::Zstd(level), "a column block")?;
                sink.write(&framed);
                blocks.push(BlockLoc { off, len: framed.len() as u64, first_granule: first });
                first = g as u32 + 1;
                (pending_bytes, pending_rows) = (0, 0);
                sink.flush(false).await?;
            }
        }
        let data_len = sink.pos() - data_off;
        if bloom_bytes > BLOOM_MAX_BYTES {
            blooms.clear();
        }
        let index = ColIndex { dict: plan.dict.clone(), blocks, stats, blooms }.encode(plan.phys)?;
        let index_off = sink.pos();
        sink.write(&index);
        sink.flush(false).await?;
        cols.push(ColMeta {
            cref: plan.cref.clone(),
            phys: plan.phys,
            json: plan.json,
            index_off,
            index_len: index.len() as u64,
            data_off,
            data_len,
            nonnull,
        });
    }
    let meta = PartMeta {
        rows,
        granules,
        starts,
        sort_ids,
        pk,
        cols,
        shared: src.shared_paths(),
        partial: src.partial_paths(),
    };
    let encoded = meta.encode()?;
    let meta_off = sink.pos();
    sink.write(&encoded);
    let size = sink.pos();
    sink.flush(true).await?;
    if let Some(whole) = sink.whole.take() {
        let whole = Bytes::from(whole);
        let n = whole.len();
        sink.shared.cache.insert(CacheKey::part(sink.db, &sink.id, WHOLE, 0), CacheVal::Any(Arc::new(whole), n));
    }
    Ok(Written { size, meta_off, meta_len: encoded.len() as u64, raw_size, rows, chunk_bytes: sink.chunk_bytes })
}

const META: u32 = u32::MAX;
const WHOLE: u32 = u32::MAX - 1;
const MASK: u32 = u32::MAX - 2;
const INDEX: u32 = u32::MAX;

/// Object-store request counters for a part read operation.
#[derive(Debug, Default)]
pub struct ReadStats {
    pub gets: AtomicU64,
    pub bytes: AtomicU64,
}

/// Per-operation cache for decoded part metadata and indexes during maintenance reads.
pub type Memo = std::sync::Mutex<std::collections::HashMap<CacheKey, CacheVal>>;

/// Cache policy for reading event parts.
pub enum Caching<'a> {
    /// Populate the shared node block cache on misses.
    Shared,
    /// Probe the shared block cache without populating it on misses, storing metadata and indexes in a local [`Memo`].
    Private(&'a Memo),
}

/// Reader for fetching and caching event part metadata, indexes, blocks, and masks.
pub struct Reading<'a> {
    pub shared: &'a Shared,
    pub db: &'a str,
    pub cache: Caching<'a>,
    pub stats: &'a ReadStats,
}

impl Reading<'_> {
    fn cached<T: Send + Sync + 'static>(&self, k: &CacheKey) -> Option<Arc<T>> {
        let v = match self.cache {
            Caching::Shared => self.shared.cache.get(k),
            Caching::Private(m) => {
                let kept = m.lock().unwrap_or_else(|e| e.into_inner()).get(k).cloned();
                kept.or_else(|| self.shared.cache.peek(k))
            }
        };
        match v {
            Some(CacheVal::Any(a, _)) => a.downcast::<T>().ok(),
            _ => None,
        }
    }

    fn remember<T: Send + Sync + 'static>(&self, k: CacheKey, v: Arc<T>, size: usize) {
        match self.cache {
            Caching::Shared => self.shared.cache.insert(k, CacheVal::Any(v, size)),
            Caching::Private(m) => {
                m.lock().unwrap_or_else(|e| e.into_inner()).insert(k, CacheVal::Any(v, size));
            }
        }
    }

    fn remember_block(&self, k: CacheKey, v: Arc<Vector>, size: usize) {
        if let Caching::Shared = self.cache {
            self.shared.cache.insert(k, CacheVal::Any(v, size));
        }
    }

    async fn get(&self, key: &str, range: Option<Range<u64>>) -> Result<Bytes> {
        let fetch = || {
            self.shared.retry.run(|| self.shared.store.get(key, GetOpts { range: range.clone(), ..Default::default() }))
        };
        let hedge_ms = self.shared.cfg.event_get_hedge_ms;
        let mut first = Box::pin(fetch());
        let g = if hedge_ms == 0 {
            first.await
        } else {
            let mut delay = Box::pin(tokio::time::sleep(std::time::Duration::from_millis(hedge_ms)));
            match race(&mut first, &mut delay).await {
                Either::Left(r) => r,
                Either::Right(()) => {
                    crate::probe::hit(crate::probe::Probe::PartGetHedged);
                    self.stats.gets.fetch_add(1, Relaxed);
                    match race(&mut first, &mut Box::pin(fetch())).await {
                        Either::Left(r) | Either::Right(r) => r,
                    }
                }
            }
        };
        let g = g.map_err(|e| match e {
            StoreError::NotFound => {
                Error::unavailable("garbage collection removed a referenced event part during the read; retry")
            }
            other => other.into(),
        })?;
        self.stats.gets.fetch_add(1, Relaxed);
        self.stats.bytes.fetch_add(g.data.len() as u64, Relaxed);
        if let Some(r) = &range
            && g.data.len() as u64 != r.end - r.start
        {
            return Err(Error::internal("short event part read"));
        }
        Ok(g.data)
    }

    async fn read(&self, r: &PartRef, range: Range<u64>) -> Result<Bytes> {
        if range.end > r.size || range.start > range.end {
            return Err(Error::internal("event part range out of bounds"));
        }
        if let Some(w) = self.whole(r).await? {
            return Ok(w.slice(range.start as usize..range.end as usize));
        }
        let mut out = Vec::with_capacity((range.end - range.start) as usize);
        for (ci, cr) in crate::engine::blob::chunk_plan(r.chunk_bytes, range) {
            let key = crate::engine::part_chunk_key(self.db, &r.id, ci);
            let chunk_len = (r.size - ci * r.chunk_bytes).min(r.chunk_bytes);
            let whole_chunk = cr.start == 0 && cr.end == chunk_len;
            let data = self.get(&key, (!whole_chunk).then_some(cr)).await?;
            out.extend_from_slice(&data);
        }
        Ok(Bytes::from(out))
    }

    async fn whole(&self, r: &PartRef) -> Result<Option<Arc<Bytes>>> {
        if r.size > self.shared.cfg.segment_whole_fetch_max {
            return Ok(None);
        }
        let k = CacheKey::part(self.db, &r.id, WHOLE, 0);
        if let Some(w) = self.cached::<Bytes>(&k) {
            return Ok(Some(w));
        }
        let mut out = Vec::with_capacity(r.size as usize);
        for ci in 0..r.chunks() {
            out.extend_from_slice(&self.get(&crate::engine::part_chunk_key(self.db, &r.id, ci), None).await?);
        }
        if out.len() as u64 != r.size {
            return Err(Error::internal("event part size does not match its reference"));
        }
        let w = Arc::new(Bytes::from(out));
        self.remember(k, w.clone(), r.size as usize);
        Ok(Some(w))
    }

    pub async fn meta(&self, r: &PartRef) -> Result<Arc<PartMeta>> {
        let k = CacheKey::part(self.db, &r.id, META, 0);
        if let Some(m) = self.cached::<PartMeta>(&k) {
            return Ok(m);
        }
        let b = self.read(r, section(r.meta_off, r.meta_len)?).await?;
        let m = Arc::new(PartMeta::decode(&b, r.rows)?);
        self.remember(k, m.clone(), m.mem_size());
        Ok(m)
    }

    pub async fn index(&self, r: &PartRef, meta: &PartMeta, ci: usize) -> Result<Arc<ColIndex>> {
        let k = CacheKey::part(self.db, &r.id, ci as u32, INDEX);
        if let Some(i) = self.cached::<ColIndex>(&k) {
            return Ok(i);
        }
        let c = &meta.cols[ci];
        let b = self.read(r, section(c.index_off, c.index_len)?).await?;
        let idx = Arc::new(ColIndex::decode(&b, meta, c)?);
        self.remember(k, idx.clone(), idx.mem_size());
        Ok(idx)
    }

    /// Reads and decodes column `ci` across granule range `g`.
    pub async fn column(&self, r: &PartRef, meta: &PartMeta, ci: usize, g: Range<usize>) -> Result<Vector> {
        let c = &meta.cols[ci];
        if g.is_empty() {
            return Ok(c.phys.nulls(0));
        }
        let idx = self.index(r, meta, ci).await?;
        let (b0, b1) = (idx.block_of(g.start), idx.block_of(g.end - 1) + 1);
        let mut decoded: Vec<Option<Arc<Vector>>> =
            (b0..b1).map(|b| self.cached::<Vector>(&CacheKey::part(self.db, &r.id, ci as u32, b as u32))).collect();
        let mut b = b0;
        while b < b1 {
            if decoded[b - b0].is_some() {
                b += 1;
                continue;
            }
            let start = idx.blocks[b].off;
            let mut end = b + 1;
            while end < b1
                && decoded[end - b0].is_none()
                && idx.blocks[end].off + idx.blocks[end].len - start <= COALESCE_BYTES
            {
                end += 1;
            }
            let span = start..idx.blocks[end - 1].off + idx.blocks[end - 1].len;
            let data = self.read(r, span.clone()).await?;
            for bi in b..end {
                let loc = idx.blocks[bi];
                let at = (loc.off - span.start) as usize;
                let raw = unframe(&data.slice(at..at + loc.len as usize))?;
                let v = Arc::new(decode_block(&raw, c.phys, idx.dict.as_ref(), idx.rows_of(bi, meta))?);
                // Omit shared dictionary memory because it is already charged to ColIndex.
                let size = match (&*v, &idx.dict) {
                    (Vector::Str(s), Some(d)) if Arc::ptr_eq(&s.dict, d) => v.values_mem_size(),
                    _ => v.mem_size(),
                };
                self.remember_block(CacheKey::part(self.db, &r.id, ci as u32, bi as u32), v.clone(), size);
                decoded[bi - b0] = Some(v);
            }
            b = end;
        }
        let rows = meta.rows_of(g.clone());
        let mut parts = Vec::with_capacity(b1 - b0);
        for (i, v) in decoded.into_iter().enumerate() {
            let v = v.ok_or_else(|| Error::internal("event block not read"))?;
            let first = meta.starts[idx.blocks[b0 + i].first_granule as usize];
            let lo = rows.start.max(first) - first;
            let hi = (rows.end - first).min(v.len() as u64);
            parts.push(if lo == 0 && hi == v.len() as u64 { (*v).clone() } else { v.slice(lo as usize..hi as usize) });
        }
        Ok(Vector::concat(parts, c.phys))
    }

    /// Loads the deletion mask for part `r`.
    pub async fn mask(&self, r: &PartRef) -> Result<Arc<Runs>> {
        match &r.mask {
            None => Ok(Arc::new(Runs::default())),
            Some(Mask::Inline(runs)) => Ok(Arc::new(runs.clone())),
            Some(Mask::Object { id, .. }) => {
                let k = CacheKey::part(self.db, id, MASK, 0);
                if let Some(m) = self.cached::<Runs>(&k) {
                    return Ok(m);
                }
                let b = self.get(&crate::engine::part_chunk_key(self.db, id, 0), None).await?;
                let runs = decode_mask_object(&b)?;
                if runs.count() != r.deleted {
                    return Err(Error::internal("mask object does not match its reference"));
                }
                let runs = Arc::new(runs);
                self.remember(k, runs.clone(), runs.0.len() * 8);
                Ok(runs)
            }
        }
    }
}

fn section(off: u64, len: u64) -> DResult<Range<u64>> {
    match off.checked_add(len) {
        Some(end) if len <= FRAMED_MAX => Ok(off..end),
        _ => Err(DecodeError("event part section out of range")),
    }
}

/// Validates all column indexes and blocks in `stream` and returns `(PartMeta, row_positions)`.
pub fn inspect(
    stream: &Bytes,
    r: &PartRef,
) -> std::result::Result<(PartMeta, Vec<crate::engine::events::Pos>), String> {
    if stream.len() as u64 != r.size {
        return Err(format!("part {} has {} bytes, expected {}", r.id, stream.len(), r.size));
    }
    let slice = |off: u64, len: u64| -> std::result::Result<Bytes, String> {
        let end = off.checked_add(len).filter(|e| *e <= r.size).ok_or("range out of bounds")?;
        Ok(stream.slice(off as usize..end as usize))
    };
    let meta = PartMeta::decode(&slice(r.meta_off, r.meta_len)?, r.rows).map_err(|e| e.to_string())?;
    let mut columns: BTreeMap<ColRef, Vector> = BTreeMap::new();
    for c in &meta.cols {
        let idx = ColIndex::decode(&slice(c.index_off, c.index_len)?, &meta, c).map_err(|e| e.to_string())?;
        let mut parts = Vec::new();
        for (bi, b) in idx.blocks.iter().enumerate() {
            let raw = unframe(&slice(b.off, b.len)?).map_err(|e| e.to_string())?;
            let rows = idx.rows_of(bi, &meta);
            parts.push(decode_block(&raw, c.phys, idx.dict.as_ref(), rows).map_err(|e| e.to_string())?);
        }
        let v = Vector::concat(parts, c.phys);
        if v.len() as u64 != meta.rows || (v.len() - v.nulls()) as u64 != c.nonnull {
            return Err(format!("part {} column {:?} row or non-null count mismatch", r.id, c.cref));
        }
        columns.insert(c.cref.clone(), v);
    }
    let ints = |c: &ColRef| match columns.get(c) {
        Some(Vector::Int(x)) => Some(x.clone()),
        _ => None,
    };
    let ords = ints(&ColRef::Ordinal).ok_or_else(|| format!("part {} is missing ordinals", r.id))?;
    let vers = match (r.version, ints(&ColRef::Version)) {
        (Some(v), None) => vec![Some(v as i64); ords.len()],
        (None, Some(v)) => v,
        _ => return Err(format!("part {} specifies versions in both reference and column", r.id)),
    };
    let pos = ords
        .iter()
        .zip(&vers)
        .map(|(o, v)| crate::engine::events::Pos { version: v.unwrap_or(0) as u64, ordinal: o.unwrap_or(0) as u32 })
        .collect();
    Ok((meta, pos))
}

/// Uploads an external deletion mask object and returns `(mask_id, size_bytes)`.
pub async fn write_mask(shared: &Shared, db: &str, runs: &Runs) -> Result<(String, u64)> {
    let id = crate::engine::types::make_blob_id(shared.time_ms(), &shared.env.random_bytes(10));
    let data = crate::engine::events::encode_mask_object(runs);
    let key = crate::engine::part_chunk_key(db, &id, 0);
    shared.retry.run(|| shared.store.put(&key, data.clone(), PutMode::Overwrite)).await?;
    Ok((id, data.len() as u64))
}

/// Generates a part ID encoding a 48-bit timestamp and 80 random bits.
pub fn new_part_id(shared: &Shared) -> String {
    crate::engine::types::make_blob_id(shared.time_ms(), &shared.env.random_bytes(10))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::util::rng::Rng;

    #[test]
    fn roundtrip_block_encodings() {
        let mut r = Rng::new(9);
        for _ in 0..300 {
            let n = 1 + r.usize(200);
            let nullish = r.chance(0.3);
            let opt = |r: &mut Rng| !(nullish && r.chance(0.2));
            let constant = r.chance(0.2);
            let base = r.next_u64() as i64;
            let ints = Vector::Int(
                (0..n)
                    .map(|i| {
                        let v = if constant {
                            base
                        } else if r.chance(0.5) {
                            base.wrapping_add(i as i64)
                        } else {
                            r.next_u64() as i64
                        };
                        opt(&mut r).then_some(v)
                    })
                    .collect(),
            );
            let floats = Vector::Float((0..n).map(|_| opt(&mut r).then(|| r.unit() * 1e6 - 5e5)).collect());
            let bools = Vector::Bool((0..n).map(|_| opt(&mut r).then(|| r.chance(0.5))).collect());
            let words: Vec<String> = (0..1 + r.usize(10)).map(|i| format!("w{i}é")).collect();
            let strs: Vec<Option<&str>> =
                (0..n).map(|_| opt(&mut r).then(|| words[r.usize(words.len())].as_str())).collect();
            let strs = Vector::Str(Strs::from_values(strs));
            for v in [ints, floats, bools] {
                assert_eq!(decode_block(&encode_block(&v, None).unwrap(), v.phys(), None, n as u64).unwrap(), v);
            }
            let Vector::Str(s) = &strs else { unreachable!() };
            let back = decode_block(&encode_block(&strs, None).unwrap(), Phys::Str, None, n as u64).unwrap();
            let Vector::Str(b) = back else { panic!() };
            assert!((0..n).all(|i| b.get(i) == s.get(i)), "plain strings");
            let dict = Arc::new(words.clone());
            let map: BTreeMap<&str, u32> = dict.iter().enumerate().map(|(i, w)| (w.as_str(), i as u32)).collect();
            let back =
                decode_block(&encode_block(&strs, Some(&map)).unwrap(), Phys::Str, Some(&dict), n as u64).unwrap();
            let Vector::Str(b) = back else { panic!() };
            assert!((0..n).all(|i| b.get(i) == s.get(i)), "dictionary strings");
        }
    }

    #[test]
    fn roundtrip_metadata_and_indexes() {
        let meta = PartMeta {
            rows: 5,
            granules: vec![3, 2],
            starts: vec![0, 3, 5],
            sort_ids: vec![2, 1],
            pk: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b"), Bytes::from_static(b"c")],
            cols: vec![ColMeta {
                cref: ColRef::Path { col: 4, key: r#"["x"]"#.into() },
                phys: Phys::Str,
                json: true,
                index_off: 10,
                index_len: 20,
                data_off: 0,
                data_len: 10,
                nonnull: 4,
            }],
            shared: BTreeMap::from([(4, vec![SharedPath { key: r#"["y"]"#.into(), count: 2, ty: JType::Dyn }])]),
            partial: BTreeSet::from([4]),
        };
        assert_eq!(PartMeta::decode(&Bytes::from(meta.encode().unwrap()), 5).unwrap(), meta);
        let mut bloom = Bloom::new(3, 10);
        bloom.insert_hash(bloom_hash_str("x"));
        let idx = ColIndex {
            dict: Some(Arc::new(vec!["x".into(), "y".into()])),
            blocks: vec![BlockLoc { off: 0, len: 5, first_granule: 0 }, BlockLoc { off: 5, len: 5, first_granule: 1 }],
            stats: vec![
                GranStat { nulls: 1, min: Some(Stat::Str("x".into())), max: Some(Stat::Str("y".into())) },
                GranStat { nulls: 2, min: None, max: None },
            ],
            blooms: vec![bloom],
        };
        assert_eq!(ColIndex::decode(&Bytes::from(idx.encode(Phys::Str).unwrap()), &meta, &meta.cols[0]).unwrap(), idx);
        assert_eq!((idx.block_of(0), idx.block_of(1)), (0, 1));
        assert_eq!(stat_prefix(&"é".repeat(40)).len(), 32);
    }

    fn single_column_part(phys: Phys, granules: Vec<u32>, data_len: u64) -> PartMeta {
        let mut starts = vec![0];
        starts.extend(granules.iter().scan(0, |at, g| {
            *at += *g as u64;
            Some(*at)
        }));
        PartMeta {
            rows: *starts.last().unwrap(),
            pk: vec![Bytes::new(); starts.len()],
            granules,
            starts,
            sort_ids: vec![],
            cols: vec![ColMeta {
                cref: ColRef::Ordinal,
                phys,
                json: false,
                index_off: data_len,
                index_len: 0,
                data_off: 0,
                data_len,
                nonnull: 0,
            }],
            shared: BTreeMap::new(),
            partial: BTreeSet::new(),
        }
    }

    async fn store_part(
        db: &crate::engine::Db,
        stream: &[u8],
        chunk_bytes: u64,
        rows: u64,
        meta_at: (u64, u64),
    ) -> PartRef {
        let shared = db.shared();
        let id = new_part_id(shared);
        for (ci, chunk) in stream.chunks(chunk_bytes as usize).enumerate() {
            let key = crate::engine::part_chunk_key(&db.id, &id, ci as u64);
            shared.store.put(&key, Bytes::copy_from_slice(chunk), PutMode::Overwrite).await.unwrap();
        }
        PartRef {
            id,
            table: 1,
            partition: 0,
            chunk_bytes,
            size: stream.len() as u64,
            raw_size: rows,
            rows,
            level: 0,
            meta_off: meta_at.0,
            meta_len: meta_at.1,
            min_time: 0,
            max_time: 0,
            arrived_ms: 0,
            version: None,
            min_pos: crate::engine::events::Pos::default(),
            max_pos: crate::engine::events::Pos::default(),
            deleted: 0,
            masked_ms: 0,
            mask: None,
        }
    }

    fn read_with_blocks(blocks: impl Fn(&[(u64, u64)]) -> Vec<BlockLoc>) -> Result<Vector> {
        use crate::engine::tests::{engine, rt};
        rt().block_on(async {
            let (eng, _store, _env) = engine(1, crate::engine::EngineConfig::small_for_tests());
            let db = eng.db("d");
            db.open(true).await.unwrap();
            let shared = db.shared();
            let mut stream = Vec::new();
            let mut locs = Vec::new();
            for x in [1i64, 2] {
                let f = framed(&encode_block(&Vector::Int(vec![Some(x)]), None).unwrap(), Compression::Zstd(1), "")
                    .unwrap();
                locs.push((stream.len() as u64, f.len() as u64));
                stream.extend_from_slice(&f);
            }
            let stat = GranStat { nulls: 0, min: Some(Stat::Int(1)), max: Some(Stat::Int(2)) };
            let index = ColIndex { dict: None, blocks: blocks(&locs), stats: vec![stat.clone(), stat], blooms: vec![] }
                .encode(Phys::Int)
                .unwrap();
            let mut meta = single_column_part(Phys::Int, vec![1, 1], stream.len() as u64);
            meta.cols[0].index_off = stream.len() as u64;
            meta.cols[0].index_len = index.len() as u64;
            stream.extend_from_slice(&index);
            let meta = meta.encode().unwrap();
            let meta_at = (stream.len() as u64, meta.len() as u64);
            stream.extend_from_slice(&meta);
            let r = store_part(&db, &stream, 1 << 20, 2, meta_at).await;
            let stats = ReadStats::default();
            let rd = Reading { shared, db: &db.id, cache: Caching::Shared, stats: &stats };
            let m = rd.meta(&r).await?;
            rd.column(&r, &m, 0, 0..2).await
        })
    }

    #[test]
    fn valid_index_is_read() {
        let v = read_with_blocks(|l| {
            vec![
                BlockLoc { off: l[0].0, len: l[0].1, first_granule: 0 },
                BlockLoc { off: l[1].0, len: l[1].1, first_granule: 1 },
            ]
        });
        assert_eq!(v.unwrap(), Vector::Int(vec![Some(1), Some(2)]));
    }

    #[test]
    fn column_index_without_blocks_is_rejected() {
        let v = std::panic::catch_unwind(|| read_with_blocks(|_| vec![]));
        assert!(matches!(v, Ok(Err(_))), "{v:?}");
    }

    #[test]
    fn column_index_with_overlapping_blocks_is_rejected() {
        let v = std::panic::catch_unwind(|| {
            read_with_blocks(|l| {
                vec![
                    BlockLoc { off: l[0].0, len: l[0].1 + l[1].1 + 1, first_granule: 0 },
                    BlockLoc { off: l[1].0, len: l[1].1, first_granule: 1 },
                ]
            })
        });
        assert!(matches!(v, Ok(Err(_))), "{v:?}");
    }

    struct Columns {
        plans: Vec<ColPlan>,
        cols: Vec<Vector>,
    }

    impl Source for Columns {
        fn rows(&self) -> u64 {
            self.cols[0].len() as u64
        }

        fn row_bytes(&self, _: u64) -> u64 {
            64
        }

        fn key_at(&self, i: u64) -> Bytes {
            Bytes::copy_from_slice(&i.to_be_bytes())
        }

        fn plans(&self) -> &[ColPlan] {
            &self.plans
        }

        fn shared_paths(&self) -> BTreeMap<u32, Vec<SharedPath>> {
            BTreeMap::from([(4, vec![SharedPath { key: r#"["y"]"#.into(), count: 3, ty: JType::Dyn }])])
        }

        fn partial_paths(&self) -> BTreeSet<u32> {
            BTreeSet::from([4])
        }

        async fn column(&mut self, plan: usize, rows: Range<u64>) -> Result<Vector> {
            Ok(self.cols[plan].slice(rows.start as usize..rows.end as usize))
        }
    }

    fn sample_columns(r: &mut Rng, n: usize) -> Columns {
        let words: Vec<String> = (0..5).map(|i| format!("w{i}é")).collect();
        let pick = |r: &mut Rng| r.chance(0.8).then(|| words[r.usize(words.len())].as_str());
        let plan = |cref, phys, dict, bloom| ColPlan { cref, phys, json: false, dict, bloom };
        let path = ColRef::Path { col: 4, key: r#"["x"]"#.into() };
        Columns {
            plans: vec![
                plan(ColRef::Ordinal, Phys::Int, None, false),
                plan(ColRef::Version, Phys::Int, None, false),
                plan(ColRef::User(1), Phys::Float, None, true),
                plan(ColRef::User(2), Phys::Bool, None, false),
                plan(ColRef::User(3), Phys::Str, Some(Arc::new(words.clone())), true),
                ColPlan { cref: path, phys: Phys::Str, json: true, dict: None, bloom: false },
            ],
            cols: vec![
                Vector::Int((0..n).map(|i| r.chance(0.9).then_some(i as i64 * 3)).collect()),
                Vector::Int(vec![Some(7); n]),
                Vector::Float((0..n).map(|_| r.chance(0.8).then(|| r.unit())).collect()),
                Vector::Bool((0..n).map(|_| r.chance(0.8).then(|| r.chance(0.5))).collect()),
                Vector::Str(Strs::from_values((0..n).map(|_| pick(r)))),
                Vector::Str(Strs::from_values((0..n).map(|_| pick(r)))),
            ],
        }
    }

    fn edge(r: &mut Rng, near: u64) -> u64 {
        match r.usize(6) {
            0 => 0,
            1 => u64::MAX - r.below(4),
            2 => near.saturating_sub(r.below(4)),
            3 => near.saturating_add(r.below(4)),
            4 => r.below(near.max(1).saturating_mul(2)),
            _ => r.next_u64(),
        }
    }

    fn mutate_bytes(r: &mut Rng, b: &mut Vec<u8>) {
        for _ in 0..1 + r.usize(4) {
            let at = r.usize(b.len() + 1);
            let byte = r.next_u32() as u8;
            match r.usize(4) {
                0 if at < b.len() => b[at] = *r.pick(&[0, 1, 0x7f, 0x80, 0xff, byte]),
                1 => b.insert(at, byte),
                2 if at < b.len() => drop(b.remove(at)),
                _ => b.truncate(at),
            }
        }
    }

    fn mutate_meta(r: &mut Rng, m: &mut PartMeta, size: u64) {
        let n = m.granules.len();
        let c = r.usize(m.cols.len());
        match r.usize(8) {
            0 => m.granules[r.usize(n)] = edge(r, 16) as u32,
            1 => {
                let (a, b) = (r.usize(n), r.usize(n));
                let k = r.below(m.granules[a] as u64) as u32;
                m.granules[a] -= k;
                m.granules[b] += k;
            }
            2 if r.chance(0.5) => drop(m.granules.pop()),
            2 => m.granules.push(16),
            3 => m.pk[r.usize(n + 1)] = Bytes::from(vec![7; PK_KEY_MAX + r.usize(2)]),
            4 => {
                let col = &mut m.cols[c];
                let field = [&mut col.index_off, &mut col.index_len, &mut col.data_off, &mut col.data_len];
                let f = r.usize(4);
                *field[f] = edge(r, size);
            }
            5 => m.cols[c].phys = *r.pick(&[Phys::Int, Phys::Float, Phys::Bool, Phys::Str]),
            6 => m.cols[c].nonnull = edge(r, m.rows),
            _ if r.chance(0.5) => drop(m.cols.pop()),
            _ => m.cols.push(m.cols[c].clone()),
        }
    }

    fn mutate_index(r: &mut Rng, idx: &mut ColIndex, size: u64) {
        let (nb, g) = (idx.blocks.len(), r.usize(idx.stats.len()));
        let b = r.usize(nb);
        match r.usize(8) {
            0 => idx.blocks.clear(),
            1 => drop(idx.blocks.remove(b)),
            2 => idx.blocks.swap(b, r.usize(nb)),
            3 => idx.blocks.insert(b, idx.blocks[b]),
            4 => idx.blocks[b].off = edge(r, size),
            5 => idx.blocks[b].len = edge(r, size),
            6 => idx.blocks[b].first_granule = edge(r, 4) as u32,
            _ => match r.usize(7) {
                0 => drop(idx.stats.pop()),
                1 => idx.stats.push(idx.stats[0].clone()),
                2 => drop(idx.blooms.pop()),
                3 => idx.blooms.push(Bloom::new(1, 10)),
                4 => idx.stats[g].nulls = edge(r, 16) as u32,
                5 => idx.dict = None,
                _ => idx.dict = Some(Arc::new(vec!["z".into()])),
            },
        }
    }

    fn corrupt(
        r: &mut Rng,
        stream: &[u8],
        meta: &PartMeta,
        meta_at: (u64, u64),
        indexes: &[ColIndex],
    ) -> (Vec<u8>, u64, (u64, u64)) {
        let mut s = stream.to_vec();
        let size = s.len() as u64;
        let mut m = meta.clone();
        let ci = r.usize(m.cols.len());
        let phys = m.cols[ci].phys;
        let mut idx = indexes[ci].clone();
        let mut index = None;
        let mut meta_bytes = None;
        let mut reference = None;
        match r.usize(6) {
            0 => mutate_meta(r, &mut m, size),
            1 => {
                mutate_index(r, &mut idx, size);
                index = Some(idx.encode(phys).unwrap());
            }
            2 => {
                let mut raw = unframe(&Bytes::from(idx.encode(phys).unwrap())).unwrap().to_vec();
                mutate_bytes(r, &mut raw);
                index = Some(framed(&raw, Compression::Zstd(1), "").unwrap());
            }
            3 => {
                let mut raw = unframe(&Bytes::from(m.encode().unwrap())).unwrap().to_vec();
                mutate_bytes(r, &mut raw);
                meta_bytes = Some(framed(&raw, Compression::Zstd(1), "").unwrap());
            }
            4 => {
                let bi = r.usize(idx.blocks.len());
                for (i, loc) in idx.blocks.iter_mut().enumerate().skip(bi) {
                    let mut block = s[loc.off as usize..(loc.off + loc.len) as usize].to_vec();
                    if i == bi {
                        let mut raw = unframe(&Bytes::from(block)).unwrap().to_vec();
                        mutate_bytes(r, &mut raw);
                        block = framed(&raw, Compression::Zstd(1), "").unwrap();
                    }
                    (loc.off, loc.len) = (s.len() as u64, block.len() as u64);
                    s.extend_from_slice(&block);
                }
                (m.cols[ci].data_off, m.cols[ci].data_len) = (0, s.len() as u64);
                index = Some(idx.encode(phys).unwrap());
            }
            _ => {
                let mut fields = [meta.rows, meta_at.0, meta_at.1];
                let f = r.usize(3);
                fields[f] = edge(r, fields[f]);
                reference = Some(fields);
            }
        }
        if let Some(index) = index {
            (m.cols[ci].index_off, m.cols[ci].index_len) = (s.len() as u64, index.len() as u64);
            s.extend_from_slice(&index);
        }
        let meta_bytes = meta_bytes.or_else(|| m.encode().ok()).unwrap_or_default();
        let mut at = (s.len() as u64, meta_bytes.len() as u64);
        s.extend_from_slice(&meta_bytes);
        let mut rows = meta.rows;
        if let Some([n, off, len]) = reference {
            (rows, at) = (n, (off, len));
        }
        (s, rows, at)
    }

    #[test]
    fn corrupt_parts_are_rejected_without_panicking() {
        use crate::engine::tests::{engine, rt};
        let rt = rt();
        let (_eng, db) = rt.block_on(async {
            let (eng, _store, _env) = engine(1, crate::engine::EngineConfig::small_for_tests());
            let db = eng.db("d");
            db.open(true).await.unwrap();
            (eng, db)
        });
        let shared = db.shared();
        let mut src = sample_columns(&mut Rng::new(5), 150);
        let (stream, r0) = rt.block_on(async {
            let mut sink = Sink::new(shared, &db.id, new_part_id(shared));
            let w = write(&mut sink, &mut src, 1, vec![]).await.unwrap();
            let mut stream = Vec::new();
            for ci in 0..w.size.div_ceil(w.chunk_bytes) {
                let key = crate::engine::part_chunk_key(&db.id, &sink.id, ci);
                stream.extend_from_slice(&shared.store.get(&key, GetOpts::default()).await.unwrap().data);
            }
            let r0 = store_part(&db, &stream, w.chunk_bytes, w.rows, (w.meta_off, w.meta_len)).await;
            (stream, r0)
        });
        let (meta, _) = inspect(&Bytes::from(stream.clone()), &r0).unwrap();
        assert!(meta.granule_count() > 4 && meta.cols.len() == src.cols.len(), "{meta:?}");
        let indexes: Vec<ColIndex> = meta
            .cols
            .iter()
            .map(|c| {
                let b = Bytes::copy_from_slice(&stream[c.index_off as usize..(c.index_off + c.index_len) as usize]);
                ColIndex::decode(&b, &meta, c).unwrap()
            })
            .collect();
        assert!(indexes.iter().any(|i| i.blocks.len() > 2), "{indexes:?}");
        crate::util::prop::check(
            "corrupt_parts_are_rejected_without_panicking",
            3000,
            |r| r.next_u64(),
            |_| Vec::new(),
            |seed| {
                let mut r = Rng::new(*seed);
                let (s, rows, meta_at) = corrupt(&mut r, &stream, &meta, (r0.meta_off, r0.meta_len), &indexes);
                let chunk_bytes = *r.pick(&[256, 1 << 20]);
                let memo = Memo::default();
                let stats = ReadStats::default();
                let cache = if r.chance(0.5) { Caching::Shared } else { Caching::Private(&memo) };
                let rd = Reading { shared, db: &db.id, cache, stats: &stats };
                rt.block_on(async {
                    let pr = store_part(&db, &s, chunk_bytes, rows, meta_at).await;
                    let _ = inspect(&Bytes::from(s), &pr);
                    let Ok(m) = rd.meta(&pr).await else { return Ok(()) };
                    for ci in 0..m.cols.len() {
                        let n = m.granule_count();
                        let first = r.usize(n);
                        let g = first..first + 1 + r.usize(n - first);
                        if let Ok(v) = rd.column(&pr, &m, ci, g.clone()).await {
                            let want = m.rows_of(g.clone());
                            if v.len() as u64 != want.end - want.start || v.phys() != m.cols[ci].phys {
                                return Err(format!("column {ci} granules {g:?} read {} rows", v.len()));
                            }
                        }
                    }
                    Ok(())
                })
            },
        );
    }

    #[test]
    fn fuzz_decoders() {
        let mut r = Rng::new(31);
        let v = Vector::Int(vec![Some(1), None, Some(3)]);
        let goods = [encode_block(&v, None).unwrap()];
        for i in 0..20_000 {
            let mut d = goods[0].clone();
            if i % 3 == 0 {
                let n = r.usize(64);
                d = r.bytes(n);
            }
            for _ in 0..r.usize(3) {
                if !d.is_empty() {
                    let at = r.usize(d.len());
                    d[at] = r.next_u32() as u8;
                }
            }
            for p in [Phys::Int, Phys::Float, Phys::Bool, Phys::Str] {
                let _ = decode_block(&d, p, Some(&Arc::new(vec!["a".into()])), 3);
                let meta = single_column_part(p, vec![2, 1], 64);
                let _ = ColIndex::decode(&Bytes::from(d.clone()), &meta, &meta.cols[0]);
            }
            let _ = PartMeta::decode(&Bytes::from(d), 3);
        }
    }
}
