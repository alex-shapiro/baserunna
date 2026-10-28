//! The event part format (SPEC §5.12): an immutable, sorted, column-major
//! byte stream stored as fixed-size chunk objects under `part/{id}/`.
//!
//! ```text
//! stream   = [column]* [metadata]
//! column   = [block]* [column index]
//! block    = framed (segment codec + CRC32C) values of one or more whole granules
//! index    = framed dictionary, block locations, per-granule statistics, blooms
//! metadata = framed "BRPT", granule sizes, sort key, primary index, column
//!            directory, shared JSON path directory
//! ```
//!
//! A granule is the unit of pruning and reading: queries skip granules by
//! the primary index (sort key values at granule starts) and by per-granule
//! statistics, then fetch the blocks holding the granules they read. A part
//! reference records the metadata's byte range, so opening a part takes one
//! ranged `GET`; a column's index is fetched only when the column is read.

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
use bytes::Bytes;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

const MAGIC: &[u8; 4] = b"BRPT";
const FORMAT: u8 = 1;
/// Prefix length of string statistics.
pub const STAT_PREFIX: usize = 32;
/// Granules per bloom filter.
pub const BLOOM_GRANULES: u32 = 4;
const BLOOM_BITS: usize = 10;
/// Most distinct strings coded into a part-level dictionary.
pub const DICT_MAX: usize = 65_536;
/// Adjacent byte ranges are fetched together up to this size.
const COALESCE_BYTES: u64 = 1 << 20;

/// What a physical column holds.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ColRef {
    /// A table column by id; for a JSON column, whether the object is present.
    User(u32),
    /// The commit version of each row.
    Version,
    /// Each row's ordinal within its version.
    Ordinal,
    /// One leaf path of a JSON column, by canonical key.
    Path { col: u32, key: String },
    /// A JSON column's other paths hashing to `bucket`, as JSON object text per row.
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
            _ => return Err(DecodeError("bad column ref")),
        })
    }
}

/// A column a part stores.
#[derive(Clone, Debug, PartialEq)]
pub struct ColMeta {
    pub cref: ColRef,
    pub phys: Phys,
    /// Strings hold JSON text.
    pub json: bool,
    pub index_off: u64,
    pub index_len: u64,
    pub data_off: u64,
    pub data_len: u64,
    pub nonnull: u64,
}

/// A JSON path kept in a part's shared buckets, with its count and type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedPath {
    pub key: String,
    pub count: u64,
    pub ty: JType,
}

/// A part's metadata: everything needed to locate its granules and columns.
#[derive(Clone, Debug, PartialEq)]
pub struct PartMeta {
    pub rows: u64,
    /// Rows of each granule.
    pub granules: Vec<u32>,
    /// First row of each granule, and the row count last.
    pub starts: Vec<u64>,
    pub sort_ids: Vec<u32>,
    /// Sort key of the first row of each granule, and of the last row.
    pub pk: Vec<Bytes>,
    pub cols: Vec<ColMeta>,
    /// Shared paths of each JSON column.
    pub shared: BTreeMap<u32, Vec<SharedPath>>,
}

impl PartMeta {
    pub fn find(&self, c: &ColRef) -> Option<usize> {
        self.cols.iter().position(|m| m.cref == *c)
    }

    pub fn rows_of(&self, g: Range<usize>) -> Range<u64> {
        self.starts[g.start]..self.starts[g.end]
    }

    pub fn granule_count(&self) -> usize {
        self.granules.len()
    }

    /// Path columns of JSON column `col`.
    pub fn paths(&self, col: u32) -> impl Iterator<Item = (usize, &str)> {
        self.cols.iter().enumerate().filter_map(move |(i, m)| match &m.cref {
            ColRef::Path { col: c, key } if *c == col => Some((i, key.as_str())),
            _ => None,
        })
    }

    fn encode(&self) -> Vec<u8> {
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
        let mut out = Vec::new();
        frame(&w.buf, Compression::Zstd(1), &mut out);
        out
    }

    pub fn decode(b: &Bytes) -> DResult<PartMeta> {
        let raw = unframe(b)?;
        let mut r = Reader::new(&raw);
        if r.take(4)? != MAGIC || r.u8()? != FORMAT {
            return Err(DecodeError("bad part metadata"));
        }
        let rows = r.varint()?;
        let n = r.len(r.remaining())?;
        let mut granules = Vec::with_capacity(n);
        let mut starts = Vec::with_capacity(n + 1);
        let mut at = 0u64;
        for _ in 0..n {
            let g = u32::try_from(r.varint()?).map_err(|_| DecodeError("bad granule"))?;
            if g == 0 {
                return Err(DecodeError("empty granule"));
            }
            starts.push(at);
            at += g as u64;
            granules.push(g);
        }
        starts.push(at);
        if at != rows || rows == 0 {
            return Err(DecodeError("granules do not cover the part"));
        }
        let ns = r.len(8)?;
        let mut sort_ids = Vec::with_capacity(ns);
        for _ in 0..ns {
            sort_ids.push(u32::try_from(r.varint()?).map_err(|_| DecodeError("bad sort id"))?);
        }
        let mut pk = Vec::with_capacity(n + 1);
        for _ in 0..=n {
            pk.push(Bytes::copy_from_slice(r.bytes()?));
        }
        let nc = r.len(r.remaining())?;
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
            cols.push(ColMeta { cref, phys, json, index_off, index_len, data_off, data_len, nonnull });
        }
        let nj = r.len(r.remaining())?;
        let mut shared = BTreeMap::new();
        for _ in 0..nj {
            let col = u32::try_from(r.varint()?).map_err(|_| DecodeError("bad column id"))?;
            let np = r.len(r.remaining())?;
            let mut paths = Vec::with_capacity(np.min(4096));
            for _ in 0..np {
                let key = r.string()?;
                let count = r.varint()?;
                let ty = JType::from_tag(r.u8()?).ok_or(DecodeError("bad path type"))?;
                paths.push(SharedPath { key, count, ty });
            }
            shared.insert(col, paths);
        }
        if !r.is_empty() {
            return Err(DecodeError("part metadata trailing data"));
        }
        Ok(PartMeta { rows, granules, starts, sort_ids, pk, cols, shared })
    }
}

/// A statistic value: min or max of a granule.
#[derive(Clone, Debug, PartialEq)]
pub enum Stat {
    Int(i64),
    Float(f64),
    Bool(bool),
    /// The value's prefix of at most [`STAT_PREFIX`] bytes.
    Str(String),
}

/// Statistics of one column in one granule.
#[derive(Clone, Debug, PartialEq)]
pub struct GranStat {
    pub nulls: u32,
    /// `None` when every value is null.
    pub min: Option<Stat>,
    pub max: Option<Stat>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockLoc {
    pub off: u64,
    pub len: u64,
    pub first_granule: u32,
}

/// A column's index: its dictionary, block locations, statistics and blooms.
#[derive(Clone, Debug, PartialEq)]
pub struct ColIndex {
    pub dict: Option<Arc<Vec<String>>>,
    pub blocks: Vec<BlockLoc>,
    pub stats: Vec<GranStat>,
    /// One filter per [`BLOOM_GRANULES`] granules.
    pub blooms: Vec<Bloom>,
}

impl ColIndex {
    pub fn block_of(&self, granule: usize) -> usize {
        self.blocks.partition_point(|b| b.first_granule as usize <= granule).saturating_sub(1)
    }

    fn mem_size(&self) -> usize {
        self.dict.as_ref().map_or(0, |d| d.iter().map(|s| s.len() + 24).sum())
            + self.blocks.len() * 24
            + self.stats.len() * 80
            + self.blooms.iter().map(Bloom::size_bytes).sum::<usize>()
    }

    fn encode(&self, phys: Phys) -> Vec<u8> {
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
        let mut out = Vec::new();
        frame(&w.buf, Compression::Zstd(1), &mut out);
        out
    }

    pub fn decode(b: &Bytes, phys: Phys) -> DResult<ColIndex> {
        let raw = unframe(b)?;
        let mut r = Reader::new(&raw);
        let dict = match r.u8()? {
            0 => None,
            1 => {
                let n = r.len(DICT_MAX)?;
                let mut d = Vec::with_capacity(n);
                for _ in 0..n {
                    d.push(r.string()?);
                }
                Some(Arc::new(d))
            }
            _ => return Err(DecodeError("bad dictionary tag")),
        };
        if dict.is_some() && phys != Phys::Str {
            return Err(DecodeError("dictionary on a non-string column"));
        }
        let nb = r.len(r.remaining())?;
        let mut blocks = Vec::with_capacity(nb);
        for _ in 0..nb {
            let b = BlockLoc {
                off: r.varint()?,
                len: r.varint()?,
                first_granule: u32::try_from(r.varint()?).map_err(|_| DecodeError("bad block"))?,
            };
            if blocks.last().is_some_and(|p: &BlockLoc| p.first_granule >= b.first_granule) {
                return Err(DecodeError("blocks out of order"));
            }
            blocks.push(b);
        }
        let ns = r.len(r.remaining())?;
        let mut stats = Vec::with_capacity(ns);
        for _ in 0..ns {
            let nulls = u32::try_from(r.varint()?).map_err(|_| DecodeError("bad null count"))?;
            let (min, max) = match r.u8()? {
                0 => (None, None),
                1 => (Some(get_stat(&mut r, phys)?), Some(get_stat(&mut r, phys)?)),
                _ => return Err(DecodeError("bad stat tag")),
            };
            stats.push(GranStat { nulls, min, max });
        }
        let nl = r.len(r.remaining())?;
        let mut blooms = Vec::with_capacity(nl);
        for _ in 0..nl {
            blooms.push(Bloom::decode(&mut r)?);
        }
        if !r.is_empty() {
            return Err(DecodeError("column index trailing data"));
        }
        if blocks.first().is_some_and(|b| b.first_granule != 0) {
            return Err(DecodeError("first block does not start the column"));
        }
        Ok(ColIndex { dict, blocks, stats, blooms })
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
        Phys::Str => Stat::Str(r.string()?),
    })
}

/// The longest prefix of `s` of at most [`STAT_PREFIX`] bytes on a character boundary.
pub fn stat_prefix(s: &str) -> &str {
    let mut end = s.len().min(STAT_PREFIX);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// The hash a bloom filter indexes for a value.
pub fn bloom_hash_int(v: i64) -> u64 {
    crate::util::bloom::key_hash(&[&[b'i'][..], &v.to_le_bytes()].concat())
}

pub fn bloom_hash_float(v: f64) -> u64 {
    let v = if v == 0.0 { 0.0 } else { v };
    crate::util::bloom::key_hash(&[&[b'f'][..], &v.to_bits().to_le_bytes()].concat())
}

pub fn bloom_hash_bool(v: bool) -> u64 {
    crate::util::bloom::key_hash(&[b'b', v as u8])
}

pub fn bloom_hash_str(v: &str) -> u64 {
    crate::util::bloom::key_hash(&[&[b's'][..], v.as_bytes()].concat())
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

/// Encodes the values of a block. Strings are coded with `dict` when the
/// column has a part-level dictionary.
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
                    .map(|x| d.get(x.as_str()).copied().ok_or_else(|| Error::internal("a string is missing from its column's dictionary")))
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

/// Decodes a block's values; `dict` is its column's part-level dictionary.
pub fn decode_block(raw: &[u8], phys: Phys, dict: Option<&Arc<Vec<String>>>) -> DResult<Vector> {
    let mut r = Reader::new(raw);
    let n = r.len(1 << 22)?;
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

/// The approximate uncompressed bytes of `v`.
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

/// How a part being written stores one column.
#[derive(Clone, Debug)]
pub struct ColPlan {
    pub cref: ColRef,
    pub phys: Phys,
    pub json: bool,
    /// A part-level dictionary for a string column, if its values are known up front.
    pub dict: Option<Arc<Vec<String>>>,
    pub bloom: bool,
}

/// Rows of a part being written, supplied a column at a time in row order.
pub trait Source {
    fn rows(&self) -> u64;
    /// Uncompressed bytes of row `i`, which decide granule boundaries.
    fn row_bytes(&self, i: u64) -> u64;
    /// The tuple-encoded sort key of row `i`.
    fn key_at(&self, i: u64) -> Bytes;
    fn plans(&self) -> &[ColPlan];
    fn shared_paths(&self) -> BTreeMap<u32, Vec<SharedPath>>;
    /// The values of column `plan` for `rows`, requested in increasing order.
    fn column(&mut self, plan: usize, rows: Range<u64>) -> impl Future<Output = Result<Vector>> + Send;
}

/// Accumulates a part's stream and uploads it in chunks as they fill.
pub struct Sink<'a> {
    shared: &'a Shared,
    db: &'a str,
    pub id: String,
    chunk_bytes: u64,
    buf: Vec<u8>,
    written: u64,
    /// The whole stream, kept while it is small enough to cache.
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

    /// Uploads every full chunk, and with `all` the rest too.
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

/// What writing a part produced, for its reference.
#[derive(Clone, Debug)]
pub struct Written {
    pub size: u64,
    pub meta_off: u64,
    pub meta_len: u64,
    pub raw_size: u64,
    pub rows: u64,
    pub min_key: Bytes,
    pub max_key: Bytes,
    pub chunk_bytes: u64,
}

/// Splits rows into granules of at most `max_rows` rows and about `max_bytes` bytes.
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

/// Writes the part `src` describes to `sink`, compressing blocks at
/// `level`, and returns its layout. Memory holds one block per column.
pub async fn write(sink: &mut Sink<'_>, src: &mut impl Source, level: i32, sort_ids: Vec<u32>) -> Result<Written> {
    let cfg = &sink.shared.cfg;
    let rows = src.rows();
    if rows == 0 {
        return Err(Error::internal("an event part needs rows"));
    }
    let granules = granules(src, cfg.event_granule_rows, cfg.event_granule_bytes);
    let mut starts = vec![0u64];
    for g in &granules {
        starts.push(starts.last().copied().unwrap_or(0) + *g as u64);
    }
    let mut pk: Vec<Bytes> = starts[..granules.len()].iter().map(|s| src.key_at(*s)).collect();
    pk.push(src.key_at(rows - 1));
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
        let mut pending: Vec<Vector> = Vec::new();
        let mut pending_bytes = 0usize;
        let mut first = 0u32;
        for (g, gr) in granules.iter().enumerate() {
            let v = src.column(pi, starts[g]..starts[g] + *gr as u64).await?;
            if v.len() != *gr as usize || v.phys() != plan.phys {
                return Err(Error::internal("a part source returned the wrong column"));
            }
            stats.push(granule_stat(&v));
            nonnull += (v.len() - v.nulls()) as u64;
            if plan.bloom {
                bloom_hashes(&v, &mut hashes);
                if (g as u32 + 1) % BLOOM_GRANULES == 0 || g + 1 == granules.len() {
                    let mut b = Bloom::new(hashes.len(), BLOOM_BITS);
                    hashes.iter().for_each(|h| b.insert_hash(*h));
                    blooms.push(b);
                    hashes.clear();
                }
            }
            pending_bytes += vector_bytes(&v);
            pending.push(v);
            if pending_bytes >= block_bytes || g + 1 == granules.len() {
                let block = Vector::concat(std::mem::take(&mut pending), plan.phys);
                let raw = encode_block(&block, dict_map.as_ref())?;
                let off = sink.pos();
                let mut framed = Vec::with_capacity(raw.len() + 16);
                frame(&raw, Compression::Zstd(level), &mut framed);
                sink.write(&framed);
                blocks.push(BlockLoc { off, len: framed.len() as u64, first_granule: first });
                first = g as u32 + 1;
                pending_bytes = 0;
                sink.flush(false).await?;
            }
        }
        let data_len = sink.pos() - data_off;
        let index = ColIndex { dict: plan.dict.clone(), blocks, stats, blooms }.encode(plan.phys);
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
    let meta = PartMeta { rows, granules, starts, sort_ids, pk: pk.clone(), cols, shared: src.shared_paths() };
    let encoded = meta.encode();
    let meta_off = sink.pos();
    sink.write(&encoded);
    let size = sink.pos();
    sink.flush(true).await?;
    if let Some(whole) = sink.whole.take() {
        let whole = Bytes::from(whole);
        let n = whole.len();
        sink.shared.cache.insert(CacheKey::part(sink.db, &sink.id, WHOLE, 0), CacheVal::Any(Arc::new(whole), n));
    }
    Ok(Written {
        size,
        meta_off,
        meta_len: encoded.len() as u64,
        raw_size,
        rows,
        min_key: pk[0].clone(),
        max_key: pk[pk.len() - 1].clone(),
        chunk_bytes: sink.chunk_bytes,
    })
}

const META: u32 = u32::MAX;
const WHOLE: u32 = u32::MAX - 1;
const MASK: u32 = u32::MAX - 2;
const INDEX: u32 = u32::MAX;

/// Counters of the work a read did.
#[derive(Debug, Default)]
pub struct ReadStats {
    pub gets: AtomicU64,
    pub bytes: AtomicU64,
}

/// How reads of parts use the caches.
pub struct Reading<'a> {
    pub shared: &'a Shared,
    pub db: &'a str,
    /// Populate caches with what is fetched; merges read without polluting them.
    pub cache: bool,
    pub stats: &'a ReadStats,
}

impl Reading<'_> {
    fn cached<T: Send + Sync + 'static>(&self, k: &CacheKey) -> Option<Arc<T>> {
        let v = if self.cache { self.shared.cache.get(k) } else { self.shared.cache.peek(k) };
        match v {
            Some(CacheVal::Any(a, _)) => a.downcast::<T>().ok(),
            _ => None,
        }
    }

    fn remember<T: Send + Sync + 'static>(&self, k: CacheKey, v: Arc<T>, size: usize) {
        if self.cache {
            self.shared.cache.insert(k, CacheVal::Any(v, size));
        }
    }

    async fn get(&self, key: &str, range: Option<Range<u64>>) -> Result<Bytes> {
        let g = self
            .shared
            .retry
            .run(|| self.shared.store.get(key, GetOpts { range: range.clone(), ..Default::default() }))
            .await
            .map_err(|e| match e {
                StoreError::NotFound => {
                    Error::unavailable("garbage collection removed an event part this read needed; retry")
                }
                other => other.into(),
            })?;
        self.stats.gets.fetch_add(1, Relaxed);
        self.stats.bytes.fetch_add(g.data.len() as u64, Relaxed);
        if let Some(r) = range
            && g.data.len() as u64 != r.end - r.start
        {
            return Err(Error::internal("short event part read"));
        }
        Ok(g.data)
    }

    /// Reads `range` of part `r`'s stream.
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

    /// The whole stream of a small part, fetched with one `GET` per chunk.
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
        let b = self.read(r, r.meta_off..r.meta_off + r.meta_len).await?;
        let m = Arc::new(PartMeta::decode(&b)?);
        if m.rows != r.rows {
            return Err(Error::internal("event part rows do not match its reference"));
        }
        let size = m.cols.len() * 96 + m.pk.iter().map(|k| k.len() + 16).sum::<usize>() + m.granules.len() * 12;
        self.remember(k, m.clone(), size);
        Ok(m)
    }

    pub async fn index(&self, r: &PartRef, meta: &PartMeta, ci: usize) -> Result<Arc<ColIndex>> {
        let k = CacheKey::part(self.db, &r.id, ci as u32, INDEX);
        if let Some(i) = self.cached::<ColIndex>(&k) {
            return Ok(i);
        }
        let c = &meta.cols[ci];
        let b = self.read(r, c.index_off..c.index_off + c.index_len).await?;
        let idx = ColIndex::decode(&b, c.phys)?;
        if idx.stats.len() != meta.granules.len()
            || idx.blocks.iter().any(|b| b.first_granule as usize >= meta.granules.len())
        {
            return Err(Error::internal("event column index does not match its part"));
        }
        let idx = Arc::new(idx);
        self.remember(k, idx.clone(), idx.mem_size());
        Ok(idx)
    }

    /// The values of column `ci` for granules `g`.
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
                let v = Arc::new(decode_block(&raw, c.phys, idx.dict.as_ref())?);
                let first = loc.first_granule as usize;
                let last = idx.blocks.get(bi + 1).map_or(meta.granules.len(), |n| n.first_granule as usize);
                if v.len() as u64 != meta.starts[last] - meta.starts[first] {
                    return Err(Error::internal("event block does not match its granules"));
                }
                self.remember(CacheKey::part(self.db, &r.id, ci as u32, bi as u32), v.clone(), raw.len() + 64);
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

    /// The rows of part `r` its mask deletes.
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

/// Decodes a whole part stream and returns its metadata and the position
/// of every row, checking each column's index and blocks; used by storage
/// invariant checks.
pub fn inspect(
    stream: &Bytes,
    r: &PartRef,
) -> std::result::Result<(PartMeta, Vec<crate::engine::events::Pos>), String> {
    if stream.len() as u64 != r.size {
        return Err(format!("part {} has {} bytes, its reference says {}", r.id, stream.len(), r.size));
    }
    let slice = |off: u64, len: u64| -> std::result::Result<Bytes, String> {
        let end = off.checked_add(len).filter(|e| *e <= r.size).ok_or("range out of bounds")?;
        Ok(stream.slice(off as usize..end as usize))
    };
    let meta = PartMeta::decode(&slice(r.meta_off, r.meta_len)?).map_err(|e| e.to_string())?;
    if meta.rows != r.rows || meta.pk.first() != Some(&r.min_key) || meta.pk.last() != Some(&r.max_key) {
        return Err(format!("part {} metadata does not match its reference", r.id));
    }
    let mut columns: BTreeMap<ColRef, Vector> = BTreeMap::new();
    for c in &meta.cols {
        let idx = ColIndex::decode(&slice(c.index_off, c.index_len)?, c.phys).map_err(|e| e.to_string())?;
        let mut parts = Vec::new();
        for b in &idx.blocks {
            let raw = unframe(&slice(b.off, b.len)?).map_err(|e| e.to_string())?;
            parts.push(decode_block(&raw, c.phys, idx.dict.as_ref()).map_err(|e| e.to_string())?);
        }
        let v = Vector::concat(parts, c.phys);
        if v.len() as u64 != meta.rows || (v.len() - v.nulls()) as u64 != c.nonnull {
            return Err(format!("part {} column {:?} does not have its rows", r.id, c.cref));
        }
        columns.insert(c.cref.clone(), v);
    }
    let ints = |c: &ColRef| match columns.get(c) {
        Some(Vector::Int(x)) => Some(x.clone()),
        _ => None,
    };
    let ords = ints(&ColRef::Ordinal).ok_or_else(|| format!("part {} has no ordinals", r.id))?;
    let vers = match (r.version, ints(&ColRef::Version)) {
        (Some(v), None) => vec![Some(v as i64); ords.len()],
        (None, Some(v)) => v,
        _ => return Err(format!("part {} has versions in both its reference and its rows", r.id)),
    };
    let pos = ords
        .iter()
        .zip(&vers)
        .map(|(o, v)| crate::engine::events::Pos { version: v.unwrap_or(0) as u64, ordinal: o.unwrap_or(0) as u32 })
        .collect();
    Ok((meta, pos))
}

/// Writes a mask object and returns its id and size.
pub async fn write_mask(shared: &Shared, db: &str, runs: &Runs) -> Result<(String, u64)> {
    let id = crate::engine::types::make_blob_id(shared.time_ms(), &shared.env.random_bytes(10));
    let data = crate::engine::events::encode_mask_object(runs);
    let key = crate::engine::part_chunk_key(db, &id, 0);
    shared.retry.run(|| shared.store.put(&key, data.clone(), PutMode::Overwrite)).await?;
    Ok((id, data.len() as u64))
}

/// A new part id: a 48-bit creation time and 80 random bits.
pub fn new_part_id(shared: &Shared) -> String {
    crate::engine::types::make_blob_id(shared.time_ms(), &shared.env.random_bytes(10))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::util::rng::Rng;

    #[test]
    fn blocks_roundtrip_every_encoding() {
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
                assert_eq!(decode_block(&encode_block(&v, None).unwrap(), v.phys(), None).unwrap(), v);
            }
            let Vector::Str(s) = &strs else { unreachable!() };
            let back = decode_block(&encode_block(&strs, None).unwrap(), Phys::Str, None).unwrap();
            let Vector::Str(b) = back else { panic!() };
            assert!((0..n).all(|i| b.get(i) == s.get(i)), "plain strings");
            let dict = Arc::new(words.clone());
            let map: BTreeMap<&str, u32> = dict.iter().enumerate().map(|(i, w)| (w.as_str(), i as u32)).collect();
            let back = decode_block(&encode_block(&strs, Some(&map)).unwrap(), Phys::Str, Some(&dict)).unwrap();
            let Vector::Str(b) = back else { panic!() };
            assert!((0..n).all(|i| b.get(i) == s.get(i)), "dictionary strings");
        }
    }

    #[test]
    fn metadata_and_indexes_roundtrip() {
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
        };
        assert_eq!(PartMeta::decode(&Bytes::from(meta.encode())).unwrap(), meta);
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
        assert_eq!(ColIndex::decode(&Bytes::from(idx.encode(Phys::Str)), Phys::Str).unwrap(), idx);
        assert_eq!((idx.block_of(0), idx.block_of(1)), (0, 1));
        assert_eq!(stat_prefix(&"é".repeat(40)).len(), 32);
    }

    #[test]
    fn fuzz_decoders_never_panic() {
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
                let _ = decode_block(&d, p, Some(&Arc::new(vec!["a".into()])));
                let _ = ColIndex::decode(&Bytes::from(d.clone()), p);
            }
            let _ = PartMeta::decode(&Bytes::from(d));
        }
    }
}
