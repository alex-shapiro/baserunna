//! Query execution (SPEC §5.13).
//!
//! A query reads a single snapshot of unsealed rows and part references in
//! the target partitions. Parts and granules are pruned using reference time
//! bounds, column presence, dictionaries, the primary index, min/max
//! statistics, and bloom filters. Filter columns are read first; remaining
//! columns are read only for granules that have matching rows.

use super::agg::{State, Val};
use super::json;
use super::part::{self, Caching, ColRef, GranStat, PartMeta, ReadStats, Reading, Stat};
use super::query::{AggFn, Expr, Filter, Interval, Mode, Named, Query, Stage};
use super::schema::{EvColumn, EvType, EventSchema, NOW, parse_rfc3339};
use super::vector::{Phys, Vector};
use crate::engine::events::{self as ev, PartRef, Pos, Runs};
use crate::engine::{Shared, Txn, Value};
use crate::error::{Error, Result};
use crate::filter::{self, Plain, Target, Test};
use crate::probe::{self, Probe};
use crate::table::schema::{Datum, Row, decode_row};
use crate::util::DetHashMap;
use crate::util::codec::{Reader, Writer};
use baserunna_api::b64;
use futures_util::StreamExt;
use futures_util::stream::FuturesOrdered;
use parking_lot::Mutex;
use serde_json::{Value as Json, json};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

/// Resource limits for a single query.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Maximum compressed bytes read from storage.
    pub max_bytes_read: u64,
    /// Maximum memory in bytes for aggregation state or top rows plus active column batches.
    pub max_memory_bytes: usize,
    /// Maximum memory in bytes for lookup maps.
    pub lookup_max_bytes: usize,
    /// Query timeout in milliseconds before returning a retryable `503`.
    pub max_ms: u64,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits { max_bytes_read: 4 << 30, max_memory_bytes: 1 << 30, lookup_max_bytes: 1 << 30, max_ms: 60_000 }
    }
}

/// Execution statistics returned in the query response.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Stats {
    pub source: String,
    pub nodes: u32,
    pub parts: u64,
    pub parts_read: u64,
    pub granules: u64,
    pub granules_read: u64,
    pub rows_read: u64,
    pub unsealed_rows: u64,
    pub bytes_read: u64,
    pub store_gets: u64,
}

impl Stats {
    fn add(&mut self, o: &Stats) {
        self.parts_read += o.parts_read;
        self.granules += o.granules;
        self.granules_read += o.granules_read;
        self.rows_read += o.rows_read;
        self.unsealed_rows += o.unsealed_rows;
    }
}

/// Query result and execution statistics.
#[derive(Clone, Debug, PartialEq)]
pub struct Output {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Json>>,
    pub next_cursor: Option<String>,
    pub stats: Stats,
}

/// Expression bound to table column IDs or first-stage output indices.
#[derive(Clone, Debug, PartialEq)]
pub enum BExpr {
    Col(u32),
    Path(u32, Vec<String>, String),
    Json(u32),
    Lookup(usize),
    Bucket(Box<BExpr>, Interval, i64),
    /// First-stage output index used in the second stage.
    Out(usize),
}

type BFilter = filter::Filter<BExpr, Json>;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Need {
    Col(u32),
    Path(u32, String),
    Json(u32),
    Lookup(usize),
}

struct Lookup {
    key: BExpr,
    default: Option<BExpr>,
    map: Arc<LookupMap>,
}

/// Maps encoded primary keys to column values in a flat byte buffer.
///
/// Entries are looked up by binary search over `sorted` rather than hashing
/// to prevent hash-collision denial-of-service attacks.
#[derive(Default)]
struct LookupMap {
    data: Vec<u8>,
    starts: Vec<u32>,
    /// Entry indices sorted by canonical key bytes, with ties ordered by insertion.
    sorted: Vec<u32>,
}

impl LookupMap {
    fn mem(&self) -> usize {
        self.data.len() + 4 * (self.starts.len() + self.sorted.len())
    }

    fn push(&mut self, key: &[u8], v: &Val) -> Result<()> {
        let start = u32::try_from(self.data.len()).map_err(|_| Error::TooLarge("a lookup map exceeds 4 GiB".into()))?;
        let mut w = Writer::new();
        w.bytes(key);
        v.encode(&mut w);
        self.data.extend_from_slice(&w.into_inner());
        self.starts.push(start);
        Ok(())
    }

    fn entry(&self, e: u32) -> Reader<'_> {
        Reader::new(&self.data[self.starts[e as usize] as usize..])
    }

    fn key(&self, e: u32) -> &[u8] {
        self.entry(e).bytes().unwrap_or_default()
    }

    fn index_mem(&self) -> usize {
        4 * self.starts.len()
    }

    fn index(&mut self) {
        let mut sorted: Vec<u32> = (0..self.starts.len() as u32).collect();
        sorted.sort_by(|a, b| self.key(*a).cmp(self.key(*b)));
        self.sorted = sorted;
    }

    fn get(&self, key: &[u8]) -> Option<Val> {
        let after = self.sorted.partition_point(|e| self.key(*e) <= key);
        let e = *self.sorted.get(after.checked_sub(1)?)?;
        let mut r = self.entry(e);
        (r.bytes().ok()? == key).then(|| Val::decode(&mut r, 0).ok()).flatten()
    }
}

struct Binder<'a> {
    schema: &'a EventSchema,
    lookups: &'a BTreeMap<String, usize>,
}

impl Binder<'_> {
    fn expr(&self, e: &Expr) -> Result<BExpr> {
        Ok(match e {
            Expr::Col { name, path } => {
                let c = self
                    .schema
                    .column(name)
                    .ok_or_else(|| Error::invalid(format!("column {name:?} does not exist")))?;
                match (c.ty, path.is_empty()) {
                    (EvType::Json, true) => BExpr::Json(c.id),
                    (EvType::Json, false) => BExpr::Path(c.id, path.clone(), json::path_key(path)),
                    (_, true) => BExpr::Col(c.id),
                    (_, false) => {
                        return Err(Error::invalid(format!("column {name:?} is not json and does not take a path")));
                    }
                }
            }
            Expr::Lookup(n) => BExpr::Lookup(
                *self.lookups.get(n).ok_or_else(|| Error::invalid(format!("lookup {n:?} is not declared")))?,
            ),
            Expr::Bucket { of, interval, offset_ms } => {
                let inner = self.expr(of)?;
                match &inner {
                    BExpr::Col(id)
                        if matches!(self.schema.column_by_id(*id).map(|c| c.ty), Some(EvType::Time | EvType::Int)) => {}
                    _ => return Err(Error::invalid("buckets take a time or int column")),
                }
                BExpr::Bucket(Box::new(inner), *interval, *offset_ms)
            }
        })
    }

    fn filter(&self, f: &Filter) -> Result<BFilter> {
        f.bind(&mut |e: &Expr, t: &Test<Json>| {
            let b = self.expr(e)?;
            let col = match &b {
                BExpr::Bucket(inner, ..) => inner.as_ref(),
                other => other,
            };
            let t = match col {
                BExpr::Col(id) => {
                    let c = self.schema.column_by_id(*id).ok_or_else(|| Error::internal("bound column missing"))?;
                    if matches!(t, Test::Prefix(_) | Test::Contains(_)) && c.ty != EvType::String {
                        return Err(Error::invalid(format!("only string columns take {}", t.name())));
                    }
                    t.try_map(|a| typed_arg(c, a))?
                }
                BExpr::Json(_) => {
                    return Err(Error::invalid(
                        "filter on a JSON column's path, such as {\"col\": .., \"path\": [..]}",
                    ));
                }
                _ => t.try_map(|a| {
                    let mut a = a.clone();
                    json::normalize(&mut a).map_err(Error::invalid)?;
                    Ok::<_, Error>(a)
                })?,
            };
            Ok((b, t))
        })
    }
}

fn typed_arg(c: &EvColumn, a: &Json) -> Result<Json> {
    let bad = || Error::invalid(format!("column {:?} compares with {} values, not {a}", c.name, c.ty.name()));
    Ok(match c.ty {
        EvType::Time => match a {
            Json::String(s) => json!(parse_rfc3339(s).ok_or_else(bad)?),
            _ => json!(a.as_i64().ok_or_else(bad)?),
        },
        EvType::Int => json!(a.as_i64().ok_or_else(bad)?),
        // Preserve 64-bit integer precision so exact integer literals can be compared with float columns.
        EvType::Float if a.is_i64() => a.clone(),
        EvType::Float if a.is_u64() => {
            return Err(Error::invalid(format!("{a} exceeds the signed 64-bit integer range")));
        }
        EvType::Float => json!(a.as_f64().filter(|f| f.is_finite()).ok_or_else(bad)? + 0.0),
        EvType::String => json!(a.as_str().ok_or_else(bad)?),
        EvType::Bool => json!(a.as_bool().ok_or_else(bad)?),
        EvType::Json => a.clone(),
    })
}

fn num_order(a: &Val, b: &Json) -> Option<Ordering> {
    let b = match b.as_i64() {
        Some(i) => Val::Int(i),
        None => Val::Float(b.as_f64()?),
    };
    a.num_cmp(&b)
}

impl Target<Json> for Val {
    fn equals(&self, arg: &Json) -> bool {
        match self {
            Val::Int(_) | Val::Float(_) => {
                matches!(arg, Json::Number(_)) && num_order(self, arg) == Some(Ordering::Equal)
            }
            Val::Str(s) => arg.as_str() == Some(s),
            Val::Bool(b) => arg.as_bool() == Some(*b),
            Val::Json(j) => (&j).equals(arg),
            Val::List(_) | Val::Null => false,
        }
    }

    fn eq_hash(&self) -> Option<u64> {
        Some(
            match self {
                Val::Int(i) => Plain::Int(i128::from(*i)),
                Val::Float(f) => Plain::Float(*f),
                Val::Str(s) => Plain::Str(s),
                Val::Bool(b) => Plain::Bool(*b),
                Val::Json(j) => Plain::Json(j),
                Val::List(_) | Val::Null => Plain::Null,
            }
            .eq_hash(),
        )
    }

    fn order(&self, arg: &Json) -> Option<Ordering> {
        match self {
            Val::Int(_) | Val::Float(_) if arg.is_number() => num_order(self, arg),
            Val::Str(s) => Some(s.as_str().cmp(arg.as_str()?)),
            Val::Bool(b) => Some(b.cmp(&arg.as_bool()?)),
            _ => None,
        }
    }

    fn starts_with(&self, arg: &Json) -> bool {
        matches!((self, arg), (Val::Str(s), Json::String(p)) if s.starts_with(p.as_str()))
    }

    fn contains(&self, arg: &Json) -> bool {
        match self {
            Val::Str(s) => arg.as_str().is_some_and(|p| s.contains(p)),
            Val::Json(j) => (&j).contains(arg),
            _ => false,
        }
    }
}

pub(crate) fn datum_val(d: &Datum) -> Val {
    match d {
        Datum::Null => Val::Null,
        Datum::Bool(b) => Val::Bool(*b),
        Datum::Int(i) => Val::Int(*i),
        Datum::Float(f) => Val::Float(*f),
        Datum::Str(s) => Val::Str(s.clone()),
        Datum::Json(j) => Val::from_json(j),
        Datum::Bytes(b) => Val::Str(b64::encode(b)),
        Datum::Blob(_) => Val::Null,
    }
}

/// Returns the default value for a column if a row or part predates the column.
fn default_val(c: Option<&EvColumn>) -> Val {
    match c.and_then(|c| c.default.as_ref().map(|d| (c, d))) {
        Some((c, Json::String(s))) if c.ty == EvType::Time && s == NOW => Val::Null,
        Some((c, d)) => super::schema::datum(c, d).map(|d| datum_val(&d)).unwrap_or(Val::Null),
        None => Val::Null,
    }
}

#[derive(Clone, Debug)]
enum ColData {
    Vec { v: Vector, json: bool },
    Vals(Vec<Val>),
    Filled { vals: Vec<Option<Val>>, default: Val },
    Const(Val),
}

impl ColData {
    fn get(&self, i: usize) -> Val {
        match self {
            ColData::Const(v) => v.clone(),
            ColData::Vals(v) => v[i].clone(),
            ColData::Filled { vals, default } => vals[i].as_ref().unwrap_or(default).clone(),
            ColData::Vec { v, json } => match v {
                Vector::Int(x) => x[i].map_or(Val::Null, Val::Int),
                Vector::Float(x) => x[i].map_or(Val::Null, Val::Float),
                Vector::Bool(x) => x[i].map_or(Val::Null, Val::Bool),
                Vector::Str(s) => match s.get(i) {
                    None => Val::Null,
                    Some(t) if *json => {
                        serde_json::from_str::<Json>(t).map(|j| Val::from_json(&j)).unwrap_or(Val::Null)
                    }
                    Some(t) => Val::Str(t.to_string()),
                },
            },
        }
    }
}

enum Derive {
    UnsealedCol(Val),
    UnsealedPath(Vec<String>),
    UnsealedJson,
    Shared(usize),
    Object { present: Vector, paths: Vec<(Vec<String>, ColData)>, buckets: Vec<usize> },
    Lookup,
}

/// Columnar row batch for a granule run or unsealed rows.
///
/// Stored columns are loaded for the entire batch. Derived values such as
/// unsealed columns, shared JSON buckets, reconstructed JSON objects, and
/// lookups are materialized per row window in [`Batch::enter`] to bound peak
/// memory.
struct Batch<'r> {
    n: usize,
    pos: Vec<Pos>,
    rows: &'r [(Pos, Row)],
    cols: BTreeMap<Need, ColData>,
    derived: BTreeMap<Need, Derive>,
    buckets: Vec<((u32, u8), Vector)>,
    window: std::ops::Range<usize>,
    window_cols: BTreeMap<Need, ColData>,
    mem: usize,
    /// Per-comparison results cached by dictionary code across windows.
    memos: Mutex<DetHashMap<usize, Vec<Option<bool>>>>,
}

impl<'r> Batch<'r> {
    fn new(n: usize, pos: Vec<Pos>) -> Batch<'r> {
        Batch {
            n,
            pos,
            rows: &[],
            cols: BTreeMap::new(),
            derived: BTreeMap::new(),
            buckets: Vec::new(),
            window: 0..0,
            window_cols: BTreeMap::new(),
            mem: 0,
            memos: Mutex::default(),
        }
    }

    fn unsealed(rows: &'r [(Pos, Row)], needs: impl IntoIterator<Item = Need>, schema: &EventSchema) -> Batch<'r> {
        let mut b = Batch { rows, ..Batch::new(rows.len(), rows.iter().map(|(p, _)| *p).collect()) };
        for need in needs {
            let d = match &need {
                Need::Col(id) => Derive::UnsealedCol(default_val(schema.column_by_id(*id))),
                Need::Path(_, key) => Derive::UnsealedPath(json::path_segments(key).unwrap_or_default()),
                Need::Json(_) => Derive::UnsealedJson,
                Need::Lookup(_) => Derive::Lookup,
            };
            b.derived.insert(need, d);
        }
        b
    }

    fn has(&self, need: &Need) -> bool {
        self.cols.contains_key(need) || self.derived.contains_key(need)
    }

    fn put(&mut self, need: Need, v: Vector, json: bool) {
        self.mem += held_bytes(&v);
        self.cols.insert(need, ColData::Vec { v, json });
    }

    fn bucket(&self, col: u32, bucket: u8) -> Option<usize> {
        self.buckets.iter().position(|(k, _)| *k == (col, bucket))
    }

    fn add_bucket(&mut self, col: u32, bucket: u8, v: Vector) -> Result<usize> {
        if v.phys() != Phys::Str {
            return Err(Error::internal("shared bucket is not text"));
        }
        self.mem += held_bytes(&v);
        self.buckets.push(((col, bucket), v));
        Ok(self.buckets.len() - 1)
    }

    /// Materializes derived values for row range `w`. Lookups are evaluated last
    /// in declaration order because `Need::Lookup` sorts after other `Need` variants.
    fn enter(&mut self, w: std::ops::Range<usize>, lookups: &[Lookup]) {
        self.window = w.clone();
        let parsed: Vec<Vec<Option<Object>>> = self.buckets.iter().map(|(_, v)| parse_objects(v, w.clone())).collect();
        let rows = &self.rows[w.start.min(self.rows.len())..w.end.min(self.rows.len())];
        let derived = std::mem::take(&mut self.derived);
        for (need, data) in self.window_cols.iter_mut() {
            if matches!(need, Need::Lookup(_)) {
                *data = ColData::Const(Val::Null);
            }
        }
        for (need, d) in &derived {
            let data = match (need, d) {
                (Need::Col(id), Derive::UnsealedCol(default)) => ColData::Filled {
                    vals: rows.iter().map(|(_, r)| r.get(id).map(datum_val)).collect(),
                    default: default.clone(),
                },
                (Need::Path(id, _), Derive::UnsealedPath(segs)) => ColData::Vals(
                    rows.iter()
                        .map(|(_, r)| match r.get(id) {
                            Some(Datum::Json(j)) => json::leaf(j, segs).map_or(Val::Null, Val::from_json),
                            _ => Val::Null,
                        })
                        .collect(),
                ),
                (Need::Json(id), Derive::UnsealedJson) => {
                    ColData::Vals(rows.iter().map(|(_, r)| r.get(id).map_or(Val::Null, datum_val)).collect())
                }
                (Need::Path(_, key), Derive::Shared(bi)) => ColData::Vals(
                    parsed[*bi]
                        .iter()
                        .map(|o| o.as_ref().and_then(|o| o.get(key)).map_or(Val::Null, Val::from_json))
                        .collect(),
                ),
                (_, Derive::Object { present, paths, buckets }) => ColData::Vals(
                    w.clone()
                        .map(|i| {
                            if present.is_null(i) {
                                return Val::Null;
                            }
                            let mut leaves = Vec::new();
                            for (segs, data) in paths {
                                let v = data.get(i);
                                if !v.is_null() {
                                    leaves.push((segs.clone(), v.to_json()));
                                }
                            }
                            for bi in buckets {
                                for (k, v) in parsed[*bi][i - w.start].iter().flat_map(|o| o.iter()) {
                                    leaves.push((json::path_segments(k).unwrap_or_default(), v.clone()));
                                }
                            }
                            Val::Json(Json::Object(json::unflatten(leaves)))
                        })
                        .collect(),
                ),
                (Need::Lookup(k), Derive::Lookup) => {
                    let Some(l) = lookups.get(*k) else { continue };
                    ColData::Vals(
                        w.clone()
                            .map(|i| {
                                let key = self.val(&l.key, i);
                                let found = if key.is_null() { None } else { l.map.get(&key.canonical_bytes()) };
                                match found {
                                    Some(v) => v,
                                    None => l.default.as_ref().map_or(Val::Null, |d| self.val(d, i)),
                                }
                            })
                            .collect(),
                    )
                }
                _ => continue,
            };
            match self.window_cols.get_mut(need) {
                Some(slot) => *slot = data,
                None => {
                    self.window_cols.insert(need.clone(), data);
                }
            }
        }
        self.derived = derived;
    }

    fn val(&self, e: &BExpr, i: usize) -> Val {
        let need = match e {
            BExpr::Col(id) => Need::Col(*id),
            BExpr::Path(id, _, key) => Need::Path(*id, key.clone()),
            BExpr::Json(id) => Need::Json(*id),
            BExpr::Lookup(k) => Need::Lookup(*k),
            BExpr::Bucket(inner, iv, off) => {
                return match self.val(inner, i) {
                    Val::Int(t) => iv.bucket(t, *off).map_or(Val::Null, Val::Int),
                    _ => Val::Null,
                };
            }
            BExpr::Out(_) => return Val::Null,
        };
        match self.cols.get(&need) {
            Some(c) => c.get(i),
            None => self.window_cols.get(&need).map_or(Val::Null, |c| c.get(i - self.window.start)),
        }
    }

    fn select(&self, f: &BFilter, sel: &[bool], start: usize) -> Vec<bool> {
        match f {
            filter::Filter::And(fs) => {
                let mut s = sel.to_vec();
                for x in fs {
                    if !s.contains(&true) {
                        break;
                    }
                    s = self.select(x, &s, start);
                }
                s
            }
            filter::Filter::Or(fs) => {
                let mut out = vec![false; sel.len()];
                for x in fs {
                    let rest: Vec<bool> = sel.iter().zip(&out).map(|(s, o)| *s && !o).collect();
                    if !rest.contains(&true) {
                        break;
                    }
                    for (o, y) in out.iter_mut().zip(self.select(x, &rest, start)) {
                        *o |= y;
                    }
                }
                out
            }
            filter::Filter::Not(x) => {
                let y = self.select(x, sel, start);
                sel.iter().zip(y).map(|(s, y)| *s && !y).collect()
            }
            filter::Filter::Cmp(e, t) => {
                let dict = match e {
                    BExpr::Col(id) => self.cols.get(&Need::Col(*id)),
                    BExpr::Path(id, _, key) => self.cols.get(&Need::Path(*id, key.clone())),
                    _ => None,
                };
                if let Some(ColData::Vec { v: Vector::Str(s), json }) = dict {
                    let mut memos = self.memos.lock();
                    let memo = memos.entry(test_addr(t)).or_insert_with(|| vec![None; s.dict.len()]);
                    return sel
                        .iter()
                        .zip(start..)
                        .map(|(sel, i)| {
                            *sel && {
                                let c = s.codes[i];
                                c != super::vector::NULL_CODE
                                    && *memo[c as usize].get_or_insert_with(|| {
                                        let text = &s.dict[c as usize];
                                        if *json {
                                            serde_json::from_str::<Json>(text)
                                                .is_ok_and(|j| holds(&Val::from_json(&j), t))
                                        } else {
                                            t.holds(&text.as_str())
                                        }
                                    })
                            }
                        })
                        .collect();
                }
                sel.iter()
                    .zip(start..)
                    .map(|(sel, i)| {
                        *sel && {
                            let v = self.val(e, i);
                            !v.is_null() && t.holds(&v)
                        }
                    })
                    .collect()
            }
        }
    }
}

fn test_addr(t: &Test<Json>) -> usize {
    std::ptr::from_ref(t) as usize
}

/// Returns the heap bytes owned exclusively by `v`, excluding dictionaries
/// shared with the part index cache.
fn held_bytes(v: &Vector) -> usize {
    v.values_mem_size()
        + match v {
            Vector::Str(s) if Arc::strong_count(&s.dict) == 1 => super::vector::dict_mem_size(&s.dict),
            _ => 0,
        }
}

type Object = Arc<serde_json::Map<String, Json>>;

fn parse_objects(v: &Vector, w: std::ops::Range<usize>) -> Vec<Option<Object>> {
    let Vector::Str(s) = v else { return vec![None; w.len()] };
    let mut parsed: DetHashMap<u32, Option<Object>> = DetHashMap::default();
    w.map(|i| match s.codes[i] {
        super::vector::NULL_CODE => None,
        c => parsed.entry(c).or_insert_with(|| serde_json::from_str(&s.dict[c as usize]).ok().map(Arc::new)).clone(),
    })
    .collect()
}

fn comparisons<'f>(f: &'f BFilter, out: &mut Vec<(&'f BExpr, &'f Test<Json>)>) {
    match f {
        filter::Filter::And(fs) | filter::Filter::Or(fs) => fs.iter().for_each(|x| comparisons(x, out)),
        filter::Filter::Not(x) => comparisons(x, out),
        filter::Filter::Cmp(e, t) => out.push((e, t)),
    }
}

fn filter_needs(f: &BFilter, out: &mut BTreeSet<Need>, lookups: &[Lookup]) {
    match f {
        filter::Filter::And(fs) | filter::Filter::Or(fs) => fs.iter().for_each(|x| filter_needs(x, out, lookups)),
        filter::Filter::Not(x) => filter_needs(x, out, lookups),
        filter::Filter::Cmp(e, _) => expr_needs(e, out, lookups),
    }
}

fn expr_needs(e: &BExpr, out: &mut BTreeSet<Need>, lookups: &[Lookup]) {
    match e {
        BExpr::Col(id) => {
            out.insert(Need::Col(*id));
        }
        BExpr::Path(id, _, key) => {
            out.insert(Need::Path(*id, key.clone()));
        }
        BExpr::Json(id) => {
            out.insert(Need::Json(*id));
        }
        BExpr::Lookup(k) => {
            if let Some(l) = lookups.get(*k) {
                expr_needs(&l.key, out, lookups);
                if let Some(d) = &l.default {
                    expr_needs(d, out, lookups);
                }
            }
            out.insert(Need::Lookup(*k));
        }
        BExpr::Bucket(inner, ..) => expr_needs(inner, out, lookups),
        BExpr::Out(_) => {}
    }
}

fn may_match(f: &BFilter, oracle: &mut impl FnMut(&BExpr, &Test<Json>) -> bool) -> bool {
    match f {
        filter::Filter::And(fs) => fs.iter().all(|x| may_match(x, oracle)),
        filter::Filter::Or(fs) => fs.iter().any(|x| may_match(x, oracle)),
        filter::Filter::Not(_) => true,
        filter::Filter::Cmp(e, t) => oracle(e, t),
    }
}

/// Returns true if `a` can be less than or equal to a string whose stored
/// maximum prefix is `s`, or strictly less than if `strict` is true.
///
/// If `s` might have been truncated at [`part::STAT_PREFIX`] bytes, only the
/// first `s.len()` bytes of `a` can be compared.
fn is_below_max(a: &str, s: &str, strict: bool) -> bool {
    if s.len() + 4 > part::STAT_PREFIX {
        a.as_bytes()[..a.len().min(s.len())] <= *s.as_bytes()
    } else if strict {
        a < s
    } else {
        a <= s
    }
}

/// Returns true if a value in `[lo, hi]` can satisfy `t`.
fn range_may_match(t: &Test<Json>, lo: &Stat, hi: &Stat) -> bool {
    let le = |a: &Json, s: &Stat| -> bool {
        match (a, s) {
            (Json::String(a), Stat::Str(s)) => is_below_max(a, s, false),
            (Json::Number(_), Stat::Int(_) | Stat::Float(_)) => {
                stat_val(s).total_cmp(&Val::from_json(a)) != Ordering::Less
            }
            (Json::Bool(a), Stat::Bool(s)) => a <= s,
            _ => false,
        }
    };
    let ge = |a: &Json, s: &Stat| -> bool {
        match (a, s) {
            (Json::String(a), Stat::Str(s)) => a.as_str() >= s.as_str(),
            (Json::Number(_), Stat::Int(_) | Stat::Float(_)) => {
                stat_val(s).total_cmp(&Val::from_json(a)) != Ordering::Greater
            }
            (Json::Bool(a), Stat::Bool(s)) => a >= s,
            _ => false,
        }
    };
    let lt_strict = |a: &Json, s: &Stat| -> bool {
        match (a, s) {
            (Json::String(a), Stat::Str(s)) => s.as_str() < a.as_str(),
            (Json::Number(_), Stat::Int(_) | Stat::Float(_)) => {
                stat_val(s).total_cmp(&Val::from_json(a)) == Ordering::Less
            }
            (Json::Bool(a), Stat::Bool(s)) => s < a,
            _ => false,
        }
    };
    let gt_strict = |a: &Json, s: &Stat| -> bool {
        match (a, s) {
            (Json::String(a), Stat::Str(s)) => is_below_max(a, s, true),
            (Json::Number(_), Stat::Int(_) | Stat::Float(_)) => {
                stat_val(s).total_cmp(&Val::from_json(a)) == Ordering::Greater
            }
            (Json::Bool(a), Stat::Bool(s)) => s > a,
            _ => false,
        }
    };
    match t {
        Test::Eq(a) => ge(a, lo) && le(a, hi),
        Test::In(l) => l.items().iter().any(|a| ge(a, lo) && le(a, hi)),
        Test::Lt(a) => lt_strict(a, lo),
        Test::Le(a) => ge(a, lo),
        Test::Gt(a) => gt_strict(a, hi),
        Test::Ge(a) => le(a, hi),
        Test::Prefix(Json::String(p)) => match (lo, hi) {
            (Stat::Str(l), Stat::Str(_)) => {
                (l.as_str() <= p.as_str() || l.starts_with(p.as_str())) && le(&Json::String(p.clone()), hi)
            }
            _ => false,
        },
        Test::Prefix(_) => false,
        Test::Ne(_) | Test::Contains(_) | Test::Exists => true,
    }
}

fn stat_val(s: &Stat) -> Val {
    match s {
        Stat::Int(i) => Val::Int(*i),
        Stat::Float(f) => Val::Float(*f),
        Stat::Bool(b) => Val::Bool(*b),
        Stat::Str(s) => Val::Str(s.clone()),
    }
}

fn key_stat(k: &crate::table::tuple::Key) -> Option<Stat> {
    use crate::table::tuple::Key;
    match k {
        Key::Int(i) => Some(Stat::Int(*i)),
        Key::Float(f) => Some(Stat::Float(*f)),
        Key::Bool(b) => Some(Stat::Bool(*b)),
        Key::Str(s) => Some(Stat::Str(s.clone())),
        _ => None,
    }
}

fn holds(v: &Val, t: &Test<Json>) -> bool {
    !v.is_null() && t.holds(v)
}

fn bloom_hash(phys: Phys, a: &Json) -> Option<u64> {
    Some(match phys {
        Phys::Int => part::bloom_hash_int(a.as_i64()?),
        Phys::Float => part::bloom_hash_float(a.as_f64()?),
        Phys::Bool => part::bloom_hash_bool(a.as_bool()?),
        Phys::Str => part::bloom_hash_str(a.as_str()?),
    })
}

const PACE_WORK: usize = 1 << 16;

struct Plan<'a> {
    schema: &'a EventSchema,
    filter: BFilter,
    lookups: Vec<Lookup>,
    kind: Kind,
    first: BTreeSet<Need>,
    rest: BTreeSet<Need>,
    positions: bool,
    deadline: Option<(tokio::time::Instant, u64)>,
    window: usize,
}

impl<'a> Plan<'a> {
    fn new(
        schema: &'a EventSchema,
        filter: BFilter,
        lookups: Vec<Lookup>,
        kind: Kind,
        deadline: Option<(tokio::time::Instant, u64)>,
    ) -> Plan<'a> {
        let mut first = BTreeSet::new();
        filter_needs(&filter, &mut first, &lookups);
        let mut rest = BTreeSet::new();
        let mut positions = false;
        let mut cmps = Vec::new();
        comparisons(&filter, &mut cmps);
        let exprs = match &kind {
            Kind::Rows { columns, order, .. } => columns.len() + order.len(),
            Kind::Aggregate { first: st, .. } => st.group_by.len() + st.aggregates.len(),
        };
        match &kind {
            Kind::Rows { columns, order, .. } => {
                columns.iter().for_each(|(_, e)| expr_needs(e, &mut rest, &lookups));
                order.iter().for_each(|(e, _)| expr_needs(e, &mut rest, &lookups));
                positions = true;
            }
            Kind::Aggregate { first: st, .. } => {
                st.group_by.iter().for_each(|(_, e)| expr_needs(e, &mut rest, &lookups));
                for a in &st.aggregates {
                    if let Some(e) = &a.of {
                        expr_needs(e, &mut rest, &lookups);
                    }
                    for f in a.filter.iter().chain(&a.conditions) {
                        filter_needs(f, &mut rest, &lookups);
                        comparisons(f, &mut cmps);
                    }
                    if matches!(a.func, AggFn::WindowFunnel { .. }) {
                        rest.insert(Need::Col(schema.time_id()));
                        positions = true;
                    }
                }
            }
        }
        let rest: BTreeSet<Need> = rest.difference(&first).cloned().collect();
        let needs = first.len() + rest.len();
        let window = (PACE_WORK / (1 + cmps.len() + exprs + needs)).max(1);
        Plan { schema, filter, lookups, kind, first, rest, positions, deadline, window }
    }

    async fn pace(&self) -> Result<()> {
        if let Some((at, max_ms)) = self.deadline
            && tokio::time::Instant::now() >= at
        {
            return Err(timed_out(max_ms));
        }
        tokio::task::yield_now().await;
        Ok(())
    }

    fn granule_work(&self) -> usize {
        let mut cmps = Vec::new();
        comparisons(&self.filter, &mut cmps);
        cmps.iter().map(|(_, t)| 1 + if let Test::In(l) = t { l.items().len() } else { 0 }).sum()
    }
}

#[derive(Default)]
struct Work(usize);

impl Work {
    async fn add(&mut self, plan: &Plan<'_>, n: usize) -> Result<()> {
        self.0 += n;
        if self.0 >= PACE_WORK {
            self.0 = 0;
            plan.pace().await?;
        }
        Ok(())
    }
}

struct BoundStage {
    group_by: Vec<(String, BExpr)>,
    aggregates: Vec<BoundAgg>,
}

struct BoundAgg {
    name: String,
    func: AggFn,
    of: Option<BExpr>,
    filter: Option<BFilter>,
    conditions: Vec<BFilter>,
}

enum Kind {
    Rows { columns: Vec<(String, BExpr)>, order: Vec<(BExpr, bool)>, after: Option<(Vec<Val>, Pos)>, limit: usize },
    Aggregate { first: BoundStage, then: Option<Stage> },
}

fn bind_stage(b: &Binder<'_>, s: &Stage) -> Result<BoundStage> {
    let group_by = s.group_by.iter().map(|g| Ok((g.name.clone(), b.expr(&g.expr)?))).collect::<Result<_>>()?;
    let mut aggregates = Vec::with_capacity(s.aggregates.len());
    for a in &s.aggregates {
        let conditions = match &a.func {
            AggFn::WindowFunnel { steps, .. } => steps.iter().map(|f| b.filter(f)).collect::<Result<_>>()?,
            AggFn::Retention(c) => c.iter().map(|f| b.filter(f)).collect::<Result<_>>()?,
            _ => Vec::new(),
        };
        aggregates.push(BoundAgg {
            name: a.name.clone(),
            func: a.func.clone(),
            of: a.of.as_ref().map(|e| b.expr(e)).transpose()?,
            filter: a.filter.as_ref().map(|f| b.filter(f)).transpose()?,
            conditions,
        });
    }
    Ok(BoundStage { group_by, aggregates })
}

struct Group {
    keys: Vec<Val>,
    states: Vec<State>,
    key_mem: usize,
    state_mem: usize,
}

struct Groups {
    map: DetHashMap<Vec<u8>, Group>,
    mem: usize,
    max_mem: usize,
}

fn state_mem(states: &[State]) -> usize {
    states.iter().map(State::mem_size).sum()
}

impl Groups {
    fn new(max_mem: usize) -> Groups {
        Groups { map: DetHashMap::default(), mem: 0, max_mem }
    }

    fn charge(&mut self, old: usize, new: usize) -> Result<()> {
        self.mem = (self.mem + new).saturating_sub(old);
        if self.mem > self.max_mem {
            return Err(Error::TooLarge(format!(
                "the query's state exceeds {} bytes of memory; narrow the time range or group by fewer values",
                self.max_mem
            )));
        }
        Ok(())
    }

    fn update(&mut self, keys: Vec<Val>, fns: &[&AggFn], f: impl FnOnce(&mut [State])) -> Result<()> {
        let mut kb = Vec::new();
        for k in &keys {
            kb.extend(k.canonical_bytes());
        }
        let (mut old, mut new) = (0, 0);
        let g = match self.map.entry(kb) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                let key_mem = 64 + 2 * e.key().len() + keys.iter().map(|k| k.mem_size() + 32).sum::<usize>();
                new += key_mem;
                let states: Vec<State> = fns.iter().map(|f| State::new(f)).collect();
                e.insert(Group { keys, states, key_mem, state_mem: 0 })
            }
        };
        f(&mut g.states);
        old += g.state_mem;
        g.state_mem = state_mem(&g.states);
        new += g.state_mem;
        self.charge(old, new)
    }

    fn full(&self) -> bool {
        self.mem > self.max_mem
    }

    fn absorb(&mut self, o: Groups) -> Result<()> {
        for (kb, og) in o.map {
            let (old, new) = match self.map.entry(kb) {
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    let g = e.get_mut();
                    g.states.iter_mut().zip(&og.states).for_each(|(a, b)| a.merge(b));
                    let old = g.state_mem;
                    g.state_mem = state_mem(&g.states);
                    (old, g.state_mem)
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    let n = og.key_mem + og.state_mem;
                    e.insert(og);
                    (0, n)
                }
            };
            self.charge(old, new)?;
        }
        Ok(())
    }

    fn finish(self, fns: &[&AggFn]) -> Vec<Vec<Val>> {
        let mut rows: Vec<(Vec<u8>, Vec<Val>)> = self
            .map
            .into_iter()
            .map(|(kb, Group { keys, states, .. })| {
                let mut row = keys;
                row.extend(states.iter().zip(fns).map(|(s, f)| s.finalize(f).canonical()));
                (kb, row)
            })
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows.into_iter().map(|(_, r)| r).collect()
    }
}

/// Retains the top `limit + 1` rows by sort key and position.
struct TopRows {
    rows: Vec<(Vec<Val>, Pos, Vec<Val>)>,
    desc: Vec<bool>,
    limit: usize,
    mem: usize,
    max_mem: usize,
}

fn row_mem(key: &[Val], row: &[Val]) -> usize {
    64 + key.iter().chain(row).map(|v| v.mem_size() + 32).sum::<usize>()
}

impl TopRows {
    fn new(desc: Vec<bool>, limit: usize, max_mem: usize) -> TopRows {
        TopRows { rows: Vec::new(), desc, limit, mem: 0, max_mem }
    }

    fn cmp(desc: &[bool], a: (&[Val], Pos), b: (&[Val], Pos)) -> Ordering {
        for (i, d) in desc.iter().enumerate() {
            let o = a.0[i].total_cmp(&b.0[i]);
            let o = if *d { o.reverse() } else { o };
            if o != Ordering::Equal {
                return o;
            }
        }
        a.1.cmp(&b.1)
    }

    fn push(&mut self, key: Vec<Val>, pos: Pos, row: Vec<Val>) -> Result<()> {
        self.mem += row_mem(&key, &row);
        self.rows.push((key, pos, row));
        if self.rows.len() > 2 * (self.limit + 1) + 64 || self.full() {
            self.trim();
        }
        if self.full() {
            return Err(Error::TooLarge(format!(
                "the query's rows exceed {} bytes of memory; select fewer columns or rows",
                self.max_mem
            )));
        }
        Ok(())
    }

    fn full(&self) -> bool {
        self.mem > self.max_mem
    }

    fn trim(&mut self) {
        let desc = std::mem::take(&mut self.desc);
        self.rows.sort_by(|a, b| TopRows::cmp(&desc, (&a.0, a.1), (&b.0, b.1)));
        self.desc = desc;
        self.rows.truncate(self.limit + 1);
        self.mem = self.rows.iter().map(|(k, _, r)| row_mem(k, r)).sum();
    }
}

struct Run<'p, 'a> {
    plan: &'p Plan<'a>,
    stats: Stats,
    groups: Option<Groups>,
    kept: Option<TopRows>,
    offered: u64,
    more: bool,
    agg_fns: Vec<AggFn>,
    max_mem: usize,
    overflowed: bool,
}

impl<'p, 'a> Run<'p, 'a> {
    fn new(plan: &'p Plan<'a>, max_mem: usize) -> Run<'p, 'a> {
        let (groups, kept, agg_fns) = match &plan.kind {
            Kind::Rows { order, limit, .. } => {
                (None, Some(TopRows::new(order.iter().map(|(_, d)| *d).collect(), *limit, max_mem)), Vec::new())
            }
            Kind::Aggregate { first, .. } => {
                (Some(Groups::new(max_mem)), None, first.aggregates.iter().map(|a| a.func.clone()).collect())
            }
        };
        Run {
            plan,
            stats: Stats::default(),
            groups,
            kept,
            offered: 0,
            more: false,
            agg_fns,
            max_mem,
            overflowed: false,
        }
    }

    fn full(&self) -> bool {
        self.overflowed
            || self.groups.as_ref().is_some_and(Groups::full)
            || self.kept.as_ref().is_some_and(TopRows::full)
    }

    fn hold(&mut self, batch: usize) -> Result<()> {
        let state = self.groups.as_ref().map_or(0, |g| g.mem) + self.kept.as_ref().map_or(0, |k| k.mem);
        if state.saturating_add(batch) > self.max_mem {
            self.overflowed = true;
            return Err(Error::TooLarge(format!(
                "the columns the query reads at once and its state exceed {} bytes of memory; read fewer columns",
                self.max_mem
            )));
        }
        Ok(())
    }

    fn absorb(&mut self, o: Run<'_, '_>) -> Result<()> {
        self.stats.add(&o.stats);
        self.offered += o.offered;
        if let (Some(k), Some(ok)) = (&mut self.kept, o.kept) {
            for (key, pos, row) in ok.rows {
                k.push(key, pos, row)?;
            }
        }
        if let (Some(g), Some(og)) = (&mut self.groups, o.groups) {
            g.absorb(og)?;
        }
        Ok(())
    }

    async fn select(&self, b: &mut Batch<'_>, f: &BFilter, base: &[bool]) -> Result<Vec<bool>> {
        let mut out = Vec::with_capacity(b.n);
        for start in (0..b.n).step_by(self.plan.window) {
            let end = (start + self.plan.window).min(b.n);
            if !base[start..end].contains(&true) {
                out.resize(end, false);
                continue;
            }
            self.plan.pace().await?;
            b.enter(start..end, &self.plan.lookups);
            out.extend(b.select(f, &base[start..end], start));
        }
        Ok(out)
    }

    async fn consume(&mut self, b: &mut Batch<'_>, sel: &[bool]) -> Result<()> {
        for start in (0..b.n).step_by(self.plan.window) {
            let end = (start + self.plan.window).min(b.n);
            if sel[start..end].contains(&true) {
                self.plan.pace().await?;
                b.enter(start..end, &self.plan.lookups);
                self.consume_window(b, &sel[start..end], start)?;
            }
        }
        Ok(())
    }

    fn consume_window(&mut self, b: &Batch, sel: &[bool], start: usize) -> Result<()> {
        let plan = &self.plan;
        let rows = || (start..start + sel.len()).filter(|i| sel[i - start]);
        match &plan.kind {
            Kind::Rows { columns, order, after, .. } => {
                let kept = self.kept.as_mut().ok_or_else(|| Error::internal("rows without a row set"))?;
                for i in rows() {
                    let key: Vec<Val> = order.iter().map(|(e, _)| b.val(e, i)).collect();
                    let pos = b.pos.get(i).copied().unwrap_or_default();
                    if let Some((ak, ap)) = after
                        && TopRows::cmp(&kept.desc, (&key, pos), (ak, *ap)) != Ordering::Greater
                    {
                        continue;
                    }
                    let row = columns.iter().map(|(_, e)| b.val(e, i)).collect();
                    kept.push(key, pos, row)?;
                    self.offered += 1;
                }
            }
            Kind::Aggregate { first, .. } => {
                let groups = self.groups.as_mut().ok_or_else(|| Error::internal("aggregates without groups"))?;
                let fns: Vec<&AggFn> = first.aggregates.iter().map(|a| &a.func).collect();
                let agg_sel: Vec<Option<Vec<bool>>> =
                    first.aggregates.iter().map(|a| a.filter.as_ref().map(|f| b.select(f, sel, start))).collect();
                let conds: Vec<Vec<Vec<bool>>> = first
                    .aggregates
                    .iter()
                    .map(|a| a.conditions.iter().map(|c| b.select(c, sel, start)).collect())
                    .collect();
                let time = BExpr::Col(plan.schema.time_id());
                for i in rows() {
                    let keys: Vec<Val> = first.group_by.iter().map(|(_, e)| b.val(e, i).canonical()).collect();
                    let w = i - start;
                    groups.update(keys, &fns, |states| {
                        for (ai, a) in first.aggregates.iter().enumerate() {
                            if agg_sel[ai].as_ref().is_some_and(|s| !s[w]) {
                                continue;
                            }
                            let event = match &a.func {
                                AggFn::WindowFunnel { mode, .. } => {
                                    let mask =
                                        conds[ai].iter().enumerate().fold(0u32, |m, (k, c)| m | ((c[w] as u32) << k));
                                    if mask == 0 && !mode.strict_order {
                                        continue;
                                    }
                                    let t = match b.val(&time, i) {
                                        Val::Int(t) => t,
                                        _ => continue,
                                    };
                                    Some((t, b.pos.get(i).copied().unwrap_or_default(), mask))
                                }
                                AggFn::Retention(_) => {
                                    let mask =
                                        conds[ai].iter().enumerate().fold(0u32, |m, (k, c)| m | ((c[w] as u32) << k));
                                    Some((0, Pos::default(), mask))
                                }
                                _ => None,
                            };
                            let v = a.of.as_ref().map_or(Val::Null, |e| b.val(e, i));
                            states[ai].add(&v, event);
                        }
                    })?;
                }
            }
        }
        Ok(())
    }

    fn lookups(&self, b: &mut Batch<'_>) {
        for k in 0..self.plan.lookups.len() {
            let need = Need::Lookup(k);
            if self.plan.first.contains(&need) || self.plan.rest.contains(&need) {
                b.derived.insert(need, Derive::Lookup);
            }
        }
    }

    async fn unsealed(&mut self, rows: &[(Pos, Row)]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.stats.unsealed_rows += rows.len() as u64;
        self.stats.rows_read += rows.len() as u64;
        let needs = self.plan.first.iter().chain(self.plan.rest.iter()).cloned();
        let mut b = Batch::unsealed(rows, needs, self.plan.schema);
        let sel = self.select(&mut b, &self.plan.filter, &vec![true; rows.len()]).await?;
        self.consume(&mut b, &sel).await
    }

    async fn read_need(
        &self,
        rd: &Reading<'_>,
        r: &PartRef,
        meta: &PartMeta,
        need: &Need,
        g: std::ops::Range<usize>,
        b: &mut Batch<'_>,
    ) -> Result<()> {
        match need {
            Need::Col(id) => match meta.find(&ColRef::User(*id)) {
                Some(ci) => b.put(need.clone(), rd.column(r, meta, ci, g).await?, false),
                None => {
                    b.cols.insert(need.clone(), ColData::Const(default_val(self.plan.schema.column_by_id(*id))));
                }
            },
            Need::Path(id, key) => {
                if let Some(ci) = meta.find(&ColRef::Path { col: *id, key: key.clone() }) {
                    b.put(need.clone(), rd.column(r, meta, ci, g).await?, meta.cols[ci].json);
                } else if meta.may_share(*id, key)
                    && let Some(ci) = meta.find(&ColRef::Shared { col: *id, bucket: json::bucket(key) })
                {
                    probe::hit(Probe::JsonSharedPathRead);
                    let bucket = json::bucket(key);
                    let bi = match b.bucket(*id, bucket) {
                        Some(bi) => bi,
                        None => b.add_bucket(*id, bucket, rd.column(r, meta, ci, g).await?)?,
                    };
                    b.derived.insert(need.clone(), Derive::Shared(bi));
                } else {
                    b.cols.insert(need.clone(), ColData::Const(Val::Null));
                }
            }
            Need::Json(id) => {
                let Some(ci) = meta.find(&ColRef::User(*id)) else {
                    b.cols.insert(need.clone(), ColData::Const(Val::Null));
                    return Ok(());
                };
                let present = rd.column(r, meta, ci, g.clone()).await?;
                b.mem += held_bytes(&present);
                let mut paths = Vec::new();
                for (ci, key) in meta.paths(*id).map(|(i, k)| (i, k.to_string())).collect::<Vec<_>>() {
                    let v = rd.column(r, meta, ci, g.clone()).await?;
                    b.mem += held_bytes(&v);
                    paths.push((
                        json::path_segments(&key).unwrap_or_default(),
                        ColData::Vec { v, json: meta.cols[ci].json },
                    ));
                }
                let mut buckets = Vec::new();
                for bucket in 0..json::BUCKETS {
                    let Some(ci) = meta.find(&ColRef::Shared { col: *id, bucket }) else { continue };
                    buckets.push(match b.bucket(*id, bucket) {
                        Some(bi) => bi,
                        None => b.add_bucket(*id, bucket, rd.column(r, meta, ci, g.clone()).await?)?,
                    });
                }
                b.derived.insert(need.clone(), Derive::Object { present, paths, buckets });
            }
            Need::Lookup(_) => {}
        }
        Ok(())
    }

    async fn positions(
        &self,
        rd: &Reading<'_>,
        r: &PartRef,
        meta: &PartMeta,
        g: std::ops::Range<usize>,
    ) -> Result<Vec<Pos>> {
        let n = (meta.starts[g.end] - meta.starts[g.start]) as usize;
        let ords = match meta.find(&ColRef::Ordinal) {
            Some(ci) => rd.column(r, meta, ci, g.clone()).await?,
            None => Vector::Int(vec![Some(0); n]),
        };
        let vers = match (r.version, meta.find(&ColRef::Version)) {
            (Some(v), _) => Vector::Int(vec![Some(v as i64); n]),
            (None, Some(ci)) => rd.column(r, meta, ci, g).await?,
            (None, None) => Vector::Int(vec![Some(0); n]),
        };
        let (Vector::Int(o), Vector::Int(v)) = (ords, vers) else {
            return Err(Error::internal("bad position columns"));
        };
        Ok(o.iter()
            .zip(&v)
            .map(|(o, v)| Pos { version: v.unwrap_or(0) as u64, ordinal: o.unwrap_or(0) as u32 })
            .collect())
    }

    async fn part_may_match(&self, rd: &Reading<'_>, r: &PartRef, meta: &PartMeta) -> Result<bool> {
        let mut dicts: BTreeMap<usize, Arc<Vec<String>>> = BTreeMap::new();
        let mut cols = BTreeSet::new();
        collect_cols(&self.plan.filter, &mut cols);
        for id in cols {
            if let Some(ci) = meta.find(&ColRef::User(id))
                && meta.cols[ci].phys == Phys::Str
                && let Some(d) = rd.index(r, meta, ci).await?.dict.clone()
            {
                dicts.insert(ci, d);
            }
        }
        // Scan dictionaries lazily only for comparisons reached during pruning.
        let mut dict_hits: DetHashMap<usize, bool> = DetHashMap::default();
        let mut work = Work::default();
        let schema = self.plan.schema;
        loop {
            let mut by_dict = false;
            let mut unknown: Option<(usize, Test<Json>, usize)> = None;
            let ok = may_match(&self.plan.filter, &mut |e, t| match e {
                BExpr::Col(id) => match meta.find(&ColRef::User(*id)) {
                    None => holds(&default_val(schema.column_by_id(*id)), t),
                    Some(ci) if meta.cols[ci].nonnull == 0 => false,
                    Some(ci) => match (dict_hits.get(&test_addr(t)), dicts.get(&ci)) {
                        (Some(hit), _) => {
                            by_dict |= !hit;
                            *hit
                        }
                        (None, Some(_)) => {
                            unknown.get_or_insert_with(|| (test_addr(t), t.clone(), ci));
                            true
                        }
                        (None, None) => true,
                    },
                },
                BExpr::Path(id, _, key) => {
                    meta.find(&ColRef::Path { col: *id, key: key.clone() }).is_some_and(|ci| meta.cols[ci].nonnull > 0)
                        || (meta.may_share(*id, key)
                            && meta.find(&ColRef::Shared { col: *id, bucket: json::bucket(key) }).is_some())
                }
                _ => true,
            });
            let Some((at, t, ci)) = unknown else {
                if !ok && by_dict {
                    probe::hit(Probe::PartPrunedByDictionary);
                }
                return Ok(ok);
            };
            let mut hit = false;
            for chunk in dicts[&ci].chunks(1024) {
                work.add(self.plan, chunk.len()).await?;
                if chunk.iter().any(|s| t.holds(&s.as_str())) {
                    hit = true;
                    break;
                }
            }
            dict_hits.insert(at, hit);
        }
    }

    async fn granules(&self, rd: &Reading<'_>, r: &PartRef, meta: &PartMeta, mask: &Runs) -> Result<Vec<bool>> {
        let mut cols = BTreeSet::new();
        collect_cols(&self.plan.filter, &mut cols);
        let mut indexes: BTreeMap<u32, (Phys, Arc<part::ColIndex>)> = BTreeMap::new();
        for id in &cols {
            if let Some(ci) = meta.find(&ColRef::User(*id)) {
                indexes.insert(*id, (meta.cols[ci].phys, rd.index(r, meta, ci).await?));
            }
        }
        let mut paths: BTreeMap<(u32, String), (bool, Arc<part::ColIndex>)> = BTreeMap::new();
        let mut pkeys = BTreeSet::new();
        collect_paths(&self.plan.filter, &mut pkeys);
        for (id, key) in pkeys {
            if let Some(ci) = meta.find(&ColRef::Path { col: id, key: key.clone() }) {
                paths.insert((id, key), (meta.cols[ci].json, rd.index(r, meta, ci).await?));
            }
        }
        let sort_ids = meta.sort_ids.clone();
        let mut out = Vec::with_capacity(meta.granule_count());
        let (mut by_stats, mut by_bloom) = (false, false);
        let (mut work, granule_work) = (Work::default(), self.plan.granule_work());
        for g in 0..meta.granule_count() {
            work.add(self.plan, granule_work).await?;
            let rows = meta.rows_of(g..g + 1);
            if mask.covers(rows.start as u32, rows.end as u32) {
                out.push(false);
                continue;
            }
            let pk: Option<(Vec<crate::table::tuple::Key>, Vec<crate::table::tuple::Key>)> = match (
                crate::table::tuple::decode_all(&meta.pk[g]),
                crate::table::tuple::decode_all(&meta.pk[g + 1]),
            ) {
                (Ok(a), Ok(b)) if a.len() == sort_ids.len() && b.len() == sort_ids.len() => Some((a, b)),
                _ => None,
            };
            let stat_ok = may_match(&self.plan.filter, &mut |e, t| {
                let stat = |s: &GranStat| match (&s.min, &s.max) {
                    (Some(lo), Some(hi)) => range_may_match(t, lo, hi),
                    _ => false,
                };
                match e {
                    BExpr::Col(id) => match indexes.get(id) {
                        Some((phys, idx)) => {
                            if !stat(&idx.stats[g]) {
                                return false;
                            }
                            if let (false, Some(bloom)) =
                                (idx.blooms.is_empty(), idx.blooms.get(g / part::BLOOM_GRANULES as usize))
                            {
                                let vals: Vec<&Json> = match t {
                                    Test::Eq(a) => vec![a],
                                    Test::In(l) => l.items().iter().collect(),
                                    _ => Vec::new(),
                                };
                                if !vals.is_empty() {
                                    let hit = vals
                                        .iter()
                                        .any(|a| bloom_hash(*phys, a).is_none_or(|h| bloom.may_contain_hash(h)));
                                    if !hit {
                                        by_bloom = true;
                                        return false;
                                    }
                                }
                            }
                            if let Some((lo, hi)) = &pk
                                && let Some(k) = sort_ids.iter().position(|s| s == id)
                                && lo[..k] == hi[..k]
                                && let (Some(l), Some(h)) = (key_stat(&lo[k]), key_stat(&hi[k]))
                                && !range_may_match(t, &l, &h)
                            {
                                return false;
                            }
                            true
                        }
                        None => true,
                    },
                    BExpr::Path(id, _, key) => match paths.get(&(*id, key.clone())) {
                        Some((false, idx)) => stat(&idx.stats[g]),
                        Some((true, idx)) => idx.stats[g].min.is_some(),
                        None => true,
                    },
                    _ => true,
                }
            });
            by_stats |= !stat_ok;
            out.push(stat_ok);
        }
        if by_bloom {
            probe::hit(Probe::GranulesPrunedByBloom);
        }
        if by_stats {
            probe::hit(Probe::GranulesPruned);
        }
        Ok(out)
    }

    async fn part(&mut self, rd: &Reading<'_>, r: &PartRef, meta: Arc<PartMeta>) -> Result<()> {
        if !self.part_may_match(rd, r, &meta).await? {
            return Ok(());
        }
        let mask = rd.mask(r).await?;
        let keep = self.granules(rd, r, &meta, &mask).await?;
        self.stats.granules += meta.granule_count() as u64;
        if !keep.iter().any(|k| *k) {
            return Ok(());
        }
        self.stats.parts_read += 1;
        let max_rows = self.run_rows(r, &meta);
        let mut g = 0;
        while g < keep.len() {
            if !keep[g] {
                g += 1;
                continue;
            }
            let mut end = g + 1;
            while end < keep.len() && keep[end] && end - g < 16 && meta.starts[end + 1] - meta.starts[g] <= max_rows {
                end += 1;
            }
            self.granule_run(rd, r, &meta, &mask, g..end).await?;
            g = end;
        }
        Ok(())
    }

    /// Returns the maximum rows of part `r` to decode in a single granule run
    /// so decoded columns use at most 25% of `max_mem`.
    fn run_rows(&self, r: &PartRef, meta: &PartMeta) -> u64 {
        let width = |ci: usize| match meta.cols[ci].phys {
            Phys::Int | Phys::Float => 16,
            Phys::Bool => 2,
            Phys::Str => 4,
        };
        let mut per_row = if self.plan.positions { 16 } else { 0 };
        for need in self.plan.first.iter().chain(&self.plan.rest) {
            per_row += match need {
                Need::Col(id) => meta.find(&ColRef::User(*id)).map_or(0, width),
                Need::Path(id, key) => meta.find(&ColRef::Path { col: *id, key: key.clone() }).map_or(4, width),
                Need::Json(id) => meta.find(&ColRef::User(*id)).map_or(0, |ci| {
                    let paths: usize = meta.paths(*id).map(|(ci, _)| width(ci)).sum();
                    width(ci) + paths + 4 * json::BUCKETS as usize + (r.raw_size / r.rows.max(1)) as usize
                }),
                Need::Lookup(_) => 0,
            };
        }
        (self.max_mem / 4 / per_row.max(1)) as u64
    }

    fn fits(&self, r: &PartRef, meta: &PartMeta) -> bool {
        let largest = meta.starts.windows(2).map(|w| w[1] - w[0]).max().unwrap_or(0);
        largest <= 4 * self.run_rows(r, meta)
    }

    async fn granule_run(
        &mut self,
        rd: &Reading<'_>,
        r: &PartRef,
        meta: &PartMeta,
        mask: &Runs,
        g: std::ops::Range<usize>,
    ) -> Result<()> {
        let plan = self.plan;
        let rows = meta.rows_of(g.clone());
        let n = (rows.end - rows.start) as usize;
        self.stats.granules_read += (g.end - g.start) as u64;
        self.stats.rows_read += n as u64;
        let mut b = Batch::new(n, Vec::new());
        for need in &plan.first {
            self.read_need(rd, r, meta, need, g.clone(), &mut b).await?;
            self.hold(b.mem)?;
        }
        let base: Vec<bool> = (0..n).map(|i| !mask.contains(rows.start as u32 + i as u32)).collect();
        let lookups_first = plan.first.iter().any(|n| matches!(n, Need::Lookup(_)));
        if lookups_first {
            for need in &plan.rest {
                if !b.has(need) {
                    self.read_need(rd, r, meta, need, g.clone(), &mut b).await?;
                    self.hold(b.mem)?;
                }
            }
            self.lookups(&mut b);
        }
        let sel = self.select(&mut b, &plan.filter, &base).await?;
        if !sel.contains(&true) {
            return Ok(());
        }
        for need in &plan.rest {
            if !b.has(need) {
                self.read_need(rd, r, meta, need, g.clone(), &mut b).await?;
                self.hold(b.mem)?;
            }
        }
        self.lookups(&mut b);
        if plan.positions {
            b.pos = self.positions(rd, r, meta, g).await?;
            b.mem += b.pos.capacity() * std::mem::size_of::<Pos>();
            self.hold(b.mem)?;
        }
        self.consume(&mut b, &sel).await
    }
}

fn collect_cols(f: &BFilter, out: &mut BTreeSet<u32>) {
    match f {
        filter::Filter::And(fs) | filter::Filter::Or(fs) => fs.iter().for_each(|x| collect_cols(x, out)),
        filter::Filter::Not(x) => collect_cols(x, out),
        filter::Filter::Cmp(BExpr::Col(id), _) => {
            out.insert(*id);
        }
        filter::Filter::Cmp(..) => {}
    }
}

fn collect_paths(f: &BFilter, out: &mut BTreeSet<(u32, String)>) {
    match f {
        filter::Filter::And(fs) | filter::Filter::Or(fs) => fs.iter().for_each(|x| collect_paths(x, out)),
        filter::Filter::Not(x) => collect_paths(x, out),
        filter::Filter::Cmp(BExpr::Path(id, _, key), _) => {
            out.insert((*id, key.clone()));
        }
        filter::Filter::Cmp(..) => {}
    }
}

fn ref_may_match(f: &BFilter, r: &PartRef, time_id: u32) -> bool {
    may_match(f, &mut |e, t| match e {
        BExpr::Col(id) if *id == time_id => range_may_match(t, &Stat::Int(r.min_time), &Stat::Int(r.max_time)),
        _ => true,
    })
}

/// Time ordering of a rows query whose leading sort key is the time column.
#[derive(Clone, Copy)]
struct ByTime {
    desc: bool,
    /// True if the time column is the only sort key, so ties are ordered by [`Pos`].
    alone: bool,
}

impl ByTime {
    fn of(kind: &Kind, time: &BExpr) -> Option<ByTime> {
        match kind {
            Kind::Rows { order, .. } => match order.first() {
                Some((e, desc)) if e == time => Some(ByTime { desc: *desc, alone: order.len() == 1 }),
                _ => None,
            },
            Kind::Aggregate { .. } => None,
        }
    }

    fn before(self, a: i64, b: i64) -> bool {
        if self.desc { a > b } else { a < b }
    }

    fn ends(self, r: &PartRef) -> (i64, i64) {
        if self.desc { (r.max_time, r.min_time) } else { (r.min_time, r.max_time) }
    }

    fn all_after(self, r: &PartRef, t: i64, p: Pos) -> bool {
        let first = self.ends(r).0;
        self.before(t, first) || (first == t && self.alone && r.min_pos > p)
    }

    fn some_after(self, r: &PartRef, t: i64, p: Pos) -> bool {
        let last = self.ends(r).1;
        self.before(t, last) || (last == t && (!self.alone || r.max_pos > p))
    }

    fn read_order(self, a: &PartRef, b: &PartRef) -> Ordering {
        let (x, y) = (self.ends(a).0, self.ends(b).0);
        (if self.desc { y.cmp(&x) } else { x.cmp(&y) }).then(a.min_pos.cmp(&b.min_pos))
    }
}

fn cursor_at(kind: &Kind) -> Option<(i64, Pos)> {
    match kind {
        Kind::Rows { after: Some((key, pos)), .. } => match key.first() {
            Some(Val::Int(t)) => Some((*t, *pos)),
            _ => None,
        },
        _ => None,
    }
}

/// Schedules part reads for time-ordered row queries and stops early once the
/// current page is complete.
struct Frontier {
    by: ByTime,
    limit: usize,
    /// For each index `i`, true if any part in `refs[i..]` is guaranteed to contain a matching row after the cursor.
    certain: Vec<bool>,
    scanned: u64,
    times: Vec<i64>,
}

impl Frontier {
    fn new(by: ByTime, limit: usize, refs: &[&PartRef], certain: impl Fn(&PartRef) -> bool, unsealed: u64) -> Frontier {
        let mut c = vec![false; refs.len() + 1];
        for i in (0..refs.len()).rev() {
            c[i] = c[i + 1] || certain(refs[i]);
        }
        Frontier { by, limit, certain: c, scanned: unsealed, times: Vec::new() }
    }

    /// Returns true if all rows in `refs[i..]` sort after the `limit`-th kept row
    /// and the existence of a next page is already determined.
    fn is_done(&mut self, run: &mut Run<'_, '_>, refs: &[&PartRef], i: usize) -> bool {
        let Some(kept) = run.kept.as_mut() else { return false };
        kept.trim();
        self.times.clear();
        for (key, ..) in &kept.rows {
            let Some(Val::Int(t)) = key.first() else { return false };
            self.times.push(*t);
        }
        let Some((_, pos, _)) = kept.rows.get(self.limit - 1) else { return false };
        let t = self.times[self.limit - 1];
        let done = self.by.all_after(refs[i], t, *pos) && (kept.rows.len() > self.limit || self.certain[i]);
        run.more |= done;
        done
    }

    /// Estimates whether part `next` can contribute to the page while `refs[i..next]`
    /// are in flight, assuming in-flight parts match at the observed selectivity rate
    /// and their matching rows fall at the end of each part's time range.
    fn should_read(&self, run: &Run<'_, '_>, refs: &[&PartRef], i: usize, next: usize) -> bool {
        let by = self.by;
        let rate = if self.scanned == 0 { 1.0 } else { run.offered as f64 / self.scanned as f64 };
        let mut reading: Vec<(i64, f64)> =
            refs[i..next].iter().map(|r| (by.ends(r).1, (r.rows - r.deleted) as f64 * rate)).collect();
        reading.sort_by(|a, b| if by.desc { b.0.cmp(&a.0) } else { a.0.cmp(&b.0) });
        let need = (self.limit + usize::from(!self.certain[next])) as f64;
        let later = |a: i64, b: i64| if by.before(a, b) { b } else { a };
        let mut bound = None;
        let mut expected = 0.0;
        for j in 0..=reading.len() {
            let floor = j.checked_sub(1).map(|j| reading[j].0);
            if let Some(j) = j.checked_sub(1) {
                expected += reading[j].1;
            }
            let rest = (need - expected).ceil().max(0.0) as usize;
            let at = match rest.checked_sub(1) {
                None => floor,
                Some(k) => self.times.get(k).map(|t| floor.map_or(*t, |f| later(f, *t))),
            };
            if let Some(at) = at {
                bound = Some(bound.map_or(at, |b| if by.before(at, b) { at } else { b }));
            }
        }
        bound.is_none_or(|b| !by.before(b, by.ends(refs[next]).0))
    }
}

fn cursor_encode(key: &[Val], pos: Pos) -> String {
    let v = json!([key.iter().map(Val::to_json).collect::<Vec<_>>(), pos.version, pos.ordinal]);
    b64::encode_url(v.to_string().as_bytes())
}

fn cursor_decode(c: &str, n: usize) -> Result<(Vec<Val>, Pos)> {
    let bad = || Error::invalid("bad cursor");
    let b = b64::decode_url(c).ok_or_else(bad)?;
    let v: Json = serde_json::from_slice(&b).map_err(|_| bad())?;
    let a = v.as_array().filter(|a| a.len() == 3).ok_or_else(bad)?;
    let keys: Vec<Val> = a[0].as_array().filter(|k| k.len() == n).ok_or_else(bad)?.iter().map(Val::from_json).collect();
    let version = a[1].as_u64().ok_or_else(bad)?;
    let ordinal = a[2].as_u64().and_then(|o| u32::try_from(o).ok()).ok_or_else(bad)?;
    Ok((keys, Pos { version, ordinal }))
}

/// Part references and unsealed rows read by a query.
pub struct Input {
    pub refs: Vec<PartRef>,
    pub unsealed: Vec<(Pos, Row)>,
}

/// Reads the part references for `[from, to)` and all unsealed rows of table `s`,
/// recording both in the transaction's read set.
pub async fn input(txn: &mut Txn, s: &EventSchema, from: i64, to: i64) -> Result<Input> {
    if txn.inserts_events(s.id) {
        return Err(Error::invalid(format!(
            "this transaction inserts into event table {:?}, so it cannot also query or delete its events: \
             positions are assigned at commit",
            s.name
        )));
    }
    let mut refs = Vec::new();
    if from < to {
        let (lo, hi) = (ev::partition_key(s.bucket(from)), ev::partition_key(s.bucket(to - 1)));
        for it in txn.scan(&ev::refs_between(s.id, lo, hi), false, usize::MAX >> 1).await? {
            refs.push(super::ref_of(&it)?);
        }
    }
    let mut unsealed = Vec::new();
    for it in txn.scan(&ev::rows(s.id), false, usize::MAX >> 1).await? {
        let (_, pos) = ev::parse_row_key(&it.key).ok_or_else(|| Error::internal("bad unsealed row key"))?;
        let Value::Inline(b) = &it.value else { return Err(Error::internal("unsealed row stored out of line")) };
        unsealed.push((pos, decode_row(b).map_err(|_| Error::internal("corrupt unsealed row"))?));
    }
    Ok(Input { refs, unsealed })
}

async fn lookup_maps(txn: &mut Txn, q: &Query, limits: &Limits) -> Result<Vec<(String, Arc<LookupMap>)>> {
    let mut values: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for l in q.lookups.values() {
        values.entry(l.table.as_str()).or_default().insert(l.value.as_str());
    }
    let too_large = || Error::TooLarge(format!("the query's lookup maps exceed {} bytes", limits.lookup_max_bytes));
    let mut budget = limits.lookup_max_bytes;
    let mut maps: BTreeMap<(&str, &str), Arc<LookupMap>> = BTreeMap::new();
    for (table, values) in values {
        let ts = crate::table::get_schema(txn, table).await?;
        if ts.primary_key.len() != 1 {
            return Err(Error::invalid(format!("lookup table {table:?} needs a single-column primary key")));
        }
        let key_id = ts.column(&ts.primary_key[0]).map(|c| c.id).ok_or_else(|| Error::internal("pk column missing"))?;
        let mut cols = Vec::with_capacity(values.len());
        for value in values {
            let id = ts
                .column(value)
                .map(|c| c.id)
                .ok_or_else(|| Error::invalid(format!("lookup table {table:?} does not have column {value:?}")))?;
            cols.push((value, id, LookupMap::default()));
        }
        crate::table::each_row(txn, &ts, |row| {
            let key = row.get(&key_id).map_or(Val::Null, datum_val).canonical_bytes();
            for (_, id, map) in cols.iter_mut() {
                let before = map.mem();
                map.push(&key, &row.get(id).map_or(Val::Null, datum_val))?;
                budget = budget.checked_sub(map.mem() - before).ok_or_else(too_large)?;
            }
            Ok(())
        })
        .await?;
        for (value, _, mut map) in cols {
            budget = budget.checked_sub(map.index_mem()).ok_or_else(too_large)?;
            map.index();
            maps.insert((table, value), Arc::new(map));
        }
    }
    Ok(q.lookups.iter().map(|(name, l)| (name.clone(), maps[&(l.table.as_str(), l.value.as_str())].clone())).collect())
}

/// Executes query `q` on event table `s` at `txn`'s snapshot.
pub async fn run(
    shared: &Shared,
    db: &str,
    txn: &mut Txn,
    s: &EventSchema,
    q: &Query,
    limits: &Limits,
) -> Result<Output> {
    let limit = std::time::Duration::from_millis(limits.max_ms);
    match tokio::time::timeout(limit, run_within(shared, db, txn, s, q, limits)).await {
        Ok(r) => r,
        Err(_) => Err(timed_out(limits.max_ms)),
    }
}

fn timed_out(max_ms: u64) -> Error {
    Error::Unavailable {
        message: format!("the query ran longer than {max_ms} ms; narrow its time range or filter"),
        retry_after_ms: Some(1_000),
        outcome_unknown: false,
    }
}

async fn run_within(
    shared: &Shared,
    db: &str,
    txn: &mut Txn,
    s: &EventSchema,
    q: &Query,
    limits: &Limits,
) -> Result<Output> {
    let maps = lookup_maps(txn, q, limits).await?;
    let names: BTreeMap<String, usize> = maps.iter().enumerate().map(|(i, (n, _))| (n.clone(), i)).collect();
    let binder = Binder { schema: s, lookups: &names };
    let mut lookups = Vec::new();
    for (name, map) in &maps {
        let spec = &q.lookups[name];
        let key = binder.expr(&spec.key)?;
        if matches!(key, BExpr::Json(_)) {
            return Err(Error::invalid("a lookup key is a column or a JSON path"));
        }
        let default = spec.default.as_ref().map(|d| binder.expr(d)).transpose()?;
        lookups.push(Lookup { key, default, map: map.clone() });
    }
    if !lookups.is_empty() {
        probe::hit(Probe::LookupApplied);
    }
    let time = BExpr::Col(s.time_id());
    let user_filter = q.filter.as_ref().map(|f| binder.filter(f)).transpose()?;
    let kind = match &q.mode {
        Mode::Rows { columns, cursor } => {
            let columns: Vec<(String, BExpr)> = if columns.is_empty() {
                s.columns
                    .iter()
                    .map(|c| Ok((c.name.clone(), binder.expr(&Expr::Col { name: c.name.clone(), path: Vec::new() })?)))
                    .collect::<Result<_>>()?
            } else {
                columns.iter().map(|c: &Named| Ok((c.name.clone(), binder.expr(&c.expr)?))).collect::<Result<_>>()?
            };
            let order: Vec<(BExpr, bool)> = if q.order_by.is_empty() {
                vec![(time.clone(), false)]
            } else {
                q.order_by
                    .iter()
                    .map(|o| {
                        let e =
                            columns.iter().find(|(n, _)| *n == o.name).map(|(_, e)| e.clone()).ok_or_else(|| {
                                Error::invalid(format!("order_by {:?} is not an output column", o.name))
                            })?;
                        Ok((e, o.desc))
                    })
                    .collect::<Result<_>>()?
            };
            let after = cursor.as_deref().map(|c| cursor_decode(c, order.len())).transpose()?;
            Kind::Rows { columns, order, after, limit: q.limit }
        }
        Mode::Aggregate { first, then } => Kind::Aggregate { first: bind_stage(&binder, first)?, then: then.clone() },
    };
    let by_time = ByTime::of(&kind, &time);
    let past = by_time.and(cursor_at(&kind));
    let mut from = shared.store_now_ms().map_or(q.from, |now| q.from.max(s.retained_from(now as i64)));
    let mut to = q.to;
    if let (Some(by), Some((t, _))) = (by_time, past) {
        if by.desc {
            to = to.min(t.saturating_add(1));
        } else {
            from = from.max(t);
        }
    }
    let mut conds = vec![
        filter::Filter::Cmp(time.clone(), Test::Ge(json!(from))),
        filter::Filter::Cmp(time.clone(), Test::Lt(json!(to))),
    ];
    conds.extend(user_filter);
    let filter = filter::Filter::And(conds);
    let input = input(txn, s, from, to).await?;
    let deadline = Some((tokio::time::Instant::now() + std::time::Duration::from_millis(limits.max_ms), limits.max_ms));
    let plan = Plan::new(s, filter, lookups, kind, deadline);
    let mut run = Run::new(&plan, limits.max_memory_bytes);
    run.stats = Stats { source: "columns".into(), nodes: 1, parts: input.refs.len() as u64, ..Default::default() };
    let read_stats = ReadStats::default();
    let rd = Reading { shared, db, cache: Caching::Shared, stats: &read_stats };
    let mut refs: Vec<&PartRef> = input
        .refs
        .iter()
        .filter(|r| {
            let may = ref_may_match(&plan.filter, r, s.time_id())
                && by_time.zip(past).is_none_or(|(by, (t, p))| by.some_after(r, t, p));
            if !may {
                probe::hit(Probe::PartPrunedByRef);
            }
            may
        })
        .collect();
    run.unsealed(&input.unsealed).await?;
    let mut frontier = by_time.map(|by| {
        refs.sort_by(|a, b| by.read_order(a, b));
        let certain = |r: &PartRef| {
            q.filter.is_none()
                && from <= r.min_time
                && r.max_time < to
                && r.rows > r.deleted
                && past.is_none_or(|(t, p)| by.all_after(r, t, p))
        };
        Frontier::new(by, q.limit, &refs, certain, input.unsealed.len() as u64)
    });
    // Read parts concurrently into partial runs bounded to an equal share of
    // `max_memory_bytes` and merge them in deterministic part order. If a part
    // exceeds its share, re-read it directly into `run`.
    let n = shared.cfg.event_read_concurrency.max(1);
    let share = limits.max_memory_bytes / n;
    let read = |i: usize| {
        let (mut partial, rd, r) = (Run::new(&plan, share), &rd, refs[i]);
        async move {
            let read = match rd.meta(r).await {
                Ok(meta) if !partial.fits(r, &meta) => {
                    partial.overflowed = true;
                    Err(Error::TooLarge("a granule exceeds a partial result's memory share".into()))
                }
                Ok(meta) => partial.part(rd, r, meta).await,
                Err(e) => Err(e),
            };
            (partial, read)
        }
    };
    let mut reads = FuturesOrdered::new();
    let mut launched = 0;
    for (i, r) in refs.iter().enumerate() {
        if frontier.as_mut().is_some_and(|f| f.is_done(&mut run, &refs, i)) {
            probe::hit(Probe::RowPageStoppedEarly);
            break;
        }
        while launched < refs.len()
            && reads.len() < n
            && (launched == i || frontier.as_ref().is_none_or(|f| f.should_read(&run, &refs, i, launched)))
        {
            reads.push_back(read(launched));
            launched += 1;
        }
        let (partial, read) = reads.next().await.ok_or_else(|| Error::internal("query reads ended early"))?;
        match read {
            Ok(()) => run.absorb(partial)?,
            Err(Error::TooLarge(_)) if partial.full() => {
                run.part(&rd, r, rd.meta(r).await?).await?;
            }
            Err(e) => return Err(e),
        }
        if let Some(f) = &mut frontier {
            f.scanned += r.rows - r.deleted;
        }
        if read_stats.bytes.load(Relaxed) > limits.max_bytes_read {
            return Err(Error::TooLarge(format!(
                "the query read more than {} bytes; narrow its time range or filter",
                limits.max_bytes_read
            )));
        }
    }
    run.stats.bytes_read = read_stats.bytes.load(Relaxed);
    run.stats.store_gets = read_stats.gets.load(Relaxed);
    finish(run, q, limits)
}

fn finish(run: Run<'_, '_>, q: &Query, limits: &Limits) -> Result<Output> {
    let Run { plan, stats, groups, kept, more, agg_fns, .. } = run;
    match &plan.kind {
        Kind::Rows { columns, limit, .. } => {
            let limit = *limit;
            let mut kept = kept.ok_or_else(|| Error::internal("rows without a row set"))?;
            kept.trim();
            let more = more || kept.rows.len() > limit;
            kept.rows.truncate(limit);
            let next_cursor = if more { kept.rows.last().map(|(k, p, _)| cursor_encode(k, *p)) } else { None };
            Ok(Output {
                columns: columns.iter().map(|(n, _)| n.clone()).collect(),
                rows: kept.rows.into_iter().map(|(_, _, r)| r.iter().map(Val::to_json).collect()).collect(),
                next_cursor,
                stats,
            })
        }
        Kind::Aggregate { first, then } => {
            let groups = groups.ok_or_else(|| Error::internal("aggregates without groups"))?;
            let fns: Vec<&AggFn> = agg_fns.iter().collect();
            let mut names: Vec<String> = first.group_by.iter().map(|(n, _)| n.clone()).collect();
            names.extend(first.aggregates.iter().map(|a| a.name.clone()));
            let mut rows = if first.group_by.is_empty() && groups.map.is_empty() {
                vec![first.aggregates.iter().map(|a| State::new(&a.func).finalize(&a.func).canonical()).collect()]
            } else {
                groups.finish(&fns)
            };
            if let Some(second) = then {
                probe::hit(Probe::QuerySecondStage);
                (names, rows) = second_stage(&names, rows, second, limits)?;
            }
            if rows.len() > q.limit && !q.limited {
                return Err(Error::TooLarge(format!(
                    "the query has more than {} groups; set a limit or group by fewer values",
                    q.limit
                )));
            }
            sort_rows(&names, &mut rows, &q.order_by)?;
            rows.truncate(q.limit);
            Ok(Output {
                columns: names,
                rows: rows.iter().map(|r| r.iter().map(Val::to_json).collect()).collect(),
                next_cursor: None,
                stats,
            })
        }
    }
}

fn sort_rows(names: &[String], rows: &mut [Vec<Val>], order: &[super::query::OrderBy]) -> Result<()> {
    let keys: Vec<(usize, bool)> = order
        .iter()
        .map(|o| {
            let i = names
                .iter()
                .position(|n| *n == o.name)
                .ok_or_else(|| Error::invalid(format!("order_by {:?} is not an output", o.name)))?;
            Ok((i, o.desc))
        })
        .collect::<Result<_>>()?;
    rows.sort_by(|a, b| {
        for (i, d) in &keys {
            let o = a[*i].total_cmp(&b[*i]);
            let o = if *d { o.reverse() } else { o };
            if o != Ordering::Equal {
                return o;
            }
        }
        a.iter().zip(b.iter()).map(|(x, y)| x.total_cmp(y)).find(|o| *o != Ordering::Equal).unwrap_or(Ordering::Equal)
    });
    Ok(())
}

fn second_stage(
    names: &[String],
    rows: Vec<Vec<Val>>,
    s: &Stage,
    limits: &Limits,
) -> Result<(Vec<String>, Vec<Vec<Val>>)> {
    let bind = |e: &Expr| -> Result<BExpr> {
        match e {
            Expr::Col { name, path } if path.is_empty() => {
                names.iter().position(|n| n == name).map(BExpr::Out).ok_or_else(|| {
                    Error::invalid(format!("then refers to {name:?}, which the first stage does not output"))
                })
            }
            Expr::Bucket { of, interval, offset_ms } => {
                Ok(BExpr::Bucket(Box::new(bind_out(names, of)?), *interval, *offset_ms))
            }
            _ => Err(Error::invalid("then refers to first-stage outputs by name")),
        }
    };
    let group_by: Vec<(String, BExpr)> =
        s.group_by.iter().map(|g| Ok((g.name.clone(), bind(&g.expr)?))).collect::<Result<_>>()?;
    let mut aggs = Vec::new();
    for a in &s.aggregates {
        if a.filter.is_some() || matches!(a.func, AggFn::WindowFunnel { .. } | AggFn::Retention(_)) {
            return Err(Error::invalid("second-stage aggregates do not support where, funnels, or retention"));
        }
        aggs.push((a.name.clone(), a.func.clone(), a.of.as_ref().map(bind).transpose()?));
    }
    let eval = |e: &BExpr, row: &[Val]| -> Val {
        match e {
            BExpr::Out(i) => row[*i].clone(),
            BExpr::Bucket(inner, iv, off) => match inner.as_ref() {
                BExpr::Out(i) => match &row[*i] {
                    Val::Int(t) => iv.bucket(*t, *off).map_or(Val::Null, Val::Int),
                    _ => Val::Null,
                },
                _ => Val::Null,
            },
            _ => Val::Null,
        }
    };
    let fns: Vec<&AggFn> = aggs.iter().map(|(_, f, _)| f).collect();
    let mut groups = Groups::new(limits.max_memory_bytes);
    for row in &rows {
        let keys: Vec<Val> = group_by.iter().map(|(_, e)| eval(e, row).canonical()).collect();
        groups.update(keys, &fns, |states| {
            for (i, (_, _, of)) in aggs.iter().enumerate() {
                let v = of.as_ref().map_or(Val::Null, |e| eval(e, row));
                states[i].add(&v, None);
            }
        })?;
    }
    let mut out_names: Vec<String> = group_by.iter().map(|(n, _)| n.clone()).collect();
    out_names.extend(aggs.iter().map(|(n, _, _)| n.clone()));
    let out = if group_by.is_empty() && groups.map.is_empty() {
        vec![aggs.iter().map(|(_, f, _)| State::new(f).finalize(f).canonical()).collect()]
    } else {
        groups.finish(&fns)
    };
    Ok((out_names, out))
}

fn bind_out(names: &[String], e: &Expr) -> Result<BExpr> {
    match e {
        Expr::Col { name, path } if path.is_empty() => {
            names.iter().position(|n| n == name).map(BExpr::Out).ok_or_else(|| {
                Error::invalid(format!("then refers to {name:?}, which the first stage does not output"))
            })
        }
        _ => Err(Error::invalid("then refers to first-stage outputs by name")),
    }
}

/// Returns the sorted row offsets of live rows in part `r` that match `f` within `[from, to)`.
pub async fn matching_rows(
    shared: &Shared,
    db: &str,
    s: &EventSchema,
    r: &PartRef,
    f: &Filter,
    from: i64,
    to: i64,
) -> Result<Vec<u32>> {
    let names = BTreeMap::new();
    let binder = Binder { schema: s, lookups: &names };
    let time = BExpr::Col(s.time_id());
    let filter = filter::Filter::And(vec![
        filter::Filter::Cmp(time.clone(), Test::Ge(json!(from))),
        filter::Filter::Cmp(time, Test::Lt(json!(to))),
        binder.filter(f)?,
    ]);
    if !ref_may_match(&filter, r, s.time_id()) {
        return Ok(Vec::new());
    }
    let plan = Plan::new(s, filter, Vec::new(), only_filter(), None);
    let run = Run::new(&plan, usize::MAX);
    let read_stats = ReadStats::default();
    let rd = Reading { shared, db, cache: Caching::Shared, stats: &read_stats };
    let meta = rd.meta(r).await?;
    if !run.part_may_match(&rd, r, &meta).await? {
        return Ok(Vec::new());
    }
    let mask = rd.mask(r).await?;
    let keep = run.granules(&rd, r, &meta, &mask).await?;
    let mut out = Vec::new();
    for (g, k) in keep.iter().enumerate() {
        if !k {
            continue;
        }
        let rows = meta.rows_of(g..g + 1);
        let n = (rows.end - rows.start) as usize;
        let mut b = Batch::new(n, Vec::new());
        for need in &plan.first {
            run.read_need(&rd, r, &meta, need, g..g + 1, &mut b).await?;
        }
        let base: Vec<bool> = (0..n).map(|i| !mask.contains(rows.start as u32 + i as u32)).collect();
        let sel = run.select(&mut b, &plan.filter, &base).await?;
        out.extend(sel.iter().enumerate().filter(|(_, s)| **s).map(|(i, _)| rows.start as u32 + i as u32));
    }
    Ok(out)
}

/// Validates that `f` references valid columns and types in `s`.
pub fn check_filter(s: &EventSchema, f: &Filter) -> Result<()> {
    let names = BTreeMap::new();
    Binder { schema: s, lookups: &names }.filter(f).map(drop)
}

/// Returns true if unsealed row `row` falls within `[from, to)` and matches `f`.
pub fn row_matches(s: &EventSchema, row: &Row, f: &Filter, from: i64, to: i64) -> Result<bool> {
    let names = BTreeMap::new();
    let binder = Binder { schema: s, lookups: &names };
    let t = s.time_of(row);
    if t < from || t >= to {
        return Ok(false);
    }
    let plan = Plan::new(s, binder.filter(f)?, Vec::new(), only_filter(), None);
    let run = Run::new(&plan, usize::MAX);
    let mut b = Batch::new(1, vec![Pos::default()]);
    for need in run.plan.first.iter() {
        let v = match need {
            Need::Col(id) => row.get(id).map_or_else(|| default_val(s.column_by_id(*id)), datum_val),
            Need::Path(id, key) => match row.get(id) {
                Some(Datum::Json(j)) => {
                    json::leaf(j, &json::path_segments(key).unwrap_or_default()).map_or(Val::Null, Val::from_json)
                }
                _ => Val::Null,
            },
            Need::Json(id) => row.get(id).map_or(Val::Null, datum_val),
            Need::Lookup(_) => Val::Null,
        };
        b.cols.insert(need.clone(), ColData::Vals(vec![v]));
    }
    Ok(b.select(&run.plan.filter, &[true], 0)[0])
}

fn only_filter() -> Kind {
    Kind::Aggregate { first: BoundStage { group_by: Vec::new(), aggregates: Vec::new() }, then: None }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_map_resists_hash_collisions() {
        let keys = |n: usize, collide: bool| -> Vec<Vec<u8>> {
            let window = (n + n / 2 + 1).next_power_of_two() as u64 / 8;
            (0i64..)
                .map(|i| Val::Int(i).canonical_bytes())
                .filter(|k| !collide || crate::util::bloom::key_hash(k) & (window * 8 - 1) < window)
                .take(n)
                .collect()
        };
        let build = |keys: &[Vec<u8>]| {
            let start = tokio::time::Instant::now();
            let mut m = LookupMap::default();
            for k in keys {
                m.push(k, &Val::Int(1)).unwrap();
            }
            m.index();
            assert!(keys.iter().all(|k| m.get(k) == Some(Val::Int(1))));
            start.elapsed()
        };
        let n = 1 << 15;
        let (honest, colliding) = (build(&keys(n, false)), build(&keys(n, true)));
        assert!(colliding < honest * 10 + std::time::Duration::from_millis(200), "{honest:?} vs {colliding:?}");
    }

    #[test]
    fn lookup_map_returns_last_value_for_key() {
        let mut m = LookupMap::default();
        assert_eq!(m.get(&Val::Int(1).canonical_bytes()), None, "empty map returns None");
        let keys: Vec<Val> = (0..1000).map(Val::Int).chain([Val::Str("a".into()), Val::Float(0.5)]).collect();
        for (i, k) in keys.iter().enumerate() {
            m.push(&k.canonical_bytes(), &Val::Str(format!("v{i}"))).unwrap();
        }
        m.push(&Val::Int(7).canonical_bytes(), &Val::Null).unwrap();
        m.index();
        for (i, k) in keys.iter().enumerate() {
            let want = if *k == Val::Int(7) { Val::Null } else { Val::Str(format!("v{i}")) };
            assert_eq!(m.get(&k.canonical_bytes()), Some(want), "{k:?}");
        }
        for k in [Val::Int(1000), Val::Str("b".into()), Val::Float(1.5), Val::Null] {
            assert_eq!(m.get(&k.canonical_bytes()), None, "{k:?}");
        }
    }
}
