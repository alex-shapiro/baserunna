//! Query execution (SPEC §5.13): pruning at every level before data is
//! read, then columnar batches through filters and aggregation.
//!
//! A query reads one snapshot: the part references of the partitions its
//! time range overlaps and the table's unsealed rows. Parts are pruned by
//! their references, then by dictionaries and missing columns, then granule
//! by granule by the primary index, statistics and bloom filters. The
//! filter's columns are read first, and the other columns only for granules
//! with selected rows.

use super::agg::{State, Val};
use super::json;
use super::part::{self, Caching, ColRef, GranStat, PartMeta, ReadStats, Reading, Stat};
use super::query::{AggFn, Expr, Filter, Interval, Mode, Named, Query, Stage};
use super::schema::{EvColumn, EvType, EventSchema, NOW, parse_rfc3339};
use super::vector::{Phys, Vector};
use crate::engine::events::{self as ev, PartRef, Pos, Runs};
use crate::engine::{Shared, Txn, Value};
use crate::error::{Error, Result};
use crate::filter::{self, Target, Test};
use crate::probe::{self, Probe};
use crate::table::schema::{Datum, Row, decode_row};
use crate::util::DetHashMap;
use crate::util::b64;
use futures_util::{StreamExt, stream};
use serde_json::{Value as Json, json};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

/// Limits on the work one query does.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Compressed bytes read from storage.
    pub max_bytes_read: u64,
    /// Bytes of aggregation state.
    pub max_memory_bytes: usize,
    /// Bytes of rows read from the query's lookup tables.
    pub lookup_max_bytes: usize,
    /// Milliseconds a query runs before it stops with a retryable `503`.
    pub max_ms: u64,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits { max_bytes_read: 4 << 30, max_memory_bytes: 1 << 30, lookup_max_bytes: 1 << 30, max_ms: 60_000 }
    }
}

/// What a query did, for its response.
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

/// A query's answer.
#[derive(Clone, Debug, PartialEq)]
pub struct Output {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Json>>,
    pub next_cursor: Option<String>,
    pub stats: Stats,
}

/// An expression bound to the table's columns.
#[derive(Clone, Debug, PartialEq)]
pub enum BExpr {
    Col(u32),
    Path(u32, Vec<String>, String),
    /// A whole JSON column.
    Json(u32),
    Lookup(usize),
    Bucket(Box<BExpr>, Interval, i64),
    /// A first-stage output, in the second stage.
    Out(usize),
}

type BFilter = filter::Filter<BExpr, Json>;

/// A column, path or derived value a batch provides.
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
    map: Arc<DetHashMap<Vec<u8>, Val>>,
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
                    (_, false) => return Err(Error::invalid(format!("column {name:?} is not json and takes no path"))),
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
                // Constants compare with stored values, so they are normalized the same way.
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

/// Converts a filter argument to the type of column `c`, refusing a mismatch.
fn typed_arg(c: &EvColumn, a: &Json) -> Result<Json> {
    let bad = || Error::invalid(format!("column {:?} compares with {} values, not {a}", c.name, c.ty.name()));
    Ok(match c.ty {
        EvType::Time => match a {
            Json::String(s) => json!(parse_rfc3339(s).ok_or_else(bad)?),
            _ => json!(a.as_i64().ok_or_else(bad)?),
        },
        EvType::Int => json!(a.as_i64().ok_or_else(bad)?),
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

/// Values compare as JSON: numbers by value, strings by bytes, booleans with
/// false first, and arrays and objects only for equality.
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
        Datum::Json(j) => Val::Json(j.clone()),
        Datum::Bytes(b) => Val::Str(b64::encode(b)),
        Datum::Blob(_) => Val::Null,
    }
}

/// A column's value where a row or part predates it.
fn default_val(c: Option<&EvColumn>) -> Val {
    match c.and_then(|c| c.default.as_ref().map(|d| (c, d))) {
        Some((_, Json::String(s))) if s == NOW => Val::Null,
        Some((c, d)) => super::schema::datum(c, d).map(|d| datum_val(&d)).unwrap_or(Val::Null),
        None => Val::Null,
    }
}

/// The values of one need across a batch.
#[derive(Clone, Debug)]
enum ColData {
    Vec { v: Vector, json: bool },
    Vals(Vec<Val>),
    Const(Val),
}

impl ColData {
    fn get(&self, i: usize) -> Val {
        match self {
            ColData::Const(v) => v.clone(),
            ColData::Vals(v) => v[i].clone(),
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

/// Rows of one part's granules or of the unsealed rows, by column.
struct Batch {
    n: usize,
    cols: BTreeMap<Need, ColData>,
    pos: Vec<Pos>,
}

impl Batch {
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
        self.cols.get(&need).map_or(Val::Null, |c| c.get(i))
    }

    /// Rows of the batch for which `f` holds, among `sel`.
    fn select(&self, f: &BFilter, sel: &[bool]) -> Vec<bool> {
        match f {
            filter::Filter::And(fs) => {
                let mut s = sel.to_vec();
                for x in fs {
                    if !s.iter().any(|b| *b) {
                        break;
                    }
                    s = self.select(x, &s);
                }
                s
            }
            filter::Filter::Or(fs) => {
                let mut out = vec![false; self.n];
                for x in fs {
                    let rest: Vec<bool> = sel.iter().zip(&out).map(|(s, o)| *s && !o).collect();
                    if !rest.iter().any(|b| *b) {
                        break;
                    }
                    for (o, y) in out.iter_mut().zip(self.select(x, &rest)) {
                        *o |= y;
                    }
                }
                out
            }
            filter::Filter::Not(x) => {
                let y = self.select(x, sel);
                sel.iter().zip(y).map(|(s, y)| *s && !y).collect()
            }
            filter::Filter::Cmp(e, t) => {
                let dict = match e {
                    BExpr::Col(id) => self.cols.get(&Need::Col(*id)),
                    BExpr::Path(id, _, key) => self.cols.get(&Need::Path(*id, key.clone())),
                    _ => None,
                };
                if let Some(ColData::Vec { v: Vector::Str(s), json }) = dict {
                    let mut memo: Vec<Option<bool>> = vec![None; s.dict.len()];
                    return (0..self.n)
                        .map(|i| {
                            sel[i] && {
                                let c = s.codes[i];
                                c != super::vector::NULL_CODE
                                    && *memo[c as usize].get_or_insert_with(|| {
                                        let text = &s.dict[c as usize];
                                        let v = if *json {
                                            serde_json::from_str::<Json>(text)
                                                .map(|j| Val::from_json(&j))
                                                .unwrap_or(Val::Null)
                                        } else {
                                            Val::Str(text.clone())
                                        };
                                        !v.is_null() && t.holds(&v)
                                    })
                            }
                        })
                        .collect();
                }
                (0..self.n)
                    .map(|i| {
                        sel[i] && {
                            let v = self.val(e, i);
                            !v.is_null() && t.holds(&v)
                        }
                    })
                    .collect()
            }
        }
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

/// Whether `f` may hold given what `may` says of each comparison.
fn may(f: &BFilter, oracle: &mut impl FnMut(&BExpr, &Test<Json>) -> bool) -> bool {
    match f {
        filter::Filter::And(fs) => fs.iter().all(|x| may(x, oracle)),
        filter::Filter::Or(fs) => fs.iter().any(|x| may(x, oracle)),
        filter::Filter::Not(_) => true,
        filter::Filter::Cmp(e, t) => oracle(e, t),
    }
}

/// Whether `a` is at most some string whose stored max is `s`. A stored
/// max may be a cut prefix of the real one, and then only `a`'s first
/// `s.len()` bytes can be compared.
fn below_max(a: &str, s: &str, strict: bool) -> bool {
    if s.len() + 4 > part::STAT_PREFIX {
        a.as_bytes()[..a.len().min(s.len())] <= *s.as_bytes()
    } else if strict {
        a < s
    } else {
        a <= s
    }
}

/// Whether a value in `[lo, hi]` may satisfy `t`. String bounds are
/// prefixes; the upper one may stand for longer strings.
fn range_may(t: &Test<Json>, lo: &Stat, hi: &Stat) -> bool {
    let le = |a: &Json, s: &Stat| -> bool {
        match (a, s) {
            (Json::String(a), Stat::Str(s)) => below_max(a, s, false),
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
            (Json::String(a), Stat::Str(s)) => below_max(a, s, true),
            (Json::Number(_), Stat::Int(_) | Stat::Float(_)) => {
                stat_val(s).total_cmp(&Val::from_json(a)) == Ordering::Greater
            }
            (Json::Bool(a), Stat::Bool(s)) => s > a,
            _ => false,
        }
    };
    match t {
        Test::Eq(a) => ge(a, lo) && le(a, hi),
        Test::In(l) => l.iter().any(|a| ge(a, lo) && le(a, hi)),
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

/// Whether a constant value satisfies `t`.
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

/// A query bound to a table, ready to run on parts and unsealed rows.
struct Plan<'a> {
    schema: &'a EventSchema,
    filter: BFilter,
    lookups: Vec<Lookup>,
    kind: Kind,
    /// Needs of the filter, read before the rest.
    first: BTreeSet<Need>,
    rest: BTreeSet<Need>,
    positions: bool,
    /// When the query stops, and its time limit.
    deadline: Option<(tokio::time::Instant, u64)>,
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
    /// Steps of a funnel or conditions of a retention aggregate.
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

/// A group: its key values, its states, and the bytes charged for each.
struct Group {
    keys: Vec<Val>,
    states: Vec<State>,
    key_mem: usize,
    state_mem: usize,
}

/// Group states of one aggregation stage, and the memory they hold.
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

    /// Accounts for state that held `old` bytes holding `new`, failing past the limit.
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

    /// Applies `f` to the states of the group with `keys`, creating it.
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

    /// Merges the groups of `o`, a partial result of the same stage.
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

    /// Finalized rows sorted by group key.
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

/// Rows kept for rows mode: the best `limit + 1` by order key and position.
struct Kept {
    rows: Vec<(Vec<Val>, Pos, Vec<Val>)>,
    desc: Vec<bool>,
    limit: usize,
    /// Bytes the rows hold, and the most they may.
    mem: usize,
    max_mem: usize,
}

fn row_mem(key: &[Val], row: &[Val]) -> usize {
    64 + key.iter().chain(row).map(|v| v.mem_size() + 32).sum::<usize>()
}

impl Kept {
    fn new(desc: Vec<bool>, limit: usize, max_mem: usize) -> Kept {
        Kept { rows: Vec::new(), desc, limit, mem: 0, max_mem }
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
        if self.rows.len() > 2 * (self.limit + 1) + 64 {
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
        self.rows.sort_by(|a, b| Kept::cmp(&desc, (&a.0, a.1), (&b.0, b.1)));
        self.desc = desc;
        self.rows.truncate(self.limit + 1);
        self.mem = self.rows.iter().map(|(k, _, r)| row_mem(k, r)).sum();
    }
}

/// A query's progress over the rows it has read: the whole query's, or one
/// part's partial result, which [`Run::absorb`] merges.
struct Run<'p, 'a> {
    plan: &'p Plan<'a>,
    stats: Stats,
    groups: Option<Groups>,
    kept: Option<Kept>,
    agg_fns: Vec<AggFn>,
}

impl<'p, 'a> Run<'p, 'a> {
    fn new(plan: &'p Plan<'a>, max_mem: usize) -> Run<'p, 'a> {
        let (groups, kept, agg_fns) = match &plan.kind {
            Kind::Rows { order, limit, .. } => {
                (None, Some(Kept::new(order.iter().map(|(_, d)| *d).collect(), *limit, max_mem)), Vec::new())
            }
            Kind::Aggregate { first, .. } => {
                (Some(Groups::new(max_mem)), None, first.aggregates.iter().map(|a| a.func.clone()).collect())
            }
        };
        Run { plan, stats: Stats::default(), groups, kept, agg_fns }
    }

    /// Merges `o`, a partial result over other rows.
    fn absorb(&mut self, o: Run<'_, '_>) -> Result<()> {
        self.stats.add(&o.stats);
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

    /// Feeds the selected rows of a batch to the aggregation or row set.
    fn consume(&mut self, b: &Batch, sel: &[bool]) -> Result<()> {
        let plan = &self.plan;
        match &plan.kind {
            Kind::Rows { columns, order, after, .. } => {
                let kept = self.kept.as_mut().ok_or_else(|| Error::internal("rows without a row set"))?;
                for i in (0..b.n).filter(|i| sel[*i]) {
                    let key: Vec<Val> = order.iter().map(|(e, _)| b.val(e, i)).collect();
                    let pos = b.pos.get(i).copied().unwrap_or_default();
                    if let Some((ak, ap)) = after
                        && Kept::cmp(&kept.desc, (&key, pos), (ak, *ap)) != Ordering::Greater
                    {
                        continue;
                    }
                    let row = columns.iter().map(|(_, e)| b.val(e, i)).collect();
                    kept.push(key, pos, row)?;
                }
            }
            Kind::Aggregate { first, .. } => {
                let groups = self.groups.as_mut().ok_or_else(|| Error::internal("aggregates without groups"))?;
                let fns: Vec<&AggFn> = first.aggregates.iter().map(|a| &a.func).collect();
                let agg_sel: Vec<Option<Vec<bool>>> =
                    first.aggregates.iter().map(|a| a.filter.as_ref().map(|f| b.select(f, sel))).collect();
                let conds: Vec<Vec<Vec<bool>>> =
                    first.aggregates.iter().map(|a| a.conditions.iter().map(|c| b.select(c, sel)).collect()).collect();
                let time = BExpr::Col(plan.schema.time_id());
                for i in (0..b.n).filter(|i| sel[*i]) {
                    let keys: Vec<Val> = first.group_by.iter().map(|(_, e)| b.val(e, i).canonical()).collect();
                    groups.update(keys, &fns, |states| {
                        for (ai, a) in first.aggregates.iter().enumerate() {
                            if agg_sel[ai].as_ref().is_some_and(|s| !s[i]) {
                                continue;
                            }
                            let event = match &a.func {
                                AggFn::WindowFunnel { mode, .. } => {
                                    let mask =
                                        conds[ai].iter().enumerate().fold(0u32, |m, (k, c)| m | ((c[i] as u32) << k));
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
                                        conds[ai].iter().enumerate().fold(0u32, |m, (k, c)| m | ((c[i] as u32) << k));
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

    fn lookups(&self, b: &mut Batch) {
        for (k, l) in self.plan.lookups.iter().enumerate() {
            if !self.plan.first.contains(&Need::Lookup(k)) && !self.plan.rest.contains(&Need::Lookup(k)) {
                continue;
            }
            let vals: Vec<Val> = (0..b.n)
                .map(|i| {
                    let key = b.val(&l.key, i);
                    if key.is_null() {
                        return l.default.as_ref().map_or(Val::Null, |d| b.val(d, i));
                    }
                    match l.map.get(&key.canonical_bytes()) {
                        Some(v) => v.clone(),
                        None => l.default.as_ref().map_or(Val::Null, |d| b.val(d, i)),
                    }
                })
                .collect();
            b.cols.insert(Need::Lookup(k), ColData::Vals(vals));
        }
    }

    /// Runs the query over unsealed rows.
    fn unsealed(&mut self, rows: &[(Pos, Row)]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.stats.unsealed_rows += rows.len() as u64;
        self.stats.rows_read += rows.len() as u64;
        let mut b = Batch { n: rows.len(), cols: BTreeMap::new(), pos: rows.iter().map(|(p, _)| *p).collect() };
        let needs: BTreeSet<Need> = self.plan.first.iter().chain(self.plan.rest.iter()).cloned().collect();
        for need in needs {
            let data = match &need {
                Need::Col(id) => {
                    let dflt = default_val(self.plan.schema.column_by_id(*id));
                    ColData::Vals(rows.iter().map(|(_, r)| r.get(id).map_or_else(|| dflt.clone(), datum_val)).collect())
                }
                Need::Path(id, key) => {
                    let path = json::path_segments(key).unwrap_or_default();
                    ColData::Vals(
                        rows.iter()
                            .map(|(_, r)| match r.get(id) {
                                Some(Datum::Json(j)) => json::leaf(j, &path).map_or(Val::Null, Val::from_json),
                                _ => Val::Null,
                            })
                            .collect(),
                    )
                }
                Need::Json(id) => {
                    ColData::Vals(rows.iter().map(|(_, r)| r.get(id).map_or(Val::Null, datum_val)).collect())
                }
                Need::Lookup(_) => continue,
            };
            b.cols.insert(need, data);
        }
        self.lookups(&mut b);
        let sel = b.select(&self.plan.filter, &vec![true; b.n]);
        self.consume(&b, &sel)
    }

    /// Reads need `need` of granules `g` of part `r`.
    async fn read_need(
        &self,
        rd: &Reading<'_>,
        r: &PartRef,
        meta: &PartMeta,
        need: &Need,
        g: std::ops::Range<usize>,
    ) -> Result<ColData> {
        let n = (meta.starts[g.end] - meta.starts[g.start]) as usize;
        Ok(match need {
            Need::Col(id) => match meta.find(&ColRef::User(*id)) {
                Some(ci) => ColData::Vec { v: rd.column(r, meta, ci, g).await?, json: false },
                None => ColData::Const(default_val(self.plan.schema.column_by_id(*id))),
            },
            Need::Path(id, key) => {
                if let Some(ci) = meta.find(&ColRef::Path { col: *id, key: key.clone() }) {
                    ColData::Vec { v: rd.column(r, meta, ci, g).await?, json: meta.cols[ci].json }
                } else if meta.may_share(*id, key)
                    && let Some(ci) = meta.find(&ColRef::Shared { col: *id, bucket: json::bucket(key) })
                {
                    probe::hit(Probe::JsonSharedPathRead);
                    let v = rd.column(r, meta, ci, g).await?;
                    let Vector::Str(s) = v else { return Err(Error::internal("shared bucket is not text")) };
                    ColData::Vals(
                        (0..n)
                            .map(|i| {
                                match s
                                    .get(i)
                                    .and_then(|t| serde_json::from_str::<serde_json::Map<String, Json>>(t).ok())
                                {
                                    Some(o) => o.get(key).map_or(Val::Null, Val::from_json),
                                    None => Val::Null,
                                }
                            })
                            .collect(),
                    )
                } else {
                    ColData::Const(Val::Null)
                }
            }
            Need::Json(id) => {
                let present = match meta.find(&ColRef::User(*id)) {
                    Some(ci) => rd.column(r, meta, ci, g.clone()).await?,
                    None => return Ok(ColData::Const(Val::Null)),
                };
                let mut leaves: Vec<Vec<(Vec<String>, Json)>> = vec![Vec::new(); n];
                let paths: Vec<(usize, String)> = meta.paths(*id).map(|(i, k)| (i, k.to_string())).collect();
                for (ci, key) in paths {
                    let segs = json::path_segments(&key).unwrap_or_default();
                    let data = ColData::Vec { v: rd.column(r, meta, ci, g.clone()).await?, json: meta.cols[ci].json };
                    for (i, l) in leaves.iter_mut().enumerate() {
                        let v = data.get(i);
                        if !v.is_null() {
                            l.push((segs.clone(), v.to_json()));
                        }
                    }
                }
                for b in 0..json::BUCKETS {
                    let Some(ci) = meta.find(&ColRef::Shared { col: *id, bucket: b }) else { continue };
                    let Vector::Str(s) = rd.column(r, meta, ci, g.clone()).await? else { continue };
                    for (i, l) in leaves.iter_mut().enumerate() {
                        if let Some(o) =
                            s.get(i).and_then(|t| serde_json::from_str::<serde_json::Map<String, Json>>(t).ok())
                        {
                            for (k, v) in o {
                                l.push((json::path_segments(&k).unwrap_or_default(), v));
                            }
                        }
                    }
                }
                ColData::Vals(
                    leaves
                        .into_iter()
                        .enumerate()
                        .map(
                            |(i, l)| {
                                if present.is_null(i) { Val::Null } else { Val::Json(Json::Object(json::unflatten(l))) }
                            },
                        )
                        .collect(),
                )
            }
            Need::Lookup(_) => ColData::Const(Val::Null),
        })
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

    /// Whether a part can hold matching rows, by what its metadata and
    /// dictionaries say of each comparison.
    async fn part_may(&self, rd: &Reading<'_>, r: &PartRef, meta: &PartMeta) -> Result<bool> {
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
        let schema = self.plan.schema;
        let mut by_dict = false;
        let ok = may(&self.plan.filter, &mut |e, t| match e {
            BExpr::Col(id) => match meta.find(&ColRef::User(*id)) {
                None => holds(&default_val(schema.column_by_id(*id)), t),
                Some(ci) if meta.cols[ci].nonnull == 0 => false,
                Some(ci) => match dicts.get(&ci) {
                    Some(d) => {
                        let hit = d.iter().any(|s| holds(&Val::Str(s.clone()), t));
                        by_dict |= !hit;
                        hit
                    }
                    None => true,
                },
            },
            BExpr::Path(id, _, key) => {
                meta.find(&ColRef::Path { col: *id, key: key.clone() }).is_some_and(|ci| meta.cols[ci].nonnull > 0)
                    || (meta.may_share(*id, key)
                        && meta.find(&ColRef::Shared { col: *id, bucket: json::bucket(key) }).is_some())
            }
            _ => true,
        });
        if !ok && by_dict {
            probe::hit(Probe::PartPrunedByDictionary);
        }
        Ok(ok)
    }

    /// The granules of a part that may hold matching rows.
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
        for g in 0..meta.granule_count() {
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
            let stat_ok = may(&self.plan.filter, &mut |e, t| {
                let stat = |s: &GranStat| match (&s.min, &s.max) {
                    (Some(lo), Some(hi)) => range_may(t, lo, hi),
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
                                    Test::In(l) => l.iter().collect(),
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
                                && !range_may(t, &l, &h)
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

    /// Runs the query over part `r`.
    async fn part(&mut self, rd: &Reading<'_>, r: &PartRef, meta: Arc<PartMeta>) -> Result<()> {
        if !self.part_may(rd, r, &meta).await? {
            return Ok(());
        }
        let mask = rd.mask(r).await?;
        let keep = self.granules(rd, r, &meta, &mask).await?;
        self.stats.granules += meta.granule_count() as u64;
        if !keep.iter().any(|k| *k) {
            return Ok(());
        }
        self.stats.parts_read += 1;
        let mut g = 0;
        while g < keep.len() {
            if !keep[g] {
                g += 1;
                continue;
            }
            let mut end = g + 1;
            while end < keep.len() && keep[end] && end - g < 16 {
                end += 1;
            }
            self.granule_run(rd, r, &meta, &mask, g..end).await?;
            g = end;
        }
        Ok(())
    }

    async fn granule_run(
        &mut self,
        rd: &Reading<'_>,
        r: &PartRef,
        meta: &PartMeta,
        mask: &Runs,
        g: std::ops::Range<usize>,
    ) -> Result<()> {
        // Granules read from the cache never wait, so the query yields and
        // checks its deadline here, as reads from the store would let it.
        if let Some((at, max_ms)) = self.plan.deadline
            && tokio::time::Instant::now() >= at
        {
            return Err(timed_out(max_ms));
        }
        tokio::task::consume_budget().await;
        let rows = meta.rows_of(g.clone());
        let n = (rows.end - rows.start) as usize;
        self.stats.granules_read += (g.end - g.start) as u64;
        self.stats.rows_read += n as u64;
        let mut b = Batch { n, cols: BTreeMap::new(), pos: Vec::new() };
        for need in self.plan.first.clone() {
            if !matches!(need, Need::Lookup(_)) {
                let d = self.read_need(rd, r, meta, &need, g.clone()).await?;
                b.cols.insert(need, d);
            }
        }
        let base: Vec<bool> = (0..n).map(|i| !mask.contains(rows.start as u32 + i as u32)).collect();
        let lookups_first = self.plan.first.iter().any(|n| matches!(n, Need::Lookup(_)));
        if lookups_first {
            for need in self.plan.rest.clone() {
                if !matches!(need, Need::Lookup(_)) && !b.cols.contains_key(&need) {
                    let d = self.read_need(rd, r, meta, &need, g.clone()).await?;
                    b.cols.insert(need, d);
                }
            }
            self.lookups(&mut b);
        }
        let sel = b.select(&self.plan.filter, &base);
        if !sel.iter().any(|s| *s) {
            return Ok(());
        }
        for need in self.plan.rest.clone() {
            if !matches!(need, Need::Lookup(_)) && !b.cols.contains_key(&need) {
                let d = self.read_need(rd, r, meta, &need, g.clone()).await?;
                b.cols.insert(need, d);
            }
        }
        if !lookups_first {
            self.lookups(&mut b);
        }
        if self.plan.positions {
            b.pos = self.positions(rd, r, meta, g).await?;
        }
        self.consume(&b, &sel)
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

/// Whether part `r` may hold rows matching `f`, by its reference alone.
fn ref_may(f: &BFilter, r: &PartRef, time_id: u32) -> bool {
    may(f, &mut |e, t| match e {
        BExpr::Col(id) if *id == time_id => range_may(t, &Stat::Int(r.min_time), &Stat::Int(r.max_time)),
        _ => true,
    })
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

/// A query's input: the references and unsealed rows it reads.
pub struct Input {
    pub refs: Vec<PartRef>,
    pub unsealed: Vec<(Pos, Row)>,
}

/// Reads the references and unsealed rows a query over `[from, to)` needs,
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

/// Builds the maps of the query's lookups at the transaction's snapshot.
async fn lookup_maps(
    txn: &mut Txn,
    q: &Query,
    limits: &Limits,
) -> Result<Vec<(String, Arc<DetHashMap<Vec<u8>, Val>>)>> {
    let mut out = Vec::new();
    let mut budget = limits.lookup_max_bytes;
    let mut tables: BTreeMap<&str, Arc<(crate::table::schema::TableSchema, Vec<Row>)>> = BTreeMap::new();
    for (name, l) in &q.lookups {
        let table = match tables.get(l.table.as_str()) {
            Some(t) => t.clone(),
            None => {
                let t = Arc::new(crate::table::rows_within(txn, &l.table, &mut budget).await.map_err(|e| match e {
                    Error::TooLarge(_) => {
                        Error::TooLarge(format!("the query's lookup tables exceed {} bytes", limits.lookup_max_bytes))
                    }
                    e => e,
                })?);
                tables.insert(&l.table, t.clone());
                t
            }
        };
        let (ts, rows) = (&table.0, &table.1);
        if ts.primary_key.len() != 1 {
            return Err(Error::invalid(format!("lookup table {:?} needs a single-column primary key", l.table)));
        }
        let key_id = ts.column(&ts.primary_key[0]).map(|c| c.id).ok_or_else(|| Error::internal("pk column missing"))?;
        let val_id = ts
            .column(&l.value)
            .map(|c| c.id)
            .ok_or_else(|| Error::invalid(format!("lookup table {:?} has no column {:?}", l.table, l.value)))?;
        let mut map = DetHashMap::default();
        for r in rows {
            let k = r.get(&key_id).map_or(Val::Null, datum_val);
            let v = r.get(&val_id).map_or(Val::Null, datum_val);
            map.insert(k.canonical_bytes(), v);
        }
        out.push((name.clone(), Arc::new(map)));
    }
    Ok(out)
}

/// Runs `q` on table `s` at the transaction's snapshot.
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
    let from = q.from.max(s.retained_from(shared.time_ms() as i64));
    let mut conds = vec![
        filter::Filter::Cmp(time.clone(), Test::Ge(json!(from))),
        filter::Filter::Cmp(time.clone(), Test::Lt(json!(q.to))),
    ];
    if let Some(f) = &q.filter {
        conds.push(binder.filter(f)?);
    }
    let filter = filter::Filter::And(conds);
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
    let mut first = BTreeSet::new();
    filter_needs(&filter, &mut first, &lookups);
    let mut rest = BTreeSet::new();
    let mut positions = false;
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
                if let Some(f) = &a.filter {
                    filter_needs(f, &mut rest, &lookups);
                }
                a.conditions.iter().for_each(|c| filter_needs(c, &mut rest, &lookups));
                if matches!(a.func, AggFn::WindowFunnel { .. }) {
                    rest.insert(Need::Col(s.time_id()));
                    positions = true;
                }
            }
        }
    }
    let rest: BTreeSet<Need> = rest.difference(&first).cloned().collect();
    let input = input(txn, s, from, q.to).await?;
    let deadline = Some((tokio::time::Instant::now() + std::time::Duration::from_millis(limits.max_ms), limits.max_ms));
    let plan = Plan { schema: s, filter, lookups, kind, first, rest, positions, deadline };
    let mut run = Run::new(&plan, limits.max_memory_bytes);
    run.stats = Stats { source: "columns".into(), nodes: 1, parts: input.refs.len() as u64, ..Default::default() };
    let read_stats = ReadStats::default();
    let rd = Reading { shared, db, cache: Caching::Shared, stats: &read_stats };
    let refs: Vec<&PartRef> = input
        .refs
        .iter()
        .filter(|r| {
            let may = ref_may(&plan.filter, r, s.time_id());
            if !may {
                probe::hit(Probe::PartPrunedByRef);
            }
            may
        })
        .collect();
    // Parts are read concurrently into partial results, each holding at most
    // its share of the state limit, and merged in part order, so the merged
    // result grows as a serial read's would and the limit does not depend on
    // which part arrives first. A part whose groups outgrow its share is
    // read again into the merged result at its turn; rows mode holds rows the same way.
    let n = shared.cfg.event_read_concurrency.max(1);
    let share = limits.max_memory_bytes / n;
    let reads: Vec<_> = refs
        .iter()
        .map(|r| {
            let (mut partial, rd) = (Run::new(&plan, share), &rd);
            async move {
                let read = match rd.meta(r).await {
                    Ok(meta) => partial.part(rd, r, meta).await,
                    Err(e) => Err(e),
                };
                (partial, read)
            }
        })
        .collect();
    let mut reads = stream::iter(reads).buffered(n);
    for r in &refs {
        let (partial, read) = reads.next().await.ok_or_else(|| Error::internal("query reads ended early"))?;
        match read {
            Ok(()) => run.absorb(partial)?,
            Err(Error::TooLarge(_))
                if partial.groups.as_ref().is_some_and(Groups::full)
                    || partial.kept.as_ref().is_some_and(Kept::full) =>
            {
                run.part(&rd, r, rd.meta(r).await?).await?;
            }
            Err(e) => return Err(e),
        }
        if read_stats.bytes.load(Relaxed) > limits.max_bytes_read {
            return Err(Error::TooLarge(format!(
                "the query read more than {} bytes; narrow its time range or filter",
                limits.max_bytes_read
            )));
        }
    }
    run.unsealed(&input.unsealed)?;
    run.stats.bytes_read = read_stats.bytes.load(Relaxed);
    run.stats.store_gets = read_stats.gets.load(Relaxed);
    finish(run, q, limits)
}

fn finish(run: Run<'_, '_>, q: &Query, limits: &Limits) -> Result<Output> {
    let Run { plan, stats, groups, kept, agg_fns } = run;
    match &plan.kind {
        Kind::Rows { columns, limit, .. } => {
            let limit = *limit;
            let mut kept = kept.ok_or_else(|| Error::internal("rows without a row set"))?;
            kept.trim();
            let more = kept.rows.len() > limit;
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

/// Aggregates first-stage rows, whose columns are `names`, by `s`.
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
            return Err(Error::invalid("second-stage aggregates take no where, funnels or retention"));
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

/// The rows of `r` matching `f` that its mask does not already delete, as
/// sorted offsets; used by deletes.
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
    if !ref_may(&filter, r, s.time_id()) {
        return Ok(Vec::new());
    }
    let mut first = BTreeSet::new();
    filter_needs(&filter, &mut first, &[]);
    let plan = Plan {
        schema: s,
        filter,
        lookups: Vec::new(),
        kind: Kind::Aggregate { first: BoundStage { group_by: Vec::new(), aggregates: Vec::new() }, then: None },
        first,
        rest: BTreeSet::new(),
        positions: false,
        deadline: None,
    };
    let run = Run::new(&plan, usize::MAX);
    let read_stats = ReadStats::default();
    let rd = Reading { shared, db, cache: Caching::Shared, stats: &read_stats };
    let meta = rd.meta(r).await?;
    if !run.part_may(&rd, r, &meta).await? {
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
        let mut b = Batch { n, cols: BTreeMap::new(), pos: Vec::new() };
        for need in run.plan.first.clone() {
            let d = run.read_need(&rd, r, &meta, &need, g..g + 1).await?;
            b.cols.insert(need, d);
        }
        let base: Vec<bool> = (0..n).map(|i| !mask.contains(rows.start as u32 + i as u32)).collect();
        let sel = b.select(&run.plan.filter, &base);
        out.extend(sel.iter().enumerate().filter(|(_, s)| **s).map(|(i, _)| rows.start as u32 + i as u32));
    }
    Ok(out)
}

/// Whether unsealed row `row` matches `f` within `[from, to)`; used by deletes.
pub fn row_matches(s: &EventSchema, row: &Row, f: &Filter, from: i64, to: i64) -> Result<bool> {
    let names = BTreeMap::new();
    let binder = Binder { schema: s, lookups: &names };
    let t = s.time_of(row);
    if t < from || t >= to {
        return Ok(false);
    }
    let bf = binder.filter(f)?;
    let mut needs = BTreeSet::new();
    filter_needs(&bf, &mut needs, &[]);
    let plan = Plan {
        schema: s,
        filter: bf,
        lookups: Vec::new(),
        kind: Kind::Aggregate { first: BoundStage { group_by: Vec::new(), aggregates: Vec::new() }, then: None },
        first: needs,
        rest: BTreeSet::new(),
        deadline: None,
        positions: false,
    };
    let run = Run::new(&plan, usize::MAX);
    let mut b = Batch { n: 1, cols: BTreeMap::new(), pos: vec![Pos::default()] };
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
    Ok(b.select(&run.plan.filter, &[true])[0])
}
