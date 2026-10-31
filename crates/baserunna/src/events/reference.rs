//! Reference evaluator for event queries.
//! Evaluates queries directly over JSON events with exact arithmetic.

use super::query::{AggFn, Expr, Filter, FunnelMode, Interval, LookupSpec, Mode, Query, Stage};
use crate::filter::{Filter as F, Test};
use serde_json::{Map, Value as Json, json};
use std::cmp::Ordering;
use std::collections::BTreeMap;

pub const DAY: i64 = 86_400_000;
/// Timestamp bounds from 0000-01-01T00:00:00Z to 9999-12-31T23:59:59.999Z.
pub const TIME_MIN: i64 = -62_167_219_200_000;
pub const TIME_MAX: i64 = 253_402_300_799_999;

/// Returns the number of days from 0000-01-01 to January 1 of year `y`.
fn days_before_year(y: i64) -> i64 {
    365 * y + (y + 3).div_euclid(4) - (y + 99).div_euclid(100) + (y + 399).div_euclid(400)
}

const EPOCH_DAYS: i64 = 719_528;

fn leap(y: i64) -> bool {
    y.rem_euclid(4) == 0 && (y.rem_euclid(100) != 0 || y.rem_euclid(400) == 0)
}

fn month_len(y: i64, m: i64) -> i64 {
    match m {
        2 if leap(y) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Converts a day offset from 1970-01-01 to `(year, month, day)`.
pub fn civil(days: i64) -> (i64, i64, i64) {
    let d = days + EPOCH_DAYS;
    let mut y = (d as f64 / 365.2425).floor() as i64;
    while days_before_year(y) > d {
        y -= 1;
    }
    while days_before_year(y + 1) <= d {
        y += 1;
    }
    let mut rest = d - days_before_year(y);
    let mut m = 1;
    while rest >= month_len(y, m) {
        rest -= month_len(y, m);
        m += 1;
    }
    (y, m, rest + 1)
}

/// Converts `(year, month, day)` to a day offset from 1970-01-01.
pub fn days_of(y: i64, m: i64, d: i64) -> i64 {
    days_before_year(y) - EPOCH_DAYS + (1..m).map(|k| month_len(y, k)).sum::<i64>() + d - 1
}

/// Returns the start timestamp of the bucket containing `t` at `offset_ms` from UTC.
pub fn bucket(t: i64, iv: Interval, offset_ms: i64) -> Option<i64> {
    if !(TIME_MIN..=TIME_MAX).contains(&t) {
        return None;
    }
    let local = t + offset_ms;
    let start = match iv {
        Interval::Fixed(w) => local.div_euclid(w) * w,
        Interval::Weeks(n) => {
            let monday = days_of(1969, 12, 29);
            let week = (local.div_euclid(DAY) - monday).div_euclid(7);
            (monday + week.div_euclid(n) * n * 7) * DAY
        }
        Interval::Months(n) => {
            let (y, m, _) = civil(local.div_euclid(DAY));
            let month = (y - 1970) * 12 + m - 1;
            let first = month.div_euclid(n) * n;
            days_of(1970 + first.div_euclid(12), first.rem_euclid(12) + 1, 1) * DAY
        }
    };
    Some(start - offset_ms)
}

/// Formats `t` as RFC 3339 at `offset_min` minutes from UTC.
pub fn format_rfc3339(t: i64, offset_min: i64) -> String {
    let local = t + offset_min * 60_000;
    let (y, mo, d) = civil(local.div_euclid(DAY));
    let ms = local.rem_euclid(DAY);
    let (h, mi, s, frac) = (ms / 3_600_000, ms / 60_000 % 60, ms / 1000 % 60, ms % 1000);
    let zone = match offset_min {
        0 => "Z".to_string(),
        o => format!("{}{:02}:{:02}", if o < 0 { '-' } else { '+' }, o.abs() / 60, o.abs() % 60),
    };
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{frac:03}{zone}")
}

/// Parses an RFC 3339 timestamp or plain date into milliseconds from the Unix epoch.
pub fn parse_rfc3339(s: &str) -> Option<i64> {
    let n = |r: std::ops::Range<usize>| -> Option<i64> {
        let t = s.get(r)?;
        t.bytes().all(|b| b.is_ascii_digit()).then(|| t.parse().ok())?
    };
    let (y, mo, d) = (n(0..4)?, n(5..7)?, n(8..10)?);
    if !(1..=12).contains(&mo) || d < 1 || d > month_len(y, mo) {
        return None;
    }
    let day = days_of(y, mo, d) * DAY;
    if s.len() == 10 {
        return Some(day);
    }
    let (h, mi, sec, ms) = (n(11..13)?, n(14..16)?, n(17..19)?, n(20..23)?);
    let zone = s.get(23..)?;
    let off = match zone {
        "Z" => 0,
        z if z.len() == 6 => {
            let o = z[1..3].parse::<i64>().ok()? * 60 + z[4..6].parse::<i64>().ok()?;
            if z.starts_with('-') { -o } else { o }
        }
        _ => return None,
    };
    Some(day + ((h * 60 + mi - off) * 60 + sec) * 1000 + ms)
}

#[derive(Clone, Copy, Debug)]
enum Num {
    I(i64),
    F(f64),
}

fn num(n: &serde_json::Number) -> Num {
    match n.as_i64() {
        Some(i) => Num::I(i),
        None => Num::F(n.as_f64().unwrap_or(f64::NAN)),
    }
}

/// Compares integer `i` with finite float `f` by exact value.
fn int_float(i: i64, f: f64) -> Ordering {
    const TWO_63: f64 = 9_223_372_036_854_775_808.0;
    if f >= TWO_63 {
        return Ordering::Less;
    }
    if f < -TWO_63 {
        return Ordering::Greater;
    }
    let floor = f.floor();
    match i.cmp(&(floor as i64)) {
        Ordering::Equal if f > floor => Ordering::Less,
        o => o,
    }
}

fn num_cmp(a: Num, b: Num) -> Ordering {
    match (a, b) {
        (Num::I(x), Num::I(y)) => x.cmp(&y),
        (Num::F(x), Num::F(y)) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
        (Num::I(x), Num::F(y)) => int_float(x, y),
        (Num::F(x), Num::I(y)) => int_float(y, x).reverse(),
    }
}

/// Compares two JSON values for filter equality: numbers by exact value,
/// arrays and objects recursively, and false across different types.
pub fn json_eq(a: &Json, b: &Json) -> bool {
    match (a, b) {
        (Json::Number(x), Json::Number(y)) => num_cmp(num(x), num(y)) == Ordering::Equal,
        (Json::Array(x), Json::Array(y)) => x.len() == y.len() && x.iter().zip(y).all(|(x, y)| json_eq(x, y)),
        (Json::Object(x), Json::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| json_eq(v, w)))
        }
        _ => a == b,
    }
}

/// Compares two JSON values of the same scalar type.
fn order(a: &Json, b: &Json) -> Option<Ordering> {
    match (a, b) {
        (Json::Number(x), Json::Number(y)) => Some(num_cmp(num(x), num(y))),
        (Json::String(x), Json::String(y)) => Some(x.as_bytes().cmp(y.as_bytes())),
        (Json::Bool(x), Json::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Converts integral floats within the `i64` range to JSON integers.
pub fn canon(v: &Json) -> Json {
    match v {
        Json::Number(n) => match num(n) {
            Num::F(f) if f.fract() == 0.0 && (-9.223372036854776e18..9.223372036854776e18).contains(&f) => {
                json!(f as i64)
            }
            _ => v.clone(),
        },
        Json::Array(a) => Json::Array(a.iter().map(canon).collect()),
        Json::Object(o) => Json::Object(o.iter().map(|(k, v)| (k.clone(), canon(v))).collect()),
        _ => v.clone(),
    }
}

fn identity(v: &Json) -> String {
    canon(v).to_string()
}

fn rank(v: &Json) -> u8 {
    match v {
        Json::Null => 0,
        Json::Bool(_) => 1,
        Json::Number(_) => 2,
        Json::String(_) => 3,
        _ => 4,
    }
}

/// Total order for sorting, `min`, and `max`: null, booleans, numbers,
/// strings, then arrays and objects by canonical JSON.
pub fn total(a: &Json, b: &Json) -> Ordering {
    match (rank(a), rank(b)) {
        (x, y) if x != y => x.cmp(&y),
        (4, 4) => identity(a).as_bytes().cmp(identity(b).as_bytes()),
        _ => order(a, b).unwrap_or(Ordering::Equal),
    }
}

pub fn same(a: &Json, b: &Json) -> bool {
    json_eq(a, b)
}

/// Normalizes an inserted JSON value: removes null object fields, normalizes
/// `-0.0` to `0.0`, and rejects integers outside `i64`.
fn normalize(v: &Json) -> Result<Json, String> {
    Ok(match v {
        Json::Object(o) => Json::Object(
            o.iter()
                .filter(|(_, x)| !x.is_null())
                .map(|(k, x)| Ok((k.clone(), normalize(x)?)))
                .collect::<Result<_, String>>()?,
        ),
        Json::Array(a) => Json::Array(a.iter().map(normalize).collect::<Result<_, _>>()?),
        Json::Number(n) if n.as_i64().is_none() && n.as_u64().is_some() => return Err(format!("{n} beyond i64")),
        Json::Number(n) if n.as_i64().is_none() && n.as_f64() == Some(0.0) => json!(0.0),
        _ => v.clone(),
    })
}

/// Extracts the leaf value at `path` from `o`, returning `Null` for missing paths or non-empty objects.
fn leaf(o: &Json, path: &[String]) -> Json {
    let mut cur = o;
    for seg in path {
        match cur.as_object().and_then(|m| m.get(seg)) {
            Some(v) => cur = v,
            None => return Json::Null,
        }
    }
    match cur {
        Json::Object(m) if !m.is_empty() => Json::Null,
        v => v.clone(),
    }
}

const LIMBS: usize = 72;

/// Exact accumulator for integers and floats in units of 2^-1074, stored as
/// unnormalized signed base-2^32 limbs.
#[derive(Clone)]
struct Exact(Vec<i128>);

impl Exact {
    fn new() -> Exact {
        Exact(vec![0; LIMBS])
    }

    fn add_shifted(&mut self, mag: u128, pos: u32, neg: bool) {
        let (i, sh) = ((pos / 32) as usize, pos % 32);
        let wide = mag << sh;
        for k in 0..4 {
            let d = ((wide >> (32 * k)) & 0xFFFF_FFFF) as i128;
            self.0[i + k] += if neg { -d } else { d };
        }
    }

    fn add_f64(&mut self, f: f64) {
        if f == 0.0 {
            return;
        }
        let bits = f.to_bits();
        let e = ((bits >> 52) & 0x7FF) as u32;
        let frac = bits & ((1 << 52) - 1);
        let (m, pos) = if e == 0 { (frac, 0) } else { (frac | 1 << 52, e - 1) };
        self.add_shifted(m as u128, pos, f < 0.0);
    }

    fn add_int(&mut self, i: i64) {
        self.add_shifted(i.unsigned_abs() as u128, 1074, i < 0);
    }

    fn sign_mag(&self) -> (bool, Vec<u32>) {
        let norm = |src: &[i128]| -> (Vec<u32>, i128) {
            let mut d = src.to_vec();
            for k in 0..LIMBS - 1 {
                let c = d[k].div_euclid(1 << 32);
                d[k] -= c << 32;
                d[k + 1] += c;
            }
            let top = d[LIMBS - 1];
            (d[..LIMBS - 1].iter().map(|x| *x as u32).collect(), top)
        };
        let (mag, top) = norm(&self.0);
        if top >= 0 {
            assert!(top < 1 << 32);
            let mut mag = mag;
            mag.push(top as u32);
            return (false, mag);
        }
        let negated: Vec<i128> = self.0.iter().map(|x| -x).collect();
        let (mut mag, top) = norm(&negated);
        assert!((0..1 << 32).contains(&top));
        mag.push(top as u32);
        (true, mag)
    }

    fn value(&self) -> f64 {
        let (neg, mag) = self.sign_mag();
        let v = round(&mag, 0, false);
        if neg { -v } else { v }
    }

    fn mean(&self, n: u64) -> f64 {
        let (neg, mag) = self.sign_mag();
        let mut shifted = vec![0u32, 0u32];
        shifted.extend(mag);
        let mut q = vec![0u32; shifted.len()];
        let mut rem: u128 = 0;
        for i in (0..shifted.len()).rev() {
            let cur = (rem << 32) | shifted[i] as u128;
            q[i] = (cur / n as u128) as u32;
            rem = cur % n as u128;
        }
        let v = round(&q, 64, rem != 0);
        if neg { -v } else { v }
    }
}

/// Rounds `mag * 2^-(1074 + k)` to the nearest `f64` with ties to even, where
/// `sticky` indicates non-zero trailing bits.
fn round(mag: &[u32], k: u32, sticky: bool) -> f64 {
    let bit = |i: i64| i >= 0 && (mag[(i / 32) as usize] >> (i % 32)) & 1 == 1;
    let Some(top) = (0..mag.len() as i64 * 32).rev().find(|i| bit(*i)) else { return 0.0 };
    let cut = (top - 52).max(k as i64);
    let mut kept: u64 = 0;
    for i in (cut..=top).rev() {
        kept = kept << 1 | bit(i) as u64;
    }
    let round_bit = bit(cut - 1);
    let rest = sticky || (0..cut - 1).any(bit);
    if round_bit && (rest || kept & 1 == 1) {
        kept += 1;
    }
    let mut s = cut - k as i64;
    if kept == 1 << 53 {
        kept >>= 1;
        s += 1;
    }
    if s == 0 {
        return f64::from_bits(kept);
    }
    let e = s as u64 + 1;
    if e >= 2047 {
        return f64::INFINITY;
    }
    f64::from_bits(e << 52 | (kept & ((1 << 52) - 1)))
}

/// Computes the exact sum of `vals` as an `i64` if all inputs are integers or
/// booleans and the total fits in `i64`, or as the nearest `f64` otherwise.
fn sum(vals: &[&Json]) -> Json {
    let (mut ex, mut ints, mut float) = (Exact::new(), 0i128, false);
    for v in vals {
        match v {
            Json::Bool(b) => {
                ints += *b as i128;
                ex.add_int(*b as i64);
            }
            Json::Number(n) => match num(n) {
                Num::I(i) => {
                    ints += i as i128;
                    ex.add_int(i);
                }
                Num::F(f) => {
                    float = true;
                    ex.add_f64(f);
                }
            },
            _ => {}
        }
    }
    match i64::try_from(ints) {
        Ok(i) if !float => json!(i),
        _ => float_json(ex.value()),
    }
}

fn float_json(f: f64) -> Json {
    if f.is_finite() { json!(f) } else { Json::Null }
}

/// Computes the exact mean of numeric and boolean values in `vals`, rounded once.
fn avg(vals: &[&Json]) -> Json {
    let mut ex = Exact::new();
    let mut n = 0u64;
    for v in vals {
        match v {
            Json::Bool(b) => ex.add_int(*b as i64),
            Json::Number(x) => match num(x) {
                Num::I(i) => ex.add_int(i),
                Num::F(f) => ex.add_f64(f),
            },
            _ => continue,
        }
        n += 1;
    }
    if n == 0 { Json::Null } else { float_json(ex.mean(n)) }
}

#[derive(Clone, Debug)]
pub struct Col {
    pub id: u32,
    pub name: String,
    pub ty: String,
    pub default: Option<Json>,
    /// First sequence number at which this column existed.
    pub since: u64,
}

#[derive(Clone, Debug)]
pub struct Row {
    pub seq: u64,
    pub vals: BTreeMap<u32, Json>,
}

#[derive(Clone, Debug)]
pub struct Table {
    pub key: String,
    pub columns: Vec<String>,
    pub rows: Vec<Map<String, Json>>,
}

/// Reference state for an event table and its lookup tables.
#[derive(Clone, Debug)]
pub struct Reference {
    pub cols: Vec<Col>,
    pub time: String,
    pub rows: Vec<Row>,
    pub tables: BTreeMap<String, Table>,
    next_id: u32,
    next_seq: u64,
}

/// Validates and normalizes `v` for a column of type `ty`.
fn typed(ty: &str, v: &Json) -> Result<Json, String> {
    let bad = || Err(format!("{v} is not a {ty}"));
    Ok(match ty {
        "time" => match v {
            Json::String(s) => json!(parse_rfc3339(s).ok_or("bad time")?),
            _ => match v.as_i64() {
                Some(t) if (TIME_MIN..=TIME_MAX).contains(&t) => json!(t),
                _ => return bad(),
            },
        },
        "int" => json!(v.as_i64().ok_or("not an int")?),
        "float" => match v.as_f64() {
            Some(f) if f.is_finite() => json!(f + 0.0),
            _ => return bad(),
        },
        "string" if v.is_string() => v.clone(),
        "bool" if v.is_boolean() => v.clone(),
        "json" if v.is_object() => normalize(v)?,
        _ => return bad(),
    })
}

impl Reference {
    pub fn new(columns: &[Json], time: &str, tables: BTreeMap<String, Table>) -> Reference {
        let mut r =
            Reference { cols: Vec::new(), time: time.into(), rows: Vec::new(), tables, next_id: 1, next_seq: 0 };
        for c in columns {
            r.add_column(c);
        }
        r
    }

    pub fn add_column(&mut self, c: &Json) {
        self.cols.push(Col {
            id: self.next_id,
            name: c["name"].as_str().unwrap().into(),
            ty: c["type"].as_str().unwrap().into(),
            default: c.get("default").cloned(),
            since: self.next_seq,
        });
        self.next_id += 1;
    }

    pub fn drop_column(&mut self, name: &str) {
        self.cols.retain(|c| c.name != name);
    }

    pub fn insert(&mut self, rows: &[Json]) -> Result<(), String> {
        let mut out = Vec::new();
        for (i, r) in rows.iter().enumerate() {
            let mut vals = BTreeMap::new();
            for c in &self.cols {
                // Reference tests evaluate `"now"` defaults at timestamp 0.
                let v = match (r.get(&c.name).filter(|v| !v.is_null()), &c.default) {
                    (Some(v), _) => typed(&c.ty, v)?,
                    (None, Some(Json::String(s))) if c.ty == "time" && s == "now" => json!(0),
                    (None, Some(d)) => typed(&c.ty, d)?,
                    (None, None) => continue,
                };
                vals.insert(c.id, v);
            }
            out.push(Row { seq: self.next_seq + i as u64, vals });
        }
        self.next_seq += rows.len() as u64;
        self.rows.extend(out);
        Ok(())
    }

    fn col(&self, name: &str) -> Option<&Col> {
        self.cols.iter().find(|c| c.name == name)
    }

    /// Reads column `c` from `row`, returning the column default if `row` precedes the column.
    fn cell(&self, row: &Row, c: &Col) -> Json {
        if row.seq < c.since {
            return c.default.as_ref().and_then(|d| typed(&c.ty, d).ok()).unwrap_or(Json::Null);
        }
        row.vals.get(&c.id).cloned().unwrap_or(Json::Null)
    }

    fn time_of(&self, row: &Row) -> i64 {
        let c = self.col(&self.time).expect("time column");
        self.cell(row, c).as_i64().unwrap_or(0)
    }

    /// Deletes events in `[from, to)` that match `f` and returns the deleted count.
    pub fn delete(&mut self, from: i64, to: i64, f: &Filter) -> Result<u64, String> {
        let ev = Ev { r: self, lookups: BTreeMap::new() };
        let f = ev.filter(f)?;
        let gone: Vec<u64> = self
            .rows
            .iter()
            .filter(|r| (from..to).contains(&self.time_of(r)) && ev.holds(r, &f))
            .map(|r| r.seq)
            .collect();
        self.rows.retain(|r| !gone.contains(&r.seq));
        Ok(gone.len() as u64)
    }

    /// Evaluates `q` against the reference state.
    pub fn query(&self, q: &Query) -> Result<Answer, String> {
        let mut ev = Ev { r: self, lookups: BTreeMap::new() };
        let mut lookups = BTreeMap::new();
        for (name, spec) in &q.lookups {
            lookups.insert(name.clone(), ev.lookup(spec)?);
        }
        ev.lookups = lookups;
        let filter = q.filter.as_ref().map(|f| ev.filter(f)).transpose()?;
        let rows: Vec<&Row> = self
            .rows
            .iter()
            .filter(|r| (q.from..q.to).contains(&self.time_of(r)) && filter.as_ref().is_none_or(|f| ev.holds(r, f)))
            .collect();
        match &q.mode {
            Mode::Rows { columns, .. } => {
                let cols: Vec<(String, RE)> = if columns.is_empty() {
                    self.cols.iter().map(|c| (c.name.clone(), RE::Col(c.clone(), Vec::new()))).collect()
                } else {
                    columns.iter().map(|c| Ok((c.name.clone(), ev.expr(&c.expr)?))).collect::<Result<_, String>>()?
                };
                let order: Vec<(RE, bool)> = if q.order_by.is_empty() {
                    vec![(RE::Col(self.col(&self.time).unwrap().clone(), Vec::new()), false)]
                } else {
                    q.order_by
                        .iter()
                        .map(|o| {
                            let e = cols.iter().find(|(n, _)| *n == o.name).ok_or("unknown order_by column")?;
                            Ok((e.1.clone(), o.desc))
                        })
                        .collect::<Result<_, String>>()?
                };
                let mut keyed: Vec<(Vec<Json>, u64, Vec<Json>)> = rows
                    .iter()
                    .map(|r| {
                        (
                            order.iter().map(|(e, _)| ev.value(r, e)).collect(),
                            r.seq,
                            cols.iter().map(|(_, e)| ev.value(r, e)).collect(),
                        )
                    })
                    .collect();
                keyed.sort_by(|a, b| {
                    for (i, (_, desc)) in order.iter().enumerate() {
                        let o = total(&a.0[i], &b.0[i]);
                        let o = if *desc { o.reverse() } else { o };
                        if o != Ordering::Equal {
                            return o;
                        }
                    }
                    a.1.cmp(&b.1)
                });
                Ok(Answer::Rows {
                    columns: cols.into_iter().map(|(n, _)| n).collect(),
                    rows: keyed.into_iter().map(|(_, _, r)| r).collect(),
                    limit: q.limit,
                })
            }
            Mode::Aggregate { first, then } => {
                let (names, out, kinds) = ev.first_stage(first, &rows)?;
                let (names, out, kinds) = match then {
                    None => (names, out, kinds),
                    Some(s) => match second_stage(&names, out, &kinds, s)? {
                        Some(x) => x,
                        None => {
                            order_of(&names_of(s), q)?;
                            return Ok(Answer::Unverifiable);
                        }
                    },
                };
                if out.len() > q.limit && !q.limited {
                    return Err(format!("{} groups", out.len()));
                }
                let order = order_of(&names, q)?;
                let exact_order = order.iter().all(|(i, _)| kinds[*i] == ColKind::Plain);
                Ok(Answer::Groups { columns: names, rows: out, order, limit: q.limit, exact_order })
            }
        }
    }
}

fn names_of(s: &Stage) -> Vec<String> {
    s.group_by.iter().map(|g| g.name.clone()).chain(s.aggregates.iter().map(|a| a.name.clone())).collect()
}

fn order_of(names: &[String], q: &Query) -> Result<Vec<(usize, bool)>, String> {
    q.order_by
        .iter()
        .map(|o| names.iter().position(|n| *n == o.name).map(|i| (i, o.desc)).ok_or("unknown order_by column".into()))
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColKind {
    Plain,
    /// List output from `retention`, ordered by internal encoding in the engine.
    List,
    /// Approximate output from `quantiles`, or `uniq` above 1,024 distinct values.
    Approx,
}

#[derive(Clone, Debug)]
pub enum Cell {
    V(Json),
    Quant(Vec<Option<(f64, f64)>>),
    About(f64, f64),
}

impl Cell {
    pub fn matches(&self, got: &Json) -> bool {
        let within = |g: &Json, lo: f64, hi: f64| g.as_f64().is_some_and(|g| lo <= g && g <= hi);
        match self {
            Cell::V(v) => same(v, got),
            Cell::About(lo, hi) => within(got, *lo, *hi),
            Cell::Quant(qs) => got.as_array().is_some_and(|a| {
                a.len() == qs.len()
                    && a.iter().zip(qs).all(|(g, q)| match q {
                        None => g.is_null(),
                        Some((lo, hi)) => within(g, *lo, *hi),
                    })
            }),
        }
    }

    fn plain(&self) -> Option<&Json> {
        match self {
            Cell::V(v) => Some(v),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum Answer {
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Json>>,
        limit: usize,
    },
    Groups {
        columns: Vec<String>,
        rows: Vec<Vec<Cell>>,
        order: Vec<(usize, bool)>,
        limit: usize,
        exact_order: bool,
    },
    /// Second-stage query over approximate first-stage outputs.
    Unverifiable,
}

impl Answer {
    /// Verifies engine output `got` and pagination metadata `pages` against this reference answer.
    pub fn check(&self, columns: &[String], got: &[Vec<Json>], pages: &[(usize, bool)]) -> Result<(), String> {
        match self {
            Answer::Unverifiable => Ok(()),
            Answer::Rows { columns: names, rows, limit } => {
                if names != columns {
                    return Err(format!("columns {columns:?}, want {names:?}"));
                }
                let mut at = 0;
                for (i, (n, more)) in pages.iter().enumerate() {
                    let want = (*limit).min(rows.len().saturating_sub(at));
                    let want_more = at + want < rows.len();
                    if *n != want || *more != want_more {
                        return Err(format!("page {i} has {n} rows, more {more}, want {want}, more {want_more}"));
                    }
                    at += n;
                }
                for (i, (g, w)) in got.iter().zip(rows).enumerate() {
                    if g.len() != w.len() || !g.iter().zip(w).all(|(a, b)| same(a, b)) {
                        return Err(format!("row {i}: got {g:?}, want {w:?}"));
                    }
                }
                Ok(())
            }
            Answer::Groups { columns: names, rows, order, limit, exact_order } => {
                if names != columns {
                    return Err(format!("columns {columns:?}, want {names:?}"));
                }
                let n = (*limit).min(rows.len());
                if got.len() != n {
                    return Err(format!("{} groups, want {n} of {}: want {rows:?}", got.len(), rows.len()));
                }
                let mut used = vec![false; rows.len()];
                for g in got {
                    let hit = rows.iter().enumerate().position(|(i, w)| {
                        !used[i] && w.len() == g.len() && w.iter().zip(g).all(|(c, v)| c.matches(v))
                    });
                    match hit {
                        Some(i) => used[i] = true,
                        None => return Err(format!("group {g:?} is not among {rows:?}")),
                    }
                }
                if !exact_order || order.is_empty() {
                    return Ok(());
                }
                let key_cmp = |a: &[&Json], b: &[&Json]| {
                    for (k, (_, desc)) in order.iter().enumerate() {
                        let o = total(a[k], b[k]);
                        let o = if *desc { o.reverse() } else { o };
                        if o != Ordering::Equal {
                            return o;
                        }
                    }
                    Ordering::Equal
                };
                let got_keys: Vec<Vec<&Json>> =
                    got.iter().map(|g| order.iter().map(|(i, _)| &g[*i]).collect()).collect();
                if let Some(w) = got_keys.windows(2).position(|w| key_cmp(&w[0], &w[1]) == Ordering::Greater) {
                    return Err(format!("groups {} and {} are out of order", w, w + 1));
                }
                if let Some(last) = got_keys.last() {
                    for (i, w) in rows.iter().enumerate() {
                        let key: Vec<&Json> = order.iter().map(|(k, _)| w[*k].plain().unwrap()).collect();
                        if !used[i] && key_cmp(&key, last) == Ordering::Less {
                            return Err(format!("group {w:?} belongs before the last answered"));
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug)]
enum RE {
    Col(Col, Vec<String>),
    Lookup(String),
    Bucket(Box<RE>, Interval, i64),
}

#[derive(Clone, Debug)]
enum Op {
    Eq(Json),
    Ne(Json),
    Lt(Json),
    Le(Json),
    Gt(Json),
    Ge(Json),
    In(Vec<Json>),
    Prefix(Json),
    Contains(Json),
    Exists,
}

#[derive(Clone, Debug)]
enum RF {
    And(Vec<RF>),
    Or(Vec<RF>),
    Not(Box<RF>),
    Cmp(RE, Op),
}

struct BoundLookup {
    key: RE,
    default: Option<RE>,
    rows: Vec<(Json, Json)>,
}

struct Ev<'a> {
    r: &'a Reference,
    lookups: BTreeMap<String, BoundLookup>,
}

/// Validates and normalizes filter literal `a` for a column of type `ty`.
fn typed_arg(ty: &str, a: &Json) -> Result<Json, String> {
    let bad = || Err(format!("{a} does not compare with {ty}"));
    Ok(match ty {
        "time" => match a {
            Json::String(s) => json!(parse_rfc3339(s).ok_or("bad time")?),
            _ => json!(a.as_i64().ok_or("not a time")?),
        },
        "int" => json!(a.as_i64().ok_or("not an int")?),
        "float" if a.is_i64() => a.clone(),
        "float" if a.is_u64() => return bad(),
        "float" => match a.as_f64() {
            Some(f) if f.is_finite() => json!(f + 0.0),
            _ => return bad(),
        },
        "string" if a.is_string() => a.clone(),
        "bool" if a.is_boolean() => a.clone(),
        _ => return bad(),
    })
}

fn test(v: &Json, op: &Op) -> bool {
    match op {
        Op::Eq(a) => json_eq(v, a),
        Op::Ne(a) => !json_eq(v, a),
        Op::Lt(a) => order(v, a) == Some(Ordering::Less),
        Op::Le(a) => matches!(order(v, a), Some(Ordering::Less | Ordering::Equal)),
        Op::Gt(a) => order(v, a) == Some(Ordering::Greater),
        Op::Ge(a) => matches!(order(v, a), Some(Ordering::Greater | Ordering::Equal)),
        Op::In(l) => l.iter().any(|a| json_eq(v, a)),
        Op::Prefix(a) => matches!((v, a), (Json::String(s), Json::String(p)) if s.starts_with(p.as_str())),
        Op::Contains(a) => match (v, a) {
            (Json::String(s), Json::String(p)) => s.contains(p.as_str()),
            (Json::Array(items), x) => items.iter().any(|i| json_eq(i, x)),
            _ => false,
        },
        Op::Exists => true,
    }
}

impl Ev<'_> {
    fn expr(&self, e: &Expr) -> Result<RE, String> {
        Ok(match e {
            Expr::Col { name, path } => {
                let c = self.r.col(name).ok_or(format!("unknown column {name}"))?;
                if !path.is_empty() && c.ty != "json" {
                    return Err(format!("{name} is not a JSON column"));
                }
                RE::Col(c.clone(), path.clone())
            }
            Expr::Lookup(n) => {
                if !self.lookups.contains_key(n) {
                    return Err(format!("unknown lookup {n}"));
                }
                RE::Lookup(n.clone())
            }
            Expr::Bucket { of, interval, offset_ms } => match self.expr(of)? {
                RE::Col(c, p) if p.is_empty() && (c.ty == "time" || c.ty == "int") => {
                    RE::Bucket(Box::new(RE::Col(c, p)), *interval, *offset_ms)
                }
                _ => return Err("bucket requires a time or int column".into()),
            },
        })
    }

    fn lookup(&self, spec: &LookupSpec) -> Result<BoundLookup, String> {
        let t = self.r.tables.get(&spec.table).ok_or(format!("unknown table {}", spec.table))?;
        if !t.columns.contains(&spec.value) {
            return Err(format!("unknown column {}", spec.value));
        }
        let key = self.expr(&spec.key)?;
        if matches!(&key, RE::Col(c, p) if c.ty == "json" && p.is_empty()) {
            return Err("lookup key cannot be a JSON column without a path".into());
        }
        let default = spec.default.as_ref().map(|d| self.expr(d)).transpose()?;
        let rows = t
            .rows
            .iter()
            .map(|r| (r.get(&t.key).cloned().unwrap_or(Json::Null), r.get(&spec.value).cloned().unwrap_or(Json::Null)))
            .collect();
        Ok(BoundLookup { key, default, rows })
    }

    fn filter(&self, f: &Filter) -> Result<RF, String> {
        Ok(match f {
            F::And(fs) => RF::And(fs.iter().map(|x| self.filter(x)).collect::<Result<_, _>>()?),
            F::Or(fs) => RF::Or(fs.iter().map(|x| self.filter(x)).collect::<Result<_, _>>()?),
            F::Not(x) => RF::Not(Box::new(self.filter(x)?)),
            F::Cmp(e, t) => {
                let e = self.expr(e)?;
                let ty = match &e {
                    RE::Col(c, p) if c.ty == "json" && p.is_empty() => {
                        return Err("filter on a whole JSON column".into());
                    }
                    RE::Col(c, p) if p.is_empty() => Some(c.ty.clone()),
                    RE::Bucket(inner, ..) => match inner.as_ref() {
                        RE::Col(c, _) => Some(c.ty.clone()),
                        _ => None,
                    },
                    _ => None,
                };
                if let Some(ty) = &ty
                    && matches!(t, Test::Prefix(_) | Test::Contains(_))
                    && ty != "string"
                {
                    return Err(format!("{ty} does not support prefix or contains"));
                }
                let arg = |a: &Json| match &ty {
                    Some(ty) => typed_arg(ty, a),
                    None => normalize(a),
                };
                let op = match t {
                    Test::Eq(a) => Op::Eq(arg(a)?),
                    Test::Ne(a) => Op::Ne(arg(a)?),
                    Test::Lt(a) => Op::Lt(arg(a)?),
                    Test::Le(a) => Op::Le(arg(a)?),
                    Test::Gt(a) => Op::Gt(arg(a)?),
                    Test::Ge(a) => Op::Ge(arg(a)?),
                    Test::In(l) => Op::In(l.items().iter().map(arg).collect::<Result<_, _>>()?),
                    Test::Prefix(a) => Op::Prefix(arg(a)?),
                    Test::Contains(a) => Op::Contains(arg(a)?),
                    Test::Exists => Op::Exists,
                };
                RF::Cmp(e, op)
            }
        })
    }

    fn value(&self, row: &Row, e: &RE) -> Json {
        match e {
            RE::Col(c, p) if p.is_empty() => self.r.cell(row, c),
            RE::Col(c, p) => leaf(&self.r.cell(row, c), p),
            RE::Lookup(n) => {
                let l = &self.lookups[n];
                let key = self.value(row, &l.key);
                if !key.is_null()
                    && let Some((_, v)) = l.rows.iter().find(|(k, _)| json_eq(k, &key))
                {
                    return v.clone();
                }
                l.default.as_ref().map_or(Json::Null, |d| self.value(row, d))
            }
            RE::Bucket(inner, iv, off) => match self.value(row, inner).as_i64() {
                Some(t) => bucket(t, *iv, *off).map_or(Json::Null, |b| json!(b)),
                None => Json::Null,
            },
        }
    }

    fn holds(&self, row: &Row, f: &RF) -> bool {
        match f {
            RF::And(fs) => fs.iter().all(|x| self.holds(row, x)),
            RF::Or(fs) => fs.iter().any(|x| self.holds(row, x)),
            RF::Not(x) => !self.holds(row, x),
            RF::Cmp(e, op) => {
                let v = self.value(row, e);
                !v.is_null() && test(&v, op)
            }
        }
    }

    #[allow(clippy::type_complexity)]
    fn first_stage(&self, s: &Stage, rows: &[&Row]) -> Result<(Vec<String>, Vec<Vec<Cell>>, Vec<ColKind>), String> {
        let group: Vec<RE> = s.group_by.iter().map(|g| self.expr(&g.expr)).collect::<Result<_, _>>()?;
        struct Agg {
            func: AggFn,
            of: Option<RE>,
            filter: Option<RF>,
            conds: Vec<RF>,
        }
        let mut aggs = Vec::new();
        for a in &s.aggregates {
            let conds = match &a.func {
                AggFn::WindowFunnel { steps, .. } => steps.iter().map(|f| self.filter(f)).collect::<Result<_, _>>()?,
                AggFn::Retention(c) => c.iter().map(|f| self.filter(f)).collect::<Result<_, _>>()?,
                _ => Vec::new(),
            };
            aggs.push(Agg {
                func: a.func.clone(),
                of: a.of.as_ref().map(|e| self.expr(e)).transpose()?,
                filter: a.filter.as_ref().map(|f| self.filter(f)).transpose()?,
                conds,
            });
        }
        let mut groups: Vec<(Vec<Json>, Vec<&Row>)> = Vec::new();
        let mut index: BTreeMap<Vec<String>, usize> = BTreeMap::new();
        for r in rows {
            let keys: Vec<Json> = group.iter().map(|e| canon(&self.value(r, e))).collect();
            let id: Vec<String> = keys.iter().map(identity).collect();
            let i = *index.entry(id).or_insert_with(|| {
                groups.push((keys, Vec::new()));
                groups.len() - 1
            });
            groups[i].1.push(r);
        }
        if group.is_empty() && groups.is_empty() {
            groups.push((Vec::new(), Vec::new()));
        }
        let mut kinds = vec![ColKind::Plain; group.len()];
        for a in &aggs {
            kinds.push(match a.func {
                AggFn::Quantiles(_) => ColKind::Approx,
                AggFn::Retention(_) => ColKind::List,
                _ => ColKind::Plain,
            });
        }
        let mut out = Vec::new();
        for (keys, members) in groups {
            let mut row: Vec<Cell> = keys.into_iter().map(Cell::V).collect();
            for (ai, a) in aggs.iter().enumerate() {
                let rows: Vec<&Row> =
                    members.iter().copied().filter(|r| a.filter.as_ref().is_none_or(|f| self.holds(r, f))).collect();
                let vals: Vec<Json> =
                    rows.iter().map(|r| a.of.as_ref().map_or(Json::Null, |e| self.value(r, e))).collect();
                let cell = match &a.func {
                    AggFn::WindowFunnel { window_ms, mode, .. } => {
                        let evs: Vec<(i64, u64, Vec<bool>)> = rows
                            .iter()
                            .map(|r| (self.r.time_of(r), r.seq, a.conds.iter().map(|c| self.holds(r, c)).collect()))
                            .collect();
                        Cell::V(json!(funnel(evs, a.conds.len(), *window_ms, *mode)))
                    }
                    AggFn::Retention(_) => {
                        let met: Vec<bool> =
                            (0..a.conds.len()).map(|k| rows.iter().any(|r| self.holds(r, &a.conds[k]))).collect();
                        Cell::V(json!(met.iter().map(|m| met[0] && *m).collect::<Vec<_>>()))
                    }
                    f => {
                        let cell = scalar_agg(f, &vals, false);
                        if unverifiable(&cell) {
                            kinds[group.len() + ai] = ColKind::Approx;
                        }
                        cell
                    }
                };
                row.push(cell);
            }
            out.push(row);
        }
        Ok((names_of(s), out, kinds))
    }
}

fn unverifiable(cell: &Cell) -> bool {
    matches!(cell, Cell::About(..))
}

/// Evaluates a scalar aggregate over `vals`. If `lists` is true, `sum` adds
/// array elements positionally.
fn scalar_agg(f: &AggFn, vals: &[Json], lists: bool) -> Cell {
    let present: Vec<&Json> = vals.iter().filter(|v| !v.is_null()).collect();
    match f {
        AggFn::Count => Cell::V(json!(vals.len())),
        AggFn::Sum if lists => {
            let n = present.iter().filter_map(|v| v.as_array()).map(Vec::len).max().unwrap_or(0);
            if present.is_empty() {
                return Cell::V(json!(0));
            }
            Cell::V(Json::Array(
                (0..n)
                    .map(|i| {
                        sum(&present.iter().filter_map(|v| v.as_array().and_then(|a| a.get(i))).collect::<Vec<_>>())
                    })
                    .collect(),
            ))
        }
        AggFn::Sum => Cell::V(sum(&present)),
        AggFn::Avg if lists => Cell::V(Json::Null),
        AggFn::Avg => Cell::V(avg(&present)),
        AggFn::Min => Cell::V(present.iter().min_by(|a, b| total(a, b)).map_or(Json::Null, |v| canon(v))),
        AggFn::Max => Cell::V(present.iter().max_by(|a, b| total(a, b)).map_or(Json::Null, |v| canon(v))),
        AggFn::Uniq | AggFn::UniqExact => {
            let n = present.iter().map(|v| identity(v)).collect::<std::collections::BTreeSet<_>>().len();
            if matches!(f, AggFn::Uniq) && n > 1024 {
                Cell::About(n as f64 * 0.95, n as f64 * 1.05)
            } else {
                Cell::V(json!(n))
            }
        }
        AggFn::Quantiles(qs) => {
            let mut xs: Vec<f64> = if lists {
                Vec::new()
            } else {
                present.iter().filter(|v| v.is_number()).filter_map(|v| v.as_f64()).collect()
            };
            xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
            Cell::Quant(
                qs.iter()
                    .map(|q| {
                        if xs.is_empty() {
                            return None;
                        }
                        let at = q * (xs.len() - 1) as f64;
                        let (lo, hi) = (xs[at.floor() as usize], xs[at.ceil() as usize]);
                        let a = [lo * 0.99, lo * 1.01, hi * 0.99, hi * 1.01];
                        let min = a.iter().copied().fold(f64::INFINITY, f64::min);
                        let max = a.iter().copied().fold(f64::NEG_INFINITY, f64::max).min(f64::MAX);
                        Some((min - min.abs() * 1e-9, max + max.abs() * 1e-9))
                    })
                    .collect(),
            )
        }
        AggFn::WindowFunnel { .. } | AggFn::Retention(_) => Cell::V(Json::Null),
    }
}

/// Evaluates `window_funnel` over events sorted by `(timestamp, seq)`.
fn funnel(mut evs: Vec<(i64, u64, Vec<bool>)>, steps: usize, window_ms: i64, mode: FunnelMode) -> usize {
    evs.sort_by_key(|e| (e.0, e.1));
    let mut best = 0;
    for (i, (start, _, m)) in evs.iter().enumerate() {
        if !m[0] {
            continue;
        }
        let (mut level, mut last) = (1, *start);
        for (t, _, m) in &evs[i + 1..] {
            if level == steps || *t > start.saturating_add(window_ms) {
                break;
            }
            let next = m[level];
            if next && (!mode.strict_increase || *t > last) {
                level += 1;
                last = *t;
            } else if (mode.strict_order && !next) || (mode.strict_deduplication && m[..level].iter().any(|x| *x)) {
                break;
            }
        }
        best = best.max(level);
    }
    best
}

/// Evaluates the `then` stage over first-stage outputs, returning `None` if it
/// depends on approximate values or orders list outputs.
#[allow(clippy::type_complexity)]
fn second_stage(
    names: &[String],
    rows: Vec<Vec<Cell>>,
    kinds: &[ColKind],
    s: &Stage,
) -> Result<Option<(Vec<String>, Vec<Vec<Cell>>, Vec<ColKind>)>, String> {
    let out = |n: &str| names.iter().position(|x| x == n).ok_or(format!("unknown first-stage output {n}"));
    let bind = |e: &Expr| -> Result<(usize, Option<(Interval, i64)>), String> {
        match e {
            Expr::Col { name, path } if path.is_empty() => Ok((out(name)?, None)),
            Expr::Bucket { of, interval, offset_ms } => match of.as_ref() {
                Expr::Col { name, path } if path.is_empty() => Ok((out(name)?, Some((*interval, *offset_ms)))),
                _ => Err("then bucket must reference a first-stage output".into()),
            },
            _ => Err("then expression must reference a first-stage output".into()),
        }
    };
    let group: Vec<(usize, Option<(Interval, i64)>)> =
        s.group_by.iter().map(|g| bind(&g.expr)).collect::<Result<_, _>>()?;
    let mut aggs = Vec::new();
    for a in &s.aggregates {
        if a.filter.is_some() || matches!(a.func, AggFn::WindowFunnel { .. } | AggFn::Retention(_)) {
            return Err("second-stage aggregates do not support where, window_funnel, or retention".into());
        }
        aggs.push((a.func.clone(), a.of.as_ref().map(bind).transpose()?));
    }
    let reads = group.iter().map(|g| g.0).chain(aggs.iter().filter_map(|a| a.1.map(|o| o.0)));
    if reads.clone().any(|i| kinds[i] == ColKind::Approx) {
        return Ok(None);
    }
    if aggs
        .iter()
        .any(|(f, of)| matches!(f, AggFn::Min | AggFn::Max) && of.is_some_and(|o| kinds[o.0] == ColKind::List))
    {
        return Ok(None);
    }
    let rows: Vec<Vec<Json>> =
        rows.into_iter().map(|r| r.into_iter().map(|c| c.plain().map_or(Json::Null, canon)).collect()).collect();
    let eval = |(i, b): (usize, Option<(Interval, i64)>), row: &[Json]| -> Json {
        match b {
            None => row[i].clone(),
            Some((iv, off)) => row[i].as_i64().and_then(|t| bucket(t, iv, off)).map_or(Json::Null, |b| json!(b)),
        }
    };
    let mut groups: Vec<(Vec<Json>, Vec<&Vec<Json>>)> = Vec::new();
    let mut index: BTreeMap<Vec<String>, usize> = BTreeMap::new();
    for r in &rows {
        let keys: Vec<Json> = group.iter().map(|g| canon(&eval(*g, r))).collect();
        let id: Vec<String> = keys.iter().map(identity).collect();
        let i = *index.entry(id).or_insert_with(|| {
            groups.push((keys, Vec::new()));
            groups.len() - 1
        });
        groups[i].1.push(r);
    }
    if group.is_empty() && groups.is_empty() {
        groups.push((Vec::new(), Vec::new()));
    }
    let mut out_kinds: Vec<ColKind> =
        group.iter().map(|g| if g.1.is_some() { ColKind::Plain } else { kinds[g.0] }).collect();
    for (f, of) in &aggs {
        out_kinds.push(match f {
            AggFn::Quantiles(_) => ColKind::Approx,
            AggFn::Sum if of.is_some_and(|o| kinds[o.0] == ColKind::List) => ColKind::List,
            _ => ColKind::Plain,
        });
    }
    let mut out = Vec::new();
    for (keys, members) in groups {
        let mut row: Vec<Cell> = keys.into_iter().map(Cell::V).collect();
        for (k, (f, of)) in aggs.iter().enumerate() {
            let vals: Vec<Json> = members.iter().map(|r| of.map_or(Json::Null, |o| eval(o, r))).collect();
            let lists = of.is_some_and(|o| o.1.is_none() && kinds[o.0] == ColKind::List);
            let cell = scalar_agg(f, &vals, lists);
            if unverifiable(&cell) {
                out_kinds[group.len() + k] = ColKind::Approx;
            }
            row.push(cell);
        }
        out.push(row);
    }
    Ok(Some((names_of(s), out, out_kinds)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calendars_and_buckets() {
        assert_eq!(days_of(1970, 1, 1), 0);
        assert_eq!(civil(-1), (1969, 12, 31));
        assert_eq!(days_of(0, 1, 1) * DAY, TIME_MIN);
        assert_eq!(days_of(10000, 1, 1) * DAY - 1, TIME_MAX);
        assert_eq!(civil(days_of(2024, 2, 29)), (2024, 2, 29));
        for d in [-800_000, -719_528, -1, 0, 59, 11_016, 2_932_896] {
            let (y, m, dd) = civil(d);
            assert_eq!(days_of(y, m, dd), d);
        }
        assert_eq!(bucket(0, Interval::Weeks(1), 0), Some(-3 * DAY));
        assert_eq!(bucket(-1, Interval::Months(1), 0), Some(-31 * DAY));
        assert_eq!(bucket(0, Interval::Fixed(DAY), -3_600_000), Some(-DAY + 3_600_000));
        assert_eq!(parse_rfc3339(&format_rfc3339(-123_456_789, -330)), Some(-123_456_789));
        assert_eq!(parse_rfc3339("1970-01-02"), Some(DAY));
    }

    #[test]
    fn exact_sums_round_once() {
        let s = |xs: &[f64]| {
            let mut e = Exact::new();
            xs.iter().for_each(|x| e.add_f64(*x));
            e.value()
        };
        assert_eq!(s(&[0.1, 0.2]), 0.30000000000000004);
        assert_eq!(s(&[1e308, -1e308, 0.5]), 0.5);
        assert_eq!(s(&[1e20, 1.0, -1e20]), 1.0);
        assert_eq!(s(&[5e-324, 5e-324]), 1e-323);
        assert_eq!(s(&[-2.25, 1.5]), -0.75);
        assert_eq!(s(&[f64::MAX, f64::MAX]), f64::INFINITY);
        let mut e = Exact::new();
        [1.0, 2.0, 2.0].iter().for_each(|x| e.add_f64(*x));
        assert_eq!(e.mean(3), 5.0 / 3.0);
        e.add_int(i64::MIN);
        assert_eq!(e.value(), i64::MIN as f64 + 5.0);
        let mut e = Exact::new();
        [f64::MAX, f64::MAX].iter().for_each(|x| e.add_f64(*x));
        assert_eq!(e.mean(2), f64::MAX);
    }
}
