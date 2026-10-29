//! The event query language (SPEC §4.8): a time range, a filter, and either
//! columns to return or grouped aggregates, with an optional second stage.

use crate::error::{Error, Result};
use crate::filter::{self, Operand, Test};
use serde_json::{Map, Value as Json};
use std::collections::BTreeMap;

/// Most rows a query returns in rows mode.
pub const ROWS_LIMIT_MAX: usize = 10_000;
/// Most groups a query returns in aggregate mode.
pub const GROUPS_LIMIT_MAX: usize = 100_000;
/// Most expressions in a stage's `group_by`, `columns` or `aggregates`.
pub const EXPRS_MAX: usize = 64;
/// Most steps of a funnel or conditions of a retention aggregate.
pub const STEPS_MAX: usize = 32;

/// A time bucket width.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Interval {
    /// A fixed width in milliseconds: minutes, hours or days.
    Fixed(i64),
    /// Weeks starting on Monday.
    Weeks(i64),
    Months(i64),
}

impl Interval {
    pub fn parse(s: &str) -> Result<Interval> {
        let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
        let (n, unit) = s.split_at(split);
        let n: i64 = n.parse().ok().filter(|n| (1..=100_000).contains(n)).ok_or_else(|| bad_interval(s))?;
        Ok(match unit {
            "s" => Interval::Fixed(n * 1000),
            "m" => Interval::Fixed(n * 60_000),
            "h" => Interval::Fixed(n * 3_600_000),
            "d" => Interval::Fixed(n * 86_400_000),
            "w" => Interval::Weeks(n),
            "mo" => Interval::Months(n),
            _ => return Err(bad_interval(s)),
        })
    }

    /// The start of the bucket holding `t`, in a fixed offset of `offset_ms` from UTC,
    /// or `None` for a `t` outside [`TIME_RANGE`](super::schema::TIME_RANGE).
    pub fn bucket(&self, t: i64, offset_ms: i64) -> Option<i64> {
        use super::schema::{Granularity, TIME_RANGE, bucket_of, bucket_span};
        if !TIME_RANGE.contains(&t) {
            return None;
        }
        let local = t + offset_ms;
        let start = match *self {
            Interval::Fixed(w) => local.div_euclid(w) * w,
            Interval::Weeks(n) => {
                let w = bucket_of(Granularity::Week, local);
                bucket_span(Granularity::Week, w.div_euclid(n) * n).0
            }
            Interval::Months(n) => {
                let m = bucket_of(Granularity::Month, local);
                bucket_span(Granularity::Month, m.div_euclid(n) * n).0
            }
        };
        Some(start - offset_ms)
    }
}

fn bad_interval(s: &str) -> Error {
    Error::invalid(format!("bad interval {s:?}: use a count and a unit of s, m, h, d, w or mo, such as 15m or 1d"))
}

/// Parses a fixed offset such as `+02:00`, `-05:30` or `Z`.
pub fn parse_offset(s: &str) -> Result<i64> {
    if s == "Z" || s == "UTC" {
        return Ok(0);
    }
    let bad = || Error::invalid(format!("bad offset {s:?}: use Z or ±HH:MM"));
    let b = s.as_bytes();
    if b.len() != 6 || !matches!(b[0], b'+' | b'-') || b[3] != b':' {
        return Err(bad());
    }
    let h: i64 = s[1..3].parse().map_err(|_| bad())?;
    let m: i64 = s[4..6].parse().map_err(|_| bad())?;
    if h > 23 || m > 59 {
        return Err(bad());
    }
    let ms = (h * 60 + m) * 60_000;
    Ok(if b[0] == b'-' { -ms } else { ms })
}

/// An expression evaluated per row (or, in a second stage, per first-stage row).
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    /// A column, or a leaf path within a JSON column.
    Col { name: String, path: Vec<String> },
    /// A named lookup.
    Lookup(String),
    /// The start of the time bucket holding the value.
    Bucket { of: Box<Expr>, interval: Interval, offset_ms: i64 },
}

impl Expr {
    /// The default output name.
    pub fn default_name(&self) -> String {
        match self {
            Expr::Col { name, path } if path.is_empty() => name.clone(),
            Expr::Col { name, path } => format!("{name}.{}", path.join(".")),
            Expr::Lookup(n) => n.clone(),
            Expr::Bucket { .. } => "bucket".into(),
        }
    }

    /// Parses an expression from its operand keys, removing those it reads.
    fn take(keys: &mut Map<String, Json>) -> Result<Expr> {
        if let Some(b) = keys.remove("bucket") {
            let of = Expr::parse(b)?;
            let interval = match keys.remove("interval") {
                Some(Json::String(s)) => Interval::parse(&s)?,
                _ => return Err(Error::invalid("a bucket needs an interval such as \"1d\"")),
            };
            let offset_ms = match keys.remove("offset") {
                None => 0,
                Some(Json::String(s)) => parse_offset(&s)?,
                Some(_) => return Err(Error::invalid("offset is a string such as \"+02:00\"")),
            };
            return Ok(Expr::Bucket { of: Box::new(of), interval, offset_ms });
        }
        if let Some(l) = keys.remove("lookup") {
            return match l {
                Json::String(s) => Ok(Expr::Lookup(s)),
                _ => Err(Error::invalid("lookup names a lookup")),
            };
        }
        let name = match keys.remove("col") {
            Some(Json::String(s)) => s,
            Some(_) => return Err(Error::invalid("col is a string")),
            None => return Err(Error::invalid("an expression is {col}, {col, path}, {lookup} or {bucket, interval}")),
        };
        let path = match keys.remove("path") {
            None => Vec::new(),
            Some(Json::Array(a)) if !a.is_empty() && a.len() <= 32 => a
                .into_iter()
                .map(|s| match s {
                    Json::String(s) => Ok(s),
                    _ => Err(Error::invalid("path segments are strings")),
                })
                .collect::<Result<_>>()?,
            Some(_) => return Err(Error::invalid("path is an array of 1..=32 strings")),
        };
        Ok(Expr::Col { name, path })
    }

    pub fn parse(v: Json) -> Result<Expr> {
        match v {
            Json::String(s) => Ok(Expr::Col { name: s, path: Vec::new() }),
            Json::Object(mut o) => {
                let e = Expr::take(&mut o)?;
                if let Some(k) = o.keys().next() {
                    return Err(Error::invalid(format!("unknown expression key {k:?}")));
                }
                Ok(e)
            }
            _ => Err(Error::invalid("an expression is a column name or an object")),
        }
    }
}

impl Operand for Expr {
    fn parse(keys: &mut Map<String, Json>) -> Result<Expr> {
        Expr::take(keys)
    }

    fn check(&self, test: &Test<Json>) -> Result<()> {
        if matches!(self, Expr::Bucket { .. }) && matches!(test, Test::Prefix(_) | Test::Contains(_)) {
            return Err(Error::invalid("buckets take numeric comparisons"));
        }
        Ok(())
    }
}

pub type Filter = filter::Filter<Expr>;

/// An expression and its output name.
#[derive(Clone, Debug, PartialEq)]
pub struct Named {
    pub expr: Expr,
    pub name: String,
}

fn named(v: Json) -> Result<Named> {
    let alias = match &v {
        Json::Object(o) => match o.get("as") {
            Some(Json::String(s)) => Some(s.clone()),
            Some(_) => return Err(Error::invalid("as is a string")),
            None => None,
        },
        _ => None,
    };
    let v = match v {
        Json::Object(mut o) => {
            o.remove("as");
            Json::Object(o)
        }
        v => v,
    };
    let expr = Expr::parse(v)?;
    let name = alias.unwrap_or_else(|| expr.default_name());
    Ok(Named { expr, name })
}

/// How [`AggFn::WindowFunnel`] treats events between steps.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FunnelMode {
    /// Any event that is not the next step, once the funnel started, ends it.
    pub strict_order: bool,
    /// A repeat of the step last reached ends the funnel.
    pub strict_deduplication: bool,
    /// Each step must come strictly after the previous one.
    pub strict_increase: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AggFn {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    /// Approximate distinct count.
    Uniq,
    UniqExact,
    /// Values at each quantile in `0..=1`, within 1% relative error.
    Quantiles(Vec<f64>),
    /// The longest prefix of `steps` matched in time order, each within
    /// `window_ms` of the first.
    WindowFunnel {
        window_ms: i64,
        steps: Vec<Filter>,
        mode: FunnelMode,
    },
    /// Whether the first condition held, then whether it and each other one held.
    Retention(Vec<Filter>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggSpec {
    pub name: String,
    pub func: AggFn,
    pub of: Option<Expr>,
    /// Applied before each row is added (ClickHouse's `-If`).
    pub filter: Option<Filter>,
}

fn agg(name: String, v: Json) -> Result<AggSpec> {
    let Json::Object(mut o) = v else { return Err(Error::invalid(format!("aggregate {name:?} is an object"))) };
    let f = match o.remove("fn") {
        Some(Json::String(f)) => f,
        _ => return Err(Error::invalid(format!("aggregate {name:?} needs fn"))),
    };
    let of = o.remove("of").map(Expr::parse).transpose()?;
    let filter = o.remove("where").map(Filter::parse).transpose()?;
    let filters = |v: Option<Json>, what: &str| -> Result<Vec<Filter>> {
        match v {
            Some(Json::Array(a)) if !a.is_empty() && a.len() <= STEPS_MAX => a.into_iter().map(Filter::parse).collect(),
            _ => Err(Error::invalid(format!("{f} takes {what}: an array of 1..={STEPS_MAX} filters"))),
        }
    };
    let func = match f.as_str() {
        "count" => AggFn::Count,
        "sum" => AggFn::Sum,
        "min" => AggFn::Min,
        "max" => AggFn::Max,
        "avg" => AggFn::Avg,
        "uniq" => AggFn::Uniq,
        "uniq_exact" => AggFn::UniqExact,
        "quantiles" => {
            let qs = match o.remove("q") {
                Some(Json::Array(a)) if !a.is_empty() && a.len() <= 32 => {
                    a.iter().map(|q| q.as_f64().filter(|q| (0.0..=1.0).contains(q))).collect::<Option<Vec<f64>>>()
                }
                _ => None,
            };
            AggFn::Quantiles(
                qs.ok_or_else(|| Error::invalid("quantiles takes q: an array of 1..=32 numbers in 0..=1"))?,
            )
        }
        "window_funnel" => {
            let window_ms = o
                .remove("window_ms")
                .and_then(|w| w.as_i64())
                .filter(|w| *w > 0)
                .ok_or_else(|| Error::invalid("window_funnel takes a positive window_ms"))?;
            let steps = filters(o.remove("steps"), "steps")?;
            let mut mode = FunnelMode::default();
            match o.remove("mode") {
                None => {}
                Some(Json::String(m)) => match m.as_str() {
                    "strict_order" => mode.strict_order = true,
                    "strict_deduplication" => mode.strict_deduplication = true,
                    "strict_increase" => mode.strict_increase = true,
                    _ => return Err(bad_mode()),
                },
                Some(Json::Array(ms)) => {
                    for m in ms {
                        match m.as_str() {
                            Some("strict_order") => mode.strict_order = true,
                            Some("strict_deduplication") => mode.strict_deduplication = true,
                            Some("strict_increase") => mode.strict_increase = true,
                            _ => return Err(bad_mode()),
                        }
                    }
                }
                Some(_) => return Err(bad_mode()),
            }
            AggFn::WindowFunnel { window_ms, steps, mode }
        }
        "retention" => AggFn::Retention(filters(o.remove("conditions"), "conditions")?),
        other => {
            return Err(Error::invalid(format!(
                "unknown aggregate {other:?}; aggregates are count, sum, min, max, avg, uniq, uniq_exact, quantiles, \
                 window_funnel and retention"
            )));
        }
    };
    if let Some(k) = o.keys().next() {
        return Err(Error::invalid(format!("unknown key {k:?} in aggregate {name:?}")));
    }
    let needs_of = matches!(
        func,
        AggFn::Sum | AggFn::Min | AggFn::Max | AggFn::Avg | AggFn::Uniq | AggFn::UniqExact | AggFn::Quantiles(_)
    );
    match (needs_of, &of) {
        (true, None) => return Err(Error::invalid(format!("aggregate {name:?} needs of"))),
        (false, Some(_)) => return Err(Error::invalid(format!("aggregate {name:?} takes no of"))),
        _ => {}
    }
    Ok(AggSpec { name, func, of, filter })
}

fn bad_mode() -> Error {
    Error::invalid("mode is strict_order, strict_deduplication, strict_increase, or an array of them")
}

/// One aggregation: grouping expressions and named aggregates.
#[derive(Clone, Debug, PartialEq)]
pub struct Stage {
    pub group_by: Vec<Named>,
    pub aggregates: Vec<AggSpec>,
}

fn stage(o: &mut Map<String, Json>) -> Result<Stage> {
    let group_by = match o.remove("group_by") {
        None => Vec::new(),
        Some(Json::Array(a)) if a.len() <= EXPRS_MAX => a.into_iter().map(named).collect::<Result<_>>()?,
        Some(_) => return Err(Error::invalid(format!("group_by is an array of at most {EXPRS_MAX} expressions"))),
    };
    let aggregates = match o.remove("aggregates") {
        None => Vec::new(),
        Some(Json::Object(m)) if m.len() <= EXPRS_MAX => {
            m.into_iter().map(|(k, v)| agg(k, v)).collect::<Result<Vec<_>>>()?
        }
        Some(_) => return Err(Error::invalid(format!("aggregates is an object of at most {EXPRS_MAX} aggregates"))),
    };
    if group_by.is_empty() && aggregates.is_empty() {
        return Err(Error::invalid("a stage needs group_by or aggregates"));
    }
    unique_names(group_by.iter().map(|g| &g.name).chain(aggregates.iter().map(|a| &a.name)))?;
    Ok(Stage { group_by, aggregates })
}

fn unique_names<'a>(names: impl Iterator<Item = &'a String>) -> Result<()> {
    let mut seen = std::collections::BTreeSet::new();
    for n in names {
        if !seen.insert(n) {
            return Err(Error::invalid(format!("output {n:?} is named twice; rename one with \"as\"")));
        }
    }
    Ok(())
}

/// A lookup through a row table whose primary key is a single column.
#[derive(Clone, Debug, PartialEq)]
pub struct LookupSpec {
    pub table: String,
    pub key: Expr,
    pub value: String,
    pub default: Option<Expr>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Mode {
    /// Rows of the named columns; no columns means every column.
    Rows {
        columns: Vec<Named>,
        cursor: Option<String>,
    },
    Aggregate {
        first: Stage,
        then: Option<Stage>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct OrderBy {
    pub name: String,
    pub desc: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub from: i64,
    pub to: i64,
    pub filter: Option<Filter>,
    pub lookups: BTreeMap<String, LookupSpec>,
    pub mode: Mode,
    pub order_by: Vec<OrderBy>,
    pub limit: usize,
    /// Whether the query gave its `limit`, rather than taking the default.
    pub limited: bool,
}

/// Keys of a query body that the HTTP layer reads rather than the query.
pub const TRANSPORT_KEYS: [&str; 2] = ["consistency", "max_staleness_ms"];

impl Query {
    pub fn parse(v: Json) -> Result<Query> {
        let Json::Object(mut o) = v else { return Err(Error::invalid("a query is a JSON object")) };
        for k in TRANSPORT_KEYS {
            o.remove(k);
        }
        let time = |o: &mut Map<String, Json>, k: &str| -> Result<i64> {
            match o.remove(k) {
                Some(Json::String(s)) => super::schema::parse_rfc3339(&s)
                    .ok_or_else(|| Error::invalid(format!("{k} is milliseconds or an RFC 3339 timestamp"))),
                Some(v) => {
                    v.as_i64().ok_or_else(|| Error::invalid(format!("{k} is milliseconds or an RFC 3339 timestamp")))
                }
                None => Err(Error::invalid(format!("a query needs {k}"))),
            }
        };
        let (from, to) = (time(&mut o, "from")?, time(&mut o, "to")?);
        if from > to {
            return Err(Error::invalid("from is after to"));
        }
        let filter = o.remove("where").map(Filter::parse).transpose()?;
        let mut lookups = BTreeMap::new();
        if let Some(l) = o.remove("lookups") {
            let Json::Object(m) = l else { return Err(Error::invalid("lookups is an object")) };
            if m.len() > 8 {
                return Err(Error::invalid("a query has at most 8 lookups"));
            }
            for (name, spec) in m {
                let Json::Object(mut s) = spec else {
                    return Err(Error::invalid(format!("lookup {name:?} is an object")));
                };
                let table = match s.remove("table") {
                    Some(Json::String(t)) => t,
                    _ => return Err(Error::invalid(format!("lookup {name:?} needs a table"))),
                };
                let key = Expr::parse(
                    s.remove("key").ok_or_else(|| Error::invalid(format!("lookup {name:?} needs a key")))?,
                )?;
                let value = match s.remove("value") {
                    Some(Json::String(v)) => v,
                    _ => return Err(Error::invalid(format!("lookup {name:?} needs a value column"))),
                };
                let default = s.remove("default").map(Expr::parse).transpose()?;
                if let Some(k) = s.keys().next() {
                    return Err(Error::invalid(format!("unknown key {k:?} in lookup {name:?}")));
                }
                if matches!(key, Expr::Lookup(_)) || matches!(default, Some(Expr::Lookup(_))) {
                    return Err(Error::invalid("lookups cannot use other lookups"));
                }
                lookups.insert(name, LookupSpec { table, key, value, default });
            }
        }
        let rows_mode =
            o.contains_key("columns") || !["group_by", "aggregates", "then"].iter().any(|k| o.contains_key(*k));
        if rows_mode && (o.contains_key("group_by") || o.contains_key("aggregates") || o.contains_key("then")) {
            return Err(Error::invalid("a query returns either columns or aggregates"));
        }
        let mode = if rows_mode {
            let columns = match o.remove("columns") {
                None => Vec::new(),
                Some(Json::Array(a)) if !a.is_empty() && a.len() <= EXPRS_MAX => {
                    a.into_iter().map(named).collect::<Result<Vec<_>>>()?
                }
                _ => return Err(Error::invalid(format!("columns is an array of 1..={EXPRS_MAX} expressions"))),
            };
            unique_names(columns.iter().map(|c| &c.name))?;
            let cursor = match o.remove("cursor") {
                None | Some(Json::Null) => None,
                Some(Json::String(c)) => Some(c),
                Some(_) => return Err(Error::invalid("cursor is a string")),
            };
            Mode::Rows { columns, cursor }
        } else {
            let first = stage(&mut o)?;
            let then = match o.remove("then") {
                None => None,
                Some(Json::Object(mut t)) => {
                    let s = stage(&mut t)?;
                    if let Some(k) = t.keys().next() {
                        return Err(Error::invalid(format!("unknown key {k:?} in then")));
                    }
                    Some(s)
                }
                Some(_) => return Err(Error::invalid("then is an object")),
            };
            Mode::Aggregate { first, then }
        };
        let order_by = match o.remove("order_by") {
            None => Vec::new(),
            Some(Json::Array(a)) if a.len() <= EXPRS_MAX => a
                .into_iter()
                .map(|x| match x {
                    Json::String(name) => Ok(OrderBy { name, desc: false }),
                    Json::Object(mut m) => {
                        let name = match m.remove("col") {
                            Some(Json::String(n)) => n,
                            _ => return Err(Error::invalid("order_by entries are names or {col, desc}")),
                        };
                        let desc = match m.remove("desc") {
                            None => false,
                            Some(Json::Bool(b)) => b,
                            Some(_) => return Err(Error::invalid("desc is a boolean")),
                        };
                        if let Some(k) = m.keys().next() {
                            return Err(Error::invalid(format!("unknown key {k:?} in order_by")));
                        }
                        Ok(OrderBy { name, desc })
                    }
                    _ => Err(Error::invalid("order_by entries are names or {col, desc}")),
                })
                .collect::<Result<_>>()?,
            Some(_) => return Err(Error::invalid("order_by is an array")),
        };
        let max = if rows_mode { ROWS_LIMIT_MAX } else { GROUPS_LIMIT_MAX };
        let limited = o.get("limit").is_some_and(|l| !l.is_null());
        let limit = match o.remove("limit").filter(|l| !l.is_null()) {
            None => {
                if rows_mode {
                    100
                } else {
                    max
                }
            }
            Some(v) => v
                .as_u64()
                .filter(|l| (1..=max as u64).contains(l))
                .ok_or_else(|| Error::invalid(format!("limit must be in 1..={max}")))? as usize,
        };
        if let Some(k) = o.keys().next() {
            return Err(Error::invalid(format!("unknown query key {k:?}")));
        }
        Ok(Query { from, to, filter, lookups, mode, order_by, limit, limited })
    }

    /// The names a query outputs, in order.
    pub fn outputs(&self) -> Vec<String> {
        match &self.mode {
            Mode::Rows { columns, .. } => columns.iter().map(|c| c.name.clone()).collect(),
            Mode::Aggregate { first, then } => {
                let s = then.as_ref().unwrap_or(first);
                s.group_by.iter().map(|g| g.name.clone()).chain(s.aggregates.iter().map(|a| a.name.clone())).collect()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn intervals_bucket_and_divide() {
        let d = Interval::parse("1d").unwrap();
        assert_eq!(d, Interval::Fixed(86_400_000));
        assert_eq!(d.bucket(86_400_000 + 5, 0), Some(86_400_000));
        assert_eq!(d.bucket(-1, 0), Some(-86_400_000));
        assert_eq!(d.bucket(i64::MIN, 3_600_000), None);
        assert_eq!(d.bucket(86_400_000 - 3_600_000, 3_600_000), Some(86_400_000 - 3_600_000), "a local midnight");
        assert_eq!(Interval::parse("1mo").unwrap().bucket(31 * 86_400_000 + 1, 0), Some(31 * 86_400_000));
        assert_eq!(Interval::parse("1w").unwrap().bucket(0, 0), Some(-3 * 86_400_000), "weeks start on Monday");
        for bad in ["", "d", "0d", "1y", "1.5h"] {
            assert!(Interval::parse(bad).is_err(), "{bad}");
        }
        assert_eq!(parse_offset("-05:30").unwrap(), -19_800_000);
        assert!(parse_offset("+5").is_err());
    }

    #[test]
    fn queries_parse() {
        let q = Query::parse(json!({
            "from": 0, "to": "1970-01-02T00:00:00Z",
            "where": {"col": "event", "op": "eq", "value": "$pageview"},
            "group_by": [{"bucket": {"col": "time"}, "interval": "1d"}, {"col": "properties", "path": ["$browser"], "as": "browser"}],
            "aggregates": {"views": {"fn": "count"}, "users": {"fn": "uniq", "of": {"col": "distinct_id"}}},
            "then": {"group_by": ["browser"], "aggregates": {"days": {"fn": "count"}}},
            "order_by": [{"col": "days", "desc": true}],
            "limit": 5,
        }))
        .unwrap();
        assert_eq!(q.to, 86_400_000);
        assert_eq!(q.outputs(), vec!["browser", "days"]);
        let Mode::Aggregate { first, .. } = &q.mode else { panic!() };
        assert_eq!(first.group_by[0].name, "bucket");
        assert_eq!(first.group_by[1].name, "browser");
        let all = Query::parse(json!({"from": 0, "to": 1})).unwrap();
        assert_eq!(all.mode, Mode::Rows { columns: Vec::new(), cursor: None });
        let unlimited = Query::parse(json!({"from": 0, "to": 1, "limit": null})).unwrap();
        assert_eq!((unlimited.limit, unlimited.limited), (100, false), "a null limit is no limit");
        for bad in [
            json!({"to": 1, "aggregates": {"n": {"fn": "count"}}}),
            json!({"from": 2, "to": 1, "aggregates": {"n": {"fn": "count"}}}),
            json!({"from": 0, "to": 1, "columns": []}),
            json!({"from": 0, "to": 1, "columns": ["a"], "aggregates": {"n": {"fn": "count"}}}),
            json!({"from": 0, "to": 1, "aggregates": {"n": {"fn": "sum"}}}),
            json!({"from": 0, "to": 1, "aggregates": {"n": {"fn": "count", "of": "x"}}}),
            json!({"from": 0, "to": 1, "aggregates": {"n": {"fn": "median", "of": "x"}}}),
            json!({"from": 0, "to": 1, "group_by": ["a", "a"]}),
            json!({"from": 0, "to": 1, "columns": ["a"], "limit": 0}),
            json!({"from": 0, "to": 1, "columns": ["a"], "bogus": 1}),
            json!({"from": 0, "to": 1, "aggregates": {"f": {"fn": "window_funnel", "window_ms": 0, "steps": []}}}),
        ] {
            assert!(Query::parse(bad.clone()).is_err(), "{bad}");
        }
    }
}
