//! Event table schemas, row validation, time parsing and partitioning.

use crate::error::{Error, Result};
use crate::table::schema::{Datum, Row, valid_ident};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json, json};

/// Most columns an event table declares.
pub const COLUMNS_MAX: usize = 256;
/// Largest encoded row.
pub const ROW_MAX_BYTES: usize = 1 << 20;
/// The most bytes an event's columns hold in a part, its JSON objects
/// flattened to paths; see [`super::build::stored_len`].
pub const ROW_STORED_MAX_BYTES: usize = 4 * ROW_MAX_BYTES;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EvType {
    /// Milliseconds since the epoch.
    Time,
    String,
    Int,
    Float,
    Bool,
    /// A JSON object, flattened into paths in parts.
    Json,
}

impl EvType {
    pub fn name(self) -> &'static str {
        match self {
            EvType::Time => "time",
            EvType::String => "string",
            EvType::Int => "int",
            EvType::Float => "float",
            EvType::Bool => "bool",
            EvType::Json => "json",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EvColumn {
    pub id: u32,
    pub name: String,
    #[serde(rename = "type")]
    pub ty: EvType,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Json>,
}

/// The span of time each partition covers, in UTC.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Granularity {
    Hour,
    Day,
    /// Weeks starting on Monday.
    Week,
    Month,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SkipKind {
    Bloom,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkipIndex {
    pub column: String,
    #[serde(rename = "type")]
    pub kind: SkipKind,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventSchema {
    pub id: u32,
    pub name: String,
    pub columns: Vec<EvColumn>,
    pub time_column: String,
    pub order_by: Vec<String>,
    pub partition: Granularity,
    #[serde(default)]
    pub skip_indexes: Vec<SkipIndex>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_ms: Option<u64>,
    pub next_column_id: u32,
}

/// The default of a time column that takes the inserting node's clock.
pub const NOW: &str = "now";

const MS_PER_HOUR: i64 = 3_600_000;
const MS_PER_DAY: i64 = 24 * MS_PER_HOUR;

impl EventSchema {
    pub fn column(&self, name: &str) -> Option<&EvColumn> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn column_by_id(&self, id: u32) -> Option<&EvColumn> {
        self.columns.iter().find(|c| c.id == id)
    }

    pub fn time_id(&self) -> u32 {
        self.column(&self.time_column).map_or(0, |c| c.id)
    }

    /// Column ids of the sort key, in order.
    pub fn sort_ids(&self) -> Vec<u32> {
        self.order_by.iter().filter_map(|n| self.column(n)).map(|c| c.id).collect()
    }

    pub fn bloom_ids(&self) -> Vec<u32> {
        self.skip_indexes.iter().filter_map(|i| self.column(&i.column)).map(|c| c.id).collect()
    }

    pub fn validate(&self) -> Result<()> {
        if !valid_ident(&self.name) {
            return Err(Error::invalid(format!("invalid event table name {:?}", self.name)));
        }
        if self.columns.is_empty() || self.columns.len() > COLUMNS_MAX {
            return Err(Error::invalid(format!("an event table needs 1..={COLUMNS_MAX} columns")));
        }
        let mut seen = std::collections::BTreeSet::new();
        for c in &self.columns {
            if !valid_ident(&c.name) || c.name.starts_with('_') {
                return Err(Error::invalid(format!(
                    "invalid column name {:?}: names match [a-zA-Z][a-zA-Z0-9_]{{0,63}}",
                    c.name
                )));
            }
            if !seen.insert(c.name.as_str()) {
                return Err(Error::invalid(format!("duplicate column {:?}", c.name)));
            }
            if let Some(d) = &c.default {
                if c.ty == EvType::Json {
                    return Err(Error::invalid(format!("json column {:?} cannot have a default", c.name)));
                }
                if d.to_string().len() > DEFAULT_MAX_BYTES {
                    return Err(Error::invalid(format!(
                        "column {:?} has a default of more than {DEFAULT_MAX_BYTES} bytes",
                        c.name
                    )));
                }
                match (c.ty, d) {
                    (EvType::Time, Json::String(s)) if s == NOW => {}
                    _ => {
                        let v = datum(c, d)?;
                        if v.is_null() {
                            return Err(Error::invalid(format!("column {:?} has a null default", c.name)));
                        }
                    }
                }
            }
        }
        let time = self
            .column(&self.time_column)
            .ok_or_else(|| Error::invalid(format!("time column {:?} does not exist", self.time_column)))?;
        if time.ty != EvType::Time || time.nullable {
            return Err(Error::invalid("the time column has type time and is not nullable"));
        }
        if self.order_by.is_empty() || self.order_by.len() > 8 {
            return Err(Error::invalid("order_by names 1..=8 columns"));
        }
        let mut keyed = std::collections::BTreeSet::new();
        for n in &self.order_by {
            let c = self.column(n).ok_or_else(|| Error::invalid(format!("order_by column {n:?} does not exist")))?;
            if c.ty == EvType::Json {
                return Err(Error::invalid(format!("json column {n:?} cannot be in order_by")));
            }
            if !keyed.insert(n) {
                return Err(Error::invalid(format!("duplicate order_by column {n:?}")));
            }
        }
        let mut indexed = std::collections::BTreeSet::new();
        for i in &self.skip_indexes {
            let c = self
                .column(&i.column)
                .ok_or_else(|| Error::invalid(format!("skip index column {:?} does not exist", i.column)))?;
            if c.ty == EvType::Json {
                return Err(Error::invalid(format!("json column {:?} cannot have a skip index", i.column)));
            }
            if !indexed.insert(&i.column) {
                return Err(Error::invalid(format!("duplicate skip index on {:?}", i.column)));
            }
        }
        if self.retention_ms == Some(0) {
            return Err(Error::invalid("retention_ms must be positive"));
        }
        Ok(())
    }

    pub fn to_json(&self) -> Json {
        json!({
            "name": self.name,
            "columns": self.columns.iter().map(|c| {
                let mut o = json!({"name": c.name, "type": c.ty, "nullable": c.nullable});
                if let Some(d) = &c.default { o["default"] = d.clone(); }
                o
            }).collect::<Vec<_>>(),
            "time_column": self.time_column,
            "order_by": self.order_by,
            "partition": self.partition,
            "skip_indexes": self.skip_indexes,
            "retention_ms": self.retention_ms,
        })
    }

    /// Validates a JSON object as a row of this table, filling defaults; a
    /// time column defaulting to [`NOW`] takes `now_ms`.
    pub fn row(&self, v: &Json, now_ms: i64) -> Result<Row> {
        let Json::Object(o) = v else { return Err(Error::invalid("an event is a JSON object")) };
        if let Some(k) = o.keys().find(|k| self.column(k).is_none()) {
            return Err(Error::invalid(format!("unknown column {k:?}")));
        }
        let mut row = Row::new();
        for c in &self.columns {
            let d = match o.get(&c.name) {
                Some(v) if !v.is_null() => datum(c, v)?,
                _ => match &c.default {
                    Some(Json::String(s)) if c.ty == EvType::Time && s == NOW => Datum::Int(now_ms),
                    Some(d) => datum(c, d)?,
                    None if c.nullable => Datum::Null,
                    None => return Err(Error::invalid(format!("column {:?} is required", c.name))),
                },
            };
            if !d.is_null() {
                row.insert(c.id, d);
            }
        }
        Ok(row)
    }

    /// The time of a validated row.
    pub fn time_of(&self, row: &Row) -> i64 {
        match row.get(&self.time_id()) {
            Some(Datum::Int(t)) => *t,
            _ => 0,
        }
    }

    /// The earliest time retained at `now_ms`: queries skip earlier events,
    /// and maintenance drops parts holding only earlier events.
    pub fn retained_from(&self, now_ms: i64) -> i64 {
        match self.retention_ms {
            Some(r) => now_ms.saturating_sub(r.min(i64::MAX as u64) as i64),
            None => i64::MIN,
        }
    }

    /// The partition bucket holding time `t`.
    pub fn bucket(&self, t: i64) -> i64 {
        bucket_of(self.partition, t)
    }

    /// The first millisecond of partition bucket `b`, and the first of the next.
    pub fn bucket_span(&self, b: i64) -> (i64, i64) {
        bucket_span(self.partition, b)
    }
}

pub fn bucket_of(g: Granularity, t: i64) -> i64 {
    match g {
        Granularity::Hour => t.div_euclid(MS_PER_HOUR),
        Granularity::Day => t.div_euclid(MS_PER_DAY),
        Granularity::Week => (t.div_euclid(MS_PER_DAY) + 3).div_euclid(7),
        Granularity::Month => {
            let (y, m, _) = civil_from_days(t.div_euclid(MS_PER_DAY));
            (y - 1970) * 12 + (m as i64 - 1)
        }
    }
}

pub fn bucket_span(g: Granularity, b: i64) -> (i64, i64) {
    match g {
        Granularity::Hour => (b * MS_PER_HOUR, (b + 1) * MS_PER_HOUR),
        Granularity::Day => (b * MS_PER_DAY, (b + 1) * MS_PER_DAY),
        Granularity::Week => ((b * 7 - 3) * MS_PER_DAY, ((b + 1) * 7 - 3) * MS_PER_DAY),
        Granularity::Month => (month_start(b), month_start(b + 1)),
    }
}

/// The first millisecond of month `m` counted from January 1970.
pub fn month_start(m: i64) -> i64 {
    let y = 1970 + m.div_euclid(12);
    let mo = m.rem_euclid(12) as u32 + 1;
    days_from_civil(y, mo, 1) * MS_PER_DAY
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's algorithm).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Parses an RFC 3339 timestamp such as `2025-10-01T12:30:00.250Z` or
/// `2025-10-01 12:30:00+02:00` into milliseconds since the epoch.
pub fn parse_rfc3339(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let num = |r: std::ops::Range<usize>| -> Option<i64> {
        let t = b.get(r)?;
        if t.is_empty() || !t.iter().all(u8::is_ascii_digit) {
            return None;
        }
        std::str::from_utf8(t).ok()?.parse().ok()
    };
    if b.len() < 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let (y, mo, d) = (num(0..4)?, num(5..7)? as u32, num(8..10)? as u32);
    if !(1..=12).contains(&mo) || d == 0 || d > days_in_month(y, mo) {
        return None;
    }
    let days = days_from_civil(y, mo, d);
    if b.len() == 10 {
        return Some(days * MS_PER_DAY);
    }
    if !matches!(b[10], b'T' | b't' | b' ') || b.len() < 19 || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let (h, mi, sec) = (num(11..13)?, num(14..16)?, num(17..19)?);
    if h > 23 || mi > 59 || sec > 60 {
        return None;
    }
    let mut i = 19;
    let mut ms = 0i64;
    if b.get(i) == Some(&b'.') {
        let start = i + 1;
        i = start;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if i == start {
            return None;
        }
        let digits = &s[start..i.min(start + 3)];
        ms = format!("{digits:0<3}").parse().ok()?;
    }
    let offset_min = match b.get(i) {
        Some(b'Z' | b'z') if i + 1 == b.len() => 0,
        Some(c @ (b'+' | b'-')) if i + 6 == b.len() && b[i + 3] == b':' => {
            let (oh, om) = (num(i + 1..i + 3)?, num(i + 4..i + 6)?);
            if oh > 23 || om > 59 {
                return None;
            }
            let o = oh * 60 + om;
            if *c == b'-' { -o } else { o }
        }
        _ => return None,
    };
    Some(days * MS_PER_DAY + ((h * 60 + mi - offset_min) * 60 + sec) * 1000 + ms)
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// The most bytes a column's default takes as JSON: every event written
/// without the column holds a copy.
pub const DEFAULT_MAX_BYTES: usize = 1024;

/// Converts a JSON value to a datum of column `c`'s type.
/// Times an event table stores, in milliseconds: years 0000 to 9999, which
/// RFC 3339 can express.
pub const TIME_RANGE: std::ops::RangeInclusive<i64> = -62_167_219_200_000..=253_402_300_799_999;

pub fn datum(c: &EvColumn, v: &Json) -> Result<Datum> {
    if v.is_null() {
        return Ok(Datum::Null);
    }
    let bad = || Error::invalid(format!("column {:?} expects {}, got {v}", c.name, c.ty.name()));
    Ok(match c.ty {
        EvType::Time => {
            let t = match v {
                Json::String(s) => parse_rfc3339(s).ok_or_else(|| {
                    Error::invalid(format!(
                        "column {:?} expects an RFC 3339 time with an offset (Z or ±HH:MM), got {v}",
                        c.name
                    ))
                })?,
                _ => v.as_i64().ok_or_else(|| {
                    Error::invalid(format!("column {:?} expects whole milliseconds since the epoch, got {v}", c.name))
                })?,
            };
            if !TIME_RANGE.contains(&t) {
                return Err(Error::invalid(format!(
                    "column {:?} expects a time in years 0000 to 9999, got {v}",
                    c.name
                )));
            }
            Datum::Int(t)
        }
        EvType::String => Datum::Str(v.as_str().ok_or_else(bad)?.to_string()),
        EvType::Int => Datum::Int(v.as_i64().ok_or_else(bad)?),
        EvType::Float => {
            let f = v.as_f64().ok_or_else(bad)?;
            if !f.is_finite() {
                return Err(bad());
            }
            // -0.0 is stored as 0.0, which it equals, so statistics need not order the two.
            Datum::Float(if f == 0.0 { 0.0 } else { f })
        }
        EvType::Bool => Datum::Bool(v.as_bool().ok_or_else(bad)?),
        EvType::Json => match v {
            Json::Object(_) => Datum::Json(v.clone()),
            _ => return Err(Error::invalid(format!("column {:?} expects a JSON object, got {v}", c.name))),
        },
    })
}

/// A row as JSON with column names.
pub fn row_json(s: &EventSchema, row: &Row) -> Map<String, Json> {
    let mut o = Map::new();
    for c in &s.columns {
        o.insert(c.name.clone(), row.get(&c.id).map_or(Json::Null, Datum::to_json));
    }
    o
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::rng::Rng;

    pub(crate) fn sample() -> EventSchema {
        let col = |id, name: &str, ty, nullable| EvColumn { id, name: name.into(), ty, nullable, default: None };
        EventSchema {
            id: 1,
            name: "events".into(),
            columns: vec![
                EvColumn { default: Some(json!("now")), ..col(1, "time", EvType::Time, false) },
                col(2, "event", EvType::String, false),
                col(3, "distinct_id", EvType::String, true),
                col(4, "properties", EvType::Json, true),
                col(5, "value", EvType::Float, true),
            ],
            time_column: "time".into(),
            order_by: vec!["event".into(), "time".into()],
            partition: Granularity::Day,
            skip_indexes: vec![SkipIndex { column: "distinct_id".into(), kind: SkipKind::Bloom }],
            retention_ms: None,
            next_column_id: 6,
        }
    }

    #[test]
    fn schemas_validate() {
        let s = sample();
        s.validate().unwrap();
        let bad = |f: &dyn Fn(&mut EventSchema)| {
            let mut x = sample();
            f(&mut x);
            x.validate().is_err()
        };
        assert!(bad(&|s| s.time_column = "event".into()));
        assert!(bad(&|s| s.columns[0].nullable = true));
        assert!(bad(&|s| s.order_by = vec!["properties".into()]));
        assert!(bad(&|s| s.order_by = vec![]));
        assert!(bad(&|s| s.order_by = vec!["nope".into()]));
        assert!(bad(&|s| s.skip_indexes[0].column = "properties".into()));
        assert!(bad(&|s| s.columns[1].name = "_hidden".into()));
        assert!(bad(&|s| s.columns[4].default = Some(json!("x"))));
        assert!(bad(&|s| s.retention_ms = Some(0)));
        let long = |n: usize| move |s: &mut EventSchema| s.columns[2].default = Some(json!("x".repeat(n)));
        assert!(!bad(&long(DEFAULT_MAX_BYTES - 2)));
        assert!(bad(&long(DEFAULT_MAX_BYTES - 1)), "a default takes at most {DEFAULT_MAX_BYTES} bytes");
    }

    #[test]
    fn rows_validate_and_default() {
        let s = sample();
        let r = s.row(&json!({"event": "a", "properties": {"x": 1}}), 77).unwrap();
        assert_eq!(s.time_of(&r), 77);
        assert_eq!(r.get(&2), Some(&Datum::Str("a".into())));
        assert!(!r.contains_key(&3));
        assert!(s.row(&json!({"event": "a", "bogus": 1}), 0).is_err());
        assert!(s.row(&json!({"time": 5}), 0).is_err(), "event is required");
        assert!(s.row(&json!({"event": "a", "properties": [1]}), 0).is_err(), "json columns hold objects");
        assert!(s.row(&json!({"event": 3}), 0).is_err());
        let r = s.row(&json!({"event": "a", "time": "2025-10-01T00:00:01.5Z"}), 0).unwrap();
        assert_eq!(s.time_of(&r), 1_759_276_801_500);
    }

    #[test]
    fn rfc3339() {
        assert_eq!(parse_rfc3339("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_rfc3339("1970-01-01"), Some(0));
        assert_eq!(parse_rfc3339("1970-01-01T01:00:00+01:00"), Some(0));
        assert_eq!(parse_rfc3339("1969-12-31T23:59:59.999Z"), Some(-1));
        assert_eq!(parse_rfc3339("2024-02-29 12:00:00.1234-00:30"), Some(1_709_209_800_123 + 1_800_000 - 1_800_000));
        for bad in ["", "2023-02-29", "2024-13-01", "2024-01-01T24:00:00Z", "2024-01-01T00:00:00", "x"] {
            assert_eq!(parse_rfc3339(bad), None, "{bad}");
        }
    }

    #[test]
    fn calendars_roundtrip_and_buckets_contain_their_times() {
        let mut r = Rng::new(3);
        for _ in 0..20_000 {
            let days = r.below(200_000) as i64 - 100_000;
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
            let t = days * MS_PER_DAY + r.below(MS_PER_DAY as u64) as i64;
            for g in [Granularity::Hour, Granularity::Day, Granularity::Week, Granularity::Month] {
                let b = bucket_of(g, t);
                let (lo, hi) = bucket_span(g, b);
                assert!(lo <= t && t < hi, "{g:?} {t} in [{lo}, {hi})");
                assert_eq!(bucket_of(g, lo), b);
                assert_eq!(bucket_of(g, hi), b + 1);
            }
        }
        assert_eq!(civil_from_days(bucket_span(Granularity::Week, 0).0.div_euclid(MS_PER_DAY)), (1969, 12, 29));
    }
}
