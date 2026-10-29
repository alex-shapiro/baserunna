//! Differential tests: the same events in a database that never seals and in
//! one that seals and merges with tiny granules and JSON directories, both
//! checked against the reference evaluator of [`super::reference`].

use super::exec::{self, Limits};
use super::query::{Filter, Query};
use super::reference::{self as rf, DAY, Reference, TIME_MAX, TIME_MIN};
use super::*;
use crate::engine::tests::{engine, rt};
use crate::engine::{Consistency, Db, Engine, EngineConfig, Isolation};
use crate::util::rng::Rng;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;

static ERRS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
const EVS: [&str; 10] = [
    "a",
    "b",
    "c",
    "page",
    "pager",
    "",
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\u{e9}a",
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\u{e9}b",
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "a\u{0}b",
];
const T0: i64 = 1_700_000_000_000;
const INTERVALS: [&str; 11] = ["1s", "90m", "1h", "6h", "1d", "7d", "1w", "2w", "1mo", "3mo", "12mo"];
const OFFSETS: [&str; 5] = ["Z", "+05:30", "-08:00", "+14:00", "-23:59"];
/// Lookups that queries declare, besides `plan`, `nn` and `meta`.
const MORE_LOOKUPS: [&str; 4] = ["pk", "fl", "xs", "bk"];

/// The lookups every generated query declares: with defaults of each kind
/// of expression or none, and keys of other types than the table's.
fn lookups() -> Json {
    json!({
        "plan": {"table": "people", "key": {"col": "user"}, "value": "plan", "default": {"col": "event"}},
        "nn": {"table": "nums", "key": {"col": "props", "path": ["k1"]}, "value": "score"},
        "meta": {"table": "nums", "key": {"col": "n"}, "value": "meta"},
        "pk": {"table": "people", "key": {"col": "props", "path": ["k1"]}, "value": "score", "default": {"col": "x"}},
        "fl": {"table": "nums", "key": "flag", "value": "plan",
            "default": {"bucket": {"col": "time"}, "interval": "1d", "offset": "+05:30"}},
        "xs": {"table": "nums", "key": "x", "value": "plan", "default": {"col": "props", "path": ["k2"]}},
        "bk": {"table": "nums", "key": {"bucket": {"col": "n"}, "interval": "1s"}, "value": "id", "default": "user"}
    })
}

/// Times at the edges of what tables hold and of calendar units.
fn extreme_times() -> Vec<i64> {
    let at = |y, m, d, ms: i64| rf::days_of(y, m, d) * DAY + ms;
    vec![
        TIME_MIN,
        TIME_MIN + 1,
        -1,
        0,
        at(1969, 12, 29, 0) - 1,
        at(1960, 2, 29, 43_200_000),
        at(2000, 3, 1, 0) - 1,
        at(2024, 2, 29, 84_600_000),
        at(2023, 12, 31, 86_399_999),
        TIME_MAX,
        TIME_MAX - 40 * DAY,
    ]
}

async fn txn_do<T>(db: &Arc<Db>, mut f: impl AsyncFnMut(&mut crate::engine::Txn) -> Result<T>) -> Result<T> {
    for _ in 0..20 {
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await?;
        let out = f(&mut t).await?;
        match db.commit(t.into_request()).await {
            Ok(_) => return Ok(out),
            Err(Error::Conflict(_)) => continue,
            Err(e) => return Err(e),
        }
    }
    Err(Error::internal("too many conflicts"))
}

pub(super) async fn query(db: &Arc<Db>, q: &Json) -> Result<exec::Output> {
    query_at(db, q, Consistency::Strong).await
}

async fn query_at(db: &Arc<Db>, q: &Json, c: Consistency) -> Result<exec::Output> {
    let q = Query::parse(q.clone())?;
    let mut t = db.begin(Isolation::Serializable, c).await?;
    let s = get_schema(&mut t, "events").await?;
    exec::run(db.shared(), &db.id, &mut t, &s, &q, &Limits::default()).await
}

static VARIANT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// The table's definition: a sort key and partitioning that vary by seed.
pub(super) fn table_json() -> Json {
    let v = VARIANT.load(std::sync::atomic::Ordering::Relaxed);
    let order_by =
        [json!(["event", "user", "time"]), json!(["x", "event"]), json!(["n", "user", "time"]), json!(["time"])][v % 4]
            .clone();
    let partition = ["day", "hour", "week", "month"][(v / 4) % 4];
    json!({
        "name": "events",
        "columns": [
            {"name": "time", "type": "time"},
            {"name": "event", "type": "string", "nullable": false},
            {"name": "user", "type": "string"},
            {"name": "n", "type": "int"},
            {"name": "x", "type": "float"},
            {"name": "flag", "type": "bool"},
            {"name": "props", "type": "json"},
            {"name": "props2", "type": "json"}
        ],
        "time_column": "time",
        "order_by": order_by,
        "partition": partition,
        "skip_indexes": [{"column": "user", "type": "bloom"}, {"column": "n", "type": "bloom"}, {"column": "x", "type": "bloom"}]
    })
}

pub(super) fn table_def() -> CreateEvents {
    serde_json::from_value(table_json()).unwrap()
}

/// Rows of the lookup tables: `people`, keyed by string, one of whose
/// people has no plan, and `nums`, keyed by int.
fn lookup_rows() -> Vec<(&'static str, Json)> {
    let mut rows = Vec::new();
    for i in 0..8 {
        rows.push((
            "people",
            json!({"id": format!("u{i}"), "plan": (["free", "pro", "team"][i % 3]), "score": i as f64 / 2.0,
                "meta": {"v": i}}),
        ));
    }
    rows.push(("people", json!({"id": "u9", "score": 2.5})));
    for i in -2..4i64 {
        rows.push(("nums", json!({"id": i, "plan": format!("n{i}"), "score": i as f64, "meta": {"v": i * 2}})));
    }
    rows
}

/// A reference holding what [`create_db`] creates.
pub(super) fn reference() -> Reference {
    let mut tables = BTreeMap::new();
    for name in ["people", "nums"] {
        let rows = lookup_rows()
            .into_iter()
            .filter(|(t, _)| *t == name)
            .map(|(_, r)| r.as_object().unwrap().clone())
            .collect();
        let columns = ["id", "plan", "score", "meta"].map(String::from).to_vec();
        tables.insert(name.to_string(), rf::Table { key: "id".into(), columns, rows });
    }
    Reference::new(table_json()["columns"].as_array().unwrap(), "time", tables)
}

pub(super) async fn create_db(eng: &Arc<Engine>, id: &str) -> Arc<Db> {
    let db = eng.db(id);
    db.open(true).await.unwrap();
    txn_do(&db, async |t| create(t, &table_def()).await).await.unwrap();
    for (name, key_ty) in [("people", "string"), ("nums", "int")] {
        let tdef: crate::table::CreateTable = serde_json::from_value(json!({"name": name,
            "columns": [{"name": "id", "type": key_ty}, {"name": "plan", "type": "string"},
                {"name": "score", "type": "float"}, {"name": "meta", "type": "json"}],
            "primary_key": ["id"]}))
        .unwrap();
        txn_do(&db, async |t| crate::table::create_table(t, &tdef).await.map(|_| ())).await.unwrap();
    }
    for (table, row) in lookup_rows() {
        let d = db.clone();
        txn_do(&db, async |t| {
            let b = crate::table::BlobResolver { db: &d };
            crate::table::insert(t, &b, table, &row, crate::table::InsertMode::Insert).await
        })
        .await
        .unwrap();
    }
    db
}

pub(super) struct G {
    pub r: Rng,
    pub t: i64,
    pub added: Vec<&'static str>,
    pub dropped: bool,
}

fn pick<T: Clone>(r: &mut Rng, xs: &[T]) -> T {
    xs[r.usize(xs.len())].clone()
}

impl G {
    fn scalar(&mut self) -> Json {
        let r = &mut self.r;
        match r.usize(12) {
            9 => json!(pick(r, &EVS)),
            10 => json!(pick(r, &[1e308, 1152921504606846976.0, -0.5, 5e-324])),
            11 => json!(pick(r, &[i64::MIN, i64::MAX, (1i64 << 60) + 1])),
            0 => json!(r.below(5) as i64 - 1),
            1 => json!(pick(r, &["s0", "s1", "s10", "pa", "pag", "page", "", "u1", "u3", "1"])),
            2 => json!(r.chance(0.5)),
            3 => json!(0.5 + r.below(3) as f64),
            4 => json!(r.below(3) as f64),
            5 => json!([r.below(3), "a"]),
            6 => json!([]),
            7 => json!({}),
            _ => json!(1i64 << 60),
        }
    }

    pub fn row(&mut self) -> Json {
        if !self.r.chance(0.3) {
            self.t += 1 + self.r.below(DAY as u64 / 4) as i64;
        }
        let time = if self.r.chance(0.03) {
            json!(pick(&mut self.r, &extreme_times()))
        } else if self.r.chance(0.1) {
            json!(rf::format_rfc3339(self.t, pick(&mut self.r, &[0, 330, -480, -1439, 840])))
        } else {
            json!(self.t)
        };
        let ev = pick(&mut self.r, &EVS);
        let mut row = json!({"time": time, "event": ev});
        if self.r.chance(0.8) {
            row["user"] = json!(format!("u{}", self.r.below(10)));
        }
        if self.r.chance(0.7) {
            let small = self.r.below(8) as i64 - 3;
            row["n"] = pick(&mut self.r, &[json!(small), json!(i64::MIN), json!(i64::MAX), json!(1i64 << 60)]);
        }
        if self.r.chance(0.6) {
            row["x"] = json!(pick(
                &mut self.r,
                &[0.0, 1.0, 1.5, -2.25, 3.0, 1e20, 0.1, 1e308, -1e308, 5e-324, 1152921504606846976.0]
            ));
        }
        if self.r.chance(0.5) {
            row["flag"] = if self.dropped { json!(pick(&mut self.r, &["p", "q"])) } else { json!(self.r.chance(0.5)) };
        }
        let seen = json!(T0 + self.r.below(10) as i64 * DAY / 3);
        for (c, v) in self.added.clone().into_iter().zip([json!("given"), json!(9), seen]) {
            if self.r.chance(0.5) {
                row[c] = v;
            }
        }
        if self.r.chance(0.85) {
            let mut p = serde_json::Map::new();
            for _ in 0..self.r.usize(6) {
                let key = if self.r.chance(0.1) {
                    pick(&mut self.r, &["a.b", "", "\u{e9}", "[\"k1\"]", "a"]).to_string()
                } else {
                    format!("k{}", self.r.below(6))
                };
                let v = if self.r.chance(0.2) {
                    let mut inner = serde_json::Map::new();
                    for _ in 0..1 + self.r.usize(2) {
                        inner.insert(pick(&mut self.r, &["deep", "z"]).to_string(), self.scalar());
                    }
                    Json::Object(inner)
                } else {
                    self.scalar()
                };
                p.insert(key, v);
            }
            if self.r.chance(0.2) {
                p.insert(format!("uniq{}", self.r.below(1000)), json!(self.r.below(3)));
            }
            if self.r.chance(0.05) {
                p.insert("L".repeat(300), json!("long"));
            }
            if self.r.chance(0.5) {
                row["props2"] = Json::Object(p.clone());
            }
            row["props"] = Json::Object(p);
        }
        row
    }

    fn path(&mut self) -> Json {
        let r = &mut self.r;
        match r.usize(6) {
            0 => json!([format!("k{}", r.below(6)), pick(r, &["deep", "z"])]),
            1 => json!(["L".repeat(300)]),
            2 if r.chance(0.5) => {
                pick(r, &[json!(["a.b"]), json!(["a", "deep"]), json!([""]), json!(["\u{e9}"]), json!(["[\"k1\"]"])])
            }
            _ => json!([format!("k{}", r.below(6))]),
        }
    }

    /// An `in` test with up to the longest list, mostly of values that no
    /// row holds, and otherwise of values that rows hold in another form:
    /// integers for integral floats, `-0.0` for `0`, objects with their keys
    /// in another order.
    fn long_in(&mut self) -> Json {
        let kind = self.r.usize(4);
        let n = 1 + self.r.usize(crate::filter::LIST_MAX);
        let mut list = Vec::with_capacity(n);
        for _ in 0..n {
            let noise = self.r.chance(0.95);
            let i = self.r.below(1 << 20) as i64;
            list.push(match kind {
                0 if noise => json!(i - (1 << 19)),
                0 => pick(&mut self.r, &[json!(i % 8 - 3), json!(i64::MIN), json!(i64::MAX), json!(1i64 << 60)]),
                1 if noise => json!(i as f64 + 0.25),
                1 => pick(
                    &mut self.r,
                    &[
                        json!(1),
                        json!(3),
                        json!(-0.0),
                        json!(1.5),
                        json!(-2.25),
                        json!(1e20),
                        json!(1e308),
                        json!(5e-324),
                        json!(1i64 << 60),
                        json!((1i64 << 60) + 1),
                        json!(1152921504606846976.0),
                    ],
                ),
                2 if noise => json!(format!("e{i}")),
                2 => json!(pick(&mut self.r, &EVS)),
                _ if noise => pick(&mut self.r, &[json!(i), json!(format!("s{i}")), json!([i, "a"])]),
                _ => {
                    let v = self.scalar();
                    pick(&mut self.r, &[v, json!(-0.0), json!(1.0), json!([1.0, "a"]), json!({"z": 1.0, "deep": "s0"})])
                }
            });
        }
        match kind {
            0 => json!({"col": "n", "op": "in", "value": list}),
            1 => json!({"col": "x", "op": "in", "value": list}),
            2 => json!({"col": "event", "op": "in", "value": list}),
            _ => json!({"col": "props", "path": self.path(), "op": "in", "value": list}),
        }
    }

    fn bucket(&mut self, col: &str) -> Json {
        json!({"bucket": {"col": col}, "interval": pick(&mut self.r, &INTERVALS), "offset": pick(&mut self.r, &OFFSETS)})
    }

    fn leaf(&mut self) -> Json {
        let ops = ["eq", "ne", "lt", "le", "gt", "ge"];
        match self.r.usize(22) {
            18 => {
                let l = pick(&mut self.r, &MORE_LOOKUPS.iter().chain(&["nn", "meta"]).copied().collect::<Vec<_>>());
                let v = self.scalar();
                match self.r.usize(3) {
                    0 => json!({"lookup": l, "op": "exists"}),
                    1 => json!({"lookup": l, "op": pick(&mut self.r, &["in", "contains"]), "value": json!([v])}),
                    _ => json!({"lookup": l, "op": pick(&mut self.r, &ops), "value": v}),
                }
            }
            19 => {
                let t = T0 + self.r.below(20) as i64 * DAY / 4 + self.r.below(1000) as i64;
                let s = rf::format_rfc3339(t, pick(&mut self.r, &[0, 330, -480]));
                json!({"col": "time", "op": pick(&mut self.r, &ops), "value": s})
            }
            20 => {
                let col = pick(&mut self.r, &["time", "time", "n"]);
                let mut b = self.bucket(col);
                let v = if col == "n" {
                    json!(self.r.below(8) as i64 - 3)
                } else {
                    json!(T0 + self.r.below(8) as i64 * DAY / 2)
                };
                b["op"] = json!(pick(&mut self.r, &ops));
                b["value"] = v;
                b
            }
            21 => {
                let v = self.scalar();
                json!({"col": pick(&mut self.r, &["n", "x", "flag", "time", "event", "props"]),
                    "op": pick(&mut self.r, &["prefix", "contains", "eq", "lt"]), "value": v})
            }
            0 => json!({"col": "event", "op": pick(&mut self.r, &ops), "value": pick(&mut self.r, &EVS)}),
            1 => json!({"col": "event", "op": "in", "value": ["a", "c", ""]}),
            2 => {
                json!({"col": "event", "op": pick(&mut self.r, &["prefix", "contains"]), "value": pick(&mut self.r, &["pag", "ge", "", "a", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\u{e9}", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "a\u{0}"])})
            }
            3 => json!({"col": "user", "op": pick(&mut self.r, &ops), "value": format!("u{}", self.r.below(11))}),
            4 => {
                let small = self.r.below(8) as i64 - 3;
                json!({"col": "n", "op": pick(&mut self.r, &ops), "value": pick(&mut self.r, &[json!(small), json!(i64::MIN), json!(i64::MAX), json!(1i64 << 60)])})
            }
            5 => {
                json!({"col": "x", "op": pick(&mut self.r, &ops), "value": pick(&mut self.r, &[json!(1), json!(1.5), json!(0), json!(3), json!(1e20), json!(1i64 << 60), json!((1i64 << 60) + 1), json!(1e308), json!(5e-324)])})
            }
            6 => json!({"col": "flag", "op": pick(&mut self.r, &ops), "value": self.r.chance(0.5)}),
            7 => json!({"col": pick(&mut self.r, &["props", "props2"]), "path": self.path(), "op": "exists"}),
            8 | 9 => {
                let v = self.scalar();
                json!({"col": "props", "path": self.path(), "op": pick(&mut self.r, &ops), "value": v})
            }
            10 => {
                let v = self.scalar();
                json!({"col": "props", "path": self.path(), "op": pick(&mut self.r, &["contains", "prefix", "in"]),
                    "value": if v.is_array() { v } else { json!([v]) }})
            }
            11 => {
                let v = pick(&mut self.r, &[json!("s"), json!("pa"), json!("a"), json!(1), json!("")]);
                json!({"col": "props", "path": self.path(), "op": pick(&mut self.r, &["contains", "prefix"]), "value": v})
            }
            12 => json!({"bucket": {"col": "time"}, "interval": pick(&mut self.r, &["1d", "6h", "1w"]),
                "op": pick(&mut self.r, &ops), "value": T0 + self.r.below(8) as i64 * DAY / 2}),
            13 => {
                json!({"lookup": "plan", "op": pick(&mut self.r, &["eq", "ne", "prefix"]), "value": pick(&mut self.r, &["pro", "free", "a", "t"])})
            }
            14 => {
                json!({"col": "time", "op": pick(&mut self.r, &ops), "value": T0 + self.r.below(20) as i64 * DAY / 4})
            }
            16 => self.long_in(),
            15 if !self.added.is_empty() => {
                json!({"col": self.added[0], "op": pick(&mut self.r, &["eq", "ne", "prefix"]), "value": pick(&mut self.r, &["dflt", "given", "d"])})
            }
            17 if self.added.len() > 2 => {
                let ops = ["eq", "ne", "lt", "ge", "exists"];
                let op = pick(&mut self.r, &ops);
                match op {
                    "exists" => json!({"col": "seen", "op": op}),
                    _ => json!({"col": "seen", "op": op, "value": pick(&mut self.r, &[0, T0, T0 + DAY])}),
                }
            }
            _ => json!({"col": pick(&mut self.r, &["user", "n", "x", "flag", "props"]), "op": "exists"}),
        }
    }

    pub fn filter(&mut self, depth: u32) -> Json {
        let k = if depth == 0 { 9 } else { self.r.usize(10) };
        match k {
            0 => json!({"and": [self.filter(depth - 1), self.filter(depth - 1)]}),
            1 => json!({"or": [self.filter(depth - 1), self.filter(depth - 1)]}),
            2 => json!({"not": self.filter(depth - 1)}),
            _ => self.leaf(),
        }
    }

    fn expr(&mut self) -> (Json, bool) {
        let e = match self.r.usize(17) {
            14 => json!({"lookup": pick(&mut self.r, &MORE_LOOKUPS)}),
            15 => {
                let col = pick(&mut self.r, &["time", "n"]);
                self.bucket(col)
            }
            0 => json!("event"),
            1 => json!("user"),
            2 => json!("n"),
            3 => json!("x"),
            4 => json!("flag"),
            5 => json!("props"),
            6 => json!({"col": "props", "path": self.path()}),
            7 => json!({"col": pick(&mut self.r, &["props", "props2"]), "path": self.path()}),
            8 => json!({"lookup": "plan"}),
            9 => json!({"lookup": "nn"}),
            10 => json!({"bucket": {"col": "time"}, "interval": pick(&mut self.r, &["1d", "6h", "1w", "1mo"]),
                "offset": pick(&mut self.r, &["Z", "+05:30", "-08:00"])}),
            11 => json!({"lookup": "meta"}),
            12 if !self.added.is_empty() => json!(pick(&mut self.r, &self.added.clone())),
            _ => json!("time"),
        };
        (e, true)
    }

    pub fn query(&mut self) -> Json {
        let (from, to) = if self.r.chance(0.15) {
            (json!(TIME_MIN), json!(TIME_MAX + 1))
        } else {
            let from = T0 + self.r.below(6) as i64 * DAY - DAY;
            let to = from + (1 + self.r.below(14)) as i64 * DAY;
            if self.r.chance(0.1) {
                (json!(rf::format_rfc3339(from + 1, -300)), json!(rf::format_rfc3339(to, 90)))
            } else {
                (json!(from), json!(to))
            }
        };
        let mut q = json!({"from": from, "to": to, "lookups": lookups()});
        if self.r.chance(0.7) {
            q["where"] = self.filter(2);
        }
        if self.r.chance(0.3) {
            let mut cols = Vec::new();
            for i in 0..1 + self.r.usize(5) {
                let (e, _) = self.expr();
                let mut e = if e.is_string() { json!({"col": e}) } else { e };
                e["as"] = json!(format!("c{i}"));
                cols.push(e);
            }
            q["columns"] = Json::Array(cols.clone());
            if self.r.chance(0.15) {
                q.as_object_mut().unwrap().remove("columns");
                q["limit"] = json!(1 + self.r.usize(15));
                if self.r.chance(0.5) {
                    q["order_by"] = json!([{"col": pick(&mut self.r, &["event", "user", "n", "x", "props", "time"]), "desc": self.r.chance(0.5)}]);
                }
                return q;
            }
            if self.r.chance(0.9) {
                q["limit"] = json!(1 + self.r.usize(15));
            }
            let mut order = Vec::new();
            for _ in 0..self.r.usize(3) {
                order.push(json!({"col": format!("c{}", self.r.usize(cols.len())), "desc": self.r.chance(0.5)}));
            }
            q["order_by"] = Json::Array(order);
            return q;
        }
        let mut group = Vec::new();
        for i in 0..self.r.usize(3) {
            let (e, _) = self.expr();
            let mut e = if e.is_string() { json!({"col": e}) } else { e };
            e["as"] = json!(format!("g{i}"));
            group.push(e);
        }
        q["group_by"] = Json::Array(group.clone());
        let mut aggs = serde_json::Map::new();
        aggs.insert("count".into(), json!({"fn": "count"}));
        let fns = ["sum", "min", "max", "avg", "uniq", "uniq_exact", "quantiles"];
        for i in 0..self.r.usize(5) {
            let f = pick(&mut self.r, &fns);
            let (of, _) = self.expr();
            let mut a = json!({"fn": f, "of": of});
            if f == "quantiles" {
                let qs: Vec<f64> = (0..1 + self.r.usize(3))
                    .map(|_| pick(&mut self.r, &[0.0, 0.01, 0.25, 0.5, 0.9, 0.99, 1.0]))
                    .collect();
                a["q"] = json!(qs);
            }
            if self.r.chance(0.3) {
                a["where"] = self.filter(1);
            }
            aggs.insert(format!("a{i}"), a);
        }
        if self.r.chance(0.3) {
            let steps: Vec<Json> = (0..2 + self.r.usize(2)).map(|_| self.filter(0)).collect();
            let mode = pick(
                &mut self.r,
                &[
                    json!([]),
                    json!("strict_order"),
                    json!("strict_deduplication"),
                    json!("strict_increase"),
                    json!(["strict_order", "strict_increase"]),
                ],
            );
            aggs.insert(
                "funnel".into(),
                json!({"fn": "window_funnel", "window_ms": pick(&mut self.r, &[1, DAY / 2, DAY * 3]), "steps": steps, "mode": mode}),
            );
        }
        if self.r.chance(0.3) {
            let conds: Vec<Json> = (0..1 + self.r.usize(3)).map(|_| self.filter(0)).collect();
            aggs.insert("ret".into(), json!({"fn": "retention", "conditions": conds}));
        }
        let names: Vec<String> = (0..group.len()).map(|i| format!("g{i}")).chain(aggs.keys().cloned()).collect();
        q["aggregates"] = Json::Object(aggs);
        let mut outs = names.clone();
        if self.r.chance(0.3) {
            let mut then = json!({});
            let mut tg = Vec::new();
            for i in 0..self.r.usize(2) {
                let n = pick(&mut self.r, &names);
                if self.r.chance(0.3) {
                    tg.push(json!({"bucket": {"col": n}, "interval": pick(&mut self.r, &["1h", "1d", "1w", "1mo"]), "as": format!("t{i}")}));
                } else {
                    tg.push(json!({"col": n, "as": format!("t{i}")}));
                }
            }
            let mut ta = serde_json::Map::new();
            ta.insert("tc".into(), json!({"fn": "count"}));
            for i in 0..self.r.usize(3) {
                let f = pick(&mut self.r, &fns);
                let mut a = json!({"fn": f, "of": pick(&mut self.r, &names)});
                if f == "quantiles" {
                    a["q"] = json!([0.5]);
                }
                ta.insert(format!("ta{i}"), a);
            }
            outs = (0..tg.len()).map(|i| format!("t{i}")).chain(ta.keys().cloned()).collect();
            then["group_by"] = Json::Array(tg);
            then["aggregates"] = Json::Object(ta);
            q["then"] = then;
        }
        if self.r.chance(0.5) {
            q["limit"] = json!(1 + self.r.usize(5));
            let mut order = Vec::new();
            for _ in 0..self.r.usize(3) {
                order.push(json!({"col": pick(&mut self.r, &outs), "desc": self.r.chance(0.5)}));
            }
            q["order_by"] = Json::Array(order);
        }
        q
    }
}

/// Runs `q` on both databases, paging through rows mode with each one's own cursors.
async fn compare(a: &Arc<Db>, b: &Arc<Db>, q: &Json) -> std::result::Result<bool, String> {
    let mut qa = q.clone();
    let mut qb = q.clone();
    let mut pages = 0;
    let mut nonempty = false;
    let mut all: Vec<Vec<Json>> = Vec::new();
    loop {
        let ra = query(a, &qa).await;
        let rb = query(b, &qb).await;
        let (oa, ob) = match (ra, rb) {
            (Ok(x), Ok(y)) => (x, y),
            (Err(x), Err(y)) if x.to_string() == y.to_string() => {
                ERRS.lock().unwrap().push(x.to_string());
                return Ok(false);
            }
            (x, y) => {
                return Err(format!("query {qa}\nunsealed {:?}\n  sealed {:?}", x.map(|o| o.rows), y.map(|o| o.rows)));
            }
        };
        if oa.columns != ob.columns || oa.rows != ob.rows || oa.next_cursor.is_some() != ob.next_cursor.is_some() {
            return Err(format!(
                "query {qa}\npage {pages}\nunsealed {:?} more {}\n  sealed {:?} more {}\nstats {:?}",
                oa.rows,
                oa.next_cursor.is_some(),
                ob.rows,
                ob.next_cursor.is_some(),
                ob.stats
            ));
        }
        {
            let parsed = Query::parse(qb.clone()).map_err(|e| e.to_string())?;
            for max in [16 << 10, 64 << 10, 256 << 10] {
                let mut t = b.begin(Isolation::Serializable, Consistency::Strong).await.map_err(|e| e.to_string())?;
                let s = get_schema(&mut t, "events").await.map_err(|e| e.to_string())?;
                let limits = Limits { max_memory_bytes: max, ..Limits::default() };
                match exec::run(b.shared(), &b.id, &mut t, &s, &parsed, &limits).await {
                    Ok(o) if o.rows != ob.rows || o.next_cursor != ob.next_cursor => {
                        return Err(format!("query {qb} with {max} bytes\n got {:?}\nwant {:?}", o.rows, ob.rows));
                    }
                    Ok(_) | Err(Error::TooLarge(_)) => {}
                    Err(e) => return Err(format!("query {qb} with {max} bytes: {e}")),
                }
            }
        }
        nonempty |= !oa.rows.is_empty();
        all.extend(oa.rows.iter().cloned());
        pages += 1;
        if q.get("columns").is_some() && oa.next_cursor.is_none() {
            let mut fq = q.clone();
            fq["limit"] = json!(10_000);
            let full = query(a, &fq).await.map_err(|e| e.to_string())?;
            if full.rows != all {
                return Err(format!("pages of {q}\n concatenated {all:?}\n full {:?}", full.rows));
            }
        }
        match (oa.next_cursor, ob.next_cursor) {
            (Some(ca), Some(cb)) if pages < 30 => {
                qa["cursor"] = json!(ca);
                qb["cursor"] = json!(cb);
            }
            _ => return Ok(nonempty),
        }
    }
}

/// Checks a snapshot read's answer to `q` against some recent state of the
/// reference: a snapshot is at most 5 s old, so it may miss recent writes.
async fn check_snapshot(db: &Arc<Db>, history: &[Reference], q: &Json) -> std::result::Result<(), String> {
    let mut q = q.clone();
    if q.get("columns").is_some() || q.get("aggregates").is_none() {
        q["limit"] = json!(10_000);
    }
    let c = Consistency::Snapshot { max_staleness_ms: 5_000 };
    let mut last = Ok(());
    for r in history.iter().rev() {
        last = check_ref(db, r, &q, c).await;
        if last.is_ok() {
            return Ok(());
        }
    }
    last.map_err(|e| format!("a snapshot read matches no recent state; against the oldest: {e}"))
}

/// Checks `db`'s answer to `q`, every page of it in rows mode, against the
/// reference's.
async fn check_ref(db: &Arc<Db>, r: &Reference, q: &Json, c: Consistency) -> std::result::Result<(), String> {
    let Ok(parsed) = Query::parse(q.clone()) else { return Ok(()) };
    let want = r.query(&parsed);
    let mut page_q = q.clone();
    let (mut columns, mut rows, mut pages): (Vec<String>, _, _) = (Vec::new(), Vec::new(), Vec::new());
    loop {
        match (query_at(db, &page_q, c).await, &want) {
            (Err(_), Err(_)) if pages.is_empty() => return Ok(()),
            (Err(e), _) => return Err(format!("query {page_q}\nthe engine refused: {e}; the reference: {want:?}")),
            (Ok(o), Err(w)) => {
                return Err(format!("query {q}\nthe engine answered {:?}; the reference refuses: {w}", o.rows));
            }
            (Ok(o), Ok(_)) => {
                columns.clear();
                columns.extend(o.columns);
                pages.push((o.rows.len(), o.next_cursor.is_some()));
                rows.extend(o.rows);
                match o.next_cursor {
                    Some(cur) if matches!(parsed.mode, query::Mode::Rows { .. }) && pages.len() < 30 => {
                        page_q["cursor"] = json!(cur);
                    }
                    _ => break,
                }
            }
        }
    }
    let Err(e) = want.unwrap().check(&columns, &rows, &pages) else { return Ok(()) };
    let mut first = String::new();
    if let Some(o) = q.as_object()
        && o.contains_key("then")
    {
        let mut fq = q.clone();
        for k in ["then", "order_by", "limit"] {
            fq.as_object_mut().unwrap().remove(k);
        }
        let got = query_at(db, &fq, c).await.map(|o| o.rows);
        let want = r.query(&Query::parse(fq).unwrap());
        first = format!("\nfirst stage: the engine {got:?}\nthe reference {want:?}");
    }
    Err(format!("query {q}\n{e}{first}"))
}

pub(super) async fn run(seed: u64, steps: usize) -> std::result::Result<usize, String> {
    let mut r = Rng::new(seed);
    let cfg = EngineConfig {
        event_seal_bytes: usize::MAX >> 1,
        event_granule_rows: 1 + r.usize(6),
        event_json_max_paths: r.usize(3).max(1),
        event_json_directory_bytes: pick(&mut r, &[1, 8, 30, 1 << 20]),
        event_mask_inline_bytes: pick(&mut r, &[1, 4096]),
        ..EngineConfig::small_for_tests()
    };
    VARIANT.store(seed as usize, std::sync::atomic::Ordering::Relaxed);
    let (eng, _store, env) = engine(seed, cfg);
    env.spawn(async {});
    let a = create_db(&eng, "unsealed").await;
    let b = create_db(&eng, "sealed").await;
    let table = {
        let mut t = b.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        get_schema(&mut t, "events").await.unwrap().id
    };
    let mut g = G { r, t: T0, added: Vec::new(), dropped: false };
    let mut model = reference();
    // Reference states, newest last, for snapshot reads.
    let mut history = vec![model.clone()];
    let bind: super::bulk::BindFn = Arc::new(|db: Arc<Db>, b| Box::pin(async move { db.bind(b).await }));
    let bufs = super::bulk::Buffers::new(super::bulk::BufferConfig { wait_ms: 0, max_bytes: 4096 }, bind);
    let err = |e: Error| e.to_string();
    let mut answered = 0;
    for step in 0..steps {
        match g.r.usize(12) {
            0..=3 => {
                let rows: Vec<Json> = (0..1 + g.r.usize(12)).map(|_| g.row()).collect();
                txn_do(&a, async |t| insert(t, "events", &rows, 0).await).await.map_err(err)?;
                model.insert(&rows)?;
                if g.r.chance(0.5) {
                    txn_do(&b, async |t| insert(t, "events", &rows, 0).await).await.map_err(err)?;
                } else {
                    let mut t = b.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                    let s = get_schema(&mut t, "events").await.map_err(err)?;
                    let valid = validate_rows(&s, &rows, 0).map_err(err)?;
                    bufs.submit(
                        b.clone(),
                        s,
                        valid,
                        env.random_u64(),
                        tokio::time::Instant::now() + std::time::Duration::from_secs(10),
                    )
                    .await
                    .map_err(err)?;
                }
            }
            4 | 5 => match maint::seal(&b, table).await {
                Ok(_) | Err(Error::Conflict(_)) => {}
                Err(e) => return Err(e.to_string()),
            },
            6 => maint::maintain(&b, table, 8).await.map_err(err)?,
            7 => {
                let fj = g.filter(1);
                if fj.to_string().contains("lookup") {
                    continue;
                }
                let Ok(f) = Filter::parse(fj) else { continue };
                let (from, to) = (T0 + g.r.below(8) as i64 * DAY / 2, T0 + (2 + g.r.below(10)) as i64 * DAY);
                let x = txn_do(&a, async |t| maint::delete(&a, t, "events", from, to, &f).await).await;
                let y = txn_do(&b, async |t| maint::delete(&b, t, "events", from, to, &f).await).await;
                let want = model.delete(from, to, &f);
                match (x, y, want) {
                    (Ok(x), Ok(y), Ok(w)) if x == y && x == w => {}
                    (Err(Error::InvalidArgument(_)), Err(Error::InvalidArgument(_)), Err(_)) => {}
                    (x, y, w) => {
                        return Err(format!(
                            "seed {seed} step {step}: delete [{from}, {to}) {f:?}: unsealed {x:?}, sealed {y:?}, reference {w:?}"
                        ));
                    }
                }
            }
            8 if g.r.chance(0.1) && !g.dropped => {
                for db in [&a, &b] {
                    txn_do(db, async |t| drop_column(t, "events", "flag").await).await.map_err(err)?;
                    let spec: ColumnSpec =
                        serde_json::from_value(json!({"name": "flag", "type": "string", "default": "re"})).unwrap();
                    txn_do(db, async |t| add_column(t, "events", &spec).await).await.map_err(err)?;
                }
                model.drop_column("flag");
                model.add_column(&json!({"name": "flag", "type": "string", "default": "re"}));
                g.dropped = true;
            }
            8 if g.added.len() < 3 && g.r.chance(0.3) => {
                let (name, spec) = match g.added.len() {
                    0 => ("tier", json!({"name": "tier", "type": "string", "default": "dflt"})),
                    1 => ("lvl", json!({"name": "lvl", "type": "int", "default": 7})),
                    _ => ("seen", json!({"name": "seen", "type": "time", "nullable": true, "default": "now"})),
                };
                model.add_column(&spec);
                let spec: ColumnSpec = serde_json::from_value(spec).unwrap();
                for db in [&a, &b] {
                    txn_do(db, async |t| add_column(t, "events", &spec).await).await.map_err(err)?;
                }
                g.added.push(name);
            }
            _ => {
                let q = g.query();
                answered += compare(&a, &b, &q).await.map_err(|e| format!("seed {seed} step {step}: {e}"))? as usize;
                check_ref(&a, &model, &q, Consistency::Strong)
                    .await
                    .map_err(|e| format!("seed {seed} step {step}: {e}"))?;
                if g.r.chance(0.3) {
                    check_snapshot(&a, &history, &q).await.map_err(|e| format!("seed {seed} step {step}: {e}"))?;
                }
                continue;
            }
        }
        history.push(model.clone());
        if history.len() > 20 {
            history.remove(0);
        }
    }
    maint::seal(&b, table).await.map_err(err)?;
    maint::maintain(&b, table, 8).await.map_err(err)?;
    for _ in 0..40 {
        let q = g.query();
        answered += compare(&a, &b, &q).await.map_err(|e| format!("seed {seed} final: {e}"))? as usize;
        for db in [&a, &b] {
            check_ref(db, &model, &q, Consistency::Strong).await.map_err(|e| format!("seed {seed} final: {e}"))?;
        }
    }
    Ok(answered)
}

#[test]
fn diff_unsealed_and_sealed_answer_alike() {
    let seeds: u64 = std::env::var("DIFF_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(20);
    let first: u64 = std::env::var("DIFF_FIRST").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let steps: usize = std::env::var("DIFF_STEPS").ok().and_then(|s| s.parse().ok()).unwrap_or(150);
    let mut answered = 0;
    let mut fails = Vec::new();
    for seed in first..first + seeds {
        rt().block_on(async {
            match run(seed, steps).await {
                Ok(n) => answered += n,
                Err(e) => fails.push(e),
            }
        });
    }
    for f in &fails {
        eprintln!("FAIL {}\n", &f[..f.len().min(3000)]);
    }
    assert!(fails.is_empty(), "{} failing seeds; {answered} answered", fails.len());
    let mut errs = ERRS.lock().unwrap().clone();
    errs.sort();
    errs.dedup_by(|a, b| a[..a.len().min(40)] == b[..b.len().min(40)]);
    eprintln!("answered {answered}; errors {:?}", errs);
}

/// `funnel` answers like a literal simulation of every attempt, one per event
/// matching the first step, in every combination of modes.
#[test]
fn diff_funnel_matches_brute_force() {
    use super::agg::funnel;
    use super::query::FunnelMode;
    use crate::engine::events::Pos;
    fn brute(evs: &[(i64, Pos, u32)], steps: usize, w: i64, m: FunnelMode) -> usize {
        let mut e = evs.to_vec();
        e.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        let mut best = 0;
        for (i, &(start, _, mask)) in e.iter().enumerate() {
            if steps == 0 || mask & 1 == 0 {
                continue;
            }
            let (mut level, mut last) = (1, start);
            for &(t, _, mask) in &e[i + 1..] {
                if level == steps || t > start + w {
                    break;
                }
                let next = mask & (1 << level) != 0;
                if next && (!m.strict_increase || t > last) {
                    level += 1;
                    last = t;
                } else if (m.strict_order && !next) || (m.strict_deduplication && mask & ((1 << level) - 1) != 0) {
                    break;
                }
            }
            best = best.max(level);
        }
        best
    }
    let mut r = Rng::new(3);
    for it in 0..100_000 {
        let steps = r.usize(5);
        let n = r.usize(10);
        let mut evs: Vec<(i64, Pos, u32)> = (0..n)
            .map(|i| {
                let mask = if r.chance(0.6) { 1 << r.usize(steps.max(1)) } else { r.below(1 << steps) as u32 };
                (r.below(8) as i64, Pos { version: 1, ordinal: i as u32 }, mask)
            })
            .collect();
        r.shuffle(&mut evs);
        let w = r.below(6) as i64;
        for bits in 0..8 {
            let m = FunnelMode {
                strict_order: bits & 1 != 0,
                strict_deduplication: bits & 2 != 0,
                strict_increase: bits & 4 != 0,
            };
            let got = funnel(&evs, steps, w, m);
            let want = brute(&evs, steps, w, m);
            assert_eq!(got, want, "iter {it} {m:?} steps {steps} w {w} evs {evs:?}");
        }
    }
}

/// Counts of random filters over sealed and unsealed events, against the
/// reference's: comparisons of every op on columns, JSON paths of mixed
/// types, lookups and buckets, and refusals of mistyped arguments.
#[test]
fn diff_filters_match_the_json_reference() {
    let seeds: u64 = std::env::var("DIFF_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(20);
    let mut bad = Vec::new();
    let mut checked = 0;
    for seed in 0..seeds {
        rt().block_on(async {
            let mut r = Rng::new(seed);
            let cfg = EngineConfig {
                event_seal_bytes: usize::MAX >> 1,
                event_granule_rows: 1 + r.usize(6),
                event_json_max_paths: r.usize(3).max(1),
                event_json_directory_bytes: pick(&mut r, &[1, 8, 30, 1 << 20]),
                ..EngineConfig::small_for_tests()
            };
            VARIANT.store(seed as usize, std::sync::atomic::Ordering::Relaxed);
            let (eng, _store, env) = engine(seed, cfg);
            env.spawn(async {});
            let db = create_db(&eng, "d").await;
            let id = {
                let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                get_schema(&mut t, "events").await.unwrap().id
            };
            let mut g = G { r, t: T0, added: Vec::new(), dropped: false };
            let mut model = reference();
            for _ in 0..10 {
                let batch: Vec<Json> = (0..20).map(|_| g.row()).collect();
                txn_do(&db, async |t| insert(t, "events", &batch, 0).await).await.unwrap();
                model.insert(&batch).unwrap();
                if g.r.chance(0.5) {
                    maint::seal(&db, id).await.unwrap();
                }
            }
            for _ in 0..300 {
                let q = json!({"from": TIME_MIN, "to": TIME_MAX + 1, "where": g.filter(2), "lookups": lookups(),
                    "aggregates": {"n": {"fn": "count"}}});
                checked += 1;
                if let Err(e) = check_ref(&db, &model, &q, Consistency::Strong).await {
                    bad.push(format!("seed {seed}: {e}"));
                }
            }
        });
    }
    for b in bad.iter().take(20) {
        eprintln!("{b}");
    }
    assert!(bad.is_empty(), "{} of {checked} filters differ", bad.len());
}

/// A database named `id` holding `rows`, inserted in batches of which some
/// are sealed into parts, then merged, and a reference holding them.
async fn load(eng: &Arc<Engine>, id: &str, rows: &[Json], r: &mut Rng) -> (Arc<Db>, Reference) {
    let db = create_db(eng, id).await;
    let table = {
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        get_schema(&mut t, "events").await.unwrap().id
    };
    let mut model = reference();
    let mut at = 0;
    while at < rows.len() {
        let batch = &rows[at..rows.len().min(at + 1 + r.usize(400))];
        at += batch.len();
        txn_do(&db, async |t| insert(t, "events", batch, 0).await).await.unwrap();
        model.insert(batch).unwrap();
        if r.chance(0.7) {
            maint::seal(&db, table).await.unwrap();
        }
    }
    maint::maintain(&db, table, 8).await.unwrap();
    (db, model)
}

/// `uniq` counts exactly up to 1,024 distinct values, whether they arrive in
/// one part or many, as integers or as equal floats, and within a few
/// percent beyond.
#[test]
fn diff_uniq_is_exact_up_to_1024_values() {
    rt().block_on(async {
        let cfg = EngineConfig { event_seal_bytes: usize::MAX >> 1, event_granule_rows: 97, ..EngineConfig::small_for_tests() };
        let (eng, _store, env) = engine(5, cfg);
        env.spawn(async {});
        let mut r = Rng::new(5);
        let mut bad = Vec::new();
        for n in [1, 2, 700, 1023, 1024, 1025, 1100, 3000] {
            let mut rows = Vec::new();
            for i in 0..n {
                let v = i as i64 * 7 - 3000;
                for copy in 0..2 {
                    let num = if (i + copy) % 2 == 0 { json!(v) } else { json!(v as f64) };
                    rows.push(json!({"time": T0 + r.below(3 * DAY as u64) as i64, "event": (["a", "b"][copy]),
                        "n": v, "user": format!("u{i}"), "x": v as f64 + 0.5, "props": {"v": num}}));
                }
            }
            r.shuffle(&mut rows);
            let (db, model) = load(&eng, &format!("uniq{n}"), &rows, &mut r).await;
            let aggs = json!({"un": {"fn": "uniq", "of": "n"}, "ux": {"fn": "uniq", "of": "x"},
                "uu": {"fn": "uniq", "of": "user"}, "up": {"fn": "uniq", "of": {"col": "props", "path": ["v"]}},
                "half": {"fn": "uniq", "of": "n", "where": {"col": "n", "op": "lt", "value": n as i64 * 7 / 2 - 3000}}});
            for group in [json!([]), json!(["event"])] {
                let q = json!({"from": T0, "to": T0 + 3 * DAY, "group_by": group, "aggregates": aggs});
                if let Err(e) = check_ref(&db, &model, &q, Consistency::Strong).await {
                    bad.push(format!("{n} values: {e}"));
                }
            }
        }
        assert!(bad.is_empty(), "{}", bad.join("\n"));
    });
}

/// `quantiles` answers within 1% of a value at each level's rank, over
/// values spanning every magnitude and sign, sealed and merged or not.
#[test]
fn diff_quantiles_are_within_one_percent() {
    rt().block_on(async {
        let cfg = EngineConfig { event_seal_bytes: usize::MAX >> 1, event_granule_rows: 61, ..EngineConfig::small_for_tests() };
        let (eng, _store, env) = engine(6, cfg);
        env.spawn(async {});
        let mut r = Rng::new(6);
        let mut bad = Vec::new();
        for round in 0..6 {
            let mut rows = Vec::new();
            for i in 0..1500 {
                let x = match r.usize(8) {
                    0 => pick(&mut r, &[0.0, 1e308, -1e308, 5e-324, -5e-324, f64::MAX, 2.2250738585072014e-308, 1.0]),
                    1 => (r.below(5) as f64 - 2.0) * 0.5,
                    _ => {
                        let mag = 10f64.powf(r.unit() * [600.0, 20.0, 4.0][round % 3] - [300.0, 10.0, 2.0][round % 3]);
                        if r.chance(0.3) { -mag } else { mag }
                    }
                };
                let n = match r.usize(4) {
                    0 => pick(&mut r, &[i64::MIN, i64::MAX, 0, -1, 1 << 53, (1 << 53) + 1]),
                    _ => r.below(2000) as i64 - 1000,
                };
                rows.push(json!({"time": T0 + i, "event": (["a", "b", "c"][r.usize(3)]), "x": x, "n": n,
                    "props": {"q": if r.chance(0.5) { json!(x) } else { json!(n) }}}));
            }
            let (db, model) = load(&eng, &format!("q{round}"), &rows, &mut r).await;
            let qs = json!([0.0, 0.001, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 0.999, 1.0]);
            let aggs = json!({"x": {"fn": "quantiles", "of": "x", "q": qs}, "n": {"fn": "quantiles", "of": "n", "q": qs},
                "p": {"fn": "quantiles", "of": {"col": "props", "path": ["q"]}, "q": qs}});
            for group in [json!([]), json!(["event"])] {
                let q = json!({"from": T0, "to": T0 + DAY, "group_by": group, "aggregates": aggs});
                if let Err(e) = check_ref(&db, &model, &q, Consistency::Strong).await {
                    bad.push(format!("round {round}: {e}"));
                }
            }
        }
        assert!(bad.is_empty(), "{}", bad.join("\n"));
    });
}

/// A float sum beyond the double range answers null, so a second stage
/// that reads it should see null: not count it as a distinct value, nor
/// group it apart from other nulls, nor sort it above every number.
#[test]
#[ignore = "product bug: an overflowed sum is +inf inside the engine but null in answers"]
fn float_sum_overflow_is_null_in_then() {
    rt().block_on(async {
        let (eng, _store, env) = engine(1, EngineConfig::small_for_tests());
        env.spawn(async {});
        let db = create_db(&eng, "d").await;
        let rows = vec![
            json!({"time": T0, "event": "a", "user": "big", "x": 1e308}),
            json!({"time": T0 + 1, "event": "a", "user": "big", "x": 1e308}),
            json!({"time": T0 + 2, "event": "a", "user": "one", "x": 1.0}),
            json!({"time": T0 + 3, "event": "a", "user": "none"}),
        ];
        txn_do(&db, async |t| insert(t, "events", &rows, 0).await).await.unwrap();
        let first = json!({"from": T0, "to": T0 + DAY, "group_by": ["user"],
            "aggregates": {"s": {"fn": "sum", "of": "x"}, "a": {"fn": "avg", "of": "x"}}});
        let out = query(&db, &first).await.unwrap();
        assert_eq!(out.rows[0], [json!("big"), json!(1e308), Json::Null], "the sum answers null");
        let mut q = first.clone();
        q["then"] =
            json!({"aggregates": {"sums": {"fn": "uniq_exact", "of": "s"}, "avgs": {"fn": "uniq_exact", "of": "a"}}});
        let distinct = query(&db, &q).await.unwrap().rows;
        let mut q = first.clone();
        q["order_by"] = json!([{"col": "s", "desc": true}, {"col": "a", "desc": true}]);
        let order = query(&db, &q).await.unwrap().rows;
        assert_eq!(
            (distinct[0].clone(), order.iter().map(|r| r[0].clone()).collect::<Vec<_>>()),
            (vec![json!(2), json!(2)], vec![json!("one"), json!("none"), json!("big")]),
            "distinct non-null sums and averages; users by sum, nulls last when descending"
        );
    });
}

/// A delete's filter is bound only when a row or part falls in its range, so
/// one naming a column that does not exist (or a lookup, or a whole JSON
/// column) deletes nothing with 200 over unsealed rows, but fails with 400
/// once the same events are sealed into a part of the partition.
#[test]
fn delete_filters_are_validated_whatever_the_storage() {
    rt().block_on(async {
        let cfg = EngineConfig { event_seal_bytes: usize::MAX >> 1, ..EngineConfig::small_for_tests() };
        let (eng, _store, env) = engine(1, cfg);
        env.spawn(async {});
        let unsealed = create_db(&eng, "u").await;
        let sealed = create_db(&eng, "s").await;
        for db in [&unsealed, &sealed] {
            txn_do(db, async |t| insert(t, "events", &[json!({"time": T0, "event": "a"})], 0).await).await.unwrap();
        }
        let id = {
            let mut t = sealed.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            get_schema(&mut t, "events").await.unwrap().id
        };
        assert!(maint::seal(&sealed, id).await.unwrap());
        let f = Filter::parse(json!({"col": "no_such_column", "op": "eq", "value": 1})).unwrap();
        let del =
            async |db: &Arc<Db>| txn_do(db, async |t| maint::delete(db, t, "events", T0 + 1, T0 + 2, &f).await).await;
        let (a, b) = (del(&unsealed).await, del(&sealed).await);
        assert!(
            matches!(a, Err(Error::InvalidArgument(_))) && matches!(b, Err(Error::InvalidArgument(_))),
            "unsealed {a:?}, sealed {b:?}"
        );
    });
}

/// Numbers compare by exact value as scalars, but through f64 inside JSON
/// arrays: [2^60 as a float] equals and contains 2^60 + 1.
#[test]
fn json_array_numbers_compare_exactly() {
    rt().block_on(async {
        let (eng, _store, env) = engine(1, EngineConfig::small_for_tests());
        env.spawn(async {});
        let db = create_db(&eng, "d").await;
        let row =
            json!({"time": T0, "event": "a", "props": {"s": 1152921504606846976.0, "a": [1152921504606846976.0]}});
        txn_do(&db, async |t| insert(t, "events", std::slice::from_ref(&row), 0).await).await.unwrap();
        let count = async |w: Json| {
            let q = json!({"from": T0, "to": T0 + DAY, "where": w, "aggregates": {"n": {"fn": "count"}}});
            query(&db, &q).await.unwrap().rows[0][0].as_i64().unwrap()
        };
        let big = 1152921504606846977i64;
        let scalar = count(json!({"col": "props", "path": ["s"], "op": "eq", "value": big})).await;
        let eq = count(json!({"col": "props", "path": ["a"], "op": "eq", "value": [big]})).await;
        let contains = count(json!({"col": "props", "path": ["a"], "op": "contains", "value": big})).await;
        assert_eq!((scalar, eq, contains), (0, 0, 0), "scalar eq, array eq, array contains");
    });
}

#[test]
fn json_values_equal_in_filters_group_alike() {
    rt().block_on(async {
        let (eng, _store, env) = engine(1, EngineConfig::small_for_tests());
        env.spawn(async {});
        let db = create_db(&eng, "d").await;
        let rows = vec![
            json!({"time": T0, "event": "a", "props": {"a": [1]}}),
            json!({"time": T0 + 1, "event": "a", "props": {"a": [1.0]}}),
        ];
        txn_do(&db, async |t| insert(t, "events", &rows, 0).await).await.unwrap();
        let q = json!({"from": T0, "to": T0 + DAY, "where": {"col": "props", "path": ["a"], "op": "eq", "value": [1]},
            "group_by": [{"col": "props", "path": ["a"], "as": "a"}], "aggregates": {"n": {"fn": "count"},
            "u": {"fn": "uniq_exact", "of": {"col": "props", "path": ["a"]}}}});
        let out = query(&db, &q).await.unwrap();
        assert_eq!(out.rows.len(), 1, "values the filter calls equal form {:?}", out.rows);
    });
}

#[test]
fn rows_mode_refuses_duplicate_output_names() {
    rt().block_on(async {
        let (eng, _store, env) = engine(1, EngineConfig::small_for_tests());
        env.spawn(async {});
        let db = create_db(&eng, "d").await;
        let q = json!({"from": T0, "to": T0 + DAY, "columns": ["n", {"col": "x", "as": "n"}]});
        let r = query(&db, &q).await;
        assert!(r.is_err(), "answered columns {:?}", r.map(|o| o.columns));
    });
}

/// A float column refuses integer arguments beyond i64, as no event holds
/// them, rather than rounding u64::MAX to 2^64; floats still compare.
#[test]
fn float_columns_refuse_integers_beyond_i64() {
    rt().block_on(async {
        let (eng, _store, env) = engine(1, EngineConfig::small_for_tests());
        env.spawn(async {});
        let db = create_db(&eng, "d").await;
        let row = json!({"time": T0, "event": "a", "x": 18446744073709551616.0});
        txn_do(&db, async |t| insert(t, "events", std::slice::from_ref(&row), 0).await).await.unwrap();
        let count = async |w: Json| {
            let q = json!({"from": T0, "to": T0 + DAY, "where": w, "aggregates": {"n": {"fn": "count"}}});
            query(&db, &q).await.map(|o| o.rows[0][0].as_i64().unwrap())
        };
        let big = u64::MAX;
        let eq = count(json!({"col": "x", "op": "eq", "value": big})).await;
        let gt = count(json!({"col": "x", "op": "gt", "value": big})).await;
        let le = count(json!({"col": "x", "op": "le", "value": big})).await;
        assert!(
            [&eq, &gt, &le].iter().all(|r| matches!(r, Err(Error::InvalidArgument(_)))),
            "x = 2^64: eq, gt, le u64::MAX answered {eq:?} {gt:?} {le:?}"
        );
        let ge = count(json!({"col": "x", "op": "ge", "value": 18446744073709551616.0})).await;
        assert_eq!(ge.ok(), Some(1));
    });
}

/// A row-table float column refuses an integer no float equals instead of
/// rounding it, so it never answers unlike a JSON column holding the value.
#[test]
fn row_table_float_columns_refuse_integers_no_float_equals() {
    rt().block_on(async {
        let (eng, _store, env) = engine(1, EngineConfig::small_for_tests());
        env.spawn(async {});
        let db = create_db(&eng, "d").await;
        let row = json!({"id": 100, "plan": "big", "score": 9007199254740992.0, "meta": {"v": 9007199254740992.0}});
        let d = db.clone();
        txn_do(&db, async |t| {
            let b = crate::table::BlobResolver { db: &d };
            crate::table::insert(t, &b, "nums", &row, crate::table::InsertMode::Insert).await
        })
        .await
        .unwrap();
        let n = async |f: Json| {
            let q: crate::table::Query = serde_json::from_value(json!({"filter": f})).unwrap();
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            crate::table::query(&mut t, "nums", &q).await.map(|r| r.rows.len())
        };
        let json_col = n(json!({"col": "meta", "op": "eq", "value": {"v": 9007199254740993i64}})).await;
        let float_col = n(json!({"col": "score", "op": "eq", "value": 9007199254740993i64})).await;
        assert!(
            json_col.as_ref().is_ok_and(|n| *n == 0) && matches!(float_col, Err(Error::InvalidArgument(_))),
            "2^53 eq 2^53 + 1: in a JSON column {json_col:?}, in a float column {float_col:?}"
        );
        let exact = n(json!({"col": "score", "op": "eq", "value": 9007199254740992i64})).await;
        assert_eq!(exact.ok(), Some(1), "an integer a float equals compares");
    });
}

/// Each funnel attempt is judged on its own events: an attempt whose window
/// expired days ago neither ends nor shortens a later, clean one.
#[test]
fn funnel_modes_ignore_expired_attempts() {
    rt().block_on(async {
        let (eng, _store, env) = engine(1, EngineConfig::small_for_tests());
        env.spawn(async {});
        let db = create_db(&eng, "d").await;
        let order = [(0, "a"), (1, "b"), (2 * DAY, "a"), (2 * DAY + 1, "b"), (2 * DAY + 2, "c")];
        let dedup = [(0, "a"), (2 * DAY, "a"), (2 * DAY + 1, "b"), (2 * DAY + 2, "c")];
        let mut rows = Vec::new();
        for (user, evs) in [("order", &order[..]), ("dedup", &dedup[..])] {
            rows.extend(evs.iter().map(|(t, e)| json!({"time": T0 + t, "event": e, "user": user})));
        }
        txn_do(&db, async |t| insert(t, "events", &rows, 0).await).await.unwrap();
        let steps: Vec<Json> =
            ["a", "b", "c"].iter().map(|e| json!({"col": "event", "op": "eq", "value": e})).collect();
        let mut got = Vec::new();
        for (user, mode) in [("order", "strict_order"), ("dedup", "strict_deduplication")] {
            let q = json!({"from": T0, "to": T0 + 3 * DAY, "where": {"col": "user", "op": "eq", "value": user},
                "aggregates": {"f": {"fn": "window_funnel", "window_ms": 60_000, "steps": steps, "mode": mode}}});
            got.push(query(&db, &q).await.unwrap().rows[0][0].clone());
        }
        assert_eq!(got, [json!(3), json!(3)], "strict_order, strict_deduplication");
    });
}

/// A mean of finite values is finite even when their sum is beyond f64.
#[test]
fn avg_of_huge_values_is_finite() {
    rt().block_on(async {
        let (eng, _store, env) = engine(1, EngineConfig::small_for_tests());
        env.spawn(async {});
        let db = create_db(&eng, "d").await;
        let rows: Vec<Json> = (0..2).map(|i| json!({"time": T0 + i, "event": "a", "x": 1e308})).collect();
        txn_do(&db, async |t| insert(t, "events", &rows, 0).await).await.unwrap();
        let q = json!({"from": T0, "to": T0 + DAY, "aggregates": {"m": {"fn": "avg", "of": "x"}}});
        assert_eq!(query(&db, &q).await.unwrap().rows[0][0], json!(1e308));
    });
}
