//! End-to-end tests of event tables on a single-node engine.
//!
//! The main property test drives a table through random transactional and
//! bulk inserts, seals, merges and deletes, and checks that every random
//! query answers exactly as it does on a model table that holds the same
//! rows unsealed: so part encoding, JSON flattening, pruning, masks and
//! merges never change an answer.

use super::bulk::{BindFn, BufferConfig, Buffers};
use super::exec::{self, Limits};
use super::query::{Filter, Query};
use super::*;
use crate::engine::tests::{engine, rt};
use crate::engine::{Consistency, Db, Engine, EngineConfig, Isolation};
use crate::probe::Probe;
use crate::util::rng::Rng;
use serde_json::json;
use std::sync::Arc;

const DAY: i64 = 86_400_000;
const T0: i64 = 1_700_000_000_000;

fn table_def() -> CreateEvents {
    serde_json::from_value(json!({
        "name": "events",
        "columns": [
            {"name": "time", "type": "time"},
            {"name": "event", "type": "string", "nullable": false},
            {"name": "user", "type": "string"},
            {"name": "n", "type": "int"},
            {"name": "x", "type": "float"},
            {"name": "flag", "type": "bool"},
            {"name": "props", "type": "json"}
        ],
        "time_column": "time",
        "order_by": ["event", "time"],
        "partition": "day",
        "skip_indexes": [{"column": "user", "type": "bloom"}]
    }))
    .unwrap()
}

fn cfg() -> EngineConfig {
    EngineConfig { event_seal_bytes: usize::MAX >> 1, ..EngineConfig::small_for_tests() }
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

async fn query(db: &Arc<Db>, q: &Json) -> Result<exec::Output> {
    let q = Query::parse(q.clone())?;
    let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await?;
    let s = get_schema(&mut t, "events").await?;
    exec::run(db.shared(), &db.id, &mut t, &s, &q, &Limits::default()).await
}

fn buffers() -> Arc<Buffers> {
    let bind: BindFn = Arc::new(|db: Arc<Db>, b| Box::pin(async move { db.bind(b).await }));
    Buffers::new(BufferConfig { wait_ms: 5, max_bytes: 4096 }, bind)
}

struct Gen {
    r: Rng,
    next_time: i64,
}

impl Gen {
    fn row(&mut self) -> Json {
        let r = &mut self.r;
        self.next_time += 1 + r.below(DAY as u64 / 3) as i64;
        let mut row = json!({
            "time": self.next_time,
            "event": (["a", "b", "c", "page", "pager"][r.usize(5)]),
        });
        if r.chance(0.8) {
            row["user"] = json!(format!("u{}", r.below(12)));
        }
        if r.chance(0.7) {
            row["n"] = json!(r.below(200) as i64 - 100);
        }
        if r.chance(0.6) {
            row["x"] = json!(((r.unit() - 0.3) * 1000.0 * 8.0).round() / 8.0);
        }
        if r.chance(0.5) {
            row["flag"] = json!(r.chance(0.5));
        }
        if r.chance(0.8) {
            let mut p = serde_json::Map::new();
            for k in 0..r.usize(6) {
                let key = format!("k{}", (k * 3 + r.usize(4)) % 9);
                let v = match r.usize(6) {
                    0 => json!(r.below(5)),
                    1 => json!(format!("s{}", r.below(4))),
                    2 => json!(r.chance(0.5)),
                    3 => json!({"deep": r.below(3), "z": null}),
                    4 => json!([r.below(3), "a"]),
                    _ => json!(0.5 + r.below(3) as f64),
                };
                p.insert(key, v);
            }
            row["props"] = Json::Object(p);
        }
        row
    }

    fn filter(&mut self) -> Json {
        let r = &mut self.r;
        let leaf = |r: &mut Rng| match r.usize(12) {
            0 => json!({"col": "event", "op": "eq", "value": (["a", "b", "page", "zzz"][r.usize(4)])}),
            1 => json!({"col": "event", "op": "in", "value": ["a", "c"]}),
            2 => json!({"col": "event", "op": "prefix", "value": "pag"}),
            3 => json!({"col": "user", "op": "eq", "value": format!("u{}", r.below(14))}),
            4 => json!({"col": "n", "op": (["lt", "ge", "eq", "ne"][r.usize(4)]), "value": r.below(200) as i64 - 100}),
            5 => json!({"col": "x", "op": "gt", "value": r.unit() * 500.0}),
            6 => json!({"col": "flag", "op": "eq", "value": r.chance(0.5)}),
            7 => json!({"col": "props", "path": [format!("k{}", r.below(9))], "op": "exists"}),
            8 => json!({"col": "props", "path": [format!("k{}", r.below(9))], "op": "eq", "value": r.below(5)}),
            9 => json!({"col": "props", "path": [format!("k{}", r.below(9)), "deep"], "op": "le", "value": 1}),
            10 => json!({"col": "time", "op": "lt", "value": T0 + r.below(20) as i64 * DAY / 2}),
            _ => json!({"col": "user", "op": "exists"}),
        };
        match r.usize(5) {
            0 => json!({"and": [leaf(r), leaf(r)]}),
            1 => json!({"or": [leaf(r), leaf(r)]}),
            2 => json!({"not": leaf(r)}),
            _ => leaf(r),
        }
    }

    fn query(&mut self) -> Json {
        let from = T0 + self.r.below(6) as i64 * DAY - DAY;
        let to = from + (1 + self.r.below(12)) as i64 * DAY;
        let mut q = json!({"from": from, "to": to});
        if self.r.chance(0.7) {
            q["where"] = self.filter();
        }
        let r = &mut self.r;
        if r.chance(0.25) {
            if r.chance(0.7) {
                q["columns"] =
                    json!(["time", "event", "user", "n", "x", "flag", "props", {"col": "props", "path": ["k1"]}]);
            }
            q["limit"] = json!(1 + r.usize(30));
            if r.chance(0.3) {
                q["order_by"] = json!([{"col": "n", "desc": true}, "time"]);
            }
            return q;
        }
        let group = match r.usize(6) {
            0 => json!([]),
            1 => json!(["event"]),
            2 => json!([{"bucket": {"col": "time"}, "interval": (["1d", "6h", "1w"][r.usize(3)])}]),
            3 => json!([{"col": "props", "path": [format!("k{}", r.below(9))], "as": "p"}]),
            4 => json!(["user", "flag"]),
            _ => json!(["user"]),
        };
        q["group_by"] = group.clone();
        let mut aggs = serde_json::Map::new();
        aggs.insert("count".into(), json!({"fn": "count"}));
        for (name, a) in [
            ("sum_n", json!({"fn": "sum", "of": "n"})),
            ("sum_x", json!({"fn": "sum", "of": "x"})),
            ("min_n", json!({"fn": "min", "of": "n"})),
            ("max_x", json!({"fn": "max", "of": "x"})),
            ("avg_n", json!({"fn": "avg", "of": "n"})),
            ("users", json!({"fn": "uniq", "of": "user"})),
            ("k1s", json!({"fn": "uniq_exact", "of": {"col": "props", "path": ["k1"]}})),
            ("q", json!({"fn": "quantiles", "of": "x", "q": [0.1, 0.5, 0.99]})),
            ("max_p", json!({"fn": "max", "of": {"col": "props", "path": ["k2"]}})),
            ("b_count", json!({"fn": "count", "where": {"col": "event", "op": "eq", "value": "b"}})),
            (
                "funnel",
                json!({"fn": "window_funnel", "window_ms": DAY, "steps": [
                    {"col": "event", "op": "eq", "value": "a"},
                    {"col": "event", "op": "eq", "value": "b"},
                    {"col": "event", "op": "prefix", "value": "pag"}
                ]}),
            ),
            (
                "ret",
                json!({"fn": "retention", "conditions": [
                    {"col": "event", "op": "eq", "value": "a"},
                    {"col": "flag", "op": "eq", "value": true}
                ]}),
            ),
        ] {
            if r.chance(0.4) {
                aggs.insert(name.into(), a);
            }
        }
        q["aggregates"] = Json::Object(aggs);
        if r.chance(0.25) && group == json!(["user"]) {
            let keep: Vec<String> = q["aggregates"].as_object().unwrap().keys().cloned().collect();
            if keep.iter().any(|k| k == "funnel") {
                q["then"] = json!({"group_by": ["funnel"], "aggregates": {"people": {"fn": "count"}}});
            } else if keep.iter().any(|k| k == "ret") {
                q["then"] = json!({"aggregates": {"retained": {"fn": "sum", "of": "ret"}}});
            } else {
                q["then"] = json!({"aggregates": {"people": {"fn": "count"}, "total": {"fn": "sum", "of": "count"}}});
            }
        }
        if r.chance(0.3) {
            q["limit"] = json!(1 + r.usize(5));
            q["order_by"] = json!([{"col": "count", "desc": true}]);
            if q.get("then").is_some() {
                q.as_object_mut().unwrap().remove("order_by");
            }
        }
        q
    }
}

async fn create_db(eng: &Arc<Engine>, id: &str) -> Arc<Db> {
    let db = eng.db(id);
    db.open(true).await.unwrap();
    txn_do(&db, async |t| create(t, &table_def()).await).await.unwrap();
    db
}

async fn compare(db: &Arc<Db>, model: &Arc<Db>, q: &Json) -> Result<usize, String> {
    let got = query(db, q).await.map_err(|e| format!("{q}: {e}"))?;
    let want = query(model, q).await.map_err(|e| format!("model {q}: {e}"))?;
    if got.columns != want.columns || got.rows != want.rows {
        return Err(format!("query {q}\n got {:?}\nwant {:?}\nstats {:?}", got.rows, want.rows, got.stats));
    }
    if q.get("columns").is_some() && got.next_cursor.is_some() != want.next_cursor.is_some() {
        return Err(format!("cursors differ for {q}"));
    }
    let nonzero =
        got.rows.iter().filter(|r| r.iter().any(|v| v.as_i64().is_some_and(|n| n > 0) || v.is_string())).count();
    Ok(nonzero)
}

async fn run(seed: u64, steps: usize) -> Result<usize, String> {
    let (eng, _store, env) = engine(seed, cfg());
    env.spawn(async {});
    let db = create_db(&eng, "real").await;
    let model = create_db(&eng, "model").await;
    let bufs = buffers();
    let mut g = Gen { r: Rng::new(seed), next_time: T0 };
    let table = {
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        get_schema(&mut t, "events").await.unwrap().id
    };
    let err = |e: Error| e.to_string();
    let mut answered = 0;
    for step in 0..steps {
        match g.r.usize(10) {
            0..=2 => {
                let rows: Vec<Json> = (0..1 + g.r.usize(20)).map(|_| g.row()).collect();
                txn_do(&db, async |t| insert(t, "events", &rows, 0).await).await.map_err(err)?;
                txn_do(&model, async |t| insert(t, "events", &rows, 0).await).await.map_err(err)?;
            }
            3..=4 => {
                let rows: Vec<Json> = (0..1 + g.r.usize(40)).map(|_| g.row()).collect();
                let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                let s = get_schema(&mut t, "events").await.map_err(err)?;
                let valid = validate_rows(&s, &rows, 0).map_err(err)?;
                let c = bufs
                    .submit(
                        db.clone(),
                        s,
                        valid,
                        env.random_u64(),
                        tokio::time::Instant::now() + std::time::Duration::from_secs(10),
                    )
                    .await;
                c.map_err(err)?;
                txn_do(&model, async |t| insert(t, "events", &rows, 0).await).await.map_err(err)?;
            }
            5 => match maint::seal(&db, table).await {
                Ok(_) | Err(Error::Conflict(_)) => {}
                Err(e) => return Err(e.to_string()),
            },
            6 => maint::maintain(&db, table, 8).await.map_err(err)?,
            7 => {
                let f: Filter = Filter::parse(g.filter()).map_err(err)?;
                let (from, to) = (T0 + g.r.below(8) as i64 * DAY / 2, T0 + (4 + g.r.below(10)) as i64 * DAY);
                let a =
                    txn_do(&db, async |t| maint::delete(&db, t, "events", from, to, &f).await).await.map_err(err)?;
                let b = txn_do(&model, async |t| maint::delete(&model, t, "events", from, to, &f).await)
                    .await
                    .map_err(err)?;
                if a != b {
                    return Err(format!("step {step}: delete counted {a}, model {b}"));
                }
            }
            _ => {
                let q = g.query();
                answered += compare(&db, &model, &q).await.map_err(|e| format!("step {step}: {e}"))?;
            }
        }
    }
    for _ in 0..8 {
        let q = g.query();
        answered += compare(&db, &model, &q).await?;
    }
    Ok(answered)
}

#[test]
fn queries_match_a_model_through_seals_merges_and_deletes() {
    let seeds: u64 = std::env::var("EVENT_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(12);
    let mut answered = 0;
    let mut hits = crate::probe::Hits::default();
    for seed in 0..seeds {
        rt().block_on(async {
            match run(seed, 120).await {
                Ok(n) => answered += n,
                Err(e) => panic!("seed {seed}: {e}"),
            }
        });
        hits.add(&crate::probe::finish());
    }
    for p in [
        Probe::EventsSealed,
        Probe::PartsBound,
        Probe::PartsMerged,
        Probe::EventRowsMasked,
        Probe::GranulesPruned,
        Probe::GranulesPrunedByBloom,
        Probe::PartPrunedByRef,
        Probe::PartPrunedByDictionary,
        Probe::JsonSharedPathRead,
        Probe::QuerySecondStage,
    ] {
        assert!(hits.count(p) > 0, "{p:?} was never reached");
    }
    assert!(answered > 50 * seeds as usize / 4, "too few queries found rows: {answered}");
}

/// A table of `days` single-row parts behind a store with 25 ms latency,
/// opened by a node whose caches are cold.
async fn many_parts_cold(days: i64) -> (Arc<Db>, EngineConfig, Arc<Engine>) {
    let origin = tokio::time::Instant::now();
    let env = Arc::new(crate::env::Env::sim(1, T0 as u64, origin, 0));
    let env2 = env.clone();
    let mem = Arc::new(crate::store::MemStore::new(Box::new(move || env2.now_ms())));
    let store = Arc::new(crate::sim::cost::MeteredStore::new(mem, 0));
    let cfg = EngineConfig { cache_bytes: 64 << 20, ..cfg() };
    let eng = Engine::new(store.clone(), env.clone(), cfg.clone(), "node-a".into());
    let db = create_db(&eng, "db").await;
    let table = {
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        get_schema(&mut t, "events").await.unwrap().id
    };
    let rows: Vec<Json> =
        (0..days).map(|d| json!({"time": T0 + d * DAY, "event": "a", "user": format!("u{}", d % 2)})).collect();
    txn_do(&db, async |t| insert(t, "events", &rows, 0).await).await.unwrap();
    assert!(maint::seal(&db, table).await.unwrap());
    eng.release_leases(std::time::Duration::from_secs(2)).await;
    let cold = Engine::new(store.clone(), env, cfg.clone(), "node-b".into());
    let db = cold.db("db");
    db.open(true).await.unwrap();
    store.set_latency_ms(25);
    (db, cfg, cold)
}

/// A delete reads every part its time range covers before it commits, and
/// it must commit before the request's deadline: on a table with hundreds
/// of parts behind a store with realistic latency, it reads them concurrently.
#[test]
fn deletes_across_many_parts_commit_before_the_deadline() {
    rt().block_on(async {
        let days = 400;
        let (db, cfg, _eng) = many_parts_cold(days).await;
        let f = Filter::parse(json!({"col": "user", "op": "eq", "value": "u1"})).unwrap();
        let start = tokio::time::Instant::now();
        let deadline = start + std::time::Duration::from_millis(cfg.commit_timeout_ms);
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let n = maint::delete(&db, &mut t, "events", i64::MIN / 4, i64::MAX / 4, &f).await.unwrap();
        assert_eq!(n, days as u64 / 2);
        let mut req = t.into_request();
        req.deadline = Some(deadline);
        let read_ms = start.elapsed().as_millis();
        db.commit(req).await.unwrap_or_else(|e| panic!("delete reading {days} parts took {read_ms} ms: {e}"));
        let left = query(&db, &json!({"from": T0, "to": T0 + days * DAY, "aggregates": {"n": {"fn": "count"}}}))
            .await
            .unwrap();
        assert_eq!(left.rows, vec![vec![json!(days / 2)]]);
    });
}

/// A cold query fetches the metadata of many parts at once rather than one
/// part after another.
#[test]
fn cold_queries_read_parts_concurrently() {
    rt().block_on(async {
        let days = 400;
        let (db, _cfg, _eng) = many_parts_cold(days).await;
        let start = tokio::time::Instant::now();
        let q = json!({"from": T0, "to": T0 + days * DAY, "where": {"col": "user", "op": "eq", "value": "u1"},
            "aggregates": {"n": {"fn": "count"}}});
        let out = query(&db, &q).await.unwrap();
        assert_eq!(out.rows, vec![vec![json!(days / 2)]]);
        let ms = start.elapsed().as_millis();
        assert!(ms < 1_000, "a cold query of {days} parts took {ms} ms");
    });
}
