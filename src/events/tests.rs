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
    many_parts_cold_with(days, cfg()).await
}

async fn many_parts_cold_with(days: i64, cfg: EngineConfig) -> (Arc<Db>, EngineConfig, Arc<Engine>) {
    let origin = tokio::time::Instant::now();
    let env = Arc::new(crate::env::Env::sim(1, T0 as u64, origin, 0));
    let env2 = env.clone();
    let mem = Arc::new(crate::store::MemStore::new(Box::new(move || env2.now_ms())));
    let store = Arc::new(crate::sim::cost::MeteredStore::new(mem, 0));
    let cfg = EngineConfig { cache_bytes: 64 << 20, ..cfg };
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

/// A cold query reads parts that are too large to fetch whole (metadata,
/// then column indexes, then blocks) concurrently, not one after another.
#[test]
fn cold_queries_read_large_parts_concurrently() {
    rt().block_on(async {
        let days = 200;
        let (db, _cfg, _eng) = many_parts_cold_with(days, EngineConfig { segment_whole_fetch_max: 0, ..cfg() }).await;
        let start = tokio::time::Instant::now();
        let q = json!({"from": T0, "to": T0 + days * DAY, "where": {"col": "user", "op": "eq", "value": "u1"},
            "group_by": ["user"], "aggregates": {"n": {"fn": "count"}}});
        let out = query(&db, &q).await.unwrap();
        assert_eq!(out.rows, vec![vec![json!("u1"), json!(days / 2)]]);
        let ms = start.elapsed().as_millis();
        assert!(out.stats.store_gets >= 3 * days as u64, "{:?}", out.stats);
        assert!(ms < 2_000, "a cold query of {days} parts in {} GETs took {ms} ms", out.stats.store_gets);
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
/// Values survive sealing and merging exactly as unsealed rows hold them.
mod stored_values {
    use super::*;

    async fn table_id(db: &Arc<Db>) -> u32 {
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        get_schema(&mut t, "events").await.unwrap().id
    }

    async fn ins(db: &Arc<Db>, rows: Vec<Json>) {
        txn_do(db, async |t| insert(t, "events", &rows, 0).await).await.unwrap();
    }

    async fn seal_all(db: &Arc<Db>) {
        let id = table_id(db).await;
        assert!(maint::seal(db, id).await.unwrap(), "sealed");
    }

    async fn merge_all(db: &Arc<Db>) {
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let s = get_schema(&mut t, "events").await.unwrap();
        let rs = refs(&mut t, s.id).await.unwrap();
        assert!(rs.len() >= 2, "parts to merge: {}", rs.len());
        maint::merge(db, &s, &rs).await.unwrap();
    }

    async fn count(db: &Arc<Db>, w: Json) -> i64 {
        let q = json!({"from": 0, "to": T0 * 2, "where": w, "aggregates": {"n": {"fn": "count"}}});
        query(db, &q).await.unwrap().rows[0][0].as_i64().unwrap()
    }

    async fn rows(db: &Arc<Db>, cols: Json) -> Vec<Vec<Json>> {
        let q = json!({"from": 0, "to": T0 * 2, "columns": cols, "limit": 100});
        query(db, &q).await.unwrap().rows
    }

    #[test]
    fn negative_zero_float_survives_stat_pruning() {
        rt().block_on(async {
            let (eng, _store, _env) = engine(1, cfg());
            let db = create_db(&eng, "d").await;
            ins(&db, vec![json!({"time": T0, "event": "a", "x": -0.0})]).await;
            let w = json!({"col": "x", "op": "eq", "value": 0});
            assert_eq!(count(&db, w.clone()).await, 1, "unsealed -0.0 equals 0");
            seal_all(&db).await;
            assert_eq!(count(&db, w).await, 1, "sealed -0.0 equals 0");
        });
    }

    #[test]
    fn truncated_string_max_does_not_prune_matches() {
        rt().block_on(async {
            let (eng, _store, _env) = engine(1, cfg());
            let db = create_db(&eng, "d").await;
            let long = "x".repeat(40);
            let max = format!("{}é", "x".repeat(31));
            ins(
                &db,
                vec![
                    json!({"time": T0, "event": "a", "user": long}),
                    json!({"time": T0 + 1, "event": "a", "user": max}),
                ],
            )
            .await;
            let eq = json!({"col": "user", "op": "eq", "value": long});
            let prefix = json!({"col": "user", "op": "prefix", "value": "x".repeat(33)});
            assert_eq!((count(&db, eq.clone()).await, count(&db, prefix.clone()).await), (1, 1), "unsealed");
            seal_all(&db).await;
            assert_eq!((count(&db, eq).await, count(&db, prefix).await), (1, 1), "sealed");
        });
    }

    #[test]
    fn default_string_column_survives_merge() {
        rt().block_on(async {
            let (eng, _store, _env) = engine(1, cfg());
            let db = create_db(&eng, "d").await;
            ins(&db, vec![json!({"time": T0, "event": "a"})]).await;
            seal_all(&db).await;
            let spec: ColumnSpec =
                serde_json::from_value(json!({"name": "plan", "type": "string", "default": "free"})).unwrap();
            txn_do(&db, async |t| add_column(t, "events", &spec).await).await.unwrap();
            ins(&db, vec![json!({"time": T0 + 1, "event": "a", "plan": "pro"})]).await;
            seal_all(&db).await;
            let before = rows(&db, json!(["time", "plan"])).await;
            assert_eq!(before, vec![vec![json!(T0), json!("free")], vec![json!(T0 + 1), json!("pro")]]);
            merge_all(&db).await;
            assert_eq!(rows(&db, json!(["time", "plan"])).await, before, "merge keeps the default");
            assert_eq!(count(&db, json!({"col": "plan", "op": "eq", "value": "free"})).await, 1);
        });
    }

    /// Integers beyond `i64` cannot be compared or aggregated exactly, so an
    /// insert refuses them instead of storing a rounded float.
    #[test]
    fn json_integers_beyond_i64_are_refused() {
        rt().block_on(async {
            let (eng, _store, _env) = engine(1, cfg());
            let db = create_db(&eng, "d").await;
            for props in [json!({"big": u64::MAX}), json!({"ids": [1, i64::MAX as u64 + 1]})] {
                let row = [json!({"time": T0, "event": "a", "props": props})];
                let r = txn_do(&db, async |t| insert(t, "events", &row, 0).await).await;
                assert!(matches!(r, Err(Error::InvalidArgument(ref m)) if m.contains("as a string")), "{r:?}");
            }
            ins(&db, vec![json!({"time": T0, "event": "a", "props": {"big": i64::MAX}})]).await;
            seal_all(&db).await;
            assert_eq!(rows(&db, json!(["props"])).await, vec![vec![json!({"big": i64::MAX})]]);
        });
    }

    fn pick<T: Clone>(r: &mut Rng, xs: &[T]) -> T {
        xs[r.usize(xs.len())].clone()
    }

    fn strs() -> Vec<String> {
        let mut v: Vec<String> = ["", "a", "é", "pag", "page", "\u{0}", "a\"b"].iter().map(|s| s.to_string()).collect();
        v.push("😀".repeat(9));
        v.push(format!("{}é", "x".repeat(31)));
        v.push("x".repeat(40));
        v.push(format!("{}y", "x".repeat(40)));
        v.push(format!("{}z", "x".repeat(28)));
        v
    }

    fn rand_json(r: &mut Rng, depth: u32) -> Json {
        let keys = ["a", "a.b", "", "é", "\"q\"", "[", "k1", "k2", "k3", "k4", "k5", "k6"];
        match r.usize(if depth > 2 { 7 } else { 9 }) {
            0 => json!(pick(r, &[i64::MIN, i64::MAX, 0, -1, 7])),
            1 => json!(pick(r, &[0.5, 1.0, 1e300, -1e-300, 5e-324, -0.0])),
            2 => json!(pick(r, &strs())),
            3 => json!(r.chance(0.5)),
            4 => json!([1, "a", null, {"z": 1}]),
            5 => json!({}),
            6 => json!([]),
            _ => {
                let mut o = serde_json::Map::new();
                for _ in 0..r.usize(5) {
                    o.insert(pick(r, &keys).to_string(), rand_json(r, depth + 1));
                }
                Json::Object(o)
            }
        }
    }

    /// Sealed and merged parts answer like unsealed rows for tricky values.
    #[test]
    fn tricky_values_roundtrip_through_parts() {
        for seed in 0..std::env::var("EVENT_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(6u64) {
            rt().block_on(async {
                let (eng, _store, _env) = engine(seed, cfg());
                let db = create_db(&eng, "real").await;
                let model = create_db(&eng, "model").await;
                let mut r = Rng::new(seed);
                let mut t = T0;
                let plan: ColumnSpec =
                    serde_json::from_value(json!({"name": "plan", "type": "string", "default": "free"})).unwrap();
                for batch in 0..3 {
                    if batch == 1 {
                        for d in [&db, &model] {
                            txn_do(d, async |t| add_column(t, "events", &plan).await).await.unwrap();
                        }
                    }
                    let rows: Vec<Json> = (0..1 + r.usize(60))
                        .map(|_| {
                            t += 1;
                            let mut row = json!({"time": t, "event": pick(&mut r, &strs())});
                            if batch > 0 && r.chance(0.7) {
                                row["plan"] = json!(pick(&mut r, &strs()));
                            }
                            if r.chance(0.8) {
                                row["user"] = json!(pick(&mut r, &strs()));
                            }
                            if r.chance(0.8) {
                                row["n"] = json!(pick(&mut r, &[i64::MIN, i64::MAX, i64::MIN + 1, 0, -1, 1]));
                            }
                            if r.chance(0.8) {
                                row["x"] = json!(pick(&mut r, &[1e300, -1e300, 0.5, 1.0, 5e-324, -2.5, -0.0, 0.0]));
                            }
                            if r.chance(0.8) {
                                let mut o = serde_json::Map::new();
                                for _ in 0..r.usize(8) {
                                    let k = pick(&mut r, &["a", "a.b", "", "é", "\"q\"", "k1", "k2", "k3", "k4"]);
                                    o.insert(k.to_string(), rand_json(&mut r, 0));
                                }
                                row["props"] = Json::Object(o);
                            }
                            row
                        })
                        .collect();
                    ins(&db, rows.clone()).await;
                    ins(&model, rows).await;
                    seal_all(&db).await;
                }
                let all = json!(["time", "event", "user", "n", "x", "flag", "props", "plan"]);
                let rows_q =
                    |cols: Json| json!({"from": 0, "to": T0 * 2, "columns": cols, "order_by": ["time"], "limit": 1000});
                let mut filters = Vec::new();
                for s in strs() {
                    filters.push(json!({"col": "event", "op": "eq", "value": s}));
                    filters.push(json!({"col": "user", "op": "eq", "value": s}));
                    filters.push(json!({"col": "event", "op": "prefix", "value": s}));
                    filters.push(json!({"col": "event", "op": "gt", "value": s}));
                    filters.push(json!({"col": "event", "op": "le", "value": s}));
                    filters.push(json!({"col": "props", "path": ["k1"], "op": "eq", "value": s}));
                    filters.push(json!({"col": "plan", "op": "eq", "value": s}));
                }
                filters.push(json!({"col": "plan", "op": "eq", "value": "free"}));
                for n in [i64::MIN, i64::MAX, 0, -1] {
                    for op in ["eq", "lt", "ge", "gt", "le"] {
                        filters.push(json!({"col": "n", "op": op, "value": n}));
                        filters.push(json!({"col": "props", "path": ["a"], "op": op, "value": n}));
                    }
                }
                for x in [1e300, 5e-324, 1.0, -1e300, 0.0] {
                    filters.push(json!({"col": "x", "op": "eq", "value": x}));
                    filters.push(json!({"col": "x", "op": "ge", "value": x}));
                }
                for k in ["a", "a.b", "", "é", "\"q\"", "k1"] {
                    filters.push(json!({"col": "props", "path": [k], "op": "exists"}));
                    filters.push(json!({"col": "props", "path": ["a", k], "op": "exists"}));
                }
                for stage in ["sealed", "merged"] {
                    if stage == "merged" {
                        merge_all(&db).await;
                    }
                    let got = query(&db, &rows_q(all.clone())).await.unwrap().rows;
                    let want = query(&model, &rows_q(all.clone())).await.unwrap().rows;
                    assert_eq!(got, want, "seed {seed} {stage} rows");
                    for f in &filters {
                        assert_eq!(
                            count(&db, f.clone()).await,
                            count(&model, f.clone()).await,
                            "seed {seed} {stage} {f}"
                        );
                    }
                }
            });
        }
    }

    /// Times are limited to what RFC 3339 can express, at insert.
    #[test]
    fn times_beyond_rfc3339_are_refused() {
        rt().block_on(async {
            let (eng, _store, _env) = engine(1, cfg());
            let db = create_db(&eng, "d").await;
            let (lo, hi) = (*schema::TIME_RANGE.start(), *schema::TIME_RANGE.end());
            for t in [i64::MIN, lo - 1, hi + 1, i64::MAX] {
                let row = [json!({"time": t, "event": "a"})];
                let r = txn_do(&db, async |tx| insert(tx, "events", &row, 0).await).await;
                assert!(matches!(r, Err(Error::InvalidArgument(_))), "time {t}: {r:?}");
            }
            assert_eq!(schema::parse_rfc3339("0000-01-01T00:00:00Z"), Some(lo));
            assert_eq!(schema::parse_rfc3339("9999-12-31T23:59:59.999Z"), Some(hi));
        });
    }

    #[test]
    fn extreme_times_seal_and_bucket_with_offset() {
        rt().block_on(async {
            let (eng, _store, _env) = engine(1, cfg());
            let db = create_db(&eng, "d").await;
            let (lo, hi) = (*schema::TIME_RANGE.start(), *schema::TIME_RANGE.end());
            ins(&db, vec![json!({"time": hi, "event": "a"}), json!({"time": lo, "event": "a"})]).await;
            seal_all(&db).await;
            let q = json!({"from": i64::MIN, "to": i64::MAX, "columns": ["time"], "order_by": ["time"]});
            assert_eq!(query(&db, &q).await.unwrap().rows, vec![vec![json!(lo)], vec![json!(hi)]]);
            for (iv, off) in [("1d", "+01:00"), ("1d", "-01:00"), ("1w", "+12:00"), ("3mo", "-12:00"), ("1s", "Z")] {
                let q = json!({"from": i64::MIN, "to": i64::MAX,
                    "group_by": [{"bucket": {"col": "time"}, "interval": iv, "offset": off}],
                    "aggregates": {"n": {"fn": "count"}}});
                let out = query(&db, &q).await.unwrap_or_else(|e| panic!("{iv} {off}: {e}"));
                assert_eq!(out.rows.len(), 2, "{iv} {off}: {:?}", out.rows);
            }
        });
    }
}
/// Query answers that pruning and part layouts must not change.
mod query_semantics {
    use super::*;

    /// A fresh engine with table `def` created; returns the db and table id.
    async fn setup(def: Json) -> (Arc<Engine>, Arc<Db>, u32) {
        let (eng, _store, env) = engine(1, cfg());
        env.spawn(async {});
        let db = eng.db("r");
        db.open(true).await.unwrap();
        let def: CreateEvents = serde_json::from_value(def).unwrap();
        let s = txn_do(&db, async |t| create(t, &def).await).await.unwrap();
        (eng, db, s.id)
    }

    async fn q(db: &Arc<Db>, table: &str, q: Json) -> exec::Output {
        let q = Query::parse(q).unwrap();
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let s = get_schema(&mut t, table).await.unwrap();
        exec::run(db.shared(), &db.id, &mut t, &s, &q, &Limits::default()).await.unwrap()
    }

    async fn ins(db: &Arc<Db>, table: &str, rows: Vec<Json>) {
        txn_do(db, async |t| insert(t, table, &rows, 0).await).await.unwrap();
    }

    fn def(cols: Json) -> Json {
        json!({"name": "e", "columns": cols, "time_column": "time", "order_by": ["time"], "partition": "day"})
    }

    /// Answers before and after sealing must agree.
    async fn before_after(db: &Arc<Db>, table: u32, query: Json) -> (exec::Output, exec::Output) {
        let a = q(db, "e", query.clone()).await;
        assert!(maint::seal(db, table).await.unwrap(), "nothing sealed");
        let b = q(db, "e", query).await;
        assert_eq!(b.stats.unsealed_rows, 0);
        (a, b)
    }

    #[test]
    fn same_path_in_two_json_columns() {
        rt().block_on(async {
            let (_e, db, id) = setup(def(json!([
                {"name": "time", "type": "time"}, {"name": "p1", "type": "json"}, {"name": "p2", "type": "json"}
            ])))
            .await;
            ins(&db, "e", vec![json!({"time": T0, "p1": {"a": 1}, "p2": {"a": 100}})]).await;
            let query = json!({"from": T0, "to": T0 + DAY, "where": {"or": [
                {"col": "p1", "path": ["a"], "op": "eq", "value": 1},
                {"col": "p2", "path": ["a"], "op": "eq", "value": 5}
            ]}, "aggregates": {"n": {"fn": "count"}}});
            let (a, b) = before_after(&db, id, query).await;
            assert_eq!(a.rows, vec![vec![json!(1)]], "unsealed");
            assert_eq!(b.rows, vec![vec![json!(1)]], "sealed");
        });
    }

    #[test]
    fn bucket_of_extreme_int_does_not_panic() {
        rt().block_on(async {
            let (_e, db, _id) =
                setup(def(json!([{"name": "time", "type": "time"}, {"name": "n", "type": "int"}]))).await;
            ins(&db, "e", vec![json!({"time": T0, "n": i64::MIN})]).await;
            let out = q(
                &db,
                "e",
                json!({"from": T0, "to": T0 + DAY,
                "group_by": [{"bucket": {"col": "n"}, "interval": "1d"}], "aggregates": {"c": {"fn": "count"}}}),
            )
            .await;
            assert_eq!(out.rows, vec![vec![Json::Null, json!(1)]], "a bucket beyond the time range is null");
        });
    }

    #[test]
    fn cursor_pages_with_ties_across_parts_and_unsealed() {
        rt().block_on(async {
            let (_e, db, id) =
                setup(def(json!([{"name": "time", "type": "time"}, {"name": "k", "type": "int"}]))).await;
            let mut want = 0;
            for round in 0..3 {
                let rows: Vec<Json> =
                    (0..17).map(|i| json!({"time": T0 + (i % 3), "k": (i * 7 + round) % 4})).collect();
                want += rows.len();
                ins(&db, "e", rows).await;
                if round < 2 {
                    maint::seal(&db, id).await.unwrap();
                }
            }
            for order in
                [json!(["time"]), json!([{"col": "k", "desc": true}]), json!(["k", {"col": "time", "desc": true}])]
            {
                let full = q(
                    &db,
                    "e",
                    json!({"from": T0, "to": T0 + DAY, "columns": ["time", "k"],
                    "order_by": order, "limit": 1000}),
                )
                .await;
                assert_eq!(full.rows.len(), want);
                let mut got = Vec::new();
                let mut cursor = Json::Null;
                loop {
                    let page = q(
                        &db,
                        "e",
                        json!({"from": T0, "to": T0 + DAY, "columns": ["time", "k"],
                        "order_by": order, "limit": 4, "cursor": cursor}),
                    )
                    .await;
                    got.extend(page.rows);
                    match page.next_cursor {
                        Some(c) => cursor = json!(c),
                        None => break,
                    }
                }
                assert_eq!(got, full.rows, "{order}");
            }
        });
    }

    #[test]
    fn added_column_defaults_through_seal_and_merge() {
        rt().block_on(async {
            let (_e, db, id) =
                setup(def(json!([{"name": "time", "type": "time"}, {"name": "k", "type": "int"}]))).await;
            ins(&db, "e", vec![json!({"time": T0, "k": 1}), json!({"time": T0 + 1, "k": 2})]).await;
            maint::seal(&db, id).await.unwrap();
            ins(&db, "e", vec![json!({"time": T0 + 2, "k": 3})]).await;
            let spec: ColumnSpec = serde_json::from_value(json!({"name": "c", "type": "int", "default": 7})).unwrap();
            txn_do(&db, async |t| add_column(t, "e", &spec).await).await.unwrap();
            ins(&db, "e", vec![json!({"time": T0 + 3, "k": 4, "c": 1}), json!({"time": T0 + 4, "k": 5})]).await;
            let query = json!({"from": i64::MIN, "to": i64::MAX, "group_by": ["c"],
                "aggregates": {"n": {"fn": "count"}, "s": {"fn": "sum", "of": "c"},
                    "sevens": {"fn": "count", "where": {"col": "c", "op": "eq", "value": 7}}}});
            let want =
                vec![vec![json!(1), json!(1), json!(1), json!(0)], vec![json!(7), json!(4), json!(28), json!(4)]];
            assert_eq!(q(&db, "e", query.clone()).await.rows, want, "mixed");
            maint::seal(&db, id).await.unwrap();
            assert_eq!(q(&db, "e", query.clone()).await.rows, want, "sealed");
            maint::maintain(&db, id, 8).await.unwrap();
            assert_eq!(q(&db, "e", query.clone()).await.rows, want, "merged");
        });
    }

    #[test]
    fn lookups_map_with_defaults() {
        rt().block_on(async {
            let (_e, db, _id) = setup(def(json!([
                {"name": "time", "type": "time"}, {"name": "uid", "type": "int"}, {"name": "alias", "type": "string"}
            ])))
            .await;
            let tdef: crate::table::CreateTable = serde_json::from_value(json!({"name": "people",
                "columns": [{"name": "id", "type": "int"}, {"name": "plan", "type": "string"}],
                "primary_key": ["id"]}))
            .unwrap();
            txn_do(&db, async |t| crate::table::create_table(t, &tdef).await.map(|_| ())).await.unwrap();
            for row in [json!({"id": 1, "plan": "pro"}), json!({"id": 2, "plan": null})] {
                let d = db.clone();
                txn_do(&db, async |t| {
                    let b = crate::table::BlobResolver { db: &d };
                    crate::table::insert(t, &b, "people", &row, crate::table::InsertMode::Insert).await
                })
                .await
                .unwrap();
            }
            ins(&db, "e", vec![
                json!({"time": T0, "uid": 1, "alias": "a"}),
                json!({"time": T0 + 1, "alias": "b"}),
                json!({"time": T0 + 2, "uid": 2, "alias": "c"}),
                json!({"time": T0 + 3, "uid": 3, "alias": "d"}),
            ])
            .await;
            let query = json!({"from": T0, "to": T0 + DAY,
                "lookups": {"plan": {"table": "people", "key": {"col": "uid"}, "value": "plan", "default": {"col": "alias"}}},
                "where": {"lookup": "plan", "op": "ne", "value": "zzz"},
                "group_by": [{"lookup": "plan"}], "aggregates": {"n": {"fn": "count"}}});
            let (a, b) = before_after(&db, _id, query).await;
            // uid 2 maps to a null plan, which is not the default.
            let want = vec![vec![json!("b"), json!(1)], vec![json!("d"), json!(1)], vec![json!("pro"), json!(1)]];
            assert_eq!(a.rows, want, "unsealed");
            assert_eq!(b.rows, want, "sealed");
        });
    }

    #[test]
    fn lookup_limit_covers_the_whole_query() {
        rt().block_on(async {
            let (_e, db, _id) =
                setup(def(json!([{"name": "time", "type": "time"}, {"name": "uid", "type": "int"}]))).await;
            for name in ["people", "teams"] {
                let tdef: crate::table::CreateTable = serde_json::from_value(json!({"name": name,
                    "columns": [{"name": "id", "type": "int"}, {"name": "plan", "type": "string"}],
                    "primary_key": ["id"]}))
                .unwrap();
                txn_do(&db, async |t| crate::table::create_table(t, &tdef).await.map(|_| ())).await.unwrap();
                for i in 0..50 {
                    let row = json!({"id": i, "plan": "p".repeat(100)});
                    let d = db.clone();
                    txn_do(&db, async |t| {
                        let b = crate::table::BlobResolver { db: &d };
                        crate::table::insert(t, &b, name, &row, crate::table::InsertMode::Insert).await
                    })
                    .await
                    .unwrap();
                }
            }
            ins(&db, "e", vec![json!({"time": T0, "uid": 1})]).await;
            let run = async |tables: &[&str], max: usize| {
                let mut lookups = serde_json::Map::new();
                let mut group = Vec::new();
                for (i, t) in tables.iter().enumerate() {
                    lookups.insert(format!("l{i}"), json!({"table": t, "key": {"col": "uid"}, "value": "plan"}));
                    group.push(json!({"lookup": format!("l{i}")}));
                }
                let q = Query::parse(json!({"from": T0, "to": T0 + DAY, "lookups": lookups, "group_by": group,
                    "aggregates": {"n": {"fn": "count"}}}))
                .unwrap();
                let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                let s = get_schema(&mut t, "e").await.unwrap();
                let limits = Limits { lookup_max_bytes: max, ..Limits::default() };
                exec::run(db.shared(), &db.id, &mut t, &s, &q, &limits).await
            };
            let mut max = 1;
            while run(&["people"], max).await.is_err() {
                max += 64;
            }
            let r = run(&["people", "teams"], max).await;
            assert!(
                matches!(r, Err(Error::TooLarge(_))),
                "two tables of {max} bytes each pass: {:?}",
                r.map(|o| o.rows)
            );
            let r = run(&["people"; 8], max).await;
            assert!(r.is_ok(), "lookups of one table read it once: {:?}", r.err());
        });
    }

    #[test]
    fn bucket_filter_takes_rfc3339() {
        rt().block_on(async {
            let (_e, db, _id) = setup(def(json!([{"name": "time", "type": "time"}]))).await;
            ins(&db, "e", vec![json!({"time": "2024-01-02T10:00:00Z"})]).await;
            let out = q(
                &db,
                "e",
                json!({"from": "2024-01-01T00:00:00Z", "to": "2024-01-05T00:00:00Z",
                "where": {"bucket": {"col": "time"}, "interval": "1d", "op": "eq", "value": "2024-01-02T00:00:00Z"},
                "aggregates": {"n": {"fn": "count"}}}),
            )
            .await;
            assert_eq!(out.rows, vec![vec![json!(1)]]);
        });
    }

    #[test]
    fn month_and_week_buckets_with_offsets() {
        use super::super::query::{Interval, parse_offset};
        let t = |s: &str| super::super::schema::parse_rfc3339(s).unwrap();
        let mo = Interval::parse("1mo").unwrap();
        let off = parse_offset("+01:00").unwrap();
        assert_eq!(mo.bucket(t("2024-02-29T23:30:00Z"), off), Some(t("2024-02-29T23:00:00Z")));
        assert_eq!(mo.bucket(t("1969-12-31T23:30:00Z"), off), Some(t("1969-12-31T23:00:00Z")));
        assert_eq!(mo.bucket(t("1969-12-31T22:30:00Z"), off), Some(t("1969-11-30T23:00:00Z")));
        let q3 = Interval::parse("3mo").unwrap();
        assert_eq!(q3.bucket(t("1969-12-15T00:00:00Z"), 0), Some(t("1969-10-01T00:00:00Z")));
        assert_eq!(q3.bucket(t("2024-05-15T00:00:00Z"), 0), Some(t("2024-04-01T00:00:00Z")));
        let w = Interval::parse("1w").unwrap();
        let est = parse_offset("-05:00").unwrap();
        assert_eq!(w.bucket(t("2024-01-01T04:59:00Z"), est), Some(t("2023-12-25T05:00:00Z")));
        assert_eq!(w.bucket(t("2024-01-01T05:00:00Z"), est), Some(t("2024-01-01T05:00:00Z")));
        assert_eq!(w.bucket(t("1969-12-28T00:00:00Z"), 0), Some(t("1969-12-22T00:00:00Z")));
    }
}
/// Bulk inserts, binds and maintenance against concurrent transactions and quotas.
mod storage {
    use super::*;

    /// Bulk-inserts `rows` through a bind that a concurrent commit of `req`
    /// joins, so that the bind and the commit land in one batch, bind first.
    async fn bind_beside(
        db: &Arc<Db>,
        s: &schema::EventSchema,
        rows: &[Json],
        req: crate::engine::CommitRequest,
    ) -> (Result<crate::engine::Committed>, Result<u64>) {
        db.shared().store_time_ms().await.unwrap();
        let req = Arc::new(std::sync::Mutex::new(Some(req)));
        let other: Arc<std::sync::Mutex<Option<Result<u64>>>> = Arc::default();
        let (req2, other2) = (req.clone(), other.clone());
        let bind: BindFn = Arc::new(move |db: Arc<Db>, b| {
            let (req, other) = (req2.lock().unwrap().take(), other2.clone());
            Box::pin(async move {
                let (rs, o) = tokio::join!(db.bind(b), async {
                    match req {
                        Some(r) => db.commit(r).await,
                        None => Ok(0),
                    }
                });
                *other.lock().unwrap() = Some(o);
                rs
            })
        });
        let bufs = Buffers::new(BufferConfig { wait_ms: 0, max_bytes: 1 << 20 }, bind);
        let valid = validate_rows(s, rows, 0).unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let inserted = bufs.submit(db.clone(), s.clone(), valid, 4242, deadline).await;
        let o = other.lock().unwrap().take().unwrap();
        (inserted, o)
    }

    #[test]
    fn a_drop_in_the_batch_after_a_bind_removes_the_bound_parts() {
        rt().block_on(async {
            let (eng, _store, env) = engine(3, cfg());
            env.spawn(async {});
            let db = create_db(&eng, "d").await;
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            let s = get_schema(&mut t, "events").await.unwrap();
            let mut d = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            super::drop(&mut d, "events").await.unwrap();
            let (ins, dropped) = bind_beside(&db, &s, &[json!({"time": T0, "event": "a"})], d.into_request()).await;
            // Either order is serializable, but a committed drop must leave nothing of the table.
            if let Ok(dv) = dropped {
                let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                let refs = super::refs(&mut t, s.id).await.unwrap();
                let part_bytes = db.current().unwrap().m.part_bytes;
                assert!(
                    refs.is_empty() && part_bytes == 0,
                    "insert {:?} and drop (v{dv}) both committed, yet the dropped table keeps {} part refs and part_bytes {}",
                    ins.map(|c| c.version),
                    refs.len(),
                    part_bytes
                );
            }
        });
    }

    #[test]
    fn a_delete_in_the_batch_after_a_bind_sees_the_bound_events() {
        rt().block_on(async {
            let (eng, _store, env) = engine(4, cfg());
            env.spawn(async {});
            let db = create_db(&eng, "d").await;
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            let s = get_schema(&mut t, "events").await.unwrap();
            let everything = Filter::parse(json!({"col": "event", "op": "exists"})).unwrap();
            // An earlier event gives the delete something to write.
            let earlier = [json!({"time": T0 + 1, "event": "b"})];
            txn_do(&db, async |t| insert(t, "events", &earlier, 0).await).await.unwrap();
            let mut d = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            maint::delete(&db, &mut d, "events", T0 - DAY, T0 + DAY, &everything).await.unwrap();
            let (ins, deleted) = bind_beside(&db, &s, &[json!({"time": T0, "event": "a"})], d.into_request()).await;
            let ins = ins.unwrap().version;
            if let Ok(del) = deleted {
                let q = json!({"from": T0 - DAY, "to": T0 + DAY, "aggregates": {"n": {"fn": "count"}}});
                let out = query(&db, &q).await.unwrap();
                assert!(
                    del < ins || out.rows[0][0] == json!(0),
                    "the insert committed at v{ins} and a delete of every event at v{del}, yet {:?} remain",
                    out.rows
                );
            }
        });
    }

    #[test]
    fn bulk_inserts_count_toward_the_database_quota() {
        rt().block_on(async {
            let quota = 64 * 1024;
            let (eng, _store, env) = engine(5, EngineConfig { db_max_bytes: quota, ..cfg() });
            env.spawn(async {});
            let db = create_db(&eng, "d").await;
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            let s = get_schema(&mut t, "events").await.unwrap();
            let bufs = Buffers::new(
                BufferConfig { wait_ms: 0, max_bytes: 1 << 20 },
                Arc::new(|db: Arc<Db>, b| Box::pin(async move { db.bind(b).await })),
            );
            let mut r = Rng::new(1);
            let mut refused = false;
            for i in 0..40 {
                let rows: Vec<Json> = (0..50)
                    .map(|j| {
                        let noise: String = (0..100).map(|_| char::from(b'a' + r.below(26) as u8)).collect();
                        json!({"time": T0 + i * 1000 + j, "event": "a", "user": noise})
                    })
                    .collect();
                let valid = validate_rows(&s, &rows, 0).unwrap();
                let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
                match bufs.submit(db.clone(), s.clone(), valid, env.random_u64(), deadline).await {
                    Ok(_) => {}
                    Err(Error::QuotaExceeded(_)) => {
                        refused = true;
                        break;
                    }
                    Err(e) => panic!("insert {i}: {e}"),
                }
            }
            let m = db.current().unwrap().m.clone();
            assert!(
                refused || m.part_bytes <= quota,
                "a {quota} byte database accepted {} bytes of event parts; it reports size {}",
                m.part_bytes,
                m.stored_bytes()
            );
        });
    }

    #[test]
    fn a_full_database_can_delete_events() {
        rt().block_on(async {
            let quota = 64 * 1024;
            let (eng, _store, env) = engine(6, EngineConfig { db_max_bytes: quota, ..cfg() });
            env.spawn(async {});
            let db = create_db(&eng, "d").await;
            let rows: Vec<Json> = (0..40).map(|i| json!({"time": T0 + i, "event": "a", "n": i})).collect();
            txn_do(&db, async |t| insert(t, "events", &rows, 0).await).await.unwrap();
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            let table = get_schema(&mut t, "events").await.unwrap().id;
            assert!(maint::seal(&db, table).await.unwrap());
            let mut i = 0;
            for n in [400, 40, 1] {
                loop {
                    i += 1;
                    let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                    t.put(Bytes::from(format!("\x01k{i}")), crate::engine::Value::Inline(Bytes::from(vec![7u8; n])))
                        .unwrap();
                    match db.commit(t.into_request()).await {
                        Ok(_) => {}
                        Err(Error::QuotaExceeded(_)) => break,
                        Err(e) => panic!("{e}"),
                    }
                }
            }
            let some = Filter::parse(json!({"col": "n", "op": "lt", "value": 5})).unwrap();
            let r = txn_do(&db, async |t| maint::delete(&db, t, "events", T0, T0 + DAY, &some).await).await;
            assert!(r.is_ok(), "deleting events from a full database failed: {:?}", r.err());
        });
    }

    #[test]
    fn a_partition_that_keeps_receiving_parts_stays_readable() {
        rt().block_on(async {
            let cfg = EngineConfig { event_merge_fan_in: 2, event_tier_base_bytes: 1 << 30, ..cfg() };
            let (eng, _store, env) = engine(7, cfg);
            env.spawn(async {});
            let db = create_db(&eng, "d").await;
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            let s = get_schema(&mut t, "events").await.unwrap();
            let bufs = Buffers::new(
                BufferConfig { wait_ms: 0, max_bytes: 1 << 20 },
                Arc::new(|db: Arc<Db>, b| Box::pin(async move { db.bind(b).await })),
            );
            let q = json!({"from": T0, "to": T0 + DAY, "aggregates": {"n": {"fn": "count"}}});
            for i in 0..80 {
                let valid = validate_rows(&s, &[json!({"time": T0 + i, "event": "a"})], 0).unwrap();
                let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
                bufs.submit(db.clone(), s.clone(), valid, env.random_u64(), deadline).await.unwrap();
                let m = maint::maintain(&db, s.id, 8).await;
                let out = query(&db, &q).await;
                assert!(
                    m.is_ok() && out.as_ref().is_ok_and(|o| o.rows[0][0] == json!(i + 1)),
                    "after {} inserts into one partition: maintenance {m:?}, query {:?}",
                    i + 1,
                    out.map(|o| o.rows)
                );
            }
        });
    }

    /// A part ref the sweep cannot decode stops the sweep instead of
    /// letting it collect the part as an orphan.
    #[test]
    fn the_sweep_keeps_parts_whose_refs_it_cannot_read() {
        rt().block_on(async {
            let (eng, store, env) = engine(8, cfg());
            env.spawn(async {});
            let db = create_db(&eng, "d").await;
            txn_do(&db, async |t| insert(t, "events", &[json!({"time": T0, "event": "a"})], 0).await).await.unwrap();
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            let id = get_schema(&mut t, "events").await.unwrap().id;
            assert!(maint::seal(&db, id).await.unwrap());
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            let r = super::refs(&mut t, id).await.unwrap().remove(0);
            let mut bad = r.encode().to_vec();
            bad.push(0);
            t.put(r.key(), crate::engine::Value::Inline(Bytes::from(bad))).unwrap();
            db.commit(t.into_request()).await.unwrap();
            let c = &db.shared().cfg;
            let wait = c.orphan_grace_ms + c.blob_stage_ttl_ms + c.gc_grace_ms;
            for _ in 0..3 {
                tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
                let _ = db.sweep_now().await;
                let _ = db.maintenance_now().await;
            }
            assert!(
                store.keys().iter().any(|k| k.contains(&format!("/part/{}/", r.id))),
                "the sweep deleted a part whose ref it could not decode"
            );
        });
    }
}

/// A query that runs past its time limit stops with a retryable error
/// instead of holding its query slot until it finishes.
#[test]
fn queries_stop_at_their_time_limit() {
    rt().block_on(async {
        let days = 200;
        let (db, _cfg, _eng) = many_parts_cold_with(days, EngineConfig { segment_whole_fetch_max: 0, ..cfg() }).await;
        let q = Query::parse(json!({"from": T0, "to": T0 + days * DAY, "aggregates": {"n": {"fn": "count"}}})).unwrap();
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let s = get_schema(&mut t, "events").await.unwrap();
        let start = tokio::time::Instant::now();
        let limits = Limits { max_ms: 300, ..Limits::default() };
        let r = exec::run(db.shared(), &db.id, &mut t, &s, &q, &limits).await;
        assert!(
            matches!(&r, Err(Error::Unavailable { outcome_unknown: false, message, .. }) if message.contains("300 ms")),
            "{r:?}"
        );
        assert!(start.elapsed().as_millis() <= 300, "{:?}", start.elapsed());
    });
}

/// A store whose first `GET` of each event part object stalls.
struct StallFirstPartGet {
    inner: crate::store::DynStore,
    stall_ms: u64,
    seen: std::sync::Mutex<std::collections::BTreeSet<String>>,
}

impl crate::store::ObjectStore for StallFirstPartGet {
    fn get<'a>(
        &'a self,
        key: &'a str,
        opts: crate::store::GetOpts,
    ) -> crate::store::BoxFut<'a, crate::store::SResult<crate::store::GetResult>> {
        let stall = key.contains("/part/") && self.seen.lock().unwrap().insert(key.to_string());
        Box::pin(async move {
            if stall {
                tokio::time::sleep(std::time::Duration::from_millis(self.stall_ms)).await;
            }
            self.inner.get(key, opts).await
        })
    }

    fn put<'a>(
        &'a self,
        key: &'a str,
        data: Bytes,
        mode: crate::store::PutMode,
    ) -> crate::store::BoxFut<'a, crate::store::SResult<String>> {
        self.inner.put(key, data, mode)
    }

    fn delete<'a>(&'a self, key: &'a str) -> crate::store::BoxFut<'a, crate::store::SResult<()>> {
        self.inner.delete(key)
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        limit: usize,
    ) -> crate::store::BoxFut<'a, crate::store::SResult<crate::store::ListPage>> {
        self.inner.list(prefix, start_after, limit)
    }
}

/// A stalled part `GET` is hedged, so it delays a query by the hedge delay
/// rather than by the stall.
#[test]
fn stalled_part_reads_are_hedged() {
    rt().block_on(async {
        let origin = tokio::time::Instant::now();
        let env = Arc::new(crate::env::Env::sim(1, T0 as u64, origin, 0));
        let env2 = env.clone();
        let mem = Arc::new(crate::store::MemStore::new(Box::new(move || env2.now_ms())));
        let store = Arc::new(StallFirstPartGet { inner: mem, stall_ms: 30_000, seen: Default::default() });
        let cfg = EngineConfig { cache_bytes: 64 << 20, ..cfg() };
        let eng = Engine::new(store.clone(), env.clone(), cfg.clone(), "node-a".into());
        let db = create_db(&eng, "db").await;
        let rows: Vec<Json> = (0..20).map(|d| json!({"time": T0 + d * DAY, "event": "a"})).collect();
        txn_do(&db, async |t| insert(t, "events", &rows, 0).await).await.unwrap();
        let id = {
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            get_schema(&mut t, "events").await.unwrap().id
        };
        assert!(maint::seal(&db, id).await.unwrap());
        eng.release_leases(std::time::Duration::from_secs(2)).await;
        let cold = Engine::new(store.clone(), env, cfg.clone(), "node-b".into());
        let db = cold.db("db");
        db.open(true).await.unwrap();
        let start = tokio::time::Instant::now();
        let out = query(&db, &json!({"from": T0, "to": T0 + 20 * DAY, "aggregates": {"n": {"fn": "count"}}})).await;
        assert_eq!(out.unwrap().rows, vec![vec![json!(20)]]);
        let ms = start.elapsed().as_millis() as u64;
        assert!(ms < 2 * cfg.event_get_hedge_ms, "a query with stalled reads took {ms} ms");
    });
}
/// Partial results of parts read concurrently, and filter constants they prune with.
mod partial_results {
    use super::*;

    async fn count_where(db: &Arc<Db>, w: Json) -> i64 {
        let q = json!({"from": 0, "to": T0 * 2, "where": w, "aggregates": {"n": {"fn": "count"}}});
        query(db, &q).await.unwrap().rows[0][0].as_i64().unwrap()
    }

    /// A `-0.0` filter constant on a JSON path matches `0.0` rows while
    /// unsealed, so sealing must not prune them by stats.
    #[test]
    fn negative_zero_json_path_filter_survives_stat_pruning() {
        rt().block_on(async {
            let (eng, _store, _env) = engine(1, cfg());
            let db = create_db(&eng, "d").await;
            let rows = vec![json!({"time": T0, "event": "a", "props": {"z": -0.0}})];
            txn_do(&db, async |t| insert(t, "events", &rows, 0).await).await.unwrap();
            let w = json!({"col": "props", "path": ["z"], "op": "eq", "value": -0.0});
            assert_eq!(count_where(&db, w.clone()).await, 1, "unsealed");
            let id = {
                let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                get_schema(&mut t, "events").await.unwrap().id
            };
            assert!(maint::seal(&db, id).await.unwrap());
            assert_eq!(count_where(&db, w).await, 1, "sealed");
        });
    }

    async fn group_by_user(db: &Arc<Db>, days: i64, max_memory_bytes: usize) -> Result<exec::Output> {
        let q = Query::parse(json!({"from": T0, "to": T0 + days * DAY, "group_by": ["user"],
            "aggregates": {"n": {"fn": "count"}}}))
        .unwrap();
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let s = get_schema(&mut t, "events").await.unwrap();
        exec::run(db.shared(), &db.id, &mut t, &s, &q, &Limits { max_memory_bytes, ..Limits::default() }).await
    }

    /// A store whose first part `GET` after arming stalls for 5 s.
    struct StallOnePartGet {
        inner: crate::store::DynStore,
        armed: std::sync::atomic::AtomicBool,
    }

    impl crate::store::ObjectStore for StallOnePartGet {
        fn get<'a>(
            &'a self,
            key: &'a str,
            opts: crate::store::GetOpts,
        ) -> crate::store::BoxFut<'a, crate::store::SResult<crate::store::GetResult>> {
            let stall = key.contains("/part/") && self.armed.swap(false, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                if stall {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
                self.inner.get(key, opts).await
            })
        }

        fn put<'a>(
            &'a self,
            key: &'a str,
            data: Bytes,
            mode: crate::store::PutMode,
        ) -> crate::store::BoxFut<'a, crate::store::SResult<String>> {
            self.inner.put(key, data, mode)
        }

        fn delete<'a>(&'a self, key: &'a str) -> crate::store::BoxFut<'a, crate::store::SResult<()>> {
            self.inner.delete(key)
        }

        fn list<'a>(
            &'a self,
            prefix: &'a str,
            start_after: Option<&'a str>,
            limit: usize,
        ) -> crate::store::BoxFut<'a, crate::store::SResult<crate::store::ListPage>> {
            self.inner.list(prefix, start_after, limit)
        }
    }

    /// Groups by user over `days` single-row parts, read by a cold node whose
    /// first part `GET` stalls.
    async fn group_by_user_with_slow_first_part(
        days: i64,
        cfg: EngineConfig,
        max_memory_bytes: usize,
    ) -> Result<exec::Output> {
        let origin = tokio::time::Instant::now();
        let env = Arc::new(crate::env::Env::sim(1, T0 as u64, origin, 0));
        let env2 = env.clone();
        let mem = Arc::new(crate::store::MemStore::new(Box::new(move || env2.now_ms())));
        let store = Arc::new(StallOnePartGet { inner: mem, armed: Default::default() });
        let cfg = EngineConfig { cache_bytes: 64 << 20, event_get_hedge_ms: 0, ..cfg };
        let eng = Engine::new(store.clone(), env.clone(), cfg.clone(), "node-a".into());
        let db = create_db(&eng, "db").await;
        let rows: Vec<Json> =
            (0..days).map(|d| json!({"time": T0 + d * DAY, "event": "a", "user": format!("u{}", d % 2)})).collect();
        txn_do(&db, async |t| insert(t, "events", &rows, 0).await).await.unwrap();
        let id = {
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            get_schema(&mut t, "events").await.unwrap().id
        };
        assert!(maint::seal(&db, id).await.unwrap());
        eng.release_leases(std::time::Duration::from_secs(2)).await;
        let cold = Engine::new(store.clone(), env, cfg, "node-b".into());
        let db = cold.db("db");
        db.open(true).await.unwrap();
        store.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        group_by_user(&db, days, max_memory_bytes).await
    }

    /// Reading parts concurrently must not change whether a query fits its
    /// memory limit: two groups fit in 450 bytes however parts are read, even
    /// when the first part is slow to arrive.
    #[test]
    fn concurrent_parts_do_not_multiply_group_memory() {
        rt().block_on(async {
            let days = 64;
            let serial = EngineConfig { event_read_concurrency: 1, ..cfg() };
            let one = group_by_user_with_slow_first_part(days, serial, 450).await;
            assert!(one.is_ok(), "one part at a time: {one:?}");
            let many = group_by_user_with_slow_first_part(days, cfg(), 450).await;
            assert_eq!(many.map(|o| o.rows), Ok(one.unwrap().rows), "32 parts at a time");
        });
    }
}

/// A merge reads each small input part once, though it reads every column
/// of each, and does so without filling the node's cache.
#[test]
fn merges_fetch_each_small_input_once() {
    rt().block_on(async {
        let origin = tokio::time::Instant::now();
        let env = Arc::new(crate::env::Env::sim(1, T0 as u64, origin, 0));
        let env2 = env.clone();
        let mem = Arc::new(crate::store::MemStore::new(Box::new(move || env2.now_ms())));
        let store = Arc::new(crate::sim::cost::MeteredStore::new(mem, 0));
        let cfg = EngineConfig { cache_bytes: 64 << 20, event_chunk_bytes: 1 << 20, ..cfg() };
        let eng = Engine::new(store.clone(), env.clone(), cfg.clone(), "node-a".into());
        let db = create_db(&eng, "db").await;
        let id = {
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            get_schema(&mut t, "events").await.unwrap().id
        };
        for k in 0..4 {
            let rows: Vec<Json> = (0..20)
                .map(|i| json!({"time": T0 + k * 100 + i, "event": "a", "user": format!("u{i}"), "n": i, "props": {"k": i}}))
                .collect();
            txn_do(&db, async |t| insert(t, "events", &rows, 0).await).await.unwrap();
            assert!(maint::seal(&db, id).await.unwrap());
        }
        eng.release_leases(std::time::Duration::from_secs(2)).await;
        let cold = Engine::new(store.clone(), env, cfg, "node-b".into());
        let db = cold.db("db");
        db.open(true).await.unwrap();
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let s = get_schema(&mut t, "events").await.unwrap();
        let inputs = refs(&mut t, id).await.unwrap();
        assert_eq!(inputs.len(), 4);
        let before = store.scope("db").gets;
        maint::merge(&db, &s, &inputs).await.unwrap();
        let part_gets = store.scope("db").gets - before;
        assert_eq!(part_gets, inputs.len() as f64, "GETs for {} small inputs", inputs.len());
        let q = json!({"from": T0, "to": T0 + DAY, "aggregates": {"n": {"fn": "count"}}});
        assert_eq!(query(&db, &q).await.unwrap().rows, vec![vec![json!(80)]]);
    });
}
/// Maintenance whose transactions must fit the write limit, and merges that fail.
mod maintenance_limits {
    use super::*;

    async fn table_id(db: &Arc<Db>) -> u32 {
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        get_schema(&mut t, "events").await.unwrap().id
    }

    async fn part_count(db: &Arc<Db>) -> usize {
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let id = get_schema(&mut t, "events").await.unwrap().id;
        refs(&mut t, id).await.unwrap().len()
    }

    async fn part_objects(db: &Arc<Db>) -> usize {
        let sh = db.shared();
        let prefix = format!("dbs/{}/part/", db.id);
        crate::store::list_all(&*sh.store, &prefix, &sh.retry).await.unwrap().len()
    }

    /// One single-row part in each of `days` day partitions ending yesterday,
    /// sealed `per_seal` partitions at a time so that every seal fits.
    async fn daily_parts(db: &Arc<Db>, days: i64, per_seal: i64) {
        let id = table_id(db).await;
        for chunk in (0..days).collect::<Vec<_>>().chunks(per_seal as usize) {
            let rows: Vec<Json> =
                chunk.iter().map(|d| json!({"time": T0 - (days + 1 - d) * DAY, "event": "a"})).collect();
            txn_do(db, async |t| insert(t, "events", &rows, 0).await).await.unwrap();
            assert!(maint::seal(db, id).await.unwrap());
        }
        assert_eq!(part_count(db).await, days as usize);
    }

    fn small_txns() -> EngineConfig {
        EngineConfig { txn_max_write_bytes: 8 * 1024, ..cfg() }
    }

    /// Retention drops expired parts in transactions that fit the write
    /// limit, however many parts have expired.
    #[test]
    fn retention_of_many_parts_fits_the_transaction_limit() {
        rt().block_on(async {
            let (eng, _store, _env) = engine(1, small_txns());
            let db = create_db(&eng, "d").await;
            daily_parts(&db, 120, 10).await;
            txn_do(&db, async |t| set_retention(t, "events", Some(DAY as u64)).await).await.unwrap();
            let id = table_id(&db).await;
            let mut errors = Vec::new();
            for _ in 0..3 {
                if let Err(e) = maint::maintain(&db, id, 64).await {
                    errors.push(e.to_string());
                }
            }
            assert_eq!((part_count(&db).await, errors), (0, Vec::<String>::new()), "expired parts remain");
        });
    }

    /// Rows spanning more partitions than one transaction may reference are
    /// sealed by several seals, and no seal leaves parts behind.
    #[test]
    fn sealing_rows_across_many_partitions_succeeds() {
        rt().block_on(async {
            let (eng, _store, _env) = engine(1, small_txns());
            let db = create_db(&eng, "d").await;
            let id = table_id(&db).await;
            let rows: Vec<Json> = (0..60).map(|d| json!({"time": T0 - (61 - d) * DAY, "event": "a"})).collect();
            txn_do(&db, async |t| insert(t, "events", &rows, 0).await).await.unwrap();
            let mut outcomes = Vec::new();
            for _ in 0..10 {
                let r = maint::seal(&db, id).await.map_err(|e| e.to_string());
                outcomes.push(r.clone());
                if r != Ok(true) {
                    break;
                }
            }
            let leaked = part_objects(&db).await;
            assert_eq!(
                (outcomes.last().cloned(), part_count(&db).await, leaked),
                (Some(Ok(false)), 60, 60),
                "attempts: {outcomes:?}; part objects in the store: {leaked}"
            );
        });
    }

    /// The same with the shipped limits: two transactions of 60,000 events,
    /// one per hour over 14 years, committed before the next seal.
    #[test]
    fn sealing_a_large_hourly_backfill_succeeds_with_default_limits() {
        rt().block_on(async {
            let cfg = EngineConfig { event_seal_bytes: usize::MAX >> 1, ..EngineConfig::default() };
            let (eng, _store, _env) = engine(1, cfg);
            let db = eng.db("d");
            db.open(true).await.unwrap();
            let mut def = table_def();
            def.partition = schema::Granularity::Hour;
            txn_do(&db, async |t| create(t, &def).await).await.unwrap();
            let id = table_id(&db).await;
            const HOUR: i64 = 3_600_000;
            let n = 120_000i64;
            for half in (0..n).collect::<Vec<_>>().chunks(n as usize / 2) {
                let rows: Vec<Json> = half.iter().map(|h| json!({"time": T0 - (n - h) * HOUR, "event": "a"})).collect();
                txn_do(&db, async |t| insert(t, "events", &rows, 0).await).await.unwrap();
            }
            let r = maint::seal(&db, id).await;
            assert!(matches!(r, Ok(true)), "seal: {r:?}; part objects left in the store: {}", part_objects(&db).await);
        });
    }

    /// A seal racing a drop and re-create of its table under the same name,
    /// or a column drop, at every relative timing.
    #[test]
    fn seals_racing_drops_and_column_changes() {
        for delay in [0u64, 10, 20, 30, 40, 50, 60, 80, 100, 150] {
            for variant in 0..2 {
                rt().block_on(async {
                    let origin = tokio::time::Instant::now();
                    let env = Arc::new(crate::env::Env::sim(1, T0 as u64, origin, 0));
                    let env2 = env.clone();
                    let mem = Arc::new(crate::store::MemStore::new(Box::new(move || env2.now_ms())));
                    let store = Arc::new(crate::sim::cost::MeteredStore::new(mem, 0));
                    let eng = Engine::new(store.clone(), env.clone(), cfg(), "node-a".into());
                    let db = create_db(&eng, "d").await;
                    let old = table_id(&db).await;
                    let rows: Vec<Json> =
                        (0..20).map(|i| json!({"time": T0 - DAY + i * 3_600_000, "event": "a", "n": 3})).collect();
                    txn_do(&db, async |t| insert(t, "events", &rows, 0).await).await.unwrap();
                    store.set_latency_ms(10);
                    let racer = async {
                        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                        if variant == 0 {
                            txn_do(&db, async |t| drop(t, "events").await).await.unwrap();
                            txn_do(&db, async |t| create(t, &table_def()).await).await.unwrap();
                            let new = [json!({"time": T0 - DAY, "event": "b", "n": 4})];
                            txn_do(&db, async |t| insert(t, "events", &new, 0).await).await.unwrap();
                        } else {
                            txn_do(&db, async |t| drop_column(t, "events", "n").await).await.unwrap();
                            let c: ColumnSpec = serde_json::from_value(
                                json!({"name": "n", "type": "string", "nullable": false, "default": "z"}),
                            )
                            .unwrap();
                            txn_do(&db, async |t| add_column(t, "events", &c).await).await.unwrap();
                        }
                    };
                    let (sealed, _) = tokio::join!(maint::seal(&db, old), racer);
                    if let Err(e) = &sealed {
                        assert!(matches!(e, Error::Conflict(_) | Error::NotFound(_)), "seal: {e}");
                    }
                    let id = table_id(&db).await;
                    while maint::seal(&db, id).await.unwrap() {}
                    let q = json!({"from": 0, "to": T0 * 2, "group_by": ["event", "n"],
                        "aggregates": {"c": {"fn": "count"}}});
                    let got = query(&db, &q).await.unwrap().rows;
                    let want = if variant == 0 {
                        vec![vec![json!("b"), json!(4), json!(1)]]
                    } else {
                        vec![vec![json!("a"), json!("z"), json!(20)]]
                    };
                    assert_eq!(got, want, "delay {delay} variant {variant}, first seal {sealed:?}");
                    let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                    let stale = refs(&mut t, old).await.unwrap();
                    let live: u64 = refs(&mut t, id).await.unwrap().iter().map(|r| r.stored_bytes()).sum();
                    if variant == 0 {
                        assert!(stale.is_empty(), "delay {delay}: the dropped table kept {} refs", stale.len());
                    }
                    let st = super::state(&mut t, old).await.unwrap();
                    if variant == 0 {
                        assert_eq!(st.unsealed_rows, 0, "delay {delay}");
                    }
                    assert_eq!(db.current().unwrap().m.part_bytes, live, "delay {delay} variant {variant}");
                });
            }
        }
    }

    /// A merge that outlives `event_merge_max_ms` is abandoned without
    /// holding up the merges of the table's other partitions.
    #[test]
    fn a_merge_that_runs_too_long_does_not_block_other_partitions() {
        rt().block_on(async {
            let origin = tokio::time::Instant::now();
            let env = Arc::new(crate::env::Env::sim(1, T0 as u64, origin, 0));
            let env2 = env.clone();
            let mem = Arc::new(crate::store::MemStore::new(Box::new(move || env2.now_ms())));
            let store = Arc::new(crate::sim::cost::MeteredStore::new(mem, 0));
            let cfg = EngineConfig { event_merge_max_ms: 2_000, segment_whole_fetch_max: 0, ..cfg() };
            let eng = Engine::new(store.clone(), env.clone(), cfg, "node-a".into());
            let db = create_db(&eng, "d").await;
            let id = table_id(&db).await;
            // Partition A gets three parts of 250 rows (many granules, so a
            // merge of about 2.5 s); partition B, a day later, three single-row
            // parts (a merge of about 1.4 s).
            for k in 0..3i64 {
                let mut rows: Vec<Json> = (0..250)
                    .map(|i| json!({"time": T0 - 10 * DAY + k * 1000 + i, "event": format!("e{i}"), "n": i}))
                    .collect();
                rows.push(json!({"time": T0 - 9 * DAY + k, "event": "b"}));
                txn_do(&db, async |t| insert(t, "events", &rows, 0).await).await.unwrap();
                assert!(maint::seal(&db, id).await.unwrap());
            }
            assert_eq!(part_count(&db).await, 6);
            store.set_latency_ms(25);
            let mut errors = Vec::new();
            for _ in 0..3 {
                let start = tokio::time::Instant::now();
                let r = maint::maintain(&db, id, 8).await;
                errors.push(format!("{:?} after {} ms", r.err(), start.elapsed().as_millis()));
            }
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            let b = ev::partition_key(T0 / DAY - 9);
            let in_b = refs(&mut t, id).await.unwrap().iter().filter(|r| r.partition == b).count();
            assert_eq!(in_b, 1, "partition B was never merged; maintenance: {errors:?}");
        });
    }
}
