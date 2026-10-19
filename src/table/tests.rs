//! Table layer model tests.

use super::*;
use crate::engine::tests::{engine, rt};
use crate::engine::{Consistency, EngineConfig, Isolation};
use crate::util::prop;
use crate::util::rng::Rng;
use serde_json::json;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq)]
struct MRow {
    name: Option<String>,
    score: i64,
    tag: Option<String>,
}

#[derive(Clone, Debug)]
enum Op {
    Insert(i64, Option<String>, i64, Option<String>),
    Upsert(i64, Option<String>, i64, Option<String>),
    UpdateScore(i64, i64),
    UpdateName(i64, Option<String>),
    Delete(i64),
    Get(i64),
    QueryTag(Option<String>, i64, usize, bool),
    QueryPk(i64, i64, usize),
    UpdateWhere(Option<String>, i64, Result<i64, Option<String>>, Option<usize>),
    DeleteWhere(i64, i64, Option<usize>),
    Commit,
}

fn name(r: &mut Rng) -> Option<String> {
    if r.chance(0.2) { None } else { Some(format!("n{}", r.below(12))) }
}

fn tag(r: &mut Rng) -> Option<String> {
    if r.chance(0.15) { None } else { Some(["red", "green", "blue"][r.usize(3)].to_string()) }
}

fn gen_ops(r: &mut Rng) -> Vec<Op> {
    (0..20 + r.usize(120))
        .map(|_| match r.below(20) {
            0..=3 => Op::Insert(r.below(30) as i64, name(r), r.below(10) as i64, tag(r)),
            4..=5 => Op::Upsert(r.below(30) as i64, name(r), r.below(10) as i64, tag(r)),
            6..=7 => Op::UpdateScore(r.below(30) as i64, r.below(10) as i64),
            8 => Op::UpdateName(r.below(30) as i64, name(r)),
            9..=10 => Op::Delete(r.below(30) as i64),
            11 => Op::Get(r.below(30) as i64),
            12..=13 => Op::QueryTag(tag(r), r.below(10) as i64, 1 + r.usize(5), r.chance(0.5)),
            14 => Op::QueryPk(r.below(30) as i64, r.below(30) as i64, 1 + r.usize(10)),
            15 => {
                let set = if r.chance(0.7) { Ok(r.below(10) as i64) } else { Err(name(r)) };
                let limit = r.chance(0.3).then(|| 1 + r.usize(4));
                Op::UpdateWhere(tag(r), r.below(10) as i64, set, limit)
            }
            16 => Op::DeleteWhere(r.below(30) as i64, r.below(30) as i64, r.chance(0.3).then(|| 1 + r.usize(4))),
            _ => Op::Commit,
        })
        .collect()
}

fn row_json(id: i64, m: &MRow) -> Json {
    json!({"id": id, "name": m.name, "score": m.score, "tag": m.tag})
}

fn strip(v: &Json) -> Json {
    json!({"id": v["id"], "name": v["name"], "score": v["score"], "tag": v["tag"]})
}

async fn run(seed: u64, ops: &[Op]) -> Result<(), String> {
    let (eng, _store, _env) = engine(seed, EngineConfig::small_for_tests());
    let db = eng.db("tbl");
    db.open(true).await.map_err(|e| e.to_string())?;
    let blobs = BlobResolver { db: &db };
    let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
    let def: CreateTable = serde_json::from_value(json!({
        "name": "t",
        "columns": [
            {"name": "id", "type": "int", "nullable": false},
            {"name": "name", "type": "string"},
            {"name": "score", "type": "int", "nullable": false, "default": 0},
            {"name": "tag", "type": "string"},
            {"name": "extra", "type": "json"}
        ],
        "primary_key": ["id"],
        "indexes": [
            {"name": "by_name", "columns": ["name"], "unique": true},
            {"name": "by_tag_score", "columns": ["tag", "score"]}
        ]
    }))
    .unwrap();
    create_table(&mut t, &def).await.map_err(|e| e.to_string())?;
    db.commit(t.into_request()).await.map_err(|e| e.to_string())?;

    let mut model: BTreeMap<i64, MRow> = BTreeMap::new();
    let mut work = model.clone();
    let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
    for op in ops {
        let res: Result<(), Error> = async {
            match op {
                Op::Insert(id, n, s, g) | Op::Upsert(id, n, s, g) => {
                    let upsert = matches!(op, Op::Upsert(..));
                    let mode = if upsert { InsertMode::Upsert } else { InsertMode::Insert };
                    let r =
                        insert(&mut t, &blobs, "t", &json!({"id": id, "name": n, "score": s, "tag": g}), mode).await;
                    let dup_pk = work.contains_key(id) && !upsert;
                    let dup_name = n.is_some() && work.iter().any(|(k, v)| k != id && v.name == *n);
                    match (&r, dup_pk || dup_name) {
                        (Ok(()), false) => {
                            work.insert(*id, MRow { name: n.clone(), score: *s, tag: g.clone() });
                        }
                        (Err(Error::AlreadyExists(_)), true) => return Err(Error::AlreadyExists("expected".into())),
                        _ => {
                            return Err(Error::internal(format!(
                                "insert {op:?}: {r:?}, dup_pk={dup_pk} dup_name={dup_name}"
                            )));
                        }
                    }
                }
                Op::UpdateScore(id, s) => {
                    let found = update(&mut t, &blobs, "t", &json!(id), &json!({"score": s})).await?;
                    if found != work.contains_key(id) {
                        return Err(Error::internal("update found mismatch"));
                    }
                    if let Some(r) = work.get_mut(id) {
                        r.score = *s;
                    }
                }
                Op::UpdateName(id, n) => {
                    let r = update(&mut t, &blobs, "t", &json!([id]), &json!({"name": n})).await;
                    let dup = n.is_some() && work.iter().any(|(k, v)| k != id && v.name == *n);
                    match r {
                        Ok(found) if !dup => {
                            if found != work.contains_key(id) {
                                return Err(Error::internal("update found mismatch"));
                            }
                            if let Some(r) = work.get_mut(id) {
                                r.name = n.clone();
                            }
                        }
                        Err(Error::AlreadyExists(_)) if dup && work.contains_key(id) => {
                            return Err(Error::AlreadyExists("expected".into()));
                        }
                        Ok(false) if dup && !work.contains_key(id) => {}
                        other => return Err(Error::internal(format!("update name: {other:?} dup={dup}"))),
                    }
                }
                Op::Delete(id) => {
                    let found = delete(&mut t, "t", &json!(id)).await?;
                    if found != work.remove(id).is_some() {
                        return Err(Error::internal("delete found mismatch"));
                    }
                }
                Op::Get(id) => {
                    let got = get(&mut t, "t", &json!(id)).await?.map(|v| strip(&v));
                    let want = work.get(id).map(|m| row_json(*id, m));
                    if got != want {
                        return Err(Error::internal(format!("get {id}: {got:?} vs {want:?}")));
                    }
                }
                Op::QueryTag(g, min, limit, rev) => {
                    let q: Query = serde_json::from_value(json!({
                        "index": "by_tag_score", "eq": [g], "range": {"gte": min},
                        "limit": limit, "reverse": rev
                    }))
                    .unwrap();
                    let mut got = Vec::new();
                    let mut q = q;
                    loop {
                        let r = query(&mut t, "t", &q).await?;
                        got.extend(r.rows.iter().map(strip));
                        match r.next_cursor {
                            Some(c) => q.cursor = Some(c),
                            None => break,
                        }
                    }
                    let mut want: Vec<(i64, i64)> = work
                        .iter()
                        .filter(|(_, m)| m.tag == *g && m.score >= *min)
                        .map(|(id, m)| (m.score, *id))
                        .collect();
                    want.sort();
                    if *rev {
                        want.reverse();
                    }
                    let want: Vec<Json> = want.iter().map(|(_, id)| row_json(*id, &work[id])).collect();
                    if got != want {
                        return Err(Error::internal(format!("query tag {g:?}>={min}: {got:?} vs {want:?}")));
                    }
                }
                Op::QueryPk(a, b, limit) => {
                    let q: Query = serde_json::from_value(json!({
                        "range": {"gte": a, "lt": b}, "limit": limit,
                        "filter": {"or": [{"col": "tag", "op": "eq", "value": "red"}, {"not": {"col": "name", "op": "exists"}}]}
                    }))
                    .unwrap();
                    let r = query(&mut t, "t", &q).await?;
                    let want: Vec<Json> = work
                        .range(*a..(*b).max(*a))
                        .filter(|(_, m)| m.tag.as_deref() == Some("red") || m.name.is_none())
                        .take(*limit)
                        .map(|(id, m)| row_json(*id, m))
                        .collect();
                    let got: Vec<Json> = r.rows.iter().map(strip).collect();
                    if got != want {
                        return Err(Error::internal(format!("query pk {a}..{b}: {got:?} vs {want:?}")));
                    }
                }
                Op::UpdateWhere(g, min, set, limit) => {
                    let sel: Selection =
                        serde_json::from_value(json!({"index": "by_tag_score", "eq": [g], "range": {"gte": min}}))
                            .unwrap();
                    let set_json = match set {
                        Ok(score) => json!({"score": score}),
                        Err(n) => json!({"name": n}),
                    };
                    let r = update_where(&mut t, &blobs, "t", &sel, &set_json, Expiry::Keep, *limit).await;
                    let mut chosen: Vec<(i64, i64)> = work
                        .iter()
                        .filter(|(_, m)| m.tag == *g && m.score >= *min)
                        .map(|(id, m)| (m.score, *id))
                        .collect();
                    chosen.sort();
                    chosen.truncate(limit.unwrap_or(usize::MAX));
                    let mut after = work.clone();
                    let mut dup = false;
                    for (_, id) in &chosen {
                        match set {
                            Ok(score) => after.get_mut(id).unwrap().score = *score,
                            Err(n) => {
                                dup |= n.is_some() && after.iter().any(|(k, v)| k != id && v.name == *n);
                                after.get_mut(id).unwrap().name = n.clone();
                            }
                        }
                    }
                    match r {
                        Ok(n) if !dup && n == chosen.len() => work = after,
                        Err(Error::AlreadyExists(_)) if dup => return Err(Error::AlreadyExists("expected".into())),
                        other => {
                            return Err(Error::internal(format!(
                                "{op:?}: {other:?}, want {} rows (dup={dup})",
                                chosen.len()
                            )));
                        }
                    }
                }
                Op::DeleteWhere(a, b, limit) => {
                    let sel: Selection = serde_json::from_value(json!({
                        "range": {"gte": a, "lt": b},
                        "filter": {"or": [{"col": "tag", "op": "eq", "value": "red"}, {"not": {"col": "name", "op": "exists"}}]}
                    }))
                    .unwrap();
                    let n = delete_where(&mut t, "t", &sel, *limit).await?;
                    let doomed: Vec<i64> = work
                        .range(*a..(*b).max(*a))
                        .filter(|(_, m)| m.tag.as_deref() == Some("red") || m.name.is_none())
                        .map(|(id, _)| *id)
                        .take(limit.unwrap_or(usize::MAX))
                        .collect();
                    if n != doomed.len() {
                        return Err(Error::internal(format!("{op:?}: deleted {n}, want {}", doomed.len())));
                    }
                    for id in doomed {
                        work.remove(&id);
                    }
                }
                Op::Commit => {
                    let next = db.begin(Isolation::Serializable, Consistency::Strong).await?;
                    let old = std::mem::replace(&mut t, next);
                    db.commit(old.into_request()).await?;
                    model = work.clone();
                    let mut c = db.begin(Isolation::Serializable, Consistency::Strong).await?;
                    check_indexes(&mut c, "t").await?;
                    t = db.begin(Isolation::Serializable, Consistency::Strong).await?;
                }
            }
            Ok(())
        }
        .await;
        match res {
            Ok(()) => {}
            Err(Error::AlreadyExists(_)) => {
                // The API aborts the transaction on any op error.
                work = model.clone();
                t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            }
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(())
}

#[test]
fn prop_tables_match_model() {
    prop::check(
        "tables-model",
        60,
        |r| (r.next_u64(), gen_ops(r)),
        |(s, ops)| prop::shrink_vec(ops).into_iter().map(|o| (*s, o)).collect(),
        |(seed, ops)| rt().block_on(run(*seed, ops)),
    );
}

#[test]
fn schema_changes_and_blobs() {
    rt().block_on(async {
        let (eng, _store, _env) = engine(7, EngineConfig::small_for_tests());
        let db = eng.db("s");
        db.open(true).await.unwrap();
        let blobs = BlobResolver { db: &db };
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let def: CreateTable = serde_json::from_value(json!({
            "name": "docs",
            "columns": [
                {"name": "org", "type": "string", "nullable": false},
                {"name": "id", "type": "int", "nullable": false},
                {"name": "title", "type": "string"},
                {"name": "body", "type": "blob"}
            ],
            "primary_key": ["org", "id"]
        }))
        .unwrap();
        create_table(&mut t, &def).await.unwrap();
        assert!(matches!(create_table(&mut t, &def).await, Err(Error::AlreadyExists(_))));
        let blob = crate::engine::blob::write_blob(db.shared(), "s", b"hello blob", "text/plain").await.unwrap();
        for i in 0..5 {
            insert(
                &mut t,
                &blobs,
                "docs",
                &json!({"org": "a", "id": i, "title": format!("t{}", i % 2), "body": if i == 0 { json!(blob.id) } else { json!(null) }}),
                InsertMode::Insert,
            )
            .await
            .unwrap();
        }
        db.commit(t.into_request()).await.unwrap();

        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let row = get(&mut t, "docs", &json!(["a", 0])).await.unwrap().unwrap();
        assert_eq!(row["body"]["size"], json!(10));
        assert!(get(&mut t, "docs", &json!(0)).await.is_err());
        add_column(&mut t, "docs", &serde_json::from_value(json!({"name": "rank", "type": "int", "nullable": false, "default": 5})).unwrap()).await.unwrap();
        db.commit(t.into_request()).await.unwrap();
        add_index(&db, "docs", &index_spec(json!({"name": "by_rank", "columns": ["rank", "title"]})), small()).await.unwrap();

        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        check_indexes(&mut t, "docs").await.unwrap();
        let q: Query = serde_json::from_value(json!({"index": "by_rank", "eq": [5, "t1"]})).unwrap();
        assert_eq!(query(&mut t, "docs", &q).await.unwrap().rows.len(), 2);
        let e = add_index(&db, "docs", &index_spec(json!({"name": "u", "columns": ["title"], "unique": true})), small()).await;
        assert!(matches!(e, Err(Error::AlreadyExists(_))));
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        assert!(get_schema(&mut t, "docs").await.unwrap().index("u").is_none(), "a failed build leaves no index");
        check_no_orphans(&mut t).await.unwrap();
        assert!(drop_column(&mut t, "docs", "rank").await.is_err());
        drop_index(&mut t, "docs", "by_rank").await.unwrap();
        drop_column(&mut t, "docs", "rank").await.unwrap();
        drop_column(&mut t, "docs", "body").await.unwrap();
        let row = get(&mut t, "docs", &json!(["a", 0])).await.unwrap().unwrap();
        assert!(row.get("body").is_none() && row.get("rank").is_none());
        db.commit(t.into_request()).await.unwrap();

        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        drop_table(&mut t, "docs").await.unwrap();
        db.commit(t.into_request()).await.unwrap();
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert_eq!(v.all_entries(false).await.unwrap().len(), 1);
    });
}

#[test]
fn checkers_reject_corruption() {
    rt().block_on(async {
        let (eng, _store, _env) = engine(11, EngineConfig::small_for_tests());
        let db = eng.db("chk");
        db.open(true).await.unwrap();
        let blobs = BlobResolver { db: &db };
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let def: CreateTable = serde_json::from_value(json!({
            "name": "t",
            "columns": [
                {"name": "id", "type": "int", "nullable": false},
                {"name": "email", "type": "string"},
                {"name": "n", "type": "int"}
            ],
            "primary_key": ["id"],
            "indexes": [
                {"name": "by_email", "columns": ["email"], "unique": true},
                {"name": "by_n", "columns": ["n"]}
            ]
        }))
        .unwrap();
        create_table(&mut t, &def).await.unwrap();
        for i in 0..4 {
            let row = json!({"id": i, "email": format!("u{i}@x.io"), "n": i % 2});
            insert(&mut t, &blobs, "t", &row, InsertMode::Insert).await.unwrap();
        }
        db.commit(t.into_request()).await.unwrap();
        let fresh = || async { db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap() };
        let mut t = fresh().await;
        check_indexes(&mut t, "t").await.unwrap();
        check_no_orphans(&mut t).await.unwrap();
        let s = get_schema(&mut t, "t").await.unwrap();
        let by_n = s.index("by_n").unwrap().id;
        let entries = t.scan(&KeyRange::prefix(&index_prefix(s.id, by_n)), false, 100).await.unwrap();
        assert_eq!(entries.len(), 4);

        let mut t = fresh().await;
        t.delete(entries[0].key.clone()).unwrap();
        assert!(check_indexes(&mut t, "t").await.is_err(), "missing index entry not detected");

        let mut t = fresh().await;
        let mut stale = entries[0].key.to_vec();
        stale.push(0);
        t.put(Bytes::from(stale), Value::Inline(Bytes::new())).unwrap();
        assert!(check_indexes(&mut t, "t").await.is_err(), "stale index entry not detected");

        let mut t = fresh().await;
        let r0 = t.get(&row_key(s.id, &[Key::Int(0)])).await.unwrap().unwrap();
        t.put(row_key(s.id, &[Key::Int(9)]), r0.value).unwrap();
        assert!(check_indexes(&mut t, "t").await.is_err(), "unique violation not detected");

        let mut t = fresh().await;
        t.put(row_key(s.id + 7, &[Key::Int(1)]), Value::Inline(Bytes::new())).unwrap();
        assert!(check_no_orphans(&mut t).await.is_err(), "row of a dropped table not detected");
        let mut t = fresh().await;
        let mut k = index_prefix(s.id, by_n + 7);
        k.push(1);
        t.put(Bytes::from(k), Value::Inline(Bytes::new())).unwrap();
        assert!(check_no_orphans(&mut t).await.is_err(), "entry of a dropped index not detected");
        let mut t = fresh().await;
        t.put(backfill_key(s.id, by_n + 7), Value::Inline(Bytes::new())).unwrap();
        assert!(check_no_orphans(&mut t).await.is_err(), "backfill cursor of a dropped index not detected");
    });
}

async fn begin(db: &Arc<Db>) -> Txn {
    db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap()
}

fn table_def(v: Json) -> CreateTable {
    serde_json::from_value(v).unwrap()
}

fn index_spec(v: Json) -> IndexSpec {
    serde_json::from_value(v).unwrap()
}

fn small() -> Backfill {
    Backfill { batch_rows: 7, ..Backfill::default() }
}

fn column(v: Json) -> ColumnSpec {
    serde_json::from_value(v).unwrap()
}

#[derive(Clone, Debug)]
enum F {
    And(Vec<F>),
    Or(Vec<F>),
    Not(Box<F>),
    Cmp(&'static str, &'static str, Json),
}

impl F {
    fn json(&self) -> Json {
        match self {
            F::And(v) => json!({"and": v.iter().map(F::json).collect::<Vec<_>>()}),
            F::Or(v) => json!({"or": v.iter().map(F::json).collect::<Vec<_>>()}),
            F::Not(f) => json!({"not": f.json()}),
            F::Cmp(col, "exists", _) => json!({"col": col, "op": "exists"}),
            F::Cmp(col, op, value) => json!({"col": col, "op": op, "value": value}),
        }
    }
}

#[derive(Clone, Debug)]
struct FRow {
    i: Option<i64>,
    f: Option<f64>,
    s: Option<String>,
    b: Option<Vec<u8>>,
    bo: Option<bool>,
    j: Option<Json>,
}

const F_FLOATS: [f64; 6] = [-1.5, -0.0, 0.0, 0.5, 1.0, 2.25];
const F_STRS: [&str; 5] = ["", "a", "ab", "b", "é"];
const F_BYTES: [&[u8]; 5] = [&[], &[0], &[0, 1], &[1], &[255]];

fn f_json(r: &mut Rng) -> Json {
    [json!({"k": 1}), json!([1]), json!("s"), json!(2)][r.usize(4)].clone()
}

fn gen_frow(r: &mut Rng) -> FRow {
    let some = |r: &mut Rng| !r.chance(0.2);
    FRow {
        i: some(r).then(|| r.below(5) as i64 - 2),
        f: some(r).then(|| F_FLOATS[r.usize(F_FLOATS.len())]),
        s: some(r).then(|| F_STRS[r.usize(F_STRS.len())].to_string()),
        b: some(r).then(|| F_BYTES[r.usize(F_BYTES.len())].to_vec()),
        bo: some(r).then(|| r.chance(0.5)),
        j: some(r).then(|| f_json(r)),
    }
}

fn gen_operand(r: &mut Rng, col: &str, op: &str) -> Json {
    match col {
        "i" => json!(r.below(5) as i64 - 2),
        "f" if r.chance(0.3) => json!(r.below(3) as i64 - 1),
        "f" => json!(F_FLOATS[r.usize(F_FLOATS.len())]),
        "s" => json!(F_STRS[r.usize(F_STRS.len())]),
        "b" => json!(b64::encode(F_BYTES[r.usize(F_BYTES.len())])),
        "bo" => json!(r.chance(0.5)),
        _ => match op {
            "prefix" => json!(["", "s", "x"][r.usize(3)]),
            "lt" | "le" | "gt" | "ge" => [json!("s"), json!("t"), json!(1), json!(2.0)][r.usize(4)].clone(),
            _ if r.chance(0.3) => [json!(1), json!(2.0), json!("x")][r.usize(3)].clone(),
            _ => f_json(r),
        },
    }
}

fn gen_filter(r: &mut Rng, depth: u32) -> F {
    match r.below(if depth > 2 { 1 } else { 6 }) {
        0..=2 => {
            let col = ["i", "f", "s", "b", "bo", "j"][r.usize(6)];
            let ops: &[&'static str] = match col {
                "s" | "b" | "j" => &["eq", "ne", "lt", "le", "gt", "ge", "in", "prefix", "contains", "exists"],
                _ => &["eq", "ne", "lt", "le", "gt", "ge", "in", "exists"],
            };
            let op = ops[r.usize(ops.len())];
            let value = match op {
                "in" => Json::Array((0..r.usize(4)).map(|_| gen_operand(r, col, op)).collect()),
                "exists" => Json::Null,
                _ => gen_operand(r, col, op),
            };
            F::Cmp(col, op, value)
        }
        3 => F::And((0..r.usize(3)).map(|_| gen_filter(r, depth + 1)).collect()),
        4 => F::Or((0..r.usize(3)).map(|_| gen_filter(r, depth + 1)).collect()),
        _ => F::Not(Box::new(gen_filter(r, depth + 1))),
    }
}

/// Every comparison of a null column fails; JSON compares numbers by value
/// and orders only numbers and strings.
fn oracle(f: &F, row: &FRow) -> bool {
    use std::cmp::Ordering::{self, *};
    let present = |col: &str| match col {
        "i" => row.i.is_some(),
        "f" => row.f.is_some(),
        "s" => row.s.is_some(),
        "b" => row.b.is_some(),
        "bo" => row.bo.is_some(),
        _ => row.j.is_some(),
    };
    let json_cmp = |a: &Json, b: &Json| match (a, b) {
        (Json::Number(x), Json::Number(y)) => x.as_f64()?.partial_cmp(&y.as_f64()?),
        (Json::String(x), Json::String(y)) => Some(x.cmp(y)),
        _ => None,
    };
    let json_eq = |a: &Json, b: &Json| a == b || json_cmp(a, b) == Some(Equal);
    let cmp = |col: &str, v: &Json| -> Option<Ordering> {
        match col {
            "i" => Some(row.i?.cmp(&v.as_i64()?)),
            "f" => row.f?.partial_cmp(&v.as_f64()?),
            "s" => Some(row.s.as_ref()?.as_bytes().cmp(v.as_str()?.as_bytes())),
            "b" => Some(row.b.as_deref()?.cmp(b64::decode(v.as_str()?)?.as_slice())),
            "bo" => Some(row.bo?.cmp(&v.as_bool()?)),
            _ => json_cmp(row.j.as_ref()?, v),
        }
    };
    let eq = |col: &str, v: &Json| match col {
        "j" => row.j.as_ref().is_some_and(|j| json_eq(j, v)),
        _ => cmp(col, v) == Some(Equal),
    };
    match f {
        F::And(v) => v.iter().all(|x| oracle(x, row)),
        F::Or(v) => v.iter().any(|x| oracle(x, row)),
        F::Not(x) => !oracle(x, row),
        F::Cmp(col, op, v) => match *op {
            "exists" => present(col),
            "in" => v.as_array().unwrap().iter().any(|x| eq(col, x)),
            "prefix" | "contains" => {
                let p = v.as_str().map(|p| if *col == "b" { b64::decode(p).unwrap() } else { p.as_bytes().to_vec() });
                let hay: Option<&[u8]> = match (*col, &row.j) {
                    ("s", _) => row.s.as_ref().map(|s| s.as_bytes()),
                    ("b", _) => row.b.as_deref(),
                    ("j", Some(Json::String(s))) => Some(s.as_bytes()),
                    _ => None,
                };
                match (*op, hay, p) {
                    ("prefix", Some(h), Some(p)) => h.starts_with(&p),
                    ("contains", Some(h), Some(p)) => p.is_empty() || h.windows(p.len()).any(|w| w == p.as_slice()),
                    ("contains", None, _) if *col == "j" => {
                        matches!(&row.j, Some(Json::Array(a)) if a.iter().any(|x| json_eq(x, v)))
                    }
                    _ => false,
                }
            }
            "eq" => eq(col, v),
            "ne" => present(col) && !eq(col, v),
            "lt" => cmp(col, v) == Some(Less),
            "le" => matches!(cmp(col, v), Some(Less | Equal)),
            "gt" => cmp(col, v) == Some(Greater),
            _ => matches!(cmp(col, v), Some(Greater | Equal)),
        },
    }
}

#[test]
fn filters_match_oracle() {
    rt().block_on(async {
        let (eng, _store, _env) = engine(13, EngineConfig::small_for_tests());
        let db = eng.db("flt");
        db.open(true).await.unwrap();
        let blobs = BlobResolver { db: &db };
        let mut t = begin(&db).await;
        let def = table_def(json!({
            "name": "f",
            "columns": [
                {"name": "id", "type": "int", "nullable": false},
                {"name": "i", "type": "int"},
                {"name": "f", "type": "float"},
                {"name": "s", "type": "string"},
                {"name": "b", "type": "bytes"},
                {"name": "bo", "type": "bool"},
                {"name": "j", "type": "json"}
            ],
            "primary_key": ["id"]
        }));
        create_table(&mut t, &def).await.unwrap();
        let mut r = Rng::new(13);
        let rows: Vec<FRow> = (0..16).map(|_| gen_frow(&mut r)).collect();
        for (id, row) in rows.iter().enumerate() {
            let obj = json!({"id": id, "i": row.i, "f": row.f, "s": row.s,
                "b": row.b.as_deref().map(b64::encode), "bo": row.bo, "j": row.j});
            insert(&mut t, &blobs, "f", &obj, InsertMode::Insert).await.unwrap();
        }
        for _ in 0..400 {
            let f = gen_filter(&mut r, 0);
            let q: Query = serde_json::from_value(json!({"filter": f.json(), "limit": 1000})).unwrap();
            let got: Vec<i64> =
                query(&mut t, "f", &q).await.unwrap().rows.iter().map(|v| v["id"].as_i64().unwrap()).collect();
            let want: Vec<i64> =
                rows.iter().enumerate().filter(|(_, row)| oracle(&f, row)).map(|(id, _)| id as i64).collect();
            assert_eq!(got, want, "filter {}", f.json());
        }
    });
}

#[test]
fn filter_validation() {
    rt().block_on(async {
        let (eng, _store, _env) = engine(14, EngineConfig::small_for_tests());
        let db = eng.db("fv");
        db.open(true).await.unwrap();
        let mut t = begin(&db).await;
        let def = table_def(json!({
            "name": "v",
            "columns": [
                {"name": "id", "type": "int", "nullable": false},
                {"name": "s", "type": "string"},
                {"name": "b", "type": "bytes"},
                {"name": "f", "type": "float"},
                {"name": "body", "type": "blob"}
            ],
            "primary_key": ["id"]
        }));
        create_table(&mut t, &def).await.unwrap();
        let nested = |n: usize, wrap: fn(Json) -> Json| {
            (0..n).fold(json!({"col": "id", "op": "eq", "value": 1}), |f, _| wrap(f))
        };
        let cases = [
            (nested(16, |f| json!({"not": f})), true),
            (nested(17, |f| json!({"not": f})), false),
            (nested(16, |f| json!({"and": [f]})), true),
            (nested(17, |f| json!({"and": [f]})), false),
            (nested(17, |f| json!({"or": [f]})), false),
            (json!({"col": "id", "op": "in", "value": [1, 2]}), true),
            (json!({"col": "id", "op": "in", "value": vec![1; 1000]}), true),
            (json!({"col": "id", "op": "in", "value": vec![1; 1001]}), false),
            (json!({"col": "id", "op": "in", "value": 1}), false),
            (json!({"col": "s", "op": "prefix", "value": "a"}), true),
            (json!({"col": "b", "op": "prefix", "value": "AA=="}), true),
            (json!({"col": "id", "op": "prefix", "value": 1}), false),
            (json!({"col": "f", "op": "gt", "value": 1}), true),
            (json!({"col": "f", "op": "gt", "value": "1"}), false),
            (json!({"col": "nope", "op": "eq", "value": 1}), false),
            (json!({"col": "body", "op": "eq", "value": "x"}), false),
            (json!({"col": "body", "op": "exists"}), true),
            (json!({"col": "body", "op": "exists", "value": null}), false),
            (json!({"col": "s", "op": "contains", "value": "a"}), true),
            (json!({"col": "b", "op": "contains", "value": "AA=="}), true),
            (json!({"col": "f", "op": "contains", "value": 1}), false),
            (json!({"col": "s", "op": "eq", "value": null}), false),
            (json!({"col": "s", "op": "eq", "value": 1}), false),
            (json!({"col": "s", "op": "in", "value": ["a", null]}), false),
            (json!({"and": [{"col": "s", "op": "exists"}], "or": [{"col": "b", "op": "exists"}]}), false),
            (json!({"not": {"col": "s", "op": "exists"}, "col": "b", "op": "exists"}), false),
            (json!({"and": [], "bogus": 1}), false),
            (json!({"col": "s", "field": "b", "op": "exists"}), false),
            (json!({"field": "s", "op": "exists"}), false),
        ];
        for (filter, ok) in cases {
            let q: Query = match serde_json::from_value(json!({"filter": filter})) {
                Ok(q) => q,
                Err(e) => {
                    assert!(!ok, "{filter} unparsed: {e}");
                    continue;
                }
            };
            match query(&mut t, "v", &q).await {
                Ok(_) => assert!(ok, "{filter} accepted"),
                Err(Error::InvalidArgument(e)) => assert!(!ok, "{filter} rejected: {e}"),
                Err(e) => panic!("{filter}: {e}"),
            }
        }
    });
}

#[test]
fn schema_limits() {
    rt().block_on(async {
        let (eng, _store, _env) = engine(15, EngineConfig::small_for_tests());
        let db = eng.db("lim");
        db.open(true).await.unwrap();
        let cols = |n: usize| -> Vec<Json> {
            (0..n).map(|i| json!({"name": format!("c{i}"), "type": "int", "nullable": i >= 9})).collect()
        };
        let names = |n: usize| -> Vec<String> { (0..n).map(|i| format!("c{i}")).collect() };
        let index = |n: usize| json!([{"name": "i", "columns": names(n)}]);
        let cases = [
            (json!({"columns": cols(256), "primary_key": ["c0"]}), true),
            (json!({"columns": cols(257), "primary_key": ["c0"]}), false),
            (json!({"columns": cols(0), "primary_key": ["c0"]}), false),
            (json!({"columns": cols(10), "primary_key": names(8)}), true),
            (json!({"columns": cols(10), "primary_key": names(9)}), false),
            (json!({"columns": cols(10), "primary_key": []}), false),
            (json!({"columns": cols(10), "primary_key": ["c0"], "indexes": index(8)}), true),
            (json!({"columns": cols(10), "primary_key": ["c0"], "indexes": index(9)}), false),
            (json!({"columns": cols(10), "primary_key": ["c0"], "indexes": index(0)}), false),
            (
                json!({"columns": [{"name": "k", "type": "int"}, {"name": "d", "type": "int", "default": "x"}],
                    "primary_key": ["k"]}),
                false,
            ),
            (
                json!({"columns": [{"name": "k", "type": "int"}, {"name": "d", "type": "blob", "default": "x"}],
                    "primary_key": ["k"]}),
                false,
            ),
        ];
        for (i, (mut def, ok)) in cases.into_iter().enumerate() {
            def["name"] = json!(format!("t{i}"));
            let r = create_table(&mut begin(&db).await, &table_def(def.clone())).await;
            assert_eq!(r.is_ok(), ok, "{def}: {:?}", r.err());
        }
    });
}

#[test]
fn nullability_and_defaults() {
    rt().block_on(async {
        let (eng, _store, _env) = engine(16, EngineConfig::small_for_tests());
        let db = eng.db("nul");
        db.open(true).await.unwrap();
        let blobs = BlobResolver { db: &db };
        let mut t = begin(&db).await;
        let def = table_def(json!({
            "name": "n",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "req", "type": "string", "nullable": false},
                {"name": "score", "type": "int", "nullable": false, "default": 7}
            ],
            "primary_key": ["id"]
        }));
        create_table(&mut t, &def).await.unwrap();
        insert(&mut t, &blobs, "n", &json!({"id": 1, "req": "x"}), InsertMode::Insert).await.unwrap();
        assert_eq!(get(&mut t, "n", &json!(1)).await.unwrap().unwrap()["score"], json!(7));
        for row in [json!({"id": 2, "req": null}), json!({"id": null, "req": "x"}), json!({"req": "x"})] {
            let r = insert(&mut t, &blobs, "n", &row, InsertMode::Insert).await;
            assert!(matches!(r, Err(Error::InvalidArgument(_))), "{row} accepted");
        }
    });
}

#[test]
fn columns_come_and_go() {
    rt().block_on(async {
        let (eng, _store, _env) = engine(17, EngineConfig::small_for_tests());
        let db = eng.db("cols");
        db.open(true).await.unwrap();
        let blobs = BlobResolver { db: &db };
        let mut t = begin(&db).await;
        let def = table_def(json!({
            "name": "c",
            "columns": [{"name": "id", "type": "int"}, {"name": "a", "type": "int"}, {"name": "x", "type": "int"}],
            "primary_key": ["id"],
            "indexes": [{"name": "by_x", "columns": ["x"]}]
        }));
        create_table(&mut t, &def).await.unwrap();
        insert(&mut t, &blobs, "c", &json!({"id": 1, "a": 10, "x": 1}), InsertMode::Insert).await.unwrap();
        assert!(
            add_column(&mut t, "c", &column(json!({"name": "m", "type": "int", "nullable": false}))).await.is_err()
        );
        add_column(&mut t, "c", &column(json!({"name": "p", "type": "int"}))).await.unwrap();
        add_column(&mut t, "c", &column(json!({"name": "q", "type": "string", "default": "d"}))).await.unwrap();
        update(&mut t, &blobs, "c", &json!(1), &json!({"p": 20})).await.unwrap();
        drop_column(&mut t, "c", "a").await.unwrap();
        add_column(&mut t, "c", &column(json!({"name": "r", "type": "int"}))).await.unwrap();
        let row = get(&mut t, "c", &json!(1)).await.unwrap().unwrap();
        assert_eq!(row, json!({"id": 1, "x": 1, "p": 20, "q": "d", "r": null}));

        update(&mut t, &blobs, "c", &json!(1), &json!({"r": 30})).await.unwrap();
        let s = get_schema(&mut t, "c").await.unwrap();
        let stored = t.get(&row_key(s.id, &[Key::Int(1)])).await.unwrap().unwrap();
        let Value::Inline(data) = stored.value else { panic!("row stored out of line") };
        let ids: Vec<u32> = schema::decode_row(&data).unwrap().keys().copied().collect();
        assert!(ids.iter().all(|id| s.columns.iter().any(|c| c.id == *id)), "stored dropped column values: {ids:?}");
    });
}

#[test]
fn dropping_blob_columns() {
    rt().block_on(async {
        let (eng, _store, _env) = engine(18, EngineConfig::small_for_tests());
        let db = eng.db("blobcol");
        db.open(true).await.unwrap();
        let blobs = BlobResolver { db: &db };
        let mut t = begin(&db).await;
        let def = table_def(json!({
            "name": "d",
            "columns": [{"name": "id", "type": "int"}, {"name": "x", "type": "blob"}, {"name": "y", "type": "blob"}],
            "primary_key": ["id"]
        }));
        create_table(&mut t, &def).await.unwrap();
        let b = crate::engine::blob::write_blob(db.shared(), "blobcol", b"payload", "text/plain").await.unwrap();
        for id in 0..1001 {
            let row = json!({"id": id, "x": b.id, "y": if id == 0 { json!(b.id) } else { Json::Null }});
            insert(&mut t, &blobs, "d", &row, InsertMode::Insert).await.unwrap();
        }
        db.commit(t.into_request()).await.unwrap();
        let mut t = begin(&db).await;
        check_no_orphans(&mut t).await.unwrap();
        let s = get_schema(&mut t, "d").await.unwrap();
        let (x, y) = (s.column("x").unwrap().id, s.column("y").unwrap().id);

        let mut c = begin(&db).await;
        c.delete(row_key(s.id, &[Key::Int(5)])).unwrap();
        assert!(check_no_orphans(&mut c).await.is_err(), "blob of a deleted row not detected");
        let mut c = begin(&db).await;
        let mut stale = s.clone();
        stale.columns.retain(|c| c.id != y);
        put_schema(&mut c, &stale).unwrap();
        assert!(check_no_orphans(&mut c).await.is_err(), "blob of a dropped column not detected");
        let mut c = begin(&db).await;
        c.put(blob_side_key(s.id + 7, &[Key::Int(1)], x), Value::Blob(b.clone())).unwrap();
        assert!(check_no_orphans(&mut c).await.is_err(), "blob of a dropped table not detected");

        drop_column(&mut t, "d", "x").await.unwrap();
        check_no_orphans(&mut t).await.unwrap();
        assert_eq!(get(&mut t, "d", &json!(0)).await.unwrap().unwrap()["y"]["size"], json!(7));
    });
}

async fn put_rows(t: &mut Txn, table: &str, ids: std::ops::Range<i64>) {
    let s = get_schema(t, table).await.unwrap();
    let (id, g) = (s.column("id").unwrap().id, s.column("g").unwrap().id);
    for i in ids {
        let row: schema::Row = [(id, Datum::Int(i)), (g, Datum::Int(i % 2))].into();
        t.put(row_key(s.id, &[Key::Int(i)]), Value::Inline(Bytes::from(schema::encode_row(&row)))).unwrap();
    }
}

fn rg_table() -> CreateTable {
    table_def(json!({
        "name": "r",
        "columns": [{"name": "id", "type": "int"}, {"name": "g", "type": "int"}],
        "primary_key": ["id"]
    }))
}

#[test]
fn queries_examine_a_bounded_number_of_rows() {
    rt().block_on(async {
        let (eng, _store, _env) = engine(21, EngineConfig::small_for_tests());
        let db = eng.db("scan");
        db.open(true).await.unwrap();
        let mut t = begin(&db).await;
        create_table(&mut t, &rg_table()).await.unwrap();
        put_rows(&mut t, "r", 0..MAX_EXAMINED_PER_QUERY as i64 + 1).await;
        let none = json!({"col": "g", "op": "eq", "value": 5});
        let mut q: Query = serde_json::from_value(json!({"filter": none})).unwrap();
        let r = query(&mut t, "r", &q).await.unwrap();
        assert!(r.rows.is_empty());
        q.cursor = Some(r.next_cursor.expect("a query that hit the limit returns a cursor"));
        let r = query(&mut t, "r", &q).await.unwrap();
        assert!(r.rows.is_empty() && r.next_cursor.is_none());
    });
}

#[test]
fn index_backfill_reads_every_row() {
    rt().block_on(async {
        let (eng, _store, _env) = engine(19, EngineConfig::small_for_tests());
        let db = eng.db("fill");
        db.open(true).await.unwrap();
        let mut t = begin(&db).await;
        create_table(&mut t, &rg_table()).await.unwrap();
        put_rows(&mut t, "r", 0..1001).await;
        db.commit(t.into_request()).await.unwrap();
        let byte_batches = |entry: &dyn Fn(i64) -> usize| {
            let (mut batches, mut bytes) = (1, 0);
            for i in 0..1001 {
                bytes += entry(i);
                if bytes >= 4096 {
                    batches += 1;
                    bytes = 0;
                }
            }
            1 + batches
        };
        let int = |i: i64| tuple::encode(&[Key::Int(i)]).len();
        // 9-byte index prefix plus encoded key and primary key tuples.
        let plain = byte_batches(&|i| 9 + int(i % 2) + int(i));
        let unique = byte_batches(&|i| 9 + int(i) + int(i));
        for (name, columns, is_unique, opts, commits) in [
            ("by_rows", "g", false, Backfill { batch_rows: 300, ..Backfill::default() }, 1 + 4),
            ("by_bytes", "g", false, Backfill { batch_rows: 1000, batch_bytes: 4096 }, plain),
            ("unique_by_bytes", "id", true, Backfill { batch_rows: 1000, batch_bytes: 4096 }, unique),
        ] {
            let before = db.current().unwrap().version();
            let spec = index_spec(json!({"name": name, "columns": [columns], "unique": is_unique}));
            let (s, v) = add_index(&db, "r", &spec, opts).await.unwrap();
            assert_eq!(s.index(name).unwrap().state, IndexState::Ready);
            assert_eq!(v, db.current().unwrap().version());
            assert_eq!(v - before, commits as u64, "{name}: commits of the build");
            let mut t = begin(&db).await;
            check_indexes(&mut t, "r").await.unwrap();
            let mut q: Query = serde_json::from_value(json!({"index": name, "limit": 1000})).unwrap();
            let mut n = 0;
            loop {
                let r = query(&mut t, "r", &q).await.unwrap();
                n += r.rows.len();
                match r.next_cursor {
                    Some(c) => q.cursor = Some(c),
                    None => break,
                }
            }
            assert_eq!(n, 1001);
        }
        assert!(unique > byte_batches(&|_| 9) + 2, "setup: values do not count");
    });
}

fn people_table() -> CreateTable {
    table_def(json!({
        "name": "p",
        "columns": [
            {"name": "id", "type": "int", "nullable": false},
            {"name": "email", "type": "string"},
            {"name": "g", "type": "int"}
        ],
        "primary_key": ["id"]
    }))
}

async fn people(seed: u64, n: i64) -> Arc<Db> {
    let (eng, _store, _env) = engine(seed, EngineConfig::small_for_tests());
    let db = eng.db("people");
    db.open(true).await.unwrap();
    let mut t = begin(&db).await;
    create_table(&mut t, &people_table()).await.unwrap();
    let blobs = BlobResolver { db: &db };
    for id in 0..n {
        let row = json!({"id": id, "email": format!("e{id}"), "g": id % 3});
        insert(&mut t, &blobs, "p", &row, InsertMode::Insert).await.unwrap();
    }
    db.commit(t.into_request()).await.unwrap();
    db
}

async fn start_build(db: &Arc<Db>, spec: &IndexSpec) -> IndexDef {
    let mut t = begin(db).await;
    let idx = begin_index(&mut t, "p", spec).await.unwrap();
    db.commit(t.into_request()).await.unwrap();
    idx
}

async fn build_step(db: &Arc<Db>, idx: &IndexDef) -> Result<Option<TableSchema>> {
    let mut t = begin(db).await;
    let r = backfill_batch(&mut t, "p", idx, small()).await?;
    db.commit(t.into_request()).await?;
    Ok(r)
}

async fn check(db: &Arc<Db>) {
    let mut t = begin(db).await;
    check_indexes(&mut t, "p").await.unwrap();
    check_no_orphans(&mut t).await.unwrap();
}

#[test]
fn building_unique_index_keeps_entries_of_other_rows() {
    rt().block_on(async {
        for change in [json!(null), json!({"email": "z"}), json!({"g": 9})] {
            let db = people(37, 20).await;
            let blobs = BlobResolver { db: &db };
            let mut t = begin(&db).await;
            update(&mut t, &blobs, "p", &json!(15), &json!({"email": "e0"})).await.unwrap();
            db.commit(t.into_request()).await.unwrap();
            let idx = start_build(&db, &index_spec(json!({"name": "ix", "columns": ["email"], "unique": true}))).await;
            assert!(build_step(&db, &idx).await.unwrap().is_none());
            let mut t = begin(&db).await;
            match &change {
                Json::Null => delete(&mut t, "p", &json!(15)).await.map(|_| ()).unwrap(),
                set => update(&mut t, &blobs, "p", &json!(15), set).await.map(|_| ()).unwrap(),
            }
            db.commit(t.into_request()).await.unwrap();
            let mut done = None;
            while done.is_none() {
                done = match build_step(&db, &idx).await {
                    Err(Error::AlreadyExists(_)) => {
                        assert_eq!(change, json!({"g": 9}), "only a remaining duplicate fails the build");
                        break;
                    }
                    r => r.unwrap(),
                };
            }
            check(&db).await;
        }
    });
}

#[test]
fn backfill_races_row_writes() {
    rt().block_on(async {
        for unique in [false, true] {
            let db = people(31, 60).await;
            let blobs = BlobResolver { db: &db };
            let cols = if unique { json!(["email"]) } else { json!(["g", "email"]) };
            let idx = start_build(&db, &index_spec(json!({"name": "ix", "columns": cols, "unique": unique}))).await;
            check(&db).await;
            let mut r = Rng::new(5);
            let mut next_id = 1000;
            let mut steps = 0;
            loop {
                for _ in 0..4 {
                    let mut t = begin(&db).await;
                    let id = r.below(60) as i64;
                    let res = match r.below(4) {
                        0 => {
                            next_id += 1;
                            let row = json!({"id": next_id, "email": format!("n{next_id}"), "g": 1});
                            insert(&mut t, &blobs, "p", &row, InsertMode::Insert).await
                        }
                        1 => delete(&mut t, "p", &json!(id)).await.map(|_| ()),
                        _ => {
                            let set = json!({"g": r.below(5), "email": format!("u{}", r.below(1000))});
                            update(&mut t, &blobs, "p", &json!(id), &set).await.map(|_| ())
                        }
                    };
                    match res {
                        Ok(()) => {}
                        Err(Error::AlreadyExists(_)) if unique => continue,
                        Err(e) => panic!("row write failed: {e}"),
                    }
                    if r.chance(0.3) {
                        let done = build_step(&db, &idx).await.unwrap();
                        match db.commit(t.into_request()).await {
                            Ok(_) | Err(Error::Conflict(_)) => {}
                            Err(e) => panic!("commit failed: {e}"),
                        }
                        if done.is_some() {
                            break;
                        }
                    } else {
                        db.commit(t.into_request()).await.unwrap();
                    }
                }
                check(&db).await;
                steps += 1;
                let mut t = begin(&db).await;
                if get_schema(&mut t, "p").await.unwrap().index("ix").unwrap().state == IndexState::Ready {
                    break;
                }
                drop(t);
                if build_step(&db, &idx).await.unwrap().is_some() {
                    break;
                }
            }
            assert!(steps > 3, "the build took several batches");
            check(&db).await;
        }
    });
}

#[test]
fn abandoning_leaves_finished_indexes_alone() {
    rt().block_on(async {
        let (eng, _store, _env) = engine(23, EngineConfig::small_for_tests());
        let db = eng.db("ab");
        db.open(true).await.unwrap();
        let mut t = begin(&db).await;
        create_table(&mut t, &rg_table()).await.unwrap();
        put_rows(&mut t, "r", 0..10).await;
        db.commit(t.into_request()).await.unwrap();
        let opts = Backfill::default();
        let (s, _) = add_index(&db, "r", &index_spec(json!({"name": "done", "columns": ["g"]})), opts).await.unwrap();
        let done = s.index("done").unwrap().clone();
        let mut t = begin(&db).await;
        let building = begin_index(&mut t, "r", &index_spec(json!({"name": "busy", "columns": ["id"]}))).await.unwrap();
        db.commit(t.into_request()).await.unwrap();
        let mut t = begin(&db).await;
        abandon_index(&mut t, "r", &done).await.unwrap();
        db.commit(t.into_request()).await.unwrap();
        let mut t = begin(&db).await;
        let s = get_schema(&mut t, "r").await.unwrap();
        assert_eq!(s.index("done").map(|i| i.state), Some(IndexState::Ready), "a finished index was abandoned");
        assert_eq!(s.index("busy").map(|i| i.state), Some(IndexState::Building));
        check_indexes(&mut t, "r").await.unwrap();
        abandon_index(&mut t, "r", &building).await.unwrap();
        db.commit(t.into_request()).await.unwrap();
        let mut t = begin(&db).await;
        assert!(get_schema(&mut t, "r").await.unwrap().index("busy").is_none());
        check_indexes(&mut t, "r").await.unwrap();
        check_no_orphans(&mut t).await.unwrap();
    });
}

#[test]
fn unique_violations_abandon_the_build() {
    rt().block_on(async {
        let db = people(32, 40).await;
        let blobs = BlobResolver { db: &db };
        let mut t = begin(&db).await;
        insert(&mut t, &blobs, "p", &json!({"id": 100, "email": "e3"}), InsertMode::Insert).await.unwrap();
        db.commit(t.into_request()).await.unwrap();
        let spec = index_spec(json!({"name": "u", "columns": ["email"], "unique": true}));
        let e = add_index(&db, "p", &spec, small()).await;
        assert!(matches!(e, Err(Error::AlreadyExists(_))), "{e:?}");
        let mut t = begin(&db).await;
        assert!(get_schema(&mut t, "p").await.unwrap().index("u").is_none());
        drop(t);
        check(&db).await;

        let mut t = begin(&db).await;
        delete(&mut t, "p", &json!(100)).await.unwrap();
        db.commit(t.into_request()).await.unwrap();
        let idx = start_build(&db, &spec).await;
        build_step(&db, &idx).await.unwrap();
        let mut t = begin(&db).await;
        let e = insert(&mut t, &blobs, "p", &json!({"id": 101, "email": "e0"}), InsertMode::Insert).await;
        assert!(matches!(e, Err(Error::AlreadyExists(_))), "{e:?}");
        insert(&mut t, &blobs, "p", &json!({"id": 102, "email": "e39"}), InsertMode::Insert).await.unwrap();
        db.commit(t.into_request()).await.unwrap();
        check(&db).await;
        let e = add_index(&db, "p", &spec, small()).await;
        assert!(matches!(e, Err(Error::AlreadyExists(_))), "{e:?}");
        check(&db).await;
    });
}

#[test]
fn interrupted_builds_resume() {
    rt().block_on(async {
        let db = people(33, 50).await;
        let spec = index_spec(json!({"name": "by_g", "columns": ["g"]}));
        let idx = start_build(&db, &spec).await;
        assert!(build_step(&db, &idx).await.unwrap().is_none());
        assert!(build_step(&db, &idx).await.unwrap().is_none());
        check(&db).await;
        let other = index_spec(json!({"name": "by_g", "columns": ["email"]}));
        assert!(matches!(add_index(&db, "p", &other, small()).await, Err(Error::AlreadyExists(_))));
        let (s, _) = add_index(&db, "p", &spec, small()).await.unwrap();
        let i = s.index("by_g").unwrap();
        assert_eq!((i.id, i.state), (idx.id, IndexState::Ready), "the same index was finished");
        check(&db).await;
        assert_eq!(
            build_step(&db, &idx).await.unwrap().map(|s| s.index("by_g").unwrap().state),
            Some(IndexState::Ready)
        );
        assert!(matches!(add_index(&db, "p", &spec, small()).await, Err(Error::AlreadyExists(_))));
    });
}

#[test]
fn dropping_a_building_index_stops_its_build() {
    rt().block_on(async {
        let db = people(34, 30).await;
        let spec = index_spec(json!({"name": "by_g", "columns": ["g"]}));
        let idx = start_build(&db, &spec).await;
        build_step(&db, &idx).await.unwrap();
        let mut t = begin(&db).await;
        drop_index(&mut t, "p", "by_g").await.unwrap();
        db.commit(t.into_request()).await.unwrap();
        check(&db).await;
        assert!(matches!(build_step(&db, &idx).await, Err(Error::NotFound(_))));

        let idx = start_build(&db, &spec).await;
        build_step(&db, &idx).await.unwrap();
        let mut t = begin(&db).await;
        drop_table(&mut t, "p").await.unwrap();
        db.commit(t.into_request()).await.unwrap();
        let mut t = begin(&db).await;
        check_no_orphans(&mut t).await.unwrap();
        assert!(matches!(build_step(&db, &idx).await, Err(Error::NotFound(_))));
    });
}

#[test]
fn building_indexes_cannot_be_queried() {
    rt().block_on(async {
        let db = people(35, 20).await;
        let idx = start_build(&db, &index_spec(json!({"name": "by_g", "columns": ["g"]}))).await;
        build_step(&db, &idx).await.unwrap();
        let q: Query = serde_json::from_value(json!({"index": "by_g", "eq": [1]})).unwrap();
        let mut t = begin(&db).await;
        let s = get_schema(&mut t, "p").await.unwrap();
        assert_eq!(s.to_json()["indexes"][0]["state"], json!("building"));
        assert!(matches!(query(&mut t, "p", &q).await, Err(Error::Unavailable { .. })));
        drop(t);
        while build_step(&db, &idx).await.unwrap().is_none() {}
        let mut t = begin(&db).await;
        assert_eq!(query(&mut t, "p", &q).await.unwrap().rows.len(), 7);
    });
}

#[test]
fn building_index_checker_rejects_corruption() {
    rt().block_on(async {
        let db = people(36, 30).await;
        let idx = start_build(&db, &index_spec(json!({"name": "by_g", "columns": ["g"]}))).await;
        build_step(&db, &idx).await.unwrap();
        check(&db).await;
        let mut t = begin(&db).await;
        let s = get_schema(&mut t, "p").await.unwrap();
        let entries = t.scan(&KeyRange::prefix(&index_prefix(s.id, idx.id)), false, 100).await.unwrap();
        assert_eq!(entries.len(), 7);
        t.delete(entries[3].key.clone()).unwrap();
        assert!(check_indexes(&mut t, "p").await.is_err(), "missing backfilled entry not detected");
        let mut t = begin(&db).await;
        let mut k = index_prefix(s.id, idx.id);
        k.extend_from_slice(&tuple::encode(&[Key::Int(9), Key::Int(9)]));
        t.put(Bytes::from(k), Value::Inline(Bytes::new())).unwrap();
        assert!(check_indexes(&mut t, "p").await.is_err(), "stale entry not detected");
        let mut t = begin(&db).await;
        t.delete(backfill_key(s.id, idx.id)).unwrap();
        assert!(check_indexes(&mut t, "p").await.is_err(), "missing cursor not detected");
    });
}

fn members_table() -> CreateTable {
    table_def(json!({
        "name": "people",
        "columns": [
            {"name": "id", "type": "int", "nullable": false},
            {"name": "email", "type": "string"},
            {"name": "age", "type": "int"}
        ],
        "primary_key": ["id"],
        "indexes": [{"name": "by_email", "columns": ["email"], "unique": true}, {"name": "by_age", "columns": ["age"]}]
    }))
}

#[test]
fn rows_expire_with_their_index_entries() {
    rt().block_on(async {
        let (eng, _store, _env) = engine(3, EngineConfig::small_for_tests());
        let db = eng.db("ttl");
        db.open(true).await.unwrap();
        eng.shared.store_time_ms().await.unwrap();
        let blobs = BlobResolver { db: &db };
        let ttl = |ms| Some(crate::engine::Ttl::For(ms));
        let mut t = begin(&db).await;
        create_table(&mut t, &members_table()).await.unwrap();
        insert(&mut t, &blobs, "people", &json!({"id": 1, "email": "old@x", "age": 1}), InsertMode::Insert)
            .await
            .unwrap();
        db.commit(t.into_request()).await.unwrap();
        let mut t = begin(&db).await;
        let up = InsertMode::Upsert;
        insert_expiring(&mut t, &blobs, "people", &json!({"id": 1, "email": "a@x", "age": 2}), up, ttl(10_000))
            .await
            .unwrap();
        insert_expiring(&mut t, &blobs, "people", &json!({"id": 2, "email": "b@x", "age": 3}), up, ttl(10_000))
            .await
            .unwrap();
        assert!(update(&mut t, &blobs, "people", &json!(2), &json!({"age": 4})).await.unwrap());
        let c = db.commit_detailed(t.into_request()).await.unwrap();
        let expires = c.time_ms + 10_000;
        let mut t = begin(&db).await;
        assert_eq!(
            get_expiring(&mut t, "people", &json!(2)).await.unwrap().map(|(r, e)| (r["age"].clone(), e)),
            Some((json!(4), Some(expires)))
        );
        check_indexes(&mut t, "people").await.unwrap();
        let mut t = begin(&db).await;
        assert!(update(&mut t, &blobs, "people", &json!(1), &json!({"age": 5})).await.unwrap());
        assert!(
            update_expiring(&mut t, &blobs, "people", &json!(2), &json!({}), Expiry::Set(ttl(60_000))).await.unwrap()
        );
        db.commit(t.into_request()).await.unwrap();
        let mut t = begin(&db).await;
        assert_eq!(get_expiring(&mut t, "people", &json!(1)).await.unwrap().and_then(|(_, e)| e), Some(expires));
        let dup =
            insert(&mut t, &blobs, "people", &json!({"id": 3, "email": "a@x", "age": 1}), InsertMode::Insert).await;
        assert!(matches!(dup, Err(Error::AlreadyExists(_))), "the unique value is taken until the row expires");
        tokio::time::sleep(std::time::Duration::from_millis(10_001)).await;
        let mut t = begin(&db).await;
        assert_eq!(get(&mut t, "people", &json!(1)).await.unwrap(), None, "expired, and the older row stays gone");
        assert!(get(&mut t, "people", &json!(2)).await.unwrap().is_some());
        let q: Query = serde_json::from_value(json!({"index": "by_email"})).unwrap();
        let rows = query(&mut t, "people", &q).await.unwrap().rows;
        assert_eq!(rows.iter().map(|r| r["id"].clone()).collect::<Vec<_>>(), vec![json!(2)]);
        check_indexes(&mut t, "people").await.unwrap();
        insert(&mut t, &blobs, "people", &json!({"id": 3, "email": "a@x", "age": 1}), InsertMode::Insert)
            .await
            .unwrap();
        let all = Selection::default();
        assert_eq!(
            update_where(&mut t, &blobs, "people", &all, &json!({"age": 9}), Expiry::Keep, None).await.unwrap(),
            2
        );
        db.commit(t.into_request()).await.unwrap();
        let mut t = begin(&db).await;
        check_indexes(&mut t, "people").await.unwrap();
        assert_eq!(delete_where(&mut t, "people", &all, None).await.unwrap(), 2);
        db.commit(t.into_request()).await.unwrap();
        let mut t = begin(&db).await;
        check_indexes(&mut t, "people").await.unwrap();
        assert!(query(&mut t, "people", &q).await.unwrap().rows.is_empty());
    });
}

#[test]
fn batch_row_operations() {
    rt().block_on(async {
        let (eng, _store, _env) =
            engine(4, EngineConfig { txn_max_write_bytes: 1_000, ..EngineConfig::small_for_tests() });
        let db = eng.db("batch");
        db.open(true).await.unwrap();
        let blobs = BlobResolver { db: &db };
        let mut t = begin(&db).await;
        create_table(&mut t, &members_table()).await.unwrap();
        db.commit(t.into_request()).await.unwrap();
        for ids in [0..5, 5..10, 10..15, 15..20] {
            let mut t = begin(&db).await;
            for id in ids {
                let row = json!({"id": id, "email": format!("e{id}"), "age": id % 4});
                insert(&mut t, &blobs, "people", &row, InsertMode::Insert).await.unwrap();
            }
            db.commit(t.into_request()).await.unwrap();
        }
        let sel = |v: Json| -> Selection { serde_json::from_value(v).unwrap() };
        let mut t = begin(&db).await;
        for (set, why) in [
            (json!({"id": 1}), "the primary key"),
            (json!({"nope": 1}), "an unknown column"),
            (json!({"age": "x"}), "a value of the wrong type"),
            (json!([1]), "not an object"),
        ] {
            let r = update_where(&mut t, &blobs, "people", &sel(json!({"eq": [-1]})), &set, Expiry::Keep, None).await;
            assert!(matches!(r, Err(Error::InvalidArgument(_))), "{why}: {r:?}");
        }
        let r = update_where(&mut t, &blobs, "people", &sel(json!({"index": "nope"})), &json!({}), Expiry::Keep, None)
            .await;
        assert!(matches!(r, Err(Error::NotFound(_))), "{r:?}");
        let by_age = sel(json!({"index": "by_age", "range": {"gte": 1}}));
        let n =
            update_where(&mut t, &blobs, "people", &by_age, &json!({"age": 7}), Expiry::Keep, Some(6)).await.unwrap();
        assert_eq!(n, 6);
        db.commit(t.into_request()).await.unwrap();
        let mut t = begin(&db).await;
        assert_eq!(
            update_where(&mut t, &blobs, "people", &by_age, &json!({"age": 8}), Expiry::Keep, Some(3)).await.unwrap(),
            3
        );
        db.commit(t.into_request()).await.unwrap();
        let mut t = begin(&db).await;
        let ages: Vec<i64> = query(&mut t, "people", &serde_json::from_value(json!({"limit": 100})).unwrap())
            .await
            .unwrap()
            .rows
            .iter()
            .map(|r| r["age"].as_i64().unwrap())
            .collect();
        assert_eq!(ages.iter().filter(|a| **a == 7).count(), 6, "{ages:?}");
        assert_eq!(ages.iter().filter(|a| **a == 8).count(), 3, "{ages:?}");
        let dup = update_where(
            &mut t,
            &blobs,
            "people",
            &sel(json!({"range": {"lt": 2}})),
            &json!({"email": "same"}),
            Expiry::Keep,
            None,
        )
        .await;
        assert!(matches!(dup, Err(Error::AlreadyExists(_))), "{dup:?}");
        let mut t = begin(&db).await;
        let big =
            update_where(&mut t, &blobs, "people", &Selection::default(), &json!({"age": 1}), Expiry::Keep, None).await;
        assert!(matches!(big, Err(Error::TooLarge(_))), "{big:?}");
        let mut t = begin(&db).await;
        assert_eq!(
            delete_where(&mut t, "people", &sel(json!({"filter": {"col": "age", "op": "eq", "value": 0}})), None)
                .await
                .unwrap(),
            5
        );
        db.commit(t.into_request()).await.unwrap();
        let mut t = begin(&db).await;
        check_indexes(&mut t, "people").await.unwrap();
        assert!(get(&mut t, "people", &json!(4)).await.unwrap().is_none());
        assert!(get(&mut t, "people", &json!(5)).await.unwrap().is_some());
    });
}

#[test]
fn row_changes_follow_writes() {
    rt().block_on(async {
        let (eng, _store, _env) = engine(5, EngineConfig::small_for_tests());
        let db = eng.db("feed");
        db.open(true).await.unwrap();
        let blobs = BlobResolver { db: &db };
        let mut t = begin(&db).await;
        create_table(&mut t, &members_table()).await.unwrap();
        let start = db.commit(t.into_request()).await.unwrap();
        let mut t = begin(&db).await;
        for id in 0..3 {
            insert(
                &mut t,
                &blobs,
                "people",
                &json!({"id": id, "email": format!("e{id}"), "age": 1}),
                InsertMode::Insert,
            )
            .await
            .unwrap();
        }
        let first = db.commit(t.into_request()).await.unwrap();
        let mut t = begin(&db).await;
        delete(&mut t, "people", &json!(1)).await.unwrap();
        update(&mut t, &blobs, "people", &json!(2), &json!({"age": 5})).await.unwrap();
        let second = db.commit(t.into_request()).await.unwrap();
        let mut t = begin(&db).await;
        let mut got = Vec::new();
        let mut after = ChangePos::after_version(start);
        loop {
            let page = row_changes(&mut t, "people", &after, 2).await.unwrap();
            got.extend(page.changes);
            after = page.next;
            if !page.more {
                break;
            }
        }
        let summary: Vec<(u64, Json, bool)> =
            got.iter().map(|c| (c["version"].as_u64().unwrap(), c["pk"].clone(), c["deleted"] == true)).collect();
        assert_eq!(
            summary,
            vec![(first, json!([0]), false), (second, json!([1]), true), (second, json!([2]), false)],
            "{got:?}"
        );
        assert_eq!(got[2]["row"]["age"], json!(5));
        assert_eq!(after, ChangePos::after_version(second));
        let page = row_changes(&mut t, "people", &after, 10).await.unwrap();
        assert!(page.changes.is_empty() && !page.more && page.next == after);
    });
}
