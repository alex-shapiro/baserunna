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
                        "filter": {"or": [{"col": "tag", "op": "eq", "value": "red"}, {"col": "name", "op": "is_null"}]}
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
        // Add a column with a default, then index it (backfill uses default).
        add_column(&mut t, "docs", &serde_json::from_value(json!({"name": "rank", "type": "int", "nullable": false, "default": 5})).unwrap()).await.unwrap();
        add_index(&mut t, "docs", &serde_json::from_value(json!({"name": "by_rank", "columns": ["rank", "title"]})).unwrap()).await.unwrap();
        check_indexes(&mut t, "docs").await.unwrap();
        db.commit(t.into_request()).await.unwrap();

        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let q: Query = serde_json::from_value(json!({"index": "by_rank", "eq": [5, "t1"]})).unwrap();
        assert_eq!(query(&mut t, "docs", &q).await.unwrap().rows.len(), 2);
        // Unique index over duplicate data fails.
        let e = add_index(&mut t, "docs", &serde_json::from_value(json!({"name": "u", "columns": ["title"], "unique": true})).unwrap()).await;
        assert!(matches!(e, Err(Error::AlreadyExists(_))));
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
        // Only the table-id counter remains.
        assert_eq!(v.all_entries(false).await.unwrap().len(), 1);
    });
}
