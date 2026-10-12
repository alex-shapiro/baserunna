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

/// The consistency checkers used by the simulator must reject corrupted
/// state, or a bug they exist to catch would go unnoticed.
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

        // A missing index entry.
        let mut t = fresh().await;
        t.delete(entries[0].key.clone()).unwrap();
        assert!(check_indexes(&mut t, "t").await.is_err(), "missing index entry not detected");

        // A stale index entry.
        let mut t = fresh().await;
        let mut stale = entries[0].key.to_vec();
        stale.push(0);
        t.put(Bytes::from(stale), Value::Inline(Bytes::new())).unwrap();
        assert!(check_indexes(&mut t, "t").await.is_err(), "stale index entry not detected");

        // Two rows with the same unique email (the second copied raw).
        let mut t = fresh().await;
        let r0 = t.get(&row_key(s.id, &[Key::Int(0)])).await.unwrap().unwrap();
        t.put(row_key(s.id, &[Key::Int(9)]), r0.value).unwrap();
        assert!(check_indexes(&mut t, "t").await.is_err(), "unique violation not detected");

        // Leftovers of a dropped table and of a dropped index.
        let mut t = fresh().await;
        t.put(row_key(s.id + 7, &[Key::Int(1)]), Value::Inline(Bytes::new())).unwrap();
        assert!(check_no_orphans(&mut t).await.is_err(), "row of a dropped table not detected");
        let mut t = fresh().await;
        let mut k = index_prefix(s.id, by_n + 7);
        k.push(1);
        t.put(Bytes::from(k), Value::Inline(Bytes::new())).unwrap();
        assert!(check_no_orphans(&mut t).await.is_err(), "entry of a dropped index not detected");
    });
}

async fn begin(db: &Arc<Db>) -> Txn {
    db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap()
}

fn table_def(v: Json) -> CreateTable {
    serde_json::from_value(v).unwrap()
}

fn column(v: Json) -> ColumnSpec {
    serde_json::from_value(v).unwrap()
}

/// A filter as the oracle sees it.
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
            F::Cmp(col, op, value) => json!({"col": col, "op": op, "value": value}),
        }
    }
}

/// A row of the filter test table: one column per filterable type.
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

/// An operand for `col`, always of an accepted type (ints for floats too).
fn gen_operand(r: &mut Rng, col: &str) -> Json {
    match col {
        "i" => json!(r.below(5) as i64 - 2),
        "f" if r.chance(0.3) => json!(r.below(3) as i64 - 1),
        "f" => json!(F_FLOATS[r.usize(F_FLOATS.len())]),
        "s" => json!(F_STRS[r.usize(F_STRS.len())]),
        "b" => json!(b64::encode(F_BYTES[r.usize(F_BYTES.len())])),
        "bo" => json!(r.chance(0.5)),
        _ => f_json(r),
    }
}

fn gen_filter(r: &mut Rng, depth: u32) -> F {
    match r.below(if depth > 2 { 1 } else { 6 }) {
        0..=2 => {
            let col = ["i", "f", "s", "b", "bo", "j"][r.usize(6)];
            let ops: &[&'static str] = match col {
                "s" | "b" => &["eq", "ne", "lt", "le", "gt", "ge", "in", "prefix", "is_null"],
                _ => &["eq", "ne", "lt", "le", "gt", "ge", "in", "is_null"],
            };
            let op = ops[r.usize(ops.len())];
            let value = match op {
                "in" => Json::Array((0..r.usize(4)).map(|_| gen_operand(r, col)).collect()),
                "is_null" => Json::Null,
                _ => gen_operand(r, col),
            };
            F::Cmp(col, op, value)
        }
        3 => F::And((0..r.usize(3)).map(|_| gen_filter(r, depth + 1)).collect()),
        4 => F::Or((0..r.usize(3)).map(|_| gen_filter(r, depth + 1)).collect()),
        _ => F::Not(Box::new(gen_filter(r, depth + 1))),
    }
}

/// Filter semantics, independent of the service: values of the column's
/// type order naturally (numbers numerically, strings and bytes bytewise,
/// false before true); JSON values are only equal or incomparable; null
/// matches nothing but `is_null`.
fn oracle(f: &F, row: &FRow) -> bool {
    use std::cmp::Ordering::{self, *};
    let cmp = |col: &str, v: &Json| -> Option<Ordering> {
        match col {
            "i" => Some(row.i?.cmp(&v.as_i64()?)),
            "f" => row.f?.partial_cmp(&v.as_f64()?),
            "s" => Some(row.s.as_ref()?.as_bytes().cmp(v.as_str()?.as_bytes())),
            "b" => Some(row.b.as_deref()?.cmp(b64::decode(v.as_str()?)?.as_slice())),
            "bo" => Some(row.bo?.cmp(&v.as_bool()?)),
            _ => (row.j.as_ref()? == v).then_some(Equal),
        }
    };
    match f {
        F::And(v) => v.iter().all(|x| oracle(x, row)),
        F::Or(v) => v.iter().any(|x| oracle(x, row)),
        F::Not(x) => !oracle(x, row),
        F::Cmp(col, op, v) => match *op {
            "is_null" => match *col {
                "i" => row.i.is_none(),
                "f" => row.f.is_none(),
                "s" => row.s.is_none(),
                "b" => row.b.is_none(),
                "bo" => row.bo.is_none(),
                _ => row.j.is_none(),
            },
            "in" => v.as_array().unwrap().iter().any(|x| cmp(col, x) == Some(Equal)),
            "prefix" if *col == "s" => row.s.as_ref().is_some_and(|s| s.starts_with(v.as_str().unwrap())),
            "prefix" => row.b.as_ref().is_some_and(|b| b.starts_with(&b64::decode(v.as_str().unwrap()).unwrap())),
            "eq" => cmp(col, v) == Some(Equal),
            "ne" => cmp(col, v).is_some_and(|o| o != Equal),
            "lt" => cmp(col, v) == Some(Less),
            "le" => matches!(cmp(col, v), Some(Less | Equal)),
            "gt" => cmp(col, v) == Some(Greater),
            _ => matches!(cmp(col, v), Some(Greater | Equal)),
        },
    }
}

/// Query filters over every filterable column type agree with the oracle.
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

/// Filters are validated before any row is read.
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
            (json!({"col": "body", "op": "is_null"}), true),
        ];
        for (filter, ok) in cases {
            let q: Query = serde_json::from_value(json!({"filter": filter})).unwrap();
            match query(&mut t, "v", &q).await {
                Ok(_) => assert!(ok, "{filter} accepted"),
                Err(Error::InvalidArgument(e)) => assert!(!ok, "{filter} rejected: {e}"),
                Err(e) => panic!("{filter}: {e}"),
            }
        }
    });
}

/// Schemas are bounded: 1..=256 columns, 1..=8 key columns and 1..=8
/// columns per index; defaults must fit their column.
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

/// Key columns are never nullable, other columns only when declared so;
/// omitted columns take their defaults.
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

/// Column ids are never reused: values of a dropped column do not reappear
/// in a column added later, and updates stop storing them.
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
        // Only `x` is indexed, so `a` can be dropped.
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

/// Dropping a blob column removes its values from every row, past the
/// first page of them; the orphan checker notices any it leaves behind.
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

        // Leftover values of a dropped column, of a deleted row and of a
        // dropped table are orphans.
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

/// Write `n` rows `{id, g: id % 2}` directly, without per-row checks.
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

/// Reading a whole table refuses tables over the limit (index backfills
/// pass `MAX_BACKFILL_ROWS`).
#[test]
fn whole_table_reads_are_bounded() {
    rt().block_on(async {
        let (eng, _store, _env) = engine(20, EngineConfig::small_for_tests());
        let db = eng.db("big");
        db.open(true).await.unwrap();
        let mut t = begin(&db).await;
        create_table(&mut t, &rg_table()).await.unwrap();
        put_rows(&mut t, "r", 0..3).await;
        let s = get_schema(&mut t, "r").await.unwrap();
        assert_eq!(all_rows(&mut t, &s, 3).await.unwrap().len(), 3);
        assert!(matches!(all_rows(&mut t, &s, 2).await, Err(Error::TooLarge(_))));
    });
}

/// A query reads at most `MAX_EXAMINED_PER_QUERY` entries, then returns a
/// cursor to continue from, even with no matching rows yet.
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

/// Index backfill reads every row, past the first page of them.
#[test]
fn index_backfill_reads_every_row() {
    rt().block_on(async {
        let (eng, _store, _env) = engine(19, EngineConfig::small_for_tests());
        let db = eng.db("fill");
        db.open(true).await.unwrap();
        let blobs = BlobResolver { db: &db };
        let mut t = begin(&db).await;
        let def = table_def(json!({
            "name": "r",
            "columns": [{"name": "id", "type": "int"}, {"name": "g", "type": "int"}],
            "primary_key": ["id"]
        }));
        create_table(&mut t, &def).await.unwrap();
        for id in 0..1001 {
            insert(&mut t, &blobs, "r", &json!({"id": id, "g": id % 2}), InsertMode::Insert).await.unwrap();
        }
        let spec: IndexSpec = serde_json::from_value(json!({"name": "by_g", "columns": ["g"]})).unwrap();
        add_index(&mut t, "r", &spec).await.unwrap();
        let mut q: Query = serde_json::from_value(json!({"index": "by_g", "limit": 1000})).unwrap();
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
    });
}
