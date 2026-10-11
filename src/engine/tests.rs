//! Engine model tests: random transactions against a `BTreeMap` model, with
//! flushes, compactions, GC, snapshots and conflicts.

use super::*;
use crate::env::Env;
use crate::store::{MemStore, ObjectStore};
use crate::util::prop;
use crate::util::rng::Rng;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap()
}

pub(crate) fn engine(seed: u64, cfg: EngineConfig) -> (Arc<Engine>, Arc<MemStore>, Arc<Env>) {
    let origin = tokio::time::Instant::now();
    let env = Arc::new(Env::sim(seed, 1_700_000_000_000, origin, 0));
    let env2 = env.clone();
    let store = Arc::new(MemStore::new(Box::new(move || env2.now_ms())));
    let e = Engine::new(store.clone(), env.clone(), cfg, "node-a".into());
    (e, store, env)
}

#[derive(Clone, Debug)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    DeleteRange(Vec<u8>, Vec<u8>),
    Commit,
    Compact,
    Advance(u64),
    Sweep,
}

fn gen_ops(r: &mut Rng) -> Vec<Op> {
    let n = 50 + r.usize(250);
    (0..n)
        .map(|_| match r.below(100) {
            0..=44 => {
                let len = if r.chance(0.1) { 600 + r.usize(2000) } else { r.usize(60) };
                Op::Put(prop::small_key(r, 4), r.bytes(len))
            }
            45..=59 => Op::Delete(prop::small_key(r, 4)),
            60..=63 => {
                let a = prop::small_key(r, 3);
                let b = prop::small_key(r, 3);
                if a <= b { Op::DeleteRange(a, b) } else { Op::DeleteRange(b, a) }
            }
            64..=89 => Op::Commit,
            90..=94 => Op::Compact,
            95..=97 => Op::Advance(r.range(1, 3_000_000)),
            _ => Op::Sweep,
        })
        .collect()
}

async fn value_bytes(db: &Db, v: &Value) -> Vec<u8> {
    match v {
        Value::Inline(b) => b.to_vec(),
        Value::Blob(r) => blob::read_range(db.shared(), &db.id, r, 0..r.size).await.unwrap().to_vec(),
        Value::Tombstone => panic!("tombstone"),
    }
}

async fn check_against_model(db: &Arc<Db>, model: &BTreeMap<Vec<u8>, Vec<u8>>, r: &mut Rng) -> Result<(), String> {
    let v = db.read_version(Consistency::Strong).await.map_err(|e| e.to_string())?;
    let all = v.scan(&KeyRange::all(), false, usize::MAX, false).await.map_err(|e| e.to_string())?;
    let mut got = BTreeMap::new();
    for e in &all {
        got.insert(e.key.to_vec(), value_bytes(db, &e.value).await);
    }
    if &got != model {
        return Err(format!("full scan mismatch: got {} keys, want {}", got.len(), model.len()));
    }
    for _ in 0..10 {
        let k = prop::small_key(r, 4);
        let g = v.get(&k).await.map_err(|e| e.to_string())?;
        let gv = match g {
            Some(e) => Some(value_bytes(db, &e.value).await),
            None => None,
        };
        if gv.as_ref() != model.get(&k) {
            return Err(format!("point get mismatch for {k:?}"));
        }
        let a = prop::small_key(r, 3);
        let reverse = r.chance(0.5);
        let limit = 1 + r.usize(8);
        let range = KeyRange::half_open(Bytes::from(a.clone()), None);
        let got: Vec<Vec<u8>> = v
            .scan(&range, reverse, limit, false)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|e| e.key.to_vec())
            .collect();
        let want: Vec<Vec<u8>> = if reverse {
            model.range(a.clone()..).rev().take(limit).map(|(k, _)| k.clone()).collect()
        } else {
            model.range(a.clone()..).take(limit).map(|(k, _)| k.clone()).collect()
        };
        if got != want {
            return Err(format!("range scan mismatch from {a:?} rev={reverse} limit={limit}: {got:?} vs {want:?}"));
        }
    }
    Ok(())
}

async fn run_ops(seed: u64, ops: &[Op]) -> Result<(), String> {
    let (eng, store, env) = engine(seed, EngineConfig::small_for_tests());
    let db = eng.db("t1");
    db.open(true).await.map_err(|e| e.to_string())?;
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut pending: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
    let mut pending_ranges: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut txn = db.begin(Isolation::Serializable, Consistency::Strong).await.map_err(|e| e.to_string())?;
    let mut rng = Rng::new(seed ^ 0xabc);
    for op in ops {
        match op {
            Op::Put(k, v) => {
                let value = if v.len() > eng.cfg().value_inline_max {
                    Value::Blob(
                        blob::write_blob(&eng.shared, "t1", v, "application/octet-stream")
                            .await
                            .map_err(|e| e.to_string())?,
                    )
                } else {
                    Value::Inline(Bytes::from(v.clone()))
                };
                txn.put(Bytes::from(k.clone()), value).map_err(|e| e.to_string())?;
                pending.insert(k.clone(), Some(v.clone()));
            }
            Op::Delete(k) => {
                txn.delete(Bytes::from(k.clone())).map_err(|e| e.to_string())?;
                pending.insert(k.clone(), None);
            }
            Op::DeleteRange(a, b) => {
                txn.delete_range(KeyRange::half_open(Bytes::from(a.clone()), Some(Bytes::from(b.clone()))))
                    .map_err(|e| e.to_string())?;
                pending.retain(|k, _| !(k >= a && k < b));
                pending_ranges.push((a.clone(), b.clone()));
            }
            Op::Commit => {
                let t = std::mem::replace(
                    &mut txn,
                    db.begin(Isolation::Serializable, Consistency::Strong).await.map_err(|e| e.to_string())?,
                );
                match db.commit(t.into_request()).await {
                    Ok(_) => {}
                    // Staged blobs older than the stage TTL are rejected.
                    Err(e) if e.message().contains("staged blob expired") => {
                        pending.clear();
                        pending_ranges.clear();
                    }
                    Err(e) => return Err(e.to_string()),
                }
                for (a, b) in pending_ranges.drain(..) {
                    model.retain(|k, _| !(k >= &a && k < &b));
                }
                for (k, v) in std::mem::take(&mut pending) {
                    match v {
                        Some(v) => model.insert(k, v),
                        None => model.remove(&k),
                    };
                }
                // The new txn began before this commit was applied; restart it.
                txn = db.begin(Isolation::Serializable, Consistency::Strong).await.map_err(|e| e.to_string())?;
                check_against_model(&db, &model, &mut rng).await?;
            }
            Op::Compact => {
                db.compact_now().await.map_err(|e| e.to_string())?;
                check_against_model(&db, &model, &mut rng).await?;
            }
            Op::Advance(ms) => {
                tokio::time::sleep(Duration::from_millis(*ms)).await;
                db.maintenance_now().await.map_err(|e| e.to_string())?;
            }
            Op::Sweep => {
                db.sweep_now().await.map_err(|e| e.to_string())?;
                // let the GC delete task run
                tokio::time::sleep(Duration::from_millis(10)).await;
                check_against_model(&db, &model, &mut rng).await?;
            }
        }
    }
    // Storage invariant: every referenced object exists.
    let v = db.read_version(Consistency::Strong).await.map_err(|e| e.to_string())?;
    let keys: std::collections::BTreeSet<String> = store.keys().into_iter().collect();
    for s in &v.m.segments {
        if !keys.contains(&seg_key("t1", &s.id)) {
            return Err(format!("referenced segment {} missing", s.id));
        }
    }
    for e in v.all_entries(false).await.map_err(|e| e.to_string())? {
        if let Value::Blob(b) = &e.value {
            let n = b.size.div_ceil(eng.cfg().blob_chunk_size as u64);
            for i in 0..n {
                if !keys.contains(&blob_chunk_key("t1", &b.id, i)) {
                    return Err(format!("referenced blob chunk {} {} missing", b.id, i));
                }
            }
        }
    }
    if std::env::var("ENGINE_STATS").is_ok() {
        eprintln!(
            "seed {seed}: ops={} version={} segments={} tail={} obsolete={} horizon={} objects={}",
            ops.len(),
            v.m.version,
            v.m.segments.len(),
            v.m.tail.len(),
            v.m.obsolete.len(),
            v.m.tombstone_horizon,
            store.len()
        );
    }
    let _ = env;
    let _ = store.list("dbs/", None, 10).await;
    Ok(())
}

#[test]
fn prop_engine_matches_model() {
    prop::check(
        "engine-model",
        40,
        |r| (r.next_u64(), gen_ops(r)),
        |(s, ops)| prop::shrink_vec(ops).into_iter().map(|o| (*s, o)).collect(),
        |(seed, ops)| rt().block_on(run_ops(*seed, ops)),
    );
}

#[test]
fn serializable_conflicts_and_snapshots() {
    rt().block_on(async {
        let (eng, _store, _env) = engine(1, EngineConfig::small_for_tests());
        let db = eng.db("c");
        db.open(true).await.unwrap();
        let k = Bytes::from_static(b"k");
        let mut t0 = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t0.put(k.clone(), Value::Inline(Bytes::from_static(b"0"))).unwrap();
        let v0 = db.commit(t0.into_request()).await.unwrap();

        // Two read-modify-write txns on the same key: second must conflict.
        let mut a = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let mut b = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        assert!(a.get(&k).await.unwrap().is_some());
        assert!(b.get(&k).await.unwrap().is_some());
        a.put(k.clone(), Value::Inline(Bytes::from_static(b"a"))).unwrap();
        b.put(k.clone(), Value::Inline(Bytes::from_static(b"b"))).unwrap();
        let va = db.commit(a.into_request()).await.unwrap();
        assert!(va > v0);
        let eb = db.commit(b.into_request()).await.unwrap_err();
        assert_eq!(eb.code(), "conflict");

        // Phantom protection: a scan conflicts with a later insert in range.
        let mut s = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let _ = s.scan(&KeyRange::prefix(b"p"), false, 10).await.unwrap();
        s.put(Bytes::from_static(b"other"), Value::Inline(Bytes::new())).unwrap();
        let mut ins = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        ins.put(Bytes::from_static(b"p1"), Value::Inline(Bytes::new())).unwrap();
        db.commit(ins.into_request()).await.unwrap();
        assert_eq!(db.commit(s.into_request()).await.unwrap_err().code(), "conflict");

        // Snapshot isolation allows write skew on disjoint writes.
        let mut x = db.begin(Isolation::Snapshot, Consistency::Strong).await.unwrap();
        let mut y = db.begin(Isolation::Snapshot, Consistency::Strong).await.unwrap();
        x.get(b"k").await.unwrap();
        y.get(b"k").await.unwrap();
        x.put(Bytes::from_static(b"x"), Value::Inline(Bytes::new())).unwrap();
        y.put(Bytes::from_static(b"y"), Value::Inline(Bytes::new())).unwrap();
        db.commit(x.into_request()).await.unwrap();
        db.commit(y.into_request()).await.unwrap();

        // Old snapshots keep seeing old data.
        let old = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let mut w = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        w.put(k.clone(), Value::Inline(Bytes::from_static(b"new"))).unwrap();
        db.commit(w.into_request()).await.unwrap();
        let e = old.snapshot.get(b"k").await.unwrap().unwrap();
        assert_eq!(e.value, Value::Inline(Bytes::from_static(b"a")));

        // Idempotent replays return the original version.
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.put(Bytes::from_static(b"idem"), Value::Inline(Bytes::new())).unwrap();
        let req = t.into_request();
        let v1 = db.commit(req.clone()).await.unwrap();
        let v2 = db.commit(req).await.unwrap();
        assert_eq!(v1, v2);
    });
}

#[test]
fn group_commit_batches_concurrent_commits() {
    rt().block_on(async {
        let (eng, store, _env) = engine(2, EngineConfig::default());
        let db = eng.db("g");
        db.open(true).await.unwrap();
        let before = store.len();
        let ws_before = db.current().unwrap().m.write_seq;
        let mut handles = Vec::new();
        for i in 0..20u32 {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                t.put(Bytes::from(format!("k{i}")), Value::Inline(Bytes::from_static(b"v"))).unwrap();
                db.commit(t.into_request()).await.unwrap()
            }));
        }
        let mut versions = Vec::new();
        for h in handles {
            versions.push(h.await.unwrap());
        }
        versions.sort();
        versions.dedup();
        // Every commit has its own version...
        assert_eq!(versions.len(), 20);
        // ...but they share manifest writes.
        let writes = db.current().unwrap().m.write_seq - ws_before;
        assert!(writes < 20, "expected batching, got {writes} manifest writes");
        assert_eq!(store.len(), before);
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert_eq!(v.scan(&KeyRange::all(), false, 100, false).await.unwrap().len(), 20);
    });
}
