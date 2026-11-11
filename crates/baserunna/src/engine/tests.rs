//! Engine model and unit tests.

use super::*;
use crate::engine::types::commit_key;
use crate::env::Env;
use crate::error::Error;
use crate::sim::lease_check::{ManifestHistory, check_lease_view};
use crate::store::StoreError;
use crate::store::{BoxFut, DynStore, GetOpts, GetResult, ListPage, MemStore, ObjectStore, PutMode, SResult};
use crate::util::prop;
use crate::util::rng::Rng;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::time::Duration;
use tokio::time::Instant;

pub(crate) fn rt() -> tokio::runtime::Runtime {
    crate::probe::record();
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
    Put(Vec<u8>, Vec<u8>, Option<u64>),
    Delete(Vec<u8>),
    DeleteRange(Vec<u8>, Vec<u8>),
    Bulk(u8, u8, Option<u64>),
    Commit,
    Snapshot,
    Settle,
    Advance(u64),
    Sweep,
    Feed(usize, usize),
    Unload,
}

fn data_key(r: &mut Rng, max_len: usize) -> Vec<u8> {
    let mut k = prop::small_key(r, max_len);
    if k[0] == types::COMMIT_SPACE {
        k[0] -= 1;
    }
    // Event tables reserve these prefixes, and sweeps fail if a part reference cannot be decoded.
    if (events::UNSEALED..=events::TABLE_STATE).contains(&k[0]) {
        k[0] = events::UNSEALED - 1;
    }
    k
}

fn gen_ttl(r: &mut Rng) -> Option<u64> {
    match r.below(10) {
        0..=6 => None,
        7 | 8 => Some(r.range(1, 5_000)),
        _ => Some(r.range(100_000, 3_000_000)),
    }
}

fn gen_ops(r: &mut Rng) -> Vec<Op> {
    let n = 50 + r.usize(250);
    (0..n)
        .map(|_| match r.below(100) {
            0..=44 => {
                let len = if r.chance(0.1) { 600 + r.usize(2000) } else { r.usize(60) };
                Op::Put(data_key(r, 4), r.bytes(len), gen_ttl(r))
            }
            45..=56 => Op::Delete(data_key(r, 4)),
            57..=60 => {
                let a = prop::small_key(r, 3);
                let b = prop::small_key(r, 3);
                if a <= b { Op::DeleteRange(a, b) } else { Op::DeleteRange(b, a) }
            }
            61..=65 => Op::Bulk(b'a' + r.below(4) as u8, 5 + r.below(40) as u8, gen_ttl(r)),
            66..=67 => Op::Snapshot,
            68..=85 => Op::Commit,
            86..=89 => Op::Settle,
            90..=93 => Op::Advance(if r.chance(0.5) { r.range(1, 6_000) } else { r.range(1, 3_000_000) }),
            94..=95 => Op::Feed(r.usize(1000), 1 + r.usize(20)),
            96..=97 => Op::Unload,
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

type Model = BTreeMap<Vec<u8>, (Vec<u8>, Option<u64>)>;

fn live(m: &Model, at: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
    m.iter().filter(|(_, (_, e))| e.is_none_or(|e| e > at)).map(|(k, (v, _))| (k.clone(), v.clone())).collect()
}

async fn check_against_model(db: &Arc<Db>, model: &Model, r: &mut Rng) -> Result<(), String> {
    let snap = db.snapshot(Consistency::Strong).await.map_err(|e| e.to_string())?;
    let model = live(model, snap.time_ms);
    let all = snap.scan(&KeyRange::data(), false, usize::MAX).await.map_err(|e| e.to_string())?;
    let mut got = BTreeMap::new();
    for e in &all {
        got.insert(e.key.to_vec(), value_bytes(db, &e.value).await);
    }
    if got != model {
        return Err(format!("full scan mismatch at {}: got {} keys, want {}", snap.time_ms, got.len(), model.len()));
    }
    for _ in 0..10 {
        let k = data_key(r, 4);
        let g = snap.get(&k).await.map_err(|e| e.to_string())?;
        let gv = match g {
            Some(e) => Some(value_bytes(db, &e.value).await),
            None => None,
        };
        if gv.as_ref() != model.get(&k) {
            return Err(format!("point get mismatch for {k:?}"));
        }
        let a = data_key(r, 3);
        let reverse = r.chance(0.5);
        let limit = 1 + r.usize(8);
        let range = KeyRange::half_open(Bytes::from(a.clone()), Some(types::commit_key(0).slice(..1)));
        let got: Vec<Vec<u8>> = snap
            .scan(&range, reverse, limit)
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

async fn check_feed(db: &Arc<Db>, version: Seq, then: &Model, now: &Model, limit: usize) -> Result<(), String> {
    let snap = db.snapshot(Consistency::Strong).await.map_err(|e| e.to_string())?;
    let v = &snap.version;
    let mut mirror = then.clone();
    let mut cursor = ChangeCursor::after_version(version);
    loop {
        let page = match v.changes(&KeyRange::data(), &cursor, limit).await {
            Err(Error::CursorExpired(_)) if version < v.m.tombstone_horizon => return Ok(()),
            Err(e) => return Err(format!("feed after v{version} (horizon {}): {e}", v.m.tombstone_horizon)),
            Ok(p) => p,
        };
        if page.changes.len() > limit || (page.more && page.changes.len() < limit) {
            return Err("feed page size".into());
        }
        for c in &page.changes {
            if c.pos() <= cursor.pos || c.seq() <= version || c.seq() > v.m.version {
                return Err(format!("feed out of order: {c:?} after {cursor:?}"));
            }
            cursor.pos = c.pos();
            match c {
                Change::Entry(e) if e.value.is_tombstone() => {
                    mirror.remove(e.key.as_ref());
                }
                Change::Entry(e) => {
                    mirror.insert(e.key.to_vec(), (value_bytes(db, &e.value).await, e.expires));
                }
                Change::DeleteRange(t) => mirror.retain(|k, _| !t.contains_key(k)),
            }
        }
        if !page.more {
            break;
        }
    }
    if live(&mirror, snap.time_ms) != live(now, snap.time_ms) {
        return Err(format!("feed after v{version} does not lead to v{}", v.m.version));
    }
    Ok(())
}

async fn run_ops(seed: u64, ops: &[Op]) -> Result<(), String> {
    let levels_inline_max = if seed.is_multiple_of(2) { 0 } else { 16 };
    let (eng, store, env) = engine(seed, EngineConfig { levels_inline_max, ..EngineConfig::small_for_tests() });
    let db = eng.db("t1");
    db.open(true).await.map_err(|e| e.to_string())?;
    eng.shared.store_time_ms().await.map_err(|e| e.to_string())?;
    let mut model = Model::new();
    type Pending = BTreeMap<Vec<u8>, Option<(Vec<u8>, Option<u64>)>>;
    let mut pending = Pending::new();
    let mut pending_ranges: Vec<KeyRange> = Vec::new();
    let mut txn = db.begin(Isolation::Serializable, Consistency::Strong).await.map_err(|e| e.to_string())?;
    let mut rng = Rng::new(seed ^ 0xabc);
    type Remembered = (Instant, Snapshot, Model);
    let mut snapshots: Vec<Remembered> = Vec::new();
    let mut history: Vec<(Seq, Model)> = vec![(0, Model::new())];
    for op in ops {
        match op {
            Op::Bulk(p, n, ttl) => {
                for i in 0..*n {
                    let (k, v) = (vec![*p, i], vec![i; 100]);
                    txn.put_expiring(Bytes::from(k.clone()), Value::Inline(Bytes::from(v.clone())), ttl.map(Ttl::For))
                        .map_err(|e| e.to_string())?;
                    pending.insert(k, Some((v, *ttl)));
                }
            }
            Op::Snapshot => {
                let s = db.snapshot(Consistency::Strong).await.map_err(|e| e.to_string())?;
                snapshots.push((Instant::now(), s, model.clone()));
            }
            Op::Put(k, v, ttl) => {
                let value = if v.len() > eng.shared.cfg.value_inline_max {
                    Value::Blob(
                        blob::write_blob(&eng.shared, "t1", v, "application/octet-stream")
                            .await
                            .map_err(|e| e.to_string())?,
                    )
                } else {
                    Value::Inline(Bytes::from(v.clone()))
                };
                txn.put_expiring(Bytes::from(k.clone()), value, ttl.map(Ttl::For)).map_err(|e| e.to_string())?;
                pending.insert(k.clone(), Some((v.clone(), *ttl)));
            }
            Op::Delete(k) => {
                txn.delete(Bytes::from(k.clone())).map_err(|e| e.to_string())?;
                pending.insert(k.clone(), None);
            }
            Op::DeleteRange(a, b) => {
                let r = KeyRange::half_open(Bytes::from(a.clone()), Some(Bytes::from(b.clone())));
                txn.delete_range(r.clone()).map_err(|e| e.to_string())?;
                pending.retain(|k, _| !r.contains(k));
                pending_ranges.push(r);
            }
            Op::Commit => {
                let t = std::mem::replace(
                    &mut txn,
                    db.begin(Isolation::Serializable, Consistency::Strong).await.map_err(|e| e.to_string())?,
                );
                let committed = match db.commit_detailed(t.into_request()).await {
                    Ok(c) => Some(c),
                    Err(e) if e.message().contains("staged blob expired") => {
                        pending.clear();
                        pending_ranges.clear();
                        None
                    }
                    Err(e) => return Err(e.to_string()),
                };
                for r in pending_ranges.drain(..) {
                    model.retain(|k, _| !r.contains(k));
                }
                let time = committed.as_ref().map_or(0, |c| c.time_ms);
                for (k, v) in std::mem::take(&mut pending) {
                    match v {
                        Some((v, ttl)) => model.insert(k, (v, ttl.map(|t| time + t))),
                        None => model.remove(&k),
                    };
                }
                if let Some(c) = committed.filter(|c| c.version > history.last().map_or(0, |h| h.0)) {
                    history.push((c.version, model.clone()));
                }
                txn = db.begin(Isolation::Serializable, Consistency::Strong).await.map_err(|e| e.to_string())?;
                check_against_model(&db, &model, &mut rng).await?;
            }
            Op::Settle => {
                tokio::time::sleep(Duration::from_millis(10)).await;
                check_against_model(&db, &model, &mut rng).await?;
                let life = Duration::from_millis(eng.shared.cfg.txn_max_lifetime_ms);
                snapshots.retain(|(t, _, _)| t.elapsed() < life);
                for (_, s, m) in &snapshots {
                    let all =
                        s.scan(&KeyRange::data(), false, usize::MAX).await.map_err(|e| format!("old snapshot: {e}"))?;
                    let mut got = BTreeMap::new();
                    for e in &all {
                        got.insert(e.key.to_vec(), value_bytes(&db, &e.value).await);
                    }
                    if got != live(m, s.time_ms) {
                        return Err(format!("snapshot v{} changed: {} keys, want {}", s.version(), got.len(), m.len()));
                    }
                }
            }
            Op::Advance(ms) => {
                tokio::time::sleep(Duration::from_millis(*ms)).await;
                db.maintenance_now().await.map_err(|e| e.to_string())?;
                txn = db.begin(Isolation::Serializable, Consistency::Strong).await.map_err(|e| e.to_string())?;
                pending.clear();
                pending_ranges.clear();
            }
            Op::Sweep => {
                db.sweep_now().await.map_err(|e| e.to_string())?;
                tokio::time::sleep(Duration::from_millis(10)).await;
                check_against_model(&db, &model, &mut rng).await?;
            }
            Op::Feed(i, limit) => {
                let (version, then) = &history[i % history.len()];
                check_feed(&db, *version, then, &model, *limit).await?;
            }
            Op::Unload => {
                db.unload();
                check_against_model(&db, &model, &mut rng).await?;
            }
        }
    }
    let v = db.read_version(Consistency::Strong).await.map_err(|e| e.to_string())?;
    let keys: std::collections::BTreeSet<String> = store.keys().into_iter().collect();
    for s in &v.m.segments {
        if !keys.contains(&seg_key("t1", &s.id)) {
            return Err(format!("referenced segment {} missing", s.id));
        }
    }
    if let Some(id) = &v.m.levels {
        let g =
            store.get_sync(&levels_key("t1", id), &GetOpts::default()).map_err(|e| format!("levels object: {e}"))?;
        let l = manifest::Levels::decode(&g.data).map_err(|e| e.to_string())?;
        if l.segments != v.m.deep_segments() {
            return Err("the levels object differs from the manifest's levels".into());
        }
    }
    for e in v.all_entries(false).await.map_err(|e| e.to_string())? {
        if let Value::Blob(b) = &e.value {
            let n = b.size.div_ceil(eng.shared.cfg.blob_chunk_size as u64);
            for i in 0..n {
                if !keys.contains(&blob_chunk_key("t1", &b.id, i)) {
                    return Err(format!("referenced blob chunk {} {} missing", b.id, i));
                }
            }
        }
    }
    if std::env::var("ENGINE_STATS").is_ok() {
        eprintln!(
            "seed {seed}: ops={} version={} segments={} tail={} obsolete={} horizon={} objects={} {:?}",
            ops.len(),
            v.m.version,
            v.m.segments.len(),
            v.m.tail.len(),
            v.m.obsolete.len(),
            v.m.tombstone_horizon,
            store.len(),
            crate::probe::Probe::ALL
                .iter()
                .map(|p| (p.name(), crate::probe::count(*p)))
                .filter(|(_, n)| *n > 0)
                .collect::<Vec<_>>()
        );
    }
    let _ = env;
    let _ = store.list("dbs/", None, 10).await;
    Ok(())
}

#[test]
fn object_class_from_key() {
    assert_eq!(ObjectClass::from_key(&manifest_key("a1")), ObjectClass::Manifest);
    assert_eq!(ObjectClass::from_key(&seg_key("a1", "0123")), ObjectClass::Segment);
    assert_eq!(ObjectClass::from_key(&levels_key("a1", "0123")), ObjectClass::Levels);
    assert_eq!(ObjectClass::from_key(&blob_chunk_key("a1", "b", 1)), ObjectClass::Blob);
    assert_eq!(ObjectClass::from_key(&pin_key("a1", "b")), ObjectClass::Blob);
    assert_eq!(ObjectClass::from_key(&seg_key("a1", "")), ObjectClass::Segment);
    assert_eq!(ObjectClass::from_key("clock"), ObjectClass::Other);
    assert_eq!(ObjectClass::from_key("dbs/a1"), ObjectClass::Other);
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

        let mut s = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let _ = s.scan(&KeyRange::prefix(b"p"), false, 10).await.unwrap();
        s.put(Bytes::from_static(b"other"), Value::Inline(Bytes::new())).unwrap();
        let mut ins = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        ins.put(Bytes::from_static(b"p1"), Value::Inline(Bytes::new())).unwrap();
        db.commit(ins.into_request()).await.unwrap();
        assert_eq!(db.commit(s.into_request()).await.unwrap_err().code(), "conflict");

        let mut x = db.begin(Isolation::Snapshot, Consistency::Strong).await.unwrap();
        let mut y = db.begin(Isolation::Snapshot, Consistency::Strong).await.unwrap();
        x.get(b"k").await.unwrap();
        y.get(b"k").await.unwrap();
        x.put(Bytes::from_static(b"x"), Value::Inline(Bytes::new())).unwrap();
        y.put(Bytes::from_static(b"y"), Value::Inline(Bytes::new())).unwrap();
        db.commit(x.into_request()).await.unwrap();
        db.commit(y.into_request()).await.unwrap();

        let old = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let mut w = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        w.put(k.clone(), Value::Inline(Bytes::from_static(b"new"))).unwrap();
        db.commit(w.into_request()).await.unwrap();
        let e = old.snapshot().get(b"k", old.read_time).await.unwrap().unwrap();
        assert_eq!(e.value, Value::Inline(Bytes::from_static(b"a")));

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
        let objects = || store.keys().iter().filter(|k| k.starts_with("dbs/")).count();
        let before = objects();
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
        assert_eq!(versions.len(), 20);
        let writes = db.current().unwrap().m.write_seq - ws_before;
        assert!(writes < 20, "expected batching, got {writes} manifest writes");
        assert_eq!(objects(), before);
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert_eq!(v.scan(&KeyRange::data(), false, 100, v.read_time()).await.unwrap().len(), 20);
    });
}

#[test]
fn lease_sightings_outlive_handles_and_prune_when_full() {
    use super::manifest::Lease;
    rt().block_on(async {
        let (eng, _store, _env) = engine(3, EngineConfig::small_for_tests());
        let sh = &eng.shared;
        let lease = |node: &str| Lease { node: node.into(), expires_ms: 7, ms: 5_000 };
        let first = sh.lease_seen("a", &lease("x"), 5);
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        assert_eq!(sh.lease_seen("a", &lease("x"), 5), first);
        assert!(sh.lease_seen("a", &lease("x"), 6) > first);
        assert!(sh.lease_seen("a", &lease("y"), 6) > first);
        let a_seen = sh.lease_seen("a", &lease("y"), 6);
        let v = sh.cfg.read_lease_ms + sh.cfg.lease_margin_ms;
        let present = |id: &str| sh.observed_leases.lock().contains_key(id);
        let age_to = |x: u64| async move {
            let now = tokio::time::Instant::now();
            tokio::time::advance((a_seen + std::time::Duration::from_millis(x * v / 100)) - now).await;
        };
        age_to(5_000).await;
        for i in 0..100_000 {
            sh.lease_seen(&format!("d{i}"), &lease("x"), 1);
        }
        assert!(present("a"));
        age_to(9_500).await;
        sh.lease_seen("b", &lease("x"), 1);
        assert!(present("a"));
        age_to(10_500).await;
        sh.lease_seen("c", &lease("x"), 1);
        assert!(!present("a") && present("c"));
    });
}

#[test]
fn blob_limits_and_ranges() {
    rt().block_on(async {
        let cfg = EngineConfig { blob_max: 3000, ..EngineConfig::small_for_tests() };
        let (eng, _store, _env) = engine(4, cfg);
        let sh = &eng.shared;
        let ok = super::blob::write_blob(sh, "b", &[7u8; 3000], "x/y").await.unwrap();
        assert_eq!(ok.size, 3000);
        let e = super::blob::write_blob(sh, "b", &[7u8; 3001], "x/y").await;
        assert!(matches!(e, Err(crate::error::Error::TooLarge(_))), "a blob over blob_max must be refused");
        let r = super::blob::read_range(sh, "b", &ok, 1000..2100).await.unwrap();
        assert_eq!(r.len(), 1100);
        assert!(super::blob::read_range(sh, "b", &ok, 0..3001).await.is_err(), "a range past the end");
        assert!(
            super::blob::read_range(sh, "b", &ok, std::ops::Range { start: 10, end: 5 }).await.is_err(),
            "an inverted range"
        );
        assert_eq!(super::blob::read_range(sh, "b", &ok, 3000..3000).await.unwrap().len(), 0);
    });
}

pub(super) struct DelayStore {
    pub(super) mem: Arc<MemStore>,
    get_reply_ms: AtomicU64,
    clock_get_reply_ms: AtomicU64,
    pub(super) put_reply_ms: AtomicU64,
    manifest_put_conflicts: AtomicU32,
    manifest_put_outages: AtomicU32,
    pub(super) manifest_put_lost: AtomicU32,
    manifest_get_outages: AtomicU32,
    stale_manifest: Mutex<Option<Bytes>>,
    manifest_puts: AtomicU32,
    pub(super) gets: Mutex<Vec<(String, bool)>>,
    seg_get_bytes: Mutex<Vec<usize>>,
}

fn take_fault(n: &AtomicU32) -> bool {
    n.try_update(SeqCst, SeqCst, |n| n.checked_sub(1)).is_ok()
}

impl DelayStore {
    fn gets_matching(&self, f: impl Fn(&str) -> bool) -> usize {
        self.gets.lock().iter().filter(|(k, _)| f(k)).count()
    }
}

impl ObjectStore for DelayStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        Box::pin(async move {
            self.gets.lock().push((key.to_string(), opts.if_none_match.is_some()));
            if key.ends_with("/manifest") && take_fault(&self.manifest_get_outages) {
                return Err(StoreError::Unavailable("injected".into()));
            }
            if key.ends_with("/manifest")
                && let Some(data) = self.stale_manifest.lock().take()
            {
                let size = data.len() as u64;
                return Ok(GetResult { data, etag: "stale".into(), size, last_modified_ms: 0 });
            }
            let r = self.mem.get_sync(key, &opts);
            if let (true, Ok(g)) = (key.contains("/seg/"), &r) {
                self.seg_get_bytes.lock().push(g.data.len());
            }
            let mut delay = self.get_reply_ms.load(SeqCst);
            if key == super::clock::PROBE_KEY {
                delay += self.clock_get_reply_ms.load(SeqCst);
            }
            tokio::time::sleep(Duration::from_millis(delay)).await;
            r
        })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move {
            if key.ends_with("/manifest") {
                self.manifest_puts.fetch_add(1, SeqCst);
                if take_fault(&self.manifest_put_conflicts) {
                    return Err(StoreError::PreconditionFailed);
                }
                if take_fault(&self.manifest_put_outages) {
                    return Err(StoreError::Unavailable("injected".into()));
                }
            }
            let lost = key.ends_with("/manifest") && take_fault(&self.manifest_put_lost);
            let r = self.mem.put_sync(key, data, &mode);
            tokio::time::sleep(Duration::from_millis(self.put_reply_ms.load(SeqCst))).await;
            match lost {
                true => Err(StoreError::Unavailable("reply lost".into())),
                false => r,
            }
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        self.mem.delete(key)
    }

    fn list<'a>(&'a self, prefix: &'a str, after: Option<&'a str>, limit: usize) -> BoxFut<'a, SResult<ListPage>> {
        self.mem.list(prefix, after, limit)
    }

    fn time_precision_ms(&self) -> u64 {
        self.mem.time_precision_ms()
    }
}

pub(super) fn delayed_engine(node: &str) -> (Arc<Engine>, Arc<DelayStore>) {
    delayed_engine_with(node, EngineConfig::small_for_tests())
}

pub(super) fn delayed_engine_with(node: &str, cfg: EngineConfig) -> (Arc<Engine>, Arc<DelayStore>) {
    let origin = tokio::time::Instant::now();
    let env = Arc::new(Env::sim(9, 1_700_000_000_000, origin, 0));
    let env2 = env.clone();
    let mem = Arc::new(MemStore::new(Box::new(move || env2.now_ms())));
    let store = Arc::new(DelayStore {
        mem,
        get_reply_ms: 0.into(),
        clock_get_reply_ms: 0.into(),
        put_reply_ms: 0.into(),
        manifest_put_conflicts: 0.into(),
        manifest_put_outages: 0.into(),
        manifest_put_lost: 0.into(),
        manifest_get_outages: 0.into(),
        stale_manifest: Mutex::default(),
        manifest_puts: 0.into(),
        gets: Mutex::default(),
        seg_get_bytes: Mutex::default(),
    });
    (Engine::new(store.clone(), env, cfg, node.into()), store)
}

pub(super) fn second_node(a: &Arc<Engine>, store: &Arc<DelayStore>, node: &str) -> Arc<Engine> {
    let store: DynStore = store.clone();
    Engine::new(store, a.shared.env.clone(), a.shared.cfg.clone(), node.into())
}

pub(super) async fn opened(eng: &Arc<Engine>, id: &str) -> Arc<Db> {
    let db = eng.db(id);
    db.open(true).await.unwrap();
    db
}

pub(super) async fn put(db: &Arc<Db>, k: &str, v: &str) -> Seq {
    let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
    t.put(Bytes::from(k.to_string()), Value::Inline(Bytes::from(v.to_string()))).unwrap();
    db.commit(t.into_request()).await.unwrap()
}

/// Commits `k = v` on `db` as soon as `db` reads no valid lease of another node.
pub(super) async fn put_when_owner(db: &Arc<Db>, k: &str, v: &str) -> Seq {
    loop {
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.put(Bytes::from(k.to_string()), Value::Inline(Bytes::from(v.to_string()))).unwrap();
        match db.commit(t.into_request()).await {
            Err(Error::NotOwner { retry_after_ms, .. }) => {
                tokio::time::sleep(Duration::from_millis(retry_after_ms)).await;
            }
            committed => return committed.unwrap(),
        }
    }
}

#[test]
fn installed_versions_never_go_back() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = eng.db("d");
        db.open(true).await.unwrap();
        put(&db, "k", "1").await;
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.put(Bytes::from_static(b"k"), Value::Inline(Bytes::from_static(b"2"))).unwrap();
        store.get_reply_ms.store(50, std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(1)).await;
        let db2 = db.clone();
        let late = tokio::spawn(async move { db2.refresh(tokio::time::Instant::now(), false).await });
        tokio::time::sleep(Duration::from_millis(5)).await;
        let v = db.commit(t.into_request()).await.unwrap();
        late.await.unwrap().unwrap();
        assert_eq!(db.current().unwrap().m.version, v, "a late, older manifest replaced the current one");
    });
}

#[test]
fn strong_reads_wait_for_the_write_in_flight() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = eng.db("d");
        db.open(true).await.unwrap();
        put(&db, "k", "1").await;
        store.put_reply_ms.store(50, std::sync::atomic::Ordering::SeqCst);
        let db2 = db.clone();
        let w = tokio::spawn(async move { put(&db2, "k", "2").await });
        tokio::time::sleep(Duration::from_millis(5)).await;
        let v = db.read_version(Consistency::Strong).await.unwrap();
        let committed = w.await.unwrap();
        assert_eq!(v.m.version, committed, "a strong read returned the version before a write others can see");
    });
}

#[test]
fn resolving_a_commit_aborts_it() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        put(&db, "k", "1").await;
        let request = |id: u64| {
            let db = db.clone();
            async move {
                let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                t.put(Bytes::from(format!("k{id}")), inline("v")).unwrap();
                let mut r = t.into_request();
                r.commit_id = id;
                r
            }
        };
        let etag = || store.mem.get_sync(&manifest_key("d"), &GetOpts::default()).unwrap().etag;
        let (stale, stale_again) = (request(42).await, request(42).await);
        let before = etag();
        assert_eq!(db.resolve_commit(42).await.unwrap(), None);
        assert_ne!(etag(), before, "resolving an unknown commit did not write the manifest");
        assert_eq!(db.commit_status(42).await.unwrap(), Some(CommitState::Aborted));
        assert!(matches!(db.commit_detailed(stale).await, Err(Error::Aborted(_))), "an attempt older than the abort");
        assert!(matches!(db.commit_detailed(request(42).await).await, Err(Error::Aborted(_))), "a new attempt");
        let before = etag();
        assert_eq!(db.resolve_commit(42).await.unwrap(), None);
        assert_eq!(etag(), before, "resolving an aborted commit again wrote the manifest");

        let late = request(43).await;
        let (resolved, attempt) = tokio::join!(db.resolve_commit(43), db.commit_detailed(late));
        assert_eq!(resolved.unwrap(), None);
        assert!(matches!(attempt, Err(Error::Aborted(_))), "an attempt batched after the abort applied");
        assert!(strong_get(&db, "k43").await.is_none());

        let early = request(44).await;
        let (attempt, resolved) = tokio::join!(db.commit_detailed(early), db.resolve_commit(44));
        let version = attempt.unwrap().version;
        assert_eq!(resolved.unwrap().map(|c| c.version), Some(version));
        let before = etag();
        assert_eq!(db.resolve_commit(44).await.unwrap().map(|c| c.version), Some(version));
        assert_eq!(etag(), before, "resolving an applied commit wrote the manifest");

        put_many(&db, "f", 20, 300).await;
        flushed(&db).await;
        assert!(db.current().unwrap().m.tail.iter().all(|e| e.key != commit_key(42)), "setup: flushed");
        assert!(matches!(db.commit_detailed(stale_again).await, Err(Error::Aborted(_))), "aborted from a segment");
        assert!(matches!(db.commit_detailed(request(42).await).await, Err(Error::Aborted(_))));
    });
}

#[test]
fn opening_without_create_never_creates() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        assert!(eng.db("gone").open(false).await.is_err());
        assert!(store.mem.keys().is_empty(), "opening created {:?}", store.mem.keys());
    });
}

#[test]
fn open_rejects_missing_manifest_if_database_objects_exist() {
    rt().block_on(async {
        let cfg = EngineConfig::small_for_tests();
        let (eng, store, env) = engine(1, cfg.clone());
        let db = opened(&eng, "d").await;
        for i in 0..60 {
            put(&db, &format!("k{i:03}"), &"x".repeat(100)).await;
        }
        tokio::time::sleep(Duration::from_millis(2_000)).await;
        db.maintenance_now().await.unwrap();
        let segments: Vec<String> = db.current().unwrap().m.segments.iter().map(|seg| seg_key("d", &seg.id)).collect();
        assert!(!segments.is_empty(), "expected the tail to flush to a segment");
        store.delete_sync(&manifest_key("d"));
        let other = Engine::new(store.clone(), env.clone(), cfg.clone(), "node-b".into());
        let err = other.db("d").open(true).await.map(|_| ()).expect_err("an empty manifest replaced the lost manifest");
        assert!(err.to_string().contains("manifest of database d is missing"), "{err}");
        assert!(store.get_sync(&manifest_key("d"), &GetOpts::default()).is_err(), "a manifest was created");
        for seg in &segments {
            assert!(store.get_sync(seg, &GetOpts::default()).is_ok(), "{seg} was deleted");
        }
    });
}

#[test]
fn scan_rejects_segment_that_mismatches_manifest_metadata() {
    rt().block_on(async {
        let cfg = EngineConfig { segment_whole_fetch_max: 1 << 20, ..EngineConfig::small_for_tests() };
        let (eng, store, env) = engine(2, cfg.clone());
        let db = opened(&eng, "d").await;
        for prefix in ["a", "b"] {
            for i in 0..25 {
                put(&db, &format!("{prefix}{i:03}"), &"x".repeat(100)).await;
            }
            tokio::time::sleep(Duration::from_millis(2_000)).await;
            db.maintenance_now().await.unwrap();
        }
        let manifest = db.current().unwrap().m.clone();
        let segment_with_prefix = |prefix: u8| {
            manifest.segments.iter().find(|seg| seg.min_key[0] == prefix && seg.max_key[0] == prefix).unwrap()
        };
        let (seg_a, seg_b) = (segment_with_prefix(b'a'), segment_with_prefix(b'b'));
        let seg_a_bytes = store.get_sync(&seg_key("d", &seg_a.id), &GetOpts::default()).unwrap().data;
        store.put_sync(&seg_key("d", &seg_b.id), seg_a_bytes, &PutMode::Overwrite).unwrap();
        let other = Engine::new(store.clone(), env.clone(), cfg.clone(), "node-b".into());
        let replica = other.db("d");
        replica.open(false).await.unwrap();
        let snap = replica.snapshot(Consistency::Strong).await.unwrap();
        let scan = snap.scan(&KeyRange::data(), false, usize::MAX).await;
        let err = scan.map(|entries| entries.len()).expect_err("the substituted segment was accepted");
        assert!(err.to_string().contains("but its manifest lists"), "{err}");
    });
}

/// Object store wrapper where the next `delayed_manifest_puts` manifest writes time out and apply
/// after `apply_delay_ms`.
struct DelayedManifestPutStore {
    mem: Arc<MemStore>,
    delayed_manifest_puts: AtomicU32,
    timeout_ms: u64,
    apply_delay_ms: u64,
}

impl ObjectStore for DelayedManifestPutStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        self.mem.get(key, opts)
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move {
            if key.ends_with("/manifest") && take_fault(&self.delayed_manifest_puts) {
                let (mem, key, apply_delay_ms) = (self.mem.clone(), key.to_string(), self.apply_delay_ms);
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(apply_delay_ms)).await;
                    let _ = mem.put_sync(&key, data, &mode);
                });
                tokio::time::sleep(Duration::from_millis(self.timeout_ms)).await;
                return Err(StoreError::Unavailable("request timed out".into()));
            }
            self.mem.put_sync(key, data, &mode)
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        self.mem.delete(key)
    }

    fn list<'a>(&'a self, prefix: &'a str, after: Option<&'a str>, limit: usize) -> BoxFut<'a, SResult<ListPage>> {
        self.mem.list(prefix, after, limit)
    }

    fn time_precision_ms(&self) -> u64 {
        self.mem.time_precision_ms()
    }
}

#[test]
fn rejection_after_ambiguous_manifest_put_returns_unknown_outcome() {
    rt().block_on(async {
        let env = Arc::new(Env::sim(9, 1_700_000_000_000, Instant::now(), 0));
        let clock = env.clone();
        let mem = Arc::new(MemStore::new(Box::new(move || clock.now_ms())));
        let store = Arc::new(DelayedManifestPutStore {
            mem,
            delayed_manifest_puts: 0.into(),
            timeout_ms: 3_000,
            apply_delay_ms: 3_500,
        });
        let cfg = EngineConfig { manifest_put_hedge_ms: 0, ..EngineConfig::small_for_tests() };
        let ttl = cfg.blob_stage_ttl_ms;
        let eng = Engine::new(store.clone(), env, cfg, "a".into());
        let db = opened(&eng, "d").await;
        let blob = blob::write_blob(&eng.shared, "d", b"payload", "x/y").await.unwrap();
        // The staged blob expires while the first manifest write is in flight.
        tokio::time::sleep(Duration::from_millis(ttl - 1_000)).await;
        let mut txn = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        txn.put(Bytes::from_static(b"k"), Value::Blob(blob)).unwrap();
        store.delayed_manifest_puts.store(1, SeqCst);
        let res = db.commit(txn.into_request()).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(strong_get(&db, "k").await.is_some(), "expected the delayed manifest write to be applied");
        assert!(matches!(res, Err(Error::Unavailable { outcome_unknown: true, .. })), "applied, but answered {res:?}");
    });
}

#[test]
fn conflict_excluded_from_ambiguous_manifest_put_remains_definite() {
    rt().block_on(async {
        let env = Arc::new(Env::sim(10, 1_700_000_000_000, Instant::now(), 0));
        let clock = env.clone();
        let mem = Arc::new(MemStore::new(Box::new(move || clock.now_ms())));
        let store = Arc::new(DelayedManifestPutStore {
            mem,
            delayed_manifest_puts: 0.into(),
            timeout_ms: 3_000,
            apply_delay_ms: 100,
        });
        let cfg = EngineConfig { manifest_put_hedge_ms: 0, ..EngineConfig::small_for_tests() };
        let eng = Engine::new(store.clone(), env, cfg, "a".into());
        let db = opened(&eng, "d").await;
        let mut winning_txn = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        winning_txn.put(Bytes::from_static(b"k"), inline("a")).unwrap();
        let mut conflicting_txn = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        assert!(conflicting_txn.get(b"k").await.unwrap().is_none());
        conflicting_txn.put(Bytes::from_static(b"j"), inline("c")).unwrap();
        store.delayed_manifest_puts.store(1, SeqCst);
        // Both commits join the same batch; `conflicting_txn` conflicts with `winning_txn` and is excluded from the manifest write.
        let (winning_res, conflicting_res) = tokio::join!(
            db.commit_detailed(winning_txn.into_request()),
            db.commit_detailed(conflicting_txn.into_request())
        );
        let winning_committed = winning_res.unwrap();
        assert!(!winning_committed.replayed, "the batch's own commit was reported as a replay");
        assert!(
            matches!(conflicting_res, Err(Error::Conflict(_))),
            "expected a definite conflict, got {conflicting_res:?}"
        );
    });
}

#[test]
fn commit_applied_by_ambiguous_manifest_put_is_not_marked_replayed() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let mut txn = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        txn.put(Bytes::from_static(b"k"), inline("v")).unwrap();
        store.manifest_put_lost.store(1, SeqCst);
        let committed = db.commit_detailed(txn.into_request()).await.unwrap();
        assert!(!committed.replayed, "the request's own commit was reported as a replay");
        assert_eq!(strong_get(&db, "k").await, Some(inline("v")));
    });
}

/// Outcome of a manifest `PUT` in [`LateManifestPutStore`].
#[derive(Clone, Copy, Debug)]
enum ManifestPutOutcome {
    Applied,
    FailedBeforeApply,
    AppliedThenFailed,
    /// Fails now, and stays buffered with its precondition until the test applies it.
    FailedThenAppliedLate,
    /// Applies the write, and returns `412`.
    AppliedThenRejected,
}

/// Random outcomes and latencies for the manifest requests of a [`LateManifestPutStore`].
struct ManifestFaultScript {
    rng: Rng,
    /// Weights of the outcomes in [`ManifestPutOutcome`] order.
    outcome_weights: [f64; 5],
    max_latency_ms: u64,
}

impl ManifestFaultScript {
    fn random(r: &mut Rng) -> ManifestFaultScript {
        let applied = *r.pick(&[1.0, 3.0, 9.0]);
        ManifestFaultScript {
            rng: r.fork(),
            outcome_weights: [applied, 1.0, 1.0, 1.0 + r.below(3) as f64, 1.0],
            max_latency_ms: *r.pick(&[0, 2, 30]),
        }
    }

    fn latency(&mut self) -> Duration {
        Duration::from_millis(self.rng.below(self.max_latency_ms + 1))
    }
}

/// Object store wrapper that fails the next `failed_manifest_puts` manifest writes and buffers them.
/// [`LateManifestPutStore::apply_late_puts`] applies those writes with their original preconditions.
/// If `manifest_fault_script` is set, it also draws an outcome for each manifest write.
struct LateManifestPutStore {
    mem: Arc<MemStore>,
    failed_manifest_puts: AtomicU32,
    /// Outcomes that manifest writes take in turn, and the number of writes that took an outcome.
    /// Overrides `manifest_fault_script`.
    cycled_manifest_put_outcomes: Mutex<Option<(Vec<ManifestPutOutcome>, usize)>>,
    late_puts: Mutex<Vec<(String, Bytes, PutMode)>>,
    /// True if the next manifest write applies the buffered writes first.
    apply_late_puts_on_next_put: std::sync::atomic::AtomicBool,
    manifest_put_rejected: tokio::sync::Notify,
    manifest_fault_script: Mutex<Option<ManifestFaultScript>>,
    manifest_gets: AtomicU32,
    /// True if manifest reads fail.
    manifest_get_outage: std::sync::atomic::AtomicBool,
    manifest_puts: AtomicU32,
    /// ETags of the manifest writes that the store applied, in order.
    stored_manifest_etags: Mutex<Vec<String>>,
    /// Instant at which the store applied each manifest write, by ETag.
    manifest_applied_at: Mutex<BTreeMap<String, Instant>>,
}

impl LateManifestPutStore {
    fn apply_late_puts(&self) -> Vec<SResult<String>> {
        let late = std::mem::take(&mut *self.late_puts.lock());
        late.into_iter().map(|(key, data, mode)| self.apply_manifest_put(&key, data, &mode)).collect()
    }

    /// Applies the buffered write at `index`, or returns `None` if `index` is out of bounds.
    fn apply_late_put(&self, index: usize) -> Option<SResult<String>> {
        let (key, data, mode) = {
            let mut late = self.late_puts.lock();
            (index < late.len()).then(|| late.remove(index))?
        };
        Some(self.apply_manifest_put(&key, data, &mode))
    }

    fn apply_manifest_put(&self, key: &str, data: Bytes, mode: &PutMode) -> SResult<String> {
        let r = self.mem.put_sync(key, data, mode);
        if let Ok(etag) = &r {
            self.stored_manifest_etags.lock().push(etag.clone());
            self.manifest_applied_at.lock().insert(etag.clone(), Instant::now());
        }
        r
    }

    fn draw_manifest_put(&self) -> (ManifestPutOutcome, Duration) {
        if let Some((outcomes, drawn)) = self.cycled_manifest_put_outcomes.lock().as_mut() {
            *drawn += 1;
            return (outcomes[(*drawn - 1) % outcomes.len()], Duration::ZERO);
        }
        let mut script = self.manifest_fault_script.lock();
        let Some(s) = script.as_mut() else { return (ManifestPutOutcome::Applied, Duration::ZERO) };
        let outcome = match s.rng.weighted(&s.outcome_weights) {
            Some(1) => ManifestPutOutcome::FailedBeforeApply,
            Some(2) => ManifestPutOutcome::AppliedThenFailed,
            Some(3) => ManifestPutOutcome::FailedThenAppliedLate,
            Some(4) => ManifestPutOutcome::AppliedThenRejected,
            _ => ManifestPutOutcome::Applied,
        };
        (outcome, s.latency())
    }

    fn manifest_get_latency(&self) -> Duration {
        self.manifest_fault_script.lock().as_mut().map_or(Duration::ZERO, ManifestFaultScript::latency)
    }
}

impl ManifestHistory for LateManifestPutStore {
    fn stored_manifest_etag(&self, db: &str) -> Option<String> {
        self.mem.get_sync(&manifest_key(db), &GetOpts::default()).ok().map(|g| g.etag)
    }

    fn manifest_applied_at(&self, _db: &str, etag: &str) -> Option<Instant> {
        self.manifest_applied_at.lock().get(etag).copied()
    }

    /// Returns the `If-Match` ETags of the buffered writes to the manifest of `db`.
    fn late_manifest_put_targets(&self, db: &str) -> Vec<String> {
        let key = manifest_key(db);
        let late = self.late_puts.lock();
        late.iter()
            .filter(|(k, ..)| *k == key)
            .filter_map(|(.., mode)| if let PutMode::IfMatch(e) = mode { Some(e.clone()) } else { None })
            .collect()
    }
}

impl ObjectStore for LateManifestPutStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        Box::pin(async move {
            if key.ends_with("/manifest") {
                self.manifest_gets.fetch_add(1, SeqCst);
                tokio::time::sleep(self.manifest_get_latency()).await;
                if self.manifest_get_outage.load(SeqCst) {
                    return Err(StoreError::Unavailable("connection refused".into()));
                }
            }
            self.mem.get_sync(key, &opts)
        })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move {
            if !key.ends_with("/manifest") {
                return self.mem.put_sync(key, data, &mode);
            }
            self.manifest_puts.fetch_add(1, SeqCst);
            let (outcome, latency) = self.draw_manifest_put();
            tokio::time::sleep(latency).await;
            if !self.late_puts.lock().is_empty() && self.apply_late_puts_on_next_put.swap(false, SeqCst) {
                self.apply_late_puts();
            }
            let outcome = match take_fault(&self.failed_manifest_puts) {
                true => ManifestPutOutcome::FailedThenAppliedLate,
                false => outcome,
            };
            let r = match outcome {
                ManifestPutOutcome::FailedBeforeApply => {
                    return Err(StoreError::Unavailable("connection refused".into()));
                }
                ManifestPutOutcome::FailedThenAppliedLate => {
                    self.late_puts.lock().push((key.to_string(), data, mode));
                    return Err(StoreError::Unavailable("connection reset".into()));
                }
                ManifestPutOutcome::Applied
                | ManifestPutOutcome::AppliedThenFailed
                | ManifestPutOutcome::AppliedThenRejected => self.apply_manifest_put(key, data, &mode),
            };
            tokio::time::sleep(self.manifest_get_latency()).await;
            let r = match outcome {
                ManifestPutOutcome::AppliedThenRejected => r.and(Err(StoreError::PreconditionFailed)),
                _ => r,
            };
            if matches!(r, Err(StoreError::PreconditionFailed)) {
                self.manifest_put_rejected.notify_one();
            }
            match outcome {
                ManifestPutOutcome::AppliedThenFailed => Err(StoreError::Unavailable("response lost".into())),
                _ => r,
            }
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        self.mem.delete(key)
    }

    fn list<'a>(&'a self, prefix: &'a str, after: Option<&'a str>, limit: usize) -> BoxFut<'a, SResult<ListPage>> {
        self.mem.list(prefix, after, limit)
    }

    fn time_precision_ms(&self) -> u64 {
        self.mem.time_precision_ms()
    }
}

/// Starts two nodes on a shared [`LateManifestPutStore`].
fn late_manifest_put_nodes() -> (Arc<Engine>, Arc<Engine>, Arc<LateManifestPutStore>) {
    late_manifest_put_nodes_with(11, EngineConfig::default())
}

fn late_manifest_put_nodes_with(seed: u64, cfg: EngineConfig) -> (Arc<Engine>, Arc<Engine>, Arc<LateManifestPutStore>) {
    let env = Arc::new(Env::sim(seed, 1_700_000_000_000, Instant::now(), 0));
    let clock = env.clone();
    let mem = Arc::new(MemStore::new(Box::new(move || clock.now_ms())));
    let store = Arc::new(LateManifestPutStore {
        mem,
        failed_manifest_puts: 0.into(),
        cycled_manifest_put_outcomes: Mutex::default(),
        late_puts: Mutex::default(),
        apply_late_puts_on_next_put: false.into(),
        manifest_put_rejected: tokio::sync::Notify::new(),
        manifest_fault_script: Mutex::default(),
        manifest_gets: 0.into(),
        manifest_get_outage: false.into(),
        manifest_puts: 0.into(),
        stored_manifest_etags: Mutex::default(),
        manifest_applied_at: Mutex::default(),
    });
    let a = Engine::new(store.clone(), env.clone(), cfg.clone(), "a".into());
    let b = Engine::new(store.clone(), env, cfg, "b".into());
    (a, b, store)
}

async fn commit_new(db: Arc<Db>) -> crate::error::Result<Committed> {
    let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await?;
    t.put(Bytes::from_static(b"k"), inline("new")).unwrap();
    db.commit_detailed(t.into_request()).await
}

/// Polls strong reads on `db` every `every_ms` until a read returns `new`.
/// Returns the elapsed milliseconds and the time at which `db` lost its lease.
async fn ms_until_strong_read_is_new(db: &Arc<Db>, every_ms: u64) -> (u64, Option<u64>) {
    // A paused clock does not advance between steps. A read at the instant of an earlier manifest
    // check reuses that check (`Db::refresh`), but a real clock does not produce equal instants.
    sleep_ms(1).await;
    let start = Instant::now();
    let mut lease_lost_ms = None;
    loop {
        if strong_get(db, "k").await == Some(inline("new")) {
            return (start.elapsed().as_millis() as u64, lease_lost_ms);
        }
        for _ in 0..every_ms {
            assert!(start.elapsed() < Duration::from_secs(60), "the strong read never returned the new value");
            if lease_lost_ms.is_none() && db.leased_view().is_none() {
                lease_lost_ms = Some(start.elapsed().as_millis() as u64);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

/// Tests strong reads if a failed manifest write is applied after an unknown commit outcome.
fn strong_read_after_late_manifest_put_once_unknown(poll_every_ms: u64) {
    rt().block_on(async {
        let (a, b, store) = late_manifest_put_nodes();
        let (db_a, db_b) = (opened(&a, "d").await, b.db("d"));
        let leased_at = Instant::now();
        put(&db_a, "k", "old").await;
        db_b.open(false).await.unwrap();
        assert_eq!(strong_get(&db_b, "k").await, Some(inline("old")));
        store.failed_manifest_puts.store(a.shared.cfg.commit_retry_max, SeqCst);
        let outcome = commit_new(db_a.clone()).await;
        assert!(matches!(outcome, Err(Error::Unavailable { outcome_unknown: true, .. })), "answered {outcome:?}");
        assert_eq!(strong_get(&db_a, "k").await, Some(inline("old")), "setup: the late write was not applied yet");
        let landed_after_lease_ms = leased_at.elapsed().as_millis();
        let applied = store.apply_late_puts();
        assert!(applied[0].is_ok(), "setup: the first late write is applied, got {applied:?}");
        assert_eq!(strong_get(&db_b, "k").await, Some(inline("new")), "B reads the late write");
        let (stale_ms, lease_lost_ms) = ms_until_strong_read_is_new(&db_a, poll_every_ms).await;
        eprintln!(
            "unknown outcome {outcome:?}; late write applied {landed_after_lease_ms} ms after A's lease; \
             polled every {poll_every_ms} ms: A read the old value for {stale_ms} ms, held its lease for {lease_lost_ms:?} ms"
        );
        assert_eq!(stale_ms, 0, "A read the old value for {stale_ms} ms after B read the new value");
    });
}

#[test]
fn strong_read_after_late_manifest_put_once_unknown_is_new() {
    strong_read_after_late_manifest_put_once_unknown(500);
}

#[test]
fn hot_strong_read_after_late_manifest_put_once_unknown_is_new() {
    strong_read_after_late_manifest_put_once_unknown(10);
}

/// Tests strong reads if a failed manifest write is applied before a commit retry.
#[test]
fn strong_read_after_late_manifest_put_during_retry_is_new() {
    rt().block_on(async {
        let (a, b, store) = late_manifest_put_nodes();
        let (db_a, db_b) = (opened(&a, "d").await, b.db("d"));
        put(&db_a, "k", "old").await;
        db_b.open(false).await.unwrap();
        assert_eq!(strong_get(&db_b, "k").await, Some(inline("old")));
        store.failed_manifest_puts.store(1, SeqCst);
        store.apply_late_puts_on_next_put.store(true, SeqCst);
        let commit = tokio::spawn(commit_new(db_a.clone()));
        store.manifest_put_rejected.notified().await;
        let rejected_at = Instant::now();
        assert_eq!(strong_get(&db_b, "k").await, Some(inline("new")), "B reads the late write");
        let (stale_ms, _) = ms_until_strong_read_is_new(&db_a, 1).await;
        let outcome = commit.await.unwrap();
        eprintln!(
            "retry rejected: A read the old value for {stale_ms} ms; commit answered {:?} after {} ms",
            outcome.as_ref().map(|c| c.version),
            rejected_at.elapsed().as_millis()
        );
        assert_eq!(stale_ms, 0, "A read the old value for {stale_ms} ms after B read the new value");
    });
}

/// Tests strong reads if the store applies a manifest write from the lease holder and returns `412`.
#[test]
fn strong_read_after_an_applied_manifest_put_returns_412_is_new() {
    rt().block_on(async {
        let (a, b, store) = late_manifest_put_nodes();
        let (db_a, db_b) = (opened(&a, "d").await, b.db("d"));
        put(&db_a, "k", "old").await;
        db_b.open(false).await.unwrap();
        assert!(db_a.leased_view().is_some(), "setup: A holds its lease");
        sleep_ms(100).await;
        *store.cycled_manifest_put_outcomes.lock() = Some((vec![ManifestPutOutcome::AppliedThenRejected], 0));
        let commit = tokio::spawn(commit_new(db_a.clone()));
        store.manifest_put_rejected.notified().await;
        *store.cycled_manifest_put_outcomes.lock() = None;
        assert_eq!(strong_get(&db_b, "k").await, Some(inline("new")), "B reads the applied write");
        let (stale_ms, _) = ms_until_strong_read_is_new(&db_a, 1).await;
        let outcome = commit.await.unwrap();
        assert!(outcome.is_ok(), "setup: the commit returned {outcome:?}");
        assert_eq!(stale_ms, 0, "A read the old value for {stale_ms} ms after B read the new value");
    });
}

/// Commits `new` on A of two nodes with `cfg`. The store applies the first manifest `PUT` of the
/// commit and returns `412`. Manifest reads fail after the `412` if `reads_fail` is true, until the
/// commit answers. Returns the commit outcome, A, B, and the store.
async fn commit_applied_with_a_412(
    cfg: EngineConfig,
    reads_fail: bool,
) -> (crate::error::Result<Committed>, Arc<Db>, Arc<Db>, Arc<LateManifestPutStore>) {
    let (a, b, store) = late_manifest_put_nodes_with(11, cfg);
    let (db_a, db_b) = (opened(&a, "d").await, b.db("d"));
    put(&db_a, "k", "old").await;
    db_b.open(false).await.unwrap();
    sleep_ms(100).await;
    *store.cycled_manifest_put_outcomes.lock() = Some((vec![ManifestPutOutcome::AppliedThenRejected], 0));
    let commit = tokio::spawn(commit_new(db_a.clone()));
    store.manifest_put_rejected.notified().await;
    *store.cycled_manifest_put_outcomes.lock() = None;
    store.manifest_get_outage.store(reads_fail, SeqCst);
    let outcome = commit.await.unwrap();
    store.manifest_get_outage.store(false, SeqCst);
    assert_eq!(strong_get(&db_b, "k").await, Some(inline("new")), "setup: the store applied the commit, {outcome:?}");
    (outcome, db_a, db_b, store)
}

/// Tests that a commit that the store applies with a `412` reply is not reported as replayed, and
/// that the node then takes its lease from a lease-only `PUT`.
#[test]
fn commit_applied_with_a_412_is_not_replayed() {
    rt().block_on(async {
        let (outcome, db_a, _db_b, store) = commit_applied_with_a_412(EngineConfig::default(), false).await;
        assert!(matches!(outcome, Ok(Committed { replayed: false, .. })), "the commit answered {outcome:?}");
        sleep_ms(50).await;
        let stored = store.stored_manifest_etag("d");
        assert_eq!(db_a.leased_view().map(|v| v.etag), stored, "no lease-only PUT gave A its lease");
        let (gets, puts, value) = manifest_requests(&store, strong_get(&db_a, "k")).await;
        assert_eq!((gets, puts, value), (0, 0, Some(inline("new"))), "A served no strong read from cache");
    });
}

/// Tests that a commit whose last manifest `PUT` the store applies with a `412` reply reports the
/// commit, because a reload before the batch gives up finds it.
#[test]
fn commit_applied_with_a_412_on_the_last_attempt_is_reported() {
    rt().block_on(async {
        let cfg = EngineConfig { commit_retry_max: 1, ..EngineConfig::default() };
        let (outcome, ..) = commit_applied_with_a_412(cfg, false).await;
        assert!(matches!(outcome, Ok(Committed { replayed: false, .. })), "the commit answered {outcome:?}");
    });
}

/// Tests that a commit that the store applies with a `412` reply has an unknown outcome if no
/// manifest read succeeds before the batch gives up.
#[test]
fn commit_applied_with_a_412_and_no_reload_has_an_unknown_outcome() {
    rt().block_on(async {
        let (outcome, ..) = commit_applied_with_a_412(EngineConfig::default(), true).await;
        assert!(
            matches!(outcome, Err(Error::Unavailable { outcome_unknown: true, .. })),
            "the commit answered {outcome:?}"
        );
    });
}

/// Tests that a commit is reported as replayed if another node applied its commit ID after this
/// node sent it in a manifest `PUT` with an unknown outcome that the store did not apply.
#[test]
fn commit_applied_by_another_node_after_an_unknown_put_is_replayed() {
    rt().block_on(async {
        let (a, b, store) = late_manifest_put_nodes();
        let (db_a, db_b) = (opened(&a, "d").await, b.db("d"));
        put(&db_a, "k", "old").await;
        db_b.open(false).await.unwrap();
        sleep_ms(a.shared.cfg.lease_ms + a.shared.cfg.lease_margin_ms + 1).await;
        let request = |db: Arc<Db>| async move {
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            t.put(Bytes::from_static(b"k"), inline("new")).unwrap();
            CommitRequest { commit_id: 7, ..t.into_request() }
        };
        let (req_a, req_b) = (request(db_a.clone()).await, request(db_b.clone()).await);
        *store.cycled_manifest_put_outcomes.lock() = Some((vec![ManifestPutOutcome::FailedBeforeApply], 0));
        let puts = store.manifest_puts.load(SeqCst);
        let commit_a = tokio::spawn({
            let db_a = db_a.clone();
            async move { db_a.commit_detailed(req_a).await }
        });
        while store.manifest_puts.load(SeqCst) == puts {
            sleep_ms(1).await;
        }
        *store.cycled_manifest_put_outcomes.lock() = None;
        let outcome_b = db_b.commit_detailed(req_b).await;
        assert!(matches!(outcome_b, Ok(Committed { replayed: false, .. })), "setup: B answered {outcome_b:?}");
        let outcome_a = commit_a.await.unwrap();
        assert!(matches!(outcome_a, Ok(Committed { replayed: true, .. })), "A answered {outcome_a:?}");
    });
}

/// Tests that a node validates each strong read against storage after a rejected commit retry until
/// a later manifest `PUT` from the node succeeds. The reload that finds the landed write does not
/// give the lease back. The lease-only `PUT` that the commit batch requests gives it back.
#[test]
fn lease_reads_stay_off_from_a_rejected_retry_until_a_put_succeeds() {
    use crate::util::{Either, race};
    rt().block_on(async {
        let (a, _b, store) = late_manifest_put_nodes();
        let db_a = opened(&a, "d").await;
        put(&db_a, "k", "old").await;
        store.failed_manifest_puts.store(1, SeqCst);
        store.apply_late_puts_on_next_put.store(true, SeqCst);
        let puts_before_commit = store.manifest_puts.load(SeqCst);
        let mut commit = Box::pin(commit_new(db_a.clone()));
        let mut rejected = Box::pin(store.manifest_put_rejected.notified());
        if let Either::Left(outcome) = race(&mut commit, &mut rejected).await {
            panic!("setup: the commit answered {outcome:?} before a retry was rejected");
        }
        let rejected_at = Instant::now();
        let outcome = loop {
            if let Some(LeasedView { write_seq, etag, write_in_flight: false, .. }) = db_a.leased_view() {
                panic!(
                    "A serves lease reads at write {write_seq} ({etag}) {} ms after the rejected retry, \
                     before the commit answered",
                    rejected_at.elapsed().as_millis()
                );
            }
            if let Either::Left(outcome) = race(&mut commit, &mut Box::pin(sleep_ms(1))).await {
                break outcome;
            }
        };
        assert!(outcome.is_ok(), "setup: the commit answered {outcome:?}");
        assert_eq!(lease_reads_stopped(&a), 1, "the metrics do not count the stop of lease reads");
        sleep_ms(1).await;
        let puts = store.manifest_puts.load(SeqCst) - puts_before_commit;
        assert_eq!(puts, 3, "want a failed PUT, a rejected PUT, and a lease-only PUT");
        let stored = store.stored_manifest_etag("d").unwrap();
        assert_eq!(db_a.leased_view().map(|v| v.etag), Some(stored), "the lease-only PUT did not give A its lease");
    });
}

/// Returns the `lease_reads_stopped` metric of `e`.
fn lease_reads_stopped(e: &Engine) -> u64 {
    let mut metrics = baserunna_api::Metrics::default();
    e.shared.metrics.fill(&mut metrics);
    metrics.commits.lease_reads_stopped
}

/// Tests that a failed manifest `PUT` after the lease ended does not count as stopped lease reads.
#[test]
fn lease_reads_stopped_ignores_a_failed_put_after_the_lease_ended() {
    rt().block_on(async {
        let (a, _b, store) = late_manifest_put_nodes();
        let db_a = opened(&a, "d").await;
        put(&db_a, "k", "old").await;
        sleep_ms(6_000).await;
        assert_eq!(db_a.leased_view(), None, "setup: A's lease ended");
        store.failed_manifest_puts.store(1, SeqCst);
        put(&db_a, "k", "new").await;
        assert_eq!(lease_reads_stopped(&a), 0, "A served no lease reads before the failed PUT");
    });
}

/// Tests that a lost manifest CAS on an unleased node does not count as stopped lease reads.
#[test]
fn lease_reads_stopped_ignores_a_lost_cas_without_a_lease() {
    rt().block_on(async {
        let (a, b, _store) = late_manifest_put_nodes();
        let (db_a, db_b) = (opened(&a, "d").await, b.db("d"));
        put(&db_a, "k", "old").await;
        db_b.open(false).await.unwrap();
        sleep_ms(6_000).await;
        put(&db_a, "k", "new").await;
        let snapshot = Consistency::Snapshot { max_staleness_ms: 600_000 };
        let mut t = db_b.begin(Isolation::Serializable, snapshot).await.unwrap();
        t.put(Bytes::from_static(b"j"), inline("b")).unwrap();
        let outcome = db_b.commit_detailed(t.into_request()).await;
        assert!(matches!(outcome, Err(Error::NotOwner { .. })), "setup: B's commit answered {outcome:?}");
        assert_eq!(lease_reads_stopped(&b), 0, "B served no lease reads before its CAS was lost");
    });
}

/// How [`strong_read_after_late_manifest_put_from_an_earlier_batch`] drops A's database handle
/// after the failed commit.
#[derive(Clone, Copy, Debug)]
enum IdleHandleDrop {
    Kept,
    Evicted { idle_ms: u64 },
    UnloadedForMemory,
}

/// Drops `db`, and lets `engine` drop its handle according to `how` if the handle is idle. Returns
/// the handle for the database, and whether `engine` dropped the old handle.
async fn drop_idle_handle(engine: &Engine, db: Arc<Db>, how: IdleHandleDrop) -> (Arc<Db>, bool) {
    let (id, old) = (db.id.clone(), Arc::downgrade(&db));
    drop(db);
    match how {
        IdleHandleDrop::Kept => {}
        IdleHandleDrop::Evicted { idle_ms } => {
            sleep_ms(idle_ms).await;
            engine.evict_idle(idle_ms);
        }
        IdleHandleDrop::UnloadedForMemory => {
            let over_budget = memory::Charge::new(&engine.shared.memory, engine.shared.memory.limit() + 1);
            engine.unload_idle();
            drop(over_budget);
        }
    }
    (engine.db(&id), old.upgrade().is_none())
}

/// Tests strong reads if the retry read of a failed commit finds a late manifest write from an
/// earlier batch. Other nodes time the lease in that write from their first read of it. Therefore,
/// the holder must not time the lease from the later failed `PUT`, also after the holder dropped
/// its handle in between. The holder takes a new lease only from a `PUT` that succeeds, which B
/// also reads. B commits as soon as it reads no valid lease of A.
fn strong_read_after_late_manifest_put_from_an_earlier_batch(handle_drop: IdleHandleDrop) {
    rt().block_on(async {
        let (a, b, store) = late_manifest_put_nodes();
        let (db_a, db_b) = (opened(&a, "d").await, b.db("d"));
        put(&db_a, "k", "old").await;
        store.failed_manifest_puts.store(a.shared.cfg.commit_retry_max, SeqCst);
        let outcome = commit_new(db_a.clone()).await;
        assert!(
            matches!(outcome, Err(Error::Unavailable { outcome_unknown: true, .. })),
            "setup: answered {outcome:?}"
        );
        let (db_a, dropped) = drop_idle_handle(&a, db_a, handle_drop).await;
        assert_eq!(dropped, !matches!(handle_drop, IdleHandleDrop::Kept), "setup: {handle_drop:?} dropped {dropped}");
        let mut t = db_a.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        assert!(t.get(b"k").await.unwrap().is_some());
        let applied = store.apply_late_puts();
        assert!(applied[0].is_ok(), "setup: the first late write is applied, got {applied:?}");
        db_b.open(false).await.unwrap();
        assert_eq!(strong_get(&db_b, "k").await, Some(inline("new")), "setup: B reads the late write and A's lease");
        sleep_ms(3000).await;
        store.failed_manifest_puts.store(1, SeqCst);
        t.put(Bytes::from_static(b"k"), inline("mine")).unwrap();
        let outcome = db_a.commit_detailed(t.into_request()).await;
        assert!(outcome.is_err(), "setup: A's commit on the old manifest answered {outcome:?}");
        sleep_ms(2600).await;
        put_when_owner(&db_b, "k", "newer").await;
        let got = strong_get(&db_a, "k").await;
        assert_eq!(got, Some(inline("newer")), "A served a strong read from cache after B committed");
    });
}

#[test]
fn strong_read_after_late_manifest_put_from_an_earlier_batch_is_new() {
    strong_read_after_late_manifest_put_from_an_earlier_batch(IdleHandleDrop::Kept);
}

#[test]
fn strong_read_after_late_manifest_put_from_before_a_handle_eviction_is_new() {
    strong_read_after_late_manifest_put_from_an_earlier_batch(IdleHandleDrop::Evicted { idle_ms: 0 });
}

#[test]
fn strong_read_after_late_manifest_put_from_before_an_idle_eviction_is_new() {
    strong_read_after_late_manifest_put_from_an_earlier_batch(IdleHandleDrop::Evicted { idle_ms: 1000 });
}

#[test]
fn strong_read_after_late_manifest_put_from_before_a_memory_unload_is_new() {
    strong_read_after_late_manifest_put_from_an_earlier_batch(IdleHandleDrop::UnloadedForMemory);
}

async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

/// Starts two nodes, and commits `new` on A with all manifest writes failing and buffered.
async fn unknown_commit_outcome_nodes() -> (Arc<Db>, Arc<LateManifestPutStore>, Arc<Engine>) {
    let (a, _b, store) = late_manifest_put_nodes();
    let db_a = opened(&a, "d").await;
    put(&db_a, "k", "old").await;
    store.failed_manifest_puts.store(a.shared.cfg.commit_retry_max, SeqCst);
    let outcome = commit_new(db_a.clone()).await;
    assert!(matches!(outcome, Err(Error::Unavailable { outcome_unknown: true, .. })), "setup: answered {outcome:?}");
    (db_a, store, a)
}

/// Returns the number of manifest `GET` and `PUT` requests that `f` sends to `store`, and its output.
async fn manifest_requests<T>(store: &LateManifestPutStore, f: impl Future<Output = T>) -> (u32, u32, T) {
    let (gets, puts) = (store.manifest_gets.load(SeqCst), store.manifest_puts.load(SeqCst));
    let out = f.await;
    (store.manifest_gets.load(SeqCst) - gets, store.manifest_puts.load(SeqCst) - puts, out)
}

fn assert_late_puts_rejected(store: &LateManifestPutStore) {
    let applied = store.apply_late_puts();
    assert!(!applied.is_empty(), "setup: no late write was buffered");
    assert!(
        applied.iter().all(|r| matches!(r, Err(StoreError::PreconditionFailed))),
        "a late write was not rejected: {applied:?}"
    );
}

/// Tests that A takes no lease from a manifest that a failed `PUT` wrote, also after A dropped the
/// handle that sent the `PUT`. A validates each strong read against storage until a manifest `PUT`
/// from A succeeds.
fn landed_manifest_put_gives_no_lease(handle_drop: IdleHandleDrop) {
    rt().block_on(async {
        let (db_a, store, a) = unknown_commit_outcome_nodes().await;
        let (db_a, dropped) = drop_idle_handle(&a, db_a, handle_drop).await;
        assert_eq!(dropped, !matches!(handle_drop, IdleHandleDrop::Kept), "setup: {handle_drop:?} dropped {dropped}");
        let applied = store.apply_late_puts();
        assert!(applied[0].is_ok(), "setup: the first late write is applied, got {applied:?}");
        for read in 0..3 {
            sleep_ms(1).await;
            let (gets, puts, value) = manifest_requests(&store, strong_get(&db_a, "k")).await;
            assert_eq!(value, Some(inline("new")), "strong read {read} did not return the late write");
            assert_eq!((gets, puts), (1, 0), "strong read {read} sent (manifest GETs, PUTs) = {:?}", (gets, puts));
            assert_eq!(db_a.leased_view(), None, "strong read {read} took the lease from the late write");
        }
        let holder = db_a.current().unwrap().m.lease.as_ref().map(|l| l.node.clone());
        assert_eq!(holder.as_ref(), Some(&a.shared.node_id), "setup: the late write contains A's lease");
        put(&db_a, "k", "newer").await;
        let stored = store.stored_manifest_etag("d").unwrap();
        assert_eq!(db_a.leased_view().map(|v| v.etag), Some(stored), "a successful PUT did not give A its lease");
    });
}

#[test]
fn landed_manifest_put_gives_no_lease_to_its_handle() {
    landed_manifest_put_gives_no_lease(IdleHandleDrop::Kept);
}

#[test]
fn landed_manifest_put_gives_no_lease_after_a_handle_eviction() {
    landed_manifest_put_gives_no_lease(IdleHandleDrop::Evicted { idle_ms: 0 });
}

#[test]
fn landed_manifest_put_gives_no_lease_after_an_idle_eviction() {
    landed_manifest_put_gives_no_lease(IdleHandleDrop::Evicted { idle_ms: 1000 });
}

#[test]
fn landed_manifest_put_gives_no_lease_after_a_memory_unload() {
    landed_manifest_put_gives_no_lease(IdleHandleDrop::UnloadedForMemory);
}

/// Tests the store requests that strong reads and a commit send after an unknown commit outcome.
#[test]
fn strong_reads_after_unknown_outcome_read_the_manifest_until_a_commit() {
    rt().block_on(async {
        let (db_a, store, _a) = unknown_commit_outcome_nodes().await;
        for read in 0..3 {
            sleep_ms(500).await;
            let (gets, puts, value) = manifest_requests(&store, strong_get(&db_a, "k")).await;
            assert_eq!(value, Some(inline("old")), "setup: the late write was not applied yet");
            assert_eq!((gets, puts), (1, 0), "strong read {read} sent (manifest GETs, PUTs) = {:?}", (gets, puts));
            assert_eq!(db_a.leased_view(), None, "strong read {read} restored lease reads");
        }
        sleep_ms(500).await;
        let (gets, puts, _) = manifest_requests(&store, put(&db_a, "k", "after")).await;
        assert_eq!((gets, puts), (1, 1), "the commit sent (manifest GETs, PUTs) = {:?}", (gets, puts));
        let stored = store.stored_manifest_etag("d").unwrap();
        assert_eq!(
            db_a.leased_view().map(|v| (v.etag, v.write_in_flight)),
            Some((stored, false)),
            "lease reads stay off"
        );
        let (gets, puts, value) = manifest_requests(&store, strong_get(&db_a, "k")).await;
        assert_eq!(value, Some(inline("after")));
        assert_eq!((gets, puts), (0, 0), "a strong read after the commit went to the store");
        assert_late_puts_rejected(&store);
    });
}

/// Tests the store requests that hot strong reads send after an unknown commit outcome.
#[test]
fn hot_strong_reads_after_unknown_outcome_renew_the_lease_once() {
    rt().block_on(async {
        let (db_a, store, a) = unknown_commit_outcome_nodes().await;
        let mut gets_per_read = Vec::new();
        let (background_gets, puts, ()) = manifest_requests(&store, async {
            for _ in 0..16 {
                sleep_ms(10).await;
                let (gets, puts, value) = manifest_requests(&store, strong_get(&db_a, "k")).await;
                assert_eq!(value, Some(inline("old")), "setup: the late write was not applied yet");
                assert_eq!(puts, 0, "a strong read sent a manifest PUT");
                gets_per_read.push(gets);
            }
            sleep_ms(10).await;
        })
        .await;
        let background_gets = background_gets - gets_per_read.iter().sum::<u32>();
        let from_store = gets_per_read.iter().take_while(|g| **g == 1).count();
        eprintln!("manifest GETs per read {gets_per_read:?}, background GETs {background_gets}, PUTs {puts}");
        assert_eq!(puts, 1, "the hot reads sent {puts} manifest PUTs, want one lease renewal");
        assert_eq!(background_gets, 0, "the lease renewal sent {background_gets} manifest GETs");
        assert!(
            from_store as u32 <= a.shared.cfg.lease_read_threshold
                && from_store < gets_per_read.len()
                && gets_per_read[from_store..].iter().all(|g| *g == 0),
            "manifest GETs per read: {gets_per_read:?}; want one per read until the renewal, then none"
        );
        let stored = store.stored_manifest_etag("d").unwrap();
        assert_eq!(
            db_a.leased_view().map(|v| (v.etag, v.write_in_flight)),
            Some((stored, false)),
            "lease reads stay off"
        );
        assert_late_puts_rejected(&store);
        let (gets, _, value) = manifest_requests(&store, strong_get(&db_a, "k")).await;
        assert_eq!((gets, value), (0, Some(inline("old"))), "a strong read after the renewal went to the store");
    });
}

/// Tests the manifest `PUT`s and the latency of commits while the store applies each manifest
/// `PUT` and loses its reply. Each commit sends its own `PUT` and at most one lease-only `PUT`.
#[test]
fn commits_send_at_most_two_manifest_puts_each_while_replies_are_lost() {
    for gap_ms in [0, 2_000] {
        rt().block_on(async {
            let (eng, store) = delayed_engine("a");
            let db = opened(&eng, "d").await;
            put(&db, "k", "0").await;
            store.put_reply_ms.store(20, SeqCst);
            store.get_reply_ms.store(20, SeqCst);
            store.manifest_put_lost.store(u32::MAX, SeqCst);
            let puts_before = store.manifest_puts.load(SeqCst);
            let mut latency_ms = Vec::new();
            for i in 0..5 {
                let started = Instant::now();
                put(&db, "k", &format!("v{i}")).await;
                latency_ms.push(started.elapsed().as_millis());
                sleep_ms(gap_ms).await;
            }
            sleep_ms(eng.shared.cfg.read_lease_ms).await;
            let puts = store.manifest_puts.load(SeqCst) - puts_before;
            eprintln!("{gap_ms} ms between commits: {puts} manifest PUTs, latency ms {latency_ms:?}");
            assert!(puts <= 10, "{gap_ms} ms between commits: 5 commits sent {puts} manifest PUTs");
            assert!(
                latency_ms.iter().all(|ms| *ms <= 150),
                "{gap_ms} ms between commits: latency ms {latency_ms:?}, want at most 150 ms each"
            );
        });
    }
}

/// Tests that hot strong reads on a node without its lease send a bounded number of lease-only
/// `PUT`s while the store applies each manifest `PUT` and loses its reply.
#[test]
fn hot_strong_reads_pause_lease_puts_while_replies_are_lost() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let cfg = eng.shared.cfg.clone();
        let db = opened(&eng, "d").await;
        put(&db, "k", "0").await;
        sleep_ms(cfg.lease_ms).await;
        assert_eq!(db.leased_view(), None, "setup: the lease ended");
        store.put_reply_ms.store(20, SeqCst);
        store.get_reply_ms.store(20, SeqCst);
        store.manifest_put_lost.store(u32::MAX, SeqCst);
        let puts_before = store.manifest_puts.load(SeqCst);
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(3) {
            sleep_ms(10).await;
            assert_eq!(strong_get(&db, "k").await, Some(inline("0")));
        }
        let puts = store.manifest_puts.load(SeqCst) - puts_before;
        let max_puts = 1 + 3_000 / (cfg.lease_ms / 5);
        assert!(
            puts <= max_puts as u32,
            "3 s of hot strong reads sent {puts} lease-only PUTs, want at most {max_puts}"
        );
    });
}

/// Step of a [`run_lease_steps`] run.
#[derive(Clone, Copy, Debug)]
enum LeaseStep {
    Commit,
    /// Commits on A from a snapshot read, so the commit can start from an old cached manifest.
    CommitFromSnapshot,
    CommitOnB,
    AcquireLease,
    CommitWithLease,
    StrongRead {
        on_b: bool,
        waits_for_write: bool,
    },
    ApplyLatePut {
        pick: usize,
    },
    /// Applies the buffered writes before the next manifest write.
    ApplyLatePutsOnNextPut,
    /// Drops A's handle according to `how`, and opens a new handle.
    DropHandle {
        how: IdleHandleDrop,
    },
    Advance {
        ms: u64,
    },
}

/// Generates `n` steps. Each seed disables a random subset of the step kinds other than `Advance`.
/// This produces long step sequences that omit some step kinds, such as interfering writes.
fn gen_lease_steps(r: &mut Rng, n: usize) -> Vec<LeaseStep> {
    let mut weights = [8.0, 4.0, 6.0, 8.0, 12.0, 27.0, 10.0, 5.0, 5.0, 20.0];
    let advance = weights.len() - 1;
    for w in &mut weights[..advance] {
        if r.chance(0.5) {
            *w = 0.0;
        }
    }
    (0..n)
        .map(|_| match r.weighted(&weights) {
            Some(0) => LeaseStep::Commit,
            Some(1) => LeaseStep::CommitFromSnapshot,
            Some(2) => LeaseStep::CommitOnB,
            Some(3) => LeaseStep::AcquireLease,
            Some(4) => LeaseStep::CommitWithLease,
            Some(5) => LeaseStep::StrongRead { on_b: r.chance(0.3), waits_for_write: r.chance(0.5) },
            Some(6) => LeaseStep::ApplyLatePut { pick: r.usize(1 << 16) },
            Some(7) => LeaseStep::ApplyLatePutsOnNextPut,
            Some(8) => LeaseStep::DropHandle {
                how: match r.chance(0.5) {
                    true => IdleHandleDrop::Evicted { idle_ms: 0 },
                    false => IdleHandleDrop::UnloadedForMemory,
                },
            },
            _ => LeaseStep::Advance { ms: (6_000f64.powf(r.unit()) as u64).clamp(1, 6_000) },
        })
        .collect()
}

/// Enables buggify on the current thread until dropped.
struct BuggifyScope;

impl BuggifyScope {
    fn enable(seed: u64, p_active: f64) -> BuggifyScope {
        crate::buggify::enable(seed, crate::buggify::Config { p_active, ..Default::default() });
        BuggifyScope
    }
}

impl Drop for BuggifyScope {
    fn drop(&mut self) {
        crate::buggify::disable();
    }
}

/// Node name and database handle that [`check_lease_views`] checks.
type NodeDb<'a> = (&'a str, &'a Arc<Db>);

/// Checks each node's lease view with [`check_lease_view`].
fn check_lease_views(dbs: &[NodeDb], store: &LateManifestPutStore) -> Result<(), String> {
    for (node, db) in dbs {
        check_lease_view(db, store).map_err(|e| format!("{node} {e}"))?;
    }
    Ok(())
}

/// Tests that the lease oracle reports a holder that relies on its lease after the lease term. The
/// test moves the instant at which the store applied the holder's manifest 100 ms earlier.
#[test]
fn lease_oracle_reports_a_lease_that_outlives_its_term() {
    rt().block_on(async {
        let (a, _b, store) = late_manifest_put_nodes();
        let db_a = opened(&a, "d").await;
        put(&db_a, "k", "v").await;
        let view = db_a.leased_view().expect("setup: A holds its lease");
        assert_eq!(check_lease_view(&db_a, &*store), Ok(()), "setup: the oracle reports a valid lease");
        let applied_at = store.manifest_applied_at("d", &view.etag).expect("setup: the store applied the manifest");
        store.manifest_applied_at.lock().insert(view.etag.clone(), applied_at - Duration::from_millis(100));
        let overrun = format!("relies on its lease on d at write {} for 100 ms after the lease term", view.write_seq);
        assert_eq!(check_lease_view(&db_a, &*store), Err(overrun));
    });
}

/// Advances time by `ms` in steps of at most `every_ms`, and checks the lease views after each step.
async fn advance_checking(
    ms: u64,
    every_ms: u64,
    dbs: &[NodeDb<'_>],
    store: &LateManifestPutStore,
) -> Result<(), String> {
    let mut left = ms;
    while left > 0 {
        let step = left.min(every_ms);
        sleep_ms(step).await;
        left -= step;
        check_lease_views(dbs, store)?;
    }
    Ok(())
}

/// Checks that a strong read on `db` returns a manifest that the store held during the read.
async fn check_strong_read(db: &Arc<Db>, waits_for_write: bool, store: &LateManifestPutStore) -> Result<(), String> {
    // Paused time does not advance between steps. Advance 1 ms so the read starts at a later
    // instant than earlier store changes.
    sleep_ms(1).await;
    let first = store.stored_manifest_etags.lock().len() - 1;
    let (snap, write_in_flight) = match waits_for_write {
        true => (db.snapshot(Consistency::Strong).await, false),
        false => match db.latest_snapshot().await {
            Ok((s, w)) => (Ok(s), w),
            Err(e) => (Err(e), false),
        },
    };
    let snap = snap.map_err(|e| format!("strong read failed: {e}"))?;
    let held = store.stored_manifest_etags.lock()[first..].to_vec();
    if !write_in_flight && !held.contains(&snap.version.etag) {
        return Err(format!(
            "strong read returned write {} ({}), but the store held only {held:?} during the read",
            snap.version.m.write_seq, snap.version.etag
        ));
    }
    Ok(())
}

fn spawn_commit(db: &Arc<Db>, value: String, with_lease: bool) -> tokio::task::JoinHandle<()> {
    let db = db.clone();
    tokio::spawn(async move {
        let Ok(mut t) = db.begin(Isolation::Serializable, Consistency::Strong).await else { return };
        t.put(Bytes::from_static(b"k"), Value::Inline(Bytes::from(value))).unwrap();
        let req = t.into_request();
        match with_lease {
            true => drop(tokio::join!(db.acquire_lease(), db.commit_detailed(req))),
            false => drop(db.commit_detailed(req).await),
        }
    })
}

/// Manifest `PUT` outcomes of a [`run_lease_steps`] run.
enum LeaseRunOutcomes {
    Random(ManifestFaultScript),
    /// Outcomes that manifest writes take in turn.
    Cycled(Vec<ManifestPutOutcome>),
}

/// Setup of a [`run_lease_steps`] run.
struct LeaseRun {
    seed: u64,
    cfg: EngineConfig,
    outcomes: LeaseRunOutcomes,
    /// Milliseconds to advance, with lease checks, after each step that starts background work.
    ms_after_spawn: u64,
    /// Interval in milliseconds between lease checks while time advances.
    check_every_ms: u64,
}

impl LeaseRun {
    /// Returns a setup whose manifest writes fail at random.
    fn random(seed: u64) -> LeaseRun {
        let mut r = Rng::new(seed ^ 0x1ea5e);
        // Two attempts make it likely that every manifest `PUT` of a batch fails, and that the
        // store applies one of them later.
        let cfg = EngineConfig { commit_retry_max: *r.pick(&[2, 8]), ..EngineConfig::default() };
        let outcomes = LeaseRunOutcomes::Random(ManifestFaultScript::random(&mut r));
        LeaseRun { seed, cfg, outcomes, ms_after_spawn: 0, check_every_ms: 1 }
    }
}

/// Runs `steps` on two nodes, A and B, after A commits once and holds its lease. Returns the failing
/// step and the broken invariant.
async fn run_lease_steps(run: LeaseRun, steps: &[LeaseStep]) -> Result<(), String> {
    let (a, b, store) = late_manifest_put_nodes_with(run.seed, run.cfg);
    let (mut db_a, db_b) = (opened(&a, "d").await, b.db("d"));
    put(&db_a, "k", "old").await;
    db_b.open(false).await.map_err(|e| e.to_string())?;
    match run.outcomes {
        LeaseRunOutcomes::Random(script) => *store.manifest_fault_script.lock() = Some(script),
        LeaseRunOutcomes::Cycled(outcomes) => *store.cycled_manifest_put_outcomes.lock() = Some((outcomes, 0)),
    }
    let mut tasks = Vec::new();
    for (i, step) in steps.iter().enumerate() {
        if let LeaseStep::DropHandle { how } = *step {
            db_a = drop_idle_handle(&a, db_a, how).await.0;
            let _ = db_a.open(false).await;
        }
        let dbs: [NodeDb; 2] = [("A", &db_a), ("B", &db_b)];
        let spawned = tasks.len();
        let r = match *step {
            LeaseStep::Commit => {
                tasks.push(spawn_commit(&db_a, format!("v{i}"), false));
                Ok(())
            }
            LeaseStep::CommitWithLease => {
                tasks.push(spawn_commit(&db_a, format!("v{i}"), true));
                Ok(())
            }
            LeaseStep::CommitFromSnapshot => {
                let db = db_a.clone();
                tasks.push(tokio::spawn(async move {
                    let snapshot = Consistency::Snapshot { max_staleness_ms: 600_000 };
                    let Ok(mut t) = db.begin(Isolation::Serializable, snapshot).await else { return };
                    let _ = t.get(b"k").await;
                    t.put(Bytes::from_static(b"k"), Value::Inline(Bytes::from(format!("s{i}")))).unwrap();
                    drop(db.commit_detailed(t.into_request()).await);
                }));
                Ok(())
            }
            LeaseStep::CommitOnB => {
                tasks.push(spawn_commit(&db_b, format!("b{i}"), false));
                Ok(())
            }
            LeaseStep::AcquireLease => {
                let db = db_a.clone();
                tasks.push(tokio::spawn(async move { drop(db.acquire_lease().await) }));
                Ok(())
            }
            LeaseStep::StrongRead { on_b, waits_for_write } => {
                check_strong_read(if on_b { &db_b } else { &db_a }, waits_for_write, &store).await
            }
            LeaseStep::ApplyLatePut { pick } => {
                let n = store.late_puts.lock().len();
                if n > 0 {
                    store.apply_late_put(pick % n);
                }
                Ok(())
            }
            LeaseStep::ApplyLatePutsOnNextPut => {
                store.apply_late_puts_on_next_put.store(true, SeqCst);
                Ok(())
            }
            LeaseStep::DropHandle { .. } => Ok(()),
            LeaseStep::Advance { ms } => advance_checking(ms, run.check_every_ms, &dbs, &store).await,
        };
        let r = match r.is_ok() && tasks.len() > spawned {
            true => advance_checking(run.ms_after_spawn, run.check_every_ms, &dbs, &store).await,
            false => r,
        };
        r.and_then(|()| check_lease_views(&dbs, &store)).map_err(|e| format!("step {i} {step:?}: {e}"))?;
    }
    let dbs: [NodeDb; 2] = [("A", &db_a), ("B", &db_b)];
    let mut settled_ms = 0;
    while tasks.iter().any(|t| !t.is_finished()) {
        if settled_ms > 120_000 {
            return Err("background work did not finish within 120 s".into());
        }
        advance_checking(10, run.check_every_ms, &dbs, &store).await.map_err(|e| format!("after the steps: {e}"))?;
        settled_ms += 10;
    }
    for t in tasks {
        t.await.map_err(|e| format!("a task failed: {e}"))?;
    }
    for (node, db) in dbs {
        check_strong_read(db, true, &store).await.map_err(|e| format!("final read on {node}: {e}"))?;
    }
    Ok(())
}

/// Tests that lease-served strong reads match the stored manifest, and that each lease holder
/// stops relying on its lease before its lease term ends. Manifest writes fail, apply after an
/// error, apply and return `412`, or apply late with their original precondition.
#[test]
fn prop_lease_reads_match_the_store_under_ambiguous_manifest_puts() {
    let cases = prop::env_u64("PROP_CASES").unwrap_or(LEASE_PROP_CASES);
    let first = prop::env_u64("PROP_SEED").unwrap_or(0);
    let mut failures = Vec::new();
    for seed in first..first + cases {
        let steps = gen_lease_steps(&mut Rng::new(seed), LEASE_PROP_STEPS);
        let _buggify = BuggifyScope::enable(seed, [0.0, 0.5, 1.0][seed as usize % 3]);
        if let Err(e) = rt().block_on(run_lease_steps(LeaseRun::random(seed), &steps)) {
            failures.push(format!("PROP_SEED={seed} PROP_CASES=1: {e}"));
        }
    }
    assert!(failures.is_empty(), "{} of {cases} seeds failed:\n{}", failures.len(), failures.join("\n"));
}

const LEASE_PROP_CASES: u64 = 256;
const LEASE_PROP_STEPS: usize = 200;

/// Steps of [`lease_search`]. Each step that starts a commit or a lease request also advances
/// time by 60 ms, so the work reaches the store before the next step.
const LEASE_SEARCH_STEPS: [LeaseStep; 12] = [
    LeaseStep::ApplyLatePutsOnNextPut,
    LeaseStep::Commit,
    LeaseStep::CommitFromSnapshot,
    LeaseStep::CommitOnB,
    LeaseStep::AcquireLease,
    LeaseStep::StrongRead { on_b: false, waits_for_write: true },
    LeaseStep::ApplyLatePut { pick: 0 },
    LeaseStep::DropHandle { how: IdleHandleDrop::Evicted { idle_ms: 0 } },
    LeaseStep::Advance { ms: 1_100 },
    LeaseStep::Advance { ms: 3_000 },
    LeaseStep::Advance { ms: 4_600 },
    LeaseStep::Advance { ms: 5_600 },
];

/// Runs every sequence of [`LEASE_SEARCH_STEPS`] up to `LEASE_SEARCH_DEPTH` steps (default 4) on
/// two nodes. Manifest writes take `outcomes` in turn, and the search runs with 2 and 3 commit
/// attempts.
///
/// Limits: the search drives one database on two nodes, drops handles only by eviction, and checks
/// the lease views between steps and every 20 ms. The search does not interleave steps inside a
/// commit batch, so the property test finds defects that the search misses. Run depth 5 (about
/// 100 s on 16 cores) with `LEASE_SEARCH_DEPTH=5 cargo test -p baserunna --lib lease_search`.
fn lease_search(outcomes: &[ManifestPutOutcome]) {
    let depth = prop::env_u64("LEASE_SEARCH_DEPTH").unwrap_or(4) as u32;
    let total = LEASE_SEARCH_STEPS.len().pow(depth);
    let mut failures = Vec::new();
    for commit_retry_max in [2, 3] {
        for code in 0..total {
            let steps: Vec<LeaseStep> = (0..depth as usize)
                .map(|i| LEASE_SEARCH_STEPS[code / LEASE_SEARCH_STEPS.len().pow(i as u32) % LEASE_SEARCH_STEPS.len()])
                .collect();
            let run = LeaseRun {
                seed: 7,
                cfg: EngineConfig { commit_retry_max, ..EngineConfig::default() },
                outcomes: LeaseRunOutcomes::Cycled(outcomes.to_vec()),
                ms_after_spawn: 60,
                check_every_ms: 20,
            };
            if let Err(e) = rt().block_on(run_lease_steps(run, &steps)) {
                failures.push(format!("commit_retry_max {commit_retry_max}, {steps:?}: {e}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} sequences failed:\n{}",
        failures.len(),
        2 * total,
        failures.iter().take(5).cloned().collect::<Vec<_>>().join("\n")
    );
}

/// Defines a test that runs [`lease_search`] with each outcome cycle.
macro_rules! lease_search_tests {
    ($($name:ident: [$($outcome:ident),*],)*) => {
        $(
            #[test]
            fn $name() {
                use ManifestPutOutcome::*;
                lease_search(&[$($outcome),*]);
            }
        )*
    };
}

lease_search_tests! {
    lease_search_late: [FailedThenAppliedLate],
    lease_search_late_then_applied: [FailedThenAppliedLate, Applied],
    lease_search_lost_reply: [AppliedThenFailed],
    lease_search_lost_reply_then_applied: [AppliedThenFailed, Applied],
    lease_search_two_late_then_applied: [FailedThenAppliedLate, FailedThenAppliedLate, Applied],
    lease_search_applied_then_late: [Applied, FailedThenAppliedLate],
    lease_search_late_then_lost_reply: [FailedThenAppliedLate, AppliedThenFailed],
    lease_search_two_applied_then_late: [Applied, Applied, FailedThenAppliedLate],
    lease_search_rejected_then_applied: [AppliedThenRejected, Applied],
    lease_search_late_then_rejected: [FailedThenAppliedLate, AppliedThenRejected],
}

#[test]
fn obsolete_segments_outlive_snapshots_then_go() {
    rt().block_on(async {
        let (a, store) = delayed_engine("a");
        let dbs: DynStore = store.clone();
        let b = Engine::new(dbs, a.shared.env.clone(), EngineConfig::small_for_tests(), "b".into());
        let db = a.db("d");
        db.open(true).await.unwrap();
        let v500 = "v".repeat(500);
        let mut old = None;
        for round in 0..8 {
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            for i in 0..5 {
                let val = format!("{round}{v500}");
                t.put(Bytes::from(format!("k{i}")), Value::Inline(Bytes::from(val))).unwrap();
            }
            db.commit(t.into_request()).await.unwrap();
            if round == 2 {
                old = Some(b.db("d").begin(Isolation::Snapshot, Consistency::Strong).await.unwrap());
            }
        }
        let mut old = old.unwrap();
        let old_segs: Vec<String> = old.snapshot().m.segments.iter().map(|s| s.id.clone()).collect();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let live: Vec<String> = db.current().unwrap().m.segments.iter().map(|s| s.id.clone()).collect();
        assert!(old_segs.iter().any(|s| !live.contains(s)), "setup: compaction replaced nothing");
        let grace = a.shared.cfg.gc_grace_ms;
        tokio::time::sleep(Duration::from_millis(grace / 2)).await;
        db.maintenance_now().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let got = old.get(b"k0").await.expect("an old snapshot lost its segments within gc_grace").unwrap();
        assert_eq!(got.value, Value::Inline(Bytes::from(format!("2{v500}"))));
        tokio::time::sleep(Duration::from_millis(grace)).await;
        db.maintenance_now().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let keys = store.mem.keys();
        for s in old_segs.iter().filter(|s| !live.contains(s)) {
            assert!(!keys.contains(&seg_key("d", s)), "obsolete segment {s} survived gc_grace");
        }
    });
}

#[test]
fn sweep_collects_only_old_orphans() {
    for (orphan_grace_ms, blob_stage_ttl_ms) in [(600_000, 1_800_000), (1_800_000, 600_000)] {
        rt().block_on(sweep_case(EngineConfig {
            orphan_grace_ms,
            blob_stage_ttl_ms,
            ..EngineConfig::small_for_tests()
        }));
    }
}

async fn sweep_case(cfg: EngineConfig) {
    let (eng, store) = delayed_engine_with("a", cfg.clone());
    let db = eng.db("d");
    db.open(true).await.unwrap();
    put(&db, "k", "1").await;
    let seg = eng.shared.env.random_hex(16);
    let blob = super::types::make_blob_id(eng.shared.env.now_ms(), &[3u8; 10]);
    let foreign = "dbs/d/seg/not-a-segment".to_string();
    for k in [seg_key("d", &seg), blob_chunk_key("d", &blob, 0), foreign.clone()] {
        store.mem.put_sync(&k, Bytes::from_static(b"x"), &PutMode::Overwrite).unwrap();
    }
    let listed =
        |db: &Arc<Db>| -> Vec<String> { db.current().unwrap().m.obsolete.iter().map(|o| o.id.clone()).collect() };
    db.sweep_now().await.unwrap();
    assert!(listed(&db).is_empty(), "young orphans were collected: {:?}", listed(&db));
    let first = cfg.orphan_grace_ms.min(cfg.blob_stage_ttl_ms) + 1;
    tokio::time::sleep(Duration::from_millis(first)).await;
    db.maintenance_now().await.unwrap();
    db.sweep_now().await.unwrap();
    let want: Vec<String> = if cfg.orphan_grace_ms < first { vec![seg.clone()] } else { vec![] };
    assert_eq!(listed(&db), want, "{cfg:?}");
    tokio::time::sleep(Duration::from_millis(cfg.orphan_grace_ms.max(cfg.blob_stage_ttl_ms))).await;
    db.maintenance_now().await.unwrap();
    db.sweep_now().await.unwrap();
    let l = listed(&db);
    assert!(l.contains(&blob) && l.iter().all(|id| *id == seg || *id == blob), "old blob not collected: {l:?}");
    tokio::time::sleep(Duration::from_millis(cfg.gc_grace_ms + 1)).await;
    db.maintenance_now().await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let keys = store.mem.keys();
    assert!(!keys.contains(&seg_key("d", &seg)) && !keys.iter().any(|k| k.contains(&blob)), "{keys:?}");
    assert!(keys.contains(&foreign), "an object that is not ours was deleted");
}

#[test]
fn pinned_blobs_outlive_collection_until_the_pin_lapses() {
    rt().block_on(async {
        let cfg = EngineConfig {
            orphan_grace_ms: 60_000,
            blob_stage_ttl_ms: 30_000,
            txn_max_lifetime_ms: 30_000,
            gc_grace_ms: 90_000,
            clock_sync_max_error_ms: 1_000,
            ..EngineConfig::small_for_tests()
        };
        let (eng, store) = delayed_engine_with("a", cfg.clone());
        let db = opened(&eng, "d").await;
        let r = blob::write_blob(&eng.shared, "d", &[7u8; 3000], "x/y").await.unwrap();
        for v in [Value::Blob(r.clone()), inline("small")] {
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            t.put(Bytes::from_static(b"k"), v).unwrap();
            db.commit(t.into_request()).await.unwrap();
        }
        let has_blob = || store.mem.keys().iter().any(|k| k.contains(&r.id) && k.contains("/blob/"));
        let listed = || db.current().unwrap().m.obsolete.iter().any(|o| o.id == r.id);
        tokio::time::sleep(Duration::from_millis(cfg.orphan_grace_ms + 4 * cfg.clock_sync_max_error_ms)).await;
        db.maintenance_now().await.unwrap();
        db.sweep_now().await.unwrap();
        assert!(listed(), "setup: the dereferenced blob was not found");
        let interval = blob::pin_interval_ms(&cfg);
        let mut waited = 0;
        while waited <= cfg.gc_grace_ms + interval {
            blob::pin(&eng.shared, "d", &r.id).await.unwrap();
            tokio::time::sleep(Duration::from_millis(interval)).await;
            db.maintenance_now().await.unwrap();
            waited += interval;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!listed(), "setup: collection did not run");
        assert!(has_blob(), "a pinned blob was deleted");
        assert_eq!(blob::read_range(&eng.shared, "d", &r, 0..3000).await.unwrap().len(), 3000);
        tokio::time::sleep(Duration::from_millis(blob::pin_ttl_ms(&cfg).max(cfg.orphan_grace_ms))).await;
        db.maintenance_now().await.unwrap();
        db.sweep_now().await.unwrap();
        assert!(listed());
        tokio::time::sleep(Duration::from_millis(cfg.gc_grace_ms + 1)).await;
        db.maintenance_now().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!has_blob(), "an unpinned orphan survived");
        assert!(!store.mem.keys().contains(&pin_key("d", &r.id)), "its pin was left behind");
    });
}

#[test]
fn collection_ignores_node_clock_skew() {
    rt().block_on(async {
        let cfg = EngineConfig::small_for_tests();
        let origin = tokio::time::Instant::now();
        let base = 1_700_000_000_000;
        let skewed = Arc::new(Env::sim(9, base, origin, 5 * 3_600_000));
        let mem = Arc::new(MemStore::new(Box::new(move || base + origin.elapsed().as_millis() as u64)));
        let eng = Engine::new(mem.clone(), skewed.clone(), cfg.clone(), "a".into());
        let db = opened(&eng, "d").await;
        put(&db, "k", "1").await;
        let seg = skewed.random_hex(16);
        let blob = super::types::make_blob_id(base, &[3u8; 10]);
        for k in [seg_key("d", &seg), blob_chunk_key("d", &blob, 0)] {
            mem.put_sync(&k, Bytes::from_static(b"x"), &PutMode::Overwrite).unwrap();
        }
        db.sweep_now().await.unwrap();
        assert!(db.current().unwrap().m.obsolete.is_empty(), "young orphans looked old to a fast clock");
        let store_now = eng.shared.store_now_ms().unwrap();
        assert!(store_now.abs_diff(base + origin.elapsed().as_millis() as u64) < 100, "store time is the store's");
        for i in 0..8 {
            put_many(&db, &format!("r{i}"), 5, 500).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let obsolete: Vec<String> = db
            .current()
            .unwrap()
            .m
            .obsolete
            .iter()
            .map(|o| match o.kind {
                manifest::ObjKind::Levels => levels_key("d", &o.id),
                _ => seg_key("d", &o.id),
            })
            .collect();
        assert!(!obsolete.is_empty(), "setup: compaction obsoleted nothing");
        tokio::time::sleep(Duration::from_millis(cfg.gc_grace_ms / 2)).await;
        db.maintenance_now().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        for key in &obsolete {
            assert!(mem.keys().contains(key), "{key} deleted within gc_grace");
        }
    });
}

#[test]
fn a_manifest_whose_levels_object_was_collected_is_read_again() {
    rt().block_on(async {
        let cfg = EngineConfig { levels_inline_max: 0, ..EngineConfig::small_for_tests() };
        let (a, store) = delayed_engine_with("a", cfg);
        let db = opened(&a, "d").await;
        let levels = || db.current().unwrap().m.levels.clone();
        let mut round = 0;
        let mut write_until = async |done: &dyn Fn(Option<String>) -> bool| {
            while !done(levels()) {
                assert!(round < 200, "setup: the levels object never changed");
                put_many(&db, &format!("r{round:03}-"), 5, 500).await;
                db.maintenance_now().await.unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
                round += 1;
            }
        };
        write_until(&|l| l.is_some()).await;
        let old = levels().unwrap();
        let stale = store.mem.get_sync(&manifest_key("d"), &GetOpts::default()).unwrap().data;
        write_until(&|l| l.is_some_and(|l| l != old)).await;
        store.mem.delete(&levels_key("d", &old)).await.unwrap();

        let b = second_node(&a, &store, "b");
        *store.stale_manifest.lock() = Some(stale.clone());
        let before = crate::probe::count(crate::probe::Probe::LevelsCollectedUnderRead);
        let v = b.db("d").refresh(Instant::now(), false).await.unwrap();
        assert_eq!(v.m.levels, levels(), "read the replacing manifest");
        assert_eq!(crate::probe::count(crate::probe::Probe::LevelsCollectedUnderRead), before + 1);

        let c = second_node(&a, &store, "c");
        let current = store.mem.get_sync(&manifest_key("d"), &GetOpts::default()).unwrap().data;
        store.mem.delete(&levels_key("d", levels().as_deref().unwrap())).await.unwrap();
        *store.stale_manifest.lock() = Some(current);
        let r = c.db("d").refresh(Instant::now(), false).await;
        assert!(matches!(r, Err(Error::Internal(_))), "{:?}", r.err());
    });
}

#[test]
fn orphaned_blobs_wait_out_both_graces_exactly() {
    for (orphan_grace_ms, blob_stage_ttl_ms) in [(600_000, 300_000), (300_000, 600_000)] {
        rt().block_on(async move {
            let cfg = EngineConfig { orphan_grace_ms, blob_stage_ttl_ms, ..EngineConfig::small_for_tests() };
            let (eng, store) = delayed_engine_with("a", cfg.clone());
            let db = opened(&eng, "d").await;
            put(&db, "k", "1").await;
            let start = Instant::now();
            let blob = super::types::make_blob_id(eng.shared.env.now_ms(), &[5u8; 10]);
            store.mem.put_sync(&blob_chunk_key("d", &blob, 0), Bytes::from_static(b"x"), &PutMode::Overwrite).unwrap();
            let due = Duration::from_millis(orphan_grace_ms.max(blob_stage_ttl_ms + 2 * cfg.clock_sync_max_error_ms));
            let listed_at = |at: Duration| {
                let db = db.clone();
                let blob = blob.clone();
                async move {
                    tokio::time::sleep_until(start + at).await;
                    db.maintenance_now().await.unwrap();
                    db.sweep_now().await.unwrap();
                    db.current().unwrap().m.obsolete.iter().any(|o| o.id == blob)
                }
            };
            assert!(!listed_at(due - Duration::from_secs(1)).await, "collectable a second early ({cfg:?})");
            assert!(listed_at(due + Duration::from_secs(1)).await, "not collectable a second late ({cfg:?})");
        });
    }
}

#[test]
fn a_blob_is_as_old_as_its_newest_object() {
    rt().block_on(async {
        let cfg = EngineConfig::small_for_tests();
        let (eng, store) = delayed_engine_with("a", cfg.clone());
        let db = opened(&eng, "d").await;
        put(&db, "k", "1").await;
        let blob = super::types::make_blob_id(eng.shared.env.now_ms(), &[6u8; 10]);
        for c in 0..3 {
            store.mem.put_sync(&blob_chunk_key("d", &blob, c), Bytes::from_static(b"x"), &PutMode::Overwrite).unwrap();
        }
        let wait = cfg.orphan_grace_ms.max(cfg.blob_stage_ttl_ms + 2 * cfg.clock_sync_max_error_ms);
        tokio::time::sleep(Duration::from_millis(wait)).await;
        blob::pin(&eng.shared, "d", &blob).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        db.maintenance_now().await.unwrap();
        db.sweep_now().await.unwrap();
        assert!(!db.current().unwrap().m.obsolete.iter().any(|o| o.id == blob), "a freshly pinned blob was collected");
    });
}

#[test]
fn batched_commits_validate_against_each_other() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = eng.db("d");
        db.open(true).await.unwrap();
        put(&db, "x", "0").await;
        let begin = || db.begin(Isolation::Serializable, Consistency::Strong);
        store.put_reply_ms.store(50, std::sync::atomic::Ordering::SeqCst);
        let mut filler = begin().await.unwrap();
        filler.put(Bytes::from_static(b"x"), Value::Inline(Bytes::from_static(b"1"))).unwrap();
        let mut writer = begin().await.unwrap();
        writer.put(Bytes::from_static(b"a/1"), Value::Inline(Bytes::from_static(b"w"))).unwrap();
        writer.put(Bytes::from_static(b"c"), Value::Inline(Bytes::from_static(b"w"))).unwrap();
        let mut scanner = begin().await.unwrap();
        assert!(scanner.scan(&KeyRange::prefix(b"a/"), false, 10).await.unwrap().is_empty());
        scanner.put(Bytes::from_static(b"b"), Value::Inline(Bytes::from_static(b"s"))).unwrap();
        let mut reader = begin().await.unwrap();
        assert!(reader.get(b"c").await.unwrap().is_none());
        reader.put(Bytes::from_static(b"d"), Value::Inline(Bytes::from_static(b"r"))).unwrap();
        let commit = |t: Txn| {
            let db = db.clone();
            tokio::spawn(async move { db.commit(t.into_request()).await })
        };
        let hs = [commit(filler), commit(writer), commit(scanner), commit(reader)];
        let mut out = Vec::new();
        for h in hs {
            out.push(h.await.unwrap());
        }
        let ws = db.current().unwrap().m.write_seq;
        assert!(out[0].is_ok() && out[1].is_ok(), "{out:?}");
        assert!(matches!(out[2], Err(crate::error::Error::Conflict(_))), "phantom in a batch: {:?}", out[2]);
        assert!(matches!(out[3], Err(crate::error::Error::Conflict(_))), "stale read in a batch: {:?}", out[3]);
        assert!(ws <= 3, "setup: the commits did not share a batch ({ws} manifest writes)");
    });
}

#[test]
fn expired_staged_blobs_cannot_be_bound() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = eng.db("d");
        db.open(true).await.unwrap();
        let fresh = blob::write_blob(&eng.shared, "d", b"fresh", "x/y").await.unwrap();
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.put(Bytes::from_static(b"f"), Value::Blob(fresh)).unwrap();
        db.commit(t.into_request()).await.unwrap();
        let stale = blob::write_blob(&eng.shared, "d", b"stale", "x/y").await.unwrap();
        tokio::time::sleep(Duration::from_millis(eng.shared.cfg.blob_stage_ttl_ms + 1)).await;
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.put(Bytes::from_static(b"s"), Value::Blob(stale)).unwrap();
        let e = db.commit(t.into_request()).await.expect_err("an expired staged blob was bound");
        assert!(e.message().contains("expired"), "{e}");
    });
}

#[test]
fn scans_see_through_own_deletes() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = eng.db("d");
        db.open(true).await.unwrap();
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        for i in 0..100 {
            t.put(Bytes::from(format!("k{i:03}")), Value::Inline(Bytes::from_static(b"v"))).unwrap();
        }
        db.commit(t.into_request()).await.unwrap();
        let keys = |v: Vec<ReadValue>| -> Vec<String> {
            v.iter().map(|e| String::from_utf8_lossy(&e.key).into_owned()).collect()
        };
        let names = |r: std::ops::Range<usize>| -> Vec<String> { r.map(|i| format!("k{i:03}")).collect() };
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        for i in (0..60).chain(65..99) {
            t.delete(Bytes::from(format!("k{i:03}"))).unwrap();
        }
        assert_eq!(keys(t.scan(&KeyRange::data(), false, 4).await.unwrap()), names(60..64));
        let want_rev: Vec<String> = ["k099", "k064", "k063", "k062"].iter().map(|s| s.to_string()).collect();
        assert_eq!(keys(t.scan(&KeyRange::data(), true, 4).await.unwrap()), want_rev);
        let inclusive = KeyRange::new(
            std::ops::Bound::Included(Bytes::from_static(b"k000")),
            std::ops::Bound::Included(Bytes::from_static(b"k099")),
        );
        assert_eq!(keys(t.scan(&inclusive, true, 4).await.unwrap()), want_rev);
        assert_eq!(keys(t.scan(&inclusive, false, 4).await.unwrap()), names(60..64));
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.delete_range(KeyRange::half_open(Bytes::from_static(b"k005"), Some(Bytes::from_static(b"k095")))).unwrap();
        let mut fwd = names(0..5);
        fwd.extend(names(95..98));
        assert_eq!(keys(t.scan(&KeyRange::data(), false, 8).await.unwrap()), fwd);
        let mut back = names(2..5);
        back.extend(names(95..100));
        back.reverse();
        assert_eq!(keys(t.scan(&KeyRange::data(), true, 8).await.unwrap()), back);
    });
}

#[test]
fn transaction_write_limit() {
    rt().block_on(async {
        let (eng, _store) =
            delayed_engine_with("a", EngineConfig { txn_max_write_bytes: 72, ..EngineConfig::small_for_tests() });
        let db = eng.db("d");
        db.open(true).await.unwrap();
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.put(Bytes::from_static(b"kk"), Value::Inline(Bytes::from(vec![0u8; 55]))).unwrap();
        t.delete(Bytes::from_static(b"abc")).unwrap();
        let e = t.delete(Bytes::from_static(b"d")).expect_err("72 + 9 bytes exceed the limit");
        assert!(matches!(e, crate::error::Error::TooLarge(_)), "{e}");
    });
}

#[test]
fn key_ranges_match_brute_force() {
    use std::ops::{Bound, RangeBounds};
    fn strings(alphabet: &[u8], max: usize) -> Vec<Bytes> {
        let mut out = vec![Bytes::new()];
        let mut layer = vec![Vec::new()];
        for _ in 0..max {
            layer = layer.iter().flat_map(|p| alphabet.iter().map(move |c| [p.as_slice(), &[*c]].concat())).collect();
            out.extend(layer.iter().cloned().map(Bytes::from));
        }
        out
    }
    let probes = strings(b"abcde", 3);
    let mut bounds = vec![Bound::Unbounded];
    for s in strings(b"bc", 2).into_iter().skip(1) {
        bounds.push(Bound::Included(s.clone()));
        bounds.push(Bound::Excluded(s));
    }
    let ranges: Vec<KeyRange> =
        bounds.iter().flat_map(|s| bounds.iter().map(move |e| KeyRange::new(s.clone(), e.clone()))).collect();
    let has = |r: &KeyRange, k: &Bytes| (r.start.clone(), r.end.clone()).contains(k);
    let members: Vec<Vec<bool>> = ranges.iter().map(|r| probes.iter().map(|k| has(r, k)).collect()).collect();
    for (r, m) in ranges.iter().zip(&members) {
        for k in &probes {
            assert_eq!(r.contains(k), has(r, k), "{r:?} contains {k:?}");
            assert_eq!(r.before_end(k), (Bound::Unbounded, r.end.clone()).contains(k), "{r:?} before_end {k:?}");
            assert_eq!(r.after_start(k), (r.start.clone(), Bound::Unbounded).contains(k), "{r:?} after_start {k:?}");
        }
        assert_eq!(r.is_empty(), !m.contains(&true), "{r:?} is_empty");
        for (o, n) in ranges.iter().zip(&members) {
            let both = m.iter().zip(n).any(|(a, b)| *a && *b);
            assert_eq!(r.intersects(o), both, "{r:?} intersects {o:?}");
        }
    }
}

#[test]
fn handles_are_shared_until_idle_and_gone_after_purge() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = eng.db("d");
        db.open(true).await.unwrap();
        put(&db, "k", "v").await;
        assert!(Arc::ptr_eq(&db, &eng.db("d")));
        let idle = 1000;
        tokio::time::sleep(Duration::from_millis(idle)).await;
        eng.evict_idle(idle);
        assert!(Arc::ptr_eq(&db, &eng.db("d")), "evicted a handle in use");
        eng.db("e");
        assert_eq!(eng.open_dbs().iter().map(|d| d.id.as_str()).collect::<Vec<_>>(), ["d", "e"]);
        assert_eq!(eng.open_count(), 2);

        let weak = Arc::downgrade(&db);
        drop(db);
        eng.evict_idle(idle);
        assert!(weak.upgrade().is_none(), "kept an idle, unused handle");
        assert_eq!(eng.open_count(), 1, "evicted a handle that is not idle");
        let db = eng.db("d");
        db.open(false).await.unwrap();
        let r = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap().get(b"k").await.unwrap();
        assert!(r.is_some(), "the reopened handle lost committed data");

        eng.purge("d").await.unwrap();
        assert!(!Arc::ptr_eq(&db, &eng.db("d")), "purge kept the handle");
        assert!(eng.db("d").open(false).await.is_err(), "purged database still opens");
    });
}

#[test]
fn op_batches_limits_and_defaults() {
    use crate::ops::{Op, OpCtx, exec_ops, make_value};
    use baserunna_api::Encoding;
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = eng.db("d");
        db.open(true).await.unwrap();
        let max = eng.shared.cfg.value_inline_max;
        assert!(matches!(make_value(&db, &vec![7; max], "x").await.unwrap(), Value::Inline(_)));
        assert!(matches!(make_value(&db, &vec![7; max + 1], "x").await.unwrap(), Value::Blob(_)));

        let ops = |n: usize, op: serde_json::Value| -> Vec<Op> {
            crate::ops::parse_ops(serde_json::from_value(serde_json::Value::Array(vec![op; n])).unwrap()).unwrap()
        };
        let get = serde_json::json!({"op": "get", "key": "k"});
        use crate::meta::{Access, Role};
        let owner = Access { role: Role::Owner, read_only_key: false };
        let run = |access: Access, ops: Vec<Op>| {
            let db = db.clone();
            async move {
                let ctx = OpCtx {
                    db: &db,
                    encoding: Encoding::Utf8,
                    access,
                    json_value_max: 1 << 20,
                    query_limits: Default::default(),
                };
                let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                exec_ops(&ctx, &mut t, &ops).await
            }
        };
        assert_eq!(run(owner, ops(10_000, get.clone())).await.unwrap().len(), 10_000);
        assert!(matches!(run(owner, ops(10_001, get.clone())).await, Err(Error::InvalidArgument(_))));

        let read_only_key = Access { read_only_key: true, ..owner };
        let reader = Access { role: Role::Reader, read_only_key: false };
        let incr = serde_json::json!({"op": "increment", "key": "n"});
        for access in [read_only_key, reader] {
            assert!(run(access, ops(1, get.clone())).await.is_ok(), "{access:?} must be able to read");
            assert!(matches!(run(access, ops(1, incr.clone())).await, Err(Error::PermissionDenied(_))), "{access:?}");
        }
        let incremented = serde_json::to_value(run(owner, ops(1, incr)).await.unwrap()).unwrap();
        assert_eq!(incremented, serde_json::json!([{"value": 1}]));
    });
}

pub(super) fn inline(s: &str) -> Value {
    Value::Inline(Bytes::from(s.to_string()))
}

pub(super) async fn put_many(db: &Arc<Db>, prefix: &str, n: usize, len: usize) -> Seq {
    let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
    for i in 0..n {
        t.put(Bytes::from(format!("{prefix}{i:02}")), Value::Inline(Bytes::from(vec![b'v'; len]))).unwrap();
    }
    db.commit(t.into_request()).await.unwrap()
}

pub(super) async fn flushed(db: &Arc<Db>) {
    for _ in 0..500 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if !db.busy() {
            return;
        }
    }
    panic!("background work never settled");
}

pub(super) async fn strong_get(db: &Arc<Db>, k: &str) -> Option<Value> {
    db.snapshot(Consistency::Strong).await.unwrap().get(k.as_bytes()).await.unwrap().map(|e| e.value)
}

#[test]
fn foreign_leases_block_commits_until_they_expire() {
    rt().block_on(async {
        let (a, store) = delayed_engine("a");
        put(&opened(&a, "d").await, "k", "a").await;
        let b = second_node(&a, &store, "b");
        let db = b.db("d");
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.put(Bytes::from_static(b"k"), inline("b")).unwrap();
        tokio::time::sleep(Duration::from_millis(5_000)).await;
        let left = b.shared.cfg.lease_ms + b.shared.cfg.lease_margin_ms - 5_000;
        match db.commit(t.into_request()).await {
            Err(Error::NotOwner { holder, retry_after_ms }) => {
                assert_eq!((holder.as_str(), retry_after_ms), ("a", left))
            }
            other => panic!("{other:?}"),
        }
        tokio::time::sleep(Duration::from_millis(left)).await;
        put(&db, "k", "b").await;
    });
}

/// Node B commits a write whose response is delayed by `reply_delay_leases`
/// lease durations while node A takes over the lease and commits its own write.
fn commit_with_lost_reply_after_takeover(reply_delay_leases: u64, hedge_ms: u64) -> crate::error::Result<Seq> {
    rt().block_on(async {
        let cfg = EngineConfig { manifest_put_hedge_ms: hedge_ms, ..EngineConfig::small_for_tests() };
        let (node_b, store) = delayed_engine_with("b", cfg);
        let db_b = opened(&node_b, "d").await;
        put(&db_b, "k", "b").await;
        let node_a = second_node(&node_b, &store, "a");
        let mut t = db_b.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.put(Bytes::from_static(b"k"), inline("lost")).unwrap();
        let lease_ms = node_b.shared.cfg.lease_ms + node_b.shared.cfg.lease_margin_ms;
        store.manifest_put_lost.store(1, SeqCst);
        store.put_reply_ms.store(reply_delay_leases * lease_ms, SeqCst);
        let commit = tokio::spawn(async move { db_b.commit(t.into_request()).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        store.put_reply_ms.store(0, SeqCst);
        let db_a = opened(&node_a, "d").await;
        tokio::time::sleep(Duration::from_millis(lease_ms + 100)).await;
        put_when_owner(&db_a, "k", "a").await;
        commit.await.unwrap()
    })
}

#[test]
fn ambiguous_commit_during_active_lease() {
    match commit_with_lost_reply_after_takeover(2, 0) {
        Ok(2) => {}
        other => panic!("expected the applied write's version 2, got {other:?}"),
    }
}

#[test]
fn ambiguous_commit_after_lease_expires() {
    match commit_with_lost_reply_after_takeover(4, 0) {
        Ok(2) => {}
        other => panic!("expected version 2, got {other:?}"),
    }
}

#[test]
fn hedged_commit_resolves_before_a_takeover() {
    match commit_with_lost_reply_after_takeover(2, 250) {
        Ok(2) => {}
        other => panic!("expected version 2, got {other:?}"),
    }
}

#[test]
fn released_leases_hand_over_immediately() {
    rt().block_on(async {
        let (a, store) = delayed_engine("a");
        let da = opened(&a, "d").await;
        put(&da, "k", "a").await;
        let db = second_node(&a, &store, "b").db("d");
        db.open(false).await.unwrap();
        da.release_lease().await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        put(&db, "k", "b").await;
        assert_eq!(strong_get(&da, "k").await, Some(inline("b")));
    });
}

/// Tests that a node releases a lease that the stored manifest names, also after the node stopped
/// relying on it. Another node honors that lease for longer, so only the release lets it commit at
/// once. A node that the manifest does not name has no lease to release.
#[test]
fn released_leases_hand_over_after_the_holder_stops_relying_on_them() {
    rt().block_on(async {
        let (a, store) = delayed_engine("a");
        let da = opened(&a, "d").await;
        put(&da, "k", "a").await;
        sleep_ms(a.shared.cfg.lease_ms).await;
        assert_eq!(da.leased_view(), None, "setup: A still relies on its lease");
        let db = second_node(&a, &store, "b").db("d");
        db.open(false).await.unwrap();
        assert!(!db.release_lease().await.unwrap(), "B released the lease of A");
        assert!(da.release_lease().await.unwrap(), "A did not release the lease that the manifest names");
        sleep_ms(1).await;
        put(&db, "k", "b").await;
    });
}

#[test]
fn decided_commits_are_answered_on_write_failure() {
    rt().block_on(async {
        let cfg = EngineConfig { manifest_put_hedge_ms: 0, commit_retry_max: 2, ..EngineConfig::small_for_tests() };
        let (eng, store) = delayed_engine_with("a", cfg);
        let db = opened(&eng, "d").await;
        let request = |id: u64| {
            let db = db.clone();
            async move {
                let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                t.put(Bytes::from_static(b"k"), inline("v")).unwrap();
                let mut r = t.into_request();
                r.commit_id = id;
                r
            }
        };
        let first = db.commit_detailed(request(77).await).await.unwrap();
        assert_eq!(db.resolve_commit(88).await.unwrap(), None);
        let (retry, aborted) = (request(77).await, request(88).await);
        store.manifest_put_lost.store(10, SeqCst);
        let (lease, replay, late) =
            tokio::join!(db.acquire_lease(), db.commit_detailed(retry), db.commit_detailed(aborted));
        assert!(lease.is_err(), "setup: the lease renewal's writes were not ambiguous");
        let replay = replay.expect("a commit recorded in a stored manifest was reported failed");
        assert_eq!((replay.version, replay.replayed), (first.version, true));
        assert!(matches!(late, Err(Error::Aborted(_))), "an aborted commit was reported {late:?}");
    });
}

#[test]
fn background_work_does_not_take_back_a_released_lease() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let before = crate::probe::count(crate::probe::Probe::BackgroundInstallUnleased);
        put_many(&db, "k", 5, 500).await;
        assert!(db.busy(), "setup: flush did not start");
        db.release_lease().await.unwrap();
        flushed(&db).await;
        let stored = store.mem.get_sync(&manifest_key("d"), &GetOpts::default()).unwrap().data;
        let m = manifest::Manifest::decode(&stored).unwrap();
        assert_eq!(m.lease, None, "the flush took the released lease back");
        assert!(!m.tail.is_empty(), "the flush was installed without the lease");
        assert!(crate::probe::count(crate::probe::Probe::BackgroundInstallUnleased) > before);
    });
}

/// Tests that a holder relies on its lease until a margin before the lease term ends. The term
/// starts when the holder sends the manifest `PUT`, not when the reply arrives 100 ms later.
#[test]
fn holders_stop_relying_on_their_lease_a_margin_early() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        store.put_reply_ms.store(100, SeqCst);
        let start = Instant::now();
        put(&db, "k", "v").await;
        let rely = Duration::from_millis(eng.shared.cfg.lease_ms - eng.shared.cfg.lease_margin_ms);
        let manifest_gets = || store.gets_matching(|k| k.ends_with("/manifest"));
        let before = manifest_gets();
        tokio::time::sleep_until(start + rely - Duration::from_millis(1)).await;
        db.read_version(Consistency::Strong).await.unwrap();
        assert_eq!(manifest_gets(), before, "a read within the lease went to the store");
        tokio::time::sleep_until(start + rely + Duration::from_millis(1)).await;
        db.read_version(Consistency::Strong).await.unwrap();
        assert_eq!(manifest_gets(), before + 1, "a read past the lease was served from the cache");
    });
}

/// Tests that a holder stops relying on its lease when it sends a manifest write that releases the
/// lease. Another node can commit as soon as the store applies that write.
#[test]
fn holders_stop_relying_on_their_lease_while_a_release_is_in_flight() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        put(&db, "k", "v").await;
        assert!(db.leased_view().is_some(), "setup: the node holds its lease");
        store.put_reply_ms.store(1_000, SeqCst);
        let release = tokio::spawn({
            let db = db.clone();
            async move { db.release_lease().await }
        });
        sleep_ms(10).await;
        assert_eq!(db.leased_view(), None, "the node relies on its lease while its release is in flight");
        assert!(release.await.unwrap().unwrap(), "setup: the node did not release its lease");
    });
}

#[test]
fn hot_databases_renew_their_lease_in_time() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        put(&db, "k", "v").await;
        let manifest_gets = || store.gets_matching(|k| k.ends_with("/manifest"));
        let before = manifest_gets();
        for _ in 0..600 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            db.read_version(Consistency::Strong).await.unwrap();
        }
        assert_eq!(manifest_gets(), before);
    });
}

#[test]
fn commits_give_up_after_bounded_backoff() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.put(Bytes::from_static(b"k"), inline("v")).unwrap();
        store.manifest_put_conflicts.store(u32::MAX, SeqCst);
        let puts = store.manifest_puts.load(SeqCst);
        let start = Instant::now();
        let r =
            tokio::time::timeout(Duration::from_secs(60), db.commit(t.into_request())).await.expect("retried forever");
        let took = start.elapsed().as_millis() as u64;
        assert!(matches!(r, Err(Error::Unavailable { outcome_unknown: false, .. })), "{r:?}");
        let max = eng.shared.cfg.commit_retry_max;
        assert_eq!(store.manifest_puts.load(SeqCst) - puts, max);
        let backoff: u64 = (1..max).map(|a| 5u64 << a.min(6)).sum();
        assert!((backoff..backoff + 10 * u64::from(max)).contains(&took), "backed off for {took} ms");
    });
}

#[test]
fn commits_survive_a_failed_reload_but_not_endless_ones() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.put(Bytes::from_static(b"k"), inline("v")).unwrap();
        store.manifest_put_conflicts.store(1, SeqCst);
        store.manifest_get_outages.store(eng.shared.retry.attempts, SeqCst);
        db.commit(t.into_request()).await.expect("a failed reload is retried");

        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.put(Bytes::from_static(b"k"), inline("w")).unwrap();
        store.manifest_put_conflicts.store(u32::MAX, SeqCst);
        store.manifest_get_outages.store(u32::MAX, SeqCst);
        let r = tokio::time::timeout(Duration::from_secs(60), db.commit(t.into_request()))
            .await
            .expect("reloads retried forever");
        assert!(r.is_err());
    });
}

#[test]
fn commits_past_their_deadline() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let request = |deadline: Instant| {
            let db = db.clone();
            async move {
                let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                t.put(Bytes::from_static(b"k"), inline("v")).unwrap();
                let mut r = t.into_request();
                r.deadline = Some(deadline);
                r
            }
        };
        let r = request(Instant::now() - Duration::from_millis(1)).await;
        let e = db.commit(r).await.unwrap_err();
        assert!(matches!(e, Error::Unavailable { outcome_unknown: false, .. }), "{e:?}");
        assert!(e.message().contains("deadline"), "{e:?}");
        assert_eq!(strong_get(&db, "k").await, None);

        store.manifest_put_outages.store(1, SeqCst);
        let r = request(Instant::now() + Duration::from_millis(1)).await;
        let e = db.commit(r).await.unwrap_err();
        assert!(matches!(e, Error::Unavailable { outcome_unknown: true, .. }), "{e:?}");
    });
}

#[test]
fn deleting_a_database_is_idempotent() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        db.mark_deleted().await.unwrap();
        db.mark_deleted().await.expect("deleting twice");
        assert!(db.read_version(Consistency::Strong).await.is_err());
    });
}

#[test]
fn commit_records_make_commits_idempotent_until_they_expire() {
    rt().block_on(async {
        let cfg = EngineConfig { commit_retention_ms: 60_000, ..EngineConfig::small_for_tests() };
        let (eng, _store) = delayed_engine_with("a", cfg);
        let db = opened(&eng, "d").await;
        let request = |i: u64, id: u64| {
            let db = db.clone();
            async move {
                let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                t.put(Bytes::from(format!("k{i}")), inline("v")).unwrap();
                let mut r = t.into_request();
                r.commit_id = id;
                r
            }
        };
        let (a, b) = (request(1, 7).await, request(2, 7).await);
        let (ra, rb) = tokio::join!(db.commit_detailed(a), db.commit_detailed(b));
        let (ra, rb) = (ra.unwrap(), rb.unwrap());
        assert!(!ra.replayed && rb.replayed && rb.version == ra.version, "{ra:?} {rb:?}");
        assert!(strong_get(&db, "k2").await.is_none(), "a replay applies nothing");
        let mut ids = Vec::new();
        for i in 0..60u64 {
            let id = 1000 + i;
            ids.push((id, db.commit(request(10 + i, id).await).await.unwrap()));
            put_many(&db, &format!("f{i}/"), 1, 300).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert!(v.m.segments.len() > 1 && v.m.segments.iter().any(|s| s.level > 0), "flushed and compacted");
        let again = db.commit_detailed(request(99, 7).await).await.unwrap();
        assert_eq!((again.version, again.replayed), (ra.version, true), "an old commit is still known");
        for (id, version) in &ids {
            let applied = |s| match s {
                CommitState::Applied(c) => Some(c.version),
                CommitState::Aborted => None,
            };
            assert_eq!(db.commit_status(*id).await.unwrap().and_then(applied), Some(*version));
        }
        assert_eq!(db.commit_status(424242).await.unwrap(), None);
        for _ in 0..2 {
            tokio::time::sleep(Duration::from_millis(61_000)).await;
            for i in 0..20 {
                put_many(&db, &format!("g{i}/"), 1, 300).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(db.commit_status(1000).await.unwrap(), None, "expired records are gone");
        let fresh = db.commit_detailed(request(98, 7).await).await.unwrap();
        assert!(!fresh.replayed && fresh.version > again.version);
        let expired = crate::probe::count(crate::probe::Probe::EntriesExpired);
        assert!(expired >= 60, "{expired}");
        let v = db.read_version(Consistency::Strong).await.unwrap();
        let records = v
            .iter(
                &KeyRange::new(std::ops::Bound::Included(types::commit_key(0)), std::ops::Bound::Unbounded),
                false,
                true,
                None,
            )
            .next()
            .await
            .unwrap();
        assert!(records.is_some_and(|e| !e.value.is_tombstone() && e.expires.is_none()));
        assert!(v.all_entries(true).await.unwrap().iter().all(|e| !types::is_commit_key(&e.key)));
    });
}

#[test]
fn version_samples_are_spaced_and_reach_a_lifetime_back() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let cfg = eng.shared.cfg.clone();
        let mut history = Vec::new();
        for i in 0..120 {
            tokio::time::sleep(Duration::from_millis(10_000)).await;
            let v = put(&db, &format!("k{}", i % 7), "v").await;
            history.push((eng.shared.time_ms(), v));
        }
        let m = db.current().unwrap().m.clone();
        let now = eng.shared.store_now_ms().unwrap();
        let times: Vec<u64> = m.version_times.iter().map(|(_, t)| *t).collect();
        for w in times.windows(2) {
            let gap = w[1] - w[0];
            assert!((cfg.version_sample_ms..cfg.version_sample_ms + 10_000).contains(&gap), "samples at {times:?}");
        }
        assert!(now - times[times.len() - 1] < cfg.version_sample_ms, "samples stopped at {times:?}");
        let at = |t: u64| history.iter().filter(|(ht, _)| *ht <= t).map(|(_, v)| *v).max().unwrap_or(0);
        let cutoff = now - cfg.txn_max_lifetime_ms;
        let got = m.version_at_least_age(now, cfg.txn_max_lifetime_ms);
        assert!((at(cutoff - cfg.version_sample_ms)..=at(cutoff)).contains(&got), "{got} from {:?}", m.version_times);
    });
}

/// A tombstone can be purged only after a later commit is older than the purge window.
/// The tombstone version timestamp is recorded before object storage applies the manifest.
#[test]
fn tail_tombstones_wait_for_later_commit_age() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let cfg = eng.shared.cfg.clone();
        let window = Duration::from_millis(cfg.txn_max_lifetime_ms + 2 * cfg.clock_sync_max_error_ms);
        let sample = Duration::from_millis(cfg.version_sample_ms);
        put(&db, "k", "v").await;
        tokio::time::sleep(sample).await;
        let deleted = Instant::now();
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.delete(Bytes::from_static(b"k")).unwrap();
        db.commit(t.into_request()).await.unwrap();
        let tombstone =
            || db.current().unwrap().m.tail.iter().any(|e| e.key.as_ref() == b"k" && e.value.is_tombstone());
        tokio::time::sleep(sample).await;
        let next = Instant::now();
        put(&db, "y", "1").await;
        tokio::time::sleep_until(deleted + window + sample / 2).await;
        put(&db, "x", "1").await;
        assert!(tombstone(), "purged using the tombstone version timestamp");
        tokio::time::sleep_until(next + window - Duration::from_millis(1)).await;
        put(&db, "x", "2").await;
        assert!(tombstone(), "dropped before the next commit was a window old");
        tokio::time::sleep_until(next + window).await;
        put(&db, "x", "3").await;
        assert!(!tombstone(), "kept after the next commit was a window old");
    });
}

/// Commits in a batch become visible when object storage applies the batch manifest.
/// A tombstone in a stalled batch is kept for a full purge window after that manifest write.
#[test]
fn tombstones_in_stalled_batch_wait_for_manifest_write() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let cfg = eng.shared.cfg.clone();
        let window = Duration::from_millis(cfg.txn_max_lifetime_ms + 2 * cfg.clock_sync_max_error_ms);
        let sample = Duration::from_millis(cfg.version_sample_ms);
        let stall = 4 * sample;
        put(&db, "k", "v").await;
        tokio::time::sleep(sample).await;
        let begin = || db.begin(Isolation::Serializable, Consistency::Strong);
        let commit = |t: Txn| {
            let db = db.clone();
            tokio::spawn(async move { db.commit(t.into_request()).await.unwrap() })
        };
        let written = db.current().unwrap().m.write_seq;
        store.put_reply_ms.store(stall.as_millis() as u64, std::sync::atomic::Ordering::SeqCst);
        let mut first = begin().await.unwrap();
        first.put(Bytes::from_static(b"f"), Value::Inline(Bytes::from_static(b"1"))).unwrap();
        let first = commit(first);
        tokio::time::sleep(Duration::from_millis(1)).await;
        let mut delete = begin().await.unwrap();
        delete.delete(Bytes::from_static(b"k")).unwrap();
        let mut later = begin().await.unwrap();
        later.put(Bytes::from_static(b"x"), Value::Inline(Bytes::from_static(b"1"))).unwrap();
        let prepared = Instant::now() + stall;
        let (delete, later) = (commit(delete), commit(later));
        for h in [first, delete, later] {
            h.await.unwrap();
        }
        store.put_reply_ms.store(0, std::sync::atomic::Ordering::SeqCst);
        let batches = db.current().unwrap().m.write_seq - written;
        assert_eq!(batches, 2, "setup: the delete and the later write did not share a batch");
        let tombstone =
            || db.current().unwrap().m.tail.iter().any(|e| e.key.as_ref() == b"k" && e.value.is_tombstone());
        tokio::time::sleep_until(prepared + window + stall / 2).await;
        put(&db, "y", "1").await;
        assert!(tombstone(), "dropped before readers had seen its batch for a window");
    });
}

/// Object storage can apply a manifest write before the lease holder receives the response.
/// A snapshot read on the lease holder after the staleness bound must load the new version.
#[test]
fn snapshot_reads_on_lease_holder_load_applied_write() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        put(&db, "k", "1").await;
        store.put_reply_ms.store(200, SeqCst);
        let writer = {
            let db = db.clone();
            tokio::spawn(async move { put(&db, "k", "2").await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        let replica = second_node(&eng, &store, "b").db("d");
        replica.open(false).await.unwrap();
        let seen = replica.snapshot(Consistency::Strong).await.unwrap().version.get_entry(b"k").await.unwrap();
        assert_eq!(seen.map(|e| e.value), Some(inline("2")), "setup: the write was not applied before its response");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let stale = Consistency::Snapshot { max_staleness_ms: 56 };
        let read = db.snapshot(stale).await.unwrap().version.get_entry(b"k").await.unwrap();
        assert_eq!(read.map(|e| e.value), Some(inline("2")), "the lease holder served a version older than the bound");
        writer.await.unwrap();
    });
}

/// A lease holder that loads its in-flight manifest write from storage reuses the lease from that
/// write. The commit and the read together issue a single manifest write.
#[test]
fn lease_holder_refreshing_in_flight_write_reuses_lease() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        put(&db, "k", "1").await;
        for _ in 0..eng.shared.cfg.lease_read_threshold {
            db.snapshot(Consistency::Strong).await.unwrap();
        }
        store.put_reply_ms.store(200, SeqCst);
        let puts_before = store.manifest_puts.load(SeqCst);
        let reads_during_write = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let tight = Consistency::Snapshot { max_staleness_ms: 56 };
            let read = db.snapshot(tight).await.unwrap().version.get_entry(b"k").await.unwrap();
            assert_eq!(read.map(|e| e.value), Some(inline("2")));
        };
        tokio::join!(put(&db, "k", "2"), reads_during_write);
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        assert_eq!(store.manifest_puts.load(SeqCst) - puts_before, 1, "the node wrote the manifest to request a lease");
        assert!(db.leased_view().is_some(), "the node lost its lease");
    });
}

/// Tests that a holder takes no lease from its in-flight manifest write after a refresh loaded that
/// write from storage. Only the successful response of the `PUT` gives the lease.
#[test]
fn loaded_in_flight_manifest_put_gives_no_lease_until_it_succeeds() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        put(&db, "k", "1").await;
        let held = db.leased_view().expect("setup: the node holds its lease");
        store.put_reply_ms.store(200, SeqCst);
        let writer = tokio::spawn({
            let db = db.clone();
            async move { put(&db, "k", "2").await }
        });
        sleep_ms(100).await;
        let tight = Consistency::Snapshot { max_staleness_ms: 56 };
        let loaded = db.snapshot(tight).await.unwrap().version;
        assert_eq!(loaded.m.write_seq, held.write_seq + 1, "setup: the refresh did not load the in-flight write");
        assert_eq!(db.leased_view(), None, "the node took its lease from a write that is in flight");
        writer.await.unwrap();
        assert_eq!(db.leased_view().map(|v| v.write_seq), Some(held.write_seq + 1), "the PUT gave no lease");
    });
}

/// Reads from a lease holder during continuous commits must return versions within the requested
/// staleness bound.
#[test]
fn lease_holder_reads_during_commits() {
    let modes = [
        Consistency::Snapshot { max_staleness_ms: 56 },
        Consistency::Snapshot { max_staleness_ms: 5_000 },
        Consistency::Strong,
    ];
    let mut violations = Vec::new();
    for mode in modes {
        rt().block_on(async {
            let (eng, store) = delayed_engine("a");
            let db = opened(&eng, "d").await;
            put(&db, "k", "0").await;
            store.get_reply_ms.store(20, SeqCst);
            store.put_reply_ms.store(60, SeqCst);
            let stored_version = {
                let store = store.clone();
                move || {
                    let g = store.mem.get_sync(&manifest_key("d"), &GetOpts::default()).unwrap();
                    manifest::Manifest::decode(&g.data).unwrap().version
                }
            };
            let samples = Arc::new(Mutex::new(vec![(Instant::now(), stored_version())]));
            let sampler = {
                let samples = samples.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        let v = stored_version();
                        let mut s = samples.lock();
                        if s.last().unwrap().1 != v {
                            s.push((Instant::now(), v));
                        }
                    }
                })
            };
            let writer = {
                let db = db.clone();
                tokio::spawn(async move {
                    for i in 1..=40 {
                        put(&db, "k", &i.to_string()).await;
                    }
                })
            };
            let bound = match mode {
                Consistency::Snapshot { max_staleness_ms } => Duration::from_millis(max_staleness_ms),
                Consistency::Strong => Duration::ZERO,
            };
            let manifest_gets = || store.gets_matching(|k| k.ends_with("/manifest"));
            let gets_before = manifest_gets();
            let mut latencies = Vec::new();
            while !writer.is_finished() {
                let invoked = Instant::now();
                let read = db.snapshot(mode).await.unwrap().version.m.version;
                latencies.push(invoked.elapsed());
                let floor = invoked.checked_sub(bound).unwrap_or(invoked);
                let required = samples.lock().iter().rev().find(|(t, _)| *t <= floor).map_or(0, |(_, v)| *v);
                if read < required {
                    violations.push(format!("{mode:?}: read v{read}, storage held v{required} at the bound"));
                }
                tokio::time::sleep(Duration::from_millis(7)).await;
            }
            sampler.abort();
            let gets = manifest_gets() - gets_before;
            latencies.sort();
            let n = latencies.len();
            let mean = latencies.iter().sum::<Duration>() / n as u32;
            eprintln!(
                "{mode:?}: reads={n} mean={mean:?} p99={:?} max={:?} manifest_gets={gets}",
                latencies[n * 99 / 100],
                latencies[n - 1]
            );
        });
    }
    assert!(violations.is_empty(), "{} stale reads: {:#?}", violations.len(), &violations[..violations.len().min(5)]);
}

pub(super) fn known_objects(m: &manifest::Manifest) -> Vec<String> {
    let db = &m.db_id;
    m.segments
        .iter()
        .map(|s| seg_key(db, &s.id))
        .chain(m.levels.iter().map(|id| levels_key(db, id)))
        .chain(m.obsolete.iter().map(|o| match o.kind {
            manifest::ObjKind::Levels => levels_key(db, &o.id),
            _ => seg_key(db, &o.id),
        }))
        .collect()
}

#[test]
fn flushes_are_compacted_in_the_background() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        store.put_reply_ms.store(4, SeqCst);
        for i in 0..40 {
            put_many(&db, &format!("r{i:02}-"), 5, 500).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        let m = &db.current().unwrap().m;
        m.check_levels().unwrap();
        assert!(
            m.compaction_seq > 0,
            "tail {} segments {:?}",
            m.tail_bytes(),
            m.segments.iter().map(|s| (s.level, s.size)).collect::<Vec<_>>()
        );
        assert!(m.level(0).len() < eng.shared.cfg.l0_compaction_trigger, "{} level 0 segments", m.level(0).len());
        let (base, targets) = compaction::level_targets(m, &eng.shared.cfg);
        for l in 1..manifest::MAX_LEVEL {
            assert!(m.level_bytes(l) <= targets[l as usize], "level {l} over its target (base {base})");
        }
        let known = known_objects(m);
        for k in store.mem.keys().iter().filter(|k| k.contains("/seg/")) {
            assert!(known.contains(k), "orphaned object {k}");
        }
    });
}

#[test]
fn stats_describe_the_current_version() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let v = put(&db, "k", "v").await;
        let s = db.stats(&db.current().unwrap());
        assert_eq!((s.version, s.levels.len(), s.tail_entries), (v, 0, 2), "the value and its commit record");
        assert_eq!(s.lease_holder.as_deref(), Some("a"));
        assert_eq!(s.lease_expires_at, Some(eng.shared.env.now_ms() + eng.shared.cfg.lease_ms));
    });
}

#[test]
fn unchanged_manifests_are_revalidated_conditionally() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let after_open = store.gets.lock().len();
        for _ in 0..2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
            db.read_version(Consistency::Strong).await.unwrap();
        }
        let gets = store.gets.lock()[after_open..].to_vec();
        assert!(
            gets.len() == 2 && gets.iter().all(|(k, conditional)| k.ends_with("/manifest") && *conditional),
            "{gets:?}"
        );
    });
}

#[test]
fn small_segments_load_with_one_request() {
    rt().block_on(async {
        let (a, store) = delayed_engine("a");
        let db = opened(&a, "d").await;
        put_many(&db, "k", 5, 500).await;
        flushed(&db).await;
        let seg_gets = || store.gets_matching(|k| k.contains("/seg/"));
        let before = seg_gets();
        let db = second_node(&a, &store, "b").db("d");
        assert!(strong_get(&db, "k03").await.is_some());
        assert_eq!(seg_gets() - before, 1);
    });
}

#[test]
fn validation_sees_flushed_writes_and_not_the_snapshot_itself() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        put(&db, "b", "old").await;
        let mut stale = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        stale.scan(&KeyRange::prefix(b"a"), false, 10).await.unwrap();
        stale.put(Bytes::from_static(b"z"), inline("1")).unwrap();
        let mut fine = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        fine.scan(&KeyRange::prefix(b"b"), false, 10).await.unwrap();
        fine.put(Bytes::from_static(b"y"), inline("1")).unwrap();
        put_many(&db, "a", 5, 500).await;
        flushed(&db).await;
        assert!(db.current().unwrap().m.tail.is_empty(), "setup: the concurrent write was not flushed");
        assert_eq!(db.commit(stale.into_request()).await.unwrap_err().code(), "conflict");
        db.commit(fine.into_request()).await.expect("data as old as the snapshot conflicted");
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert!(v.scan(&KeyRange::data(), false, 0, v.read_time()).await.unwrap().is_empty());
    });
}

#[test]
fn streamed_blob_writes_fill_whole_chunks() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let data: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let mut w = db.blob_writer("application/octet-stream").await;
        for piece in data.chunks(700) {
            w.write(piece).await.unwrap();
        }
        let r = w.finish().await.unwrap();
        for (i, chunk) in data.chunks(eng.shared.cfg.blob_chunk_size).enumerate() {
            let got = store.mem.get_sync(&blob_chunk_key("d", &r.id, i as u64), &GetOpts::default()).unwrap();
            assert_eq!(got.data, chunk, "chunk {i}");
        }
        assert_eq!(blob::read_range(&eng.shared, "d", &r, 1000..3000).await.unwrap(), &data[1000..3000]);
    });
}

fn stored_segment(store: &MemStore, id: char, level: u8, entries: &[Entry]) -> manifest::SegmentMeta {
    let (data, s) = segment::build(entries, 512, segment::Compression::Zstd(1));
    let id = id.to_string().repeat(32);
    store.put_sync(&seg_key("d", &id), data, &PutMode::Overwrite).unwrap();
    compaction::seg_meta(id, level, &s, 0)
}

#[test]
fn merges_purge_expired_tombstones() {
    rt().block_on(async {
        let (eng, store, _env) = engine(1, EngineConfig::small_for_tests());
        let e = |k: &str, seq, value| Entry::new(Bytes::from(k.to_string()), seq, value);
        let v = || inline("v");
        let t = Value::Tombstone;
        let newer = stored_segment(&store, 'a', 5, &[e("a", 5, t.clone()), e("b", 9, t.clone()), e("d", 4, t.clone())]);
        let older = stored_segment(&store, 'b', 6, &[e("a", 1, v()), e("b", 2, v()), e("c", 3, v())]);
        let mut m = manifest::Manifest::new("d", 0, None);
        m.segments = vec![newer.clone(), older.clone()];
        let read = |segs: &[manifest::SegmentMeta]| {
            assert_eq!(segs.len(), 1);
            let k = seg_key("d", &segs[0].id);
            segment::read_all(&store.get_sync(&k, &GetOpts::default()).unwrap().data).unwrap()
        };
        let plan = |inputs: Vec<manifest::SegmentMeta>, level| compaction::Plan {
            inputs,
            kind: compaction::Kind::Merge { level },
        };
        let out = compaction::run(&eng.shared, &m, &plan(vec![newer.clone(), older.clone()], 6), 6).await.unwrap();
        assert_eq!(out.purged_max_seq, 5);
        assert_eq!(read(&out.segments), [e("b", 9, t.clone()), e("c", 3, v())]);
        let out = compaction::run(&eng.shared, &m, &plan(vec![newer.clone()], 5), 9).await.unwrap();
        assert_eq!(out.purged_max_seq, 4);
        assert_eq!(read(&out.segments), [e("a", 5, t.clone()), e("b", 9, t.clone())]);
        m.segments = vec![newer.clone()];
        let out = compaction::run(&eng.shared, &m, &plan(vec![newer], 6), 9).await.unwrap();
        assert!(out.segments.is_empty(), "wrote an empty segment");
        assert_eq!(out.purged_max_seq, 9);
    });
}

#[test]
fn large_databases_compact_in_bounded_requests() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let cfg = eng.shared.cfg.clone();
        let db = opened(&eng, "d").await;
        let mut r = Rng::new(11);
        let mut model = BTreeMap::new();
        for _ in 0..150 {
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            for _ in 0..10 {
                let k = format!("k{:05}", r.below(3_000));
                let len = 50 + r.usize(150);
                let v = Bytes::from(r.bytes(len));
                t.put(Bytes::from(k.clone()), Value::Inline(v.clone())).unwrap();
                model.insert(Bytes::from(k), v);
            }
            db.commit(t.into_request()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let v = db.read_version(Consistency::Strong).await.unwrap();
        let total: u64 = v.m.segments.iter().map(|s| s.size).sum();
        assert!(total > 30 * cfg.segment_target_bytes as u64, "setup: only {total} bytes of segments");
        assert!(v.m.compaction_seq > 10, "setup: little compaction");
        let largest_read = store.seg_get_bytes.lock().iter().copied().max().unwrap_or(0);
        let bound = cfg.segment_whole_fetch_max.max(cfg.bulk_read_bytes) as usize;
        assert!(largest_read <= bound, "a {largest_read}-byte segment read");
        for s in v.m.segments.iter().filter(|s| s.level > 0) {
            assert!(s.size <= (cfg.segment_target_bytes + 2 * cfg.block_target) as u64, "{} bytes", s.size);
        }
        let got: BTreeMap<Bytes, Bytes> = v
            .all_entries(false)
            .await
            .unwrap()
            .into_iter()
            .map(|e| match e.value {
                Value::Inline(b) => (e.key, b),
                other => panic!("{other:?}"),
            })
            .collect();
        assert!(got == model, "contents differ after compaction");
    });
}

#[test]
fn long_scans_read_ahead() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let cfg = eng.shared.cfg.clone();
        let mut r = Rng::new(3);
        let entries: Vec<Entry> = (0..400u64)
            .map(|i| Entry::new(Bytes::from(format!("k{i:05}")), 1, Value::Inline(Bytes::from(r.bytes(100)))))
            .collect();
        let seg = stored_segment(&store.mem, 'a', 6, &entries);
        assert!(seg.size > 8 * cfg.bulk_read_bytes, "setup: a {}-byte segment", seg.size);
        let mut m = manifest::Manifest::new("d", 0, None);
        m.segments = vec![seg.clone()];
        let v = Version::new(m, String::new(), eng.shared.clone());
        let gets = || store.seg_get_bytes.lock().len();
        v.scan(&KeyRange::data(), false, 3, v.read_time()).await.unwrap();
        let short = gets();
        assert!(short <= 3, "a short scan made {short} requests (index and one block)");
        let largest_block = eng.shared.seg_index("d", &seg).await.unwrap().blocks.iter().map(|b| b.2).max().unwrap();
        let last_read = *store.seg_get_bytes.lock().last().unwrap();
        assert!(last_read <= largest_block as usize, "a short scan read {last_read} bytes ahead");
        let cold = second_node(&eng, &store, "b");
        let v2 = Version::new(v.m.clone(), String::new(), cold.shared.clone());
        let back: Vec<Entry> = v2.scan(&KeyRange::data(), true, usize::MAX, v2.read_time()).await.unwrap();
        assert!(back.iter().rev().eq(entries.iter()), "a cold reverse scan");
        let before = gets();
        assert_eq!(v.scan(&KeyRange::data(), false, usize::MAX, v.read_time()).await.unwrap().len(), 400);
        let blocks = eng.shared.seg_index("d", &seg).await.unwrap().blocks.len();
        let full = gets() - before;
        assert!(full <= 2 + blocks.div_ceil(3), "{full} requests for {blocks} blocks");
        let before = gets();
        v.scan(&KeyRange::data(), false, usize::MAX, v.read_time()).await.unwrap();
        assert_eq!(gets(), before, "blocks read ahead were cached");
    });
}

#[test]
fn written_segments_are_read_without_requests() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        put_many(&db, "k", 5, 500).await;
        flushed(&db).await;
        assert!(db.current().unwrap().m.tail.is_empty());
        let gets = || store.seg_get_bytes.lock().len();
        let before = gets();
        assert!(strong_get(&db, "k03").await.is_some());
        assert_eq!(gets(), before, "a flushed segment was fetched back");
    });
}

#[test]
fn appends_move_down_without_rewrites() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let mut r = Rng::new(17);
        let mut levels: BTreeMap<String, std::collections::BTreeSet<u8>> = BTreeMap::new();
        for i in 0..300 {
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            t.put(Bytes::from(format!("log{i:06}")), Value::Inline(Bytes::from(r.bytes(400)))).unwrap();
            db.commit(t.into_request()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
            for s in &db.current().unwrap().m.segments {
                levels.entry(s.id.clone()).or_default().insert(s.level);
            }
        }
        assert!(levels.values().any(|l| l.len() > 1 && !l.contains(&0)));
        tokio::time::sleep(Duration::from_millis(3_000)).await;
        let writes = store.manifest_puts.load(SeqCst);
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        assert_eq!(store.manifest_puts.load(SeqCst), writes, "compaction kept writing the manifest");
    });
}

#[test]
fn restarted_nodes_read_what_they_wrote_locally() {
    rt().block_on(async {
        let tier = local::LocalTier(Arc::new(local::MemLocal::new(64 << 20)));
        let cfg = EngineConfig { local_tier: Some(tier), ..EngineConfig::small_for_tests() };
        let (a, store) = delayed_engine_with("a", cfg.clone());
        let db = opened(&a, "d").await;
        let mut r = Rng::new(4);
        for i in 0..60 {
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            for j in 0..5 {
                t.put(Bytes::from(format!("k{i:03}{j}")), Value::Inline(Bytes::from(r.bytes(120)))).unwrap();
            }
            db.commit(t.into_request()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let m = db.current().unwrap().m.clone();
        let whole = |s: &manifest::SegmentMeta| s.size <= cfg.segment_whole_fetch_max;
        assert!(m.segments.iter().any(whole) && !m.segments.iter().all(whole), "setup: segment sizes");
        let restarted = Engine::new(store.clone(), a.shared.env.clone(), cfg, "a2".into());
        let gets = || store.seg_get_bytes.lock().len();
        let before = gets();
        let v = restarted.db("d").read_version(Consistency::Strong).await.unwrap();
        assert_eq!(v.all_entries(false).await.unwrap().len(), 300);
        assert_eq!(gets(), before, "segment requests after a restart");
    });
}

#[test]
fn bulk_reads_use_the_caches_without_filling_them() {
    rt().block_on(async {
        let tier = local::LocalTier(Arc::new(local::MemLocal::new(64 << 20)));
        let cfg = EngineConfig { local_tier: Some(tier), ..EngineConfig::small_for_tests() };
        let (a, store) = delayed_engine_with("a", cfg.clone());
        let gets = || store.seg_get_bytes.lock().len();
        let db = opened(&a, "d").await;
        let mut r = Rng::new(5);
        for i in 0..80 {
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            for j in 0..5 {
                t.put(Bytes::from(format!("k{:03}{j}", (i * 7) % 61)), Value::Inline(Bytes::from(r.bytes(120))))
                    .unwrap();
            }
            db.commit(t.into_request()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        let m = db.current().unwrap().m.clone();
        let whole = |s: &manifest::SegmentMeta| s.size <= cfg.segment_whole_fetch_max;
        assert!(m.segments.iter().any(whole) && !m.segments.iter().all(whole), "setup: segment sizes");
        assert_eq!(gets(), 0, "segments this node wrote were fetched back");

        let bulk = |e: &Arc<Engine>| {
            let v = Version::new(m.clone(), String::new(), e.shared.clone());
            async move {
                let mut it = v.iter_bulk(true);
                let mut got = Vec::new();
                while let Some(e) = it.next().await.unwrap() {
                    got.push(e);
                }
                got
            }
        };
        let want = bulk(&a).await;
        assert_eq!(gets(), 0);
        let restarted = Engine::new(store.clone(), a.shared.env.clone(), cfg.clone(), "a2".into());
        assert_eq!(bulk(&restarted).await, want);
        assert_eq!(gets(), 0, "a restarted node read its local tier");
        assert_eq!(restarted.shared.cache.stats().entries, 0, "bulk reads cached what they read");

        let cold = Engine::new(
            store.clone(),
            a.shared.env.clone(),
            EngineConfig { local_tier: None, ..cfg.clone() },
            "b".into(),
        );
        assert_eq!(bulk(&cold).await, want);
        let small = m.segments.iter().filter(|s| whole(s)).count();
        let large: Vec<&manifest::SegmentMeta> = m.segments.iter().filter(|s| !whole(s)).collect();
        let most = small + large.iter().map(|s| 2 + (s.size / cfg.bulk_read_bytes) as usize + 1).sum::<usize>();
        assert!(gets() <= most, "{} requests for {small} small and {} large segments", gets(), large.len());
        assert_eq!(cold.shared.cache.stats().entries, 0, "bulk reads cached what they fetched");
    });
}

#[test]
fn bulk_reads_take_whole_blocks() {
    let mut r = Rng::new(9);
    let entries: Vec<Entry> = (0..100u64)
        .map(|i| Entry::new(Bytes::from(format!("k{i:04}")), 1, Value::Inline(Bytes::from(r.bytes(80)))))
        .collect();
    for window in [1, 1_600] {
        rt().block_on(async {
            let cfg = EngineConfig { bulk_read_bytes: window, ..EngineConfig::small_for_tests() };
            let (eng, store) = delayed_engine_with("a", cfg);
            let seg = stored_segment(&store.mem, 'a', 6, &entries);
            let mut m = manifest::Manifest::new("d", 0, None);
            m.segments = vec![seg];
            let v = Version::new(m, String::new(), eng.shared.clone());
            let idx = eng.shared.seg_index("d", &v.m.segments[0]).await.unwrap();
            let largest = idx.blocks.iter().map(|b| b.2 as u64).max().unwrap();
            assert!(window == 1 || window >= 2 * largest, "setup: blocks of up to {largest} bytes");
            let before = store.seg_get_bytes.lock().len();
            let mut it = v.iter_bulk(true);
            let mut got = Vec::new();
            while let Some(e) = it.next().await.unwrap() {
                got.push(e);
            }
            assert_eq!(got, entries);
            let reads: Vec<usize> = store.seg_get_bytes.lock()[before..].to_vec();
            if window == 1 {
                assert_eq!(reads.len(), idx.blocks.len(), "one block per request");
            } else {
                assert!(reads.iter().all(|n| *n as u64 <= window), "a read over the window: {reads:?}");
                assert!(reads.len() * 2 <= idx.blocks.len(), "{} reads for {} blocks", reads.len(), idx.blocks.len());
            }
            assert_eq!(v.all_entries(true).await.unwrap(), entries, "reading ahead");
        });
    }
}

#[test]
fn lookups_consult_one_segment_per_level() {
    let (eng, _store, _env) = engine(1, EngineConfig::small_for_tests());
    let seg = |id: char, level: u8, min: &str, max: &str| manifest::SegmentMeta {
        id: id.to_string().repeat(32),
        level,
        size: 1,
        entries: 1,
        tombstones: 0,
        min_key: Bytes::copy_from_slice(min.as_bytes()),
        max_key: Bytes::copy_from_slice(max.as_bytes()),
        min_seq: 0,
        max_seq: 0,
        deletes_applied: 0,
        expiring: 0,
        expires_max: 0,
    };
    let mut m = manifest::Manifest::new("d", 0, None);
    m.segments = vec![
        seg('a', 0, "a", "z"),
        seg('b', 0, "m", "p"),
        seg('c', 5, "a", "f"),
        seg('d', 5, "g", "p"),
        seg('e', 6, "a", "k"),
        seg('f', 6, "l", "z"),
    ];
    let v = Version::new(m, String::new(), eng.shared.clone());
    for (key, want) in [("b", 3), ("n", 4), ("h", 3), ("q", 2), ("0", 0)] {
        assert_eq!(v.segments_for_key(key.as_bytes()), want, "{key}");
    }
}

#[test]
fn changes_are_entries_newer_than_the_snapshot() {
    rt().block_on(async {
        let (eng, store, _env) = engine(1, EngineConfig::small_for_tests());
        let e = |k: &str, seq| Entry::new(Bytes::from(k.to_string()), seq, inline("v"));
        let seg = stored_segment(&store, 'a', 6, &[e("a", 3), e("c", 7)]);
        let mut m = manifest::Manifest::new("d", 0, None);
        m.segments = vec![seg];
        m.tail = vec![e("b", 5)];
        let v = Version::new(m, String::new(), eng.shared.clone());
        let range =
            |a: &str, b: &str| KeyRange::half_open(Bytes::from(a.to_string()), Some(Bytes::from(b.to_string())));
        for (r, after, changed) in [
            (range("b", "c"), 5, false),
            (range("b", "c"), 4, true),
            (range("c", "d"), 7, false),
            (range("c", "d"), 6, true),
            (range("a", "b"), 2, true),
            (range("a", "b"), 3, false),
        ] {
            assert_eq!(v.changed_since(&r, after).await.unwrap(), changed, "{r:?} after {after}");
        }
    });
}

#[test]
fn one_compaction_at_a_time() {
    rt().block_on(async {
        let cfg = EngineConfig { tail_max_bytes: 8 * 1024, ..EngineConfig::small_for_tests() };
        let (eng, store) = delayed_engine_with("a", cfg);
        let db = opened(&eng, "d").await;
        store.put_reply_ms.store(50, SeqCst);
        for i in 0..16 {
            put_many(&db, &format!("a{i:02}-"), 5, 500).await;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        let m = db.current().unwrap().m.clone();
        let known = known_objects(&m);
        for k in store.mem.keys().iter().filter(|k| k.contains("/seg/")) {
            assert!(known.contains(k), "orphaned object {k}: a compaction was discarded");
        }
    });
}

#[test]
fn sweeps_page_through_long_listings() {
    rt().block_on(async {
        let cfg = EngineConfig::small_for_tests();
        let (eng, store) = delayed_engine_with("a", cfg.clone());
        let db = opened(&eng, "d").await;
        put(&db, "k", "v").await;
        let mut ids = Vec::new();
        for i in 0..1_500u64 {
            let id = super::types::make_blob_id(eng.shared.env.now_ms(), &i.to_le_bytes()[..].repeat(2)[..10]);
            for c in 0..2 {
                store
                    .mem
                    .put_sync(&blob_chunk_key("d", &id, c), Bytes::from_static(b"x"), &PutMode::Overwrite)
                    .unwrap();
            }
            ids.push(id);
        }
        tokio::time::sleep(Duration::from_millis(cfg.orphan_grace_ms.max(cfg.blob_stage_ttl_ms) + 60_000)).await;
        db.maintenance_now().await.unwrap();
        db.sweep_now().await.unwrap();
        let listed = db.current().unwrap().m.obsolete.len();
        assert_eq!(listed, ids.len());
    });
}

#[test]
fn compaction_output_is_split_into_bounded_segments() {
    rt().block_on(async {
        let (eng, store, _env) = engine(1, EngineConfig::small_for_tests());
        let cfg = eng.shared.cfg.clone();
        let mut r = Rng::new(5);
        let entries: Vec<Entry> = (0..400u64)
            .map(|i| {
                let len = r.usize(90);
                let value = Value::Inline(Bytes::from(r.bytes(len)));
                Entry::new(Bytes::from(format!("k{i:05}")), i + 1, value)
            })
            .collect();
        let input = stored_segment(&store, 'a', 0, &entries);
        let mut m = manifest::Manifest::new("d", 0, None);
        m.segments = vec![input.clone()];
        let plan = compaction::Plan { inputs: vec![input], kind: compaction::Kind::Merge { level: 6 } };
        let out = compaction::run(&eng.shared, &m, &plan, 0).await.unwrap();
        assert!(out.segments.len() >= 3, "{} segments", out.segments.len());
        for s in &out.segments {
            assert!(s.size <= (cfg.segment_target_bytes + 2 * cfg.block_target) as u64, "{} bytes", s.size);
        }
        m.segments = out.segments;
        m.check_levels().unwrap();
        let v = Version::new(m, String::new(), eng.shared.clone());
        assert_eq!(v.all_entries(true).await.unwrap(), entries);
        for e in &entries {
            assert_eq!(v.get_entry(&e.key).await.unwrap().as_ref(), Some(e));
        }
    });
}

async fn put_ttl(db: &Arc<Db>, k: &str, v: &str, ttl_ms: u64) -> Committed {
    let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
    t.put_expiring(Bytes::from(k.to_string()), inline(v), Some(Ttl::For(ttl_ms))).unwrap();
    db.commit_detailed(t.into_request()).await.unwrap()
}

#[test]
fn values_expire_a_ttl_after_their_commit() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        eng.shared.store_time_ms().await.unwrap();
        put(&db, "k", "old").await;
        let c = put_ttl(&db, "k", "new", 1_000).await;
        let snap = db.snapshot(Consistency::Strong).await.unwrap();
        assert!(snap.time_ms >= c.time_ms);
        let e = snap.get(b"k").await.unwrap().unwrap();
        assert_eq!((e.value, e.expires), (inline("new"), Some(c.time_ms + 1_000)));
        tokio::time::sleep(Duration::from_millis(999)).await;
        assert!(strong_get(&db, "k").await.is_some());
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(strong_get(&db, "k").await, None);
        assert_eq!(snap.get(b"k").await.unwrap().map(|e| e.value), Some(inline("new")));
        assert!(db.snapshot(Consistency::Strong).await.unwrap().version.get_entry(b"k").await.unwrap().is_some());
    });
}

#[test]
fn writes_do_not_wait_for_clock_probes() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let sh = &eng.shared;
        sh.store_time_ms().await.unwrap();
        tokio::time::sleep(Duration::from_millis(sh.cfg.clock_sync_interval_ms)).await;
        store.clock_get_reply_ms.store(3_600_000, SeqCst);
        sh.refresh_clock();
        tokio::task::yield_now().await;
        let t = Instant::now();
        put_ttl(&db, "k", "v", 60_000).await;
        drop(db.blob_writer("x/y").await);
        assert_eq!(t.elapsed(), Duration::ZERO);
    });
}

#[test]
fn reads_of_values_that_expire_before_the_commit_conflict() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        eng.shared.store_time_ms().await.unwrap();
        put_ttl(&db, "lock", "held", 1_000).await;
        let mut stale = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let mut si = db.begin(Isolation::Snapshot, Consistency::Strong).await.unwrap();
        let mut fresh = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        for t in [&mut stale, &mut si, &mut fresh] {
            assert!(t.get(b"lock").await.unwrap().is_some());
        }
        stale.put(Bytes::from_static(b"w1"), inline("1")).unwrap();
        si.put(Bytes::from_static(b"w2"), inline("1")).unwrap();
        fresh.put(Bytes::from_static(b"w3"), inline("1")).unwrap();
        db.commit(fresh.into_request()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1_001)).await;
        let r = db.commit(stale.into_request()).await;
        assert!(matches!(r, Err(Error::Conflict(_))), "{r:?}");
        db.commit(si.into_request()).await.unwrap();
        let mut ahead = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        ahead.read_time += 60_000;
        ahead.put(Bytes::from_static(b"w4"), inline("1")).unwrap();
        let req = ahead.into_request();
        let read_time = req.read_time;
        let c = db.commit_detailed(req).await.unwrap();
        assert!(c.time_ms >= read_time);
        assert!(db.snapshot(Consistency::Strong).await.unwrap().time_ms >= read_time);
    });
}

#[test]
fn cached_snapshots_read_at_their_validation_time() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        eng.shared.store_time_ms().await.unwrap();
        put_ttl(&db, "k", "v", 1_000).await;
        let b = second_node(&eng, &store, "b");
        b.shared.store_time_ms().await.unwrap();
        let other = opened(&b, "d").await;
        let validated = other.snapshot(Consistency::Strong).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        let stale = Consistency::Snapshot { max_staleness_ms: 5_000 };
        let s = other.snapshot(stale).await.unwrap();
        assert_eq!(s.time_ms, validated.time_ms);
        assert!(s.get(b"k").await.unwrap().is_some());
        assert!(other.snapshot(Consistency::Strong).await.unwrap().get(b"k").await.unwrap().is_none());
    });
}

#[test]
fn change_feed_cursors_expire_with_purged_deletions() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let cfg = eng.shared.cfg.clone();
        let first = put(&db, "a", "1").await;
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.delete(Bytes::from_static(b"a")).unwrap();
        let deleted = db.commit(t.into_request()).await.unwrap();
        let v = db.read_version(Consistency::Strong).await.unwrap();
        let page = v.changes(&KeyRange::data(), &ChangeCursor::after_version(first), 10).await.unwrap();
        assert_eq!(page.changes, vec![Change::Entry(Entry::tombstone(Bytes::from_static(b"a"), deleted))]);
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(cfg.txn_max_lifetime_ms.max(cfg.version_sample_ms))).await;
            put(&db, "b", "x").await;
        }
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert!(v.m.tombstone_horizon >= deleted, "setup: purged");
        let r = v.changes(&KeyRange::data(), &ChangeCursor::after_version(first), 10).await;
        assert!(matches!(r, Err(Error::CursorExpired(_))), "{r:?}");
        let fresh = v.changes(&KeyRange::data(), &ChangeCursor::after_version(v.m.tombstone_horizon), 10).await;
        assert!(fresh.is_ok(), "{fresh:?}");
        let pos = ChangePos { seq: v.m.tombstone_horizon, kind: ChangePos::KEY, key: Bytes::from_static(b"a") };
        let mid = ChangeCursor { pos, floor: 0 };
        assert!(matches!(v.changes(&KeyRange::data(), &mid, 10).await, Err(Error::CursorExpired(_))));
        // A cursor at version 0 does not require tombstones and never expires.
        let all = v.changes(&KeyRange::data(), &ChangeCursor::after_version(0), 10).await.unwrap();
        assert!(all.changes.iter().any(|c| matches!(c, Change::Entry(e) if &e.key[..] == b"b")));
        let floored = ChangeCursor { floor: 9, ..mid.clone() };
        for c in [ChangeCursor::after_version(7), mid, floored] {
            assert_eq!(ChangeCursor::decode(&c.encode()), Some(c));
        }
        for bad in ["", "x", "1.", "1.2.AA", "1.0.!!", "1.1", "1.1.AA.0", "1.1.AA.x", "1.1.AA.2.3"] {
            assert_eq!(ChangeCursor::decode(bad), None, "{bad}");
        }
    });
}

#[test]
fn copies_share_values_and_blobs_without_staging_rules() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let blob = blob::write_blob(&eng.shared, "d", b"blob bytes", "x/y").await.unwrap();
        let orphan = blob::write_blob(&eng.shared, "d", b"never bound", "x/y").await.unwrap();
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.put(Bytes::from_static(b"b"), Value::Blob(blob.clone())).unwrap();
        db.commit(t.into_request()).await.unwrap();
        put(&db, "i", "inline").await;
        // Copy succeeds after the blob staging TTL has elapsed.
        tokio::time::sleep(Duration::from_millis(eng.shared.cfg.blob_stage_ttl_ms * 2)).await;
        let blob_bytes = db.stats(&db.current().unwrap()).blob_bytes;
        let mut t = db.begin(Isolation::Snapshot, Consistency::Strong).await.unwrap();
        assert!(t.copy(b"b", Bytes::from_static(b"b2"), None).await.unwrap());
        assert!(t.copy(b"i", Bytes::from_static(b"i2"), None).await.unwrap());
        assert!(!t.copy(b"missing", Bytes::from_static(b"m2"), None).await.unwrap());
        assert!(t.copy(b"b2", Bytes::from_static(b"b4"), None).await.unwrap());
        db.commit(t.into_request()).await.unwrap();
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert_eq!(db.stats(&v).blob_bytes, blob_bytes, "copies store no new blob bytes");
        let get = |k: &'static [u8]| {
            let v = v.clone();
            async move { v.get(k, u64::MAX).await.unwrap().map(|e| e.value) }
        };
        assert_eq!(get(b"b2").await, Some(Value::Blob(blob.clone())));
        assert_eq!(get(b"b4").await, Some(Value::Blob(blob.clone())));
        assert_eq!(get(b"i2").await, get(b"i").await);
        assert_eq!(get(b"m2").await, None);
        // Modifying the source key before commit causes a conflict even under snapshot isolation.
        let mut t = db.begin(Isolation::Snapshot, Consistency::Strong).await.unwrap();
        assert!(t.copy(b"b", Bytes::from_static(b"b3"), None).await.unwrap());
        let mut d = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        d.delete(Bytes::from_static(b"b")).unwrap();
        db.commit(d.into_request()).await.unwrap();
        assert!(matches!(db.commit(t.into_request()).await, Err(Error::Conflict(_))));
        // Garbage collection retains the shared blob after the original key is deleted.
        let wait = eng.shared.cfg.orphan_grace_ms.max(eng.shared.cfg.blob_stage_ttl_ms) + 60_000;
        tokio::time::sleep(Duration::from_millis(wait)).await;
        db.maintenance_now().await.unwrap();
        db.sweep_now().await.unwrap();
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(eng.shared.cfg.gc_grace_ms + 1_000)).await;
            db.maintenance_now().await.unwrap();
            db.sweep_now().await.unwrap();
        }
        assert!(
            blob::read_range(&eng.shared, "d", &orphan, 0..orphan.size).await.is_err(),
            "setup: collection deletes unbound blobs"
        );
        assert!(!db.current().unwrap().m.obsolete.iter().any(|o| o.id == blob.id), "a copied blob was collected");
        let data = blob::read_range(&eng.shared, "d", &blob, 0..blob.size).await.unwrap();
        assert_eq!(&data[..], b"blob bytes");
    });
}

#[test]
fn blobs_bound_under_several_keys_count_once() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let blob = blob::write_blob(&eng.shared, "d", b"shared bytes", "x/y").await.unwrap();
        let before = db.stats(&db.current().unwrap()).blob_bytes;
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.put(Bytes::from_static(b"a"), Value::Blob(blob.clone())).unwrap();
        t.put(Bytes::from_static(b"b"), Value::Blob(blob.clone())).unwrap();
        assert!(t.copy(b"a", Bytes::from_static(b"c"), None).await.unwrap());
        db.commit(t.into_request()).await.unwrap();
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert_eq!(db.stats(&v).blob_bytes, before + blob.size);
        assert_eq!(v.get(b"c", u64::MAX).await.unwrap().map(|e| e.value), Some(Value::Blob(blob)));
    });
}

#[test]
fn full_state_transfers_page_past_purged_deletions() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let cfg = eng.shared.cfg.clone();
        let delete = |k: &'static str| {
            let db = db.clone();
            async move {
                let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                t.delete(Bytes::from_static(k.as_bytes())).unwrap();
                db.commit(t.into_request()).await.unwrap()
            }
        };
        let purge = || async {
            for _ in 0..3 {
                tokio::time::sleep(Duration::from_millis(cfg.txn_max_lifetime_ms.max(cfg.version_sample_ms))).await;
                put(&db, "z", "x").await;
            }
            db.read_version(Consistency::Strong).await.unwrap()
        };
        for k in ["a", "b", "c", "d", "e", "y"] {
            put(&db, k, "1").await;
        }
        let deleted = delete("y").await;
        let v = db.read_version(Consistency::Strong).await.unwrap();
        let first = v.changes(&KeyRange::data(), &ChangeCursor::after_version(0), 2).await.unwrap();
        assert!(first.more && first.next.floor == v.m.version);
        assert_eq!(ChangeCursor::decode(&first.next.encode()), Some(first.next.clone()));
        // Purging a tombstone older than the first page's snapshot does not expire the transfer cursor.
        let v = purge().await;
        assert!(v.m.tombstone_horizon >= deleted && first.next.pos.seq < deleted, "setup: purged");
        let mut held: BTreeMap<Bytes, Value> = BTreeMap::new();
        let mut cursor = first.next.clone();
        for c in &first.changes {
            let Change::Entry(e) = c else { panic!("{c:?}") };
            held.insert(e.key.clone(), e.value.clone());
        }
        loop {
            let page = v.changes(&KeyRange::data(), &cursor, 2).await.unwrap();
            for c in &page.changes {
                match c {
                    Change::Entry(e) if e.value.is_tombstone() => held.remove(&e.key),
                    Change::Entry(e) => held.insert(e.key.clone(), e.value.clone()),
                    Change::DeleteRange(_) => panic!("{c:?}"),
                };
            }
            cursor = page.next;
            if !page.more {
                break;
            }
        }
        let live: BTreeMap<Bytes, Value> =
            v.all_entries(false).await.unwrap().into_iter().map(|e| (e.key, e.value)).collect();
        assert_eq!(held, live);
        assert_eq!(cursor, ChangeCursor::after_version(v.m.version));
        // Purging a tombstone newer than the first page's snapshot expires the transfer cursor.
        let first = v.changes(&KeyRange::data(), &ChangeCursor::after_version(0), 2).await.unwrap();
        let Change::Entry(sent) = &first.changes[0] else { panic!() };
        let key: &'static str = String::from_utf8(sent.key[1..].to_vec()).unwrap().leak();
        delete(key).await;
        let v = purge().await;
        let r = v.changes(&KeyRange::data(), &first.next, 2).await;
        assert!(matches!(r, Err(Error::CursorExpired(_))), "{r:?}");
    });
}

#[test]
fn full_state_transfers_expire_after_paging_a_stale_snapshot() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let cfg = eng.shared.cfg.clone();
        for k in ["x", "k", "a", "b"] {
            put(&db, k, "1").await;
        }
        let stale = db.read_version(Consistency::Strong).await.unwrap();
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.delete(Bytes::from_static(b"k")).unwrap();
        let deleted = db.commit(t.into_request()).await.unwrap();
        let fresh = db.read_version(Consistency::Strong).await.unwrap();
        let first = fresh.changes(&KeyRange::data(), &ChangeCursor::after_version(0), 1).await.unwrap();
        assert!(first.more && first.next.floor == deleted, "{first:?}");
        // Reading a page from an older snapshot lowers the cursor floor to that snapshot's version.
        let second = stale.changes(&KeyRange::data(), &first.next, 1).await.unwrap();
        assert!(matches!(&second.changes[..], [Change::Entry(e)] if &e.key[..] == b"k" && !e.value.is_tombstone()));
        assert!(second.more && second.next.floor == stale.m.version, "{second:?}");
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(cfg.txn_max_lifetime_ms.max(cfg.version_sample_ms))).await;
            put(&db, "z", "x").await;
        }
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert!(v.m.tombstone_horizon >= deleted, "setup: purged");
        let r = v.changes(&KeyRange::data(), &second.next, 1).await;
        assert!(matches!(r, Err(Error::CursorExpired(_))), "{r:?}");
    });
}

#[test]
fn commit_notices_wake_waiting_requests() {
    rt().block_on(async {
        let (a, store) = delayed_engine("a");
        let b = second_node(&a, &store, "b");
        let writer = opened(&a, "d").await;
        let reader = opened(&b, "d").await;
        let seen = |db: &Arc<Db>| db.current().unwrap().m.version;
        let long = Duration::from_secs(3600);
        let woken_at_once = |db: Arc<Db>, after: Seq| async move {
            let started = tokio::time::Instant::now();
            db.wait_for_change(after, long).await;
            started.elapsed() < Duration::from_secs(1)
        };
        let before = seen(&reader);
        let v = put(&writer, "k", "1").await;
        reader.notice_commit(v);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(seen(&reader), before);
        assert!(woken_at_once(reader.clone(), before).await);
        assert_eq!(seen(&reader), v);
        assert!(tokio::time::timeout(Duration::from_millis(10), reader.wait_for_change(v, long)).await.is_err());
        let v2 = put(&writer, "k", "2").await;
        reader.notice_commit(v2);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(seen(&reader), v);
        assert!(woken_at_once(reader.clone(), v).await);
        let waiting = tokio::spawn(woken_at_once(reader.clone(), v2));
        tokio::time::sleep(Duration::from_millis(100)).await;
        let v3 = put(&writer, "k", "3").await;
        reader.notice_commit(v3);
        assert!(waiting.await.unwrap());
        assert_eq!(seen(&reader), v3);
        // A notice for an unavailable version triggers at most 1 manifest read.
        let manifest_gets = || store.gets.lock().iter().filter(|(k, _)| k.ends_with("/manifest")).count();
        let before = manifest_gets();
        reader.notice_commit(v3 + 100);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(manifest_gets(), before);
        tokio::time::timeout(Duration::from_secs(5), reader.wait_for_change(v3, Duration::from_secs(2))).await.unwrap();
        assert_eq!(manifest_gets(), before + 1);
    });
}

#[test]
fn commit_notices_survive_failed_reloads() {
    rt().block_on(async {
        use std::sync::atomic::Ordering::SeqCst;
        let (a, store) = delayed_engine("a");
        let b = second_node(&a, &store, "b");
        let writer = opened(&a, "d").await;
        let v0 = put(&writer, "k", "0").await;
        let reader = opened(&b, "d").await;
        let r = reader.clone();
        let waiting = tokio::spawn(async move {
            let started = tokio::time::Instant::now();
            r.wait_for_change(v0, Duration::from_secs(3600)).await;
            started.elapsed()
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        store.manifest_get_outages.store(b.shared.retry.attempts, SeqCst);
        let v1 = put(&writer, "k", "1").await;
        reader.notice_commit(v1);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let v2 = put(&writer, "k", "2").await;
        reader.notice_commit(v2);
        let waited = waiting.await.unwrap();
        assert!(waited < Duration::from_secs(5), "{waited:?}");
        assert_eq!(store.manifest_get_outages.load(SeqCst), 0, "setup: the first reload failed");
        assert_eq!(reader.current().unwrap().m.version, v2);
    });
}
