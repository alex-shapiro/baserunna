//! Engine model and unit tests.

use super::*;
use crate::env::Env;
use crate::error::Error;
use crate::store::StoreError;
use crate::store::{BoxFut, DynStore, GetOpts, GetResult, ListPage, MemStore, ObjectStore, PutMode, SResult};
use crate::util::prop;
use crate::util::rng::Rng;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

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
}

fn data_key(r: &mut Rng, max_len: usize) -> Vec<u8> {
    let mut k = prop::small_key(r, max_len);
    if k[0] == types::COMMIT_SPACE {
        k[0] -= 1;
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
            94..=96 => Op::Feed(r.usize(1000), 1 + r.usize(20)),
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
    let mut cursor = ChangePos::after_version(version);
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
            if c.pos() <= cursor || c.seq() <= version || c.seq() > v.m.version {
                return Err(format!("feed out of order: {c:?} after {cursor:?}"));
            }
            cursor = c.pos();
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
            eng.shared.counters.get()
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
        let e = old.snapshot.get(b"k", old.read_time).await.unwrap().unwrap();
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
        assert_eq!(sh.lease_seen("a", &lease("x"), 5), first, "the same renewal must keep its first sighting");
        assert!(sh.lease_seen("a", &lease("x"), 6) > first, "a renewal is a new sighting, whatever its expiry");
        assert!(sh.lease_seen("a", &lease("y"), 6) > first, "another holder is a new sighting");
        let a_seen = sh.lease_seen("a", &lease("y"), 6);
        let v = sh.cfg.read_lease_ms + sh.cfg.lease_margin_ms;
        let present = |id: &str| sh.lease_sightings.lock().unwrap().contains_key(id);
        let age_to = |x: u64| async move {
            let now = tokio::time::Instant::now();
            tokio::time::advance((a_seen + std::time::Duration::from_millis(x * v / 100)) - now).await;
        };
        age_to(5_000).await;
        for i in 0..100_000 {
            sh.lease_seen(&format!("d{i}"), &lease("x"), 1);
        }
        assert!(present("a"), "a sighting 50 leases old must survive pruning");
        age_to(9_500).await;
        sh.lease_seen("b", &lease("x"), 1);
        assert!(present("a"), "a sighting 95 leases old must survive pruning");
        age_to(10_500).await;
        sh.lease_seen("c", &lease("x"), 1);
        assert!(!present("a") && present("c"), "a full table must drop sightings over 100 leases old");
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
    put_reply_ms: AtomicU64,
    manifest_put_conflicts: AtomicU32,
    manifest_put_outages: AtomicU32,
    /// Manifest writes that apply, but whose reply is lost.
    manifest_put_lost: AtomicU32,
    manifest_get_outages: AtomicU32,
    manifest_puts: AtomicU32,
    gets: Mutex<Vec<(String, bool)>>,
    seg_get_bytes: Mutex<Vec<usize>>,
}

fn take_fault(n: &AtomicU32) -> bool {
    n.fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1)).is_ok()
}

impl DelayStore {
    fn gets_matching(&self, f: impl Fn(&str) -> bool) -> usize {
        self.gets.lock().unwrap().iter().filter(|(k, _)| f(k)).count()
    }
}

impl ObjectStore for DelayStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        Box::pin(async move {
            self.gets.lock().unwrap().push((key.to_string(), opts.if_none_match.is_some()));
            if key.ends_with("/manifest") && take_fault(&self.manifest_get_outages) {
                return Err(StoreError::Unavailable("injected".into()));
            }
            let r = self.mem.get_sync(key, &opts);
            if let (true, Ok(g)) = (key.contains("/seg/"), &r) {
                self.seg_get_bytes.lock().unwrap().push(g.data.len());
            }
            tokio::time::sleep(Duration::from_millis(self.get_reply_ms.load(SeqCst))).await;
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
        put_reply_ms: 0.into(),
        manifest_put_conflicts: 0.into(),
        manifest_put_outages: 0.into(),
        manifest_put_lost: 0.into(),
        manifest_get_outages: 0.into(),
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
fn fenced_status_writes_the_manifest() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = eng.db("d");
        db.open(true).await.unwrap();
        put(&db, "k", "1").await;
        let etag = || store.mem.get_sync(&manifest_key("d"), &GetOpts::default()).unwrap().etag;
        let before = etag();
        assert_eq!(db.commit_status_fenced(42).await.unwrap(), None);
        assert_ne!(etag(), before, "the fence did not write the manifest");
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
        let old_segs: Vec<String> = old.snapshot.m.segments.iter().map(|s| s.id.clone()).collect();
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
    use crate::ops::{Encoding, Op, OpCtx, exec_ops, make_value};
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = eng.db("d");
        db.open(true).await.unwrap();
        let max = eng.shared.cfg.value_inline_max;
        assert!(matches!(make_value(&db, &vec![7; max], "x").await.unwrap(), Value::Inline(_)));
        assert!(matches!(make_value(&db, &vec![7; max + 1], "x").await.unwrap(), Value::Blob(_)));

        let ops = |n: usize, op: serde_json::Value| -> Vec<Op> {
            serde_json::from_value(serde_json::Value::Array(vec![op; n])).unwrap()
        };
        let get = serde_json::json!({"op": "get", "key": "k"});
        use crate::meta::{Access, Role};
        let owner = Access { role: Role::Owner, read_only_key: false };
        let run = |access: Access, ops: Vec<Op>| {
            let db = db.clone();
            async move {
                let ctx = OpCtx { db: &db, encoding: Encoding::Utf8, access, json_value_max: 1 << 20 };
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
        assert_eq!(run(owner, ops(1, incr)).await.unwrap(), [serde_json::json!({"value": 1})]);
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

/// Node `b` commits a write that lands at once, but whose reply comes
/// `reply_leases` lease durations later and is lost; meanwhile node `a`
/// takes over once `b`'s lease runs out and writes once. Returns `b`'s
/// commit result.
fn commit_with_lost_reply_after_takeover(reply_leases: u64) -> crate::error::Result<Seq> {
    rt().block_on(async {
        let (b, store) = delayed_engine("b");
        let db = opened(&b, "d").await;
        put(&db, "k", "b").await;
        let a = second_node(&b, &store, "a");
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.put(Bytes::from_static(b"k"), inline("lost")).unwrap();
        let lease = b.shared.cfg.lease_ms + b.shared.cfg.lease_margin_ms;
        store.manifest_put_lost.store(1, SeqCst);
        store.put_reply_ms.store(reply_leases * lease, SeqCst);
        let commit = tokio::spawn(async move { db.commit(t.into_request()).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        store.put_reply_ms.store(0, SeqCst);
        let other = opened(&a, "d").await;
        tokio::time::sleep(Duration::from_millis(lease + 100)).await;
        put(&other, "k", "a").await;
        commit.await.unwrap()
    })
}

#[test]
fn a_lease_lost_after_an_ambiguous_write_leaves_the_outcome_unknown() {
    // `b` retries while `a`'s lease is valid, so it cannot check whether its write landed.
    match commit_with_lost_reply_after_takeover(2) {
        Err(Error::Unavailable { outcome_unknown: true, .. }) => {}
        other => panic!("the write landed, so its outcome is unknown, not {other:?}"),
    }
}

#[test]
fn an_ambiguous_write_resolves_once_the_next_lease_lapses() {
    // By the time `b` retries, the store clock shows `a`'s lease lapsed: `b`
    // reloads the manifest and finds its commit record.
    match commit_with_lost_reply_after_takeover(4) {
        Ok(2) => {}
        other => panic!("the write landed at version 2, not {other:?}"),
    }
}

#[test]
fn released_leases_hand_over_at_once() {
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

#[test]
fn holders_stop_relying_on_their_lease_a_margin_early() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
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
            assert_eq!(db.commit_status(*id).await.unwrap().map(|s| s.version), Some(*version));
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
        let a = eng.shared.counters.get();
        assert!(a.expired_entries >= 60, "{a:?}");
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
        assert!(records.is_some_and(|e| !e.value.is_tombstone() && e.expires.is_none()), "no tombstones of records");
        assert!(v.all_entries(true).await.unwrap().iter().all(|e| !types::is_commit_key(&e.key)), "data excludes them");
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

#[test]
fn tail_tombstones_are_dropped_after_a_transaction_lifetime() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let life = eng.shared.cfg.txn_max_lifetime_ms;
        put(&db, "k", "v").await;
        tokio::time::sleep(Duration::from_millis(eng.shared.cfg.version_sample_ms)).await;
        let start = Instant::now();
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.delete(Bytes::from_static(b"k")).unwrap();
        db.commit(t.into_request()).await.unwrap();
        let tombstone =
            || db.current().unwrap().m.tail.iter().any(|e| e.key.as_ref() == b"k" && e.value.is_tombstone());
        assert!(tombstone(), "dropped at once");
        tokio::time::sleep_until(start + Duration::from_millis(life - 1)).await;
        put(&db, "x", "1").await;
        assert!(tombstone(), "dropped within a transaction lifetime");
        tokio::time::sleep_until(start + Duration::from_millis(life)).await;
        put(&db, "x", "2").await;
        assert!(!tombstone(), "kept beyond a transaction lifetime");
    });
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
        assert!(m.compaction_seq > 0);
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
        assert_eq!(s.lease_expires_ms, Some(eng.shared.env.now_ms() + eng.shared.cfg.lease_ms));
    });
}

#[test]
fn unchanged_manifests_are_revalidated_conditionally() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let after_open = store.gets.lock().unwrap().len();
        for _ in 0..2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
            db.read_version(Consistency::Strong).await.unwrap();
        }
        let gets = store.gets.lock().unwrap()[after_open..].to_vec();
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
        put_many(&opened(&a, "d").await, "k", 5, 500).await;
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
fn merges_purge_only_tombstones_nothing_needs() {
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
        let largest_read = store.seg_get_bytes.lock().unwrap().iter().copied().max().unwrap_or(0);
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
        let gets = || store.seg_get_bytes.lock().unwrap().len();
        v.scan(&KeyRange::data(), false, 3, v.read_time()).await.unwrap();
        let short = gets();
        assert!(short <= 3, "a short scan made {short} requests (index and one block)");
        let largest_block = eng.shared.seg_index("d", &seg).await.unwrap().blocks.iter().map(|b| b.2).max().unwrap();
        let last_read = *store.seg_get_bytes.lock().unwrap().last().unwrap();
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
        assert!(db.current().unwrap().m.tail.is_empty(), "setup: no flush");
        let gets = || store.seg_get_bytes.lock().unwrap().len();
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
        assert!(levels.values().any(|l| l.len() > 1 && !l.contains(&0)), "no segment moved down a level as it was");
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
        let gets = || store.seg_get_bytes.lock().unwrap().len();
        let before = gets();
        let v = restarted.db("d").read_version(Consistency::Strong).await.unwrap();
        assert_eq!(v.all_entries(false).await.unwrap().len(), 300);
        assert_eq!(gets(), before, "segment requests after a restart");
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
            let before = store.seg_get_bytes.lock().unwrap().len();
            let mut it = v.iter_bulk(true);
            let mut got = Vec::new();
            while let Some(e) = it.next().await.unwrap() {
                got.push(e);
            }
            assert_eq!(got, entries);
            let reads: Vec<usize> = store.seg_get_bytes.lock().unwrap()[before..].to_vec();
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
        let (eng, store) = delayed_engine("a");
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
        assert!(snap.time_ms >= c.time_ms, "reads never go back before a commit's time");
        let e = snap.get(b"k").await.unwrap().unwrap();
        assert_eq!((e.value, e.expires), (inline("new"), Some(c.time_ms + 1_000)));
        tokio::time::sleep(Duration::from_millis(999)).await;
        assert!(strong_get(&db, "k").await.is_some());
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(strong_get(&db, "k").await, None, "expired, and the older value stays shadowed");
        assert_eq!(snap.get(b"k").await.unwrap().map(|e| e.value), Some(inline("new")), "a snapshot reads at its time");
        assert!(db.snapshot(Consistency::Strong).await.unwrap().version.get_entry(b"k").await.unwrap().is_some());
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
        db.commit(fresh.into_request()).await.expect("still held at the commit");
        tokio::time::sleep(Duration::from_millis(1_001)).await;
        let r = db.commit(stale.into_request()).await;
        assert!(matches!(r, Err(Error::Conflict(_))), "{r:?}");
        db.commit(si.into_request()).await.expect("snapshot isolation does not validate reads");
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
        assert_eq!(s.time_ms, validated.time_ms, "the cached version is as of its validation");
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
        let page = v.changes(&KeyRange::data(), &ChangePos::after_version(first), 10).await.unwrap();
        assert_eq!(page.changes, vec![Change::Entry(Entry::tombstone(Bytes::from_static(b"a"), deleted))]);
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(cfg.txn_max_lifetime_ms.max(cfg.version_sample_ms))).await;
            put(&db, "b", "x").await;
        }
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert!(v.m.tombstone_horizon >= deleted, "setup: purged");
        let r = v.changes(&KeyRange::data(), &ChangePos::after_version(first), 10).await;
        assert!(matches!(r, Err(Error::CursorExpired(_))), "{r:?}");
        let fresh = v.changes(&KeyRange::data(), &ChangePos::after_version(v.m.tombstone_horizon), 10).await;
        assert!(fresh.is_ok(), "{fresh:?}");
        let mid = ChangePos { seq: v.m.tombstone_horizon, kind: ChangePos::KEY, key: Bytes::from_static(b"a") };
        assert!(matches!(v.changes(&KeyRange::data(), &mid, 10).await, Err(Error::CursorExpired(_))));
        for pos in [ChangePos::after_version(7), mid] {
            assert_eq!(ChangePos::decode(&pos.encode()), Some(pos));
        }
        for bad in ["", "x", "1.", "1.2.AA", "1.0.!!", "1.1"] {
            assert_eq!(ChangePos::decode(bad), None, "{bad}");
        }
    });
}
