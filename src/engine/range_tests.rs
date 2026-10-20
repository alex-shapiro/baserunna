//! Tests for range tombstones, levels objects, and storage quotas.

use super::tests::{delayed_engine, delayed_engine_with, known_objects, opened, put, rt, second_node};
use super::*;
use crate::error::Error;
use crate::probe::{self, Probe};
use bytes::Bytes;
use std::time::Duration;

fn key(i: usize) -> Bytes {
    Bytes::from(format!("t/{i:04}"))
}

async fn fill(db: &Arc<Db>, n: usize, len: usize, per_txn: usize) {
    for chunk in (0..n).collect::<Vec<_>>().chunks(per_txn) {
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        for i in chunk {
            t.put(key(*i), Value::Inline(Bytes::from(vec![b'a' + (*i % 26) as u8; len]))).unwrap();
        }
        db.commit(t.into_request()).await.unwrap();
    }
}

async fn delete_prefix(db: &Arc<Db>, p: &str) -> Seq {
    let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
    t.delete_range(KeyRange::prefix(p.as_bytes())).unwrap();
    db.commit(t.into_request()).await.unwrap()
}

async fn live_keys(v: &Version, p: &str) -> usize {
    v.scan(&KeyRange::prefix(p.as_bytes()), false, usize::MAX, v.read_time()).await.unwrap().len()
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(2_000)).await;
}

async fn outlive_transactions(db: &Arc<Db>) {
    let cfg = &db.shared().cfg;
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(cfg.txn_max_lifetime_ms.max(cfg.version_sample_ms))).await;
        put(db, "other", "x").await;
    }
    settle().await;
}

#[test]
fn large_range_deletes_write_range_tombstone_and_retire() {
    rt().block_on(async {
        let (eng, store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        fill(&db, 300, 100, 20).await;
        put(&db, "u/after", "kept").await;
        settle().await;
        let before = db.read_version(Consistency::Strong).await.unwrap();
        assert!(before.m.segments.len() > 3);
        assert_eq!(live_keys(&before, "t/").await, 300);
        let size_before = before.m.stored_bytes();
        let seq = delete_prefix(&db, "t/").await;
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert_eq!(v.m.range_deletes.len(), 1);
        assert_eq!(v.m.range_deletes[0].seq, seq);
        assert!(v.m.tail.iter().all(|e| !e.key.starts_with(b"t/")));
        assert_eq!(live_keys(&v, "t/").await, 0);
        assert_eq!(v.get(&key(17), v.read_time()).await.unwrap(), None);
        assert_eq!(v.get_entry(&key(17)).await.unwrap().map(|e| (e.seq, e.value)), Some((seq, Value::Tombstone)));
        assert!(v.get(b"u/after", v.read_time()).await.unwrap().is_some());
        settle().await;
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert!(probe::count(Probe::SegmentDropped) > 0);
        assert!(v.m.stored_bytes() < size_before / 4, "{} of {size_before} bytes left", v.m.stored_bytes());
        assert!(v.m.segments.iter().all(|s| !v.m.range_deletes[0].dirties(s)));
        assert_eq!(v.m.range_deletes.len(), 1);
        assert_eq!(live_keys(&before, "t/").await, 300);
        assert!(before.get(&key(299), before.read_time()).await.unwrap().is_some());
        outlive_transactions(&db).await;
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert!(v.m.range_deletes.is_empty());
        assert!(v.m.tombstone_horizon >= seq);
        assert_eq!(probe::count(Probe::RangeTombstoneRetired), 1);
        assert_eq!(live_keys(&v, "t/").await, 0);
        assert!(v.get(b"u/after", v.read_time()).await.unwrap().is_some());
        tokio::time::sleep(Duration::from_millis(eng.shared.cfg.gc_grace_ms)).await;
        put(&db, "other", "y").await;
        settle().await;
        let m = db.read_version(Consistency::Strong).await.unwrap().m.clone();
        let known = known_objects(&m);
        let segs = store.mem.keys().into_iter().filter(|k| k.contains("/seg/")).collect::<Vec<_>>();
        assert!(segs.iter().all(|k| known.contains(k)), "unreferenced objects left: {segs:?}");
    });
}

#[test]
fn small_range_deletes_write_point_tombstones() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        let max = eng.shared.cfg.range_delete_points_max;
        fill(&db, max, 10, 100).await;
        let seq = delete_prefix(&db, "t/").await;
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert!(v.m.range_deletes.is_empty());
        let tombstones: Vec<Seq> = v.m.tail.iter().filter(|e| e.key.starts_with(b"t/")).map(|e| e.seq).collect();
        assert_eq!(tombstones, vec![seq; max]);
        assert_eq!(live_keys(&v, "t/").await, 0);
    });
}

#[test]
fn range_delete_preserves_subsequent_writes_in_same_txn() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        fill(&db, 50, 10, 50).await;
        let mut earlier = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        earlier.put(key(3), Value::Inline(Bytes::from_static(b"earlier"))).unwrap();
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.put(key(1), Value::Inline(Bytes::from_static(b"before"))).unwrap();
        t.delete_range(KeyRange::prefix(b"t/")).unwrap();
        t.put(key(2), Value::Inline(Bytes::from_static(b"after"))).unwrap();
        let (a, b) = tokio::join!(db.commit(earlier.into_request()), db.commit(t.into_request()));
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(b, a + 1);
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert_eq!(v.m.range_deletes.len(), 1);
        let got = v.scan(&KeyRange::prefix(b"t/"), false, 100, v.read_time()).await.unwrap();
        let got: Vec<(Bytes, Seq)> = got.into_iter().map(|e| (e.key, e.seq)).collect();
        assert_eq!(got, vec![(key(2), b)]);
    });
}

#[test]
fn range_deletes_conflict_with_covered_reads() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        fill(&db, 200, 100, 20).await;
        put(&db, "u/1", "x").await;
        settle().await;
        let begin = || db.begin(Isolation::Serializable, Consistency::Strong);
        let mut point = begin().await.unwrap();
        point.get(&key(5)).await.unwrap();
        point.put(Bytes::from_static(b"w/1"), Value::Inline(Bytes::from_static(b"1"))).unwrap();
        let mut range = begin().await.unwrap();
        range.scan(&KeyRange::half_open(key(100), Some(key(110))), false, 100).await.unwrap();
        range.put(Bytes::from_static(b"w/2"), Value::Inline(Bytes::from_static(b"1"))).unwrap();
        let mut missing = begin().await.unwrap();
        missing.get(b"t/zzzz").await.unwrap();
        missing.put(Bytes::from_static(b"w/3"), Value::Inline(Bytes::from_static(b"1"))).unwrap();
        let mut elsewhere = begin().await.unwrap();
        elsewhere.get(b"u/1").await.unwrap();
        elsewhere.put(Bytes::from_static(b"w/4"), Value::Inline(Bytes::from_static(b"1"))).unwrap();
        delete_prefix(&db, "t/").await;
        settle().await;
        assert!(probe::count(Probe::SegmentDropped) > 0);
        for (name, t) in [("point", point), ("range", range), ("missing key", missing)] {
            let r = db.commit(t.into_request()).await;
            assert!(matches!(r, Err(Error::Conflict(_))), "{name}: {r:?}");
        }
        db.commit(elsewhere.into_request()).await.unwrap();
    });
}

#[test]
fn partly_deleted_segments_are_rewritten() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = opened(&eng, "d").await;
        fill(&db, 300, 100, 20).await;
        settle().await;
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.delete_range(KeyRange::half_open(key(110), Some(key(190)))).unwrap();
        db.commit(t.into_request()).await.unwrap();
        settle().await;
        assert!(probe::count(Probe::SegmentRewritten) > 0);
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert!(v.m.segments.iter().all(|s| !v.m.range_deletes[0].dirties(s)));
        let got: Vec<Bytes> = v
            .scan(&KeyRange::prefix(b"t/"), false, usize::MAX, v.read_time())
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.key)
            .collect();
        let want: Vec<Bytes> = (0..300).filter(|i| !(110..190).contains(i)).map(key).collect();
        assert_eq!(got, want);
        let stored = v.all_entries(true).await.unwrap();
        assert!(stored.iter().all(|e| !(key(110)..key(190)).contains(&e.key)));
    });
}

#[test]
fn deeper_levels_move_to_levels_objects() {
    rt().block_on(async {
        let cfg = EngineConfig { levels_inline_max: 4, ..EngineConfig::small_for_tests() };
        let (eng, store) = delayed_engine_with("a", cfg);
        let db = opened(&eng, "d").await;
        fill(&db, 1500, 100, 25).await;
        settle().await;
        let v = db.read_version(Consistency::Strong).await.unwrap();
        let deep = v.m.deep_segments().len();
        assert!(deep > 4);
        let id = v.m.levels.clone().unwrap();
        let stored = store.mem.get_sync(&manifest_key("d"), &Default::default()).unwrap().data;
        let stored = manifest::Manifest::decode(&stored).unwrap().encode_body();
        let inline = manifest::Manifest { levels: None, ..v.m.clone() }.encode_body();
        assert!(stored.len() + 50 * deep < inline.len(), "{} bytes, inline {}", stored.len(), inline.len());
        assert!(probe::count(Probe::LevelsObjectWritten) > 1);
        let b = second_node(&eng, &store, "b");
        let vb = b.db("d").read_version(Consistency::Strong).await.unwrap();
        assert_eq!(vb.m.segments, v.m.segments);
        assert_eq!(live_keys(&vb, "t/").await, 1500);
        let obsolete_levels = v.m.obsolete.iter().filter(|o| o.kind == manifest::ObjKind::Levels).count();
        assert!(obsolete_levels > 0);
        tokio::time::sleep(Duration::from_millis(eng.shared.cfg.gc_grace_ms)).await;
        put(&db, "x", "1").await;
        settle().await;
        let m = db.read_version(Consistency::Strong).await.unwrap().m.clone();
        let objs: Vec<String> = store.mem.keys().into_iter().filter(|k| k.ends_with(LEVELS_SUFFIX)).collect();
        assert_eq!(objs, vec![levels_key("d", m.levels.as_deref().unwrap())]);
        let _ = id;
        delete_prefix(&db, "t/").await;
        settle().await;
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert!(v.m.deep_segments().len() <= 4 && v.m.levels.is_none(), "{:?}", v.m.levels);
    });
}

fn quota_engine(limit: u64) -> (Arc<Engine>, Arc<super::tests::DelayStore>) {
    delayed_engine_with("a", EngineConfig { db_max_bytes: limit, ..EngineConfig::small_for_tests() })
}

async fn try_put(db: &Arc<Db>, k: Bytes, v: Value) -> crate::error::Result<Seq> {
    let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await?;
    t.put(k, v)?;
    db.commit(t.into_request()).await
}

#[test]
fn quotas_refuse_growth_but_allow_deletes() {
    rt().block_on(async {
        let (eng, _store) = quota_engine(30_000);
        let db = opened(&eng, "d").await;
        let mut written = 0;
        let refused = loop {
            match try_put(&db, key(written), Value::Inline(Bytes::from(vec![b'q'; 400]))).await {
                Ok(_) => written += 1,
                Err(e) => break e,
            }
            assert!(written < 1000);
        };
        assert!(matches!(refused, Error::QuotaExceeded(_)), "{refused:?}");
        assert_eq!(refused.status(), 507);
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert!(v.m.stored_bytes() <= 30_000);
        assert_eq!(live_keys(&v, "t/").await, written);
        assert_eq!(probe::count(Probe::QuotaRefused), 1);
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.delete(key(0)).unwrap();
        db.commit(t.into_request()).await.unwrap();
        delete_prefix(&db, "t/").await;
        settle().await;
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert!(v.m.stored_bytes() < 5_000, "{} bytes left after deleting", v.m.stored_bytes());
        try_put(&db, key(0), Value::Inline(Bytes::from(vec![b'q'; 400]))).await.unwrap();
        let stats = db.stats(&db.current().unwrap());
        assert_eq!(stats.quota_bytes, Some(30_000));
        assert_eq!(stats.size_bytes, db.current().unwrap().m.stored_bytes());
    });
}

#[test]
fn staged_and_bound_blobs_count_against_the_quota() {
    rt().block_on(async {
        let (eng, _store) = quota_engine(50_000);
        let db = opened(&eng, "d").await;
        let mut a = db.blob_writer("x/y").await;
        a.write(&[1; 30_000]).await.unwrap();
        let mut b = db.blob_writer("x/y").await;
        let e = b.write(&[2; 30_000]).await.unwrap_err();
        assert!(matches!(e, Error::QuotaExceeded(_)), "{e:?}");
        let a = a.finish().await.unwrap();
        drop(b);
        let mut c = db.blob_writer("x/y").await;
        assert!(c.write(&[3; 30_000]).await.is_err());
        drop(c);
        let mut d = db.blob_writer("x/y").await;
        d.write(&[4; 10_000]).await.unwrap();
        let d = d.finish().await.unwrap();
        try_put(&db, Bytes::from_static(b"a"), Value::Blob(a.clone())).await.unwrap();
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert_eq!(v.m.blob_bytes.total(), 30_000);
        try_put(&db, Bytes::from_static(b"d"), Value::Blob(d)).await.unwrap();
        let mut e = db.blob_writer("x/y").await;
        e.write(&[5; 9_000]).await.unwrap();
        let e = e.finish().await.unwrap();
        try_put(&db, Bytes::from_static(b"e"), Value::Blob(e)).await.unwrap();
        let size = db.current().unwrap().m.stored_bytes();
        assert!(size > 49_000 && size <= 50_000, "{size}");
        let mut f = db.blob_writer("x/y").await;
        assert!(matches!(f.write(&[6; 1_000]).await, Err(Error::QuotaExceeded(_))));
        let g = blob::write_blob(&eng.shared, "d", &[7; 1_000], "x/y").await.unwrap();
        let r = try_put(&db, Bytes::from_static(b"g"), Value::Blob(g)).await;
        assert!(matches!(r, Err(Error::QuotaExceeded(_))), "{r:?}");
    });
}

#[test]
fn sweeps_recount_blob_bytes() {
    rt().block_on(async {
        let cfg =
            EngineConfig { db_max_bytes: 50_000, quota_sweep_interval_ms: 5_000, ..EngineConfig::small_for_tests() };
        let (eng, _store) = delayed_engine_with("a", cfg);
        let db = opened(&eng, "d").await;
        let blob = |n: usize, fill: u8| {
            let shared = eng.shared.clone();
            async move { blob::write_blob(&shared, "d", &vec![fill; n], "x/y").await.unwrap() }
        };
        try_put(&db, Bytes::from_static(b"k"), Value::Blob(blob(30_000, 1).await)).await.unwrap();
        try_put(&db, Bytes::from_static(b"k"), Value::Inline(Bytes::from_static(b"small"))).await.unwrap();
        assert_eq!(db.current().unwrap().m.blob_bytes.total(), 30_000);
        let r = try_put(&db, Bytes::from_static(b"j"), Value::Blob(blob(30_000, 2).await)).await;
        assert!(matches!(r, Err(Error::QuotaExceeded(_))), "{r:?}");
        tokio::time::sleep(Duration::from_millis(6_000)).await;
        put(&db, "nudge", "1").await;
        settle().await;
        let m = db.current().unwrap().m.clone();
        assert_eq!(m.blob_bytes.total(), 0, "{:?}", m.blob_bytes);
        try_put(&db, Bytes::from_static(b"j"), Value::Blob(blob(30_000, 3).await)).await.unwrap();
        assert_eq!(db.current().unwrap().m.blob_bytes.total(), 30_000);
    });
}

#[test]
fn exempt_databases_have_no_quota() {
    let (eng, _store) = quota_engine(10);
    eng.exempt_from_quota("sys");
    assert_eq!(eng.db("sys").quota(), None);
    assert_eq!(eng.db("d").quota(), Some(10));
    let (eng, _store) = delayed_engine("a");
    assert_eq!(eng.db("d").quota(), None, "0 means no quota");
}
