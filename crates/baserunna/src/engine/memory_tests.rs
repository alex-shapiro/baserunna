//! Tests for the node memory budget, database unloading, and transaction admission.

use super::tests::{delayed_engine, delayed_engine_with, opened, put, put_many, rt, second_node};
use super::*;
use crate::error::Error;
use bytes::Bytes;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;

const MIB: usize = 1024 * 1024;

fn config_with_memory_budget(memory_bytes: usize) -> EngineConfig {
    EngineConfig { memory_bytes, ..EngineConfig::default() }
}

async fn open_and_fill_db(eng: &Arc<Engine>, id: &str, kib: usize) -> Arc<Db> {
    let db = opened(eng, id).await;
    put_many(&db, "k", kib / 2, 2048).await;
    db
}

#[test]
fn least_recently_used_idle_databases_are_unloaded() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine_with("a", config_with_memory_budget(16 * MIB));
        let mem = eng.shared.memory.clone();
        let mut peak = 0;
        for i in 0..150 {
            drop(open_and_fill_db(&eng, &format!("d{i}"), 200).await);
            peak = peak.max(mem.used());
            if i == 120 {
                let db = eng.db("d0");
                let r =
                    db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap().get(b"k07").await.unwrap();
                assert!(r.is_some());
            }
        }
        assert!(peak <= mem.limit() + 700 * 1024, "memory peaked at {peak} with budget {}", mem.limit());
        let is_loaded = |id: &str| eng.db_if_open(id).is_some_and(|d| d.current().is_some());
        assert!(is_loaded("d149") && is_loaded("d148"));
        assert!(!is_loaded("d1") && !is_loaded("d50"));
        assert!(is_loaded("d0"));
        assert!(eng.open_count() < 150);
        assert!(crate::probe::count(crate::probe::Probe::DatabaseUnloaded) > 0);
    });
}

#[test]
fn unloading_cannot_reclaim_versions_pinned_by_transactions() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine_with("a", config_with_memory_budget(16 * MIB));
        let mut pins = Vec::new();
        while eng.shared.reclaim_to_budget() {
            let db = open_and_fill_db(&eng, &format!("d{}", pins.len()), 200).await;
            pins.push(db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap());
            assert!(pins.len() < 200);
        }
        assert!(!eng.shared.reclaim_to_budget(), "unloading released memory pinned by transactions");
        let db = open_and_fill_db(&eng, "loaded-anyway", 200).await;
        put(&db, "k", "v").await;
        let first = pins.remove(0);
        let read = first.read_version();
        drop(first);
        assert!(read > 0);
        drop(pins);
        assert!(eng.shared.reclaim_to_budget());
    });
}

#[test]
fn system_databases_are_exempt_from_memory_budget() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine_with("a", config_with_memory_budget(16 * MIB));
        eng.mark_system("sys");
        let sys = open_and_fill_db(&eng, "sys", 200).await;
        let used = eng.shared.memory.used();
        assert!(used < 64 * 1024, "system database charged {used} bytes");
        let mut pins = Vec::new();
        while eng.shared.reclaim_to_budget() {
            let db = open_and_fill_db(&eng, &format!("d{}", pins.len()), 200).await;
            pins.push(db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap());
        }
        assert!(eng.shared.memory.is_exceeded());
        assert!(!sys.unload());
    });
}

#[test]
fn unloaded_lease_holder_retains_lease() {
    rt().block_on(async {
        let (a, store) = delayed_engine("a");
        let db = opened(&a, "d").await;
        put(&db, "k", "a").await;
        assert!(db.unload());
        assert!(db.current().is_none());
        let b = second_node(&a, &store, "b");
        let other = b.db("d");
        let mut t = other.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        t.put(Bytes::from_static(b"k"), Value::Inline(Bytes::from_static(b"b"))).unwrap();
        assert!(matches!(other.commit(t.into_request()).await, Err(Error::NotOwner { .. })));
        put(&db, "k", "a2").await;
        let gets = store.gets.lock().len();
        let v = db.read_version(Consistency::Strong).await.unwrap();
        assert_eq!(
            v.get(b"k", v.read_time()).await.unwrap().map(|e| e.value),
            Some(Value::Inline(Bytes::from_static(b"a2")))
        );
        assert_eq!(store.gets.lock().len(), gets);
        assert!(db.unload());
        assert!(db.release_lease().await.unwrap());
        tokio::time::sleep(Duration::from_millis(1)).await;
        other.refresh(tokio::time::Instant::now(), false).await.unwrap();
        put(&other, "k", "b").await;
    });
}

#[test]
fn databases_with_in_flight_work_are_not_unloaded() {
    rt().block_on(async {
        let (a, store) = delayed_engine("a");
        let db = opened(&a, "d").await;
        put(&db, "k", "v").await;
        store.put_reply_ms.store(1_000, SeqCst);
        let writer = db.clone();
        let commit = tokio::spawn(async move { put(&writer, "k", "w").await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!db.unload());
        store.put_reply_ms.store(0, SeqCst);
        commit.await.unwrap();
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        assert!(db.unload());
    });
}

#[test]
fn transactions_charge_snapshot_read_set_and_write_set() {
    rt().block_on(async {
        let (eng, _store) = delayed_engine("a");
        let db = open_and_fill_db(&eng, "d", 20).await;
        let mem = eng.shared.memory.clone();
        let idle = mem.used();
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let begun = mem.used();
        assert!(begun > idle);
        for i in 0..100 {
            t.get(format!("k{i:02}").as_bytes()).await.unwrap();
        }
        let read = mem.used();
        assert!(read >= begun + 100 * 4);
        t.put(Bytes::from_static(b"big"), Value::Inline(Bytes::from(vec![0u8; 10_000]))).unwrap();
        assert!(mem.used() >= read + 10_000);
        db.commit(t.into_request()).await.unwrap();
        let after = mem.used();
        assert!(after < idle + 12_000);
    });
}

#[test]
fn transaction_read_set_size_is_bounded() {
    rt().block_on(async {
        let cfg = EngineConfig { txn_max_read_bytes: 2_000, ..EngineConfig::small_for_tests() };
        let (eng, _store) = delayed_engine_with("a", cfg);
        let db = opened(&eng, "d").await;
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let mut err = None;
        for i in 0..1000 {
            if let Err(e) = t.get(format!("key-{i:04}").as_bytes()).await {
                err = Some((i, e));
                break;
            }
        }
        match err {
            Some((i, Error::TooLarge(_))) => assert!((10..100).contains(&i)),
            other => panic!("{other:?}"),
        }
    });
}
