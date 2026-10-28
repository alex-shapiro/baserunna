//! Verifies that memory accounting matches actual heap usage using a counting global allocator.

use baserunna::engine::{Consistency, Engine, EngineConfig, Isolation, KeyRange, Value};
use baserunna::env::Env;
use baserunna::store::{DynStore, MemStore};
use bytes::Bytes;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering::Relaxed};

struct CountingAllocator;

static ALLOCATED_BYTES: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCATED_BYTES.fetch_add(l.size() as isize, Relaxed);
        unsafe { System.alloc(l) }
    }

    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        ALLOCATED_BYTES.fetch_sub(l.size() as isize, Relaxed);
        unsafe { System.dealloc(p, l) }
    }

    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCATED_BYTES.fetch_add(n as isize - l.size() as isize, Relaxed);
        unsafe { System.realloc(p, l, n) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn allocated_bytes() -> isize {
    ALLOCATED_BYTES.load(Relaxed)
}

const MIB: usize = 1024 * 1024;

struct World {
    env: Arc<Env>,
    store: DynStore,
}

impl World {
    fn new() -> World {
        let env = Arc::new(Env::sim(1, 1_700_000_000_000, tokio::time::Instant::now(), 0));
        let e = env.clone();
        World { store: Arc::new(MemStore::new(Box::new(move || e.now_ms()))), env }
    }

    fn engine(&self, node: &str, memory_bytes: usize) -> Arc<Engine> {
        let cfg = EngineConfig { memory_bytes, ..EngineConfig::default() };
        Engine::new(self.store.clone(), self.env.clone(), cfg, node.into())
    }
}

#[derive(Clone, Copy, Debug)]
struct DbShape {
    tail_entries: usize,
    key_bytes: usize,
    value_bytes: usize,
    range_deletes: usize,
}

async fn populate_db(eng: &Arc<Engine>, id: &str, s: DbShape) {
    let db = eng.db(id);
    db.open(true).await.unwrap();
    let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
    for i in 0..s.tail_entries {
        let mut k = format!("{i:0w$}", w = s.key_bytes).into_bytes();
        k.truncate(s.key_bytes.max(1));
        let value =
            if s.value_bytes == 0 { Value::Tombstone } else { Value::Inline(Bytes::from(vec![7u8; s.value_bytes])) };
        match value {
            Value::Tombstone => t.delete(Bytes::from(k)).unwrap(),
            v => t.put(Bytes::from(k), v).unwrap(),
        }
    }
    db.commit(t.into_request()).await.unwrap();
    for i in 0..s.range_deletes {
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
        let (a, b) = (format!("r{i:04}a"), format!("r{i:04}z"));
        t.delete_range(KeyRange::half_open(Bytes::from(a), Some(Bytes::from(b)))).unwrap();
        db.commit(t.into_request()).await.unwrap();
    }
}

async fn charges_cover_decoded_manifests(w: &World) {
    let writer = w.engine("w", 1 << 40);
    let shapes = [
        DbShape { tail_entries: 0, key_bytes: 4, value_bytes: 1, range_deletes: 0 },
        DbShape { tail_entries: 2000, key_bytes: 9, value_bytes: 100, range_deletes: 0 },
        DbShape { tail_entries: 6000, key_bytes: 4, value_bytes: 1, range_deletes: 0 },
        DbShape { tail_entries: 40, key_bytes: 16, value_bytes: 6000, range_deletes: 0 },
        DbShape { tail_entries: 3000, key_bytes: 24, value_bytes: 0, range_deletes: 0 },
        DbShape { tail_entries: 10, key_bytes: 8, value_bytes: 8, range_deletes: 200 },
    ];
    for (n, s) in shapes.iter().enumerate() {
        for i in 0..20 {
            populate_db(&writer, &format!("s{n}-{i}"), *s).await;
        }
    }
    for (n, s) in shapes.iter().enumerate() {
        let reader = w.engine("r", 1 << 40);
        let (heap_before, charged_before) = (allocated_bytes(), reader.shared.memory.used());
        let mut handles = Vec::new();
        for i in 0..20 {
            let db = reader.db(&format!("s{n}-{i}"));
            db.open(false).await.unwrap();
            handles.push(db);
        }
        let heap = (allocated_bytes() - heap_before).max(0) as usize;
        let charged = reader.shared.memory.used() - charged_before;
        eprintln!("{s:?}: heap {heap} B, charged {charged} B");
        assert!(heap <= charged + charged / 10 + 20 * 512, "{s:?}: {heap} heap bytes, but {charged} charged");
        assert!(charged <= 3 * heap + 20 * 2048, "{s:?}: {charged} charged for {heap} heap bytes");
    }
}

async fn opening_many_databases_stays_within_the_budget(w: &World) {
    let writer = w.engine("w2", 1 << 40);
    let dbs = 300;
    for i in 0..dbs {
        populate_db(
            &writer,
            &format!("big{i}"),
            DbShape { tail_entries: 2000, key_bytes: 9, value_bytes: 100, range_deletes: 0 },
        )
        .await;
    }
    let budget = 16 * MIB;
    let reader = w.engine("r2", budget);
    let before = allocated_bytes();
    let mut held = Vec::new();
    let mut peak = 0;
    let mut refused = 0;
    for i in 0..dbs {
        let db = reader.db(&format!("big{i}"));
        let mut t = match db.begin(Isolation::Serializable, Consistency::Strong).await {
            Ok(t) => t,
            Err(e) if e.retryable() => {
                refused += 1;
                continue;
            }
            Err(e) => panic!("{e}"),
        };
        assert!(t.get(b"000001234").await.unwrap().is_some());
        drop(t);
        held.push(db);
        peak = peak.max(allocated_bytes() - before);
    }
    let peak = peak.max(0) as usize;
    let charged = reader.shared.memory.used();
    eprintln!(
        "opened {} of {dbs} databases: heap peaked at {peak} B, {charged} B charged, budget {budget}",
        held.len()
    );
    assert_eq!(refused, 0, "idle databases should be unloaded rather than refusing loads");
    assert!(charged <= budget + MIB, "charged {charged} bytes with a budget of {budget}");
    assert!(peak <= budget + budget / 4 + 2 * MIB, "heap grew by {peak} bytes with a budget of {budget}");
}

#[test]
fn memory_charges_bound_real_heap_use() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
    rt.block_on(async {
        baserunna::log::set_level(Some(baserunna::log::Level::Warn));
        let w = World::new();
        charges_cover_decoded_manifests(&w).await;
        opening_many_databases_stays_within_the_budget(&w).await;
    });
}
