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
/// The most bytes allocated at once since [`reset_peak`].
static PEAK_BYTES: AtomicIsize = AtomicIsize::new(0);

fn grew(by: isize) {
    let now = ALLOCATED_BYTES.fetch_add(by, Relaxed) + by;
    PEAK_BYTES.fetch_max(now, Relaxed);
}

/// Tests take turns, so that one's allocations do not count toward another's peak.
static TURN: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn reset_peak() -> isize {
    let now = ALLOCATED_BYTES.load(Relaxed);
    PEAK_BYTES.store(now, Relaxed);
    now
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        grew(l.size() as isize);
        unsafe { System.alloc(l) }
    }

    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        ALLOCATED_BYTES.fetch_sub(l.size() as isize, Relaxed);
        unsafe { System.dealloc(p, l) }
    }

    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        grew(n as isize - l.size() as isize);
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
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
    rt.block_on(async {
        baserunna::log::set_level(Some(baserunna::log::Level::Warn));
        let w = World::new();
        charges_cover_decoded_manifests(&w).await;
        opening_many_databases_stays_within_the_budget(&w).await;
    });
}

/// A bulk insert holds memory within a small multiple of its body while it is parsed,
/// buffered and written as parts, however small its events are.
#[test]
fn bulk_inserts_hold_memory_near_their_body_size() {
    use baserunna::sim::cost::{World, call_router, run_paused};
    use tower::ServiceExt;
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    run_paused(async {
        let w = World::new(1, 0).await;
        let t = w.signup(0, "mem@api.test").await;
        w.create_db(0, &t, "d").await;
        let router = w.node(0).router().unwrap();
        let table = r#"{"name": "ev", "columns": [{"name": "time", "type": "time", "default": "now"},
            {"name": "event", "type": "string"}], "time_column": "time", "order_by": ["event", "time"]}"#;
        let r = call_router(&router, "POST", "/v1/dbs/d/events", &t, Bytes::from(table)).await;
        assert_eq!(r.status, 201, "{}", String::from_utf8_lossy(&r.body));
        let line = "{\"event\":\"a\"}\n";
        let body = line.repeat((32 * MIB) / line.len() - 10);
        let req = http::Request::builder()
            .method("POST")
            .uri("/v1/dbs/d/events/ev/rows")
            .header("authorization", format!("Bearer {t}"))
            .header("content-type", "application/x-ndjson")
            .body(axum::body::Body::from(body.clone()))
            .unwrap();
        let before = reset_peak();
        let resp = router.oneshot(req).await.unwrap();
        let grown = PEAK_BYTES.load(Relaxed) - before;
        assert_eq!(resp.status(), 201);
        assert!(
            grown < 16 * body.len() as isize,
            "a {} byte bulk insert of tiny events grew the heap by {grown} bytes ({}x its body)",
            body.len(),
            grown / body.len() as isize
        );
        w.shutdown();
    });
}

/// Small, well-framed but corrupt part metadata and column indexes do not
/// decode into gigabytes: 30M granules or statistics take 2 bytes each raw
/// and tens of bytes each decoded.
#[test]
fn a_tiny_corrupt_column_index_does_not_decode_into_gigabytes() {
    use baserunna::events::part::{ColIndex, ColMeta, ColRef, PartMeta};
    use baserunna::events::vector::Phys;
    use baserunna::util::codec::Writer;
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    let framed = |w: Writer| {
        let mut out = Vec::new();
        baserunna::engine::segment::frame(&w.into_inner(), baserunna::engine::segment::Compression::Zstd(1), &mut out);
        Bytes::from(out)
    };
    let n = 30_000_000u64;

    let mut w = Writer::new();
    w.raw(b"BRPT");
    w.u8(1);
    w.varint(n);
    w.varint(n);
    for _ in 0..n {
        w.varint(1);
    }
    w.varint(0);
    for _ in 0..=n {
        w.varint(0);
    }
    for _ in 0..3 {
        w.varint(0);
    }
    let meta = framed(w);
    let before = reset_peak();
    let decoded = PartMeta::decode(&meta, n);
    let grown = PEAK_BYTES.load(Relaxed) - before;
    let granules = decoded.map(|m| m.granule_count());
    assert!(
        grown < 256 * MIB as isize,
        "{} bytes of part metadata decoded to {granules:?} granules, growing the heap by {grown} bytes",
        meta.len()
    );

    let part = PartMeta {
        rows: 1,
        granules: vec![1],
        starts: vec![0, 1],
        sort_ids: vec![],
        pk: vec![Bytes::new(); 2],
        cols: vec![ColMeta {
            cref: ColRef::Ordinal,
            phys: Phys::Int,
            json: false,
            index_off: 16,
            index_len: 0,
            data_off: 0,
            data_len: 16,
            nonnull: 1,
        }],
        shared: Default::default(),
        partial: Default::default(),
    };
    let mut w = Writer::new();
    w.u8(0);
    w.varint(1);
    for v in [0, 16, 0] {
        w.varint(v);
    }
    w.varint(n);
    for _ in 0..n {
        w.varint(0);
        w.u8(0);
    }
    w.varint(0);
    let index = framed(w);
    let before = reset_peak();
    let decoded = ColIndex::decode(&index, &part, &part.cols[0]);
    let grown = PEAK_BYTES.load(Relaxed) - before;
    let stats = decoded.map(|i| i.stats.len());
    assert!(
        grown < 256 * MIB as isize,
        "a {} byte column index decoded to {stats:?} statistics, growing the heap by {grown} bytes",
        index.len()
    );
}

/// Events that take a column's default grow by it, so a request of `{}`
/// events against a table with the largest default expands about 300
/// times; bulk inserts (held to 8 times their body) and transactions alike
/// refuse once the expanded events pass the request's limit, and hold no
/// more than it meanwhile.
#[test]
fn events_expanded_by_defaults_stay_within_their_request_limit() {
    use baserunna::sim::cost::{World, call_router, run_paused};
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    run_paused(async {
        let w = World::new(1, 0).await;
        let t = w.signup(0, "mem@api.test").await;
        w.create_db(0, &t, "d").await;
        let router = w.node(0).router().unwrap();
        let pad = "x".repeat(baserunna::events::schema::DEFAULT_MAX_BYTES - 2);
        let table = format!(
            r#"{{"name": "ev", "columns": [{{"name": "time", "type": "time", "default": "now"}},
            {{"name": "pad", "type": "string", "default": "{pad}"}}], "time_column": "time", "order_by": ["time"]}}"#
        );
        let r = call_router(&router, "POST", "/v1/dbs/d/events", &t, Bytes::from(table)).await;
        assert_eq!(r.status, 201, "{}", String::from_utf8_lossy(&r.body));
        let events = |n: usize| vec!["{}"; n].join(",");
        let bulk = format!("[{}]", events(400_000));
        let txn = format!(r#"{{"ops": [{{"op": "insert_events", "table": "ev", "rows": [{}]}}]}}"#, events(100_000));
        for (path, body) in [("/v1/dbs/d/events/ev/rows", bulk), ("/v1/dbs/d/txn", txn)] {
            let before = reset_peak();
            let r = call_router(&router, "POST", path, &t, Bytes::from(body.clone())).await;
            let grown = PEAK_BYTES.load(Relaxed) - before;
            assert_eq!(r.status, 413, "{path}: {}", String::from_utf8_lossy(&r.body));
            assert!(
                grown < 192 * MIB as isize,
                "{path}: a {} byte body grew the heap by {grown} bytes ({}x)",
                body.len(),
                grown / body.len() as isize
            );
        }
        w.shutdown();
    });
}
