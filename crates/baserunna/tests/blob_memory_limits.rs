//! Verifies that transaction ops that read blob values stay within the node's memory budget.

use baserunna::node::NodeConfig;
use baserunna::sim::cost::{Resp, World, call_router};
use bytes::Bytes;
use serde_json::{Value as Json, json};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering::Relaxed};
use std::time::Duration;
use tokio::time::Instant;

struct CountingAllocator;

static ALLOCATED_BYTES: AtomicIsize = AtomicIsize::new(0);
static PEAK_BYTES: AtomicIsize = AtomicIsize::new(0);

fn add_allocated(by: isize) {
    let now = ALLOCATED_BYTES.fetch_add(by, Relaxed) + by;
    PEAK_BYTES.fetch_max(now, Relaxed);
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        add_allocated(l.size() as isize);
        unsafe { System.alloc(l) }
    }

    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        ALLOCATED_BYTES.fetch_sub(l.size() as isize, Relaxed);
        unsafe { System.dealloc(p, l) }
    }

    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        add_allocated(n as isize - l.size() as isize);
        unsafe { System.realloc(p, l, n) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

// Serializes tests so allocations from concurrent tests do not inflate PEAK_BYTES.
static TURN: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

fn reset_peak() -> isize {
    let now = ALLOCATED_BYTES.load(Relaxed);
    PEAK_BYTES.store(now, Relaxed);
    now
}

fn peak_growth(before: isize) -> usize {
    (PEAK_BYTES.load(Relaxed) - before).max(0) as usize
}

const MIB: usize = 1024 * 1024;
/// Simulated store latency for each request, as in `sim::cost::LATENCY_MS`.
const STORE_LATENCY_MS: u64 = 20;
/// Size of the blob value that each `check` op compares.
const BLOB_BYTES: usize = 256 * MIB;

/// Restores the production rate limits and database quota that `cost::node_config` disables.
fn production_limits(c: &mut NodeConfig) {
    let d = NodeConfig::default();
    c.limits = d.limits;
    c.engine.db_max_bytes = d.engine.db_max_bytes;
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / MIB as f64
}

fn excerpt(r: &Resp) -> String {
    String::from_utf8_lossy(&r.body[..r.body.len().min(300)]).into_owned()
}

/// Runs `f` on a multi-thread runtime with a real clock.
fn run_threaded<F: std::future::Future>(f: F) -> F::Output {
    let level = baserunna::log::level();
    baserunna::log::set_level(Some(baserunna::log::Level::Error));
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().expect("runtime");
    let out = rt.block_on(f);
    baserunna::log::set_level(level);
    out
}

struct Outcome {
    status: u16,
    elapsed: Duration,
    body: Json,
}

/// Starts sending each body to `path` at the same time.
fn spawn_posts(w: &World, token: &str, path: &str, bodies: Vec<String>) -> Vec<tokio::task::JoinHandle<Outcome>> {
    let router = w.node(0).router().expect("router");
    bodies
        .into_iter()
        .map(|b| {
            let (router, token, path) = (router.clone(), token.to_string(), path.to_string());
            tokio::spawn(async move {
                let started = Instant::now();
                let r = call_router(&router, "POST", &path, &token, Bytes::from(b)).await;
                Outcome { status: r.status, elapsed: started.elapsed(), body: r.json() }
            })
        })
        .collect()
}

async fn join_posts(tasks: Vec<tokio::task::JoinHandle<Outcome>>) -> Vec<Outcome> {
    let mut out = Vec::new();
    for t in tasks {
        out.push(t.await.expect("request task"));
    }
    out
}

fn statuses(out: &[Outcome]) -> Vec<u16> {
    out.iter().map(|o| o.status).collect()
}

/// Samples the peak usage of node 0's `MemoryGate` every 10 ms.
struct MemoryGateSampler {
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<usize>,
}

impl MemoryGateSampler {
    fn start(w: &World) -> MemoryGateSampler {
        let shared = w.node(0).engine.shared.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let task = tokio::spawn(async move {
            let mut peak = 0;
            while !stopped.load(Relaxed) {
                peak = peak.max(shared.memory.used());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            peak
        });
        MemoryGateSampler { stop, task }
    }

    async fn finish(self) -> usize {
        self.stop.store(true, Relaxed);
        self.task.await.expect("sampler")
    }
}

#[test]
#[ignore = "untriaged: check and increment ops read whole blobs into memory"]
fn full_blob_reads_respect_node_memory_budget() {
    let _turn = TURN.lock();
    run_threaded(async {
        let w = World::with_config(1, STORE_LATENCY_MS, production_limits).await;
        let t = w.signup(0, "w4@blob.test").await;
        w.create_db(0, &t, "d").await;
        let cfg = &w.node(0).cfg.engine;
        let budget = cfg.memory_bytes + cfg.cache_bytes;
        eprintln!(
            "node memory_bytes {} MiB, cache_bytes {} MiB, blob_max {} MiB",
            mib(cfg.memory_bytes),
            mib(cfg.cache_bytes),
            mib(cfg.blob_max as usize)
        );
        let started = Instant::now();
        let r = w.call(0, "PUT", "/v1/dbs/d/kv/big", &t, Bytes::from(vec![b'x'; BLOB_BYTES])).await;
        assert_eq!(r.status, 200, "{}", excerpt(&r));
        eprintln!("uploaded a {:.0} MiB blob in {:?}", mib(BLOB_BYTES), started.elapsed());
        // The expected value differs from the blob, so each check reads the whole blob and then fails.
        let check = json!({"ops": [{"op": "check", "key": "big", "value": "not the blob"}]}).to_string();
        let mut measured = Vec::new();
        for n in [1, 2, 4] {
            let gate_before = w.node(0).engine.shared.memory.used();
            let sampler = MemoryGateSampler::start(&w);
            let before = reset_peak();
            let out = join_posts(spawn_posts(&w, &t, "/v1/dbs/d/txn", vec![check.clone(); n])).await;
            let peak = peak_growth(before);
            let gate_growth = sampler.finish().await.saturating_sub(gate_before);
            let longest = out.iter().map(|o| o.elapsed).max().unwrap_or_default();
            eprintln!(
                "{n} concurrent checks of a {:.0} MiB blob value: statuses {:?}, heap peak +{:.0} MiB \
                 ({:.0} MiB per request), MemoryGate peak +{:.1} MiB, longest {longest:?}",
                mib(BLOB_BYTES),
                statuses(&out),
                mib(peak),
                mib(peak / n),
                mib(gate_growth),
            );
            for o in &out {
                assert_eq!(o.status, 412, "a check of a mismatched blob value returned {}", o.body);
            }
            measured.push((n, peak));
        }
        for (n, peak) in measured {
            assert!(
                peak <= budget + budget / 8,
                "{n} concurrent checks of a {:.0} MiB blob value grew the heap by {:.0} MiB; the node's memory \
                 budgets total {:.0} MiB",
                mib(BLOB_BYTES),
                mib(peak),
                mib(budget)
            );
        }
        w.shutdown();
    });
}
