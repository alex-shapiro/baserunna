//! Measures the store requests and the strong-read latency of one node while manifest `PUT`s
//! fail with an unknown outcome.
//!
//! ```text
//! cargo run --release --example ambiguous_put_cost
//! AMBIGUOUS_COMMITS=100000 AMBIGUOUS_SEEDS=8 cargo run --release --example ambiguous_put_cost
//! ```
//!
//! One node runs one database on a paused clock. Each store request takes 20 ms. A strong read
//! starts at each multiple of the read interval. One commit starts at a seeded random instant in
//! each commit interval, so the workload is the same for each engine version. The store draws
//! each manifest `PUT` from a seeded generator: the `PUT` returns a connection error with the
//! ambiguous rate. The store applies `AMBIGUOUS_APPLIED_PCT` percent of these `PUT`s before the
//! error (lost response) and applies none of the others (lost request).
//!
//! Each row is the sum of all seeds. `req/commit` counts every store request of the run. `$/1M`
//! is the request cost of one million commits. `read ms` is the mean latency of the strong reads.
//! The run is deterministic: the same build prints the same rows.
//!
//! | variable                | default | meaning                                                      |
//! | ----------------------- | ------- | ------------------------------------------------------------ |
//! | `AMBIGUOUS_COMMITS`     | 20000   | commits per seed                                             |
//! | `AMBIGUOUS_SEEDS`       | 4       | seeds per row, one thread each                               |
//! | `AMBIGUOUS_APPLIED_PCT` | 50      | percent of ambiguous `PUT`s that the store applies           |
//! | `AMBIGUOUS_WORKLOAD`    | unset   | `<read ms>,<commit ms>`: replaces the four default workloads |

use baserunna::engine::{Consistency, Db, Engine, EngineConfig, Isolation, Value, manifest_key};
use baserunna::env::Env;
use baserunna::sim::cost::{Counts, LATENCY_MS, MeteredStore, run_paused};
use baserunna::store::{BoxFut, GetOpts, GetResult, ListPage, MemStore, ObjectStore, PutMode, SResult, StoreError};
use baserunna::util::rng::Rng;
use bytes::Bytes;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Duration;
use tokio::time::Instant;

struct Workload {
    name: &'static str,
    read_every_ms: u64,
    commit_every_ms: u64,
}

/// A hot database has at least 16 strong reads per 15 s and renews its lease for reads. A cold
/// database holds its lease only for 4.5 s after each commit.
const WORKLOADS: [Workload; 4] = [
    Workload { name: "hot-commit-1s", read_every_ms: 50, commit_every_ms: 1_000 },
    Workload { name: "hot-commit-10s", read_every_ms: 50, commit_every_ms: 10_000 },
    Workload { name: "cold-commit-3s", read_every_ms: 1_500, commit_every_ms: 3_000 },
    Workload { name: "cold-commit-15s", read_every_ms: 1_500, commit_every_ms: 15_000 },
];

const AMBIGUOUS_RATES: [f64; 3] = [0.0, 0.001, 0.01];

/// Object store wrapper that fails manifest `PUT`s with an unknown outcome at a fixed rate.
struct AmbiguousPutStore {
    mem: Arc<MemStore>,
    manifest_key: String,
    faults_enabled: AtomicBool,
    rng: Mutex<Rng>,
    ambiguous_rate: f64,
    applied_pct: u64,
    ambiguous_puts: AtomicU64,
}

impl ObjectStore for AmbiguousPutStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        self.mem.get(key, opts)
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move {
            let (ambiguous, applied) = {
                let mut rng = self.rng.lock();
                let faulty = key == self.manifest_key && self.faults_enabled.load(Relaxed);
                (faulty && rng.unit() < self.ambiguous_rate, rng.below(100) < self.applied_pct)
            };
            if !ambiguous {
                return self.mem.put_sync(key, data, &mode);
            }
            self.ambiguous_puts.fetch_add(1, Relaxed);
            if applied {
                let _ = self.mem.put_sync(key, data, &mode);
            }
            Err(StoreError::Unavailable("connection reset".into()))
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

/// Totals of one run, or the sum of several runs.
#[derive(Default)]
struct Totals {
    gets: u64,
    puts: u64,
    lists: u64,
    deletes: u64,
    commits: u64,
    failed_commits: u64,
    ambiguous_puts: u64,
    reads: u64,
    read_micros: u64,
}

impl Totals {
    fn requests(&self) -> Counts {
        Counts {
            gets: self.gets as f64,
            puts: self.puts as f64,
            lists: self.lists as f64,
            deletes: self.deletes as f64,
            put_bytes: 0.0,
        }
    }

    fn add(&mut self, t: &Totals) {
        self.gets += t.gets;
        self.puts += t.puts;
        self.lists += t.lists;
        self.deletes += t.deletes;
        self.commits += t.commits;
        self.failed_commits += t.failed_commits;
        self.ambiguous_puts += t.ambiguous_puts;
        self.reads += t.reads;
        self.read_micros += t.read_micros;
    }
}

#[derive(Default)]
struct OpStats {
    in_flight: AtomicU64,
    failed_commits: AtomicU64,
    reads: AtomicU64,
    read_micros: AtomicU64,
}

async fn commit(db: &Arc<Db>, value: u64) -> baserunna::Result<()> {
    let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await?;
    t.put(Bytes::from_static(b"k"), Value::Inline(Bytes::from(value.to_string())))?;
    db.commit_detailed(t.into_request()).await.map(drop)
}

async fn strong_read(db: &Arc<Db>) -> baserunna::Result<()> {
    db.snapshot(Consistency::Strong).await?.get(b"k").await.map(drop)
}

async fn run(w: &Workload, ambiguous_rate: f64, applied_pct: u64, commits: u64, seed: u64) -> Totals {
    let env = Arc::new(Env::sim(seed, 1_750_000_000_000, Instant::now(), 0));
    let clock = env.clone();
    let faulty = Arc::new(AmbiguousPutStore {
        mem: Arc::new(MemStore::new(Box::new(move || clock.now_ms()))),
        manifest_key: manifest_key("d"),
        faults_enabled: AtomicBool::new(false),
        rng: Mutex::new(Rng::new(seed)),
        ambiguous_rate,
        applied_pct,
        ambiguous_puts: AtomicU64::new(0),
    });
    let store = Arc::new(MeteredStore::new(faulty.clone(), LATENCY_MS));
    let engine = Engine::new(store.clone(), env.clone(), EngineConfig::default(), "n".into());
    let db = engine.db("d");
    db.open(true).await.expect("the database opens");
    commit(&db, 0).await.expect("the first commit succeeds");
    faulty.faults_enabled.store(true, Relaxed);
    let before = store.total();

    let stats = Arc::new(OpStats::default());
    let mut commit_offsets = Rng::new(seed ^ 0xc0117);
    let start = Instant::now();
    let reads_per_commit = w.commit_every_ms / w.read_every_ms;
    for i in 0..commits {
        let interval_start = start + Duration::from_millis(i * w.commit_every_ms);
        let commit_at = interval_start + Duration::from_millis(commit_offsets.below(w.commit_every_ms));
        stats.in_flight.fetch_add(1, Relaxed);
        tokio::spawn({
            let (db, stats) = (db.clone(), stats.clone());
            async move {
                tokio::time::sleep_until(commit_at).await;
                if commit(&db, i + 1).await.is_err() {
                    stats.failed_commits.fetch_add(1, Relaxed);
                }
                stats.in_flight.fetch_sub(1, Relaxed);
            }
        });
        for r in 0..reads_per_commit {
            tokio::time::sleep_until(interval_start + Duration::from_millis(r * w.read_every_ms)).await;
            let (db, stats) = (db.clone(), stats.clone());
            stats.in_flight.fetch_add(1, Relaxed);
            tokio::spawn(async move {
                let invoked = Instant::now();
                strong_read(&db).await.expect("a strong read succeeds");
                stats.reads.fetch_add(1, Relaxed);
                stats.read_micros.fetch_add(invoked.elapsed().as_micros() as u64, Relaxed);
                stats.in_flight.fetch_sub(1, Relaxed);
            });
        }
    }
    tokio::time::sleep_until(start + Duration::from_millis(commits * w.commit_every_ms)).await;
    while stats.in_flight.load(Relaxed) > 0 || db.busy() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let after = store.total();
    env.shutdown();
    Totals {
        gets: (after.gets - before.gets) as u64,
        puts: (after.puts - before.puts) as u64,
        lists: (after.lists - before.lists) as u64,
        deletes: (after.deletes - before.deletes) as u64,
        commits,
        failed_commits: stats.failed_commits.load(Relaxed),
        ambiguous_puts: faulty.ambiguous_puts.load(Relaxed),
        reads: stats.reads.load(Relaxed),
        read_micros: stats.read_micros.load(Relaxed),
    }
}

fn num(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn main() {
    let (commits, seeds) = (num("AMBIGUOUS_COMMITS", 20_000), num("AMBIGUOUS_SEEDS", 4));
    let applied_pct = num("AMBIGUOUS_APPLIED_PCT", 50);
    let custom = std::env::var("AMBIGUOUS_WORKLOAD").ok().map(|v| {
        let (read, commit) = v.split_once(',').expect("AMBIGUOUS_WORKLOAD is <read ms>,<commit ms>");
        let ms = |s: &str| s.trim().parse().expect("AMBIGUOUS_WORKLOAD intervals are milliseconds");
        Workload { name: "custom", read_every_ms: ms(read), commit_every_ms: ms(commit) }
    });
    baserunna::log::set_level(Some(baserunna::log::Level::Error));
    println!("{commits} commits per seed, {seeds} seeds, {applied_pct}% of ambiguous PUTs applied");
    println!(
        "{:<16} {:>9} {:>10} {:>10} {:>9} {:>11} {:>9} {:>9} {:>9} {:>10}",
        "workload", "ambiguous", "GET", "PUT", "other", "req/commit", "$/1M", "read ms", "amb PUTs", "failed"
    );
    for w in custom.as_ref().map_or(&WORKLOADS[..], std::slice::from_ref) {
        for rate in AMBIGUOUS_RATES {
            let mut sum = Totals::default();
            std::thread::scope(|scope| {
                let runs: Vec<_> = (0..seeds)
                    .map(|seed| scope.spawn(move || run_paused(run(w, rate, applied_pct, commits, seed))))
                    .collect();
                for r in runs {
                    sum.add(&r.join().expect("a run panicked"));
                }
            });
            let r = sum.requests();
            println!(
                "{:<16} {:>8.1}% {:>10} {:>10} {:>9} {:>11.4} {:>9.3} {:>9.4} {:>9} {:>10}",
                w.name,
                rate * 100.0,
                sum.gets,
                sum.puts,
                sum.lists + sum.deletes,
                r.total() / sum.commits as f64,
                r.dollars() / sum.commits as f64 * 1e6,
                sum.read_micros as f64 / sum.reads as f64 / 1e3,
                sum.ambiguous_puts,
                sum.failed_commits,
            );
        }
    }
}
