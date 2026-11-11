//! Verifies that request parsing, bulk event inserts, and event maintenance stay within the node's memory
//! budget and per-request limits.

use baserunna::node::NodeConfig;
use baserunna::probe::{self, Probe};
use baserunna::sim::cost::{Resp, World, call_router, run_paused};
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

fn heap_growth(before: isize) -> isize {
    ALLOCATED_BYTES.load(Relaxed) - before
}

const MIB: usize = 1024 * 1024;
const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 24 * HOUR_MS;
/// Simulated store latency for each request, as in `sim::cost::LATENCY_MS`.
const STORE_LATENCY_MS: u64 = 20;

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

async fn post(w: &World, token: &str, path: &str, body: impl Into<Bytes>) -> Resp {
    call_router(&w.node(0).router().expect("router"), "POST", path, token, body.into()).await
}

async fn create_event_table(w: &World, token: &str, db: &str, table: Json) {
    let r = post(w, token, &format!("/v1/dbs/{db}/events"), table.to_string()).await;
    assert_eq!(r.status, 201, "{}", excerpt(&r));
}

async fn bulk_insert(w: &World, token: &str, db: &str, table: &str, body: String) {
    let r = post(w, token, &format!("/v1/dbs/{db}/events/{table}/rows"), body).await;
    assert_eq!(r.status, 201, "{}", excerpt(&r));
}

async fn table_info(w: &World, token: &str, db: &str, table: &str) -> Json {
    let r = w.call(0, "GET", &format!("/v1/dbs/{db}/events/{table}"), token, Bytes::new()).await;
    assert_eq!(r.status, 200, "{}", excerpt(&r));
    r.json()
}

async fn db_info(w: &World, token: &str, db: &str) -> Json {
    let r = w.call(0, "GET", &format!("/v1/dbs/{db}"), token, Bytes::new()).await;
    assert_eq!(r.status, 200, "{}", excerpt(&r));
    r.json()
}

fn part_count(info: &Json) -> usize {
    info["parts"].as_array().map_or(0, Vec::len)
}

/// Returns a JSON array of rows `row(0), row(1), ...` that is at most `max_bytes` long.
fn json_rows(max_bytes: usize, row: impl Fn(usize) -> String) -> (String, usize) {
    let mut s = String::with_capacity(max_bytes);
    s.push('[');
    let mut n = 0;
    loop {
        let r = row(n);
        if s.len() + r.len() + 2 > max_bytes {
            break;
        }
        if n > 0 {
            s.push(',');
        }
        s.push_str(&r);
        n += 1;
    }
    s.push(']');
    (s, n)
}

struct Outcome {
    status: u16,
    elapsed: Duration,
    body: Json,
}

/// Starts sending each `(path, body)` request at the same time.
fn spawn_posts(w: &World, token: &str, requests: Vec<(String, String)>) -> Vec<tokio::task::JoinHandle<Outcome>> {
    let router = w.node(0).router().expect("router");
    requests
        .into_iter()
        .map(|(path, b)| {
            let (router, token) = (router.clone(), token.to_string());
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

/// Maximum values of node gauges observed while a [`GaugeSampler`] runs.
#[derive(Default, Clone, Copy, Debug)]
struct GaugeMaxima {
    memory_gate_bytes: usize,
    active_maintenance: usize,
    min_compaction_permits: usize,
}

/// Samples node 0's `MemoryGate` usage, active maintenance tasks, and free `compaction_slots` every 10 ms.
struct GaugeSampler {
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<GaugeMaxima>,
}

impl GaugeSampler {
    fn start(w: &World) -> GaugeSampler {
        let node = w.node(0).clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let task = tokio::spawn(async move {
            let shared = node.engine.shared.clone();
            let mut m = GaugeMaxima { min_compaction_permits: usize::MAX, ..GaugeMaxima::default() };
            while !stopped.load(Relaxed) {
                m.memory_gate_bytes = m.memory_gate_bytes.max(shared.memory.used());
                m.active_maintenance = m.active_maintenance.max(node.maintainer.active());
                m.min_compaction_permits = m.min_compaction_permits.min(shared.compaction_slots.available_permits());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            m
        });
        GaugeSampler { stop, task }
    }

    async fn finish(self) -> GaugeMaxima {
        self.stop.store(true, Relaxed);
        self.task.await.expect("sampler")
    }
}

/// Event table with a time column that defaults to the insert time.
fn defaulted_time_table(name: &str) -> Json {
    json!({"name": name, "columns": [{"name": "time", "type": "time", "default": "now"},
        {"name": "user", "type": "string"}, {"name": "event", "type": "string"}],
        "time_column": "time", "order_by": ["time"]})
}

type RowShape = (&'static str, fn(usize) -> String);

/// Small row shapes for transactional and bulk event inserts.
const ROW_SHAPES: &[RowShape] = &[
    ("{}", |_| "{}".to_string()),
    ("{\"event\":\"x\"}", |_| r#"{"event":"x"}"#.to_string()),
    ("user event", |i| {
        format!(r#"{{"time": {}, "user": "user-{:07}", "event": "e{}"}}"#, 1_749_000_000_000 + i as i64, i, i % 8)
    }),
];

/// Body of a `/txn` request with an `insert_events` op whose rows fill at most `max_bytes`.
fn txn_insert_body(table: &str, max_bytes: usize, row: fn(usize) -> String) -> (String, usize) {
    let head = format!(r#"{{"ops":[{{"op":"insert_events","table":"{table}","rows":"#);
    let (rows, n) = json_rows(max_bytes - head.len() - 3, row);
    (format!("{head}{rows}}}]}}"), n)
}

#[test]
#[ignore = "TODO blocker: stream JSON request bodies and charge bulk insert buffers to MemoryGate"]
fn txn_event_insert_parse_stays_near_body_size() {
    let _turn = TURN.lock();
    run_paused(async {
        let w = World::with_config(1, 0, production_limits).await;
        let t = w.signup(0, "d1@ingest.test").await;
        w.create_db(0, &t, "d").await;
        create_event_table(&w, &t, "d", defaulted_time_table("ev")).await;
        let limit = w.node(0).cfg.max_json_body;
        let mut measured = Vec::new();
        // The body is parsed before row validation, so unknown columns cost as much as valid rows.
        let unknown_column: (&str, fn(usize) -> String) = ("{\"a\":1}", |_| r#"{"a":1}"#.to_string());
        for &(shape, row) in ROW_SHAPES.iter().chain([&unknown_column]) {
            for size_mib in [4, 8, 16, 32] {
                let (body, rows) = txn_insert_body("ev", (size_mib * MIB).min(limit), row);
                let body_bytes = body.len();
                let gate_before = w.node(0).engine.shared.memory.used();
                let before = reset_peak();
                let r = post(&w, &t, "/v1/dbs/d/txn", body).await;
                let peak = peak_growth(before);
                eprintln!(
                    "rows {shape}: {:.1} MiB body, {rows} rows: status {}, heap peak +{:.0} MiB = {:.1}x the body, \
                     MemoryGate {:+.1} MiB: {}",
                    mib(body_bytes),
                    r.status,
                    mib(peak),
                    peak as f64 / body_bytes as f64,
                    (w.node(0).engine.shared.memory.used() as f64 - gate_before as f64) / MIB as f64,
                    excerpt(&r)
                );
                measured.push((shape, body_bytes, peak));
            }
        }
        for (shape, body_bytes, peak) in measured {
            assert!(
                peak <= 3 * body_bytes,
                "a /txn body of {:.0} MiB with rows {shape} grew the heap by {:.0} MiB, {:.1}x the body",
                mib(body_bytes),
                mib(peak),
                peak as f64 / body_bytes as f64
            );
        }
        w.shutdown();
    });
}

/// Maximum bulk insert body size used in tests, below the 32 MiB limit.
const BULK_BODY_BYTES: usize = 30 * MIB;

/// Runs `f` on a multi-thread runtime with a real clock.
fn run_threaded<F: std::future::Future>(f: F) -> F::Output {
    let level = baserunna::log::level();
    baserunna::log::set_level(Some(baserunna::log::Level::Error));
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().expect("runtime");
    let out = rt.block_on(f);
    baserunna::log::set_level(level);
    out
}

#[test]
#[ignore = "TODO blocker: stream JSON request bodies and charge bulk insert buffers to MemoryGate"]
fn concurrent_bulk_inserts_respect_node_memory_budget() {
    let _turn = TURN.lock();
    run_threaded(async {
        let defaults = NodeConfig::default();
        let budget = defaults.engine.memory_bytes + defaults.engine.cache_bytes;
        eprintln!(
            "node memory_bytes {} MiB, cache_bytes {} MiB",
            mib(defaults.engine.memory_bytes),
            mib(defaults.engine.cache_bytes)
        );
        let mut measured = Vec::new();
        for &(shape, row) in ROW_SHAPES.iter().filter(|(s, _)| *s != "{\"event\":\"x\"}") {
            // Run a 4 MiB request first to estimate the heap usage per body byte.
            let (w, t) = bulk_world().await;
            let (small, _) = json_rows(4 * MIB, row);
            let before = reset_peak();
            let r = post(&w, &t, "/v1/dbs/d/events/ev/rows", small.clone()).await;
            let per_byte = peak_growth(before) as f64 / small.len() as f64;
            eprintln!("rows {shape}: a 4 MiB bulk insert: status {}, heap peak {per_byte:.1}x the body", r.status);
            close_bulk_world(w, &t).await;
            let body_bytes = BULK_BODY_BYTES.min(((1u64 << 30) as f64 / per_byte) as usize);
            let (body, rows) = json_rows(body_bytes, row);
            let mut single = 0;
            for n in [1, 2, 4] {
                if n > 1 && single * n > 5 << 30 {
                    eprintln!("rows {shape}: skipping {n} requests, which would need about {:.0} MiB", mib(single * n));
                    continue;
                }
                // Start a fresh node for each round so that earlier merges do not overlap the measurement.
                let (w, t) = bulk_world().await;
                let requests = vec![("/v1/dbs/d/events/ev/rows".to_string(), body.clone()); n];
                let gate_before = w.node(0).engine.shared.memory.used();
                let sampler = GaugeSampler::start(&w);
                let before = reset_peak();
                let out = join_posts(spawn_posts(&w, &t, requests)).await;
                let peak = peak_growth(before);
                let gauges = sampler.finish().await;
                let longest = out.iter().map(|o| o.elapsed).max().unwrap_or_default();
                let gate_growth = gauges.memory_gate_bytes.saturating_sub(gate_before);
                eprintln!(
                    "rows {shape}: {n} concurrent bulk inserts of {:.1} MiB and {rows} rows: statuses {:?}, \
                     heap peak +{:.0} MiB ({:.0} MiB per request, {:.1}x the body), MemoryGate peak +{:.1} MiB, \
                     longest {longest:?}",
                    mib(body.len()),
                    statuses(&out),
                    mib(peak),
                    mib(peak / n),
                    peak as f64 / n as f64 / body.len() as f64,
                    mib(gate_growth),
                );
                if let Some(o) = out.iter().find(|o| o.status != 201) {
                    eprintln!("  rejected: {}", o.body);
                }
                let bound = close_bulk_world(w, &t).await;
                eprintln!("  the table holds {bound} rows of the {} sent", n * rows);
                if n == 1 {
                    single = peak;
                }
                measured.push((shape, n, peak));
            }
        }
        for (shape, n, peak) in measured {
            assert!(
                peak <= budget + budget / 8,
                "{n} concurrent bulk inserts of rows {shape} grew the heap by {:.0} MiB; the node's memory budgets \
                 total {:.0} MiB",
                mib(peak),
                mib(budget)
            );
        }
    });
}

/// Starts a node with database `d` and event table `ev`.
async fn bulk_world() -> (World, String) {
    let w = World::with_config(1, STORE_LATENCY_MS, production_limits).await;
    let t = w.signup(0, "d2@ingest.test").await;
    w.create_db(0, &t, "d").await;
    create_event_table(&w, &t, "d", defaulted_time_table("ev")).await;
    (w, t)
}

/// Waits for bulk insert flushes and maintenance to stop, stops the node, and returns the row count.
async fn close_bulk_world(w: World, token: &str) -> u64 {
    let mut idle_for = 0;
    while idle_for < 3 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        idle_for = if w.node(0).maintainer.active() == 0 { idle_for + 1 } else { 0 };
    }
    let rows = table_info(&w, token, "d", "ev").await["rows"].as_u64().unwrap_or(0);
    w.shutdown();
    rows
}

/// Request body that yields frames from a channel until the sender is dropped.
struct StalledBody {
    rx: tokio::sync::mpsc::Receiver<Bytes>,
    remaining: u64,
}

impl http_body::Body for StalledBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        self.rx.poll_recv(cx).map(|b| {
            b.map(|b| {
                self.remaining = self.remaining.saturating_sub(b.len() as u64);
                Ok(http_body::Frame::data(b))
            })
        })
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.remaining)
    }
}

const STALLED_BODY_BYTES: usize = 16 * MIB;
const STALLED_PREFIX_BYTES: usize = 8 * MIB;
const STALL: Duration = Duration::from_secs(600);

/// Sends a partial body to `path`, pauses for `STALL`, and returns the response status and retained heap bytes.
async fn stall_request_body(w: &World, token: &str, path: &str) -> (Option<u16>, isize) {
    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(64);
    let body = StalledBody { rx, remaining: STALLED_BODY_BYTES as u64 };
    let req = http::Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .header("content-length", STALLED_BODY_BYTES.to_string())
        .body(axum::body::Body::new(body))
        .expect("request");
    let router = w.node(0).router().expect("router");
    let before = ALLOCATED_BYTES.load(Relaxed);
    let task = tokio::spawn(async move {
        use tower::ServiceExt;
        let Ok(resp) = router.oneshot(req).await;
        resp.status().as_u16()
    });
    let chunk = format!("[{}", "{},".repeat(MIB / 3));
    for _ in 0..STALLED_PREFIX_BYTES / chunk.len() {
        tx.send(Bytes::from(chunk.clone().into_bytes())).await.expect("send");
    }
    let started = Instant::now();
    while started.elapsed() < STALL && !task.is_finished() {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let held = heap_growth(before);
    let status = match task.is_finished() {
        true => Some(task.await.expect("request task")),
        false => {
            drop(tx);
            let s = task.await.expect("request task");
            eprintln!(
                "  after the client closed the body: status {s}, heap {:+.1} MiB",
                heap_growth(before) as f64 / MIB as f64
            );
            None
        }
    };
    (status, held)
}

#[test]
#[ignore = "TODO blocker: stream JSON request bodies and charge bulk insert buffers to MemoryGate (body read timeout)"]
fn stalled_request_body_times_out() {
    let _turn = TURN.lock();
    run_paused(async {
        let w = World::with_config(1, 0, production_limits).await;
        let t = w.signup(0, "d3@ingest.test").await;
        w.create_db(0, &t, "d").await;
        create_event_table(&w, &t, "d", defaulted_time_table("ev")).await;
        let mut measured = Vec::new();
        for path in ["/v1/dbs/d/events/ev/rows", "/v1/dbs/d/txn"] {
            let (status, held) = stall_request_body(&w, &t, path).await;
            eprintln!(
                "{path}: {} MiB of a {} MiB body, then no data for {STALL:?} of test-clock time: response {status:?}, \
                 heap held at the end of the stall {:+.1} MiB",
                STALLED_PREFIX_BYTES / MIB,
                STALLED_BODY_BYTES / MIB,
                held as f64 / MIB as f64
            );
            measured.push((path, status));
        }
        for (path, status) in measured {
            assert!(status.is_some(), "{path}: a request body that stalled for {STALL:?} did not time out");
        }
        w.shutdown();
    });
}

/// Writes all of `data` to `s`.
async fn write_all(s: &tokio::net::TcpStream, mut data: &[u8]) {
    while !data.is_empty() {
        s.writable().await.expect("writable");
        match s.try_write(data) {
            Ok(n) => data = &data[n..],
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => panic!("write: {e}"),
        }
    }
}

#[test]
#[ignore = "TODO blocker: stream JSON request bodies and charge bulk insert buffers to MemoryGate (body read timeout)"]
fn stalled_request_body_times_out_over_tcp() {
    let _turn = TURN.lock();
    let level = baserunna::log::level();
    baserunna::log::set_level(Some(baserunna::log::Level::Error));
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().expect("runtime");
    rt.block_on(async {
        let w = World::with_config(1, 0, production_limits).await;
        let t = w.signup(0, "d3tcp@ingest.test").await;
        w.create_db(0, &t, "d").await;
        create_event_table(&w, &t, "d", defaulted_time_table("ev")).await;
        // Serves the router as `main.rs` does.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("listen");
        let addr = listener.local_addr().expect("address");
        let app = w.node(0).router().expect("router");
        tokio::spawn(async move {
            axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>()).await
        });
        let s = tokio::net::TcpStream::connect(addr).await.expect("connect");
        let before = ALLOCATED_BYTES.load(Relaxed);
        let head = format!(
            "POST /v1/dbs/d/events/ev/rows HTTP/1.1\r\nhost: {addr}\r\ncontent-type: application/json\r\n\
             authorization: Bearer {t}\r\ncontent-length: {STALLED_BODY_BYTES}\r\n\r\n["
        );
        write_all(&s, head.as_bytes()).await;
        write_all(&s, "{},".repeat(STALLED_PREFIX_BYTES / 3).as_bytes()).await;
        let started = Instant::now();
        let mut answer = None;
        while started.elapsed() < STALL {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let mut buf = [0u8; 512];
            match s.try_read(&mut buf) {
                Ok(0) => {
                    answer = Some(format!("connection closed after {:?}", started.elapsed()));
                    break;
                }
                Ok(n) => {
                    answer = Some(format!(
                        "response after {:?}: {}",
                        started.elapsed(),
                        String::from_utf8_lossy(&buf[..n.min(200)])
                    ));
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => {
                    answer = Some(format!("read error after {:?}: {e}", started.elapsed()));
                    break;
                }
            }
        }
        let held = heap_growth(before);
        eprintln!(
            "TCP: {} MiB of a {} MiB bulk insert body, then no data for up to {STALL:?}: {}, heap held {:+.1} MiB",
            STALLED_PREFIX_BYTES / MIB,
            STALLED_BODY_BYTES / MIB,
            answer.as_deref().unwrap_or("no response and the connection is open"),
            held as f64 / MIB as f64
        );
        drop(s);
        tokio::time::sleep(Duration::from_secs(1)).await;
        eprintln!(
            "TCP: after the client closed the connection: heap {:+.1} MiB",
            heap_growth(before) as f64 / MIB as f64
        );
        w.shutdown();
        assert!(answer.is_some(), "a request body that stalled for {STALL:?} over TCP did not time out");
    });
    baserunna::log::set_level(level);
}

/// Start of the day that holds every row of a merge measurement.
const MERGE_DAY_MS: i64 = 1_749_000_000_000 / DAY_MS * DAY_MS;

/// Small event `i` in the merge day, from 1 of 100,000 users and 8 event names.
fn small_event(i: usize) -> String {
    let time = MERGE_DAY_MS + (i as i64 * 7_919) % DAY_MS;
    format!(r#"{{"t":{time},"u":"u{:07}","e":"e{}"}}"#, i % 100_000, i % 8)
}

fn small_event_table(name: &str) -> Json {
    json!({"name": name, "columns": [{"name": "t", "type": "time"}, {"name": "u", "type": "string"},
        {"name": "e", "type": "string"}], "time_column": "t", "order_by": ["t"]})
}

/// Bulk inserts that each create a single part. `event_merge_fan_in` parts of a tier start a merge.
const PARTS_BEFORE_TRIGGER: usize = 7;

/// Inserts `rows` events into `table` as `PARTS_BEFORE_TRIGGER` parts of a size tier.
async fn insert_merge_inputs(w: &World, token: &str, db: &str, table: &str, rows: usize, first_row: usize) {
    let per_part = rows.div_ceil(PARTS_BEFORE_TRIGGER);
    for p in 0..PARTS_BEFORE_TRIGGER {
        let start = first_row + p * per_part;
        let end = (first_row + rows).min(start + per_part);
        let body = format!("[{}]", (start..end).map(small_event).collect::<Vec<_>>().join(","));
        bulk_insert(w, token, db, table, body).await;
    }
}

/// Single-row bulk insert that adds the final part needed to trigger a merge.
fn trigger_body(i: usize) -> String {
    format!("[{}]", small_event(i))
}

fn print_parts(info: &Json) {
    for p in info["parts"].as_array().into_iter().flatten() {
        eprintln!(
            "  part: {} rows, {:.1} MiB raw, {:.2} MiB stored, level {}",
            p["rows"],
            mib(p["raw_bytes"].as_u64().unwrap_or(0) as usize),
            mib(p["bytes"].as_u64().unwrap_or(0) as usize),
            p["level"]
        );
    }
}

/// Waits until `PartsMerged` reaches `target` or `limit` elapses, and returns the elapsed time.
async fn wait_for_merges(target: u64, limit: Duration) -> Option<Duration> {
    let started = Instant::now();
    while probe::count(Probe::PartsMerged) < target {
        if started.elapsed() > limit {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Some(started.elapsed())
}

struct MergePeak {
    rows: u64,
    peak: usize,
    retained: isize,
    stored_bytes: u64,
}

/// Waits until background maintenance leaves the table's part count unchanged for 10 minutes.
async fn settle_parts(w: &World, token: &str, db: &str, table: &str) -> Json {
    let mut last = usize::MAX;
    loop {
        tokio::time::sleep(Duration::from_secs(600)).await;
        let info = table_info(w, token, db, table).await;
        if part_count(&info) == last {
            return info;
        }
        last = part_count(&info);
    }
}

/// Rows per bulk insert during merge setup. Each insert creates a single part.
const ROWS_PER_INSERT: usize = 100_000;

/// Inserts `rows` small events into a past partition and returns the heap peak of the settling merge.
async fn measure_merge(rows: usize) -> MergePeak {
    let w = World::with_config(1, 0, production_limits).await;
    let t = w.signup(0, "e@merge.test").await;
    w.create_db(0, &t, "d").await;
    create_event_table(&w, &t, "d", small_event_table("ev")).await;
    for start in (0..rows).step_by(ROWS_PER_INSERT) {
        let body =
            format!("[{}]", (start..rows.min(start + ROWS_PER_INSERT)).map(small_event).collect::<Vec<_>>().join(","));
        bulk_insert(&w, &t, "d", "ev", body).await;
    }
    let info = settle_parts(&w, &t, "d", "ev").await;
    eprintln!("{rows} rows after tier merges:");
    print_parts(&info);
    // Advance past `event_settle_ms` and insert a row into another day to trigger the settling merge.
    tokio::time::sleep(Duration::from_millis(w.node(0).cfg.engine.event_settle_ms) + Duration::from_secs(3600)).await;
    let merged = probe::count(Probe::PartsMerged);
    let before = reset_peak();
    let other_day = format!(r#"[{{"t":{},"u":"x","e":"x"}}]"#, MERGE_DAY_MS - DAY_MS);
    bulk_insert(&w, &t, "d", "ev", other_day).await;
    let started = Instant::now();
    let mut took = None;
    while started.elapsed() < Duration::from_secs(3600) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if part_count(&table_info(&w, &t, "d", "ev").await) == 2 {
            took = Some(started.elapsed());
            break;
        }
    }
    eprintln!("{} merges ran while the partition settled", probe::count(Probe::PartsMerged) - merged);
    let peak = peak_growth(before);
    let retained = heap_growth(before);
    let info = table_info(&w, &t, "d", "ev").await;
    print_parts(&info);
    let parts: Vec<Json> =
        info["parts"].as_array().into_iter().flatten().filter(|p| p["rows"].as_u64() != Some(1)).cloned().collect();
    let out = MergePeak {
        rows: parts.iter().map(|p| p["rows"].as_u64().unwrap_or(0)).sum(),
        peak,
        retained,
        stored_bytes: parts.iter().map(|p| p["bytes"].as_u64().unwrap_or(0)).sum(),
    };
    eprintln!(
        "{rows} rows: merge done after {took:?}, {} parts left; heap peak +{:.0} MiB ({:.0} bytes per row), \
         retained {:+.0} MiB, transient {:.0} MiB ({:.0} bytes per row); stored part {:.1} MiB ({:.2} bytes per row)",
        parts.len(),
        mib(peak),
        peak as f64 / out.rows as f64,
        out.retained as f64 / MIB as f64,
        (peak as f64 - out.retained as f64) / MIB as f64,
        (peak as f64 - out.retained as f64) / out.rows as f64,
        mib(out.stored_bytes as usize),
        out.stored_bytes as f64 / out.rows as f64,
    );
    assert!(took.is_some(), "the merge of {rows} rows did not finish");
    w.shutdown();
    out
}

#[test]
#[ignore = "TODO blocker: bound memory usage during part merges"]
fn merge_memory_does_not_grow_with_rows() {
    let _turn = TURN.lock();
    run_paused(async {
        probe::record();
        let mut measured = Vec::new();
        for rows in [1_000_000, 2_000_000, 4_000_000, 8_000_000] {
            measured.push(measure_merge(rows).await);
        }
        probe::finish();
        let (small, large) = (&measured[0], &measured[measured.len() - 1]);
        let row_ratio = large.rows as f64 / small.rows as f64;
        let peak_ratio = large.peak as f64 / small.peak as f64;
        let cap = NodeConfig::default().engine.event_part_max_bytes;
        let stored_per_row = large.stored_bytes as f64 / large.rows as f64;
        let per_row = large.peak as f64 / large.rows as f64;
        eprintln!(
            "rows grew {row_ratio:.2}x and the merge heap peak grew {peak_ratio:.2}x; extrapolation to 100M rows: \
             heap peak {:.1} GiB, stored part {:.2} GiB against the {:.0} GiB merge planning cap",
            per_row * 1e8 / (1u64 << 30) as f64,
            stored_per_row * 1e8 / (1u64 << 30) as f64,
            cap as f64 / (1u64 << 30) as f64,
        );
        assert!(
            peak_ratio <= 1.5,
            "a merge of {} rows grew the heap by {:.0} MiB, {peak_ratio:.2}x the {:.0} MiB of a merge of {} rows",
            large.rows,
            mib(large.peak),
            mib(small.peak),
            small.rows
        );
    });
}

/// Production limits with a 1 ms merge timeout so that every merge fails.
fn merges_time_out(c: &mut NodeConfig) {
    production_limits(c);
    c.engine.event_merge_max_ms = 1;
}

/// Triggers maintenance with a single-row bulk insert and returns the store request counts for `db_id`.
async fn trigger_maintenance(w: &World, token: &str, db_id: &str, row: usize) -> baserunna::sim::cost::Counts {
    let before = w.store.scope(db_id);
    let other_day = format!(r#"[{{"t":{},"u":"x","e":"x"}}]"#, MERGE_DAY_MS - 30 * DAY_MS + row as i64);
    bulk_insert(w, token, "d", "ev", other_day).await;
    tokio::time::sleep(Duration::from_secs(60)).await;
    let after = w.store.scope(db_id);
    baserunna::sim::cost::Counts {
        gets: after.gets - before.gets,
        puts: after.puts - before.puts,
        lists: after.lists - before.lists,
        deletes: after.deletes - before.deletes,
        put_bytes: after.put_bytes - before.put_bytes,
    }
}

#[test]
#[ignore = "TODO blocker: bound memory usage during part merges (failed merges repeat after a restart)"]
fn failed_merge_is_not_retried_after_restart() {
    let _turn = TURN.lock();
    run_paused(async {
        let mut w = World::with_config(1, STORE_LATENCY_MS, merges_time_out).await;
        let t = w.signup(0, "e2@merge.test").await;
        let db_id = w.create_db(0, &t, "d").await;
        create_event_table(&w, &t, "d", small_event_table("ev")).await;
        insert_merge_inputs(&w, &t, "d", "ev", 7_000, 0).await;
        let first = trigger_maintenance(&w, &t, &db_id, 0).await;
        bulk_insert(&w, &t, "d", "ev", trigger_body(7_000)).await;
        tokio::time::sleep(Duration::from_secs(60)).await;
        let failing = w.store.scope(&db_id);
        let info = table_info(&w, &t, "d", "ev").await;
        eprintln!("after the eighth part: {} parts", part_count(&info));
        let control = trigger_maintenance(&w, &t, &db_id, 1).await;
        eprintln!("insert into another partition without a merge: {first:?}");
        eprintln!("insert into another partition while the failed merge is remembered: {control:?}");
        let stopped = w.store.scope(&db_id);
        w.node(0).stop();
        w.restart_node(0).await;
        let r = w.call(0, "PUT", "/v1/dbs/d/kv/lease", &t, Bytes::from_static(b"1")).await;
        assert_eq!(r.status, 200, "{}", excerpt(&r));
        tokio::time::sleep(Duration::from_secs(60)).await;
        let reopened = w.store.scope(&db_id);
        eprintln!(
            "restart and a KV write: {} GETs, {} PUTs, {} LISTs, {} DELETEs",
            reopened.gets - stopped.gets,
            reopened.puts - stopped.puts,
            reopened.lists - stopped.lists,
            reopened.deletes - stopped.deletes
        );
        let after_restart = trigger_maintenance(&w, &t, &db_id, 2).await;
        eprintln!("insert into another partition after a restart: {after_restart:?}");
        let info = table_info(&w, &t, "d", "ev").await;
        eprintln!("{} parts at the end; store requests before the restart {failing:?}", part_count(&info));
        w.shutdown();
        assert!(
            after_restart.deletes <= control.deletes && after_restart.gets <= control.gets + 2.0,
            "after a restart, maintenance attempted the failed merge again: {after_restart:?}, compared with \
             {control:?} before the restart"
        );
    });
}

const SPREAD_FROM_MS: i64 = 1_700_000_000_000 / HOUR_MS * HOUR_MS;

#[test]
#[ignore = "TODO blocker: limit partitions per bulk insert"]
fn bulk_insert_partitions_are_limited() {
    let _turn = TURN.lock();
    run_paused(async {
        let mut measured = Vec::new();
        for k in [100usize, 1_000, 10_000] {
            let w = World::with_config(1, 0, production_limits).await;
            let t = w.signup(0, "f@partitions.test").await;
            let db_id = w.create_db(0, &t, "d").await;
            create_event_table(
                &w,
                &t,
                "d",
                json!({"name": "ev", "columns": [{"name": "t", "type": "time"}], "time_column": "t",
                    "order_by": ["t"], "partition": "hour"}),
            )
            .await;
            let body = format!(
                "[{}]",
                (0..k)
                    .map(|h| format!(r#"{{"t":{}}}"#, SPREAD_FROM_MS + h as i64 * HOUR_MS))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let body_bytes = body.len();
            let size_before = db_info(&w, &t, "d").await["stats"]["size_bytes"].as_u64().unwrap_or(0);
            let stored_before = w.stored_bytes(&db_id);
            w.store.set_latency_ms(STORE_LATENCY_MS);
            let puts_before = w.store.scope(&db_id).puts;
            let before = reset_peak();
            let started = Instant::now();
            let task = spawn_posts(&w, &t, vec![("/v1/dbs/d/events/ev/rows".to_string(), body)]).pop().expect("task");
            let (mut answered, mut puts_at_answer) = (None, 0.0);
            let (mut last_puts, mut last_put_at) = (puts_before, Duration::ZERO);
            let mut heap_max_during = 0isize;
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                heap_max_during = heap_max_during.max(heap_growth(before));
                let puts = w.store.scope(&db_id).puts;
                if puts != last_puts {
                    (last_puts, last_put_at) = (puts, started.elapsed());
                }
                if answered.is_none() && task.is_finished() {
                    answered = Some(started.elapsed());
                    puts_at_answer = puts - puts_before;
                }
                if (answered.is_some() && started.elapsed() > last_put_at + Duration::from_secs(120))
                    || started.elapsed() > Duration::from_secs(4 * 3600)
                {
                    break;
                }
            }
            let out = task.await.expect("request task");
            let peak = peak_growth(before);
            w.store.set_latency_ms(0);
            tokio::time::sleep(Duration::from_secs(60)).await;
            let info = table_info(&w, &t, "d", "ev").await;
            let size_after = db_info(&w, &t, "d").await["stats"]["size_bytes"].as_u64().unwrap_or(0);
            let stored_after = w.stored_bytes(&db_id);
            let total_puts = last_puts - puts_before;
            eprintln!(
                "K={k}: {:.2} MiB body; status {} after {:?}: {}",
                mib(body_bytes),
                out.status,
                out.elapsed,
                out.body.to_string().chars().take(200).collect::<String>()
            );
            eprintln!(
                "  PUTs: {puts_at_answer} at response arrival, {total_puts} in total, the last at {last_put_at:?} \
                 after request start; heap peak +{:.1} MiB, heap growth sampled up to +{:.1} MiB",
                mib(peak),
                heap_max_during as f64 / MIB as f64
            );
            eprintln!(
                "  table: {} rows in {} parts; database size_bytes {:+} bytes, stored objects {:+} bytes",
                info["rows"],
                part_count(&info),
                size_after as i64 - size_before as i64,
                stored_after as i64 - stored_before as i64
            );
            // Commits keep the lease so that the lease holder's hourly sweeps can remove unbound parts.
            for hour in 0..3 {
                let r = w.call(0, "PUT", "/v1/dbs/d/kv/sweep", &t, Bytes::from(hour.to_string())).await;
                assert_eq!(r.status, 200, "{}", excerpt(&r));
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
            eprintln!(
                "  stored objects 3 hours later {:+} bytes",
                w.stored_bytes(&db_id) as i64 - stored_before as i64
            );
            measured.push((k, out.status, total_puts, puts_at_answer, answered));
            w.shutdown();
        }
        for (k, status, total, at_answer, answered) in measured {
            assert!(
                total <= at_answer + 2.0,
                "a bulk insert into {k} partitions (status {status}) caused {} PUTs after its response at {answered:?}",
                total - at_answer
            );
            assert!(total <= 1_000.0, "a single bulk insert into {k} partitions caused {total} PUTs");
        }
    });
}

/// Rows in each table of a maintenance concurrency measurement.
const CONCURRENT_MERGE_ROWS: usize = 500_000;

#[test]
#[ignore = "TODO blocker: limit event maintenance concurrency across the node and charge its memory"]
fn event_merges_share_a_node_wide_limit() {
    let _turn = TURN.lock();
    run_paused(async {
        probe::record();
        let w = World::with_config(1, 0, production_limits).await;
        let t = w.signup(0, "g@maint.test").await;
        let compaction = w.node(0).cfg.engine.compaction_concurrency;
        let mut measured = Vec::new();
        for tables in [1usize, 4, 16] {
            let db = format!("g{tables}");
            w.create_db(0, &t, &db).await;
            for i in 0..tables {
                let name = format!("ev{i}");
                create_event_table(&w, &t, &db, small_event_table(&name)).await;
                insert_merge_inputs(&w, &t, &db, &name, CONCURRENT_MERGE_ROWS, i * CONCURRENT_MERGE_ROWS).await;
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
            assert_eq!(part_count(&table_info(&w, &t, &db, "ev0").await), PARTS_BEFORE_TRIGGER);
            w.store.set_latency_ms(STORE_LATENCY_MS);
            let merged = probe::count(Probe::PartsMerged);
            let gate_before = w.node(0).engine.shared.memory.used();
            let sampler = GaugeSampler::start(&w);
            let before = reset_peak();
            let triggers: Vec<(String, String)> =
                (0..tables).map(|i| (format!("/v1/dbs/{db}/events/ev{i}/rows"), trigger_body(i))).collect();
            let out = join_posts(spawn_posts(&w, &t, triggers)).await;
            let took = wait_for_merges(merged + tables as u64, Duration::from_secs(3600)).await;
            let peak = peak_growth(before);
            let gauges = sampler.finish().await;
            w.store.set_latency_ms(0);
            eprintln!(
                "{tables} tables of {CONCURRENT_MERGE_ROWS} rows: trigger statuses {:?}; all merges done after \
                 {took:?}; heap peak +{:.0} MiB ({:.0} MiB per table); at most {} tables in maintenance at once; \
                 fewest free compaction slots {} of {compaction}; MemoryGate peak {:+.1} MiB",
                statuses(&out),
                mib(peak),
                mib(peak / tables),
                gauges.active_maintenance,
                gauges.min_compaction_permits,
                (gauges.memory_gate_bytes as f64 - gate_before as f64) / MIB as f64,
            );
            assert!(took.is_some(), "the merges of {tables} tables did not finish");
            measured.push((tables, peak, gauges));
        }
        probe::finish();
        let single = measured[0].1;
        let budget = w.node(0).cfg.engine.memory_bytes;
        for (tables, peak, _) in &measured {
            assert!(
                *peak <= (2 * single).max(budget),
                "merges in {tables} tables grew the heap by {:.0} MiB, {:.1}x the {:.0} MiB of a single merge",
                mib(*peak),
                *peak as f64 / single as f64,
                mib(single)
            );
        }
        w.shutdown();
    });
}
