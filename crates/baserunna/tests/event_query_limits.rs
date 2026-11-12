//! Verifies that event queries stay within the node's memory budgets and query slots.

use baserunna::node::NodeConfig;
use baserunna::sim::cost::{Resp, World, call_router, run_paused};
use bytes::Bytes;
use serde_json::{Value as Json, json};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering::Relaxed};
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
const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 24 * HOUR_MS;
const QUERY_TO_MS: i64 = 9_999_999_999_999;
/// Store latency for each request while queries run, so that part reads overlap.
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

async fn bulk_insert(w: &World, token: &str, db: &str, table: &str, rows: &[String]) {
    let body = format!("[{}]", rows.join(","));
    let r = post(w, token, &format!("/v1/dbs/{db}/events/{table}/rows"), body).await;
    assert_eq!(r.status, 201, "{}", excerpt(&r));
}

async fn table_info(w: &World, token: &str, db: &str, table: &str) -> Json {
    let r = w.call(0, "GET", &format!("/v1/dbs/{db}/events/{table}"), token, Bytes::new()).await;
    assert_eq!(r.status, 200, "{}", excerpt(&r));
    r.json()
}

/// Waits until background maintenance leaves the table's part count unchanged for 10 minutes.
async fn settle_parts(w: &World, token: &str, db: &str, table: &str) -> Json {
    let mut last = usize::MAX;
    loop {
        tokio::time::sleep(Duration::from_secs(600)).await;
        let info = table_info(w, token, db, table).await;
        let parts = info["parts"].as_array().map_or(0, Vec::len);
        if parts == last {
            return info;
        }
        last = parts;
    }
}

/// Restarts node 0 so that later reads start with an empty block cache.
async fn restart_with_cold_cache(w: &mut World) {
    w.node(0).stop();
    w.restart_node(0).await;
}

struct Outcome {
    status: u16,
    elapsed: Duration,
    body: Json,
}

/// Starts sending `bodies` to `path` at the same time.
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

/// Sends `bodies` to `path` at the same time and returns each outcome and the heap growth peak.
async fn concurrent_posts(w: &World, token: &str, path: &str, bodies: Vec<String>) -> (Vec<Outcome>, usize) {
    let before = reset_peak();
    let out = join_posts(spawn_posts(w, token, path, bodies)).await;
    (out, peak_growth(before))
}

fn statuses(out: &[Outcome]) -> Vec<u16> {
    out.iter().map(|o| o.status).collect()
}

/// Event of user `user` at `time_ms` with 1 of 8 event names.
fn user_event(time_ms: i64, user: usize, i: usize) -> String {
    format!(r#"{{"time": {time_ms}, "user": "user-{user:07}", "event": "e{}"}}"#, i % 8)
}

const USER_EVENTS_FROM_MS: i64 = 1_749_000_000_000;
const USER_EVENT_DAYS: i64 = 10;

/// Creates table `ev` in database `d` with `rows` events from `users` distinct users over 10 days.
async fn setup_user_events(w: &World, token: &str, rows: usize, users: usize) -> Json {
    w.create_db(0, token, "d").await;
    create_event_table(
        w,
        token,
        "d",
        json!({"name": "ev", "columns": [{"name": "time", "type": "time"}, {"name": "user", "type": "string"},
            {"name": "event", "type": "string"}], "time_column": "time", "order_by": ["time"]}),
    )
    .await;
    let span = USER_EVENT_DAYS * DAY_MS;
    for start in (0..rows).step_by(100_000) {
        let batch: Vec<String> = (start..rows.min(start + 100_000))
            .map(|i| user_event(USER_EVENTS_FROM_MS + (i as i64 * 7_919) % span, (i * 7_919) % users, i))
            .collect();
        bulk_insert(w, token, "d", "ev", &batch).await;
    }
    settle_parts(w, token, "d", "ev").await
}

/// Top 100 users by event count between `from` and `to`, a query that holds a group per distinct user.
fn top_users_query(from: i64, to: i64) -> Json {
    json!({"from": from, "to": to, "group_by": ["user"], "aggregates": {"n": {"fn": "count"}},
        "order_by": [{"col": "n", "desc": true}], "limit": 100})
}

#[test]
fn concurrent_event_queries_respect_node_memory_budget() {
    let _turn = TURN.lock();
    run_paused(async {
        let w = World::with_config(1, 0, production_limits).await;
        let t = w.signup(0, "a1@mem.test").await;
        let info = setup_user_events(&w, &t, 1_000_000, 500_000).await;
        let cfg = &w.node(0).cfg;
        let budget = cfg.engine.memory_bytes + cfg.engine.cache_bytes;
        eprintln!(
            "table: {} rows in {} parts, {} stored bytes; node memory_bytes {} MiB, cache_bytes {} MiB, \
             query_concurrency {}",
            info["rows"],
            info["parts"].as_array().map_or(0, Vec::len),
            info["bytes"],
            mib(cfg.engine.memory_bytes),
            mib(cfg.engine.cache_bytes),
            cfg.query_concurrency
        );
        w.store.set_latency_ms(STORE_LATENCY_MS);
        let path = "/v1/dbs/d/events/ev/rows/query";
        let q = top_users_query(0, QUERY_TO_MS).to_string();
        let (warm, _) = concurrent_posts(&w, &t, path, vec![q.clone()]).await;
        assert_eq!(warm[0].status, 200, "{}", warm[0].body);
        let mut peaks = Vec::new();
        for n in [1, 2, 4, 8] {
            let shared = &w.node(0).engine.shared;
            let (cache_before, gate_before) = (shared.cache.stats().bytes, shared.memory.used());
            let (out, peak) = concurrent_posts(&w, &t, path, vec![q.clone(); n]).await;
            let statuses = statuses(&out);
            let longest = out.iter().map(|o| o.elapsed).max().unwrap_or_default();
            eprintln!(
                "{n} concurrent queries: statuses {statuses:?}, heap peak +{:.1} MiB ({:.1} MiB per query), \
                 cache {:+.1} MiB, MemoryGate {:+.1} MiB, longest {longest:?}",
                mib(peak),
                mib(peak / n),
                (shared.cache.stats().bytes as f64 - cache_before as f64) / MIB as f64,
                (shared.memory.used() as f64 - gate_before as f64) / MIB as f64,
            );
            peaks.push((n, peak, statuses));
        }
        for (n, peak, statuses) in &peaks {
            let admitted = statuses.iter().filter(|s| **s == 200).count();
            assert!(admitted >= 1, "none of {n} concurrent queries succeeded: {statuses:?}");
            assert!(
                *peak <= budget + budget / 8,
                "{n} concurrent queries ({admitted} admitted) grew heap by {:.0} MiB; the node's memory budgets \
                 total {:.0} MiB",
                mib(*peak),
                mib(budget)
            );
        }
        w.shutdown();
    });
}

const HOURLY_FROM_MS: i64 = 1_650_000_000_000 / HOUR_MS * HOUR_MS;

/// Creates table `ev` in database `db` with an event in each of `hours` hourly partitions.
async fn setup_hourly_parts(w: &World, token: &str, db: &str, hours: usize) -> Json {
    w.create_db(0, token, db).await;
    create_event_table(
        w,
        token,
        db,
        json!({"name": "ev", "columns": [{"name": "time", "type": "time"}, {"name": "user", "type": "string"},
            {"name": "event", "type": "string"}], "time_column": "time", "order_by": ["time"], "partition": "hour"}),
    )
    .await;
    for start in (0..hours).step_by(2_000) {
        let batch: Vec<String> =
            (start..hours.min(start + 2_000)).map(|h| user_event(HOURLY_FROM_MS + h as i64 * HOUR_MS, h, h)).collect();
        bulk_insert(w, token, db, "ev", &batch).await;
    }
    settle_parts(w, token, db, "ev").await
}

fn count_query() -> Json {
    json!({"from": 0, "to": QUERY_TO_MS, "aggregates": {"n": {"fn": "count"}}})
}

fn txn_query_body(table: &str, query: &Json) -> String {
    let mut op = query.clone();
    op["op"] = json!("query_events");
    op["table"] = json!(table);
    json!({"ops": [op]}).to_string()
}

/// Commits a key so that node 0 holds the lease of database `db` before a timed request.
async fn acquire_lease(w: &World, token: &str, db: &str) {
    let r = w.call(0, "PUT", &format!("/v1/dbs/{db}/kv/lease"), token, Bytes::from_static(b"1")).await;
    assert_eq!(r.status, 200, "{}", excerpt(&r));
}

#[test]
fn one_user_cannot_hold_every_query_slot() {
    let _turn = TURN.lock();
    run_paused(async {
        let mut w = World::with_config(1, 0, production_limits).await;
        let heavy = w.signup(0, "heavy@slots.test").await;
        setup_hourly_parts(&w, &heavy, "d", 20_000).await;
        let light = w.signup(0, "light@slots.test").await;
        setup_hourly_parts(&w, &light, "small", 1).await;
        restart_with_cold_cache(&mut w).await;
        acquire_lease(&w, &heavy, "d").await;
        acquire_lease(&w, &light, "small").await;
        let light_path = "/v1/dbs/small/events/ev/rows/query";
        let r = post(&w, &light, light_path, count_query().to_string()).await;
        assert_eq!(r.status, 200, "{}", excerpt(&r));
        w.store.set_latency_ms(STORE_LATENCY_MS);
        let slots = w.node(0).cfg.query_concurrency;
        let heavy_tasks =
            spawn_posts(&w, &heavy, "/v1/dbs/d/events/ev/rows/query", vec![count_query().to_string(); slots]);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = Instant::now();
        let r = post(&w, &light, light_path, count_query().to_string()).await;
        let light_wait = started.elapsed();
        let heavy_out = join_posts(heavy_tasks).await;
        let heavy_times: Vec<Duration> = heavy_out.iter().map(|o| o.elapsed).collect();
        eprintln!(
            "{slots} query slots; heavy user statuses {:?}, durations {:?}; light user status {} after {light_wait:?}",
            statuses(&heavy_out),
            heavy_times,
            r.status
        );
        assert_eq!(
            r.status,
            200,
            "a user with a small query was rejected while another user ran {slots} queries: {}",
            excerpt(&r)
        );
        w.shutdown();
    });
}

#[test]
fn transactional_event_queries_take_query_slots() {
    let _turn = TURN.lock();
    run_paused(async {
        let mut w = World::with_config(1, 0, production_limits).await;
        let t = w.signup(0, "b1@slots.test").await;
        setup_user_events(&w, &t, 200_000, 100_000).await;
        restart_with_cold_cache(&mut w).await;
        acquire_lease(&w, &t, "d").await;
        w.store.set_latency_ms(STORE_LATENCY_MS);
        let q = top_users_query(0, QUERY_TO_MS);
        let slots = w.node(0).cfg.query_concurrency;
        let tasks = spawn_posts(&w, &t, "/v1/dbs/d/txn", vec![txn_query_body("ev", &q); 2 * slots]);
        let mut most_running = 0;
        while !tasks.iter().all(|t| t.is_finished()) {
            most_running = most_running.max(w.node(0).queries.running_count());
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let out = join_posts(tasks).await;
        eprintln!(
            "{} concurrent /txn queries: statuses {:?}, at most {most_running} running",
            2 * slots,
            statuses(&out)
        );
        for o in &out {
            assert!(o.status == 200 || (o.status == 503 && o.body["error"]["retryable"] == json!(true)), "{}", o.body);
        }
        assert!(out.iter().any(|o| o.status == 200), "no /txn query succeeded");
        assert!(
            (1..=slots).contains(&most_running),
            "{most_running} /txn queries ran at once on a node with {slots} query slots"
        );
        w.shutdown();
    });
}

#[test]
#[ignore = "TODO blocker: extend the deadline of transactional query_events ops"]
fn transactional_event_queries_get_the_event_query_deadline() {
    let _turn = TURN.lock();
    run_paused(async {
        let mut w = World::with_config(1, 0, production_limits).await;
        let t = w.signup(0, "b2@slots.test").await;
        setup_hourly_parts(&w, &t, "d", 20_000).await;
        let mut timed = Vec::new();
        for (route, path, body) in [
            ("REST", "/v1/dbs/d/events/ev/rows/query", count_query().to_string()),
            ("/txn", "/v1/dbs/d/txn", txn_query_body("ev", &count_query())),
        ] {
            w.store.set_latency_ms(0);
            restart_with_cold_cache(&mut w).await;
            acquire_lease(&w, &t, "d").await;
            w.store.set_latency_ms(STORE_LATENCY_MS);
            let started = Instant::now();
            let r = post(&w, &t, path, body).await;
            let elapsed = started.elapsed();
            eprintln!("{route}: status {} after {elapsed:?}: {}", r.status, excerpt(&r));
            timed.push((route, r, elapsed));
        }
        let (_, rest, rest_elapsed) = &timed[0];
        assert_eq!(rest.status, 200, "{}", excerpt(rest));
        assert!(*rest_elapsed > Duration::from_secs(10), "the REST query took only {rest_elapsed:?}");
        let (_, txn, txn_elapsed) = &timed[1];
        assert_eq!(
            txn.status,
            200,
            "a query that succeeds on the event query route in {rest_elapsed:?} failed in /txn after {txn_elapsed:?}: {}",
            excerpt(txn)
        );
        w.shutdown();
    });
}

const BLOB_DAYS_FROM_MS: i64 = 1_740_000_000_000 / DAY_MS * DAY_MS;
const BLOB_BYTES: usize = 60_000;
const BLOB_ROWS_PER_INSERT: usize = 500;
/// Bulk inserts per day. `event_merge_fan_in` parts merge into a single part.
const BLOB_INSERTS_PER_DAY: usize = 8;

/// Generates a unique string of `BLOB_BYTES` characters drawn from 2 letters.
fn blob(row: u64) -> String {
    let mut s = format!("{row:016x}");
    let mut x = row.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    while s.len() < BLOB_BYTES {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        for k in 0..64 {
            s.push(if (x >> k) & 1 == 0 { 'a' } else { 'b' });
        }
    }
    s.truncate(BLOB_BYTES);
    s
}

/// Creates table `blobs` in database `d` with a merged part per day for `days` days.
async fn setup_blob_parts(w: &World, token: &str, days: usize) -> Json {
    w.create_db(0, token, "d").await;
    create_event_table(
        w,
        token,
        "d",
        json!({"name": "blobs", "columns": [{"name": "time", "type": "time"}, {"name": "s", "type": "string"}],
            "time_column": "time", "order_by": ["time"]}),
    )
    .await;
    let mut row = 0u64;
    for day in 0..days {
        for _ in 0..BLOB_INSERTS_PER_DAY {
            let batch: Vec<String> = (0..BLOB_ROWS_PER_INSERT)
                .map(|_| {
                    row += 1;
                    let time = BLOB_DAYS_FROM_MS + day as i64 * DAY_MS + row as i64 * 1000;
                    format!(r#"{{"time": {time}, "s": "{}"}}"#, blob(row))
                })
                .collect();
            bulk_insert(w, token, "d", "blobs", &batch).await;
        }
    }
    settle_parts(w, token, "d", "blobs").await
}

fn print_parts(info: &Json) {
    for p in info["parts"].as_array().into_iter().flatten() {
        eprintln!(
            "part: {} rows, {:.1} MiB raw, {:.1} MiB stored, level {}",
            p["rows"],
            mib(p["raw_bytes"].as_u64().unwrap_or(0) as usize),
            mib(p["bytes"].as_u64().unwrap_or(0) as usize),
            p["level"]
        );
    }
}

/// Runs a distinct count of the long string column over `days` days on a cold block cache and
/// returns the heap peak outside the block cache.
async fn long_string_query_peak(w: &mut World, token: &str, days: usize) -> usize {
    w.store.set_latency_ms(0);
    restart_with_cold_cache(w).await;
    acquire_lease(w, token, "d").await;
    w.store.set_latency_ms(STORE_LATENCY_MS);
    let q = json!({"from": BLOB_DAYS_FROM_MS, "to": BLOB_DAYS_FROM_MS + days as i64 * DAY_MS,
        "aggregates": {"u": {"fn": "uniq", "of": {"col": "s"}}}});
    let shared = w.node(0).engine.shared.clone();
    let cache_before = shared.cache.stats().bytes;
    let (out, peak) = concurrent_posts(w, token, "/v1/dbs/d/events/blobs/rows/query", vec![q.to_string()]).await;
    let cache_growth = shared.cache.stats().bytes.saturating_sub(cache_before);
    let query_peak = peak.saturating_sub(cache_growth);
    let limit = w.node(0).cfg.query_limits.max_memory_bytes;
    let stats = &out[0].body["stats"];
    eprintln!(
        "{days} days ({} parts, {} granules): status {}, heap peak +{:.1} MiB, block cache +{:.1} MiB, \
         query peak {:.1} MiB ({:.2}x the {:.0} MiB limit)",
        stats["parts_read"],
        stats["granules_read"],
        out[0].status,
        mib(peak),
        mib(cache_growth),
        mib(query_peak),
        query_peak as f64 / limit as f64,
        mib(limit),
    );
    assert_eq!(out[0].status, 200, "{}", out[0].body);
    query_peak
}

#[test]
#[ignore = "TODO blocker: estimate granule run memory from actual string lengths"]
fn long_string_queries_respect_memory_limit() {
    let _turn = TURN.lock();
    run_paused(async {
        let days = 8;
        let mut w = World::with_config(1, 0, production_limits).await;
        let t = w.signup(0, "c@mem.test").await;
        let info = setup_blob_parts(&w, &t, days).await;
        print_parts(&info);
        let mut measured = Vec::new();
        for d in [1, 2, 4, 8] {
            measured.push(("tier-merged", d, long_string_query_peak(&mut w, &t, d).await));
        }
        tokio::time::sleep(Duration::from_millis(w.node(0).cfg.engine.event_settle_ms + HOUR_MS as u64)).await;
        let today = w.node(0).now_ms() as i64;
        bulk_insert(&w, &t, "d", "blobs", &[format!(r#"{{"time": {today}, "s": "x"}}"#)]).await;
        let info = settle_parts(&w, &t, "d", "blobs").await;
        print_parts(&info);
        for d in [4, 8] {
            measured.push(("settled", d, long_string_query_peak(&mut w, &t, d).await));
        }
        let limit = w.node(0).cfg.query_limits.max_memory_bytes;
        for (stage, d, query_peak) in measured {
            assert!(
                query_peak <= limit + limit / 2,
                "a query of {d} days of {stage} parts held {:.0} MiB outside the block cache against its {:.0} MiB \
                 memory limit",
                mib(query_peak),
                mib(limit)
            );
        }
        w.shutdown();
    });
}
