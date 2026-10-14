//! Wall-clock throughput benchmark for a single node serving thousands of
//! active databases in-process through its HTTP router.
//!
//! ```text
//! cargo bench --bench throughput
//! BENCH_DBS=5000 BENCH_LATENCY_MS=20 BENCH_SECS=20 cargo bench --bench throughput
//! ```
//!
//! | variable           | default | meaning                                              |
//! | ------------------ | ------- | ---------------------------------------------------- |
//! | `BENCH_DBS`        | 2000    | active databases (one key each, spread over users)   |
//! | `BENCH_CLIENTS`    | 64      | concurrent clients                                   |
//! | `BENCH_SECS`       | 10      | measured duration                                    |
//! | `BENCH_WRITE_PCT`  | 10      | percentage of requests that are writes               |
//! | `BENCH_LATENCY_MS` | 0       | added per object-store request (20 emulates S3)      |
//! | `BENCH_CACHE_MB`   | 64      | block cache budget                                   |
//! | `BENCH_THREADS`    | 4       | runtime worker threads                               |

use baserunna::env::Env;
use baserunna::node::{Node, Services};
use baserunna::sim::cost::{MeteredStore, call_router, node_config};
use baserunna::store::{DynStore, MemStore};
use baserunna::util::rng::Rng;
use bytes::Bytes;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

const DBS_PER_USER: usize = 200;

fn main() {
    let threads = env("BENCH_THREADS", 4) as usize;
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(threads).enable_all().build().expect("runtime");
    rt.block_on(bench(threads));
}

async fn bench(threads: usize) {
    let dbs = env("BENCH_DBS", 2000) as usize;
    let clients = env("BENCH_CLIENTS", 64) as usize;
    let secs = env("BENCH_SECS", 10);
    let write_pct = env("BENCH_WRITE_PCT", 10);
    let latency = env("BENCH_LATENCY_MS", 0);
    baserunna::log::set_level(Some(baserunna::log::Level::Error));

    let store = Arc::new(MeteredStore::new(Arc::new(MemStore::with_system_clock()), 0));
    let mut cfg = node_config("bench", "http://127.0.0.1:0");
    cfg.engine.cache_bytes = env("BENCH_CACHE_MB", 64) as usize * 1024 * 1024;
    let dyn_store: DynStore = store.clone();
    let node = Node::start(cfg, Arc::new(Env::real()), dyn_store, Services::local()).await.expect("node starts");
    let router = baserunna::api::router(node.clone());

    let t0 = Instant::now();
    let mut tokens = Vec::new();
    for u in 0..dbs.div_ceil(DBS_PER_USER) {
        let body = json!({"email": format!("u{u}@bench.test"), "password": "password-123"});
        let r = call_router(&router, "POST", "/v1/auth/signup", "", Bytes::from(body.to_string())).await;
        tokens.push(r.json()["token"].as_str().expect("token").to_string());
    }
    let tokens = Arc::new(tokens);
    for d in 0..dbs {
        let t = &tokens[d / DBS_PER_USER];
        let body = Bytes::from(json!({"name": format!("d{d}")}).to_string());
        let r = call_router(&router, "POST", "/v1/dbs", t, body).await;
        assert_eq!(r.status, 201, "create db d{d}");
        let r = call_router(&router, "PUT", &format!("/v1/dbs/d{d}/kv/k"), t, Bytes::from_static(b"0")).await;
        assert_eq!(r.status, 200, "seed d{d}");
    }
    eprintln!("setup: {dbs} databases in {:.1}s", t0.elapsed().as_secs_f64());

    store.set_latency_ms(latency);
    let before = store.total();
    let stop = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    let mut hs = Vec::new();
    for c in 0..clients {
        let (router, tokens, stop) = (router.clone(), tokens.clone(), stop.clone());
        hs.push(tokio::spawn(async move {
            let mut rng = Rng::new(c as u64 + 1);
            let mut lat = Vec::new();
            let mut errors = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let d = rng.usize(dbs);
                let t = &tokens[d / DBS_PER_USER];
                let path = format!("/v1/dbs/d{d}/kv/k");
                let t1 = Instant::now();
                let r = if rng.below(100) < write_pct {
                    call_router(&router, "PUT", &path, t, Bytes::from(rng.next_u64().to_string())).await
                } else {
                    call_router(&router, "GET", &path, t, Bytes::new()).await
                };
                lat.push(t1.elapsed().as_micros() as u32);
                if r.status != 200 {
                    errors += 1;
                }
            }
            (lat, errors)
        }));
    }
    tokio::time::sleep(Duration::from_secs(secs)).await;
    stop.store(true, Ordering::Relaxed);
    let mut lat = Vec::new();
    let mut errors = 0;
    for h in hs {
        let (l, e) = h.await.expect("client");
        lat.extend(l);
        errors += e;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let reqs = store.total().total() - before.total();
    lat.sort_unstable();
    let pct = |p: f64| lat.get(((lat.len() as f64 * p) as usize).min(lat.len().saturating_sub(1))).copied();
    let ms = |v: Option<u32>| v.map(|v| v as f64 / 1000.0).unwrap_or(0.0);
    let cache = node.engine.shared.cache.stats();
    println!(
        "throughput: {dbs} dbs, {clients} clients, {threads} threads, {write_pct}% writes, {latency} ms store latency"
    );
    println!(
        "  ops/s            {:.0} ({} ops in {elapsed:.1}s, {errors} errors)",
        lat.len() as f64 / elapsed,
        lat.len()
    );
    println!(
        "  latency          p50 {:.2} ms, p99 {:.2} ms, max {:.2} ms",
        ms(pct(0.5)),
        ms(pct(0.99)),
        ms(lat.last().copied())
    );
    println!("  store requests   {:.3} per op", reqs / lat.len().max(1) as f64);
    println!("  open handles     {}", node.engine.open_count());
    println!("  cache            {} bytes in {} entries", cache.bytes, cache.entries);
    node.env.shutdown();
}
