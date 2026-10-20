//! Measures commit latency across manifest tail limits (`tail_max_bytes`) and
//! raw object-store `PUT`/`GET` latency by object size.
//!
//! ```text
//! BASERUNNA_STORE=s3://bucket cargo run --release --example commit_latency
//! LAT_TAILS_KB=16,64,256 LAT_WRITERS=1,8 cargo run --release --example commit_latency
//! ```
//!
//! | variable           | default            | meaning                                               |
//! | ------------------ | ------------------ | ----------------------------------------------------- |
//! | `BASERUNNA_STORE`  | required           | store URL (`mem:` for a dry run); uses `<url>/bench-<random>` |
//! | `LAT_TAILS_KB`     | `16,32,64,128,256` | tail limits in KiB                                    |
//! | `LAT_WRITERS`      | `1,8`              | concurrent writers                                    |
//! | `LAT_VALUE_BYTES`  | 256                | bytes per written value                               |
//! | `LAT_KEYS`         | 2000               | distinct keys written                                 |
//! | `LAT_CYCLES`       | 3                  | multiples of the tail limit written per run           |
//! | `LAT_RAW_BYTES`    | `1024,16384,…`     | object sizes for raw `PUT` and `GET` measurements     |
//! | `LAT_RAW_COUNT`    | 20                 | requests per size in raw measurements                 |

use baserunna::engine::{Consistency, Engine, EngineConfig, Isolation, Value};
use baserunna::env::Env;
use baserunna::sim::cost::MeteredStore;
use baserunna::store::{CloudStore, DynStore, MemStore, PutMode};
use baserunna::util::rng::Rng;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn list(name: &str, default: &str) -> Vec<u64> {
    let v = std::env::var(name).unwrap_or_else(|_| default.to_string());
    v.split(',').map(|s| s.trim().parse().unwrap_or_else(|_| panic!("{name}: bad number {s:?}"))).collect()
}

fn num(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn pct(sorted: &[Duration], p: f64) -> f64 {
    let i = ((sorted.len() as f64 * p) as usize).min(sorted.len().saturating_sub(1));
    sorted.get(i).map_or(0.0, |d| d.as_secs_f64() * 1e3)
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().expect("runtime");
    rt.block_on(run());
}

async fn run() {
    let url = std::env::var("BASERUNNA_STORE").expect("BASERUNNA_STORE is required");
    let mut rng = Rng::new(Env::real().random_u64());
    let prefix = format!("bench-{:016x}", rng.next_u64());
    let cloud: DynStore = match url.as_str() {
        "mem:" => Arc::new(MemStore::with_system_clock()),
        _ => Arc::new(CloudStore::from_url(&format!("{}/{prefix}", url.trim_end_matches('/'))).expect("store url")),
    };
    baserunna::log::set_level(Some(baserunna::log::Level::Error));
    println!("store {url}/{prefix}");

    println!("\nraw latency by object size ({} each)", num("LAT_RAW_COUNT", 20));
    println!("      bytes  PUT p50  PUT p90  GET p50  GET p90");
    for bytes in list("LAT_RAW_BYTES", "1024,16384,65536,131072,262144") {
        let (mut puts, mut gets) = (Vec::new(), Vec::new());
        for i in 0..num("LAT_RAW_COUNT", 20) {
            let data = Bytes::from((0..bytes).map(|_| rng.next_u64() as u8).collect::<Vec<u8>>());
            let key = format!("raw/{bytes}/{i}");
            let t = Instant::now();
            cloud.put(&key, data, PutMode::Overwrite).await.expect("put");
            puts.push(t.elapsed());
            let t = Instant::now();
            cloud.get(&key, Default::default()).await.expect("get");
            gets.push(t.elapsed());
        }
        puts.sort();
        gets.sort();
        let p = |v: &[Duration], q| pct(v, q);
        println!(
            "  {bytes:>9}  {:>7.1}  {:>7.1}  {:>7.1}  {:>7.1}",
            p(&puts, 0.5),
            p(&puts, 0.9),
            p(&gets, 0.5),
            p(&gets, 0.9)
        );
    }

    let value_bytes = num("LAT_VALUE_BYTES", 256) as usize;
    let keys = num("LAT_KEYS", 2000);
    let cycles = num("LAT_CYCLES", 3);
    println!("\ncommits of one {value_bytes}-byte put over {keys} keys");
    println!("  tail KiB  writers  commits  p50 ms  p90 ms  p99 ms  max ms  PUTs/commit  KiB/PUT");
    for tail_kb in list("LAT_TAILS_KB", "16,32,64,128,256") {
        for writers in list("LAT_WRITERS", "1,8") {
            let store = Arc::new(MeteredStore::new(cloud.clone(), 0));
            let cfg = EngineConfig { tail_max_bytes: tail_kb as usize * 1024, ..EngineConfig::default() };
            let dyn_store: DynStore = store.clone();
            let engine = Engine::new(dyn_store, Arc::new(Env::real()), cfg, format!("lat-{tail_kb}-{writers}"));
            let db = engine.db(&format!("t{tail_kb}w{writers}"));
            db.open(true).await.expect("open");
            let commits = (cycles * tail_kb * 1024 / value_bytes as u64).max(200);
            let warmup = commits / 3;
            let before = std::sync::Mutex::new(None);
            let lat = std::sync::Mutex::new(Vec::new());
            let next = std::sync::atomic::AtomicU64::new(0);
            let work = (0..writers).map(|w| {
                let (db, lat, next, before, store) = (db.clone(), &lat, &next, &before, &store);
                let mut rng = Rng::new(rng.next_u64() ^ w);
                async move {
                    loop {
                        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if i >= commits {
                            return;
                        }
                        if i == warmup {
                            *before.lock().unwrap() = Some(store.total());
                        }
                        let t = Instant::now();
                        let mut txn = db.begin(Isolation::Serializable, Consistency::Strong).await.expect("begin");
                        let key = Bytes::from(format!("k{:06}", rng.below(keys)));
                        txn.put(key, Value::Inline(Bytes::from(rng.text(value_bytes)))).expect("put");
                        db.commit(txn.into_request()).await.expect("commit");
                        if i >= warmup {
                            lat.lock().unwrap().push(t.elapsed());
                        }
                    }
                }
            });
            futures_join_all(work.collect()).await;
            let (after, before) = (store.total(), before.lock().unwrap().take().unwrap_or_default());
            let mut lat = lat.into_inner().unwrap();
            lat.sort();
            let n = lat.len() as f64;
            let puts = after.puts - before.puts;
            println!(
                "  {tail_kb:>8}  {writers:>7}  {:>7}  {:>6.1}  {:>6.1}  {:>6.1}  {:>6.1}  {:>11.3}  {:>7.1}",
                lat.len(),
                pct(&lat, 0.5),
                pct(&lat, 0.9),
                pct(&lat, 0.99),
                pct(&lat, 1.0),
                puts / n,
                (after.put_bytes - before.put_bytes) / puts.max(1.0) / 1024.0,
            );
            engine.env().shutdown();
        }
    }

    let mut after: Option<String> = None;
    let mut deleted = 0;
    loop {
        let page = cloud.list("", after.as_deref(), 1000).await.expect("list");
        for o in &page.objects {
            cloud.delete(&o.key).await.expect("delete");
            deleted += 1;
        }
        match page.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    println!("\ndeleted {deleted} objects under {prefix}");
}

async fn futures_join_all<F: std::future::Future<Output = ()>>(futs: Vec<F>) {
    let mut futs: Vec<std::pin::Pin<Box<F>>> = futs.into_iter().map(Box::pin).collect();
    std::future::poll_fn(|cx| {
        futs.retain_mut(|f| f.as_mut().poll(cx).is_pending());
        if futs.is_empty() { std::task::Poll::Ready(()) } else { std::task::Poll::Pending }
    })
    .await
}
