//! HTTP load generator for a deployed Baserunna server. Prints throughput and
//! latency percentiles per operation at each concurrency step. Request IDs use
//! the format `lg-<step>-<worker>-<n>`; step 0 is the warmup phase.
//!
//! ```text
//! LOAD_TOKEN=<api key> cargo run --release --example loadgen
//! LOAD_TOKEN=... LOAD_STEPS=8,32,128 LOAD_MIX=put=100 cargo run --release --example loadgen
//! ```
//!
//! | variable           | default                        | description                                           |
//! | ------------------ | ------------------------------ | ----------------------------------------------------- |
//! | `LOAD_URL`         | `https://baserunna.fly.dev`    | target server URL                                     |
//! | `LOAD_TOKEN`       | required                       | API key or session token for the test account         |
//! | `LOAD_DBS`         | 8                              | database count (`loadtest-<i>`, created if missing)   |
//! | `LOAD_KEYS`        | 1000                           | keys preloaded per database before the run            |
//! | `LOAD_VALUE_BYTES` | 256                            | byte length of written values                         |
//! | `LOAD_MIX`         | `get=80,put=15,txn=4,append=1` | relative weights of `get`, `put`, `scan`, `txn`, `append` |
//! | `LOAD_STEPS`       | `4,16,64`                      | concurrency levels executed in order                  |
//! | `LOAD_SECS`        | 20                             | duration in seconds of each measured step             |
//! | `LOAD_WARMUP_SECS` | 5                              | warmup duration in seconds before the first step      |
//! | `LOAD_CLEANUP`     | 0                              | set to `1` to delete the test databases after the run |

use baserunna::util::rng::Rng;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

fn env_str(name: &str, default: &str) -> String {
    std::env::var(name).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| default.to_string())
}

fn env_u64(name: &str, default: u64) -> u64 {
    let v = env_str(name, &default.to_string());
    v.parse().unwrap_or_else(|_| panic!("{name} must be a number, got {v:?}"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Op {
    Get,
    Put,
    Scan,
    Txn,
    Append,
}

impl Op {
    fn parse(s: &str) -> Op {
        match s {
            "get" => Op::Get,
            "put" => Op::Put,
            "scan" => Op::Scan,
            "txn" => Op::Txn,
            "append" => Op::Append,
            _ => panic!("unknown op {s:?} in LOAD_MIX (expected get, put, scan, txn, or append)"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Op::Get => "get",
            Op::Put => "put",
            Op::Scan => "scan",
            Op::Txn => "txn",
            Op::Append => "append",
        }
    }
}

#[derive(Clone)]
struct Config {
    url: String,
    token: String,
    db_count: usize,
    keys_per_db: u64,
    value_bytes: usize,
    mix: Vec<(Op, u64)>,
}

impl Config {
    fn db_name(&self, i: usize) -> String {
        format!("loadtest-{i}")
    }

    fn pick_op(&self, rng: &mut Rng) -> Op {
        let total: u64 = self.mix.iter().map(|(_, w)| w).sum();
        let mut n = rng.below(total);
        for &(op, w) in &self.mix {
            if n < w {
                return op;
            }
            n -= w;
        }
        unreachable!()
    }
}

#[derive(Default)]
struct Stats {
    /// Latencies of successful requests, in microseconds.
    latencies_us: BTreeMap<Op, Vec<u32>>,
    /// Failed requests by operation and HTTP status (`0` for transport errors).
    failures: BTreeMap<(Op, u16), u64>,
    throttled: u64,
    first_error: Option<String>,
}

impl Stats {
    fn merge(&mut self, other: Stats) {
        for (op, mut samples) in other.latencies_us {
            self.latencies_us.entry(op).or_default().append(&mut samples);
        }
        for (k, n) in other.failures {
            *self.failures.entry(k).or_default() += n;
        }
        self.throttled += other.throttled;
        if self.first_error.is_none() {
            self.first_error = other.first_error;
        }
    }
}

struct Response {
    status: u16,
    body: Value,
}

async fn send(cfg: &Config, req: reqwest::RequestBuilder) -> Result<Response, String> {
    let resp = req.bearer_auth(&cfg.token).send().await.map_err(|e| e.to_string())?;
    let status = resp.status().as_u16();
    let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
    Ok(Response { status, body: serde_json::from_slice(&bytes).unwrap_or(Value::Null) })
}

fn build_request(http: &reqwest::Client, cfg: &Config, rng: &mut Rng, op: Op) -> reqwest::RequestBuilder {
    let db = cfg.db_name(rng.usize(cfg.db_count));
    let base = format!("{}/v1/dbs/{db}", cfg.url);
    let random_key = |rng: &mut Rng| format!("k{:06}", rng.below(cfg.keys_per_db));
    let val = |rng: &mut Rng| random_value(rng, cfg.value_bytes);
    match op {
        Op::Get => http.get(format!("{base}/kv/{}", random_key(rng))),
        Op::Put => http.put(format!("{base}/kv/{}", random_key(rng))).body(val(rng)),
        Op::Scan => http.get(format!("{base}/kv?prefix={}&limit=20", &random_key(rng)[..5])),
        Op::Txn => http.post(format!("{base}/txn")).json(&json!({"ops": [
            {"op": "get", "key": random_key(rng)},
            {"op": "increment", "key": "counter", "by": 1},
            {"op": "put", "key": random_key(rng), "value": val(rng)},
        ]})),
        Op::Append => http
            .post(format!("{base}/streams/events/records"))
            .json(&json!({"records": [{"json": {"level": "info", "msg": val(rng)}}]})),
    }
}

fn random_value(rng: &mut Rng, len: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    (0..len).map(|_| *rng.pick(ALPHABET) as char).collect()
}

async fn run_worker(http: reqwest::Client, cfg: Config, step: usize, worker_id: usize, deadline: Instant) -> Stats {
    let mut rng = Rng::new((step * 1_000_003 + worker_id) as u64);
    let mut stats = Stats::default();
    for n in 0.. {
        if Instant::now() >= deadline {
            break;
        }
        let op = cfg.pick_op(&mut rng);
        let req = build_request(&http, &cfg, &mut rng, op).header("x-request-id", format!("lg-{step}-{worker_id}-{n}"));
        let started = Instant::now();
        match send(&cfg, req).await {
            Ok(r) if (200..300).contains(&r.status) || (op == Op::Get && r.status == 404) => {
                let us = started.elapsed().as_micros().min(u32::MAX as u128) as u32;
                stats.latencies_us.entry(op).or_default().push(us);
            }
            Ok(r) if r.status == 429 => {
                stats.throttled += 1;
                let ms = r.body["error"]["details"]["retry_after_ms"].as_u64().unwrap_or(1000);
                tokio::time::sleep(Duration::from_millis(ms.max(1))).await;
            }
            Ok(r) => {
                *stats.failures.entry((op, r.status)).or_default() += 1;
                stats.first_error.get_or_insert_with(|| format!("{} {}: {}", op.name(), r.status, r.body));
            }
            Err(e) => {
                *stats.failures.entry((op, 0)).or_default() += 1;
                stats.first_error.get_or_insert_with(|| format!("{}: {e}", op.name()));
            }
        }
    }
    stats
}

async fn run_step(
    http: &reqwest::Client,
    cfg: &Config,
    concurrency: usize,
    secs: u64,
    step: usize,
) -> (Stats, Duration) {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(secs);
    let tasks: Vec<_> =
        (0..concurrency).map(|w| tokio::spawn(run_worker(http.clone(), cfg.clone(), step, w, deadline))).collect();
    let mut stats = Stats::default();
    for t in tasks {
        stats.merge(t.await.expect("worker panicked"));
    }
    (stats, started.elapsed())
}

fn percentile_ms(sorted_us: &[u32], p: f64) -> f64 {
    let i = ((sorted_us.len() as f64 * p).ceil() as usize).clamp(1, sorted_us.len()) - 1;
    sorted_us[i] as f64 / 1000.0
}

fn utc_now() -> String {
    let s = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() % 86_400;
    format!("{:02}:{:02}:{:02}Z", s / 3600, s / 60 % 60, s % 60)
}

fn report(step: usize, concurrency: usize, stats: &mut Stats, elapsed: Duration, start_time: &str, end_time: &str) {
    let secs = elapsed.as_secs_f64();
    let ok: usize = stats.latencies_us.values().map(Vec::len).sum();
    let failed: u64 = stats.failures.values().sum();
    println!(
        "\nstep {step} (lg-{step}-*, {start_time} to {end_time}), concurrency {concurrency}: {:.0} ok/s, {failed} failed, {} throttled",
        ok as f64 / secs,
        stats.throttled,
    );
    println!(
        "  {:<7} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "op", "ok/s", "p50 ms", "p90 ms", "p99 ms", "max ms", "failed"
    );
    for (op, samples) in stats.latencies_us.iter_mut() {
        samples.sort_unstable();
        let failed: u64 = stats.failures.iter().filter(|((o, _), _)| o == op).map(|(_, n)| n).sum();
        println!(
            "  {:<7} {:>8.0} {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>8}",
            op.name(),
            samples.len() as f64 / secs,
            percentile_ms(samples, 0.50),
            percentile_ms(samples, 0.90),
            percentile_ms(samples, 0.99),
            percentile_ms(samples, 1.0),
            failed
        );
    }
    if !stats.failures.is_empty() {
        let by_status: Vec<String> =
            stats.failures.iter().map(|((op, st), n)| format!("{} {st}: {n}", op.name())).collect();
        println!("  failures: {}", by_status.join(", "));
    }
    if let Some(e) = &stats.first_error {
        println!("  first error: {}", e.chars().take(300).collect::<String>());
    }
}

/// Sends `req`, retrying retryable errors for up to 60 seconds.
async fn send_with_retry(cfg: &Config, action: &str, req: reqwest::RequestBuilder, allowed_statuses: &[u16]) {
    let started = Instant::now();
    loop {
        let attempt = req.try_clone().expect("setup requests must have cloneable bodies");
        match send(cfg, attempt).await {
            Ok(r) if (200..300).contains(&r.status) || allowed_statuses.contains(&r.status) => return,
            Ok(r) if r.body["error"]["retryable"] == true && started.elapsed() < Duration::from_secs(60) => {
                let ms = r.body["error"]["details"]["retry_after_ms"].as_u64().unwrap_or(500);
                tokio::time::sleep(Duration::from_millis(ms.clamp(100, 5_000))).await;
            }
            Ok(r) => panic!("{action}: HTTP {} {}", r.status, r.body),
            Err(e) => panic!("{action}: {e}"),
        }
    }
}

async fn setup(http: &reqwest::Client, cfg: &Config) {
    let mut rng = Rng::new(0);
    for i in 0..cfg.db_count {
        let db = cfg.db_name(i);
        let base = format!("{}/v1/dbs/{db}", cfg.url);
        let create = http.post(format!("{}/v1/dbs", cfg.url)).json(&json!({"name": db}));
        send_with_retry(cfg, &format!("create {db}"), create, &[409]).await;
        let stream = http.post(format!("{base}/streams")).json(&json!({"name": "events", "retention_ms": 3_600_000}));
        send_with_retry(cfg, &format!("create {db} stream"), stream, &[409]).await;
        for chunk in (0..cfg.keys_per_db).collect::<Vec<_>>().chunks(500) {
            let ops: Vec<Value> = chunk
                .iter()
                .map(|k| json!({"op": "put", "key": format!("k{k:06}"), "value": random_value(&mut rng, cfg.value_bytes)}))
                .collect();
            let txn = http.post(format!("{base}/txn")).json(&json!({ "ops": ops }));
            send_with_retry(cfg, &format!("preload {db}"), txn, &[]).await;
        }
    }
}

async fn wait_healthy(http: &reqwest::Client, url: &str) -> Value {
    let started = Instant::now();
    loop {
        if let Ok(r) = http.get(format!("{url}/healthz")).send().await
            && r.status().is_success()
            && let Ok(v) = r.json::<Value>().await
        {
            return v;
        }
        if started.elapsed() > Duration::from_secs(60) {
            panic!("{url}/healthz did not return 200 within 60s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::main]
async fn main() {
    let cfg = Config {
        url: env_str("LOAD_URL", "https://baserunna.fly.dev").trim_end_matches('/').to_string(),
        token: std::env::var("LOAD_TOKEN").expect("set LOAD_TOKEN to an API key or session token"),
        db_count: env_u64("LOAD_DBS", 8) as usize,
        keys_per_db: env_u64("LOAD_KEYS", 1000).max(1),
        value_bytes: env_u64("LOAD_VALUE_BYTES", 256) as usize,
        mix: env_str("LOAD_MIX", "get=80,put=15,txn=4,append=1")
            .split(',')
            .map(|kv| {
                let (k, w) =
                    kv.split_once('=').unwrap_or_else(|| panic!("LOAD_MIX entries must be op=weight, got {kv:?}"));
                (Op::parse(k.trim()), w.trim().parse().expect("LOAD_MIX weights must be numbers"))
            })
            .filter(|&(_, w)| w > 0)
            .collect(),
    };
    assert!(!cfg.mix.is_empty(), "LOAD_MIX has no positive weights");
    let steps: Vec<usize> = env_str("LOAD_STEPS", "4,16,64")
        .split(',')
        .map(|s| s.trim().parse().expect("LOAD_STEPS must be numbers"))
        .collect();
    let secs = env_u64("LOAD_SECS", 20);
    let warmup = env_u64("LOAD_WARMUP_SECS", 5);
    let max_conc = steps.iter().copied().max().unwrap_or(1);
    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(max_conc)
        .timeout(Duration::from_secs(30))
        .build()
        .expect("http client");

    let before = wait_healthy(&http, &cfg.url).await;
    println!("{}: node {}", cfg.url, before["node"]["node_id"]);
    let started = Instant::now();
    setup(&http, &cfg).await;
    println!("setup: {} databases x {} keys in {:.1}s", cfg.db_count, cfg.keys_per_db, started.elapsed().as_secs_f64());

    if warmup > 0 {
        run_step(&http, &cfg, steps[0], warmup, 0).await;
    }
    for (i, &concurrency) in steps.iter().enumerate() {
        let start_time = utc_now();
        let (mut stats, elapsed) = run_step(&http, &cfg, concurrency, secs, i + 1).await;
        report(i + 1, concurrency, &mut stats, elapsed, &start_time, &utc_now());
    }

    let after = wait_healthy(&http, &cfg.url).await;
    let cache = &after["node"]["cache"];
    if before["node"]["node_id"] != after["node"]["node_id"] {
        println!("\nnode changed during the run: {} -> {}", before["node"]["node_id"], after["node"]["node_id"]);
    }
    println!("\nnode cache: {} bytes, {} hits, {} misses", cache["bytes"], cache["hits"], cache["misses"]);

    if env_u64("LOAD_CLEANUP", 0) == 1 {
        for i in 0..cfg.db_count {
            let db = cfg.db_name(i);
            send_with_retry(&cfg, &format!("delete {db}"), http.delete(format!("{}/v1/dbs/{db}", cfg.url)), &[404])
                .await;
        }
        println!("deleted {} databases", cfg.db_count);
    }
}
