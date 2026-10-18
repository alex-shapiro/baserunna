//! HTTP load generator for a deployed Baserunna server.
//!
//! Runs a weighted mix of KV, transaction and stream requests against a few
//! databases at each concurrency level in `LOAD_STEPS`, and prints throughput
//! and per-operation latency for each level. Workers are closed-loop: each
//! sends its next request when the previous one answers. Requests carry the
//! ID `lg-<step>-<worker>-<n>` (step 0 is the warmup), and each step prints
//! its UTC start and end, to find it in the server's logs and metrics.
//!
//! ```text
//! LOAD_TOKEN=<api key> cargo run --release --example loadgen
//! LOAD_TOKEN=... LOAD_STEPS=8,32,128 LOAD_MIX=put=100 cargo run --release --example loadgen
//! ```
//!
//! | variable           | default                     | meaning                                            |
//! | ------------------ | --------------------------- | -------------------------------------------------- |
//! | `LOAD_URL`         | `https://baserunna.fly.dev` | server                                             |
//! | `LOAD_TOKEN`       | required                    | API key or session token of the account under test |
//! | `LOAD_DBS`         | 8                           | databases (`loadtest-<i>`, created if missing)     |
//! | `LOAD_KEYS`        | 1000                        | keys per database, written before the run          |
//! | `LOAD_VALUE_BYTES` | 256                         | size of written values                             |
//! | `LOAD_MIX`         | `get=80,put=15,txn=4,append=1` | relative weights of get, put, scan, txn, append |
//! | `LOAD_STEPS`       | `4,16,64`                   | concurrency levels, run in order                   |
//! | `LOAD_SECS`        | 20                          | measured duration of each step                     |
//! | `LOAD_WARMUP_SECS` | 5                           | unmeasured load before the first step              |
//! | `LOAD_CLEANUP`     | 0                           | 1 deletes the databases afterwards                 |
//!
//! Rate limits apply per user and per API key (`BASERUNNA_USER_RPS`,
//! `BASERUNNA_API_KEY_RPS`, 500 by default); throttled requests are counted
//! separately and the worker waits out the server's `retry_after_ms`.

use baserunna::util::rng::Rng;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

fn var(name: &str, default: &str) -> String {
    std::env::var(name).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| default.to_string())
}

fn num(name: &str, default: u64) -> u64 {
    let v = var(name, &default.to_string());
    v.parse().unwrap_or_else(|_| panic!("{name} must be a number, not {v:?}"))
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
            _ => panic!("unknown op {s:?} in LOAD_MIX (get, put, scan, txn, append)"),
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
    dbs: usize,
    keys: u64,
    value_bytes: usize,
    mix: Vec<(Op, u64)>,
}

impl Config {
    fn db(&self, i: usize) -> String {
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
    latencies: BTreeMap<Op, Vec<u32>>,
    /// Failed requests by operation and status (`0` for transport errors).
    failures: BTreeMap<(Op, u16), u64>,
    throttled: u64,
    first_error: Option<String>,
}

impl Stats {
    fn merge(&mut self, o: Stats) {
        for (op, mut l) in o.latencies {
            self.latencies.entry(op).or_default().append(&mut l);
        }
        for (k, n) in o.failures {
            *self.failures.entry(k).or_default() += n;
        }
        self.throttled += o.throttled;
        if self.first_error.is_none() {
            self.first_error = o.first_error;
        }
    }
}

struct Outcome {
    status: u16,
    body: Value,
}

async fn send(cfg: &Config, req: reqwest::RequestBuilder) -> Result<Outcome, String> {
    let resp = req.bearer_auth(&cfg.token).send().await.map_err(|e| e.to_string())?;
    let status = resp.status().as_u16();
    let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
    Ok(Outcome { status, body: serde_json::from_slice(&bytes).unwrap_or(Value::Null) })
}

fn request(http: &reqwest::Client, cfg: &Config, rng: &mut Rng, op: Op) -> reqwest::RequestBuilder {
    let db = cfg.db(rng.usize(cfg.dbs));
    let base = format!("{}/v1/dbs/{db}", cfg.url);
    let key = |rng: &mut Rng| format!("k{:06}", rng.below(cfg.keys));
    let value = |rng: &mut Rng| value_string(rng, cfg.value_bytes);
    match op {
        Op::Get => http.get(format!("{base}/kv/{}", key(rng))),
        Op::Put => http.put(format!("{base}/kv/{}", key(rng))).body(value(rng)),
        Op::Scan => http.get(format!("{base}/kv?prefix={}&limit=20", &key(rng)[..5])),
        Op::Txn => http.post(format!("{base}/txn")).json(&json!({"ops": [
            {"op": "get", "key": key(rng)},
            {"op": "increment", "key": "counter", "by": 1},
            {"op": "put", "key": key(rng), "value": value(rng)},
        ]})),
        Op::Append => http
            .post(format!("{base}/streams/events/records"))
            .json(&json!({"records": [{"json": {"level": "info", "msg": value(rng)}}]})),
    }
}

fn value_string(rng: &mut Rng, n: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    (0..n).map(|_| *rng.pick(ALPHABET) as char).collect()
}

async fn worker(http: reqwest::Client, cfg: Config, step: usize, w: usize, until: Instant) -> Stats {
    let mut rng = Rng::new((step * 1_000_003 + w) as u64);
    let mut stats = Stats::default();
    for n in 0.. {
        if Instant::now() >= until {
            break;
        }
        let op = cfg.pick_op(&mut rng);
        let req = request(&http, &cfg, &mut rng, op).header("x-request-id", format!("lg-{step}-{w}-{n}"));
        let t = Instant::now();
        match send(&cfg, req).await {
            Ok(o) if (200..300).contains(&o.status) || (op == Op::Get && o.status == 404) => {
                let us = t.elapsed().as_micros().min(u32::MAX as u128) as u32;
                stats.latencies.entry(op).or_default().push(us);
            }
            Ok(o) if o.status == 429 => {
                stats.throttled += 1;
                let ms = o.body["error"]["details"]["retry_after_ms"].as_u64().unwrap_or(1000);
                tokio::time::sleep(Duration::from_millis(ms.max(1))).await;
            }
            Ok(o) => {
                *stats.failures.entry((op, o.status)).or_default() += 1;
                stats.first_error.get_or_insert_with(|| format!("{} {}: {}", op.name(), o.status, o.body));
            }
            Err(e) => {
                *stats.failures.entry((op, 0)).or_default() += 1;
                stats.first_error.get_or_insert_with(|| format!("{}: {e}", op.name()));
            }
        }
    }
    stats
}

/// Runs `concurrency` workers for `secs`. Their requests carry the ID
/// `lg-<step>-<worker>-<n>`, so server logs can be matched to a step.
async fn run_step(
    http: &reqwest::Client,
    cfg: &Config,
    concurrency: usize,
    secs: u64,
    step: usize,
) -> (Stats, Duration) {
    let t = Instant::now();
    let until = t + Duration::from_secs(secs);
    let tasks: Vec<_> =
        (0..concurrency).map(|w| tokio::spawn(worker(http.clone(), cfg.clone(), step, w, until))).collect();
    let mut stats = Stats::default();
    for t in tasks {
        stats.merge(t.await.expect("worker"));
    }
    (stats, t.elapsed())
}

fn pct(sorted: &[u32], p: f64) -> f64 {
    let i = ((sorted.len() as f64 * p).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[i] as f64 / 1000.0
}

/// The current UTC time of day, to line steps up with server logs and metrics.
fn utc_now() -> String {
    let s = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() % 86_400;
    format!("{:02}:{:02}:{:02}Z", s / 3600, s / 60 % 60, s % 60)
}

fn report(step: usize, concurrency: usize, stats: &mut Stats, elapsed: Duration, from: &str, to: &str) {
    let secs = elapsed.as_secs_f64();
    let ok: usize = stats.latencies.values().map(Vec::len).sum();
    let failed: u64 = stats.failures.values().sum();
    println!(
        "\nstep {step} (lg-{step}-*, {from} to {to}), concurrency {concurrency}: {:.0} ok/s, {failed} failed, {} throttled",
        ok as f64 / secs,
        stats.throttled,
    );
    println!(
        "  {:<7} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "op", "ok/s", "p50 ms", "p90 ms", "p99 ms", "max ms", "failed"
    );
    for (op, l) in stats.latencies.iter_mut() {
        l.sort_unstable();
        let failed: u64 = stats.failures.iter().filter(|((o, _), _)| o == op).map(|(_, n)| n).sum();
        println!(
            "  {:<7} {:>8.0} {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>8}",
            op.name(),
            l.len() as f64 / secs,
            pct(l, 0.50),
            pct(l, 0.90),
            pct(l, 0.99),
            pct(l, 1.0),
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

/// Sends `req` until it succeeds (or answers one of `also`), waiting out
/// retryable errors such as a lease left by a stopped node, for up to a minute.
async fn expect_ok(cfg: &Config, what: &str, req: reqwest::RequestBuilder, also: &[u16]) {
    let t = Instant::now();
    loop {
        let attempt = req.try_clone().expect("setup requests have buffered bodies");
        match send(cfg, attempt).await {
            Ok(o) if (200..300).contains(&o.status) || also.contains(&o.status) => return,
            Ok(o) if o.body["error"]["retryable"] == true && t.elapsed() < Duration::from_secs(60) => {
                let ms = o.body["error"]["details"]["retry_after_ms"].as_u64().unwrap_or(500);
                tokio::time::sleep(Duration::from_millis(ms.clamp(100, 5_000))).await;
            }
            Ok(o) => panic!("{what}: HTTP {} {}", o.status, o.body),
            Err(e) => panic!("{what}: {e}"),
        }
    }
}

/// Creates the databases and stream if missing and writes every key once.
async fn setup(http: &reqwest::Client, cfg: &Config) {
    let mut rng = Rng::new(0);
    for i in 0..cfg.dbs {
        let db = cfg.db(i);
        let base = format!("{}/v1/dbs/{db}", cfg.url);
        let create = http.post(format!("{}/v1/dbs", cfg.url)).json(&json!({"name": db}));
        expect_ok(cfg, &format!("create {db}"), create, &[409]).await;
        let stream = http.post(format!("{base}/streams")).json(&json!({"name": "events", "retention_ms": 3_600_000}));
        expect_ok(cfg, &format!("create {db} stream"), stream, &[409]).await;
        for chunk in (0..cfg.keys).collect::<Vec<_>>().chunks(500) {
            let ops: Vec<Value> = chunk
                .iter()
                .map(|k| json!({"op": "put", "key": format!("k{k:06}"), "value": value_string(&mut rng, cfg.value_bytes)}))
                .collect();
            let txn = http.post(format!("{base}/txn")).json(&json!({ "ops": ops }));
            expect_ok(cfg, &format!("preload {db}"), txn, &[]).await;
        }
    }
}

async fn wait_healthy(http: &reqwest::Client, url: &str) -> Value {
    let t = Instant::now();
    loop {
        if let Ok(r) = http.get(format!("{url}/healthz")).send().await
            && r.status().is_success()
            && let Ok(v) = r.json::<Value>().await
        {
            return v;
        }
        if t.elapsed() > Duration::from_secs(60) {
            panic!("{url}/healthz did not answer 200 within 60 s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::main]
async fn main() {
    let cfg = Config {
        url: var("LOAD_URL", "https://baserunna.fly.dev").trim_end_matches('/').to_string(),
        token: std::env::var("LOAD_TOKEN").expect("set LOAD_TOKEN to an API key or session token"),
        dbs: num("LOAD_DBS", 8) as usize,
        keys: num("LOAD_KEYS", 1000).max(1),
        value_bytes: num("LOAD_VALUE_BYTES", 256) as usize,
        mix: var("LOAD_MIX", "get=80,put=15,txn=4,append=1")
            .split(',')
            .map(|kv| {
                let (k, w) = kv.split_once('=').unwrap_or_else(|| panic!("LOAD_MIX entries are op=weight, not {kv:?}"));
                (Op::parse(k.trim()), w.trim().parse().expect("LOAD_MIX weights are numbers"))
            })
            .filter(|&(_, w)| w > 0)
            .collect(),
    };
    assert!(!cfg.mix.is_empty(), "LOAD_MIX has no positive weights");
    let steps: Vec<usize> =
        var("LOAD_STEPS", "4,16,64").split(',').map(|s| s.trim().parse().expect("LOAD_STEPS are numbers")).collect();
    let secs = num("LOAD_SECS", 20);
    let warmup = num("LOAD_WARMUP_SECS", 5);
    let max_conc = steps.iter().copied().max().unwrap_or(1);
    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(max_conc)
        .timeout(Duration::from_secs(30))
        .build()
        .expect("http client");

    let before = wait_healthy(&http, &cfg.url).await;
    println!("{}: node {}", cfg.url, before["node"]["node_id"]);
    let t = Instant::now();
    setup(&http, &cfg).await;
    println!("setup: {} databases x {} keys in {:.1}s", cfg.dbs, cfg.keys, t.elapsed().as_secs_f64());

    if warmup > 0 {
        run_step(&http, &cfg, steps[0], warmup, 0).await;
    }
    for (i, &c) in steps.iter().enumerate() {
        let from = utc_now();
        let (mut stats, elapsed) = run_step(&http, &cfg, c, secs, i + 1).await;
        report(i + 1, c, &mut stats, elapsed, &from, &utc_now());
    }

    let after = wait_healthy(&http, &cfg.url).await;
    let cache = &after["node"]["cache"];
    if before["node"]["node_id"] != after["node"]["node_id"] {
        println!("\nnode changed during the run: {} -> {}", before["node"]["node_id"], after["node"]["node_id"]);
    }
    println!("\nnode cache: {} bytes, {} hits, {} misses", cache["bytes"], cache["hits"], cache["misses"]);

    if num("LOAD_CLEANUP", 0) == 1 {
        for i in 0..cfg.dbs {
            let db = cfg.db(i);
            expect_ok(&cfg, &format!("delete {db}"), http.delete(format!("{}/v1/dbs/{db}", cfg.url)), &[404]).await;
        }
        println!("deleted {} databases", cfg.dbs);
    }
}
