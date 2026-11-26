//! KV throughput benchmark from `benches/throughput.rs`, run over HTTP: closed-loop clients send 90% reads and 10% writes to 2,000 databases with 1 key each.

use crate::cgroup::NodeCgroup;
use crate::http::Api;
use baserunna::util::rng::Rng;
use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper::header::{AUTHORIZATION, HeaderValue};
use hyper::{Method, Request, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::Serialize;
use serde_json::{Value as Json, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering::Relaxed};
use std::time::{Duration, Instant};

const DBS: usize = 2000;
const DBS_PER_OWNER: usize = 200;
const VALUE_BYTES: usize = 8;
const WRITE_PCT: u64 = 10;
const SETUP_CONCURRENCY: usize = 64;
const WARMUP: Duration = Duration::from_secs(3);
const WINDOW: Duration = Duration::from_secs(15);

type LoadClient = Client<HttpConnector, Full<Bytes>>;

/// KV key of one database, with the credentials of the database's owner.
struct DbKey {
    uri: Uri,
    auth: HeaderValue,
}

/// Measurements of one load window.
#[derive(Clone, Debug, Serialize)]
pub struct LoadWindow {
    pub clients: usize,
    pub seconds: f64,
    pub ops: usize,
    pub errors: u64,
    pub ops_per_s: f64,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    /// Delta of `usage_usec` in the node cgroup's `cpu.stat`.
    pub node_cpu_usage_usec: Option<u64>,
    pub node_cpu_usec_per_op: Option<f64>,
    /// Node CPU in vCPUs: `node_cpu_usage_usec` over the window.
    pub node_cpu_vcpus: Option<f64>,
    /// CPU of this client process in vCPUs, which the node does not get.
    pub client_cpu_vcpus: f64,
    pub error_samples: Vec<String>,
}

/// Creates the databases and measures one load window with `clients` clients.
pub async fn run(api: &Api, node: &str, cg: &NodeCgroup, clients: usize) -> Result<Json, String> {
    let setup_started = Instant::now();
    let keys = Arc::new(setup(api, node).await?);
    let setup_s = setup_started.elapsed().as_secs_f64();
    let http: LoadClient = Client::builder(TokioExecutor::new()).build_http();
    let mut out = json!({
        "dbs": DBS, "owners": DBS.div_ceil(DBS_PER_OWNER), "keys_per_db": 1, "value_bytes": VALUE_BYTES,
        "write_pct": WRITE_PCT, "setup_s": setup_s,
    });
    let window = measure(&http, &keys, cg, clients, WARMUP, WINDOW).await;
    eprintln!(
        "kv_throughput: {clients} clients: {:.0} ops/s, {} errors, p50 {:.2} ms, p99 {:.2} ms, node CPU {:.2} vCPUs, \
         {:.1} us per op, client CPU {:.2} vCPUs",
        window.ops_per_s,
        window.errors,
        window.p50_ms,
        window.p99_ms,
        window.node_cpu_vcpus.unwrap_or(f64::NAN),
        window.node_cpu_usec_per_op.unwrap_or(f64::NAN),
        window.client_cpu_vcpus,
    );
    out["result"] = json!(window);
    Ok(out)
}

/// Signs up the owners, then creates each database `d{n}` and its key `k0`.
async fn setup(api: &Api, node: &str) -> Result<Vec<DbKey>, String> {
    let mut tokens = Vec::new();
    for o in 0..DBS.div_ceil(DBS_PER_OWNER) {
        tokens.push(api.create_owner(&format!("kv{o}@iso.test"), &format!("d{}", o * DBS_PER_OWNER)).await?.token);
    }
    let tokens = &tokens;
    let results: Vec<Result<(), String>> = stream::iter(0..DBS)
        .map(|d| async move {
            let token = &tokens[d / DBS_PER_OWNER];
            if d % DBS_PER_OWNER != 0 {
                create_db_with_retries(api, token, &format!("d{d}")).await?;
            }
            let value = Rng::new(d as u64).text(VALUE_BYTES);
            api.expect(Method::PUT, &format!("/v1/dbs/d{d}/kv/k0"), token, value, &[200]).await.map(|_| ())
        })
        .buffer_unordered(SETUP_CONCURRENCY)
        .collect()
        .await;
    results.into_iter().collect::<Result<(), String>>()?;
    let base = node.trim_end_matches('/');
    (0..DBS)
        .map(|d| {
            let uri = format!("{base}/v1/dbs/d{d}/kv/k0").parse().map_err(|e| format!("bad node URL {base}: {e}"))?;
            let auth =
                HeaderValue::from_str(&format!("Bearer {}", tokens[d / DBS_PER_OWNER])).map_err(|e| e.to_string())?;
            Ok(DbKey { uri, auth })
        })
        .collect()
}

/// Creates database `db`, and retries when it conflicts with a concurrent create of the same owner.
async fn create_db_with_retries(api: &Api, token: &str, db: &str) -> Result<(), String> {
    let body = json!({"name": db}).to_string();
    for _ in 0..50 {
        let r = api.send(Method::POST, "/v1/dbs", token, Bytes::from(body.clone())).await?;
        match r.status {
            200 | 201 => return Ok(()),
            409 if r.json()["error"]["retryable"] == json!(true) => continue,
            s => return Err(format!("POST /v1/dbs {db}: status {s}: {}", r.excerpt())),
        }
    }
    Err(format!("POST /v1/dbs {db}: 50 conflicts"))
}

const WARMING: u8 = 0;
const MEASURING: u8 = 1;
const STOPPED: u8 = 2;

/// Runs `clients` closed-loop clients for `warmup`, then measures them for `window`.
async fn measure(
    http: &LoadClient,
    keys: &Arc<Vec<DbKey>>,
    cg: &NodeCgroup,
    clients: usize,
    warmup: Duration,
    window: Duration,
) -> LoadWindow {
    let phase = Arc::new(AtomicU8::new(WARMING));
    let values: Arc<Vec<Bytes>> =
        Arc::new((0..64).map(|i| Bytes::from(Rng::new(1_000 + i).text(VALUE_BYTES))).collect());
    let tasks: Vec<_> = (0..clients)
        .map(|c| tokio::spawn(client_loop(http.clone(), keys.clone(), values.clone(), phase.clone(), c as u64 + 1)))
        .collect();
    tokio::time::sleep(warmup).await;
    let cpu_before = cg.cpu_usage_usec();
    let client_cpu_before = process_cpu_secs();
    let started = Instant::now();
    phase.store(MEASURING, Relaxed);
    tokio::time::sleep(window).await;
    phase.store(STOPPED, Relaxed);
    let seconds = started.elapsed().as_secs_f64();
    let cpu = match (cpu_before, cg.cpu_usage_usec()) {
        (Some(a), Some(b)) => Some(b.saturating_sub(a)),
        _ => None,
    };
    let client_cpu = process_cpu_secs() - client_cpu_before;
    let (mut lat_us, mut errors, mut error_samples) = (Vec::new(), 0, Vec::new());
    for t in tasks {
        let (l, e, s) = t.await.expect("kv client");
        lat_us.extend(l);
        errors += e;
        error_samples.extend(s.into_iter().take(5usize.saturating_sub(error_samples.len())));
    }
    lat_us.sort_unstable();
    let pct = |p: f64| {
        lat_us
            .get(((lat_us.len() as f64 * p) as usize).min(lat_us.len().saturating_sub(1)))
            .map_or(0.0, |&v| v as f64 / 1000.0)
    };
    let ops = lat_us.len();
    LoadWindow {
        clients,
        seconds,
        ops,
        errors,
        ops_per_s: ops as f64 / seconds.max(1e-9),
        p50_ms: pct(0.5),
        p99_ms: pct(0.99),
        max_ms: lat_us.last().map_or(0.0, |&v| v as f64 / 1000.0),
        node_cpu_usage_usec: cpu,
        node_cpu_usec_per_op: cpu.map(|c| c as f64 / ops.max(1) as f64),
        node_cpu_vcpus: cpu.map(|c| c as f64 / 1e6 / seconds.max(1e-9)),
        client_cpu_vcpus: client_cpu / seconds.max(1e-9),
        error_samples,
    }
}

/// One closed-loop client. Returns the latencies in microseconds and the errors of the requests
/// that completed while `phase` was [`MEASURING`].
async fn client_loop(
    http: LoadClient,
    keys: Arc<Vec<DbKey>>,
    values: Arc<Vec<Bytes>>,
    phase: Arc<AtomicU8>,
    seed: u64,
) -> (Vec<u32>, u64, Vec<String>) {
    let mut rng = Rng::new(seed);
    let mut lat_us = Vec::with_capacity(1 << 14);
    let (mut errors, mut error_samples) = (0u64, Vec::new());
    loop {
        let p = phase.load(Relaxed);
        if p == STOPPED {
            break;
        }
        let key = &keys[rng.usize(keys.len())];
        let write = rng.below(100) < WRITE_PCT;
        let body = if write { values[rng.usize(values.len())].clone() } else { Bytes::new() };
        let mut req = Request::new(Full::new(body));
        *req.method_mut() = if write { Method::PUT } else { Method::GET };
        *req.uri_mut() = key.uri.clone();
        req.headers_mut().insert(AUTHORIZATION, key.auth.clone());
        let started = Instant::now();
        let failure = match http.request(req).await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                match resp.into_body().collect().await {
                    Ok(_) if status == 200 => None,
                    Ok(b) => Some(format!("status {status}: {}", String::from_utf8_lossy(&b.to_bytes()[..])))
                        .map(|s| s.chars().take(200).collect()),
                    Err(e) => Some(format!("body: {e}")),
                }
            }
            Err(e) => Some(format!("{e:?}")),
        };
        if phase.load(Relaxed) != MEASURING || p != MEASURING {
            continue;
        }
        lat_us.push(started.elapsed().as_micros() as u32);
        if let Some(f) = failure {
            errors += 1;
            if error_samples.len() < 5 {
                error_samples.push(f);
            }
        }
    }
    (lat_us, errors, error_samples)
}

fn process_cpu_secs() -> f64 {
    let mut u = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the rusage struct.
    let u = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, u.as_mut_ptr());
        u.assume_init()
    };
    let secs = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    secs(u.ru_utime) + secs(u.ru_stime)
}
