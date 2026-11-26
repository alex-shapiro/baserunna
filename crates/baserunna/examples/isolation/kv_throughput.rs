//! HTTP KV throughput benchmark matching `benches/throughput.rs`.
//!
//! Closed-loop clients send 90% reads and 10% writes across 2,000 single-key databases.

use crate::cgroup::NodeCgroup;
use crate::http::NodeApi;
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

const DB_COUNT: usize = 2000;
const DBS_PER_OWNER: usize = 200;
const VALUE_BYTES: usize = 8;
const WRITE_PERCENT: u64 = 10;
const SETUP_CONCURRENCY: usize = 64;
const WARMUP_DURATION: Duration = Duration::from_secs(3);
const MEASURE_DURATION: Duration = Duration::from_secs(15);

type LoadClient = Client<HttpConnector, Full<Bytes>>;

/// Target key URI and authorization header for a database.
struct DbKey {
    uri: Uri,
    auth: HeaderValue,
}

/// Throughput, latency, and CPU measurements for a load window.
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
    /// Node cgroup CPU time used during the window, in microseconds.
    pub node_cpu_usage_usec: Option<u64>,
    pub node_cpu_usec_per_op: Option<f64>,
    /// Average node CPU use in vCPUs during the window.
    pub node_cpu_vcpus: Option<f64>,
    /// Average client process CPU use in vCPUs during the window.
    pub client_cpu_vcpus: f64,
    pub error_samples: Vec<String>,
}

/// Creates the benchmark databases and measures a load window with `client_count` clients.
pub async fn run(api: &NodeApi, node_url: &str, cgroup: &NodeCgroup, client_count: usize) -> Result<Json, String> {
    let setup_started = Instant::now();
    let keys = Arc::new(setup(api, node_url).await?);
    let setup_s = setup_started.elapsed().as_secs_f64();
    let http: LoadClient = Client::builder(TokioExecutor::new()).build_http();
    let mut report = json!({
        "dbs": DB_COUNT, "owners": DB_COUNT.div_ceil(DBS_PER_OWNER), "keys_per_db": 1, "value_bytes": VALUE_BYTES,
        "write_pct": WRITE_PERCENT, "setup_s": setup_s,
    });
    let window = measure(&http, &keys, cgroup, client_count, WARMUP_DURATION, MEASURE_DURATION).await;
    eprintln!(
        "kv_throughput: {client_count} clients: {:.0} ops/s, {} errors, p50 {:.2} ms, p99 {:.2} ms, node CPU {:.2} vCPUs, \
         {:.1} us per op, client CPU {:.2} vCPUs",
        window.ops_per_s,
        window.errors,
        window.p50_ms,
        window.p99_ms,
        window.node_cpu_vcpus.unwrap_or(f64::NAN),
        window.node_cpu_usec_per_op.unwrap_or(f64::NAN),
        window.client_cpu_vcpus,
    );
    report["result"] = json!(window);
    Ok(report)
}

/// Creates owners, databases, and initial `k0` keys.
async fn setup(api: &NodeApi, node_url: &str) -> Result<Vec<DbKey>, String> {
    let mut tokens = Vec::new();
    for owner_index in 0..DB_COUNT.div_ceil(DBS_PER_OWNER) {
        tokens.push(
            api.create_owner(&format!("kv{owner_index}@iso.test"), &format!("d{}", owner_index * DBS_PER_OWNER))
                .await?
                .token,
        );
    }
    let tokens = &tokens;
    let results: Vec<Result<(), String>> = stream::iter(0..DB_COUNT)
        .map(|db_index| async move {
            let token = &tokens[db_index / DBS_PER_OWNER];
            if db_index % DBS_PER_OWNER != 0 {
                create_db_with_retries(api, token, &format!("d{db_index}")).await?;
            }
            let value = Rng::new(db_index as u64).text(VALUE_BYTES);
            api.send_expect(Method::PUT, &format!("/v1/dbs/d{db_index}/kv/k0"), token, value, &[200]).await.map(|_| ())
        })
        .buffer_unordered(SETUP_CONCURRENCY)
        .collect()
        .await;
    results.into_iter().collect::<Result<(), String>>()?;
    let base_url = node_url.trim_end_matches('/');
    (0..DB_COUNT)
        .map(|db_index| {
            let uri = format!("{base_url}/v1/dbs/d{db_index}/kv/k0")
                .parse()
                .map_err(|err| format!("invalid node URL {base_url}: {err}"))?;
            let auth = HeaderValue::from_str(&format!("Bearer {}", tokens[db_index / DBS_PER_OWNER]))
                .map_err(|err| err.to_string())?;
            Ok(DbKey { uri, auth })
        })
        .collect()
}

/// Creates database `db` and retries if a concurrent creation conflicts.
async fn create_db_with_retries(api: &NodeApi, token: &str, db: &str) -> Result<(), String> {
    let body = json!({"name": db}).to_string();
    for _ in 0..50 {
        let reply = api.send(Method::POST, "/v1/dbs", token, Bytes::from(body.clone())).await?;
        match reply.status {
            200 | 201 => return Ok(()),
            409 if reply.json()["error"]["retryable"] == json!(true) => continue,
            status => return Err(format!("POST /v1/dbs {db}: status {status}: {}", reply.excerpt())),
        }
    }
    Err(format!("POST /v1/dbs {db}: 50 conflicts"))
}

const PHASE_WARMING: u8 = 0;
const PHASE_MEASURING: u8 = 1;
const PHASE_STOPPED: u8 = 2;

/// Warms up `client_count` closed-loop clients. Then measures throughput for `measure_window`.
async fn measure(
    http: &LoadClient,
    keys: &Arc<Vec<DbKey>>,
    cgroup: &NodeCgroup,
    client_count: usize,
    warmup: Duration,
    measure_window: Duration,
) -> LoadWindow {
    let phase = Arc::new(AtomicU8::new(PHASE_WARMING));
    let values: Arc<Vec<Bytes>> =
        Arc::new((0..64).map(|i| Bytes::from(Rng::new(1_000 + i).text(VALUE_BYTES))).collect());
    let tasks: Vec<_> = (0..client_count)
        .map(|client_index| {
            tokio::spawn(client_loop(
                http.clone(),
                keys.clone(),
                values.clone(),
                phase.clone(),
                client_index as u64 + 1,
            ))
        })
        .collect();
    tokio::time::sleep(warmup).await;
    let node_cpu_usec_before = cgroup.cpu_usage_usec();
    let client_cpu_secs_before = process_cpu_secs();
    let started = Instant::now();
    phase.store(PHASE_MEASURING, Relaxed);
    tokio::time::sleep(measure_window).await;
    phase.store(PHASE_STOPPED, Relaxed);
    let seconds = started.elapsed().as_secs_f64();
    let node_cpu_usec = match (node_cpu_usec_before, cgroup.cpu_usage_usec()) {
        (Some(before), Some(after)) => Some(after.saturating_sub(before)),
        _ => None,
    };
    let client_cpu_secs = process_cpu_secs() - client_cpu_secs_before;
    let (mut latencies_us, mut errors, mut error_samples) = (Vec::new(), 0, Vec::new());
    for task in tasks {
        let (task_latencies, task_errors, task_samples) = task.await.expect("kv client");
        latencies_us.extend(task_latencies);
        errors += task_errors;
        error_samples.extend(task_samples.into_iter().take(5usize.saturating_sub(error_samples.len())));
    }
    latencies_us.sort_unstable();
    let percentile_ms = |quantile: f64| {
        latencies_us
            .get(((latencies_us.len() as f64 * quantile) as usize).min(latencies_us.len().saturating_sub(1)))
            .map_or(0.0, |&value| value as f64 / 1000.0)
    };
    let ops = latencies_us.len();
    LoadWindow {
        clients: client_count,
        seconds,
        ops,
        errors,
        ops_per_s: ops as f64 / seconds.max(1e-9),
        p50_ms: percentile_ms(0.5),
        p99_ms: percentile_ms(0.99),
        max_ms: latencies_us.last().map_or(0.0, |&value| value as f64 / 1000.0),
        node_cpu_usage_usec: node_cpu_usec,
        node_cpu_usec_per_op: node_cpu_usec.map(|value| value as f64 / ops.max(1) as f64),
        node_cpu_vcpus: node_cpu_usec.map(|value| value as f64 / 1e6 / seconds.max(1e-9)),
        client_cpu_vcpus: client_cpu_secs / seconds.max(1e-9),
        error_samples,
    }
}

/// Runs a closed-loop client task until `phase` becomes [`PHASE_STOPPED`].
///
/// Returns request latencies in microseconds, error count, and sample errors collected during
/// [`PHASE_MEASURING`].
async fn client_loop(
    http: LoadClient,
    keys: Arc<Vec<DbKey>>,
    values: Arc<Vec<Bytes>>,
    phase: Arc<AtomicU8>,
    seed: u64,
) -> (Vec<u32>, u64, Vec<String>) {
    let mut rng = Rng::new(seed);
    let mut latencies_us = Vec::with_capacity(1 << 14);
    let (mut errors, mut error_samples) = (0u64, Vec::new());
    loop {
        let phase_before = phase.load(Relaxed);
        if phase_before == PHASE_STOPPED {
            break;
        }
        let key = &keys[rng.usize(keys.len())];
        let is_write = rng.below(100) < WRITE_PERCENT;
        let body = if is_write { values[rng.usize(values.len())].clone() } else { Bytes::new() };
        let mut request = Request::new(Full::new(body));
        *request.method_mut() = if is_write { Method::PUT } else { Method::GET };
        *request.uri_mut() = key.uri.clone();
        request.headers_mut().insert(AUTHORIZATION, key.auth.clone());
        let started = Instant::now();
        let failure = match http.request(request).await {
            Ok(response) => {
                let status = response.status().as_u16();
                match response.into_body().collect().await {
                    Ok(_) if status == 200 => None,
                    Ok(body) => Some(format!("status {status}: {}", String::from_utf8_lossy(&body.to_bytes()[..])))
                        .map(|text| text.chars().take(200).collect()),
                    Err(err) => Some(format!("response body error: {err}")),
                }
            }
            Err(err) => Some(format!("{err:?}")),
        };
        if phase.load(Relaxed) != PHASE_MEASURING || phase_before != PHASE_MEASURING {
            continue;
        }
        latencies_us.push(started.elapsed().as_micros() as u32);
        if let Some(err) = failure {
            errors += 1;
            if error_samples.len() < 5 {
                error_samples.push(err);
            }
        }
    }
    (latencies_us, errors, error_samples)
}

fn process_cpu_secs() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: libc::getrusage initializes the rusage struct for RUSAGE_SELF.
    let usage = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init()
    };
    let to_secs = |timeval: libc::timeval| timeval.tv_sec as f64 + timeval.tv_usec as f64 / 1e6;
    to_secs(usage.ru_utime) + to_secs(usage.ru_stime)
}
