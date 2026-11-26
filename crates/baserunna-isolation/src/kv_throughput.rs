//! HTTP KV throughput benchmark matching `benches/throughput.rs`.
//!
//! Closed-loop clients send 90% reads and 10% writes across 2,000 single-key databases.

use crate::cgroup::NodeCgroup;
use crate::http::NodeApi;
use crate::percentile;
use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper::header::{AUTHORIZATION, HeaderValue};
use hyper::{Method, Request, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
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

/// Creates the benchmark databases and measures a load window with `client_count` clients.
pub async fn run(api: &NodeApi, node_url: &str, cgroup: &NodeCgroup, client_count: usize) -> Result<Json, String> {
    let setup_started = Instant::now();
    let keys = Arc::new(setup(api, node_url).await?);
    let setup_s = setup_started.elapsed().as_secs_f64();
    let http: LoadClient = Client::builder(TokioExecutor::new()).build_http();
    Ok(json!({
        "dbs": DB_COUNT, "owners": DB_COUNT.div_ceil(DBS_PER_OWNER), "keys_per_db": 1, "value_bytes": VALUE_BYTES,
        "write_pct": WRITE_PERCENT, "setup_s": setup_s,
        "result": measure(&http, &keys, cgroup, client_count).await,
    }))
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
                api.create_db(token, &format!("d{db_index}")).await?;
            }
            api.put_value(token, &format!("d{db_index}"), "k0", kv_value(db_index)).await
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

const PHASE_WARMING: u8 = 0;
const PHASE_MEASURING: u8 = 1;
const PHASE_STOPPED: u8 = 2;

/// Warms up `client_count` closed-loop clients, then returns throughput, latency, and CPU use over
/// [`MEASURE_DURATION`].
async fn measure(http: &LoadClient, keys: &Arc<Vec<DbKey>>, cgroup: &NodeCgroup, client_count: usize) -> Json {
    let phase = Arc::new(AtomicU8::new(PHASE_WARMING));
    let values: Arc<Vec<Bytes>> = Arc::new((0..64).map(|i| Bytes::from(kv_value(1_000 + i))).collect());
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
    tokio::time::sleep(WARMUP_DURATION).await;
    let node_cpu_usec_before = cgroup.cpu_usage_usec();
    let client_cpu_secs_before = process_cpu_secs();
    let measure_started = Instant::now();
    phase.store(PHASE_MEASURING, Relaxed);
    tokio::time::sleep(MEASURE_DURATION).await;
    phase.store(PHASE_STOPPED, Relaxed);
    let seconds = measure_started.elapsed().as_secs_f64();
    let node_cpu_usec = cgroup.cpu_usage_usec_since(node_cpu_usec_before);
    let client_cpu_secs = process_cpu_secs() - client_cpu_secs_before;
    let (mut latencies_us, mut errors, mut error_samples) = (Vec::new(), 0, Vec::new());
    for task in tasks {
        let (task_latencies, task_errors, task_samples) = task.await.expect("kv client");
        latencies_us.extend(task_latencies);
        errors += task_errors;
        error_samples.extend(task_samples.into_iter().take(5usize.saturating_sub(error_samples.len())));
    }
    latencies_us.sort_unstable();
    let latencies_ms: Vec<f64> = latencies_us.iter().map(|&micros| micros as f64 / 1000.0).collect();
    let ops = latencies_ms.len();
    json!({
        "clients": client_count,
        "seconds": seconds,
        "ops": ops,
        "errors": errors,
        "ops_per_s": ops as f64 / seconds.max(1e-9),
        "p50_ms": percentile(&latencies_ms, 0.5),
        "p99_ms": percentile(&latencies_ms, 0.99),
        "max_ms": latencies_ms.last().copied().unwrap_or(0.0),
        "node_cpu_usage_usec": node_cpu_usec,
        "node_cpu_usec_per_op": node_cpu_usec.map(|value| value as f64 / ops.max(1) as f64),
        "node_cpu_vcpus": node_cpu_usec.map(|value| value as f64 / 1e6 / seconds.max(1e-9)),
        "client_cpu_vcpus": client_cpu_secs / seconds.max(1e-9),
        "error_samples": error_samples,
    })
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
    let mut rng = LoadRng::new(seed);
    let mut latencies_us = Vec::with_capacity(1 << 14);
    let (mut errors, mut error_samples) = (0u64, Vec::new());
    loop {
        let phase_before = phase.load(Relaxed);
        if phase_before == PHASE_STOPPED {
            break;
        }
        let key = &keys[rng.below(keys.len() as u64) as usize];
        let is_write = rng.below(100) < WRITE_PERCENT;
        let body = if is_write { values[rng.below(values.len() as u64) as usize].clone() } else { Bytes::new() };
        let mut request = Request::new(Full::new(body));
        *request.method_mut() = if is_write { Method::PUT } else { Method::GET };
        *request.uri_mut() = key.uri.clone();
        request.headers_mut().insert(AUTHORIZATION, key.auth.clone());
        let request_started = Instant::now();
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
        latencies_us.push(request_started.elapsed().as_micros() as u32);
        if let Some(err) = failure {
            errors += 1;
            if error_samples.len() < 5 {
                error_samples.push(err);
            }
        }
    }
    (latencies_us, errors, error_samples)
}

/// Target key URI and authorization header for a database.
struct DbKey {
    uri: Uri,
    auth: HeaderValue,
}

type LoadClient = Client<HttpConnector, Full<Bytes>>;

/// SplitMix64 generator that picks keys and operations for one load client.
struct LoadRng {
    state: u64,
}

impl LoadRng {
    fn new(seed: u64) -> LoadRng {
        LoadRng { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Returns a number in `[0, n)`.
    fn below(&mut self, n: u64) -> u64 {
        ((self.next_u64() as u128 * n as u128) >> 64) as u64
    }
}

/// Returns the KV value of `VALUE_BYTES` bytes for `index`.
fn kv_value(index: usize) -> String {
    format!("{index:0width$}", width = VALUE_BYTES)
}

fn process_cpu_secs() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: libc::getrusage initializes the rusage struct for RUSAGE_SELF.
    let usage = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init()
    };
    let timeval_to_secs = |timeval: libc::timeval| timeval.tv_sec as f64 + timeval.tv_usec as f64 / 1e6;
    timeval_to_secs(usage.ru_utime) + timeval_to_secs(usage.ru_stime)
}
