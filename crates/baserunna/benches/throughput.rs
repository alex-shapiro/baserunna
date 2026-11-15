//! Wall-clock throughput benchmark for a single node serving thousands of
//! active databases in-process through its HTTP router.
//!
//! ```text
//! cargo bench --bench throughput
//! BENCH_DBS=5000 BENCH_LATENCY_MS=20 BENCH_SECS=20 cargo bench --bench throughput
//! # CPU cost of commits against full manifest tails:
//! BENCH_THREADS=1 BENCH_DBS=8 BENCH_KEYS=1000 BENCH_VALUE_BYTES=256 BENCH_WRITE_PCT=100 \
//!   BENCH_LATENCY_MS=30 BENCH_CLIENTS=256 cargo bench --bench throughput
//! ```
//!
//! | variable           | default | meaning                                              |
//! | ------------------ | ------- | ---------------------------------------------------- |
//! | `BENCH_DBS`        | 2000    | active databases                                     |
//! | `BENCH_CLIENTS`    | 64      | concurrent clients                                   |
//! | `BENCH_SECS`       | 10      | measured duration                                    |
//! | `BENCH_WRITE_PCT`  | 10      | percentage of requests that are writes               |
//! | `BENCH_LATENCY_MS` | 0       | added per object-store request (20 emulates S3)      |
//! | `BENCH_CACHE_MB`   | 64      | block cache budget                                   |
//! | `BENCH_THREADS`    | 4       | runtime worker threads                               |
//! | `BENCH_KEYS`       | 1       | keys per database, preloaded and then written        |
//! | `BENCH_VALUE_BYTES`| 8       | bytes per written value                              |
//! | `BENCH_TAIL_KB`    | default | manifest tail limit (`tail_max_bytes`) in KiB        |
//! | `BENCH_BOUNDARY`   | none    | isolation boundary each request crosses twice: `none`, `pipe` (owner worker process), `wasm` (owner WebAssembly instance) |
//! | `BENCH_TRANSPORT`  | inproc  | `inproc` calls the router directly; `tcp` sends HTTP/1.1 over localhost keep-alive connections |

mod boundary;

use baserunna::env::Env;
use boundary::{BoundaryHost, BoundaryMode, ClientCrossings, PooledCrossings, WORKER_SOCKET_VAR};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest;
use std::net::SocketAddr;
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

fn cpu_secs() -> f64 {
    let mut u = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the rusage struct.
    let u = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, u.as_mut_ptr());
        u.assume_init()
    };
    let secs = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    secs(u.ru_utime) + secs(u.ru_stime)
}

fn value(seed: u64, len: usize) -> Bytes {
    Bytes::from(Rng::new(seed).text(len.max(1)))
}

/// Path that a client task's requests take to the router.
enum ClientPath {
    /// Crosses the boundary in the client task and calls the router in-process.
    InProcess(axum::Router, ClientCrossings),
    /// Sends HTTP/1.1 on one keep-alive connection to a server that crosses the boundary.
    Tcp(SendRequest<Full<Bytes>>),
}

impl ClientPath {
    async fn call(&mut self, owner: usize, method: &str, path: &str, token: &str, body: Bytes) -> (u16, Bytes) {
        match self {
            ClientPath::InProcess(router, crossings) => {
                crossings.round_trip(owner, &[method.as_bytes(), b" ", path.as_bytes(), b"\n", &body[..]]).await;
                let r = call_router(router, method, path, token, body).await;
                crossings.round_trip(owner, &[&r.body[..]]).await;
                (r.status, r.body)
            }
            ClientPath::Tcp(sender) => {
                let req = http::Request::builder()
                    .method(method)
                    .uri(path)
                    .header("host", "127.0.0.1")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Full::new(body))
                    .expect("http request");
                sender.ready().await.expect("http connection ready");
                let resp = sender.send_request(req).await.expect("http response");
                let status = resp.status().as_u16();
                (status, resp.into_body().collect().await.expect("http body").to_bytes())
            }
        }
    }
}

/// Owner of the database that a `/v1/dbs/d{n}/...` path names.
fn path_owner(path: &str) -> Option<usize> {
    let rest = path.strip_prefix("/v1/dbs/d")?;
    let end = rest.find('/').unwrap_or(rest.len());
    rest[..end].parse::<usize>().ok().map(|d| d / DBS_PER_USER)
}

/// Middleware that sends the request and then the response body across the owner's boundary.
async fn cross_boundary(
    axum::extract::State(crossings): axum::extract::State<Arc<PooledCrossings>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let Some(owner) = path_owner(req.uri().path()) else {
        return next.run(req).await;
    };
    let (parts, body) = req.into_parts();
    let body = axum::body::to_bytes(body, usize::MAX).await.expect("request body");
    let request = [parts.method.as_str().as_bytes(), b" ", parts.uri.path().as_bytes(), b"\n", &body[..]];
    crossings.round_trip(owner, &request).await;
    let resp = next.run(axum::extract::Request::from_parts(parts, axum::body::Body::from(body))).await;
    let (parts, body) = resp.into_parts();
    let body = axum::body::to_bytes(body, usize::MAX).await.expect("response body");
    crossings.round_trip(owner, &[&body[..]]).await;
    axum::response::Response::from_parts(parts, axum::body::Body::from(body))
}

/// Serves `app` on a localhost TCP listener the way `main.rs` does.
async fn serve_tcp(app: axum::Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind localhost");
    let addr = listener.local_addr().expect("listener address");
    let listener = axum::serve::ListenerExt::tap_io(listener, |tcp| {
        let _ = tcp.set_nodelay(true);
    });
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.expect("serve");
    });
    addr
}

async fn connect_http(addr: SocketAddr) -> SendRequest<Full<Bytes>> {
    let tcp = tokio::net::TcpStream::connect(addr).await.expect("connect server");
    tcp.set_nodelay(true).expect("nodelay");
    let (sender, conn) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tcp)).await.expect("http handshake");
    tokio::spawn(conn);
    sender
}

/// Raises the open file limit to its hard maximum for thousands of sockets.
fn raise_fd_limit() {
    let mut lim = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    // SAFETY: getrlimit and setrlimit read and write a valid rlimit struct.
    unsafe {
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0 {
            lim.rlim_cur = lim.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
        }
    }
}

fn main() {
    if let Ok(socket) = std::env::var(WORKER_SOCKET_VAR) {
        boundary::run_worker(&socket);
    }
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
    let keys = env("BENCH_KEYS", 1).max(1);
    let value_bytes = env("BENCH_VALUE_BYTES", 8) as usize;
    let mode = BoundaryMode::from_env();
    let tcp = match std::env::var("BENCH_TRANSPORT").as_deref() {
        Err(_) | Ok("") | Ok("inproc") => false,
        Ok("tcp") => true,
        Ok(other) => panic!("BENCH_TRANSPORT={other}: expected inproc or tcp"),
    };
    let owners = dbs.div_ceil(DBS_PER_USER);
    baserunna::log::set_level(Some(baserunna::log::Level::Error));

    let store = Arc::new(MeteredStore::new(Arc::new(MemStore::with_system_clock()), 0));
    let mut cfg = node_config("bench", "http://127.0.0.1:0");
    cfg.engine.cache_bytes = env("BENCH_CACHE_MB", 64) as usize * 1024 * 1024;
    cfg.engine.tail_max_bytes = env("BENCH_TAIL_KB", cfg.engine.tail_max_bytes as u64 / 1024) as usize * 1024;
    let dyn_store: DynStore = store.clone();
    let node = Node::start(cfg, Arc::new(Env::real()), dyn_store, Services::local()).await.expect("node starts");
    let router = baserunna::api::router(node.clone());

    let t0 = Instant::now();
    let mut tokens = Vec::new();
    for u in 0..owners {
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
        for k in 0..keys {
            let path = format!("/v1/dbs/d{d}/kv/k{k}");
            let r = call_router(&router, "PUT", &path, t, value(k, value_bytes)).await;
            assert_eq!(r.status, 200, "seed d{d}");
        }
    }
    eprintln!("setup: {dbs} databases in {:.1}s", t0.elapsed().as_secs_f64());

    let boundary = Arc::new(BoundaryHost::start(mode, owners));
    let mut client_paths = Vec::with_capacity(clients);
    if tcp {
        raise_fd_limit();
        let app = match mode {
            BoundaryMode::None => router.clone(),
            BoundaryMode::Pipe | BoundaryMode::Wasm => {
                let crossings = Arc::new(PooledCrossings::new(boundary.clone(), owners));
                router.clone().layer(axum::middleware::from_fn_with_state(crossings, cross_boundary))
            }
        };
        let addr = serve_tcp(app).await;
        for _ in 0..clients {
            client_paths.push(ClientPath::Tcp(connect_http(addr).await));
        }
    } else {
        for _ in 0..clients {
            client_paths.push(ClientPath::InProcess(router.clone(), boundary.client_crossings(owners).await));
        }
    }

    store.set_latency_ms(latency);
    let before = store.total();
    let cpu_before = cpu_secs();
    let far_cpu_before = boundary.far_cpu_secs();
    let stop = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    let mut hs = Vec::new();
    for (c, mut client_path) in client_paths.into_iter().enumerate() {
        let (tokens, stop) = (tokens.clone(), stop.clone());
        hs.push(tokio::spawn(async move {
            let mut rng = Rng::new(c as u64 + 1);
            let mut lat = Vec::new();
            let mut errors = 0u64;
            let (mut request_bytes, mut response_bytes) = (0u64, 0u64);
            while !stop.load(Ordering::Relaxed) {
                let d = rng.usize(dbs);
                let owner = d / DBS_PER_USER;
                let t = &tokens[owner];
                let path = format!("/v1/dbs/d{d}/kv/k{}", rng.below(keys));
                let t1 = Instant::now();
                let (method, body) = if rng.below(100) < write_pct {
                    ("PUT", value(rng.next_u64(), value_bytes))
                } else {
                    ("GET", Bytes::new())
                };
                let request_len = method.len() + 1 + path.len() + 1 + body.len();
                let (status, response) = client_path.call(owner, method, &path, t, body).await;
                lat.push(t1.elapsed().as_micros() as u32);
                request_bytes += request_len as u64;
                response_bytes += response.len() as u64;
                if status != 200 {
                    errors += 1;
                }
            }
            (lat, errors, request_bytes, response_bytes)
        }));
    }
    tokio::time::sleep(Duration::from_secs(secs)).await;
    stop.store(true, Ordering::Relaxed);
    let mut lat = Vec::new();
    let mut errors = 0;
    let (mut request_bytes, mut response_bytes) = (0u64, 0u64);
    for h in hs {
        let (l, e, req, resp) = h.await.expect("client");
        lat.extend(l);
        errors += e;
        request_bytes += req;
        response_bytes += resp;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let far_cpu = boundary.far_cpu_secs() - far_cpu_before;
    let cpu = cpu_secs() - cpu_before + far_cpu;
    let reqs = store.total().total() - before.total();
    lat.sort_unstable();
    let pct = |p: f64| lat.get(((lat.len() as f64 * p) as usize).min(lat.len().saturating_sub(1))).copied();
    let ms = |v: Option<u32>| v.map(|v| v as f64 / 1000.0).unwrap_or(0.0);
    let cache = node.engine.shared.cache.stats();
    println!(
        "throughput: {dbs} dbs × {keys} keys of {value_bytes} B, {clients} clients, {threads} threads, \
         {write_pct}% writes, {latency} ms store latency"
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
    println!(
        "  cpu              {:.1} µs per op ({:.0}% of one core)",
        cpu * 1e6 / lat.len().max(1) as f64,
        cpu * 100.0 / elapsed
    );
    let ops = lat.len().max(1) as f64;
    println!(
        "  boundary         {} over {} ({:.1} µs worker cpu per op; per op {:.1} B request, {:.1} B response)",
        mode.name(),
        if tcp { "tcp" } else { "inproc" },
        far_cpu * 1e6 / ops,
        request_bytes as f64 / ops,
        response_bytes as f64 / ops
    );
    println!("  store requests   {:.3} per op", reqs / lat.len().max(1) as f64);
    println!("  open handles     {}", node.engine.open_count());
    println!("  cache            {} bytes in {} entries", cache.bytes, cache.entries);
    node.stop();
}
