//! Server binary entry point and environment configuration.

use baserunna::cluster::{HttpTransport, Transport};
use baserunna::engine::local::{DiskCache, LocalTier};
use baserunna::env::Env;
use baserunna::limits::Rate;
use baserunna::meta::SignupPolicy;
use baserunna::node::{Node, NodeConfig, Services};
use baserunna::store::{DynStore, FsStore, MemStore};
use futures_util::future::Either;
use std::future::IntoFuture;
use std::io::IsTerminal;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::Instrument;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

fn parse<T: std::str::FromStr>(k: &str, default: T) -> Result<T, String> {
    match var(k) {
        None => Ok(default),
        Some(v) => v.parse().map_err(|_| format!("invalid value for {k}: {v:?}")),
    }
}

pub fn store_from_url(url: &str) -> Result<DynStore, String> {
    if let Some(dir) = url.strip_prefix("fs:") {
        return Ok(Arc::new(FsStore::new(dir).map_err(|e| format!("cannot open {dir}: {e}"))?));
    }
    if url == "mem:" || url == "mem" {
        return Ok(Arc::new(MemStore::with_system_clock()));
    }
    #[cfg(feature = "cloud")]
    {
        Ok(Arc::new(baserunna::store::CloudStore::from_url(url)?))
    }
    #[cfg(not(feature = "cloud"))]
    {
        Err(format!("unsupported store {url:?} (build with the `cloud` feature)"))
    }
}

#[cfg(feature = "cloud")]
async fn verify_store_conditional_writes(url: &str) -> Result<(), String> {
    let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    let ns = format!("{}/preflight/{nonce:x}-{}", url.trim_end_matches('/'), std::process::id());
    let store: DynStore = Arc::new(baserunna::store::CloudStore::from_url(&ns)?);
    baserunna::store::conformance::preflight(store)
        .await
        .map_err(|e| format!("object store at {url} failed conditional-write verification: {e}"))
}

struct Shutdown {
    /// Delay before stopping connection acceptance to allow health-check draining.
    drain_delay: Duration,
    /// Grace period for in-flight requests before forced termination.
    timeout: Duration,
}

fn config() -> Result<(NodeConfig, String, String, Shutdown), String> {
    let listen = var("BASERUNNA_LISTEN").unwrap_or_else(|| "0.0.0.0:8080".into());
    let port = listen.rsplit(':').next().unwrap_or("8080").to_string();
    let advertise = var("BASERUNNA_ADVERTISE").unwrap_or_else(|| format!("http://127.0.0.1:{port}"));
    let public_url = var("BASERUNNA_PUBLIC_URL").unwrap_or_else(|| advertise.clone());
    let mut cfg = NodeConfig {
        name: var("BASERUNNA_NODE_ID").unwrap_or_else(|| "node".into()),
        addr: advertise,
        secure_cookies: public_url.starts_with("https://"),
        public_url,
        cluster_secret: var("BASERUNNA_CLUSTER_SECRET"),
        signup: match var("BASERUNNA_SIGNUP") {
            None => SignupPolicy::FirstUserOnly,
            Some(s) => SignupPolicy::parse(&s).ok_or_else(|| format!("invalid BASERUNNA_SIGNUP {s:?}"))?,
        },
        admin_email: var("BASERUNNA_ADMIN_EMAIL"),
        admin_password: var("BASERUNNA_ADMIN_PASSWORD"),
        pbkdf2_iterations: parse("BASERUNNA_PBKDF2_ITERATIONS", 600_000u32)?,
        ..NodeConfig::default()
    };
    cfg.engine.cache_bytes = parse("BASERUNNA_CACHE_MB", 512usize)? * 1024 * 1024;
    cfg.engine.memory_bytes = parse("BASERUNNA_DB_MEMORY_MB", 256usize)? * 1024 * 1024;
    if let Some(dir) = var("BASERUNNA_DISK_CACHE_DIR") {
        let bytes = parse("BASERUNNA_DISK_CACHE_MB", 10_240u64)? * 1024 * 1024;
        let tier = DiskCache::open(&dir, bytes).map_err(|e| format!("cannot open disk cache {dir}: {e}"))?;
        cfg.engine.local_tier = Some(LocalTier(Arc::new(tier)));
    }
    cfg.engine.lease_ms = parse("BASERUNNA_LEASE_MS", 5_000u64)?;
    cfg.engine.read_lease_ms = 3 * cfg.engine.lease_ms;
    cfg.engine.manifest_put_hedge_ms = parse("BASERUNNA_PUT_HEDGE_MS", 250u64)?;
    cfg.engine.db_max_bytes = parse("BASERUNNA_DB_MAX_MB", 10_240u64)? * 1024 * 1024;
    let l = &mut cfg.limits;
    l.user = Rate::per_sec(parse("BASERUNNA_USER_RPS", 500.0)?);
    l.api_key = Rate::per_sec(parse("BASERUNNA_API_KEY_RPS", 500.0)?);
    l.auth_per_client = Rate { per_sec: parse("BASERUNNA_AUTH_RPS_PER_CLIENT", 1.0)?, burst: 20.0 };
    l.signup_per_client = Rate::per_hour(parse("BASERUNNA_SIGNUPS_PER_HOUR_PER_CLIENT", 20.0)?, 5.0);
    l.trusted_proxy_hops = parse("BASERUNNA_TRUSTED_PROXY_HOPS", 0usize)?;
    l.hash_concurrency = parse("BASERUNNA_HASH_CONCURRENCY", l.hash_concurrency)?;
    let shutdown = Shutdown {
        drain_delay: Duration::from_millis(parse("BASERUNNA_DRAIN_DELAY_MS", 0u64)?),
        timeout: Duration::from_millis(parse("BASERUNNA_SHUTDOWN_TIMEOUT_MS", 20_000u64)?),
    };
    let store = var("BASERUNNA_STORE").unwrap_or_else(|| "fs:./data".into());
    Ok((cfg, listen, store, shutdown))
}

/// Installs the global tracing subscriber configured by `BASERUNNA_LOG` and
/// `BASERUNNA_LOG_FORMAT`. Events are written to stderr from a background
/// thread that drops them when stderr falls behind; dropping the returned guard
/// flushes queued events.
fn init_logging() -> Result<WorkerGuard, String> {
    let filter = var("BASERUNNA_LOG").unwrap_or_else(|| "info".into());
    let filter = EnvFilter::try_new(&filter).map_err(|e| format!("invalid BASERUNNA_LOG {filter:?}: {e}"))?;
    let (writer, guard) = tracing_appender::non_blocking(std::io::stderr());
    let dropped = writer.error_counter();
    let layer = tracing_subscriber::fmt::layer().with_writer(writer);
    let layer = match var("BASERUNNA_LOG_FORMAT").as_deref() {
        None | Some("json") => layer.json().flatten_event(true).with_current_span(false).with_span_list(true).boxed(),
        Some("text") => layer.with_ansi(std::io::stderr().is_terminal()).boxed(),
        Some(o) => return Err(format!("unknown log format {o:?} (json or text)")),
    };
    tracing_subscriber::registry().with(filter).with(layer).try_init().map_err(|e| e.to_string())?;
    std::thread::Builder::new()
        .name("log-drops".into())
        .spawn(move || {
            let mut reported = 0;
            loop {
                std::thread::sleep(Duration::from_secs(1));
                let n = dropped.dropped_lines();
                if n > reported {
                    tracing::warn!(lines = n - reported, "dropped log lines because stderr fell behind");
                    reported = n;
                }
            }
        })
        .map_err(|e| format!("cannot start the log monitor: {e}"))?;
    Ok(guard)
}

/// Resolves on SIGTERM or SIGINT.
async fn stop_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match (signal(SignalKind::terminate()), signal(SignalKind::interrupt())) {
            (Ok(mut term), Ok(mut int)) => {
                let either = std::future::poll_fn(|cx| match (term.poll_recv(cx), int.poll_recv(cx)) {
                    (std::task::Poll::Pending, std::task::Poll::Pending) => std::task::Poll::Pending,
                    _ => std::task::Poll::Ready(()),
                });
                either.await;
                return;
            }
            _ => tracing::warn!("cannot install signal handlers; stopping on Ctrl-C only"),
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() {
    let logs = match init_logging() {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("baserunna: {e}");
            std::process::exit(1);
        }
    };
    let result = run().await;
    if let Err(e) = &result {
        tracing::error!("{e}");
    }
    drop(logs);
    std::process::exit(i32::from(result.is_err()));
}

async fn run() -> Result<(), String> {
    let (cfg, listen, store_url, shutdown) = config()?;
    let store = store_from_url(&store_url)?;
    #[cfg(feature = "cloud")]
    if store_url.starts_with("s3://") {
        verify_store_conditional_writes(&store_url).await?;
    }
    let oidc: Option<Arc<dyn baserunna::auth::OidcProvider>> =
        match (var("BASERUNNA_GOOGLE_CLIENT_ID"), var("BASERUNNA_GOOGLE_CLIENT_SECRET")) {
            #[cfg(feature = "cloud")]
            (Some(id), Some(secret)) => Some(Arc::new(baserunna::auth::GoogleOidc::new(id, secret))),
            _ => None,
        };
    let mailer: Option<Arc<dyn baserunna::mail::Mailer>> = match var("BASERUNNA_MAIL_WEBHOOK_URL") {
        #[cfg(feature = "cloud")]
        Some(url) => Some(Arc::new(baserunna::mail::WebhookMailer::new(
            url,
            var("BASERUNNA_MAIL_WEBHOOK_TOKEN"),
            var("BASERUNNA_MAIL_FROM").unwrap_or_else(|| "Baserunna <noreply@localhost>".into()),
        ))),
        #[cfg(not(feature = "cloud"))]
        Some(_) => return Err("BASERUNNA_MAIL_WEBHOOK_URL needs the `cloud` feature".into()),
        None => None,
    };
    let transport: Arc<dyn Transport> = Arc::new(HttpTransport::new());
    let env = Arc::new(Env::real());
    let services = Services { transport, oidc, mailer };
    let node = Node::start(cfg, env.clone(), store, services).await.map_err(|e| format!("startup failed: {e}"))?;
    let n = node.clone();
    env.spawn(async move {
        const PERIOD: std::time::Duration = std::time::Duration::from_millis(100);
        loop {
            let started = tokio::time::Instant::now();
            tokio::time::sleep(PERIOD).await;
            n.metrics.lag(started.elapsed().saturating_sub(PERIOD));
        }
    });
    serve(node.clone(), &listen, &store_url, shutdown).instrument(env.span()).await
}

/// Serves `node` on `listen` until a stop signal, then drains and releases its leases.
async fn serve(node: Arc<Node>, listen: &str, store_url: &str, shutdown: Shutdown) -> Result<(), String> {
    let app = baserunna::api::router(node.clone());
    let listener =
        tokio::net::TcpListener::bind(listen).await.map_err(|e| format!("cannot listen on {listen}: {e}"))?;
    tracing::info!(addr = listen, store = store_url, "listening");
    let drained = Arc::new(tokio::sync::Notify::new());
    let stopping = {
        let (node, drained) = (node.clone(), drained.clone());
        async move {
            stop_signal().await;
            tracing::info!(drain_ms = shutdown.drain_delay.as_millis() as u64, "shutting down: draining");
            node.drain();
            tokio::time::sleep(shutdown.drain_delay).await;
            node.end_waits();
            drained.notify_one();
        }
        .in_current_span()
    };
    let listener = axum::serve::ListenerExt::tap_io(listener, |tcp| {
        let _ = tcp.set_nodelay(true);
    });
    let serving = axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(stopping)
        .into_future();
    let overdue = async {
        drained.notified().await;
        tokio::time::sleep(shutdown.timeout).await;
    };
    let serving = std::pin::pin!(serving);
    let overdue = std::pin::pin!(overdue);
    match futures_util::future::select(serving, overdue).await {
        Either::Left((served, _)) => served.map_err(|e| e.to_string())?,
        Either::Right(((), _)) => tracing::warn!(
            timeout_ms = shutdown.timeout.as_millis() as u64,
            "shutting down: requests still in flight after the timeout"
        ),
    }
    tracing::info!("shutting down: releasing leases");
    node.shutdown().await;
    Ok(())
}
