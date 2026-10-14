//! Production entry point. Configuration comes from environment variables
//! (SPEC §12).

use baserunna::cluster::{HttpTransport, Transport};
use baserunna::engine::local::{DiskCache, LocalTier};
use baserunna::env::Env;
use baserunna::limits::Rate;
use baserunna::meta::SignupPolicy;
use baserunna::node::{Node, NodeConfig};
use baserunna::store::{DynStore, FsStore, MemStore};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

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
    if let Some(dir) = var("BASERUNNA_DISK_CACHE_DIR") {
        let bytes = parse("BASERUNNA_DISK_CACHE_MB", 10_240u64)? * 1024 * 1024;
        let tier = DiskCache::open(&dir, bytes).map_err(|e| format!("cannot open disk cache {dir}: {e}"))?;
        cfg.engine.local_tier = Some(LocalTier(Arc::new(tier)));
    }
    cfg.engine.lease_ms = parse("BASERUNNA_LEASE_MS", 5_000u64)?;
    cfg.engine.read_lease_ms = 3 * cfg.engine.lease_ms;
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
            _ => eprintln!("baserunna: cannot install signal handlers; stopping on Ctrl-C only"),
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() {
    baserunna::log::init_from_env();
    if let Err(e) = run().await {
        eprintln!("baserunna: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let (cfg, listen, store_url, shutdown) = config()?;
    let store = store_from_url(&store_url)?;
    let oidc: Option<Arc<dyn baserunna::auth::OidcProvider>> =
        match (var("BASERUNNA_GOOGLE_CLIENT_ID"), var("BASERUNNA_GOOGLE_CLIENT_SECRET")) {
            #[cfg(feature = "cloud")]
            (Some(id), Some(secret)) => Some(Arc::new(baserunna::auth::GoogleOidc::new(id, secret))),
            _ => None,
        };
    let transport: Arc<dyn Transport> = Arc::new(HttpTransport::new());
    let env = Arc::new(Env::real());
    let node =
        Node::start(cfg, env.clone(), store, transport, oidc).await.map_err(|e| format!("startup failed: {e}"))?;
    let app = baserunna::api::router(node.clone());
    let listener =
        tokio::net::TcpListener::bind(&listen).await.map_err(|e| format!("cannot listen on {listen}: {e}"))?;
    eprintln!("baserunna node {} listening on {listen} (store {store_url})", node.id);
    let n2 = node.clone();
    let stopping = async move {
        stop_signal().await;
        eprintln!("shutting down: draining for {:?}", shutdown.drain_delay);
        n2.drain();
        tokio::time::sleep(shutdown.drain_delay).await;
        // Allow in-flight requests up to `timeout` to complete before forcing lease release.
        let n3 = n2.clone();
        env.spawn(async move {
            tokio::time::sleep(shutdown.timeout).await;
            eprintln!("shutting down: requests still in flight after {:?}", shutdown.timeout);
            n3.shutdown().await;
            std::process::exit(0);
        });
    };
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(stopping)
        .await
        .map_err(|e| e.to_string())?;
    eprintln!("shutting down: releasing leases");
    node.shutdown().await;
    Ok(())
}
