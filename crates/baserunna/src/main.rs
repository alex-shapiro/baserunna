//! Server binary entry point and environment configuration.

use baserunna::cluster::{HttpTransport, Transport};
use baserunna::engine::local::{DiskCache, LocalTier};
use baserunna::env::Env;
use baserunna::limits::Rate;
use baserunna::meta::SignupPolicy;
use baserunna::node::{Node, NodeConfig, Services};
use baserunna::store::{DynStore, FsStore, MemStore};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// Reads the value of a configuration variable.
type Lookup = dyn Fn(&str) -> Option<String>;

/// Configuration variables. An empty value counts as unset.
struct Vars {
    lookup: Box<Lookup>,
}

impl Vars {
    fn from_env() -> Vars {
        Vars { lookup: Box::new(|k| std::env::var(k).ok()) }
    }

    fn get(&self, k: &str) -> Option<String> {
        (self.lookup)(k).filter(|v| !v.is_empty())
    }

    fn parse<T: std::str::FromStr>(&self, k: &str, default: T) -> Result<T, String> {
        match self.get(k) {
            None => Ok(default),
            Some(v) => v.parse().map_err(|_| format!("invalid value for {k}: {v:?}")),
        }
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

fn config(vars: &Vars) -> Result<(NodeConfig, String, String, Shutdown), String> {
    let listen = vars.get("BASERUNNA_LISTEN").unwrap_or_else(|| "0.0.0.0:8080".into());
    let port = listen.rsplit(':').next().unwrap_or("8080").to_string();
    let advertise = vars.get("BASERUNNA_ADVERTISE").unwrap_or_else(|| format!("http://127.0.0.1:{port}"));
    let public_url = vars.get("BASERUNNA_PUBLIC_URL").unwrap_or_else(|| advertise.clone());
    let d = NodeConfig::default();
    let mut cfg = NodeConfig {
        name: vars.get("BASERUNNA_NODE_ID").unwrap_or_else(|| "node".into()),
        addr: advertise,
        secure_cookies: public_url.starts_with("https://"),
        public_url,
        cluster_secret: vars.get("BASERUNNA_CLUSTER_SECRET"),
        signup: match vars.get("BASERUNNA_SIGNUP") {
            None => SignupPolicy::FirstUserOnly,
            Some(s) => SignupPolicy::parse(&s).ok_or_else(|| format!("invalid BASERUNNA_SIGNUP {s:?}"))?,
        },
        admin_email: vars.get("BASERUNNA_ADMIN_EMAIL"),
        admin_password: vars.get("BASERUNNA_ADMIN_PASSWORD"),
        pbkdf2_iterations: vars.parse("BASERUNNA_PBKDF2_ITERATIONS", d.pbkdf2_iterations)?,
        max_dbs_per_owner: vars.parse("BASERUNNA_MAX_DBS_PER_OWNER", d.max_dbs_per_owner)?,
        ..d.clone()
    };
    const MIB: usize = 1024 * 1024;
    cfg.engine.cache_bytes = vars.parse("BASERUNNA_CACHE_MB", d.engine.cache_bytes / MIB)? * MIB;
    cfg.engine.memory_bytes = vars.parse("BASERUNNA_DB_MEMORY_MB", d.engine.memory_bytes / MIB)? * MIB;
    if let Some(dir) = vars.get("BASERUNNA_DISK_CACHE_DIR") {
        let bytes = vars.parse("BASERUNNA_DISK_CACHE_MB", 10_240u64)? * 1024 * 1024;
        let tier = DiskCache::open(&dir, bytes).map_err(|e| format!("cannot open disk cache {dir}: {e}"))?;
        cfg.engine.local_tier = Some(LocalTier(Arc::new(tier)));
    }
    cfg.engine.lease_ms = vars.parse("BASERUNNA_LEASE_MS", d.engine.lease_ms)?;
    cfg.engine.read_lease_ms = 3 * cfg.engine.lease_ms;
    cfg.engine.manifest_put_hedge_ms = vars.parse("BASERUNNA_PUT_HEDGE_MS", d.engine.manifest_put_hedge_ms)?;
    cfg.engine.db_max_bytes = vars.parse("BASERUNNA_DB_MAX_MB", d.engine.db_max_bytes / MIB as u64)? * MIB as u64;
    let (l, dl) = (&mut cfg.limits, &d.limits);
    l.user = Rate::per_sec(vars.parse("BASERUNNA_USER_RPS", dl.user.per_sec)?);
    l.api_key = Rate::per_sec(vars.parse("BASERUNNA_API_KEY_RPS", dl.api_key.per_sec)?);
    l.auth_per_client.per_sec = vars.parse("BASERUNNA_AUTH_RPS_PER_CLIENT", dl.auth_per_client.per_sec)?;
    let signups = vars.parse("BASERUNNA_SIGNUPS_PER_HOUR_PER_CLIENT", dl.signup_per_client.per_sec * 3600.0)?;
    l.signup_per_client = Rate::per_hour(signups, dl.signup_per_client.burst);
    l.trusted_proxy_hops = vars.parse("BASERUNNA_TRUSTED_PROXY_HOPS", dl.trusted_proxy_hops)?;
    l.hash_concurrency = vars.parse("BASERUNNA_HASH_CONCURRENCY", dl.hash_concurrency)?;
    let shutdown = Shutdown {
        drain_delay: Duration::from_millis(vars.parse("BASERUNNA_DRAIN_DELAY_MS", 0u64)?),
        timeout: Duration::from_millis(vars.parse("BASERUNNA_SHUTDOWN_TIMEOUT_MS", 20_000u64)?),
    };
    let store = vars.get("BASERUNNA_STORE").unwrap_or_else(|| "fs:./data".into());
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
            _ => baserunna::warn!("cannot install signal handlers; stopping on Ctrl-C only"),
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() {
    if let Err(e) = baserunna::log::init_from_env() {
        eprintln!("baserunna: {e}");
        std::process::exit(1);
    }
    baserunna::log::start_background_writer();
    let result = run().await;
    if let Err(e) = &result {
        baserunna::error!("{e}");
    }
    baserunna::log::flush();
    if result.is_err() {
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let vars = Vars::from_env();
    let (cfg, listen, store_url, shutdown) = config(&vars)?;
    let store = store_from_url(&store_url)?;
    #[cfg(feature = "cloud")]
    if store_url.starts_with("s3://") {
        verify_store_conditional_writes(&store_url).await?;
    }
    let oidc: Option<Arc<dyn baserunna::auth::OidcProvider>> =
        match (vars.get("BASERUNNA_GOOGLE_CLIENT_ID"), vars.get("BASERUNNA_GOOGLE_CLIENT_SECRET")) {
            #[cfg(feature = "cloud")]
            (Some(id), Some(secret)) => Some(Arc::new(baserunna::auth::GoogleOidc::new(id, secret))),
            _ => None,
        };
    let mailer: Option<Arc<dyn baserunna::mail::Mailer>> = match vars.get("BASERUNNA_MAIL_WEBHOOK_URL") {
        #[cfg(feature = "cloud")]
        Some(url) => Some(Arc::new(baserunna::mail::WebhookMailer::new(
            url,
            vars.get("BASERUNNA_MAIL_WEBHOOK_TOKEN"),
            vars.get("BASERUNNA_MAIL_FROM").unwrap_or_else(|| "Baserunna <noreply@localhost>".into()),
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
    baserunna::log::set_process_node(&node.id);
    let app = baserunna::api::router(node.clone());
    let listener =
        tokio::net::TcpListener::bind(&listen).await.map_err(|e| format!("cannot listen on {listen}: {e}"))?;
    baserunna::info!("listening on {listen} (store {store_url})");
    let n2 = node.clone();
    let stopping = async move {
        stop_signal().await;
        baserunna::info!("shutting down: draining for {:?}", shutdown.drain_delay);
        n2.drain();
        tokio::time::sleep(shutdown.drain_delay).await;
        n2.end_waits();
        // Allow in-flight requests up to `timeout` to complete before forcing lease release.
        let n3 = n2.clone();
        env.spawn(async move {
            tokio::time::sleep(shutdown.timeout).await;
            baserunna::warn!("shutting down: requests still in flight after {:?}", shutdown.timeout);
            n3.shutdown().await;
            baserunna::log::flush();
            std::process::exit(0);
        });
    };
    let listener = axum::serve::ListenerExt::tap_io(listener, |tcp| {
        let _ = tcp.set_nodelay(true);
    });
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(stopping)
        .await
        .map_err(|e| e.to_string())?;
    baserunna::info!("shutting down: releasing leases");
    node.shutdown().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_variables_keep_node_defaults() {
        let (cfg, ..) = config(&Vars { lookup: Box::new(|_| None) }).unwrap();
        let d = NodeConfig::default();
        assert_eq!(cfg.max_dbs_per_owner, d.max_dbs_per_owner);
        assert_eq!(cfg.pbkdf2_iterations, d.pbkdf2_iterations);
        assert_eq!(cfg.engine.cache_bytes, d.engine.cache_bytes);
        assert_eq!(cfg.engine.memory_bytes, d.engine.memory_bytes);
        assert_eq!(cfg.engine.lease_ms, d.engine.lease_ms);
        assert_eq!(cfg.engine.db_max_bytes, d.engine.db_max_bytes);
        assert_eq!(cfg.limits.user.per_sec, d.limits.user.per_sec);
        assert_eq!(cfg.limits.auth_per_client.burst, d.limits.auth_per_client.burst);
        assert!((cfg.limits.signup_per_client.per_sec - d.limits.signup_per_client.per_sec).abs() < 1e-12);
    }

    #[test]
    fn set_variables_override_node_defaults() {
        let vars = Vars {
            lookup: Box::new(|k| match k {
                "BASERUNNA_LEASE_MS" => Some("1234".into()),
                "BASERUNNA_CACHE_MB" => Some(String::new()),
                _ => None,
            }),
        };
        let (cfg, ..) = config(&vars).unwrap();
        assert_eq!(cfg.engine.lease_ms, 1234);
        assert_eq!(cfg.engine.cache_bytes, NodeConfig::default().engine.cache_bytes, "an empty value is unset");
        let bad = Vars { lookup: Box::new(|k| (k == "BASERUNNA_LEASE_MS").then(|| "soon".into())) };
        assert!(config(&bad).is_err());
    }
}
