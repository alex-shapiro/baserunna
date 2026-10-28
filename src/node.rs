//! A service node: engine + metadata + auth + routing + background work.

use crate::auth::{self, OidcProvider};
use crate::cluster::{self, CLUSTER_KEY_HEADER, DEADLINE_HEADER, NoTransport, Transport};
use crate::engine::{Consistency, Db, Engine, EngineConfig, Isolation, Txn};
use crate::env::Env;
use crate::error::{Error, Result};
use crate::limits::{LimitConfig, Limiter, Rate};
use crate::mail::{Mail, Mailer};
use crate::meta::{
    self, Access, ApiKey, Batch, DbRecord, DbRef, MetaCmd, NodeRecord, Org, Role, SYS_DB, SignInMethod, SignedUp,
    SignupPolicy, User, UserDeletion,
};
use crate::store::DynStore;
use crate::table::BlobResolver;
use crate::txns::{OpenTxns, TxnLimits};
use crate::util::{DetHashMap, TtlCache};
use axum::body::Body;
use http::Request;
use serde::de::DeserializeOwned;
use serde_json::{Value as Json, json};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// Human-readable prefix of the node id.
    pub name: String,
    /// Base URL peers use to reach this node.
    pub addr: String,
    /// External base URL for browser redirects and emailed links.
    pub public_url: String,
    pub cluster_secret: Option<String>,
    pub signup: SignupPolicy,
    pub admin_email: Option<String>,
    pub admin_password: Option<String>,
    pub pbkdf2_iterations: u32,
    pub session_ttl_ms: u64,
    /// Interval between node membership heartbeats.
    pub heartbeat_ms: u64,
    /// Interval between membership table refreshes.
    pub members_refresh_ms: u64,
    pub auth_cache_ms: u64,
    pub sys_staleness_ms: u64,
    pub db_delete_grace_ms: u64,
    /// Maximum databases per user or organization.
    pub max_dbs_per_owner: usize,
    /// Maximum organizations a user may belong to.
    pub max_orgs_per_user: usize,
    pub max_members_per_org: usize,
    /// Rows per transaction when backfilling a new index.
    pub index_backfill_batch_rows: usize,
    pub json_value_max: u64,
    pub max_json_body: usize,
    pub txn_idle_ms: u64,
    pub txn_max_ms: u64,
    pub txns_per_user: usize,
    pub db_idle_evict_ms: u64,
    pub secure_cookies: bool,
    pub txn_retries: u32,
    /// Manifest poll interval while waiting on a change feed.
    pub watch_poll_ms: u64,
    pub limits: LimitConfig,
    pub engine: EngineConfig,
}

impl NodeConfig {
    pub fn txn_limits(&self) -> TxnLimits {
        TxnLimits {
            per_user: self.txns_per_user,
            idle: Duration::from_millis(self.txn_idle_ms),
            lifetime: Duration::from_millis(self.txn_max_ms),
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        NodeConfig {
            name: "node".into(),
            addr: "http://127.0.0.1:8080".into(),
            public_url: "http://127.0.0.1:8080".into(),
            cluster_secret: None,
            signup: SignupPolicy::FirstUserOnly,
            admin_email: None,
            admin_password: None,
            pbkdf2_iterations: 600_000,
            session_ttl_ms: 30 * 24 * 3_600_000,
            heartbeat_ms: 120_000,
            members_refresh_ms: 30_000,
            auth_cache_ms: 30_000,
            sys_staleness_ms: 2_000,
            db_delete_grace_ms: 10 * 60_000,
            max_dbs_per_owner: 1000,
            max_orgs_per_user: 100,
            max_members_per_org: 1000,
            index_backfill_batch_rows: 1000,
            json_value_max: 1024 * 1024,
            max_json_body: 32 * 1024 * 1024,
            txn_idle_ms: 30_000,
            txns_per_user: 100,
            txn_max_ms: 5 * 60_000,
            db_idle_evict_ms: 5 * 60_000,
            secure_cookies: false,
            txn_retries: 4,
            watch_poll_ms: 1_000,
            limits: LimitConfig::default(),
            engine: EngineConfig { db_max_bytes: 10 << 30, ..EngineConfig::default() },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Credential {
    Session { token_hash: String },
    ApiKey { id: String, db_id: Option<String>, read_only: bool, rate_limit: Option<u32> },
}

/// Rate-limit accounting mode for a request (§9).
#[derive(Clone, Copy, Debug)]
pub enum Meter {
    /// Consumes a token and rejects the request if a bucket is empty.
    Take,
    /// Consumes a token and allows overdrawing a bucket by up to its burst.
    Borrow,
    /// Does not consume tokens because the forwarding peer already charged the request.
    Exempt,
}

#[derive(Clone, Debug)]
pub struct Principal {
    pub user: User,
    pub cred: Credential,
}

impl Principal {
    pub fn read_only(&self) -> bool {
        matches!(self.cred, Credential::ApiKey { read_only: true, .. })
    }

    pub fn scoped_db(&self) -> Option<&str> {
        match &self.cred {
            Credential::ApiKey { db_id, .. } => db_id.as_deref(),
            _ => None,
        }
    }

    pub fn is_session(&self) -> bool {
        matches!(self.cred, Credential::Session { .. })
    }

    pub fn require_writable(&self) -> Result<()> {
        if self.read_only() {
            return Err(Error::PermissionDenied("this credential is read-only".into()));
        }
        Ok(())
    }

    pub fn require_unscoped(&self) -> Result<()> {
        if self.scoped_db().is_some() {
            return Err(Error::PermissionDenied("this API key is limited to one database".into()));
        }
        Ok(())
    }

    pub fn require_session(&self) -> Result<()> {
        self.session_hash().map(|_| ())
    }

    /// Returns the session's token hash, or `PermissionDenied` for API keys.
    pub fn session_hash(&self) -> Result<&str> {
        match &self.cred {
            Credential::Session { token_hash } => Ok(token_hash),
            Credential::ApiKey { .. } => {
                Err(Error::PermissionDenied("this operation requires a signed-in session".into()))
            }
        }
    }

    pub fn require_admin(&self) -> Result<()> {
        if !self.user.is_admin {
            return Err(Error::PermissionDenied("administrator access required".into()));
        }
        Ok(())
    }
}

/// External services used by a node.
#[derive(Clone)]
pub struct Services {
    pub transport: Arc<dyn Transport>,
    pub oidc: Option<Arc<dyn OidcProvider>>,
    pub mailer: Option<Arc<dyn Mailer>>,
}

impl Services {
    pub fn local() -> Services {
        Services { transport: Arc::new(NoTransport), oidc: None, mailer: None }
    }
}

struct LinkLife {
    ms: u64,
    words: &'static str,
}

const RESET_LINK: LinkLife = LinkLife { ms: 3_600_000, words: "an hour" };
const VERIFY_LINK: LinkLife = LinkLife { ms: 7 * 24 * 3_600_000, words: "7 days" };

pub const REAP_INTERVAL_MS: u64 = 60_000;

const REAPER_KEY: &str = "reaper";

const META_BATCH: usize = 500;

tokio::task_local! {
    static REQUEST_DEADLINE: Instant;
}

/// Runs `f` with `deadline` as the commit deadline for all metadata commands inside `f`.
pub async fn with_deadline<F: std::future::Future>(deadline: Instant, f: F) -> F::Output {
    REQUEST_DEADLINE.scope(deadline, f).await
}

pub struct Node {
    pub id: String,
    pub cfg: NodeConfig,
    pub env: Arc<Env>,
    pub engine: Arc<Engine>,
    pub services: Services,
    members: Mutex<Vec<NodeRecord>>,
    members_refreshed_on_demand: Mutex<Option<Instant>>,
    suspects: Mutex<DetHashMap<String, Suspect>>,
    /// Map from database ID to the latest committed version for databases with an active
    /// [`Node::notify_home`] task.
    pending_notices: Mutex<DetHashMap<String, u64>>,
    auth_cache: TtlCache<String, Principal>,
    name_cache: TtlCache<(String, String), DbRecord>,
    org_cache: TtlCache<String, Org>,
    /// Caches positive `(org_id, user_id)` role lookups; misses read strongly.
    role_cache: TtlCache<(String, String), Role>,
    pub txns: Arc<OpenTxns>,
    cookie_key: Mutex<Vec<u8>>,
    cluster_key: Mutex<Option<String>>,
    pub forwards: cluster::ForwardDedup,
    pub started_ms: u64,
    router: OnceLock<axum::Router>,
    pub limiter: Limiter,
    pub metrics: crate::metrics::ServerMetrics,
    hash_slots: tokio::sync::Semaphore,
    draining: std::sync::atomic::AtomicBool,
}

/// Tracks backoff state for a peer after an undelivered forward.
struct Suspect {
    started_at: i64,
    heartbeat_at: i64,
    strikes: u32,
    until: Instant,
}

const MEMBERS_REFRESH_ON_DEMAND_MIN: Duration = Duration::from_secs(1);

const SUSPECT_MIN: Duration = Duration::from_secs(10);

/// Timeout for a `/internal/v1/notice` request to a peer node.
const NOTICE_TIMEOUT: Duration = Duration::from_secs(2);

const CACHE_MAX: usize = 100_000;

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Node {
    /// Starts a node, bootstrapping the system database and background tasks.
    pub async fn start(cfg: NodeConfig, env: Arc<Env>, store: DynStore, services: Services) -> Result<Arc<Node>> {
        cfg.engine.validate().map_err(|e| Error::invalid(format!("engine configuration: {e}")))?;
        let id = format!("{}-{}", cfg.name, env.random_hex(4));
        let engine = Engine::new(store, env.clone(), cfg.engine.clone(), id.clone());
        let txns = OpenTxns::new(engine.clone(), cfg.txn_limits());
        engine.mark_system(SYS_DB);
        let node = Arc::new(Node {
            id,
            started_ms: engine.shared.time_ms(),
            env,
            engine,
            services,
            members: Mutex::new(Vec::new()),
            members_refreshed_on_demand: Mutex::new(None),
            suspects: Mutex::new(DetHashMap::default()),
            pending_notices: Mutex::new(DetHashMap::default()),
            auth_cache: TtlCache::new(Duration::from_millis(cfg.auth_cache_ms), CACHE_MAX),
            name_cache: TtlCache::new(Duration::from_millis(cfg.auth_cache_ms), CACHE_MAX),
            org_cache: TtlCache::new(Duration::from_millis(cfg.auth_cache_ms), CACHE_MAX),
            role_cache: TtlCache::new(Duration::from_millis(cfg.auth_cache_ms), CACHE_MAX),
            txns,
            cookie_key: Mutex::new(Vec::new()),
            cluster_key: Mutex::new(None),
            forwards: cluster::ForwardDedup::new(Duration::from_secs(60)),
            router: OnceLock::new(),
            limiter: Limiter::default(),
            metrics: Default::default(),
            hash_slots: tokio::sync::Semaphore::new(cfg.limits.hash_concurrency.max(1)),
            draining: std::sync::atomic::AtomicBool::new(false),
            cfg,
        });
        let weak = Arc::downgrade(&node);
        node.engine.set_commit_hook(Box::new(move |db: &str, version: u64| {
            if let Some(n) = weak.upgrade() {
                n.notify_home(db, version);
            }
        }));
        node.bootstrap().await?;
        node.spawn_background();
        Ok(node)
    }

    /// Sends a best-effort `/internal/v1/notice` request with `version` to the database's
    /// rendezvous home node so that long-polling readers on the home node wake immediately.
    /// Coalesces concurrent notices so at most 1 request is in flight per database.
    fn notify_home(self: &Arc<Self>, db: &str, version: u64) {
        if db == SYS_DB {
            return;
        }
        {
            let mut pending = lock(&self.pending_notices);
            if let Some(v) = pending.get_mut(db) {
                *v = (*v).max(version);
                return;
            }
            if self.peer_home_addr(db).is_none() {
                return;
            }
            pending.insert(db.to_string(), version);
        }
        let (node, db) = (Arc::downgrade(self), db.to_string());
        self.env.spawn(async move {
            let mut sent = 0;
            while let Some(n) = node.upgrade() {
                let version = {
                    let mut pending = lock(&n.pending_notices);
                    match pending.get(&db) {
                        Some(&v) if v > sent => v,
                        _ => {
                            pending.remove(&db);
                            return;
                        }
                    }
                };
                let call = n.peer_home_addr(&db).and_then(|addr| {
                    let req = n.internal_request("/internal/v1/notice", &json!({"db": db, "version": version})).ok()?;
                    Some(n.services.transport.call(&addr, req))
                });
                drop(n);
                if let Some(call) = call {
                    let _ = tokio::time::timeout(NOTICE_TIMEOUT, call).await;
                }
                sent = version;
            }
        });
    }

    /// Returns the address of the rendezvous home node for `db` if the home node is a remote peer.
    fn peer_home_addr(&self, db: &str) -> Option<String> {
        let live = self.live_members();
        let home = cluster::rendezvous(db, &live)?;
        (home.id != self.id && home.addr != self.cfg.addr).then(|| home.addr.clone())
    }

    /// Builds a JSON `POST` request to `path` authenticated with the cluster key.
    fn internal_request(&self, path: &str, body: &impl serde::Serialize) -> Result<Request<Body>> {
        let body = serde_json::to_vec(body).map_err(|e| Error::internal(e.to_string()))?;
        let mut req = Request::post(path)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .map_err(|e| Error::internal(e.to_string()))?;
        if let Some(k) = self.cluster_key()
            && let Ok(v) = k.parse()
        {
            req.headers_mut().insert(CLUSTER_KEY_HEADER, v);
        }
        Ok(req)
    }

    async fn bootstrap(self: &Arc<Self>) -> Result<()> {
        if let Err(e) = self.engine.shared.store_time_ms().await {
            crate::warn!("{}: could not measure the store clock yet: {e}", self.id);
        }
        let sys = self.engine.db(SYS_DB);
        retry_unavailable(|| sys.open(true)).await?;
        // Load members and cluster key before running metadata commands so they can be forwarded.
        let _ = self.refresh_members().await;
        if let Some(k) = &self.cfg.cluster_secret {
            *lock(&self.cluster_key) = Some(k.clone());
        } else if let Ok(mut t) = self.sys_read_strong().await
            && meta::is_migrated(&mut t).await.unwrap_or(false)
            && let Ok(Some(k)) = meta::get_config(&mut t, "cluster_key").await
        {
            *lock(&self.cluster_key) = Some(k);
        }
        retry_unavailable(|| self.run_meta(MetaCmd::Migrate)).await?;
        let ensure = |key: &str| MetaCmd::EnsureConfig { key: key.into(), value: self.env.random_hex(32) };
        if self.cfg.cluster_secret.is_none() {
            let k: String = retry_unavailable(|| self.run_meta_as(ensure("cluster_key"))).await?;
            *lock(&self.cluster_key) = Some(k);
        }
        let key: String = retry_unavailable(|| self.run_meta_as(ensure("cookie_key"))).await?;
        *lock(&self.cookie_key) = crate::util::b64::unhex(&key).unwrap_or_else(|| key.into_bytes());
        if let (Some(e), Some(p)) = (&self.cfg.admin_email, &self.cfg.admin_password) {
            let email = auth::normalize_email(e)?;
            let mut t = self.sys_read().await?;
            if meta::user_by_email(&mut t, &email).await?.is_none() {
                let hash = self.hash_password(p).await?;
                retry_unavailable(|| {
                    self.run_meta(MetaCmd::Signup {
                        id: self.env.random_hex(8),
                        email: email.clone(),
                        password_hash: Some(hash.clone()),
                        policy: SignupPolicy::Closed,
                        bootstrap_admin: true,
                    })
                })
                .await?;
            }
        }
        let _ = self.heartbeat().await;
        let _ = self.refresh_members().await;
        Ok(())
    }

    fn spawn_background(self: &Arc<Self>) {
        let me = Arc::downgrade(self);
        let (refresh, hb) = (self.cfg.members_refresh_ms, Duration::from_millis(self.cfg.heartbeat_ms));
        self.env.spawn(async move {
            let mut last_heartbeat = Instant::now();
            loop {
                tokio::time::sleep(Duration::from_millis(refresh)).await;
                let Some(n) = me.upgrade() else { return };
                if last_heartbeat.elapsed() >= hb {
                    match n.heartbeat().await {
                        Ok(()) => last_heartbeat = Instant::now(),
                        Err(e) => crate::debug!("{}: heartbeat failed: {e}", n.id),
                    }
                }
                let _ = n.refresh_members().await;
            }
        });
        let me = Arc::downgrade(self);
        self.env.spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(5_000)).await;
                let Some(n) = me.upgrade() else { return };
                n.housekeeping();
            }
        });
        let me = Arc::downgrade(self);
        self.env.spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(REAP_INTERVAL_MS)).await;
                let Some(n) = me.upgrade() else { return };
                if let Err(e) = n.reap_deleted().await {
                    crate::debug!("{}: reaper: {e}", n.id);
                }
            }
        });
    }

    pub fn set_router(&self, r: axum::Router) {
        let _ = self.router.set(r);
    }

    /// Returns the node's HTTP router.
    pub fn router(&self) -> Option<axum::Router> {
        self.router.get().cloned()
    }

    /// Returns the shared cluster secret for internal endpoints.
    pub fn cluster_key(&self) -> Option<String> {
        lock(&self.cluster_key).clone()
    }

    pub fn cookie_key(&self) -> Vec<u8> {
        lock(&self.cookie_key).clone()
    }

    /// Marks the node as draining so health checks return 503 while in-flight requests finish.
    pub fn drain(&self) {
        self.draining.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn draining(&self) -> bool {
        self.draining.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Wakes all active `wait_ms` long polls and causes future waits on this node to return
    /// immediately. Call after the server stops accepting new connections during shutdown.
    pub fn end_waits(&self) {
        self.engine.end_waits();
    }

    /// Leaves the cluster, releases held database leases, and stops background tasks.
    /// Returns the database IDs whose leases were released.
    pub async fn shutdown(&self) -> Vec<String> {
        self.drain();
        self.end_waits();
        let leave = self.run_meta(MetaCmd::Leave { id: self.id.clone() });
        let _ = tokio::time::timeout(Duration::from_secs(2), leave).await;
        let released = self.engine.release_leases(Duration::from_secs(2)).await;
        self.env.shutdown();
        released
    }

    pub fn members(&self) -> Vec<NodeRecord> {
        lock(&self.members).clone()
    }

    /// Returns live non-suspect peers with distinct addresses, plus this node if it is not draining.
    pub fn live_members(&self) -> Vec<NodeRecord> {
        let now = self.now_ms() as i64;
        let mut live: Vec<NodeRecord> = cluster::live(&self.members(), now, self.cfg.heartbeat_ms as i64)
            .into_iter()
            .filter(|n| n.id != self.id && !self.is_suspect(&n.id) && n.addr != self.cfg.addr)
            .collect();
        if !self.draining() {
            live.push(NodeRecord {
                id: self.id.clone(),
                addr: self.cfg.addr.clone(),
                started_at: 0,
                heartbeat_at: now,
            });
        }
        live
    }

    pub async fn refresh_members(&self) -> Result<()> {
        self.refresh_members_at(false).await
    }

    async fn refresh_members_at(&self, strong: bool) -> Result<()> {
        let mut t = self.sys_read_at(strong).await?;
        let nodes = meta::list_nodes(&mut t).await?;
        // Clear suspicion for peers that have heartbeated, restarted, or left.
        lock(&self.suspects).retain(|id, s| {
            nodes.iter().any(|n| n.id == *id && n.started_at == s.started_at && n.heartbeat_at <= s.heartbeat_at)
        });
        *lock(&self.members) = nodes;
        Ok(())
    }

    async fn heartbeat(&self) -> Result<()> {
        let now = self.now_ms() as i64;
        self.run_meta(MetaCmd::Heartbeat {
            node: NodeRecord {
                id: self.id.clone(),
                addr: self.cfg.addr.clone(),
                started_at: self.started_ms as i64,
                heartbeat_at: now,
            },
            dead_after_ms: 3_600_000,
        })
        .await
        .map(|_| ())
    }

    pub fn addr_of(&self, node_id: &str) -> Option<String> {
        lock(&self.members).iter().find(|n| n.id == node_id).map(|n| n.addr.clone())
    }

    /// Returns the address of `node_id`. If `node_id` is not in the cached membership list,
    /// performs a rate-limited strong refresh of the membership table first.
    async fn addr_of_or_refresh(&self, node_id: &str) -> Option<String> {
        if let Some(a) = self.addr_of(node_id) {
            return Some(a);
        }
        let due = {
            let mut last = lock(&self.members_refreshed_on_demand);
            let due = last.is_none_or(|t| t.elapsed() >= MEMBERS_REFRESH_ON_DEMAND_MIN);
            if due {
                *last = Some(Instant::now());
            }
            due
        };
        if due {
            crate::probe::hit(crate::probe::Probe::MembersRefreshedForUnknownPeer);
            let _ = self.refresh_members_at(true).await;
        }
        self.addr_of(node_id)
    }

    /// Excludes `node_id` from routing with exponential backoff after an undelivered forward.
    pub fn mark_suspect(&self, node_id: &str) {
        let rec = lock(&self.members).iter().find(|n| n.id == node_id).cloned();
        let (started_at, heartbeat_at) = rec.map(|n| (n.started_at, n.heartbeat_at)).unwrap_or((0, i64::MIN));
        let mut g = lock(&self.suspects);
        let strikes = g.get(node_id).map_or(1, |s| s.strikes.saturating_add(1));
        let max = Duration::from_millis(3 * self.cfg.heartbeat_ms).max(SUSPECT_MIN);
        let backoff = SUSPECT_MIN.saturating_mul(1u32 << (strikes - 1).min(16)).min(max);
        crate::probe::hit(crate::probe::Probe::PeerSuspected);
        g.insert(node_id.to_string(), Suspect { started_at, heartbeat_at, strikes, until: Instant::now() + backoff });
    }

    fn is_suspect(&self, node_id: &str) -> bool {
        lock(&self.suspects).get(node_id).is_some_and(|s| Instant::now() < s.until)
    }

    /// Returns the `(node_id, addr)` of the peer that should handle a request for `db_id`, or
    /// `None` if this node should handle the request.
    ///
    /// Long-poll requests (`is_long_poll`) are always routed to the rendezvous home node, which
    /// receives all commit notices, so that waiting requests are still woken if another node
    /// acquires the lease. All other requests are routed to the lease holder if a lease holder is
    /// known, or to the rendezvous home node otherwise.
    pub async fn route(&self, db_id: &str, is_long_poll: bool) -> Option<(String, String)> {
        if crate::buggify::fault(0.02) {
            return None;
        }
        let db = self.engine.db(db_id);
        let _ = db.open(false).await;
        let target = match db.lease_holder().filter(|_| !is_long_poll) {
            Some(h) => h,
            None => {
                let live = self.live_members();
                cluster::rendezvous(db_id, &live)?.id.clone()
            }
        };
        if target == self.id {
            return None;
        }
        if self.is_suspect(&target) {
            self.env.trace(|| format!("{db_id}: {target} is suspect, handling locally"));
            return None;
        }
        let Some(addr) = self.addr_of_or_refresh(&target).await else {
            self.env.trace(|| format!("{db_id}: address for {target} is missing, handling locally"));
            return None;
        };
        if addr == self.cfg.addr {
            crate::probe::hit(crate::probe::Probe::OwnAddressIgnored);
            return None;
        }
        Some((target, addr))
    }

    /// Begins a snapshot read transaction on the system database.
    pub async fn sys_read(&self) -> Result<Txn> {
        let sys = self.engine.db(SYS_DB);
        sys.begin(Isolation::Serializable, Consistency::Snapshot { max_staleness_ms: self.cfg.sys_staleness_ms }).await
    }

    /// Begins a linearizable transaction on the system database.
    pub async fn sys_read_strong(&self) -> Result<Txn> {
        let sys = self.engine.db(SYS_DB);
        sys.begin(Isolation::Serializable, Consistency::Strong).await
    }

    /// Executes `cmd` on the system database.
    pub async fn run_meta(&self, cmd: MetaCmd) -> Result<()> {
        self.run_meta_json(cmd).await.map(|_| ())
    }

    /// Executes `cmd` on the system database and deserializes its result.
    pub async fn run_meta_as<T: DeserializeOwned>(&self, cmd: MetaCmd) -> Result<T> {
        let answer = self.run_meta_json(cmd).await?;
        serde_json::from_value(answer).map_err(|e| Error::internal(format!("a metadata command's answer: {e}")))
    }

    async fn run_meta_json(&self, cmd: MetaCmd) -> Result<Json> {
        with_deadline(self.meta_deadline(), self.run_meta_attempts(cmd)).await
    }

    async fn run_meta_attempts(&self, cmd: MetaCmd) -> Result<Json> {
        let sys = self.engine.db(SYS_DB);
        let deadline = self.meta_deadline();
        let mut last_err = Error::unavailable("system database unavailable");
        for attempt in 0..10u32 {
            if attempt > 0 {
                let d = 10 * (1u64 << attempt.min(7)) + self.env.random_below(20);
                tokio::time::sleep(Duration::from_millis(d)).await;
                if Instant::now() >= deadline {
                    break;
                }
            }
            if let Some(holder) = sys.lease_holder()
                && holder != self.id
                && let Some(addr) = self.addr_of(&holder).filter(|a| *a != self.cfg.addr)
            {
                crate::probe::hit(crate::probe::Probe::MetaForwarded);
                match self.forward_meta(&holder, &addr, &cmd, deadline).await {
                    Ok(r) => return Ok(r),
                    // Reload the cluster key if it was initialized by another node after we started.
                    Err(Ok(Error::PermissionDenied(m))) if m.contains("cluster key") => {
                        if self.cfg.cluster_secret.is_none()
                            && let Ok(mut t) = self.sys_read_strong().await
                            && let Ok(Some(k)) = meta::get_config(&mut t, "cluster_key").await
                        {
                            *lock(&self.cluster_key) = Some(k);
                        }
                        last_err = Error::unavailable("cluster key mismatch");
                        continue;
                    }
                    Err(Ok(e @ Error::Unavailable { outcome_unknown: false, .. })) => {
                        last_err = e;
                        continue;
                    }
                    Err(Ok(e)) => return Err(e),
                    Err(Err(te)) => {
                        if te.maybe_delivered {
                            return Err(Error::Unavailable {
                                message: format!("metadata update outcome unknown: {}", te.message),
                                retry_after_ms: Some(500),
                                outcome_unknown: true,
                            });
                        }
                        self.mark_suspect(&holder);
                    }
                }
            }
            match self.run_meta_local(&cmd).await {
                Ok(r) => return Ok(r),
                Err(e @ (Error::Conflict(_) | Error::NotOwner { .. })) => {
                    last_err = e;
                    let _ = sys.refresh(Instant::now(), false).await;
                }
                Err(e @ Error::Unavailable { outcome_unknown: false, .. }) => last_err = e,
                Err(e) => return Err(e),
            }
        }
        Err(last_err)
    }

    /// Executes `cmd` locally on this node.
    pub async fn run_meta_local(&self, cmd: &MetaCmd) -> Result<Json> {
        let sys = self.engine.db(SYS_DB);
        let mut t = sys.begin(Isolation::Serializable, Consistency::Strong).await?;
        let r = meta::apply(&mut t, &BlobResolver { db: &sys }, cmd, self.now_ms() as i64).await?;
        let mut req = t.into_request();
        req.deadline = Some(self.meta_deadline());
        sys.commit(req).await?;
        Ok(r)
    }

    fn meta_deadline(&self) -> Instant {
        REQUEST_DEADLINE
            .try_with(|d| *d)
            .unwrap_or_else(|_| Instant::now() + Duration::from_millis(self.cfg.engine.commit_timeout_ms))
    }

    async fn forward_meta(
        &self,
        holder: &str,
        addr: &str,
        cmd: &MetaCmd,
        deadline: Instant,
    ) -> std::result::Result<Json, std::result::Result<Error, cluster::TransportError>> {
        let mut req = self.internal_request("/internal/v1/meta", cmd).map_err(Ok)?;
        if let Ok(v) = self.env.random_hex(16).parse() {
            req.headers_mut().insert(cluster::FORWARD_ID_HEADER, v);
        }
        if let Ok(v) = holder.parse() {
            req.headers_mut().insert(cluster::FORWARD_TO_HEADER, v);
        }
        let remaining = deadline.saturating_duration_since(Instant::now()).as_millis() as u64;
        req.headers_mut().insert(DEADLINE_HEADER, (self.now_ms() + remaining).into());
        if let Some(rid) = crate::log::request_id()
            && let Ok(v) = rid.parse()
        {
            req.headers_mut().insert(crate::log::REQUEST_ID_HEADER, v);
        }
        let resp = self.services.transport.call(addr, req).await.map_err(Err)?;
        let status = resp.status();
        let bytes = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .map_err(|e| Err(cluster::TransportError { message: e.to_string(), maybe_delivered: true }))?
            .to_bytes();
        let j: Json = serde_json::from_slice(&bytes).unwrap_or(Json::Null);
        if status.is_success() { Ok(j) } else { Err(Ok(crate::api::error_from_json(status.as_u16(), &j))) }
    }

    /// Authenticates a bearer token or session cookie value and checks rate limits.
    pub async fn authenticate(&self, token: &str) -> Result<Principal> {
        self.authenticate_metered(token, Meter::Take).await
    }

    /// Authenticates `token` and applies rate limits according to `meter`.
    pub async fn authenticate_metered(&self, token: &str, meter: Meter) -> Result<Principal> {
        let p = self.principal(token).await?;
        let user_key = format!("user:{}", p.user.id);
        let lim = &self.cfg.limits;
        let key;
        let keys: &[(&str, Rate)] = match &p.cred {
            Credential::ApiKey { id, rate_limit, .. } => {
                key = format!("key:{id}");
                &[(&key, rate_limit.map_or(lim.api_key, |r| Rate::per_sec(r as f64))), (&user_key, lim.user)]
            }
            Credential::Session { .. } => &[(&user_key, lim.user)],
        };
        match meter {
            Meter::Take => self.limiter.take(keys, "requests")?,
            Meter::Borrow => self.limiter.borrow(keys, "requests")?,
            Meter::Exempt => {}
        }
        Ok(p)
    }

    async fn principal(&self, token: &str) -> Result<Principal> {
        let cache_key = auth::secret_hash(token);
        if let Some(p) = self.auth_cache.get(&cache_key).filter(|_| !crate::buggify::fires(0.1)) {
            return Ok(p);
        }
        let read_at = Instant::now();
        let p = match self.lookup_principal(token, &cache_key, false).await {
            Err(Error::Unauthenticated(_)) => self.lookup_principal(token, &cache_key, true).await?,
            other => other?,
        };
        if p.user.disabled {
            return Err(Error::PermissionDenied("this account is disabled".into()));
        }
        self.auth_cache.store(cache_key, Some(p.clone()), read_at);
        Ok(p)
    }

    async fn lookup_principal(&self, token: &str, cache_key: &str, strong: bool) -> Result<Principal> {
        let unauth = || Error::Unauthenticated("invalid or expired credentials".into());
        let mut t = self.sys_read_at(strong).await?;
        if let Some((id, secret)) = auth::parse_api_key(token) {
            let k: ApiKey = meta::get_api_key(&mut t, id).await?.ok_or_else(unauth)?;
            if !auth::verify_api_secret(secret, &k.secret_hash) {
                return Err(unauth());
            }
            let user = meta::get_user(&mut t, &k.user_id).await?.ok_or_else(unauth)?;
            let cred =
                Credential::ApiKey { id: k.id, db_id: k.db_id, read_only: k.read_only, rate_limit: k.rate_limit };
            return Ok(Principal { user, cred });
        }
        if !token.starts_with(auth::SESSION_PREFIX) {
            return Err(unauth());
        }
        let s = meta::get_session(&mut t, cache_key).await?.ok_or_else(unauth)?;
        let user = meta::get_user(&mut t, &s.user_id).await?.ok_or_else(unauth)?;
        if !s.is_live(&user, self.now_ms() as i64) {
            return Err(unauth());
        }
        Ok(Principal { user, cred: Credential::Session { token_hash: cache_key.to_string() } })
    }

    /// Evicts cached principals for `key_id`.
    pub fn forget_api_key(&self, key_id: &str) {
        self.auth_cache.retain(|_, p| !matches!(&p.cred, Credential::ApiKey { id, .. } if id == key_id));
    }

    /// Evicts all cached credentials, roles, and database names for `user_id`.
    pub fn forget_user(&self, user_id: &str) {
        self.auth_cache.retain(|_, p| p.user.id != user_id);
        self.role_cache.retain(|(_, u), _| u != user_id);
        self.name_cache.retain(|(owner, _), _| owner != user_id);
    }

    pub fn forget_credential(&self, token: &str) {
        self.auth_cache.remove(&auth::secret_hash(token));
    }

    fn forget_sessions(&self, user_id: &str, keep: Option<&str>) {
        self.auth_cache.retain(|_, p| match &p.cred {
            Credential::Session { token_hash } => p.user.id != user_id || Some(token_hash.as_str()) == keep,
            Credential::ApiKey { .. } => true,
        });
    }

    /// Computes a PBKDF2 password hash within bounded concurrency slots.
    pub async fn hash_password(&self, password: &str) -> Result<String> {
        let (env, password, iters) = (self.env.clone(), password.to_string(), self.cfg.pbkdf2_iterations);
        self.hashing(move || auth::hash_password(&env, &password, iters)).await
    }

    async fn hashing<T: Send + 'static>(&self, f: impl FnOnce() -> T + Send + 'static) -> Result<T> {
        let wait = Duration::from_millis(self.cfg.limits.hash_wait_ms);
        let busy = || Error::Unavailable {
            message: "too many sign-ins in progress; retry".into(),
            retry_after_ms: Some(1_000),
            outcome_unknown: false,
        };
        let _slot = match tokio::time::timeout(wait, self.hash_slots.acquire()).await {
            Ok(Ok(slot)) => slot,
            _ => return Err(busy()),
        };
        Ok(self.env.run_blocking(f).await)
    }

    fn limit_client(&self, client: &str, what: &str) -> Result<()> {
        self.limiter.take(&[(&format!("auth:{client}"), self.cfg.limits.auth_per_client)], what)
    }

    fn failures_of(email: &str) -> String {
        format!("login:{email}")
    }

    /// Authenticates an email and password against rate limits and failure backoff.
    pub async fn password_login(&self, client: &str, email: &str, password: &str) -> Result<User> {
        let lim = &self.cfg.limits;
        self.limit_client(client, "sign-in attempts")?;
        let bad = || Error::Unauthenticated("invalid email or password".into());
        let email = auth::normalize_email(email).map_err(|_| bad())?;
        let (account, from) = (Self::failures_of(&email), format!("login-from:{client}"));
        self.limiter.check_backoff(&[&account, &from], "failed sign-ins")?;
        let mut t = self.sys_read().await?;
        let mut user = meta::user_by_email(&mut t, &email).await?;
        if user.is_none() {
            // Read-your-writes for an account created moments ago elsewhere.
            let mut t = self.sys_read_strong().await?;
            user = meta::user_by_email(&mut t, &email).await?;
        }
        let ok = match user.as_ref().and_then(|u| u.password_hash.clone()) {
            Some(h) => {
                let password = password.to_string();
                self.hashing(move || auth::verify_password(&password, &h)).await?
            }
            None => {
                // Equalize timing for unknown accounts.
                self.hash_password(password).await?;
                false
            }
        };
        let Some(user) = user.filter(|_| ok) else {
            self.limiter.failed(&account, lim.account_free_failures, lim);
            self.limiter.failed(&from, lim.client_free_failures, lim);
            return Err(bad());
        };
        self.limiter.succeeded(&account);
        if user.disabled {
            return Err(Error::PermissionDenied("this account is disabled".into()));
        }
        Ok(user)
    }

    /// Verifies `user`'s current password if one is set.
    async fn reauthenticate(&self, user: &User, password: Option<&str>) -> Result<()> {
        let Some(hash) = user.password_hash.clone() else { return Ok(()) };
        let lim = &self.cfg.limits;
        let account = Self::failures_of(&user.email);
        self.limiter.check_backoff(&[&account], "failed sign-ins")?;
        let password = password.ok_or_else(|| Error::invalid("confirm with your current password"))?.to_string();
        if !self.hashing(move || auth::verify_password(&password, &hash)).await? {
            self.limiter.failed(&account, lim.account_free_failures, lim);
            return Err(Error::PermissionDenied("the current password is incorrect".into()));
        }
        self.limiter.succeeded(&account);
        Ok(())
    }

    /// Registers a new user with email and password and sends a verification email if configured.
    pub async fn password_signup(&self, client: &str, email: &str, password: &str) -> Result<User> {
        let lim = &self.cfg.limits;
        let (auth_key, signup_key) = (format!("auth:{client}"), format!("signup:{client}"));
        self.limiter.take(&[(&auth_key, lim.auth_per_client), (&signup_key, lim.signup_per_client)], "sign-ups")?;
        let email = auth::normalize_email(email)?;
        auth::validate_password(password)?;
        let hash = self.hash_password(password).await?;
        let r: SignedUp = self
            .run_meta_as(MetaCmd::Signup {
                id: self.env.random_hex(8),
                email,
                password_hash: Some(hash),
                policy: self.cfg.signup,
                bootstrap_admin: false,
            })
            .await?;
        self.env.crash_point().await;
        if let (true, Ok(mailer)) = (r.created, self.mailer()) {
            self.mail_verification(mailer, &r.user);
        }
        Ok(r.user)
    }

    /// Creates a session for `user_id` and returns its bearer token.
    pub async fn create_session(&self, user_id: &str, client: Option<&str>, method: SignInMethod) -> Result<String> {
        let (token, hash) = auth::new_session_token(&self.env);
        self.run_meta(MetaCmd::CreateSession {
            token_hash: hash,
            id: self.env.random_hex(8),
            user_id: user_id.to_string(),
            expires_at: (self.now_ms() + self.cfg.session_ttl_ms) as i64,
            client: client.map(|c| c.to_string()),
            method,
        })
        .await?;
        Ok(token)
    }

    /// Reads user `id` with strong consistency, returning `Unauthenticated` if not found.
    pub async fn user_strong(&self, id: &str) -> Result<User> {
        let mut t = self.sys_read_strong().await?;
        meta::get_user(&mut t, id).await?.ok_or_else(|| Error::Unauthenticated("this account no longer exists".into()))
    }

    /// Updates the signed-in user's password and revokes all other sessions.
    pub async fn change_password(&self, p: &Principal, current: Option<&str>, new: &str) -> Result<()> {
        let token_hash = p.session_hash()?;
        auth::validate_password(new)?;
        let user = self.user_strong(&p.user.id).await?;
        self.reauthenticate(&user, current).await?;
        let hash = self.hash_password(new).await?;
        self.run_meta(MetaCmd::SetPassword {
            user_id: user.id.clone(),
            password_hash: hash,
            expected: user.password_hash.clone(),
            keep_session: Some(token_hash.to_string()),
            verify_email: false,
        })
        .await?;
        self.env.crash_point().await;
        self.delete_ended_sessions(&user.id, Some(token_hash)).await;
        Ok(())
    }

    async fn delete_ended_sessions(&self, user_id: &str, keep: Option<&str>) {
        if let Err(e) = self.revoke_sessions(user_id, keep).await {
            crate::warn!("deleting sessions ended by a password change: {e}");
        }
    }

    /// Revokes all sessions for `user_id` except `keep`, returning the number of revoked sessions.
    pub async fn revoke_sessions(&self, user_id: &str, keep: Option<&str>) -> Result<usize> {
        self.forget_sessions(user_id, keep);
        let mut total = 0;
        loop {
            let cmd = MetaCmd::RevokeSessions {
                user_id: user_id.to_string(),
                except: keep.map(|k| k.to_string()),
                limit: META_BATCH,
            };
            let b: Batch = self.run_meta_as(cmd).await?;
            total += b.deleted;
            if !b.more {
                return Ok(total);
            }
        }
    }

    /// Revokes session `id` and returns whether it was the caller's current session.
    pub async fn revoke_session(&self, p: &Principal, id: &str) -> Result<bool> {
        let current = p.session_hash()?;
        let cmd = MetaCmd::RevokeSession { user_id: p.user.id.clone(), id: id.to_string() };
        let hash: String = self.run_meta_as(cmd).await?;
        self.auth_cache.remove(&hash);
        Ok(hash == current)
    }

    async fn delete_api_keys(&self, user_id: &str) -> Result<()> {
        loop {
            let cmd = MetaCmd::DeleteApiKeys { user_id: user_id.to_string(), limit: META_BATCH };
            if !self.run_meta_as::<Batch>(cmd).await?.more {
                return Ok(());
            }
        }
    }

    /// Deletes the signed-in user's account, credentials, memberships, and personal databases.
    pub async fn delete_account(&self, p: &Principal, password: Option<&str>) -> Result<()> {
        p.require_session()?;
        let user = self.user_strong(&p.user.id).await?;
        self.reauthenticate(&user, password).await?;
        let dbs = loop {
            match self.run_meta_as(MetaCmd::DeleteUser { user_id: user.id.clone() }).await? {
                UserDeletion::Deleted { dbs } => break dbs,
                UserDeletion::TooManyCredentials => {
                    self.revoke_sessions(&user.id, None).await?;
                    self.delete_api_keys(&user.id).await?;
                }
            }
        };
        // Mark each database manifest deleted so nodes with cached handles stop serving it immediately.
        for id in dbs {
            let db = self.engine.db(&id);
            if db.open(false).await.is_ok()
                && let Err(e) = db.mark_deleted().await
            {
                crate::warn!("flagging database {id} of a deleted account: {e}");
            }
            self.engine.forget(&id);
        }
        self.forget_user(&user.id);
        Ok(())
    }

    /// Formats an absolute public URL for `path`.
    pub fn link(&self, path: &str) -> String {
        format!("{}{path}", self.cfg.public_url.trim_end_matches('/'))
    }

    fn mailer(&self) -> Result<Arc<dyn Mailer>> {
        self.services.mailer.clone().ok_or_else(|| Error::not_found("this server is not configured to send email"))
    }

    fn send_mail(&self, mailer: Arc<dyn Mailer>, mail: Mail) {
        self.env.spawn(async move {
            for attempt in 0..5u32 {
                match mailer.send(&mail).await {
                    Ok(()) => return,
                    Err(e) => crate::warn!("sending mail (attempt {}): {e}", attempt + 1),
                }
                tokio::time::sleep(Duration::from_millis(1_000 << attempt)).await;
            }
        });
    }

    fn token_link(&self, path: &str, purpose: auth::TokenPurpose, user: &User, fp: &str, life: &LinkLife) -> String {
        let token = auth::sign_token(&self.cookie_key(), purpose, &user.id, fp, self.now_ms() + life.ms);
        self.link(&format!("{path}?token={token}"))
    }

    fn mail_verification(&self, mailer: Arc<dyn Mailer>, user: &User) {
        let link = self.token_link(
            "/console/verify",
            auth::TokenPurpose::Verify,
            user,
            &auth::fingerprint(&user.email),
            &VERIFY_LINK,
        );
        let text = format!(
            "Confirm that this is your address by opening\n\n{link}\n\nThe link is valid for {}.",
            VERIFY_LINK.words
        );
        let subject = "Verify your Baserunna email address".into();
        self.send_mail(mailer, Mail { to: user.email.clone(), subject, text });
    }

    /// Sends an email verification link to the signed-in user.
    pub async fn send_verification(&self, p: &Principal) -> Result<()> {
        p.require_session()?;
        let mailer = self.mailer()?;
        let user = self.user_strong(&p.user.id).await?;
        if user.email_verified {
            return Err(Error::invalid("this email address is already verified"));
        }
        let mail_key = format!("mail:{}", user.email);
        self.limiter.take(&[(&mail_key, self.cfg.limits.mail_per_account)], "emails")?;
        self.mail_verification(mailer, &user);
        Ok(())
    }

    async fn open_link(&self, client: &str, purpose: auth::TokenPurpose, token: &str) -> Result<(User, String)> {
        self.limit_client(client, "attempts")?;
        let bad = || Error::Unauthenticated("this link is invalid or has expired".into());
        let (user_id, fp) = auth::open_token(&self.cookie_key(), purpose, token, self.now_ms()).ok_or_else(bad)?;
        match self.user_strong(&user_id).await {
            Ok(user) => Ok((user, fp)),
            Err(e) if e.retryable() => Err(e),
            Err(_) => Err(bad()),
        }
    }

    /// Verifies an email address using a signed verification token.
    pub async fn verify_email(&self, client: &str, token: &str) -> Result<()> {
        let (user, fp) = self.open_link(client, auth::TokenPurpose::Verify, token).await?;
        if auth::fingerprint(&user.email) != fp {
            return Err(Error::Unauthenticated("this link is for an earlier email address".into()));
        }
        self.run_meta(MetaCmd::VerifyEmail { user_id: user.id.clone(), email: user.email }).await?;
        self.forget_user(&user.id);
        Ok(())
    }

    /// Sends a password reset email if an active account exists for `email`.
    pub async fn request_password_reset(&self, client: &str, email: &str) -> Result<()> {
        let mailer = self.mailer()?;
        let lim = &self.cfg.limits;
        self.limit_client(client, "attempts")?;
        let email = auth::normalize_email(email)?;
        self.limiter.take(&[(&format!("mail:{email}"), lim.mail_per_account)], "emails")?;
        let mut t = self.sys_read_strong().await?;
        let Some(user) = meta::user_by_email(&mut t, &email).await?.filter(|u| !u.disabled) else {
            return Ok(());
        };
        let link =
            self.token_link("/console/reset", auth::TokenPurpose::Reset, &user, &meta::password_fp(&user), &RESET_LINK);
        let text = format!(
            "Someone asked to reset the password of your Baserunna account. To choose a new one, open\n\n{link}\n\n\
             The link is valid for {} and sets one password. If you did not ask for it, ignore this email.",
            RESET_LINK.words
        );
        self.send_mail(mailer, Mail { to: email, subject: "Reset your Baserunna password".into(), text });
        Ok(())
    }

    /// Resets a user's password with a reset token, revokes existing sessions, and returns a new
    /// session. Repeating the reset with the same password succeeds idempotently.
    pub async fn reset_password(&self, client: &str, token: &str, password: &str) -> Result<(User, String)> {
        auth::validate_password(password)?;
        let (user, fp) = self.open_link(client, auth::TokenPurpose::Reset, token).await?;
        if user.disabled {
            return Err(Error::PermissionDenied("this account is disabled".into()));
        }
        if meta::password_fp(&user) != fp {
            return self.retry_password_reset(client, &user, password).await;
        }
        let hash = self.hash_password(password).await?;
        let set = MetaCmd::SetPassword {
            user_id: user.id.clone(),
            password_hash: hash,
            expected: user.password_hash.clone(),
            keep_session: None,
            verify_email: true,
        };
        match self.run_meta(set).await {
            Ok(_) => {}
            Err(Error::PreconditionFailed { .. }) => {
                let user = self.user_strong(&user.id).await?;
                return self.retry_password_reset(client, &user, password).await;
            }
            Err(e) => return Err(e),
        }
        self.env.crash_point().await;
        self.finish_reset(client, &user).await
    }

    /// Completes a retried password reset when the reset token was already applied with `password`.
    /// Enforces sign-in failure backoff before verifying `password`.
    async fn retry_password_reset(&self, client: &str, user: &User, password: &str) -> Result<(User, String)> {
        let used = || Error::Unauthenticated("this link has been used already".into());
        let (lim, account) = (&self.cfg.limits, Self::failures_of(&user.email));
        self.limiter.check_backoff(&[&account], "failed sign-ins")?;
        let Some(h) = user.password_hash.clone() else { return Err(used()) };
        let pw = password.to_string();
        if !self.hashing(move || auth::verify_password(&pw, &h)).await? {
            self.limiter.failed(&account, lim.account_free_failures, lim);
            return Err(used());
        }
        crate::probe::hit(crate::probe::Probe::PasswordResetFinished);
        self.finish_reset(client, user).await
    }

    async fn finish_reset(&self, client: &str, user: &User) -> Result<(User, String)> {
        self.forget_user(&user.id);
        self.delete_ended_sessions(&user.id, None).await;
        self.limiter.succeeded(&Self::failures_of(&user.email));
        let session = self.create_session(&user.id, Some(client), SignInMethod::Reset).await?;
        Ok((self.user_strong(&user.id).await?, session))
    }

    /// Resolves a database reference (`name`, `org.name`, or `id:<db_id>`) and the caller's access.
    pub async fn resolve_db(&self, p: &Principal, name: &str) -> Result<(DbRecord, Access)> {
        match self.resolve_db_with(p, name, false).await {
            Err(Error::NotFound(_)) => self.resolve_db_with(p, name, true).await,
            other => other,
        }
    }

    /// Resolves a database reference with strong consistency, bypassing caches.
    pub async fn resolve_db_strong(&self, p: &Principal, name: &str) -> Result<(DbRecord, Access)> {
        self.resolve_db_with(p, name, true).await
    }

    async fn resolve_db_with(&self, p: &Principal, name: &str, strong: bool) -> Result<(DbRecord, Access)> {
        let not_found = || Error::not_found(format!("database {name:?} not found"));
        let rec = match DbRef::parse(name).ok_or_else(not_found)? {
            DbRef::Id(id) => {
                p.require_admin()?;
                let mut t = self.sys_read_strong().await?;
                meta::get_db(&mut t, &id).await?.ok_or_else(|| Error::not_found("database not found"))?
            }
            DbRef::Personal(n) => self.lookup_db(&p.user.id, &n, strong).await?.ok_or_else(not_found)?,
            DbRef::Org { org, name: n } => {
                let o = self.lookup_org(&org, strong).await?.ok_or_else(not_found)?;
                self.lookup_db(&o.id, &n, strong).await?.ok_or_else(not_found)?
            }
        };
        let role = self.role_for(&p.user, &rec.owner_id, strong).await?.ok_or_else(not_found)?;
        if let Some(scope) = p.scoped_db()
            && scope != rec.id
        {
            return Err(Error::PermissionDenied("this API key is limited to another database".into()));
        }
        Ok((rec, Access { role, read_only_key: p.read_only() }))
    }

    /// Computes `p`'s access to `rec` with strong consistency, bypassing caches.
    pub async fn access_strong(&self, p: &Principal, rec: &DbRecord) -> Result<Access> {
        let user = self.user_strong(&p.user.id).await?;
        let role = self.role_for(&user, &rec.owner_id, true).await?;
        let role = role.ok_or_else(|| Error::not_found("database not found"))?;
        Ok(Access { role, read_only_key: p.read_only() })
    }

    async fn role_for(&self, user: &User, owner_id: &str, strong: bool) -> Result<Option<Role>> {
        if let Some(r) = Role::of_site_admin(user) {
            return Ok(Some(r));
        }
        if !meta::is_org_id(owner_id) {
            return Ok((owner_id == user.id).then_some(Role::Owner));
        }
        let key = (owner_id.to_string(), user.id.clone());
        if !strong && let Some(r) = self.role_cache.get(&key) {
            return Ok(Some(r));
        }
        let read_at = Instant::now();
        let mut t = self.sys_read_at(strong).await?;
        let role = meta::member_role(&mut t, owner_id, &user.id).await?;
        self.role_cache.store(key, role, read_at);
        Ok(role)
    }

    async fn sys_read_at(&self, strong: bool) -> Result<Txn> {
        if strong { self.sys_read_strong().await } else { self.sys_read().await }
    }

    async fn lookup_db(&self, owner_id: &str, name: &str, strong: bool) -> Result<Option<DbRecord>> {
        let key = (owner_id.to_string(), name.to_string());
        if !strong && let Some(rec) = self.name_cache.get(&key) {
            return Ok(Some(rec));
        }
        let read_at = Instant::now();
        let mut t = self.sys_read_at(strong).await?;
        let rec = meta::resolve_db(&mut t, owner_id, name).await?;
        self.name_cache.store(key, rec.clone(), read_at);
        Ok(rec)
    }

    async fn lookup_org(&self, name: &str, strong: bool) -> Result<Option<Org>> {
        if !strong && let Some(o) = self.org_cache.get(&name.to_string()) {
            return Ok(Some(o));
        }
        let read_at = Instant::now();
        let mut t = self.sys_read_at(strong).await?;
        let o = meta::org_by_name(&mut t, name).await?;
        self.org_cache.store(name.to_string(), o.clone(), read_at);
        Ok(o)
    }

    /// Resolves organization `name` and `p`'s role with strong consistency.
    pub async fn resolve_org(&self, p: &Principal, name: &str) -> Result<(Org, Role)> {
        let not_found = || Error::not_found(format!("organization {name:?} not found"));
        let o = self.lookup_org(name, true).await?.ok_or_else(not_found)?;
        let role = self.role_for(&p.user, &o.id, true).await?.ok_or_else(not_found)?;
        Ok((o, role))
    }

    pub fn forget_db_name(&self, owner_id: &str, name: &str) {
        self.name_cache.remove(&(owner_id.to_string(), name.to_string()));
    }

    pub fn forget_org(&self, name: &str) {
        self.org_cache.remove(&name.to_string());
    }

    pub fn forget_role(&self, org_id: &str, user_id: &str) {
        self.role_cache.remove(&(org_id.to_string(), user_id.to_string()));
    }

    /// Opens the engine handle for `rec`, creating its manifest only after a strong existence check.
    pub async fn open_db(&self, rec: &DbRecord) -> Result<Arc<Db>> {
        let db = self.engine.db(&rec.id);
        match db.open(false).await {
            Ok(_) => Ok(db),
            Err(Error::NotFound(_)) => {
                let mut t = self.sys_read_strong().await?;
                if meta::get_db(&mut t, &rec.id).await?.is_none() {
                    return Err(Error::not_found("database not found"));
                }
                db.open(true).await?;
                Ok(db)
            }
            Err(e) => Err(e),
        }
    }

    fn housekeeping(&self) {
        self.engine.evict_idle(self.cfg.db_idle_evict_ms);
        self.engine.shared.refresh_clock();
        self.txns.expire();
        self.auth_cache.prune();
        self.name_cache.prune();
        self.org_cache.prune();
        self.role_cache.prune();
        self.forwards.expire();
        self.limiter.prune(&self.cfg.limits);
    }

    /// Purges deleted databases past their grace period and deletes expired sessions
    /// if this node is selected by rendezvous hashing on `REAPER_KEY`.
    pub async fn reap_deleted(&self) -> Result<usize> {
        if cluster::rendezvous(REAPER_KEY, &self.live_members()).is_some_and(|n| n.id != self.id) {
            return Ok(0);
        }
        let Some(store_now) = self.engine.shared.store_now_ms() else {
            return Ok(0);
        };
        let now = self.now_ms() as i64;
        let sys = self.engine.db(SYS_DB);
        let stale_ok = Consistency::Snapshot { max_staleness_ms: 2 * self.cfg.members_refresh_ms };
        let mut t = sys.begin(Isolation::Serializable, stale_ok).await?;
        let mut n = 0;
        for d in meta::list_deleted_dbs(&mut t).await? {
            if d.deleted_at + (self.cfg.db_delete_grace_ms as i64) > store_now as i64 {
                continue;
            }
            self.engine.purge(&d.id).await?;
            self.env.crash_point().await;
            self.run_meta(MetaCmd::PurgePass { id: d.id.clone(), passes: d.purge_passes }).await?;
            n += 1;
        }
        if meta::any_session_expired(&mut t, now).await? {
            self.run_meta_as::<Batch>(MetaCmd::ExpireSessions { before_ms: now, limit: META_BATCH }).await?;
        }
        Ok(n)
    }

    /// Returns cluster time in milliseconds derived from the object store clock.
    pub fn now_ms(&self) -> u64 {
        self.engine.shared.time_ms()
    }

    pub fn status_json(&self) -> Json {
        json!({
            "node_id": self.id,
            "addr": self.cfg.addr,
            "open_databases": self.engine.open_count(),
            "open_transactions": self.txns.len(),
            "memory": {
                "used_bytes": self.engine.shared.memory.used(),
                "limit_bytes": self.engine.shared.memory.limit(),
            },
            "cache": {
                "bytes": self.engine.shared.cache.stats().bytes,
                "hits": self.engine.shared.cache.stats().hits,
                "misses": self.engine.shared.cache.stats().misses,
            },
        })
    }
}

/// Retries `f` with exponential backoff while it returns a retryable error.
pub async fn retry_unavailable<T, F, Fut>(mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut delay = 50;
    for _ in 0..12 {
        match f().await {
            Err(e) if e.retryable() => {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                delay = (delay * 2).min(2000);
            }
            other => return other,
        }
    }
    f().await
}

#[cfg(test)]
mod tests {
    use crate::engine::{Consistency, Isolation};
    use crate::sim::cost::{World, run_paused};
    use bytes::Bytes;
    use std::time::Duration;

    #[test]
    fn password_hashing_is_bounded() {
        run_paused(async {
            let w = World::new(1, 1).await;
            let n = w.node(0).clone();
            let slots = n.cfg.limits.hash_concurrency;
            let held = n.hash_slots.acquire_many(slots as u32).await.unwrap();
            let e = n.hash_password("password-1").await.unwrap_err();
            assert!(matches!(e, crate::error::Error::Unavailable { outcome_unknown: false, .. }), "{e:?}");
            let r = n.password_login("1.2.3.4", "nobody@x.io", "password-1").await.unwrap_err();
            assert!(matches!(r, crate::error::Error::Unavailable { .. }), "{r:?}");
            let (tx, waiting) = tokio::sync::oneshot::channel();
            let n2 = n.clone();
            n.env.spawn(async move {
                let _ = tx.send(n2.hash_password("password-1").await);
            });
            tokio::time::sleep(Duration::from_millis(n.cfg.limits.hash_wait_ms / 2)).await;
            drop(held);
            assert!(waiting.await.unwrap().unwrap().starts_with("pbkdf2-sha256$"));
        });
    }

    #[test]
    fn earlier_runs_at_this_address_are_not_routed_to() {
        run_paused(async {
            let w = World::new(1, 1).await;
            let a = w.node(0).clone();
            let ghost = super::NodeRecord {
                id: "earlier-run".into(),
                addr: a.cfg.addr.clone(),
                started_at: 1,
                heartbeat_at: a.now_ms() as i64,
            };
            a.run_meta(super::MetaCmd::Heartbeat { node: ghost.clone(), dead_after_ms: 3_600_000 }).await.unwrap();
            a.refresh_members().await.unwrap();
            assert!(a.members().iter().any(|m| m.id == ghost.id));
            assert!(a.live_members().iter().all(|m| m.id != ghost.id));
            let both = [a.members().into_iter().find(|m| m.id == a.id).unwrap(), ghost];
            let homed = (0..)
                .map(|i| format!("db{i}"))
                .find(|d| super::cluster::rendezvous(d, &both).unwrap().id == "earlier-run");
            assert_eq!(a.route(&homed.unwrap(), false).await, None);
        });
    }

    #[test]
    fn reset_link_is_idempotent_for_same_password_only() {
        run_paused(async {
            let w = World::new(1, 1).await;
            let n = w.node(0).clone();
            let user = n.password_signup("c", "reset@x.io", "password-1").await.unwrap();
            let fp = super::meta::password_fp(&user);
            let token = super::auth::sign_token(
                &n.cookie_key(),
                super::auth::TokenPurpose::Reset,
                &user.id,
                &fp,
                n.now_ms() + 60_000,
            );
            let (a, b) =
                tokio::join!(n.reset_password("c", &token, "password-2"), n.reset_password("c", &token, "password-2"));
            assert!(a.is_ok() && b.is_ok(), "{a:?} {b:?}");
            assert!(n.reset_password("c", &token, "password-3").await.is_err());
        });
    }

    #[test]
    fn restart_replaces_predecessor_with_skewed_clock() {
        run_paused(async {
            let w = World::new(1, 1).await;
            let a = w.node(0).clone();
            let now = a.now_ms() as i64;
            let at = |id: &str, started_at| super::NodeRecord {
                id: id.into(),
                addr: "http://elsewhere".into(),
                started_at,
                heartbeat_at: now,
            };
            for rec in [at("before", now + 3_600_000), at("after", now)] {
                a.run_meta(super::MetaCmd::Heartbeat { node: rec, dead_after_ms: 3_600_000 }).await.unwrap();
            }
            a.refresh_members().await.unwrap();
            let live = super::cluster::live(&a.members(), now, a.cfg.heartbeat_ms as i64);
            let there: Vec<&str> =
                live.iter().filter(|m| m.addr == "http://elsewhere").map(|m| m.id.as_str()).collect();
            assert_eq!(there, ["after"]);
        });
    }

    #[test]
    fn end_waits_wakes_active_and_future_long_polls() {
        run_paused(async {
            let w = World::new(1, 1).await;
            let t = w.signup(0, "waits@x.io").await;
            w.create_db(0, &t, "d").await;
            let feed = w.json(0, "GET", "/v1/dbs/d/changes?after=0", &t, serde_json::Value::Null).await;
            let cursor = feed["next_cursor"].as_str().unwrap().to_string();
            let waiting = |path: String| {
                let (router, t) = (w.node(0).router().unwrap(), t.clone());
                let (tx, rx) = tokio::sync::oneshot::channel();
                w.node(0).env.spawn(async move {
                    let started = tokio::time::Instant::now();
                    let r = crate::sim::cost::call_router(&router, "GET", &path, &t, Bytes::new()).await;
                    let _ = tx.send((r.status, started.elapsed()));
                });
                rx
            };
            let paths = [
                format!("/v1/dbs/d/changes?after={cursor}&wait_ms=50000"),
                format!("/v1/dbs/d/changes?after={cursor}&wait_ms=50000&consistency=snapshot&max_staleness_ms=300000"),
            ];
            let polls: Vec<_> = paths.iter().cloned().map(waiting).collect();
            tokio::time::sleep(Duration::from_secs(5)).await;
            w.node(0).end_waits();
            for p in polls {
                let (status, waited) = p.await.unwrap();
                assert_eq!(status, 200);
                assert!(waited < Duration::from_secs(6), "{waited:?}");
            }
            for path in paths {
                let (status, waited) = waiting(path).await.unwrap();
                assert_eq!(status, 200);
                assert!(waited < Duration::from_secs(1), "{waited:?}");
            }
        });
    }

    #[test]
    fn writes_routed_to_a_restarted_home_are_handled_locally() {
        run_paused(async {
            let mut w = World::new(2, 1).await;
            for i in 0..2 {
                w.node(i).refresh_members().await.unwrap();
            }
            let t = w.signup(0, "fwd@x.io").await;
            let id = w.create_db(0, &t, "d").await;
            assert_eq!(w.holder(&id), None, "setup: an unleased database is routed to its home");
            let live = w.node(0).live_members();
            let home_id = super::cluster::rendezvous(&id, &live).unwrap().id.clone();
            let home = (0..2).find(|&i| w.node(i).id == home_id).unwrap();
            let other = 1 - home;
            w.restart_node(home).await;
            let put = Bytes::from(serde_json::json!({"ops": [{"op": "put", "key": "k", "value": "v"}]}).to_string());
            let r = w.call(other, "POST", "/v1/dbs/d/txn", &t, put).await;
            assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
        });
    }

    #[test]
    fn streamed_writes_routed_to_a_restarted_home_are_retried_locally() {
        run_paused(async {
            let mut w = World::new(2, 1).await;
            for i in 0..2 {
                w.node(i).refresh_members().await.unwrap();
            }
            let t = w.signup(0, "fwd@x.io").await;
            let id = w.create_db(0, &t, "d").await;
            let live = w.node(0).live_members();
            let home_id = super::cluster::rendezvous(&id, &live).unwrap().id.clone();
            let home = (0..2).find(|&i| w.node(i).id == home_id).unwrap();
            let other = 1 - home;
            w.restart_node(home).await;
            let big = Bytes::from(vec![7u8; 2 * crate::api::BUFFER_FORWARD_MAX as usize]);
            let r = w.call(other, "PUT", "/v1/dbs/d/kv/big", &t, big.clone()).await;
            assert_eq!(r.status, 503, "{}", String::from_utf8_lossy(&r.body));
            assert_eq!(r.json()["error"]["details"]["outcome"], "not_applied", "{}", r.json());
            assert!(r.headers.keys().all(|h| !h.as_str().starts_with("x-baserunna-stale")), "{:?}", r.headers);
            let r = w.call(other, "PUT", "/v1/dbs/d/kv/big", &t, big).await;
            assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
        });
    }

    #[test]
    fn commit_notices_to_a_stalled_home_are_coalesced() {
        run_paused(async {
            let w = World::new(2, 1).await;
            for i in 0..2 {
                w.node(i).refresh_members().await.unwrap();
            }
            let t = w.signup(0, "notice@x.io").await;
            let id = w.create_db(0, &t, "d").await;
            let live = w.node(0).live_members();
            let home_id = super::cluster::rendezvous(&id, &live).unwrap().id.clone();
            let home = (0..2).find(|&i| w.node(i).id == home_id).unwrap();
            let r = w.call(0, "PUT", "/v1/dbs/d/kv/k", &t, Bytes::from_static(b"v")).await;
            assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
            tokio::time::sleep(Duration::from_secs(30)).await;
            let db = w.node(1 - home).engine.db(&id);
            let commit = || async {
                let mut txn = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                txn.put(crate::ops::user_key(b"k").unwrap(), crate::engine::Value::Inline(Bytes::new())).unwrap();
                db.commit(txn.into_request()).await.unwrap();
            };
            let notices = || w.peer_calls("/internal/v1/notice");
            commit().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            let before = notices();
            assert!(before > 0, "setup: a commit off the home sends a notice");
            w.stall_peer(home, true);
            for _ in 0..20 {
                commit().await;
            }
            assert_eq!(notices() - before, 1, "one notice is in flight per database");
            tokio::time::sleep(super::NOTICE_TIMEOUT + Duration::from_millis(100)).await;
            assert_eq!(notices() - before, 2, "a timed-out notice is replaced by the newest version");
            w.stall_peer(home, false);
            tokio::time::sleep(super::NOTICE_TIMEOUT * 2).await;
            assert_eq!(notices() - before, 2, "every commit was announced");
            commit().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(notices() - before, 3);
        });
    }

    #[test]
    fn limits_endpoint_returns_configured_limits() {
        run_paused(async {
            let world = World::new(1, 1).await;
            let node = world.node(0);
            let limits = world.json(0, "GET", "/v1/limits", "", serde_json::Value::Null).await;
            assert_eq!(limits["long_poll_max_ms"], crate::api::LONG_POLL_MAX_MS, "{limits}");
            assert_eq!(limits["txn_max_ops"], crate::ops::MAX_TXN_OPS);
            assert_eq!(limits["scan_limit_max"], crate::ops::MAX_SCAN_LIMIT);
            assert_eq!(limits["value_inline_max"], node.engine.shared.cfg.value_inline_max);
            assert_eq!(limits["txn_max_write_bytes"], node.engine.shared.cfg.txn_max_write_bytes);
            assert_eq!(limits["txn_max_read_bytes"], node.engine.shared.cfg.txn_max_read_bytes);
            assert_eq!(limits["txn_max_open_per_user"], node.cfg.txns_per_user);
            assert_eq!(limits["json_value_max"], node.cfg.json_value_max);
            assert_eq!(limits["json_body_max"], node.cfg.max_json_body);
            assert_eq!(limits["blob_stage_ttl_ms"], node.engine.shared.cfg.blob_stage_ttl_ms);
        });
    }

    #[test]
    fn long_poll_is_woken_after_home_node_acquires_lease() {
        run_paused(async {
            let world = World::new(2, 1).await;
            for i in 0..2 {
                world.node(i).refresh_members().await.unwrap();
            }
            let token = world.signup(0, "moves@x.io").await;
            let db_id = world.create_db(0, &token, "d").await;
            let live = world.node(0).live_members();
            let home_id = super::cluster::rendezvous(&db_id, &live).unwrap().id.clone();
            let home = (0..2).find(|&i| world.node(i).id == home_id).unwrap();
            let other = 1 - home;
            let commit = |i: usize, value: &'static [u8]| {
                let db = world.node(i).engine.db(&db_id);
                async move {
                    let mut txn = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                    txn.put(crate::ops::user_key(b"k").unwrap(), crate::engine::Value::Inline(Bytes::from(value)))
                        .unwrap();
                    db.commit(txn.into_request()).await.unwrap()
                }
            };
            world.json(other, "GET", "/v1/dbs/d/changes?after=0", &token, serde_json::Value::Null).await;
            tokio::time::sleep(Duration::from_secs(30)).await;
            commit(other, b"1").await;
            assert_eq!(world.holder(&db_id), Some(other), "other node holds the lease");
            let feed = world.json(other, "GET", "/v1/dbs/d/changes?after=0", &token, serde_json::Value::Null).await;
            let cursor = feed["next_cursor"].as_str().unwrap().to_string();
            let (router, poll_token) = (world.node(other).router().unwrap(), token.clone());
            let (tx, rx) = tokio::sync::oneshot::channel();
            world.node(other).env.spawn(async move {
                let path = format!(
                    "/v1/dbs/d/changes?after={cursor}&wait_ms=50000&consistency=snapshot&max_staleness_ms=300000"
                );
                let resp = crate::sim::cost::call_router(&router, "GET", &path, &poll_token, Bytes::new()).await;
                let _ = tx.send((resp.status, resp.body, tokio::time::Instant::now()));
            });
            tokio::time::sleep(Duration::from_secs(30)).await;
            commit(home, b"2").await;
            let committed_at = tokio::time::Instant::now();
            let (status, body, completed_at) = rx.await.unwrap();
            assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
            let page: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(page["changes"].as_array().map(Vec::len), Some(1), "{page}");
            assert!(
                completed_at - committed_at < Duration::from_secs(2),
                "long poll completed {:?} after commit",
                completed_at - committed_at
            );
        });
    }

    #[test]
    fn suspects_recover_and_shutdown_nodes_leave() {
        run_paused(async {
            let w = World::new(2, 1).await;
            let (a, b) = (w.node(0).clone(), w.node(1).clone());
            a.refresh_members().await.unwrap();
            let live = |n: &super::Node| n.live_members().into_iter().map(|m| m.id).collect::<Vec<_>>();
            assert!(live(&a).contains(&b.id));
            a.mark_suspect(&b.id);
            assert!(!live(&a).contains(&b.id));
            tokio::time::sleep(Duration::from_secs(11)).await;
            assert!(live(&a).contains(&b.id));
            a.mark_suspect(&b.id);
            tokio::time::sleep(Duration::from_secs(11)).await;
            assert!(!live(&a).contains(&b.id));
            for _ in 0..8 {
                a.mark_suspect(&b.id);
            }
            tokio::time::sleep(Duration::from_millis(a.cfg.heartbeat_ms + 2 * a.cfg.members_refresh_ms)).await;
            assert!(live(&a).contains(&b.id));
            b.shutdown().await;
            a.refresh_members().await.unwrap();
            assert!(a.members().iter().all(|m| m.id != b.id));
        });
    }
}
