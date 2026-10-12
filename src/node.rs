//! A service node: engine + metadata + auth + routing + background work.

use crate::auth::{self, OidcProvider};
use crate::cluster::{self, CLUSTER_KEY_HEADER, Transport};
use crate::engine::{Consistency, Db, Engine, EngineConfig, Isolation, Txn};
use crate::env::Env;
use crate::error::{Error, Result};
use crate::meta::{self, ApiKey, DbRecord, MetaCmd, NodeRecord, SYS_DB, SignupPolicy, User};
use crate::store::DynStore;
use crate::table::BlobResolver;
use crate::util::DetHashMap;
use axum::body::Body;
use http::Request;
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
    /// Base URL browsers use (OAuth redirects, cookies).
    pub public_url: String,
    pub cluster_secret: Option<String>,
    pub signup: SignupPolicy,
    pub admin_email: Option<String>,
    pub admin_password: Option<String>,
    pub pbkdf2_iterations: u32,
    pub session_ttl_ms: u64,
    /// How often a node rewrites its membership record. A node whose record
    /// is older than three intervals is dead.
    pub heartbeat_ms: u64,
    /// How often a node re-reads the membership table.
    pub members_refresh_ms: u64,
    pub auth_cache_ms: u64,
    pub sys_staleness_ms: u64,
    pub db_delete_grace_ms: u64,
    pub max_dbs_per_user: usize,
    /// Rows per transaction when backfilling a new index.
    pub index_backfill_batch_rows: usize,
    pub json_value_max: u64,
    pub max_json_body: usize,
    pub txn_idle_ms: u64,
    pub txn_max_ms: u64,
    pub db_idle_evict_ms: u64,
    pub secure_cookies: bool,
    pub txn_retries: u32,
    pub engine: EngineConfig,
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
            max_dbs_per_user: 1000,
            index_backfill_batch_rows: 1000,
            json_value_max: 1024 * 1024,
            max_json_body: 32 * 1024 * 1024,
            txn_idle_ms: 30_000,
            txn_max_ms: 5 * 60_000,
            db_idle_evict_ms: 5 * 60_000,
            secure_cookies: false,
            txn_retries: 4,
            engine: EngineConfig::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Credential {
    Session { token_hash: String },
    ApiKey { id: String, db_id: Option<String>, read_only: bool },
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
        if !self.is_session() {
            return Err(Error::PermissionDenied("this operation requires a signed-in session".into()));
        }
        Ok(())
    }

    pub fn require_admin(&self) -> Result<()> {
        if !self.user.is_admin {
            return Err(Error::PermissionDenied("administrator access required".into()));
        }
        Ok(())
    }
}

/// An interactive transaction held on its database's owner node.
pub struct OpenTxn {
    pub db_id: String,
    pub user_id: String,
    pub txn: Option<Txn>,
    pub created: Instant,
    pub last_used: Instant,
    pub encoding: crate::ops::Encoding,
}

pub struct Node {
    pub id: String,
    pub cfg: NodeConfig,
    pub env: Arc<Env>,
    pub engine: Arc<Engine>,
    pub transport: Arc<dyn Transport>,
    pub oidc: Option<Arc<dyn OidcProvider>>,
    members: Mutex<Vec<NodeRecord>>,
    suspects: Mutex<DetHashMap<String, Suspect>>,
    auth_cache: Mutex<DetHashMap<String, (Instant, Principal)>>,
    name_cache: Mutex<DetHashMap<(String, String), (Instant, DbRecord)>>,
    pub(crate) txns: Mutex<DetHashMap<String, Arc<tokio::sync::Mutex<OpenTxn>>>>,
    cookie_key: Mutex<Vec<u8>>,
    /// Shared secret for internal endpoints: the configured
    /// `cluster_secret`, else a key generated once per cluster and stored in
    /// the system database (every node reads the same store).
    cluster_key: Mutex<Option<String>>,
    pub forwards: cluster::ForwardDedup,
    pub started_ms: u64,
    router: OnceLock<axum::Router>,
}

/// A peer that a forwarded request certainly did not reach.
struct Suspect {
    /// Its membership record when it was last suspected.
    started_at: i64,
    heartbeat_at: i64,
    strikes: u32,
    /// It stays out of routing until then, unless it heartbeats first.
    until: Instant,
}

/// Suspicion after a first failed forward; it doubles with each further
/// failure, up to the membership liveness window.
const SUSPECT_MIN: Duration = Duration::from_secs(10);

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Node {
    /// Start a node: bootstrap the system database, register, and spawn
    /// background work.
    pub async fn start(
        cfg: NodeConfig,
        env: Arc<Env>,
        store: DynStore,
        transport: Arc<dyn Transport>,
        oidc: Option<Arc<dyn OidcProvider>>,
    ) -> Result<Arc<Node>> {
        cfg.engine.validate().map_err(|e| Error::invalid(format!("engine configuration: {e}")))?;
        let id = format!("{}-{}", cfg.name, env.random_hex(4));
        let engine = Engine::new(store, env.clone(), cfg.engine.clone(), id.clone());
        let node = Arc::new(Node {
            id,
            started_ms: engine.shared.time_ms(),
            env,
            engine,
            transport,
            oidc,
            members: Mutex::new(Vec::new()),
            suspects: Mutex::new(DetHashMap::default()),
            auth_cache: Mutex::new(DetHashMap::default()),
            name_cache: Mutex::new(DetHashMap::default()),
            txns: Mutex::new(DetHashMap::default()),
            cookie_key: Mutex::new(Vec::new()),
            cluster_key: Mutex::new(None),
            forwards: cluster::ForwardDedup::new(Duration::from_secs(60)),
            router: OnceLock::new(),
            cfg,
        });
        node.bootstrap().await?;
        node.spawn_background();
        Ok(node)
    }

    async fn bootstrap(self: &Arc<Self>) -> Result<()> {
        if let Err(e) = self.engine.shared.store_time_ms().await {
            crate::warn!("{}: could not measure the store clock yet: {e}", self.id);
        }
        let sys = self.engine.db(SYS_DB);
        retry_unavailable(|| sys.open(true)).await?;
        // Members and the cluster key first, so that metadata commands can be
        // forwarded to the system database's lease holder.
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
        if self.cfg.cluster_secret.is_none() {
            let k = retry_unavailable(|| {
                self.run_meta(MetaCmd::EnsureConfig { key: "cluster_key".into(), value: self.env.random_hex(32) })
            })
            .await?;
            *lock(&self.cluster_key) = k["value"].as_str().map(|s| s.to_string());
        }
        let key = retry_unavailable(|| {
            self.run_meta(MetaCmd::EnsureConfig { key: "cookie_key".into(), value: self.env.random_hex(32) })
        })
        .await?;
        let key = key["value"].as_str().unwrap_or_default().to_string();
        *lock(&self.cookie_key) = crate::util::b64::unhex(&key).unwrap_or_else(|| key.into_bytes());
        if let (Some(e), Some(p)) = (&self.cfg.admin_email, &self.cfg.admin_password) {
            let email = auth::normalize_email(e)?;
            let mut t = self.sys_read().await?;
            if meta::user_by_email(&mut t, &email).await?.is_none() {
                let hash = auth::hash_password(&self.env, p, self.cfg.pbkdf2_iterations);
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
                tokio::time::sleep(Duration::from_millis(60_000)).await;
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

    /// The node's own API router (used by the console to call the API).
    pub fn router(&self) -> Option<axum::Router> {
        self.router.get().cloned()
    }

    /// The secret internal endpoints require (`None` until bootstrapped).
    pub fn cluster_key(&self) -> Option<String> {
        lock(&self.cluster_key).clone()
    }

    pub fn cookie_key(&self) -> Vec<u8> {
        lock(&self.cookie_key).clone()
    }

    /// Give up leases, leave the membership table and stop background work.
    pub async fn shutdown(&self) {
        for db in self.engine.open_dbs() {
            let _ = tokio::time::timeout(Duration::from_secs(2), db.release_lease()).await;
        }
        let leave = self.run_meta(MetaCmd::Leave { id: self.id.clone() });
        let _ = tokio::time::timeout(Duration::from_secs(2), leave).await;
        self.env.shutdown();
    }

    // ------------------------------------------------------------ members

    pub fn members(&self) -> Vec<NodeRecord> {
        lock(&self.members).clone()
    }

    /// Members with a recent heartbeat that no failed forward has made
    /// suspect.
    pub fn live_members(&self) -> Vec<NodeRecord> {
        let now = self.now_ms() as i64;
        cluster::live(&self.members(), now, self.cfg.heartbeat_ms as i64)
            .into_iter()
            .filter(|n| n.id == self.id || !self.is_suspect(&n.id))
            .collect()
    }

    pub async fn refresh_members(&self) -> Result<()> {
        let mut t = self.sys_read().await?;
        let nodes = meta::list_nodes(&mut t).await?;
        // A suspect that heartbeated since (or restarted, or left) is
        // forgiven.
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

    /// Keep `node_id` out of routing after a request certainly failed to
    /// reach it: until membership shows a newer heartbeat from it, or a
    /// backoff passes that starts at 10 s and doubles with every further
    /// failure, up to the liveness window.
    pub fn mark_suspect(&self, node_id: &str) {
        let rec = lock(&self.members).iter().find(|n| n.id == node_id).cloned();
        let (started_at, heartbeat_at) = rec.map(|n| (n.started_at, n.heartbeat_at)).unwrap_or((0, i64::MIN));
        let mut g = lock(&self.suspects);
        let strikes = g.get(node_id).map_or(1, |s| s.strikes.saturating_add(1));
        let max = Duration::from_millis(3 * self.cfg.heartbeat_ms).max(SUSPECT_MIN);
        let backoff = SUSPECT_MIN.saturating_mul(1u32 << (strikes - 1).min(16)).min(max);
        g.insert(node_id.to_string(), Suspect { started_at, heartbeat_at, strikes, until: Instant::now() + backoff });
    }

    fn is_suspect(&self, node_id: &str) -> bool {
        lock(&self.suspects).get(node_id).is_some_and(|s| Instant::now() < s.until)
    }

    /// Where requests for `db_id` should go: `None` = handle here.
    pub async fn route(&self, db_id: &str) -> Option<(String, String)> {
        let db = self.engine.db(db_id);
        let _ = db.open(false).await;
        let target = match db.lease_holder() {
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
            return None;
        }
        let addr = self.addr_of(&target)?;
        Some((target, addr))
    }

    // ------------------------------------------------------- system db

    /// A snapshot transaction on the system database.
    pub async fn sys_read(&self) -> Result<Txn> {
        let sys = self.engine.db(SYS_DB);
        sys.begin(Isolation::Serializable, Consistency::Snapshot { max_staleness_ms: self.cfg.sys_staleness_ms }).await
    }

    /// A linearizable transaction on the system database (used after a
    /// snapshot miss, so clients always see their own metadata writes).
    pub async fn sys_read_strong(&self) -> Result<Txn> {
        let sys = self.engine.db(SYS_DB);
        sys.begin(Isolation::Serializable, Consistency::Strong).await
    }

    /// Run a metadata command on the system database's lease holder.
    pub async fn run_meta(&self, cmd: MetaCmd) -> Result<Json> {
        let sys = self.engine.db(SYS_DB);
        let mut last_err = Error::unavailable("system database unavailable");
        for attempt in 0..10u32 {
            if attempt > 0 {
                let d = 10 * (1u64 << attempt.min(7)) + self.env.random_below(20);
                tokio::time::sleep(Duration::from_millis(d)).await;
            }
            if let Some(holder) = sys.lease_holder()
                && holder != self.id
                && let Some(addr) = self.addr_of(&holder)
            {
                match self.forward_meta(&holder, &addr, &cmd).await {
                    Ok(r) => return Ok(r),
                    // A peer rejected our cluster key: it was created after
                    // we started. Reload it and retry.
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
                    // Not applied there (e.g. the holder restarted): retry.
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

    /// Execute a metadata command here (no forwarding).
    pub async fn run_meta_local(&self, cmd: &MetaCmd) -> Result<Json> {
        let sys = self.engine.db(SYS_DB);
        let mut t = sys.begin(Isolation::Serializable, Consistency::Strong).await?;
        let r = meta::apply(&mut t, &BlobResolver { db: &sys }, cmd, self.now_ms() as i64).await?;
        sys.commit(t.into_request()).await?;
        Ok(r)
    }

    async fn forward_meta(
        &self,
        holder: &str,
        addr: &str,
        cmd: &MetaCmd,
    ) -> std::result::Result<Json, std::result::Result<Error, cluster::TransportError>> {
        let body = serde_json::to_vec(cmd).map_err(|e| Ok(Error::internal(e.to_string())))?;
        let mut req = Request::post("/internal/v1/meta")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .map_err(|e| Ok(Error::internal(e.to_string())))?;
        if let Some(k) = self.cluster_key()
            && let Ok(v) = k.parse()
        {
            req.headers_mut().insert(CLUSTER_KEY_HEADER, v);
        }
        if let Ok(v) = self.env.random_hex(16).parse() {
            req.headers_mut().insert(cluster::FORWARD_ID_HEADER, v);
        }
        if let Ok(v) = holder.parse() {
            req.headers_mut().insert(cluster::FORWARD_TO_HEADER, v);
        }
        let resp = self.transport.call(addr, req).await.map_err(Err)?;
        let status = resp.status();
        let bytes = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .map_err(|e| Err(cluster::TransportError { message: e.to_string(), maybe_delivered: true }))?
            .to_bytes();
        let j: Json = serde_json::from_slice(&bytes).unwrap_or(Json::Null);
        if status.is_success() { Ok(j) } else { Err(Ok(crate::api::error_from_json(status.as_u16(), &j))) }
    }

    // ------------------------------------------------------------- auth

    /// Authenticate a bearer token or session cookie value.
    pub async fn authenticate(&self, token: &str) -> Result<Principal> {
        let cache_key = auth::secret_hash(token);
        if let Some((t, p)) = lock(&self.auth_cache).get(&cache_key)
            && t.elapsed() < Duration::from_millis(self.cfg.auth_cache_ms)
        {
            return Ok(p.clone());
        }
        let p = match self.lookup_principal(token, &cache_key, false).await {
            Err(Error::Unauthenticated(_)) => self.lookup_principal(token, &cache_key, true).await?,
            other => other?,
        };
        if p.user.disabled {
            return Err(Error::PermissionDenied("this account is disabled".into()));
        }
        let mut c = lock(&self.auth_cache);
        if c.len() > 100_000 {
            c.clear();
        }
        c.insert(cache_key, (Instant::now(), p.clone()));
        Ok(p)
    }

    async fn lookup_principal(&self, token: &str, cache_key: &str, strong: bool) -> Result<Principal> {
        let unauth = || Error::Unauthenticated("invalid or expired credentials".into());
        let mut t = if strong { self.sys_read_strong().await? } else { self.sys_read().await? };
        let p = if let Some((id, secret)) = auth::parse_api_key(token) {
            let k: ApiKey = meta::get_api_key(&mut t, id).await?.ok_or_else(unauth)?;
            if !auth::verify_api_secret(secret, &k.secret_hash) {
                return Err(unauth());
            }
            let user = meta::get_user(&mut t, &k.user_id).await?.ok_or_else(unauth)?;
            Principal { user, cred: Credential::ApiKey { id: k.id, db_id: k.db_id, read_only: k.read_only } }
        } else if token.starts_with(auth::SESSION_PREFIX) {
            let s = meta::get_session(&mut t, cache_key).await?.ok_or_else(unauth)?;
            if s.expires_at < self.now_ms() as i64 {
                return Err(unauth());
            }
            let user = meta::get_user(&mut t, &s.user_id).await?.ok_or_else(unauth)?;
            Principal { user, cred: Credential::Session { token_hash: cache_key.to_string() } }
        } else {
            return Err(unauth());
        };
        Ok(p)
    }

    /// Drop cached principals for an API key (revocation takes effect on
    /// this node immediately; other nodes within the cache TTL).
    pub fn forget_api_key(&self, key_id: &str) {
        lock(&self.auth_cache).retain(|_, (_, p)| !matches!(&p.cred, Credential::ApiKey { id, .. } if id == key_id));
    }

    /// Drop cached principals of a user (e.g. after disabling them).
    pub fn forget_user(&self, user_id: &str) {
        lock(&self.auth_cache).retain(|_, (_, p)| p.user.id != user_id);
    }

    pub fn forget_credential(&self, token: &str) {
        lock(&self.auth_cache).remove(&auth::secret_hash(token));
    }

    /// Create a session for a user, returning the token.
    pub async fn create_session(&self, user_id: &str) -> Result<String> {
        let (token, hash) = auth::new_session_token(&self.env);
        self.run_meta(MetaCmd::CreateSession {
            token_hash: hash,
            user_id: user_id.to_string(),
            expires_at: (self.now_ms() + self.cfg.session_ttl_ms) as i64,
        })
        .await?;
        Ok(token)
    }

    /// Resolve a database by name (or `id:<db_id>` for admins) for a
    /// principal, enforcing ownership and API key scope.
    pub async fn resolve_db(&self, p: &Principal, name: &str) -> Result<DbRecord> {
        let rec = if let Some(id) = name.strip_prefix("id:") {
            p.require_admin()?;
            let mut t = self.sys_read_strong().await?;
            meta::get_db(&mut t, id).await?.ok_or_else(|| Error::not_found("database not found"))?
        } else {
            let key = (p.user.id.clone(), name.to_string());
            let cached = lock(&self.name_cache)
                .get(&key)
                .filter(|(t, _)| t.elapsed() < Duration::from_millis(self.cfg.auth_cache_ms))
                .map(|(_, r)| r.clone());
            match cached {
                Some(r) => r,
                None => {
                    let mut t = self.sys_read().await?;
                    let r = match meta::resolve_db(&mut t, &p.user.id, name).await? {
                        Some(r) => r,
                        None => {
                            let mut t = self.sys_read_strong().await?;
                            meta::resolve_db(&mut t, &p.user.id, name)
                                .await?
                                .ok_or_else(|| Error::not_found(format!("database {name:?} not found")))?
                        }
                    };
                    lock(&self.name_cache).insert(key, (Instant::now(), r.clone()));
                    r
                }
            }
        };
        if rec.owner_id != p.user.id && !p.user.is_admin {
            return Err(Error::not_found(format!("database {name:?} not found")));
        }
        if let Some(scope) = p.scoped_db()
            && scope != rec.id
        {
            return Err(Error::PermissionDenied("this API key is limited to another database".into()));
        }
        Ok(rec)
    }

    pub fn forget_db_name(&self, owner_id: &str, name: &str) {
        lock(&self.name_cache).remove(&(owner_id.to_string(), name.to_string()));
    }

    /// The engine handle for a user database. Its manifest is created
    /// lazily, but only after confirming (strongly) that the database
    /// exists, so stale caches can never resurrect a purged database.
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

    /// Resolve a database name bypassing caches (linearizable).
    pub async fn resolve_db_strong(&self, p: &Principal, name: &str) -> Result<DbRecord> {
        self.forget_db_name(&p.user.id, name);
        let mut t = self.sys_read_strong().await?;
        let rec = meta::resolve_db(&mut t, &p.user.id, name)
            .await?
            .ok_or_else(|| Error::not_found(format!("database {name:?} not found")))?;
        lock(&self.name_cache).insert((p.user.id.clone(), name.to_string()), (Instant::now(), rec.clone()));
        Ok(rec)
    }

    // ------------------------------------------------- background work

    fn housekeeping(&self) {
        self.engine.evict_idle(self.cfg.db_idle_evict_ms);
        self.engine.shared.refresh_clock();
        let idle = Duration::from_millis(self.cfg.txn_idle_ms);
        let max = Duration::from_millis(self.cfg.txn_max_ms);
        lock(&self.txns).retain(|_, t| match t.try_lock() {
            Ok(t) => t.last_used.elapsed() < idle && t.created.elapsed() < max,
            Err(_) => true,
        });
        let ttl = Duration::from_millis(self.cfg.auth_cache_ms);
        lock(&self.auth_cache).retain(|_, (t, _)| t.elapsed() < ttl);
        lock(&self.name_cache).retain(|_, (t, _)| t.elapsed() < ttl);
        self.forwards.expire();
    }

    /// Purge deleted databases whose grace period has passed. Runs on the
    /// node that requests for the system database go to, and only with a
    /// store clock measurement (deletion times are store time).
    pub async fn reap_deleted(&self) -> Result<usize> {
        if self.route(SYS_DB).await.is_some() {
            return Ok(0);
        }
        let Some(store_now) = self.engine.shared.store_now_ms() else {
            return Ok(0);
        };
        let now = self.now_ms() as i64;
        // Minutes of staleness are harmless against a grace period of
        // minutes, and membership refreshes keep this snapshot validated.
        let sys = self.engine.db(SYS_DB);
        let stale_ok = Consistency::Snapshot { max_staleness_ms: 2 * self.cfg.members_refresh_ms };
        let mut t = sys.begin(Isolation::Serializable, stale_ok).await?;
        let mut n = 0;
        for d in meta::list_deleted_dbs(&mut t).await? {
            if d.deleted_at + (self.cfg.db_delete_grace_ms as i64) > store_now as i64 {
                continue;
            }
            self.engine.purge(&d.id).await?;
            self.run_meta(MetaCmd::PurgePass { id: d.id.clone(), passes: d.purge_passes }).await?;
            n += 1;
        }
        // Expired sessions (bounded work per pass).
        if meta::any_session_expired(&mut t, now).await? {
            self.run_meta(MetaCmd::ExpireSessions { before_ms: now, limit: 500 }).await?;
        }
        Ok(n)
    }

    /// Cluster time for metadata: the store's clock as this node measured
    /// it, so that timestamps written by one node and compared by another
    /// (heartbeats, session expiry, deadlines, deletions) do not depend on
    /// node clock skew. This node's wall clock until the first measurement.
    pub fn now_ms(&self) -> u64 {
        self.engine.shared.time_ms()
    }

    pub fn open_txn_count(&self) -> usize {
        lock(&self.txns).len()
    }

    pub fn status_json(&self) -> Json {
        json!({
            "node_id": self.id,
            "addr": self.cfg.addr,
            "open_databases": self.engine.open_count(),
            "open_transactions": self.open_txn_count(),
            "cache": {
                "bytes": self.engine.shared.cache.stats().bytes,
                "hits": self.engine.shared.cache.stats().hits,
                "misses": self.engine.shared.cache.stats().misses,
            },
        })
    }
}

/// Retry an operation while it reports a retryable error.
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
    use crate::sim::cost::{World, run_paused};
    use std::time::Duration;

    /// A peer that a forward failed to reach leaves routing until it
    /// heartbeats again, or for a backoff that grows with each failure;
    /// a node that shuts down leaves the membership table.
    #[test]
    fn suspects_come_back_and_leavers_go() {
        run_paused(async {
            let w = World::new(2, 1).await;
            let (a, b) = (w.node(0).clone(), w.node(1).clone());
            a.refresh_members().await.unwrap();
            let live = |n: &super::Node| n.live_members().into_iter().map(|m| m.id).collect::<Vec<_>>();
            assert!(live(&a).contains(&b.id));
            a.mark_suspect(&b.id);
            assert!(!live(&a).contains(&b.id), "a suspect is not routed to");
            tokio::time::sleep(Duration::from_secs(11)).await;
            assert!(live(&a).contains(&b.id), "the first suspicion lasts 10 s");
            a.mark_suspect(&b.id);
            tokio::time::sleep(Duration::from_secs(11)).await;
            assert!(!live(&a).contains(&b.id), "repeated failures back off longer");
            for _ in 0..8 {
                a.mark_suspect(&b.id);
            }
            // The backoff is now the liveness window, but b's next
            // heartbeat ends it.
            tokio::time::sleep(Duration::from_millis(a.cfg.heartbeat_ms + 2 * a.cfg.members_refresh_ms)).await;
            assert!(live(&a).contains(&b.id), "a new heartbeat forgives a suspect");
            b.shutdown().await;
            a.refresh_members().await.unwrap();
            assert!(a.members().iter().all(|m| m.id != b.id), "a node that shut down left the membership");
        });
    }
}
