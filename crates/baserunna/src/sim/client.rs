//! HTTP client for simulation workloads.

use super::{NetError, Sim, SimResponse};
use crate::util::rng::Rng;
use axum::body::Body;
use bytes::Bytes;
use http::Request;
use serde_json::{Value as Json, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Upper bound on how long an in-flight server request can remain active.
pub const ZOMBIE_WAIT_MS: u64 = 30_000;

struct Backoff {
    attempts: u32,
    delay: u64,
}

impl Backoff {
    fn new() -> Backoff {
        Backoff { attempts: 0, delay: 50 }
    }

    async fn wait(&mut self, sim: &Sim) -> bool {
        if self.attempts > 0 {
            sim.sleep(self.delay).await;
            self.delay = (self.delay * 2).min(2_000);
        }
        self.attempts += 1;
        self.attempts <= 400
    }
}

pub struct Client {
    pub sim: Arc<Sim>,
    pub token: Option<String>,
    pub pin: Option<usize>,
    rng: Mutex<Rng>,
    pub name: String,
    /// Client IP address sent in `X-Forwarded-For`.
    pub addr: String,
    /// Whether to automatically retry 429 responses after `Retry-After`.
    pub retry_rate_limited: bool,
}

/// Result of a commit status query.
#[derive(Clone, Debug, PartialEq)]
pub enum Status {
    /// The commit was applied; contains the status response payload.
    Committed(Json),
    NotCommitted,
    /// The commit record retention window may have elapsed, so the outcome is indeterminate.
    Unknown,
}

impl Status {
    /// Returns the version at which the commit was applied, if committed.
    pub fn version(&self) -> Option<u64> {
        match self {
            Status::Committed(j) => j["version"].as_u64(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Resp {
    pub status: u16,
    pub json: Json,
    pub raw: SimResponse,
}

impl Resp {
    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }

    pub fn code(&self) -> &str {
        self.json["error"]["code"].as_str().unwrap_or("")
    }

    /// Returns `true` if the request failed with HTTP 503 and was not applied.
    pub fn retryable_not_applied(&self) -> bool {
        self.status == 503 && self.json["error"]["details"]["outcome"] != "unknown"
    }

    pub fn outcome_unknown(&self) -> bool {
        self.status == 503 && self.json["error"]["details"]["outcome"] == "unknown"
    }

    /// Returns the index of the failed precondition operation from an HTTP 412 response.
    pub fn failed_op(&self) -> Option<usize> {
        (self.status == 412).then(|| self.json["error"]["details"]["op_index"].as_u64())?.map(|i| i as usize)
    }
}

pub fn url_escape(b: &[u8]) -> String {
    let mut s = String::new();
    for &c in b {
        if c.is_ascii_alphanumeric() || b"-_.~".contains(&c) {
            s.push(c as char);
        } else {
            s.push_str(&format!("%{c:02X}"));
        }
    }
    s
}

impl Client {
    pub fn new(sim: &Arc<Sim>, name: &str) -> Client {
        let h = crate::util::sha256(name.as_bytes());
        Client {
            rng: Mutex::new(sim.fork_rng()),
            sim: sim.clone(),
            token: None,
            pin: None,
            name: name.to_string(),
            addr: format!("10.{}.{}.{}", h[0], h[1], h[2]),
            retry_rate_limited: true,
        }
    }

    pub fn with_token(sim: &Arc<Sim>, name: &str, token: &str) -> Client {
        let mut c = Client::new(sim, name);
        c.token = Some(token.to_string());
        c
    }

    fn pick(&self) -> Option<usize> {
        if let Some(p) = self.pin {
            return Some(p);
        }
        let up = self.sim.up_nodes();
        if up.is_empty() {
            return None;
        }
        let i = self.rng.lock().unwrap_or_else(|e| e.into_inner()).usize(up.len());
        Some(up[i])
    }

    pub async fn send(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, String)],
        body: Bytes,
    ) -> Result<SimResponse, NetError> {
        loop {
            let r = self.send_once(method, path, headers, body.clone()).await;
            match &r {
                Ok(resp) if resp.status == 429 && self.retry_rate_limited => {
                    let secs = resp.headers.get("retry-after").and_then(|v| v.to_str().ok()?.parse::<u64>().ok());
                    self.sim.sleep(1_000 * secs.unwrap_or(1).clamp(1, 60)).await;
                }
                _ => return r,
            }
        }
    }

    async fn send_once(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, String)],
        body: Bytes,
    ) -> Result<SimResponse, NetError> {
        let Some(i) = self.pick() else {
            self.sim.sleep(100).await;
            return Err(NetError::Refused);
        };
        let mut b = Request::builder().method(method).uri(path).header("x-forwarded-for", self.addr.as_str());
        if let Some(t) = &self.token {
            b = b.header("authorization", format!("Bearer {t}"));
        }
        for (k, v) in headers {
            b = b.header(*k, v.as_str());
        }
        let req = b.header("content-length", body.len().to_string()).body(Body::from(body)).expect("request");
        let r = self.sim.call_node(i, req).await;
        self.sim.trace.event(&format!(
            "t={} {} {method} {path} via n{i} -> {}",
            self.sim.now_ms() % 100_000,
            self.name,
            match &r {
                Ok(r) => format!(
                    "{} {}{}{}",
                    r.status,
                    crate::util::b64::hex(&crate::util::sha256(&r.body)[..6]),
                    match r.status >= 400 {
                        true => format!(" {}", String::from_utf8_lossy(&r.body[..r.body.len().min(160)])),
                        false => String::new(),
                    },
                    r.headers
                        .get("x-baserunna-version")
                        .and_then(|v| v.to_str().ok())
                        .map(|v| format!(" v{v}"))
                        .unwrap_or_default()
                ),
                Err(e) => format!("{e:?}"),
            }
        ));
        if r.is_ok() {
            super::openapi_check::record_coverage(method, path);
        }
        if let Ok(resp) = &r
            && let Err(e) = super::openapi_check::check_response(method, path, resp)
        {
            panic!(
                "OpenAPI conformance: {method} {path} -> {}: {e}\nbody: {}",
                resp.status,
                String::from_utf8_lossy(&resp.body)
            );
        }
        r
    }

    pub async fn call(&self, method: &str, path: &str, body: Option<Json>) -> Result<Resp, NetError> {
        let (bytes, headers) = match &body {
            Some(b) => (Bytes::from(b.to_string()), vec![("content-type", "application/json".to_string())]),
            None => (Bytes::new(), vec![]),
        };
        let raw = self.send(method, path, &headers, bytes).await?;
        let json =
            if raw.body.is_empty() { Json::Null } else { serde_json::from_slice(&raw.body).unwrap_or(Json::Null) };
        Ok(Resp { status: raw.status, json, raw })
    }

    /// Retries an idempotent request across network failures, 503s, and conflicts.
    pub async fn call_retry(&self, method: &str, path: &str, body: Option<Json>) -> Resp {
        let mut backoff = Backoff::new();
        while backoff.wait(&self.sim).await {
            match self.call(method, path, body.clone()).await {
                Ok(r) if r.status == 503 || (r.status == 409 && r.code() == "conflict") => {}
                Ok(r) => return r,
                Err(_) => {}
            }
        }
        panic!("{}: {method} {path} never succeeded", self.name);
    }

    /// Retries a request until it receives a definitive response, waiting out
    /// in-flight attempts after any ambiguous failure. The boolean indicates
    /// whether an earlier attempt may have already taken effect.
    pub async fn settle(&self, method: &str, path: &str, body: Option<Json>) -> Result<(Resp, bool), String> {
        let mut ambiguous = false;
        for _ in 0..400 {
            match self.call(method, path, body.clone()).await {
                Ok(r) if r.outcome_unknown() => ambiguous = true,
                Ok(r) if r.status == 503 || (r.status == 409 && r.code() == "conflict") => {}
                Ok(r) => return Ok((r, ambiguous)),
                Err(_) => ambiguous = true,
            }
            self.sim.sleep(if ambiguous { ZOMBIE_WAIT_MS } else { 100 }).await;
        }
        Err(format!("{}: {method} {path} never settled", self.name))
    }

    /// Like [`Client::settle`], discarding the ambiguity flag for idempotent requests.
    pub async fn call_settled(&self, method: &str, path: &str, body: Option<Json>) -> Resp {
        match self.settle(method, path, body).await {
            Ok((r, _)) => r,
            Err(e) => panic!("{e}"),
        }
    }

    /// Queries and fences the status of commit `id` (a commit ID or `key:<idempotency_key>`)
    /// in `db` that was first attempted at `began` (SPEC §6.2).
    pub async fn commit_status(&self, db: &str, id: &str, began: u64) -> Result<Status, String> {
        let r = self.call_retry("GET", &format!("/v1/dbs/{db}/commits/{id}"), None).await;
        match r.json["status"].as_str() {
            Some("committed") if r.json["version"].is_u64() => Ok(Status::Committed(r.json)),
            Some("not_committed") => {
                let kept = r.json["retention_ms"].as_u64().ok_or(format!("missing retention_ms: {}", r.json))?;
                let slack = 2 * self.sim.cfg.engine.clock_sync_max_error_ms;
                match self.sim.now_ms() + slack < began.saturating_add(kept) {
                    true => Ok(Status::NotCommitted),
                    false => Ok(Status::Unknown),
                }
            }
            _ => Err(format!("{}: commit status {id}: {} {}", self.name, r.status, r.json)),
        }
    }

    /// Fences commit `id` in `db` so that subsequent attempts cannot be applied (SPEC §6.2).
    pub async fn fence(&self, db: &str, id: &str) -> Result<(), String> {
        self.commit_status(db, id, 0).await.map(|_| ())
    }

    /// Determines whether an ambiguous commit `id` in `db` (first attempted at `began`)
    /// was applied. Checks the fenced commit status first, and falls back to reading
    /// `marker` if the commit retention window has elapsed.
    pub async fn resolve(&self, db: &str, id: &str, began: u64, marker: &str) -> Result<bool, String> {
        match self.commit_status(db, id, began).await? {
            Status::Committed(_) => Ok(true),
            Status::NotCommitted => Ok(false),
            Status::Unknown => {
                let r =
                    self.call_retry("GET", &format!("/v1/dbs/{db}/kv/{}", url_escape(marker.as_bytes())), None).await;
                match r.status {
                    200 => Ok(true),
                    404 => Ok(false),
                    s => Err(format!("{}: marker {marker}: {s} {}", self.name, r.json)),
                }
            }
        }
    }

    /// Returns `true` if a commit started at `began` is still within the server's commit retention window.
    pub fn is_commit_retained(&self, began: u64) -> bool {
        let e = &self.sim.cfg.engine;
        self.sim.now_ms() + 2 * e.clock_sync_max_error_ms < began.saturating_add(e.commit_retention_ms)
    }

    /// Returns `true` if a retry of a request started at `began` is guaranteed
    /// to fall within the server's commit retention window.
    pub fn dedups(&self, began: u64) -> bool {
        let e = &self.sim.cfg.engine;
        self.sim.now_ms() + ZOMBIE_WAIT_MS + 2 * e.clock_sync_max_error_ms < began.saturating_add(e.commit_retention_ms)
    }

    /// Collects all items in `field` across `next_cursor` pages of `path`.
    pub async fn paginate(&self, path: &str, field: &str) -> Result<Vec<Json>, String> {
        let sep = if path.contains('?') { '&' } else { '?' };
        let mut out = Vec::new();
        let mut page = path.to_string();
        for _ in 0..1000 {
            let r = self.call_retry("GET", &page, None).await;
            if r.status != 200 {
                return Err(format!("{}: GET {page}: {} {}", self.name, r.status, r.json));
            }
            out.extend(r.json[field].as_array().cloned().ok_or(format!("GET {page}: missing {field}: {}", r.json))?);
            match r.json["next_cursor"].as_str() {
                Some(c) => page = format!("{path}{sep}cursor={}", url_escape(c.as_bytes())),
                None => return Ok(out),
            }
        }
        Err(format!("{}: listing {path} did not terminate", self.name))
    }

    pub async fn get(&self, path: &str) -> Result<Resp, NetError> {
        self.call("GET", path, None).await
    }

    pub async fn post(&self, path: &str, body: Json) -> Result<Resp, NetError> {
        self.call("POST", path, Some(body)).await
    }

    /// Signs up a user and returns an authenticated client.
    pub async fn signup(sim: &Arc<Sim>, name: &str, email: &str, password: &str) -> Result<Client, String> {
        let c = Client::new(sim, name);
        let body = json!({"email": email, "password": password});
        let mut backoff = Backoff::new();
        while backoff.wait(sim).await {
            match c.post("/v1/auth/signup", body.clone()).await {
                Ok(r) if r.status == 201 => {
                    return Ok(Client::with_token(sim, name, r.json["token"].as_str().unwrap_or_default()));
                }
                Ok(r) if r.status == 409 && r.code() == "already_exists" => {
                    return Client::login(sim, name, email, password).await;
                }
                Ok(r) if r.status == 503 || r.status == 409 => {}
                Ok(r) => return Err(format!("signup failed: {} {}", r.status, r.json)),
                Err(_) => {}
            }
        }
        Err("signup never succeeded".into())
    }

    pub async fn login(sim: &Arc<Sim>, name: &str, email: &str, password: &str) -> Result<Client, String> {
        let c = Client::new(sim, name);
        let body = json!({"email": email, "password": password});
        let mut backoff = Backoff::new();
        while backoff.wait(sim).await {
            match c.post("/v1/auth/login", body.clone()).await {
                Ok(r) if r.status == 200 => {
                    return Ok(Client::with_token(sim, name, r.json["token"].as_str().unwrap_or_default()));
                }
                Ok(r) if r.status == 503 || r.status == 401 => {}
                Ok(r) => return Err(format!("login failed: {} {}", r.status, r.json)),
                Err(_) => {}
            }
        }
        Err("login never succeeded".into())
    }

    pub async fn create_db(&self, name: &str) -> Result<(), String> {
        self.create_db_owned(None, name).await
    }

    pub async fn create_db_owned(&self, org: Option<&str>, name: &str) -> Result<(), String> {
        let body = match org {
            Some(o) => json!({"name": name, "org": o}),
            None => json!({"name": name}),
        };
        let mut backoff = Backoff::new();
        while backoff.wait(&self.sim).await {
            match self.post("/v1/dbs", body.clone()).await {
                Ok(r) if r.status == 201 => return Ok(()),
                Ok(r) if r.status == 409 && r.code() == "already_exists" => return Ok(()),
                Ok(r) if r.outcome_unknown() => self.sim.sleep(ZOMBIE_WAIT_MS).await,
                Ok(r) if r.status == 503 || r.status == 409 => {}
                Ok(r) => return Err(format!("create db {body}: {} {}", r.status, r.json)),
                Err(_) => self.sim.sleep(ZOMBIE_WAIT_MS).await,
            }
        }
        Err(format!("create db {body} never succeeded"))
    }

    pub async fn pause(&self, ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    pub fn rng(&self) -> std::sync::MutexGuard<'_, Rng> {
        self.rng.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Sends the requests of a [`baserunna_client::Client`] through the simulated network.
impl baserunna_client::Transport for Client {
    fn send(
        &self,
        req: Request<Bytes>,
    ) -> baserunna_client::BoxFuture<'_, Result<http::Response<Bytes>, baserunna_client::TransportError>> {
        use baserunna_client::TransportError;
        Box::pin(async move {
            let (parts, body) = req.into_parts();
            let headers: Vec<(&str, String)> =
                parts.headers.iter().filter_map(|(k, v)| Some((k.as_str(), v.to_str().ok()?.to_string()))).collect();
            match Client::send(self, parts.method.as_str(), &parts.uri.to_string(), &headers, body).await {
                Ok(r) => {
                    let mut out = http::Response::new(r.body);
                    *out.status_mut() = http::StatusCode::from_u16(r.status)
                        .map_err(|e| TransportError::Unknown(format!("bad status: {e}")))?;
                    *out.headers_mut() = r.headers;
                    Ok(out)
                }
                Err(NetError::Refused) => Err(TransportError::NotSent("node unreachable".into())),
                Err(NetError::Lost) => Err(TransportError::Unknown("response lost".into())),
                Err(NetError::Timeout) => Err(TransportError::TimedOut("request timed out".into())),
            }
        })
    }
}
