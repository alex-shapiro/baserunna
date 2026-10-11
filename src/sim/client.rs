//! HTTP client used by simulation workloads. Requests go through the full
//! axum router of a (randomly chosen or pinned) node; responses are traced
//! and checked against the OpenAPI document.

use super::{NetError, Sim, SimResponse};
use crate::util::rng::Rng;
use axum::body::Body;
use bytes::Bytes;
use http::Request;
use serde_json::{Value as Json, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Longer than any server-side request can linger (commit deadline plus
/// store latency).
pub const ZOMBIE_WAIT_MS: u64 = 30_000;

pub struct Client {
    pub sim: Arc<Sim>,
    pub token: Option<String>,
    pub pin: Option<usize>,
    rng: Mutex<Rng>,
    pub name: String,
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

    /// 503 whose outcome is known not to have been applied.
    pub fn retryable_not_applied(&self) -> bool {
        self.status == 503 && self.json["error"]["details"]["outcome"] != "unknown"
    }

    pub fn outcome_unknown(&self) -> bool {
        self.status == 503 && self.json["error"]["details"]["outcome"] == "unknown"
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
        Client { rng: Mutex::new(sim.fork_rng()), sim: sim.clone(), token: None, pin: None, name: name.to_string() }
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
        let Some(i) = self.pick() else {
            self.sim.sleep(100).await;
            return Err(NetError::Refused);
        };
        let mut b = Request::builder().method(method).uri(path);
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
                    "{} {}{}",
                    r.status,
                    crate::util::b64::hex(&crate::util::sha256(&r.body)[..6]),
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

    /// Call until the network delivers a definitive answer (retrying
    /// refused connections and retryable errors known not to be applied).
    /// Only for idempotent requests.
    pub async fn call_retry(&self, method: &str, path: &str, body: Option<Json>) -> Resp {
        let mut delay = 50;
        for _ in 0..400 {
            match self.call(method, path, body.clone()).await {
                Ok(r) if r.status == 503 || (r.status == 409 && r.code() == "conflict") => {}
                Ok(r) => return r,
                Err(_) => {}
            }
            self.sim.sleep(delay).await;
            delay = (delay * 2).min(2_000);
        }
        panic!("{}: {method} {path} never succeeded", self.name);
    }

    /// Like [`Client::call_retry`], but if any attempt had an ambiguous
    /// outcome, wait for zombie requests to expire and repeat the request
    /// so that the returned answer reflects the final state. Only for
    /// idempotent requests.
    pub async fn call_settled(&self, method: &str, path: &str, body: Option<Json>) -> Resp {
        let mut ambiguous = false;
        for _ in 0..400 {
            match self.call(method, path, body.clone()).await {
                Ok(r) if r.outcome_unknown() => ambiguous = true,
                Ok(r) if r.status == 503 || (r.status == 409 && r.code() == "conflict") => {}
                Ok(r) if ambiguous => {
                    ambiguous = false;
                    let _ = r;
                    self.sim.sleep(ZOMBIE_WAIT_MS).await;
                    continue;
                }
                Ok(r) => return r,
                Err(_) => ambiguous = true,
            }
            if ambiguous {
                self.sim.sleep(ZOMBIE_WAIT_MS).await;
            } else {
                self.sim.sleep(100).await;
            }
        }
        panic!("{}: {method} {path} never settled", self.name);
    }

    pub async fn get(&self, path: &str) -> Result<Resp, NetError> {
        self.call("GET", path, None).await
    }

    pub async fn post(&self, path: &str, body: Json) -> Result<Resp, NetError> {
        self.call("POST", path, Some(body)).await
    }

    /// Sign up (retrying transient failures) and return a client holding
    /// the session token.
    pub async fn signup(sim: &Arc<Sim>, name: &str, email: &str, password: &str) -> Result<Client, String> {
        let c = Client::new(sim, name);
        let body = json!({"email": email, "password": password});
        for _ in 0..200 {
            match c.post("/v1/auth/signup", body.clone()).await {
                Ok(r) if r.status == 201 => {
                    return Ok(Client::with_token(sim, name, r.json["token"].as_str().unwrap_or_default()));
                }
                // An earlier attempt may have succeeded.
                Ok(r) if r.status == 409 => return Client::login(sim, name, email, password).await,
                Ok(r) if r.status == 503 || r.status == 409 => {}
                Ok(r) => return Err(format!("signup failed: {} {}", r.status, r.json)),
                Err(_) => {}
            }
            sim.sleep(200).await;
        }
        Err("signup never succeeded".into())
    }

    pub async fn login(sim: &Arc<Sim>, name: &str, email: &str, password: &str) -> Result<Client, String> {
        let c = Client::new(sim, name);
        let body = json!({"email": email, "password": password});
        for _ in 0..200 {
            match c.post("/v1/auth/login", body.clone()).await {
                Ok(r) if r.status == 200 => {
                    return Ok(Client::with_token(sim, name, r.json["token"].as_str().unwrap_or_default()));
                }
                Ok(r) if r.status == 503 || r.status == 401 => {}
                Ok(r) => return Err(format!("login failed: {} {}", r.status, r.json)),
                Err(_) => {}
            }
            sim.sleep(200).await;
        }
        Err("login never succeeded".into())
    }

    /// Create a database (idempotent from the caller's point of view).
    pub async fn create_db(&self, name: &str) -> Result<(), String> {
        for _ in 0..200 {
            match self.post("/v1/dbs", json!({"name": name})).await {
                Ok(r) if r.status == 201 => return Ok(()),
                Ok(r) if r.status == 409 && r.code() == "already_exists" => return Ok(()),
                Ok(r) if r.status == 503 || r.status == 409 => {}
                Ok(r) => return Err(format!("create db failed: {} {}", r.status, r.json)),
                Err(_) => {}
            }
            self.sim.sleep(200).await;
        }
        Err("create db never succeeded".into())
    }

    pub async fn pause(&self, ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    pub fn rng(&self) -> std::sync::MutexGuard<'_, Rng> {
        self.rng.lock().unwrap_or_else(|e| e.into_inner())
    }
}
