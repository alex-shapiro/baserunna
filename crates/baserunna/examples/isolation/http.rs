//! HTTP client for the node's public API.

use bytes::Bytes;
use reqwest::Method;
use serde::Serialize;
use serde_json::{Value as Json, json};
use std::time::{Duration, Instant};

/// Node API client that sends requests to one node URL.
pub struct Api {
    http: reqwest::Client,
    base: String,
}

/// Response of a request that reached the node.
pub struct Reply {
    pub status: u16,
    pub body: Bytes,
}

impl Reply {
    pub fn json(&self) -> Json {
        serde_json::from_slice(&self.body).unwrap_or(Json::Null)
    }

    pub fn excerpt(&self) -> String {
        String::from_utf8_lossy(&self.body[..self.body.len().min(300)]).into_owned()
    }
}

/// Result of one timed request: its HTTP status, or the transport error when no status arrived.
#[derive(Clone, Debug, Serialize)]
pub struct RequestRecord {
    pub label: String,
    pub status: Option<u16>,
    pub elapsed_ms: f64,
    /// True if the workload expects this status from a request that the node completes.
    pub expected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// User that owns a database, with its session token.
#[derive(Clone)]
pub struct Owner {
    pub token: String,
    pub db: String,
}

impl Api {
    pub fn new(base: &str, timeout: Duration) -> Api {
        let http = reqwest::Client::builder().timeout(timeout).pool_max_idle_per_host(64).build().expect("http client");
        Api { http, base: base.trim_end_matches('/').to_string() }
    }

    pub async fn send(&self, method: Method, path: &str, token: &str, body: Bytes) -> Result<Reply, String> {
        let mut req = self.http.request(method, format!("{}{path}", self.base)).header("content-type", "application/json");
        if !token.is_empty() {
            req = req.bearer_auth(token);
        }
        let resp = req.body(body).send().await.map_err(|e| format!("{e:?}"))?;
        let status = resp.status().as_u16();
        let body = resp.bytes().await.map_err(|e| format!("{e:?}"))?;
        Ok(Reply { status, body })
    }

    /// Sends a request and fails unless its status is in `expect`.
    pub async fn expect(&self, method: Method, path: &str, token: &str, body: impl Into<Bytes>, expect: &[u16]) -> Result<Reply, String> {
        let r = self.send(method.clone(), path, token, body.into()).await.map_err(|e| format!("{method} {path}: {e}"))?;
        if !expect.contains(&r.status) {
            return Err(format!("{method} {path}: status {}: {}", r.status, r.excerpt()));
        }
        Ok(r)
    }

    /// Sends a request and records its status and elapsed time.
    pub async fn timed(&self, label: &str, method: Method, path: &str, token: &str, body: Bytes, ok: &[u16]) -> RequestRecord {
        let started = Instant::now();
        let r = self.send(method, path, token, body).await;
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        match r {
            Ok(r) => RequestRecord {
                label: label.into(),
                status: Some(r.status),
                elapsed_ms,
                expected: ok.contains(&r.status),
                error: (!ok.contains(&r.status)).then(|| r.excerpt()),
            },
            Err(e) => RequestRecord { label: label.into(), status: None, elapsed_ms, expected: false, error: Some(e) },
        }
    }

    pub async fn wait_healthy(&self, limit: Duration) -> Result<(), String> {
        let started = Instant::now();
        while started.elapsed() < limit {
            if let Ok(r) = self.send(Method::GET, "/healthz", "", Bytes::new()).await
                && r.status == 200
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Err(format!("{}/healthz did not return 200 within {limit:?}", self.base))
    }

    /// Signs up a user and creates its database `db`.
    pub async fn create_owner(&self, email: &str, db: &str) -> Result<Owner, String> {
        let body = json!({"email": email, "password": "password-123"}).to_string();
        let r = self.expect(Method::POST, "/v1/auth/signup", "", body, &[200, 201]).await?;
        let token = r.json()["token"].as_str().ok_or("signup returned no token")?.to_string();
        self.create_db(&token, db).await?;
        Ok(Owner { token, db: db.into() })
    }

    pub async fn create_db(&self, token: &str, db: &str) -> Result<(), String> {
        self.expect(Method::POST, "/v1/dbs", token, json!({"name": db}).to_string(), &[200, 201]).await.map(|_| ())
    }

    pub async fn create_event_table(&self, token: &str, db: &str, table: Json) -> Result<(), String> {
        self.expect(Method::POST, &format!("/v1/dbs/{db}/events"), token, table.to_string(), &[201]).await.map(|_| ())
    }

    pub async fn bulk_insert(&self, token: &str, db: &str, table: &str, body: impl Into<Bytes>) -> Result<(), String> {
        self.expect(Method::POST, &format!("/v1/dbs/{db}/events/{table}/rows"), token, body, &[201]).await.map(|_| ())
    }

    pub async fn table_info(&self, token: &str, db: &str, table: &str) -> Result<Json, String> {
        let r = self.expect(Method::GET, &format!("/v1/dbs/{db}/events/{table}"), token, Bytes::new(), &[200]).await?;
        Ok(r.json())
    }

    pub async fn part_count(&self, token: &str, db: &str, table: &str) -> Result<usize, String> {
        Ok(self.table_info(token, db, table).await?["parts"].as_array().map_or(0, Vec::len))
    }
}

/// Returns a JSON array of rows `row(0), row(1), ...` that is at most `max_bytes` long, and its row count.
pub fn json_rows(max_bytes: usize, row: impl Fn(usize) -> String) -> (String, usize) {
    let mut s = String::with_capacity(max_bytes);
    s.push('[');
    let mut n = 0;
    loop {
        let r = row(n);
        if s.len() + r.len() + 2 > max_bytes {
            break;
        }
        if n > 0 {
            s.push(',');
        }
        s.push_str(&r);
        n += 1;
    }
    s.push(']');
    (s, n)
}
