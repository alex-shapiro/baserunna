//! Async Rust client for the Baserunna HTTP API.

use axum::body::Body;
use bytes::Bytes;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::fmt;

#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: u16,
    pub code: String,
    pub message: String,
    pub body: Value,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HTTP {} {}: {}", self.status, self.code, self.message)
    }
}

impl std::error::Error for ApiError {}

impl ApiError {
    pub fn retryable(&self) -> bool {
        self.body["error"]["retryable"] == true
    }
}

#[derive(Clone)]
pub struct Client {
    base: String,
    token: Option<String>,
    http: hyper_util::client::legacy::Client<hyper_util::client::legacy::connect::HttpConnector, Body>,
}

/// Extracts the retry duration from a 429 response using `details.retry_after_ms`
/// or the `Retry-After` header.
fn retry_after(h: &http::HeaderMap, body: &Bytes) -> std::time::Duration {
    let v: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let ms = v["error"]["details"]["retry_after_ms"].as_u64().or_else(|| {
        let secs = h.get(http::header::RETRY_AFTER)?.to_str().ok()?.parse::<u64>().ok()?;
        Some(secs * 1000)
    });
    std::time::Duration::from_millis(ms.unwrap_or(1000).max(1))
}

/// Percent-encodes a URL path segment.
pub fn seg(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

impl Client {
    pub fn new(base: &str) -> Client {
        Client {
            base: base.trim_end_matches('/').to_string(),
            token: None,
            http: hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build(hyper_util::client::legacy::connect::HttpConnector::new()),
        }
    }

    /// Returns a copy of the client that authenticates with `token`.
    pub fn with_token(&self, token: &str) -> Client {
        Client { token: Some(token.to_string()), ..self.clone() }
    }

    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// Sends a request and returns the status, headers, and body bytes.
    /// Rate-limited (429) requests are automatically retried up to 60 seconds.
    pub async fn raw(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: Bytes,
    ) -> Result<(u16, http::HeaderMap, Bytes), ApiError> {
        let mut waited = std::time::Duration::ZERO;
        loop {
            let r = self.send(method, path, headers, body.clone()).await?;
            let wait = match &r {
                (429, h, b) => retry_after(h, b),
                _ => return Ok(r),
            };
            if waited + wait > std::time::Duration::from_secs(60) {
                return Ok(r);
            }
            tokio::time::sleep(wait).await;
            waited += wait;
        }
    }

    async fn send(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: Bytes,
    ) -> Result<(u16, http::HeaderMap, Bytes), ApiError> {
        let mut b = http::Request::builder().method(method).uri(format!("{}{}", self.base, path));
        if let Some(t) = &self.token {
            b = b.header("authorization", format!("Bearer {t}"));
        }
        for (k, v) in headers {
            b = b.header(*k, *v);
        }
        let req = b.body(Body::from(body)).map_err(|e| net(e.to_string()))?;
        let resp = self.http.request(req).await.map_err(|e| net(e.to_string()))?;
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        let bytes = resp.into_body().collect().await.map_err(|e| net(e.to_string()))?.to_bytes();
        Ok((status, headers, bytes))
    }

    /// Sends a JSON request, returning an [`ApiError`] on non-2xx responses.
    pub async fn call(&self, method: &str, path: &str, body: Option<Value>) -> Result<Value, ApiError> {
        let bytes = body.map(|b| Bytes::from(b.to_string())).unwrap_or_default();
        let (status, _, out) = self.raw(method, path, &[("content-type", "application/json")], bytes).await?;
        let v: Value = if out.is_empty() { Value::Null } else { serde_json::from_slice(&out).unwrap_or(Value::Null) };
        if (200..300).contains(&status) {
            Ok(v)
        } else {
            Err(ApiError {
                status,
                code: v["error"]["code"].as_str().unwrap_or("").to_string(),
                message: v["error"]["message"].as_str().unwrap_or("").to_string(),
                body: v,
            })
        }
    }

    pub async fn get(&self, path: &str) -> Result<Value, ApiError> {
        self.call("GET", path, None).await
    }

    pub async fn post(&self, path: &str, body: Value) -> Result<Value, ApiError> {
        self.call("POST", path, Some(body)).await
    }

    pub async fn delete(&self, path: &str) -> Result<Value, ApiError> {
        self.call("DELETE", path, None).await
    }

    pub async fn signup(&self, email: &str, password: &str) -> Result<Client, ApiError> {
        let r = self.post("/v1/auth/signup", json!({"email": email, "password": password})).await?;
        Ok(self.with_token(r["token"].as_str().unwrap_or_default()))
    }

    pub async fn login(&self, email: &str, password: &str) -> Result<Client, ApiError> {
        let r = self.post("/v1/auth/login", json!({"email": email, "password": password})).await?;
        Ok(self.with_token(r["token"].as_str().unwrap_or_default()))
    }

    pub async fn create_db(&self, name: &str) -> Result<Value, ApiError> {
        self.post("/v1/dbs", json!({"name": name})).await
    }

    /// Runs a one-shot transaction.
    pub async fn txn(&self, db: &str, ops: Value) -> Result<Value, ApiError> {
        self.post(&format!("/v1/dbs/{}/txn", seg(db)), json!({"ops": ops})).await
    }

    pub async fn put_bytes(&self, db: &str, key: &str, data: Bytes, content_type: &str) -> Result<u64, ApiError> {
        let (st, _, out) = self
            .raw("PUT", &format!("/v1/dbs/{}/kv/{}", seg(db), seg(key)), &[("content-type", content_type)], data)
            .await?;
        let v: Value = serde_json::from_slice(&out).unwrap_or(Value::Null);
        if st == 200 { Ok(v["version"].as_u64().unwrap_or(0)) } else { Err(api_err(st, v)) }
    }

    /// Reads the raw bytes of `key`, or `None` if not found.
    pub async fn get_bytes(&self, db: &str, key: &str, range: Option<&str>) -> Result<Option<Bytes>, ApiError> {
        let h: Vec<(&str, &str)> = range.map(|r| vec![("range", r)]).unwrap_or_default();
        let (st, _, out) = self.raw("GET", &format!("/v1/dbs/{}/kv/{}", seg(db), seg(key)), &h, Bytes::new()).await?;
        match st {
            200 | 206 => Ok(Some(out)),
            404 => Ok(None),
            _ => Err(api_err(st, serde_json::from_slice(&out).unwrap_or(Value::Null))),
        }
    }

    /// Reads a page of KV changes after `after`, waiting up to `wait_ms` for new changes.
    pub async fn changes(&self, db: &str, after: &str, wait_ms: u64) -> Result<Value, ApiError> {
        self.get(&format!("/v1/dbs/{}/changes?after={}&wait_ms={wait_ms}", seg(db), seg(after))).await
    }

    pub async fn stage_blob(&self, db: &str, data: Bytes, content_type: &str) -> Result<Value, ApiError> {
        let (st, _, out) =
            self.raw("POST", &format!("/v1/dbs/{}/blobs", seg(db)), &[("content-type", content_type)], data).await?;
        let v: Value = serde_json::from_slice(&out).unwrap_or(Value::Null);
        if st == 201 { Ok(v) } else { Err(api_err(st, v)) }
    }
}

fn net(m: String) -> ApiError {
    ApiError { status: 0, code: "network".into(), message: m, body: Value::Null }
}

fn api_err(status: u16, v: Value) -> ApiError {
    ApiError {
        status,
        code: v["error"]["code"].as_str().unwrap_or("").to_string(),
        message: v["error"]["message"].as_str().unwrap_or("").to_string(),
        body: v,
    }
}
