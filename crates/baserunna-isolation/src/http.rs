//! HTTP client for the node's public API.

use bytes::Bytes;
use reqwest::Method;
use serde::Serialize;
use serde_json::{Value as Json, json};
use std::time::{Duration, Instant};

/// Maximum time that [`NodeApi::send_expect`] retries a request.
pub const SETUP_RETRY_LIMIT: Duration = Duration::from_secs(60);

/// HTTP client bound to a single node URL.
pub struct NodeApi {
    http: reqwest::Client,
    base_url: String,
}

/// HTTP response returned by the node.
pub struct HttpReply {
    pub status: u16,
    pub body: Bytes,
}

impl HttpReply {
    pub fn json(&self) -> Json {
        serde_json::from_slice(&self.body).unwrap_or(Json::Null)
    }

    pub fn excerpt(&self) -> String {
        String::from_utf8_lossy(&self.body[..self.body.len().min(300)]).into_owned()
    }
}

/// Timing and outcome of a request, including its HTTP status or transport error.
#[derive(Clone, Debug, Serialize)]
pub struct RequestRecord {
    pub label: String,
    pub status: Option<u16>,
    pub elapsed_ms: f64,
    /// True if the response status is in the expected status list.
    pub expected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Authenticated database owner with a bearer token and database name.
#[derive(Clone)]
pub struct Owner {
    pub token: String,
    pub db: String,
}

impl NodeApi {
    pub fn new(base_url: &str, timeout: Duration) -> NodeApi {
        let http = reqwest::Client::builder().timeout(timeout).pool_max_idle_per_host(64).build().expect("http client");
        NodeApi { http, base_url: base_url.trim_end_matches('/').to_string() }
    }

    pub async fn send(&self, method: Method, path: &str, token: &str, body: Bytes) -> Result<HttpReply, String> {
        let mut request =
            self.http.request(method, format!("{}{path}", self.base_url)).header("content-type", "application/json");
        if !token.is_empty() {
            request = request.bearer_auth(token);
        }
        let response = request.body(body).send().await.map_err(|err| format!("{err:?}"))?;
        let status = response.status().as_u16();
        let body = response.bytes().await.map_err(|err| format!("{err:?}"))?;
        Ok(HttpReply { status, body })
    }

    /// Sends a request and requires the response status to match `expected_statuses`.
    ///
    /// Retries any error response that the node marks retryable, such as HTTP 503 or HTTP 409.
    /// Uses exponential backoff up to [`SETUP_RETRY_LIMIT`].
    pub async fn send_expect(
        &self,
        method: Method,
        path: &str,
        token: &str,
        body: impl Into<Bytes>,
        expected_statuses: &[u16],
    ) -> Result<HttpReply, String> {
        let body = body.into();
        let started = Instant::now();
        let mut backoff = Duration::from_millis(100);
        loop {
            let reply = self
                .send(method.clone(), path, token, body.clone())
                .await
                .map_err(|err| format!("{method} {path}: {err}"))?;
            if expected_statuses.contains(&reply.status) {
                return Ok(reply);
            }
            let retryable = reply.json()["error"]["retryable"] == json!(true);
            if !retryable || started.elapsed() + backoff > SETUP_RETRY_LIMIT {
                return Err(format!("{method} {path}: status {}: {}", reply.status, reply.excerpt()));
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(5));
        }
    }

    /// Sends a request and records its status and elapsed time.
    pub async fn send_timed(
        &self,
        label: &str,
        method: Method,
        path: &str,
        token: &str,
        body: Bytes,
        expected_statuses: &[u16],
    ) -> RequestRecord {
        let started = Instant::now();
        let result = self.send(method, path, token, body).await;
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        match result {
            Ok(reply) => RequestRecord {
                label: label.into(),
                status: Some(reply.status),
                elapsed_ms,
                expected: expected_statuses.contains(&reply.status),
                error: (!expected_statuses.contains(&reply.status)).then(|| reply.excerpt()),
            },
            Err(err) => {
                RequestRecord { label: label.into(), status: None, elapsed_ms, expected: false, error: Some(err) }
            }
        }
    }

    /// Sends a request and discards its response body in chunks. Returns the request record and
    /// total response bytes.
    ///
    /// Reads chunks as they arrive so the client does not buffer large responses in memory.
    pub async fn send_timed_drain(
        &self,
        label: &str,
        method: Method,
        path: &str,
        token: &str,
        body: Bytes,
        expected_statuses: &[u16],
    ) -> (RequestRecord, u64) {
        let started = Instant::now();
        let mut request =
            self.http.request(method, format!("{}{path}", self.base_url)).header("content-type", "application/json");
        if !token.is_empty() {
            request = request.bearer_auth(token);
        }
        let build_record = |status: Option<u16>, error: Option<String>| RequestRecord {
            label: label.into(),
            status,
            elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
            expected: error.is_none() && status.is_some_and(|code| expected_statuses.contains(&code)),
            error,
        };
        let mut response = match request.body(body).send().await {
            Ok(response) => response,
            Err(err) => return (build_record(None, Some(format!("{err:?}"))), 0),
        };
        let status = response.status().as_u16();
        let (mut total_bytes, mut excerpt) = (0u64, Vec::new());
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    total_bytes += chunk.len() as u64;
                    excerpt.extend_from_slice(&chunk[..chunk.len().min(300usize.saturating_sub(excerpt.len()))]);
                }
                Ok(None) => break,
                Err(err) => {
                    return (
                        build_record(
                            Some(status),
                            Some(format!("response body error after {total_bytes} bytes: {err:?}")),
                        ),
                        total_bytes,
                    );
                }
            }
        }
        let error = (!expected_statuses.contains(&status)).then(|| String::from_utf8_lossy(&excerpt).into_owned());
        (build_record(Some(status), error), total_bytes)
    }

    pub async fn wait_healthy(&self, timeout: Duration) -> Result<(), String> {
        let started = Instant::now();
        while started.elapsed() < timeout {
            if let Ok(reply) = self.send(Method::GET, "/healthz", "", Bytes::new()).await
                && reply.status == 200
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Err(format!("{}/healthz did not return 200 within {timeout:?}", self.base_url))
    }

    /// Registers a user account and creates database `db`.
    pub async fn create_owner(&self, email: &str, db: &str) -> Result<Owner, String> {
        let body = json!({"email": email, "password": "password-123"}).to_string();
        let reply = self.send_expect(Method::POST, "/v1/auth/signup", "", body, &[200, 201]).await?;
        let token = reply.json()["token"].as_str().ok_or("signup response did not contain a token")?.to_string();
        self.create_db(&token, db).await?;
        Ok(Owner { token, db: db.into() })
    }

    pub async fn create_db(&self, token: &str, db: &str) -> Result<(), String> {
        self.send_expect(Method::POST, "/v1/dbs", token, json!({"name": db}).to_string(), &[200, 201]).await.map(|_| ())
    }

    pub async fn create_event_table(&self, token: &str, db: &str, table: Json) -> Result<(), String> {
        self.send_expect(Method::POST, &format!("/v1/dbs/{db}/events"), token, table.to_string(), &[201])
            .await
            .map(|_| ())
    }

    pub async fn bulk_insert(&self, token: &str, db: &str, table: &str, body: impl Into<Bytes>) -> Result<(), String> {
        self.send_expect(Method::POST, &format!("/v1/dbs/{db}/events/{table}/rows"), token, body, &[201])
            .await
            .map(|_| ())
    }

    pub async fn table_info(&self, token: &str, db: &str, table: &str) -> Result<Json, String> {
        let reply =
            self.send_expect(Method::GET, &format!("/v1/dbs/{db}/events/{table}"), token, Bytes::new(), &[200]).await?;
        Ok(reply.json())
    }

    pub async fn part_count(&self, token: &str, db: &str, table: &str) -> Result<usize, String> {
        Ok(self.table_info(token, db, table).await?["parts"].as_array().map_or(0, Vec::len))
    }
}

/// Builds a JSON array of generated rows up to `max_bytes`. Returns the JSON text and row count.
pub fn json_rows(max_bytes: usize, format_row: impl Fn(usize) -> String) -> (String, usize) {
    let mut json_text = String::with_capacity(max_bytes);
    json_text.push('[');
    let mut row_count = 0;
    loop {
        let entry = format_row(row_count);
        if json_text.len() + entry.len() + 2 > max_bytes {
            break;
        }
        if row_count > 0 {
            json_text.push(',');
        }
        json_text.push_str(&entry);
        row_count += 1;
    }
    json_text.push(']');
    (json_text, row_count)
}
