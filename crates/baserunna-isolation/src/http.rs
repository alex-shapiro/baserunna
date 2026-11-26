//! HTTP client for the node's public API.

use baserunna_api::{Body, CreateDb, Credentials, Endpoint, NewDb, Octets, PutOptions, PutValue, SignUp};
use bytes::Bytes;
use reqwest::header::{CONTENT_TYPE, HeaderMap};
use reqwest::{Method, RequestBuilder};
use serde::Serialize;
use serde_json::{Value as Json, json};
use std::time::{Duration, Instant};

/// Maximum time that [`NodeApi::call`] and [`NodeApi::send_expect`] retry a request.
pub const SETUP_RETRY_LIMIT: Duration = Duration::from_secs(60);

/// HTTP client bound to a single node URL.
pub struct NodeApi {
    http: reqwest::Client,
    base_url: String,
}

impl NodeApi {
    pub fn new(base_url: &str, timeout: Duration) -> NodeApi {
        let http = reqwest::Client::builder().timeout(timeout).pool_max_idle_per_host(64).build().expect("http client");
        NodeApi { http, base_url: base_url.trim_end_matches('/').to_string() }
    }

    /// Sends a typed API request and decodes its response. Retries like [`NodeApi::send_expect`].
    pub async fn call<E: Endpoint>(&self, endpoint: &E, token: &str, body: &E::Body) -> Result<E::Response, String> {
        let path = endpoint.path();
        let build_request = || {
            let mut request = self.request(E::METHOD, &path, token);
            if let Some(content_type) = E::Body::CONTENT_TYPE {
                request = request.header(CONTENT_TYPE, content_type);
            }
            request.headers(endpoint.headers()).body(body.encode())
        };
        let label = format!("{} {path}", E::METHOD);
        let reply = self.send_with_retries(&label, build_request, &[E::STATUS.as_u16()]).await?;
        let (mut head, ()) = ::http::Response::new(()).into_parts();
        head.status = E::STATUS;
        head.headers = reply.headers;
        E::response(&head, reply.body).map_err(|err| format!("{label}: {err}"))
    }

    /// Sends a JSON request and requires the response status to match `expected_statuses`.
    ///
    /// Use this method only for routes that have no [`Endpoint`] type, such as event tables.
    pub async fn send_expect(
        &self,
        method: Method,
        path: &str,
        token: &str,
        body: impl Into<Bytes>,
        expected_statuses: &[u16],
    ) -> Result<HttpReply, String> {
        let body = body.into();
        let build_request = || self.json_request(method.clone(), path, token, body.clone());
        self.send_with_retries(&format!("{method} {path}"), build_request, expected_statuses).await
    }

    /// Sends a JSON request once and records its status and elapsed time.
    pub async fn send_timed(
        &self,
        label: &str,
        method: Method,
        path: &str,
        token: &str,
        body: Bytes,
        expected_statuses: &[u16],
    ) -> RequestRecord {
        self.send_timed_drain(label, method, path, token, body, expected_statuses).await.0
    }

    /// Sends a JSON request once and discards its response body in chunks. Returns the request
    /// record and total response bytes.
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
        let build_record = |status: Option<u16>, error: Option<String>| RequestRecord {
            label: label.into(),
            status,
            elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
            expected: error.is_none() && status.is_some_and(|code| expected_statuses.contains(&code)),
            error,
        };
        let mut response = match self.json_request(method, path, token, body).send().await {
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

    /// Registers a user account and creates database `db`.
    pub async fn create_owner(&self, email: &str, db: &str) -> Result<Owner, String> {
        let credentials = Credentials { email: email.into(), password: "password-123".into() };
        let session = self.call(&SignUp, "", &credentials).await?;
        self.create_db(&session.token, db).await?;
        Ok(Owner { token: session.token, db: db.into() })
    }

    pub async fn create_db(&self, token: &str, db: &str) -> Result<(), String> {
        self.call(&CreateDb, token, &NewDb { name: db.into(), org: None }).await.map(|_| ())
    }

    pub async fn put_value(&self, token: &str, db: &str, key: &str, value: impl Into<Bytes>) -> Result<(), String> {
        let endpoint = PutValue {
            db: db.into(),
            key: Bytes::copy_from_slice(key.as_bytes()),
            options: PutOptions::default(),
            idempotency_key: None,
        };
        self.call(&endpoint, token, &Octets(value.into())).await.map(|_| ())
    }

    /// Creates an event table with time column `time_column` and string columns `string_columns`.
    ///
    /// Rows without a time value get the insert time.
    pub async fn create_event_table(
        &self,
        token: &str,
        db: &str,
        table: &str,
        time_column: &str,
        string_columns: &[&str],
    ) -> Result<(), String> {
        let mut columns = vec![json!({"name": time_column, "type": "time", "default": "now"})];
        columns.extend(string_columns.iter().map(|name| json!({"name": name, "type": "string"})));
        let schema = json!({"name": table, "columns": columns, "time_column": time_column, "order_by": [time_column]});
        self.send_expect(Method::POST, &format!("/v1/dbs/{db}/events"), token, schema.to_string(), &[201])
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

    /// Polls event table `table` until its part count stays the same for 3 seconds, or for at most
    /// 60 seconds. Returns the table info.
    pub async fn wait_for_stable_parts(&self, token: &str, db: &str, table: &str) -> Result<Json, String> {
        let started = Instant::now();
        let mut part_count = self.part_count(token, db, table).await?;
        let mut stable_polls = 0;
        while stable_polls < 6 && started.elapsed() < Duration::from_secs(60) {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let current_part_count = self.part_count(token, db, table).await?;
            stable_polls = if current_part_count == part_count { stable_polls + 1 } else { 0 };
            part_count = current_part_count;
        }
        self.table_info(token, db, table).await
    }

    pub async fn wait_healthy(&self, timeout: Duration) -> Result<(), String> {
        let started = Instant::now();
        while started.elapsed() < timeout {
            if let Ok(reply) = Self::reply(self.request(Method::GET, "/healthz", "")).await
                && reply.status == 200
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Err(format!("{}/healthz did not return 200 within {timeout:?}", self.base_url))
    }

    /// Sends the request from `build_request` until the status matches `expected_statuses`.
    ///
    /// Retries any error response that the node marks retryable, such as HTTP 503 or HTTP 409.
    /// Uses exponential backoff up to [`SETUP_RETRY_LIMIT`].
    async fn send_with_retries(
        &self,
        label: &str,
        build_request: impl Fn() -> RequestBuilder,
        expected_statuses: &[u16],
    ) -> Result<HttpReply, String> {
        let started = Instant::now();
        let mut backoff = Duration::from_millis(100);
        loop {
            let reply = Self::reply(build_request()).await.map_err(|err| format!("{label}: {err}"))?;
            if expected_statuses.contains(&reply.status) {
                return Ok(reply);
            }
            let retryable = reply.json()["error"]["retryable"] == json!(true);
            if !retryable || started.elapsed() + backoff > SETUP_RETRY_LIMIT {
                return Err(format!("{label}: status {}: {}", reply.status, reply.excerpt()));
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(5));
        }
    }

    fn request(&self, method: Method, path: &str, token: &str) -> RequestBuilder {
        let request = self.http.request(method, format!("{}{path}", self.base_url));
        if token.is_empty() { request } else { request.bearer_auth(token) }
    }

    fn json_request(&self, method: Method, path: &str, token: &str, body: Bytes) -> RequestBuilder {
        self.request(method, path, token).header(CONTENT_TYPE, "application/json").body(body)
    }

    async fn reply(request: RequestBuilder) -> Result<HttpReply, String> {
        let response = request.send().await.map_err(|err| format!("{err:?}"))?;
        let (status, headers) = (response.status().as_u16(), response.headers().clone());
        let body = response.bytes().await.map_err(|err| format!("{err:?}"))?;
        Ok(HttpReply { status, headers, body })
    }
}

/// Authenticated database owner with a bearer token and database name.
#[derive(Clone)]
pub struct Owner {
    pub token: String,
    pub db: String,
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

/// HTTP response returned by the node.
pub struct HttpReply {
    pub status: u16,
    pub headers: HeaderMap,
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
