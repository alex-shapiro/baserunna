//! Typed client for the Baserunna HTTP API.
//!
//! [`Client`] retries transient failures if the request is idempotent or was not applied.
//! [`Db::retry`] restarts interactive transactions that fail with a conflict.
//!
//! ```no_run
//! # async fn example() -> Result<(), baserunna_client::Error> {
//! use baserunna_client::{Client, HttpTransport, Op};
//! let client = Client::new(HttpTransport::new("https://example.com")).with_token("TOKEN");
//! let db = client.db("notes");
//! db.write(&[Op::put("a", "1")]).await?;
//! let mut retry = db.retry();
//! let a = loop {
//!     let mut txn = db.begin();
//!     let a = txn.run_one(Op::get("a")).await;
//!     txn.write([Op::put("b", "2")]);
//!     if let Some(a) = retry.finish(txn, a).await? {
//!         break a;
//!     }
//! };
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

mod changes;
mod error;
mod op;
mod result;
mod transport;
mod txn;

pub use baserunna_api as api;
pub use changes::{Change, ChangePage, ChangesRequest};
pub use error::Error;
pub use op::{Expect, KeyRange, NewBody, NewRecord, Op, Row};
pub use result::{Entry, OpResult, Record, RecordBody, RecordPage, RowPage, ScanPage, TxnOutcome, Value};
#[cfg(feature = "reqwest")]
pub use transport::HttpTransport;
pub use transport::{BoxFuture, Route, Transport, TransportError};
pub use txn::{Committed, Retry, Txn, TxnError};
pub use {bytes, http};

use api::{Body, Endpoint, ErrorBody};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

/// Retry policy for failed requests.
///
/// Before each retry, the client waits a random delay up to a bound. The bound starts at
/// `initial_backoff_ms` and triples after each attempt, up to `max_backoff_ms`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Maximum attempts per request. Must be at least 1.
    pub attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    /// Maximum delay if a server or proxy returns `Retry-After`.
    pub max_retry_after_ms: u64,
    /// Maximum attempts for a [`Db::retry`] loop. Must be at least 1.
    pub txn_attempts: u32,
}

impl Default for RetryPolicy {
    fn default() -> RetryPolicy {
        RetryPolicy {
            attempts: 4,
            initial_backoff_ms: 100,
            max_backoff_ms: 2_000,
            max_retry_after_ms: 10_000,
            txn_attempts: 10,
        }
    }
}

impl RetryPolicy {
    /// Returns the maximum backoff delay for 1-based `attempt`.
    fn backoff_ms(&self, attempt: u32) -> u64 {
        let growth = 3u64.saturating_pow(attempt.saturating_sub(1));
        self.initial_backoff_ms.saturating_mul(growth).min(self.max_backoff_ms)
    }
}

/// Source of random 64-bit values.
type Entropy = Arc<dyn Fn() -> u64 + Send + Sync>;

/// Client for a Baserunna server.
///
/// Clones share the underlying transport.
#[derive(Clone)]
pub struct Client {
    transport: Arc<dyn Transport>,
    token: Option<Arc<str>>,
    retry_policy: RetryPolicy,
    entropy: Entropy,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("token", &self.token.as_ref().map(|_| "<hidden>"))
            .field("retry", &self.retry_policy)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Creates a client that sends requests through `transport`.
    pub fn new(transport: impl Transport + 'static) -> Client {
        let entropy = Arc::new(random_u64);
        Client { transport: Arc::new(transport), token: None, retry_policy: RetryPolicy::default(), entropy }
    }

    /// Returns a client that authenticates with the given session token or API key.
    pub fn with_token(&self, token: &str) -> Client {
        Client { token: Some(token.into()), ..self.clone() }
    }

    pub fn with_retry_policy(&self, retry_policy: RetryPolicy) -> Client {
        Client { retry_policy, ..self.clone() }
    }

    /// Returns a client that uses `entropy` for backoff jitter and idempotency keys.
    pub fn with_entropy(&self, entropy: impl Fn() -> u64 + Send + Sync + 'static) -> Client {
        Client { entropy: Arc::new(entropy), ..self.clone() }
    }

    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    pub fn retry_policy(&self) -> RetryPolicy {
        self.retry_policy
    }

    /// Sends a request to endpoint `E`.
    ///
    /// Transient failures are retried if the request is idempotent or was not applied. If an
    /// attempt fails with [`api::Outcome::Unknown`] and a later attempt fails with
    /// [`api::Outcome::NotApplied`], this method returns the earlier error unless the server
    /// fenced earlier attempts with `precondition_failed` or `aborted`.
    pub async fn send<E: Endpoint>(&self, endpoint: &E, body: &E::Body) -> Result<E::Response, Error> {
        let r = self.attempts(endpoint, body, None).await.map_err(|f| f.error)?;
        Ok(r.expect("response is present"))
    }

    /// Sends a request like [`Client::send`], or returns `Ok(None)` on `allowed_error`.
    ///
    /// Requests with an `allowed_error` are treated as idempotent.
    async fn attempts<E: Endpoint>(
        &self,
        endpoint: &E,
        body: &E::Body,
        allowed_error: Option<api::ErrorCode>,
    ) -> Result<Option<E::Response>, Box<Failed>> {
        let idempotent = endpoint.is_idempotent(body) || allowed_error.is_some();
        let read_only = E::is_read_only(body);
        let path = endpoint.path();
        let body = body.encode();
        let mut attempt = 1;
        let mut unresolved = None;
        loop {
            let err = match self.attempt(endpoint, &path, &body).await {
                Ok(r) => return Ok(Some(r)),
                Err(e) if allowed_error.is_some() && e.code() == allowed_error => return Ok(None),
                Err(e) => e,
            };
            let transient = err.is_transient(idempotent)
                && !(E::conflict_ends_txn() && err.code() == Some(api::ErrorCode::Conflict));
            if unresolved.is_none() && err.outcome() == api::Outcome::Unknown && !read_only {
                unresolved = Some(err.clone());
            }
            if !transient || attempt >= self.retry_policy.attempts {
                // The server fences earlier attempts for the idempotency key before returning
                // `PreconditionFailed` or `Aborted`.
                let resolved = matches!(err.code(), Some(api::ErrorCode::PreconditionFailed | api::ErrorCode::Aborted));
                return Err(match (err.outcome(), unresolved) {
                    (api::Outcome::NotApplied, Some(earlier)) if !resolved => {
                        Box::new(Failed { error: earlier, rejected: Some(err) })
                    }
                    _ => Box::new(Failed { error: err, rejected: None }),
                });
            }
            self.backoff(attempt, &err).await;
            attempt += 1;
        }
    }

    /// Sleeps for the server-requested delay or a random backoff delay after `attempt` fails.
    pub(crate) async fn backoff(&self, attempt: u32, err: &Error) {
        let ms = match err.retry_after_ms() {
            Some(ms) => ms.min(self.retry_policy.max_retry_after_ms),
            None => 1 + (self.entropy)() % self.retry_policy.backoff_ms(attempt).max(1),
        };
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    /// Returns a random 128-bit idempotency key as 32 lowercase hex digits.
    pub(crate) fn random_id(&self) -> String {
        format!("{:016x}{:016x}", (self.entropy)(), (self.entropy)())
    }

    async fn attempt<E: Endpoint>(&self, endpoint: &E, path: &str, body: &Bytes) -> Result<E::Response, Error> {
        let mut req = http::Request::builder().method(E::METHOD).uri(path).extension(Route(E::ROUTE));
        if let Some(t) = &self.token {
            req = req.header(http::header::AUTHORIZATION, format!("Bearer {t}"));
        }
        if let Some(ct) = <E::Body as Body>::CONTENT_TYPE {
            req = req.header(http::header::CONTENT_TYPE, ct);
        }
        if let Some(h) = req.headers_mut() {
            h.extend(endpoint.headers());
        }
        let req = req.body(body.clone()).map_err(|e| TransportError::NotSent(e.to_string()))?;
        decode::<E>(path, self.transport.send(req).await?)
    }

    pub async fn limits(&self) -> Result<api::Limits, Error> {
        self.send(&api::GetLimits, &api::NoBody {}).await
    }

    pub async fn sign_up(&self, email: &str, password: &str) -> Result<api::Session, Error> {
        let c = api::Credentials { email: email.into(), password: password.into() };
        self.send(&api::SignUp, &c).await
    }

    pub async fn log_in(&self, email: &str, password: &str) -> Result<api::Session, Error> {
        let c = api::Credentials { email: email.into(), password: password.into() };
        self.send(&api::LogIn, &c).await
    }

    pub async fn me(&self) -> Result<api::Me, Error> {
        self.send(&api::GetMe, &api::NoBody {}).await
    }

    pub async fn create_db(&self, name: &str, org: Option<&str>) -> Result<api::Db, Error> {
        let b = api::NewDb { name: name.into(), org: org.map(Into::into) };
        self.send(&api::CreateDb, &b).await
    }

    /// Creates a database if it does not already exist.
    ///
    /// Unlike [`Client::create_db`], this method retries failures with [`api::Outcome::Unknown`].
    pub async fn ensure_db(&self, name: &str, org: Option<&str>) -> Result<(), Error> {
        let b = api::NewDb { name: name.into(), org: org.map(Into::into) };
        let r = self.attempts(&api::CreateDb, &b, Some(api::ErrorCode::AlreadyExists)).await;
        r.map(drop).map_err(|f| f.error)
    }

    pub async fn list_dbs(&self, query: api::ListDbsQuery) -> Result<api::DbPage, Error> {
        self.send(&api::ListDbs { query }, &api::NoBody {}).await
    }

    /// Creates an API key. Requires a user session token.
    pub async fn create_api_key(&self, key: &api::NewApiKey) -> Result<api::CreatedApiKey, Error> {
        self.send(&api::CreateApiKey, key).await
    }

    pub async fn list_api_keys(&self, query: api::PageQuery) -> Result<api::ApiKeyPage, Error> {
        self.send(&api::ListApiKeys { query }, &api::NoBody {}).await
    }

    pub async fn delete_api_key(&self, id: &str) -> Result<(), Error> {
        self.send(&api::DeleteApiKey { id: id.into() }, &api::NoBody {}).await.map(drop)
    }

    pub async fn create_org(&self, name: &str) -> Result<api::Org, Error> {
        self.send(&api::CreateOrg, &api::NewOrg { name: name.into() }).await
    }

    pub async fn add_member(&self, org: &str, email: &str, role: api::Role) -> Result<api::Member, Error> {
        let b = api::NewMember { email: email.into(), role };
        self.send(&api::AddMember { org: org.into() }, &b).await
    }

    pub async fn remove_member(&self, org: &str, user: &str) -> Result<(), Error> {
        let e = api::RemoveMember { org: org.into(), user: user.into() };
        self.send(&e, &api::NoBody {}).await.map(|_| ())
    }

    /// Reads the metrics of the serving node. Requires an admin token.
    pub async fn metrics(&self) -> Result<api::Metrics, Error> {
        self.send(&api::GetMetrics, &api::NoBody {}).await
    }

    /// Reads and resets the metrics of the serving node. Requires an admin token.
    pub async fn reset_metrics(&self) -> Result<api::Metrics, Error> {
        self.send(&api::ResetMetrics, &api::NoBody {}).await
    }

    /// Returns a handle to database `name`.
    pub fn db(&self, name: &str) -> Db {
        Db { client: self.clone(), name: name.into() }
    }
}

/// Failure of a request after its final attempt.
struct Failed {
    /// Error to report to the caller.
    error: Error,
    /// Error from the final attempt if `error` came from an earlier attempt.
    rejected: Option<Error>,
}

fn decode<E: Endpoint>(path: &str, resp: http::Response<Bytes>) -> Result<E::Response, Error> {
    let (parts, body) = resp.into_parts();
    let status = parts.status.as_u16();
    let declared = parts.headers.get(http::header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()?.parse::<usize>().ok());
    if declared.is_some_and(|n| n != body.len()) {
        let m = format!("{path} returned {} of {} bytes", body.len(), declared.unwrap_or_default());
        return Err(TransportError::Unknown(m).into());
    }
    let retry_after_ms = parts
        .headers
        .get(http::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok()?.trim().parse::<u64>().ok())
        .map(|s| s.saturating_mul(1000));
    let foreign = || Error::Foreign { status, retry_after_ms, path: path.into() };
    if parts.status.is_success() {
        return E::response(&parts, body.clone()).map_err(|reason| match is_json(&body) {
            true => Error::Malformed { path: path.into(), reason },
            false => foreign(),
        });
    }
    match ErrorBody::decode(body) {
        Ok(ErrorBody { error }) => {
            let retry_after_ms = error.details.as_ref().and_then(|d| d.retry_after_ms).or(retry_after_ms);
            Err(Error::Api { status, error, retry_after_ms })
        }
        Err(_) => Err(foreign()),
    }
}

fn txn_request(ops: &[Op], idempotency_key: Option<&str>) -> api::TxnRequest {
    api::TxnRequest {
        ops: ops.iter().map(Op::to_api).collect(),
        encoding: api::Encoding::Base64,
        idempotency_key: idempotency_key.map(Into::into),
        ..api::TxnRequest::default()
    }
}

/// Returns a random value from a SipHash keyed with operating system randomness.
fn random_u64() -> u64 {
    use std::hash::{BuildHasher, RandomState};
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    RandomState::new().hash_one(NEXT.fetch_add(1, Ordering::Relaxed))
}

/// Returns true if `e` reports that the requested key is absent.
fn missing_key(e: &api::ApiError) -> bool {
    match e.details.as_ref().and_then(|d| d.missing) {
        Some(m) => m == api::Missing::Key,
        None => e.code == api::ErrorCode::NotFound && e.message == "key not found",
    }
}

fn is_json(b: &[u8]) -> bool {
    serde_json::from_slice::<serde::de::IgnoredAny>(b).is_ok()
}

/// Handle to a database on a [`Client`].
#[derive(Clone, Debug)]
pub struct Db {
    client: Client,
    name: String,
}

impl Db {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Creates the database if it does not exist.
    pub async fn ensure(&self) -> Result<(), Error> {
        match self.name.split_once('.') {
            Some((org, name)) => self.client.ensure_db(name, Some(org)).await,
            None => self.client.ensure_db(&self.name, None).await,
        }
    }

    /// Reads metadata, the caller's role, and storage statistics for the database.
    pub async fn info(&self) -> Result<api::DbInfo, Error> {
        self.client.send(&api::GetDb { db: self.name.clone() }, &api::NoBody {}).await
    }

    /// Deletes the database.
    pub async fn delete(&self) -> Result<(), Error> {
        self.client.send(&api::DeleteDb { db: self.name.clone() }, &api::NoBody {}).await.map(drop)
    }

    pub async fn list_streams(&self, query: api::ListStreamsQuery) -> Result<api::StreamPage, Error> {
        self.client.send(&api::ListStreams { db: self.name.clone(), query }, &api::NoBody {}).await
    }

    pub async fn create_stream(&self, stream: &api::NewStream) -> Result<api::CreatedStream, Error> {
        self.client.send(&api::CreateStream { db: self.name.clone() }, stream).await
    }

    /// Reads state and consumer offsets for `stream`.
    pub async fn stream_info(&self, stream: &str) -> Result<api::StreamInfo, Error> {
        self.client.send(&api::GetStream { db: self.name.clone(), stream: stream.into() }, &api::NoBody {}).await
    }

    /// Appends records to `stream`.
    ///
    /// The records are appended at most once if `append` includes an `idempotency_key`.
    pub async fn append_records(&self, stream: &str, append: &api::AppendRequest) -> Result<api::Appended, Error> {
        self.client.send(&api::AppendRecords { db: self.name.clone(), stream: stream.into() }, append).await
    }

    /// Appends newline-delimited JSON records to `stream`.
    pub async fn append_lines(&self, stream: &str, lines: Bytes) -> Result<api::Appended, Error> {
        let e = api::AppendLines { db: self.name.clone(), stream: stream.into() };
        self.client.send(&e, &api::JsonLines(lines)).await
    }

    /// Reads a page of records from `stream`.
    pub async fn read_records(&self, stream: &str, query: api::ReadRecordsQuery) -> Result<api::RecordPage, Error> {
        let e = api::ReadRecords { db: self.name.clone(), stream: stream.into(), query };
        self.client.send(&e, &api::NoBody {}).await
    }

    /// Queries a page of records from `stream` that match `query`.
    pub async fn query_records(&self, stream: &str, query: &api::RecordQuery) -> Result<api::RecordPage, Error> {
        self.client.send(&api::QueryRecords { db: self.name.clone(), stream: stream.into() }, query).await
    }

    pub async fn create_table(&self, table: &api::NewTable) -> Result<api::CreatedTable, Error> {
        self.client.send(&api::CreateTable { db: self.name.clone() }, table).await
    }

    /// Adds a column to `table`.
    pub async fn add_column(&self, table: &str, column: &api::NewColumn) -> Result<api::CreatedTable, Error> {
        self.client.send(&api::AddColumn { db: self.name.clone(), table: table.into() }, column).await
    }

    /// Adds a secondary index to `table`.
    pub async fn add_index(&self, table: &str, index: &api::NewIndex) -> Result<api::CreatedTable, Error> {
        self.client.send(&api::AddIndex { db: self.name.clone(), table: table.into() }, index).await
    }

    pub async fn get_table(&self, name: &str) -> Result<api::Table, Error> {
        let e = api::GetTable { db: self.name.clone(), table: name.into(), query: api::GetTableQuery::default() };
        self.client.send(&e, &api::NoBody {}).await
    }

    pub async fn list_tables(&self, query: api::ListTablesQuery) -> Result<api::TablePage, Error> {
        self.client.send(&api::ListTables { db: self.name.clone(), query }, &api::NoBody {}).await
    }

    /// Executes read operations at the latest committed version.
    pub async fn read(&self, ops: &[Op]) -> Result<Vec<OpResult>, Error> {
        self.txn(ops, None).await?.results.ok_or_else(|| Error::Malformed {
            path: api::Txn { db: self.name.clone() }.path(),
            reason: "a read was replayed".into(),
        })
    }

    /// Executes read operations against a snapshot that is at most `max_staleness` old.
    pub async fn read_snapshot(&self, ops: &[Op], max_staleness: Duration) -> Result<Vec<OpResult>, Error> {
        let req = api::TxnRequest {
            consistency: Some(api::Consistency::Snapshot),
            max_staleness_ms: Some(u64::try_from(max_staleness.as_millis()).unwrap_or(u64::MAX)),
            ..txn_request(ops, None)
        };
        let e = api::Txn { db: self.name.clone() };
        let path = e.path();
        let out = TxnOutcome::decode(ops, self.client.send(&e, &req).await?);
        let out = out.map_err(|reason| Error::Malformed { path: path.clone(), reason })?;
        out.results.ok_or_else(|| Error::Malformed { path, reason: "a read was replayed".into() })
    }

    pub async fn read_one(&self, op: Op) -> Result<OpResult, Error> {
        Ok(self.read(std::slice::from_ref(&op)).await?.swap_remove(0))
    }

    /// Executes write operations in a single transaction with exactly-once delivery.
    ///
    /// Returns `None` if the transaction did not modify any data. If the outcome remains unknown
    /// after retries, this method queries the commit status on the server.
    pub async fn write(&self, ops: &[Op]) -> Result<Option<Committed>, Error> {
        if ops.is_empty() {
            return Ok(None);
        }
        let request = |key: &str| (api::Txn { db: self.name.clone() }, txn_request(ops, Some(key)));
        let applied = |r: api::TxnResponse| {
            r.commit_id.map(|commit_id| Committed {
                version: r.version,
                commit_id,
                appends: r.appends.unwrap_or_default(),
            })
        };
        self.exactly_once(request, applied, Some).await
    }

    /// Sends a request with a new idempotency key and resolves unknown outcomes on the server.
    async fn exactly_once<E: Endpoint, T>(
        &self,
        request: impl Fn(&str) -> (E, E::Body),
        applied: impl Fn(E::Response) -> T,
        resolved: impl Fn(Committed) -> T,
    ) -> Result<T, Error> {
        // An aborted idempotency key cannot be committed, so retry once with a fresh key.
        match self.once(&request, &applied, &resolved).await {
            Err(e) if e.code() == Some(api::ErrorCode::Aborted) => self.once(&request, &applied, &resolved).await,
            r => r,
        }
    }

    async fn once<E: Endpoint, T>(
        &self,
        request: &impl Fn(&str) -> (E, E::Body),
        applied: &impl Fn(E::Response) -> T,
        resolved: &impl Fn(Committed) -> T,
    ) -> Result<T, Error> {
        let key = self.client.random_id();
        let started = tokio::time::Instant::now();
        let (e, body) = request(&key);
        match self.client.attempts(&e, &body, None).await {
            Ok(r) => Ok(applied(r.expect("response is present"))),
            Err(f) if f.error.outcome() == api::Outcome::Unknown => {
                let Failed { error, rejected } = *f;
                self.resolve(&format!("key:{key}"), started, error, rejected).await.map(resolved)
            }
            Err(f) => Err(f.error),
        }
    }

    /// Queries the server for the status of commit `id` after `err` left its outcome unknown.
    ///
    /// If the commit was not applied, this method returns `rejected` or wraps `err` in
    /// [`Error::NotApplied`].
    pub(crate) async fn resolve(
        &self,
        id: &str,
        started: tokio::time::Instant,
        err: Error,
        rejected: Option<Error>,
    ) -> Result<Committed, Error> {
        let e = api::GetCommit { db: self.name.clone(), id: id.into() };
        match self.client.send(&e, &api::NoBody {}).await {
            Ok(api::CommitStatus::Committed { version, commit_id, appends }) => {
                Ok(Committed { version, commit_id, appends })
            }
            // The server expires commit records after `retention_ms`.
            Ok(api::CommitStatus::NotCommitted { retention_ms, .. })
                if started.elapsed() < Duration::from_millis(retention_ms) / 2 =>
            {
                Err(rejected.unwrap_or_else(|| Error::NotApplied(Box::new(err))))
            }
            _ => Err(err),
        }
    }

    /// Executes `ops` in a single serializable transaction.
    ///
    /// Pass an `idempotency_key` to retry write transactions after [`api::Outcome::Unknown`].
    /// Generate a new key for each distinct transaction, including after
    /// [`api::ErrorCode::Aborted`]. [`Db::write`] generates keys and resolves unknown outcomes
    /// automatically.
    pub async fn txn(&self, ops: &[Op], idempotency_key: Option<&str>) -> Result<TxnOutcome, Error> {
        let e = api::Txn { db: self.name.clone() };
        let path = e.path();
        let resp = self.client.send(&e, &txn_request(ops, idempotency_key)).await?;
        TxnOutcome::decode(ops, resp).map_err(|reason| Error::Malformed { path, reason })
    }

    pub async fn changes(&self, req: &ChangesRequest) -> Result<ChangePage, Error> {
        let e = api::Changes { db: self.name.clone(), query: req.to_api() };
        let page = self.client.send(&e, &api::NoBody {}).await?;
        ChangePage::decode(page).map_err(|reason| Error::Malformed { path: e.path(), reason })
    }

    /// Uploads a blob with `content_type` to reference in [`Op::PutBlob`].
    pub async fn stage_blob(&self, data: Bytes, content_type: &str) -> Result<api::BlobMeta, Error> {
        let e = api::StageBlob { db: self.name.clone(), content_type: Some(content_type.into()) };
        self.client.send(&e, &api::Octets(data)).await
    }

    /// Reads a blob or a byte range of a blob.
    pub async fn read_blob(&self, blob_id: &str, range: Option<api::ByteRange>) -> Result<api::Content, Error> {
        let e = api::ReadBlob { db: self.name.clone(), blob_id: blob_id.into(), range };
        self.client.send(&e, &api::NoBody {}).await
    }

    /// Reads the value at `key`, or returns `None` if `key` is absent.
    pub async fn get_value(
        &self,
        key: impl Into<Bytes>,
        range: Option<api::ByteRange>,
    ) -> Result<Option<api::KvValue>, Error> {
        let e = api::GetValue { db: self.name.clone(), key: key.into(), range };
        match self.client.send(&e, &api::NoBody {}).await {
            Ok(v) => Ok(Some(v)),
            Err(Error::Api { error, .. }) if missing_key(&error) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Writes `data` to `key` with exactly-once delivery.
    ///
    /// If the outcome remains unknown after retries, this method queries the commit status on the
    /// server.
    pub async fn put_value(
        &self,
        key: impl Into<Bytes>,
        data: impl Into<Bytes>,
        options: api::PutOptions,
    ) -> Result<api::Written, Error> {
        let (key, data) = (key.into(), api::Octets(data.into()));
        let request = |id: &str| {
            let e = api::PutValue {
                db: self.name.clone(),
                key: key.clone(),
                options: options.clone(),
                idempotency_key: Some(id.into()),
            };
            (e, data.clone())
        };
        self.exactly_once(request, |w| w, |c| api::Written { version: c.version }).await
    }
}

#[cfg(test)]
mod tests;
