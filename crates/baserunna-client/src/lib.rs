//! Typed client for the Baserunna HTTP API.
//!
//! [`Client`] retries transient failures if the request was not applied or is idempotent, and
//! [`Db::retry`] restarts interactive transactions that conflict.
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
pub use op::{Expect, KeyRange, NewRecord, Op, Row};
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
/// Each retry waits a random delay up to a bound that starts at `initial_backoff_ms` and triples
/// after each attempt, up to `max_backoff_ms`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Maximum number of attempts per request; must be at least 1.
    pub attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    /// Maximum delay if a server or proxy returns `Retry-After`.
    pub max_retry_after_ms: u64,
    /// Maximum number of attempts of a transaction loop of [`Db::retry`]; must be at least 1.
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
    /// Returns the bound on the delay after failed attempt `attempt`, starting at 1.
    fn backoff_ms(&self, attempt: u32) -> u64 {
        let growth = 3u64.saturating_pow(attempt.saturating_sub(1));
        self.initial_backoff_ms.saturating_mul(growth).min(self.max_backoff_ms)
    }
}

/// Source of random 64-bit values.
type Entropy = Arc<dyn Fn() -> u64 + Send + Sync>;

/// Client for a Baserunna server. Clones share the underlying transport.
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
    /// Creates a client that sends requests through `transport`. Pass an `Arc` to retain a
    /// handle to the transport.
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

    /// Returns a client that draws backoff jitter and idempotency keys from `entropy` instead of
    /// the operating system, such as to make retries reproducible in a simulation.
    pub fn with_entropy(&self, entropy: impl Fn() -> u64 + Send + Sync + 'static) -> Client {
        Client { entropy: Arc::new(entropy), ..self.clone() }
    }

    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    pub fn retry_policy(&self) -> RetryPolicy {
        self.retry_policy
    }

    /// Sends a request to endpoint `E`, retrying transient failures if the request was not applied
    /// or is idempotent.
    ///
    /// If the final attempt fails with [`api::Outcome::NotApplied`] after an earlier attempt failed
    /// with [`api::Outcome::Unknown`], the earlier error is returned because that attempt might
    /// have been applied. If the server returns `precondition_failed` or `aborted`, that error is
    /// returned directly because the server fences earlier attempts for the idempotency key first.
    pub async fn send<E: Endpoint>(&self, endpoint: &E, body: &E::Body) -> Result<E::Response, Error> {
        let r = self.send_allowing(endpoint, body, None).await?;
        Ok(r.expect("an allowed error is not configured"))
    }

    /// Sends a request like [`Client::send`], but returns `Ok(None)` if the server responds with
    /// `allowed_error`. If `allowed_error` is set, the request is treated as idempotent.
    async fn send_allowing<E: Endpoint>(
        &self,
        endpoint: &E,
        body: &E::Body,
        allowed_error: Option<api::ErrorCode>,
    ) -> Result<Option<E::Response>, Error> {
        self.attempts(endpoint, body, allowed_error).await.map_err(|f| f.error)
    }

    /// Sends a request like [`Client::send_allowing`], and also returns the final attempt's error
    /// when an earlier attempt's error is reported instead.
    async fn attempts<E: Endpoint>(
        &self,
        endpoint: &E,
        body: &E::Body,
        allowed_error: Option<api::ErrorCode>,
    ) -> Result<Option<E::Response>, Box<Failed>> {
        let idempotent = E::is_idempotent(body) || allowed_error.is_some();
        let path = endpoint.path();
        let body = body.encode();
        let mut attempt = 1;
        let mut unresolved = None;
        loop {
            let err = match self.attempt::<E>(&path, &body).await {
                Ok(r) => return Ok(Some(r)),
                Err(e) if allowed_error.is_some() && e.code() == allowed_error => return Ok(None),
                Err(e) => e,
            };
            let transient = err.is_transient(idempotent)
                && !(E::conflict_ends_txn() && err.code() == Some(api::ErrorCode::Conflict));
            if unresolved.is_none() && err.outcome() == api::Outcome::Unknown {
                unresolved = Some(err.clone());
            }
            if !transient || attempt >= self.retry_policy.attempts {
                // Only `PreconditionFailed` and `Aborted` fence earlier attempts for the idempotency key.
                let resolved = matches!(err.code(), Some(api::ErrorCode::PreconditionFailed | api::ErrorCode::Aborted));
                return Err(match (err.outcome(), unresolved) {
                    (api::Outcome::NotApplied, Some(earlier)) if !resolved => {
                        Box::new(Failed { error: earlier, refusal: Some(err) })
                    }
                    _ => Box::new(Failed { error: err, refusal: None }),
                });
            }
            match err.retry_after_ms() {
                Some(ms) => {
                    tokio::time::sleep(Duration::from_millis(ms.min(self.retry_policy.max_retry_after_ms))).await
                }
                None => self.backoff(attempt).await,
            }
            attempt += 1;
        }
    }

    /// Sleeps for a random delay up to the backoff bound of failed attempt `attempt`, so that
    /// clients that failed together do not retry together.
    pub(crate) async fn backoff(&self, attempt: u32) {
        let bound = self.retry_policy.backoff_ms(attempt);
        tokio::time::sleep(Duration::from_millis(1 + (self.entropy)() % bound.max(1))).await;
    }

    /// Returns a random 128-bit idempotency key as 32 lowercase hex digits.
    pub(crate) fn random_id(&self) -> String {
        format!("{:016x}{:016x}", (self.entropy)(), (self.entropy)())
    }

    async fn attempt<E: Endpoint>(&self, path: &str, body: &Bytes) -> Result<E::Response, Error> {
        let mut req = http::Request::builder().method(E::METHOD).uri(path).extension(Route(E::ROUTE));
        if let Some(t) = &self.token {
            req = req.header(http::header::AUTHORIZATION, format!("Bearer {t}"));
        }
        if let Some(ct) = <E::Body as Body>::CONTENT_TYPE {
            req = req.header(http::header::CONTENT_TYPE, ct);
        }
        let req = req.body(body.clone()).map_err(|e| TransportError::NotSent(e.to_string()))?;
        decode(path, self.transport.send(req).await?)
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
        self.send_allowing(&api::CreateDb, &b, Some(api::ErrorCode::AlreadyExists)).await.map(drop)
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

    /// Reads the metrics of the node that serves the request; requires an admin.
    pub async fn metrics(&self) -> Result<api::Metrics, Error> {
        self.send(&api::GetMetrics, &api::NoBody {}).await
    }

    /// Reads and then resets the metrics of the node that serves the request; requires an admin.
    pub async fn reset_metrics(&self) -> Result<api::Metrics, Error> {
        self.send(&api::ResetMetrics, &api::NoBody {}).await
    }

    /// Returns a handle to database `name`, such as `"notes"` or `"org.notes"`.
    pub fn db(&self, name: &str) -> Db {
        Db { client: self.clone(), name: name.into() }
    }
}

/// Failure of a request after its final attempt.
struct Failed {
    /// Error to report: the final attempt's, or an earlier attempt's whose outcome is unknown.
    error: Error,
    /// Final attempt's error, which applied nothing, if `error` is an earlier attempt's.
    refusal: Option<Error>,
}

fn decode<T: Body>(path: &str, resp: http::Response<Bytes>) -> Result<T, Error> {
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
        return T::decode(body.clone()).map_err(|reason| match is_json(&body) {
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

    pub async fn create_table(&self, table: &api::NewTable) -> Result<api::CreatedTable, Error> {
        self.client.send(&api::CreateTable { db: self.name.clone() }, table).await
    }

    pub async fn get_table(&self, name: &str) -> Result<api::Table, Error> {
        let e = api::GetTable { db: self.name.clone(), table: name.into(), query: api::GetTableQuery::default() };
        self.client.send(&e, &api::NoBody {}).await
    }

    pub async fn list_tables(&self, query: api::ListTablesQuery) -> Result<api::TablePage, Error> {
        self.client.send(&api::ListTables { db: self.name.clone(), query }, &api::NoBody {}).await
    }

    /// Executes read operations at the latest committed version and returns one result per
    /// operation.
    pub async fn read(&self, ops: &[Op]) -> Result<Vec<OpResult>, Error> {
        self.txn(ops, None).await?.results.ok_or_else(|| Error::Malformed {
            path: api::Txn { db: self.name.clone() }.path(),
            reason: "a read was replayed".into(),
        })
    }

    pub async fn read_one(&self, op: Op) -> Result<OpResult, Error> {
        Ok(self.read(std::slice::from_ref(&op)).await?.swap_remove(0))
    }

    /// Executes `ops` in a single transaction that applies exactly once, or returns `None` if the
    /// transaction wrote nothing, such as one of only checks or a delete of a missing row.
    ///
    /// If the outcome remains unknown after retries, the commit is resolved through the server: the
    /// result is the commit, an [`Error::NotApplied`], or the original error if the server cannot
    /// be reached.
    pub async fn write(&self, ops: &[Op]) -> Result<Option<Committed>, Error> {
        if ops.is_empty() {
            return Ok(None);
        }
        let key = self.client.random_id();
        let started = tokio::time::Instant::now();
        let e = api::Txn { db: self.name.clone() };
        let sent = self.client.attempts(&e, &txn_request(ops, Some(&key)), None).await;
        let out = sent.and_then(|resp| {
            let resp = resp.expect("no error is allowed");
            let malformed =
                |reason| Box::new(Failed { error: Error::Malformed { path: e.path(), reason }, refusal: None });
            TxnOutcome::decode(ops, resp).map_err(malformed)
        });
        match out {
            Ok(out) => {
                Ok(out.commit_id.map(|commit_id| Committed { version: out.version, commit_id, appends: out.appends }))
            }
            Err(f) if f.error.outcome() == api::Outcome::Unknown => {
                let Failed { error, refusal } = *f;
                self.resolve(&format!("key:{key}"), started, error, refusal).await.map(Some)
            }
            Err(f) => Err(f.error),
        }
    }

    /// Resolves commit `id`, first attempted at `started`, after `err` left its outcome unknown.
    ///
    /// If the commit was not applied, returns `refusal`, the error of a later attempt that applied
    /// nothing, or else `err` as [`Error::NotApplied`].
    pub(crate) async fn resolve(
        &self,
        id: &str,
        started: tokio::time::Instant,
        err: Error,
        refusal: Option<Error>,
    ) -> Result<Committed, Error> {
        let e = api::GetCommit { db: self.name.clone(), id: id.into() };
        match self.client.send(&e, &api::NoBody {}).await {
            Ok(api::CommitStatus::Committed { version, commit_id, appends }) => {
                Ok(Committed { version, commit_id, appends })
            }
            // The server forgets commits after `retention_ms`, so an older commit may have applied.
            Ok(api::CommitStatus::NotCommitted { retention_ms, .. })
                if started.elapsed() < Duration::from_millis(retention_ms) / 2 =>
            {
                Err(refusal.unwrap_or_else(|| Error::NotApplied(Box::new(err))))
            }
            _ => Err(err),
        }
    }

    /// Executes `ops` in a single serializable transaction.
    ///
    /// Write transactions require an `idempotency_key` to retry after a failure with
    /// [`api::Outcome::Unknown`]. Generate a new key for each distinct transaction, including after
    /// [`api::ErrorCode::Aborted`]. [`Db::write`] generates the key and resolves unknown outcomes.
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

    /// Uploads a blob to reference in [`Op::PutBlob`].
    pub async fn stage_blob(&self, data: Bytes) -> Result<api::BlobMeta, Error> {
        self.client.send(&api::StageBlob { db: self.name.clone() }, &api::Octets(data)).await
    }

    pub async fn read_blob(&self, blob_id: &str) -> Result<Bytes, Error> {
        let e = api::ReadBlob { db: self.name.clone(), blob_id: blob_id.into() };
        Ok(self.client.send(&e, &api::NoBody {}).await?.0)
    }
}

#[cfg(test)]
mod tests;
