//! Typed client for the Baserunna HTTP API.
//!
//! [`Client`] retries transient failures if the request was not applied or is idempotent.
//!
//! ```no_run
//! # async fn example() -> Result<(), baserunna_client::Error> {
//! use baserunna_client::{Client, HttpTransport, Op};
//! let client = Client::new(HttpTransport::new("https://example.com")).with_token("TOKEN");
//! let db = client.db("notes");
//! let out = db.txn(&[Op::Put { key: "a".into(), value: "1".into() }], Some("key-1")).await?;
//! println!("committed at version {}", out.version);
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

mod error;
mod kv;
mod transport;

pub use baserunna_api as api;
pub use error::Error;
pub use kv::{Change, ChangePage, ChangesRequest, Entry, Expect, KeyRange, Op, OpResult, ScanPage, TxnOutcome, Value};
#[cfg(feature = "reqwest")]
pub use transport::HttpTransport;
pub use transport::{BoxFuture, Transport, TransportError};

use api::{Body, Endpoint, ErrorBody};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

/// Retry policy for failed requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Maximum number of attempts per request; must be at least 1.
    pub attempts: u32,
    /// Delay before the second attempt. Each subsequent delay is multiplied by 3, up to
    /// `max_backoff_ms`.
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    /// Maximum delay if a server or proxy returns `Retry-After`.
    pub max_retry_after_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> RetryPolicy {
        RetryPolicy { attempts: 4, initial_backoff_ms: 100, max_backoff_ms: 2_000, max_retry_after_ms: 10_000 }
    }
}

/// Client for a Baserunna server. Clones share the underlying transport.
#[derive(Clone)]
pub struct Client {
    transport: Arc<dyn Transport>,
    token: Option<Arc<str>>,
    retry_policy: RetryPolicy,
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
        Client { transport: Arc::new(transport), token: None, retry_policy: RetryPolicy::default() }
    }

    /// Returns a client that authenticates with the given session token or API key.
    pub fn with_token(&self, token: &str) -> Client {
        Client { token: Some(token.into()), ..self.clone() }
    }

    pub fn with_retry_policy(&self, retry_policy: RetryPolicy) -> Client {
        Client { retry_policy, ..self.clone() }
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
        let idempotent = E::is_idempotent(body) || allowed_error.is_some();
        let path = endpoint.path();
        let body = body.encode();
        let mut backoff = self.retry_policy.initial_backoff_ms;
        let mut attempt = 1;
        let mut unresolved = None;
        loop {
            let err = match self.attempt::<E>(&path, &body).await {
                Ok(r) => return Ok(Some(r)),
                Err(e) if allowed_error.is_some() && e.code() == allowed_error => return Ok(None),
                Err(e) => e,
            };
            let transient = err.is_transient(idempotent);
            if unresolved.is_none() && err.outcome() == api::Outcome::Unknown {
                unresolved = Some(err.clone());
            }
            if !transient || attempt >= self.retry_policy.attempts {
                // Only `PreconditionFailed` and `Aborted` fence earlier attempts for the idempotency key.
                let resolved = matches!(err.code(), Some(api::ErrorCode::PreconditionFailed | api::ErrorCode::Aborted));
                return Err(match (err.outcome(), unresolved) {
                    (api::Outcome::NotApplied, Some(earlier)) if !resolved => earlier,
                    _ => err,
                });
            }
            let pause = err.retry_after_ms().map_or(backoff, |ms| ms.min(self.retry_policy.max_retry_after_ms));
            tokio::time::sleep(Duration::from_millis(pause)).await;
            backoff = backoff.saturating_mul(3).min(self.retry_policy.max_backoff_ms);
            attempt += 1;
        }
    }

    async fn attempt<E: Endpoint>(&self, path: &str, body: &Bytes) -> Result<E::Response, Error> {
        let mut req = http::Request::builder().method(E::METHOD).uri(path);
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

    /// Returns a handle to database `name`, such as `"notes"` or `"org.notes"`.
    pub fn db(&self, name: &str) -> Db {
        Db { client: self.clone(), name: name.into() }
    }
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

    /// Executes `ops` in a single serializable transaction.
    ///
    /// Write transactions require an `idempotency_key` to retry after a failure with
    /// [`api::Outcome::Unknown`]. Generate a new key for each distinct transaction, including after
    /// [`api::ErrorCode::Aborted`].
    pub async fn txn(&self, ops: &[Op], idempotency_key: Option<&str>) -> Result<TxnOutcome, Error> {
        let req = api::TxnRequest {
            ops: ops.iter().map(Op::to_api).collect(),
            encoding: api::Encoding::Base64,
            idempotency_key: idempotency_key.map(Into::into),
            ..api::TxnRequest::default()
        };
        let e = api::Txn { db: self.name.clone() };
        let path = e.path();
        let resp = self.client.send(&e, &req).await?;
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
