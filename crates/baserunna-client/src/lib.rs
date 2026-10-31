//! Typed client for the Baserunna HTTP API.
//!
//! [`Client`] sends the requests of [`baserunna_api`], the crate whose types the server also
//! uses, so a request that compiles matches a route of the server. The client retries failures
//! that cannot have changed data, and failures of requests that are safe to repeat.
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

/// When and how often the client sends a failed request again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Maximum number of attempts per request (at least 1).
    pub attempts: u32,
    /// Pause before the second attempt. Each later pause is 3 times longer, up to
    /// `max_backoff_ms`.
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    /// Upper bound on a pause that the server or a proxy requests with `Retry-After`.
    pub max_retry_after_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> RetryPolicy {
        RetryPolicy { attempts: 4, initial_backoff_ms: 100, max_backoff_ms: 2_000, max_retry_after_ms: 10_000 }
    }
}

/// Client for one Baserunna server. Clones share the transport.
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
    /// Creates a client that sends requests through `transport`. Pass an `Arc` of the
    /// transport to keep a handle to it.
    pub fn new(transport: impl Transport + 'static) -> Client {
        Client { transport: Arc::new(transport), token: None, retry_policy: RetryPolicy::default() }
    }

    /// Returns a client that authenticates with a session token or an API key.
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

    /// Sends a request to endpoint `E`, retrying transient failures that did not apply it, and
    /// any transient failure if the request is idempotent ([`Endpoint::is_idempotent`]).
    ///
    /// If the last attempt fails without applying the request, but an earlier attempt failed
    /// with an unknown outcome, the error of the earlier attempt is returned, because that
    /// attempt may have been applied. A `precondition_failed` or `aborted` error is returned as
    /// is, because the server resolves the earlier attempts of an idempotency key before it
    /// returns one.
    pub async fn send<E: Endpoint>(&self, endpoint: &E, body: &E::Body) -> Result<E::Response, Error> {
        let r = self.send_allowing(endpoint, body, None).await?;
        Ok(r.expect("no error is allowed"))
    }

    /// Like [`Client::send`], but returns `Ok(None)` if the server answers with `allowed_error`,
    /// which means that the goal of the request is already reached. A request with an allowed
    /// error is idempotent, because a repeated request can only succeed or fail with that error.
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
        // The first failure that may have applied the request, unless a later response is
        // definitive.
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
                // Only the server's verdict on the idempotency key resolves an earlier attempt.
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

    /// Reads the request limits of the server.
    pub async fn limits(&self) -> Result<api::Limits, Error> {
        self.send(&api::GetLimits, &api::NoBody {}).await
    }

    /// Registers an account and returns its first session.
    pub async fn sign_up(&self, email: &str, password: &str) -> Result<api::Session, Error> {
        let c = api::Credentials { email: email.into(), password: password.into() };
        self.send(&api::SignUp, &c).await
    }

    pub async fn log_in(&self, email: &str, password: &str) -> Result<api::Session, Error> {
        let c = api::Credentials { email: email.into(), password: password.into() };
        self.send(&api::LogIn, &c).await
    }

    /// Reads the user and the credential of the token.
    pub async fn me(&self) -> Result<api::Me, Error> {
        self.send(&api::GetMe, &api::NoBody {}).await
    }

    /// Creates a database owned by the user, or by organization `org`.
    pub async fn create_db(&self, name: &str, org: Option<&str>) -> Result<api::Db, Error> {
        let b = api::NewDb { name: name.into(), org: org.map(Into::into) };
        self.send(&api::CreateDb, &b).await
    }

    /// Creates a database unless one with the same name exists.
    ///
    /// Unlike [`Client::create_db`], this retries a failure with an unknown outcome, because a
    /// repeated request has no further effect.
    pub async fn ensure_db(&self, name: &str, org: Option<&str>) -> Result<(), Error> {
        let b = api::NewDb { name: name.into(), org: org.map(Into::into) };
        self.send_allowing(&api::CreateDb, &b, Some(api::ErrorCode::AlreadyExists)).await.map(drop)
    }

    pub async fn create_org(&self, name: &str) -> Result<api::Org, Error> {
        self.send(&api::CreateOrg, &api::NewOrg { name: name.into() }).await
    }

    /// Adds the registered user with `email` to organization `org`.
    pub async fn add_member(&self, org: &str, email: &str, role: api::Role) -> Result<api::Member, Error> {
        let b = api::NewMember { email: email.into(), role };
        self.send(&api::AddMember { org: org.into() }, &b).await
    }

    /// Removes the user with ID `user` from organization `org`.
    pub async fn remove_member(&self, org: &str, user: &str) -> Result<(), Error> {
        let e = api::RemoveMember { org: org.into(), user: user.into() };
        self.send(&e, &api::NoBody {}).await.map(|_| ())
    }

    /// Returns a handle to the database `name` (`name` or `org.name`).
    pub fn db(&self, name: &str) -> Db {
        Db { client: self.clone(), name: name.into() }
    }
}

/// Decodes a response, classifying responses that the API did not produce.
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

/// A database of a [`Client`].
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

    /// Runs `ops` as one serializable transaction.
    ///
    /// A transaction that writes needs an `idempotency_key` to be retried after a failure with
    /// an unknown outcome; the server applies it at most once per key. Use a new key for each
    /// transaction, including after an [`api::ErrorCode::Aborted`] error.
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

    /// Reads a page of changes, waiting up to `wait_ms` if no change is available.
    pub async fn changes(&self, req: &ChangesRequest) -> Result<ChangePage, Error> {
        let e = api::Changes { db: self.name.clone(), query: req.to_api() };
        let page = self.client.send(&e, &api::NoBody {}).await?;
        ChangePage::decode(page).map_err(|reason| Error::Malformed { path: e.path(), reason })
    }

    /// Uploads a blob for [`Op::PutBlob`].
    pub async fn stage_blob(&self, data: Bytes) -> Result<api::BlobMeta, Error> {
        self.client.send(&api::StageBlob { db: self.name.clone() }, &api::Octets(data)).await
    }

    /// Reads the content of a blob.
    pub async fn read_blob(&self, blob_id: &str) -> Result<Bytes, Error> {
        let e = api::ReadBlob { db: self.name.clone(), blob_id: blob_id.into() };
        Ok(self.client.send(&e, &api::NoBody {}).await?.0)
    }
}

#[cfg(test)]
mod tests;
