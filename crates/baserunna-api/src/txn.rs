//! One-shot and interactive transactions.

use crate::{Empty, Endpoint, NoBody, Op, OpResult, fill_route, is_default, is_false, json_body};
use http::{Method, StatusCode};
use serde::{Deserialize, Serialize};

/// `POST /v1/dbs/{db}/txn`: executes operations in a single transaction.
#[derive(Clone, Debug)]
pub struct Txn {
    pub db: String,
}

impl Endpoint for Txn {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/dbs/{db}/txn";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = TxnRequest;
    type Response = TxnResponse;

    fn is_idempotent(body: &TxnRequest) -> bool {
        body.is_idempotent()
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db])
    }
}

/// `POST /v1/dbs/{db}/transactions`: begins an interactive transaction and executes `ops` in it.
///
/// The transaction expires after `expires_at` or after `idle_timeout_ms` without a request.
#[derive(Clone, Debug)]
pub struct BeginTxn {
    pub db: String,
}

impl Endpoint for BeginTxn {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/dbs/{db}/transactions";
    const STATUS: StatusCode = StatusCode::CREATED;
    type Body = BeginRequest;
    type Response = Begun;

    fn is_idempotent(_: &BeginRequest) -> bool {
        false
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db])
    }
}

/// `POST /v1/dbs/{db}/transactions/{id}/ops`: executes operations in an open transaction.
///
/// If the request fails, the server aborts the transaction and later requests on it fail with
/// `txn_not_found`.
#[derive(Clone, Debug)]
pub struct TxnOps {
    pub db: String,
    pub id: String,
}

impl Endpoint for TxnOps {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/dbs/{db}/transactions/{id}/ops";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = OpsRequest;
    type Response = OpsResponse;

    fn is_idempotent(_: &OpsRequest) -> bool {
        false
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db, &self.id])
    }
}

/// `POST /v1/dbs/{db}/transactions/{id}/commit`: executes `ops` and commits the transaction.
///
/// A read-only transaction must be committed to validate its reads.
#[derive(Clone, Debug)]
pub struct CommitTxn {
    pub db: String,
    pub id: String,
}

impl Endpoint for CommitTxn {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/dbs/{db}/transactions/{id}/commit";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = CommitRequest;
    type Response = Committed;

    fn is_idempotent(_: &CommitRequest) -> bool {
        false
    }

    fn conflict_ends_txn() -> bool {
        true
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db, &self.id])
    }
}

/// `DELETE /v1/dbs/{db}/transactions/{id}`: aborts an open transaction.
#[derive(Clone, Debug)]
pub struct AbortTxn {
    pub db: String,
    pub id: String,
}

impl Endpoint for AbortTxn {
    const METHOD: Method = Method::DELETE;
    const ROUTE: &'static str = "/v1/dbs/{db}/transactions/{id}";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = Empty;

    fn is_idempotent(_: &NoBody) -> bool {
        true
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db, &self.id])
    }
}

/// `GET /v1/dbs/{db}/commits/{id}`: resolves whether a commit was applied.
///
/// `id` is a 16-character commit ID or `key:` followed by an idempotency key. If the commit was
/// not applied, the server fences it so that it can never be applied later.
#[derive(Clone, Debug)]
pub struct GetCommit {
    pub db: String,
    pub id: String,
}

impl Endpoint for GetCommit {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/dbs/{db}/commits/{id}";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = CommitStatus;

    fn is_idempotent(_: &NoBody) -> bool {
        true
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db, &self.id])
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Encoding {
    #[default]
    Utf8,
    Base64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Isolation {
    #[default]
    Serializable,
    Snapshot,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Consistency {
    /// Reads the latest committed version.
    #[default]
    Strong,
    /// Reads a cached version that is at most `max_staleness_ms` old, defaulting to 5000 ms.
    Snapshot,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TxnRequest {
    pub ops: Vec<Op>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub encoding: Encoding,
    #[serde(default, skip_serializing_if = "is_default")]
    pub isolation: Isolation,
    /// Deduplication key for write transactions. The server applies a transaction at most once per
    /// key and sets `replayed: true` on repeated requests. Ignored for read-only transactions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Consistency level for read-only transactions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consistency: Option<Consistency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_staleness_ms: Option<u64>,
}

impl TxnRequest {
    pub fn is_idempotent(&self) -> bool {
        self.idempotency_key.is_some() || !self.ops.iter().any(Op::is_write)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TxnResponse {
    /// Commit version for a write transaction, or snapshot version for a read transaction.
    pub version: u64,
    pub committed: bool,
    /// Commit ID for checking commit status at `GET /v1/dbs/{db}/commits/{id}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_id: Option<String>,
    /// True if an earlier request with the same idempotency key already committed the transaction.
    #[serde(default, skip_serializing_if = "is_false")]
    pub replayed: bool,
    /// Per-operation results, or `None` if `replayed` is true.
    pub results: Option<Vec<OpResult>>,
    /// First sequence number assigned to each `append` operation, in order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub appends: Option<Vec<u64>>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeginRequest {
    #[serde(default, skip_serializing_if = "is_default")]
    pub isolation: Isolation,
    /// Encoding of keys and values for the lifetime of the transaction.
    #[serde(default, skip_serializing_if = "is_default")]
    pub encoding: Encoding,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ops: Vec<Op>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Begun {
    pub txn_id: String,
    /// Commit ID for checking commit status at `GET /v1/dbs/{db}/commits/{id}`.
    pub commit_id: String,
    pub read_version: u64,
    /// Time in milliseconds since the Unix epoch after which the transaction is aborted.
    pub expires_at: u64,
    pub idle_timeout_ms: u64,
    pub results: Vec<OpResult>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpsRequest {
    pub ops: Vec<Op>,
}

/// Operations to execute before committing; a `null` or empty body has none.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(from = "Option<CommitBody>")]
pub struct CommitRequest {
    pub ops: Vec<Op>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitBody {
    #[serde(default)]
    ops: Vec<Op>,
}

impl From<Option<CommitBody>> for CommitRequest {
    fn from(b: Option<CommitBody>) -> CommitRequest {
        CommitRequest { ops: b.map(|b| b.ops).unwrap_or_default() }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpsResponse {
    pub results: Vec<OpResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Committed {
    pub version: u64,
    pub commit_id: String,
    /// First sequence number assigned to each `append` operation in the transaction, in order.
    pub appends: Vec<u64>,
    /// Results of the operations in the commit request.
    pub results: Vec<OpResult>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CommitStatus {
    Committed {
        version: u64,
        commit_id: String,
        appends: Vec<u64>,
    },
    /// The commit was not applied and never will be. This is definitive only if the commit began
    /// less than `retention_ms` ago.
    NotCommitted {
        commit_id: String,
        retention_ms: u64,
    },
}

json_body!(
    TxnRequest,
    TxnResponse,
    BeginRequest,
    Begun,
    OpsRequest,
    CommitRequest,
    OpsResponse,
    Committed,
    CommitStatus
);
