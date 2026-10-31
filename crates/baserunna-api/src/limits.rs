use crate::{Endpoint, NoBody, json_body};
use http::{Method, StatusCode};
use serde::{Deserialize, Serialize};

/// `GET /v1/limits`: reads the request limits of the server.
#[derive(Clone, Copy, Debug, Default)]
pub struct GetLimits;

impl Endpoint for GetLimits {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/limits";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = Limits;

    fn is_idempotent(_: &Self::Body) -> bool {
        true
    }

    fn path(&self) -> String {
        Self::ROUTE.into()
    }
}

/// Request limits of a server.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    /// Maximum number of operations in a transaction.
    pub txn_max_ops: u64,
    /// Maximum bytes that a transaction writes.
    pub txn_max_write_bytes: u64,
    /// Maximum bytes that a transaction reads.
    pub txn_max_read_bytes: u64,
    /// Maximum number of open interactive transactions per user.
    pub txn_max_open_per_user: u64,
    /// Maximum size of a value stored in the database record; larger values are stored as
    /// blobs.
    pub value_inline_max: u64,
    /// Maximum `limit` of a scan.
    pub scan_limit_max: u64,
    /// Maximum `wait_ms` of a long poll.
    pub long_poll_max_ms: u64,
    /// Maximum size of a blob value that a JSON response includes.
    pub json_value_max: u64,
    /// Maximum size of a JSON request body.
    pub json_body_max: u64,
    /// Maximum size of a blob.
    pub blob_max: u64,
    /// Time after which a staged blob that no key references is deleted.
    pub blob_stage_ttl_ms: u64,
}

json_body!(Limits);
