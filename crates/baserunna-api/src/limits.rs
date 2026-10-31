use crate::{Endpoint, NoBody, json_body};
use http::{Method, StatusCode};
use serde::{Deserialize, Serialize};

/// `GET /v1/limits`: reads the server request limits.
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    pub txn_max_ops: u64,
    pub txn_max_write_bytes: u64,
    pub txn_max_read_bytes: u64,
    pub txn_max_open_per_user: u64,
    /// Maximum size of an inline value; larger values are stored as blobs.
    pub value_inline_max: u64,
    pub scan_limit_max: u64,
    pub long_poll_max_ms: u64,
    /// Maximum blob size included inline in a JSON response.
    pub json_value_max: u64,
    pub json_body_max: u64,
    pub blob_max: u64,
    /// Time in milliseconds before an unreferenced staged blob is deleted.
    pub blob_stage_ttl_ms: u64,
}

json_body!(Limits);
