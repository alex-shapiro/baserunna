use crate::{Endpoint, NoBody, json_body};

/// Largest key, in bytes.
pub const MAX_USER_KEY: usize = 4096;
/// Largest `limit` of a scan.
pub const MAX_SCAN_LIMIT: usize = 1000;
/// Most operations in one transaction.
pub const MAX_TXN_OPS: usize = 10_000;
/// Default of [`Limits::value_inline_max`].
pub const DEFAULT_VALUE_INLINE_MAX: usize = 32 * 1024;
/// Default of [`Limits::json_value_max`].
pub const DEFAULT_JSON_VALUE_MAX: u64 = 1024 * 1024;
/// Default of [`Limits::txn_max_write_bytes`].
pub const DEFAULT_TXN_MAX_WRITE_BYTES: usize = 16 * 1024 * 1024;

/// Returns true if `id` is 32 lowercase ASCII hex characters, the form of blob ids and other
/// object ids.
pub fn valid_object_id(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
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
