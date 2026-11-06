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
/// Default of [`Limits::json_body_max`].
pub const DEFAULT_JSON_BODY_MAX: usize = 32 * 1024 * 1024;

/// Bytes that Baserunna adds before a user key to store it: the key-value keyspace prefix. The
/// `key_len` of a [`KvWrite`] of a key-value operation is the user key's length plus this.
pub const KV_KEY_PREFIX_LEN: usize = 1;

/// Content type of the blob that a transaction put creates for a value larger than
/// [`Limits::value_inline_max`].
pub const PUT_VALUE_CONTENT_TYPE: &str = "application/octet-stream";

/// A stored value, by the sizes that its encoding uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoredValue {
    /// A value of `len` bytes that is stored in the key.
    Inline { len: usize },
    /// A reference to a blob.
    Blob { blob_id_len: usize, content_type_len: usize },
}

impl StoredValue {
    /// The approximate encoded size of the value.
    pub const fn size(self) -> usize {
        match self {
            StoredValue::Inline { len } => len + 4,
            StoredValue::Blob { blob_id_len, content_type_len } => blob_id_len + content_type_len + 48,
        }
    }
}

/// One key-value write of a transaction. Each `key_len` is the length of the stored key, which
/// for a key-value operation is [`KV_KEY_PREFIX_LEN`] more than the user key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvWrite {
    /// A put, an increment, or the destination of a copy. `value_size` is the
    /// [`StoredValue::size`] of the written value, and `expires` is true if the key has an
    /// expiration time.
    Put { key_len: usize, value_size: usize, expires: bool },
    Delete { key_len: usize },
    DeleteRange,
}

/// The bytes that `w` adds to the written bytes of its transaction, which
/// [`Limits::txn_max_write_bytes`] limits. Each write counts, also a write to a key that the
/// transaction wrote before.
pub const fn txn_write_cost(w: KvWrite) -> usize {
    match w {
        KvWrite::Put { key_len, value_size, expires } => key_len + value_size + if expires { 8 } else { 0 },
        KvWrite::Delete { key_len } => key_len + 8,
        KvWrite::DeleteRange => 64,
    }
}

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
    /// Largest body of a JSON request, such as a transaction.
    pub json_body_max: u64,
    pub blob_max: u64,
    /// Time in milliseconds before an unreferenced staged blob is deleted.
    pub blob_stage_ttl_ms: u64,
}

json_body!(Limits);
