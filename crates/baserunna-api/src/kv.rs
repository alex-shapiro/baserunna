//! Change-feed and blob types.
//!
//! Keys and values are encoded as UTF-8 strings or as padded standard base64.
//! If a response uses `utf8` encoding and a byte slice is not valid UTF-8, the server omits the
//! plain field and writes base64 to the corresponding `_b64` field.

use crate::{Consistency, Encoding, Endpoint, NoBody, Octets, fill_route, is_false, json_body, present, with_query};
use http::{Method, StatusCode};
use serde::{Deserialize, Deserializer, Serialize};

/// `GET /v1/dbs/{db}/changes`: reads changes after a cursor, optionally waiting for new changes.
#[derive(Clone, Debug)]
pub struct Changes {
    pub db: String,
    pub query: ChangesQuery,
}

impl Endpoint for Changes {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/dbs/{db}/changes";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = ChangePage;

    fn is_idempotent(_: &Self::Body) -> bool {
        true
    }

    fn path(&self) -> String {
        with_query(fill_route(Self::ROUTE, &[&self.db]), &self.query)
    }
}

/// `POST /v1/dbs/{db}/blobs`: uploads a blob to reference by `blob_id` in a later `put`.
///
/// Unreferenced staged blobs expire after `blob_stage_ttl_ms`.
#[derive(Clone, Debug)]
pub struct StageBlob {
    pub db: String,
}

impl Endpoint for StageBlob {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/dbs/{db}/blobs";
    const STATUS: StatusCode = StatusCode::CREATED;
    type Body = Octets;
    type Response = BlobMeta;

    fn is_idempotent(_: &Self::Body) -> bool {
        true
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db])
    }
}

/// `GET /v1/dbs/{db}/blobs/{blob_id}`: downloads a blob.
#[derive(Clone, Debug)]
pub struct ReadBlob {
    pub db: String,
    pub blob_id: String,
}

impl Endpoint for ReadBlob {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/dbs/{db}/blobs/{blob_id}";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = Octets;

    fn is_idempotent(_: &Self::Body) -> bool {
        true
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db, &self.blob_id])
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMeta {
    pub blob_id: String,
    pub size: u64,
    /// Lowercase hex SHA-256 digest of the blob content.
    pub sha256: String,
    pub content_type: String,
}

json_body!(BlobMeta);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangesQuery {
    /// Version or `next_cursor` from the previous page. `"0"` starts at the beginning of retained
    /// history.
    pub after: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encoding: Option<Encoding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    #[serde(default, skip_serializing_if = "is_false", deserialize_with = "flag")]
    pub keys_only: bool,
    /// Maximum number of changes, defaulting to 100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Maximum time in milliseconds to wait if no changes are available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consistency: Option<Consistency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_staleness_ms: Option<u64>,
}

fn flag<'de, D: Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
    match String::deserialize(d)?.as_str() {
        "true" | "1" | "" => Ok(true),
        "false" | "0" => Ok(false),
        o => Err(serde::de::Error::custom(format!("bad boolean {o:?}"))),
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangePage {
    /// Changes in commit order.
    pub changes: Vec<Change>,
    /// True if more changes are immediately available after `next_cursor`.
    pub more: bool,
    pub next_cursor: String,
    /// Snapshot version read by the server.
    pub version: u64,
}

json_body!(ChangePage);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    pub version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_b64: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_b64: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<BlobMeta>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
    /// Range deleted by this change; mutually exclusive with `key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_range: Option<DeletedRange>,
}

/// Key range `[start, end)`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletedRange {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_b64: Option<String>,
    /// `Some(None)` if the upper bound is unbounded, or `None` if `end_b64` is set.
    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "present")]
    pub end: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_b64: Option<String>,
}
