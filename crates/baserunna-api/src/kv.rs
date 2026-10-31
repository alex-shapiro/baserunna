//! Transactions, the change feed, and blobs.
//!
//! Keys and values appear in JSON as text in the `encoding` of the request: UTF-8 text for
//! `utf8`, or standard base64 with padding for `base64`. If a response with `utf8` encoding holds
//! bytes that are not valid UTF-8, the server writes them as base64 to a field with the suffix
//! `_b64` (such as `key_b64`) and omits the plain field.

use crate::{Endpoint, NoBody, Octets, fill_route, is_false, json_body, with_query};
use http::{Method, StatusCode};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as Json;
use serde_json::value::RawValue;

/// `POST /v1/dbs/{db}/txn`: runs operations as one transaction.
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

    /// A transaction is idempotent if it only reads or if it has an idempotency key.
    fn is_idempotent(body: &TxnRequest) -> bool {
        body.is_idempotent()
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db])
    }
}

/// `GET /v1/dbs/{db}/changes`: reads the changes after a cursor, and optionally waits for them.
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

/// `POST /v1/dbs/{db}/blobs`: uploads a blob for a later `put` with `blob_id`.
///
/// A staged blob that no key references is deleted after `blob_stage_ttl_ms`, so a repeated
/// upload has no lasting effect.
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

/// `GET /v1/dbs/{db}/blobs/{blob_id}`: reads the content of a blob.
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

/// How keys and values appear in JSON.
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

/// Consistency of a read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Consistency {
    /// Reads the latest committed version.
    #[default]
    Strong,
    /// Reads a cached version that is at most `max_staleness_ms` old (default 5000).
    Snapshot,
}

fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    *t == T::default()
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TxnRequest {
    pub ops: Vec<Op>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub encoding: Encoding,
    #[serde(default, skip_serializing_if = "is_default")]
    pub isolation: Isolation,
    /// Key that makes a write transaction safe to send again: the server applies the
    /// transaction at most once per key and returns `replayed: true` for a repeated request.
    /// The server ignores the key of a transaction without writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Consistency of a transaction without writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consistency: Option<Consistency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_staleness_ms: Option<u64>,
}

impl TxnRequest {
    /// Checks whether sending the request again after it succeeds has no effect.
    pub fn is_idempotent(&self) -> bool {
        self.idempotency_key.is_some() || !self.ops.iter().any(Op::is_write)
    }
}

/// An operation of a transaction, identified by its `op` field.
///
/// Fields that hold keys or values are text in the `encoding` of the request. Operations on
/// tables, streams, and event tables hold their filters, queries, and rows as JSON, which the
/// server validates.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Op {
    Get {
        key: String,
    },
    /// Reads keys with `prefix`, or keys in `[start, end)`, in key order.
    Scan {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end: Option<String>,
        /// Maximum number of items (default 100).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
        #[serde(default, skip_serializing_if = "is_false")]
        reverse: bool,
        /// Omits values from the items.
        #[serde(default, skip_serializing_if = "is_false")]
        keys_only: bool,
        /// `next_cursor` of the previous page.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
    },
    /// Writes exactly one of `value` and `blob_id` (a staged blob) to `key`.
    Put {
        key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        blob_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
    },
    Delete {
        key: String,
    },
    /// Writes the value of `from` to `to`. Fails with `precondition_failed` if `from` does not
    /// exist.
    Copy {
        from: String,
        to: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
    },
    /// Deletes keys with `prefix`, or keys in `[start, end)`.
    DeleteRange {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end: Option<String>,
    },
    /// Fails the transaction with `precondition_failed` unless `key` satisfies every given
    /// condition.
    Check {
        key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exists: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value: Option<String>,
    },
    /// Adds `by` to the decimal integer stored at `key` (0 if absent).
    Increment {
        key: String,
        #[serde(default = "one")]
        by: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
    },
    GetRow {
        table: String,
        pk: Json,
    },
    InsertRow {
        table: String,
        row: Json,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
    },
    UpsertRow {
        table: String,
        row: Json,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
    },
    UpdateRow {
        table: String,
        pk: Json,
        set: Json,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
    },
    DeleteRow {
        table: String,
        pk: Json,
    },
    UpdateRows {
        table: String,
        #[serde(rename = "where")]
        selection: Json,
        set: Json,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
    },
    DeleteRows {
        table: String,
        #[serde(rename = "where")]
        selection: Json,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
    },
    QueryRows {
        table: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        index: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        eq: Vec<Json>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        range: Option<Json>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<Json>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
        #[serde(default, skip_serializing_if = "is_false")]
        reverse: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
    },
    Append {
        stream: String,
        records: Vec<Json>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expect_seq: Option<u64>,
        #[serde(default, skip_serializing_if = "is_false")]
        create: bool,
    },
    ReadStream(ReadStream),
    StreamInfo {
        stream: String,
    },
    TrimStream {
        stream: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        before_seq: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        before_time: Option<u64>,
    },
    GetConsumer {
        stream: String,
        consumer: String,
    },
    SetConsumer {
        stream: String,
        consumer: String,
        seq: u64,
    },
    InsertEvents {
        table: String,
        rows: Vec<Json>,
    },
    QueryEvents(QueryEvents),
    DeleteEvents {
        table: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        from: Option<Json>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        to: Option<Json>,
        #[serde(rename = "where")]
        filter: Json,
    },
}

fn one() -> i64 {
    1
}

impl Op {
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            Op::Put { .. }
                | Op::Delete { .. }
                | Op::Copy { .. }
                | Op::DeleteRange { .. }
                | Op::Increment { .. }
                | Op::InsertRow { .. }
                | Op::UpsertRow { .. }
                | Op::UpdateRow { .. }
                | Op::DeleteRow { .. }
                | Op::UpdateRows { .. }
                | Op::DeleteRows { .. }
                | Op::Append { .. }
                | Op::TrimStream { .. }
                | Op::SetConsumer { .. }
                | Op::InsertEvents { .. }
                | Op::DeleteEvents { .. }
        )
    }
}

/// A `read_stream` operation: a stream and the read options beside it.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadStream {
    pub stream: String,
    /// The other fields of the operation.
    pub spec: serde_json::Map<String, Json>,
}

/// A `query_events` operation: an event table and the query fields beside it.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryEvents {
    pub table: String,
    /// The other fields of the operation.
    pub query: serde_json::Map<String, Json>,
}

/// Implements serde for a struct of a name field and the map of all other fields.
macro_rules! named_map {
    ($t:ident, $name:ident, $rest:ident) => {
        impl Serialize for $t {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                use serde::ser::SerializeMap;
                let mut m = s.serialize_map(Some(self.$rest.len() + 1))?;
                m.serialize_entry(stringify!($name), &self.$name)?;
                for (k, v) in &self.$rest {
                    m.serialize_entry(k, v)?;
                }
                m.end()
            }
        }

        impl<'de> Deserialize<'de> for $t {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<$t, D::Error> {
                use serde::de::Error;
                let mut $rest = serde_json::Map::deserialize(d)?;
                let $name =
                    $rest.remove(stringify!($name)).ok_or_else(|| D::Error::missing_field(stringify!($name)))?;
                Ok($t { $name: serde_json::from_value($name).map_err(D::Error::custom)?, $rest })
            }
        }
    };
}

named_map!(ReadStream, stream, spec);
named_map!(QueryEvents, table, query);

/// Response of a transaction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TxnResponse {
    /// Commit version of a write transaction, or the version that a read transaction read.
    pub version: u64,
    pub committed: bool,
    /// Commit ID of a write transaction, which `GET /v1/dbs/{db}/commits/{id}` resolves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_id: Option<String>,
    /// Set if an earlier request with the same idempotency key committed the transaction.
    #[serde(default, skip_serializing_if = "is_false")]
    pub replayed: bool,
    /// One result per operation, or `None` if `replayed` is set.
    pub results: Option<Vec<OpResult>>,
    /// First sequence number of each `append` operation, in order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub appends: Option<Vec<u64>>,
}

json_body!(TxnRequest, TxnResponse);

/// Result of one operation.
///
/// The JSON of a result has no tag, so deserialization keeps each result as [`OpResult::Raw`];
/// [`OpResult::decode`] reads it as the result type of its operation.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum OpResult {
    /// Result of `get`.
    Get(GetResult),
    /// Result of `scan`.
    Scan(ScanResult),
    /// Result of `put`, `delete`, `copy`, `delete_range`, `check`, and other operations that
    /// return `{}`.
    Done(crate::Empty),
    /// Result of an operation on a table, stream, or event table, or of `increment`.
    Other(Json),
    /// A result as received.
    Raw(Box<RawValue>),
}

impl OpResult {
    /// Reads the result as `T`, such as [`GetResult`] for a `get` operation.
    pub fn decode<T: serde::de::DeserializeOwned>(&self) -> Result<T, String> {
        let r = match self {
            OpResult::Raw(r) => serde_json::from_str(r.get()),
            other => serde_json::to_value(other).and_then(serde_json::from_value),
        };
        r.map_err(|e| e.to_string())
    }
}

impl<'de> Deserialize<'de> for OpResult {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<OpResult, D::Error> {
        Box::<RawValue>::deserialize(d).map(OpResult::Raw)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetResult {
    pub found: bool,
    /// The value, unless it is a blob that exceeds `json_value_max`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_b64: Option<String>,
    /// Set if the value is a blob.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<BlobMeta>,
    /// Set if `found` is set: the version of the key, or `Some(None)` (JSON `null`) if the
    /// transaction wrote the key earlier.
    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "present")]
    pub version: Option<Option<u64>>,
    /// Expiry time in milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanResult {
    pub items: Vec<ScanItem>,
    /// Cursor of the next page, or `None` after the last page.
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_b64: Option<String>,
    /// The value, unless the scan is `keys_only` or the value is a blob that exceeds
    /// `json_value_max`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_b64: Option<String>,
    /// Set if the value is a blob.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<BlobMeta>,
    /// Version of the key, or `None` if the transaction wrote the key earlier.
    pub version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

json_body!(GetResult, ScanResult);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMeta {
    pub blob_id: String,
    pub size: u64,
    /// SHA-256 digest of the content in lowercase hex.
    pub sha256: String,
    pub content_type: String,
}

json_body!(BlobMeta);

/// Query parameters of [`Changes`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangesQuery {
    /// A version, or the `next_cursor` of the previous page. `0` reads from the start of the
    /// retained history.
    pub after: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encoding: Option<Encoding>,
    /// Reads keys with `prefix`, or keys in `[start, end)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    /// Omits values from the changes.
    #[serde(default, skip_serializing_if = "is_false", deserialize_with = "flag")]
    pub keys_only: bool,
    /// Maximum number of changes (default 100).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Time to wait for a change if none is available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consistency: Option<Consistency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_staleness_ms: Option<u64>,
}

/// Deserializes a query flag: `true`, `1`, or an empty value, or `false` or `0`.
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
    /// Whether more changes are available after `next_cursor` now.
    pub more: bool,
    pub next_cursor: String,
    /// Version that the server read.
    pub version: u64,
}

json_body!(ChangePage);

/// A write to one key, or a deletion of a key range.
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
    /// Set instead of `key` if the change deletes a range.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_range: Option<DeletedRange>,
}

/// Keys in `[start, end)`; an `end` of `None` extends to the end of the key space.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletedRange {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_b64: Option<String>,
    /// `Some(None)` (JSON `null`) for an unbounded range; `None` if `end_b64` holds the end.
    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "present")]
    pub end: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_b64: Option<String>,
}

/// Deserializes a field that is present, including JSON `null`, as `Some`.
fn present<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<T>, D::Error> {
    T::deserialize(d).map(Some)
}
