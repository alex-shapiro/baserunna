//! Transaction, change-feed, and blob types.
//!
//! Keys and values are encoded as UTF-8 strings or as padded standard base64.
//! If a response uses `utf8` encoding and a byte slice is not valid UTF-8, the server omits the
//! plain field and writes base64 to the corresponding `_b64` field.

use crate::{Endpoint, NoBody, Octets, fill_route, is_false, json_body, with_query};
use http::{Method, StatusCode};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as Json;
use serde_json::value::RawValue;

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

/// Key and value fields use the request `encoding`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Op {
    Get {
        key: String,
    },
    /// Reads keys matching `prefix` or in `[start, end)`.
    Scan {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end: Option<String>,
        /// Maximum number of items, defaulting to 100.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
        #[serde(default, skip_serializing_if = "is_false")]
        reverse: bool,
        /// If true, omits values from the returned items.
        #[serde(default, skip_serializing_if = "is_false")]
        keys_only: bool,
        /// `next_cursor` from the previous page.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
    },
    /// Writes `value` or a staged `blob_id` to `key`. Exactly one of `value` or `blob_id` must be
    /// set.
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
    /// Copies the value of `from` to `to`, or fails with `precondition_failed` if `from` is
    /// absent.
    Copy {
        from: String,
        to: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
    },
    /// Deletes keys matching `prefix` or in `[start, end)`.
    DeleteRange {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end: Option<String>,
    },
    /// Fails the transaction with `precondition_failed` unless `key` satisfies all specified
    /// conditions.
    Check {
        key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exists: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value: Option<String>,
    },
    /// Adds `by` to the base-10 integer at `key`, treating an absent key as 0.
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

#[derive(Clone, Debug, PartialEq)]
pub struct ReadStream {
    pub stream: String,
    pub spec: serde_json::Map<String, Json>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryEvents {
    pub table: String,
    pub query: serde_json::Map<String, Json>,
}

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

json_body!(TxnRequest, TxnResponse);

/// Because operation results are untagged in JSON, deserialization stores each result as
/// [`OpResult::Raw`]; call [`OpResult::decode`] to parse it as the expected type.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum OpResult {
    Get(GetResult),
    Scan(ScanResult),
    Done(crate::Empty),
    Other(Json),
    Raw(Box<RawValue>),
}

impl OpResult {
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
    /// Value, omitted if the value is a blob larger than `json_value_max`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_b64: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<BlobMeta>,
    /// Key version if `found` is true, or `Some(None)` if the key was written earlier in the same
    /// transaction.
    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "present")]
    pub version: Option<Option<u64>>,
    /// Expiration time in milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanResult {
    pub items: Vec<ScanItem>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_b64: Option<String>,
    /// Value, omitted if `keys_only` is true or if the value is a blob larger than
    /// `json_value_max`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_b64: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<BlobMeta>,
    /// Key version, or `None` if the key was written earlier in the same transaction.
    pub version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

json_body!(GetResult, ScanResult);

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

/// Wraps a present field value, including JSON `null`, in `Some`.
fn present<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<T>, D::Error> {
    T::deserialize(d).map(Some)
}
