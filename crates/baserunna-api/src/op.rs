//! Transaction operations and their results.
//!
//! Keys and values are encoded as UTF-8 strings or as padded standard base64.
//! If a response uses `utf8` encoding and a byte slice is not valid UTF-8, the server omits the
//! plain field and writes base64 to the corresponding `_b64` field.

use crate::{BlobMeta, Empty, is_false, json_body, present};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as Json;
use serde_json::value::RawValue;
use std::collections::BTreeMap;

/// Maximum number of items in a page of a `scan`, `query_rows`, or `read_stream` result.
pub const PAGE_MAX: usize = 1000;

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
        selection: Selection,
        set: Json,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
    },
    DeleteRows {
        table: String,
        #[serde(rename = "where")]
        selection: Selection,
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
        range: Option<RangeSpec>,
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
        records: Vec<NewRecord>,
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
        match self {
            Op::Get { .. }
            | Op::Scan { .. }
            | Op::Check { .. }
            | Op::GetRow { .. }
            | Op::QueryRows { .. }
            | Op::ReadStream(_)
            | Op::StreamInfo { .. }
            | Op::GetConsumer { .. }
            | Op::QueryEvents(_) => false,
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
            | Op::DeleteEvents { .. } => true,
        }
    }
}

/// Rows matching an equality prefix and an optional range over the primary key or `index`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selection {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
    /// Values of the leading key columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub eq: Vec<Json>,
    /// Bounds on the key column after `eq`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<RangeSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Json>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gt: Option<Json>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gte: Option<Json>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lt: Option<Json>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lte: Option<Json>,
}

/// Stream record to append, with exactly one of `value` (in the request `encoding`) or `json`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// JSON body, or `Some(Json::Null)` for a JSON `null` body.
    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "present")]
    pub json: Option<Json>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
}

/// Reads records from `stream`, starting at no more than one of `from_seq`, `from_time`, `tail`, or
/// `consumer`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadStream {
    pub stream: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_time: Option<u64>,
    /// Starts at the last `tail` records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail: Option<u64>,
    /// Starts at this consumer's cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until_time: Option<u64>,
    /// Returns the newest records first, from the end of the range.
    #[serde(default, skip_serializing_if = "is_false")]
    pub reverse: bool,
    /// Maximum number of records, defaulting to 100 and at most [`PAGE_MAX`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Stops once the returned records hold this many body bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Json>,
    /// Examines at most this many records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryEvents {
    pub table: String,
    pub query: serde_json::Map<String, Json>,
}

impl Serialize for QueryEvents {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut m = s.serialize_map(Some(self.query.len() + 1))?;
        m.serialize_entry("table", &self.table)?;
        for (k, v) in &self.query {
            m.serialize_entry(k, v)?;
        }
        m.end()
    }
}

impl<'de> Deserialize<'de> for QueryEvents {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<QueryEvents, D::Error> {
        use serde::de::Error;
        let mut query = serde_json::Map::deserialize(d)?;
        let table = query.remove("table").ok_or_else(|| D::Error::missing_field("table"))?;
        Ok(QueryEvents { table: serde_json::from_value(table).map_err(D::Error::custom)?, query })
    }
}

/// Result of one operation.
///
/// Because results are untagged in JSON, deserialization stores each result as
/// [`OpResult::Raw`]; call [`OpResult::decode`] to parse it as the type its operation returns.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum OpResult {
    Get(GetResult),
    Scan(ScanResult),
    /// Result of `put`, `delete`, `copy`, `delete_range`, `check`, `insert_row`, `upsert_row`,
    /// `trim_stream`, and `set_consumer`.
    Done(Empty),
    Increment(IncrementResult),
    GetRow(RowResult),
    QueryRows(RowsResult),
    /// Result of `update_row` and `delete_row`.
    Found(FoundResult),
    /// Result of `update_rows`, `delete_rows`, and `insert_events`.
    Count(CountResult),
    Append(AppendResult),
    ReadStream(RecordsResult),
    StreamInfo(StreamState),
    GetConsumer(ConsumerResult),
    QueryEvents(Json),
    DeleteEvents(DeletedResult),
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncrementResult {
    pub value: i64,
}

/// Row read by `get_row`, with each row type `R` decoding the row object.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RowResult<R = Json> {
    pub found: bool,
    #[serde(default = "none", skip_serializing_if = "Option::is_none")]
    pub row: Option<R>,
    /// Expiration time in milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RowsResult<R = Json> {
    pub rows: Vec<R>,
    /// `cursor` for the next page, or `None` if this is the last page.
    pub next_cursor: Option<String>,
}

fn none<T>() -> Option<T> {
    None
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FoundResult {
    pub found: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountResult {
    pub count: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendResult {
    pub count: u64,
    /// Sequence number of the first appended record, set once the transaction commits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_seq: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RecordsResult<J = Json> {
    pub records: Vec<StreamRecord<J>>,
    /// Next `from_seq` of a forward read, or next `until_seq` of a reverse read.
    pub next_seq: u64,
    /// True if the read reached the end of its range.
    pub done: bool,
}

/// Stream record with exactly one of `json`, `value`, or `value_b64`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(bound(deserialize = "J: Deserialize<'de>"))]
pub struct StreamRecord<J = Json> {
    pub seq: u64,
    /// Append time in milliseconds since the Unix epoch.
    pub time: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    #[serde(default = "none", skip_serializing_if = "Option::is_none", deserialize_with = "present")]
    pub json: Option<J>,
    /// Text body in the request `encoding`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Base64 body if the request `encoding` is `utf8` and the body is not valid UTF-8.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_b64: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamState {
    pub first_seq: u64,
    pub next_seq: u64,
    pub bytes: u64,
    /// Append time of the first retained record, or `None` if the stream is empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_time: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_time: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerResult {
    pub found: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletedResult {
    pub deleted: u64,
}

json_body!(GetResult, ScanResult);
