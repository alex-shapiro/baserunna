//! Stream endpoints and types.

use crate::{
    Body, Consistency, Encoding, Endpoint, NewRecord, NoBody, StreamRecord, StreamState, fill_route, is_false,
    json_body, with_query,
};
use bytes::Bytes;
use http::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

/// `GET /v1/dbs/{db}/streams`.
#[derive(Clone, Debug)]
pub struct ListStreams {
    pub db: String,
    pub query: ListStreamsQuery,
}

impl Endpoint for ListStreams {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/dbs/{db}/streams";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = StreamPage;

    fn is_idempotent(&self, _: &NoBody) -> bool {
        true
    }

    fn path(&self) -> String {
        with_query(fill_route(Self::ROUTE, &[&self.db]), &self.query)
    }
}

/// `POST /v1/dbs/{db}/streams`.
#[derive(Clone, Debug)]
pub struct CreateStream {
    pub db: String,
}

impl Endpoint for CreateStream {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/dbs/{db}/streams";
    const STATUS: StatusCode = StatusCode::CREATED;
    type Body = NewStream;
    type Response = CreatedStream;

    fn is_idempotent(&self, _: &NewStream) -> bool {
        false
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db])
    }
}

/// `GET /v1/dbs/{db}/streams/{s}`.
#[derive(Clone, Debug)]
pub struct GetStream {
    pub db: String,
    pub stream: String,
}

impl Endpoint for GetStream {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/dbs/{db}/streams/{s}";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = StreamInfo;

    fn is_idempotent(&self, _: &NoBody) -> bool {
        true
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db, &self.stream])
    }
}

/// `POST /v1/dbs/{db}/streams/{s}/records`.
#[derive(Clone, Debug)]
pub struct AppendRecords {
    pub db: String,
    pub stream: String,
}

impl Endpoint for AppendRecords {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/dbs/{db}/streams/{s}/records";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = AppendRequest;
    type Response = Appended;

    fn is_idempotent(&self, body: &AppendRequest) -> bool {
        body.idempotency_key.is_some()
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db, &self.stream])
    }
}

/// `POST /v1/dbs/{db}/streams/{s}/records` with `application/x-ndjson`.
#[derive(Clone, Debug)]
pub struct AppendLines {
    pub db: String,
    pub stream: String,
}

impl Endpoint for AppendLines {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/dbs/{db}/streams/{s}/records";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = JsonLines;
    type Response = Appended;

    fn is_idempotent(&self, _: &JsonLines) -> bool {
        false
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db, &self.stream])
    }
}

/// `GET /v1/dbs/{db}/streams/{s}/records`.
#[derive(Clone, Debug)]
pub struct ReadRecords {
    pub db: String,
    pub stream: String,
    pub query: ReadRecordsQuery,
}

impl Endpoint for ReadRecords {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/dbs/{db}/streams/{s}/records";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = RecordPage;

    fn is_idempotent(&self, _: &NoBody) -> bool {
        true
    }

    fn path(&self) -> String {
        with_query(fill_route(Self::ROUTE, &[&self.db, &self.stream]), &self.query)
    }
}

/// `POST /v1/dbs/{db}/streams/{s}/records/query`.
#[derive(Clone, Debug)]
pub struct QueryRecords {
    pub db: String,
    pub stream: String,
}

impl Endpoint for QueryRecords {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/dbs/{db}/streams/{s}/records/query";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = RecordQuery;
    type Response = RecordPage;

    fn is_idempotent(&self, _: &RecordQuery) -> bool {
        true
    }

    fn is_read_only(_: &RecordQuery) -> bool {
        true
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db, &self.stream])
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListStreamsQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consistency: Option<Consistency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_staleness_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewStream {
    pub name: String,
    /// Retention period in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_ms: Option<u64>,
    /// Maximum total record bytes retained before the oldest records are trimmed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stream {
    pub name: String,
    pub created_at: u64,
    pub retention_ms: Option<u64>,
    pub max_bytes: Option<u64>,
    #[serde(flatten)]
    pub state: StreamState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamPage {
    pub streams: Vec<Stream>,
    pub next_cursor: Option<String>,
    /// Snapshot version read by the server.
    pub version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreatedStream {
    #[serde(flatten)]
    pub stream: Stream,
    pub version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamInfo {
    #[serde(flatten)]
    pub stream: Stream,
    /// Up to 100 consumers in name order.
    pub consumers: Vec<Consumer>,
    pub consumers_next_cursor: Option<String>,
    pub version: u64,
}

/// Named read position in a stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Consumer {
    pub name: String,
    /// Next sequence number for the consumer to read.
    pub seq: u64,
    /// Number of retained records at or after `seq`.
    pub lag: u64,
    pub updated_at: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppendRequest {
    pub records: Vec<NewRecord>,
    /// Expected sequence number of the first appended record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect_seq: Option<u64>,
    /// Creates the stream if it does not exist.
    #[serde(default, skip_serializing_if = "is_false")]
    pub create: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Encoding of record `value` fields, defaulting to `utf8`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encoding: Option<Encoding>,
}

/// Newline-delimited JSON request body with content type `application/x-ndjson`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JsonLines(pub Bytes);

impl Body for JsonLines {
    const CONTENT_TYPE: Option<&'static str> = Some("application/x-ndjson");

    fn encode(&self) -> Bytes {
        self.0.clone()
    }

    fn decode(bytes: Bytes) -> Result<JsonLines, String> {
        Ok(JsonLines(bytes))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Appended {
    pub first_seq: u64,
    /// Sequence number after the last appended record.
    pub next_seq: u64,
    pub count: u64,
    /// Append time in milliseconds since the Unix epoch.
    pub time: u64,
    pub version: u64,
    pub commit_id: String,
    /// True if an earlier request with the same idempotency key already appended the records.
    pub replayed: bool,
}

/// Query parameters for [`ReadRecords`]. Specify at most one start position: `from_seq`,
/// `from_time`, `tail`, or `consumer`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadRecordsQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_time: Option<u64>,
    /// Starts at the last `tail` records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail: Option<u64>,
    /// Starts at this consumer's position.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until_time: Option<u64>,
    /// Reads backward from the end of the range, newest records first.
    #[serde(default, skip_serializing_if = "is_false")]
    pub reverse: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<usize>,
    /// Maximum number of records to scan before returning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_limit: Option<usize>,
    /// Maximum time in milliseconds to wait for new records if a forward read is empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encoding: Option<Encoding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consistency: Option<Consistency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_staleness_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RecordQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_time: Option<u64>,
    /// Starts at the last `tail` records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until_time: Option<u64>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub reverse: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<usize>,
    /// Record filter, such as `{"field": "level", "op": "eq", "value": "error"}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Json>,
    /// Maximum number of records to scan before returning a partial page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encoding: Option<Encoding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consistency: Option<Consistency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_staleness_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RecordPage {
    pub records: Vec<StreamRecord>,
    /// Next `from_seq` of a forward read, or next `until_seq` of a reverse read.
    pub next_seq: u64,
    pub first_seq: u64,
    /// Sequence number after the last record in the stream.
    pub tail_seq: u64,
    /// True if the read reached the end of its range.
    pub done: bool,
    pub version: u64,
}

json_body!(NewStream, StreamPage, CreatedStream, StreamInfo, AppendRequest, Appended, RecordQuery, RecordPage);
