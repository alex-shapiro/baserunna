//! Raw key-value, change-feed, and blob types.
//!
//! Keys and values are encoded as UTF-8 strings or as padded standard base64.
//! If a response uses `utf8` encoding and a byte slice is not valid UTF-8, the server omits the
//! plain field and writes base64 to the corresponding `_b64` field.

use crate::{
    Body, Consistency, Encoding, Endpoint, NoBody, Octets, fill_route, is_false, json_body, percent_encode, present,
    with_query,
};
use bytes::Bytes;
use http::header::{CONTENT_RANGE, CONTENT_TYPE, HeaderMap, HeaderValue, IF_MATCH, IF_NONE_MATCH, RANGE};
use http::{Method, StatusCode};
use serde::{Deserialize, Deserializer, Serialize};

/// `GET /v1/dbs/{db}/changes`.
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

    fn is_idempotent(&self, _: &Self::Body) -> bool {
        true
    }

    fn path(&self) -> String {
        with_query(fill_route(Self::ROUTE, &[&self.db]), &self.query)
    }
}

/// `GET /v1/dbs/{db}/kv/{*key}`.
#[derive(Clone, Debug)]
pub struct GetValue {
    pub db: String,
    pub key: Bytes,
    pub range: Option<ByteRange>,
}

impl Endpoint for GetValue {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/dbs/{db}/kv/{*key}";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = KvValue;

    fn is_idempotent(&self, _: &NoBody) -> bool {
        true
    }

    fn path(&self) -> String {
        kv_path(&self.db, &self.key)
    }

    fn headers(&self) -> HeaderMap {
        range_header(self.range)
    }

    fn response(head: &http::response::Parts, body: Bytes) -> Result<KvValue, String> {
        let headers = &head.headers;
        let version = header_u64(headers, "x-baserunna-version")?.ok_or("missing x-baserunna-version header")?;
        let expires_at = header_u64(headers, "x-baserunna-expires-at")?;
        Ok(KvValue { content: Content::new(headers, body), version, expires_at })
    }
}

/// `PUT /v1/dbs/{db}/kv/{*key}`.
#[derive(Clone, Debug)]
pub struct PutValue {
    pub db: String,
    pub key: Bytes,
    pub options: PutOptions,
    /// Deduplication key for the write. The server applies the write at most once per key.
    pub idempotency_key: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PutOptions {
    /// `Content-Type` stored if the value exceeds `value_inline_max` and is written as a blob.
    /// Inline values are served as `application/octet-stream`.
    pub content_type: Option<String>,
    /// Time-to-live in milliseconds.
    pub ttl_ms: Option<u64>,
    /// Precondition on the current state of the key.
    pub expect: Option<Expect>,
}

impl Endpoint for PutValue {
    const METHOD: Method = Method::PUT;
    const ROUTE: &'static str = "/v1/dbs/{db}/kv/{*key}";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = Octets;
    type Response = Written;

    fn is_idempotent(&self, _: &Octets) -> bool {
        self.headers().contains_key("idempotency-key")
    }

    fn path(&self) -> String {
        let mut path = kv_path(&self.db, &self.key);
        if let Some(t) = self.options.ttl_ms {
            path.push_str(&format!("?ttl_ms={t}"));
        }
        path
    }

    fn headers(&self) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(ct) = self.options.content_type.as_deref().and_then(|c| HeaderValue::from_str(c).ok()) {
            h.insert(CONTENT_TYPE, ct);
        }
        match self.options.expect {
            Some(Expect::Absent) => h.insert(IF_NONE_MATCH, HeaderValue::from_static("*")),
            Some(Expect::Version(v)) => h.insert(IF_MATCH, HeaderValue::from(v)),
            None => None,
        };
        if let Some(k) = self.idempotency_key.as_deref().and_then(|k| HeaderValue::from_str(k).ok()) {
            h.insert("idempotency-key", k);
        }
        h
    }
}

/// Key precondition for [`PutValue`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expect {
    Absent,
    /// The key exists at this version.
    Version(u64),
}

/// Byte range for a partial value or blob read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteRange {
    /// Bytes `start..=end`.
    Span { start: u64, end: u64 },
    /// Bytes from `start` to the end.
    From(u64),
    /// The last `n` bytes.
    Last(u64),
}

/// Value and metadata returned by [`GetValue`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvValue {
    pub content: Content,
    pub version: u64,
    /// Expiration time in milliseconds since the Unix epoch.
    pub expires_at: Option<u64>,
}

/// Payload and content headers of a value or blob read.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Content {
    pub data: Bytes,
    pub content_type: Option<String>,
    /// `Content-Range` of a range read, such as `bytes 0-99/1000`.
    pub content_range: Option<String>,
}

impl Content {
    fn new(h: &HeaderMap, data: Bytes) -> Content {
        let text = |name| h.get(name).and_then(|v: &HeaderValue| v.to_str().ok()).map(str::to_string);
        Content { data, content_type: text(CONTENT_TYPE), content_range: text(CONTENT_RANGE) }
    }
}

impl Body for Content {
    const CONTENT_TYPE: Option<&'static str> = None;

    fn encode(&self) -> Bytes {
        self.data.clone()
    }

    fn decode(data: Bytes) -> Result<Content, String> {
        Ok(Content { data, ..Content::default() })
    }
}

impl Body for KvValue {
    const CONTENT_TYPE: Option<&'static str> = None;

    fn encode(&self) -> Bytes {
        self.content.data.clone()
    }

    fn decode(_: Bytes) -> Result<KvValue, String> {
        Err("KvValue must be decoded with Endpoint::response".into())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Written {
    pub version: u64,
}

fn kv_path(db: &str, key: &[u8]) -> String {
    let mut path = fill_route("/v1/dbs/{db}/kv/", &[db]);
    percent_encode(key, &mut path);
    path
}

fn range_header(range: Option<ByteRange>) -> HeaderMap {
    let mut h = HeaderMap::new();
    let value = match range {
        None => return h,
        Some(ByteRange::Span { start, end }) => format!("bytes={start}-{end}"),
        Some(ByteRange::From(start)) => format!("bytes={start}-"),
        Some(ByteRange::Last(n)) => format!("bytes=-{n}"),
    };
    h.insert(RANGE, HeaderValue::from_str(&value).expect("ranges are ASCII"));
    h
}

fn header_u64(h: &HeaderMap, name: &str) -> Result<Option<u64>, String> {
    let Some(v) = h.get(name) else { return Ok(None) };
    v.to_str().ok().and_then(|s| s.trim_matches('"').parse().ok()).map(Some).ok_or(format!("bad {name} header"))
}

/// `POST /v1/dbs/{db}/blobs`.
#[derive(Clone, Debug)]
pub struct StageBlob {
    pub db: String,
    /// `Content-Type` returned when the blob is read, defaulting to `application/octet-stream`.
    pub content_type: Option<String>,
}

impl Endpoint for StageBlob {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/dbs/{db}/blobs";
    const STATUS: StatusCode = StatusCode::CREATED;
    type Body = Octets;
    type Response = BlobMeta;

    fn is_idempotent(&self, _: &Self::Body) -> bool {
        true
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db])
    }

    fn headers(&self) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(ct) = self.content_type.as_deref().and_then(|c| HeaderValue::from_str(c).ok()) {
            h.insert(CONTENT_TYPE, ct);
        }
        h
    }
}

/// `GET /v1/dbs/{db}/blobs/{blob_id}`.
#[derive(Clone, Debug)]
pub struct ReadBlob {
    pub db: String,
    pub blob_id: String,
    pub range: Option<ByteRange>,
}

impl Endpoint for ReadBlob {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/dbs/{db}/blobs/{blob_id}";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = Content;

    fn is_idempotent(&self, _: &Self::Body) -> bool {
        true
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db, &self.blob_id])
    }

    fn headers(&self) -> HeaderMap {
        range_header(self.range)
    }

    fn response(head: &http::response::Parts, body: Bytes) -> Result<Content, String> {
        Ok(Content::new(&head.headers, body))
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

json_body!(BlobMeta, Written);

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
    #[serde(default, skip_serializing_if = "is_false", deserialize_with = "deserialize_bool_param")]
    pub keys_only: bool,
    /// Maximum number of changes, defaulting to 100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Maximum time in milliseconds to wait for new changes if the page is empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consistency: Option<Consistency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_staleness_ms: Option<u64>,
}

fn deserialize_bool_param<'de, D: Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
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
