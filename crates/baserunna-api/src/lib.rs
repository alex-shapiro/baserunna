//! HTTP API request and response types.
//!
//! If the API changes only by addition, different releases of the server and the client stay
//! compatible: response types ignore unknown fields, and new request fields must be optional.

#![forbid(unsafe_code)]

mod account;
mod admin;
pub mod b64;
mod db;
mod error;
mod kv;
mod limits;
mod op;
mod table;
mod txn;

pub use account::{
    AddMember, CreateOrg, Credential, Credentials, GetMe, LogIn, Me, Member, NewMember, NewOrg, Org, RemoveMember,
    Role, Session, SignUp, User,
};
pub use admin::{CommitMetrics, Cpu, GetMetrics, Histogram, Metrics, ResetMetrics};
pub use db::{CreateDb, Db, NewDb};
pub use error::{ApiError, ErrorBody, ErrorCode, ErrorDetails, Outcome};
pub use kv::{BlobMeta, Change, ChangePage, Changes, ChangesQuery, DeletedRange, ReadBlob, StageBlob};
pub use limits::{GetLimits, Limits};
pub use op::{
    AppendResult, ConsumerResult, CountResult, DeletedResult, FoundResult, GetResult, IncrementResult, NewRecord, Op,
    OpResult, PAGE_MAX, QueryEvents, RangeSpec, ReadStream, RecordsResult, RowResult, RowsResult, ScanItem, ScanResult,
    Selection, StreamRecord, StreamState,
};
pub use table::{
    Column, ColumnType, CreateTable, CreatedTable, GetTable, GetTableQuery, Index, IndexState, ListTables,
    ListTablesQuery, NewIndex, NewTable, Table, TablePage,
};
pub use txn::{
    AbortTxn, BeginRequest, BeginTxn, Begun, CommitRequest, CommitStatus, CommitTxn, Committed, Consistency, Encoding,
    GetCommit, Isolation, OpsRequest, OpsResponse, Txn, TxnOps, TxnRequest, TxnResponse,
};

use bytes::Bytes;
use http::{Method, StatusCode};
use serde::{Deserialize, Deserializer, Serialize};

/// An HTTP route and its request and response types.
pub trait Endpoint {
    const METHOD: Method;
    /// Route in axum syntax, with a `{name}` segment for each path parameter.
    const ROUTE: &'static str;
    const STATUS: StatusCode;
    type Body: Body;
    type Response: Body;

    /// Returns true if repeating a successful request has no effect.
    fn is_idempotent(body: &Self::Body) -> bool;

    /// Returns true if the request ends its transaction even if it fails with `conflict`, so that
    /// only a new transaction can succeed.
    fn conflict_ends_txn() -> bool {
        false
    }

    fn path(&self) -> String;
}

/// Request or response body encoding.
pub trait Body: Sized {
    /// `Content-Type` header value, or `None` if the body is empty.
    const CONTENT_TYPE: Option<&'static str>;

    fn encode(&self) -> Bytes;

    fn decode(b: Bytes) -> Result<Self, String>;
}

/// Empty request or response body.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoBody {}

impl Body for NoBody {
    const CONTENT_TYPE: Option<&'static str> = None;

    fn encode(&self) -> Bytes {
        Bytes::new()
    }

    fn decode(_: Bytes) -> Result<NoBody, String> {
        Ok(NoBody {})
    }
}

/// Binary `application/octet-stream` body.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Octets(pub Bytes);

impl Body for Octets {
    const CONTENT_TYPE: Option<&'static str> = Some("application/octet-stream");

    fn encode(&self) -> Bytes {
        self.0.clone()
    }

    fn decode(b: Bytes) -> Result<Octets, String> {
        Ok(Octets(b))
    }
}

/// Empty `{}` JSON response body.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Empty {}

/// Implements [`Body`] as `application/json` for each type.
macro_rules! json_body {
    ($($t:ty),* $(,)?) => {$(
        impl $crate::Body for $t {
            const CONTENT_TYPE: Option<&'static str> = Some("application/json");

            fn encode(&self) -> bytes::Bytes {
                serde_json::to_vec(self).expect("API types serialize to JSON").into()
            }

            fn decode(b: bytes::Bytes) -> Result<Self, String> {
                serde_json::from_slice(&b).map_err(|e| e.to_string())
            }
        }
    )*};
}
pub(crate) use json_body;

json_body!(Empty, ErrorBody);

/// Replaces each `{name}` segment in `route` with the corresponding percent-encoded value from
/// `params`.
///
/// Panics if `params.len()` does not equal the number of `{name}` segments.
pub fn fill_route(route: &str, params: &[&str]) -> String {
    let mut out = String::with_capacity(route.len() + params.iter().map(|p| p.len()).sum::<usize>());
    let mut params = params.iter();
    for (i, seg) in route.split('/').enumerate() {
        if i > 0 {
            out.push('/');
        }
        if seg.starts_with('{') && seg.ends_with('}') {
            let p = params.next().unwrap_or_else(|| panic!("{route} needs more parameters"));
            percent_encode(p, &mut out);
        } else {
            out.push_str(seg);
        }
    }
    assert!(params.next().is_none(), "{route} has fewer parameters than given");
    out
}

/// Appends `s` to `out`, percent-encoding all bytes except RFC 3986 unreserved characters.
pub fn percent_encode(s: &str, out: &mut String) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX[usize::from(b >> 4)] as char);
            out.push(HEX[usize::from(b & 15)] as char);
        }
    }
}

pub(crate) fn with_query<Q: Serialize>(mut path: String, query: &Q) -> String {
    let q = serde_urlencoded::to_string(query).expect("API query types serialize");
    if !q.is_empty() {
        path.push('?');
        path.push_str(&q);
    }
    path
}

pub(crate) fn is_false(b: &bool) -> bool {
    !*b
}

pub(crate) fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    *t == T::default()
}

/// Wraps a present field value, including JSON `null`, in `Some`.
pub(crate) fn present<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<T>, D::Error> {
    T::deserialize(d).map(Some)
}

#[cfg(test)]
mod tests;
