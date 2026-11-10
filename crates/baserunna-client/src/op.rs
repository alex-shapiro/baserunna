//! Transaction operations.

use crate::OpResult;
pub use crate::api::Expect;
use crate::api::{self, b64};
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value as Json;
use std::collections::BTreeMap;
use std::time::Duration;

/// Table row type whose fields are the table's columns.
pub trait Row: Serialize + DeserializeOwned {
    const TABLE: &'static str;
}

/// Transaction operation.
///
/// Constructors that accept serializable rows or keys panic if JSON serialization fails.
#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Get {
        key: Bytes,
    },
    /// Scans up to `limit` entries in `range` after `cursor`.
    ///
    /// Scans in descending order if `reverse` is true. Omits inline values and blob data if
    /// `keys_only` is true.
    Scan {
        range: KeyRange,
        limit: usize,
        reverse: bool,
        keys_only: bool,
        cursor: Option<String>,
    },
    Put {
        key: Bytes,
        value: Bytes,
    },
    /// Writes a blob staged by [`Db::stage_blob`](crate::Db::stage_blob) to `key`.
    PutBlob {
        key: Bytes,
        blob_id: String,
    },
    Delete {
        key: Bytes,
    },
    DeleteRange {
        range: KeyRange,
    },
    /// Copies the value at `from` to `to`.
    ///
    /// Fails with `precondition_failed` if `from` is absent.
    Copy {
        from: Bytes,
        to: Bytes,
    },
    /// Verifies that `key` satisfies `expect`, or fails with `precondition_failed`.
    Check {
        key: Bytes,
        expect: Expect,
    },
    /// Adds `by` to the decimal integer at `key`, treating an absent key as 0.
    Increment {
        key: Bytes,
        by: i64,
    },
    /// Reads the row at primary key `pk`.
    GetRow {
        table: String,
        pk: Json,
    },
    /// Inserts `row`, or fails with `already_exists` if its primary key exists.
    ///
    /// If `ttl_ms` is set, the server deletes the row after `ttl_ms` milliseconds.
    InsertRow {
        table: String,
        row: Json,
        ttl_ms: Option<u64>,
    },
    UpsertRow {
        table: String,
        row: Json,
    },
    /// Sets the columns in `set` on the row with primary key `pk`, if it exists.
    UpdateRow {
        table: String,
        pk: Json,
        set: Json,
    },
    DeleteRow {
        table: String,
        pk: Json,
    },
    /// Updates the columns in `set` on all rows that match `selection`.
    UpdateRows {
        table: String,
        selection: api::Selection,
        set: Json,
    },
    DeleteRows {
        table: String,
        selection: api::Selection,
    },
    /// Queries up to `limit` rows that match `selection` after `cursor`.
    QueryRows {
        table: String,
        selection: api::Selection,
        limit: usize,
        reverse: bool,
        cursor: Option<String>,
    },
    /// Appends `records` to `stream`, creating the stream if `create` is true.
    Append {
        stream: String,
        records: Vec<NewRecord>,
        create: bool,
    },
    ReadStream(api::ReadStream),
    SetConsumer {
        stream: String,
        consumer: String,
        seq: u64,
    },
    TrimStream {
        stream: String,
        before_seq: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyRange {
    Prefix(Bytes),
    /// Keys in `[start, end)`.
    Span {
        start: Bytes,
        end: Bytes,
    },
}

/// Stream record to append.
#[derive(Clone, Debug, PartialEq)]
pub struct NewRecord {
    pub body: NewBody,
    pub headers: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum NewBody {
    Json(Json),
    Bytes(Bytes),
}

impl NewRecord {
    pub fn json(body: &impl Serialize) -> NewRecord {
        NewRecord { body: NewBody::Json(json(body)), headers: BTreeMap::new() }
    }

    pub fn bytes(body: impl Into<Bytes>) -> NewRecord {
        NewRecord { body: NewBody::Bytes(body.into()), headers: BTreeMap::new() }
    }

    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> NewRecord {
        self.headers.insert(name.into(), value.into());
        self
    }
}

fn json(v: &impl Serialize) -> Json {
    serde_json::to_value(v).expect("value is serializable to JSON")
}

/// Builds a selection that matches the key prefix `eq`.
fn prefix(eq: &impl Serialize) -> api::Selection {
    let eq = match json(eq) {
        // An empty `eq` array selects all rows on the server, so use `[null]` to match none.
        Json::Array(a) if a.is_empty() => vec![Json::Null],
        Json::Array(a) => a,
        val => vec![val],
    };
    api::Selection { eq, ..api::Selection::default() }
}

impl Op {
    pub fn get(key: impl Into<Bytes>) -> Op {
        Op::Get { key: key.into() }
    }

    pub fn put(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Op {
        Op::Put { key: key.into(), value: value.into() }
    }

    pub fn increment(key: impl Into<Bytes>, by: i64) -> Op {
        Op::Increment { key: key.into(), by }
    }

    /// Reads the first page of keys with `prefix`.
    pub fn scan(prefix: impl Into<Bytes>) -> Op {
        let range = KeyRange::Prefix(prefix.into());
        Op::Scan { range, limit: api::PAGE_MAX, reverse: false, keys_only: false, cursor: None }
    }

    /// Reads the row of `R` with primary key `pk`.
    pub fn get_row<R: Row>(pk: impl Serialize) -> Op {
        Op::GetRow { table: R::TABLE.into(), pk: json(&pk) }
    }

    /// Inserts `row`, or fails with `already_exists` if its primary key exists.
    pub fn insert<R: Row>(row: &R) -> Op {
        Op::InsertRow { table: R::TABLE.into(), row: json(row), ttl_ms: None }
    }

    /// Inserts `row` with a time-to-live of `ttl`.
    pub fn insert_expiring<R: Row>(row: &R, ttl: Duration) -> Op {
        let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1);
        Op::InsertRow { table: R::TABLE.into(), row: json(row), ttl_ms: Some(ttl_ms) }
    }

    pub fn upsert<R: Row>(row: &R) -> Op {
        Op::UpsertRow { table: R::TABLE.into(), row: json(row) }
    }

    /// Updates the columns in `set` on the row of `R` with primary key `pk`.
    pub fn update<R: Row>(pk: impl Serialize, set: &impl Serialize) -> Op {
        Op::UpdateRow { table: R::TABLE.into(), pk: json(&pk), set: json(set) }
    }

    pub fn delete_row<R: Row>(pk: impl Serialize) -> Op {
        Op::DeleteRow { table: R::TABLE.into(), pk: json(&pk) }
    }

    /// Deletes the rows of `R` whose primary key starts with `eq`.
    pub fn delete_rows<R: Row>(eq: impl Serialize) -> Op {
        Op::delete_where::<R>(prefix(&eq))
    }

    pub fn delete_all_rows<R: Row>() -> Op {
        Op::delete_where::<R>(api::Selection::default())
    }

    /// Deletes all rows of `R` that match `selection`.
    pub fn delete_where<R: Row>(selection: api::Selection) -> Op {
        Op::DeleteRows { table: R::TABLE.into(), selection }
    }

    /// Updates the columns in `set` on all rows of `R` that match `selection`.
    pub fn update_where<R: Row>(selection: api::Selection, set: &impl Serialize) -> Op {
        Op::UpdateRows { table: R::TABLE.into(), selection, set: json(set) }
    }

    /// Reads the first page of rows of `R` whose primary key starts with `eq`.
    pub fn query<R: Row>(eq: impl Serialize) -> Op {
        Op::query_where::<R>(prefix(&eq))
    }

    /// Reads the first page of rows in `R`.
    pub fn query_all<R: Row>() -> Op {
        Op::query_where::<R>(api::Selection::default())
    }

    /// Reads the first page of rows in `R` that match `selection`.
    pub fn query_where<R: Row>(selection: api::Selection) -> Op {
        Op::QueryRows { table: R::TABLE.into(), selection, limit: api::PAGE_MAX, reverse: false, cursor: None }
    }

    /// Appends JSON `records` to `stream`, creating the stream if needed.
    pub fn append<T: Serialize>(stream: impl Into<String>, records: &[T]) -> Op {
        Op::append_records(stream, records.iter().map(NewRecord::json).collect())
    }

    /// Appends `records` to `stream`, creating the stream if needed.
    pub fn append_records(stream: impl Into<String>, records: Vec<NewRecord>) -> Op {
        Op::Append { stream: stream.into(), records, create: true }
    }

    /// Sets the next sequence number for `consumer` on `stream`.
    pub fn set_consumer(stream: impl Into<String>, consumer: impl Into<String>, seq: u64) -> Op {
        Op::SetConsumer { stream: stream.into(), consumer: consumer.into(), seq }
    }

    /// Deletes records in `stream` before `before_seq`.
    pub fn trim_stream(stream: impl Into<String>, before_seq: u64) -> Op {
        Op::TrimStream { stream: stream.into(), before_seq }
    }

    /// Reads the first page of records in `stream` starting at `from_seq`.
    pub fn read_stream(stream: impl Into<String>, from_seq: u64) -> Op {
        Op::ReadStream(api::ReadStream {
            stream: stream.into(),
            from_seq: Some(from_seq),
            limit: Some(api::PAGE_MAX),
            ..api::ReadStream::default()
        })
    }

    /// Returns the operation for the page after `result`, or `None` if pagination is complete.
    pub fn next_page(&self, result: &OpResult) -> Option<Op> {
        let mut next = self.clone();
        match (&mut next, result) {
            (Op::Scan { cursor, .. }, OpResult::Scan(p)) => *cursor = Some(p.cursor.clone()?),
            (Op::QueryRows { cursor, .. }, OpResult::QueryRows(p)) => *cursor = Some(p.cursor.clone()?),
            (Op::ReadStream(r), OpResult::ReadStream(p)) if !p.done => {
                if r.reverse {
                    (r.until_seq, r.until_time) = (Some(p.next_seq), None);
                } else {
                    r.from_seq = Some(p.next_seq);
                    (r.from_time, r.tail, r.consumer) = (None, None, None);
                }
            }
            _ => return None,
        }
        Some(next)
    }

    pub(crate) fn to_api(&self) -> api::Op {
        self.clone().into_api()
    }

    pub(crate) fn into_api(self) -> api::Op {
        let k = |b: &Bytes| b64::encode(b);
        match self {
            Op::Get { key } => api::Op::Get { key: k(&key) },
            Op::Scan { range, limit, reverse, keys_only, cursor } => {
                let (prefix, start, end) = prefix_start_end(&range);
                api::Op::Scan { prefix, start, end, limit: Some(limit), reverse, keys_only, cursor }
            }
            Op::Put { key, value } => {
                api::Op::Put { key: k(&key), value: Some(b64::encode(&value)), blob_id: None, ttl_ms: None }
            }
            Op::PutBlob { key, blob_id } => {
                api::Op::Put { key: k(&key), value: None, blob_id: Some(blob_id), ttl_ms: None }
            }
            Op::Delete { key } => api::Op::Delete { key: k(&key) },
            Op::DeleteRange { range } => {
                let (prefix, start, end) = prefix_start_end(&range);
                api::Op::DeleteRange { prefix, start, end }
            }
            Op::Copy { from, to } => api::Op::Copy { from: k(&from), to: k(&to), ttl_ms: None },
            Op::Check { key, expect: Expect::Absent } => {
                api::Op::Check { key: k(&key), exists: Some(false), version: None, value: None }
            }
            Op::Check { key, expect: Expect::Version(v) } => {
                api::Op::Check { key: k(&key), exists: None, version: Some(v), value: None }
            }
            Op::Increment { key, by } => api::Op::Increment { key: k(&key), by, ttl_ms: None },
            Op::GetRow { table, pk } => api::Op::GetRow { table, pk },
            Op::InsertRow { table, row, ttl_ms } => api::Op::InsertRow { table, row, ttl_ms },
            Op::UpsertRow { table, row } => api::Op::UpsertRow { table, row, ttl_ms: None },
            Op::UpdateRow { table, pk, set } => api::Op::UpdateRow { table, pk, set, ttl_ms: None },
            Op::DeleteRow { table, pk } => api::Op::DeleteRow { table, pk },
            Op::UpdateRows { table, selection, set } => {
                api::Op::UpdateRows { table, selection, set, ttl_ms: None, limit: None }
            }
            Op::DeleteRows { table, selection } => api::Op::DeleteRows { table, selection, limit: None },
            Op::QueryRows { table, selection, limit, reverse, cursor } => {
                let api::Selection { index, eq, range, filter } = selection;
                api::Op::QueryRows { table, index, eq, range, filter, limit: Some(limit), reverse, cursor }
            }
            Op::Append { stream, records, create } => {
                let records = records.into_iter().map(new_record).collect();
                api::Op::Append { stream, records, expect_seq: None, create }
            }
            Op::ReadStream(r) => api::Op::ReadStream(r),
            Op::SetConsumer { stream, consumer, seq } => api::Op::SetConsumer { stream, consumer, seq },
            Op::TrimStream { stream, before_seq } => {
                api::Op::TrimStream { stream, before_seq: Some(before_seq), before_time: None }
            }
        }
    }
}

fn new_record(NewRecord { body, headers }: NewRecord) -> api::NewRecord {
    match body {
        NewBody::Json(j) => api::NewRecord { json: Some(j), value: None, headers },
        NewBody::Bytes(b) => api::NewRecord { json: None, value: Some(b64::encode(&b)), headers },
    }
}

pub(crate) fn prefix_start_end(r: &KeyRange) -> (Option<String>, Option<String>, Option<String>) {
    match r {
        KeyRange::Prefix(p) => (Some(b64::encode(p)), None, None),
        KeyRange::Span { start, end } => (None, Some(b64::encode(start)), Some(b64::encode(end))),
    }
}
