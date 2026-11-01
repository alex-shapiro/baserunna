//! Transaction operations.

use crate::OpResult;
use crate::api::{self, b64};
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value as Json;
use std::time::Duration;

/// Table row type whose fields are the table's columns.
pub trait Row: Serialize + DeserializeOwned {
    const TABLE: &'static str;
}

/// Transaction operation. Constructors that take rows or keys panic if they do not serialize to
/// JSON, such as a map with non-string keys.
#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Get {
        key: Bytes,
    },
    /// Reads up to `limit` entries in `range` in ascending order, or descending if `reverse` is
    /// true, starting after `cursor`. If `keys_only` is true, entries omit inline values and blob
    /// data.
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
    /// Copies the value of `from` to `to` without re-uploading blob data, or fails with
    /// `precondition_failed` if `from` is absent.
    Copy {
        from: Bytes,
        to: Bytes,
    },
    /// Fails the transaction with `precondition_failed` unless `key` satisfies `expect`.
    Check {
        key: Bytes,
        expect: Expect,
    },
    /// Adds `by` to the decimal integer at `key`, treating an absent key as 0.
    Increment {
        key: Bytes,
        by: i64,
    },
    /// Reads the row with primary key `pk`, a single value or an array for a composite key.
    GetRow {
        table: String,
        pk: Json,
    },
    /// Inserts `row`, failing the transaction with `already_exists` if its primary key exists.
    /// The server deletes the row `ttl_ms` milliseconds after the commit.
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
    DeleteRows {
        table: String,
        selection: api::Selection,
    },
    /// Reads up to `limit` rows in key order, or in descending order if `reverse` is true,
    /// starting after `cursor`.
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expect {
    Absent,
    Version(u64),
}

/// Stream record to append.
#[derive(Clone, Debug, PartialEq)]
pub enum NewRecord {
    Json(Json),
    Bytes(Bytes),
}

fn json(v: &impl Serialize) -> Json {
    serde_json::to_value(v).expect("rows and keys serialize to JSON")
}

/// Converts a key prefix, one value or a tuple of the leading key columns, to its column values.
fn prefix(eq: &impl Serialize) -> api::Selection {
    let eq = match json(eq) {
        // The server reads an empty prefix as every row, which must be asked for explicitly.
        Json::Array(a) if a.is_empty() => vec![Json::Null],
        Json::Array(a) => a,
        one => vec![one],
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

    /// Reads the row of `R` with primary key `pk`, a single value or a tuple for a composite key.
    pub fn get_row<R: Row>(pk: impl Serialize) -> Op {
        Op::GetRow { table: R::TABLE.into(), pk: json(&pk) }
    }

    /// Inserts `row`, failing the transaction with `already_exists` if its primary key exists.
    pub fn insert<R: Row>(row: &R) -> Op {
        Op::InsertRow { table: R::TABLE.into(), row: json(row), ttl_ms: None }
    }

    /// Inserts `row` and deletes it `ttl` after the commit.
    pub fn insert_expiring<R: Row>(row: &R, ttl: Duration) -> Op {
        let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1);
        Op::InsertRow { table: R::TABLE.into(), row: json(row), ttl_ms: Some(ttl_ms) }
    }

    pub fn upsert<R: Row>(row: &R) -> Op {
        Op::UpsertRow { table: R::TABLE.into(), row: json(row) }
    }

    /// Sets the columns that `set` serializes on the row of `R` with primary key `pk`.
    pub fn update<R: Row>(pk: impl Serialize, set: &impl Serialize) -> Op {
        Op::UpdateRow { table: R::TABLE.into(), pk: json(&pk), set: json(set) }
    }

    pub fn delete_row<R: Row>(pk: impl Serialize) -> Op {
        Op::DeleteRow { table: R::TABLE.into(), pk: json(&pk) }
    }

    /// Deletes the rows of `R` whose leading primary key columns equal `eq`, a value or a tuple. An
    /// empty or `None` prefix matches no rows.
    pub fn delete_rows<R: Row>(eq: impl Serialize) -> Op {
        Op::DeleteRows { table: R::TABLE.into(), selection: prefix(&eq) }
    }

    pub fn delete_all_rows<R: Row>() -> Op {
        Op::DeleteRows { table: R::TABLE.into(), selection: api::Selection::default() }
    }

    /// Reads the first page of rows of `R` whose leading primary key columns equal `eq`, a value
    /// or a tuple. An empty or `None` prefix matches no rows.
    pub fn query<R: Row>(eq: impl Serialize) -> Op {
        Op::query_rows::<R>(prefix(&eq))
    }

    /// Reads the first page of every row of `R`.
    pub fn query_all<R: Row>() -> Op {
        Op::query_rows::<R>(api::Selection::default())
    }

    fn query_rows<R: Row>(selection: api::Selection) -> Op {
        Op::QueryRows { table: R::TABLE.into(), selection, limit: api::PAGE_MAX, reverse: false, cursor: None }
    }

    /// Appends each of `records` as a JSON record, creating `stream` if it does not exist.
    pub fn append<T: Serialize>(stream: impl Into<String>, records: &[T]) -> Op {
        let records = records.iter().map(|r| NewRecord::Json(json(r))).collect();
        Op::Append { stream: stream.into(), records, create: true }
    }

    /// Reads the first page of records of `stream` from sequence number `from_seq`.
    pub fn read_stream(stream: impl Into<String>, from_seq: u64) -> Op {
        Op::ReadStream(api::ReadStream {
            stream: stream.into(),
            from_seq: Some(from_seq),
            limit: Some(api::PAGE_MAX),
            ..api::ReadStream::default()
        })
    }

    pub fn is_write(&self) -> bool {
        match self {
            Op::Get { .. }
            | Op::Scan { .. }
            | Op::Check { .. }
            | Op::GetRow { .. }
            | Op::QueryRows { .. }
            | Op::ReadStream(_) => false,
            Op::Put { .. }
            | Op::PutBlob { .. }
            | Op::Delete { .. }
            | Op::DeleteRange { .. }
            | Op::Copy { .. }
            | Op::Increment { .. }
            | Op::InsertRow { .. }
            | Op::UpsertRow { .. }
            | Op::UpdateRow { .. }
            | Op::DeleteRow { .. }
            | Op::DeleteRows { .. }
            | Op::Append { .. } => true,
        }
    }

    /// Returns the operation that reads the page after `result`, or `None` if `result` is the
    /// last page or this operation is not paginated.
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
        let k = |b: &Bytes| b64::encode(b);
        match self.clone() {
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
        }
    }
}

fn new_record(r: NewRecord) -> api::NewRecord {
    match r {
        NewRecord::Json(j) => api::NewRecord { json: Some(j), ..api::NewRecord::default() },
        NewRecord::Bytes(b) => api::NewRecord { value: Some(b64::encode(&b)), ..api::NewRecord::default() },
    }
}

pub(crate) fn prefix_start_end(r: &KeyRange) -> (Option<String>, Option<String>, Option<String>) {
    match r {
        KeyRange::Prefix(p) => (Some(b64::encode(p)), None, None),
        KeyRange::Span { start, end } => (None, Some(b64::encode(start)), Some(b64::encode(end))),
    }
}
