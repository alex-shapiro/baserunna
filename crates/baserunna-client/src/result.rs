//! Operation results.

use crate::api::{self, b64};
use crate::{Error, Op, Row};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Inline(Bytes),
    /// Blob metadata, plus the blob content if its size is at most `json_value_max`.
    Blob {
        meta: api::BlobMeta,
        data: Option<Bytes>,
    },
}

impl Value {
    /// Returns the inline value or the blob content, or `None` if the blob content was omitted.
    pub fn data(&self) -> Option<&Bytes> {
        match self {
            Value::Inline(b) | Value::Blob { data: Some(b), .. } => Some(b),
            Value::Blob { data: None, .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub key: Bytes,
    /// Committed version, or `None` if the key was written earlier in the same transaction.
    pub version: Option<u64>,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanPage {
    pub entries: Vec<Entry>,
    pub cursor: Option<String>,
}

/// Rows of a table as raw JSON, decoded by [`OpResult::rows`].
#[derive(Clone, Debug)]
pub struct RowPage {
    pub table: String,
    pub rows: Vec<Box<RawValue>>,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub seq: u64,
    /// Append time in milliseconds since the Unix epoch.
    pub time_ms: u64,
    pub headers: BTreeMap<String, String>,
    pub body: RecordBody,
}

#[derive(Clone, Debug)]
pub enum RecordBody {
    Json(Box<RawValue>),
    Bytes(Bytes),
}

impl PartialEq for RecordBody {
    fn eq(&self, other: &RecordBody) -> bool {
        match (self, other) {
            (RecordBody::Json(a), RecordBody::Json(b)) => a.get() == b.get(),
            (RecordBody::Bytes(a), RecordBody::Bytes(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for RecordBody {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordPage {
    pub records: Vec<Record>,
    /// Next `from_seq` of a forward read, or next `until_seq` of a reverse read.
    pub next_seq: u64,
    /// True if the read reached the end of its range.
    pub done: bool,
}

/// Result of an [`Op`], in the variant named after the operation.
#[derive(Clone, Debug)]
pub enum OpResult {
    Get(Option<Entry>),
    Scan(ScanPage),
    /// Result of an operation that returns no data.
    Done,
    /// Value after an `increment`.
    Increment(i64),
    /// Row of `table` as raw JSON, decoded by [`OpResult::row`].
    GetRow {
        table: String,
        row: Option<Box<RawValue>>,
    },
    QueryRows(RowPage),
    /// Whether an `update_row` or `delete_row` found its row.
    Found(bool),
    /// Number of rows that a `delete_rows` deleted.
    Count(u64),
    /// Sequence number of the first appended record, or `None` before the transaction commits.
    Append {
        first_seq: Option<u64>,
    },
    ReadStream(RecordPage),
}

impl PartialEq for OpResult {
    fn eq(&self, other: &OpResult) -> bool {
        let raw = |a: &[Box<RawValue>], b: &[Box<RawValue>]| a.iter().map(|r| r.get()).eq(b.iter().map(|r| r.get()));
        match (self, other) {
            (OpResult::Get(a), OpResult::Get(b)) => a == b,
            (OpResult::Scan(a), OpResult::Scan(b)) => a == b,
            (OpResult::Done, OpResult::Done) => true,
            (OpResult::Increment(a), OpResult::Increment(b)) => a == b,
            (OpResult::GetRow { table: t, row: a }, OpResult::GetRow { table: u, row: b }) => {
                t == u && raw(a.as_slice(), b.as_slice())
            }
            (OpResult::QueryRows(a), OpResult::QueryRows(b)) => {
                a.table == b.table && a.cursor == b.cursor && raw(&a.rows, &b.rows)
            }
            (OpResult::Found(a), OpResult::Found(b)) => a == b,
            (OpResult::Count(a), OpResult::Count(b)) => a == b,
            (OpResult::Append { first_seq: a }, OpResult::Append { first_seq: b }) => a == b,
            (OpResult::ReadStream(a), OpResult::ReadStream(b)) => a == b,
            _ => false,
        }
    }
}

fn mismatch(want: &str, got: &OpResult) -> Error {
    Error::Decode { what: format!("{want} result"), reason: format!("the result is {got:?}") }
}

fn table<R: Row>(table: &str) -> Result<(), Error> {
    match table == R::TABLE {
        true => Ok(()),
        false => Err(Error::Decode { what: format!("{table} rows"), reason: format!("read as {} rows", R::TABLE) }),
    }
}

fn decode<T: DeserializeOwned>(what: &str, r: &RawValue) -> Result<T, Error> {
    serde_json::from_str(r.get()).map_err(|e| Error::Decode { what: what.into(), reason: e.to_string() })
}

impl OpResult {
    pub fn entry(self) -> Result<Option<Entry>, Error> {
        match self {
            OpResult::Get(e) => Ok(e),
            other => Err(mismatch("get", &other)),
        }
    }

    pub fn scan_page(self) -> Result<ScanPage, Error> {
        match self {
            OpResult::Scan(p) => Ok(p),
            other => Err(mismatch("scan", &other)),
        }
    }

    /// Decodes the row of a [`Op::get_row`] on `R`.
    pub fn row<R: Row>(self) -> Result<Option<R>, Error> {
        match self {
            OpResult::GetRow { table: t, row } => {
                table::<R>(&t)?;
                row.map(|r| decode(R::TABLE, &r)).transpose()
            }
            other => Err(mismatch("get_row", &other)),
        }
    }

    /// Decodes the rows of a [`Op::query`] on `R`.
    pub fn rows<R: Row>(self) -> Result<Vec<R>, Error> {
        match self {
            OpResult::QueryRows(p) => {
                table::<R>(&p.table)?;
                p.rows.iter().map(|r| decode(R::TABLE, r)).collect()
            }
            other => Err(mismatch("query_rows", &other)),
        }
    }

    /// Decodes the JSON records of a [`Op::read_stream`].
    pub fn records<T: DeserializeOwned>(self) -> Result<Vec<T>, Error> {
        let page = match self {
            OpResult::ReadStream(p) => p,
            other => return Err(mismatch("read_stream", &other)),
        };
        let json = |r: &Record| match &r.body {
            RecordBody::Json(j) => decode("stream record", j),
            RecordBody::Bytes(_) => {
                Err(Error::Decode { what: "stream record".into(), reason: "the record is not JSON".into() })
            }
        };
        page.records.iter().map(json).collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TxnOutcome {
    /// Commit version for a write transaction, or snapshot version for a read transaction.
    pub version: u64,
    pub commit_id: Option<String>,
    /// First sequence number assigned to each `append` operation of a write, in order.
    pub appends: Vec<u64>,
    /// Per-operation results, or `None` if an earlier request with the same idempotency key
    /// already committed the transaction.
    pub results: Option<Vec<OpResult>>,
}

impl TxnOutcome {
    pub(crate) fn decode(ops: &[Op], r: api::TxnResponse) -> Result<TxnOutcome, String> {
        let results = match r.results {
            _ if r.replayed => None,
            None => return Err("missing results".into()),
            Some(rs) => Some(decode_results(ops, &rs)?),
        };
        Ok(TxnOutcome { version: r.version, commit_id: r.commit_id, appends: r.appends.unwrap_or_default(), results })
    }
}

/// Decodes one result per operation.
pub(crate) fn decode_results(ops: &[Op], results: &[api::OpResult]) -> Result<Vec<OpResult>, String> {
    if results.len() != ops.len() {
        return Err(format!("{} results for {} operations", results.len(), ops.len()));
    }
    ops.iter().zip(results).map(|(op, r)| decode_result(op, r)).collect()
}

fn decode_result(op: &Op, r: &api::OpResult) -> Result<OpResult, String> {
    Ok(match op {
        Op::Get { key } => {
            let g: api::GetResult = r.decode()?;
            OpResult::Get(match g.found {
                false => None,
                true => Some(Entry {
                    key: key.clone(),
                    version: g.version.flatten(),
                    value: value(g.value.as_deref(), g.blob)?,
                }),
            })
        }
        Op::Scan { .. } => {
            let s: api::ScanResult = r.decode()?;
            let mut entries = Vec::with_capacity(s.items.len());
            for it in s.items {
                let key = it.key.as_deref().ok_or("scan item is missing a key")?;
                entries.push(Entry {
                    key: bytes(key)?,
                    version: it.version,
                    value: value(it.value.as_deref(), it.blob)?,
                });
            }
            OpResult::Scan(ScanPage { entries, cursor: s.next_cursor })
        }
        Op::Increment { .. } => OpResult::Increment(r.decode::<api::IncrementResult>()?.value),
        Op::GetRow { table, .. } => {
            let g: api::RowResult<Box<RawValue>> = r.decode()?;
            OpResult::GetRow { table: table.clone(), row: g.row.filter(|_| g.found) }
        }
        Op::QueryRows { table, .. } => {
            let q: api::RowsResult<Box<RawValue>> = r.decode()?;
            OpResult::QueryRows(RowPage { table: table.clone(), rows: q.rows, cursor: q.next_cursor })
        }
        Op::UpdateRow { .. } | Op::DeleteRow { .. } => OpResult::Found(r.decode::<api::FoundResult>()?.found),
        Op::DeleteRows { .. } => OpResult::Count(r.decode::<api::CountResult>()?.count),
        Op::Append { .. } => OpResult::Append { first_seq: r.decode::<api::AppendResult>()?.first_seq },
        Op::ReadStream(_) => {
            let p: api::RecordsResult<Box<RawValue>> = r.decode()?;
            let records = p.records.into_iter().map(record).collect::<Result<_, _>>()?;
            OpResult::ReadStream(RecordPage { records, next_seq: p.next_seq, done: p.done })
        }
        Op::Put { .. }
        | Op::PutBlob { .. }
        | Op::Delete { .. }
        | Op::DeleteRange { .. }
        | Op::Copy { .. }
        | Op::Check { .. }
        | Op::InsertRow { .. }
        | Op::UpsertRow { .. } => {
            r.decode::<api::Empty>()?;
            OpResult::Done
        }
    })
}

fn record(r: api::StreamRecord<Box<RawValue>>) -> Result<Record, String> {
    let body = match (r.json, r.value) {
        (Some(j), None) => RecordBody::Json(j),
        (None, Some(v)) => RecordBody::Bytes(bytes(&v)?),
        _ => return Err(format!("record {} needs exactly one of json and value", r.seq)),
    };
    Ok(Record { seq: r.seq, time_ms: r.time, headers: r.headers, body })
}

pub(crate) fn bytes(s: &str) -> Result<Bytes, String> {
    b64::decode(s).map(Bytes::from).ok_or_else(|| format!("{s:?} is not base64"))
}

pub(crate) fn value(data: Option<&str>, blob: Option<api::BlobMeta>) -> Result<Value, String> {
    let data = data.map(bytes).transpose()?;
    Ok(match blob {
        Some(meta) => Value::Blob { meta, data },
        None => Value::Inline(data.unwrap_or_default()),
    })
}
