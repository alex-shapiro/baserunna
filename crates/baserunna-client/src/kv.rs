//! Binary key-value operations.

use crate::api::{self, b64};
use bytes::Bytes;

#[derive(Clone, Debug, PartialEq, Eq)]
/// An operation of a [`Db::txn`](crate::Db::txn). Reads use
/// [`Db::get_many`](crate::Db::get_many) and [`Db::scan`](crate::Db::scan).
pub enum Op {
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
    /// Adds `by` to the ASCII decimal integer stored at `key`, treating a missing key as 0, and
    /// returns the new value as [`OpResult::Counter`]. If `ttl_ms` is `None`, the key keeps its
    /// current expiration.
    Increment {
        key: Bytes,
        by: i64,
        ttl_ms: Option<u64>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expect {
    Absent,
    Version(u64),
}

impl Op {
    pub fn is_write(&self) -> bool {
        !matches!(self, Op::Check { .. })
    }

    pub(crate) fn to_api(&self) -> api::Op {
        let k = |b: &Bytes| b64::encode(b);
        match self {
            Op::Put { key, value } => {
                api::Op::Put { key: k(key), value: Some(b64::encode(value)), blob_id: None, ttl_ms: None }
            }
            Op::PutBlob { key, blob_id } => {
                api::Op::Put { key: k(key), value: None, blob_id: Some(blob_id.clone()), ttl_ms: None }
            }
            Op::Delete { key } => api::Op::Delete { key: k(key) },
            Op::DeleteRange { range } => {
                let (prefix, start, end) = prefix_start_end(range);
                api::Op::DeleteRange { prefix, start, end }
            }
            Op::Copy { from, to } => api::Op::Copy { from: k(from), to: k(to), ttl_ms: None },
            Op::Check { key, expect: Expect::Absent } => {
                api::Op::Check { key: k(key), exists: Some(false), version: None, value: None }
            }
            Op::Check { key, expect: Expect::Version(v) } => {
                api::Op::Check { key: k(key), exists: None, version: Some(*v), value: None }
            }
            Op::Increment { key, by, ttl_ms } => api::Op::Increment { key: k(key), by: *by, ttl_ms: *ttl_ms },
        }
    }
}

/// Reads up to `limit` entries in `range` in ascending order, or descending if `reverse` is true.
/// If `keys_only` is true, entries omit inline values and blob data. To read the next page, repeat
/// the scan with `cursor` set to [`CommittedPage::cursor`] of the previous page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanRequest {
    pub range: KeyRange,
    pub limit: usize,
    pub reverse: bool,
    pub keys_only: bool,
    pub cursor: Option<String>,
}

impl ScanRequest {
    pub(crate) fn to_api(&self) -> api::Op {
        let (prefix, start, end) = prefix_start_end(&self.range);
        api::Op::Scan {
            prefix,
            start,
            end,
            limit: Some(self.limit),
            reverse: self.reverse,
            keys_only: self.keys_only,
            cursor: self.cursor.clone(),
        }
    }
}

fn prefix_start_end(r: &KeyRange) -> (Option<String>, Option<String>, Option<String>) {
    match r {
        KeyRange::Prefix(p) => (Some(b64::encode(p)), None, None),
        KeyRange::Span { start, end } => (None, Some(b64::encode(start)), Some(b64::encode(end))),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Inline(Bytes),
    /// Blob metadata, plus the blob content if its size is at most `json_value_max`.
    Blob {
        meta: api::BlobMeta,
        data: Option<Bytes>,
    },
}

/// A value that a read-only snapshot returned. Every value in a snapshot is committed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedEntry {
    pub key: Bytes,
    /// Commit version of the value.
    pub version: u64,
    pub value: Value,
}

const UNCOMMITTED: &str = "a read-only transaction returned an uncommitted value";

/// One page of a [`Db::scan`](crate::Db::scan).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedPage {
    /// Version of the snapshot that the scan read.
    pub version: u64,
    pub entries: Vec<CommittedEntry>,
    /// Opaque position after the last entry, or `None` if the range has no more entries.
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpResult {
    /// The new value of an [`Op::Increment`].
    Counter(i64),
    Done,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnOutcome {
    /// Commit version for a write transaction, or snapshot version for a read transaction.
    pub version: u64,
    pub commit_id: Option<String>,
    pub results: TxnResults,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxnResults {
    /// The request executed the operations. Holds one result per operation, in order:
    /// [`OpResult::Counter`] for [`Op::Increment`], and [`OpResult::Done`] for the others.
    Executed(Vec<OpResult>),
    /// An earlier request with the same idempotency key committed the transaction at `version`.
    /// The server does not keep the results of that request. Only a write transaction with an
    /// idempotency key can be replayed.
    Replayed,
}

impl TxnResults {
    /// The results of an executed transaction, or `None` if it was replayed.
    pub fn executed(self) -> Option<Vec<OpResult>> {
        match self {
            TxnResults::Executed(r) => Some(r),
            TxnResults::Replayed => None,
        }
    }
}

impl TxnOutcome {
    pub(crate) fn decode(ops: &[Op], r: api::TxnResponse) -> Result<TxnOutcome, String> {
        let results = match r.replayed {
            true => TxnResults::Replayed,
            false => TxnResults::Executed(
                ops.iter()
                    .zip(&executed(r.results, ops.len())?)
                    .map(|(op, r)| decode_result(op, r))
                    .collect::<Result<_, _>>()?,
            ),
        };
        Ok(TxnOutcome { version: r.version, commit_id: r.commit_id, results })
    }
}

/// The results of a transaction that executed `ops` operations.
pub(crate) fn executed(results: Option<Vec<api::OpResult>>, ops: usize) -> Result<Vec<api::OpResult>, String> {
    match results {
        None => Err("missing results".into()),
        Some(rs) if rs.len() != ops => Err(format!("{} results for {ops} operations", rs.len())),
        Some(rs) => Ok(rs),
    }
}

/// Decodes the result of a read-only get of `key`.
pub(crate) fn committed_get(key: &Bytes, r: &api::OpResult) -> Result<Option<CommittedEntry>, String> {
    let g: api::GetResult = r.decode()?;
    if !g.found {
        return Ok(None);
    }
    Ok(Some(CommittedEntry {
        key: key.clone(),
        version: g.version.ok_or("get result is missing its version")?.ok_or(UNCOMMITTED)?,
        value: value(g.value.as_deref(), g.blob)?,
    }))
}

/// Decodes the result of a read-only scan in the snapshot at `version`.
pub(crate) fn committed_page(version: u64, r: &api::OpResult) -> Result<CommittedPage, String> {
    let s: api::ScanResult = r.decode()?;
    let mut entries = Vec::with_capacity(s.items.len());
    for it in s.items {
        let key = it.key.as_deref().ok_or("scan item is missing a key")?;
        let v = it.version.ok_or(UNCOMMITTED)?;
        entries.push(CommittedEntry { key: bytes(key)?, version: v, value: value(it.value.as_deref(), it.blob)? });
    }
    Ok(CommittedPage { version, entries, cursor: s.next_cursor })
}

fn decode_result(op: &Op, r: &api::OpResult) -> Result<OpResult, String> {
    Ok(match op {
        Op::Increment { .. } => OpResult::Counter(r.decode::<api::IncrementResult>()?.value),
        Op::Put { .. }
        | Op::PutBlob { .. }
        | Op::Delete { .. }
        | Op::DeleteRange { .. }
        | Op::Copy { .. }
        | Op::Check { .. } => {
            r.decode::<api::Empty>()?;
            OpResult::Done
        }
    })
}

fn bytes(s: &str) -> Result<Bytes, String> {
    b64::decode(s).map(Bytes::from).ok_or_else(|| format!("{s:?} is not base64"))
}

fn value(data: Option<&str>, blob: Option<api::BlobMeta>) -> Result<Value, String> {
    let data = data.map(bytes).transpose()?;
    Ok(match blob {
        Some(meta) => Value::Blob { meta, data },
        None => Value::Inline(data.unwrap_or_default()),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangesRequest {
    pub range: KeyRange,
    /// `cursor` from the previous page, or `"0"` for the start of retained history.
    pub after: String,
    pub limit: usize,
    /// Maximum time in milliseconds to wait if no changes are available, up to `long_poll_max_ms`.
    pub wait_ms: u64,
    /// Maximum snapshot staleness in milliseconds, or `None` for a strong read.
    pub max_staleness_ms: Option<u64>,
}

impl ChangesRequest {
    pub(crate) fn to_api(&self) -> api::ChangesQuery {
        let (prefix, start, end) = prefix_start_end(&self.range);
        api::ChangesQuery {
            after: self.after.clone(),
            encoding: Some(api::Encoding::Base64),
            prefix,
            start,
            end,
            limit: Some(self.limit),
            wait_ms: (self.wait_ms > 0).then_some(self.wait_ms),
            consistency: self.max_staleness_ms.map(|_| api::Consistency::Snapshot),
            max_staleness_ms: self.max_staleness_ms,
            ..api::ChangesQuery::default()
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    Put {
        key: Bytes,
        version: u64,
        value: Value,
    },
    Delete {
        key: Bytes,
        version: u64,
    },
    /// Range deletion over `[start, end)`, or `[start, ..)` if `end` is `None`.
    DeleteRange {
        start: Bytes,
        end: Option<Bytes>,
        version: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangePage {
    pub changes: Vec<Change>,
    /// True if more changes are immediately available after `cursor`.
    pub more: bool,
    /// Cursor for `after` in the next request.
    pub cursor: String,
}

impl ChangePage {
    pub(crate) fn decode(p: api::ChangePage) -> Result<ChangePage, String> {
        let mut changes = Vec::with_capacity(p.changes.len());
        for c in p.changes {
            changes.push(match (c.deleted_range, c.key) {
                (Some(r), _) => Change::DeleteRange {
                    start: bytes(r.start.as_deref().ok_or("deleted range is missing a start")?)?,
                    end: r.end.flatten().as_deref().map(bytes).transpose()?,
                    version: c.version,
                },
                (None, None) => return Err("change is missing a key".into()),
                (None, Some(k)) if c.deleted => Change::Delete { key: bytes(&k)?, version: c.version },
                (None, Some(k)) => {
                    Change::Put { key: bytes(&k)?, version: c.version, value: value(c.value.as_deref(), c.blob)? }
                }
            });
        }
        Ok(ChangePage { changes, more: p.more, cursor: p.next_cursor })
    }
}
