//! Binary key-value operations.

use crate::api::{self, b64};
use bytes::Bytes;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Get {
        key: Bytes,
    },
    /// Reads up to `limit` entries in `range` in ascending order, or descending if `reverse` is
    /// true. If `keys_only` is true, entries omit inline values and blob data.
    Scan {
        range: KeyRange,
        limit: usize,
        reverse: bool,
        keys_only: bool,
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
        !matches!(self, Op::Get { .. } | Op::Scan { .. } | Op::Check { .. })
    }

    pub(crate) fn to_api(&self) -> api::Op {
        let k = |b: &Bytes| b64::encode(b);
        match self {
            Op::Get { key } => api::Op::Get { key: k(key) },
            Op::Scan { range, limit, reverse, keys_only } => {
                let (prefix, start, end) = prefix_start_end(range);
                api::Op::Scan {
                    prefix,
                    start,
                    end,
                    limit: Some(*limit),
                    reverse: *reverse,
                    keys_only: *keys_only,
                    cursor: None,
                }
            }
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub key: Bytes,
    pub version: u64,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanPage {
    pub entries: Vec<Entry>,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpResult {
    Get(Option<Entry>),
    Scan(ScanPage),
    Done,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnOutcome {
    /// Commit version for a write transaction, or snapshot version for a read transaction.
    pub version: u64,
    pub commit_id: Option<String>,
    /// Per-operation results, or `None` if an earlier request with the same idempotency key
    /// already committed the transaction.
    pub results: Option<Vec<OpResult>>,
}

impl TxnOutcome {
    pub(crate) fn decode(ops: &[Op], r: api::TxnResponse) -> Result<TxnOutcome, String> {
        let results = match r.results {
            _ if r.replayed => None,
            None => return Err("missing results".into()),
            Some(rs) if rs.len() != ops.len() => {
                return Err(format!("{} results for {} operations", rs.len(), ops.len()));
            }
            Some(rs) => Some(ops.iter().zip(&rs).map(|(op, r)| decode_result(op, r)).collect::<Result<_, _>>()?),
        };
        Ok(TxnOutcome { version: r.version, commit_id: r.commit_id, results })
    }
}

fn decode_result(op: &Op, r: &api::OpResult) -> Result<OpResult, String> {
    Ok(match op {
        Op::Get { key } => {
            let g: api::GetResult = r.decode()?;
            OpResult::Get(match g.found {
                false => None,
                true => Some(Entry {
                    key: key.clone(),
                    version: g.version.flatten().ok_or("get result is missing a committed version")?,
                    value: value(g.value.as_deref(), g.blob)?,
                }),
            })
        }
        Op::Scan { .. } => {
            let s: api::ScanResult = r.decode()?;
            let mut entries = Vec::with_capacity(s.items.len());
            for it in s.items {
                let key = it.key.as_deref().ok_or("scan item is missing a key")?;
                let version = it.version.ok_or("scan item is missing a committed version")?;
                entries.push(Entry { key: bytes(key)?, version, value: value(it.value.as_deref(), it.blob)? });
            }
            OpResult::Scan(ScanPage { entries, cursor: s.next_cursor })
        }
        _ => OpResult::Done,
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
