//! Change-feed requests and pages.

use crate::api;
use crate::op::prefix_start_end;
use crate::result::{bytes, value};
use crate::{KeyRange, Value};
use bytes::Bytes;

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
