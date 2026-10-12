//! Engine-level optimistic transactions: a snapshot, a read set, and a
//! buffered write set. Reads see the transaction's own writes.

use super::types::{Entry, Seq, Value};
use super::version::{KeyRange, Version};
use crate::error::{Error, Result};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Isolation {
    #[default]
    Serializable,
    Snapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadItem {
    Key(Bytes),
    Range(KeyRange),
}

/// What a transaction asks the committer to apply.
#[derive(Clone, Debug, Default)]
pub struct WriteSet {
    /// `None` = delete.
    pub puts: BTreeMap<Bytes, Option<Value>>,
    /// Range deletes, applied to committed state before `puts`.
    pub range_deletes: Vec<KeyRange>,
}

impl WriteSet {
    pub fn is_empty(&self) -> bool {
        self.puts.is_empty() && self.range_deletes.is_empty()
    }

    pub fn touches_key(&self, k: &[u8]) -> bool {
        self.puts.contains_key(k) || self.range_deletes.iter().any(|r| r.contains(k))
    }

    pub fn touches_range(&self, r: &KeyRange) -> bool {
        self.puts.keys().any(|k| r.contains(k)) || self.range_deletes.iter().any(|d| d.intersects(r))
    }

    pub fn blob_ids(&self) -> Vec<String> {
        self.puts
            .values()
            .filter_map(|v| match v {
                Some(Value::Blob(b)) => Some(b.id.clone()),
                _ => None,
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct CommitRequest {
    pub commit_id: u128,
    /// The committer refuses (without applying) after this instant, which
    /// bounds how long a request can linger server-side. `None` = the
    /// engine's `commit_timeout_ms` after enqueueing.
    pub deadline: Option<tokio::time::Instant>,
    pub read_version: Seq,
    pub isolation: Isolation,
    pub reads: Vec<ReadItem>,
    /// Reads validated under every isolation level (see [`Txn::get_pinned`]).
    pub pinned_reads: Vec<Bytes>,
    pub writes: WriteSet,
}

/// A value as seen by a transaction read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadValue {
    pub key: Bytes,
    pub value: Value,
    /// `None` if the value was written by this (uncommitted) transaction.
    pub seq: Option<Seq>,
}

pub struct Txn {
    pub snapshot: Arc<Version>,
    pub isolation: Isolation,
    pub reads: Vec<ReadItem>,
    pinned_reads: Vec<Bytes>,
    pub writes: WriteSet,
    pub commit_id: u128,
    write_bytes: usize,
    max_write_bytes: usize,
}

impl Txn {
    pub fn new(snapshot: Arc<Version>, isolation: Isolation, commit_id: u128, max_write_bytes: usize) -> Txn {
        Txn {
            snapshot,
            isolation,
            reads: Vec::new(),
            pinned_reads: Vec::new(),
            writes: WriteSet::default(),
            commit_id,
            write_bytes: 0,
            max_write_bytes,
        }
    }

    pub fn read_version(&self) -> Seq {
        self.snapshot.version()
    }

    pub fn is_read_only(&self) -> bool {
        self.writes.is_empty()
    }

    fn masked(&self, k: &[u8]) -> bool {
        self.writes.range_deletes.iter().any(|r| r.contains(k))
    }

    /// Point read (records the read).
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<ReadValue>> {
        if let Some(v) = self.writes.puts.get(key) {
            return Ok(v.clone().map(|value| ReadValue { key: Bytes::copy_from_slice(key), value, seq: None }));
        }
        self.reads.push(ReadItem::Key(Bytes::copy_from_slice(key)));
        if self.masked(key) {
            return Ok(None);
        }
        Ok(self.snapshot.get(key).await?.map(|e| ReadValue { key: e.key, value: e.value, seq: Some(e.seq) }))
    }

    /// Point read that the commit validates even under snapshot isolation:
    /// for data the transaction's writes are derived from, such as a table
    /// schema that decides which index entries a row write maintains.
    pub async fn get_pinned(&mut self, key: &[u8]) -> Result<Option<ReadValue>> {
        self.pinned_reads.push(Bytes::copy_from_slice(key));
        self.get(key).await
    }

    /// Range read with read-your-writes. Records the interval actually
    /// covered (up to the last returned key when `limit` is hit).
    pub async fn scan(&mut self, range: &KeyRange, reverse: bool, limit: usize) -> Result<Vec<ReadValue>> {
        let mut out: Vec<ReadValue> = Vec::new();
        if limit == 0 || range.is_empty() {
            return Ok(out);
        }
        let batch = limit.clamp(32, 1024);
        let mut window = range.clone();
        loop {
            let snap = self.snapshot.scan(&window, reverse, batch, false).await?;
            // Where this round stopped, unless it exhausted the window.
            let last = snap.last().filter(|_| snap.len() == batch).map(|e| e.key.clone());
            let examined = match &last {
                None => window.clone(),
                Some(k) => {
                    if reverse {
                        KeyRange::new(Bound::Included(k.clone()), window.end.clone())
                    } else {
                        KeyRange::new(window.start.clone(), Bound::Included(k.clone()))
                    }
                }
            };
            let mut merged: BTreeMap<Bytes, ReadValue> = BTreeMap::new();
            for e in snap {
                if self.writes.puts.contains_key(&e.key) || self.masked(&e.key) {
                    continue;
                }
                merged.insert(e.key.clone(), ReadValue { key: e.key, value: e.value, seq: Some(e.seq) });
            }
            for (k, v) in self.writes.puts.range::<Bytes, _>((examined.start.clone(), examined.end.clone())) {
                if let Some(v) = v {
                    merged.insert(k.clone(), ReadValue { key: k.clone(), value: v.clone(), seq: None });
                }
            }
            let items: Vec<ReadValue> =
                if reverse { merged.into_values().rev().collect() } else { merged.into_values().collect() };
            for it in items {
                if out.len() >= limit {
                    break;
                }
                out.push(it);
            }
            if out.len() >= limit {
                let last = out[out.len() - 1].key.clone();
                let covered = if reverse {
                    KeyRange::new(Bound::Included(last), range.end.clone())
                } else {
                    KeyRange::new(range.start.clone(), Bound::Included(last))
                };
                self.reads.push(ReadItem::Range(covered));
                return Ok(out);
            }
            let Some(k) = last else {
                self.reads.push(ReadItem::Range(range.clone()));
                return Ok(out);
            };
            // Continue after the examined part.
            window = if reverse {
                KeyRange::new(window.start.clone(), Bound::Excluded(k))
            } else {
                KeyRange::new(Bound::Excluded(k), window.end.clone())
            };
        }
    }

    fn account(&mut self, n: usize) -> Result<()> {
        self.write_bytes += n;
        if self.write_bytes > self.max_write_bytes {
            return Err(Error::TooLarge(format!("transaction writes exceed {} bytes", self.max_write_bytes)));
        }
        Ok(())
    }

    pub fn put(&mut self, key: Bytes, value: Value) -> Result<()> {
        self.account(key.len() + value.size())?;
        self.writes.puts.insert(key, Some(value));
        Ok(())
    }

    pub fn delete(&mut self, key: Bytes) -> Result<()> {
        self.account(key.len() + 8)?;
        self.writes.puts.insert(key, None);
        Ok(())
    }

    pub fn delete_range(&mut self, range: KeyRange) -> Result<()> {
        if range.is_empty() {
            return Ok(());
        }
        self.account(64)?;
        let doomed: Vec<Bytes> = self.writes.puts.keys().filter(|k| range.contains(k)).cloned().collect();
        for k in doomed {
            self.writes.puts.remove(&k);
        }
        self.writes.range_deletes.push(range);
        Ok(())
    }

    pub fn into_request(self) -> CommitRequest {
        CommitRequest {
            commit_id: self.commit_id,
            deadline: None,
            read_version: self.snapshot.version(),
            isolation: self.isolation,
            reads: self.reads,
            pinned_reads: self.pinned_reads,
            writes: self.writes,
        }
    }
}

/// Validate a commit request against `base` and the writes of
/// transactions accepted earlier in the same batch.
pub async fn validate(req: &CommitRequest, base: &Version, earlier: &[&WriteSet]) -> Result<()> {
    let rv = req.read_version;
    let items: Vec<ReadItem> = match req.isolation {
        Isolation::Serializable => req.reads.clone(),
        Isolation::Snapshot => {
            let mut v: Vec<ReadItem> = req.writes.puts.keys().cloned().map(ReadItem::Key).collect();
            v.extend(req.writes.range_deletes.iter().cloned().map(ReadItem::Range));
            v.extend(req.pinned_reads.iter().cloned().map(ReadItem::Key));
            v
        }
    };
    if items.is_empty() {
        return Ok(());
    }
    if rv < base.m.tombstone_horizon {
        return Err(Error::Conflict("transaction snapshot is older than the tombstone purge horizon".into()));
    }
    for it in &items {
        let (changed, touched) = match it {
            ReadItem::Key(k) => (base.key_changed_since(k, rv).await?, earlier.iter().any(|w| w.touches_key(k))),
            ReadItem::Range(r) => (base.changed_since(r, rv).await?, earlier.iter().any(|w| w.touches_range(r))),
        };
        if changed || touched {
            return Err(Error::Conflict("transaction conflicts with a concurrent commit".into()));
        }
    }
    Ok(())
}

/// Entries as of the newest state for keys in `range` that are live in
/// `base` or in `tail` (the batch's working tail).
pub async fn live_keys_in_range(base: &Version, tail: &BTreeMap<Bytes, Entry>, range: &KeyRange) -> Result<Vec<Bytes>> {
    let mut keys: BTreeMap<Bytes, bool> = BTreeMap::new();
    for e in base.scan(range, false, usize::MAX, false).await? {
        keys.insert(e.key, true);
    }
    for (k, e) in tail.range::<Bytes, _>((range.start.clone(), range.end.clone())) {
        keys.insert(k.clone(), !e.value.is_tombstone());
    }
    Ok(keys.into_iter().filter(|(_, live)| *live).map(|(k, _)| k).collect())
}
