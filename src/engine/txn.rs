//! Engine-level optimistic transactions: a snapshot read at a fixed time, a
//! read set, and a buffered write set. Reads see the transaction's own
//! writes.

use super::types::{Seq, Value, is_commit_key};
use super::version::{KeyRange, Snapshot, Version};
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

/// A value a transaction writes, and when it expires.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Put {
    pub value: Value,
    pub ttl: Option<Ttl>,
}

/// When a written value expires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ttl {
    /// At this store time (ms).
    Until(u64),
    /// This many ms after the time of the version that commits it.
    For(u64),
}

impl Ttl {
    /// The expiry of a value committed at store time `time`.
    pub fn at(self, time: u64) -> u64 {
        match self {
            Ttl::Until(e) => e,
            Ttl::For(d) => time.saturating_add(d),
        }
    }
}

/// What a transaction asks the committer to apply.
#[derive(Clone, Debug, Default)]
pub struct WriteSet {
    /// `None` = delete.
    pub puts: BTreeMap<Bytes, Option<Put>>,
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

    /// Whether a value expires relative to the commit's time, which needs
    /// the store's clock.
    pub fn has_relative_ttl(&self) -> bool {
        self.puts.values().any(|p| matches!(p, Some(Put { ttl: Some(Ttl::For(_)), .. })))
    }

    pub fn blob_ids(&self) -> Vec<String> {
        self.puts
            .values()
            .filter_map(|v| match v {
                Some(Put { value: Value::Blob(b), .. }) => Some(b.id.clone()),
                _ => None,
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct CommitRequest {
    pub commit_id: u64,
    /// The committer refuses (without applying) after this instant, which
    /// bounds how long a request can linger server-side. `None` = the
    /// engine's `commit_timeout_ms` after enqueueing.
    pub deadline: Option<tokio::time::Instant>,
    pub read_version: Seq,
    /// The store time the transaction read at: its view of what had
    /// expired. The commit's version time is never earlier.
    pub read_time: u64,
    /// The earliest expiry among values the transaction read that
    /// validation covers: the commit fails if that has passed by its time.
    pub read_expiry: Option<u64>,
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
    /// When the value expires: at a store time once committed, or as the
    /// transaction's own write asked.
    pub ttl: Option<Ttl>,
}

impl ReadValue {
    /// When the value expires (store ms), if it does and that is known.
    pub fn expires(&self) -> Option<u64> {
        match self.ttl {
            Some(Ttl::Until(e)) => Some(e),
            _ => None,
        }
    }
}

pub struct Txn {
    pub snapshot: Arc<Version>,
    /// The store time every read evaluates expiries at.
    pub read_time: u64,
    pub isolation: Isolation,
    pub reads: Vec<ReadItem>,
    pinned_reads: Vec<Bytes>,
    /// Earliest expiry among the stored values read, all reads and pinned
    /// reads alone.
    read_expiry: Option<u64>,
    pinned_expiry: Option<u64>,
    pub writes: WriteSet,
    pub commit_id: u64,
    write_bytes: usize,
    max_write_bytes: usize,
}

fn earliest(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (x, None) | (None, x) => x,
    }
}

impl Txn {
    pub fn new(snapshot: Snapshot, isolation: Isolation, commit_id: u64, max_write_bytes: usize) -> Txn {
        Txn {
            snapshot: snapshot.version,
            read_time: snapshot.time_ms,
            isolation,
            reads: Vec::new(),
            pinned_reads: Vec::new(),
            read_expiry: None,
            pinned_expiry: None,
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

    /// A value the transaction wrote.
    fn own(key: &[u8], p: &Option<Put>) -> Option<ReadValue> {
        p.clone().map(|p| ReadValue { key: Bytes::copy_from_slice(key), value: p.value, seq: None, ttl: p.ttl })
    }

    /// A value read from the snapshot.
    fn stored(e: super::types::Entry) -> ReadValue {
        ReadValue { key: e.key, value: e.value, seq: Some(e.seq), ttl: e.expires.map(Ttl::Until) }
    }

    /// Point read (records the read).
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<ReadValue>> {
        if let Some(p) = self.writes.puts.get(key) {
            return Ok(Txn::own(key, p));
        }
        self.reads.push(ReadItem::Key(Bytes::copy_from_slice(key)));
        if self.masked(key) {
            return Ok(None);
        }
        let found = self.snapshot.get(key, self.read_time).await?;
        if let Some(e) = &found {
            self.read_expiry = earliest(self.read_expiry, e.expires);
        }
        Ok(found.map(Txn::stored))
    }

    /// Point read that the commit validates even under snapshot isolation:
    /// for data the transaction's writes are derived from, such as a table
    /// schema that decides which index entries a row write maintains.
    pub async fn get_pinned(&mut self, key: &[u8]) -> Result<Option<ReadValue>> {
        self.pinned_reads.push(Bytes::copy_from_slice(key));
        let r = self.get(key).await?;
        if let Some(rv @ ReadValue { seq: Some(_), .. }) = &r {
            self.pinned_expiry = earliest(self.pinned_expiry, rv.expires());
        }
        Ok(r)
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
            let snap = self.snapshot.scan(&window, reverse, batch, self.read_time).await?;
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
                merged.insert(e.key.clone(), Txn::stored(e));
            }
            for (k, p) in self.writes.puts.range::<Bytes, _>((examined.start.clone(), examined.end.clone())) {
                if let Some(rv) = Txn::own(k, p) {
                    merged.insert(k.clone(), rv);
                }
            }
            let items: Vec<ReadValue> =
                if reverse { merged.into_values().rev().collect() } else { merged.into_values().collect() };
            for it in items {
                if out.len() >= limit {
                    break;
                }
                if it.seq.is_some() {
                    self.read_expiry = earliest(self.read_expiry, it.expires());
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

    /// Bytes the transaction may still write.
    pub fn write_budget(&self) -> usize {
        self.max_write_bytes.saturating_sub(self.write_bytes)
    }

    /// Keys of commit records are the engine's own.
    fn writable(key: &[u8]) -> Result<()> {
        match is_commit_key(key) {
            true => Err(Error::internal("writes to the commit record keyspace")),
            false => Ok(()),
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
        self.put_expiring(key, value, None)
    }

    /// Write `value`, to expire as `ttl` says.
    pub fn put_expiring(&mut self, key: Bytes, value: Value, ttl: Option<Ttl>) -> Result<()> {
        Txn::writable(&key)?;
        self.account(key.len() + value.size() + if ttl.is_some() { 8 } else { 0 })?;
        self.writes.puts.insert(key, Some(Put { value, ttl }));
        Ok(())
    }

    pub fn delete(&mut self, key: Bytes) -> Result<()> {
        Txn::writable(&key)?;
        self.account(key.len() + 8)?;
        self.writes.puts.insert(key, None);
        Ok(())
    }

    /// Delete every key in `range` (commit records aside).
    pub fn delete_range(&mut self, range: KeyRange) -> Result<()> {
        let range = range.intersect(&KeyRange::data());
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
        let read_expiry = match self.isolation {
            Isolation::Serializable => self.read_expiry,
            Isolation::Snapshot => self.pinned_expiry,
        };
        CommitRequest {
            commit_id: self.commit_id,
            deadline: None,
            read_version: self.snapshot.version(),
            read_time: self.read_time,
            read_expiry,
            isolation: self.isolation,
            reads: self.reads,
            pinned_reads: self.pinned_reads,
            writes: self.writes,
        }
    }
}

/// Validate a commit request against `base` and the writes of
/// transactions accepted earlier in the same batch, to commit at store time
/// `time`: whatever the transaction read must not have changed since its
/// snapshot, nor expired by `time`.
pub async fn validate(req: &CommitRequest, base: &Version, earlier: &[&WriteSet], time: u64) -> Result<()> {
    if req.read_expiry.is_some_and(|e| e <= time) {
        return Err(Error::Conflict("a value the transaction read has expired".into()));
    }
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
