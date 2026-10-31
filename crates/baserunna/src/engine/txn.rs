//! Engine-level optimistic transactions: a snapshot read at a fixed time, a
//! read set, and a buffered write set. Reads see the transaction's own
//! writes.

use super::events::{self, EventOp};
use super::memory::{Charge, MemoryGate, alloc_bytes};
use super::stream::{self, StreamOp, StreamState};
use super::types::{BlobRef, Seq, Value, is_commit_key};
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
    /// Changes to streams, applied after `puts` by the committer, which
    /// gives appends their seqs (SPEC §5.11).
    pub streams: Vec<StreamOp>,
    /// IDs of blobs copied from committed entries via [`Txn::copy`]. Because these
    /// blobs are already bound in the manifest, commit validation skips staging checks
    /// and quota additions for them.
    pub copied_blobs: std::collections::BTreeSet<String>,
    /// Event table operations applied after `streams` at commit (SPEC §5.13).
    pub events: Vec<EventOp>,
}

impl WriteSet {
    pub fn is_empty(&self) -> bool {
        self.puts.is_empty() && self.range_deletes.is_empty() && self.streams.is_empty() && self.events.is_empty()
    }

    pub fn touches_key(&self, k: &[u8]) -> bool {
        self.puts.contains_key(k) || self.range_deletes.iter().any(|r| r.contains(k))
    }

    pub fn touches_range(&self, r: &KeyRange) -> bool {
        self.puts.keys().any(|k| r.contains(k)) || self.range_deletes.iter().any(|d| d.intersects(r))
    }

    /// Whether the commit needs the store's clock: for a value that expires
    /// relative to the commit's time, or to give appended records theirs.
    pub fn needs_clock(&self) -> bool {
        self.puts.values().any(|p| matches!(p, Some(Put { ttl: Some(Ttl::For(_)), .. })))
            || self.streams.iter().any(|s| matches!(s, StreamOp::Append { .. }))
            || self.events.iter().any(|e| !e.new_objects().is_empty())
    }

    /// Returns deduplicated blob references bound by this write set that are not in
    /// `copied_blobs`.
    pub fn staged_blobs(&self) -> impl Iterator<Item = &BlobRef> {
        let mut seen = std::collections::BTreeSet::new();
        self.puts.values().filter_map(move |v| match v {
            Some(Put { value: Value::Blob(b), .. }) if !self.copied_blobs.contains(&b.id) && seen.insert(&b.id) => {
                Some(b)
            }
            _ => None,
        })
    }
}

#[derive(Clone, Debug)]
pub struct CommitRequest {
    pub commit_id: u64,
    /// Instant after which the committer rejects this request without applying it.
    /// Defaults to `commit_timeout_ms` after enqueueing when `None`.
    pub deadline: Option<tokio::time::Instant>,
    pub read_version: Seq,
    /// Store time at which the transaction evaluated expiries.
    pub read_time: u64,
    /// Earliest expiry among values read by the transaction that validation covers.
    pub read_expiry: Option<u64>,
    pub isolation: Isolation,
    pub reads: Vec<ReadItem>,
    /// Reads validated under every isolation level; see [`Txn::get_pinned`].
    pub pinned_reads: Vec<Bytes>,
    pub writes: WriteSet,
    pub charge: Charge,
}

/// A value as seen by a transaction read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadValue {
    pub key: Bytes,
    pub value: Value,
    /// `None` if the value was written by this transaction.
    pub seq: Option<Seq>,
    /// Expiration policy of the value.
    pub ttl: Option<Ttl>,
}

impl ReadValue {
    /// Absolute expiration timestamp in store milliseconds, if known.
    pub fn expires(&self) -> Option<u64> {
        match self.ttl {
            Some(Ttl::Until(e)) => Some(e),
            _ => None,
        }
    }
}

pub struct Txn {
    snapshot: Arc<Version>,
    /// The store time every read evaluates expiries at.
    pub read_time: u64,
    pub isolation: Isolation,
    pub reads: Vec<ReadItem>,
    pinned_reads: Vec<Bytes>,
    /// Earliest expiry among all stored values read, and among pinned reads alone.
    read_expiry: Option<u64>,
    pinned_expiry: Option<u64>,
    pub writes: WriteSet,
    pub commit_id: u64,
    write_bytes: usize,
    max_write_bytes: usize,
    read_bytes: usize,
    max_read_bytes: usize,
    charge: Charge,
    /// Admission gate for memory growth and the charged byte threshold for the next check.
    gate: Option<(Arc<dyn MemoryGate>, usize)>,
    appended: usize,
    inserted: usize,
    optimistic: bool,
    has_untracked_reads: bool,
}

fn earliest(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (x, None) | (None, x) => x,
    }
}

impl Txn {
    pub fn new(snapshot: Snapshot, isolation: Isolation, commit_id: u64, max_write_bytes: usize) -> Txn {
        let charge = Charge::new(&snapshot.version.shared.memory, std::mem::size_of::<Txn>() + 256);
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
            read_bytes: 0,
            max_read_bytes: max_write_bytes,
            charge,
            gate: None,
            appended: 0,
            inserted: 0,
            optimistic: false,
            has_untracked_reads: false,
        }
    }

    pub fn meter(&mut self) -> Arc<std::sync::atomic::AtomicUsize> {
        self.charge.meter()
    }

    pub fn set_memory_gate(&mut self, gate: Option<Arc<dyn MemoryGate>>) {
        self.gate = gate.map(|g| {
            let next_check = self.charge.bytes() + g.step_bytes();
            (g, next_check)
        });
    }

    fn admit_growth(&mut self) -> Result<()> {
        if let Some((gate, next_check)) = &mut self.gate
            && self.charge.bytes() > *next_check
        {
            gate.admit()?;
            *next_check = self.charge.bytes() + gate.step_bytes();
        }
        Ok(())
    }

    pub fn with_read_limit(mut self, max_read_bytes: usize) -> Txn {
        self.max_read_bytes = max_read_bytes;
        self
    }

    /// Returns true if this transaction began during an in-flight manifest
    /// write and cannot rely on commit validation (either because it will not
    /// commit or because it performed untracked snapshot reads).
    pub fn needs_revalidation(&self, will_commit: bool) -> bool {
        self.optimistic && (!will_commit || self.has_untracked_reads)
    }

    pub(crate) fn mark_optimistic(&mut self) {
        self.optimistic = true;
    }

    pub(crate) fn has_untracked_reads(&self) -> bool {
        self.has_untracked_reads
    }

    /// Returns the underlying version snapshot. Reads performed directly on
    /// [`Version`] are not added to the transaction's read set; use
    /// [`Txn::untracked_snapshot`] if the transaction's output depends on them.
    pub fn snapshot(&self) -> &Arc<Version> {
        &self.snapshot
    }

    /// Returns the underlying version snapshot and marks the transaction as
    /// having performed untracked reads, forcing [`super::Db::revalidate`] to
    /// re-run the transaction if a manifest write was in flight when it began.
    pub fn untracked_snapshot(&mut self) -> &Arc<Version> {
        self.has_untracked_reads = true;
        &self.snapshot
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

    fn own(key: &[u8], p: &Option<Put>) -> Option<ReadValue> {
        p.clone().map(|p| ReadValue { key: Bytes::copy_from_slice(key), value: p.value, seq: None, ttl: p.ttl })
    }

    fn stored(e: super::types::Entry) -> ReadValue {
        ReadValue { key: e.key, value: e.value, seq: Some(e.seq), ttl: e.expires.map(Ttl::Until) }
    }

    /// Reads `key`, recording it in the transaction's read set.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<ReadValue>> {
        if let Some(p) = self.writes.puts.get(key) {
            return Ok(Txn::own(key, p));
        }
        self.record_read(ReadItem::Key(Bytes::copy_from_slice(key)))?;
        if self.masked(key) {
            return Ok(None);
        }
        let found = self.snapshot.get(key, self.read_time).await?;
        if let Some(e) = &found {
            self.read_expiry = earliest(self.read_expiry, e.expires);
        }
        Ok(found.map(Txn::stored))
    }

    /// Reads `key` and validates it at commit time under all isolation levels.
    pub async fn get_pinned(&mut self, key: &[u8]) -> Result<Option<ReadValue>> {
        self.add_read_bytes(alloc_bytes(key.len()) + std::mem::size_of::<Bytes>())?;
        self.pinned_reads.push(Bytes::copy_from_slice(key));
        let r = self.get(key).await?;
        if let Some(rv @ ReadValue { seq: Some(_), .. }) = &r {
            self.pinned_expiry = earliest(self.pinned_expiry, rv.expires());
        }
        Ok(r)
    }

    /// Copies the value of `from` to `to` with expiration policy `ttl`, and returns `true` if
    /// `from` exists. Pins `from` in the read set so that the commit fails if `from` is
    /// modified before commit under any isolation level, preventing a referenced blob from
    /// becoming unreferenced before the commit (SPEC §5.8).
    pub async fn copy(&mut self, from: &[u8], to: Bytes, ttl: Option<Ttl>) -> Result<bool> {
        let Some(rv) = self.get_pinned(from).await? else { return Ok(false) };
        if let (Value::Blob(b), Some(_)) = (&rv.value, rv.seq) {
            self.writes.copied_blobs.insert(b.id.clone());
        }
        self.put_expiring(to, rv.value, ttl)?;
        Ok(true)
    }

    /// Scans `range` with read-your-writes, recording the scanned interval in the read set.
    pub async fn scan(&mut self, range: &KeyRange, reverse: bool, limit: usize) -> Result<Vec<ReadValue>> {
        let mut out: Vec<ReadValue> = Vec::new();
        if limit == 0 || range.is_empty() {
            return Ok(out);
        }
        let batch = limit.clamp(32, 1024);
        let mut window = range.clone();
        loop {
            let snap = self.snapshot.scan(&window, reverse, batch, self.read_time).await?;
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
                self.record_read(ReadItem::Range(covered))?;
                return Ok(out);
            }
            let Some(k) = last else {
                self.record_read(ReadItem::Range(range.clone()))?;
                return Ok(out);
            };
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

    fn writable(key: &[u8]) -> Result<()> {
        match is_commit_key(key) {
            true => Err(Error::internal("writes to the commit record keyspace")),
            false => Ok(()),
        }
    }

    fn record_read(&mut self, item: ReadItem) -> Result<()> {
        let n = std::mem::size_of::<ReadItem>()
            + match &item {
                ReadItem::Key(k) => alloc_bytes(k.len()),
                ReadItem::Range(r) => r.heap_bytes(),
            };
        self.add_read_bytes(n)?;
        self.reads.push(item);
        Ok(())
    }

    fn add_read_bytes(&mut self, n: usize) -> Result<()> {
        self.read_bytes += n;
        self.charge.add(n);
        if self.read_bytes > self.max_read_bytes {
            return Err(Error::TooLarge(format!("transaction read sets exceed {} bytes", self.max_read_bytes)));
        }
        self.admit_growth()
    }

    fn account(&mut self, n: usize) -> Result<()> {
        self.charge.add(n);
        self.write_bytes += n;
        if self.write_bytes > self.max_write_bytes {
            return Err(Error::TooLarge(format!("transaction writes exceed {} bytes", self.max_write_bytes)));
        }
        self.admit_growth()
    }

    pub fn put(&mut self, key: Bytes, value: Value) -> Result<()> {
        self.put_expiring(key, value, None)
    }

    /// Writes `value` with an optional expiration policy `ttl`.
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

    /// Deletes all user keys in `range`.
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

    /// Appends records with `payloads` to stream `id` at commit; with
    /// `expect`, only if the stream's next seq is then `expect`.
    pub fn append(
        &mut self,
        id: u64,
        payloads: Vec<Bytes>,
        expect: Option<u64>,
        op_index: Option<usize>,
    ) -> Result<()> {
        self.appended += payloads.len();
        if self.appended > stream::APPEND_MAX_RECORDS {
            return Err(Error::TooLarge(format!(
                "a transaction appends at most {} records",
                stream::APPEND_MAX_RECORDS
            )));
        }
        self.account(payloads.iter().map(|p| p.len() + 24).sum())?;
        self.writes.streams.push(StreamOp::Append { id, payloads, expect, op_index });
        Ok(())
    }

    /// Inserts encoded rows into event table `table` at commit.
    pub fn insert_events(&mut self, table: u32, rows: Vec<Bytes>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.inserted += rows.len();
        if self.inserted > events::INSERT_MAX_ROWS {
            return Err(Error::TooLarge(format!("a transaction inserts at most {} events", events::INSERT_MAX_ROWS)));
        }
        let op = EventOp::Insert { table, rows };
        self.account(op.inserted_bytes() as usize)?;
        self.writes.events.push(op);
        Ok(())
    }

    /// Returns true if the transaction has buffered event inserts for `table`.
    /// Uncommitted event inserts are not visible to reads in the same transaction.
    pub fn inserts_events(&self, table: u32) -> bool {
        self.writes.events.iter().any(|op| matches!(op, EventOp::Insert { table: t, .. } if *t == table))
    }

    pub fn event_op(&mut self, op: EventOp) -> Result<()> {
        self.account(64)?;
        self.writes.events.push(op);
        Ok(())
    }

    /// Adds `range` to the transaction's read set for reads performed directly on the snapshot.
    pub fn note_read(&mut self, range: KeyRange) {
        self.reads.push(ReadItem::Range(range));
    }

    /// Creates, configures or trims a stream at commit.
    pub fn stream_op(&mut self, op: StreamOp) -> Result<()> {
        self.account(32)?;
        self.writes.streams.push(op);
        Ok(())
    }

    /// Reads the state of stream `id`, recording it in the read set, so the
    /// transaction conflicts with every append and trim committed after
    /// its snapshot.
    pub async fn stream_state(&mut self, id: u64) -> Result<Option<StreamState>> {
        match self.get(&stream::state_key(id)).await? {
            Some(ReadValue { value: Value::Inline(b), .. }) => {
                Ok(Some(StreamState::decode(&b).map_err(|_| Error::internal("corrupt stream state"))?))
            }
            Some(_) => Err(Error::internal("corrupt stream state")),
            None => Ok(None),
        }
    }

    /// The seq before which this transaction trims stream `id`, if it does.
    pub fn pending_trim(&self, id: u64) -> Option<u64> {
        let trims = self.writes.streams.iter().filter_map(|op| match op {
            StreamOp::Trim { id: i, before, .. } if *i == id => Some(*before),
            _ => None,
        });
        trims.max()
    }

    /// Records a read of stream `id`'s records with seqs in `[lo, hi)`, or
    /// from `lo` on, in the read set.
    pub fn note_stream_read(&mut self, id: u64, lo: u64, hi: Option<u64>) -> Result<()> {
        self.record_read(ReadItem::Range(stream::records_between(id, lo, hi)))
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
            charge: self.charge,
        }
    }
}

/// Validate a commit request against `base` and the writes of
/// transactions accepted earlier in the same batch (`earlier`, and the keys
/// their stream ops wrote, `touched`), to commit at store time `time`:
/// whatever the transaction read must not have changed since its snapshot,
/// nor expired by `time`.
pub async fn validate(
    req: &CommitRequest,
    base: &Version,
    earlier: &[&WriteSet],
    touched: &[KeyRange],
    time: u64,
) -> Result<()> {
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
        let (changed, overlapped) = match it {
            ReadItem::Key(k) => (
                base.key_changed_since(k, rv).await?,
                earlier.iter().any(|w| w.touches_key(k)) || touched.iter().any(|t| t.contains(k)),
            ),
            ReadItem::Range(r) => (
                base.changed_since(r, rv).await?,
                earlier.iter().any(|w| w.touches_range(r)) || touched.iter().any(|t| t.intersects(r)),
            ),
        };
        if changed || overlapped {
            if !changed {
                crate::probe::hit(crate::probe::Probe::CommitConflictInBatch);
            }
            return Err(Error::Conflict("transaction conflicts with a concurrent commit".into()));
        }
    }
    Ok(())
}
