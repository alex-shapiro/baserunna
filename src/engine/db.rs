//! Database handle for a single node.

use super::blob::{BlobWriter, StageGuard};
use super::compaction::{self, Kind};
use super::manifest::{Lease, Manifest, ObjKind, Obsolete, PageCache, SegmentMeta};
use super::range_delete::RangeTombstone;
use super::segment::SegmentBuilder;
use super::stream::{self, Appended, Pending, Sealer, Seek, StreamOp};
use super::txn::{CommitRequest, Isolation, Txn, WriteSet, validate};
use super::types::{
    ABORT_RECORD, CommitRecord, Entry, Seq, Value, commit_key, decode_commit_record, encode_commit_record,
    is_commit_key,
};
use super::version::{KeyRange, LevelsMissing, Snapshot, Version};
use super::{Shared, blob_prefix, levels_key, manifest_key, seg_key};
use crate::buggify::buggify;
use crate::error::{Error, Result};
use crate::probe::{self, Probe};
use crate::store::{GetOpts, PutMode, StoreError};
use crate::timing::{self, StoreOp, Tally};
use crate::util::{Either, race};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consistency {
    /// Linearizable.
    Strong,
    /// A consistent snapshot no older than `max_staleness_ms`.
    Snapshot { max_staleness_ms: u64 },
}

tokio::task_local! {
    static INVOKED_AT: Instant;
}

/// Runs `f` as a single operation invoked at `at` so strong reads within the
/// scope can reuse a manifest validated at or after `at`.
pub async fn invoked_at<F: std::future::Future>(at: Instant, f: F) -> F::Output {
    INVOKED_AT.scope(at, f).await
}

enum Work {
    Commit {
        req: CommitRequest,
        tx: oneshot::Sender<Result<Committed>>,
    },
    Lease {
        release: bool,
        /// Renewed for strong reads: takes the longer `read_lease_ms`.
        for_reads: bool,
        tx: Option<oneshot::Sender<Result<()>>>,
    },
    Compaction {
        inputs: Vec<String>,
        outputs: Vec<SegmentMeta>,
        purged_max_seq: Seq,
        sealed_bytes: u64,
        compaction_seq: u64,
        started_ms: u64,
        deep: Vec<SegmentMeta>,
        levels: Option<String>,
        tx: oneshot::Sender<Result<()>>,
    },
    Obsolete {
        items: Vec<Obsolete>,
        blob_bytes: (u64, u64),
        chunk_bytes: (u64, u64),
        tx: oneshot::Sender<Result<()>>,
    },
    MarkDeleted {
        tx: oneshot::Sender<Result<()>>,
    },
    Flush {
        flushed_version: Seq,
        flush_seq: u64,
        output: FlushOutput,
        started_ms: u64,
        tx: oneshot::Sender<Result<()>>,
    },
    Resolve {
        id: u64,
        tx: oneshot::Sender<Result<Option<Committed>>>,
    },
}

enum Outcome {
    Seq(Result<Committed>),
    Unit(Result<()>),
    Resolved(Result<Option<Committed>>),
}

impl Outcome {
    fn unit(self) -> Result<()> {
        match self {
            Outcome::Seq(r) => r.map(drop),
            Outcome::Unit(r) => r,
            Outcome::Resolved(r) => r.map(drop),
        }
    }

    fn error(self) -> Error {
        self.unit().err().unwrap_or_else(|| Error::internal("mismatched commit outcome"))
    }
}

impl Work {
    fn deliver(self, o: Outcome) {
        match self {
            Work::Commit { tx, .. } => {
                let _ = tx.send(match o {
                    Outcome::Seq(r) => r,
                    o => Err(o.error()),
                });
            }
            Work::Resolve { tx, .. } => {
                let _ = tx.send(match o {
                    Outcome::Resolved(r) => r,
                    o => Err(o.error()),
                });
            }
            Work::Lease { tx: Some(tx), .. }
            | Work::Compaction { tx, .. }
            | Work::Flush { tx, .. }
            | Work::Obsolete { tx, .. }
            | Work::MarkDeleted { tx } => {
                let _ = tx.send(o.unit());
            }
            Work::Lease { tx: None, .. } => {}
        }
    }

    fn fail(self, e: Error) {
        self.deliver(Outcome::Unit(Err(e)));
    }
}

struct State {
    current: Option<Arc<Version>>,
    /// False after an ambiguous write until a manifest read that started after the failure completes.
    trusted: bool,
    distrusted_at: Option<Instant>,
    verified_at: Option<Instant>,
    last_access: Instant,
    window_start: Instant,
    reads_in_window: u32,
    reads_last_window: u32,
    lease_requested: bool,
    /// `(write_seq, expires_at)` for the lease held by this node.
    own_lease: Option<(u64, Instant)>,
    /// Hedged manifest `PUT` requests that remain in flight in the background.
    /// Lease-based reads are disabled while this count is non-zero.
    in_flight_hedged_puts: u32,
    compacting: bool,
    flushing: bool,
    sweeping: bool,
    /// Set if a write fails due to quota exhaustion to trigger an early sweep.
    quota_pressure: bool,
    trim_queue: BTreeSet<u64>,
    trimming: bool,
}

impl State {
    fn distrust(&mut self) {
        self.trusted = false;
        self.distrusted_at = Some(Instant::now());
    }

    fn hot(&self, threshold: u32) -> bool {
        self.reads_in_window.max(self.reads_last_window) >= threshold
    }
}

struct Queue {
    items: VecDeque<QueuedWork>,
    running: bool,
    /// Completed background flush waiting to batch with the next commit.
    parked_flush: Option<QueuedWork>,
}

struct QueuedWork {
    work: Work,
    enqueued_at: Instant,
    request_id: Option<Arc<str>>,
}

pub struct Db {
    pub id: String,
    shared: Arc<Shared>,
    st: Mutex<State>,
    q: Mutex<Queue>,
    load_lock: tokio::sync::Mutex<()>,
    /// A manifest write is in flight if `writes_finished < writes_started`.
    writes_started: std::sync::atomic::AtomicU64,
    writes_finished: std::sync::atomic::AtomicU64,
    write_done: tokio::sync::Notify,
    installed: tokio::sync::Notify,
    /// Count of active [`Db::wait_for_change`] calls.
    waiters: std::sync::atomic::AtomicUsize,
    /// Highest version reported by [`Db::notice_commit`].
    noticed_version: std::sync::atomic::AtomicU64,
    /// True while [`Db::load_noticed`] is reloading the manifest.
    refreshing: std::sync::atomic::AtomicBool,
    staging: Mutex<Staging>,
    pages: Mutex<PageCache>,
}

/// Tracks in-flight and completed uncommitted blob uploads against the database quota.
#[derive(Default)]
struct Staging {
    uploading: u64,
    staged: BTreeMap<String, (u64, Instant)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitState {
    Applied(Committed),
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed {
    pub version: Seq,
    pub replayed: bool,
    pub time_ms: u64,
    pub appends: Vec<Appended>,
}

/// Database storage statistics at a single version.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DbStats {
    pub version: Seq,
    /// Non-empty segment levels, shallowest first.
    pub levels: Vec<LevelStats>,
    pub tail_entries: usize,
    pub tail_bytes: usize,
    pub range_tombstones: usize,
    /// Referenced blob bytes.
    pub blob_bytes: u64,
    /// Segments and blobs no longer referenced, awaiting collection.
    pub pending_gc_objects: usize,
    pub lease_holder: Option<String>,
    #[serde(rename = "lease_expires_at")]
    pub lease_expires_ms: Option<u64>,
    /// Store time of the last commit or maintenance write.
    #[serde(rename = "updated_at")]
    pub updated_ms: u64,
    /// Total stored bytes tracked against the database quota.
    pub size_bytes: u64,
    pub quota_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LevelStats {
    pub level: u8,
    pub segments: usize,
    pub bytes: u64,
    pub entries: u64,
}

impl Db {
    pub(crate) fn new(id: String, shared: Arc<Shared>) -> Arc<Db> {
        let now = Instant::now();
        Arc::new(Db {
            id,
            shared,
            st: Mutex::new(State {
                current: None,
                trusted: false,
                distrusted_at: None,
                verified_at: None,
                last_access: now,
                window_start: now,
                reads_in_window: 0,
                reads_last_window: 0,
                lease_requested: false,
                own_lease: None,
                in_flight_hedged_puts: 0,
                compacting: false,
                flushing: false,
                sweeping: false,
                quota_pressure: false,
                trim_queue: BTreeSet::new(),
                trimming: false,
            }),
            q: Mutex::new(Queue { items: VecDeque::new(), running: false, parked_flush: None }),
            load_lock: tokio::sync::Mutex::new(()),
            writes_started: 0.into(),
            writes_finished: 0.into(),
            write_done: tokio::sync::Notify::new(),
            installed: tokio::sync::Notify::new(),
            waiters: Default::default(),
            noticed_version: Default::default(),
            refreshing: Default::default(),
            staging: Mutex::new(Staging::default()),
            pages: Mutex::new(PageCache::default()),
        })
    }

    fn st(&self) -> std::sync::MutexGuard<'_, State> {
        self.st.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn queue(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.q.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    pub fn current(&self) -> Option<Arc<Version>> {
        self.st().current.clone()
    }

    /// Returns true if commits, compaction, sweeps, or retention trims are running or queued.
    pub fn busy(&self) -> bool {
        let st = self.st();
        let q = self.queue();
        q.running || !q.items.is_empty() || st.compacting || st.flushing || st.sweeping || st.trimming
    }

    pub(crate) fn idle_for(&self, ms: u64) -> bool {
        let st = self.st();
        let q = self.queue();
        !q.running
            && q.items.is_empty()
            && !st.compacting
            && !st.flushing
            && !st.sweeping
            && !st.trimming
            && st.last_access.elapsed() >= Duration::from_millis(ms)
    }

    // Lease validity is measured on each node's monotonic clock rather than wall clocks.
    // The lease holder measures duration from before sending the renewing write, and other nodes
    // measure from when they first observe that write. Because observation occurs after sending,
    // other nodes always treat a lease as valid for longer than its holder relies on it.

    /// Remaining duration we may rely on our lease in `m`, or zero if `m` is
    /// not the manifest write that last renewed it.
    fn own_lease_left(&self, st: &State, m: &Manifest) -> Duration {
        let ours = m.lease.as_ref().is_some_and(|l| l.node == self.shared.node_id);
        match st.own_lease {
            Some((ws, until)) if ours && ws == m.write_seq => until.saturating_duration_since(Instant::now()),
            _ => Duration::ZERO,
        }
    }

    fn holds_lease(&self, st: &State, m: &Manifest) -> bool {
        !self.own_lease_left(st, m).is_zero()
    }

    fn note_own_lease(&self, m: &Manifest, sent: Instant) {
        if let Some(l) = m.lease.as_ref().filter(|l| l.node == self.shared.node_id) {
            let margin = self.shared.cfg.lease_margin_ms.min(l.ms);
            self.st().own_lease = Some((m.write_seq, sent + Duration::from_millis(l.ms - margin)));
        }
    }

    /// Caps `own_lease` to the expiration granted by `m` at `sent` because peers can observe `m`
    /// immediately after `sent`.
    fn limit_own_lease(&self, m: &Manifest, sent: Instant) {
        let mut st = self.st();
        let Some((ws, until)) = st.own_lease else { return };
        let granted = match m.lease.as_ref().filter(|l| l.node == self.shared.node_id) {
            Some(l) => sent + Duration::from_millis(l.ms - self.shared.cfg.lease_margin_ms.min(l.ms)),
            None => sent,
        };
        if granted < until {
            probe::hit(Probe::LeaseShortenedInFlight);
            st.own_lease = Some((ws, granted));
        }
    }

    fn foreign_lease<'a>(&self, v: &'a Version) -> Option<(&'a Lease, Duration)> {
        self.observed_lease(v).filter(|(l, _)| l.node != self.shared.node_id)
    }

    fn observed_lease<'a>(&self, v: &'a Version) -> Option<(&'a Lease, Duration)> {
        let m = &v.m;
        let l = m.lease.as_ref()?;
        if self.lease_expired_on_store(v, l) {
            return None;
        }
        let valid_for = Duration::from_millis(l.ms + self.shared.cfg.lease_margin_ms);
        let seen = self.shared.lease_seen(&self.id, l, m.write_seq);
        let left = (seen + valid_for).saturating_duration_since(Instant::now());
        (!left.is_zero()).then_some((l, left))
    }

    fn lease_expired_on_store(&self, v: &Version, l: &Lease) -> bool {
        let (Some(last_modified_ms), Some(now_ms)) = (v.stored_ms, self.shared.store_earliest_now_ms()) else {
            return false;
        };
        let precision_ms = self.shared.store.time_precision_ms();
        let expired = now_ms > last_modified_ms + precision_ms + 2 * l.ms + self.shared.cfg.lease_margin_ms;
        if expired {
            probe::hit(Probe::ExpiredLeaseByStoreClock);
        }
        expired
    }

    /// Returns `(write_seq, etag, has_in_flight_write)` if this node holds a valid lease for strong reads.
    pub fn leased_view(&self) -> Option<(u64, String, bool)> {
        let st = self.st();
        let cur = st.current.as_ref()?;
        (st.trusted && st.in_flight_hedged_puts == 0 && self.holds_lease(&st, &cur.m))
            .then(|| (cur.m.write_seq, cur.etag.clone(), self.has_in_flight_write()))
    }

    pub fn lease_holder(&self) -> Option<String> {
        let st = self.st();
        let cur = st.current.clone()?;
        if self.holds_lease(&st, &cur.m) {
            return Some(self.shared.node_id.clone());
        }
        self.observed_lease(&cur).map(|(l, _)| l.node.clone())
    }

    /// Storage statistics at version `v`.
    pub fn stats(&self, v: &Version) -> DbStats {
        let m = &v.m;
        let mut levels: Vec<LevelStats> = Vec::new();
        for s in &m.segments {
            match levels.last_mut() {
                Some(l) if l.level == s.level => {
                    l.segments += 1;
                    l.bytes += s.size;
                    l.entries += s.entries;
                }
                _ => levels.push(LevelStats { level: s.level, segments: 1, bytes: s.size, entries: s.entries }),
            }
        }
        DbStats {
            version: m.version,
            levels,
            tail_entries: m.tail.len(),
            tail_bytes: m.tail_bytes(),
            range_tombstones: m.range_deletes.len(),
            blob_bytes: m.blob_bytes.total(),
            pending_gc_objects: m.obsolete.len(),
            lease_holder: m.lease.as_ref().map(|l| l.node.clone()),
            lease_expires_ms: m.lease.as_ref().map(|l| l.expires_ms),
            updated_ms: m.time_ms,
            size_bytes: m.stored_bytes(),
            quota_bytes: self.quota(),
        }
    }

    /// Effective size quota for this database in bytes, or None if unlimited or exempt.
    pub fn quota(&self) -> Option<u64> {
        let limit = self.shared.cfg.db_max_bytes;
        (limit > 0 && !self.shared.quota_exempt(&self.id)).then_some(limit)
    }

    fn staging(&self) -> std::sync::MutexGuard<'_, Staging> {
        let mut g = self.staging.lock().unwrap_or_else(|e| e.into_inner());
        let ttl = Duration::from_millis(self.shared.cfg.blob_stage_ttl_ms);
        g.staged.retain(|_, (_, at)| at.elapsed() < ttl);
        g
    }

    /// Reserves `n` bytes for an in-flight upload against the database size quota.
    pub(crate) fn reserve_staging(&self, n: u64) -> Result<()> {
        let Some(limit) = self.quota() else { return Ok(()) };
        let stored = self.current().map_or(0, |v| v.m.stored_bytes());
        let mut g = self.staging();
        let pending = g.uploading + g.staged.values().map(|(n, _)| n).sum::<u64>();
        if stored + pending + n > limit {
            return Err(quota_exceeded(limit));
        }
        g.uploading += n;
        Ok(())
    }

    /// Finalizes or cancels a staging reservation.
    pub(crate) fn end_staging(&self, reserved: u64, done: Option<(&str, u64)>) {
        let mut g = self.staging();
        g.uploading = g.uploading.saturating_sub(reserved);
        if let Some((id, size)) = done {
            g.staged.insert(id.to_string(), (size, Instant::now()));
        }
    }

    /// Installs `v` if it is newer than `current`, and marks `current` as trusted if `verified`
    /// is not earlier than `distrusted_at`.
    fn install(&self, v: Arc<Version>, verified: Instant) -> Arc<Version> {
        let mut st = self.st();
        self.shared.env.trace(|| {
            format!(
                "{} install v{} ws{} (current {:?})",
                self.id,
                v.m.version,
                v.m.write_seq,
                st.current.as_ref().map(|c| (c.m.version, c.m.write_seq))
            )
        });
        let installed = match &st.current {
            Some(c) if c.m.write_seq >= v.m.write_seq => {
                if c.m.write_seq > v.m.write_seq {
                    probe::hit(Probe::StaleVersionIgnored);
                }
                c.clone()
            }
            _ => {
                st.current = Some(v.clone());
                self.installed.notify_waiters();
                v
            }
        };
        if let Some(l) = installed.m.lease.as_ref().filter(|l| l.node != self.shared.node_id) {
            self.shared.lease_seen(&self.id, l, installed.m.write_seq);
        }
        if st.distrusted_at.is_none_or(|d| verified >= d) {
            st.trusted = true;
            if st.verified_at.is_none_or(|old| verified > old) {
                st.verified_at = Some(verified);
            }
        }
        installed
    }

    /// Returns a version validated at or after `not_before`, fetching the manifest from storage
    /// if needed. Creates the database if `create` is true and the manifest does not exist.
    pub async fn refresh(self: &Arc<Self>, not_before: Instant, create: bool) -> Result<Arc<Version>> {
        Ok(self.refresh_at(not_before, create).await?.0)
    }

    async fn refresh_at(self: &Arc<Self>, not_before: Instant, create: bool) -> Result<(Arc<Version>, Instant)> {
        let _g = self.load_lock.lock().await;
        let (etag, cur) = {
            let st = self.st();
            if let (Some(c), Some(va)) = (&st.current, st.verified_at)
                && va >= not_before
                && st.trusted
            {
                return Ok((c.clone(), va));
            }
            (st.current.as_ref().map(|c| c.etag.clone()), st.current.clone())
        };
        let key = manifest_key(&self.id);
        let mut levels_missing: Option<String> = None;
        loop {
            let started = Instant::now();
            let res = self
                .shared
                .retry
                .run(|| self.shared.store.get(&key, GetOpts { if_none_match: etag.clone(), ..Default::default() }))
                .await;
            match res {
                Err(StoreError::NotModified) => {
                    let c = cur.clone().ok_or_else(|| Error::internal("not modified without current"))?;
                    return Ok((self.install(c, started), started));
                }
                Ok(g) => {
                    let m = match self.shared.load_manifest(&self.id, &g.data, cur.as_deref()).await? {
                        Ok(m) => m,
                        Err(LevelsMissing(id)) if levels_missing.as_ref() != Some(&id) => {
                            probe::hit(Probe::LevelsCollectedUnderRead);
                            levels_missing = Some(id);
                            continue;
                        }
                        Err(LevelsMissing(id)) => {
                            return Err(Error::internal(format!("levels object {id} is missing")));
                        }
                    };
                    let mut v = Version::new(m, g.etag, self.shared.clone());
                    v.stored_ms = Some(g.last_modified_ms);
                    let v = Arc::new(v);
                    return Ok((self.install(v, started), started));
                }
                Err(StoreError::NotFound) if create => {
                    let m = Manifest::new(&self.id, self.shared.time_ms(), self.shared.store_now_ms());
                    match self.shared.store.put(&key, m.encode(), PutMode::Create).await {
                        Ok(etag) => {
                            let v = Arc::new(Version::new(m, etag, self.shared.clone()));
                            return Ok((self.install(v, started), started));
                        }
                        Err(StoreError::PreconditionFailed) | Err(StoreError::Unavailable(_)) => {
                            probe::hit(Probe::ManifestCreateRaced);
                            tokio::time::sleep(Duration::from_millis(5)).await;
                            continue;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                Err(StoreError::NotFound) => return Err(Error::not_found("database not found")),
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Load the database, creating it if `create` and it does not exist.
    pub async fn open(self: &Arc<Self>, create: bool) -> Result<Arc<Version>> {
        if let Some(c) = self.current() {
            return Ok(c);
        }
        self.refresh(Instant::now(), create).await
    }

    /// A version satisfying `c`.
    pub async fn read_version(self: &Arc<Self>, c: Consistency) -> Result<Arc<Version>> {
        Ok(self.snapshot(c).await?.version)
    }

    /// Returns a snapshot satisfying `c` paired with the store time at which
    /// that version was current.
    pub async fn snapshot(self: &Arc<Self>, c: Consistency) -> Result<Snapshot> {
        if c == Consistency::Strong {
            self.wait_for_in_flight_write().await;
        }
        self.resolve_snapshot(c).await
    }

    /// Resolves a strong snapshot without waiting for an in-flight manifest
    /// write on this node. The boolean is true if a manifest write was in flight.
    pub async fn latest_snapshot(self: &Arc<Self>) -> Result<(Snapshot, bool)> {
        let write_in_flight = self.has_in_flight_write();
        Ok((self.resolve_snapshot(Consistency::Strong).await?, write_in_flight))
    }

    fn has_in_flight_write(&self) -> bool {
        use std::sync::atomic::Ordering::SeqCst;
        self.writes_finished.load(SeqCst) < self.writes_started.load(SeqCst)
    }

    /// Waits for any manifest write currently in flight on this node, ignoring
    /// writes that start after this call begins.
    async fn wait_for_in_flight_write(&self) {
        use std::sync::atomic::Ordering::SeqCst;
        let target = self.writes_started.load(SeqCst);
        let mut waited_since = None;
        loop {
            let notified = self.write_done.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            if self.writes_finished.load(SeqCst) >= target {
                break;
            }
            if waited_since.is_none() {
                probe::hit(Probe::StrongReadWaitedForWrite);
            }
            waited_since.get_or_insert_with(Instant::now);
            notified.await;
        }
        if let Some(t) = waited_since {
            timing::record_read_wait(t.elapsed());
        }
    }

    /// Returns a snapshot from cache if it satisfies `c`, or refreshes the manifest from storage.
    async fn resolve_snapshot(self: &Arc<Self>, c: Consistency) -> Result<Snapshot> {
        let now = Instant::now();
        if let Some((cur, current_at)) = self.cached(c, now) {
            return self.snapshot_at(check_live(cur)?, current_at).await;
        }
        let not_before = match c {
            Consistency::Strong => INVOKED_AT.try_with(|t| *t).unwrap_or(now).min(now),
            Consistency::Snapshot { .. } => now,
        };
        let (v, current_at) = self.refresh_at(not_before, false).await?;
        self.shared.env.trace(|| format!("{} read v{} after refresh (lease={:?})", self.id, v.m.version, v.m.lease));
        let hot = {
            let mut st = self.st();
            let hot = st.hot(self.shared.cfg.lease_read_threshold) && !st.lease_requested;
            if hot && !self.holds_lease(&st, &v.m) && self.foreign_lease(&v).is_none() {
                st.lease_requested = true;
                true
            } else {
                false
            }
        };
        if hot && !v.m.is_deleted() {
            self.enqueue(Work::Lease { release: false, for_reads: true, tx: None });
        }
        self.snapshot_at(check_live(v)?, current_at).await
    }

    fn cached(self: &Arc<Self>, c: Consistency, now: Instant) -> Option<(Arc<Version>, Instant)> {
        let cfg = &self.shared.cfg;
        let mut st = self.st();
        st.last_access = now;
        let window = Duration::from_millis(cfg.read_lease_ms);
        let since = now.duration_since(st.window_start);
        if since > window {
            st.reads_last_window = if since > 2 * window { 0 } else { st.reads_in_window };
            st.window_start = now;
            st.reads_in_window = 0;
        }
        st.reads_in_window += 1;
        let hot = st.hot(cfg.lease_read_threshold);
        let cur = st.current.clone()?;
        if buggify() {
            return None;
        }
        let lease_left = match st.trusted && st.in_flight_hedged_puts == 0 {
            true => self.own_lease_left(&st, &cur.m),
            false => Duration::ZERO,
        };
        let leased = !lease_left.is_zero();
        let fresh = match c {
            Consistency::Strong => leased,
            Consistency::Snapshot { max_staleness_ms } => {
                leased
                    || st.verified_at.is_some_and(|v| now.duration_since(v) <= Duration::from_millis(max_staleness_ms))
            }
        };
        let want_lease = leased && hot && !st.lease_requested && lease_left < Duration::from_millis(cfg.lease_ms / 5);
        if want_lease {
            st.lease_requested = true;
        }
        let current_at = match (leased, st.verified_at) {
            (false, Some(va)) => va,
            _ => now,
        };
        drop(st);
        if want_lease {
            self.enqueue(Work::Lease { release: false, for_reads: true, tx: None });
        }
        if !fresh {
            return None;
        }
        self.shared.env.trace(|| {
            format!("{} read v{} from cache (leased={leased}, lease={:?})", self.id, cur.m.version, cur.m.lease)
        });
        Some((cur, current_at))
    }

    /// Pairs `v` with the store time corresponding to `at`, syncing the store
    /// clock first if `v` contains expiring entries and no measurement exists.
    async fn snapshot_at(&self, v: Arc<Version>, at: Instant) -> Result<Snapshot> {
        let store = match self.shared.store_now_ms() {
            Some(t) => t,
            None if v.expiring => self.shared.store_time_ms().await?,
            None => 0,
        };
        let store = store.saturating_sub(at.elapsed().as_millis() as u64);
        Ok(Snapshot { time_ms: v.m.time_ms.max(store), version: v })
    }

    /// Marks the database deleted so all subsequent reads and commits fail with not-found.
    pub async fn mark_deleted(self: &Arc<Self>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.enqueue(Work::MarkDeleted { tx });
        rx.await.map_err(|_| Error::unavailable("delete interrupted"))?
    }

    pub async fn begin(self: &Arc<Self>, isolation: Isolation, c: Consistency) -> Result<Txn> {
        let snap = self.snapshot(c).await?;
        Ok(Txn::new(snap, isolation, self.shared.env.random_u64(), self.shared.cfg.txn_max_write_bytes))
    }

    /// Begins a strong serializable transaction without waiting for an
    /// in-flight manifest write on this node. If [`Txn::needs_revalidation`]
    /// is true when the transaction finishes, pass it to [`Db::revalidate`]
    /// before returning a result.
    pub async fn begin_optimistic(self: &Arc<Self>) -> Result<Txn> {
        let (snap, write_in_flight) = self.latest_snapshot().await?;
        let mut t =
            Txn::new(snap, Isolation::Serializable, self.shared.env.random_u64(), self.shared.cfg.txn_max_write_bytes);
        if write_in_flight || buggify() {
            t.mark_optimistic();
            probe::hit(Probe::OptimisticBegin);
        }
        Ok(t)
    }

    /// Waits for a strong snapshot and validates `t`'s read set against it.
    /// Returns the snapshot version if all reads remain valid, or `None` if `t`
    /// conflicted (or performed untracked reads) and must be re-executed.
    pub async fn revalidate(self: &Arc<Self>, t: Txn) -> Result<Option<Seq>> {
        if t.has_untracked_reads() {
            probe::hit(Probe::OptimisticRerun);
            return Ok(None);
        }
        let snap = self.snapshot(Consistency::Strong).await?;
        match validate(&t.into_request(), &snap.version, &[], &[], snap.time_ms).await {
            Ok(()) => Ok(Some(snap.version.m.version)),
            Err(Error::Conflict(_)) => {
                probe::hit(Probe::OptimisticRerun);
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// Creates a writer for a new staged blob, syncing the store clock first if needed.
    pub async fn blob_writer(self: &Arc<Self>, content_type: &str) -> BlobWriter {
        if let Err(e) = self.shared.store_time_ms().await {
            crate::debug!("staging a blob without store time: {e}");
        }
        let w = BlobWriter::new(self.shared.clone(), &self.id, content_type);
        match self.quota() {
            Some(_) => w.with_quota(StageGuard::new(self.clone())),
            None => w,
        }
    }

    /// Commit a transaction's request. Read-only requests return their read
    /// version without touching storage.
    pub async fn commit(self: &Arc<Self>, req: CommitRequest) -> Result<Seq> {
        Ok(self.commit_detailed(req).await?.version)
    }

    /// Commits `req` and reports whether the commit ID was already applied.
    pub async fn commit_detailed(self: &Arc<Self>, mut req: CommitRequest) -> Result<Committed> {
        if req.writes.is_empty() {
            return Ok(Committed {
                version: req.read_version,
                replayed: false,
                time_ms: req.read_time,
                appends: Vec::new(),
            });
        }
        if req.deadline.is_none() {
            req.deadline = Some(Instant::now() + Duration::from_millis(self.shared.cfg.commit_timeout_ms));
        }
        if req.writes.staged_blobs().next().is_some() || req.writes.needs_clock() {
            self.shared.store_time_ms().await?;
        }
        let (tx, rx) = oneshot::channel();
        let enqueued = Instant::now();
        self.enqueue(Work::Commit { req, tx });
        let r = rx.await;
        timing::record_commit_wait(enqueued.elapsed());
        r.map_err(|_| Error::Unavailable {
            message: "commit interrupted".into(),
            retry_after_ms: None,
            outcome_unknown: true,
        })?
    }

    /// Acquire or renew this node's lease.
    pub async fn acquire_lease(self: &Arc<Self>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.enqueue(Work::Lease { release: false, for_reads: false, tx: Some(tx) });
        rx.await.map_err(|_| Error::unavailable("lease request interrupted"))?
    }

    /// Releases this node's lease so other nodes can take over immediately. Returns whether
    /// this node held the lease, and released it.
    pub async fn release_lease(self: &Arc<Self>) -> Result<bool> {
        let held = self.current().is_some_and(|c| c.m.lease.as_ref().is_some_and(|l| l.node == self.shared.node_id));
        if !held {
            return Ok(false);
        }
        let (tx, rx) = oneshot::channel();
        self.enqueue(Work::Lease { release: true, for_reads: false, tx: Some(tx) });
        rx.await.map_err(|_| Error::unavailable("lease release interrupted"))?.map(|()| true)
    }

    /// Reads the status of commit `id` at a linearizable snapshot.
    pub async fn commit_status(self: &Arc<Self>, id: u64) -> Result<Option<CommitState>> {
        let v = self.read_version(Consistency::Strong).await?;
        commit_state(&v, None, id).await
    }

    /// Returns the committed result for `id`, or records an abort marker so later attempts with
    /// `id` cannot be applied.
    pub async fn resolve_commit(self: &Arc<Self>, id: u64) -> Result<Option<Committed>> {
        let (tx, rx) = oneshot::channel();
        self.enqueue(Work::Resolve { id, tx });
        rx.await.map_err(|_| Error::unavailable("commit resolution interrupted"))?
    }

    /// Waits until a version greater than `after` is installed locally, `timeout` elapses, or
    /// [`super::Engine::end_waits`] is called. If [`Db::notice_commit`] has recorded a version
    /// greater than `after`, triggers a background manifest reload.
    pub async fn wait_for_change(self: &Arc<Self>, after: Seq, timeout: Duration) {
        use std::sync::atomic::Ordering::SeqCst;
        let notified = self.installed.notified();
        let mut notified = std::pin::pin!(notified);
        notified.as_mut().enable();
        if self.shared.waits_ended() || self.current().is_some_and(|c| c.m.version > after) {
            return;
        }
        let _guard = WaiterGuard::new(&self.waiters);
        if self.noticed_version.load(SeqCst) > after {
            self.load_noticed();
        }
        let _ = tokio::time::timeout(timeout, notified).await;
    }

    pub(crate) fn wake_waiters(&self) {
        self.installed.notify_waiters();
    }

    /// Returns the instant at which the cached manifest exceeds `max_staleness`, or `None` if
    /// the manifest has not been validated.
    pub fn stale_at(&self, max_staleness: Duration) -> Option<Instant> {
        self.st().verified_at.map(|v| v + max_staleness)
    }

    /// Records that another node committed `version` and reloads the manifest if any callers
    /// are waiting in [`Db::wait_for_change`].
    pub fn notice_commit(self: &Arc<Self>, version: Seq) {
        use std::sync::atomic::Ordering::SeqCst;
        self.noticed_version.fetch_max(version, SeqCst);
        if self.waiters.load(SeqCst) > 0 {
            self.load_noticed();
        }
    }

    /// Reloads the manifest in the background until the cached version reaches
    /// `noticed_version` or all waiters leave. At most 1 reload task runs at a time. If a
    /// reload fails or does not advance the cached version, the task stops unless a higher
    /// `noticed_version` arrived during the read.
    fn load_noticed(self: &Arc<Self>) {
        use std::sync::atomic::Ordering::SeqCst;
        let version = |db: &Db| db.current().map(|c| c.m.version);
        let behind = move |db: &Db| {
            db.waiters.load(SeqCst) > 0 && version(db).is_none_or(|v| v < db.noticed_version.load(SeqCst))
        };
        if !behind(self) || self.refreshing.swap(true, SeqCst) {
            return;
        }
        let db = self.clone();
        self.shared.env.spawn(async move {
            loop {
                let mut stalled_at = None;
                while behind(&db) {
                    probe::hit(Probe::NoticedVersionLoaded);
                    let (before, noticed) = (version(&db), db.noticed_version.load(SeqCst));
                    if db.read_version(Consistency::Strong).await.is_err() || version(&db) == before {
                        stalled_at = Some(noticed);
                        break;
                    }
                }
                db.refreshing.store(false, SeqCst);
                let stalled = stalled_at.is_some_and(|n| n == db.noticed_version.load(SeqCst));
                if stalled || !behind(&db) || db.refreshing.swap(true, SeqCst) {
                    return;
                }
            }
        });
    }

    fn enqueue(self: &Arc<Self>, w: Work) {
        let spawn = {
            let mut q = self.queue();
            q.items.push_back(QueuedWork {
                work: w,
                enqueued_at: Instant::now(),
                request_id: crate::log::request_id(),
            });
            if q.running {
                false
            } else {
                q.running = true;
                true
            }
        };
        if spawn {
            let me = self.clone();
            self.shared.env.spawn(async move { me.committer().await });
        }
    }

    async fn committer(self: Arc<Self>) {
        loop {
            let queued: Vec<QueuedWork> = {
                let mut q = self.queue();
                if q.items.is_empty() {
                    q.running = false;
                    return;
                }
                let mut n = q.items.len().min(self.shared.cfg.commit_batch_max);
                if buggify() {
                    n = 1 + crate::buggify::below(n as u64) as usize;
                }
                let mut batch: Vec<QueuedWork> = q.items.drain(..n).collect();
                if batch.iter().any(|w| matches!(w.work, Work::Commit { .. }))
                    && let Some(flush) = q.parked_flush.take()
                {
                    batch.insert(0, flush);
                }
                batch
            };
            let started = Instant::now();
            let tally = Arc::new(Tally::default());
            let (enqueued, batch): (Vec<_>, Vec<_>) =
                queued.into_iter().map(|q| ((q.enqueued_at, q.request_id), q.work)).unzip();
            let txn_count = batch.iter().filter(|w| matches!(w, Work::Commit { .. })).count();
            timing::scoped(tally.clone(), self.process(batch)).await;
            if tally.store_calls(super::ObjectClass::Manifest, StoreOp::Put) > 0 {
                self.shared.metrics.commit_batch(started.elapsed(), txn_count);
            }
            self.log_batch(&enqueued, txn_count, started, &tally);
        }
    }

    fn log_batch(&self, enqueued: &[(Instant, Option<Arc<str>>)], txn_count: usize, started: Instant, tally: &Tally) {
        use crate::log::{self, Level};
        use serde_json::json;
        if tally.store_calls(super::ObjectClass::Manifest, StoreOp::Put) == 0 || !log::enabled(Level::Info) {
            return;
        }
        let oldest = enqueued.iter().map(|(at, _)| *at).min().unwrap_or(started);
        let mut fields = vec![
            ("db", json!(self.id)),
            ("txns", json!(txn_count)),
            ("work", json!(enqueued.len())),
            ("queued_ms", json!((started - oldest).as_millis() as u64)),
            ("build_ms", json!(tally.build_ms())),
            ("ms", json!(started.elapsed().as_millis() as u64)),
        ];
        if let Some(store) = tally.store_json() {
            fields.push(("store", store));
        }
        let ids: Vec<&str> = enqueued.iter().filter_map(|(_, id)| id.as_deref()).collect();
        if !ids.is_empty() {
            fields.push(("request_ids", json!(ids)));
        }
        log::event(Level::Info, "commit", &fields);
    }

    async fn base_for_commit(self: &Arc<Self>) -> Result<Arc<Version>> {
        let (cur, trusted) = {
            let st = self.st();
            (st.current.clone(), st.trusted)
        };
        match cur {
            Some(c) if trusted => Ok(c),
            _ => self.refresh(Instant::now(), false).await,
        }
    }

    async fn process(self: &Arc<Self>, batch: Vec<Work>) {
        let cfg = self.shared.cfg.clone();
        let mut maybe_applied = false;
        let mut unknown_writes: Vec<(u64, Instant, String)> = Vec::new();
        let mut attempt = 0u32;
        let mut base = match self.base_for_commit().await {
            Ok(v) => v,
            Err(e) => {
                self.clear_lease_flag(&batch);
                for w in batch {
                    w.fail(e.clone());
                }
                return;
            }
        };
        loop {
            attempt += 1;
            let attempt_started = Instant::now();
            let now = Now { wall_ms: self.shared.env.now_ms(), store_ms: self.shared.store_now_ms() };
            let foreign = self.foreign_lease(&base).map(|(l, left)| (l.node.clone(), left));
            if let Some((holder, left)) = foreign {
                let e = Error::NotOwner { holder, retry_after_ms: left.as_millis() as u64 };
                probe::hit(if maybe_applied { Probe::CommitUnknownAfterLeaseMoved } else { Probe::CommitNotOwner });
                let e = if maybe_applied { unknown(e) } else { e };
                self.fail_batch(&base, batch, e).await;
                return;
            }
            if self.shared.stopping() && !base.m.lease.as_ref().is_some_and(|l| l.node == self.shared.node_id) {
                let e = Error::unavailable("this node is shutting down");
                let e = if maybe_applied { unknown(e) } else { e };
                self.fail_batch(&base, batch, e).await;
                return;
            }
            let build_started = Instant::now();
            let plan = self.build(&base, &batch, now).await;
            timing::record_build_time(build_started.elapsed());
            let plan = match plan {
                Ok(p) => p,
                Err(e) => {
                    let e = if maybe_applied { unknown(e) } else { e };
                    self.fail_batch(&base, batch, e).await;
                    return;
                }
            };
            if plan.quota_refused {
                self.st().quota_pressure = true;
                probe::hit(Probe::QuotaRefused);
            }
            let Some(m) = plan.manifest else {
                self.maybe_sweep(&base);
                self.schedule_trims(plan.due);
                self.clear_lease_flag(&batch);
                for (w, o) in batch.into_iter().zip(plan.outcomes) {
                    // If a prior attempt in this batch was ambiguous and is not yet fenced, mark the outcome as unknown.
                    let o = match o {
                        Outcome::Seq(Err(e @ Error::Unavailable { .. })) if maybe_applied => {
                            Outcome::Seq(Err(unknown(e)))
                        }
                        other => mark_superseded_flush_unknown(&w, other, maybe_applied),
                    };
                    w.deliver(o);
                }
                return;
            };
            if crate::buggify::fires(0.02) {
                let ms = crate::buggify::below(cfg.lease_ms);
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
            self.shared.env.crash_point().await;
            let started = Instant::now();
            self.limit_own_lease(&m, attempt_started);
            self.writes_started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let data = m.encode_with(&mut self.pages.lock().unwrap_or_else(|e| e.into_inner()));
            let res = self.put_manifest(data, &base.etag).await;
            if !res.is_ok() {
                if matches!(res, Err(StoreError::Unavailable(_))) {
                    self.st().distrust();
                }
                self.write_finished();
            }
            match res {
                Ok(etag) => {
                    self.note_own_lease(&m, attempt_started);
                    for (p, n) in plan.probes.into_iter().flatten() {
                        probe::hit_n(p, n);
                    }
                    self.shared.env.crash_point().await;
                    let v = Arc::new(Version::new(m, etag, self.shared.clone()));
                    let v = self.install(v, started);
                    self.write_finished();
                    self.shared.env.trace(|| {
                        format!("{} committed v{} ws{} lease={:?}", self.id, v.m.version, v.m.write_seq, v.m.lease)
                    });
                    self.clear_lease_flag(&batch);
                    for (w, o) in batch.into_iter().zip(plan.outcomes) {
                        let o = mark_superseded_flush_unknown(&w, o, maybe_applied);
                        w.deliver(o);
                    }
                    self.after_commit(&v, plan.gc, plan.due);
                    if v.m.version > base.m.version {
                        self.shared.committed(&self.id, v.m.version);
                    }
                    return;
                }
                Err(StoreError::PreconditionFailed) => probe::hit(Probe::ManifestCasLost),
                Err(StoreError::Unavailable(_)) => {
                    probe::hit(Probe::ManifestWriteAmbiguous);
                    if !plan.gc.is_empty() {
                        probe::hit(Probe::ObsoleteDeletedAfterAmbiguousWrite);
                    }
                    maybe_applied = true;
                    unknown_writes.push((m.write_seq, attempt_started, base.etag.clone()));
                    self.st().distrust();
                    // If the write was applied, `plan.gc` was removed from `obsolete` in the manifest.
                    // These objects have already passed their grace period, so they can be deleted safely
                    // even if the write failed.
                    self.delete_obsolete(plan.gc);
                }
                Err(e) => {
                    let e: Error = e.into();
                    let e = if maybe_applied { unknown(e) } else { e };
                    self.fail_batch(&base, batch, e).await;
                    return;
                }
            }
            if attempt >= cfg.commit_retry_max {
                probe::hit(Probe::CommitRetriesExhausted);
                let e = Error::Unavailable {
                    message: "could not commit after retries".into(),
                    retry_after_ms: Some(100),
                    outcome_unknown: maybe_applied,
                };
                self.fail_batch(&base, batch, e).await;
                return;
            }
            let delay = 5 * (1u64 << attempt.min(6)) + self.shared.env.random_below(10);
            tokio::time::sleep(Duration::from_millis(delay)).await;
            loop {
                match self.refresh(Instant::now(), false).await {
                    Ok(v) => {
                        let sent =
                            unknown_writes.iter().filter(|(ws, ..)| *ws == v.m.write_seq).map(|(_, s, _)| *s).min();
                        if let Some(sent) = sent {
                            probe::hit(Probe::AmbiguousWriteLanded);
                            self.note_own_lease(&v.m, sent);
                        }
                        // Once the manifest moves past every ETag targeted by an ambiguous write,
                        // any write that was not applied cannot succeed.
                        if maybe_applied
                            && !unknown_writes.iter().any(|(.., etag)| *etag == v.etag)
                            && !self.has_applied_work(&v, &batch).await
                        {
                            probe::hit(Probe::AmbiguousWriteSettled);
                            maybe_applied = false;
                        }
                        base = v;
                        break;
                    }
                    Err(e) => {
                        attempt += 1;
                        if attempt >= cfg.commit_retry_max {
                            let e = if maybe_applied { unknown(e) } else { e };
                            self.fail_batch(&base, batch, e).await;
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                    }
                }
            }
        }
    }

    /// Delivers committed or aborted outcomes already recorded in `base`, and fails the remaining
    /// work with `e`.
    async fn fail_batch(self: &Arc<Self>, base: &Version, batch: Vec<Work>, e: Error) {
        let mut rest = Vec::with_capacity(batch.len());
        for w in batch {
            let decided = match &w {
                Work::Commit { req, .. } => commit_state(base, None, req.commit_id).await.ok().flatten(),
                _ => None,
            };
            match (decided, &w) {
                (Some(CommitState::Applied(c)), Work::Commit { req, .. }) => {
                    probe::hit(Probe::DecidedCommitAnswered);
                    self.schedule_trims(req.writes.streams.iter().map(StreamOp::id).collect());
                    w.deliver(Outcome::Seq(Ok(c)));
                }
                (Some(CommitState::Aborted), Work::Commit { req, .. }) => {
                    probe::hit(Probe::DecidedCommitAnswered);
                    let id = req.commit_id;
                    w.deliver(Outcome::Seq(Err(aborted(id))));
                }
                _ => rest.push(w),
            }
        }
        self.finish_failed(rest, e);
    }

    /// Returns true if `v` may include changes from `batch`.
    async fn has_applied_work(&self, v: &Version, batch: &[Work]) -> bool {
        for w in batch {
            match w {
                Work::Flush { .. } => return true,
                Work::Commit { req, .. }
                    if !matches!(commit_state(v, None, req.commit_id).await, Ok(None | Some(CommitState::Aborted))) =>
                {
                    return true;
                }
                _ => {}
            }
        }
        false
    }

    /// Writes the manifest with `If-Match: etag`, issuing a hedged `PUT` after `manifest_put_hedge_ms`.
    /// If the hedged `PUT` fails with `PreconditionFailed` while the first `PUT` is still in flight,
    /// returns `StoreError::Unavailable` and tracks the first `PUT` in `in_flight_hedged_puts` until
    /// it finishes.
    async fn put_manifest(self: &Arc<Self>, data: Bytes, etag: &str) -> crate::store::SResult<String> {
        let (store, key, etag) = (self.shared.store.clone(), manifest_key(&self.id), etag.to_string());
        let put = || -> crate::store::BoxFut<'static, crate::store::SResult<String>> {
            let (store, key, data, etag) = (store.clone(), key.clone(), data.clone(), etag.clone());
            Box::pin(async move { store.put(&key, data, PutMode::IfMatch(etag)).await })
        };
        let mut hedge_ms = self.shared.cfg.manifest_put_hedge_ms;
        if hedge_ms > 0 && buggify() {
            hedge_ms = 1;
        }
        let mut first = put();
        if hedge_ms == 0 {
            return first.await;
        }
        if let Either::Left(r) =
            race(&mut first, &mut Box::pin(tokio::time::sleep(Duration::from_millis(hedge_ms)))).await
        {
            return r;
        }
        probe::hit(Probe::ManifestPutHedged);
        let ambiguous =
            || StoreError::Unavailable("hedged manifest write rejected; the first write may have been applied".into());
        let mut hedge = put();
        let (r, hedge_won) = match race(&mut first, &mut hedge).await {
            Either::Left(r) => (r, false),
            Either::Right(r) => (r, true),
        };
        let e = match r {
            Ok(etag) => {
                if hedge_won {
                    probe::hit(Probe::HedgeWon);
                }
                return Ok(etag);
            }
            Err(StoreError::PreconditionFailed) if hedge_won => {
                probe::hit(Probe::HedgeRejectedFirstInFlight);
                self.st().in_flight_hedged_puts += 1;
                let me = self.clone();
                self.shared.env.spawn(async move {
                    let landed = !matches!(first.await, Err(StoreError::PreconditionFailed));
                    let mut st = me.st();
                    st.in_flight_hedged_puts -= 1;
                    if landed {
                        st.distrust();
                    }
                });
                return Err(ambiguous());
            }
            Err(e) => e,
        };
        match (e, if hedge_won { first.await } else { hedge.await }) {
            (_, Ok(etag)) => Ok(etag),
            (StoreError::Unavailable(_), _) | (_, Err(StoreError::Unavailable(_))) => Err(ambiguous()),
            (e, Err(_)) => Err(e),
        }
    }

    fn write_finished(&self) {
        self.writes_finished.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.write_done.notify_waiters();
    }

    fn clear_lease_flag(&self, batch: &[Work]) {
        if batch.iter().any(|w| matches!(w, Work::Lease { .. })) {
            self.st().lease_requested = false;
        }
    }

    fn finish_failed(&self, batch: Vec<Work>, e: Error) {
        self.clear_lease_flag(&batch);
        for w in batch {
            w.fail(e.clone());
        }
    }

    async fn build(&self, base: &Version, batch: &[Work], now: Now) -> Result<Plan> {
        let cfg = &self.shared.cfg;
        let mut m = base.m.clone();
        let mut tail: BTreeMap<Bytes, Entry> = m.tail.drain(..).map(|e| (e.key.clone(), e)).collect();
        // Ensure the manifest timestamp does not move backward relative to `base` or any committed transaction's read time.
        let time = batch
            .iter()
            .filter_map(|w| match w {
                Work::Commit { req, .. } => Some(req.read_time),
                _ => None,
            })
            .chain([base.m.time_ms, now.store_ms.unwrap_or(0)])
            .max()
            .unwrap_or(0);
        let mut last_seq = base.m.version;
        let mut outcomes = Vec::with_capacity(batch.len());
        let mut accepted: Vec<&WriteSet> = Vec::new();
        let mut touched: Vec<KeyRange> = Vec::new();
        let mut due: BTreeSet<u64> = BTreeSet::new();
        let mut data_changed = false;
        let mut meta_changed = false;
        let mut release = false;
        let mut flush_tally = FlushTally::default();
        let mut lease_ms = cfg.lease_ms;
        let quota = self.quota();
        let mut size = base.m.stored_bytes();
        let mut quota_refused = false;
        if base.m.is_deleted() {
            probe::hit(Probe::DeletedDatabaseWrite);
            let outcomes = batch
                .iter()
                .map(|w| match w {
                    Work::MarkDeleted { .. } => Outcome::Unit(Ok(())),
                    _ => Outcome::Unit(Err(Error::not_found("database deleted"))),
                })
                .collect();
            return Ok(Plan { manifest: None, outcomes, gc: Vec::new(), quota_refused, probes: None, due });
        }
        // Background work is planned under the local lease and must not be installed if the lease
        // was released or lost.
        let leased = base.m.lease.as_ref().is_some_and(|l| l.node == self.shared.node_id);
        for w in batch {
            if !leased && matches!(w, Work::Compaction { .. } | Work::Flush { .. } | Work::Obsolete { .. }) {
                probe::hit(Probe::BackgroundInstallUnleased);
                outcomes.push(Outcome::Unit(Err(Error::Conflict("lease lost before background install".into()))));
                continue;
            }
            match w {
                Work::Commit { req, .. } => {
                    let record = commit_key(req.commit_id);
                    let state = commit_state(base, Some(&tail), req.commit_id).await?;
                    if state == Some(CommitState::Aborted) {
                        probe::hit(Probe::AbortedCommitRejected);
                        outcomes.push(Outcome::Seq(Err(aborted(req.commit_id))));
                        continue;
                    }
                    if let Some(CommitState::Applied(c)) = state {
                        probe::hit(Probe::CommitReplayed);
                        let at = pending(base, &m, &tail);
                        for id in req.writes.streams.iter().map(StreamOp::id) {
                            if at.state(id).await?.is_some_and(|s| s.trim_due(time)) {
                                due.insert(id);
                            }
                        }
                        outcomes.push(Outcome::Seq(Ok(c)));
                        continue;
                    }
                    if req.deadline.is_some_and(|d| Instant::now() > d) {
                        probe::hit(Probe::CommitDeadlinePassed);
                        outcomes.push(Outcome::Seq(Err(Error::Unavailable {
                            message: "commit deadline exceeded".into(),
                            retry_after_ms: Some(100),
                            outcome_unknown: false,
                        })));
                        continue;
                    }
                    if let Err(e) = validate(req, base, &accepted, &touched, time).await {
                        if matches!(e, Error::Conflict(_)) {
                            probe::hit(Probe::CommitConflict);
                        }
                        outcomes.push(Outcome::Seq(Err(e)));
                        continue;
                    }
                    if let Err(e) = bindable(&req.writes, &m, now.store_ms, cfg) {
                        probe::hit(Probe::StagedBlobUnbindable);
                        outcomes.push(Outcome::Seq(Err(e)));
                        continue;
                    }
                    if req.writes.needs_clock() && now.store_ms.is_none() {
                        outcomes.push(Outcome::Seq(Err(Error::unavailable("store clock not synchronized"))));
                        continue;
                    }
                    let (added, blob_bytes) = growth(&req.writes);
                    if let Some(limit) = quota
                        && added > 0
                        && size + added > limit
                    {
                        quota_refused = true;
                        outcomes.push(Outcome::Seq(Err(quota_exceeded(limit))));
                        continue;
                    }
                    let new_seq = last_seq + 1;
                    let fx = match req.writes.streams.is_empty() {
                        true => stream::Effects::default(),
                        false => {
                            match stream::plan(&req.writes.streams, &pending(base, &m, &tail), new_seq, time).await {
                                Ok(fx) => fx,
                                Err(
                                    e @ (Error::NotFound(_)
                                    | Error::AlreadyExists(_)
                                    | Error::PreconditionFailed { .. }),
                                ) => {
                                    outcomes.push(Outcome::Seq(Err(e)));
                                    continue;
                                }
                                Err(e) => return Err(e),
                            }
                        }
                    };
                    size += added;
                    for r in &req.writes.range_deletes {
                        delete_range(base, &mut m, &mut tail, r, new_seq, cfg.range_delete_points_max).await?;
                    }
                    for (k, p) in &req.writes.puts {
                        let e = match p {
                            Some(p) => Entry {
                                key: k.clone(),
                                seq: new_seq,
                                value: p.value.clone(),
                                expires: p.ttl.map(|t| t.at(time)),
                            },
                            None => Entry::tombstone(k.clone(), new_seq),
                        };
                        tail.insert(k.clone(), e);
                    }
                    for e in fx.entries {
                        tail.insert(e.key.clone(), e);
                    }
                    for rt in fx.trims.iter().filter_map(|r| RangeTombstone::new(r, new_seq)) {
                        put_range_tombstone(&mut m, &mut tail, rt);
                    }
                    let value = encode_commit_record(time, &fx.appends);
                    tail.insert(record.clone(), Entry::new(record, new_seq, Value::Inline(value)));
                    if blob_bytes > 0 {
                        m.blob_bytes.bound += blob_bytes;
                        let mut g = self.staging();
                        for b in req.writes.staged_blobs() {
                            g.staged.remove(&b.id);
                        }
                    }
                    accepted.push(&req.writes);
                    touched.extend(fx.touched);
                    due.extend(fx.states.iter().filter(|(_, s)| s.trim_due(time)).map(|(id, _)| *id));
                    last_seq = new_seq;
                    data_changed = true;
                    outcomes.push(Outcome::Seq(Ok(Committed {
                        version: new_seq,
                        replayed: false,
                        time_ms: time,
                        appends: fx.appends,
                    })));
                }
                Work::Lease { release: r, for_reads, .. } => {
                    release |= *r;
                    if *for_reads {
                        lease_ms = lease_ms.max(cfg.read_lease_ms);
                    }
                    meta_changed = true;
                    outcomes.push(Outcome::Unit(Ok(())));
                }
                Work::Compaction {
                    inputs,
                    outputs,
                    purged_max_seq,
                    sealed_bytes,
                    compaction_seq,
                    started_ms,
                    deep,
                    levels,
                    ..
                } => {
                    let Some(now_ms) = now.store_ms else {
                        outcomes.push(Outcome::Unit(Err(Error::unavailable("store clock not synchronized"))));
                        continue;
                    };
                    // Reject compaction outputs that are old enough for a concurrent sweep to treat as orphans.
                    if *started_ms + cfg.orphan_grace_ms <= now_ms + bind_margin_ms(cfg) + cfg.clock_sync_max_error_ms {
                        probe::hit(Probe::CompactionOutlivedOrphanGrace);
                        outcomes.push(Outcome::Unit(Err(Error::unavailable("compaction outlived the orphan grace"))));
                        continue;
                    }
                    let present = inputs.iter().all(|id| m.segments.iter().any(|s| s.id == *id));
                    let installed = !outputs.is_empty()
                        && outputs.iter().all(|o| m.segments.iter().any(|s| s.id == o.id && s.level == o.level));
                    if m.compaction_seq == *compaction_seq && present {
                        if compaction::deep_after(&m, inputs, outputs) != *deep {
                            outcomes.push(Outcome::Unit(Err(Error::internal("compaction planned on other levels"))));
                            continue;
                        }
                        let mut segments: Vec<SegmentMeta> =
                            m.level(0).iter().filter(|s| !inputs.contains(&s.id)).cloned().collect();
                        segments.extend(deep.iter().cloned());
                        let before = std::mem::replace(&mut m.segments, segments);
                        if m.check_levels().is_err() || outputs.iter().any(|o| o.level == 0) {
                            m.segments = before;
                            outcomes.push(Outcome::Unit(Err(Error::internal("compaction output overlaps its level"))));
                            continue;
                        }
                        for id in inputs.iter().filter(|id| !outputs.iter().any(|o| o.id == **id)) {
                            m.obsolete.push(Obsolete { kind: ObjKind::Segment, id: id.clone(), since_ms: now_ms });
                        }
                        if m.levels != *levels
                            && let Some(old) = std::mem::replace(&mut m.levels, levels.clone())
                        {
                            m.obsolete.push(Obsolete { kind: ObjKind::Levels, id: old, since_ms: now_ms });
                        }
                        m.tombstone_horizon = m.tombstone_horizon.max(*purged_max_seq);
                        m.chunk_bytes.bound += *sealed_bytes;
                        m.compaction_seq += 1;
                        meta_changed = true;
                        outcomes.push(Outcome::Unit(Ok(())));
                    } else if installed {
                        probe::hit(Probe::CompactionInstallReplayed);
                        outcomes.push(Outcome::Unit(Ok(())));
                    } else {
                        probe::hit(Probe::CompactionInputsChanged);
                        outcomes.push(Outcome::Unit(Err(Error::Conflict("compaction inputs changed".into()))));
                    }
                }
                Work::Flush { flushed_version, flush_seq, output, started_ms, .. } => {
                    let Some(now_ms) = now.store_ms else {
                        outcomes.push(Outcome::Unit(Err(Error::unavailable("store clock not synchronized"))));
                        continue;
                    };
                    let installed = !output.segments.is_empty()
                        && output.segments.iter().all(|f| m.segments.iter().any(|s| s.id == f.id));
                    if installed {
                        probe::hit(Probe::FlushInstallReplayed);
                        outcomes.push(Outcome::Unit(Ok(())));
                        continue;
                    }
                    // Reject the flush if another flush was installed, if tombstones covering
                    // flushed entries were purged, or if the output objects exceeded the orphan
                    // grace period.
                    let superseded = m.flush_seq != *flush_seq
                        || m.tombstone_horizon > *flushed_version
                        || output.segments.iter().any(|f| m.obsolete.iter().any(|o| o.id == f.id))
                        || *started_ms + cfg.orphan_grace_ms
                            <= now_ms + bind_margin_ms(cfg) + cfg.clock_sync_max_error_ms;
                    if superseded {
                        self.shared.env.trace(|| format!("{} flush through v{flushed_version} superseded", self.id));
                        probe::hit(Probe::FlushSuperseded);
                        outcomes.push(Outcome::Unit(Err(Error::Conflict("flush superseded".into()))));
                        continue;
                    }
                    self.trace_flush("background", output, *flushed_version);
                    output.install(&mut m, &mut tail, *flushed_version, &mut flush_tally);
                    meta_changed = true;
                    outcomes.push(Outcome::Unit(Ok(())));
                }
                Work::Obsolete { items, blob_bytes: (swept, at), chunk_bytes: (chunks_swept, chunks_at), .. } => {
                    let Some(now_ms) = now.store_ms else {
                        outcomes.push(Outcome::Unit(Err(Error::unavailable("store clock not synchronized"))));
                        continue;
                    };
                    for it in items {
                        let listed = m.obsolete.iter().any(|o| o.kind == it.kind && o.id == it.id);
                        let live = match it.kind {
                            ObjKind::Segment => m.segments.iter().any(|s| s.id == it.id),
                            ObjKind::Levels => m.levels.as_deref() == Some(it.id.as_str()),
                            ObjKind::Blob => false,
                        };
                        if !listed && !live {
                            m.obsolete.push(it.clone());
                        }
                    }
                    if *at >= m.blob_bytes.swept_at {
                        m.blob_bytes.swept = *swept;
                        m.blob_bytes.swept_at = *at;
                    }
                    if *chunks_at >= m.chunk_bytes.swept_at {
                        m.chunk_bytes.swept = *chunks_swept;
                        m.chunk_bytes.swept_at = *chunks_at;
                    }
                    m.last_sweep_ms = now_ms;
                    meta_changed = true;
                    outcomes.push(Outcome::Unit(Ok(())));
                }
                Work::Resolve { id, .. } => match commit_state(base, Some(&tail), *id).await? {
                    Some(CommitState::Applied(c)) => outcomes.push(Outcome::Resolved(Ok(Some(c)))),
                    Some(CommitState::Aborted) => outcomes.push(Outcome::Resolved(Ok(None))),
                    None => {
                        let (record, seq) = (commit_key(*id), last_seq + 1);
                        tail.insert(record.clone(), Entry::new(record, seq, Value::Inline(ABORT_RECORD)));
                        last_seq = seq;
                        data_changed = true;
                        probe::hit(Probe::CommitAborted);
                        outcomes.push(Outcome::Resolved(Ok(None)));
                    }
                },
                Work::MarkDeleted { .. } => {
                    m.flags |= super::manifest::FLAG_DELETED;
                    meta_changed = true;
                    release = true;
                    outcomes.push(Outcome::Unit(Ok(())));
                }
            }
        }
        if !data_changed && !meta_changed {
            return Ok(Plan { manifest: None, outcomes, gc: Vec::new(), quota_refused, probes: None, due });
        }
        if data_changed {
            m.version = last_seq;
        }
        m.time_ms = time;
        let records_horizon = now.store_ms.map_or(0, |t| m.version_at_least_age(t, cfg.commit_retention_ms));
        let mut expired = 0;
        tail.retain(|k, e| {
            if is_commit_key(k) {
                let keep = e.seq > records_horizon;
                expired += u64::from(!keep);
                return keep;
            }
            if e.expired(time) {
                expired += 1;
                *e = Entry::tombstone(e.key.clone(), e.seq);
            }
            true
        });
        m.tail = tail.into_values().collect();
        let mut gc = Vec::new();
        let mut retired = 0;
        let mut purged_tombstones = 0;
        if let Some(now_ms) = now.store_ms {
            let purge = m.version_at_least_age(now_ms, cfg.txn_max_lifetime_ms);
            let mut horizon = m.tombstone_horizon;
            if m.segments.iter().all(compaction::holds_records) {
                m.tail.retain(|e| {
                    let purged = e.value.is_tombstone() && e.seq <= purge;
                    if purged {
                        horizon = horizon.max(e.seq);
                        purged_tombstones += 1;
                    }
                    !purged
                });
            }
            // Retire range tombstones when no remaining segment overlaps and no active transaction can read them.
            let segments = &m.segments;
            m.range_deletes.retain(|t| {
                let retire = t.seq <= purge && !segments.iter().any(|s| t.dirties(s));
                if retire {
                    horizon = horizon.max(t.seq);
                    retired += 1;
                }
                !retire
            });
            m.tombstone_horizon = horizon;
            if data_changed && m.version_times.last().is_none_or(|(_, t)| *t + cfg.version_sample_ms <= now_ms) {
                m.version_times.push((m.version, now_ms));
            }
            let keep_after = now_ms.saturating_sub(cfg.txn_max_lifetime_ms.max(cfg.commit_retention_ms));
            while m.version_times.len() >= 2 && m.version_times[1].1 <= keep_after {
                m.version_times.remove(0);
            }
            let keep;
            (gc, keep) = m.obsolete.drain(..).partition(|o| o.since_ms + cfg.gc_grace_ms <= now_ms);
            m.obsolete = keep;
        }
        let takeover = !release && base.m.lease.as_ref().is_some_and(|l| l.node != self.shared.node_id);
        m.lease = if release {
            None
        } else {
            Some(Lease { node: self.shared.node_id.clone(), expires_ms: now.wall_ms + lease_ms, ms: lease_ms })
        };
        let sync_flush = m.tail_bytes() > TAIL_SYNC_FLUSH_FACTOR * cfg.tail_max_bytes;
        if sync_flush || (!m.tail.is_empty() && crate::buggify::fires(0.05)) {
            let (entries, through) = (std::mem::take(&mut m.tail), m.version);
            let f = flush_tail(&self.shared, &self.id, &entries, through).await?;
            probe::hit(Probe::TailFlushedInCommit);
            self.trace_flush("in commit", &f, through);
            let mut rest = BTreeMap::new();
            f.install(&mut m, &mut rest, through, &mut flush_tally);
            m.tail = rest.into_values().collect();
        }
        m.write_seq += 1;
        let probes: [(Probe, u64); 11] = [
            (Probe::RangeTombstoneWritten, m.range_deletes.iter().filter(|t| t.seq > base.m.version).count() as u64),
            (Probe::RangeTombstoneRetired, retired),
            (Probe::EntriesExpired, expired),
            (Probe::ChunkSealedAtFlush, flush_tally.chunks),
            (Probe::TailFlushed, flush_tally.to_segments as u64),
            (Probe::TailSealedInPlace, flush_tally.in_place as u64),
            (Probe::TailTombstonesPurged, purged_tombstones),
            (Probe::CommitBatched, (accepted.len() >= 2) as u64),
            (Probe::LeaseTakeover, takeover as u64),
            (Probe::ReadLeaseRenewed, (meta_changed && !data_changed && lease_ms > cfg.lease_ms) as u64),
            (Probe::LeaseReleased, release as u64),
        ];
        Ok(Plan { manifest: Some(m), outcomes, gc, quota_refused, probes: Some(probes), due })
    }

    fn delete_obsolete(&self, gc: Vec<Obsolete>) {
        if gc.is_empty() {
            return;
        }
        let shared = self.shared.clone();
        let db = self.id.clone();
        self.shared.env.spawn(async move {
            for o in gc {
                if let Err(e) = delete_object(&shared, &db, &o).await {
                    crate::warn!("gc delete {}/{} failed: {e}", db, o.id);
                }
            }
        });
    }

    fn after_commit(self: &Arc<Self>, v: &Arc<Version>, gc: Vec<Obsolete>, due: BTreeSet<u64>) {
        let cfg = &self.shared.cfg;
        self.delete_obsolete(gc);
        self.shared.refresh_clock();
        {
            let mut st = self.st();
            if !st.compacting && compaction::pick(&v.m, cfg).is_some() {
                st.compacting = true;
                let me = self.clone();
                self.shared.env.spawn(async move {
                    loop {
                        if let Err(e) = me.clone().compact().await {
                            crate::debug!("compaction of {} stopped: {e}", me.id);
                            break;
                        }
                        if !me.has_more_work(|st| &mut st.compacting, |m| compaction::pick(m, &me.shared.cfg).is_some())
                        {
                            return;
                        }
                    }
                    me.st().compacting = false;
                });
            }
        }
        self.maybe_flush(v);
        self.maybe_sweep(v);
        self.schedule_trims(due);
    }

    fn maybe_flush(self: &Arc<Self>, v: &Arc<Version>) {
        let over = v.m.tail_bytes() > self.shared.cfg.tail_max_bytes;
        if v.m.tail.is_empty() || !(over || crate::buggify::fires(0.05)) {
            return;
        }
        {
            let mut st = self.st();
            if st.flushing || !self.holds_lease(&st, &v.m) {
                return;
            }
            st.flushing = true;
        }
        let (me, mut v) = (self.clone(), v.clone());
        self.shared.env.spawn(async move {
            loop {
                if let Err(e) = me.clone().flush(v).await {
                    crate::debug!("flush of {} stopped: {e}", me.id);
                    break;
                }
                let limit = me.shared.cfg.tail_max_bytes;
                if !me.has_more_work(|st| &mut st.flushing, |m| m.tail_bytes() > limit) {
                    return;
                }
                match me.current() {
                    Some(c) => v = c,
                    None => break,
                }
            }
            me.st().flushing = false;
        });
    }

    /// Checks whether background work is still needed on the current version while holding the
    /// state lock, clearing `flag` if not.
    fn has_more_work(&self, flag: fn(&mut State) -> &mut bool, due: impl Fn(&Manifest) -> bool) -> bool {
        let mut st = self.st();
        let again = st.current.as_ref().is_some_and(|c| self.holds_lease(&st, &c.m) && due(&c.m));
        if !again {
            *flag(&mut st) = false;
        }
        again
    }

    async fn flush(self: Arc<Self>, v: Arc<Version>) -> Result<()> {
        let started_ms = self.shared.store_time_ms().await?;
        let output = flush_tail(&self.shared, &self.id, &v.m.tail, v.m.version).await?;
        self.shared.env.crash_point().await;
        let objects: Vec<Obsolete> = output
            .segments
            .iter()
            .map(|s| &s.id)
            .chain(&output.chunk_ids)
            .map(|id| Obsolete { kind: ObjKind::Segment, id: id.clone(), since_ms: started_ms })
            .collect();
        let (tx, rx) = oneshot::channel();
        let (flushed_version, flush_seq) = (v.m.version, v.m.flush_seq);
        self.park_flush(Work::Flush { flushed_version, flush_seq, output, started_ms, tx });
        match rx.await.map_err(|_| Error::unavailable("flush install interrupted"))? {
            Ok(()) => {
                probe::hit(Probe::TailFlushedInBackground);
                Ok(())
            }
            // If a superseded flush did not encounter an ambiguous write, it was never installed,
            // so its output objects are unreferenced.
            Err(e @ Error::Conflict(_)) => {
                self.delete_obsolete(objects);
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    fn trace_flush(&self, how: &str, f: &FlushOutput, through: Seq) {
        self.shared.env.trace(|| {
            let segs: Vec<&str> = f.segments.iter().map(|s| s.id.as_str()).collect();
            let retained = f.retained_tail.as_ref().map(Vec::len);
            format!("{} flush {how} through v{through} segments {segs:?} retained {retained:?}", self.id)
        });
    }

    /// Parks a flush for up to `FLUSH_INSTALL_WAIT_MS` so it can be batched with the next commit.
    fn park_flush(self: &Arc<Self>, w: Work) {
        let queued = QueuedWork { work: w, enqueued_at: Instant::now(), request_id: None };
        if let Some(old) = self.queue().parked_flush.replace(queued) {
            old.work.fail(Error::internal("a parked flush was replaced"));
        }
        let me = self.clone();
        self.shared.env.spawn(async move {
            tokio::time::sleep(Duration::from_millis(FLUSH_INSTALL_WAIT_MS)).await;
            let parked = me.queue().parked_flush.take();
            if let Some(q) = parked {
                me.enqueue(q.work);
            }
        });
    }

    fn schedule_trims(self: &Arc<Self>, ids: BTreeSet<u64>) {
        let start = {
            let mut st = self.st();
            st.trim_queue.extend(ids);
            let start = !st.trimming && !st.trim_queue.is_empty();
            st.trimming |= start;
            start
        };
        if start {
            let me = self.clone();
            self.shared.env.spawn(async move {
                if let Err(e) = me.clone().trim_queued().await {
                    crate::debug!("retention trims of {} stopped: {e}", me.id);
                    let mut st = me.st();
                    st.trim_queue.clear();
                    st.trimming = false;
                }
            });
        }
    }

    async fn trim_queued(self: Arc<Self>) -> Result<()> {
        let mut failures = 0;
        loop {
            let id = {
                let mut st = self.st();
                match st.trim_queue.pop_first() {
                    Some(id) => id,
                    None => {
                        st.trimming = false;
                        return Ok(());
                    }
                }
            };
            match self.trim_for_retention(id).await {
                Ok(()) => failures = 0,
                Err(e) if e.retryable() && failures < TRIM_RETRIES => {
                    failures += 1;
                    probe::hit(Probe::RetentionTrimRetried);
                    self.st().trim_queue.insert(id);
                    tokio::time::sleep(Duration::from_millis(100 << failures)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Commits any pending retention trim for stream `id`.
    async fn trim_for_retention(self: &Arc<Self>, id: u64) -> Result<()> {
        let Some(now) = self.shared.store_now_ms() else { return Ok(()) };
        let Some(v) = self.current() else { return Ok(()) };
        let Some(st) = v.stream_state(id).await? else { return Ok(()) };
        if !st.trim_due(now) {
            return Ok(());
        }
        let mut before = st.first_seq;
        if let Some(r) = st.retention.retention_ms {
            before = before.max(v.stream_seek(id, &st, Seek::Time(now.saturating_sub(r))).await?);
        }
        if let Some(b) = st.retention.max_bytes {
            before = before.max(v.stream_seek(id, &st, Seek::Pos(st.next_pos.saturating_sub(b))).await?);
        }
        if before == st.first_seq {
            return Ok(());
        }
        let mut writes = WriteSet::default();
        writes.streams.push(StreamOp::Trim { id, before, retention: Some(st.retention) });
        let req = CommitRequest {
            commit_id: self.shared.env.random_u64(),
            deadline: None,
            read_version: v.m.version,
            read_time: v.m.time_ms,
            read_expiry: None,
            isolation: Isolation::Serializable,
            reads: Vec::new(),
            pinned_reads: Vec::new(),
            writes,
        };
        match self.commit(req).await {
            Ok(_) => {
                probe::hit(Probe::RetentionTrimmed);
                Ok(())
            }
            Err(Error::NotOwner { .. }) => {
                probe::hit(Probe::RetentionTrimLeftToOwner);
                Ok(())
            }
            Err(Error::NotFound(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Triggers a garbage collection sweep if due, using a shorter interval if under quota pressure.
    fn maybe_sweep(self: &Arc<Self>, v: &Version) {
        let cfg = &self.shared.cfg;
        let Some(now) = self.shared.store_now_ms() else { return };
        let mut st = self.st();
        let interval = if st.quota_pressure { cfg.quota_sweep_interval_ms } else { cfg.sweep_interval_ms };
        if st.sweeping || (v.m.last_sweep_ms + interval > now && !crate::buggify::fires(0.02)) {
            return;
        }
        st.sweeping = true;
        st.quota_pressure = false;
        let me = self.clone();
        self.shared.env.spawn(async move {
            if let Err(e) = me.clone().sweep().await {
                crate::debug!("sweep of {} stopped: {e}", me.id);
            }
            me.st().sweeping = false;
        });
    }

    async fn compact(self: Arc<Self>) -> Result<()> {
        let _permit =
            self.shared.compaction_slots.acquire().await.map_err(|_| Error::internal("compaction semaphore closed"))?;
        let cfg = self.shared.cfg.clone();
        loop {
            let store_now = self.shared.store_time_ms().await?;
            let Some(v) = self.current() else { return Ok(()) };
            if !self.holds_lease(&self.st(), &v.m) {
                probe::hit(Probe::CompactionLostLease);
                return Ok(());
            }
            let Some(plan) = compaction::pick(&v.m, &cfg) else {
                return Ok(());
            };
            let (outputs, purged_max_seq, expired, (chunks, sealed_bytes)) = match plan.kind {
                Kind::Move { level } => (vec![SegmentMeta { level, ..plan.inputs[0].clone() }], 0, 0, (0, 0)),
                Kind::Drop => (Vec::new(), 0, 0, (0, 0)),
                Kind::DropExpired => (Vec::new(), 0, plan.inputs.iter().map(|s| s.entries).sum(), (0, 0)),
                Kind::Merge { .. } | Kind::Rewrite | Kind::Expire => {
                    let purge_seq = v.m.version_at_least_age(store_now, cfg.txn_max_lifetime_ms);
                    let out = compaction::run(&self.shared, &v.m, &plan, purge_seq).await?;
                    (out.segments, out.purged_max_seq, out.expired, (out.chunks, out.sealed_bytes))
                }
            };
            let inputs: Vec<String> = plan.inputs.iter().map(|s| s.id.clone()).collect();
            let deep = compaction::deep_after(&v.m, &inputs, &outputs);
            let levels = match deep.len() > cfg.levels_inline_max {
                true => Some(self.shared.write_levels(&self.id, deep.clone()).await?),
                false => None,
            };
            self.shared.env.crash_point().await;
            let (tx, rx) = oneshot::channel();
            self.enqueue(Work::Compaction {
                inputs,
                outputs,
                purged_max_seq,
                sealed_bytes,
                compaction_seq: v.m.compaction_seq,
                started_ms: store_now,
                deep,
                levels,
                tx,
            });
            rx.await.map_err(|_| Error::unavailable("compaction install interrupted"))??;
            let n = plan.inputs.len() as u64;
            match plan.kind {
                Kind::Drop => probe::hit_n(Probe::SegmentDropped, n),
                Kind::Rewrite => probe::hit_n(Probe::SegmentRewritten, n),
                Kind::DropExpired | Kind::Expire => probe::hit_n(Probe::SegmentExpired, n),
                Kind::Move { .. } => probe::hit(Probe::CompactionMoved),
                Kind::Merge { .. } => {}
            }
            probe::hit(Probe::CompactionInstalled);
            probe::hit_n(Probe::EntriesExpired, expired);
            probe::hit_n(Probe::ChunkSealedInCompaction, chunks);
        }
    }

    /// Scans storage for unreferenced objects and records them as obsolete in the manifest.
    async fn sweep(self: Arc<Self>) -> Result<()> {
        let cfg = self.shared.cfg.clone();
        let now_ms = self.shared.store_time_ms().await?;
        let v = self.refresh(Instant::now(), false).await?;
        if !self.holds_lease(&self.st(), &v.m) {
            return Ok(());
        }
        let mut referenced: BTreeSet<(ObjKind, String)> =
            v.m.segments.iter().map(|s| (ObjKind::Segment, s.id.clone())).collect();
        referenced.extend(v.m.levels.iter().map(|id| (ObjKind::Levels, id.clone())));
        referenced.extend(v.m.obsolete.iter().filter(|o| o.kind != ObjKind::Blob).map(|o| (o.kind, o.id.clone())));
        let mut referenced_blobs: BTreeSet<String> =
            v.m.obsolete.iter().filter(|o| o.kind == ObjKind::Blob).map(|o| o.id.clone()).collect();
        let mut live_blobs: BTreeMap<String, u64> = BTreeMap::new();
        let mut live_chunks: BTreeMap<String, u64> = BTreeMap::new();
        let mut due = BTreeSet::new();
        let mut it = v.iter_bulk(false);
        while let Some(e) = it.next().await? {
            if let Some((_, stream::Stored::Chunk(c))) = stream::stored(&e) {
                referenced.insert((ObjKind::Segment, c.id.clone()));
                live_chunks.insert(c.id, c.size);
            } else if let Some((id, st)) = stream::state_of(&e) {
                if st.trim_due(now_ms) {
                    due.insert(id);
                }
            } else if let Value::Blob(b) = e.value {
                referenced_blobs.insert(b.id.clone());
                live_blobs.insert(b.id, b.size);
            }
        }
        let mut items = Vec::new();
        let mut segs = Listing::new(format!("dbs/{}/seg/", self.id));
        while let Some(o) = segs.next(&self.shared).await? {
            let name = o.key.rsplit('/').next().unwrap_or("");
            let (kind, id) = match name.strip_suffix(super::LEVELS_SUFFIX) {
                Some(id) => (ObjKind::Levels, id),
                None => (ObjKind::Segment, name),
            };
            if super::types::valid_object_id(id)
                && !referenced.contains(&(kind, id.to_string()))
                && o.last_modified_ms + cfg.orphan_grace_ms <= now_ms
            {
                items.push(Obsolete { kind, id: id.to_string(), since_ms: now_ms });
            }
        }
        let blob_of = |key: &str| key.rsplit('/').nth(1).map(|s| s.to_string());
        let found = |id: String, mtime: u64, items: &mut Vec<Obsolete>| {
            let created = super::types::blob_id_time(&id).unwrap_or(0);
            if super::types::valid_object_id(&id)
                && !referenced_blobs.contains(&id)
                && mtime + cfg.orphan_grace_ms <= now_ms
                && created + cfg.blob_stage_ttl_ms + bind_margin_ms(&cfg) <= now_ms
            {
                items.push(Obsolete { kind: ObjKind::Blob, id, since_ms: now_ms });
            }
        };
        let mut blobs = Listing::new(format!("dbs/{}/blob/", self.id));
        let mut cur: Option<(String, u64)> = None;
        while let Some(o) = blobs.next(&self.shared).await? {
            let Some(id) = blob_of(&o.key) else { continue };
            match &mut cur {
                Some((c, mtime)) if *c == id => *mtime = (*mtime).max(o.last_modified_ms),
                _ => {
                    if let Some((c, mtime)) = cur.replace((id, o.last_modified_ms)) {
                        found(c, mtime, &mut items);
                    }
                }
            }
        }
        if let Some((c, mtime)) = cur {
            found(c, mtime, &mut items);
        }
        let blob_bytes = (live_blobs.values().sum(), v.m.blob_bytes.bound);
        let chunk_bytes = (live_chunks.values().sum(), v.m.chunk_bytes.bound);
        self.schedule_trims(due);
        if !items.is_empty() {
            probe::hit(Probe::OrphansMarked);
            self.shared.env.crash_point().await;
        }
        let (tx, rx) = oneshot::channel();
        self.enqueue(Work::Obsolete { items, blob_bytes, chunk_bytes, tx });
        rx.await.map_err(|_| Error::unavailable("sweep interrupted"))?
    }

    /// Runs a garbage collection sweep immediately.
    pub async fn sweep_now(self: &Arc<Self>) -> Result<()> {
        self.clone().sweep().await
    }

    /// Commits a lease renewal that also runs due garbage collection.
    pub async fn maintenance_now(self: &Arc<Self>) -> Result<()> {
        self.acquire_lease().await
    }
}

/// Wall-clock and store-clock timestamps for a manifest write. If `store_ms`
/// is `None`, garbage collection is paused.
#[derive(Clone, Copy, Debug)]
struct Now {
    wall_ms: u64,
    store_ms: Option<u64>,
}

async fn commit_state(v: &Version, tail: Option<&BTreeMap<Bytes, Entry>>, id: u64) -> Result<Option<CommitState>> {
    let key = commit_key(id);
    let entry = match tail.map(|t| t.get(&key)) {
        Some(Some(e)) => Some(e.clone()),
        _ => v.get_entry(&key).await?,
    };
    let Some(e) = entry.filter(|e| !e.value.is_tombstone()) else { return Ok(None) };
    let record = match &e.value {
        Value::Inline(b) => decode_commit_record(b).ok(),
        _ => None,
    };
    Ok(Some(match record.ok_or_else(|| Error::internal("corrupt commit record"))? {
        CommitRecord::Applied { time_ms, appends } => {
            CommitState::Applied(Committed { version: e.seq, replayed: true, time_ms, appends })
        }
        CommitRecord::Aborted => CommitState::Aborted,
    }))
}

pub(crate) fn aborted(id: u64) -> Error {
    Error::Aborted(format!(
        "commit {id:016x} was aborted by a status check and will not be applied; retry with a new idempotency key or \
         transaction"
    ))
}

const TRIM_RETRIES: u32 = 5;
const FLUSH_INSTALL_WAIT_MS: u64 = 1_000;
const TAIL_SYNC_FLUSH_FACTOR: usize = 4;

struct FlushOutput {
    segments: Vec<SegmentMeta>,
    /// Unsealed tail entries and chunk references retained inline if the post-seal tail is small.
    retained_tail: Option<Vec<Entry>>,
    chunk_ids: Vec<String>,
    sealed_bytes: u64,
}

impl FlushOutput {
    fn install(&self, m: &mut Manifest, tail: &mut BTreeMap<Bytes, Entry>, through: Seq, tally: &mut FlushTally) {
        tail.retain(|_, e| e.seq > through);
        match &self.retained_tail {
            Some(retained) => {
                for e in retained {
                    let deleted = m.range_deletes.iter().any(|t| t.seq > e.seq && t.contains_key(&e.key));
                    if !deleted && !tail.contains_key(&e.key) {
                        tail.insert(e.key.clone(), e.clone());
                    }
                }
                tally.in_place = true;
            }
            None => {
                m.segments.splice(0..0, self.segments.iter().cloned());
                tally.to_segments = true;
            }
        }
        tally.chunks += self.chunk_ids.len() as u64;
        m.chunk_bytes.bound += self.sealed_bytes;
        m.flush_seq += 1;
    }
}

#[derive(Default)]
struct FlushTally {
    chunks: u64,
    to_segments: bool,
    in_place: bool,
}

/// Seals eligible stream records into chunk objects and writes the remaining tail entries and
/// commit records to level-0 segments. If stream sealing leaves at most `tail_max_bytes / 2` of
/// entries, retains them in the manifest tail instead of writing segments.
async fn flush_tail(shared: &Shared, db: &str, tail: &[Entry], version: Seq) -> Result<FlushOutput> {
    let cfg = &shared.cfg;
    let split = tail.partition_point(|e| !is_commit_key(&e.key));
    let mut sealer = Sealer::new(cfg.stream_seal_min_bytes, cfg.stream_chunk_bytes);
    let mut data = Vec::with_capacity(split);
    for e in tail[..split].iter().cloned() {
        sealer.push(shared, db, e, &mut data).await?;
    }
    sealer.finish(shared, db, &mut data).await?;
    let commit_records = &tail[split..];
    let (chunk_ids, sealed_bytes) = (sealer.chunk_ids, sealer.sealed_bytes);
    let rest: usize = data.iter().chain(commit_records).map(Entry::size).sum();
    if !chunk_ids.is_empty() && rest <= cfg.tail_max_bytes / 2 {
        let retained_tail = Some(data.into_iter().chain(commit_records.iter().cloned()).collect());
        return Ok(FlushOutput { segments: Vec::new(), retained_tail, chunk_ids, sealed_bytes });
    }
    let write = |part: &[Entry]| {
        let mut b = SegmentBuilder::new(cfg.block_target, cfg.compression);
        for e in part {
            b.add(e);
        }
        async move {
            match b.is_empty() {
                true => Ok(None),
                false => compaction::write(shared, db, b, 0, version).await.map(Some),
            }
        }
    };
    let (data, records) = tokio::join!(write(&data), write(commit_records));
    let segments = [data?, records?].into_iter().flatten().collect();
    Ok(FlushOutput { segments, retained_tail: None, chunk_ids, sealed_bytes })
}

/// Marks a superseded flush outcome as unknown if an earlier attempt in the batch was ambiguous.
fn mark_superseded_flush_unknown(w: &Work, o: Outcome, maybe_applied: bool) -> Outcome {
    match (w, o) {
        (Work::Flush { .. }, Outcome::Unit(Err(e @ Error::Conflict(_)))) if maybe_applied => {
            Outcome::Unit(Err(unknown(e)))
        }
        (_, o) => o,
    }
}

fn pending<'a>(base: &'a Version, m: &Manifest, tail: &'a BTreeMap<Bytes, Entry>) -> Pending<'a> {
    let batch_deletes = m.range_deletes.iter().filter(|t| t.seq > base.m.version).cloned().collect();
    Pending { base, tail, batch_deletes }
}

/// Increments `waiters` on creation and decrements it on drop.
struct WaiterGuard<'a>(&'a std::sync::atomic::AtomicUsize);

impl<'a> WaiterGuard<'a> {
    fn new(n: &'a std::sync::atomic::AtomicUsize) -> Self {
        n.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        WaiterGuard(n)
    }
}

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Clock-error margin added to `blob_stage_ttl_ms` during sweeps so a blob is never marked
/// obsolete while a committing node can still accept it.
fn bind_margin_ms(cfg: &super::EngineConfig) -> u64 {
    2 * cfg.clock_sync_max_error_ms
}

/// Validates that all staged blobs referenced by `w` are within the stage TTL
/// and have not been marked obsolete.
fn bindable(w: &WriteSet, m: &Manifest, store_now: Option<u64>, cfg: &super::EngineConfig) -> Result<()> {
    let mut staged = w.staged_blobs().peekable();
    if staged.peek().is_none() {
        return Ok(());
    }
    let now = store_now.ok_or_else(|| Error::unavailable("store clock not synchronized"))?;
    let expired = staged.any(|b| {
        super::types::blob_id_time(&b.id).unwrap_or(0) + cfg.blob_stage_ttl_ms < now
            || m.obsolete.iter().any(|o| o.kind == ObjKind::Blob && o.id == b.id)
    });
    if expired {
        return Err(Error::invalid("a staged blob expired before it was committed"));
    }
    Ok(())
}

struct Listing {
    prefix: String,
    page: std::vec::IntoIter<crate::store::ObjectMeta>,
    after: Option<String>,
    done: bool,
}

impl Listing {
    fn new(prefix: String) -> Listing {
        Listing { prefix, page: Vec::new().into_iter(), after: None, done: false }
    }

    async fn next(&mut self, shared: &Shared) -> Result<Option<crate::store::ObjectMeta>> {
        loop {
            if let Some(o) = self.page.next() {
                return Ok(Some(o));
            }
            if self.done {
                return Ok(None);
            }
            let after = self.after.clone();
            let p = shared.retry.run(|| shared.store.list(&self.prefix, after.as_deref(), 1000)).await?;
            self.done = p.next.is_none();
            self.after = p.next;
            self.page = p.objects.into_iter();
        }
    }
}

fn check_live(v: Arc<Version>) -> Result<Arc<Version>> {
    if v.m.is_deleted() { Err(Error::not_found("database deleted")) } else { Ok(v) }
}

struct Plan {
    manifest: Option<Manifest>,
    outcomes: Vec<Outcome>,
    gc: Vec<Obsolete>,
    quota_refused: bool,
    probes: Option<[(Probe, u64); 11]>,
    due: BTreeSet<u64>,
}

pub(crate) fn quota_exceeded(limit: u64) -> Error {
    Error::QuotaExceeded(format!("the database is at its size quota of {limit} bytes; delete data to make room"))
}

/// Returns `(total_added_bytes, newly_bound_blob_bytes)` for a commit write set.
fn growth(w: &WriteSet) -> (u64, u64) {
    let added: u64 = w.streams.iter().map(StreamOp::appended_bytes).sum::<u64>()
        + w.puts.iter().filter_map(|(k, p)| Some((k.len() + p.as_ref()?.value.size()) as u64)).sum::<u64>();
    let blobs: u64 = w.staged_blobs().map(|b| b.size).sum();
    (added + blobs, blobs)
}

/// Applies a range deletion to the manifest and tail, emitting point tombstones
/// when fewer than `points_max` stored entries are covered, or a range tombstone otherwise.
async fn delete_range(
    base: &Version,
    m: &mut Manifest,
    tail: &mut BTreeMap<Bytes, Entry>,
    r: &KeyRange,
    seq: Seq,
    points_max: usize,
) -> Result<()> {
    let Some(rt) = RangeTombstone::new(r, seq) else { return Ok(()) };
    let mut stored: BTreeMap<Bytes, bool> = BTreeMap::new();
    let mut it = base.iter(r, false, true, None);
    while let Some(e) = it.next().await? {
        if stored.len() >= points_max {
            put_range_tombstone(m, tail, rt);
            return Ok(());
        }
        stored.insert(e.key, !e.value.is_tombstone());
    }
    // Account for range deletions in earlier commits within the current batch.
    let batch_deletes: Vec<&RangeTombstone> = m.range_deletes.iter().filter(|t| t.seq > base.m.version).collect();
    let mut doomed: Vec<Bytes> = stored
        .into_iter()
        .filter(|(k, live)| *live && !tail.contains_key(k) && !batch_deletes.iter().any(|t| t.contains_key(k)))
        .map(|(k, _)| k)
        .collect();
    let bounds = (r.start.clone(), r.end.clone());
    doomed.extend(tail.range::<Bytes, _>(bounds).filter(|(_, e)| !e.value.is_tombstone()).map(|(k, _)| k.clone()));
    for k in doomed {
        tail.insert(k.clone(), Entry::tombstone(k, seq));
    }
    Ok(())
}

fn put_range_tombstone(m: &mut Manifest, tail: &mut BTreeMap<Bytes, Entry>, rt: RangeTombstone) {
    let end = rt.end.clone().map_or(Bound::Unbounded, Bound::Excluded);
    let in_tail: Vec<Bytes> =
        tail.range::<Bytes, _>((Bound::Included(rt.start.clone()), end)).map(|(k, _)| k.clone()).collect();
    for k in in_tail {
        tail.remove(&k);
    }
    m.range_deletes.retain(|t| !rt.contains_range(t));
    m.range_deletes.push(rt);
}

fn unknown(e: Error) -> Error {
    Error::Unavailable {
        message: format!("commit outcome unknown: {}", e.message()),
        retry_after_ms: Some(100),
        outcome_unknown: true,
    }
}

async fn delete_object(shared: &Shared, db: &str, o: &Obsolete) -> Result<()> {
    match o.kind {
        ObjKind::Segment | ObjKind::Levels => {
            let key = if o.kind == ObjKind::Segment { seg_key(db, &o.id) } else { levels_key(db, &o.id) };
            shared.retry.run(|| shared.store.delete(&key)).await?;
        }
        ObjKind::Blob => {
            let now = shared.store_now_ms().ok_or_else(|| Error::unavailable("store clock not synchronized"))?;
            if super::blob::pinned(shared, db, &o.id, now).await? {
                // Pinned by an active read; a later sweep will collect it.
                probe::hit(Probe::BlobPinnedAtCollection);
                return Ok(());
            }
            let objs = crate::store::list_all(&*shared.store, &blob_prefix(db, &o.id), &shared.retry).await?;
            for ob in objs {
                shared.retry.run(|| shared.store.delete(&ob.key)).await?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineConfig;
    use crate::engine::tests::{engine, rt};

    fn meta(c: char, level: u8, min: &str, max: &str) -> SegmentMeta {
        SegmentMeta {
            id: c.to_string().repeat(32),
            level,
            size: 100,
            entries: 1,
            tombstones: 0,
            min_key: Bytes::copy_from_slice(min.as_bytes()),
            max_key: Bytes::copy_from_slice(max.as_bytes()),
            min_seq: 0,
            max_seq: 0,
            deletes_applied: 0,
            expiring: 0,
            expires_max: 0,
        }
    }

    async fn plan(segments: &[SegmentMeta], obsolete: Vec<Obsolete>, batch: Vec<Work>) -> Plan {
        let (eng, _, _) = engine(1, EngineConfig::small_for_tests());
        let lease = Some(Lease { node: eng.shared.node_id.clone(), expires_ms: 5_000, ms: 5_000 });
        let mut m =
            Manifest { segments: segments.to_vec(), obsolete, compaction_seq: 2, lease, ..Manifest::new("d", 0, None) };
        m.sort_segments();
        let mut batch = batch;
        for w in &mut batch {
            if let Work::Compaction { inputs, outputs, deep, .. } = w {
                *deep = compaction::deep_after(&m, inputs, outputs);
            }
        }
        let base = Version::new(m, String::new(), eng.shared.clone());
        eng.db("d").build(&base, &batch, Now { wall_ms: 1_000, store_ms: Some(1_000) }).await.unwrap()
    }

    fn compaction(inputs: &str, outputs: Vec<SegmentMeta>, compaction_seq: u64) -> Work {
        let (tx, _) = oneshot::channel();
        let inputs: Vec<String> = inputs.chars().map(|c| c.to_string().repeat(32)).collect();
        Work::Compaction {
            inputs,
            outputs,
            purged_max_seq: 0,
            sealed_bytes: 0,
            compaction_seq,
            started_ms: 1_000,
            deep: Vec::new(),
            levels: None,
            tx,
        }
    }

    use crate::store::{BoxFut, GetResult, ListPage, ObjectStore};

    struct Scripted(std::sync::Mutex<VecDeque<(u64, crate::store::SResult<String>)>>);

    impl ObjectStore for Scripted {
        fn get<'a>(&'a self, _: &'a str, _: GetOpts) -> BoxFut<'a, crate::store::SResult<GetResult>> {
            Box::pin(async { Err(StoreError::NotFound) })
        }
        fn put<'a>(&'a self, _: &'a str, _: Bytes, _: PutMode) -> BoxFut<'a, crate::store::SResult<String>> {
            let (ms, r) = self.0.lock().unwrap().pop_front().expect("unexpected put");
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                r
            })
        }
        fn delete<'a>(&'a self, _: &'a str) -> BoxFut<'a, crate::store::SResult<()>> {
            Box::pin(async { Ok(()) })
        }
        fn list<'a>(&'a self, _: &'a str, _: Option<&'a str>, _: usize) -> BoxFut<'a, crate::store::SResult<ListPage>> {
            Box::pin(async { Ok(ListPage::default()) })
        }
    }

    #[test]
    fn ambiguous_write_recovery_renews_lease() {
        use crate::engine::tests::{delayed_engine_with, opened, put};
        use std::sync::atomic::Ordering::SeqCst;
        rt().block_on(async {
            let cfg = EngineConfig { manifest_put_hedge_ms: 0, ..EngineConfig::small_for_tests() };
            let (eng, store) = delayed_engine_with("a", cfg);
            let db = opened(&eng, "d").await;
            put(&db, "k", "1").await;
            store.manifest_put_lost.store(1, SeqCst);
            put(&db, "k", "2").await;
            let cur = db.current().unwrap();
            assert!(db.holds_lease(&db.st(), &cur.m));
        });
    }

    #[test]
    fn hedged_write_outcomes() {
        use StoreError::{Fatal, PreconditionFailed as Refused, Unavailable};
        let ok = || Ok::<_, StoreError>("e2".to_string());
        let down = || Unavailable("timeout".into());
        let fatal = || Fatal("403".into());
        let cases = [
            ((100, ok()), None, Ok(()), 100, false),
            ((1_000, ok()), Some((10, Err(Refused))), Err(down()), 260, true),
            ((300, Err(Refused)), Some((500, ok())), Ok(()), 750, false),
            ((300, Err(down())), Some((100, Err(fatal()))), Err(down()), 350, false),
            ((300, Err(Refused)), Some((200, Err(Refused))), Err(Refused), 450, false),
            ((300, Err(fatal())), Some((100, Err(Refused))), Err(fatal()), 350, false),
            ((1_000, Err(down())), Some((100, Err(fatal()))), Err(down()), 1_000, false),
        ];
        for (first, hedge, want, ms, in_flight) in cases {
            rt().block_on(async {
                let script = Scripted(std::sync::Mutex::new(
                    [Some(first.clone()), hedge.clone()].into_iter().flatten().collect(),
                ));
                let origin = tokio::time::Instant::now();
                let env = Arc::new(crate::env::Env::sim(1, 1_700_000_000_000, origin, 0));
                let cfg = EngineConfig { manifest_put_hedge_ms: 250, ..EngineConfig::small_for_tests() };
                let eng = crate::engine::Engine::new(Arc::new(script), env, cfg, "n".into());
                let db = eng.db("d");
                let got = db.put_manifest(Bytes::from_static(b"m"), "e1").await.map(drop);
                let case = format!("first {first:?}, hedge {hedge:?}");
                assert_eq!(db.st().in_flight_hedged_puts, u32::from(in_flight), "{case}");
                match (&got, &want) {
                    (Err(Unavailable(_)), Err(Unavailable(_))) => {}
                    _ => assert_eq!(got, want, "{case}"),
                }
                assert_eq!(origin.elapsed().as_millis() as u64, ms, "{case}");
            });
        }
    }

    fn succeeded(p: &Plan) -> bool {
        matches!(p.outcomes[..], [Outcome::Unit(Ok(()))])
    }

    fn segment_names(p: &Plan) -> Option<String> {
        p.manifest.as_ref().map(|m| m.segments.iter().map(|s| format!("{}{}", &s.id[..1], s.level)).collect())
    }

    #[test]
    fn compaction_installs_check_their_inputs() {
        rt().block_on(async {
            let segs = [meta('a', 0, "a", "z"), meta('b', 6, "a", "c"), meta('c', 6, "d", "f"), meta('d', 6, "x", "z")];
            let x = || vec![meta('x', 6, "a", "f")];
            let p = plan(&segs, vec![], vec![compaction("bc", x(), 2)]).await;
            assert!(succeeded(&p));
            assert_eq!(segment_names(&p).as_deref(), Some("a0x6d6"));
            let m = p.manifest.unwrap();
            assert_eq!(m.compaction_seq, 3);
            assert_eq!(m.obsolete.iter().map(|o| &o.id[..1]).collect::<String>(), "bc");
            for (inputs, seq, out) in [("bc", 1, x()), ("be", 2, x()), ("b", 2, vec![meta('y', 6, "a", "z")])] {
                let p = plan(&segs, vec![], vec![compaction(inputs, out, seq)]).await;
                assert!(!succeeded(&p) && p.manifest.is_none(), "inputs {inputs} at {seq} were installed");
            }
            let installed = [meta('a', 0, "a", "z"), meta('x', 6, "a", "f")];
            let p = plan(&installed, vec![], vec![compaction("bc", x(), 2)]).await;
            assert!(succeeded(&p) && p.manifest.is_none());
        });
    }

    #[test]
    fn collection_pauses_without_store_time() {
        rt().block_on(async {
            let (eng, _, _) = engine(1, EngineConfig::small_for_tests());
            let ob = |c: char| Obsolete { kind: ObjKind::Segment, id: c.to_string().repeat(32), since_ms: 0 };
            let lease = Some(Lease { node: eng.shared.node_id.clone(), expires_ms: 5_000, ms: 5_000 });
            let m = Manifest { obsolete: vec![ob('a'), ob('b')], lease, ..Manifest::new("d", 0, None) };
            let base = Version::new(m, String::new(), eng.shared.clone());
            let (tx, _) = oneshot::channel();
            let sweep = Work::Obsolete { items: vec![ob('c')], blob_bytes: (0, 0), chunk_bytes: (0, 0), tx };
            let lease = Work::Lease { release: false, for_reads: true, tx: None };
            let db = eng.db("d");
            let p = db.build(&base, &[lease, sweep], Now { wall_ms: 10_000_000, store_ms: None }).await.unwrap();
            assert!(p.gc.is_empty());
            assert!(matches!(p.outcomes[1], Outcome::Unit(Err(Error::Unavailable { .. }))));
            assert_eq!(p.manifest.unwrap().obsolete.len(), 2);
        });
    }

    #[test]
    fn binding_checks_age_and_obsolete_list() {
        let cfg = EngineConfig::small_for_tests();
        let now = 10_000_000;
        let id = super::super::types::make_blob_id(now - 1_000, &[1; 10]);
        let old = super::super::types::make_blob_id(now - cfg.blob_stage_ttl_ms - 1, &[1; 10]);
        let other = super::super::types::make_blob_id(now, &[2; 10]);
        let writes = |id: &str| {
            let mut w = WriteSet::default();
            let r =
                super::super::types::BlobRef { id: id.into(), size: 1, sha256: [0; 32], content_type: String::new() };
            w.puts.insert(Bytes::from_static(b"k"), Some(super::super::txn::Put { value: Value::Blob(r), ttl: None }));
            w
        };
        let listed = |kind, id: &str| Manifest {
            obsolete: vec![Obsolete { kind, id: id.into(), since_ms: 0 }],
            ..Manifest::new("d", 0, None)
        };
        let fresh = Manifest::new("d", 0, None);
        assert!(bindable(&writes(&id), &fresh, Some(now), &cfg).is_ok());
        assert!(bindable(&WriteSet::default(), &fresh, None, &cfg).is_ok());
        assert!(bindable(&writes(&id), &listed(ObjKind::Blob, &other), Some(now), &cfg).is_ok());
        assert!(bindable(&writes(&id), &listed(ObjKind::Segment, &id), Some(now), &cfg).is_ok());
        assert!(bindable(&writes(&id), &listed(ObjKind::Blob, &id), Some(now), &cfg).is_err());
        assert!(bindable(&writes(&old), &fresh, Some(now), &cfg).is_err());
        assert!(matches!(bindable(&writes(&id), &fresh, None, &cfg), Err(Error::Unavailable { .. })));
    }

    #[test]
    fn trivial_moves_keep_their_segment() {
        rt().block_on(async {
            let segs = [meta('a', 5, "a", "c"), meta('b', 6, "d", "f")];
            let p = plan(&segs, vec![], vec![compaction("a", vec![meta('a', 6, "a", "c")], 2)]).await;
            assert!(succeeded(&p));
            assert_eq!(segment_names(&p).as_deref(), Some("a6b6"));
            assert!(p.manifest.unwrap().obsolete.is_empty());
        });
    }

    #[test]
    fn obsolete_objects_are_recorded_once_and_never_live_segments() {
        rt().block_on(async {
            let ob = |kind, c: char| Obsolete { kind, id: c.to_string().repeat(32), since_ms: 0 };
            let (seg, blob) = (ObjKind::Segment, ObjKind::Blob);
            let (tx, _) = oneshot::channel();
            let items = vec![ob(seg, 'a'), ob(seg, 'b'), ob(seg, 'c'), ob(blob, 'a'), ob(blob, 'd')];
            let p = plan(
                &[meta('a', 0, "a", "b")],
                vec![ob(seg, 'b'), ob(blob, 'd')],
                vec![Work::Obsolete { items, blob_bytes: (0, 0), chunk_bytes: (0, 0), tx }],
            )
            .await;
            let got: Vec<(ObjKind, char)> =
                p.manifest.unwrap().obsolete.iter().map(|o| (o.kind, o.id.chars().next().unwrap())).collect();
            assert_eq!(got, [(seg, 'b'), (blob, 'd'), (seg, 'c'), (blob, 'a')]);
        });
    }
}
