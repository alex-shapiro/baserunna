//! A database handle on one node: snapshot management, strong/snapshot
//! reads, leases, group commit with manifest CAS, compaction and GC
//! scheduling. See SPEC §5.5–§5.9.

use super::blob::{BlobWriter, StageGuard};
use super::compaction::{self, Kind};
use super::manifest::{Lease, Manifest, ObjKind, Obsolete, SegmentMeta};
use super::range_delete::RangeTombstone;
use super::segment::SegmentBuilder;
use super::stream::{self, Appended, Pending, Sealer, Seek, StreamOp};
use super::txn::{CommitRequest, Isolation, Txn, WriteSet, validate};
use super::types::{Entry, Seq, Value, commit_key, decode_commit_record, encode_commit_record, is_commit_key};
use super::version::{KeyRange, Snapshot, Version};
use super::{Shared, blob_prefix, levels_key, manifest_key, seg_key};
use crate::error::{Error, Result};
use crate::store::{GetOpts, PutMode, StoreError};
use crate::timing::{self, StoreOp, Tally};
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
        /// Bytes of the stream chunks its merges sealed.
        sealed_bytes: u64,
        /// The manifest's `compaction_seq` when the compaction was planned.
        compaction_seq: u64,
        /// Segments below level 0 after compaction, and optional levels object ID if not stored inline.
        deep: Vec<SegmentMeta>,
        levels: Option<String>,
        tx: oneshot::Sender<Result<()>>,
    },
    /// Sweep results for obsolete objects and referenced blob byte totals.
    Obsolete {
        items: Vec<Obsolete>,
        /// Distinct referenced blob bytes from the swept version and its bound blob total.
        blob_bytes: (u64, u64),
        /// Referenced chunk bytes from the swept version and its bound chunk total.
        chunk_bytes: (u64, u64),
        tx: oneshot::Sender<Result<()>>,
    },
    MarkDeleted {
        tx: oneshot::Sender<Result<()>>,
    },
}

enum Outcome {
    Seq(Result<Committed>),
    Unit(Result<()>),
}

impl Work {
    fn deliver(self, o: Outcome) {
        match (self, o) {
            (Work::Commit { tx, .. }, Outcome::Seq(r)) => {
                let _ = tx.send(r);
            }
            (Work::Commit { tx, .. }, Outcome::Unit(r)) => {
                let _ = tx.send(r.map(|_| Committed { version: 0, replayed: false, time_ms: 0, appends: Vec::new() }));
            }
            (Work::Lease { tx: Some(tx), .. }, o)
            | (Work::Compaction { tx, .. }, o)
            | (Work::Obsolete { tx, .. }, o)
            | (Work::MarkDeleted { tx }, o) => {
                let _ = tx.send(match o {
                    Outcome::Unit(r) => r,
                    Outcome::Seq(r) => r.map(|_| ()),
                });
            }
            (Work::Lease { tx: None, .. }, _) => {}
        }
    }

    fn fail(self, e: Error) {
        self.deliver(Outcome::Unit(Err(e)));
    }
}

struct State {
    current: Option<Arc<Version>>,
    /// False after an ambiguous write until the manifest is refreshed.
    trusted: bool,
    /// Start time of the most recent successful validation of `current`.
    verified_at: Option<Instant>,
    last_access: Instant,
    /// Strong reads in the current and previous read lease windows.
    window_start: Instant,
    reads_in_window: u32,
    reads_last_window: u32,
    lease_requested: bool,
    /// Our lease, as of the manifest write (`write_seq`) that last renewed
    /// it, and the monotonic instant until which we may rely on it.
    own_lease: Option<(u64, Instant)>,
    compacting: bool,
    sweeping: bool,
    /// Indicates a write was rejected due to quota, signaling an earlier sweep.
    quota_pressure: bool,
    /// Streams whose retention may call for a trim, and whether a task is
    /// trimming them.
    trim_queue: BTreeSet<u64>,
    trimming: bool,
}

impl State {
    /// Whether reads are frequent enough to pay for lease renewals.
    fn hot(&self, threshold: u32) -> bool {
        self.reads_in_window.max(self.reads_last_window) >= threshold
    }
}

struct Queue {
    items: VecDeque<Queued>,
    running: bool,
}

struct Queued {
    work: Work,
    at: Instant,
    /// The request that enqueued the work, if any.
    request_id: Option<Arc<str>>,
}

pub struct Db {
    pub id: String,
    shared: Arc<Shared>,
    st: Mutex<State>,
    q: Mutex<Queue>,
    load_lock: tokio::sync::Mutex<()>,
    /// Manifest writes by this node started and finished; one is in flight
    /// while they differ. Its result may already be visible to others, so
    /// strong reads that arrive meanwhile wait for it.
    writes_started: std::sync::atomic::AtomicU64,
    writes_finished: std::sync::atomic::AtomicU64,
    write_done: tokio::sync::Notify,
    /// Notified whenever a newer version is installed.
    installed: tokio::sync::Notify,
    staging: Mutex<Staging>,
}

/// Tracks in-flight and completed uncommitted blob uploads against the database quota.
#[derive(Default)]
struct Staging {
    uploading: u64,
    staged: BTreeMap<String, (u64, Instant)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed {
    pub version: Seq,
    /// The commit id had already been applied by an earlier attempt.
    pub replayed: bool,
    /// Store time of the committed version.
    pub time_ms: u64,
    /// The records each append received, in the order of the appends.
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
                verified_at: None,
                last_access: now,
                window_start: now,
                reads_in_window: 0,
                reads_last_window: 0,
                lease_requested: false,
                own_lease: None,
                compacting: false,
                sweeping: false,
                quota_pressure: false,
                trim_queue: BTreeSet::new(),
                trimming: false,
            }),
            q: Mutex::new(Queue { items: VecDeque::new(), running: false }),
            load_lock: tokio::sync::Mutex::new(()),
            writes_started: 0.into(),
            writes_finished: 0.into(),
            write_done: tokio::sync::Notify::new(),
            installed: tokio::sync::Notify::new(),
            staging: Mutex::new(Staging::default()),
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

    /// Whether background work (commits, compaction, sweeps, retention
    /// trims) is running or queued.
    pub fn busy(&self) -> bool {
        let st = self.st();
        let q = self.queue();
        q.running || !q.items.is_empty() || st.compacting || st.sweeping || st.trimming
    }

    pub(crate) fn idle_for(&self, ms: u64) -> bool {
        let st = self.st();
        let q = self.queue();
        !q.running
            && q.items.is_empty()
            && !st.compacting
            && !st.sweeping
            && !st.trimming
            && st.last_access.elapsed() >= Duration::from_millis(ms)
    }

    // Lease validity is measured on each node's monotonic clock, never by
    // comparing wall clocks: the holder counts from before it sent the
    // renewing write, everyone else from when they first saw that renewal.
    // Every observation follows the send, so others always consider a lease
    // valid for longer than its holder relies on it, however skewed the
    // wall clocks are. `expires_ms` in the manifest only identifies a
    // renewal and serves as a hint.

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

    /// A lease in `v` held by another node, with how long it may still be
    /// valid from our viewpoint.
    fn foreign_lease<'a>(&self, v: &'a Version) -> Option<(&'a Lease, Duration)> {
        let m = &v.m;
        let l = m.lease.as_ref().filter(|l| l.node != self.shared.node_id)?;
        if self.lapsed(v, l) {
            return None;
        }
        let valid_for = Duration::from_millis(l.ms + self.shared.cfg.lease_margin_ms);
        let seen = self.shared.lease_seen(&self.id, l, m.write_seq);
        let left = (seen + valid_for).saturating_duration_since(Instant::now());
        (!left.is_zero()).then_some((l, left))
    }

    /// Whether the store's clock shows that `l`, which the manifest write
    /// behind `v` granted, has lapsed, however late we first saw it. The
    /// holder relies on a lease for less than its duration from before it
    /// sent the write, and the store stamped the write after that (to its
    /// precision). A lease left by a node that stopped or crashed long ago
    /// then costs no wait. Allowing twice the duration and the margin
    /// absorbs clock drift and skew between the store's servers.
    fn lapsed(&self, v: &Version, l: &Lease) -> bool {
        let (Some(stamped), Some(now)) = (v.stored_ms, self.shared.store_earliest_now_ms()) else {
            return false;
        };
        let precision = self.shared.store.time_precision_ms();
        let lapsed = now > stamped + precision + 2 * l.ms + self.shared.cfg.lease_margin_ms;
        if lapsed {
            self.shared.counters.add(&super::Activity { lapsed_leases: 1, ..Default::default() });
        }
        lapsed
    }

    /// The node holding a valid lease on this database, if known.
    pub fn lease_holder(&self) -> Option<String> {
        let st = self.st();
        let cur = st.current.clone()?;
        if self.holds_lease(&st, &cur.m) {
            return Some(self.shared.node_id.clone());
        }
        self.foreign_lease(&cur).map(|(l, _)| l.node.clone())
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

    fn install(&self, v: Arc<Version>, verified: Option<Instant>) -> Arc<Version> {
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
            Some(c) if c.m.write_seq >= v.m.write_seq => c.clone(),
            _ => {
                st.current = Some(v.clone());
                self.installed.notify_waiters();
                v
            }
        };
        if let Some(l) = installed.m.lease.as_ref().filter(|l| l.node != self.shared.node_id) {
            self.shared.lease_seen(&self.id, l, installed.m.write_seq);
        }
        st.trusted = true;
        if let Some(t) = verified
            && st.verified_at.is_none_or(|old| t > old)
        {
            st.verified_at = Some(t);
        }
        installed
    }

    /// Fetches the latest manifest unless one validated at or after `not_before`
    /// is already cached. Creates the database if `create` is true and it is missing.
    pub async fn refresh(self: &Arc<Self>, not_before: Instant, create: bool) -> Result<Arc<Version>> {
        Ok(self.refresh_at(not_before, create).await?.0)
    }

    /// [`Db::refresh`], returning the instant at which the version was validated.
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
                    return Ok((self.install(c, Some(started)), started));
                }
                Ok(g) => {
                    let m = self.shared.load_manifest(&self.id, &g.data, cur.as_deref()).await?;
                    let mut v = Version::new(m, g.etag, self.shared.clone());
                    v.stored_ms = Some(g.last_modified_ms);
                    let v = Arc::new(v);
                    return Ok((self.install(v, Some(started)), started));
                }
                Err(StoreError::NotFound) if create => {
                    let m = Manifest::new(&self.id, self.shared.time_ms(), self.shared.store_now_ms());
                    match self.shared.store.put(&key, m.encode(), PutMode::Create).await {
                        Ok(etag) => {
                            let v = Arc::new(Version::new(m, etag, self.shared.clone()));
                            return Ok((self.install(v, Some(started)), started));
                        }
                        // Created concurrently, or ambiguous: read it back.
                        Err(StoreError::PreconditionFailed) | Err(StoreError::Unavailable(_)) => {
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
            // A write of ours that is in flight may already be visible to
            // readers elsewhere; serving the cached version now could go
            // back in time relative to them.
            self.wait_for_write().await;
        }
        self.resolve(c).await
    }

    /// The newest version this node can vouch for, as of a strong snapshot,
    /// but without waiting for this node's in-flight manifest write. Also
    /// returns whether the version is settled: no write of ours was in
    /// flight, so the snapshot is as strong as [`Db::snapshot`]'s. An
    /// unsettled one may predate a write that other nodes can already read.
    pub async fn latest(self: &Arc<Self>) -> Result<(Snapshot, bool)> {
        let settled = !self.writing();
        Ok((self.resolve(Consistency::Strong).await?, settled))
    }

    fn writing(&self) -> bool {
        use std::sync::atomic::Ordering::SeqCst;
        self.writes_finished.load(SeqCst) < self.writes_started.load(SeqCst)
    }

    /// Waits for the manifest write in flight when called, if any. Writes
    /// started later are not waited for: nobody can have seen their result
    /// before the caller arrived, so it is ordered before them, and readers
    /// are not starved while commits follow one another.
    async fn wait_for_write(&self) {
        use std::sync::atomic::Ordering::SeqCst;
        let started = self.writes_started.load(SeqCst);
        let mut waited_since = None;
        loop {
            let notified = self.write_done.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            if self.writes_finished.load(SeqCst) >= started {
                break;
            }
            waited_since.get_or_insert_with(Instant::now);
            notified.await;
        }
        if let Some(t) = waited_since {
            timing::read_waited(t.elapsed());
        }
    }

    /// The cached version if it satisfies `c`, otherwise the manifest from the store.
    async fn resolve(self: &Arc<Self>, c: Consistency) -> Result<Snapshot> {
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

    /// Returns the cached version and validation time if it satisfies `c` at
    /// `now`, scheduling a background lease renewal when reads are frequent.
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
        let lease_left = if st.trusted { self.own_lease_left(&st, &cur.m) } else { Duration::ZERO };
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

    /// Begins a serializable transaction that is expected to write, on
    /// [`Db::latest`]. Unlike a strong [`Db::begin`], it does not wait for
    /// this node's in-flight manifest write, so concurrent writers reach the
    /// commit queue while it is in flight and commit together in the next
    /// batch.
    ///
    /// A transaction that commits is strictly serializable as usual: commit
    /// validation checks its reads against every later write, the in-flight
    /// one included. One that ends without committing, or that read through
    /// [`Txn::untracked`], answers from reads no commit checks: if
    /// [`Txn::needs_recheck`], pass it to [`Db::recheck`] before answering.
    pub async fn begin_write(self: &Arc<Self>) -> Result<Txn> {
        let (snap, settled) = self.latest().await?;
        let mut t =
            Txn::new(snap, Isolation::Serializable, self.shared.env.random_u64(), self.shared.cfg.txn_max_write_bytes);
        if !settled {
            t.unsettle();
            self.shared.counters.add(&super::Activity { unsettled_begins: 1, ..Default::default() });
        }
        Ok(t)
    }

    /// Checks the reads of `t`, which [`Txn::needs_recheck`]. Waits for a
    /// strong snapshot and validates the reads against it, as a commit would.
    /// Returns the version at which they are current, or `None` if `t` must
    /// run again: a later write changed them, or `t` read through
    /// [`Txn::untracked`], which no validation can check.
    pub async fn recheck(self: &Arc<Self>, t: Txn) -> Result<Option<Seq>> {
        if t.read_untracked() {
            self.shared.counters.add(&super::Activity { unsettled_reruns: 1, ..Default::default() });
            return Ok(None);
        }
        let snap = self.snapshot(Consistency::Strong).await?;
        let req = t.into_request();
        match validate(&req, &snap.version, &[], &[], snap.time_ms).await {
            Ok(()) => Ok(Some(snap.version.m.version)),
            Err(Error::Conflict(_)) => {
                self.shared.counters.add(&super::Activity { unsettled_reruns: 1, ..Default::default() });
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
        if !req.writes.blob_ids().is_empty() || req.writes.needs_clock() {
            self.shared.store_time_ms().await?;
        }
        let (tx, rx) = oneshot::channel();
        let enqueued = Instant::now();
        self.enqueue(Work::Commit { req, tx });
        let r = rx.await;
        timing::commit_waited(enqueued.elapsed());
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

    /// Releases this node's lease so other nodes can take over immediately.
    pub async fn release_lease(self: &Arc<Self>) -> Result<()> {
        let held = self.current().is_some_and(|c| c.m.lease.as_ref().is_some_and(|l| l.node == self.shared.node_id));
        if !held {
            return Ok(());
        }
        let (tx, rx) = oneshot::channel();
        self.enqueue(Work::Lease { release: true, for_reads: false, tx: Some(tx) });
        rx.await.map_err(|_| Error::unavailable("lease release interrupted"))?
    }

    /// Checks whether commit `id` has been applied as of a strong read, and
    /// how, as a replay of it would report.
    pub async fn commit_status(self: &Arc<Self>, id: u64) -> Result<Option<Committed>> {
        let v = self.read_version(Consistency::Strong).await?;
        committed(&v, id).await
    }

    /// Fences any in-flight commit attempts by writing the manifest, then
    /// checks whether commit `id` was applied.
    pub async fn commit_status_fenced(self: &Arc<Self>, id: u64) -> Result<Option<Committed>> {
        self.acquire_lease().await?;
        self.commit_status(id).await
    }

    /// Wait until this node installs a version newer than `than`, or `max`
    /// passes. A commit through another node is installed here only once a
    /// read on this node fetches it, so callers poll as well.
    pub async fn wait_for_change(&self, than: Seq, max: Duration) {
        let notified = self.installed.notified();
        let mut notified = std::pin::pin!(notified);
        notified.as_mut().enable();
        if self.current().is_some_and(|c| c.m.version > than) {
            return;
        }
        let _ = tokio::time::timeout(max, notified).await;
    }

    fn enqueue(self: &Arc<Self>, w: Work) {
        let spawn = {
            let mut q = self.queue();
            q.items.push_back(Queued { work: w, at: Instant::now(), request_id: crate::log::request_id() });
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
            let queued: Vec<Queued> = {
                let mut q = self.queue();
                if q.items.is_empty() {
                    q.running = false;
                    return;
                }
                let n = q.items.len().min(self.shared.cfg.commit_batch_max);
                q.items.drain(..n).collect()
            };
            let started = Instant::now();
            let tally = Arc::new(Tally::default());
            let (meta, batch): (Vec<_>, Vec<_>) = queued.into_iter().map(|q| ((q.at, q.request_id), q.work)).unzip();
            let txns = batch.iter().filter(|w| matches!(w, Work::Commit { .. })).count();
            timing::scoped(tally.clone(), self.process(batch)).await;
            self.log_batch(&meta, txns, started, &tally);
        }
    }

    /// Logs a batch that wrote the manifest: how long its oldest work waited
    /// in the queue, and where the batch spent its time.
    fn log_batch(&self, queued: &[(Instant, Option<Arc<str>>)], txns: usize, started: Instant, tally: &Tally) {
        use crate::log::{self, Level};
        use serde_json::json;
        if tally.store_calls(super::ObjectClass::Manifest, StoreOp::Put) == 0 || !log::enabled(Level::Info) {
            return;
        }
        let oldest = queued.iter().map(|(at, _)| *at).min().unwrap_or(started);
        let mut fields = vec![
            ("db", json!(self.id)),
            ("txns", json!(txns)),
            ("work", json!(queued.len())),
            ("queued_ms", json!((started - oldest).as_millis() as u64)),
            ("build_ms", json!(tally.build_ms())),
            ("ms", json!(started.elapsed().as_millis() as u64)),
        ];
        if let Some(store) = tally.store_json() {
            fields.push(("store", store));
        }
        let ids: Vec<&str> = queued.iter().filter_map(|(_, id)| id.as_deref()).collect();
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
                // An earlier attempt may have landed before the lease moved.
                let e = if maybe_applied { unknown(e) } else { e };
                self.finish_failed(batch, e);
                return;
            }
            let build_started = Instant::now();
            let plan = self.build(&base, &batch, now).await;
            timing::built(build_started.elapsed());
            let plan = match plan {
                Ok(p) => p,
                Err(e) => {
                    let e = if maybe_applied { unknown(e) } else { e };
                    self.finish_failed(batch, e);
                    return;
                }
            };
            if plan.quota_refused {
                self.st().quota_pressure = true;
                self.shared.counters.add(&super::Activity { quota_refusals: 1, ..Default::default() });
            }
            let Some(m) = plan.manifest else {
                self.maybe_sweep(&base);
                self.schedule_trims(plan.due);
                self.clear_lease_flag(&batch);
                for (w, o) in batch.into_iter().zip(plan.outcomes) {
                    // Without a new manifest write, an earlier ambiguous
                    // attempt of this batch has not been fenced.
                    let o = match o {
                        Outcome::Seq(Err(e @ Error::Unavailable { .. })) if maybe_applied => {
                            Outcome::Seq(Err(unknown(e)))
                        }
                        other => other,
                    };
                    w.deliver(o);
                }
                return;
            };
            let started = Instant::now();
            self.writes_started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let res =
                self.shared.store.put(&manifest_key(&self.id), m.encode(), PutMode::IfMatch(base.etag.clone())).await;
            if !res.is_ok() {
                // Success clears the flag after installing (below).
                if matches!(res, Err(StoreError::Unavailable(_))) {
                    self.st().trusted = false;
                }
                self.write_finished();
            }
            match res {
                Ok(etag) => {
                    if let Some(l) = m.lease.as_ref().filter(|l| l.node == self.shared.node_id) {
                        let until = attempt_started + Duration::from_millis(l.ms)
                            - Duration::from_millis(cfg.lease_margin_ms.min(l.ms));
                        self.st().own_lease = Some((m.write_seq, until));
                    }
                    self.shared.counters.add(&plan.activity);
                    let v = Arc::new(Version::new(m, etag, self.shared.clone()));
                    let v = self.install(v, Some(started));
                    self.write_finished();
                    self.shared.env.trace(|| {
                        format!("{} committed v{} ws{} lease={:?}", self.id, v.m.version, v.m.write_seq, v.m.lease)
                    });
                    self.clear_lease_flag(&batch);
                    for (w, o) in batch.into_iter().zip(plan.outcomes) {
                        w.deliver(o);
                    }
                    self.after_commit(&v, plan.gc, plan.due);
                    return;
                }
                Err(StoreError::PreconditionFailed) => {}
                Err(StoreError::Unavailable(_)) => {
                    maybe_applied = true;
                    self.st().trusted = false;
                }
                Err(e) => {
                    let e: Error = e.into();
                    let e = if maybe_applied { unknown(e) } else { e };
                    self.finish_failed(batch, e);
                    return;
                }
            }
            if attempt >= cfg.commit_retry_max {
                let e = Error::Unavailable {
                    message: "could not commit after retries".into(),
                    retry_after_ms: Some(100),
                    outcome_unknown: maybe_applied,
                };
                self.finish_failed(batch, e);
                return;
            }
            let delay = 5 * (1u64 << attempt.min(6)) + self.shared.env.random_below(10);
            tokio::time::sleep(Duration::from_millis(delay)).await;
            loop {
                match self.refresh(Instant::now(), false).await {
                    Ok(v) => {
                        base = v;
                        break;
                    }
                    Err(e) => {
                        attempt += 1;
                        if attempt >= cfg.commit_retry_max {
                            let e = if maybe_applied { unknown(e) } else { e };
                            self.finish_failed(batch, e);
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                    }
                }
            }
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
        // The time of the versions this write creates. Never before the
        // base's time, so readers' view of expiries only moves forward, nor
        // before a committed transaction's read time, so readers agree with
        // what it saw expired.
        let time = batch
            .iter()
            .filter_map(|w| match w {
                Work::Commit { req, .. } => Some(req.read_time),
                _ => None,
            })
            .chain([base.m.time_ms, now.store_ms.unwrap_or(0)])
            .max()
            .unwrap_or(0);
        // Each accepted transaction gets its own sequence number, so every
        // committed state of a key has a unique version.
        let mut last_seq = base.m.version;
        let mut outcomes = Vec::with_capacity(batch.len());
        let mut accepted: Vec<&WriteSet> = Vec::new();
        // Keys the accepted transactions' stream ops wrote.
        let mut touched: Vec<KeyRange> = Vec::new();
        let mut due: BTreeSet<u64> = BTreeSet::new();
        let mut data_changed = false;
        let mut meta_changed = false;
        let mut release = false;
        let mut lease_ms = cfg.lease_ms;
        let quota = self.quota();
        let mut size = base.m.stored_bytes();
        let mut quota_refused = false;
        if base.m.is_deleted() {
            let outcomes = batch
                .iter()
                .map(|w| match w {
                    Work::MarkDeleted { .. } => Outcome::Unit(Ok(())),
                    _ => Outcome::Unit(Err(Error::not_found("database deleted"))),
                })
                .collect();
            return Ok(Plan {
                manifest: None,
                outcomes,
                gc: Vec::new(),
                quota_refused,
                activity: Default::default(),
                due,
            });
        }
        for w in batch {
            match w {
                Work::Commit { req, .. } => {
                    // Earlier in this batch, or in the base.
                    let record = commit_key(req.commit_id);
                    let applied = match tail.get(&record) {
                        Some(e) => Some(applied_commit(e)?),
                        None => committed(base, req.commit_id).await?,
                    };
                    if let Some(c) = applied {
                        // The attempt that applied it may not have lived to
                        // schedule the trims its retention calls for.
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
                        outcomes.push(Outcome::Seq(Err(Error::Unavailable {
                            message: "commit deadline exceeded".into(),
                            retry_after_ms: Some(100),
                            outcome_unknown: false,
                        })));
                        continue;
                    }
                    if let Err(e) = validate(req, base, &accepted, &touched, time).await {
                        outcomes.push(Outcome::Seq(Err(e)));
                        continue;
                    }
                    if let Err(e) = bindable(&req.writes, &m, now.store_ms, cfg) {
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
                        for id in req.writes.blob_ids() {
                            g.staged.remove(&id);
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
                    deep,
                    levels,
                    ..
                } => {
                    let Some(now_ms) = now.store_ms else {
                        outcomes.push(Outcome::Unit(Err(Error::unavailable("store clock not synchronized"))));
                        continue;
                    };
                    let present = inputs.iter().all(|id| m.segments.iter().any(|s| s.id == *id));
                    let installed = !outputs.is_empty()
                        && outputs.iter().all(|o| m.segments.iter().any(|s| s.id == o.id && s.level == o.level));
                    if m.compaction_seq == *compaction_seq && present {
                        // Only compactions change the deeper levels, and none
                        // installed since this one was planned.
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
                        // An earlier, ambiguous attempt of this install landed.
                        outcomes.push(Outcome::Unit(Ok(())));
                    } else {
                        outcomes.push(Outcome::Unit(Err(Error::Conflict("compaction inputs changed".into()))));
                    }
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
                Work::MarkDeleted { .. } => {
                    m.flags |= super::manifest::FLAG_DELETED;
                    meta_changed = true;
                    release = true;
                    outcomes.push(Outcome::Unit(Ok(())));
                }
            }
        }
        if !data_changed && !meta_changed {
            return Ok(Plan {
                manifest: None,
                outcomes,
                gc: Vec::new(),
                quota_refused,
                activity: Default::default(),
                due,
            });
        }
        if data_changed {
            m.version = last_seq;
        }
        m.time_ms = time;
        // Readers of this manifest read at `time` or later: what has expired
        // by then keeps only its tombstone. Commit records past their
        // retention go entirely.
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
        if let Some(now_ms) = now.store_ms {
            let purge = m.version_at_least_age(now_ms, cfg.txn_max_lifetime_ms);
            let mut horizon = m.tombstone_horizon;
            // Without data segments, tail tombstones shadow nothing: purge
            // old ones.
            if m.segments.iter().all(compaction::holds_records) {
                m.tail.retain(|e| {
                    let purged = e.value.is_tombstone() && e.seq <= purge;
                    if purged {
                        horizon = horizon.max(e.seq);
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
            // Keep the newest sample at least a transaction lifetime old, and
            // one a commit retention old: the purge horizons of tombstones
            // and commit records are read from them.
            let keep_after = now_ms.saturating_sub(cfg.txn_max_lifetime_ms.max(cfg.commit_retention_ms));
            while m.version_times.len() >= 2 && m.version_times[1].1 <= keep_after {
                m.version_times.remove(0);
            }
            let keep;
            (gc, keep) = m.obsolete.drain(..).partition(|o| o.since_ms + cfg.gc_grace_ms <= now_ms);
            m.obsolete = keep;
        }
        m.lease = if release {
            None
        } else {
            Some(Lease { node: self.shared.node_id.clone(), expires_ms: now.wall_ms + lease_ms, ms: lease_ms })
        };
        let mut sealed = 0;
        if m.tail_bytes() > cfg.tail_max_bytes {
            // Data and commit records go to separate segments (see
            // `compaction::pick`), each with every range deletion applied
            // through `m.version`. Long runs of stream records are sealed
            // instead (SPEC §5.11).
            let flushed = std::mem::take(&mut m.tail);
            let split = flushed.partition_point(|e| !is_commit_key(&e.key));
            let mut sealer = Sealer::new(cfg.stream_seal_min_bytes, cfg.stream_chunk_bytes);
            let mut data = Vec::with_capacity(split);
            for e in flushed[..split].iter().cloned() {
                sealer.push(&self.shared, &self.id, e, &mut data).await?;
            }
            sealer.finish(&self.shared, &self.id, &mut data).await?;
            m.chunk_bytes.bound += sealer.sealed_bytes;
            sealed = sealer.chunks;
            let commit_records = &flushed[split..];
            let rest: usize = data.iter().chain(commit_records).map(Entry::size).sum();
            if sealed > 0 && rest <= cfg.tail_max_bytes / 2 {
                // Sealing alone made room: what is left stays inline.
                m.tail = data.into_iter().chain(commit_records.iter().cloned()).collect();
            } else {
                for part in [commit_records, &data[..]] {
                    if part.is_empty() {
                        continue;
                    }
                    let mut b = SegmentBuilder::new(cfg.block_target, cfg.compression);
                    for e in part {
                        b.add(e);
                    }
                    m.segments.insert(0, compaction::write(&self.shared, &self.id, b, 0, m.version).await?);
                }
            }
        }
        m.write_seq += 1;
        let activity = super::Activity {
            range_tombstones_written: m.range_deletes.iter().filter(|t| t.seq > base.m.version).count() as u64,
            range_tombstones_retired: retired,
            expired_entries: expired,
            chunks_sealed_at_flush: sealed,
            ..Default::default()
        };
        Ok(Plan { manifest: Some(m), outcomes, gc, quota_refused, activity, due })
    }

    fn after_commit(self: &Arc<Self>, v: &Arc<Version>, gc: Vec<Obsolete>, due: BTreeSet<u64>) {
        let cfg = &self.shared.cfg;
        if !gc.is_empty() {
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
        self.shared.refresh_clock();
        {
            let mut st = self.st();
            if !st.compacting && compaction::pick(&v.m, cfg).is_some() {
                st.compacting = true;
                let me = self.clone();
                self.shared.env.spawn(async move {
                    if let Err(e) = me.clone().compact().await {
                        crate::debug!("compaction of {} stopped: {e}", me.id);
                    }
                    me.st().compacting = false;
                });
            }
        }
        self.maybe_sweep(v);
        self.schedule_trims(due);
    }

    /// Queues streams whose retention may call for a trim, starting a task
    /// that trims them unless one is running.
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
            self.trim_for_retention(id).await?;
        }
    }

    /// Commits the trim that stream `id`'s retention calls for, if any. The
    /// commit is refused if another node holds the lease, which then trims.
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
                self.shared.counters.add(&super::Activity { retention_trims: 1, ..Default::default() });
                Ok(())
            }
            Err(Error::NotFound(_) | Error::NotOwner { .. }) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Triggers a garbage collection sweep if due, using a shorter interval if under quota pressure.
    fn maybe_sweep(self: &Arc<Self>, v: &Version) {
        let cfg = &self.shared.cfg;
        let Some(now) = self.shared.store_now_ms() else { return };
        let mut st = self.st();
        let interval = if st.quota_pressure { cfg.quota_sweep_interval_ms } else { cfg.sweep_interval_ms };
        if st.sweeping || v.m.last_sweep_ms + interval > now {
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
            let (tx, rx) = oneshot::channel();
            self.enqueue(Work::Compaction {
                inputs,
                outputs,
                purged_max_seq,
                sealed_bytes,
                compaction_seq: v.m.compaction_seq,
                deep,
                levels,
                tx,
            });
            rx.await.map_err(|_| Error::unavailable("compaction install interrupted"))??;
            let n = plan.inputs.len() as u64;
            let mut a = match plan.kind {
                Kind::Drop => super::Activity { segments_dropped: n, ..Default::default() },
                Kind::Rewrite => super::Activity { segments_rewritten: n, ..Default::default() },
                Kind::DropExpired | Kind::Expire => super::Activity { segments_expired: n, ..Default::default() },
                _ => Default::default(),
            };
            a.expired_entries = expired;
            a.chunks_sealed_in_compaction = chunks;
            self.shared.counters.add(&a);
        }
    }

    /// Find unreferenced (orphaned) objects and schedule them for deletion.
    ///
    /// Everything is compared in store time, read before the version whose
    /// references are scanned: an object old enough at that moment cannot
    /// be referenced by a commit that version does not contain (see
    /// [`bindable`]).
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
        // Live blob and chunk sizes for quota accounting, and the streams
        // whose retention calls for a trim.
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
        // A blob's objects are listed together; it is as old as its newest.
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

/// Wall-clock and store-clock timestamps for a manifest write. When `store_ms`
/// is `None`, garbage collection is paused.
#[derive(Clone, Copy, Debug)]
struct Now {
    wall_ms: u64,
    store_ms: Option<u64>,
}

/// How a commit applied, if `v` holds its commit record.
async fn committed(v: &Version, id: u64) -> Result<Option<Committed>> {
    match v.get_entry(&commit_key(id)).await? {
        Some(e) if !e.value.is_tombstone() => Ok(Some(applied_commit(&e)?)),
        _ => Ok(None),
    }
}

/// The commit a commit record tells of, as a replay of it reports.
fn applied_commit(record: &Entry) -> Result<Committed> {
    let decoded = match &record.value {
        Value::Inline(b) => decode_commit_record(b).ok(),
        _ => None,
    };
    let (time_ms, appends) = decoded.ok_or_else(|| Error::internal("corrupt commit record"))?;
    Ok(Committed { version: record.seq, replayed: true, time_ms, appends })
}

/// Committed state partway through building a batch: the base version, the
/// batch's tail so far, and the range deletions of its earlier commits.
fn pending<'a>(base: &'a Version, m: &Manifest, tail: &'a BTreeMap<Bytes, Entry>) -> Pending<'a> {
    let batch_deletes = m.range_deletes.iter().filter(|t| t.seq > base.m.version).cloned().collect();
    Pending { base, tail, batch_deletes }
}

/// Measurement errors on the committing and sweeping nodes together stay
/// below this, so a blob a sweep finds too old to bind stays too old for
/// every committer.
fn bind_margin_ms(cfg: &super::EngineConfig) -> u64 {
    2 * cfg.clock_sync_max_error_ms
}

/// Validates that all staged blobs referenced by `w` are within the stage TTL
/// and have not been marked obsolete.
fn bindable(w: &WriteSet, m: &Manifest, store_now: Option<u64>, cfg: &super::EngineConfig) -> Result<()> {
    let ids = w.blob_ids();
    if ids.is_empty() {
        return Ok(());
    }
    let now = store_now.ok_or_else(|| Error::unavailable("store clock not synchronized"))?;
    let expired = ids.iter().any(|id| {
        super::types::blob_id_time(id).unwrap_or(0) + cfg.blob_stage_ttl_ms < now
            || m.obsolete.iter().any(|o| o.kind == ObjKind::Blob && o.id == *id)
    });
    if expired {
        return Err(Error::invalid("a staged blob expired before it was committed"));
    }
    Ok(())
}

/// A paged listing of a prefix, one object at a time.
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
    /// Indicates whether a commit in this batch was rejected due to quota.
    quota_refused: bool,
    /// Storage activity recorded upon successful manifest write.
    activity: super::Activity,
    /// Streams whose retention may call for a trim after this write.
    due: BTreeSet<u64>,
}

pub(crate) fn quota_exceeded(limit: u64) -> Error {
    Error::QuotaExceeded(format!("the database is at its size quota of {limit} bytes; delete data to make room"))
}

/// Returns `(total_added_bytes, bound_blob_bytes)` for a commit write set.
fn growth(w: &WriteSet) -> (u64, u64) {
    let mut added: u64 = w.streams.iter().map(StreamOp::appended_bytes).sum();
    let mut blobs = 0;
    for (k, p) in w.puts.iter().filter_map(|(k, p)| Some((k, p.as_ref()?))) {
        added += (k.len() + p.value.size()) as u64;
        if let Value::Blob(b) = &p.value {
            blobs += b.size;
        }
    }
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

/// Writes range tombstone `rt`, which replaces the range tombstones it
/// covers and the tail's entries in its range.
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
        let mut m =
            Manifest { segments: segments.to_vec(), obsolete, compaction_seq: 2, ..Manifest::new("d", 0, None) };
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
            deep: Vec::new(),
            levels: None,
            tx,
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
            let m = Manifest { obsolete: vec![ob('a'), ob('b')], ..Manifest::new("d", 0, None) };
            let base = Version::new(m, String::new(), eng.shared.clone());
            let (tx, _) = oneshot::channel();
            let sweep = Work::Obsolete { items: vec![ob('c')], blob_bytes: (0, 0), chunk_bytes: (0, 0), tx };
            let lease = Work::Lease { release: false, for_reads: true, tx: None };
            let db = eng.db("d");
            let p = db.build(&base, &[lease, sweep], Now { wall_ms: 10_000_000, store_ms: None }).await.unwrap();
            assert!(p.gc.is_empty());
            assert!(matches!(p.outcomes[1], Outcome::Unit(Err(Error::Unavailable { .. }))));
            assert_eq!(p.manifest.unwrap().obsolete.len(), 2, "kept for later");
        });
    }

    #[test]
    fn binding_checks_age_and_the_obsolete_list() {
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
        assert!(bindable(&WriteSet::default(), &fresh, None, &cfg).is_ok(), "no blob, no clock needed");
        assert!(bindable(&writes(&id), &listed(ObjKind::Blob, &other), Some(now), &cfg).is_ok());
        assert!(bindable(&writes(&id), &listed(ObjKind::Segment, &id), Some(now), &cfg).is_ok());
        assert!(bindable(&writes(&id), &listed(ObjKind::Blob, &id), Some(now), &cfg).is_err(), "an orphaned blob");
        assert!(bindable(&writes(&old), &fresh, Some(now), &cfg).is_err(), "an expired blob");
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
