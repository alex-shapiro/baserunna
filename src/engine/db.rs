//! A database handle on one node: snapshot management, strong/snapshot
//! reads, leases, group commit with manifest CAS, compaction and GC
//! scheduling. See SPEC §5.5–§5.9.

use super::blob::BlobWriter;
use super::compaction::{merge, pick_run};
use super::manifest::{Lease, Manifest, ObjKind, Obsolete, SegmentMeta};
use super::segment;
use super::txn::{CommitRequest, Isolation, Txn, WriteSet, live_keys_in_range, validate};
use super::types::{Entry, Seq, Value};
use super::version::Version;
use super::{Shared, blob_prefix, manifest_key, seg_key};
use crate::error::{Error, Result};
use crate::store::{GetOpts, PutMode, StoreError};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
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

/// Run `f` as one operation invoked at `at`: its strong reads may use a
/// manifest validated at or after `at` (still linearizable), so several
/// reads within one request pay for one validation.
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
        tx: Option<oneshot::Sender<Result<()>>>,
    },
    Compaction {
        inputs: Vec<String>,
        output: Option<SegmentMeta>,
        purged_max_seq: Seq,
        tx: oneshot::Sender<Result<()>>,
    },
    Obsolete {
        items: Vec<Obsolete>,
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
                let _ = tx.send(r.map(|_| Committed { version: 0, replayed: false }));
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
    /// `current` is known to equal the store's latest manifest unless
    /// someone else wrote since (false after an ambiguous write).
    trusted: bool,
    /// Start time of the most recent successful validation of `current`.
    verified_at: Option<Instant>,
    last_access: Instant,
    window_start: Instant,
    reads_in_window: u32,
    lease_requested: bool,
    /// Our lease, as of the manifest write (`write_seq`) that last renewed
    /// it, and the monotonic instant until which we may rely on it.
    own_lease: Option<(u64, Instant)>,
    compacting: bool,
    sweeping: bool,
}

struct Queue {
    items: VecDeque<Work>,
    running: bool,
}

pub struct Db {
    pub id: String,
    shared: Arc<Shared>,
    st: Mutex<State>,
    q: Mutex<Queue>,
    load_lock: tokio::sync::Mutex<()>,
    /// True while a manifest write by this node is in flight. Its result
    /// may already be visible to others, so lease reads wait for it.
    writing: std::sync::atomic::AtomicBool,
    write_done: tokio::sync::Notify,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Committed {
    pub version: Seq,
    /// The commit id had already been applied by an earlier attempt.
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbStats {
    pub version: Seq,
    pub segments: usize,
    pub segment_bytes: u64,
    pub tail_entries: usize,
    pub tail_bytes: usize,
    pub lease_holder: Option<String>,
    pub lease_expires_ms: Option<u64>,
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
                lease_requested: false,
                own_lease: None,
                compacting: false,
                sweeping: false,
            }),
            q: Mutex::new(Queue { items: VecDeque::new(), running: false }),
            load_lock: tokio::sync::Mutex::new(()),
            writing: std::sync::atomic::AtomicBool::new(false),
            write_done: tokio::sync::Notify::new(),
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

    pub(crate) fn idle_for(&self, ms: u64) -> bool {
        let st = self.st();
        let q = self.queue();
        !q.running
            && q.items.is_empty()
            && !st.compacting
            && !st.sweeping
            && st.last_access.elapsed() >= Duration::from_millis(ms)
    }

    // Lease validity is measured on each node's monotonic clock, never by
    // comparing wall clocks: the holder counts from before it sent the
    // renewing write, everyone else from when they first saw that renewal.
    // Every observation follows the send, so others always consider a lease
    // valid for longer than its holder relies on it, however skewed the
    // wall clocks are. `expires_ms` in the manifest only identifies a
    // renewal and serves as a hint.

    /// How long we may still rely on our lease in `m` (zero if `m` is not
    /// the manifest write that last renewed it).
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

    /// A lease in `m` held by another node, with how long it may still be
    /// valid from our viewpoint.
    fn foreign_lease<'a>(&self, m: &'a Manifest) -> Option<(&'a Lease, Duration)> {
        let l = m.lease.as_ref().filter(|l| l.node != self.shared.node_id)?;
        let valid_for = Duration::from_millis(self.shared.cfg.lease_ms + self.shared.cfg.clock_skew_margin_ms);
        let left = (self.shared.lease_seen(&self.id, l) + valid_for).saturating_duration_since(Instant::now());
        (!left.is_zero()).then_some((l, left))
    }

    /// The node holding a valid lease on this database, if known.
    pub fn lease_holder(&self) -> Option<String> {
        let st = self.st();
        let cur = st.current.clone()?;
        if self.holds_lease(&st, &cur.m) {
            return Some(self.shared.node_id.clone());
        }
        self.foreign_lease(&cur.m).map(|(l, _)| l.node.clone())
    }

    pub fn stats(&self) -> Option<DbStats> {
        let cur = self.current()?;
        Some(DbStats {
            version: cur.m.version,
            segments: cur.m.segments.len(),
            segment_bytes: cur.m.segments.iter().map(|s| s.size).sum(),
            tail_entries: cur.m.tail.len(),
            tail_bytes: cur.m.tail_bytes(),
            lease_holder: cur.m.lease.as_ref().map(|l| l.node.clone()),
            lease_expires_ms: cur.m.lease.as_ref().map(|l| l.expires_ms),
        })
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
                v
            }
        };
        if let Some(l) = installed.m.lease.as_ref().filter(|l| l.node != self.shared.node_id) {
            self.shared.lease_seen(&self.id, l);
        }
        st.trusted = true;
        if let Some(t) = verified
            && st.verified_at.is_none_or(|old| t > old)
        {
            st.verified_at = Some(t);
        }
        installed
    }

    /// Fetch the latest manifest unless one fetched after `not_before` is
    /// already known. Creates the database if `create` and it is missing.
    pub async fn refresh(self: &Arc<Self>, not_before: Instant, create: bool) -> Result<Arc<Version>> {
        let _g = self.load_lock.lock().await;
        let (etag, cur) = {
            let st = self.st();
            if let (Some(c), Some(va)) = (&st.current, st.verified_at)
                && va >= not_before
                && st.trusted
            {
                return Ok(c.clone());
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
                    return Ok(self.install(c, Some(started)));
                }
                Ok(g) => {
                    let m = Manifest::decode(&g.data)?;
                    if m.db_id != self.id {
                        return Err(Error::internal("manifest belongs to another database"));
                    }
                    let v = Arc::new(Version { m, etag: g.etag, shared: self.shared.clone() });
                    return Ok(self.install(v, Some(started)));
                }
                Err(StoreError::NotFound) if create => {
                    let m = Manifest::new(&self.id, self.shared.env.now_ms());
                    match self.shared.store.put(&key, m.encode(), PutMode::Create).await {
                        Ok(etag) => {
                            let v = Arc::new(Version { m, etag, shared: self.shared.clone() });
                            return Ok(self.install(v, Some(started)));
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

    /// A snapshot satisfying `c`.
    pub async fn read_version(self: &Arc<Self>, c: Consistency) -> Result<Arc<Version>> {
        if c == Consistency::Strong {
            // A write of ours that is in flight may already be visible to
            // readers elsewhere; serving the cached version now could go
            // back in time relative to them.
            loop {
                let notified = self.write_done.notified();
                let mut notified = std::pin::pin!(notified);
                notified.as_mut().enable();
                if !self.writing.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                notified.await;
            }
        }
        let now = Instant::now();
        let lease_ms = self.shared.cfg.lease_ms;
        let mut want_lease = false;
        {
            let mut st = self.st();
            st.last_access = now;
            if now.duration_since(st.window_start) > Duration::from_millis(lease_ms) {
                st.window_start = now;
                st.reads_in_window = 0;
            }
            st.reads_in_window += 1;
            let hot = st.reads_in_window >= self.shared.cfg.lease_read_threshold;
            if let Some(cur) = st.current.clone() {
                let lease_left = if st.trusted { self.own_lease_left(&st, &cur.m) } else { Duration::ZERO };
                let leased = !lease_left.is_zero();
                let fresh = match c {
                    Consistency::Strong => leased,
                    Consistency::Snapshot { max_staleness_ms } => {
                        leased
                            || st
                                .verified_at
                                .is_some_and(|v| now.duration_since(v) <= Duration::from_millis(max_staleness_ms))
                    }
                };
                if leased && hot && !st.lease_requested && lease_left < Duration::from_millis(lease_ms / 2) {
                    st.lease_requested = true;
                    want_lease = true;
                }
                if fresh {
                    drop(st);
                    self.shared.env.trace(|| {
                        format!(
                            "{} read v{} from cache (leased={leased}, lease={:?})",
                            self.id, cur.m.version, cur.m.lease
                        )
                    });
                    if want_lease {
                        self.enqueue(Work::Lease { release: false, tx: None });
                    }
                    return check_live(cur);
                }
            }
        }
        let not_before = match c {
            Consistency::Strong => INVOKED_AT.try_with(|t| *t).unwrap_or(now).min(now),
            Consistency::Snapshot { .. } => now,
        };
        let v = self.refresh(not_before, false).await?;
        self.shared.env.trace(|| format!("{} read v{} after refresh (lease={:?})", self.id, v.m.version, v.m.lease));
        let hot = {
            let mut st = self.st();
            let hot = st.reads_in_window >= self.shared.cfg.lease_read_threshold && !st.lease_requested;
            if hot && !self.holds_lease(&st, &v.m) && self.foreign_lease(&v.m).is_none() {
                st.lease_requested = true;
                true
            } else {
                false
            }
        };
        if hot && !v.m.is_deleted() {
            self.enqueue(Work::Lease { release: false, tx: None });
        }
        check_live(v)
    }

    /// Mark the database deleted (idempotent). Afterwards every read and
    /// commit on any node fails with not-found.
    pub async fn mark_deleted(self: &Arc<Self>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.enqueue(Work::MarkDeleted { tx });
        rx.await.map_err(|_| Error::unavailable("delete interrupted"))?
    }

    pub async fn begin(self: &Arc<Self>, isolation: Isolation, c: Consistency) -> Result<Txn> {
        let snap = self.read_version(c).await?;
        Ok(Txn::new(snap, isolation, self.shared.env.random_u128(), self.shared.cfg.txn_max_write_bytes))
    }

    pub fn blob_writer(&self, content_type: &str) -> BlobWriter {
        BlobWriter::new(self.shared.clone(), &self.id, content_type)
    }

    /// Commit a transaction's request. Read-only requests return their read
    /// version without touching storage.
    pub async fn commit(self: &Arc<Self>, req: CommitRequest) -> Result<Seq> {
        Ok(self.commit_detailed(req).await?.version)
    }

    /// Like [`Db::commit`], also reporting whether the commit id had already
    /// been applied (an idempotent replay; nothing new was written).
    pub async fn commit_detailed(self: &Arc<Self>, mut req: CommitRequest) -> Result<Committed> {
        if req.writes.is_empty() {
            return Ok(Committed { version: req.read_version, replayed: false });
        }
        if req.deadline.is_none() {
            req.deadline = Some(Instant::now() + Duration::from_millis(self.shared.cfg.commit_timeout_ms));
        }
        let (tx, rx) = oneshot::channel();
        self.enqueue(Work::Commit { req, tx });
        rx.await.map_err(|_| Error::Unavailable {
            message: "commit interrupted".into(),
            retry_after_ms: None,
            outcome_unknown: true,
        })?
    }

    /// Acquire or renew this node's lease.
    pub async fn acquire_lease(self: &Arc<Self>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.enqueue(Work::Lease { release: false, tx: Some(tx) });
        rx.await.map_err(|_| Error::unavailable("lease request interrupted"))?
    }

    /// Give up our lease (graceful shutdown) so others can take over at once.
    pub async fn release_lease(self: &Arc<Self>) -> Result<()> {
        let held = self.current().is_some_and(|c| c.m.lease.as_ref().is_some_and(|l| l.node == self.shared.node_id));
        if !held {
            return Ok(());
        }
        let (tx, rx) = oneshot::channel();
        self.enqueue(Work::Lease { release: true, tx: Some(tx) });
        rx.await.map_err(|_| Error::unavailable("lease release interrupted"))?
    }

    /// Whether a commit id is known to be applied, from a strong read
    /// (cheap; an in-flight ambiguous attempt may still land later).
    pub async fn commit_status(self: &Arc<Self>, id: u128) -> Result<Option<Seq>> {
        let v = self.read_version(Consistency::Strong).await?;
        Ok(v.m.commit_version(id))
    }

    /// Definitive commit status: writes the manifest first, fencing any
    /// in-flight attempt of an earlier ambiguous commit, then checks the
    /// commit ring. `None` means not committed, if the commit is recent
    /// enough to be in the ring.
    pub async fn commit_status_fenced(self: &Arc<Self>, id: u128) -> Result<Option<Seq>> {
        self.acquire_lease().await?;
        self.commit_status(id).await
    }

    fn enqueue(self: &Arc<Self>, w: Work) {
        let spawn = {
            let mut q = self.queue();
            q.items.push_back(w);
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
            let batch: Vec<Work> = {
                let mut q = self.queue();
                if q.items.is_empty() {
                    q.running = false;
                    return;
                }
                let n = q.items.len().min(self.shared.cfg.commit_batch_max);
                q.items.drain(..n).collect()
            };
            self.process(batch).await;
        }
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
            let now_ms = self.shared.env.now_ms();
            let foreign = self.foreign_lease(&base.m).map(|(l, left)| (l.node.clone(), left));
            if let Some((holder, left)) = foreign {
                let e = Error::NotOwner { holder, retry_after_ms: left.as_millis() as u64 };
                self.finish_failed(batch, e);
                return;
            }
            let plan = match self.build(&base, &batch, now_ms).await {
                Ok(p) => p,
                Err(e) => {
                    let e = if maybe_applied { unknown(e) } else { e };
                    self.finish_failed(batch, e);
                    return;
                }
            };
            let Some(m) = plan.manifest else {
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
            self.writing.store(true, std::sync::atomic::Ordering::SeqCst);
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
                    if m.lease.as_ref().is_some_and(|l| l.node == self.shared.node_id) {
                        let until = attempt_started + Duration::from_millis(cfg.lease_ms)
                            - Duration::from_millis(cfg.clock_skew_margin_ms.min(cfg.lease_ms));
                        self.st().own_lease = Some((m.write_seq, until));
                    }
                    let v = Arc::new(Version { m, etag, shared: self.shared.clone() });
                    let v = self.install(v, Some(started));
                    self.write_finished();
                    self.shared.env.trace(|| {
                        format!("{} committed v{} ws{} lease={:?}", self.id, v.m.version, v.m.write_seq, v.m.lease)
                    });
                    self.clear_lease_flag(&batch);
                    for (w, o) in batch.into_iter().zip(plan.outcomes) {
                        w.deliver(o);
                    }
                    self.after_commit(&v, plan.gc);
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
        self.writing.store(false, std::sync::atomic::Ordering::SeqCst);
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

    async fn build(&self, base: &Version, batch: &[Work], now_ms: u64) -> Result<Plan> {
        let cfg = &self.shared.cfg;
        let mut m = base.m.clone();
        let mut tail: BTreeMap<Bytes, Entry> = m.tail.drain(..).map(|e| (e.key.clone(), e)).collect();
        // Each accepted transaction gets its own sequence number, so every
        // committed state of a key has a unique version.
        let mut last_seq = base.m.version;
        let mut outcomes = Vec::with_capacity(batch.len());
        let mut accepted: Vec<&WriteSet> = Vec::new();
        let mut data_changed = false;
        let mut meta_changed = false;
        let mut release = false;
        if base.m.is_deleted() {
            let outcomes = batch
                .iter()
                .map(|w| match w {
                    Work::MarkDeleted { .. } => Outcome::Unit(Ok(())),
                    _ => Outcome::Unit(Err(Error::not_found("database deleted"))),
                })
                .collect();
            return Ok(Plan { manifest: None, outcomes, gc: Vec::new() });
        }
        for w in batch {
            match w {
                Work::Commit { req, .. } => {
                    if let Some(v) = base.m.commit_version(req.commit_id) {
                        outcomes.push(Outcome::Seq(Ok(Committed { version: v, replayed: true })));
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
                    if let Err(e) = validate(req, base, &accepted).await {
                        outcomes.push(Outcome::Seq(Err(e)));
                        continue;
                    }
                    let expired = req
                        .writes
                        .blob_ids()
                        .iter()
                        .any(|id| super::types::blob_id_time(id).unwrap_or(0) + cfg.blob_stage_ttl_ms < now_ms);
                    if expired {
                        outcomes
                            .push(Outcome::Seq(Err(Error::invalid("a staged blob expired before it was committed"))));
                        continue;
                    }
                    let new_seq = last_seq + 1;
                    for r in &req.writes.range_deletes {
                        for k in live_keys_in_range(base, &tail, r).await? {
                            if !req.writes.puts.contains_key(&k) {
                                tail.insert(k.clone(), Entry { key: k, seq: new_seq, value: Value::Tombstone });
                            }
                        }
                    }
                    for (k, v) in &req.writes.puts {
                        tail.insert(
                            k.clone(),
                            Entry { key: k.clone(), seq: new_seq, value: v.clone().unwrap_or(Value::Tombstone) },
                        );
                    }
                    accepted.push(&req.writes);
                    m.recent_commits.push((req.commit_id, new_seq));
                    last_seq = new_seq;
                    data_changed = true;
                    outcomes.push(Outcome::Seq(Ok(Committed { version: new_seq, replayed: false })));
                }
                Work::Lease { release: r, .. } => {
                    release |= *r;
                    meta_changed = true;
                    outcomes.push(Outcome::Unit(Ok(())));
                }
                Work::Compaction { inputs, output, purged_max_seq, .. } => {
                    let pos = m.segments.iter().position(|s| Some(&s.id) == inputs.first());
                    let ok = pos.is_some_and(|p| {
                        p + inputs.len() <= m.segments.len()
                            && m.segments[p..p + inputs.len()].iter().zip(inputs).all(|(s, i)| s.id == *i)
                    });
                    if let (true, Some(p)) = (ok, pos) {
                        let out: Vec<SegmentMeta> = output.iter().cloned().collect();
                        m.segments.splice(p..p + inputs.len(), out);
                        for id in inputs {
                            m.obsolete.push(Obsolete { kind: ObjKind::Segment, id: id.clone(), since_ms: now_ms });
                        }
                        m.tombstone_horizon = m.tombstone_horizon.max(*purged_max_seq);
                        meta_changed = true;
                        outcomes.push(Outcome::Unit(Ok(())));
                    } else if output.as_ref().is_some_and(|o| m.segments.iter().any(|s| s.id == o.id)) {
                        outcomes.push(Outcome::Unit(Ok(())));
                    } else {
                        outcomes.push(Outcome::Unit(Err(Error::Conflict("compaction inputs changed".into()))));
                    }
                }
                Work::Obsolete { items, .. } => {
                    for it in items {
                        let listed = m.obsolete.iter().any(|o| o.kind == it.kind && o.id == it.id);
                        let live_seg = it.kind == ObjKind::Segment && m.segments.iter().any(|s| s.id == it.id);
                        if !listed && !live_seg {
                            m.obsolete.push(it.clone());
                        }
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
            return Ok(Plan { manifest: None, outcomes, gc: Vec::new() });
        }
        if data_changed {
            m.version = last_seq;
        }
        m.tail = tail.into_values().collect();
        // Without segments, tail tombstones shadow nothing: purge old ones.
        if m.segments.is_empty() {
            let purge = m.version_at_least_age(now_ms, cfg.txn_max_lifetime_ms);
            let mut horizon = m.tombstone_horizon;
            m.tail.retain(|e| {
                if e.value.is_tombstone() && e.seq <= purge {
                    horizon = horizon.max(e.seq);
                    false
                } else {
                    true
                }
            });
            m.tombstone_horizon = horizon;
        }
        if m.recent_commits.len() > cfg.commit_ring_max {
            let n = m.recent_commits.len() - cfg.commit_ring_max;
            m.recent_commits.drain(..n);
        }
        if data_changed && m.version_times.last().is_none_or(|(_, t)| *t + cfg.version_sample_ms <= now_ms) {
            m.version_times.push((m.version, now_ms));
        }
        // Keep the newest sample at least a transaction lifetime old: the
        // tombstone purge horizons are read from it.
        let keep_after = now_ms.saturating_sub(cfg.txn_max_lifetime_ms);
        while m.version_times.len() >= 2 && m.version_times[1].1 <= keep_after {
            m.version_times.remove(0);
        }
        m.lease = if release {
            None
        } else {
            Some(Lease { node: self.shared.node_id.clone(), expires_ms: now_ms + cfg.lease_ms })
        };
        let (gc, keep): (Vec<Obsolete>, Vec<Obsolete>) =
            m.obsolete.drain(..).partition(|o| o.since_ms + cfg.gc_grace_ms <= now_ms);
        m.obsolete = keep;
        if m.tail_bytes() > cfg.tail_max_bytes {
            let (data, stats) = segment::build(&m.tail, cfg.block_target);
            let id = self.shared.env.random_hex(16);
            let key = seg_key(&self.id, &id);
            self.shared.retry.run(|| self.shared.store.put(&key, data.clone(), PutMode::Overwrite)).await?;
            m.segments.insert(0, seg_meta(id, &stats));
            m.tail.clear();
        }
        m.write_seq += 1;
        Ok(Plan { manifest: Some(m), outcomes, gc })
    }

    fn after_commit(self: &Arc<Self>, v: &Arc<Version>, gc: Vec<Obsolete>) {
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
        let now_ms = self.shared.env.now_ms();
        let mut st = self.st();
        if !st.compacting && pick_run(&v.m.segments, cfg.compaction_fanout, cfg.max_segments).is_some() {
            st.compacting = true;
            let me = self.clone();
            self.shared.env.spawn(async move {
                if let Err(e) = me.clone().compact().await {
                    crate::debug!("compaction of {} stopped: {e}", me.id);
                }
                me.st().compacting = false;
            });
        }
        if !st.sweeping && v.m.last_sweep_ms + cfg.sweep_interval_ms <= now_ms {
            st.sweeping = true;
            let me = self.clone();
            self.shared.env.spawn(async move {
                if let Err(e) = me.clone().sweep().await {
                    crate::debug!("sweep of {} stopped: {e}", me.id);
                }
                me.st().sweeping = false;
            });
        }
    }

    async fn compact(self: Arc<Self>) -> Result<()> {
        let _permit =
            self.shared.compaction_slots.acquire().await.map_err(|_| Error::internal("compaction semaphore closed"))?;
        let cfg = self.shared.cfg.clone();
        loop {
            let Some(v) = self.current() else { return Ok(()) };
            let now_ms = self.shared.env.now_ms();
            if !self.holds_lease(&self.st(), &v.m) {
                return Ok(());
            }
            let Some(run) = pick_run(&v.m.segments, cfg.compaction_fanout, cfg.max_segments) else {
                return Ok(());
            };
            let bottom = run.end == v.m.segments.len();
            let purge_seq = v.m.version_at_least_age(now_ms, cfg.txn_max_lifetime_ms);
            let inputs: Vec<SegmentMeta> = v.m.segments[run].to_vec();
            let out = merge(&self.shared, &v.m, &inputs, bottom, purge_seq).await?;
            let output = match out.data {
                None => None,
                Some((data, stats)) => {
                    let id = self.shared.env.random_hex(16);
                    let key = seg_key(&self.id, &id);
                    self.shared.retry.run(|| self.shared.store.put(&key, data.clone(), PutMode::Overwrite)).await?;
                    Some(seg_meta(id, &stats))
                }
            };
            let (tx, rx) = oneshot::channel();
            self.enqueue(Work::Compaction {
                inputs: inputs.iter().map(|s| s.id.clone()).collect(),
                output,
                purged_max_seq: out.purged_max_seq,
                tx,
            });
            rx.await.map_err(|_| Error::unavailable("compaction install interrupted"))??;
        }
    }

    /// Find unreferenced (orphaned) objects and schedule them for deletion.
    async fn sweep(self: Arc<Self>) -> Result<()> {
        let cfg = self.shared.cfg.clone();
        let v = self.refresh(Instant::now(), false).await?;
        let now_ms = self.shared.env.now_ms();
        if !self.holds_lease(&self.st(), &v.m) {
            return Ok(());
        }
        let store = &*self.shared.store;
        let segs = crate::store::list_all(store, &format!("dbs/{}/seg/", self.id), &self.shared.retry).await?;
        let blobs = crate::store::list_all(store, &format!("dbs/{}/blob/", self.id), &self.shared.retry).await?;
        let mut referenced_segs: BTreeSet<&str> = v.m.segments.iter().map(|s| s.id.as_str()).collect();
        let mut referenced_blobs: BTreeSet<String> = BTreeSet::new();
        for o in &v.m.obsolete {
            match o.kind {
                ObjKind::Segment => {
                    referenced_segs.insert(o.id.as_str());
                }
                ObjKind::Blob => {
                    referenced_blobs.insert(o.id.clone());
                }
            }
        }
        for e in v.all_entries(false).await? {
            if let Value::Blob(b) = e.value {
                referenced_blobs.insert(b.id);
            }
        }
        let mut items = Vec::new();
        for o in &segs {
            let id = o.key.rsplit('/').next().unwrap_or("");
            if super::types::valid_object_id(id)
                && !referenced_segs.contains(id)
                && o.last_modified_ms + cfg.orphan_grace_ms <= now_ms
            {
                items.push(Obsolete { kind: ObjKind::Segment, id: id.to_string(), since_ms: now_ms });
            }
        }
        let mut blob_mtimes: BTreeMap<String, u64> = BTreeMap::new();
        for o in &blobs {
            let mut parts = o.key.rsplit('/');
            let _chunk = parts.next();
            let Some(id) = parts.next() else { continue };
            let e = blob_mtimes.entry(id.to_string()).or_insert(0);
            *e = (*e).max(o.last_modified_ms);
        }
        for (id, mtime) in blob_mtimes {
            let created = super::types::blob_id_time(&id).unwrap_or(0);
            if super::types::valid_object_id(&id)
                && !referenced_blobs.contains(&id)
                && mtime + cfg.orphan_grace_ms <= now_ms
                && created + cfg.blob_stage_ttl_ms <= now_ms
            {
                items.push(Obsolete { kind: ObjKind::Blob, id, since_ms: now_ms });
            }
        }
        let (tx, rx) = oneshot::channel();
        self.enqueue(Work::Obsolete { items, tx });
        rx.await.map_err(|_| Error::unavailable("sweep interrupted"))?
    }

    /// Run a sweep now (tests and admin tooling).
    pub async fn sweep_now(self: &Arc<Self>) -> Result<()> {
        self.clone().sweep().await
    }

    /// Commit a no-op lease renewal that also performs due GC (tests).
    pub async fn maintenance_now(self: &Arc<Self>) -> Result<()> {
        self.acquire_lease().await
    }
}

fn check_live(v: Arc<Version>) -> Result<Arc<Version>> {
    if v.m.is_deleted() { Err(Error::not_found("database deleted")) } else { Ok(v) }
}

struct Plan {
    manifest: Option<Manifest>,
    outcomes: Vec<Outcome>,
    gc: Vec<Obsolete>,
}

fn unknown(e: Error) -> Error {
    Error::Unavailable {
        message: format!("commit outcome unknown: {}", e.message()),
        retry_after_ms: Some(100),
        outcome_unknown: true,
    }
}

fn seg_meta(id: String, s: &segment::SegmentStats) -> SegmentMeta {
    SegmentMeta {
        id,
        size: s.size,
        entries: s.entries,
        tombstones: s.tombstones,
        min_key: s.min_key.clone(),
        max_key: s.max_key.clone(),
        min_seq: s.min_seq,
        max_seq: s.max_seq,
    }
}

async fn delete_object(shared: &Shared, db: &str, o: &Obsolete) -> Result<()> {
    match o.kind {
        ObjKind::Segment => {
            let key = seg_key(db, &o.id);
            shared.retry.run(|| shared.store.delete(&key)).await?;
        }
        ObjKind::Blob => {
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

    fn meta(c: char) -> SegmentMeta {
        SegmentMeta {
            id: c.to_string().repeat(32),
            size: 100,
            entries: 1,
            tombstones: 0,
            min_key: Bytes::new(),
            max_key: Bytes::new(),
            min_seq: 0,
            max_seq: 0,
        }
    }

    /// The plan for `batch` on a manifest with `segments` (newest first)
    /// and `obsolete` objects.
    async fn plan(segments: &[SegmentMeta], obsolete: Vec<Obsolete>, batch: Vec<Work>) -> Plan {
        let (eng, _, _) = engine(1, EngineConfig::small_for_tests());
        let m = Manifest { segments: segments.to_vec(), obsolete, ..Manifest::new("d", 0) };
        let base = Version { m, etag: String::new(), shared: eng.shared.clone() };
        eng.db("d").build(&base, &batch, 1_000).await.unwrap()
    }

    fn compaction(inputs: &str, output: char) -> Work {
        let (tx, _) = oneshot::channel();
        Work::Compaction {
            inputs: inputs.chars().map(|c| meta(c).id).collect(),
            output: Some(meta(output)),
            purged_max_seq: 0,
            tx,
        }
    }

    fn succeeded(p: &Plan) -> bool {
        matches!(p.outcomes[..], [Outcome::Unit(Ok(()))])
    }

    fn segment_names(p: &Plan) -> Option<String> {
        p.manifest.as_ref().map(|m| m.segments.iter().map(|s| &s.id[..1]).collect())
    }

    /// A compaction installs only if its inputs are still a contiguous run
    /// of the current segments; installing it again is a no-op success.
    #[test]
    fn compaction_installs_check_their_inputs() {
        rt().block_on(async {
            let abc = [meta('a'), meta('b'), meta('c')];
            let p = plan(&abc, vec![], vec![compaction("bc", 'x')]).await;
            assert!(succeeded(&p));
            assert_eq!(segment_names(&p).as_deref(), Some("ax"));
            for inputs in ["bx", "bcd", "cb", "ac"] {
                let p = plan(&abc, vec![], vec![compaction(inputs, 'y')]).await;
                assert!(!succeeded(&p) && p.manifest.is_none(), "inputs {inputs} were installed");
            }
            let p = plan(&[meta('a'), meta('x')], vec![], vec![compaction("bc", 'x')]).await;
            assert!(succeeded(&p) && p.manifest.is_none());
        });
    }

    /// A sweep's findings are recorded once each, and never for a live
    /// segment.
    #[test]
    fn obsolete_objects_are_recorded_once_and_never_live_segments() {
        rt().block_on(async {
            let ob = |kind, c: char| Obsolete { kind, id: c.to_string().repeat(32), since_ms: 0 };
            let (seg, blob) = (ObjKind::Segment, ObjKind::Blob);
            let (tx, _) = oneshot::channel();
            let items = vec![ob(seg, 'a'), ob(seg, 'b'), ob(seg, 'c'), ob(blob, 'a'), ob(blob, 'd')];
            let p = plan(&[meta('a')], vec![ob(seg, 'b'), ob(blob, 'd')], vec![Work::Obsolete { items, tx }]).await;
            let got: Vec<(ObjKind, char)> =
                p.manifest.unwrap().obsolete.iter().map(|o| (o.kind, o.id.chars().next().unwrap())).collect();
            assert_eq!(got, [(seg, 'b'), (blob, 'd'), (seg, 'c'), (blob, 'a')]);
        });
    }
}
