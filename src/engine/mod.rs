//! LSM storage engine backed by object storage.

pub mod blob;
pub mod cache;
pub mod clock;
pub mod compaction;
mod config;
pub mod db;
pub mod events;
pub mod local;
pub mod manifest;
pub mod memory;
pub mod range_delete;
pub mod segment;
pub mod stream;
pub mod txn;
pub mod types;
pub mod version;

use crate::env::Env;
use crate::store::{DynStore, Retry};
use crate::util::DetHashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

pub use config::EngineConfig;
pub use db::{CommitState, Committed, Consistency, Db, DbStats, LevelStats};
pub use stream::{Appended, Record, Retention, Seek, StreamOp, StreamState};
pub use txn::{CommitRequest, Isolation, Put, ReadItem, ReadValue, Ttl, Txn};
pub use types::{BlobRef, Entry, Seq, Value};
pub use version::{Change, ChangeCursor, ChangePage, ChangePos, KeyRange, Snapshot, Version};

/// State shared by every database handle on a node.
pub struct Shared {
    pub store: DynStore,
    pub env: Arc<Env>,
    pub cfg: EngineConfig,
    pub cache: cache::Cache,
    /// Unique lease identity of this node process.
    pub node_id: String,
    pub retry: Retry,
    pub compaction_slots: Semaphore,
    /// Store clock used to generate garbage collection timestamps.
    pub clock: clock::StoreClock,
    /// Latest observed foreign lease `(holder, write_seq, first_seen)` per
    /// database, retained across `Db` handle evictions.
    pub(crate) observed_leases: Mutex<DetHashMap<String, (String, u64, tokio::time::Instant)>>,
    system: Mutex<std::collections::BTreeSet<String>>,
    pub memory: Arc<memory::Memory>,
    engine: std::sync::OnceLock<std::sync::Weak<Engine>>,
    unload_failed_at: Mutex<Option<tokio::time::Instant>>,
    access_seq: std::sync::atomic::AtomicU64,
    pub metrics: Arc<crate::metrics::StoreMetrics>,
    /// True once the node begins releasing leases for shutdown.
    stopping: std::sync::atomic::AtomicBool,
    /// True once [`Engine::end_waits`] is called.
    waits_ended: std::sync::atomic::AtomicBool,
    /// Called with a database id and version after this node commits a new data version.
    commit_hook: std::sync::OnceLock<CommitHook>,
    /// Called on the lease holder with the event tables a commit changed.
    event_hook: std::sync::OnceLock<EventHook>,
}

/// Callback invoked on the lease holder with a database and the event tables
/// whose unsealed rows or parts a commit changed, to schedule sealing,
/// merges and retention (SPEC §5.13).
pub type EventHook = Box<dyn Fn(&Arc<Db>, std::collections::BTreeSet<u32>) + Send + Sync>;

/// Callback invoked with `(db_id, version)` after this node commits a new data version.
pub type CommitHook = Box<dyn Fn(&str, u64) + Send + Sync>;

impl Shared {
    pub(crate) fn committed(&self, db: &str, version: u64) {
        if let Some(h) = self.commit_hook.get() {
            h(db, version);
        }
    }

    pub(crate) fn events_changed(&self, db: &Arc<Db>, tables: std::collections::BTreeSet<u32>) {
        if let Some(h) = self.event_hook.get() {
            h(db, tables);
        }
    }

    pub(crate) fn stopping(&self) -> bool {
        self.stopping.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Returns true if [`Engine::end_waits`] has been called.
    pub fn waits_ended(&self) -> bool {
        self.waits_ended.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn next_access_seq(&self) -> u64 {
        self.access_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1
    }

    pub(crate) fn is_system(&self, db: &str) -> bool {
        self.system.lock().unwrap_or_else(|e| e.into_inner()).contains(db)
    }

    /// Unloads idle databases if memory usage exceeds the budget, and returns true if usage is
    /// then within the budget.
    pub fn reclaim_to_budget(&self) -> bool {
        if !self.memory.is_exceeded() {
            return true;
        }
        self.unload_idle();
        !self.memory.is_exceeded()
    }

    pub(crate) fn unload_idle(&self) {
        if self.memory.is_exceeded()
            && let Some(e) = self.engine.get().and_then(std::sync::Weak::upgrade)
        {
            e.unload_idle();
        }
    }

    /// Returns the current store time if the clock offset was synchronized within `clock_sync_max_age_ms`.
    pub fn store_now_ms(&self) -> Option<u64> {
        self.clock.now_ms(Duration::from_millis(self.cfg.clock_sync_max_age_ms))
    }

    /// Returns the lower bound on the current store time if synchronized within `clock_sync_max_age_ms`.
    pub fn store_earliest_now_ms(&self) -> Option<u64> {
        self.clock.earliest_now_ms(Duration::from_millis(self.cfg.clock_sync_max_age_ms))
    }

    /// Returns the synchronized store time if available, or the local wall clock time.
    pub fn time_ms(&self) -> u64 {
        self.store_now_ms().unwrap_or_else(|| self.env.now_ms())
    }

    /// Returns the current store time, synchronizing the clock if it was not synchronized within `clock_sync_max_age_ms`.
    pub async fn store_time_ms(&self) -> crate::error::Result<u64> {
        if let Some(now) = self.store_now_ms() {
            return Ok(now);
        }
        self.sync_clock().await?;
        self.store_now_ms().ok_or_else(|| crate::error::Error::unavailable("store clock not synchronized"))
    }

    async fn sync_clock(&self) -> crate::error::Result<()> {
        let interval = Duration::from_millis(self.cfg.clock_sync_interval_ms);
        self.clock.sync(&*self.store, &self.retry, interval, self.cfg.clock_sync_max_error_ms).await
    }

    /// Spawns a background clock synchronization task if the store clock is stale.
    pub(crate) fn refresh_clock(self: &Arc<Self>) {
        if self.clock.stale(Duration::from_millis(self.cfg.clock_sync_interval_ms)) {
            let me = self.clone();
            self.env.spawn(async move {
                if let Err(e) = me.sync_clock().await {
                    crate::debug!("store clock sync failed: {e}");
                }
            });
        }
    }

    /// Returns the local instant when this node first observed the lease renewal at
    /// `write_seq` for `db`.
    pub(crate) fn lease_seen(&self, db: &str, lease: &manifest::Lease, write_seq: u64) -> tokio::time::Instant {
        let now = tokio::time::Instant::now();
        let mut g = self.observed_leases.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((node, ws, seen)) = g.get(db)
            && *node == lease.node
            && *ws == write_seq
        {
            return *seen;
        }
        if g.len() >= 100_000 {
            let old = std::time::Duration::from_millis(100 * (self.cfg.read_lease_ms + self.cfg.lease_margin_ms));
            g.retain(|_, (_, _, t)| now.duration_since(*t) < old);
        }
        g.insert(db.to_string(), (lease.node.clone(), write_seq, now));
        now
    }
}

pub fn manifest_key(db: &str) -> String {
    format!("dbs/{db}/manifest")
}

pub fn seg_key(db: &str, seg: &str) -> String {
    format!("dbs/{db}/seg/{seg}")
}

/// Returns the storage key for an external levels object.
pub fn levels_key(db: &str, id: &str) -> String {
    format!("dbs/{db}/seg/{id}{LEVELS_SUFFIX}")
}

pub const LEVELS_SUFFIX: &str = ".levels";

pub fn blob_prefix(db: &str, blob: &str) -> String {
    format!("dbs/{db}/blob/{blob}/")
}

pub fn blob_chunk_key(db: &str, blob: &str, idx: u64) -> String {
    format!("dbs/{db}/blob/{blob}/{idx:08}")
}

/// Returns the storage key for a blob read pin (see [`blob::pin`]).
/// The pin is stored under the blob prefix so that garbage collection treats a pinned blob
/// as recently modified and deleting the blob also deletes its pin.
pub fn pin_key(db: &str, blob: &str) -> String {
    format!("{}pin", blob_prefix(db, blob))
}

/// Returns the prefix of every object of an event table part or mask.
pub fn part_prefix(db: &str, part: &str) -> String {
    format!("dbs/{db}/part/{part}/")
}

/// Returns the storage key of chunk `idx` of an event table part or mask.
pub fn part_chunk_key(db: &str, part: &str, idx: u64) -> String {
    format!("dbs/{db}/part/{part}/{idx:04}")
}

pub fn db_prefix(db: &str) -> String {
    format!("dbs/{db}/")
}

/// Object category in storage, derived from the object key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectClass {
    Manifest,
    Segment,
    Levels,
    Blob,
    Part,
    Other,
}

impl ObjectClass {
    pub const ALL: [ObjectClass; 6] = [
        ObjectClass::Manifest,
        ObjectClass::Segment,
        ObjectClass::Levels,
        ObjectClass::Blob,
        ObjectClass::Part,
        ObjectClass::Other,
    ];

    pub fn from_key(key: &str) -> ObjectClass {
        let Some((_, rest)) = key.strip_prefix("dbs/").and_then(|k| k.split_once('/')) else {
            return ObjectClass::Other;
        };
        if rest == "manifest" {
            ObjectClass::Manifest
        } else if let Some(seg) = rest.strip_prefix("seg/") {
            if seg.ends_with(LEVELS_SUFFIX) { ObjectClass::Levels } else { ObjectClass::Segment }
        } else if rest.starts_with("blob/") {
            ObjectClass::Blob
        } else if rest.starts_with("part/") {
            ObjectClass::Part
        } else {
            ObjectClass::Other
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            ObjectClass::Manifest => "manifest",
            ObjectClass::Segment => "segment",
            ObjectClass::Levels => "levels",
            ObjectClass::Blob => "blob",
            ObjectClass::Part => "part",
            ObjectClass::Other => "other",
        }
    }
}

/// Registry of open database handles on a node.
pub struct Engine {
    pub shared: Arc<Shared>,
    dbs: Mutex<DetHashMap<String, Arc<Db>>>,
}

impl Engine {
    pub fn new(store: DynStore, env: Arc<Env>, cfg: EngineConfig, node_id: String) -> Arc<Engine> {
        let metrics = Arc::new(crate::metrics::StoreMetrics::default());
        let memory = Arc::new(memory::Memory::new(cfg.memory_bytes));
        let shared = Arc::new(Shared {
            store: Arc::new(crate::timing::TimedStore { inner: store, metrics: metrics.clone() }),
            metrics,
            cache: cache::Cache::new(cfg.cache_bytes),
            compaction_slots: Semaphore::new(cfg.compaction_concurrency.max(1)),
            cfg,
            env,
            node_id,
            retry: Retry::default(),
            clock: clock::StoreClock::new(),
            observed_leases: Mutex::new(DetHashMap::default()),
            system: Mutex::new(Default::default()),
            memory,
            engine: std::sync::OnceLock::new(),
            unload_failed_at: Mutex::new(None),
            access_seq: 0.into(),
            stopping: false.into(),
            waits_ended: false.into(),
            commit_hook: std::sync::OnceLock::new(),
            event_hook: std::sync::OnceLock::new(),
        });
        let engine = Arc::new(Engine { shared, dbs: Mutex::new(DetHashMap::default()) });
        let _ = engine.shared.engine.set(Arc::downgrade(&engine));
        engine
    }

    pub fn env(&self) -> &Arc<Env> {
        &self.shared.env
    }

    /// Marks `id` as a system database, exempting it from size quotas, memory limits, and
    /// manifest unloading.
    pub fn mark_system(&self, id: &str) {
        self.shared.system.lock().unwrap_or_else(|e| e.into_inner()).insert(id.to_string());
    }

    /// Returns the handle for database `id`, creating an unloaded handle if not already open.
    pub fn db(&self, id: &str) -> Arc<Db> {
        let mut g = self.dbs.lock().unwrap_or_else(|e| e.into_inner());
        g.entry(id.to_string()).or_insert_with(|| Db::new(id.to_string(), self.shared.clone())).clone()
    }

    /// Returns the open database handle for `id` if present in memory.
    pub fn db_if_open(&self, id: &str) -> Option<Arc<Db>> {
        self.dbs.lock().unwrap_or_else(|e| e.into_inner()).get(id).cloned()
    }

    /// Sets the callback invoked after this node commits a new data version.
    pub fn set_commit_hook(&self, h: CommitHook) {
        let _ = self.shared.commit_hook.set(h);
    }

    /// Sets the callback that schedules event table maintenance.
    pub fn set_event_hook(&self, h: EventHook) {
        let _ = self.shared.event_hook.set(h);
    }

    pub fn open_count(&self) -> usize {
        self.dbs.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Wakes all current [`Db::wait_for_change`] calls and causes future calls to return immediately.
    pub fn end_waits(&self) {
        self.shared.waits_ended.store(true, std::sync::atomic::Ordering::SeqCst);
        for db in self.open_dbs() {
            db.wake_waiters();
        }
    }

    /// Returns currently open database handles sorted by ID.
    pub fn open_dbs(&self) -> Vec<Arc<Db>> {
        let g = self.dbs.lock().unwrap_or_else(|e| e.into_inner());
        let mut v: Vec<Arc<Db>> = g.values().cloned().collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        v
    }

    /// Releases leases for all open databases with a timeout of `per_db` per database, and
    /// prevents this node from acquiring new leases. Returns the IDs of databases whose leases
    /// were released.
    pub async fn release_leases(&self, per_db: Duration) -> Vec<String> {
        self.shared.stopping.store(true, std::sync::atomic::Ordering::SeqCst);
        let mut released = Vec::new();
        for db in self.open_dbs() {
            if let Ok(Ok(true)) = tokio::time::timeout(per_db, db.release_lease()).await {
                released.push(db.id.clone());
            }
        }
        released
    }

    /// Unloads least-recently-used idle databases until memory usage is at or below 7/8 of the
    /// budget, and removes unreferenced unloaded handles. If a pass fails to bring usage within
    /// the budget, subsequent passes are skipped for 50 ms.
    pub(crate) fn unload_idle(&self) {
        let shared = &self.shared;
        {
            let failed = shared.unload_failed_at.lock().unwrap_or_else(|e| e.into_inner());
            if failed.is_some_and(|t| t.elapsed() < Duration::from_millis(50)) {
                return;
            }
        }
        let target = shared.memory.limit() / 8 * 7;
        let mut candidates: Vec<(u64, Arc<Db>)> = {
            let g = self.dbs.lock().unwrap_or_else(|e| e.into_inner());
            g.values().filter(|d| !shared.is_system(&d.id)).map(|d| (d.last_access_seq(), d.clone())).collect()
        };
        candidates.sort_by_key(|(at, _)| *at);
        for (_, db) in &candidates {
            if shared.memory.used() <= target {
                break;
            }
            db.unload();
        }
        drop(candidates);
        self.dbs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, db| Arc::strong_count(db) > 1 || db.current().is_some());
        let mut failed = shared.unload_failed_at.lock().unwrap_or_else(|e| e.into_inner());
        *failed = shared.memory.is_exceeded().then(tokio::time::Instant::now);
    }

    /// Drops unreferenced handles that have been idle for `idle_ms`.
    pub fn evict_idle(&self, idle_ms: u64) {
        let mut g = self.dbs.lock().unwrap_or_else(|e| e.into_inner());
        let idle_ms = if crate::buggify::buggify() { 0 } else { idle_ms };
        g.retain(|_, db| Arc::strong_count(db) > 1 || !db.idle_for(idle_ms));
    }

    /// Removes the handle and cached entries for database `id`.
    pub fn forget(&self, id: &str) {
        self.dbs.lock().unwrap_or_else(|e| e.into_inner()).remove(id);
        self.shared.cache.evict_db(id);
    }

    /// Deletes all objects belonging to database `id` from storage.
    pub async fn purge(&self, id: &str) -> crate::error::Result<()> {
        self.forget(id);
        let (store, retry, prefix) = (&self.shared.store, &self.shared.retry, db_prefix(id));
        let mut after: Option<String> = None;
        loop {
            let page = retry.run(|| store.list(&prefix, after.as_deref(), 1000)).await?;
            for o in &page.objects {
                retry.run(|| store.delete(&o.key)).await?;
            }
            match page.next {
                Some(next) => after = Some(next),
                None => return Ok(()),
            }
        }
    }
}

#[cfg(test)]
mod memory_tests;
#[cfg(test)]
mod range_tests;
#[cfg(test)]
pub(crate) mod tests;
