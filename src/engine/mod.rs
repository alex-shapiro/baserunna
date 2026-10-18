//! The storage engine: many small LSM databases on object storage, each
//! rooted at a CAS-updated manifest. See SPEC §5.

pub mod blob;
pub mod cache;
pub mod clock;
pub mod compaction;
mod config;
pub mod db;
pub mod local;
pub mod manifest;
pub mod range_delete;
pub mod segment;
pub mod stream;
pub mod txn;
pub mod types;
pub mod version;

use crate::env::Env;
use crate::store::{DynStore, Retry};
use crate::util::DetHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

pub use config::EngineConfig;
pub use db::{Committed, Consistency, Db, DbStats, LevelStats};
pub use stream::{Appended, Record, Retention, Seek, StreamOp, StreamState};
pub use txn::{CommitRequest, Isolation, Put, ReadItem, ReadValue, Ttl, Txn};
pub use types::{BlobRef, Entry, Seq, Value};
pub use version::{Change, ChangePage, ChangePos, KeyRange, Snapshot, Version};

macro_rules! activity {
    ($($(#[$doc:meta])* $field:ident),* $(,)?) => {
        /// Cumulative storage engine activity metrics.
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
        pub struct Activity {
            $($(#[$doc])* pub $field: u64,)*
        }

        impl Activity {
            pub fn add(&mut self, o: &Activity) {
                $(self.$field += o.$field;)*
            }
        }

        /// Thread-safe counters for tracking [`Activity`] metrics.
        #[derive(Default)]
        pub struct Counters {
            $($field: AtomicU64,)*
        }

        impl Counters {
            pub fn add(&self, a: &Activity) {
                $(if a.$field > 0 {
                    self.$field.fetch_add(a.$field, Ordering::Relaxed);
                })*
            }

            pub fn get(&self) -> Activity {
                Activity { $($field: self.$field.load(Ordering::Relaxed),)* }
            }
        }
    };
}

activity! {
    range_tombstones_written,
    range_tombstones_retired,
    /// Segments dropped entirely because all entries were covered by range tombstones.
    segments_dropped,
    /// Segments rewritten during compaction to purge range-deleted entries.
    segments_rewritten,
    levels_objects_written,
    quota_refusals,
    /// Entries dropped or reduced to tombstones because they expired.
    expired_entries,
    /// Segments rewritten or dropped because their entries expired.
    segments_expired,
    /// Stream chunks sealed as a flush wrote their records.
    chunks_sealed_at_flush,
    /// Stream chunks sealed as a compaction merged their records.
    chunks_sealed_in_compaction,
    /// Trims committed for stream retention.
    retention_trims,
    /// Foreign leases determined to have expired via the store clock.
    expired_leases,
    /// Transactions started while a manifest write was in flight on this node.
    optimistic_begins,
    /// Optimistic transactions re-executed after failing revalidation against a strong snapshot.
    optimistic_reruns,
    /// Manifest writes sent a second time because the store was slow to answer.
    manifest_puts_hedged,
}

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
    /// The store's clock, which every garbage collection timestamp uses.
    pub clock: clock::StoreClock,
    /// Newest foreign lease sighting `(holder, write_seq, first_seen)` per
    /// database, retained across `Db` handle evictions.
    pub(crate) lease_sightings: Mutex<DetHashMap<String, (String, u64, tokio::time::Instant)>>,
    /// Databases exempt from size quota limits.
    quota_exempt: Mutex<std::collections::BTreeSet<String>>,
    pub counters: Counters,
    pub metrics: Arc<crate::metrics::StoreMetrics>,
}

impl Shared {
    pub(crate) fn quota_exempt(&self, db: &str) -> bool {
        self.quota_exempt.lock().unwrap_or_else(|e| e.into_inner()).contains(db)
    }

    /// The store's current time, if this node measured its offset from the
    /// store recently enough for garbage collection to rely on it.
    pub fn store_now_ms(&self) -> Option<u64> {
        self.clock.now_ms(Duration::from_millis(self.cfg.clock_sync_max_age_ms))
    }

    /// Lower bound on the store's current time, if measured within the max sync age.
    pub fn store_earliest_now_ms(&self) -> Option<u64> {
        self.clock.earliest_now_ms(Duration::from_millis(self.cfg.clock_sync_max_age_ms))
    }

    /// Store time if known, or the local wall clock otherwise.
    pub fn time_ms(&self) -> u64 {
        self.store_now_ms().unwrap_or_else(|| self.env.now_ms())
    }

    /// The store's current time, measuring the offset first if it is due.
    pub async fn store_time_ms(&self) -> crate::error::Result<u64> {
        self.sync_clock().await?;
        self.store_now_ms().ok_or_else(|| crate::error::Error::unavailable("store clock not synchronized"))
    }

    async fn sync_clock(&self) -> crate::error::Result<()> {
        let interval = Duration::from_millis(self.cfg.clock_sync_interval_ms);
        self.clock.sync(&*self.store, &self.retry, interval, self.cfg.clock_sync_max_error_ms).await
    }

    /// Re-measure the store clock in the background if it is due.
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

    /// Returns the instant when this node first observed the lease renewal at
    /// `write_seq` for `db`.
    pub(crate) fn lease_seen(&self, db: &str, lease: &manifest::Lease, write_seq: u64) -> tokio::time::Instant {
        let now = tokio::time::Instant::now();
        let mut g = self.lease_sightings.lock().unwrap_or_else(|e| e.into_inner());
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

/// Storage key for a blob read pin; see [`blob::pin`]. Stored under the blob's
/// prefix so sweeps treat a pinned blob as recently modified and deleting the
/// blob removes its pin.
pub fn pin_key(db: &str, blob: &str) -> String {
    format!("{}pin", blob_prefix(db, blob))
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
    Other,
}

impl ObjectClass {
    pub const ALL: [ObjectClass; 5] =
        [ObjectClass::Manifest, ObjectClass::Segment, ObjectClass::Levels, ObjectClass::Blob, ObjectClass::Other];

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
            lease_sightings: Mutex::new(DetHashMap::default()),
            quota_exempt: Mutex::new(Default::default()),
            counters: Counters::default(),
        });
        Arc::new(Engine { shared, dbs: Mutex::new(DetHashMap::default()) })
    }

    pub fn env(&self) -> &Arc<Env> {
        &self.shared.env
    }

    /// Exempts a database from size quota enforcement.
    pub fn exempt_from_quota(&self, id: &str) {
        self.shared.quota_exempt.lock().unwrap_or_else(|e| e.into_inner()).insert(id.to_string());
    }

    /// Returns the handle for database `id`, creating an unloaded handle if not already open.
    pub fn db(&self, id: &str) -> Arc<Db> {
        let mut g = self.dbs.lock().unwrap_or_else(|e| e.into_inner());
        g.entry(id.to_string()).or_insert_with(|| Db::new(id.to_string(), self.shared.clone())).clone()
    }

    pub fn open_count(&self) -> usize {
        self.dbs.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Currently open database handles, sorted by ID.
    pub fn open_dbs(&self) -> Vec<Arc<Db>> {
        let g = self.dbs.lock().unwrap_or_else(|e| e.into_inner());
        let mut v: Vec<Arc<Db>> = g.values().cloned().collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        v
    }

    /// Drop handles that are idle for `idle_ms` and not in use.
    pub fn evict_idle(&self, idle_ms: u64) {
        let mut g = self.dbs.lock().unwrap_or_else(|e| e.into_inner());
        g.retain(|_, db| Arc::strong_count(db) > 1 || !db.idle_for(idle_ms));
    }

    /// Removes the handle and cached entries for database `id`.
    pub fn forget(&self, id: &str) {
        self.dbs.lock().unwrap_or_else(|e| e.into_inner()).remove(id);
        self.shared.cache.evict_prefix(&db_prefix(id));
    }

    /// Delete every object of a database, a page of the listing at a time.
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
mod range_tests;
#[cfg(test)]
pub(crate) mod tests;
