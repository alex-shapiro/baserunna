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
pub mod segment;
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
pub use db::{Committed, Consistency, Db};
pub use txn::{CommitRequest, Isolation, ReadItem, ReadValue, Txn};
pub use types::{BlobRef, Entry, Seq, Value};
pub use version::{KeyRange, Version};

/// State shared by every database handle on a node.
pub struct Shared {
    pub store: DynStore,
    pub env: Arc<Env>,
    pub cfg: EngineConfig,
    pub cache: cache::Cache,
    /// Lease identity of this process (unique per start).
    pub node_id: String,
    pub retry: Retry,
    pub compaction_slots: Semaphore,
    /// The store's clock, which every garbage collection timestamp uses.
    pub clock: clock::StoreClock,
    /// Per database, the newest foreign lease renewal this node has seen
    /// (holder, manifest `write_seq`) and when it first saw it. Kept beyond
    /// the lifetime of database handles, so a lease is never mistaken for a
    /// new one.
    pub(crate) lease_sightings: Mutex<DetHashMap<String, (String, u64, tokio::time::Instant)>>,
}

impl Shared {
    /// The store's current time, if this node measured its offset from the
    /// store recently enough for garbage collection to rely on it.
    pub fn store_now_ms(&self) -> Option<u64> {
        self.clock.now_ms(Duration::from_millis(self.cfg.clock_sync_max_age_ms))
    }

    /// Store time if known, else the local wall clock: for timestamps that
    /// only inform (creation times, blob ids).
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

    /// When this node first saw the lease renewal made by the manifest
    /// write `write_seq` of database `db`. Every manifest write renews its
    /// writer's lease, so the write identifies the renewal; `expires_ms`
    /// does not, as renewals of different durations can share it.
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
            // Long-expired sightings: a lease renewed since has a new id.
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

pub fn blob_prefix(db: &str, blob: &str) -> String {
    format!("dbs/{db}/blob/{blob}/")
}

pub fn blob_chunk_key(db: &str, blob: &str, idx: u64) -> String {
    format!("dbs/{db}/blob/{blob}/{idx:08}")
}

/// Marks a blob as being read (see [`blob::pin`]). It lives with the
/// blob's chunks, so a pinned blob looks recently written to sweeps, and
/// deleting the blob deletes it.
pub fn pin_key(db: &str, blob: &str) -> String {
    format!("{}pin", blob_prefix(db, blob))
}

pub fn db_prefix(db: &str) -> String {
    format!("dbs/{db}/")
}

/// Registry of open database handles on a node.
pub struct Engine {
    pub shared: Arc<Shared>,
    dbs: Mutex<DetHashMap<String, Arc<Db>>>,
}

impl Engine {
    pub fn new(store: DynStore, env: Arc<Env>, cfg: EngineConfig, node_id: String) -> Arc<Engine> {
        let shared = Arc::new(Shared {
            store,
            cache: cache::Cache::new(cfg.cache_bytes),
            compaction_slots: Semaphore::new(cfg.compaction_concurrency.max(1)),
            cfg,
            env,
            node_id,
            retry: Retry::default(),
            clock: clock::StoreClock::new(),
            lease_sightings: Mutex::new(DetHashMap::default()),
        });
        Arc::new(Engine { shared, dbs: Mutex::new(DetHashMap::default()) })
    }

    pub fn env(&self) -> &Arc<Env> {
        &self.shared.env
    }

    /// Handle for a database (not loaded until first use).
    pub fn db(&self, id: &str) -> Arc<Db> {
        let mut g = self.dbs.lock().unwrap_or_else(|e| e.into_inner());
        g.entry(id.to_string()).or_insert_with(|| Db::new(id.to_string(), self.shared.clone())).clone()
    }

    pub fn open_count(&self) -> usize {
        self.dbs.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Handles currently open (for lease release, metrics).
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

    /// Forget a database (after deletion).
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
pub(crate) mod tests;
