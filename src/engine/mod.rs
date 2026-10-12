//! The storage engine: many small LSM databases on object storage, each
//! rooted at a CAS-updated manifest. See SPEC §5.

pub mod blob;
pub mod cache;
pub mod compaction;
mod config;
pub mod db;
pub mod manifest;
pub mod segment;
pub mod txn;
pub mod types;
pub mod version;

use crate::env::Env;
use crate::store::{DynStore, Retry};
use crate::util::DetHashMap;
use std::sync::{Arc, Mutex};
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
    /// Per database, the newest foreign lease this node has seen (holder,
    /// `expires_ms`) and when it first saw it. Kept beyond the lifetime of
    /// database handles, so a lease is never mistaken for a new one.
    pub(crate) lease_sightings: Mutex<DetHashMap<String, (String, u64, tokio::time::Instant)>>,
}

impl Shared {
    /// When this node first saw `lease` on database `db`.
    pub(crate) fn lease_seen(&self, db: &str, lease: &manifest::Lease) -> tokio::time::Instant {
        let now = tokio::time::Instant::now();
        let mut g = self.lease_sightings.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((node, exp, seen)) = g.get(db)
            && *node == lease.node
            && *exp == lease.expires_ms
        {
            return *seen;
        }
        if g.len() >= 100_000 {
            // Long-expired sightings: a lease renewed since has a new id.
            let old = std::time::Duration::from_millis(100 * (self.cfg.lease_ms + self.cfg.clock_skew_margin_ms));
            g.retain(|_, (_, _, t)| now.duration_since(*t) < old);
        }
        g.insert(db.to_string(), (lease.node.clone(), lease.expires_ms, now));
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

    /// Delete every object of a database. Returns the number deleted.
    pub async fn purge(&self, id: &str) -> crate::error::Result<usize> {
        self.forget(id);
        let objs = crate::store::list_all(&*self.shared.store, &db_prefix(id), &self.shared.retry).await?;
        let n = objs.len();
        for o in objs {
            self.shared.retry.run(|| self.shared.store.delete(&o.key)).await?;
        }
        Ok(n)
    }
}

#[cfg(test)]
pub(crate) mod tests;
