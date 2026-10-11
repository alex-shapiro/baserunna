//! The storage engine: many small LSM databases on object storage, each
//! rooted at a CAS-updated manifest. See SPEC §5.

pub mod blob;
pub mod cache;
pub mod compaction;
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

pub use db::{Committed, Consistency, Db};
pub use txn::{CommitRequest, Isolation, ReadItem, ReadValue, Txn};
pub use types::{BlobRef, Entry, Seq, Value};
pub use version::{KeyRange, Version};

#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Flush the manifest tail to a segment above this size.
    pub tail_max_bytes: usize,
    /// Values above this size are stored out-of-band as blobs.
    pub value_inline_max: usize,
    pub block_target: usize,
    /// Segments up to this size are fetched with a single GET.
    pub segment_whole_fetch_max: u64,
    pub blob_chunk_size: usize,
    pub blob_max: u64,
    pub cache_bytes: usize,
    pub lease_ms: u64,
    pub clock_skew_margin_ms: u64,
    /// Strong reads per lease period that justify holding a lease.
    pub lease_read_threshold: u32,
    pub txn_max_lifetime_ms: u64,
    pub txn_max_write_bytes: usize,
    pub gc_grace_ms: u64,
    pub orphan_grace_ms: u64,
    pub blob_stage_ttl_ms: u64,
    pub sweep_interval_ms: u64,
    pub commit_ring_max: usize,
    pub commit_ring_age_ms: u64,
    pub compaction_fanout: usize,
    pub max_segments: usize,
    pub compaction_concurrency: usize,
    pub commit_retry_max: u32,
    /// Maximum time a commit may wait in the committer before it is
    /// refused (bounds server-side "zombie" requests).
    pub commit_timeout_ms: u64,
    pub commit_batch_max: usize,
    pub version_sample_ms: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            tail_max_bytes: 256 * 1024,
            value_inline_max: 32 * 1024,
            block_target: 64 * 1024,
            segment_whole_fetch_max: 1024 * 1024,
            blob_chunk_size: 8 * 1024 * 1024,
            blob_max: 5 * 1024 * 1024 * 1024,
            cache_bytes: 512 * 1024 * 1024,
            lease_ms: 15_000,
            clock_skew_margin_ms: 1_000,
            lease_read_threshold: 16,
            txn_max_lifetime_ms: 5 * 60_000,
            txn_max_write_bytes: 16 * 1024 * 1024,
            gc_grace_ms: 30 * 60_000,
            orphan_grace_ms: 2 * 3_600_000,
            blob_stage_ttl_ms: 3_600_000,
            sweep_interval_ms: 3_600_000,
            commit_ring_max: 1024,
            commit_ring_age_ms: 15 * 60_000,
            compaction_fanout: 4,
            max_segments: 12,
            compaction_concurrency: 2,
            commit_retry_max: 8,
            commit_timeout_ms: 10_000,
            commit_batch_max: 256,
            version_sample_ms: 30_000,
        }
    }
}

impl EngineConfig {
    /// Small sizes that exercise every code path quickly (simulation).
    pub fn small_for_tests() -> EngineConfig {
        EngineConfig {
            tail_max_bytes: 2 * 1024,
            value_inline_max: 512,
            block_target: 512,
            segment_whole_fetch_max: 4 * 1024,
            blob_chunk_size: 1024,
            blob_max: 1024 * 1024,
            cache_bytes: 256 * 1024,
            ..EngineConfig::default()
        }
    }
}

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

/// Valid database ids: 1-64 chars of `[a-z0-9_-]`.
pub fn valid_db_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
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
        });
        Arc::new(Engine { shared, dbs: Mutex::new(DetHashMap::default()) })
    }

    pub fn node_id(&self) -> &str {
        &self.shared.node_id
    }

    pub fn cfg(&self) -> &EngineConfig {
        &self.shared.cfg
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
    pub fn evict_idle(&self, idle_ms: u64) -> usize {
        let mut g = self.dbs.lock().unwrap_or_else(|e| e.into_inner());
        let before = g.len();
        g.retain(|_, db| Arc::strong_count(db) > 1 || !db.idle_for(idle_ms));
        before - g.len()
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
