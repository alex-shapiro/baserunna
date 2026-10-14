//! Engine tuning: sizes, timeouts and graces (SPEC §5).

use super::local::LocalTier;
use super::segment::Compression;

#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Flush the manifest tail to a segment above this size.
    pub tail_max_bytes: usize,
    /// Values above this size are stored out-of-band as blobs.
    pub value_inline_max: usize,
    pub block_target: usize,
    pub compression: Compression,
    /// Segments up to this size are fetched with a single GET.
    pub segment_whole_fetch_max: u64,
    /// Compactions split their output into segments of about this size.
    pub segment_target_bytes: usize,
    /// Compaction and sweep reads fetch consecutive blocks in requests of
    /// up to this many bytes.
    pub bulk_read_bytes: u64,
    /// Level 0 segments that trigger a compaction into the levels.
    pub l0_compaction_trigger: usize,
    /// Most level 0 segments one compaction merges.
    pub l0_compaction_max_inputs: usize,
    /// Size ratio between adjacent levels.
    pub level_size_ratio: u64,
    /// Smallest target size of the level that level 0 compacts into.
    pub level_base_bytes: u64,
    pub blob_chunk_size: usize,
    pub blob_max: u64,
    pub cache_bytes: usize,
    /// A node-local tier below the memory cache (local disk), if any.
    pub local_tier: Option<LocalTier>,
    /// How long a lease that a commit renews lasts, measured on each node's
    /// monotonic clock (SPEC §5.7). It bounds write failover.
    pub lease_ms: u64,
    /// How long a lease renewed for strong reads alone lasts. Longer than
    /// `lease_ms`, so each renewal (a PUT) covers more reads.
    pub read_lease_ms: u64,
    /// The holder stops relying on its lease this much early, and others
    /// wait this much longer, to absorb clock rate drift and scheduling.
    pub lease_margin_ms: u64,
    /// Strong reads per `read_lease_ms` that justify renewing a lease for
    /// reads alone (each renewal is a PUT).
    pub lease_read_threshold: u32,
    pub txn_max_lifetime_ms: u64,
    pub txn_max_write_bytes: usize,
    pub gc_grace_ms: u64,
    pub orphan_grace_ms: u64,
    pub blob_stage_ttl_ms: u64,
    pub sweep_interval_ms: u64,
    /// How often a node re-measures its offset from the store's clock.
    pub clock_sync_interval_ms: u64,
    /// A store clock measurement is trusted for this long; without one, a
    /// node does no garbage collection.
    pub clock_sync_max_age_ms: u64,
    /// Measurements less precise than this are discarded.
    pub clock_sync_max_error_ms: u64,
    pub commit_ring_max: usize,
    pub compaction_concurrency: usize,
    pub commit_retry_max: u32,
    /// Maximum time a commit may wait in the committer before it is
    /// refused (bounds server-side "zombie" requests).
    pub commit_timeout_ms: u64,
    pub commit_batch_max: usize,
    pub version_sample_ms: u64,
    /// Maximum stored entries in a deleted range before converting to a range tombstone.
    pub range_delete_points_max: usize,
    /// Maximum segments below L0 stored inline in the manifest before offloading to a levels object.
    pub levels_inline_max: usize,
    /// Maximum storage size allowed per database (segments, tail, and blobs), or 0 for unlimited.
    pub db_max_bytes: u64,
    /// Minimum interval before triggering a blob-recounting sweep after a quota refusal.
    pub quota_sweep_interval_ms: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            tail_max_bytes: 256 * 1024,
            value_inline_max: 32 * 1024,
            block_target: 64 * 1024,
            compression: Compression::Zstd(3),
            segment_whole_fetch_max: 1024 * 1024,
            segment_target_bytes: 2 * 1024 * 1024,
            bulk_read_bytes: 1024 * 1024,
            l0_compaction_trigger: 4,
            l0_compaction_max_inputs: 12,
            level_size_ratio: 8,
            level_base_bytes: 1024 * 1024,
            blob_chunk_size: 8 * 1024 * 1024,
            blob_max: 5 * 1024 * 1024 * 1024,
            cache_bytes: 512 * 1024 * 1024,
            local_tier: None,
            lease_ms: 5_000,
            read_lease_ms: 15_000,
            lease_margin_ms: 500,
            lease_read_threshold: 16,
            txn_max_lifetime_ms: 5 * 60_000,
            txn_max_write_bytes: 16 * 1024 * 1024,
            gc_grace_ms: 30 * 60_000,
            orphan_grace_ms: 2 * 3_600_000,
            blob_stage_ttl_ms: 3_600_000,
            sweep_interval_ms: 3_600_000,
            clock_sync_interval_ms: 10 * 60_000,
            clock_sync_max_age_ms: 3_600_000,
            clock_sync_max_error_ms: 10_000,
            commit_ring_max: 1024,
            compaction_concurrency: 2,
            commit_retry_max: 8,
            commit_timeout_ms: 10_000,
            commit_batch_max: 256,
            version_sample_ms: 30_000,
            range_delete_points_max: 256,
            levels_inline_max: 16,
            db_max_bytes: 0,
            quota_sweep_interval_ms: 60_000,
        }
    }
}

impl EngineConfig {
    /// Check the relations between settings that safety depends on.
    pub fn validate(&self) -> Result<(), String> {
        let rules = [
            (
                self.gc_grace_ms >= self.txn_max_lifetime_ms + 2 * self.clock_sync_max_error_ms,
                "gc_grace_ms must exceed txn_max_lifetime_ms by twice clock_sync_max_error_ms",
            ),
            (self.lease_margin_ms < self.lease_ms, "lease_margin_ms must be below lease_ms"),
            (self.read_lease_ms >= self.lease_ms, "read_lease_ms must be at least lease_ms"),
            (
                self.clock_sync_interval_ms < self.clock_sync_max_age_ms,
                "clock_sync_interval_ms must be below clock_sync_max_age_ms",
            ),
            (self.level_size_ratio >= 2, "level_size_ratio must be at least 2"),
        ];
        match rules.iter().find(|(ok, _)| !ok) {
            Some((_, why)) => Err((*why).to_string()),
            None => Ok(()),
        }
    }

    /// Small sizes that exercise every code path quickly (simulation).
    pub fn small_for_tests() -> EngineConfig {
        EngineConfig {
            tail_max_bytes: 2 * 1024,
            value_inline_max: 512,
            block_target: 512,
            compression: Compression::Zstd(1),
            segment_whole_fetch_max: 4 * 1024,
            segment_target_bytes: 6 * 1024,
            bulk_read_bytes: 2 * 1024,
            level_size_ratio: 4,
            level_base_bytes: 8 * 1024,
            blob_chunk_size: 1024,
            blob_max: 1024 * 1024,
            cache_bytes: 256 * 1024,
            range_delete_points_max: 4,
            levels_inline_max: 2,
            ..EngineConfig::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_configurations_are_valid() {
        EngineConfig::default().validate().unwrap();
        EngineConfig::small_for_tests().validate().unwrap();
        let tight = EngineConfig { gc_grace_ms: 5 * 60_000, ..EngineConfig::default() };
        assert!(tight.validate().is_err(), "no room for clock error");
    }
}
