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
    /// Memory budget in bytes for open database manifests and active transactions.
    pub memory_bytes: usize,
    /// Node-local cache tier below the memory cache.
    pub local_tier: Option<LocalTier>,
    /// Duration of a lease renewed by a commit, measured on each node's
    /// monotonic clock (SPEC §5.7).
    pub lease_ms: u64,
    /// Duration of a lease renewed for strong reads alone.
    pub read_lease_ms: u64,
    /// Safety margin subtracted by the lease holder and added by other nodes
    /// to absorb clock rate drift and scheduling delays.
    pub lease_margin_ms: u64,
    /// Strong reads per `read_lease_ms` required to renew a lease for reads alone.
    pub lease_read_threshold: u32,
    pub txn_max_lifetime_ms: u64,
    pub txn_max_write_bytes: usize,
    pub txn_max_read_bytes: usize,
    pub gc_grace_ms: u64,
    pub orphan_grace_ms: u64,
    pub blob_stage_ttl_ms: u64,
    pub sweep_interval_ms: u64,
    /// How often a node re-measures its offset from the store's clock.
    pub clock_sync_interval_ms: u64,
    /// Maximum age of a store clock measurement before garbage collection pauses.
    pub clock_sync_max_age_ms: u64,
    /// Measurements less precise than this are discarded.
    pub clock_sync_max_error_ms: u64,
    /// Retention period for commit records, used for idempotent retries and
    /// resolving ambiguous commit outcomes.
    pub commit_retention_ms: u64,
    pub compaction_concurrency: usize,
    pub commit_retry_max: u32,
    /// Delay in milliseconds before sending a duplicate manifest `PUT` (SPEC §5.5), or 0 to disable hedging.
    pub manifest_put_hedge_ms: u64,
    /// Maximum time a commit may wait in the committer before it is rejected.
    pub commit_timeout_ms: u64,
    pub commit_batch_max: usize,
    pub version_sample_ms: u64,
    /// Maximum stored entries in a deleted range before converting to a range tombstone.
    pub range_delete_points_max: usize,
    /// Maximum segments below L0 stored inline in the manifest before offloading to a levels object.
    pub levels_inline_max: usize,
    /// Maximum storage size in bytes per database, or 0 for unlimited.
    pub db_max_bytes: u64,
    /// Minimum interval before triggering a blob-recounting sweep after a quota rejection.
    pub quota_sweep_interval_ms: u64,
    /// Runs of one stream's records at least this large are sealed into a
    /// chunk as a flush or a compaction writes them (SPEC §5.11).
    pub stream_seal_min_bytes: usize,
    /// Sealed chunks are cut at this size.
    pub stream_chunk_bytes: usize,
    /// Largest stream record payload.
    pub stream_record_max: usize,
    /// Event part bytes allowed per database (SPEC §5.10), or 0 for unlimited.
    pub event_max_bytes: u64,
    /// A table's unsealed rows are sealed into parts once they reach this
    /// many bytes (SPEC §5.13).
    pub event_seal_bytes: usize,
    /// Event parts are stored as objects of this many bytes.
    pub event_chunk_bytes: usize,
    /// Most rows in a granule, the unit of pruning and reading.
    pub event_granule_rows: usize,
    /// A granule ends early once its uncompressed bytes reach this.
    pub event_granule_bytes: usize,
    /// A column block, the unit of compression, holds whole granules up to
    /// about this many uncompressed bytes.
    pub event_block_bytes: usize,
    /// Most bytes of rows sorted in memory and written as one staged or sealed part.
    pub event_insert_part_max_bytes: usize,
    /// Parts of one tier merged at a time, and the ratio between tiers.
    pub event_merge_fan_in: usize,
    /// Raw size of the parts in the lowest merge tier.
    pub event_tier_base_bytes: u64,
    /// Parts at or above this stored size are not merged further.
    pub event_part_max_bytes: u64,
    /// Merges running longer than this, or than the stage TTL allows, are abandoned.
    pub event_merge_max_ms: u64,
    /// Partitions whose time range has ended and that received no new part
    /// for this long are merged into as few parts as the size cap allows.
    pub event_settle_ms: u64,
    /// JSON paths stored as their own columns in each part; the rest share buckets.
    pub event_json_max_paths: usize,
    /// Bytes of shared JSON path keys a part's metadata lists; past it, the
    /// most common are listed and readers check the buckets for the rest.
    pub event_json_directory_bytes: usize,
    /// Parts a query or delete reads at once.
    pub event_read_concurrency: usize,
    /// An event part `GET` unanswered after this long is sent again, and the
    /// first answer wins; 0 disables hedging.
    pub event_get_hedge_ms: u64,
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
            memory_bytes: 256 * 1024 * 1024,
            local_tier: None,
            lease_ms: 5_000,
            read_lease_ms: 15_000,
            lease_margin_ms: 500,
            lease_read_threshold: 16,
            txn_max_lifetime_ms: 5 * 60_000,
            txn_max_write_bytes: 16 * 1024 * 1024,
            txn_max_read_bytes: 16 * 1024 * 1024,
            gc_grace_ms: 30 * 60_000,
            orphan_grace_ms: 2 * 3_600_000,
            blob_stage_ttl_ms: 3_600_000,
            sweep_interval_ms: 3_600_000,
            clock_sync_interval_ms: 10 * 60_000,
            clock_sync_max_age_ms: 3_600_000,
            clock_sync_max_error_ms: 10_000,
            commit_retention_ms: 10 * 60_000,
            compaction_concurrency: 2,
            commit_retry_max: 8,
            commit_timeout_ms: 10_000,
            commit_batch_max: 256,
            version_sample_ms: 30_000,
            range_delete_points_max: 256,
            levels_inline_max: 16,
            manifest_put_hedge_ms: 250,
            db_max_bytes: 0,
            quota_sweep_interval_ms: 60_000,
            stream_seal_min_bytes: 64 * 1024,
            stream_chunk_bytes: 2 * 1024 * 1024,
            stream_record_max: 1024 * 1024,
            event_max_bytes: 1 << 40,
            event_seal_bytes: 128 * 1024,
            event_chunk_bytes: 32 * 1024 * 1024,
            event_granule_rows: 8192,
            event_granule_bytes: 10 * 1024 * 1024,
            event_block_bytes: 64 * 1024,
            event_insert_part_max_bytes: 64 * 1024 * 1024,
            event_merge_fan_in: 8,
            event_tier_base_bytes: 4 * 1024 * 1024,
            event_part_max_bytes: 4 << 30,
            event_merge_max_ms: 30 * 60_000,
            event_settle_ms: 24 * 3_600_000,
            event_json_max_paths: 1024,
            event_json_directory_bytes: 1 << 20,
            event_read_concurrency: 32,
            event_get_hedge_ms: 1_000,
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
            (self.memory_bytes >= 64 * self.tail_max_bytes, "memory_bytes must be at least 64 * tail_max_bytes"),
            (
                self.commit_retention_ms >= 2 * self.commit_timeout_ms,
                "commit_retention_ms must leave time to resolve commits after commit_timeout_ms",
            ),
            (
                self.stream_seal_min_bytes <= self.stream_chunk_bytes,
                "stream_seal_min_bytes must not exceed stream_chunk_bytes",
            ),
            (self.event_merge_fan_in >= 2, "event_merge_fan_in must be at least 2"),
            (self.event_read_concurrency >= 1, "event_read_concurrency must be at least 1"),
            (self.event_granule_rows >= 1 && self.event_chunk_bytes >= 1, "event granules and chunks are not empty"),
        ];
        match rules.iter().find(|(ok, _)| !ok) {
            Some((_, why)) => Err((*why).to_string()),
            None => Ok(()),
        }
    }

    /// Small sizes that exercise every code path quickly in tests.
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
            stream_seal_min_bytes: 1024,
            stream_chunk_bytes: 4 * 1024,
            stream_record_max: 8 * 1024,
            event_seal_bytes: 1024,
            event_chunk_bytes: 2 * 1024,
            event_granule_rows: 16,
            event_granule_bytes: 2 * 1024,
            event_block_bytes: 512,
            event_insert_part_max_bytes: 16 * 1024,
            event_merge_fan_in: 3,
            event_tier_base_bytes: 1024,
            event_part_max_bytes: 64 * 1024,
            event_json_max_paths: 4,
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
        assert!(tight.validate().is_err());
        let starved = EngineConfig { memory_bytes: 8 * 1024 * 1024, ..EngineConfig::default() };
        assert!(starved.validate().is_err());
    }
}
