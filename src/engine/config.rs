//! Engine tuning: sizes, timeouts and graces (SPEC §5).

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
