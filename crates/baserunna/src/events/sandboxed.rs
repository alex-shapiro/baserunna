//! Inputs of event work that the node runs in its WebAssembly sandbox.
//!
//! The host and the guest share these types, so both sides encode them the
//! same way.

use super::schema::EventSchema;
use crate::engine::EngineConfig;
use serde::{Deserialize, Serialize};

/// Engine settings that event part code reads.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventConfig {
    blob_stage_ttl_ms: u64,
    clock_sync_max_error_ms: u64,
    event_block_bytes: usize,
    event_chunk_bytes: usize,
    event_get_hedge_ms: u64,
    event_granule_bytes: usize,
    event_granule_rows: usize,
    event_insert_part_max_bytes: usize,
    event_json_directory_bytes: usize,
    event_json_max_paths: usize,
    event_mask_inline_bytes: usize,
    event_merge_max_ms: u64,
    event_part_max_bytes: u64,
    event_read_concurrency: usize,
    segment_whole_fetch_max: u64,
    txn_max_write_bytes: usize,
}

macro_rules! event_config_fields {
    ($m:ident) => {
        $m!(
            blob_stage_ttl_ms,
            clock_sync_max_error_ms,
            event_block_bytes,
            event_chunk_bytes,
            event_get_hedge_ms,
            event_granule_bytes,
            event_granule_rows,
            event_insert_part_max_bytes,
            event_json_directory_bytes,
            event_json_max_paths,
            event_mask_inline_bytes,
            event_merge_max_ms,
            event_part_max_bytes,
            event_read_concurrency,
            segment_whole_fetch_max,
            txn_max_write_bytes
        )
    };
}

impl EventConfig {
    pub fn of(cfg: &EngineConfig) -> EventConfig {
        macro_rules! copy_out {
            ($($f:ident),*) => { EventConfig { $($f: cfg.$f),* } };
        }
        event_config_fields!(copy_out)
    }

    /// Returns the default engine settings with these event settings.
    pub fn engine_config(&self) -> EngineConfig {
        let mut cfg = EngineConfig::default();
        macro_rules! copy_in {
            ($($f:ident),*) => { $(cfg.$f = self.$f;)* };
        }
        event_config_fields!(copy_in);
        cfg
    }
}

/// Input of a part merge in the sandbox.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MergeInput {
    pub db: String,
    pub schema: EventSchema,
    /// Part reference keys and encodings.
    pub inputs: Vec<(Vec<u8>, Vec<u8>)>,
    pub config: EventConfig,
    pub seed: u64,
    pub now_ms: u64,
}
