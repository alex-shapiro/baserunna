//! Inputs of event work that the node runs in its WebAssembly sandbox.
//!
//! The host and the guest share these types, so both sides encode them the
//! same way.

use super::schema::EventSchema;
use crate::engine::EngineConfig;
use crate::engine::events::PartRef;
use crate::error::Result;
use crate::store::BoxFut;
use serde::{Deserialize, Serialize};

/// Engine settings that event part code reads.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventConfig {
    blob_chunk_size: usize,
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
            blob_chunk_size,
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

/// Input of an event query in the sandbox.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryInput {
    pub db: String,
    pub schema: EventSchema,
    /// The query body, which the guest parses with `Query::parse`.
    pub query: serde_json::Value,
    pub limits: super::exec::Limits,
    /// Encoded manifest of the host transaction's snapshot version.
    pub manifest: Vec<u8>,
    pub etag: String,
    pub read_time: u64,
    pub config: EventConfig,
    pub seed: u64,
    pub now_ms: u64,
}

/// Output of an event query in the sandbox.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryOutput {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub next_cursor: Option<String>,
    pub stats: super::exec::Stats,
}

/// Header of a part build in the sandbox. Encoded rows follow it in guest
/// memory as `(version u64, ordinal u32, len u32, bytes)` records.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WriteInput {
    pub db: String,
    pub schema: EventSchema,
    pub partition: u32,
    pub versions: bool,
    pub config: EventConfig,
    pub seed: u64,
    pub now_ms: u64,
}

/// One part that a sandboxed part build wrote: key, encoded reference, and
/// `(version, ordinal)` row positions.
pub type WrittenPart = (Vec<u8>, Vec<u8>, Vec<(u64, u32)>);

/// Header of a blob value check in the sandbox. The expected value follows it
/// in guest memory.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckInput {
    pub db: String,
    /// Encoded `BlobRef` of the stored value.
    pub blob: Vec<u8>,
    pub config: EventConfig,
    pub seed: u64,
    pub now_ms: u64,
}

/// Runs event work in the node's WebAssembly sandbox, charged to the database owner.
pub trait SandboxRunner: Send + Sync {
    /// Runs `maint::merge_parts` over `inputs` in the sandbox.
    fn merge_parts<'a>(
        &'a self,
        db: &'a str,
        schema: &'a EventSchema,
        inputs: &'a [PartRef],
    ) -> BoxFut<'a, Result<Option<PartRef>>>;

    /// Reads the blob value `blob` in the sandbox and returns whether it equals `want`.
    fn blob_equals<'a>(
        &'a self,
        db: &'a str,
        blob: &'a crate::engine::types::BlobRef,
        want: &'a [u8],
    ) -> BoxFut<'a, Result<bool>>;

    /// Runs `build::write_encoded` for one partition's encoded rows in the sandbox.
    fn write_encoded<'a>(
        &'a self,
        db: &'a str,
        schema: &'a EventSchema,
        partition: u32,
        rows: Vec<(crate::engine::events::Pos, &'a [u8])>,
        versions: bool,
        out: &'a mut Vec<(PartRef, Vec<crate::engine::events::Pos>)>,
    ) -> BoxFut<'a, Result<()>>;

    /// Runs `exec::run` for `query` in the sandbox, at the snapshot of `txn`.
    fn query_events<'a>(
        &'a self,
        db: &'a str,
        txn: &'a crate::engine::Txn,
        schema: &'a EventSchema,
        query: &'a serde_json::Value,
        limits: &'a super::exec::Limits,
    ) -> BoxFut<'a, Result<super::exec::Output>>;
}
