//! Attacker workload definitions.
//!
//! Implement [`Workload`] in a submodule and register it in [`by_name`] and [`NAMES`].

mod blob_checks;
mod blob_downloads;
mod bulk_inserts;
mod long_strings;
mod merges;
mod stream_filter_scans;
mod table_filter_scans;
mod txn_scans;

use crate::http::{NodeApi, Owner, RequestRecord};
use futures_util::future::BoxFuture;
use serde::Serialize;
use serde_json::Value as Json;
use std::sync::Arc;

/// Workload scale. A healthy node must complete the [`Variant::Small`] variant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Variant {
    Small,
    Attack,
}

impl Variant {
    pub fn parse(text: &str) -> Option<Variant> {
        match text {
            "small" => Some(Variant::Small),
            "attack" => Some(Variant::Attack),
            _ => None,
        }
    }
}

/// Execution phase for an attack round: alone in phase B, or alongside the victim in phase C.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Round {
    Alone,
    WithVictim,
}

impl Round {
    pub fn index(self) -> usize {
        match self {
            Round::Alone => 0,
            Round::WithVictim => 1,
        }
    }
}

/// Shared state for running attacker requests against the node.
pub struct AttackContext {
    pub api: Arc<NodeApi>,
    pub attacker: Owner,
    pub variant: Variant,
    pub cgroup: crate::cgroup::NodeCgroup,
}

/// Recorded requests and completion status for an attack round.
#[derive(Debug, Default, Serialize)]
pub struct AttackOutcome {
    /// All attacker requests sent during the round.
    pub requests: Vec<RequestRecord>,
    /// True if background work started by the round finished before the timeout.
    pub completed: bool,
    /// Workload-specific measurements.
    pub notes: Json,
}

/// Attacker workload whose resource cost the node must bound per tenant.
pub trait Workload: Send + Sync {
    /// Creates attacker data for `round` before any measured phase begins.
    fn prepare<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>>;

    /// Sends attack requests for `round` and waits for triggered background work to finish.
    fn attack<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>>;
}

pub const NAMES: &[&str] = &[
    "long_strings",
    "bulk_inserts",
    "merges",
    "blob_checks",
    "txn_scans",
    "table_filter_scans",
    "blob_downloads",
    "stream_filter_scans",
];

pub fn by_name(name: &str) -> Option<Box<dyn Workload>> {
    Some(match name {
        "long_strings" => Box::new(long_strings::LongStrings),
        "bulk_inserts" => Box::new(bulk_inserts::BulkInserts),
        "merges" => Box::new(merges::Merges),
        "blob_checks" => Box::new(blob_checks::BlobChecks),
        "txn_scans" => Box::new(txn_scans::TxnScans),
        "table_filter_scans" => Box::new(table_filter_scans::TableFilterScans),
        "blob_downloads" => Box::new(blob_downloads::BlobDownloads),
        "stream_filter_scans" => Box::new(stream_filter_scans::StreamFilterScans),
        _ => return None,
    })
}

/// Sends all `(label, path, body)` POST requests concurrently and returns their records.
pub async fn concurrent_posts(
    ctx: &AttackContext,
    requests: Vec<(String, String, bytes::Bytes)>,
    expected_statuses: &'static [u16],
) -> Vec<RequestRecord> {
    let tasks: Vec<_> = requests
        .into_iter()
        .map(|(label, path, body)| {
            let (api, token) = (ctx.api.clone(), ctx.attacker.token.clone());
            tokio::spawn(async move {
                api.send_timed(&label, reqwest::Method::POST, &path, &token, body, expected_statuses).await
            })
        })
        .collect();
    let mut records = Vec::new();
    for task in tasks {
        records.push(task.await.expect("attack request"));
    }
    records
}
