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
use std::time::{Duration, Instant};

/// Duration of a [`request_loop`] attack round, so the attack overlaps with victim requests.
const ATTACK_DURATION: Duration = Duration::from_secs(10);

/// Attacker workload whose resource cost the node must limit per tenant.
pub trait Workload: Send + Sync {
    /// Creates attacker data for `round` before the harness starts the measured phases.
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
    /// True if background work from this round finished before the timeout.
    pub completed: bool,
    /// Workload-specific measurements.
    pub notes: Json,
}

/// Workload scale. A healthy node must complete [`Variant::Small`].
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

/// Sends requests from concurrent attacker clients until the round ends.
///
/// The small variant runs 1 client that sends `small_requests` requests. The attack variant runs
/// `attack_clients` clients that send requests for [`ATTACK_DURATION`]. A client stops after a
/// dropped connection.
pub async fn request_loop<F, Fut>(
    ctx: &AttackContext,
    small_requests: usize,
    attack_clients: usize,
    send_request: F,
) -> Result<Vec<RequestRecord>, String>
where
    F: Fn(Arc<NodeApi>, String, usize) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = RequestRecord> + Send,
{
    let (client_count, deadline, max_requests) = match ctx.variant {
        Variant::Small => (1, None, small_requests),
        Variant::Attack => (attack_clients, Some(Instant::now() + ATTACK_DURATION), usize::MAX),
    };
    let send_request = Arc::new(send_request);
    let tasks: Vec<_> = (0..client_count)
        .map(|client_index| {
            let (api, token, send_request) = (ctx.api.clone(), ctx.attacker.token.clone(), send_request.clone());
            tokio::spawn(async move {
                let mut records = Vec::new();
                while records.len() < max_requests && deadline.is_none_or(|end| Instant::now() < end) {
                    let record = send_request(api.clone(), token.clone(), client_index).await;
                    let dropped = record.status.is_none();
                    records.push(record);
                    if dropped {
                        break;
                    }
                }
                records
            })
        })
        .collect();
    let mut requests = Vec::new();
    for task in tasks {
        requests.extend(task.await.map_err(|err| err.to_string())?);
    }
    Ok(requests)
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
