//! Attacker workloads. Each workload is one module that implements [`Workload`].
//!
//! To add a workload, write a module with a type that implements [`Workload`] and add it to
//! [`by_name`] and [`NAMES`].

mod blob_checks;
mod bulk_inserts;
mod long_strings;
mod merges;

use crate::http::{Api, Owner, RequestRecord};
use futures_util::future::BoxFuture;
use serde::Serialize;
use serde_json::Value as Json;
use std::sync::Arc;

/// Size of a workload run: the small variant must succeed on a correct node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Variant {
    Small,
    Attack,
}

impl Variant {
    pub fn parse(s: &str) -> Option<Variant> {
        match s {
            "small" => Some(Variant::Small),
            "attack" => Some(Variant::Attack),
            _ => None,
        }
    }
}

/// Attack round: phase B runs the attacker alone, and phase C runs it with the victim.
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

/// Context that a workload uses to reach the node as the attacker.
pub struct AttackContext {
    pub api: Arc<Api>,
    pub attacker: Owner,
    pub variant: Variant,
    pub cgroup: crate::cgroup::NodeCgroup,
}

/// Result of one attack round.
#[derive(Debug, Default, Serialize)]
pub struct AttackOutcome {
    /// Every attacker request of the round.
    pub requests: Vec<RequestRecord>,
    /// False if background work that the round started, such as merges, did not finish in time.
    pub completed: bool,
    /// Workload-specific measurements.
    pub notes: Json,
}

/// Attacker workload: a request pattern whose cost the node must bound per tenant.
pub trait Workload: Send + Sync {
    /// Creates the attacker's data for `round`. The harness calls this for both rounds before
    /// the victim's first phase, so the setup cost is outside every measured phase.
    fn prepare<'a>(&'a self, cx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>>;

    /// Sends the attack requests of `round` and waits for the background work that they start.
    fn attack<'a>(&'a self, cx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>>;
}

pub const NAMES: &[&str] = &["long_strings", "bulk_inserts", "merges", "blob_checks"];

pub fn by_name(name: &str) -> Option<Box<dyn Workload>> {
    Some(match name {
        "long_strings" => Box::new(long_strings::LongStrings),
        "bulk_inserts" => Box::new(bulk_inserts::BulkInserts::default()),
        "merges" => Box::new(merges::Merges),
        "blob_checks" => Box::new(blob_checks::BlobChecks),
        _ => return None,
    })
}

/// Sends every `(label, path, body)` POST at the same time and returns their records.
pub async fn concurrent_posts(cx: &AttackContext, requests: Vec<(String, String, bytes::Bytes)>, ok: &'static [u16]) -> Vec<RequestRecord> {
    let tasks: Vec<_> = requests
        .into_iter()
        .map(|(label, path, body)| {
            let (api, token) = (cx.api.clone(), cx.attacker.token.clone());
            tokio::spawn(async move { api.timed(&label, reqwest::Method::POST, &path, &token, body, ok).await })
        })
        .collect();
    let mut out = Vec::new();
    for t in tasks {
        out.push(t.await.expect("attack request"));
    }
    out
}
