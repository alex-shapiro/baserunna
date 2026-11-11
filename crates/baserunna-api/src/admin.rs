//! Admin metrics endpoints and types.

use crate::{Endpoint, NoBody, json_body};
use http::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// `GET /v1/admin/metrics`.
#[derive(Clone, Copy, Debug, Default)]
pub struct GetMetrics;

impl Endpoint for GetMetrics {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/admin/metrics";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = Metrics;

    fn is_idempotent(&self, _: &NoBody) -> bool {
        true
    }

    fn path(&self) -> String {
        Self::ROUTE.into()
    }
}

/// `DELETE /v1/admin/metrics`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ResetMetrics;

impl Endpoint for ResetMetrics {
    const METHOD: Method = Method::DELETE;
    const ROUTE: &'static str = "/v1/admin/metrics";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = Metrics;

    fn is_idempotent(&self, _: &NoBody) -> bool {
        false
    }

    fn path(&self) -> String {
        Self::ROUTE.into()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Metrics {
    pub node: String,
    /// Object-store request latencies by `{class}.{operation}`.
    pub store: BTreeMap<String, Histogram>,
    pub commits: CommitMetrics,
    /// HTTP request latencies by `{method} {route}`.
    pub routes: BTreeMap<String, Histogram>,
    /// Tokio task scheduling delay.
    pub lag: Histogram,
    pub cpu: Cpu,
}

/// Latency distribution in milliseconds. Quantiles are `None` if `count` is 0.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Histogram {
    pub count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mean_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p50_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p90_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p99_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p999_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ms: Option<f64>,
    /// Failed object-store requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub errors: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CommitMetrics {
    /// Latency of commit batches.
    #[serde(flatten)]
    pub latency: Histogram,
    pub txns_per_batch: f64,
    pub max_txns_per_batch: u64,
    /// Failed manifest `PUT`s that stopped strong reads from cache on a database. Until this node
    /// confirms a manifest write, each strong read on that database reads the manifest.
    pub lease_reads_stopped: u64,
    /// Manifest reloads that found a write from a failed `PUT` of this node, and started strong
    /// reads from cache again.
    pub lease_reads_restored_from_landed_writes: u64,
}

/// Process and host-steal CPU time in milliseconds, or `None` if `/proc` is unavailable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cpu {
    pub process_ms: Option<u64>,
    pub steal_ms: Option<u64>,
}

json_body!(Metrics);
