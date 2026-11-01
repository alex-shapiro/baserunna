//! Node administration.

use crate::{Endpoint, NoBody, json_body};
use http::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// `GET /v1/admin/metrics`: reads the latency metrics of the node that serves the request.
#[derive(Clone, Copy, Debug, Default)]
pub struct GetMetrics;

impl Endpoint for GetMetrics {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/admin/metrics";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = Metrics;

    fn is_idempotent(_: &NoBody) -> bool {
        true
    }

    fn path(&self) -> String {
        Self::ROUTE.into()
    }
}

/// `DELETE /v1/admin/metrics`: reads and then resets the metrics of the node that serves the
/// request.
#[derive(Clone, Copy, Debug, Default)]
pub struct ResetMetrics;

impl Endpoint for ResetMetrics {
    const METHOD: Method = Method::DELETE;
    const ROUTE: &'static str = "/v1/admin/metrics";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = Metrics;

    fn is_idempotent(_: &NoBody) -> bool {
        false
    }

    fn path(&self) -> String {
        Self::ROUTE.into()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Metrics {
    pub node: String,
    /// Object store requests by `{object class}.{operation}`.
    pub store: BTreeMap<String, Histogram>,
    pub commits: CommitMetrics,
    /// HTTP requests by `{method} {route}`.
    pub routes: BTreeMap<String, Histogram>,
    /// Tokio scheduling delay.
    pub lag: Histogram,
    pub cpu: Cpu,
}

/// Latency distribution; quantiles are omitted if `count` is 0.
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
    /// Failed object store requests.
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
}

/// Process and steal CPU time, or `None` where `/proc` is unavailable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cpu {
    pub process_ms: Option<u64>,
    pub steal_ms: Option<u64>,
}

json_body!(Metrics);
