//! HTTP router that an owner instance runs for the database routes of its owner.

use axum::Router;
use axum::routing::get;

/// Builds the router of the database routes that run in an owner instance.
pub fn router() -> Router {
    Router::new().route("/v1/guest/healthz", get(|| async { "ok" }))
}
