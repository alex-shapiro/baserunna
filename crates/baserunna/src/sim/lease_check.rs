//! Lease oracle that the simulator and the engine tests share.

use crate::engine::{Db, LeasedView};
use std::time::Duration;
use tokio::time::Instant;

/// Manifest writes that a fault-injecting store applied or can still apply.
pub(crate) trait ManifestHistory {
    /// Returns the ETag of the stored manifest of database `db`, or `None` if it does not exist.
    fn stored_manifest_etag(&self, db: &str) -> Option<String>;

    /// Returns the instant at which the store applied the manifest of `db` with ETag `etag`.
    fn manifest_applied_at(&self, db: &str, etag: &str) -> Option<Instant>;

    /// Returns the `If-Match` ETags of the failed manifest writes for `db` that the store can still apply.
    fn late_manifest_put_targets(&self, db: &str) -> Vec<String>;
}

/// Checks `view`, the lease view of the node of `db`. The node must stop relying on its lease
/// before its lease term ends. A view without an in-flight manifest write must match the stored
/// manifest, and a failed write must not be able to replace it.
pub(crate) fn check_lease_view(db: &Db, view: &LeasedView, history: &impl ManifestHistory) -> Result<(), String> {
    if let Some(applied_at) = history.manifest_applied_at(&db.id, &view.etag) {
        let overrun = lease_overrun(view, applied_at, db.shared().cfg.lease_margin_ms);
        if !overrun.is_zero() {
            return Err(format!(
                "relies on its lease on {} at write {} for {} ms after the lease term",
                db.id,
                view.write_seq,
                overrun.as_millis()
            ));
        }
    }
    let LeasedView { write_seq, etag, write_in_flight: false, .. } = view else { return Ok(()) };
    if let Some(stored) = history.stored_manifest_etag(&db.id)
        && stored != *etag
    {
        return Err(format!(
            "serves lease reads on {} at write {write_seq} ({etag}), but the store has {stored}",
            db.id
        ));
    }
    if history.late_manifest_put_targets(&db.id).contains(etag) {
        return Err(format!(
            "serves lease reads on {} at write {write_seq} ({etag}), but a late write can replace it",
            db.id
        ));
    }
    Ok(())
}

/// Returns how long the node of `view` relies on its lease after the lease term ends, if the store
/// applied the manifest at `applied_at`. The holder times its lease from before it sends the
/// manifest, so the term is `lease_ms - margin_ms` from `applied_at`. Another node honors the lease
/// for `lease_ms + margin_ms` from its first read of the manifest, so the term ends first.
fn lease_overrun(view: &LeasedView, applied_at: Instant, margin_ms: u64) -> Duration {
    let term = Duration::from_millis(view.lease_ms - margin_ms.min(view.lease_ms));
    view.expires_at.saturating_duration_since(applied_at + term)
}
