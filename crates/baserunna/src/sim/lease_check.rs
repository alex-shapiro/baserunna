//! Lease oracle that the simulator and the engine tests share.

use crate::engine::{Db, LeasedView};
use tokio::time::Instant;

/// Manifest writes that a fault-injecting store applied or can still apply.
pub(crate) trait ManifestHistory {
    /// Returns the ETag of the stored manifest of database `db`, or `None` if it does not exist.
    fn stored_manifest_etag(&self, db: &str) -> Option<String>;

    /// Returns the instant at which the store applied the manifest of `db` with ETag `etag`.
    fn manifest_applied_at(&self, db: &str, etag: &str) -> Option<Instant>;

    /// Returns the `If-Match` ETags of the failed manifest writes for `db` that can still apply.
    fn late_manifest_put_targets(&self, db: &str) -> Vec<String>;
}

/// Checks that the node of `db` stops relying on its lease before its lease term ends. Also checks
/// that a lease-served view without an in-flight manifest write is the stored manifest, and that no
/// failed write can replace it.
pub(crate) fn check_lease_view(db: &Db, history: &impl ManifestHistory) -> Result<(), String> {
    let Some(view) = db.leased_view() else { return Ok(()) };
    if let Some(applied_at) = history.manifest_applied_at(&db.id, &view.etag) {
        let overrun = view.lease_overrun(applied_at, db.shared().cfg.lease_margin_ms);
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
        && stored != etag
    {
        return Err(format!(
            "serves lease reads on {} at write {write_seq} ({etag}), but the store has {stored}",
            db.id
        ));
    }
    if history.late_manifest_put_targets(&db.id).contains(&etag) {
        return Err(format!(
            "serves lease reads on {} at write {write_seq} ({etag}), but a late write can replace it",
            db.id
        ));
    }
    Ok(())
}
