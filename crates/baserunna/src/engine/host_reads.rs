//! Default-deny guard for tenant data reads in the node process.
//!
//! A request on a database of a node with a sandbox runs in a scope that
//! names that database. In that scope, the transaction and blob reads of the
//! database fail, so a route reaches tenant data only through the sandbox.
//! A route on the named list in `api::host_routes` runs without the scope.

use crate::error::{Error, Result};

tokio::task_local! {
    static SANDBOXED_DB: String;
}

/// Runs `work` as request work on `db`, whose data the host may not read.
pub async fn sandboxed_request<F: Future>(db: String, work: F) -> F::Output {
    SANDBOXED_DB.scope(db, work).await
}

/// Fails if the current request may read `db` only through the sandbox.
pub fn check_host_read(db: &str) -> Result<()> {
    match SANDBOXED_DB.try_with(|d| d == db) {
        Ok(true) => Err(Error::internal(format!(
            "a route read data of database {db} outside the sandbox; the route must run in the sandbox or be on the host route list"
        ))),
        _ => Ok(()),
    }
}
