//! The store handle of the node process when workers serve tenants: it fails every access to a
//! tenant database's objects, so the node process cannot read tenant data on any path.

use super::{BoxFut, DynStore, GetOpts, GetResult, ListPage, ObjectStore, PutMode, SResult, StoreError};
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};

/// The node process's store: it passes through the system database's objects and the objects
/// outside any database, and fails every access to a tenant database's objects.
pub struct TenantDenyingStore {
    inner: DynStore,
    denied: AtomicU64,
}

impl TenantDenyingStore {
    pub fn new(inner: DynStore) -> TenantDenyingStore {
        TenantDenyingStore { inner, denied: AtomicU64::new(0) }
    }

    /// Returns the number of accesses that the guard failed.
    pub fn denied(&self) -> u64 {
        self.denied.load(Ordering::Relaxed)
    }

    fn check(&self, key_or_prefix: &str) -> SResult<()> {
        if tenant_key(key_or_prefix) {
            self.denied.fetch_add(1, Ordering::Relaxed);
            crate::warn!("the node process tried to access tenant objects at {key_or_prefix}");
            return Err(StoreError::Fatal(format!("the node process may not access {key_or_prefix}")));
        }
        Ok(())
    }
}

/// Returns true if `key_or_prefix` names, or may cover, an object of a tenant database: any
/// key or prefix under `dbs/` except the system database's, and the prefixes that cover all
/// databases.
fn tenant_key(key_or_prefix: &str) -> bool {
    let system = crate::engine::db_prefix(crate::meta::SYS_DB);
    if key_or_prefix.is_empty() || "dbs/".starts_with(key_or_prefix) {
        return true;
    }
    key_or_prefix.starts_with("dbs/") && !key_or_prefix.starts_with(&system)
}

impl ObjectStore for TenantDenyingStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        match self.check(key) {
            Ok(()) => self.inner.get(key, opts),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        match self.check(key) {
            Ok(()) => self.inner.put(key, data, mode),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        match self.check(key) {
            Ok(()) => self.inner.delete(key),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        limit: usize,
    ) -> BoxFut<'a, SResult<ListPage>> {
        match self.check(prefix) {
            Ok(()) => self.inner.list(prefix, start_after, limit),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }

    fn time_precision_ms(&self) -> u64 {
        self.inner.time_precision_ms()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_tenant_keys_are_denied() {
        assert!(tenant_key("dbs/abc/manifest"));
        assert!(tenant_key("dbs/abc/"));
        assert!(tenant_key("dbs/"));
        assert!(tenant_key("dbs"));
        assert!(tenant_key(""));
        assert!(!tenant_key(&format!("{}manifest", crate::engine::db_prefix(crate::meta::SYS_DB))));
        assert!(!tenant_key("clock/now"));
    }

    #[tokio::test]
    async fn tenant_reads_fail_and_count() {
        let store = TenantDenyingStore::new(std::sync::Arc::new(super::super::MemStore::new(Box::new(|| 1000))));
        assert!(store.get("dbs/t1/manifest", GetOpts::default()).await.is_err());
        assert!(store.list("dbs/t1/", None, 10).await.is_err());
        assert!(store.list("", None, 10).await.is_err());
        let sys = format!("{}x", crate::engine::db_prefix(crate::meta::SYS_DB));
        assert!(store.put(&sys, Bytes::from_static(b"1"), PutMode::Overwrite).await.is_ok());
        assert_eq!(store.denied(), 3);
    }
}
