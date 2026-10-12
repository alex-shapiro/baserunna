//! In-memory object store with S3 semantics. Used for tests, the simulator
//! (wrapped by the fault-injecting `SimStore`) and `mem:` dev mode.

use super::*;
use std::collections::BTreeMap;
use std::sync::Mutex;

struct Obj {
    data: Bytes,
    etag: String,
    last_modified_ms: u64,
}

pub struct MemStore {
    inner: Mutex<Inner>,
    clock: Box<dyn Fn() -> u64 + Send + Sync>,
}

struct Inner {
    objects: BTreeMap<String, Obj>,
    next_etag: u64,
}

impl MemStore {
    /// `clock` supplies last-modified timestamps (ms since epoch).
    pub fn new(clock: Box<dyn Fn() -> u64 + Send + Sync>) -> MemStore {
        MemStore { inner: Mutex::new(Inner { objects: BTreeMap::new(), next_etag: 1 }), clock }
    }

    pub fn with_system_clock() -> MemStore {
        MemStore::new(Box::new(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        }))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    // Synchronous primitives (also used by the simulator to apply operations
    // at a chosen virtual instant).

    pub fn get_sync(&self, key: &str, opts: &GetOpts) -> SResult<GetResult> {
        let g = self.lock();
        let o = g.objects.get(key).ok_or(StoreError::NotFound)?;
        if let Some(e) = &opts.if_none_match
            && *e == o.etag
        {
            return Err(StoreError::NotModified);
        }
        let size = o.data.len() as u64;
        let r = clamp_range(&opts.range, size)?;
        Ok(GetResult {
            data: o.data.slice(r.start as usize..r.end as usize),
            etag: o.etag.clone(),
            size,
            last_modified_ms: o.last_modified_ms,
        })
    }

    pub fn put_sync(&self, key: &str, data: Bytes, mode: &PutMode) -> SResult<String> {
        let now = (self.clock)();
        let mut g = self.lock();
        match (mode, g.objects.get(key)) {
            (PutMode::Create, Some(_)) => return Err(StoreError::PreconditionFailed),
            (PutMode::IfMatch(_), None) => return Err(StoreError::PreconditionFailed),
            (PutMode::IfMatch(e), Some(o)) if *e != o.etag => {
                return Err(StoreError::PreconditionFailed);
            }
            _ => {}
        }
        let etag = format!("e{}", g.next_etag);
        g.next_etag += 1;
        g.objects.insert(key.to_string(), Obj { data, etag: etag.clone(), last_modified_ms: now });
        Ok(etag)
    }

    pub fn delete_sync(&self, key: &str) {
        self.lock().objects.remove(key);
    }

    pub fn list_sync(&self, prefix: &str, start_after: Option<&str>, limit: usize) -> ListPage {
        let g = self.lock();
        let mut objects = Vec::new();
        let lower = match start_after {
            Some(s) if s >= prefix => std::ops::Bound::Excluded(s.to_string()),
            _ => std::ops::Bound::Included(prefix.to_string()),
        };
        let limit = limit.max(1);
        let mut next = None;
        for (k, o) in g.objects.range((lower, std::ops::Bound::Unbounded)) {
            if !k.starts_with(prefix) {
                break;
            }
            if objects.len() == limit {
                next = objects.last().map(|m: &ObjectMeta| m.key.clone());
                break;
            }
            objects.push(ObjectMeta {
                key: k.clone(),
                size: o.data.len() as u64,
                etag: o.etag.clone(),
                last_modified_ms: o.last_modified_ms,
            });
        }
        ListPage { objects, next }
    }

    /// Every key currently stored (for invariant checks).
    pub fn keys(&self) -> Vec<String> {
        self.lock().objects.keys().cloned().collect()
    }

    pub fn total_bytes(&self) -> u64 {
        self.lock().objects.values().map(|o| o.data.len() as u64).sum()
    }

    pub fn len(&self) -> usize {
        self.lock().objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl ObjectStore for MemStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        Box::pin(async move { self.get_sync(key, &opts) })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move { self.put_sync(key, data, &mode) })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        Box::pin(async move {
            self.delete_sync(key);
            Ok(())
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        limit: usize,
    ) -> BoxFut<'a, SResult<ListPage>> {
        Box::pin(async move { Ok(self.list_sync(prefix, start_after, limit)) })
    }

    fn time_precision_ms(&self) -> u64 {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> MemStore {
        MemStore::new(Box::new(|| 1000))
    }

    #[tokio::test]
    async fn cas_semantics() {
        let s = store();
        let e1 = s.put("m", Bytes::from_static(b"a"), PutMode::Create).await.unwrap();
        assert_eq!(s.put("m", Bytes::from_static(b"b"), PutMode::Create).await, Err(StoreError::PreconditionFailed));
        assert_eq!(
            s.put("m", Bytes::from_static(b"b"), PutMode::IfMatch("zz".into())).await,
            Err(StoreError::PreconditionFailed)
        );
        let e2 = s.put("m", Bytes::from_static(b"b"), PutMode::IfMatch(e1.clone())).await.unwrap();
        assert_ne!(e1, e2);
        let g = s.get("m", GetOpts { if_none_match: Some(e2.clone()), ..Default::default() }).await;
        assert_eq!(g.unwrap_err(), StoreError::NotModified);
        let g = s.get("m", GetOpts { if_none_match: Some(e1), ..Default::default() }).await.unwrap();
        assert_eq!(&g.data[..], b"b");
        assert_eq!(s.put("x", Bytes::new(), PutMode::IfMatch(e2)).await, Err(StoreError::PreconditionFailed));
    }

    #[tokio::test]
    async fn ranges_and_list() {
        let s = store();
        s.put("a/1", Bytes::from_static(b"0123456789"), PutMode::Overwrite).await.unwrap();
        s.put("a/2", Bytes::new(), PutMode::Overwrite).await.unwrap();
        s.put("b/1", Bytes::new(), PutMode::Overwrite).await.unwrap();
        let g = s.get("a/1", GetOpts { range: Some(2..5), ..Default::default() }).await.unwrap();
        assert_eq!(&g.data[..], b"234");
        assert_eq!(g.size, 10);
        let g = s.get("a/1", GetOpts { range: Some(8..50), ..Default::default() }).await.unwrap();
        assert_eq!(&g.data[..], b"89");
        assert!(s.get("a/1", GetOpts { range: Some(10..12), ..Default::default() }).await.is_err());
        let p = s.list("a/", None, 1).await.unwrap();
        assert_eq!(p.objects.len(), 1);
        assert_eq!(p.next.as_deref(), Some("a/1"));
        let p = s.list("a/", p.next.as_deref(), 10).await.unwrap();
        assert_eq!(p.objects[0].key, "a/2");
        assert!(p.next.is_none());
        s.delete("a/1").await.unwrap();
        s.delete("a/1").await.unwrap();
        assert_eq!(s.get("a/1", GetOpts::default()).await.unwrap_err(), StoreError::NotFound);
    }
}
