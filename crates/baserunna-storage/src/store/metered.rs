//! Object store wrapper that counts requests per database and adds fixed latency.

use super::{DynStore, GetOpts, GetResult, ListPage, ObjectStore, PutMode, SResult};
use baserunna_util::BoxFut;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Object-store request and byte counts.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Counts {
    pub gets: f64,
    pub puts: f64,
    pub lists: f64,
    pub deletes: f64,
    pub put_bytes: f64,
}

impl Counts {
    pub fn total(&self) -> f64 {
        self.gets + self.puts + self.lists + self.deletes
    }

    pub fn per(&self, n: f64) -> Counts {
        Counts {
            gets: self.gets / n,
            puts: self.puts / n,
            lists: self.lists / n,
            deletes: self.deletes / n,
            put_bytes: self.put_bytes / n,
        }
    }

    pub fn minus(&self, o: &Counts) -> Counts {
        Counts {
            gets: self.gets - o.gets,
            puts: self.puts - o.puts,
            lists: self.lists - o.lists,
            deletes: self.deletes - o.deletes,
            put_bytes: self.put_bytes - o.put_bytes,
        }
    }

    pub fn plus(&self, o: &Counts) -> Counts {
        Counts {
            gets: self.gets + o.gets,
            puts: self.puts + o.puts,
            lists: self.lists + o.lists,
            deletes: self.deletes + o.deletes,
            put_bytes: self.put_bytes + o.put_bytes,
        }
    }
}

/// Returns the database ID for a store key, `"sys"`, or `""` for keys outside `dbs/`.
pub fn scope_of(key: &str) -> &str {
    key.strip_prefix("dbs/").and_then(|r| r.split('/').next()).unwrap_or("")
}

/// Object store wrapper that counts requests per database and injects fixed latency.
pub struct MeteredStore {
    inner: DynStore,
    latency_ms: AtomicU64,
    counts: Mutex<BTreeMap<String, Counts>>,
}

impl MeteredStore {
    pub fn new(inner: DynStore, latency_ms: u64) -> MeteredStore {
        MeteredStore { inner, latency_ms: AtomicU64::new(latency_ms), counts: Mutex::new(BTreeMap::new()) }
    }

    pub fn set_latency_ms(&self, ms: u64) {
        self.latency_ms.store(ms, Ordering::Relaxed);
    }

    fn count(&self, key: &str, f: impl FnOnce(&mut Counts)) {
        let mut g = self.counts.lock();
        f(g.entry(scope_of(key).to_string()).or_default());
    }

    async fn delay(&self) {
        let ms = self.latency_ms.load(Ordering::Relaxed);
        if ms > 0 {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
    }

    pub fn scope(&self, scope: &str) -> Counts {
        self.counts.lock().get(scope).copied().unwrap_or_default()
    }

    pub fn total(&self) -> Counts {
        self.counts.lock().values().fold(Counts::default(), |a, c| a.plus(c))
    }
}

impl ObjectStore for MeteredStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        Box::pin(async move {
            self.count(key, |c| c.gets += 1.0);
            self.delay().await;
            self.inner.get(key, opts).await
        })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move {
            let n = data.len() as f64;
            self.count(key, |c| {
                c.puts += 1.0;
                c.put_bytes += n;
            });
            self.delay().await;
            self.inner.put(key, data, mode).await
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        Box::pin(async move {
            self.count(key, |c| c.deletes += 1.0);
            self.delay().await;
            self.inner.delete(key).await
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        limit: usize,
    ) -> BoxFut<'a, SResult<ListPage>> {
        Box::pin(async move {
            self.count(prefix, |c| c.lists += 1.0);
            self.delay().await;
            self.inner.list(prefix, start_after, limit).await
        })
    }

    fn time_precision_ms(&self) -> u64 {
        self.inner.time_precision_ms()
    }
}
