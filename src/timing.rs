//! Task-local latency and object-store operation tracking.
//!
//! Wrapping a future in [`scoped`] attaches a [`Tally`] to the task so that
//! object-store operations (via [`TimedStore`]), commit waits, and manifest
//! write waits on that task are recorded. Work spawned onto background tasks
//! is not attributed to the caller's tally.

use crate::engine::ObjectClass;
use crate::metrics::StoreMetrics;
use crate::store::{BoxFut, DynStore, GetOpts, GetResult, ListPage, ObjectStore, PutMode, SResult, StoreError};
use bytes::Bytes;
use serde_json::{Map, Value as Json, json};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreOp {
    Get,
    Put,
    List,
    Delete,
}

impl StoreOp {
    pub const ALL: [StoreOp; 4] = [StoreOp::Get, StoreOp::Put, StoreOp::List, StoreOp::Delete];

    pub fn name(self) -> &'static str {
        match self {
            StoreOp::Get => "get",
            StoreOp::Put => "put",
            StoreOp::List => "list",
            StoreOp::Delete => "delete",
        }
    }
}

#[derive(Default)]
struct OpStats {
    count: AtomicU64,
    errors: AtomicU64,
    micros: AtomicU64,
    bytes: AtomicU64,
}

/// Latency and object-store call counters for a single request or commit batch.
#[derive(Default)]
pub struct Tally {
    store: [[OpStats; StoreOp::ALL.len()]; ObjectClass::ALL.len()],
    commits: AtomicU64,
    commit_us: AtomicU64,
    read_wait_us: AtomicU64,
    build_us: AtomicU64,
}

fn to_ms(us: u64) -> u64 {
    us / 1000
}

impl Tally {
    fn stats(&self, class: ObjectClass, op: StoreOp) -> &OpStats {
        &self.store[class as usize][op as usize]
    }

    /// Number of `op` store calls on objects of `class`.
    pub fn store_calls(&self, class: ObjectClass, op: StoreOp) -> u64 {
        self.stats(class, op).count.load(Relaxed)
    }

    /// Formats recorded store operations as a JSON map keyed by `"<class>.<op>"`,
    /// or returns `None` if no store calls were made.
    pub fn store_json(&self) -> Option<Json> {
        let mut m = Map::new();
        for class in ObjectClass::ALL {
            for op in StoreOp::ALL {
                let s = self.stats(class, op);
                let n = s.count.load(Relaxed);
                if n == 0 {
                    continue;
                }
                let mut v = json!({"n": n, "ms": to_ms(s.micros.load(Relaxed)), "bytes": s.bytes.load(Relaxed)});
                let errors = s.errors.load(Relaxed);
                if errors > 0 {
                    v["errors"] = json!(errors);
                }
                m.insert(format!("{}.{}", class.name(), op.name()), v);
            }
        }
        (!m.is_empty()).then_some(Json::Object(m))
    }

    /// Total milliseconds spent in object-store calls.
    pub fn store_ms(&self) -> u64 {
        to_ms(self.store.iter().flatten().map(|s| s.micros.load(Relaxed)).sum())
    }

    /// Returns the number of commits awaited and total wait time in milliseconds.
    pub fn commits(&self) -> (u64, u64) {
        (self.commits.load(Relaxed), to_ms(self.commit_us.load(Relaxed)))
    }

    /// Total milliseconds spent waiting for an in-flight manifest write before serving a strong read.
    pub fn read_wait_ms(&self) -> u64 {
        to_ms(self.read_wait_us.load(Relaxed))
    }

    /// Formats a `Server-Timing` header value with durations in milliseconds for total request time (`total`),
    /// object-store operations (`store`), commit waits (`commit`), and in-flight manifest waits (`wait`).
    pub fn server_timing(&self, total: Duration) -> String {
        let ms = |us: u64| us as f64 / 1000.0;
        let calls: u64 = self.store.iter().flatten().map(|s| s.count.load(Relaxed)).sum();
        let store_us: u64 = self.store.iter().flatten().map(|s| s.micros.load(Relaxed)).sum();
        let mut v = format!("total;dur={:.1}", ms(to_micros(total)));
        if calls > 0 {
            v += &format!(", store;dur={:.1};desc={calls}", ms(store_us));
        }
        if self.commits.load(Relaxed) > 0 {
            v += &format!(", commit;dur={:.1}", ms(self.commit_us.load(Relaxed)));
        }
        let wait = self.read_wait_us.load(Relaxed);
        if wait > 0 {
            v += &format!(", wait;dur={:.1}", ms(wait));
        }
        v
    }

    /// Milliseconds spent building commit batches, including segment flushes.
    pub fn build_ms(&self) -> u64 {
        to_ms(self.build_us.load(Relaxed))
    }
}

tokio::task_local! {
    static TALLY: Arc<Tally>;
}

/// Runs `f` with `tally` attached to the current task.
pub async fn scoped<F: Future>(tally: Arc<Tally>, f: F) -> F::Output {
    TALLY.scope(tally, f).await
}

fn with_current(f: impl FnOnce(&Tally)) {
    let _ = TALLY.try_with(|t| f(t));
}

fn to_micros(d: Duration) -> u64 {
    d.as_micros().min(u64::MAX as u128) as u64
}

pub fn record_commit_wait(d: Duration) {
    with_current(|t| {
        t.commits.fetch_add(1, Relaxed);
        t.commit_us.fetch_add(to_micros(d), Relaxed);
    });
}

pub fn record_read_wait(d: Duration) {
    with_current(|t| {
        t.read_wait_us.fetch_add(to_micros(d), Relaxed);
    });
}

pub fn record_build_time(d: Duration) {
    with_current(|t| {
        t.build_us.fetch_add(to_micros(d), Relaxed);
    });
}

/// Object-store wrapper that records each operation in the task-local [`Tally`] and in
/// [`StoreMetrics`]. Operations that are dropped before completion are recorded with their
/// elapsed duration when dropped.
pub struct TimedStore {
    pub inner: DynStore,
    pub metrics: Arc<StoreMetrics>,
}

struct Call<'a> {
    store: &'a TimedStore,
    key: &'a str,
    op: StoreOp,
    started: Instant,
    bytes: u64,
    failed: bool,
}

impl Call<'_> {
    fn finish<T>(&mut self, res: &SResult<T>, bytes: impl FnOnce(&T) -> u64) {
        self.failed = matches!(res, Err(e) if !matches!(e, StoreError::NotFound | StoreError::NotModified));
        self.bytes += res.as_ref().map_or(0, bytes);
    }
}

impl Drop for Call<'_> {
    fn drop(&mut self) {
        let class = ObjectClass::from_key(self.key);
        let elapsed = self.started.elapsed();
        self.store.metrics.store_call(class, self.op, elapsed, self.failed);
        with_current(|t| {
            let s = t.stats(class, self.op);
            s.count.fetch_add(1, Relaxed);
            s.micros.fetch_add(to_micros(elapsed), Relaxed);
            s.bytes.fetch_add(self.bytes, Relaxed);
            if self.failed {
                s.errors.fetch_add(1, Relaxed);
            }
        });
    }
}

impl TimedStore {
    fn call<'a>(&'a self, key: &'a str, op: StoreOp, bytes: u64) -> Call<'a> {
        Call { store: self, key, op, started: Instant::now(), bytes, failed: false }
    }
}

impl ObjectStore for TimedStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        Box::pin(async move {
            let mut call = self.call(key, StoreOp::Get, 0);
            let res = self.inner.get(key, opts).await;
            call.finish(&res, |g| g.data.len() as u64);
            res
        })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move {
            let mut call = self.call(key, StoreOp::Put, data.len() as u64);
            let res = self.inner.put(key, data, mode).await;
            call.finish(&res, |_| 0);
            res
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        Box::pin(async move {
            let mut call = self.call(key, StoreOp::Delete, 0);
            let res = self.inner.delete(key).await;
            call.finish(&res, |_| 0);
            res
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        limit: usize,
    ) -> BoxFut<'a, SResult<ListPage>> {
        Box::pin(async move {
            let mut call = self.call(prefix, StoreOp::List, 0);
            let res = self.inner.list(prefix, start_after, limit).await;
            call.finish(&res, |_| 0);
            res
        })
    }

    fn time_precision_ms(&self) -> u64 {
        self.inner.time_precision_ms()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemStore;

    #[test]
    fn tally_scoping() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
        rt.block_on(async {
            let store = TimedStore { inner: Arc::new(MemStore::with_system_clock()), metrics: Arc::default() };
            let tally = Arc::new(Tally::default());
            scoped(tally.clone(), async {
                store.put("dbs/a/manifest", Bytes::from_static(b"abc"), PutMode::Create).await.unwrap();
                let again = store.put("dbs/a/manifest", Bytes::from_static(b"de"), PutMode::Create).await;
                assert_eq!(again, Err(StoreError::PreconditionFailed));
                assert_eq!(store.get("dbs/a/seg/x", GetOpts::default()).await.err(), Some(StoreError::NotFound));
                record_commit_wait(Duration::from_millis(3));
            })
            .await;
            store.put("dbs/a/manifest", Bytes::from_static(b"outside"), PutMode::Overwrite).await.unwrap();
            assert_eq!(tally.store_calls(ObjectClass::Manifest, StoreOp::Put), 2);
            assert_eq!(
                tally.store_json(),
                Some(json!({
                    "manifest.put": {"n": 2, "ms": 0, "bytes": 5, "errors": 1},
                    "segment.get": {"n": 1, "ms": 0, "bytes": 0},
                }))
            );
            assert_eq!(tally.commits(), (1, 3));
        });
    }

    struct Hung;

    impl ObjectStore for Hung {
        fn get<'a>(&'a self, _: &'a str, _: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
            Box::pin(std::future::pending())
        }
        fn put<'a>(&'a self, _: &'a str, _: Bytes, _: PutMode) -> BoxFut<'a, SResult<String>> {
            Box::pin(std::future::pending())
        }
        fn delete<'a>(&'a self, _: &'a str) -> BoxFut<'a, SResult<()>> {
            Box::pin(std::future::pending())
        }
        fn list<'a>(&'a self, _: &'a str, _: Option<&'a str>, _: usize) -> BoxFut<'a, SResult<ListPage>> {
            Box::pin(std::future::pending())
        }
    }

    #[test]
    fn abandoned_calls_are_recorded() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
        rt.block_on(async {
            let store = TimedStore { inner: Arc::new(Hung), metrics: Arc::default() };
            let tally = Arc::new(Tally::default());
            scoped(tally.clone(), async {
                let put = store.put("dbs/a/manifest", Bytes::from_static(b"abc"), PutMode::Create);
                assert!(tokio::time::timeout(Duration::from_millis(400), put).await.is_err());
            })
            .await;
            assert_eq!(tally.store_json(), Some(json!({"manifest.put": {"n": 1, "ms": 400, "bytes": 3}})));
            let mut out = serde_json::Map::new();
            store.metrics.json_into(&mut out);
            assert_eq!(out["store"]["manifest.put"]["max_ms"], 400.0);
        });
    }
}
