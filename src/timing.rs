//! Task-local latency and object-store operation tracking.
//!
//! Wrapping a future in [`scoped`] attaches a [`Tally`] to the task so that
//! object-store operations (via [`TimedStore`]), commit waits, and manifest
//! write waits on that task are recorded. Work spawned onto background tasks
//! is not attributed to the caller's tally.

use crate::engine::ObjectClass;
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
    const ALL: [StoreOp; 4] = [StoreOp::Get, StoreOp::Put, StoreOp::List, StoreOp::Delete];

    fn name(self) -> &'static str {
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

fn record_store_call<T>(class: ObjectClass, op: StoreOp, started: Instant, bytes: u64, res: &SResult<T>) {
    let elapsed = started.elapsed();
    with_current(|t| {
        let s = t.stats(class, op);
        s.count.fetch_add(1, Relaxed);
        s.micros.fetch_add(to_micros(elapsed), Relaxed);
        s.bytes.fetch_add(bytes, Relaxed);
        if matches!(res, Err(e) if !matches!(e, StoreError::NotFound | StoreError::NotModified)) {
            s.errors.fetch_add(1, Relaxed);
        }
    });
}

/// Object-store wrapper that records each operation in the task-local [`Tally`].
pub struct TimedStore(pub DynStore);

impl ObjectStore for TimedStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        Box::pin(async move {
            let started = Instant::now();
            let res = self.0.get(key, opts).await;
            let bytes = res.as_ref().map_or(0, |g| g.data.len() as u64);
            record_store_call(ObjectClass::from_key(key), StoreOp::Get, started, bytes, &res);
            res
        })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move {
            let started = Instant::now();
            let bytes = data.len() as u64;
            let res = self.0.put(key, data, mode).await;
            record_store_call(ObjectClass::from_key(key), StoreOp::Put, started, bytes, &res);
            res
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        Box::pin(async move {
            let started = Instant::now();
            let res = self.0.delete(key).await;
            record_store_call(ObjectClass::from_key(key), StoreOp::Delete, started, 0, &res);
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
            let started = Instant::now();
            let res = self.0.list(prefix, start_after, limit).await;
            record_store_call(ObjectClass::from_key(prefix), StoreOp::List, started, 0, &res);
            res
        })
    }

    fn time_precision_ms(&self) -> u64 {
        self.0.time_precision_ms()
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
            let store = TimedStore(Arc::new(MemStore::with_system_clock()));
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
}
