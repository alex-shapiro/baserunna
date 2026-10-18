//! Per-operation time accounting for diagnosing latency.
//!
//! A request or a commit batch runs inside [`scoped`] with its own [`Tally`].
//! Object-store calls (through [`TimedStore`]), commit waits and read waits
//! made on the same task add to that tally, and the scope's owner logs it.
//! Work on other tasks, such as the committer or background compaction, is
//! not attributed to the request that triggered it.

use crate::engine::LEVELS_SUFFIX;
use crate::store::{BoxFut, DynStore, GetOpts, GetResult, ListPage, ObjectStore, PutMode, SResult, StoreError};
use bytes::Bytes;
use serde_json::{Map, Value as Json, json};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;
use tokio::time::Instant;

/// The kind of object a store key names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    Manifest,
    Segment,
    Levels,
    Blob,
    Other,
}

impl Class {
    const ALL: [Class; 5] = [Class::Manifest, Class::Segment, Class::Levels, Class::Blob, Class::Other];

    pub fn of(key: &str) -> Class {
        let Some((_, rest)) = key.strip_prefix("dbs/").and_then(|k| k.split_once('/')) else {
            return Class::Other;
        };
        if rest == "manifest" {
            Class::Manifest
        } else if rest.starts_with("seg/") {
            if rest.ends_with(LEVELS_SUFFIX) { Class::Levels } else { Class::Segment }
        } else if rest.starts_with("blob/") {
            Class::Blob
        } else {
            Class::Other
        }
    }

    fn name(self) -> &'static str {
        match self {
            Class::Manifest => "manifest",
            Class::Segment => "segment",
            Class::Levels => "levels",
            Class::Blob => "blob",
            Class::Other => "other",
        }
    }
}

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
struct Calls {
    n: AtomicU64,
    errors: AtomicU64,
    us: AtomicU64,
    bytes: AtomicU64,
}

/// Time spent by one request or commit batch, by cause.
#[derive(Default)]
pub struct Tally {
    store: [[Calls; 4]; 5],
    commits: AtomicU64,
    commit_us: AtomicU64,
    read_wait_us: AtomicU64,
    build_us: AtomicU64,
}

fn ms(us: u64) -> u64 {
    us / 1000
}

impl Tally {
    fn calls(&self, class: Class, op: StoreOp) -> &Calls {
        &self.store[class as usize][op as usize]
    }

    /// Number of store calls of `op` on objects of `class`.
    pub fn store_calls(&self, class: Class, op: StoreOp) -> u64 {
        self.calls(class, op).n.load(Relaxed)
    }

    /// Store calls as `{"manifest.put": {"n", "ms", "bytes", "errors"}, ..}`,
    /// or `None` if there were none.
    pub fn store_json(&self) -> Option<Json> {
        let mut m = Map::new();
        for class in Class::ALL {
            for op in StoreOp::ALL {
                let c = self.calls(class, op);
                let n = c.n.load(Relaxed);
                if n == 0 {
                    continue;
                }
                let mut v = json!({"n": n, "ms": ms(c.us.load(Relaxed)), "bytes": c.bytes.load(Relaxed)});
                let errors = c.errors.load(Relaxed);
                if errors > 0 {
                    v["errors"] = json!(errors);
                }
                m.insert(format!("{}.{}", class.name(), op.name()), v);
            }
        }
        (!m.is_empty()).then_some(Json::Object(m))
    }

    /// Total milliseconds spent in store calls.
    pub fn store_ms(&self) -> u64 {
        ms(self.store.iter().flatten().map(|c| c.us.load(Relaxed)).sum())
    }

    /// Commits awaited and the total milliseconds spent waiting for them.
    pub fn commits(&self) -> (u64, u64) {
        (self.commits.load(Relaxed), ms(self.commit_us.load(Relaxed)))
    }

    /// Milliseconds strong reads waited for an in-flight manifest write.
    pub fn read_wait_ms(&self) -> u64 {
        ms(self.read_wait_us.load(Relaxed))
    }

    /// Milliseconds spent building commit batches, including segment flushes.
    pub fn build_ms(&self) -> u64 {
        ms(self.build_us.load(Relaxed))
    }
}

tokio::task_local! {
    static TALLY: Arc<Tally>;
}

/// Runs `f` with `tally` collecting the time spent on its task.
pub async fn scoped<F: Future>(tally: Arc<Tally>, f: F) -> F::Output {
    TALLY.scope(tally, f).await
}

fn add(f: impl FnOnce(&Tally)) {
    let _ = TALLY.try_with(|t| f(t));
}

fn micros(d: Duration) -> u64 {
    d.as_micros().min(u64::MAX as u128) as u64
}

pub fn commit_waited(d: Duration) {
    add(|t| {
        t.commits.fetch_add(1, Relaxed);
        t.commit_us.fetch_add(micros(d), Relaxed);
    });
}

pub fn read_waited(d: Duration) {
    add(|t| {
        t.read_wait_us.fetch_add(micros(d), Relaxed);
    });
}

pub fn built(d: Duration) {
    add(|t| {
        t.build_us.fetch_add(micros(d), Relaxed);
    });
}

fn store_call<T>(class: Class, op: StoreOp, started: Instant, bytes: u64, r: &SResult<T>) {
    let elapsed = started.elapsed();
    add(|t| {
        let c = t.calls(class, op);
        c.n.fetch_add(1, Relaxed);
        c.us.fetch_add(micros(elapsed), Relaxed);
        c.bytes.fetch_add(bytes, Relaxed);
        if matches!(r, Err(e) if !matches!(e, StoreError::NotFound | StoreError::NotModified)) {
            c.errors.fetch_add(1, Relaxed);
        }
    });
}

/// An object store that records each call in the current [`Tally`].
pub struct TimedStore(pub DynStore);

impl ObjectStore for TimedStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        Box::pin(async move {
            let t = Instant::now();
            let r = self.0.get(key, opts).await;
            let bytes = r.as_ref().map_or(0, |g| g.data.len() as u64);
            store_call(Class::of(key), StoreOp::Get, t, bytes, &r);
            r
        })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move {
            let t = Instant::now();
            let bytes = data.len() as u64;
            let r = self.0.put(key, data, mode).await;
            store_call(Class::of(key), StoreOp::Put, t, bytes, &r);
            r
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        Box::pin(async move {
            let t = Instant::now();
            let r = self.0.delete(key).await;
            store_call(Class::of(key), StoreOp::Delete, t, 0, &r);
            r
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        limit: usize,
    ) -> BoxFut<'a, SResult<ListPage>> {
        Box::pin(async move {
            let t = Instant::now();
            let r = self.0.list(prefix, start_after, limit).await;
            store_call(Class::of(prefix), StoreOp::List, t, 0, &r);
            r
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
    fn classes_follow_the_key_layout() {
        assert_eq!(Class::of("dbs/a1/manifest"), Class::Manifest);
        assert_eq!(Class::of("dbs/a1/seg/0123"), Class::Segment);
        assert_eq!(Class::of(&format!("dbs/a1/seg/0123{LEVELS_SUFFIX}")), Class::Levels);
        assert_eq!(Class::of("dbs/a1/blob/b/00000001"), Class::Blob);
        assert_eq!(Class::of("dbs/a1/seg/"), Class::Segment);
        assert_eq!(Class::of("clock"), Class::Other);
        assert_eq!(Class::of("dbs/a1"), Class::Other);
    }

    #[test]
    fn store_calls_count_toward_the_enclosing_scope_only() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
        rt.block_on(async {
            let store = TimedStore(Arc::new(MemStore::with_system_clock()));
            let tally = Arc::new(Tally::default());
            scoped(tally.clone(), async {
                store.put("dbs/a/manifest", Bytes::from_static(b"abc"), PutMode::Create).await.unwrap();
                let again = store.put("dbs/a/manifest", Bytes::from_static(b"de"), PutMode::Create).await;
                assert_eq!(again, Err(StoreError::PreconditionFailed));
                assert_eq!(store.get("dbs/a/seg/x", GetOpts::default()).await.err(), Some(StoreError::NotFound));
                commit_waited(Duration::from_millis(3));
            })
            .await;
            store.put("dbs/a/manifest", Bytes::from_static(b"outside"), PutMode::Overwrite).await.unwrap();
            assert_eq!(tally.store_calls(Class::Manifest, StoreOp::Put), 2);
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
