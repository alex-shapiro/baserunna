//! Object storage abstraction.
//!
//! The engine only needs a handful of primitives, all of which S3, GCS,
//! Azure Blob and R2 provide with strong consistency:
//! ranged/conditional GET, conditional PUT (create-only or compare-and-swap
//! on an opaque etag), idempotent DELETE and ordered LIST.

mod fs;
mod mem;

#[cfg(feature = "cloud")]
mod cloud;

pub use fs::FsStore;
pub use mem::MemStore;

#[cfg(feature = "cloud")]
pub use cloud::CloudStore;

use bytes::Bytes;
use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    NotFound,
    /// Conditional put failed (etag mismatch, or object exists for create-only).
    PreconditionFailed,
    /// Conditional get: the object still has the given etag.
    NotModified,
    /// Possibly transient failure; the operation may or may not have applied.
    Unavailable(String),
    /// Permanent failure (bad configuration, permissions, ...).
    Fatal(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::NotFound => write!(f, "object not found"),
            StoreError::PreconditionFailed => write!(f, "precondition failed"),
            StoreError::NotModified => write!(f, "not modified"),
            StoreError::Unavailable(m) => write!(f, "object store unavailable: {m}"),
            StoreError::Fatal(m) => write!(f, "object store error: {m}"),
        }
    }
}

impl std::error::Error for StoreError {}

pub type SResult<T> = Result<T, StoreError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutMode {
    Overwrite,
    /// Fail with `PreconditionFailed` if the object exists.
    Create,
    /// Fail with `PreconditionFailed` unless the current etag matches.
    IfMatch(String),
}

#[derive(Debug, Clone, Default)]
pub struct GetOpts {
    pub range: Option<Range<u64>>,
    /// Return `NotModified` if the current etag equals this.
    pub if_none_match: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GetResult {
    pub data: Bytes,
    pub etag: String,
    /// Full object size (not the range length).
    pub size: u64,
    pub last_modified_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ListPage {
    pub objects: Vec<ObjectMeta>,
    /// Pass as `start_after` to continue.
    pub next: Option<String>,
}

pub trait ObjectStore: Send + Sync + 'static {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>>;
    /// Returns the new etag.
    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>>;
    /// Idempotent: deleting a missing object succeeds.
    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>>;
    /// Objects with the given prefix and key > `start_after`, in key order.
    fn list<'a>(&'a self, prefix: &'a str, start_after: Option<&'a str>, limit: usize)
    -> BoxFut<'a, SResult<ListPage>>;
}

pub type DynStore = Arc<dyn ObjectStore>;

/// Retry policy for idempotent operations (reads, deletes, lists and puts of
/// immutable, uniquely named objects). Conditional manifest updates are NOT
/// retried here; the engine resolves their ambiguity itself.
#[derive(Clone, Debug)]
pub struct Retry {
    pub attempts: u32,
    pub base_delay_ms: u64,
}

impl Default for Retry {
    fn default() -> Self {
        Retry { attempts: 5, base_delay_ms: 50 }
    }
}

impl Retry {
    pub async fn run<T, F, Fut>(&self, mut f: F) -> SResult<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = SResult<T>>,
    {
        let mut attempt = 0;
        loop {
            match f().await {
                Err(StoreError::Unavailable(e)) => {
                    attempt += 1;
                    if attempt >= self.attempts {
                        return Err(StoreError::Unavailable(e));
                    }
                    let delay = self.base_delay_ms << (attempt - 1).min(6);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                other => return other,
            }
        }
    }
}

/// List every object under `prefix` (all pages).
pub async fn list_all(store: &dyn ObjectStore, prefix: &str, retry: &Retry) -> SResult<Vec<ObjectMeta>> {
    let mut out = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let page = retry.run(|| store.list(prefix, after.as_deref(), 1000)).await?;
        out.extend(page.objects);
        match page.next {
            Some(n) => after = Some(n),
            None => return Ok(out),
        }
    }
}

/// Clamp a requested range to an object of `size` bytes (S3 semantics:
/// a range starting past the end is an error; an end past the end is
/// truncated).
pub(crate) fn clamp_range(range: &Option<Range<u64>>, size: u64) -> SResult<Range<u64>> {
    match range {
        None => Ok(0..size),
        Some(r) => {
            if r.start > r.end || (r.start >= size && !(r.start == 0 && size == 0)) {
                return Err(StoreError::Fatal("invalid range".into()));
            }
            Ok(r.start..r.end.min(size))
        }
    }
}
