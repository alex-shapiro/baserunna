//! Object storage abstraction.

pub mod conformance;
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
    /// Conditional put failed.
    PreconditionFailed,
    /// Conditional get matched `if_none_match`.
    NotModified,
    /// Transient failure; the operation may or may not have applied.
    Unavailable(String),
    /// Permanent failure.
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
    /// Total object size in bytes.
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

/// Strongly consistent object store shared by all nodes in a cluster.
///
/// Every operation observes all writes completed before it started. Each write produces a
/// distinct etag that is returned by `put`, `get`, and `list`. [`conformance::run`] verifies
/// this contract against a live store.
///
/// Keys and `start_after` cursors consist of `/`-separated, non-empty segments of
/// [`is_valid_key_char`] characters. A key cannot be a `/`-delimited prefix of another key.
pub trait ObjectStore: Send + Sync + 'static {
    /// Reads an object, or the slice in `opts.range` clamped to its size. Returns
    /// [`StoreError::Fatal`] if a non-empty range starts at or past the end of the object, or if
    /// an empty range starts past the end of the object.
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>>;
    /// Writes an object and returns its etag. At most 1 concurrent conditional write with a
    /// given precondition can succeed.
    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>>;
    /// Deletes an object if it exists.
    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>>;
    /// Lists up to `limit` objects under `prefix` after `start_after`, sorted by key. `prefix`
    /// must be empty or end with `/`.
    fn list<'a>(&'a self, prefix: &'a str, start_after: Option<&'a str>, limit: usize)
    -> BoxFut<'a, SResult<ListPage>>;
    /// Granularity of `last_modified_ms` in milliseconds. Timestamps are rounded down to a
    /// multiple of this value on a single store clock.
    fn time_precision_ms(&self) -> u64 {
        1_000
    }
}

pub type DynStore = Arc<dyn ObjectStore>;

pub fn is_valid_key_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')
}

/// Exponential-backoff retry policy for idempotent store operations.
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

/// List all objects under `prefix`.
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

/// Clamps `range` to `[0, size)`, returning [`StoreError::Fatal`] if the start is out of bounds.
pub(crate) fn clamp_range(range: &Option<Range<u64>>, size: u64) -> SResult<Range<u64>> {
    match range {
        None => Ok(0..size),
        Some(r) if r.start <= r.end && (r.start < size || r.start == r.end && r.start <= size) => {
            Ok(r.start..r.end.min(size))
        }
        Some(_) => Err(StoreError::Fatal("invalid range".into())),
    }
}
