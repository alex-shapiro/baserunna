//! Streaming blob request/response bodies, request body tracking, and HTTP
//! Range parsing.

use crate::engine::blob::{self, BlobWriter, chunk_plan, read_chunk};
use crate::engine::{BlobRef, Shared};
use crate::error::{Error, Result};
use crate::store::BoxFut;
use axum::body::Body;
use bytes::Bytes;
use http_body::Frame;
use http_body_util::BodyExt;
use std::collections::VecDeque;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Instant;

/// Streams a byte range of a blob chunk by chunk, periodically pinning the
/// blob so garbage collection cannot remove it during a long read.
pub struct BlobBody {
    shared: Arc<Shared>,
    db: String,
    blob: BlobRef,
    plan: VecDeque<(u64, Range<u64>)>,
    remaining: u64,
    fut: Option<BoxFut<'static, Result<Bytes>>>,
    pinned_at: Instant,
    pin_now: bool,
}

impl BlobBody {
    pub fn new(shared: Arc<Shared>, db: &str, blob: BlobRef, range: Range<u64>) -> BlobBody {
        let plan = chunk_plan(shared.cfg.blob_chunk_size as u64, range.clone()).into();
        BlobBody {
            shared,
            db: db.to_string(),
            blob,
            plan,
            remaining: range.end - range.start,
            fut: None,
            pinned_at: Instant::now(),
            pin_now: false,
        }
    }

    /// Pins the blob before reading the first chunk, for blobs already marked obsolete.
    pub fn pinned_first(mut self) -> BlobBody {
        self.pin_now = true;
        self
    }
}

impl http_body::Body for BlobBody {
    type Data = Bytes;
    type Error = Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Bytes>, Error>>> {
        let this = self.get_mut();
        loop {
            if let Some(f) = this.fut.as_mut() {
                return match f.as_mut().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(r) => {
                        this.fut = None;
                        if let Ok(b) = &r {
                            this.remaining = this.remaining.saturating_sub(b.len() as u64);
                        }
                        Poll::Ready(Some(r.map(Frame::data)))
                    }
                };
            }
            match this.plan.pop_front() {
                None => return Poll::Ready(None),
                Some((ci, r)) => {
                    let shared = this.shared.clone();
                    let db = this.db.clone();
                    let blob = this.blob.clone();
                    let interval = Duration::from_millis(blob::pin_interval_ms(&shared.cfg));
                    let pin = std::mem::take(&mut this.pin_now) || this.pinned_at.elapsed() >= interval;
                    if pin {
                        this.pinned_at = Instant::now();
                    }
                    this.fut = Some(Box::pin(async move {
                        if pin && let Err(e) = blob::pin(&shared, &db, &blob.id).await {
                            crate::warn!("could not pin blob {} of {db} for a long read: {e}", blob.id);
                        }
                        read_chunk(&shared, &db, &blob, ci, r).await
                    }));
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.fut.is_none() && self.plan.is_empty()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.remaining)
    }
}

/// Uploaded request body, buffered in memory when small or written as a blob.
pub enum Upload {
    Small(Bytes),
    Blob(BlobRef),
}

/// Reads `body`, buffering up to `inline_max` bytes in memory unless `force_blob`
/// is set, and streaming larger payloads into a blob.
pub async fn read_upload<F: std::future::Future<Output = BlobWriter>>(
    body: Body,
    inline_max: usize,
    force_blob: bool,
    writer: impl FnOnce() -> F,
) -> Result<Upload> {
    let mut body = body;
    let mut buf: Vec<u8> = Vec::new();
    let mut w: Option<BlobWriter> = None;
    let mut make = Some(writer);
    if force_blob && let Some(f) = make.take() {
        w = Some(f().await);
    }
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| Error::invalid(format!("could not read body: {e}")))?;
        let Ok(data) = frame.into_data() else { continue };
        match w.as_mut() {
            Some(w) => w.write(&data).await?,
            None => {
                buf.extend_from_slice(&data);
                if buf.len() > inline_max {
                    let mut nw = make.take().ok_or_else(|| Error::internal("writer"))?().await;
                    nw.write(&buf).await?;
                    buf = Vec::new();
                    w = Some(nw);
                }
            }
        }
    }
    match w {
        Some(w) => Ok(Upload::Blob(w.finish().await?)),
        None => Ok(Upload::Small(Bytes::from(buf))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unsatisfiable;

/// A request body that records in `read` whether it was read to its end.
pub struct Tracked {
    inner: Body,
    read: Arc<std::sync::atomic::AtomicBool>,
}

impl Tracked {
    pub fn new(inner: Body) -> (Tracked, Arc<std::sync::atomic::AtomicBool>) {
        let read = Arc::new(std::sync::atomic::AtomicBool::new(http_body::Body::is_end_stream(&inner)));
        (Tracked { inner, read: read.clone() }, read)
    }
}

impl http_body::Body for Tracked {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let polled = Pin::new(&mut self.inner).poll_frame(cx);
        // Readers such as hyper's client stop at `is_end_stream` after the
        // last frame instead of polling for the end.
        let ended = match &polled {
            Poll::Ready(None) => true,
            Poll::Ready(Some(Ok(_))) => self.inner.is_end_stream(),
            _ => false,
        };
        if ended {
            self.read.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// Parses a single-range `Range` header against an object of `size` bytes.
/// Returns `Ok(None)` when no single-range header is present.
pub fn parse_range(h: Option<&str>, size: u64) -> std::result::Result<Option<Range<u64>>, Unsatisfiable> {
    let Some(h) = h else { return Ok(None) };
    let Some(spec) = h.trim().strip_prefix("bytes=") else { return Ok(None) };
    if spec.contains(',') {
        return Ok(None);
    }
    let (a, b) = spec.split_once('-').ok_or(Unsatisfiable)?;
    let (a, b) = (a.trim(), b.trim());
    let r = if a.is_empty() {
        let n: u64 = b.parse().map_err(|_| Unsatisfiable)?;
        if n == 0 {
            return Err(Unsatisfiable);
        }
        size.saturating_sub(n)..size
    } else {
        let start: u64 = a.parse().map_err(|_| Unsatisfiable)?;
        let end = if b.is_empty() {
            size
        } else {
            let e: u64 = b.parse().map_err(|_| Unsatisfiable)?;
            if e < start {
                return Err(Unsatisfiable);
            }
            (e + 1).min(size)
        };
        start..end
    };
    if r.start >= size || r.start >= r.end {
        return Err(Unsatisfiable);
    }
    Ok(Some(r))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges() {
        assert_eq!(parse_range(None, 10), Ok(None));
        assert_eq!(parse_range(Some("bytes=0-4"), 10), Ok(Some(0..5)));
        assert_eq!(parse_range(Some("bytes=5-"), 10), Ok(Some(5..10)));
        assert_eq!(parse_range(Some("bytes=-3"), 10), Ok(Some(7..10)));
        assert_eq!(parse_range(Some("bytes=-30"), 10), Ok(Some(0..10)));
        assert_eq!(parse_range(Some("bytes=8-100"), 10), Ok(Some(8..10)));
        assert_eq!(parse_range(Some("bytes=10-"), 10), Err(Unsatisfiable));
        assert_eq!(parse_range(Some("bytes=5-2"), 10), Err(Unsatisfiable));
        assert_eq!(parse_range(Some("bytes=0-1,3-4"), 10), Ok(None));
        assert_eq!(parse_range(Some("items=0-1"), 10), Ok(None));
        assert_eq!(parse_range(Some("bytes=x-1"), 10), Err(Unsatisfiable));
        assert_eq!(parse_range(Some("bytes=0-0"), 0), Err(Unsatisfiable));
    }
}
