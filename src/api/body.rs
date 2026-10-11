//! Streaming request/response bodies for blobs, and HTTP Range parsing.

use crate::engine::blob::{BlobWriter, chunk_plan, read_chunk};
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

/// Streams a byte range of a blob chunk by chunk.
pub struct BlobBody {
    shared: Arc<Shared>,
    db: String,
    blob: BlobRef,
    plan: VecDeque<(u64, Range<u64>)>,
    remaining: u64,
    fut: Option<BoxFut<'static, Result<Bytes>>>,
}

impl BlobBody {
    pub fn new(shared: Arc<Shared>, db: &str, blob: BlobRef, range: Range<u64>) -> BlobBody {
        let plan = chunk_plan(shared.cfg.blob_chunk_size as u64, range.clone()).into();
        BlobBody { shared, db: db.to_string(), blob, plan, remaining: range.end - range.start, fut: None }
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
                    this.fut = Some(Box::pin(async move { read_chunk(&shared, &db, &blob, ci, r).await }));
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

/// Read a request body into a blob or (if small) into memory.
pub enum Upload {
    Small(Bytes),
    Blob(BlobRef),
}

/// Stream `body` into storage. Bodies up to `inline_max` stay in memory
/// (unless `force_blob`); larger ones are written as a blob as they arrive.
pub async fn read_upload(
    body: Body,
    inline_max: usize,
    force_blob: bool,
    writer: impl FnOnce() -> BlobWriter,
) -> Result<Upload> {
    let mut body = body;
    let mut buf: Vec<u8> = Vec::new();
    let mut w: Option<BlobWriter> = None;
    let mut make = Some(writer);
    if force_blob {
        w = make.take().map(|f| f());
    }
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| Error::invalid(format!("could not read body: {e}")))?;
        let Ok(data) = frame.into_data() else { continue };
        match w.as_mut() {
            Some(w) => w.write(&data).await?,
            None => {
                buf.extend_from_slice(&data);
                if buf.len() > inline_max {
                    let mut nw = make.take().map(|f| f()).ok_or_else(|| Error::internal("writer"))?;
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

/// The requested range cannot be served (HTTP 416).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unsatisfiable;

/// Parse a single-range `Range` header against an object of `size` bytes.
/// `Ok(None)` = no (usable) range header.
pub fn parse_range(h: Option<&str>, size: u64) -> std::result::Result<Option<Range<u64>>, Unsatisfiable> {
    let Some(h) = h else { return Ok(None) };
    let Some(spec) = h.trim().strip_prefix("bytes=") else { return Ok(None) };
    if spec.contains(',') {
        return Ok(None); // multi-range: serve the whole object
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
