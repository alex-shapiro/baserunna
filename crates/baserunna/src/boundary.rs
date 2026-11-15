//! The byte channel between the main process and a worker: binary frames that carry many
//! HTTP request streams, each with a credit window on its body in both directions.
//!
//! A request stream is `Open`, then body `Chunk`s, then `End`, from the main process. The
//! worker answers with `Head`, then body `Chunk`s, then `End`. A side may send a chunk only
//! while it holds a credit; the receiver returns one `Credit` for each chunk that its reader
//! takes. So each side holds at most [`WINDOW_CHUNKS`] chunks of [`CHUNK_MAX`] bytes for each
//! direction of each stream.

use crate::error::{Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc, oneshot};

/// The largest body chunk in one frame.
pub const CHUNK_MAX: usize = 64 * 1024;

/// The chunks that one direction of one stream may have in flight.
pub const WINDOW_CHUNKS: usize = 2;

/// The largest `Open` or `Head` frame: method, URI, headers, and the request context.
pub const HEAD_MAX: usize = 256 * 1024;

/// The kind of a [`Frame`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    /// A whole job frame of the job protocol in `worker.rs`.
    Job = 0,
    /// The head of a request stream.
    Open = 1,
    /// Body bytes of a request or a response stream.
    Chunk = 2,
    /// The end of a body.
    End = 3,
    /// One more chunk that the receiver of a body admits.
    Credit = 4,
    /// The head of a response stream.
    Head = 5,
    /// The end of a stream with an error; the data is the error message.
    Reset = 6,
}

impl FrameKind {
    fn from_u8(b: u8) -> Option<FrameKind> {
        use FrameKind::*;
        [Job, Open, Chunk, End, Credit, Head, Reset].into_iter().find(|k| *k as u8 == b)
    }

    /// The largest data length of a frame of this kind, or `None` for a job frame.
    fn data_max(self) -> Option<usize> {
        match self {
            FrameKind::Job => None,
            FrameKind::Open | FrameKind::Head => Some(HEAD_MAX),
            FrameKind::Chunk => Some(CHUNK_MAX),
            FrameKind::End | FrameKind::Credit => Some(0),
            FrameKind::Reset => Some(4096),
        }
    }
}

/// One frame on the channel: a kind, the stream or job ID, and the data.
#[derive(Debug)]
pub struct Frame {
    pub kind: FrameKind,
    pub id: u64,
    pub data: Bytes,
}

impl Frame {
    pub fn new(kind: FrameKind, id: u64, data: Bytes) -> Frame {
        Frame { kind, id, data }
    }
}

/// The sender of frames to the other side of a channel.
pub type Outbox = mpsc::UnboundedSender<Frame>;

/// Writes the frames of `frames` to `w`, with one flush for all the frames that wait at once.
pub async fn write_frames(
    w: impl tokio::io::AsyncWrite + Unpin,
    mut frames: mpsc::UnboundedReceiver<Frame>,
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut w = tokio::io::BufWriter::with_capacity(2 * CHUNK_MAX, w);
    while let Some(first) = frames.recv().await {
        let mut next = Some(first);
        while let Some(f) = next {
            w.write_u8(f.kind as u8).await?;
            w.write_u64_le(f.id).await?;
            w.write_u64_le(f.data.len() as u64).await?;
            w.write_all(&f.data).await?;
            next = frames.try_recv().ok();
        }
        w.flush().await?;
    }
    Ok(())
}

/// Reads one frame from `r`. A frame longer than its kind allows is an error.
pub async fn read_frame(r: &mut (impl tokio::io::AsyncRead + Unpin)) -> std::io::Result<Frame> {
    use tokio::io::AsyncReadExt;
    let bad = |m: String| std::io::Error::new(std::io::ErrorKind::InvalidData, m);
    let kind = r.read_u8().await?;
    let kind = FrameKind::from_u8(kind).ok_or_else(|| bad(format!("unknown frame kind {kind}")))?;
    let id = r.read_u64_le().await?;
    let len = r.read_u64_le().await? as usize;
    if kind.data_max().is_some_and(|max| len > max) {
        return Err(bad(format!("a {kind:?} frame of {len} bytes is too long")));
    }
    let mut data = vec![0u8; len];
    r.read_exact(&mut data).await?;
    Ok(Frame { kind, id, data: data.into() })
}

/// The head of a request stream.
#[derive(Clone, Debug, PartialEq)]
pub struct RequestHead {
    pub method: String,
    pub uri: String,
    pub headers: Vec<(String, Vec<u8>)>,
    pub deadline_ms: u64,
    /// The encoded request context (principal, database record, access) of the worker's router.
    pub context: Bytes,
}

/// The head of a response stream.
#[derive(Clone, Debug, PartialEq)]
pub struct ResponseHead {
    pub status: u16,
    pub headers: Vec<(String, Vec<u8>)>,
}

fn put_bytes(b: &mut BytesMut, v: &[u8]) {
    b.put_u32_le(v.len() as u32);
    b.put_slice(v);
}

fn get_bytes(b: &mut Bytes) -> Result<Bytes> {
    let short = || Error::internal("a boundary frame is too short");
    if b.remaining() < 4 {
        return Err(short());
    }
    let n = b.get_u32_le() as usize;
    if b.remaining() < n {
        return Err(short());
    }
    Ok(b.split_to(n))
}

fn get_string(b: &mut Bytes) -> Result<String> {
    String::from_utf8(get_bytes(b)?.to_vec()).map_err(|_| Error::internal("a boundary frame has invalid UTF-8"))
}

fn put_headers(b: &mut BytesMut, headers: &[(String, Vec<u8>)]) {
    b.put_u32_le(headers.len() as u32);
    for (k, v) in headers {
        put_bytes(b, k.as_bytes());
        put_bytes(b, v);
    }
}

fn get_headers(b: &mut Bytes) -> Result<Vec<(String, Vec<u8>)>> {
    if b.remaining() < 4 {
        return Err(Error::internal("a boundary frame is too short"));
    }
    let n = b.get_u32_le() as usize;
    let mut headers = Vec::with_capacity(n.min(256));
    for _ in 0..n {
        headers.push((get_string(b)?, get_bytes(b)?.to_vec()));
    }
    Ok(headers)
}

/// Copies the headers of an HTTP message into their frame form.
pub fn header_pairs(h: &http::HeaderMap) -> Vec<(String, Vec<u8>)> {
    h.iter().map(|(k, v)| (k.as_str().to_string(), v.as_bytes().to_vec())).collect()
}

/// Adds the frame form `pairs` of headers to `h`, and skips each invalid one.
pub fn append_headers(h: &mut http::HeaderMap, pairs: Vec<(String, Vec<u8>)>) {
    for (k, v) in pairs {
        if let (Ok(k), Ok(v)) = (http::HeaderName::try_from(k), http::HeaderValue::from_bytes(&v)) {
            h.append(k, v);
        }
    }
}

impl RequestHead {
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::new();
        put_bytes(&mut b, self.method.as_bytes());
        put_bytes(&mut b, self.uri.as_bytes());
        put_headers(&mut b, &self.headers);
        b.put_u64_le(self.deadline_ms);
        put_bytes(&mut b, &self.context);
        b.freeze()
    }

    pub fn decode(mut b: Bytes) -> Result<RequestHead> {
        let (method, uri, headers) = (get_string(&mut b)?, get_string(&mut b)?, get_headers(&mut b)?);
        if b.remaining() < 8 {
            return Err(Error::internal("a boundary frame is too short"));
        }
        let deadline_ms = b.get_u64_le();
        Ok(RequestHead { method, uri, headers, deadline_ms, context: get_bytes(&mut b)? })
    }
}

impl ResponseHead {
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u16_le(self.status);
        put_headers(&mut b, &self.headers);
        b.freeze()
    }

    pub fn decode(mut b: Bytes) -> Result<ResponseHead> {
        if b.remaining() < 2 {
            return Err(Error::internal("a boundary frame is too short"));
        }
        let status = b.get_u16_le();
        Ok(ResponseHead { status, headers: get_headers(&mut b)? })
    }
}

/// The receiving end of one body: the chunks that the other side sent and that the reader has
/// not taken yet.
type BodyInbox = mpsc::Sender<std::result::Result<Bytes, Error>>;

/// Returns an empty body inbox and the [`axum::body::Body`] that reads it. The body sends one
/// `Credit` frame for stream `id` to `out` for each chunk that its reader takes.
pub fn credited_body(id: u64, out: Outbox) -> (BodyInbox, axum::body::Body) {
    let (tx, rx) = mpsc::channel::<std::result::Result<Bytes, Error>>(WINDOW_CHUNKS);
    let stream = futures_util::stream::unfold((rx, out), move |(mut rx, out)| async move {
        let item = rx.recv().await?;
        if item.is_ok() {
            let _ = out.send(Frame::new(FrameKind::Credit, id, Bytes::new()));
        }
        Some((item, (rx, out)))
    });
    (tx, axum::body::Body::from_stream(stream))
}

/// Sends `body` as `Chunk` frames of stream `id` to `out`, each after it takes one credit from
/// `credits`, then sends `End`. A body error sends `Reset`.
pub async fn send_body(id: u64, body: axum::body::Body, credits: Arc<Semaphore>, out: Outbox) {
    use futures_util::StreamExt;
    let mut body = body.into_data_stream();
    while let Some(item) = body.next().await {
        let mut data = match item {
            Ok(data) => data,
            Err(e) => {
                let _ = out.send(Frame::new(FrameKind::Reset, id, reset_message(&e.to_string())));
                return;
            }
        };
        while !data.is_empty() {
            let Ok(permit) = credits.acquire().await else { return };
            permit.forget();
            let chunk = data.split_to(data.len().min(CHUNK_MAX));
            if out.send(Frame::new(FrameKind::Chunk, id, chunk)).is_err() {
                return;
            }
        }
    }
    let _ = out.send(Frame::new(FrameKind::End, id, Bytes::new()));
}

/// Cuts `m` to the length that a `Reset` frame allows.
pub fn reset_message(m: &str) -> Bytes {
    let mut end = m.len().min(4096);
    while !m.is_char_boundary(end) {
        end -= 1;
    }
    Bytes::copy_from_slice(&m.as_bytes()[..end])
}

/// One stream's state on the side that receives its body.
struct StreamSlot {
    /// Waits for the response head; only the main process side has one.
    head: Option<oneshot::Sender<Result<ResponseHead>>>,
    /// The inbox of the body that this side receives, until `End`.
    inbox: Option<BodyInbox>,
    /// The credits of the body that this side sends.
    send_credits: Arc<Semaphore>,
}

/// The streams of one channel, by stream ID, on either side.
#[derive(Default)]
pub struct Streams {
    slots: Mutex<HashMap<u64, StreamSlot>>,
}

impl Streams {
    /// Registers stream `id` with the inbox of the body that this side receives, and returns
    /// the credits of the body that this side sends.
    pub fn register(
        &self,
        id: u64,
        inbox: Option<BodyInbox>,
        head: Option<oneshot::Sender<Result<ResponseHead>>>,
    ) -> Arc<Semaphore> {
        let send_credits = Arc::new(Semaphore::new(WINDOW_CHUNKS));
        self.slots.lock().insert(id, StreamSlot { head, inbox, send_credits: send_credits.clone() });
        send_credits
    }

    /// Forgets stream `id`, and stops the sender of its outgoing body.
    pub fn remove(&self, id: u64) {
        if let Some(slot) = self.slots.lock().remove(&id) {
            slot.send_credits.close();
        }
    }

    /// Returns the number of open streams.
    pub fn len(&self) -> usize {
        self.slots.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Applies a `Head`, `Chunk`, `End`, `Credit`, or `Reset` frame. A chunk beyond the
    /// stream's window ends the stream with an error.
    pub fn deliver(&self, f: Frame) {
        let mut slots = self.slots.lock();
        let Some(slot) = slots.get_mut(&f.id) else { return };
        match f.kind {
            FrameKind::Head => {
                if let Some(head) = slot.head.take() {
                    let _ = head.send(ResponseHead::decode(f.data));
                }
            }
            FrameKind::Chunk => {
                let over = slot.inbox.as_ref().is_some_and(|inbox| inbox.try_send(Ok(f.data)).is_err());
                if over {
                    if let Some(inbox) = slot.inbox.take() {
                        let _ = inbox.try_send(Err(Error::internal("a body chunk arrived beyond its window")));
                    }
                }
            }
            FrameKind::End => slot.inbox = None,
            FrameKind::Credit => slot.send_credits.add_permits(1),
            FrameKind::Reset => {
                let e = Error::internal(String::from_utf8_lossy(&f.data).into_owned());
                if let Some(head) = slot.head.take() {
                    let _ = head.send(Err(e.clone()));
                }
                if let Some(inbox) = slot.inbox.take() {
                    let _ = inbox.try_send(Err(e));
                }
                slot.send_credits.close();
                slots.remove(&f.id);
            }
            FrameKind::Job | FrameKind::Open => {}
        }
    }

    /// Ends every stream with error `e`, as when the other side exits.
    pub fn fail_all(&self, e: &Error) {
        for (_, mut slot) in self.slots.lock().drain() {
            if let Some(head) = slot.head.take() {
                let _ = head.send(Err(e.clone()));
            }
            if let Some(inbox) = slot.inbox.take() {
                let _ = inbox.try_send(Err(e.clone()));
            }
            slot.send_credits.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heads_round_trip() {
        let h = RequestHead {
            method: "PUT".into(),
            uri: "/v1/dbs/a/kv/k?x=1".into(),
            headers: vec![("content-type".into(), b"text/plain".to_vec())],
            deadline_ms: 1234,
            context: Bytes::from_static(b"{}"),
        };
        assert_eq!(RequestHead::decode(h.encode()).unwrap(), h);
        let r = ResponseHead { status: 201, headers: vec![("etag".into(), b"\"1\"".to_vec())] };
        assert_eq!(ResponseHead::decode(r.encode()).unwrap(), r);
    }

    #[tokio::test]
    async fn frames_over_the_length_limit_fail() {
        let mut wire = Vec::new();
        wire.push(FrameKind::Chunk as u8);
        wire.extend_from_slice(&7u64.to_le_bytes());
        wire.extend_from_slice(&((CHUNK_MAX + 1) as u64).to_le_bytes());
        let err = read_frame(&mut wire.as_slice()).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    /// A 1 MiB body crosses in chunks of at most `CHUNK_MAX`, and the sender never has more
    /// than `WINDOW_CHUNKS` chunks in flight while the reader does not read.
    #[tokio::test]
    async fn bodies_cross_within_the_window() {
        let (out, mut wire) = mpsc::unbounded_channel::<Frame>();
        let streams = Arc::new(Streams::default());
        let (inbox, body) = credited_body(1, out.clone());
        let credits = streams.register(1, Some(inbox), None);
        let sent = vec![5u8; 1 << 20];
        tokio::spawn(send_body(1, axum::body::Body::from(sent.clone()), credits, out.clone()));
        // With no reader, only the window's chunks leave the sender.
        let mut in_flight = Vec::new();
        for _ in 0..WINDOW_CHUNKS {
            in_flight.push(wire.recv().await.unwrap());
        }
        tokio::task::yield_now().await;
        assert!(wire.try_recv().is_err());
        for f in in_flight {
            assert!(f.data.len() <= CHUNK_MAX);
            streams.deliver(f);
        }
        // The reader takes chunks; each take returns a credit, and the router of frames
        // delivers chunks and credits to the stream.
        let reader = tokio::spawn(async move { http_body_util::BodyExt::collect(body).await.map(|b| b.to_bytes()) });
        while let Some(f) = wire.recv().await {
            let end = f.kind == FrameKind::End;
            assert!(f.data.len() <= CHUNK_MAX);
            streams.deliver(f);
            if end {
                break;
            }
        }
        assert_eq!(reader.await.unwrap().unwrap().to_vec(), sent);
    }
}
