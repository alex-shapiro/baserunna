//! Owner instance: one owner's databases, run on an executor that the host drives.
//!
//! The host calls `owner_start` once, then delivers work with
//! `owner_request` and `owner_complete` and runs the executor with
//! `owner_poll`. Store calls start with a host call that returns a ticket;
//! the task that waits on a ticket resumes when the host completes it.

use baserunna::engine::{Consistency, Engine, Isolation, Value};
use baserunna::env::SIM_TASK_POLLS;
use baserunna::store::{BoxFut, GetOpts, GetResult, ListPage, ObjectStore, PutMode, SResult, StoreError};
use bytes::Bytes;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};

mod host {
    #[link(wasm_import_module = "baserunna_owner")]
    unsafe extern "C" {
        /// Starts a store GET. The completion holds a `GetMeta` header and then the data.
        pub fn start_get(key_ptr: *const u8, key_len: u32, start: u64, end: u64, ranged: u32) -> u32;
        /// Starts a store PUT. The completion holds the new etag.
        pub fn start_put(
            key_ptr: *const u8,
            key_len: u32,
            data_ptr: *const u8,
            data_len: u32,
            mode: u32,
            etag_ptr: *const u8,
            etag_len: u32,
        ) -> u32;
        /// Starts a store DELETE. The completion is empty.
        pub fn start_delete(key_ptr: *const u8, key_len: u32) -> u32;
        /// Starts a store LIST. The completion holds the encoded page (see `decode_list`).
        pub fn start_list(
            prefix_ptr: *const u8,
            prefix_len: u32,
            after_ptr: *const u8,
            after_len: u32,
            has_after: u32,
            limit: u32,
        ) -> u32;
        /// Reports that this node's lease of database `db` has `left_ms` left; 0 means none.
        pub fn lease_noted(db_ptr: *const u8, db_len: u32, left_ms: u64);
        /// Sends the response to request `request_id`.
        pub fn respond(request_id: u64, status: u32, body_ptr: *const u8, body_len: u32);
    }
}

/// Result of a host call, as the host delivers it.
struct TicketDone {
    code: i64,
    data: Vec<u8>,
}

#[derive(Default)]
struct TicketSlot {
    done: Option<TicketDone>,
    waker: Option<Waker>,
}

/// Engine and executor of this owner instance.
struct OwnerRuntime {
    rt: tokio::runtime::Runtime,
    engine: Arc<Engine>,
}

thread_local! {
    static TICKETS: RefCell<HashMap<u32, TicketSlot>> = RefCell::new(HashMap::new());
    /// Tickets whose waiting task went away before completion.
    static ABANDONED: RefCell<std::collections::HashSet<u32>> = RefCell::new(std::collections::HashSet::new());
    /// PUT bodies that the host reads in place, by ticket, until it releases them.
    static PUT_BODIES: RefCell<HashMap<u32, Bytes>> = RefCell::new(HashMap::new());
    static OWNER_RUNTIME: RefCell<Option<std::rc::Rc<OwnerRuntime>>> = const { RefCell::new(None) };
}

/// Future of a host call that the host completes with `owner_complete`.
struct Ticket(u32);

impl Future for Ticket {
    type Output = TicketDone;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<TicketDone> {
        TICKETS.with(|t| {
            let mut t = t.borrow_mut();
            let slot = t.entry(self.0).or_default();
            match slot.done.take() {
                Some(done) => {
                    t.remove(&self.0);
                    Poll::Ready(done)
                }
                None => {
                    slot.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
    }
}

impl Ticket {
    /// Keeps `body` alive and unchanged until the host releases it with `owner_release_body`.
    fn hold(self, body: Bytes) -> Self {
        PUT_BODIES.with(|b| b.borrow_mut().insert(self.0, body));
        self
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        TICKETS.with(|t| {
            let mut t = t.borrow_mut();
            if !t.remove(&self.0).is_some_and(|slot| slot.done.is_some()) {
                ABANDONED.with(|a| a.borrow_mut().insert(self.0));
            }
        });
    }
}

fn store_error(code: i64) -> StoreError {
    match code {
        -1 => StoreError::NotFound,
        -2 => StoreError::PreconditionFailed,
        -3 => StoreError::NotModified,
        -4 => StoreError::Unavailable("store unavailable".into()),
        _ => StoreError::Fatal("store error".into()),
    }
}

/// Object store whose calls the host runs, one ticket per call.
struct TicketStore;

/// Length of the `GetMeta` header of a GET completion: size, last modified, etag length.
const GET_META_LEN: usize = 20;

impl ObjectStore for TicketStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        Box::pin(async move {
            let (start, end, ranged) = opts.range.map_or((0, 0, 0), |r| (r.start, r.end, 1));
            let ticket = Ticket(unsafe { host::start_get(key.as_ptr(), key.len() as u32, start, end, ranged) });
            let done = ticket.await;
            if done.code < 0 {
                return Err(store_error(done.code));
            }
            let d = &done.data;
            if d.len() < GET_META_LEN {
                return Err(StoreError::Fatal("short get completion".into()));
            }
            let size = u64::from_le_bytes(d[0..8].try_into().unwrap());
            let last_modified_ms = u64::from_le_bytes(d[8..16].try_into().unwrap());
            let etag_len = u32::from_le_bytes(d[16..20].try_into().unwrap()) as usize;
            let etag = String::from_utf8_lossy(&d[GET_META_LEN..GET_META_LEN + etag_len]).into_owned();
            let data = Bytes::from(done.data).slice(GET_META_LEN + etag_len..);
            Ok(GetResult { data, etag, size, last_modified_ms })
        })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move {
            let (mode, etag) = match &mode {
                PutMode::Overwrite => (0, ""),
                PutMode::Create => (1, ""),
                PutMode::IfMatch(e) => (2, e.as_str()),
            };
            let body = data.clone();
            let ticket = Ticket(unsafe {
                host::start_put(
                    key.as_ptr(),
                    key.len() as u32,
                    data.as_ptr(),
                    data.len() as u32,
                    mode,
                    etag.as_ptr(),
                    etag.len() as u32,
                )
            })
            .hold(body);
            let done = ticket.await;
            drop(data);
            if done.code < 0 {
                return Err(store_error(done.code));
            }
            Ok(String::from_utf8_lossy(&done.data).into_owned())
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        Box::pin(async move {
            let done = Ticket(unsafe { host::start_delete(key.as_ptr(), key.len() as u32) }).await;
            if done.code < 0 { Err(store_error(done.code)) } else { Ok(()) }
        })
    }

    fn list<'a>(&'a self, prefix: &'a str, start_after: Option<&'a str>, limit: usize) -> BoxFut<'a, SResult<ListPage>> {
        Box::pin(async move {
            let after = start_after.unwrap_or("");
            let ticket = Ticket(unsafe {
                host::start_list(
                    prefix.as_ptr(),
                    prefix.len() as u32,
                    after.as_ptr(),
                    after.len() as u32,
                    start_after.is_some() as u32,
                    limit.min(u32::MAX as usize) as u32,
                )
            });
            let done = ticket.await;
            if done.code < 0 {
                return Err(store_error(done.code));
            }
            decode_list(&done.data).ok_or_else(|| StoreError::Fatal("bad list completion".into()))
        })
    }
}

/// Decodes a list page: next (flag and piece), object count, then key, size, etag, and last modified of each.
fn decode_list(mut buf: &[u8]) -> Option<ListPage> {
    fn u64_at(buf: &mut &[u8]) -> Option<u64> {
        let v = u64::from_le_bytes(buf.get(..8)?.try_into().ok()?);
        *buf = &buf[8..];
        Some(v)
    }
    let string = |buf: &mut &[u8]| take_piece(buf).map(|p| String::from_utf8_lossy(&p).into_owned());
    let has_next = *buf.first()? == 1;
    buf = &buf[1..];
    let next = string(&mut buf)?;
    let count = u32::from_le_bytes(buf.get(..4)?.try_into().ok()?);
    buf = &buf[4..];
    let mut objects = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let key = string(&mut buf)?;
        let size = u64_at(&mut buf)?;
        let etag = string(&mut buf)?;
        let last_modified_ms = u64_at(&mut buf)?;
        objects.push(baserunna::store::ObjectMeta { key, size, etag, last_modified_ms });
    }
    Some(ListPage { objects, next: has_next.then_some(next) })
}

/// Starts the engine of this owner instance at host time `now_ms`.
#[unsafe(no_mangle)]
pub extern "C" fn owner_start(seed: u64, now_ms: u64) {
    let _ = baserunna::engine::OWN_LEASE_SINK.set(|db, left_ms| unsafe { host::lease_noted(db.as_ptr(), db.len() as u32, left_ms) });
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
    let engine = rt.block_on(async {
        let env = baserunna::env::Env::sim(seed, now_ms, tokio::time::Instant::now(), 0);
        Engine::new(Arc::new(TicketStore), Arc::new(env), baserunna::engine::EngineConfig::default(), "owner".into())
    });
    OWNER_RUNTIME.with(|r| *r.borrow_mut() = Some(std::rc::Rc::new(OwnerRuntime { rt, engine })));
}

fn owner_runtime() -> std::rc::Rc<OwnerRuntime> {
    OWNER_RUNTIME.with(|r| r.borrow().clone()).expect("owner_start runs first")
}

/// Completes host call `ticket` with `code` and the `len` bytes at `ptr` from `guest_alloc`.
///
/// With a negative `code`, the buffer is freed unread.
///
/// # Safety
/// `ptr` and `len` must come from one `guest_alloc(len)` call. With a code of 0 or more, all
/// `len` bytes must be written.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn owner_complete(ticket: u32, code: i64, ptr: *mut u8, len: u32) {
    let data = match (len, code < 0) {
        (0, _) => Vec::new(),
        (_, true) => {
            drop(unsafe { Vec::from_raw_parts(ptr, 0, len as usize) });
            Vec::new()
        }
        (_, false) => unsafe { Vec::from_raw_parts(ptr, len as usize, len as usize) },
    };
    if ABANDONED.with(|a| a.borrow_mut().remove(&ticket)) {
        return;
    }
    TICKETS.with(|t| {
        let mut t = t.borrow_mut();
        let slot = t.entry(ticket).or_default();
        slot.done = Some(TicketDone { code, data });
        if let Some(w) = slot.waker.take() {
            w.wake();
        }
    });
}

/// Releases the PUT body of `ticket`, which the host no longer reads.
#[unsafe(no_mangle)]
pub extern "C" fn owner_release_body(ticket: u32) {
    PUT_BODIES.with(|b| b.borrow_mut().remove(&ticket));
}

/// Request kinds of the KV protocol that the owner instance answers.
const KV_GET: u8 = 1;
const KV_PUT: u8 = 2;

/// Reads a `u32` length and that many bytes from the front of `buf`.
fn take_piece(buf: &mut &[u8]) -> Option<Vec<u8>> {
    let len = u32::from_le_bytes(buf.get(..4)?.try_into().ok()?) as usize;
    let piece = buf.get(4..4 + len)?.to_vec();
    *buf = &buf[4 + len..];
    Some(piece)
}

async fn kv_request(engine: Arc<Engine>, body: Vec<u8>) -> (u32, Vec<u8>) {
    let mut rest = &body[1..];
    let (Some(db), Some(key)) = (take_piece(&mut rest), take_piece(&mut rest)) else {
        return (400, b"bad request".to_vec());
    };
    let db = engine.db(&String::from_utf8_lossy(&db));
    let out = async {
        db.open(body[0] == KV_PUT).await?;
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await?;
        match body[0] {
            KV_GET => Ok::<_, baserunna::Error>(match t.get(&key).await? {
                Some(v) => match v.value {
                    Value::Inline(b) => (200, b.to_vec()),
                    _ => (200, Vec::new()),
                },
                None => (404, Vec::new()),
            }),
            KV_PUT => {
                t.put(Bytes::from(key), Value::Inline(Bytes::copy_from_slice(rest)))?;
                db.commit(t.into_request()).await?;
                Ok((200, Vec::new()))
            }
            _ => Ok((400, b"unknown request kind".to_vec())),
        }
    }
    .await;
    out.unwrap_or_else(|e| (500, format!("{e:?}").into_bytes()))
}

/// Starts request `request_id` with the `len` bytes at `ptr` from `guest_alloc`.
///
/// # Safety
/// `ptr` and `len` must come from one `guest_alloc(len)` call, with all `len` bytes written.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn owner_request(request_id: u64, ptr: *mut u8, len: u32) {
    let body = unsafe { Vec::from_raw_parts(ptr, len as usize, len as usize) };
    let owner = owner_runtime();
    let engine = owner.engine.clone();
    let env = engine.shared.env.clone();
    let _guard = owner.rt.enter();
    env.spawn(async move {
        let (status, out) = if body.is_empty() { (400, b"empty request".to_vec()) } else { kv_request(engine, body).await };
        unsafe { host::respond(request_id, status, out.as_ptr(), out.len() as u32) };
    });
}

/// Runs every ready task at host time `now_ms` until no task makes progress.
///
/// Returns the count of live tasks, so the host knows whether timers may be pending.
#[unsafe(no_mangle)]
pub extern "C" fn owner_poll(now_ms: u64) -> u32 {
    let owner = owner_runtime();
    owner.rt.block_on(async {
        let behind = now_ms.saturating_sub(owner.engine.shared.env.now_ms());
        if behind > 0 {
            tokio::time::advance(std::time::Duration::from_millis(behind)).await;
        }
        let mut quiet_steps = 0;
        while quiet_steps < 2 {
            let before = SIM_TASK_POLLS.load(Ordering::Relaxed);
            tokio::task::yield_now().await;
            quiet_steps = if SIM_TASK_POLLS.load(Ordering::Relaxed) == before { quiet_steps + 1 } else { 0 };
        }
    });
    owner.engine.shared.env.task_count() as u32
}
