//! Owner instances: one guest instance per owner, driven by one host task.
//!
//! The host task delivers requests and store call completions into the
//! instance and runs its executor with `owner_poll`. Store calls from the
//! guest return a ticket at once; the host runs the call and completes the
//! ticket in a later poll.

use super::{GuestState, OwnerLimiter, Rng, link_host_calls, read_guest, store_code};
use crate::error::{Error, Result};
use crate::store::{DynStore, GetOpts, PutMode};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use wasmtime::{Caller, Config, Engine, Instance, Linker, Module, Store, TypedFunc};

/// Linear memory bound of one owner instance.
pub const OWNER_INSTANCE_MEMORY_BOUND: usize = 512 << 20;
/// Host poll interval of an instance that has live tasks, which may wait on timers.
const TIMER_TICK: std::time::Duration = std::time::Duration::from_millis(5);
/// Requests that may wait for one owner instance before callers wait.
const REQUEST_QUEUE: usize = 256;
/// Store calls of one owner instance that may run at once; later calls wait for a slot.
const STORE_CALLS_IN_FLIGHT: usize = 16;

/// Response of an owner instance to one request.
#[derive(Debug)]
pub struct OwnerResponse {
    pub status: u32,
    pub body: Vec<u8>,
}

/// Largest GET header: size, last modified, etag length, and an etag of up to 256 bytes.
const GET_HEADER_MAX: usize = 20 + 256;

/// Progress of a store call that the driver delivers into the guest.
enum StoreEvent {
    /// A GET's header and data length; the driver allocates the guest buffer for both.
    GetHead { ticket: u32, header: Vec<u8>, len: u64 },
    /// One frame of a GET's data, as the store client received it.
    GetFrame { ticket: u32, frame: Bytes },
    /// The end of a call. A GET's data is already in its guest buffer.
    Done { ticket: u32, code: i64, data: Vec<u8> },
}

/// Guest buffer that receives the frames of one GET.
struct GetBuffer {
    ptr: u32,
    cap: u32,
    written: u32,
}

/// Host side of the calls between an owner instance and its driver task.
pub(super) struct OwnerIo {
    store: DynStore,
    next_ticket: u32,
    events: mpsc::Sender<StoreEvent>,
    responses: HashMap<u64, oneshot::Sender<OwnerResponse>>,
    store_slots: Arc<tokio::sync::Semaphore>,
    /// Held for reading by every PUT body in guest memory; the driver takes it for writing
    /// before it drops the instance.
    store_live: Arc<tokio::sync::RwLock<()>>,
}

fn owner_io<'a>(caller: &'a mut Caller<'_, GuestState>) -> wasmtime::Result<&'a mut OwnerIo> {
    caller.data_mut().owner_io.as_mut().ok_or_else(|| wasmtime::Error::msg("not an owner instance"))
}

/// Starts `call` on the host and returns its ticket; `call` yields the completion code and data.
fn start_ticket<F>(
    caller: &mut Caller<'_, GuestState>,
    call: impl FnOnce(DynStore, u32, mpsc::Sender<StoreEvent>) -> F,
) -> wasmtime::Result<u32>
where
    F: Future<Output = (i64, Vec<u8>)> + Send + 'static,
{
    let io = owner_io(caller)?;
    let ticket = io.next_ticket;
    io.next_ticket = io.next_ticket.wrapping_add(1);
    let events = io.events.clone();
    let slots = io.store_slots.clone();
    let work = call(io.store.clone(), ticket, events.clone());
    tokio::spawn(async move {
        let Ok(_slot) = slots.acquire_owned().await else { return };
        let (code, data) = work.await;
        let _ = events.send(StoreEvent::Done { ticket, code, data }).await;
    });
    Ok(ticket)
}

/// Runs a streamed GET and sends its header and frames to the driver.
async fn stream_get(store: DynStore, key: String, opts: GetOpts, ticket: u32, events: mpsc::Sender<StoreEvent>) -> (i64, Vec<u8>) {
    let got = match store.get_stream(&key, opts).await {
        Ok(g) => g,
        Err(e) => return (store_code(&e), Vec::new()),
    };
    let etag = got.etag.as_bytes();
    let etag = &etag[..etag.len().min(GET_HEADER_MAX - 20)];
    let mut header = Vec::with_capacity(20 + etag.len());
    header.extend_from_slice(&got.size.to_le_bytes());
    header.extend_from_slice(&got.last_modified_ms.to_le_bytes());
    header.extend_from_slice(&(etag.len() as u32).to_le_bytes());
    header.extend_from_slice(etag);
    if events.send(StoreEvent::GetHead { ticket, header, len: got.len }).await.is_err() {
        return (-5, Vec::new());
    }
    let mut body = got.body;
    while let Some(frame) = futures_util::StreamExt::next(&mut body).await {
        match frame {
            Ok(frame) => {
                if events.send(StoreEvent::GetFrame { ticket, frame }).await.is_err() {
                    return (-5, Vec::new());
                }
            }
            Err(e) => return (store_code(&e), Vec::new()),
        }
    }
    (0, Vec::new())
}

/// Returns the `len` bytes at `ptr` in guest memory as `Bytes`, with no copy.
fn guest_bytes(caller: &mut Caller<'_, GuestState>, ptr: u32, len: u32) -> wasmtime::Result<Bytes> {
    let memory = super::guest_memory(caller)?;
    let live = owner_io(caller)?
        .store_live
        .clone()
        .try_read_owned()
        .map_err(|_| wasmtime::Error::msg("owner instance is stopping"))?;
    super::guest_bytes::in_place(memory.data_ptr(&*caller), memory.data_size(&*caller), ptr, len, live)
        .ok_or_else(|| wasmtime::Error::msg("guest PUT body out of bounds"))
}

/// Encodes a list page for the guest's `decode_list`.
fn encode_list(page: &crate::store::ListPage) -> Vec<u8> {
    fn piece(out: &mut Vec<u8>, p: &[u8]) {
        out.extend_from_slice(&(p.len() as u32).to_le_bytes());
        out.extend_from_slice(p);
    }
    let mut out = vec![page.next.is_some() as u8];
    piece(&mut out, page.next.as_deref().unwrap_or("").as_bytes());
    out.extend_from_slice(&(page.objects.len() as u32).to_le_bytes());
    for o in &page.objects {
        piece(&mut out, o.key.as_bytes());
        out.extend_from_slice(&o.size.to_le_bytes());
        piece(&mut out, o.etag.as_bytes());
        out.extend_from_slice(&o.last_modified_ms.to_le_bytes());
    }
    out
}

pub(super) fn link_owner_calls(linker: &mut Linker<GuestState>) -> wasmtime::Result<()> {
    const M: &str = "baserunna_owner";
    linker.func_wrap(
        M,
        "start_get",
        |mut caller: Caller<'_, GuestState>, key_ptr: u32, key_len: u32, start: u64, end: u64, ranged: u32| {
            let key = String::from_utf8(read_guest(&mut caller, key_ptr, key_len)?)?;
            let opts = GetOpts { range: (ranged == 1).then_some(start..end), if_none_match: None };
            start_ticket(&mut caller, move |store, ticket, events| stream_get(store, key, opts, ticket, events))
        },
    )?;
    linker.func_wrap(
        M,
        "start_put",
        |mut caller: Caller<'_, GuestState>,
         key_ptr: u32,
         key_len: u32,
         data_ptr: u32,
         data_len: u32,
         mode: u32,
         etag_ptr: u32,
         etag_len: u32| {
            let key = String::from_utf8(read_guest(&mut caller, key_ptr, key_len)?)?;
            let data = guest_bytes(&mut caller, data_ptr, data_len)?;
            let etag = String::from_utf8(read_guest(&mut caller, etag_ptr, etag_len)?)?;
            let mode = match mode {
                0 => PutMode::Overwrite,
                1 => PutMode::Create,
                _ => PutMode::IfMatch(etag),
            };
            start_ticket(&mut caller, move |store, _, _| async move {
                match store.put(&key, data, mode).await {
                    Ok(etag) => (0, etag.into_bytes()),
                    Err(e) => (store_code(&e), Vec::new()),
                }
            })
        },
    )?;
    linker.func_wrap(M, "start_delete", |mut caller: Caller<'_, GuestState>, key_ptr: u32, key_len: u32| {
        let key = String::from_utf8(read_guest(&mut caller, key_ptr, key_len)?)?;
        start_ticket(&mut caller, move |store, _, _| async move {
            match store.delete(&key).await {
                Ok(()) => (0, Vec::new()),
                Err(e) => (store_code(&e), Vec::new()),
            }
        })
    })?;
    linker.func_wrap(
        M,
        "start_list",
        |mut caller: Caller<'_, GuestState>,
         prefix_ptr: u32,
         prefix_len: u32,
         after_ptr: u32,
         after_len: u32,
         has_after: u32,
         limit: u32| {
            let prefix = String::from_utf8(read_guest(&mut caller, prefix_ptr, prefix_len)?)?;
            let after = String::from_utf8(read_guest(&mut caller, after_ptr, after_len)?)?;
            start_ticket(&mut caller, move |store, _, _| async move {
                let after = (has_after == 1).then_some(after.as_str());
                match store.list(&prefix, after, limit as usize).await {
                    Ok(page) => (0, encode_list(&page)),
                    Err(e) => (store_code(&e), Vec::new()),
                }
            })
        },
    )?;
    linker.func_wrap(
        M,
        "respond",
        |mut caller: Caller<'_, GuestState>, request_id: u64, status: u32, body_ptr: u32, body_len: u32| {
            let body = read_guest(&mut caller, body_ptr, body_len)?;
            if let Some(tx) = owner_io(&mut caller)?.responses.remove(&request_id) {
                let _ = tx.send(OwnerResponse { status, body });
            }
            Ok(())
        },
    )?;
    Ok(())
}

/// Compiled guest module that starts owner instances.
pub struct OwnerInstances {
    engine: Engine,
    module: Module,
    linker: Linker<GuestState>,
}

/// Request that waits for an owner instance's driver task.
struct QueuedRequest {
    body: Vec<u8>,
    reply: oneshot::Sender<OwnerResponse>,
}

/// Handle to a running owner instance.
pub struct OwnerInstance {
    requests: mpsc::Sender<QueuedRequest>,
    memory: Arc<parking_lot::Mutex<HashMap<String, super::OwnerMemory>>>,
}

/// Guest exports that the driver task calls.
struct OwnerExports {
    alloc: TypedFunc<u32, u32>,
    complete: TypedFunc<(u32, i64, u32, u32), ()>,
    request: TypedFunc<(u64, u32, u32), ()>,
    poll: TypedFunc<u64, u32>,
}

fn host_now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

fn sandbox_err(e: wasmtime::Error) -> Error {
    Error::internal(format!("owner instance: {e}"))
}

impl OwnerInstances {
    /// Compiles the guest module for owner instances.
    pub fn new(guest_wasm: &[u8]) -> Result<OwnerInstances> {
        let mut config = Config::new();
        config.memory_may_move(false).memory_reservation(1 << 32);
        let engine = Engine::new(&config).map_err(sandbox_err)?;
        let module = Module::new(&engine, guest_wasm).map_err(sandbox_err)?;
        let mut linker = Linker::new(&engine);
        link_host_calls(&mut linker).map_err(sandbox_err)?;
        link_owner_calls(&mut linker).map_err(sandbox_err)?;
        Ok(OwnerInstances { engine, module, linker })
    }

    /// Starts the instance of `owner` on `store` and the host task that drives it.
    pub async fn start(&self, owner: &str, store: DynStore, seed: u64) -> Result<OwnerInstance> {
        let (events_tx, events) = mpsc::channel(STORE_CALLS_IN_FLIGHT);
        let memory: Arc<parking_lot::Mutex<HashMap<String, super::OwnerMemory>>> = Arc::default();
        let limiter = OwnerLimiter {
            owner: owner.to_string(),
            owner_memory: memory.clone(),
            owner_memory_bound: OWNER_INSTANCE_MEMORY_BOUND,
            idle: Arc::default(),
            charged: 0,
            refused_grow: None,
            peak: Arc::default(),
        };
        let io = OwnerIo {
            store,
            next_ticket: 0,
            events: events_tx,
            responses: HashMap::new(),
            store_slots: Arc::new(tokio::sync::Semaphore::new(STORE_CALLS_IN_FLIGHT)),
            store_live: Arc::default(),
        };
        let state = GuestState { limiter, store: None, clock_ns: 0, rng: Rng::new(seed), stderr: Vec::new(), owner_io: Some(io) };
        let mut wstore = Store::new(&self.engine, state);
        wstore.limiter(|s| &mut s.limiter);
        let instance = self.linker.instantiate_async(&mut wstore, &self.module).await.map_err(sandbox_err)?;
        if let Ok(init) = instance.get_typed_func::<(), ()>(&mut wstore, "_initialize") {
            init.call_async(&mut wstore, ()).await.map_err(sandbox_err)?;
        }
        let start = instance.get_typed_func::<(u64, u64), ()>(&mut wstore, "owner_start").map_err(sandbox_err)?;
        start.call_async(&mut wstore, (seed, host_now_ms())).await.map_err(sandbox_err)?;
        let exports = OwnerExports {
            alloc: instance.get_typed_func(&mut wstore, "guest_alloc").map_err(sandbox_err)?,
            complete: instance.get_typed_func(&mut wstore, "owner_complete").map_err(sandbox_err)?,
            request: instance.get_typed_func(&mut wstore, "owner_request").map_err(sandbox_err)?,
            poll: instance.get_typed_func(&mut wstore, "owner_poll").map_err(sandbox_err)?,
        };
        let (requests_tx, requests) = mpsc::channel(REQUEST_QUEUE);
        tokio::spawn(drive(wstore, instance, exports, requests, events));
        Ok(OwnerInstance { requests: requests_tx, memory })
    }
}

impl OwnerInstance {
    /// Linear memory that the instance holds.
    pub fn linear_memory(&self) -> usize {
        self.memory.lock().values().map(|m| m.used).sum()
    }

    /// Sends `body` to the instance and waits for its response.
    pub async fn call(&self, body: Vec<u8>) -> Result<OwnerResponse> {
        let (reply, rx) = oneshot::channel();
        self.requests
            .send(QueuedRequest { body, reply })
            .await
            .map_err(|_| Error::unavailable("owner instance stopped"))?;
        rx.await.map_err(|_| Error::unavailable("owner instance stopped"))
    }
}

/// Copies `data` into a fresh guest allocation and returns its address.
async fn place(store: &mut Store<GuestState>, instance: &Instance, alloc: &TypedFunc<u32, u32>, data: &[u8]) -> Result<u32> {
    if data.is_empty() {
        return Ok(0);
    }
    let len = u32::try_from(data.len()).map_err(|_| Error::TooLarge("guest input over 4 GiB".into()))?;
    let ptr = alloc.call_async(&mut *store, len).await.map_err(sandbox_err)?;
    let memory = instance.get_memory(&mut *store, "memory").ok_or_else(|| Error::internal("owner instance memory"))?;
    memory.write(&mut *store, ptr as usize, data).map_err(|e| Error::internal(format!("owner instance input: {e}")))?;
    Ok(ptr)
}

/// Drives one owner instance until every request handle is gone and no task is live.
///
/// It drops the instance only after every PUT body in guest memory is released.
async fn drive(
    mut store: Store<GuestState>,
    instance: Instance,
    exports: OwnerExports,
    requests: mpsc::Receiver<QueuedRequest>,
    events: mpsc::Receiver<StoreEvent>,
) {
    let live = store.data().owner_io.as_ref().expect("owner instance").store_live.clone();
    if let Err(e) = drive_until_done(&mut store, &instance, &exports, requests, events).await {
        crate::warn!("owner instance stopped: {e}");
    }
    let _stopped = live.write().await;
    drop(store);
}

async fn drive_until_done(
    store: &mut Store<GuestState>,
    instance: &Instance,
    exports: &OwnerExports,
    mut requests: mpsc::Receiver<QueuedRequest>,
    mut events: mpsc::Receiver<StoreEvent>,
) -> Result<()> {
    let memory = instance.get_memory(&mut *store, "memory").ok_or_else(|| Error::internal("owner instance memory"))?;
    let mut gets: HashMap<u32, GetBuffer> = HashMap::new();
    let mut live_tasks = 0u32;
    let mut next_request = 0u64;
    let mut requests_open = true;
    loop {
        let mut ready_events = Vec::new();
        let mut ready_requests = Vec::new();
        tokio::select! {
            Some(e) = events.recv() => ready_events.push(e),
            r = requests.recv(), if requests_open => match r {
                Some(r) => ready_requests.push(r),
                None => requests_open = false,
            },
            _ = tokio::time::sleep(TIMER_TICK), if live_tasks > 0 => {}
        }
        while let Ok(e) = events.try_recv() {
            ready_events.push(e);
        }
        while requests_open && let Ok(r) = requests.try_recv() {
            ready_requests.push(r);
        }
        for e in ready_events {
            match e {
                StoreEvent::GetHead { ticket, header, len } => {
                    let cap = u32::try_from(header.len() as u64 + len).map_err(|_| Error::TooLarge("GET over 4 GiB".into()))?;
                    let ptr = exports.alloc.call_async(&mut *store, cap).await.map_err(sandbox_err)?;
                    memory.write(&mut *store, ptr as usize, &header).map_err(|e| Error::internal(format!("GET header: {e}")))?;
                    gets.insert(ticket, GetBuffer { ptr, cap, written: header.len() as u32 });
                }
                StoreEvent::GetFrame { ticket, frame } => {
                    let Some(b) = gets.get_mut(&ticket) else { continue };
                    if b.written as usize + frame.len() > b.cap as usize {
                        b.written = u32::MAX;
                        continue;
                    }
                    memory
                        .write(&mut *store, (b.ptr + b.written) as usize, &frame)
                        .map_err(|e| Error::internal(format!("GET frame: {e}")))?;
                    b.written += frame.len() as u32;
                }
                StoreEvent::Done { ticket, code, data } => {
                    let (code, ptr, len) = match gets.remove(&ticket) {
                        Some(b) if code == 0 && b.written == b.cap => (0, b.ptr, b.cap),
                        Some(b) => (if code < 0 { code } else { -5 }, b.ptr, b.cap),
                        None => {
                            let ptr = place(store, instance, &exports.alloc, &data).await?;
                            (code, ptr, data.len() as u32)
                        }
                    };
                    exports.complete.call_async(&mut *store, (ticket, code, ptr, len)).await.map_err(sandbox_err)?;
                }
            }
        }
        for r in ready_requests {
            let id = next_request;
            next_request += 1;
            store.data_mut().owner_io.as_mut().expect("owner instance").responses.insert(id, r.reply);
            let ptr = place(store, instance, &exports.alloc, &r.body).await?;
            exports.request.call_async(&mut *store, (id, ptr, r.body.len() as u32)).await.map_err(sandbox_err)?;
        }
        live_tasks = exports.poll.call_async(&mut *store, host_now_ms()).await.map_err(sandbox_err)?;
        if !requests_open && live_tasks == 0 {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv_body(kind: u8, db: &str, key: &str, value: &[u8]) -> Vec<u8> {
        let mut b = vec![kind];
        for piece in [db.as_bytes(), key.as_bytes()] {
            b.extend_from_slice(&(piece.len() as u32).to_le_bytes());
            b.extend_from_slice(piece);
        }
        b.extend_from_slice(value);
        b
    }

    #[test]
    fn owner_instance_answers_kv_put_and_get() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let instances = OwnerInstances::new(&super::super::tests::guest_wasm()).unwrap();
            let store: DynStore = Arc::new(crate::store::MemStore::new(Box::new(host_now_ms)));
            let owner = instances.start("o1", store, 7).await.unwrap();
            let put = owner.call(kv_body(2, "d1", "k", b"v1")).await.unwrap();
            assert_eq!(put.status, 200, "{}", String::from_utf8_lossy(&put.body));
            let got = owner.call(kv_body(1, "d1", "k", b"")).await.unwrap();
            assert_eq!((got.status, &got.body[..]), (200, &b"v1"[..]), "{}", String::from_utf8_lossy(&got.body));
            let miss = owner.call(kv_body(1, "d1", "other", b"")).await.unwrap();
            assert_eq!(miss.status, 404, "{}", String::from_utf8_lossy(&miss.body));
            eprintln!("owner instance linear memory with one open database: {} KiB", owner.linear_memory() >> 10);
            assert!(owner.linear_memory() > 0 && owner.linear_memory() <= OWNER_INSTANCE_MEMORY_BOUND);
        });
    }
}
