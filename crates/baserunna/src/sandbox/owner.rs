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

/// Response of an owner instance to one request.
#[derive(Debug)]
pub struct OwnerResponse {
    pub status: u32,
    pub body: Vec<u8>,
}

/// Result of a store call that the host completes into the guest.
struct Completion {
    ticket: u32,
    code: i64,
    data: Vec<u8>,
}

/// Host side of the calls between an owner instance and its driver task.
pub(super) struct OwnerIo {
    store: DynStore,
    next_ticket: u32,
    completions: mpsc::UnboundedSender<Completion>,
    responses: HashMap<u64, oneshot::Sender<OwnerResponse>>,
}

fn owner_io<'a>(caller: &'a mut Caller<'_, GuestState>) -> wasmtime::Result<&'a mut OwnerIo> {
    caller.data_mut().owner_io.as_mut().ok_or_else(|| wasmtime::Error::msg("not an owner instance"))
}

/// Starts `call` on the host and returns its ticket; `call` yields the completion code and data.
fn start_ticket<F>(caller: &mut Caller<'_, GuestState>, call: impl FnOnce(DynStore) -> F) -> wasmtime::Result<u32>
where
    F: Future<Output = (i64, Vec<u8>)> + Send + 'static,
{
    let io = owner_io(caller)?;
    let ticket = io.next_ticket;
    io.next_ticket = io.next_ticket.wrapping_add(1);
    let done = io.completions.clone();
    let work = call(io.store.clone());
    tokio::spawn(async move {
        let (code, data) = work.await;
        let _ = done.send(Completion { ticket, code, data });
    });
    Ok(ticket)
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

fn link_owner_calls(linker: &mut Linker<GuestState>) -> wasmtime::Result<()> {
    const M: &str = "baserunna_owner";
    linker.func_wrap(
        M,
        "start_get",
        |mut caller: Caller<'_, GuestState>, key_ptr: u32, key_len: u32, start: u64, end: u64, ranged: u32| {
            let key = String::from_utf8(read_guest(&mut caller, key_ptr, key_len)?)?;
            start_ticket(&mut caller, move |store| async move {
                let opts = GetOpts { range: (ranged == 1).then_some(start..end), if_none_match: None };
                match store.get(&key, opts).await {
                    Ok(got) => {
                        let etag = got.etag.as_bytes();
                        let mut out = Vec::with_capacity(20 + etag.len() + got.data.len());
                        out.extend_from_slice(&got.size.to_le_bytes());
                        out.extend_from_slice(&got.last_modified_ms.to_le_bytes());
                        out.extend_from_slice(&(etag.len() as u32).to_le_bytes());
                        out.extend_from_slice(etag);
                        out.extend_from_slice(&got.data);
                        (0, out)
                    }
                    Err(e) => (store_code(&e), Vec::new()),
                }
            })
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
            let data = Bytes::from(read_guest(&mut caller, data_ptr, data_len)?);
            let etag = String::from_utf8(read_guest(&mut caller, etag_ptr, etag_len)?)?;
            let mode = match mode {
                0 => PutMode::Overwrite,
                1 => PutMode::Create,
                _ => PutMode::IfMatch(etag),
            };
            start_ticket(&mut caller, move |store| async move {
                match store.put(&key, data, mode).await {
                    Ok(etag) => (0, etag.into_bytes()),
                    Err(e) => (store_code(&e), Vec::new()),
                }
            })
        },
    )?;
    linker.func_wrap(M, "start_delete", |mut caller: Caller<'_, GuestState>, key_ptr: u32, key_len: u32| {
        let key = String::from_utf8(read_guest(&mut caller, key_ptr, key_len)?)?;
        start_ticket(&mut caller, move |store| async move {
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
            start_ticket(&mut caller, move |store| async move {
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
        let engine = Engine::new(&Config::new()).map_err(sandbox_err)?;
        let module = Module::new(&engine, guest_wasm).map_err(sandbox_err)?;
        let mut linker = Linker::new(&engine);
        link_host_calls(&mut linker).map_err(sandbox_err)?;
        link_owner_calls(&mut linker).map_err(sandbox_err)?;
        Ok(OwnerInstances { engine, module, linker })
    }

    /// Starts the instance of `owner` on `store` and the host task that drives it.
    pub async fn start(&self, owner: &str, store: DynStore, seed: u64) -> Result<OwnerInstance> {
        let (completions_tx, completions) = mpsc::unbounded_channel();
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
        let io = OwnerIo { store, next_ticket: 0, completions: completions_tx, responses: HashMap::new() };
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
        tokio::spawn(drive(wstore, instance, exports, requests, completions));
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
async fn drive(
    mut store: Store<GuestState>,
    instance: Instance,
    exports: OwnerExports,
    mut requests: mpsc::Receiver<QueuedRequest>,
    mut completions: mpsc::UnboundedReceiver<Completion>,
) {
    let mut live_tasks = 0u32;
    let mut next_request = 0u64;
    let mut requests_open = true;
    loop {
        let mut ready_completions = Vec::new();
        let mut ready_requests = Vec::new();
        tokio::select! {
            Some(c) = completions.recv() => ready_completions.push(c),
            r = requests.recv(), if requests_open => match r {
                Some(r) => ready_requests.push(r),
                None => requests_open = false,
            },
            _ = tokio::time::sleep(TIMER_TICK), if live_tasks > 0 => {}
        }
        while let Ok(c) = completions.try_recv() {
            ready_completions.push(c);
        }
        while requests_open && let Ok(r) = requests.try_recv() {
            ready_requests.push(r);
        }
        let step = async {
            for c in ready_completions {
                let ptr = place(&mut store, &instance, &exports.alloc, &c.data).await?;
                exports.complete.call_async(&mut store, (c.ticket, c.code, ptr, c.data.len() as u32)).await.map_err(sandbox_err)?;
            }
            for r in ready_requests {
                let id = next_request;
                next_request += 1;
                store.data_mut().owner_io.as_mut().expect("owner instance").responses.insert(id, r.reply);
                let ptr = place(&mut store, &instance, &exports.alloc, &r.body).await?;
                exports.request.call_async(&mut store, (id, ptr, r.body.len() as u32)).await.map_err(sandbox_err)?;
            }
            exports.poll.call_async(&mut store, host_now_ms()).await.map_err(sandbox_err)
        };
        match step.await {
            Ok(n) => live_tasks = n,
            Err(e) => {
                crate::warn!("owner instance stopped: {e}");
                return;
            }
        }
        if !requests_open && live_tasks == 0 {
            return;
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
