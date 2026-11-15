//! WebAssembly sandbox for tenant work whose cost grows with tenant input.
//!
//! Each call runs in a fresh instance of the guest module. The runtime, not
//! our code, enforces the bounds: a linear-memory grow over the owner's
//! memory bound fails, and an instruction budget (fuel) stops the instance.

use crate::error::{Error, Result};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use crate::engine::events::PartRef;
use crate::events::sandboxed::MergeInput;
use crate::store::{DynStore, GetOpts, PutMode, StoreError};
use crate::util::rng::Rng;
use bytes::Bytes;
use wasmtime::{Caller, Config, Engine, Instance, Linker, Memory, Module, ResourceLimiter, Store, Trap};

/// Memory bound for all running instances of one owner.
pub const OWNER_MEMORY_BOUND: usize = 512 << 20;

/// Linear memory that one owner's running instances hold.
#[derive(Debug, Default)]
struct OwnerMemory {
    used: usize,
}

/// Sandbox that runs guest work under per-owner memory and CPU bounds.
pub struct Sandbox {
    engine: Engine,
    module: Module,
    linker: Linker<GuestState>,
    owner_memory: Arc<Mutex<HashMap<String, OwnerMemory>>>,
    owner_memory_bound: usize,
}

/// Charges an instance's linear memory to its owner's bound.
struct OwnerLimiter {
    owner: String,
    owner_memory: Arc<Mutex<HashMap<String, OwnerMemory>>>,
    owner_memory_bound: usize,
    charged: usize,
    refused_grow: Option<usize>,
}

impl ResourceLimiter for OwnerLimiter {
    fn memory_growing(&mut self, current: usize, desired: usize, _maximum: Option<usize>) -> wasmtime::Result<bool> {
        let grow = desired.saturating_sub(current);
        let mut owners = self.owner_memory.lock();
        let owner = owners.entry(self.owner.clone()).or_default();
        if owner.used + grow > self.owner_memory_bound {
            self.refused_grow = Some(desired);
            return Ok(false);
        }
        owner.used += grow;
        self.charged += grow;
        Ok(true)
    }

    fn table_growing(&mut self, _current: usize, desired: usize, _maximum: Option<usize>) -> wasmtime::Result<bool> {
        Ok(desired <= 10_000)
    }
}

impl Drop for OwnerLimiter {
    fn drop(&mut self) {
        let mut owners = self.owner_memory.lock();
        if let Some(owner) = owners.get_mut(&self.owner) {
            owner.used -= self.charged;
            if owner.used == 0 {
                owners.remove(&self.owner);
            }
        }
    }
}

/// Fuel that one guest call may burn before the runtime stops it.
const CALL_FUEL: u64 = 1 << 44;
/// Fuel between two yields of a running instance to the node's scheduler.
const YIELD_FUEL: u64 = 10_000_000;
/// Error code of a host store call whose destination buffer is too small.
const ERR_TOO_SMALL: i64 = -6;
const ETAG_MAX: usize = 256;

/// State of one guest instance.
struct GuestState {
    limiter: OwnerLimiter,
    store: Option<DynStore>,
    clock_ns: u64,
    rng: Rng,
    stderr: Vec<u8>,
}

fn store_code(e: &StoreError) -> i64 {
    match e {
        StoreError::NotFound => -1,
        StoreError::PreconditionFailed => -2,
        StoreError::NotModified => -3,
        StoreError::Unavailable(_) => -4,
        StoreError::Fatal(_) => -5,
    }
}

fn guest_memory(caller: &mut Caller<'_, GuestState>) -> wasmtime::Result<Memory> {
    caller
        .get_export("memory")
        .and_then(|e| e.into_memory())
        .ok_or_else(|| wasmtime::Error::msg("guest has no memory"))
}

fn read_guest(caller: &mut Caller<'_, GuestState>, ptr: u32, len: u32) -> wasmtime::Result<Vec<u8>> {
    let mem = guest_memory(caller)?;
    let mut buf = vec![0u8; len as usize];
    mem.read(&*caller, ptr as usize, &mut buf)?;
    Ok(buf)
}

fn write_guest(caller: &mut Caller<'_, GuestState>, ptr: u32, data: &[u8]) -> wasmtime::Result<()> {
    let mem = guest_memory(caller)?;
    mem.write(&mut *caller, ptr as usize, data)?;
    Ok(())
}

fn guest_store(caller: &Caller<'_, GuestState>) -> wasmtime::Result<DynStore> {
    caller.data().store.clone().ok_or_else(|| wasmtime::Error::msg("this guest call has no store"))
}

/// Links the store host calls and the WASI calls that the guest imports.
fn link_host_calls(linker: &mut Linker<GuestState>) -> wasmtime::Result<()> {
    linker.func_wrap_async(
        "baserunna",
        "store_get",
        |mut caller: Caller<'_, GuestState>,
         (key_ptr, key_len, start, end, ranged, dst, dst_cap, reply): (u32, u32, u64, u64, u32, u32, u32, u32)| {
            Box::new(async move {
                let key = String::from_utf8(read_guest(&mut caller, key_ptr, key_len)?)?;
                let store = guest_store(&caller)?;
                let opts = GetOpts { range: (ranged == 1).then_some(start..end), if_none_match: None };
                let got = match store.get(&key, opts).await {
                    Ok(g) => g,
                    Err(e) => return Ok(store_code(&e)),
                };
                if got.data.len() > dst_cap as usize {
                    write_guest(&mut caller, reply, &(got.data.len() as u64).to_le_bytes())?;
                    return Ok(ERR_TOO_SMALL);
                }
                write_guest(&mut caller, dst, &got.data)?;
                let etag = got.etag.as_bytes();
                let etag = &etag[..etag.len().min(ETAG_MAX)];
                let mut meta = Vec::with_capacity(20 + etag.len());
                meta.extend_from_slice(&got.size.to_le_bytes());
                meta.extend_from_slice(&got.last_modified_ms.to_le_bytes());
                meta.extend_from_slice(&(etag.len() as u32).to_le_bytes());
                meta.extend_from_slice(etag);
                write_guest(&mut caller, reply, &meta)?;
                Ok(got.data.len() as i64)
            })
        },
    )?;
    linker.func_wrap_async(
        "baserunna",
        "store_put",
        |mut caller: Caller<'_, GuestState>,
         (key_ptr, key_len, data_ptr, data_len, mode, etag_ptr, etag_len, etag_out): (
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
        )| {
            Box::new(async move {
                let key = String::from_utf8(read_guest(&mut caller, key_ptr, key_len)?)?;
                let data = Bytes::from(read_guest(&mut caller, data_ptr, data_len)?);
                let mode = match mode {
                    0 => PutMode::Overwrite,
                    1 => PutMode::Create,
                    _ => PutMode::IfMatch(String::from_utf8(read_guest(&mut caller, etag_ptr, etag_len)?)?),
                };
                let store = guest_store(&caller)?;
                match store.put(&key, data, mode).await {
                    Ok(etag) => {
                        let etag = &etag.as_bytes()[..etag.len().min(ETAG_MAX)];
                        write_guest(&mut caller, etag_out, etag)?;
                        Ok(etag.len() as i64)
                    }
                    Err(e) => Ok(store_code(&e)),
                }
            })
        },
    )?;
    linker.func_wrap_async(
        "baserunna",
        "store_delete",
        |mut caller: Caller<'_, GuestState>, (key_ptr, key_len): (u32, u32)| {
            Box::new(async move {
                let key = String::from_utf8(read_guest(&mut caller, key_ptr, key_len)?)?;
                let store = guest_store(&caller)?;
                Ok(match store.delete(&key).await {
                    Ok(()) => 0i64,
                    Err(e) => store_code(&e),
                })
            })
        },
    )?;
    const WASI: &str = "wasi_snapshot_preview1";
    linker.func_wrap(WASI, "random_get", |mut caller: Caller<'_, GuestState>, ptr: u32, len: u32| {
        let mut buf = vec![0u8; len as usize];
        caller.data_mut().rng.fill(&mut buf);
        write_guest(&mut caller, ptr, &buf)?;
        Ok(0i32)
    })?;
    linker.func_wrap(WASI, "environ_get", |_: Caller<'_, GuestState>, _: u32, _: u32| 0i32)?;
    linker.func_wrap(WASI, "environ_sizes_get", |mut caller: Caller<'_, GuestState>, count: u32, size: u32| {
        write_guest(&mut caller, count, &0u32.to_le_bytes())?;
        write_guest(&mut caller, size, &0u32.to_le_bytes())?;
        Ok(0i32)
    })?;
    linker.func_wrap(WASI, "clock_time_get", |mut caller: Caller<'_, GuestState>, _id: u32, _precision: u64, out: u32| {
        caller.data_mut().clock_ns += 1_000;
        let now = caller.data().clock_ns;
        write_guest(&mut caller, out, &now.to_le_bytes())?;
        Ok(0i32)
    })?;
    linker.func_wrap(WASI, "fd_write", |mut caller: Caller<'_, GuestState>, _fd: u32, iovs: u32, iovs_len: u32, written: u32| {
        let mut total = 0u32;
        for i in 0..iovs_len {
            let iov = read_guest(&mut caller, iovs + 8 * i, 8)?;
            let (ptr, len) = (u32::from_le_bytes(iov[..4].try_into()?), u32::from_le_bytes(iov[4..].try_into()?));
            let text = read_guest(&mut caller, ptr, len)?;
            let stderr = &mut caller.data_mut().stderr;
            stderr.extend_from_slice(&text[..text.len().min(4096usize.saturating_sub(stderr.len()))]);
            total += len;
        }
        write_guest(&mut caller, written, &total.to_le_bytes())?;
        Ok(0i32)
    })?;
    linker.func_wrap(WASI, "poll_oneoff", |_: Caller<'_, GuestState>, _: u32, _: u32, _: u32, _: u32| -> wasmtime::Result<i32> {
        Err(wasmtime::Error::msg("guest work may not sleep"))
    })?;
    linker.func_wrap(WASI, "proc_exit", |_: Caller<'_, GuestState>, code: u32| -> wasmtime::Result<()> {
        Err(wasmtime::Error::msg(format!("guest exited with code {code}")))
    })?;
    linker.func_wrap(WASI, "sched_yield", |_: Caller<'_, GuestState>| 0i32)?;
    Ok(())
}

impl Sandbox {
    /// Compiles the guest module.
    pub fn new(guest_wasm: &[u8], owner_memory_bound: usize) -> Result<Sandbox> {
        let sandbox_err = |e: wasmtime::Error| Error::internal(format!("sandbox setup: {e}"));
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config).map_err(sandbox_err)?;
        let module = Module::new(&engine, guest_wasm).map_err(sandbox_err)?;
        let mut linker = Linker::new(&engine);
        link_host_calls(&mut linker).map_err(sandbox_err)?;
        Ok(Sandbox { engine, module, linker, owner_memory: Arc::default(), owner_memory_bound })
    }

    /// Linear memory that `owner`'s running instances hold.
    pub fn owner_memory_used(&self, owner: &str) -> usize {
        self.owner_memory.lock().get(owner).map_or(0, |o| o.used)
    }

    /// Parses a JSON body inside the sandbox and returns its canonical encoding.
    pub async fn parse_json(&self, owner: &str, body: &[u8]) -> Result<Vec<u8>> {
        self.call_with_input(owner, None, 0, body, CALL_FUEL, "guest_parse_json").await
    }

    /// Merges event parts with `merge_parts` inside the sandbox.
    pub async fn merge_parts(&self, owner: &str, store: DynStore, input: &MergeInput) -> Result<Option<PartRef>> {
        let body = serde_json::to_vec(input).map_err(|e| Error::internal(format!("merge input: {e}")))?;
        let out = self.call_with_input(owner, Some(store), input.seed, &body, CALL_FUEL, "guest_merge_parts").await;
        match out {
            Ok(out) if out.is_empty() => Ok(None),
            Ok(out) => {
                let (key, enc): (Vec<u8>, Vec<u8>) =
                    serde_json::from_slice(&out).map_err(|e| Error::internal(format!("merge output: {e}")))?;
                PartRef::decode(&key, &enc).map(Some).map_err(|_| Error::internal("corrupt merge output"))
            }
            Err(e) => Err(e),
        }
    }

    /// Runs the guest CPU probe with a fuel budget.
    pub async fn spin(&self, owner: &str, iters: u64, fuel: u64) -> Result<u64> {
        let (mut store, instance) = self.instantiate(owner, None, 0, fuel).await?;
        let spin = instance
            .get_typed_func::<u64, u64>(&mut store, "guest_spin")
            .map_err(|e| Error::internal(format!("sandbox export: {e}")))?;
        spin.call_async(&mut store, iters).await.map_err(|e| guest_error(&store, e))
    }

    async fn instantiate(
        &self,
        owner: &str,
        object_store: Option<DynStore>,
        seed: u64,
        fuel: u64,
    ) -> Result<(Store<GuestState>, Instance)> {
        let limiter = OwnerLimiter {
            owner: owner.to_string(),
            owner_memory: self.owner_memory.clone(),
            owner_memory_bound: self.owner_memory_bound,
            charged: 0,
            refused_grow: None,
        };
        let state = GuestState { limiter, store: object_store, clock_ns: 0, rng: Rng::new(seed), stderr: Vec::new() };
        let mut store = Store::new(&self.engine, state);
        store.limiter(|s| &mut s.limiter);
        store.set_fuel(fuel).map_err(|e| Error::internal(format!("sandbox fuel: {e}")))?;
        store.fuel_async_yield_interval(Some(YIELD_FUEL)).map_err(|e| Error::internal(format!("sandbox fuel: {e}")))?;
        let instance = self.linker.instantiate_async(&mut store, &self.module).await.map_err(|e| guest_error(&store, e))?;
        if let Ok(init) = instance.get_typed_func::<(), ()>(&mut store, "_initialize") {
            init.call_async(&mut store, ()).await.map_err(|e| guest_error(&store, e))?;
        }
        Ok((store, instance))
    }

    async fn call_with_input(
        &self,
        owner: &str,
        object_store: Option<DynStore>,
        seed: u64,
        input: &[u8],
        fuel: u64,
        export: &str,
    ) -> Result<Vec<u8>> {
        let (mut store, instance) = self.instantiate(owner, object_store, seed, fuel).await?;
        let export_err = |e: wasmtime::Error| Error::internal(format!("sandbox export: {e}"));
        let memory = instance.get_memory(&mut store, "memory").ok_or_else(|| Error::internal("sandbox memory"))?;
        let alloc = instance.get_typed_func::<u32, u32>(&mut store, "guest_alloc").map_err(export_err)?;
        let work = instance.get_typed_func::<(u32, u32), u32>(&mut store, export).map_err(export_err)?;
        let out_ptr = instance.get_typed_func::<(), u32>(&mut store, "guest_output_ptr").map_err(export_err)?;
        let out_len = instance.get_typed_func::<(), u32>(&mut store, "guest_output_len").map_err(export_err)?;
        let len = u32::try_from(input.len()).map_err(|_| Error::TooLarge("sandbox input over 4 GiB".into()))?;
        let ptr = alloc.call_async(&mut store, len).await.map_err(|e| guest_error(&store, e))?;
        memory
            .write(&mut store, ptr as usize, input)
            .map_err(|e| Error::internal(format!("sandbox input: {e}")))?;
        let status = work.call_async(&mut store, (ptr, len)).await.map_err(|e| guest_error(&store, e))?;
        let ptr = out_ptr.call_async(&mut store, ()).await.map_err(|e| guest_error(&store, e))? as usize;
        let len = out_len.call_async(&mut store, ()).await.map_err(|e| guest_error(&store, e))? as usize;
        let out = memory
            .data(&store)
            .get(ptr..ptr + len)
            .ok_or_else(|| Error::internal("sandbox output out of bounds"))?
            .to_vec();
        match status {
            0 | 2 => Ok(out),
            _ => Err(Error::invalid(String::from_utf8_lossy(&out).into_owned())),
        }
    }
}

/// Maps a guest trap to the error that the request returns.
fn guest_error(store: &Store<GuestState>, e: wasmtime::Error) -> Error {
    let limiter = &store.data().limiter;
    if let Some(desired) = limiter.refused_grow {
        return if desired > limiter.owner_memory_bound {
            Error::TooLarge(format!(
                "request needs more than the owner memory bound of {} MiB",
                limiter.owner_memory_bound >> 20
            ))
        } else {
            Error::unavailable("owner memory bound reached; retry later")
        };
    }
    match e.downcast_ref::<Trap>() {
        Some(Trap::OutOfFuel) => Error::unavailable("request exceeded its CPU budget"),
        _ => Error::internal(format!(
            "sandbox trap: {e}; guest stderr: {}",
            String::from_utf8_lossy(&store.data().stderr)
        )),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn block_on<F: Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(f)
    }

    pub(crate) fn guest_wasm() -> Vec<u8> {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../target/wasm32-wasip1/release/baserunna_guest.wasm");
        std::fs::read(path).expect("build the guest: cargo build -p baserunna-guest --target wasm32-wasip1 --release")
    }

    #[test]
    fn sandbox_parses_small_json() {
        let sandbox = Sandbox::new(&guest_wasm(), OWNER_MEMORY_BOUND).unwrap();
        let out = block_on(sandbox.parse_json("o1", br#"{"a": [1, 2, {"b": "c"}]}"#)).unwrap();
        assert_eq!(out, br#"{"a":[1,2,{"b":"c"}]}"#);
        assert!(block_on(sandbox.parse_json("o1", b"{")).is_err());
        assert_eq!(sandbox.owner_memory_used("o1"), 0);
    }

    #[test]
    fn sandbox_refuses_json_tree_over_owner_bound() {
        let sandbox = Sandbox::new(&guest_wasm(), OWNER_MEMORY_BOUND).unwrap();
        // 64 MiB of small arrays: the value tree grows to many times the body.
        let mut body = Vec::with_capacity(64 << 20);
        body.push(b'[');
        while body.len() < (64 << 20) {
            body.extend_from_slice(b"[0],");
        }
        body.extend_from_slice(b"[0]]");
        let start = std::time::Instant::now();
        let err = block_on(sandbox.parse_json("o1", &body)).unwrap_err();
        eprintln!("refused in {:?}: {err:?}", start.elapsed());
        assert!(matches!(err, Error::TooLarge(_) | Error::Unavailable { .. }), "{err:?}");
        assert_eq!(sandbox.owner_memory_used("o1"), 0);
    }

    #[test]
    fn sandbox_stops_spin_out_of_fuel() {
        let sandbox = Sandbox::new(&guest_wasm(), OWNER_MEMORY_BOUND).unwrap();
        assert!(block_on(sandbox.spin("o1", 1000, 1 << 20)).is_ok());
        let err = block_on(sandbox.spin("o1", u64::MAX, 1 << 24)).unwrap_err();
        assert!(matches!(err, Error::Unavailable { .. }), "{err:?}");
    }
}
