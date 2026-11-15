//! WebAssembly sandbox for tenant work whose cost grows with tenant input.
//!
//! Each owner's calls run in warm instances of the guest module, which keep
//! their engine caches between calls. The runtime, not our code, enforces the
//! bounds: a linear-memory grow over the owner's memory bound fails, and an
//! instruction budget (fuel) stops the instance.

use crate::error::{Error, Result};

pub mod owner;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use crate::engine::events::PartRef;
use crate::engine::Shared;
use crate::engine::events::Pos;
use crate::events::sandboxed::{
    CheckInput, EventConfig, GuestOpResults, ValidateInput, ValidateOutput, MergeInput, OpsInput, OpsOutput, OpsSettings, QueryInput, QueryOutput, SandboxRunner,
    WriteInput, WrittenPart,
};
use crate::events::schema::EventSchema;
use crate::store::BoxFut;
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

/// Instances that finished a call, kept for the next call of the same owner.
///
/// An idle instance stays charged to its owner's memory bound.
type IdleInstances = Arc<Mutex<HashMap<String, Vec<WarmInstance>>>>;

/// Guest instance with the store that holds its state.
struct WarmInstance {
    store: Store<GuestState>,
    instance: Instance,
    linear_memory: usize,
    idle_since: tokio::time::Instant,
}

/// Count of idle instances that one owner may keep.
const IDLE_INSTANCES_PER_OWNER: usize = 4;
/// Largest linear memory of an instance that goes back to the idle pool.
const IDLE_INSTANCE_MEMORY_MAX: usize = 160 << 20;
/// Linear memory that all idle instances of the node may hold.
const NODE_IDLE_MEMORY_MAX: usize = 256 << 20;
/// Time after which an idle instance is dropped, so an idle owner holds no memory.
const IDLE_INSTANCE_TTL: std::time::Duration = std::time::Duration::from_secs(10);

/// Sandbox that runs guest work under per-owner memory and CPU bounds.
pub struct Sandbox {
    engine: Engine,
    module: Module,
    linker: Linker<GuestState>,
    owner_memory: Arc<Mutex<HashMap<String, OwnerMemory>>>,
    idle: IdleInstances,
    idle_sweeper_running: Arc<std::sync::atomic::AtomicBool>,
    owner_memory_bound: usize,
    last_instance_peak: Arc<std::sync::atomic::AtomicUsize>,
    cpu: Arc<crate::cpu_slots::CpuSlots>,
}

/// Charges an instance's linear memory to its owner's bound.
struct OwnerLimiter {
    owner: String,
    owner_memory: Arc<Mutex<HashMap<String, OwnerMemory>>>,
    owner_memory_bound: usize,
    idle: IdleInstances,
    charged: usize,
    refused_grow: Option<usize>,
    /// Receives `charged` when the instance ends: its peak linear memory.
    peak: Arc<std::sync::atomic::AtomicUsize>,
}

impl ResourceLimiter for OwnerLimiter {
    fn memory_growing(&mut self, current: usize, desired: usize, _maximum: Option<usize>) -> wasmtime::Result<bool> {
        let grow = desired.saturating_sub(current);
        if self.try_charge(grow) {
            return Ok(true);
        }
        let evicted = self.idle.lock().remove(&self.owner);
        drop(evicted);
        if self.try_charge(grow) {
            return Ok(true);
        }
        self.refused_grow = Some(desired);
        Ok(false)
    }

    fn table_growing(&mut self, _current: usize, desired: usize, _maximum: Option<usize>) -> wasmtime::Result<bool> {
        Ok(desired <= 10_000)
    }
}

impl OwnerLimiter {
    /// Charges `grow` bytes to the owner, or returns false if they exceed its bound.
    fn try_charge(&mut self, grow: usize) -> bool {
        let mut owners = self.owner_memory.lock();
        let owner = owners.entry(self.owner.clone()).or_default();
        if owner.used + grow > self.owner_memory_bound {
            return false;
        }
        owner.used += grow;
        self.charged += grow;
        true
    }
}

impl Drop for OwnerLimiter {
    fn drop(&mut self) {
        self.peak.store(self.charged, std::sync::atomic::Ordering::Relaxed);
        let mut owners = self.owner_memory.lock();
        if let Some(owner) = owners.get_mut(&self.owner) {
            owner.used -= self.charged;
            if owner.used == 0 {
                owners.remove(&self.owner);
            }
        }
    }
}

/// Output of a guest call, copied out of its instance.
///
/// Every byte that leaves the sandbox passes through this type, and it stays
/// charged to the owner's memory bound until it is dropped.
pub struct GuestOutput {
    bytes: Vec<u8>,
    _charge: OwnerCharge,
}

impl std::fmt::Debug for GuestOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GuestOutput({} bytes)", self.bytes.len())
    }
}

impl std::ops::Deref for GuestOutput {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}

impl AsRef<[u8]> for GuestOutput {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl GuestOutput {
    /// Converts the output into `Bytes` that keep the charge until the last clone drops.
    pub fn into_bytes(self) -> Bytes {
        Bytes::from_owner(self)
    }
}

/// Host memory charged to an owner's bound, released on drop.
struct OwnerCharge {
    owner: String,
    owner_memory: Arc<Mutex<HashMap<String, OwnerMemory>>>,
    bytes: usize,
}

impl Drop for OwnerCharge {
    fn drop(&mut self) {
        let mut owners = self.owner_memory.lock();
        if let Some(owner) = owners.get_mut(&self.owner) {
            owner.used -= self.bytes;
            if owner.used == 0 {
                owners.remove(&self.owner);
            }
        }
    }
}

/// CPU slots for guest work: one per core, less one core for host request work.
fn guest_cpu_slots() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get().saturating_sub(1).max(1))
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
    /// Calls of an owner instance; `None` in a round 2 call instance.
    owner_io: Option<owner::OwnerIo>,
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
        Ok(Sandbox {
            engine,
            module,
            linker,
            owner_memory: Arc::default(),
            idle: Arc::default(),
            idle_sweeper_running: Arc::default(),
            cpu: crate::cpu_slots::CpuSlots::new(guest_cpu_slots()),
            owner_memory_bound,
            last_instance_peak: Arc::default(),
        })
    }

    /// Linear memory that `owner`'s running instances hold.
    pub fn owner_memory_used(&self, owner: &str) -> usize {
        self.owner_memory.lock().get(owner).map_or(0, |o| o.used)
    }

    /// Memory that all owners' instances and outputs hold.
    #[cfg(test)]
    pub fn all_owners_memory_used(&self) -> usize {
        self.owner_memory.lock().values().map(|o| o.used).sum()
    }

    /// Drops every idle instance, which returns its memory to its owner.
    pub fn evict_idle_instances(&self) {
        let idle = std::mem::take(&mut *self.idle.lock());
        drop(idle);
    }

    /// Peak linear memory of the instance that finished a call last.
    pub fn last_instance_peak(&self) -> usize {
        self.last_instance_peak.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Parses a JSON body inside the sandbox and returns its canonical encoding.
    pub async fn parse_json(&self, owner: &str, body: &[u8]) -> Result<GuestOutput> {
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
            idle: self.idle.clone(),
            charged: 0,
            refused_grow: None,
            peak: self.last_instance_peak.clone(),
        };
        let state = GuestState { limiter, store: object_store, clock_ns: 0, rng: Rng::new(seed), stderr: Vec::new(), owner_io: None };
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
    ) -> Result<GuestOutput> {
        self.call_with_pieces(owner, object_store, seed, &[input], fuel, export).await
    }

    /// Writes the concatenation of `pieces` straight into guest memory and runs `export` on it.
    async fn call_with_pieces(
        &self,
        owner: &str,
        object_store: Option<DynStore>,
        seed: u64,
        pieces: &[&[u8]],
        fuel: u64,
        export: &str,
    ) -> Result<GuestOutput> {
        let warm = self.idle.lock().get_mut(owner).and_then(|v| v.pop());
        let (mut store, instance) = match warm {
            Some(WarmInstance { mut store, instance, .. }) => {
                let state = store.data_mut();
                state.store = object_store;
                state.rng = Rng::new(seed);
                state.stderr.clear();
                store.set_fuel(fuel).map_err(|e| Error::internal(format!("sandbox fuel: {e}")))?;
                (store, instance)
            }
            None => self.instantiate(owner, object_store, seed, fuel).await?,
        };
        let export_err = |e: wasmtime::Error| Error::internal(format!("sandbox export: {e}"));
        let memory = instance.get_memory(&mut store, "memory").ok_or_else(|| Error::internal("sandbox memory"))?;
        let alloc = instance.get_typed_func::<u32, u32>(&mut store, "guest_alloc").map_err(export_err)?;
        let work = instance.get_typed_func::<(u32, u32), u32>(&mut store, export).map_err(export_err)?;
        let out_ptr = instance.get_typed_func::<(), u32>(&mut store, "guest_output_ptr").map_err(export_err)?;
        let out_len = instance.get_typed_func::<(), u32>(&mut store, "guest_output_len").map_err(export_err)?;
        let total: usize = pieces.iter().map(|p| p.len()).sum();
        let len = u32::try_from(total).map_err(|_| Error::TooLarge("sandbox input over 4 GiB".into()))?;
        let ptr = alloc.call_async(&mut store, len).await.map_err(|e| guest_error(&store, e))?;
        let mut at = ptr as usize;
        for piece in pieces {
            memory
                .write(&mut store, at, piece)
                .map_err(|e| Error::internal(format!("sandbox input: {e}")))?;
            at += piece.len();
        }
        let free = instance.get_typed_func::<(u32, u32), ()>(&mut store, "guest_free").map_err(export_err)?;
        let status = self.cpu.run(owner, work.call_async(&mut store, (ptr, len))).await;
        let status = status.map_err(|e| guest_error(&store, e))?;
        free.call_async(&mut store, (ptr, len)).await.map_err(|e| guest_error(&store, e))?;
        let out_at = out_ptr.call_async(&mut store, ()).await.map_err(|e| guest_error(&store, e))? as usize;
        let out_len = out_len.call_async(&mut store, ()).await.map_err(|e| guest_error(&store, e))? as usize;
        let charge = self.charge_output(owner, out_len)?;
        let out = memory
            .data(&store)
            .get(out_at..out_at + out_len)
            .ok_or_else(|| Error::internal("sandbox output out of bounds"))?
            .to_vec();
        let out = GuestOutput { bytes: out, _charge: charge };
        let linear_memory = memory.data_size(&store);
        self.release(owner, WarmInstance { store, instance, linear_memory, idle_since: tokio::time::Instant::now() });
        match status {
            0 | 2 => Ok(out),
            _ => Err(Error::invalid(String::from_utf8_lossy(&out.bytes).into_owned())),
        }
    }
}

impl Sandbox {
    /// Charges `bytes` of guest output to `owner`, after its idle instances if needed.
    fn charge_output(&self, owner: &str, bytes: usize) -> Result<OwnerCharge> {
        for attempt in 0..2 {
            {
                let mut owners = self.owner_memory.lock();
                let o = owners.entry(owner.to_string()).or_default();
                if o.used + bytes <= self.owner_memory_bound {
                    o.used += bytes;
                    return Ok(OwnerCharge { owner: owner.to_string(), owner_memory: self.owner_memory.clone(), bytes });
                }
                if o.used == 0 {
                    owners.remove(owner);
                }
            }
            if attempt == 0 {
                let evicted = self.idle.lock().remove(owner);
                drop(evicted);
            }
        }
        if bytes > self.owner_memory_bound / 2 {
            Err(Error::TooLarge(format!(
                "response needs more than half of the owner memory bound of {} MiB",
                self.owner_memory_bound >> 20
            )))
        } else {
            Err(Error::unavailable("owner memory bound reached; retry later"))
        }
    }

    /// Keeps `warm` for the next call of `owner` within the idle pool's limits.
    ///
    /// The pool drops instances idle for longer than `IDLE_INSTANCE_TTL`, and
    /// the oldest instances while the node's idle memory exceeds `NODE_IDLE_MEMORY_MAX`.
    fn release(&self, owner: &str, mut warm: WarmInstance) {
        let state = warm.store.data_mut();
        state.store = None;
        self.last_instance_peak.store(state.limiter.charged, std::sync::atomic::Ordering::Relaxed);
        let mut dropped = Vec::new();
        let mut idle = self.idle.lock();
        if warm.linear_memory <= IDLE_INSTANCE_MEMORY_MAX {
            let pool = idle.entry(owner.to_string()).or_default();
            if pool.len() < IDLE_INSTANCES_PER_OWNER {
                pool.push(warm);
            } else {
                dropped.push(warm);
            }
        } else {
            dropped.push(warm);
        }
        dropped.extend(take_stale(&mut idle));
        let mut idle_memory: usize = idle.values().flatten().map(|w| w.linear_memory).sum();
        while idle_memory > NODE_IDLE_MEMORY_MAX {
            let oldest = idle
                .iter()
                .flat_map(|(o, pool)| pool.iter().enumerate().map(move |(i, w)| (w.idle_since, o.clone(), i)))
                .min_by_key(|(since, _, _)| *since);
            let Some((_, o, i)) = oldest else { break };
            let w = idle.get_mut(&o).expect("owner of the oldest idle instance").remove(i);
            idle_memory -= w.linear_memory;
            dropped.push(w);
        }
        idle.retain(|_, pool| !pool.is_empty());
        let start_sweeper = !idle.is_empty() && !self.idle_sweeper_running.swap(true, std::sync::atomic::Ordering::AcqRel);
        drop(idle);
        drop(dropped);
        if start_sweeper {
            tokio::spawn(sweep_idle(Arc::downgrade(&self.idle), self.idle_sweeper_running.clone()));
        }
    }
}

/// Removes the instances idle for `IDLE_INSTANCE_TTL` or longer from `idle`.
fn take_stale(idle: &mut HashMap<String, Vec<WarmInstance>>) -> Vec<WarmInstance> {
    let now = tokio::time::Instant::now();
    let mut stale = Vec::new();
    for pool in idle.values_mut() {
        let (fresh, old): (Vec<_>, Vec<_>) =
            std::mem::take(pool).into_iter().partition(|w| now.duration_since(w.idle_since) < IDLE_INSTANCE_TTL);
        *pool = fresh;
        stale.extend(old);
    }
    idle.retain(|_, pool| !pool.is_empty());
    stale
}

/// Drops stale idle instances until the pool is empty, so an idle node holds no instance and wakes no task.
async fn sweep_idle(idle: std::sync::Weak<Mutex<HashMap<String, Vec<WarmInstance>>>>, running: Arc<std::sync::atomic::AtomicBool>) {
    loop {
        tokio::time::sleep(IDLE_INSTANCE_TTL / 4).await;
        let Some(idle) = idle.upgrade() else { break };
        let mut pool = idle.lock();
        let stale = take_stale(&mut pool);
        let empty = pool.is_empty();
        if empty {
            running.store(false, std::sync::atomic::Ordering::Release);
        }
        drop(pool);
        drop(stale);
        if empty {
            break;
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

/// Resolves the `owner_id` of a database from the system database.
pub type OwnerResolver = Arc<dyn Fn(String) -> BoxFut<'static, Result<Option<String>>> + Send + Sync>;

/// Sandbox runner of one node: it charges each job to the database owner.
pub struct NodeSandbox {
    pub sandbox: Sandbox,
    pub shared: Arc<Shared>,
    pub owner_of: OwnerResolver,
}

impl NodeSandbox {
    async fn owner(&self, db: &str) -> Result<String> {
        (self.owner_of)(db.to_string())
            .await?
            .ok_or_else(|| Error::not_found(format!("database {db} has no owner record")))
    }
}

impl NodeSandbox {
    fn seed(&self) -> u64 {
        let mut seed = [0u8; 8];
        self.shared.env.fill_random(&mut seed);
        u64::from_le_bytes(seed)
    }
}

/// Bytes of an ops output header beyond the transaction's read and write limits.
const OPS_HEADER_SLACK: usize = 1 << 20;

impl SandboxRunner for NodeSandbox {
    fn charge_host_memory(&self, owner: &str, bytes: usize) -> Result<Box<dyn std::any::Any + Send + Sync>> {
        Ok(Box::new(self.sandbox.charge_output(owner, bytes)?))
    }

    fn validate_events<'a>(
        &'a self,
        owner: &'a str,
        input: ValidateInput,
        body: &'a [u8],
    ) -> BoxFut<'a, Result<(ValidateOutput, Box<dyn std::any::Any + Send + Sync>)>> {
        Box::pin(async move {
            let header = serde_json::to_vec(&input).map_err(|e| Error::internal(format!("validate input: {e}")))?;
            let len_prefix = (header.len() as u32).to_le_bytes();
            let out = self
                .sandbox
                .call_with_pieces(owner, None, self.seed(), &[&len_prefix, &header, body], CALL_FUEL, "guest_validate_events")
                .await?;
            let parsed: ValidateOutput =
                postcard::from_bytes(&out).map_err(|e| Error::internal(format!("validate output: {e}")))?;
            let decoded = self.sandbox.charge_output(owner, out.len())?;
            Ok((parsed, Box::new((out, decoded)) as Box<dyn std::any::Any + Send + Sync>))
        })
    }

    fn exec_ops<'a>(
        &'a self,
        db: &'a str,
        txn: &'a mut crate::engine::Txn,
        ops: Vec<u8>,
        settings: OpsSettings<'a>,
    ) -> BoxFut<'a, Result<GuestOpResults>> {
        Box::pin(async move {
            let seed = self.seed();
            let version = txn.snapshot();
            let (max_write_bytes, max_read_bytes) = txn.limits();
            let input = OpsInput {
                db: db.to_string(),
                config: EventConfig::of(&self.shared.cfg),
                seed,
                now_ms: self.shared.env.now_ms(),
                manifest: version.m.encode().to_vec(),
                etag: version.etag.clone(),
                read_time: txn.read_time,
                isolation: txn.isolation,
                commit_id: txn.commit_id,
                max_write_bytes,
                max_read_bytes,
                effects: txn.effects(),
                ops,
                encoding: settings.encoding,
                json_value_max: settings.json_value_max,
                query_limits: settings.query_limits,
                user_id: settings.user_id.to_string(),
            };
            let body = postcard::to_allocvec(&input).map_err(|e| Error::internal(format!("ops input: {e}")))?;
            drop(input);
            let out = self
                .sandbox
                .call_with_input(settings.owner, Some(self.shared.store.clone()), seed, &body, CALL_FUEL, "guest_exec_ops")
                .await?;
            drop(body);
            let n = out.len();
            let header_len = out
                .get(n.saturating_sub(4)..)
                .and_then(|b| b.try_into().ok())
                .map(|b: [u8; 4]| u32::from_le_bytes(b) as usize)
                .ok_or_else(|| Error::internal("ops output has no header"))?;
            if header_len > max_write_bytes + max_read_bytes + OPS_HEADER_SLACK || header_len + 4 > n {
                return Err(Error::TooLarge(format!(
                    "transaction reads and writes exceed {} bytes",
                    max_write_bytes + max_read_bytes
                )));
            }
            let results_len = n - 4 - header_len;
            let header: OpsOutput = postcard::from_bytes(&out[results_len..n - 4])
                .map_err(|e| Error::internal(format!("ops output: {e}")))?;
            txn.adopt_effects(header.effects)?;
            let ends = header.result_ends?;
            GuestOpResults::new(out.into_bytes().slice(..results_len), ends)
        })
    }

    fn blob_equals<'a>(
        &'a self,
        db: &'a str,
        blob: &'a crate::engine::types::BlobRef,
        want: &'a [u8],
    ) -> BoxFut<'a, Result<bool>> {
        Box::pin(async move {
            let owner = self.owner(db).await?;
            let seed = self.seed();
            let mut w = crate::util::codec::Writer::with_capacity(96);
            blob.encode(&mut w);
            let header = CheckInput {
                db: db.to_string(),
                blob: w.buf,
                config: EventConfig::of(&self.shared.cfg),
                seed,
                now_ms: self.shared.env.now_ms(),
            };
            let header = serde_json::to_vec(&header).map_err(|e| Error::internal(format!("check input: {e}")))?;
            let len_prefix = (header.len() as u32).to_le_bytes();
            let out = self
                .sandbox
                .call_with_pieces(&owner, Some(self.shared.store.clone()), seed, &[&len_prefix, &header, want], CALL_FUEL, "guest_blob_equals")
                .await?;
            Ok(&out[..] == b"1")
        })
    }

    fn write_encoded<'a>(
        &'a self,
        db: &'a str,
        schema: &'a EventSchema,
        partition: u32,
        rows: Vec<(Pos, &'a [u8])>,
        versions: bool,
        out: &'a mut Vec<(PartRef, Vec<Pos>)>,
    ) -> BoxFut<'a, Result<()>> {
        Box::pin(async move {
            let owner = self.owner(db).await?;
            let seed = self.seed();
            let header = WriteInput {
                db: db.to_string(),
                schema: schema.clone(),
                partition,
                versions,
                config: EventConfig::of(&self.shared.cfg),
                seed,
                now_ms: self.shared.env.now_ms(),
            };
            let header = serde_json::to_vec(&header).map_err(|e| Error::internal(format!("write input: {e}")))?;
            let len_prefix = (header.len() as u32).to_le_bytes();
            let records: Vec<[u8; 16]> = rows
                .iter()
                .map(|(pos, b)| {
                    let mut r = [0u8; 16];
                    r[..8].copy_from_slice(&pos.version.to_le_bytes());
                    r[8..12].copy_from_slice(&pos.ordinal.to_le_bytes());
                    r[12..].copy_from_slice(&(b.len() as u32).to_le_bytes());
                    r
                })
                .collect();
            let mut pieces: Vec<&[u8]> = Vec::with_capacity(2 + 2 * rows.len());
            pieces.push(&len_prefix);
            pieces.push(&header);
            for (r, (_, b)) in records.iter().zip(&rows) {
                pieces.push(r);
                pieces.push(b);
            }
            let out_bytes = self
                .sandbox
                .call_with_pieces(&owner, Some(self.shared.store.clone()), seed, &pieces, CALL_FUEL, "guest_write_encoded")
                .await?;
            let parts: Vec<WrittenPart> =
                serde_json::from_slice(&out_bytes).map_err(|e| Error::internal(format!("write output: {e}")))?;
            for (key, enc, positions) in parts {
                let r = PartRef::decode(&key, &enc).map_err(|_| Error::internal("corrupt written part"))?;
                out.push((r, positions.into_iter().map(|(version, ordinal)| Pos { version, ordinal }).collect()));
            }
            Ok(())
        })
    }

    fn query_events<'a>(
        &'a self,
        db: &'a str,
        txn: &'a crate::engine::Txn,
        schema: &'a EventSchema,
        query: &'a serde_json::Value,
        limits: &'a crate::events::exec::Limits,
    ) -> BoxFut<'a, Result<crate::events::exec::Output>> {
        Box::pin(async move {
            let owner = self.owner(db).await?;
            let version = txn.snapshot();
            let input = QueryInput {
                db: db.to_string(),
                schema: schema.clone(),
                query: query.clone(),
                limits: *limits,
                manifest: version.m.encode().to_vec(),
                etag: version.etag.clone(),
                read_time: txn.read_time,
                config: EventConfig::of(&self.shared.cfg),
                seed: self.seed(),
                now_ms: self.shared.env.now_ms(),
            };
            let body = serde_json::to_vec(&input).map_err(|e| Error::internal(format!("query input: {e}")))?;
            let out = self
                .sandbox
                .call_with_input(&owner, Some(self.shared.store.clone()), input.seed, &body, CALL_FUEL, "guest_query_events")
                .await?;
            let out: QueryOutput =
                serde_json::from_slice(&out).map_err(|e| Error::internal(format!("query output: {e}")))?;
            Ok(crate::events::exec::Output {
                columns: out.columns,
                rows: out.rows,
                next_cursor: out.next_cursor,
                stats: out.stats,
            })
        })
    }

    fn merge_parts<'a>(
        &'a self,
        db: &'a str,
        schema: &'a EventSchema,
        inputs: &'a [PartRef],
    ) -> BoxFut<'a, Result<Option<PartRef>>> {
        Box::pin(async move {
            let owner = self.owner(db).await?;
            let mut seed = [0u8; 8];
            self.shared.env.fill_random(&mut seed);
            let input = MergeInput {
                db: db.to_string(),
                schema: schema.clone(),
                inputs: inputs.iter().map(|r| (r.key().to_vec(), r.encode().to_vec())).collect(),
                config: EventConfig::of(&self.shared.cfg),
                seed: u64::from_le_bytes(seed),
                now_ms: self.shared.env.now_ms(),
            };
            self.sandbox.merge_parts(&owner, self.shared.store.clone(), &input).await
        })
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
        assert_eq!(&out[..], br#"{"a":[1,2,{"b":"c"}]}"#);
        assert!(block_on(sandbox.parse_json("o1", b"{")).is_err());
        assert!(sandbox.owner_memory_used("o1") >= out.len(), "the output stays charged to its owner");
        drop(out);
        sandbox.evict_idle_instances();
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
        sandbox.evict_idle_instances();
        assert_eq!(sandbox.owner_memory_used("o1"), 0);
    }

    #[test]
    fn idle_instances_return_memory_to_their_owner() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap();
        rt.block_on(async {
            let sandbox = Sandbox::new(&guest_wasm(), OWNER_MEMORY_BOUND).unwrap();
            sandbox.parse_json("o1", b"[1]").await.unwrap();
            let held = sandbox.owner_memory_used("o1");
            assert!(held > 0, "the idle instance stays charged to its owner");
            sandbox.parse_json("o1", b"[2]").await.unwrap();
            assert_eq!(sandbox.owner_memory_used("o1"), held, "the second call reuses the idle instance");
            tokio::time::sleep(IDLE_INSTANCE_TTL * 2).await;
            assert_eq!(sandbox.owner_memory_used("o1"), 0);
            assert!(!sandbox.idle_sweeper_running.load(std::sync::atomic::Ordering::Acquire), "the sweeper stops");
        });
    }

    #[test]
    fn sandbox_stops_spin_out_of_fuel() {
        let sandbox = Sandbox::new(&guest_wasm(), OWNER_MEMORY_BOUND).unwrap();
        assert!(block_on(sandbox.spin("o1", 1000, 1 << 20)).is_ok());
        let err = block_on(sandbox.spin("o1", u64::MAX, 1 << 24)).unwrap_err();
        assert!(matches!(err, Error::Unavailable { .. }), "{err:?}");
    }
}
