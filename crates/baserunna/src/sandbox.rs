//! WebAssembly sandbox for tenant work whose cost grows with tenant input.
//!
//! Each call runs in a fresh instance of the guest module. The runtime, not
//! our code, enforces the bounds: a linear-memory grow over the owner's
//! memory bound fails, and an instruction budget (fuel) stops the instance.

use crate::error::{Error, Result};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use wasmtime::{Config, Engine, Instance, Module, ResourceLimiter, Store, Trap};

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
const CALL_FUEL: u64 = 1 << 40;

impl Sandbox {
    /// Compiles the guest module.
    pub fn new(guest_wasm: &[u8], owner_memory_bound: usize) -> Result<Sandbox> {
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config).map_err(|e| Error::internal(format!("sandbox engine: {e}")))?;
        let module =
            Module::new(&engine, guest_wasm).map_err(|e| Error::internal(format!("sandbox module: {e}")))?;
        Ok(Sandbox { engine, module, owner_memory: Arc::default(), owner_memory_bound })
    }

    /// Linear memory that `owner`'s running instances hold.
    pub fn owner_memory_used(&self, owner: &str) -> usize {
        self.owner_memory.lock().get(owner).map_or(0, |o| o.used)
    }

    /// Parses a JSON body inside the sandbox and returns its canonical encoding.
    pub fn parse_json(&self, owner: &str, body: &[u8]) -> Result<Vec<u8>> {
        self.call_with_input(owner, body, CALL_FUEL, "guest_parse_json")
    }

    /// Runs the guest CPU probe with a fuel budget.
    pub fn spin(&self, owner: &str, iters: u64, fuel: u64) -> Result<u64> {
        let (mut store, instance) = self.instantiate(owner, fuel)?;
        let spin = instance
            .get_typed_func::<u64, u64>(&mut store, "guest_spin")
            .map_err(|e| Error::internal(format!("sandbox export: {e}")))?;
        spin.call(&mut store, iters).map_err(|e| guest_error(&store, e))
    }

    fn instantiate(&self, owner: &str, fuel: u64) -> Result<(Store<OwnerLimiter>, Instance)> {
        let limiter = OwnerLimiter {
            owner: owner.to_string(),
            owner_memory: self.owner_memory.clone(),
            owner_memory_bound: self.owner_memory_bound,
            charged: 0,
            refused_grow: None,
        };
        let mut store = Store::new(&self.engine, limiter);
        store.limiter(|l| l);
        store.set_fuel(fuel).map_err(|e| Error::internal(format!("sandbox fuel: {e}")))?;
        let instance = Instance::new(&mut store, &self.module, &[]).map_err(|e| guest_error(&store, e))?;
        Ok((store, instance))
    }

    fn call_with_input(&self, owner: &str, input: &[u8], fuel: u64, export: &str) -> Result<Vec<u8>> {
        let (mut store, instance) = self.instantiate(owner, fuel)?;
        let export_err = |e: wasmtime::Error| Error::internal(format!("sandbox export: {e}"));
        let memory = instance.get_memory(&mut store, "memory").ok_or_else(|| Error::internal("sandbox memory"))?;
        let alloc = instance.get_typed_func::<u32, u32>(&mut store, "guest_alloc").map_err(export_err)?;
        let work = instance.get_typed_func::<(u32, u32), u32>(&mut store, export).map_err(export_err)?;
        let out_ptr = instance.get_typed_func::<(), u32>(&mut store, "guest_output_ptr").map_err(export_err)?;
        let out_len = instance.get_typed_func::<(), u32>(&mut store, "guest_output_len").map_err(export_err)?;
        let len = u32::try_from(input.len()).map_err(|_| Error::TooLarge("sandbox input over 4 GiB".into()))?;
        let ptr = alloc.call(&mut store, len).map_err(|e| guest_error(&store, e))?;
        memory
            .write(&mut store, ptr as usize, input)
            .map_err(|e| Error::internal(format!("sandbox input: {e}")))?;
        let status = work.call(&mut store, (ptr, len)).map_err(|e| guest_error(&store, e))?;
        let ptr = out_ptr.call(&mut store, ()).map_err(|e| guest_error(&store, e))? as usize;
        let len = out_len.call(&mut store, ()).map_err(|e| guest_error(&store, e))? as usize;
        let out = memory
            .data(&store)
            .get(ptr..ptr + len)
            .ok_or_else(|| Error::internal("sandbox output out of bounds"))?
            .to_vec();
        match status {
            0 => Ok(out),
            _ => Err(Error::invalid(String::from_utf8_lossy(&out).into_owned())),
        }
    }
}

/// Maps a guest trap to the error that the request returns.
fn guest_error(store: &Store<OwnerLimiter>, e: wasmtime::Error) -> Error {
    let limiter = store.data();
    if let Some(desired) = limiter.refused_grow {
        return if desired > limiter.owner_memory_bound {
            Error::TooLarge(format!("request needs more than the owner memory bound of {} MiB", limiter.owner_memory_bound >> 20))
        } else {
            Error::unavailable("owner memory bound reached; retry later")
        };
    }
    match e.downcast_ref::<Trap>() {
        Some(Trap::OutOfFuel) => Error::unavailable("request exceeded its CPU budget"),
        _ => Error::internal(format!("sandbox trap: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn guest_wasm() -> Vec<u8> {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../target/wasm32-unknown-unknown/release/baserunna_guest.wasm");
        std::fs::read(path).expect("build the guest: cargo build -p baserunna-guest --target wasm32-unknown-unknown --release")
    }

    #[test]
    fn sandbox_parses_small_json() {
        let sandbox = Sandbox::new(&guest_wasm(), OWNER_MEMORY_BOUND).unwrap();
        let out = sandbox.parse_json("o1", br#"{"a": [1, 2, {"b": "c"}]}"#).unwrap();
        assert_eq!(out, br#"{"a":[1,2,{"b":"c"}]}"#);
        assert!(sandbox.parse_json("o1", b"{").is_err());
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
        let err = sandbox.parse_json("o1", &body).unwrap_err();
        eprintln!("refused in {:?}: {err:?}", start.elapsed());
        assert!(matches!(err, Error::TooLarge(_) | Error::Unavailable { .. }), "{err:?}");
        assert_eq!(sandbox.owner_memory_used("o1"), 0);
    }

    #[test]
    fn sandbox_stops_spin_out_of_fuel() {
        let sandbox = Sandbox::new(&guest_wasm(), OWNER_MEMORY_BOUND).unwrap();
        assert!(sandbox.spin("o1", 1000, 1 << 20).is_ok());
        let err = sandbox.spin("o1", u64::MAX, 1 << 24).unwrap_err();
        assert!(matches!(err, Error::Unavailable { .. }), "{err:?}");
    }
}
