//! Tenant work that the node runs inside its WebAssembly sandbox.
//!
//! The host calls `guest_alloc` to place input in linear memory, calls a
//! work export, and reads the output that `guest_output_ptr` and
//! `guest_output_len` name. Every allocation of tenant work lands in this
//! module's linear memory, so the host's memory limit bounds it.

use std::cell::RefCell;

thread_local! {
    static OUTPUT: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Allocates `len` bytes in linear memory for host input.
#[unsafe(no_mangle)]
pub extern "C" fn guest_alloc(len: u32) -> *mut u8 {
    let mut buf = Vec::<u8>::with_capacity(len as usize);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}

/// Frees a buffer that `guest_alloc` returned.
///
/// # Safety
/// `ptr` and `len` must come from one `guest_alloc` call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn guest_free(ptr: *mut u8, len: u32) {
    drop(unsafe { Vec::from_raw_parts(ptr, 0, len as usize) });
}

#[unsafe(no_mangle)]
pub extern "C" fn guest_output_ptr() -> *const u8 {
    OUTPUT.with(|o| o.borrow().as_ptr())
}

#[unsafe(no_mangle)]
pub extern "C" fn guest_output_len() -> u32 {
    OUTPUT.with(|o| o.borrow().len() as u32)
}

fn set_output(out: Vec<u8>) {
    OUTPUT.with(|o| *o.borrow_mut() = out);
}

/// Parses a JSON body into a value tree and returns its canonical encoding.
///
/// Returns 0 on success and 1 if the body is not valid JSON; the output then
/// holds the parse error message.
///
/// # Safety
/// `ptr` and `len` must name `len` initialized bytes from `guest_alloc`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn guest_parse_json(ptr: *const u8, len: u32) -> u32 {
    let body = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
    match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(v) => {
            set_output(serde_json::to_vec(&v).unwrap_or_default());
            0
        }
        Err(e) => {
            set_output(e.to_string().into_bytes());
            1
        }
    }
}

/// Runs a loop of `iters` steps with no allocation, as a CPU-bound probe.
#[unsafe(no_mangle)]
pub extern "C" fn guest_spin(iters: u64) -> u64 {
    let mut x = 0u64;
    for i in 0..iters {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(i);
    }
    x
}
/// Builds a dictionary-encoded string vector of `n` rows and returns its row count.
///
/// This probe runs node compute code from `baserunna-core` inside the guest.
#[unsafe(no_mangle)]
pub extern "C" fn guest_vector_probe(n: u32) -> u32 {
    let texts: Vec<String> = (0..n).map(|i| format!("v{}", i % 97)).collect();
    let strs = baserunna_core::vector::Strs::from_values(texts.iter().map(|t| Some(t.as_str())));
    let raw = texts.concat().into_bytes();
    let packed = baserunna_core::zstd::compress(&raw, 3).unwrap_or_default();
    strs.codes.len() as u32 + baserunna_core::crc32c::crc32c(&packed) % 2
}

mod host {
    #[link(wasm_import_module = "baserunna")]
    unsafe extern "C" {
        /// Starts a store GET. Returns the length of the pending reply, or a negative error code.
        pub fn store_get(key_ptr: *const u8, key_len: u32, start: u64, end: u64, ranged: u32) -> i64;
        /// Runs a store PUT. Returns the length of the pending reply, or a negative error code.
        pub fn store_put(key_ptr: *const u8, key_len: u32, data_ptr: *const u8, data_len: u32, mode: u32, etag_ptr: *const u8, etag_len: u32) -> i64;
        /// Runs a store DELETE. Returns 0, or a negative error code.
        pub fn store_delete(key_ptr: *const u8, key_len: u32) -> i64;
        /// Copies the pending reply into `dst`.
        pub fn take_pending(dst: *mut u8);
    }
}

/// Object store that reaches the node's store through host calls.
struct HostStore;

fn take_pending(len: i64) -> Vec<u8> {
    let mut buf = vec![0u8; len as usize];
    unsafe { host::take_pending(buf.as_mut_ptr()) };
    buf
}

fn store_error(code: i64) -> baserunna::store::StoreError {
    use baserunna::store::StoreError;
    match code {
        -1 => StoreError::NotFound,
        -2 => StoreError::PreconditionFailed,
        -3 => StoreError::NotModified,
        -4 => StoreError::Unavailable("store unavailable".into()),
        _ => StoreError::Fatal("store error".into()),
    }
}

impl baserunna::store::ObjectStore for HostStore {
    fn get<'a>(
        &'a self,
        key: &'a str,
        opts: baserunna::store::GetOpts,
    ) -> baserunna::store::BoxFut<'a, baserunna::store::SResult<baserunna::store::GetResult>> {
        Box::pin(async move {
            let (start, end, ranged) = opts.range.map_or((0, 0, 0), |r| (r.start, r.end, 1));
            let n = unsafe { host::store_get(key.as_ptr(), key.len() as u32, start, end, ranged) };
            if n < 0 {
                return Err(store_error(n));
            }
            let reply = take_pending(n);
            let etag_len = u32::from_le_bytes(reply[0..4].try_into().unwrap()) as usize;
            let etag = String::from_utf8_lossy(&reply[4..4 + etag_len]).into_owned();
            let at = 4 + etag_len;
            let size = u64::from_le_bytes(reply[at..at + 8].try_into().unwrap());
            let last_modified_ms = u64::from_le_bytes(reply[at + 8..at + 16].try_into().unwrap());
            let data = bytes::Bytes::from(reply).slice(at + 16..);
            Ok(baserunna::store::GetResult { data, etag, size, last_modified_ms })
        })
    }

    fn put<'a>(
        &'a self,
        key: &'a str,
        data: bytes::Bytes,
        mode: baserunna::store::PutMode,
    ) -> baserunna::store::BoxFut<'a, baserunna::store::SResult<String>> {
        use baserunna::store::PutMode;
        Box::pin(async move {
            let (mode, etag) = match &mode {
                PutMode::Overwrite => (0, ""),
                PutMode::Create => (1, ""),
                PutMode::IfMatch(e) => (2, e.as_str()),
            };
            let n = unsafe {
                host::store_put(key.as_ptr(), key.len() as u32, data.as_ptr(), data.len() as u32, mode, etag.as_ptr(), etag.len() as u32)
            };
            if n < 0 {
                return Err(store_error(n));
            }
            Ok(String::from_utf8_lossy(&take_pending(n)).into_owned())
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> baserunna::store::BoxFut<'a, baserunna::store::SResult<()>> {
        Box::pin(async move {
            match unsafe { host::store_delete(key.as_ptr(), key.len() as u32) } {
                n if n < 0 => Err(store_error(n)),
                _ => Ok(()),
            }
        })
    }

    fn list<'a>(
        &'a self,
        _prefix: &'a str,
        _start_after: Option<&'a str>,
        _limit: usize,
    ) -> baserunna::store::BoxFut<'a, baserunna::store::SResult<baserunna::store::ListPage>> {
        Box::pin(async { Err(baserunna::store::StoreError::Fatal("guest work does not list".into())) })
    }
}

/// Input of `guest_merge_parts`.
#[derive(serde::Deserialize)]
struct MergeInput {
    db: String,
    schema: baserunna::events::schema::EventSchema,
    /// Part reference keys and encodings, hex.
    inputs: Vec<(String, String)>,
    seed: u64,
    now_ms: u64,
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap_or(0)).collect()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Merges event parts with the node's `merge_parts` and returns the new part reference.
///
/// Returns 0 with `[key_hex, ref_hex]` JSON output, 2 if no rows remain, and 1 with
/// an error message.
///
/// # Safety
/// `ptr` and `len` must name `len` initialized bytes from `guest_alloc`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn guest_merge_parts(ptr: *const u8, len: u32) -> u32 {
    let body = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
    let input: MergeInput = match serde_json::from_slice(body) {
        Ok(i) => i,
        Err(e) => {
            set_output(e.to_string().into_bytes());
            return 1;
        }
    };
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
    let out = rt.block_on(async move {
        let env = baserunna::env::Env::sim(input.seed, input.now_ms, tokio::time::Instant::now(), 0);
        let engine = baserunna::engine::Engine::new(
            std::sync::Arc::new(HostStore),
            std::sync::Arc::new(env),
            baserunna::engine::EngineConfig::default(),
            "guest".into(),
        );
        let mut refs = Vec::new();
        for (k, v) in &input.inputs {
            let r = baserunna::engine::events::PartRef::decode(&unhex(k), &unhex(v))
                .map_err(|_| baserunna::Error::internal("corrupt part reference"))?;
            refs.push(r);
        }
        let stats = baserunna::events::part::ReadStats::default();
        baserunna::events::maint::merge_parts(&engine.shared, &input.db, &input.schema, &refs, &stats).await
    });
    match out {
        Ok(Some(r)) => {
            set_output(serde_json::to_vec(&(hex(&r.key()), hex(&r.encode()))).unwrap_or_default());
            0
        }
        Ok(None) => 2,
        Err(e) => {
            set_output(format!("{e:?}").into_bytes());
            1
        }
    }
}
