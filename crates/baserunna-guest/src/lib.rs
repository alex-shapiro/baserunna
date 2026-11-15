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

/// Engine of this instance, with the runtime that drives it.
///
/// The instance lives across host calls, so the engine's block cache and
/// part caches serve later calls of the same owner.
struct InstanceEngine {
    rt: tokio::runtime::Runtime,
    engine: std::sync::Arc<baserunna::engine::Engine>,
}

thread_local! {
    static INSTANCE_ENGINE: RefCell<Option<std::rc::Rc<InstanceEngine>>> = const { RefCell::new(None) };
}

/// Runs `work` on this instance's engine at the host's clock time `now_ms`.
fn run_on_engine<T, F>(
    seed: u64,
    now_ms: u64,
    config: &baserunna::events::sandboxed::EventConfig,
    work: impl FnOnce(std::sync::Arc<baserunna::engine::Engine>) -> F,
) -> T
where
    F: std::future::Future<Output = T>,
{
    let instance = INSTANCE_ENGINE.with(|slot| {
        slot.borrow_mut()
            .get_or_insert_with(|| {
                let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
                let engine = rt.block_on(async {
                    let env = baserunna::env::Env::sim(seed, now_ms, tokio::time::Instant::now(), 0);
                    baserunna::engine::Engine::new(
                        std::sync::Arc::new(HostStore),
                        std::sync::Arc::new(env),
                        config.engine_config(),
                        "guest".into(),
                    )
                });
                std::rc::Rc::new(InstanceEngine { rt, engine })
            })
            .clone()
    });
    instance.rt.block_on(async {
        let behind = now_ms.saturating_sub(instance.engine.shared.env.now_ms());
        if behind > 0 {
            tokio::time::advance(std::time::Duration::from_millis(behind)).await;
        }
        work(instance.engine.clone()).await
    })
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
        /// Runs a store GET and writes the data into `dst` and a `GetReply` into `reply`.
        /// Returns the data length, -6 if `dst_cap` is too small (the reply then holds the
        /// needed length in `size`), or another negative error code.
        pub fn store_get(
            key_ptr: *const u8,
            key_len: u32,
            start: u64,
            end: u64,
            ranged: u32,
            dst: *mut u8,
            dst_cap: u32,
            reply: *mut u8,
        ) -> i64;
        /// Runs a store PUT and writes the etag into `etag_out`. Returns the etag length or a
        /// negative error code.
        pub fn store_put(
            key_ptr: *const u8,
            key_len: u32,
            data_ptr: *const u8,
            data_len: u32,
            mode: u32,
            etag_ptr: *const u8,
            etag_len: u32,
            etag_out: *mut u8,
        ) -> i64;
        /// Runs a store DELETE. Returns 0, or a negative error code.
        pub fn store_delete(key_ptr: *const u8, key_len: u32) -> i64;
    }
}

/// Reply metadata of `host::store_get`.
#[repr(C)]
struct GetReply {
    size: u64,
    last_modified_ms: u64,
    etag_len: u32,
    etag: [u8; ETAG_MAX],
}

const ETAG_MAX: usize = 256;
const ERR_TOO_SMALL: i64 = -6;
/// First destination size for a GET without a range.
const UNRANGED_GET_CAP: u64 = 1 << 20;

/// Object store that reaches the node's store through host calls.
struct HostStore;

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
            let mut cap = if ranged == 1 { end.saturating_sub(start) } else { UNRANGED_GET_CAP };
            loop {
                let mut dst = Vec::<u8>::with_capacity(cap as usize);
                let mut reply = GetReply { size: 0, last_modified_ms: 0, etag_len: 0, etag: [0; ETAG_MAX] };
                let n = unsafe {
                    host::store_get(
                        key.as_ptr(),
                        key.len() as u32,
                        start,
                        end,
                        ranged,
                        dst.as_mut_ptr(),
                        cap as u32,
                        (&raw mut reply).cast(),
                    )
                };
                if n == ERR_TOO_SMALL {
                    cap = reply.size;
                    continue;
                }
                if n < 0 {
                    return Err(store_error(n));
                }
                unsafe { dst.set_len(n as usize) };
                let etag = String::from_utf8_lossy(&reply.etag[..reply.etag_len as usize]).into_owned();
                return Ok(baserunna::store::GetResult {
                    data: bytes::Bytes::from(dst),
                    etag,
                    size: reply.size,
                    last_modified_ms: reply.last_modified_ms,
                });
            }
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
            let mut out = [0u8; ETAG_MAX];
            let n = unsafe {
                host::store_put(
                    key.as_ptr(),
                    key.len() as u32,
                    data.as_ptr(),
                    data.len() as u32,
                    mode,
                    etag.as_ptr(),
                    etag.len() as u32,
                    out.as_mut_ptr(),
                )
            };
            if n < 0 {
                return Err(store_error(n));
            }
            Ok(String::from_utf8_lossy(&out[..n as usize]).into_owned())
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

/// Merges event parts with the node's `merge_parts`.
///
/// Returns 0 with the JSON `[key, encoded part reference]` of the new part, 2 if no
/// rows remain, and 1 with an error message.
///
/// # Safety
/// `ptr` and `len` must name `len` initialized bytes from `guest_alloc`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn guest_merge_parts(ptr: *const u8, len: u32) -> u32 {
    use baserunna::events::sandboxed::MergeInput;
    let body = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
    let input: MergeInput = match serde_json::from_slice(body) {
        Ok(i) => i,
        Err(e) => {
            set_output(e.to_string().into_bytes());
            return 1;
        }
    };
    let config = input.config.clone();
    let out = run_on_engine(input.seed, input.now_ms, &config, |engine| async move {
        let mut refs = Vec::new();
        for (k, v) in &input.inputs {
            let r = baserunna::engine::events::PartRef::decode(k, v)
                .map_err(|_| baserunna::Error::internal("corrupt part reference"))?;
            refs.push(r);
        }
        let stats = baserunna::events::part::ReadStats::default();
        baserunna::events::maint::merge_parts(&engine.shared, &input.db, &input.schema, &refs, &stats).await
    });
    match out {
        Ok(Some(r)) => {
            set_output(serde_json::to_vec(&(r.key().to_vec(), r.encode().to_vec())).unwrap_or_default());
            0
        }
        Ok(None) => 2,
        Err(e) => {
            set_output(format!("{e:?}").into_bytes());
            1
        }
    }
}

/// Runs an event query with the node's `exec::run` at the host's snapshot version.
///
/// Returns 0 with a JSON `QueryOutput`, or 1 with an error message.
///
/// # Safety
/// `ptr` and `len` must name `len` initialized bytes from `guest_alloc`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn guest_query_events(ptr: *const u8, len: u32) -> u32 {
    use baserunna::engine::{Isolation, Snapshot, Txn, Version, manifest::Manifest};
    use baserunna::events::sandboxed::{QueryInput, QueryOutput};
    let body = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
    let input: QueryInput = match serde_json::from_slice(body) {
        Ok(i) => i,
        Err(e) => {
            set_output(e.to_string().into_bytes());
            return 1;
        }
    };
    let config = input.config.clone();
    let out = run_on_engine(input.seed, input.now_ms, &config, |engine| async move {
        let max_write = engine.shared.cfg.txn_max_write_bytes;
        let m =Manifest::decode(&bytes::Bytes::from(input.manifest))
            .map_err(|_| baserunna::Error::internal("corrupt snapshot manifest"))?;
        let version = std::sync::Arc::new(Version::new(m, input.etag, engine.shared.clone()));
        let mut txn = Txn::new(Snapshot { version, time_ms: input.read_time }, Isolation::Snapshot, 0, max_write);
        let q = baserunna::events::query::Query::parse(input.query)?;
        let mut grant = baserunna::queries::QueryGrant::unlimited();
        baserunna::events::exec::run(&engine.shared, &input.db, &mut txn, &input.schema, &q, &input.limits, &mut grant)
            .await
    });
    match out {
        Ok(o) => {
            let o = QueryOutput { columns: o.columns, rows: o.rows, next_cursor: o.next_cursor, stats: o.stats };
            set_output(serde_json::to_vec(&o).unwrap_or_default());
            0
        }
        Err(e) => {
            set_output(format!("{e:?}").into_bytes());
            1
        }
    }
}

/// Builds level 0 parts from one partition's encoded rows with the node's `write_encoded`.
///
/// The input is a `u32` header length, a JSON `WriteInput`, and row records of
/// `(version u64, ordinal u32, len u32, bytes)`. Returns 0 with a JSON list of
/// `WrittenPart`, or 1 with an error message.
///
/// # Safety
/// `ptr` and `len` must name `len` initialized bytes from `guest_alloc`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn guest_write_encoded(ptr: *const u8, len: u32) -> u32 {
    use baserunna::engine::events::Pos;
    use baserunna::events::sandboxed::{WriteInput, WrittenPart};
    let body = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
    let header_len = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
    let input: WriteInput = match serde_json::from_slice(&body[4..4 + header_len]) {
        Ok(i) => i,
        Err(e) => {
            set_output(e.to_string().into_bytes());
            return 1;
        }
    };
    let mut rows: Vec<(Pos, &[u8])> = Vec::new();
    let mut at = 4 + header_len;
    while at < body.len() {
        let version = u64::from_le_bytes(body[at..at + 8].try_into().unwrap());
        let ordinal = u32::from_le_bytes(body[at + 8..at + 12].try_into().unwrap());
        let n = u32::from_le_bytes(body[at + 12..at + 16].try_into().unwrap()) as usize;
        rows.push((Pos { version, ordinal }, &body[at + 16..at + 16 + n]));
        at += 16 + n;
    }
    let config = input.config.clone();
    let out = run_on_engine(input.seed, input.now_ms, &config, |engine| async move {
        let mut written = Vec::new();
        baserunna::events::build::write_encoded(
            &engine.shared,
            &input.db,
            &input.schema,
            input.partition,
            rows.into_iter(),
            input.versions,
            &mut written,
        )
        .await?;
        Ok::<_, baserunna::Error>(written)
    });
    match out {
        Ok(written) => {
            let parts: Vec<WrittenPart> = written
                .into_iter()
                .map(|(r, positions)| {
                    (r.key().to_vec(), r.encode().to_vec(), positions.into_iter().map(|p| (p.version, p.ordinal)).collect())
                })
                .collect();
            set_output(serde_json::to_vec(&parts).unwrap_or_default());
            0
        }
        Err(e) => {
            set_output(format!("{e:?}").into_bytes());
            1
        }
    }
}

/// Reads a blob value with the node's `blob::read_range` and compares it with the expected value.
///
/// The input is a `u32` header length, a JSON `CheckInput`, and the expected
/// value. Returns 0 with output `1` (equal) or `0`, or 1 with an error message.
///
/// # Safety
/// `ptr` and `len` must name `len` initialized bytes from `guest_alloc`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn guest_blob_equals(ptr: *const u8, len: u32) -> u32 {
    use baserunna::events::sandboxed::CheckInput;
    let body = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
    let header_len = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
    let input: CheckInput = match serde_json::from_slice(&body[4..4 + header_len]) {
        Ok(i) => i,
        Err(e) => {
            set_output(e.to_string().into_bytes());
            return 1;
        }
    };
    let want = &body[4 + header_len..];
    let config = input.config.clone();
    let out = run_on_engine(input.seed, input.now_ms, &config, |engine| async move {
        let blob = baserunna::engine::BlobRef::decode(&mut baserunna::util::codec::Reader::new(&input.blob))
            .map_err(|_| baserunna::Error::internal("corrupt blob reference"))?;
        let got = baserunna::engine::blob::read_range(&engine.shared, &input.db, &blob, 0..blob.size).await?;
        Ok::<_, baserunna::Error>(got[..] == want[..])
    });
    match out {
        Ok(equal) => {
            set_output(if equal { b"1".to_vec() } else { b"0".to_vec() });
            0
        }
        Err(e) => {
            set_output(format!("{e:?}").into_bytes());
            1
        }
    }
}
