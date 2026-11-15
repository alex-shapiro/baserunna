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
#[unsafe(no_mangle)] pub extern "C" fn guest_zstd_probe(n: u32) -> u32 { let v = vec![7u8; n as usize]; let c = zstd::bulk::compress(&v, 3).unwrap(); zstd::bulk::decompress(&c, n as usize).unwrap().len() as u32 + crc32c::crc32c(&c) }
