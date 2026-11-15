//! Store PUT bodies that the host reads in place from an owner instance's linear memory.
//!
//! This is the only module of the crate with unsafe code. It is sound because
//! of three facts that the callers keep:
//! - the owner instance engine sets `memory_may_move(false)`, so the linear
//!   memory never moves when it grows;
//! - each body holds a read guard of the instance's `store_live` lock, and the
//!   driver takes that lock for writing before it drops the instance, so the
//!   memory stays mapped while any body lives;
//! - the guest keeps each PUT body alive and unchanged until the host
//!   completes the PUT's ticket.
#![allow(unsafe_code)]

use bytes::Bytes;

/// PUT body bytes inside a linear memory.
struct GuestBytes {
    ptr: *const u8,
    len: usize,
    _live: tokio::sync::OwnedRwLockReadGuard<()>,
}

// SAFETY: see the module comment; the bytes do not move, stay mapped, and do not change.
unsafe impl Send for GuestBytes {}
// SAFETY: as for `Send`; the host only reads the bytes.
unsafe impl Sync for GuestBytes {}

impl AsRef<[u8]> for GuestBytes {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: `in_place` checked that `ptr..ptr + len` is inside the linear memory.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

/// Returns the `len` bytes at offset `ptr` of the linear memory at `base` of `size` bytes,
/// or `None` if they are out of bounds.
pub(super) fn in_place(
    base: *mut u8,
    size: usize,
    ptr: u32,
    len: u32,
    live: tokio::sync::OwnedRwLockReadGuard<()>,
) -> Option<Bytes> {
    let end = (ptr as usize).checked_add(len as usize)?;
    if end > size {
        return None;
    }
    // SAFETY: `ptr..end` is inside the `size` bytes at `base`, checked above.
    let at = unsafe { base.add(ptr as usize) }.cast_const();
    Some(Bytes::from_owner(GuestBytes { ptr: at, len: len as usize, _live: live }))
}
