//! Installs a global allocator that counts current and peak heap bytes.
//!
//! All tests in a test binary share these counters.
//! Lock `HEAP_MEASUREMENT_LOCK` during each measurement.
//! The lock prevents concurrent tests from changing the counters.

// Each test binary uses only some functions in this module.
#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering::Relaxed};

struct CountingAllocator;

static ALLOCATED_BYTES: AtomicIsize = AtomicIsize::new(0);
static PEAK_BYTES: AtomicIsize = AtomicIsize::new(0);

fn add_allocated_bytes(delta_bytes: isize) {
    let current_bytes = ALLOCATED_BYTES.fetch_add(delta_bytes, Relaxed) + delta_bytes;
    PEAK_BYTES.fetch_max(current_bytes, Relaxed);
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        add_allocated_bytes(layout.size() as isize);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        ALLOCATED_BYTES.fetch_sub(layout.size() as isize, Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        add_allocated_bytes(new_size as isize - layout.size() as isize);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Synchronizes heap measurements across tests in a test binary.
pub static HEAP_MEASUREMENT_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// Returns the count of currently allocated heap bytes.
pub fn allocated_bytes() -> isize {
    ALLOCATED_BYTES.load(Relaxed)
}

/// Resets the peak counter to the current allocated byte count.
/// Returns that count as a baseline for `peak_growth_bytes`.
pub fn reset_peak_bytes() -> isize {
    let current_bytes = allocated_bytes();
    PEAK_BYTES.store(current_bytes, Relaxed);
    current_bytes
}

/// Returns the peak byte increase above `baseline_bytes`.
/// Pass the baseline from `reset_peak_bytes`.
/// Returns `0` if the peak is at or below `baseline_bytes`.
pub fn peak_growth_bytes(baseline_bytes: isize) -> usize {
    (PEAK_BYTES.load(Relaxed) - baseline_bytes).max(0) as usize
}

/// Returns the change in allocated heap bytes since `baseline_bytes`.
/// Returns a negative value if allocated heap bytes decreased.
pub fn heap_change_bytes(baseline_bytes: isize) -> isize {
    allocated_bytes() - baseline_bytes
}
