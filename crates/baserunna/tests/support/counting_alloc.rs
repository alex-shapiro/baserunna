//! Global allocator that counts live heap bytes and their peak, so tests can measure heap use.
//!
//! The counters are global to the test binary. Hold `HEAP_MEASUREMENT_LOCK` during a measurement
//! so that allocations from concurrent tests do not change the result.

// Each test binary uses only some of these helpers.
#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering::Relaxed};

struct CountingAllocator;

static ALLOCATED_BYTES: AtomicIsize = AtomicIsize::new(0);
static PEAK_BYTES: AtomicIsize = AtomicIsize::new(0);

fn add_allocated(by: isize) {
    let now = ALLOCATED_BYTES.fetch_add(by, Relaxed) + by;
    PEAK_BYTES.fetch_max(now, Relaxed);
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        add_allocated(l.size() as isize);
        unsafe { System.alloc(l) }
    }

    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        ALLOCATED_BYTES.fetch_sub(l.size() as isize, Relaxed);
        unsafe { System.dealloc(p, l) }
    }

    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        add_allocated(n as isize - l.size() as isize);
        unsafe { System.realloc(p, l, n) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Serializes heap measurements across the tests in one test binary.
pub static HEAP_MEASUREMENT_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// Returns the live heap bytes.
pub fn allocated_bytes() -> isize {
    ALLOCATED_BYTES.load(Relaxed)
}

/// Sets the peak to the live heap bytes and returns them, as the baseline for `peak_growth`.
pub fn reset_peak() -> isize {
    let now = allocated_bytes();
    PEAK_BYTES.store(now, Relaxed);
    now
}

/// Returns how far the peak rose above `before`, the value that `reset_peak` returned.
pub fn peak_growth(before: isize) -> usize {
    (PEAK_BYTES.load(Relaxed) - before).max(0) as usize
}

/// Returns how far the live heap bytes changed from `before`. The result is negative if the heap shrank.
pub fn heap_growth(before: isize) -> isize {
    allocated_bytes() - before
}
