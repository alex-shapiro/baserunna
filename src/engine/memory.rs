//! The node-wide budget for memory held on behalf of databases and transactions.
//!
//! An open database keeps its manifest, tail included, in memory, and a transaction keeps a
//! snapshot and its read and write sets. Each of these is charged to one [`Memory`] budget.
//! When the budget is exceeded, the engine unloads the least recently used idle databases.
//! If that cannot free enough, starting or growing an interactive transaction, which holds
//! memory across requests, fails with a retryable error until memory is released; work that
//! completes within a request proceeds.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

/// Bytes an allocation costs beyond its contents: the allocator's header and rounding.
pub(crate) const ALLOC_OVERHEAD: usize = 16;

/// Heap bytes of a buffer holding `len` bytes.
pub(crate) fn heap(len: usize) -> usize {
    if len == 0 { 0 } else { len + ALLOC_OVERHEAD }
}

#[derive(Debug)]
pub struct Memory {
    limit: usize,
    used: AtomicUsize,
}

impl Memory {
    pub fn new(limit: usize) -> Memory {
        Memory { limit, used: AtomicUsize::new(0) }
    }

    /// Bytes currently charged.
    pub fn used(&self) -> usize {
        self.used.load(Relaxed)
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn over(&self) -> bool {
        self.used() > self.limit
    }

    pub(crate) fn add(&self, n: usize) {
        self.used.fetch_add(n, Relaxed);
    }

    pub(crate) fn sub(&self, n: usize) {
        self.used.fetch_sub(n, Relaxed);
    }
}

/// Bytes charged to a [`Memory`] budget until the charge is dropped.
#[derive(Debug, Default)]
pub struct Charge {
    memory: Option<Arc<Memory>>,
    bytes: usize,
}

impl Charge {
    pub fn new(memory: &Arc<Memory>, bytes: usize) -> Charge {
        memory.add(bytes);
        Charge { memory: Some(memory.clone()), bytes }
    }

    /// Adds `n` bytes to the charge.
    pub fn grow(&mut self, n: usize) {
        if let Some(m) = &self.memory {
            m.add(n);
        }
        self.bytes += n;
    }

    /// Changes the charge to `n` bytes.
    pub fn set(&mut self, n: usize) {
        if let Some(m) = &self.memory {
            m.add(n);
            m.sub(self.bytes);
        }
        self.bytes = n;
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

/// A clone holds its own copy of the memory, so it is charged again.
impl Clone for Charge {
    fn clone(&self) -> Charge {
        match &self.memory {
            Some(m) => Charge::new(m, self.bytes),
            None => Charge { memory: None, bytes: self.bytes },
        }
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        if let Some(m) = &self.memory {
            m.sub(self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn charges_are_released_when_dropped() {
        let m = Arc::new(Memory::new(100));
        let mut a = Charge::new(&m, 60);
        assert!(!m.over());
        a.grow(50);
        assert_eq!(m.used(), 110);
        assert!(m.over());
        let b = Charge::new(&m, 5);
        a.set(10);
        assert_eq!(m.used(), 15);
        drop(a);
        drop(b);
        assert_eq!(m.used(), 0);
        let mut free = Charge::default();
        free.grow(7);
        assert_eq!((free.bytes(), m.used()), (7, 0));
    }
}
