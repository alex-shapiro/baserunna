//! Per-node memory accounting for open databases and active transactions.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

pub(crate) const ALLOC_OVERHEAD: usize = 16;

pub(crate) fn alloc_bytes(len: usize) -> usize {
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

    pub fn used(&self) -> usize {
        self.used.load(Relaxed)
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn is_exceeded(&self) -> bool {
        self.used() > self.limit
    }

    pub(crate) fn add(&self, n: usize) {
        self.used.fetch_add(n, Relaxed);
    }

    pub(crate) fn sub(&self, n: usize) {
        self.used.fetch_sub(n, Relaxed);
    }
}

/// RAII guard that charges bytes to a [`Memory`] budget and releases them on drop. Cloning a
/// `Charge` adds an equal charge to the budget.
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

    pub fn add(&mut self, bytes: usize) {
        if let Some(m) = &self.memory {
            m.add(bytes);
        }
        self.bytes += bytes;
    }

    pub fn set(&mut self, bytes: usize) {
        if let Some(m) = &self.memory {
            m.add(bytes);
            m.sub(self.bytes);
        }
        self.bytes = bytes;
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

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
    fn charges_are_released_on_drop() {
        let m = Arc::new(Memory::new(100));
        let mut a = Charge::new(&m, 60);
        assert!(!m.is_exceeded());
        a.add(50);
        assert_eq!(m.used(), 110);
        assert!(m.is_exceeded());
        let b = Charge::new(&m, 5);
        a.set(10);
        assert_eq!(m.used(), 15);
        drop(a);
        drop(b);
        assert_eq!(m.used(), 0);
        let mut free = Charge::default();
        free.add(7);
        assert_eq!((free.bytes(), m.used()), (7, 0));
    }
}
