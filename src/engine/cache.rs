//! Node-wide LRU cache for immutable segment data (indexes and decoded
//! blocks), bounded by a byte budget. Segments are immutable, so entries
//! never need invalidation, only eviction.

use super::segment::{Block, SegmentIndex};
use crate::util::DetHashMap;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub seg_path: Arc<str>,
    /// `u32::MAX` for the segment index.
    pub block: u32,
}

#[derive(Clone)]
pub enum CacheVal {
    Index(Arc<SegmentIndex>),
    Block(Block),
}

impl CacheVal {
    fn size(&self) -> usize {
        match self {
            CacheVal::Index(i) => i.mem_size(),
            CacheVal::Block(b) => b.iter().map(|e| e.size() + 48).sum::<usize>() + 64,
        }
    }
}

struct Inner {
    map: DetHashMap<CacheKey, (CacheVal, usize, u64)>,
    lru: BTreeMap<u64, CacheKey>,
    tick: u64,
    bytes: usize,
    hits: u64,
    misses: u64,
}

pub struct Cache {
    budget: usize,
    inner: Mutex<Inner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub bytes: usize,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
}

impl Cache {
    pub fn new(budget: usize) -> Cache {
        Cache {
            budget,
            inner: Mutex::new(Inner {
                map: DetHashMap::default(),
                lru: BTreeMap::new(),
                tick: 0,
                bytes: 0,
                hits: 0,
                misses: 0,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn get(&self, k: &CacheKey) -> Option<CacheVal> {
        let mut g = self.lock();
        g.tick += 1;
        let t = g.tick;
        let Some((v, _, old)) = g.map.get_mut(k) else {
            g.misses += 1;
            return None;
        };
        let v = v.clone();
        let old_t = std::mem::replace(old, t);
        g.lru.remove(&old_t);
        g.lru.insert(t, k.clone());
        g.hits += 1;
        Some(v)
    }

    pub fn insert(&self, k: CacheKey, v: CacheVal) {
        let size = v.size();
        let mut g = self.lock();
        g.tick += 1;
        let t = g.tick;
        if let Some((_, osize, ot)) = g.map.remove(&k) {
            g.lru.remove(&ot);
            g.bytes -= osize;
        }
        if size > self.budget {
            return;
        }
        g.map.insert(k.clone(), (v, size, t));
        g.lru.insert(t, k);
        g.bytes += size;
        while g.bytes > self.budget {
            let Some((&ot, _)) = g.lru.iter().next() else { break };
            if let Some(key) = g.lru.remove(&ot)
                && let Some((_, s, _)) = g.map.remove(&key)
            {
                g.bytes -= s;
            }
        }
    }

    /// Drop everything cached for one segment path prefix (used when a
    /// database is deleted, purely to free memory early).
    pub fn evict_prefix(&self, prefix: &str) {
        let mut g = self.lock();
        let keys: Vec<CacheKey> = g.map.keys().filter(|k| k.seg_path.starts_with(prefix)).cloned().collect();
        for k in keys {
            if let Some((_, s, t)) = g.map.remove(&k) {
                g.lru.remove(&t);
                g.bytes -= s;
            }
        }
    }

    pub fn stats(&self) -> CacheStats {
        let g = self.lock();
        CacheStats { bytes: g.bytes, entries: g.map.len(), hits: g.hits, misses: g.misses }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::types::{Entry, Value};
    use bytes::Bytes;

    fn block(n: usize) -> CacheVal {
        CacheVal::Block(Arc::new(
            (0..n)
                .map(|i| Entry {
                    key: Bytes::from(format!("{i:08}")),
                    seq: 1,
                    value: Value::Inline(Bytes::from(vec![0u8; 100])),
                })
                .collect(),
        ))
    }

    #[test]
    fn evicts_lru_within_budget() {
        let c = Cache::new(10_000);
        let key = |i: u32| CacheKey { seg_path: Arc::from("s"), block: i };
        for i in 0..20 {
            c.insert(key(i), block(10));
            // keep block 0 hot
            assert!(c.get(&key(0)).is_some());
        }
        let s = c.stats();
        assert!(s.bytes <= 10_000);
        assert!(c.get(&key(0)).is_some());
        assert!(c.get(&key(1)).is_none());
        assert!(c.get(&key(19)).is_some());
        c.evict_prefix("s");
        assert_eq!(c.stats().entries, 0);
        assert_eq!(c.stats().bytes, 0);
    }

    #[test]
    fn accounts_bytes_exactly() {
        let one = block(10).size();
        let c = Cache::new(2 * one);
        let key = |i: u32| CacheKey { seg_path: Arc::from("s"), block: i };
        c.insert(key(0), block(10));
        c.insert(key(0), block(10));
        assert_eq!(c.stats().bytes, one, "replacing an entry must not double count it");
        c.insert(key(1), block(10));
        assert_eq!((c.stats().bytes, c.stats().entries), (2 * one, 2), "filling the budget exactly evicts nothing");
        let exact = Cache::new(one);
        exact.insert(key(0), block(10));
        assert!(exact.get(&key(0)).is_some(), "an entry as large as the budget is cached");
        let small = Cache::new(one - 1);
        small.insert(key(0), block(10));
        assert_eq!((small.stats().bytes, small.stats().entries), (0, 0), "an entry over budget is not cached");
        assert_eq!((small.stats().hits, small.stats().misses), (0, 0));
        assert!(small.get(&key(0)).is_none());
        assert!(exact.get(&key(0)).is_some());
        assert_eq!((small.stats().misses, exact.stats().hits), (1, 2));
        let other = CacheKey { seg_path: Arc::from("t"), block: 0 };
        c.insert(other.clone(), block(10));
        c.evict_prefix("s");
        assert_eq!((c.stats().bytes, c.stats().entries), (one, 1), "evicting a prefix releases its bytes");
        assert!(c.get(&other).is_some());
    }
}
