//! Node-wide byte-bounded LRU cache for immutable segment indexes, decoded
//! blocks, and levels objects, sharded by key.

use super::manifest::Levels;
use super::segment::{Block, SegmentIndex};
use crate::util::DetHashMap;
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
    Levels(Arc<Levels>),
}

impl CacheVal {
    fn size(&self) -> usize {
        match self {
            CacheVal::Index(i) => i.mem_size(),
            CacheVal::Block(b) => b.iter().map(|e| e.size() + 48).sum::<usize>() + 64,
            CacheVal::Levels(l) => {
                l.segments.iter().map(|s| s.min_key.len() + s.max_key.len() + 128).sum::<usize>() + 64
            }
        }
    }
}

const NIL: u32 = u32::MAX;
const SHARD_MIN_BYTES: usize = 4 << 20;
const SHARDS_MAX: usize = 64;

struct Slot {
    key: CacheKey,
    val: CacheVal,
    size: usize,
    prev: u32,
    next: u32,
}

struct Shard {
    budget: usize,
    map: DetHashMap<CacheKey, u32>,
    slots: Vec<Option<Slot>>,
    free: Vec<u32>,
    head: u32,
    tail: u32,
    bytes: usize,
    hits: u64,
    misses: u64,
}

impl Shard {
    fn new(budget: usize) -> Shard {
        Shard {
            budget,
            map: DetHashMap::default(),
            slots: Vec::new(),
            free: Vec::new(),
            head: NIL,
            tail: NIL,
            bytes: 0,
            hits: 0,
            misses: 0,
        }
    }

    fn slot(&mut self, i: u32) -> &mut Slot {
        self.slots[i as usize].as_mut().expect("linked slot is occupied")
    }

    fn unlink(&mut self, i: u32) {
        let (prev, next) = {
            let s = self.slot(i);
            (s.prev, s.next)
        };
        match prev {
            NIL => self.head = next,
            p => self.slot(p).next = next,
        }
        match next {
            NIL => self.tail = prev,
            n => self.slot(n).prev = prev,
        }
    }

    fn push_front(&mut self, i: u32) {
        let head = self.head;
        {
            let s = self.slot(i);
            s.prev = NIL;
            s.next = head;
        }
        match head {
            NIL => self.tail = i,
            h => self.slot(h).prev = i,
        }
        self.head = i;
    }

    fn remove(&mut self, k: &CacheKey) {
        if let Some(i) = self.map.remove(k) {
            self.unlink(i);
            let s = self.slots[i as usize].take().expect("mapped slot is occupied");
            self.bytes -= s.size;
            self.free.push(i);
        }
    }

    fn get(&mut self, k: &CacheKey) -> Option<CacheVal> {
        let Some(&i) = self.map.get(k) else {
            self.misses += 1;
            return None;
        };
        self.unlink(i);
        self.push_front(i);
        self.hits += 1;
        Some(self.slot(i).val.clone())
    }

    fn insert(&mut self, k: CacheKey, v: CacheVal, size: usize) {
        self.remove(&k);
        if size > self.budget {
            return;
        }
        let slot = Slot { key: k.clone(), val: v, size, prev: NIL, next: NIL };
        let i = match self.free.pop() {
            Some(i) => {
                self.slots[i as usize] = Some(slot);
                i
            }
            None => {
                self.slots.push(Some(slot));
                (self.slots.len() - 1) as u32
            }
        };
        self.push_front(i);
        self.map.insert(k, i);
        self.bytes += size;
        while self.bytes > self.budget && self.tail != NIL {
            let key = self.slot(self.tail).key.clone();
            self.remove(&key);
        }
    }
}

pub struct Cache {
    shards: Vec<Mutex<Shard>>,
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
        let n = (budget / SHARD_MIN_BYTES).clamp(1, SHARDS_MAX);
        Cache { shards: (0..n).map(|_| Mutex::new(Shard::new(budget / n))).collect() }
    }

    fn shard(&self, k: &CacheKey) -> std::sync::MutexGuard<'_, Shard> {
        let i = if self.shards.len() == 1 {
            0
        } else {
            use std::hash::BuildHasher;
            std::hash::BuildHasherDefault::<std::hash::DefaultHasher>::default().hash_one(k) as usize
                % self.shards.len()
        };
        self.shards[i].lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn get(&self, k: &CacheKey) -> Option<CacheVal> {
        if crate::buggify::fires(0.05) {
            return None;
        }
        self.shard(k).get(k)
    }

    pub fn insert(&self, k: CacheKey, v: CacheVal) {
        let size = v.size();
        self.shard(&k).insert(k, v, size);
    }

    /// Evicts all cached entries matching `prefix`.
    pub fn evict_prefix(&self, prefix: &str) {
        for s in &self.shards {
            let mut g = s.lock().unwrap_or_else(|e| e.into_inner());
            let keys: Vec<CacheKey> = g.map.keys().filter(|k| k.seg_path.starts_with(prefix)).cloned().collect();
            for k in keys {
                g.remove(&k);
            }
        }
    }

    pub fn stats(&self) -> CacheStats {
        let mut out = CacheStats { bytes: 0, entries: 0, hits: 0, misses: 0 };
        for s in &self.shards {
            let g = s.lock().unwrap_or_else(|e| e.into_inner());
            out.bytes += g.bytes;
            out.entries += g.map.len();
            out.hits += g.hits;
            out.misses += g.misses;
        }
        out
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
                .map(|i| Entry::new(Bytes::from(format!("{i:08}")), 1, Value::Inline(Bytes::from(vec![0u8; 100]))))
                .collect(),
        ))
    }

    #[test]
    fn evicts_lru_within_budget() {
        let c = Cache::new(10_000);
        let key = |i: u32| CacheKey { seg_path: Arc::from("s"), block: i };
        for i in 0..20 {
            c.insert(key(i), block(10));
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
    fn large_budgets_shard_and_stay_within_budget() {
        let one = block(10).size();
        let c = Cache::new(16 * SHARD_MIN_BYTES);
        assert_eq!(c.shards.len(), 16);
        let key = |i: u32| CacheKey { seg_path: Arc::from(format!("seg{}", i % 97)), block: i };
        let n = (16 * SHARD_MIN_BYTES / one) as u32 * 2;
        for i in 0..n {
            c.insert(key(i), block(10));
        }
        let s = c.stats();
        assert!(s.bytes <= 16 * SHARD_MIN_BYTES, "{s:?}");
        assert!(s.entries as u32 > n / 3, "{s:?}");
        assert!(c.get(&key(n - 1)).is_some());
        c.evict_prefix("seg");
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
