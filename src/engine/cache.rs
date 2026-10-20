//! Node-wide sharded CLOCK cache for immutable segment indexes, decoded
//! blocks, and levels objects.

use super::manifest::Levels;
use super::segment::{Block, SegmentIndex};
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CacheKey {
    db: u64,
    object: u128,
    part: u32,
}

const PART_INDEX: u32 = u32::MAX;
const PART_LEVELS: u32 = u32::MAX - 1;

impl CacheKey {
    pub fn block(db: &str, seg: &str, block: usize) -> CacheKey {
        CacheKey { db: hash_str(db), object: object_id(seg), part: block as u32 }
    }

    pub fn index(db: &str, seg: &str) -> CacheKey {
        CacheKey { db: hash_str(db), object: object_id(seg), part: PART_INDEX }
    }

    pub fn levels(db: &str, id: &str) -> CacheKey {
        CacheKey { db: hash_str(db), object: object_id(id), part: PART_LEVELS }
    }
}

/// Parses 32-character hex object IDs directly, or hashes non-hex test IDs.
fn object_id(id: &str) -> u128 {
    match id.len() == 32 {
        true => u128::from_str_radix(id, 16).unwrap_or_else(|_| hash_wide(id)),
        false => hash_wide(id),
    }
}

fn hash_wide(s: &str) -> u128 {
    ((hash_seeded(s, 0) as u128) << 64) | hash_seeded(s, K) as u128
}

fn hash_str(s: &str) -> u64 {
    hash_seeded(s, 0)
}

fn hash_seeded(s: &str, seed: u64) -> u64 {
    let mut h = KeyHasher(seed);
    h.write(s.as_bytes());
    h.write_u64(s.len() as u64);
    h.finish()
}

#[derive(Default)]
struct KeyHasher(u64);

const K: u64 = 0x9E37_79B9_7F4A_7C15;

impl Hasher for KeyHasher {
    fn write(&mut self, bytes: &[u8]) {
        for c in bytes.chunks(8) {
            let mut w = [0u8; 8];
            w[..c.len()].copy_from_slice(c);
            self.write_u64(u64::from_le_bytes(w));
        }
    }

    fn write_u32(&mut self, v: u32) {
        self.write_u64(v as u64);
    }

    fn write_u64(&mut self, v: u64) {
        self.0 = (self.0.rotate_left(26) ^ v).wrapping_mul(K);
    }

    fn write_u128(&mut self, v: u128) {
        self.write_u64(v as u64);
        self.write_u64((v >> 64) as u64);
    }

    fn finish(&self) -> u64 {
        self.0 ^ (self.0 >> 29)
    }
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

const SHARD_MIN_BYTES: usize = 4 << 20;
const SHARDS_MAX: usize = 64;

struct Slot {
    entry: Option<(CacheKey, CacheVal, usize)>,
    referenced: AtomicBool,
}

struct Shard {
    budget: usize,
    map: HashMap<CacheKey, u32, BuildHasherDefault<KeyHasher>>,
    slots: Vec<Slot>,
    free: Vec<u32>,
    hand: usize,
    bytes: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl Shard {
    fn new(budget: usize) -> Shard {
        Shard {
            budget,
            map: HashMap::default(),
            slots: Vec::new(),
            free: Vec::new(),
            hand: 0,
            bytes: 0,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    fn get(&self, k: &CacheKey) -> Option<CacheVal> {
        let Some(&i) = self.map.get(k) else {
            self.misses.fetch_add(1, Relaxed);
            return None;
        };
        let slot = &self.slots[i as usize];
        // Avoid dirtying the cache line if the reference bit is already set.
        if !slot.referenced.load(Relaxed) {
            slot.referenced.store(true, Relaxed);
        }
        self.hits.fetch_add(1, Relaxed);
        slot.entry.as_ref().map(|(_, v, _)| v.clone())
    }

    fn evict(&mut self, i: usize) {
        if let Some((k, _, size)) = self.slots[i].entry.take() {
            self.map.remove(&k);
            self.bytes -= size;
            self.free.push(i as u32);
        }
    }

    fn insert(&mut self, k: CacheKey, v: CacheVal, size: usize) {
        if let Some(i) = self.map.get(&k).copied() {
            self.evict(i as usize);
        }
        if size > self.budget {
            return;
        }
        while self.bytes + size > self.budget {
            let i = self.hand;
            self.hand = (self.hand + 1) % self.slots.len();
            let slot = &mut self.slots[i];
            if slot.entry.is_some() && !std::mem::replace(slot.referenced.get_mut(), false) {
                self.evict(i);
            }
        }
        let slot = Slot { entry: Some((k, v, size)), referenced: AtomicBool::new(false) };
        let i = match self.free.pop() {
            Some(i) => {
                self.slots[i as usize] = slot;
                i
            }
            None => {
                self.slots.push(slot);
                (self.slots.len() - 1) as u32
            }
        };
        self.map.insert(k, i);
        self.bytes += size;
    }
}

pub struct Cache {
    shards: Box<[RwLock<Shard>]>,
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
        Cache { shards: (0..n).map(|_| RwLock::new(Shard::new(budget / n))).collect() }
    }

    fn shard_of(&self, k: &CacheKey) -> &RwLock<Shard> {
        let i = (k.object as usize ^ k.part as usize) % self.shards.len();
        &self.shards[i]
    }

    fn read(s: &RwLock<Shard>) -> RwLockReadGuard<'_, Shard> {
        s.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(s: &RwLock<Shard>) -> RwLockWriteGuard<'_, Shard> {
        s.write().unwrap_or_else(|e| e.into_inner())
    }

    pub fn get(&self, k: &CacheKey) -> Option<CacheVal> {
        if crate::buggify::fires(0.05) {
            return None;
        }
        Self::read(self.shard_of(k)).get(k)
    }

    /// Returns the cached value for `k` without setting its reference bit or updating hit counters.
    pub fn peek(&self, k: &CacheKey) -> Option<CacheVal> {
        let g = Self::read(self.shard_of(k));
        let i = *g.map.get(k)?;
        g.slots[i as usize].entry.as_ref().map(|(_, v, _)| v.clone())
    }

    pub fn contains(&self, k: &CacheKey) -> bool {
        Self::read(self.shard_of(k)).map.contains_key(k)
    }

    pub fn insert(&self, k: CacheKey, v: CacheVal) {
        let size = v.size();
        Self::write(self.shard_of(&k)).insert(k, v, size);
    }

    pub fn evict_db(&self, db: &str) {
        let db = hash_str(db);
        for s in &self.shards {
            let mut g = Self::write(s);
            for i in 0..g.slots.len() {
                if g.slots[i].entry.as_ref().is_some_and(|(k, _, _)| k.db == db) {
                    g.evict(i);
                }
            }
        }
    }

    pub fn stats(&self) -> CacheStats {
        let mut out = CacheStats { bytes: 0, entries: 0, hits: 0, misses: 0 };
        for s in &self.shards {
            let g = Self::read(s);
            out.bytes += g.bytes;
            out.entries += g.map.len();
            out.hits += g.hits.load(Relaxed);
            out.misses += g.misses.load(Relaxed);
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

    fn key(i: usize) -> CacheKey {
        CacheKey::block("db", &format!("{:032x}", i % 7), i)
    }

    #[test]
    fn clock_evicts_unreferenced_entries() {
        let size = block(10).size();
        let c = Cache::new(4 * size);
        for i in 0..4 {
            c.insert(key(i), block(10));
        }
        assert!(c.get(&key(0)).is_some());
        c.insert(key(4), block(10));
        assert!(c.get(&key(0)).is_some());
        assert!(c.get(&key(1)).is_none());
        assert!(c.get(&key(4)).is_some());
        for i in 5..40 {
            c.insert(key(i), block(10));
            assert!(c.get(&key(0)).is_some());
        }
        let s = c.stats();
        assert_eq!((s.bytes, s.entries), (4 * size, 4));
    }

    #[test]
    fn scan_does_not_evict_hot_entries() {
        let size = block(10).size();
        let c = Cache::new(8 * size);
        let hot: Vec<CacheKey> = (0..4).map(|i| CacheKey::levels("db", &format!("{i:032x}"))).collect();
        for k in &hot {
            c.insert(*k, block(10));
            assert!(c.get(k).is_some());
        }
        for i in 0..100 {
            c.insert(key(1000 + i), block(10));
            if i % 3 == 0 {
                for k in &hot {
                    assert!(c.get(k).is_some(), "hot entry evicted by scan block {i}");
                }
            }
        }
    }

    #[test]
    fn large_budgets_shard_and_stay_within_budget() {
        let entry_size = block(10).size();
        let c = Cache::new(16 * SHARD_MIN_BYTES);
        assert_eq!(c.shards.len(), 16);
        let n = 16 * SHARD_MIN_BYTES / entry_size * 2;
        for i in 0..n {
            c.insert(key(i), block(10));
        }
        let s = c.stats();
        assert!(s.bytes <= 16 * SHARD_MIN_BYTES, "{s:?}");
        assert!(s.entries > n / 3, "{s:?}");
        assert!(c.get(&key(n - 1)).is_some());
        c.evict_db("db");
        assert_eq!((c.stats().bytes, c.stats().entries), (0, 0));
    }

    #[test]
    fn accounts_bytes_accurately() {
        let entry_size = block(10).size();
        let c = Cache::new(2 * entry_size);
        c.insert(key(0), block(10));
        c.insert(key(0), block(10));
        assert_eq!(c.stats().bytes, entry_size);
        c.insert(key(1), block(10));
        assert_eq!((c.stats().bytes, c.stats().entries), (2 * entry_size, 2));
        let exact = Cache::new(entry_size);
        exact.insert(key(0), block(10));
        assert!(exact.get(&key(0)).is_some());
        let small = Cache::new(entry_size - 1);
        small.insert(key(0), block(10));
        assert_eq!((small.stats().bytes, small.stats().entries), (0, 0));
        assert_eq!((small.stats().hits, small.stats().misses), (0, 0));
        assert!(small.get(&key(0)).is_none());
        assert!(exact.get(&key(0)).is_some());
        assert_eq!((small.stats().misses, exact.stats().hits), (1, 2));
        let other = CacheKey::block("other", &format!("{:032x}", 0), 0);
        c.insert(other, block(10));
        c.evict_db("db");
        assert_eq!((c.stats().bytes, c.stats().entries), (entry_size, 1));
        assert!(c.get(&other).is_some());
    }

    #[test]
    fn keys_distinguish_databases_objects_and_parts() {
        let id = "0123456789abcdef0123456789abcdef";
        let keys = [
            CacheKey::block("a", id, 0),
            CacheKey::block("b", id, 0),
            CacheKey::block("a", id, 1),
            CacheKey::index("a", id),
            CacheKey::levels("a", id),
            CacheKey::block("a", &"f".repeat(32), 0),
            CacheKey::block("a", "short", 0),
            CacheKey::block("a", "trohs", 0),
        ];
        let distinct: std::collections::BTreeSet<String> = keys.iter().map(|k| format!("{k:?}")).collect();
        assert_eq!(distinct.len(), keys.len());
    }
}
