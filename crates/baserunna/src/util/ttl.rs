//! Fixed-TTL bounded cache.

use super::DetHashMap;
use std::hash::Hash;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::Instant;

/// Bounded cache whose entries expire after `ttl`. If the cache holds `max` live entries, an
/// insert evicts the oldest eighth.
pub struct TtlCache<K, V> {
    ttl: Duration,
    max: usize,
    map: Mutex<DetHashMap<K, (Instant, V)>>,
}

impl<K: Hash + Eq + Clone, V: Clone> TtlCache<K, V> {
    pub fn new(ttl: Duration, max: usize) -> TtlCache<K, V> {
        TtlCache { ttl, max, map: Mutex::new(DetHashMap::default()) }
    }

    fn map(&self) -> std::sync::MutexGuard<'_, DetHashMap<K, (Instant, V)>> {
        self.map.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn get(&self, k: &K) -> Option<V> {
        self.map().get(k).filter(|(at, _)| at.elapsed() < self.ttl).map(|(_, v)| v.clone())
    }

    /// Inserts `v` under `k` with expiration `read_at + ttl`, or removes `k` if `v` is `None`.
    pub fn store(&self, k: K, v: Option<V>, read_at: Instant) {
        let mut m = self.map();
        match v {
            Some(v) => {
                if m.len() >= self.max {
                    m.retain(|_, (at, _)| at.elapsed() < self.ttl);
                }
                if m.len() >= self.max {
                    let mut by_age: Vec<(Instant, K)> = m.iter().map(|(key, (at, _))| (*at, key.clone())).collect();
                    let evict_count = (by_age.len() / 8).max(1);
                    by_age.select_nth_unstable_by_key(evict_count - 1, |(at, _)| *at);
                    for (_, key) in &by_age[..evict_count] {
                        m.remove(key);
                    }
                }
                m.insert(k, (read_at, v));
            }
            None => {
                m.remove(&k);
            }
        }
    }

    pub fn remove(&self, k: &K) {
        self.map().remove(k);
    }

    pub fn retain(&self, mut keep: impl FnMut(&K, &V) -> bool) {
        self.map().retain(|k, (_, v)| keep(k, v));
    }

    /// Remove expired entries.
    pub fn prune(&self) {
        let ttl = self.ttl;
        self.map().retain(|_, (at, _)| at.elapsed() < ttl);
    }

    pub fn len(&self) -> usize {
        self.map().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entries_expire_and_stay_bounded() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
        rt.block_on(async {
            let c: TtlCache<u32, &str> = TtlCache::new(Duration::from_secs(10), 3);
            c.store(1, Some("a"), Instant::now());
            assert_eq!(c.get(&1), Some("a"));
            c.store(2, Some("b"), Instant::now() - Duration::from_secs(10));
            assert_eq!(c.get(&2), None);
            tokio::time::sleep(Duration::from_secs(10)).await;
            assert_eq!(c.get(&1), None);
            assert_eq!(c.len(), 2);
            c.prune();
            assert!(c.is_empty());
            for k in 0..3 {
                c.store(k, Some("x"), Instant::now());
            }
            c.store(9, Some("y"), Instant::now());
            assert_eq!((c.len(), c.get(&9)), (3, Some("y")));
            c.store(9, None, Instant::now());
            assert_eq!(c.get(&9), None);
        });
    }

    #[test]
    fn full_cache_evicts_oldest_eighth_on_insert() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
        rt.block_on(async {
            let c: TtlCache<u32, u32> = TtlCache::new(Duration::from_secs(3600), 16);
            for k in 0..17 {
                c.store(k, Some(k), Instant::now());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            assert_eq!(c.len(), 15, "an insert into a full cache evicts its oldest eighth");
            for k in 2..17 {
                assert_eq!(c.get(&k), Some(k), "a recent entry was evicted");
            }
            assert_eq!((c.get(&0), c.get(&1)), (None, None));
        });
    }
}
