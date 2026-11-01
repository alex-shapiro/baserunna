//! Fixed-TTL bounded cache.

use super::DetHashMap;
use parking_lot::Mutex;
use std::hash::Hash;
use std::time::Duration;
use tokio::time::Instant;

/// Bounded cache whose entries expire after `ttl` and are cleared when `max` is exceeded.
pub struct TtlCache<K, V> {
    ttl: Duration,
    max: usize,
    map: Mutex<DetHashMap<K, (Instant, V)>>,
}

impl<K: Hash + Eq, V: Clone> TtlCache<K, V> {
    pub fn new(ttl: Duration, max: usize) -> TtlCache<K, V> {
        TtlCache { ttl, max, map: Mutex::new(DetHashMap::default()) }
    }

    pub fn get(&self, k: &K) -> Option<V> {
        self.map.lock().get(k).filter(|(at, _)| at.elapsed() < self.ttl).map(|(_, v)| v.clone())
    }

    /// Inserts `v` under `k` with expiration `read_at + ttl`, or removes `k` if `v` is `None`.
    pub fn store(&self, k: K, v: Option<V>, read_at: Instant) {
        let mut m = self.map.lock();
        match v {
            Some(v) => {
                if m.len() >= self.max {
                    m.clear();
                }
                m.insert(k, (read_at, v));
            }
            None => {
                m.remove(&k);
            }
        }
    }

    pub fn remove(&self, k: &K) {
        self.map.lock().remove(k);
    }

    pub fn retain(&self, mut keep: impl FnMut(&K, &V) -> bool) {
        self.map.lock().retain(|k, (_, v)| keep(k, v));
    }

    /// Remove expired entries.
    pub fn prune(&self) {
        let ttl = self.ttl;
        self.map.lock().retain(|_, (at, _)| at.elapsed() < ttl);
    }

    pub fn len(&self) -> usize {
        self.map.lock().len()
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
            assert_eq!((c.len(), c.get(&9)), (1, Some("y")));
            c.store(9, None, Instant::now());
            assert_eq!(c.get(&9), None);
        });
    }
}
