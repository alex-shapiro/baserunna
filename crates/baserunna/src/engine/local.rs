//! Node-local disk and memory cache for immutable segment data.

use crate::store::BoxFut;
use crate::util::DetHashMap;
use crate::util::crc32c::crc32c;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Byte-budgeted key-value cache on node-local storage.
pub trait LocalStore: Send + Sync + 'static {
    fn get<'a>(&'a self, key: &'a str) -> BoxFut<'a, Option<Bytes>>;
    /// Best-effort write; failures are ignored.
    fn put<'a>(&'a self, key: &'a str, data: Bytes) -> BoxFut<'a, ()>;
    /// Returns true if `key` is indexed in the cache without updating its recency.
    fn contains(&self, key: &str) -> bool;
    fn stats(&self) -> LocalStats;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LocalStats {
    pub bytes: u64,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
}

#[derive(Clone)]
pub struct LocalTier(pub Arc<dyn LocalStore>);

impl std::fmt::Debug for LocalTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocalTier({:?})", self.0.stats())
    }
}

#[derive(Default)]
struct Lru {
    entries: DetHashMap<String, (u64, u64)>,
    order: BTreeMap<u64, String>,
    tick: u64,
    bytes: u64,
    hits: u64,
    misses: u64,
}

impl Lru {
    fn touch(&mut self, key: &str) -> bool {
        self.tick += 1;
        let t = self.tick;
        match self.entries.get_mut(key) {
            Some((_, old)) => {
                let old = std::mem::replace(old, t);
                self.order.remove(&old);
                self.order.insert(t, key.to_string());
                self.hits += 1;
                true
            }
            None => {
                self.misses += 1;
                false
            }
        }
    }

    fn insert(&mut self, key: &str, size: u64, budget: u64) -> Vec<String> {
        self.remove(key);
        self.tick += 1;
        self.entries.insert(key.to_string(), (size, self.tick));
        self.order.insert(self.tick, key.to_string());
        self.bytes += size;
        let mut evicted = Vec::new();
        while self.bytes > budget {
            let Some((_, k)) = self.order.pop_first() else { break };
            if let Some((s, _)) = self.entries.remove(&k) {
                self.bytes -= s;
            }
            evicted.push(k);
        }
        evicted
    }

    fn remove(&mut self, key: &str) {
        if let Some((s, t)) = self.entries.remove(key) {
            self.order.remove(&t);
            self.bytes -= s;
        }
    }

    fn stats(&self) -> LocalStats {
        LocalStats { bytes: self.bytes, entries: self.entries.len(), hits: self.hits, misses: self.misses }
    }
}

/// In-memory [`LocalStore`] for tests and simulation.
pub struct MemLocal {
    budget: u64,
    inner: Mutex<(Lru, DetHashMap<String, Bytes>)>,
}

impl MemLocal {
    pub fn new(budget: u64) -> MemLocal {
        MemLocal { budget, inner: Mutex::new((Lru::default(), DetHashMap::default())) }
    }
}

impl LocalStore for MemLocal {
    fn get<'a>(&'a self, key: &'a str) -> BoxFut<'a, Option<Bytes>> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let r = if g.0.touch(key) { g.1.get(key).cloned() } else { None };
        Box::pin(async move { r })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes) -> BoxFut<'a, ()> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        for k in g.0.insert(key, data.len() as u64, self.budget) {
            g.1.remove(&k);
        }
        if g.0.entries.contains_key(key) {
            g.1.insert(key.to_string(), data);
        }
        Box::pin(async {})
    }

    fn contains(&self, key: &str) -> bool {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).0.entries.contains_key(key)
    }

    fn stats(&self) -> LocalStats {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).0.stats()
    }
}

/// Directory-backed [`LocalStore`] that persists entries across restarts as
/// checksummed files written via atomic rename.
pub struct DiskCache {
    dir: PathBuf,
    budget: u64,
    lru: Arc<Mutex<Lru>>,
}

/// Encodes `key` into a collision-free ASCII filename by hex-escaping
/// non-alphanumeric bytes as `_XX`.
fn file_name(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for b in key.bytes() {
        if b.is_ascii_alphanumeric() {
            out.push(b as char);
        } else {
            out.push_str(&format!("_{b:02x}"));
        }
    }
    out
}

impl DiskCache {
    pub fn open(dir: impl Into<PathBuf>, budget: u64) -> std::io::Result<DiskCache> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let mut found = Vec::new();
        for e in std::fs::read_dir(&dir)? {
            let e = e?;
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with(".tmp-") {
                let _ = std::fs::remove_file(e.path());
                continue;
            }
            let m = e.metadata()?;
            if m.is_file() {
                found.push((m.modified()?, name, m.len()));
            }
        }
        found.sort();
        let mut lru = Lru::default();
        for (_, name, size) in found {
            for k in lru.insert(&name, size, budget) {
                let _ = std::fs::remove_file(dir.join(k));
            }
        }
        Ok(DiskCache { dir, budget, lru: Arc::new(Mutex::new(lru)) })
    }

    fn lru(&self) -> std::sync::MutexGuard<'_, Lru> {
        self.lru.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn read(path: &Path) -> Option<Bytes> {
        let d = std::fs::read(path).ok()?;
        let (body, c) = d.split_at_checked(d.len().checked_sub(4)?)?;
        (crc32c(body).to_le_bytes() == c).then(|| Bytes::copy_from_slice(body))
    }

    fn write(dir: &Path, name: &str, data: &[u8]) -> std::io::Result<()> {
        let tmp = dir.join(format!(".tmp-{name}"));
        let mut buf = Vec::with_capacity(data.len() + 4);
        buf.extend_from_slice(data);
        buf.extend_from_slice(&crc32c(data).to_le_bytes());
        std::fs::write(&tmp, &buf)?;
        std::fs::rename(&tmp, dir.join(name))
    }
}

impl LocalStore for DiskCache {
    fn get<'a>(&'a self, key: &'a str) -> BoxFut<'a, Option<Bytes>> {
        Box::pin(async move {
            let name = file_name(key);
            if !self.lru().touch(&name) {
                return None;
            }
            let path = self.dir.join(&name);
            let r = tokio::task::spawn_blocking(move || DiskCache::read(&path)).await.ok().flatten();
            if r.is_none() {
                self.lru().remove(&name);
                let _ = std::fs::remove_file(self.dir.join(&name));
            }
            r
        })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes) -> BoxFut<'a, ()> {
        Box::pin(async move {
            let name = file_name(key);
            let evicted = self.lru().insert(&name, data.len() as u64 + 4, self.budget);
            let dir = self.dir.clone();
            let lru = self.lru.clone();
            let _ = tokio::task::spawn_blocking(move || {
                for k in &evicted {
                    let _ = std::fs::remove_file(dir.join(k));
                }
                if !evicted.contains(&name) && DiskCache::write(&dir, &name, &data).is_err() {
                    lru.lock().unwrap_or_else(|e| e.into_inner()).remove(&name);
                }
            })
            .await;
        })
    }

    fn contains(&self, key: &str) -> bool {
        self.lru().entries.contains_key(&file_name(key))
    }

    fn stats(&self) -> LocalStats {
        self.lru().stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_names_are_distinct_and_plain() {
        let keys = ["dbs/a/seg/b#all", "dbs/a/seg/b#b1", "a/b#c", "a#b/c", "a_2fb#c", "a.b", "a-b"];
        let names: std::collections::BTreeSet<String> = keys.iter().map(|k| file_name(k)).collect();
        assert_eq!(names.len(), keys.len());
        assert!(names.iter().all(|n| n.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')));
        assert_eq!(file_name("dbs/x"), "dbs_2fx");
    }

    #[test]
    fn memory_tier_evicts_least_recently_used() {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            let t = MemLocal::new(300);
            for k in ["a", "b", "c"] {
                t.put(k, Bytes::from(vec![0u8; 100])).await;
            }
            assert!(t.get("a").await.is_some());
            t.put("d", Bytes::from(vec![0u8; 100])).await;
            assert!(t.get("b").await.is_none());
            assert!(t.get("a").await.is_some() && t.get("d").await.is_some());
            t.put("huge", Bytes::from(vec![0u8; 400])).await;
            assert!(t.get("huge").await.is_none());
            assert!(t.stats().bytes <= 300);
            let t = MemLocal::new(300);
            for k in ["a", "b", "c"] {
                t.put(k, Bytes::from(vec![0u8; 100])).await;
            }
            assert!(t.get("a").await.is_some() && t.get("b").await.is_some());
            t.put("d", Bytes::from(vec![0u8; 100])).await;
            assert!(t.get("c").await.is_none());
            assert!(t.get("a").await.is_some() && t.get("b").await.is_some() && t.get("d").await.is_some());
            let s = t.stats();
            assert_eq!((s.hits, s.misses, s.entries, s.bytes), (5, 1, 3, 300));
            t.put("a", Bytes::from(vec![0u8; 40])).await;
            assert_eq!((t.stats().entries, t.stats().bytes), (3, 240));
            t.put("e", Bytes::from(vec![0u8; 60])).await;
            assert_eq!(t.stats().entries, 4);
        });
    }

    #[test]
    fn disk_tier_survives_reopening_and_rejects_corruption() {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let dir = std::env::temp_dir().join(format!("baserunna-local-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        rt.block_on(async {
            let t = DiskCache::open(&dir, 10_000).unwrap();
            t.put("dbs/x/seg/1#all", Bytes::from_static(b"segment")).await;
            t.put("dbs/x/seg/2#b0", Bytes::from_static(b"block")).await;
            assert_eq!(t.get("dbs/x/seg/1#all").await.as_deref(), Some(&b"segment"[..]));
            let bytes = t.stats().bytes;
            assert_eq!(bytes, 7 + 5 + 2 * 4);
            drop(t);
            let t = DiskCache::open(&dir, 10_000).unwrap();
            assert_eq!((t.stats().entries, t.stats().bytes), (2, bytes));
            assert_eq!(t.get("dbs/x/seg/2#b0").await.as_deref(), Some(&b"block"[..]));
            let path = dir.join(file_name("dbs/x/seg/1#all"));
            let mut d = std::fs::read(&path).unwrap();
            d[0] ^= 1;
            std::fs::write(&path, d).unwrap();
            assert!(t.get("dbs/x/seg/1#all").await.is_none());
            assert!(!path.exists());
            let t = DiskCache::open(&dir, 8).unwrap();
            assert_eq!(t.stats().entries, 0);
        });
        let _ = std::fs::remove_dir_all(&dir);
    }
}
