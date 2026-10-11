//! Fault-injecting simulated object store with S3 semantics.
//!
//! Every operation takes simulated time. Writes are applied by a detached
//! "network" task, so a request that was sent before its caller crashed can
//! still land later — exactly the ambiguity a real object store presents.

use super::Trace;
use crate::store::*;
use crate::util::rng::Rng;
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct StoreFaults {
    pub latency_min_ms: u64,
    pub latency_max_ms: u64,
    /// Probability of a slow request (10x latency).
    pub p_slow: f64,
    /// Probability that a request fails before being applied.
    pub p_fail_before: f64,
    /// Probability that a write is applied but its response is lost.
    pub p_fail_after: f64,
}

impl StoreFaults {
    pub fn none() -> StoreFaults {
        StoreFaults { latency_min_ms: 1, latency_max_ms: 5, p_slow: 0.0, p_fail_before: 0.0, p_fail_after: 0.0 }
    }

    pub fn moderate() -> StoreFaults {
        StoreFaults { latency_min_ms: 2, latency_max_ms: 40, p_slow: 0.02, p_fail_before: 0.02, p_fail_after: 0.02 }
    }
}

#[derive(Default, Debug)]
pub struct StoreStats {
    pub gets: AtomicU64,
    pub puts: AtomicU64,
    pub deletes: AtomicU64,
    pub lists: AtomicU64,
    pub injected_failures: AtomicU64,
}

pub struct SimStore {
    pub mem: Arc<MemStore>,
    rng: Arc<Mutex<Rng>>,
    faults: Mutex<StoreFaults>,
    enabled: AtomicBool,
    trace: Arc<Trace>,
    pub stats: StoreStats,
}

impl SimStore {
    pub fn new(mem: Arc<MemStore>, rng: Rng, faults: StoreFaults, trace: Arc<Trace>) -> SimStore {
        SimStore {
            mem,
            rng: Arc::new(Mutex::new(rng)),
            faults: Mutex::new(faults),
            enabled: AtomicBool::new(true),
            trace,
            stats: StoreStats::default(),
        }
    }

    /// Turn fault injection on or off (latency stays).
    pub fn set_faults_enabled(&self, on: bool) {
        self.enabled.store(on, Ordering::SeqCst);
    }

    pub fn set_faults(&self, f: StoreFaults) {
        *self.faults.lock().unwrap_or_else(|e| e.into_inner()) = f;
    }

    fn draw(&self) -> (u64, bool, bool) {
        let f = self.faults.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let mut r = self.rng.lock().unwrap_or_else(|e| e.into_inner());
        let mut lat = r.range(f.latency_min_ms, f.latency_max_ms.max(f.latency_min_ms) + 1);
        if r.chance(f.p_slow) {
            lat *= 10;
        }
        let on = self.enabled.load(Ordering::SeqCst);
        let before = on && r.chance(f.p_fail_before);
        let after = on && r.chance(f.p_fail_after);
        (lat, before, after)
    }

    fn injected(&self) {
        self.stats.injected_failures.fetch_add(1, Ordering::Relaxed);
    }
}

fn unavailable(m: &str) -> StoreError {
    StoreError::Unavailable(m.to_string())
}

impl ObjectStore for SimStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        Box::pin(async move {
            self.stats.gets.fetch_add(1, Ordering::Relaxed);
            let (lat, before, _) = self.draw();
            tokio::time::sleep(Duration::from_millis(lat)).await;
            if before {
                self.injected();
                self.trace.event(&format!("store get {key} -> injected error"));
                return Err(unavailable("injected get failure"));
            }
            let r = self.mem.get_sync(key, &opts);
            self.trace.event(&format!(
                "store get {key} -> {}",
                match &r {
                    Ok(g) => format!("ok {} {}", g.etag, g.data.len()),
                    Err(e) => e.to_string(),
                }
            ));
            r
        })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move {
            self.stats.puts.fetch_add(1, Ordering::Relaxed);
            let (lat, before, after) = self.draw();
            let (tx, rx) = tokio::sync::oneshot::channel();
            let mem = self.mem.clone();
            let trace = self.trace.clone();
            let key2 = key.to_string();
            // Detached: survives the caller being cancelled (crash).
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(lat / 2 + 1)).await;
                let r =
                    if before { Err(unavailable("injected put failure")) } else { mem.put_sync(&key2, data, &mode) };
                trace.event(&format!(
                    "store put {key2} {mode:?} -> {}",
                    match &r {
                        Ok(e) => format!("ok {e}"),
                        Err(e) => e.to_string(),
                    }
                ));
                tokio::time::sleep(Duration::from_millis(lat / 2 + 1)).await;
                let r = match r {
                    Ok(_) if after => Err(unavailable("injected: response lost")),
                    other => other,
                };
                let _ = tx.send(r);
            });
            let r = rx.await.unwrap_or_else(|_| Err(unavailable("store task dropped")));
            if before || after {
                self.injected();
            }
            r
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        Box::pin(async move {
            self.stats.deletes.fetch_add(1, Ordering::Relaxed);
            let (lat, before, _) = self.draw();
            tokio::time::sleep(Duration::from_millis(lat)).await;
            if before {
                self.injected();
                return Err(unavailable("injected delete failure"));
            }
            self.mem.delete_sync(key);
            self.trace.event(&format!("store delete {key}"));
            Ok(())
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        limit: usize,
    ) -> BoxFut<'a, SResult<ListPage>> {
        Box::pin(async move {
            self.stats.lists.fetch_add(1, Ordering::Relaxed);
            let (lat, before, _) = self.draw();
            tokio::time::sleep(Duration::from_millis(lat)).await;
            if before {
                self.injected();
                return Err(unavailable("injected list failure"));
            }
            let p = self.mem.list_sync(prefix, start_after, limit);
            self.trace.event(&format!("store list {prefix} -> {}", p.objects.len()));
            Ok(p)
        })
    }
}
