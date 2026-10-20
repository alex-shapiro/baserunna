//! Simulated object store with fault injection and optional list lag.

use super::Trace;
use crate::store::*;
use crate::util::rng::Rng;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Debug)]
pub struct StoreFaults {
    pub latency_min_ms: u64,
    pub latency_max_ms: u64,
    /// Transfer time added per MiB of a `PUT` body or `GET` response, as on
    /// real stores, where a large manifest takes measurably longer to write.
    pub ms_per_mib: u64,
    /// Probability of 10x request latency.
    pub p_slow: f64,
    /// Probability that a request stalls for `stall_ms` before succeeding.
    pub p_stall: f64,
    pub stall_ms: (u64, u64),
    /// Probability that a request fails before being applied.
    pub p_fail_before: f64,
    /// Probability that a write is applied but its response is lost.
    pub p_fail_after: f64,
    /// Maximum duration for which `list` may omit new objects or include deleted objects.
    pub list_lag_ms: u64,
    /// Probability that a write response is delayed by `late_reply_ms` after the write is applied.
    pub p_late_reply: f64,
    pub late_reply_ms: (u64, u64),
}

impl StoreFaults {
    pub fn none() -> StoreFaults {
        StoreFaults {
            latency_min_ms: 1,
            latency_max_ms: 5,
            ms_per_mib: 0,
            p_slow: 0.0,
            p_stall: 0.0,
            stall_ms: (0, 0),
            p_fail_before: 0.0,
            p_fail_after: 0.0,
            list_lag_ms: 0,
            p_late_reply: 0.0,
            late_reply_ms: (0, 0),
        }
    }
}

struct Draw {
    latency_ms: u64,
    late_reply_ms: u64,
    fail_before: bool,
    fail_after: bool,
    list_lag_ms: u64,
}

#[derive(Default)]
struct ListView {
    /// Newly created objects hidden from `list` until the given instant.
    hidden_until: BTreeMap<String, Instant>,
    /// Deleted objects still returned by `list` until the given instant.
    ghosts: BTreeMap<String, (ObjectMeta, Instant)>,
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
    view: Arc<Mutex<ListView>>,
    held_replies: Arc<HeldReplies>,
    pub stats: StoreStats,
}

#[derive(Default)]
struct HeldReplies {
    suffix: Mutex<Option<String>>,
    released: tokio::sync::Notify,
}

impl HeldReplies {
    async fn wait(&self, key: &str) {
        loop {
            let released = self.released.notified();
            let mut released = std::pin::pin!(released);
            released.as_mut().enable();
            let held = self.suffix.lock().unwrap_or_else(|e| e.into_inner()).as_ref().is_some_and(|s| key.ends_with(s));
            if !held {
                return;
            }
            released.await;
        }
    }
}

impl SimStore {
    pub fn new(mem: Arc<MemStore>, rng: Rng, faults: StoreFaults, trace: Arc<Trace>) -> SimStore {
        SimStore {
            mem,
            rng: Arc::new(Mutex::new(rng)),
            faults: Mutex::new(faults),
            enabled: AtomicBool::new(true),
            trace,
            view: Arc::new(Mutex::new(ListView::default())),
            held_replies: Arc::new(HeldReplies::default()),
            stats: StoreStats::default(),
        }
    }

    pub fn set_faults_enabled(&self, on: bool) {
        self.enabled.store(on, Ordering::SeqCst);
    }

    pub fn set_faults(&self, f: StoreFaults) {
        *self.faults.lock().unwrap_or_else(|e| e.into_inner()) = f;
    }

    /// Blocks write responses for keys ending in `suffix` until [`SimStore::release_replies`] is called.
    pub fn hold_replies(&self, suffix: &str) {
        *self.held_replies.suffix.lock().unwrap_or_else(|e| e.into_inner()) = Some(suffix.to_string());
    }

    pub fn release_replies(&self) {
        *self.held_replies.suffix.lock().unwrap_or_else(|e| e.into_inner()) = None;
        self.held_replies.released.notify_waiters();
    }

    fn draw(&self) -> Draw {
        let f = self.faults.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let mut r = self.rng.lock().unwrap_or_else(|e| e.into_inner());
        let mut latency_ms = r.range(f.latency_min_ms, f.latency_max_ms.max(f.latency_min_ms) + 1);
        if r.chance(f.p_slow) {
            latency_ms *= 10;
        }
        let on = self.enabled.load(Ordering::SeqCst);
        if on && r.chance(f.p_stall) {
            latency_ms = r.range(f.stall_ms.0, f.stall_ms.1.max(f.stall_ms.0) + 1);
            self.trace.event(&format!("store stall {latency_ms}ms"));
        }
        // Avoid advancing the RNG when disabled so existing seeds remain reproducible.
        let late_reply_ms = if on && f.p_late_reply > 0.0 && r.chance(f.p_late_reply) {
            r.range(f.late_reply_ms.0, f.late_reply_ms.1.max(f.late_reply_ms.0) + 1)
        } else {
            0
        };
        Draw {
            latency_ms,
            late_reply_ms,
            fail_before: on && r.chance(f.p_fail_before),
            fail_after: on && r.chance(f.p_fail_after),
            list_lag_ms: if on && f.list_lag_ms > 0 { r.below(f.list_lag_ms + 1) } else { 0 },
        }
    }

    fn transfer_time(&self, bytes: usize) -> Duration {
        let ms_per_mib = self.faults.lock().unwrap_or_else(|e| e.into_inner()).ms_per_mib;
        Duration::from_millis((bytes as u64 * ms_per_mib) >> 20)
    }

    fn view(&self) -> std::sync::MutexGuard<'_, ListView> {
        self.view.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn lagged_list(&self, prefix: &str, start_after: Option<&str>, limit: usize) -> ListPage {
        let now = Instant::now();
        let mut view = self.view();
        view.hidden_until.retain(|_, t| *t > now);
        view.ghosts.retain(|_, (_, t)| *t > now);
        if view.hidden_until.is_empty() && view.ghosts.is_empty() {
            return self.mem.list_sync(prefix, start_after, limit);
        }
        let after = |k: &str| k.starts_with(prefix) && start_after.is_none_or(|s| k > s);
        let mut all: BTreeMap<String, ObjectMeta> = self
            .mem
            .list_sync(prefix, start_after, usize::MAX)
            .objects
            .into_iter()
            .filter(|o| !view.hidden_until.contains_key(&o.key))
            .map(|o| (o.key.clone(), o))
            .collect();
        for (k, (m, _)) in &view.ghosts {
            if after(k) {
                all.entry(k.clone()).or_insert_with(|| m.clone());
            }
        }
        let limit = limit.max(1);
        let more = all.len() > limit;
        let objects: Vec<ObjectMeta> = all.into_values().take(limit).collect();
        let next = if more { objects.last().map(|o| o.key.clone()) } else { None };
        ListPage { objects, next }
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
            let d = self.draw();
            tokio::time::sleep(Duration::from_millis(d.latency_ms)).await;
            if d.fail_before {
                self.injected();
                self.trace.event(&format!("store get {key} -> injected error"));
                return Err(unavailable("injected get failure"));
            }
            let r = self.mem.get_sync(key, &opts);
            if let Ok(g) = &r {
                tokio::time::sleep(self.transfer_time(g.data.len())).await;
            }
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
            let Draw { latency_ms: lat, late_reply_ms, fail_before: before, fail_after: after, list_lag_ms } =
                self.draw();
            let lat = lat + self.transfer_time(data.len()).as_millis() as u64;
            let held_replies = self.held_replies.clone();
            let (tx, rx) = tokio::sync::oneshot::channel();
            let mem = self.mem.clone();
            let trace = self.trace.clone();
            let view = self.view.clone();
            let key2 = key.to_string();
            // Detached so in-flight writes can still land if the caller crashes.
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(lat / 2 + 1)).await;
                let existed = mem.get_sync(&key2, &GetOpts::default()).is_ok();
                let r =
                    if before { Err(unavailable("injected put failure")) } else { mem.put_sync(&key2, data, &mode) };
                if r.is_ok() && list_lag_ms > 0 {
                    let mut v = view.lock().unwrap_or_else(|e| e.into_inner());
                    v.ghosts.remove(&key2);
                    if !existed {
                        v.hidden_until.insert(key2.clone(), Instant::now() + Duration::from_millis(list_lag_ms));
                    }
                }
                trace.event(&format!(
                    "store put {key2} {mode:?} -> {}",
                    match &r {
                        Ok(e) => format!("ok {e}"),
                        Err(e) => e.to_string(),
                    }
                ));
                tokio::time::sleep(Duration::from_millis(lat / 2 + 1)).await;
                if r.is_ok() && late_reply_ms > 0 {
                    trace.event(&format!("store put {key2} reply delayed {late_reply_ms}ms"));
                    tokio::time::sleep(Duration::from_millis(late_reply_ms)).await;
                }
                held_replies.wait(&key2).await;
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
            let d = self.draw();
            tokio::time::sleep(Duration::from_millis(d.latency_ms)).await;
            if d.fail_before {
                self.injected();
                return Err(unavailable("injected delete failure"));
            }
            if d.list_lag_ms > 0
                && let Ok(g) = self.mem.get_sync(key, &GetOpts::default())
            {
                let meta = ObjectMeta {
                    key: key.to_string(),
                    size: g.size,
                    etag: g.etag,
                    last_modified_ms: g.last_modified_ms,
                };
                let until = Instant::now() + Duration::from_millis(d.list_lag_ms);
                let mut v = self.view();
                v.hidden_until.remove(key);
                v.ghosts.insert(key.to_string(), (meta, until));
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
            let d = self.draw();
            tokio::time::sleep(Duration::from_millis(d.latency_ms)).await;
            if d.fail_before {
                self.injected();
                return Err(unavailable("injected list failure"));
            }
            let p = self.lagged_list(prefix, start_after, limit);
            self.trace.event(&format!("store list {prefix} -> {}", p.objects.len()));
            Ok(p)
        })
    }

    fn time_precision_ms(&self) -> u64 {
        self.mem.time_precision_ms()
    }
}
