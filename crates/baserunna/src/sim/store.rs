//! Simulated object store with fault injection and optional list lag.

use super::Trace;
use crate::store::*;
use crate::util::rng::Rng;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Debug)]
pub struct StoreFaults {
    pub latency_min_ms: u64,
    pub latency_max_ms: u64,
    /// Simulated transfer latency per MiB for `GET` and `PUT` payloads.
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
    /// Probability that a write fails and is applied `late_apply_ms` later.
    pub p_late_apply: f64,
    pub late_apply_ms: (u64, u64),
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
            p_late_apply: 0.0,
            late_apply_ms: (0, 0),
        }
    }
}

struct Draw {
    latency_ms: u64,
    late_reply_ms: u64,
    late_apply_ms: u64,
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
    /// `(key, If-Match ETag)` of each failed write that the store will apply late.
    late_put_targets: Arc<Mutex<Vec<(String, String)>>>,
    /// `(ETag, apply instant)` of the last applied write to each manifest key.
    manifest_applied_at: Arc<Mutex<BTreeMap<String, (String, Instant)>>>,
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
            let held = self.suffix.lock().as_ref().is_some_and(|s| key.ends_with(s));
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
            late_put_targets: Arc::default(),
            manifest_applied_at: Arc::default(),
            stats: StoreStats::default(),
        }
    }

    pub fn set_faults_enabled(&self, on: bool) {
        self.enabled.store(on, Ordering::SeqCst);
    }

    pub fn set_faults(&self, f: StoreFaults) {
        *self.faults.lock() = f;
    }

    /// Blocks write responses for keys ending in `suffix` until [`SimStore::release_replies`] is called.
    pub fn hold_replies(&self, suffix: &str) {
        *self.held_replies.suffix.lock() = Some(suffix.to_string());
    }

    pub fn release_replies(&self) {
        *self.held_replies.suffix.lock() = None;
        self.held_replies.released.notify_waiters();
    }

    /// Returns the `If-Match` ETags of the failed writes to `key` that the store will still apply late.
    pub fn late_put_targets(&self, key: &str) -> Vec<String> {
        self.late_put_targets.lock().iter().filter(|(k, _)| k == key).map(|(_, e)| e.clone()).collect()
    }

    fn draw(&self) -> Draw {
        let f = self.faults.lock().clone();
        let mut r = self.rng.lock();
        let mut latency_ms = r.range(f.latency_min_ms, f.latency_max_ms.max(f.latency_min_ms) + 1);
        if r.chance(f.p_slow) {
            latency_ms *= 10;
        }
        let on = self.enabled.load(Ordering::SeqCst);
        if on && r.chance(f.p_stall) {
            latency_ms = r.range(f.stall_ms.0, f.stall_ms.1.max(f.stall_ms.0) + 1);
            self.trace.event(&format!("store stall {latency_ms}ms"));
        }
        let late_reply_ms = if on && f.p_late_reply > 0.0 && r.chance(f.p_late_reply) {
            r.range(f.late_reply_ms.0, f.late_reply_ms.1.max(f.late_reply_ms.0) + 1)
        } else {
            0
        };
        let late_apply_ms = if on && f.p_late_apply > 0.0 && r.chance(f.p_late_apply) {
            r.range(f.late_apply_ms.0, f.late_apply_ms.1.max(f.late_apply_ms.0) + 1)
        } else {
            0
        };
        Draw {
            latency_ms,
            late_reply_ms,
            late_apply_ms,
            fail_before: on && r.chance(f.p_fail_before),
            fail_after: on && r.chance(f.p_fail_after),
            list_lag_ms: if on && f.list_lag_ms > 0 { r.below(f.list_lag_ms + 1) } else { 0 },
        }
    }

    fn transfer_time(&self, bytes: usize) -> Duration {
        let ms_per_mib = self.faults.lock().ms_per_mib;
        Duration::from_millis((bytes as u64 * ms_per_mib) >> 20)
    }

    fn lagged_list(&self, prefix: &str, start_after: Option<&str>, limit: usize) -> ListPage {
        let now = Instant::now();
        let mut view = self.view.lock();
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
            let Draw {
                latency_ms: lat,
                late_reply_ms,
                late_apply_ms,
                fail_before: before,
                fail_after: after,
                list_lag_ms,
            } = self.draw();
            let late_apply = late_apply_ms > 0 && !before;
            let lat = lat + self.transfer_time(data.len()).as_millis() as u64;
            let held_replies = self.held_replies.clone();
            let (tx, rx) = tokio::sync::oneshot::channel();
            let mut reply = Some(tx);
            let mem = self.mem.clone();
            let trace = self.trace.clone();
            let view = self.view.clone();
            let late_put_targets = self.late_put_targets.clone();
            let manifest_applied_at = self.manifest_applied_at.clone();
            let late_put_target = match &mode {
                PutMode::IfMatch(e) if late_apply => Some((key.to_string(), e.clone())),
                _ => None,
            };
            let key2 = key.to_string();
            // Detached so in-flight writes can still land if the caller crashes.
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(lat / 2 + 1)).await;
                if late_apply && let Some(tx) = reply.take() {
                    late_put_targets.lock().extend(late_put_target.clone());
                    let _ = tx.send(Err(unavailable("injected: put failed, applies later")));
                    trace.event(&format!("store put {key2} fails now, applies in {late_apply_ms}ms"));
                    tokio::time::sleep(Duration::from_millis(late_apply_ms)).await;
                }
                let existed = mem.get_sync(&key2, &GetOpts::default()).is_ok();
                let r =
                    if before { Err(unavailable("injected put failure")) } else { mem.put_sync(&key2, data, &mode) };
                if let Some(target) = &late_put_target {
                    let mut targets = late_put_targets.lock();
                    if let Some(i) = targets.iter().position(|t| t == target) {
                        targets.remove(i);
                    }
                }
                if let Ok(etag) = &r
                    && key2.ends_with("/manifest")
                {
                    manifest_applied_at.lock().insert(key2.clone(), (etag.clone(), Instant::now()));
                }
                if r.is_ok() && list_lag_ms > 0 {
                    let mut v = view.lock();
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
                let Some(tx) = reply else { return };
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
            if before || after || late_apply {
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
                let mut v = self.view.lock();
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

impl super::lease_check::ManifestHistory for SimStore {
    fn stored_manifest_etag(&self, db: &str) -> Option<String> {
        self.mem.get_sync(&crate::engine::manifest_key(db), &GetOpts::default()).ok().map(|g| g.etag)
    }

    fn manifest_applied_at(&self, db: &str, etag: &str) -> Option<Instant> {
        let applied = self.manifest_applied_at.lock();
        applied.get(&crate::engine::manifest_key(db)).filter(|(e, _)| e == etag).map(|(_, at)| *at)
    }

    fn late_manifest_put_targets(&self, db: &str) -> Vec<String> {
        self.late_put_targets(&crate::engine::manifest_key(db))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn late_applied_puts_fail_then_apply_with_their_precondition() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
        rt.block_on(async {
            let origin = Instant::now();
            let mem = Arc::new(MemStore::new(Box::new(move || origin.elapsed().as_millis() as u64)));
            let faults = StoreFaults { p_late_apply: 1.0, late_apply_ms: (1_000, 1_000), ..StoreFaults::none() };
            let store = SimStore::new(mem.clone(), Rng::new(1), faults, Arc::new(super::super::Trace::new(false)));
            let etag = mem.put_sync("k", Bytes::from_static(b"v1"), &PutMode::Overwrite).unwrap();
            let put = |data: &'static [u8], etag: &str| {
                store.put("k", Bytes::from_static(data), PutMode::IfMatch(etag.into()))
            };
            assert!(matches!(put(b"v2", &etag).await, Err(StoreError::Unavailable(_))));
            assert!(matches!(put(b"v3", &etag).await, Err(StoreError::Unavailable(_))));
            assert_eq!(store.late_put_targets("k"), [etag.clone(), etag.clone()], "the pending puts are not listed");
            let read = || mem.get_sync("k", &GetOpts::default()).unwrap().data;
            assert_eq!(read(), Bytes::from_static(b"v1"), "a late put was applied before its delay");
            tokio::time::sleep(Duration::from_millis(1_100)).await;
            assert_eq!(
                read(),
                Bytes::from_static(b"v2"),
                "the first late put is applied; the second fails its If-Match"
            );
            assert!(store.late_put_targets("k").is_empty(), "applied puts are still listed");
        });
    }
}
