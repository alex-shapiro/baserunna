//! The object store's clock, as seen from one node.
//!
//! Garbage collection compares timestamps written by different nodes and by
//! the store itself (object `last_modified`). Taking all of them from the
//! store's clock makes those comparisons independent of node clock skew. A
//! node measures its offset from the store by writing a probe object and
//! reading back its `last_modified`; the offset is kept relative to the
//! node's monotonic clock, so wall-clock jumps do not disturb it.

use crate::error::{Error, Result};
use crate::store::{GetOpts, ObjectStore, PutMode, Retry};
use bytes::Bytes;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::Instant;

/// Probe object key used to measure the object store's clock.
pub const PROBE_KEY: &str = "clock";

#[derive(Clone, Copy, Debug)]
struct Measurement {
    /// Store milliseconds minus local monotonic milliseconds.
    offset_ms: i64,
    /// Half-width of the interval the true offset lies in.
    error_ms: u64,
    at: Instant,
}

pub struct StoreClock {
    origin: Instant,
    last: Mutex<Option<Measurement>>,
    syncing: tokio::sync::Mutex<()>,
}

impl Default for StoreClock {
    fn default() -> Self {
        StoreClock::new()
    }
}

impl StoreClock {
    pub fn new() -> StoreClock {
        StoreClock { origin: Instant::now(), last: Mutex::new(None), syncing: tokio::sync::Mutex::new(()) }
    }

    fn mono_ms(&self, t: Instant) -> i64 {
        t.saturating_duration_since(self.origin).as_millis() as i64
    }

    fn last(&self) -> Option<Measurement> {
        *self.last.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The store's current time, from a measurement at most `max_age` old.
    pub fn now_ms(&self, max_age: Duration) -> Option<u64> {
        let m = self.last().filter(|m| m.at.elapsed() <= max_age)?;
        Some((self.mono_ms(Instant::now()) + m.offset_ms).max(0) as u64)
    }

    /// Lower bound on the current store time, accounting for clock sync error.
    pub fn earliest_now_ms(&self, max_age: Duration) -> Option<u64> {
        let m = self.last().filter(|m| m.at.elapsed() <= max_age)?;
        Some((self.mono_ms(Instant::now()) + m.offset_ms - m.error_ms as i64).max(0) as u64)
    }

    /// True if the last measurement is missing or older than `interval`.
    pub fn stale(&self, interval: Duration) -> bool {
        self.last().is_none_or(|m| m.at.elapsed() >= interval)
    }

    /// Measures the store clock offset if no measurement younger than `interval`
    /// exists, probing up to three times to find a bound within `max_error_ms`.
    pub async fn sync(
        &self,
        store: &dyn ObjectStore,
        retry: &Retry,
        interval: Duration,
        max_error_ms: u64,
    ) -> Result<()> {
        let _g = self.syncing.lock().await;
        if !self.stale(interval) {
            return Ok(());
        }
        let mut best: Option<Measurement> = None;
        for _ in 0..3 {
            let m = self.probe(store, retry).await?;
            if best.is_none_or(|b| m.error_ms < b.error_ms) {
                best = Some(m);
            }
            if m.error_ms <= max_error_ms / 2 {
                break;
            }
        }
        match best {
            Some(m) if m.error_ms <= max_error_ms => {
                *self.last.lock().unwrap_or_else(|e| e.into_inner()) = Some(m);
                Ok(())
            }
            m => Err(Error::unavailable(format!(
                "store clock measurement too imprecise ({} ms)",
                m.map(|m| m.error_ms).unwrap_or_default()
            ))),
        }
    }

    async fn probe(&self, store: &dyn ObjectStore, retry: &Retry) -> Result<Measurement> {
        let t0 = Instant::now();
        retry.run(|| store.put(PROBE_KEY, Bytes::from_static(b"probe"), PutMode::Overwrite)).await?;
        let g = retry.run(|| store.get(PROBE_KEY, GetOpts::default())).await?;
        let t2 = Instant::now();
        // Our write, or a later one by another node, was applied between
        // t0 and t2; its timestamp may be truncated.
        let lm = g.last_modified_ms as i64;
        let lo = lm - self.mono_ms(t2);
        let hi = lm + store.time_precision_ms() as i64 - self.mono_ms(t0);
        Ok(Measurement { offset_ms: lo + (hi - lo) / 2, error_ms: ((hi - lo) / 2) as u64, at: t2 })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemStore;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    #[test]
    fn measures_the_store_clock_through_local_clock_jumps() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
        rt.block_on(async {
            let origin = Instant::now();
            let ahead = Arc::new(AtomicI64::new(5 * 3_600_000));
            let a = ahead.clone();
            let store = MemStore::new(Box::new(move || {
                (1_700_000_000_000 + origin.elapsed().as_millis() as i64 + a.load(Ordering::SeqCst)) as u64
            }));
            let c = StoreClock::new();
            let day = Duration::from_secs(86_400);
            assert_eq!(c.now_ms(day), None);
            assert!(c.stale(Duration::from_secs(1)));
            c.sync(&store, &Retry::default(), Duration::from_secs(60), 10_000).await.unwrap();
            let true_now = || 1_700_000_000_000 + origin.elapsed().as_millis() as i64 + 5 * 3_600_000;
            let err = 1;
            tokio::time::sleep(Duration::from_secs(600)).await;
            let now = c.now_ms(day).unwrap() as i64;
            assert!((now - true_now()).abs() <= err, "{now} vs {}", true_now());
            assert_eq!(c.now_ms(Duration::from_secs(60)), None, "too old a measurement is not used");
            ahead.store(0, Ordering::SeqCst);
            c.sync(&store, &Retry::default(), Duration::from_secs(3_600), 10_000).await.unwrap();
            assert!((c.now_ms(day).unwrap() as i64 - true_now()).abs() <= err);
            c.sync(&store, &Retry::default(), Duration::from_secs(60), 10_000).await.unwrap();
            let now = c.now_ms(day).unwrap() as i64;
            assert!((now - (true_now() - 5 * 3_600_000)).abs() <= err, "remeasured");
        });
    }

    /// Test store that delays GETs by the configured durations and truncates
    /// `last_modified` to the declared precision.
    struct Slow(MemStore, std::sync::Mutex<Vec<Duration>>, u64, std::sync::atomic::AtomicUsize);

    impl Slow {
        fn new(mem: MemStore, delays: &[u64]) -> Slow {
            let delays = delays.iter().rev().map(|d| Duration::from_millis(*d)).collect();
            Slow(mem, std::sync::Mutex::new(delays), 1, Default::default())
        }

        fn probes(&self) -> usize {
            self.3.load(Ordering::SeqCst)
        }

        fn delay(&self) -> Duration {
            self.3.fetch_add(1, Ordering::SeqCst);
            let mut d = self.1.lock().unwrap();
            if d.len() > 1 { d.pop().unwrap() } else { d[0] }
        }
    }

    impl ObjectStore for Slow {
        fn get<'a>(
            &'a self,
            key: &'a str,
            opts: GetOpts,
        ) -> crate::store::BoxFut<'a, crate::store::SResult<crate::store::GetResult>> {
            Box::pin(async move {
                tokio::time::sleep(self.delay()).await;
                let mut g = self.0.get(key, opts).await?;
                g.last_modified_ms -= g.last_modified_ms % self.2;
                Ok(g)
            })
        }

        fn put<'a>(
            &'a self,
            key: &'a str,
            data: Bytes,
            mode: PutMode,
        ) -> crate::store::BoxFut<'a, crate::store::SResult<String>> {
            self.0.put(key, data, mode)
        }

        fn delete<'a>(&'a self, key: &'a str) -> crate::store::BoxFut<'a, crate::store::SResult<()>> {
            self.0.delete(key)
        }

        fn list<'a>(
            &'a self,
            prefix: &'a str,
            after: Option<&'a str>,
            limit: usize,
        ) -> crate::store::BoxFut<'a, crate::store::SResult<crate::store::ListPage>> {
            self.0.list(prefix, after, limit)
        }

        fn time_precision_ms(&self) -> u64 {
            self.2
        }
    }

    #[test]
    fn slow_probes_are_not_trusted() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
        rt.block_on(async {
            let origin = Instant::now();
            let clock = move || 1_700_000_000_000 + origin.elapsed().as_millis() as u64;
            let truth = || clock() as i64;
            let day = Duration::from_secs(86_400);
            let c = StoreClock::new();
            let r = c
                .sync(&Slow::new(MemStore::new(Box::new(clock)), &[300]), &Retry::default(), Duration::ZERO, 100)
                .await;
            assert!(r.is_err() && c.now_ms(day).is_none(), "a 150 ms error passed a 100 ms bound");
            let store = Slow::new(MemStore::new(Box::new(clock)), &[300, 10, 300]);
            c.sync(&store, &Retry::default(), Duration::ZERO, 100).await.expect("the fast probe counts");
            assert!((c.now_ms(day).unwrap() as i64 - truth()).abs() <= 5);
            assert_eq!(store.probes(), 2, "it stopped after the fast probe");
            let fast = Slow::new(MemStore::new(Box::new(clock)), &[10, 10]);
            c.sync(&fast, &Retry::default(), Duration::ZERO, 100).await.unwrap();
            assert_eq!(fast.probes(), 1, "one precise probe is enough");
            let mut coarse = Slow::new(MemStore::new(Box::new(clock)), &[10]);
            coarse.2 = 1_000;
            tokio::time::sleep(Duration::from_millis(1_700)).await;
            assert!(c.sync(&coarse, &Retry::default(), Duration::ZERO, 400).await.is_err());
            c.sync(&coarse, &Retry::default(), Duration::ZERO, 600).await.unwrap();
            assert!((c.now_ms(day).unwrap() as i64 - truth()).abs() <= 510);
        });
    }
}
