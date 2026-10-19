//! Request admission rate limiting and authentication failure backoff.
//!
//! Enforces token-bucket rate limits per user, API key, and client address,
//! with exponential backoff on consecutive authentication failures.

use crate::error::Error;
use crate::util::DetHashMap;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::Instant;

/// A sustained rate with a burst allowance.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Rate {
    pub per_sec: f64,
    pub burst: f64,
}

impl Rate {
    /// Disables rate limiting.
    pub const OFF: Rate = Rate { per_sec: f64::INFINITY, burst: f64::INFINITY };

    /// Sustained rate of `n` requests per second with a burst capacity of `2n` (minimum 1.0).
    /// Zero or negative disables the limit.
    pub fn per_sec(n: f64) -> Rate {
        if n <= 0.0 { Rate::OFF } else { Rate { per_sec: n, burst: (2.0 * n).max(1.0) } }
    }

    /// Sustained rate of `n` requests per hour with custom `burst` capacity.
    pub fn per_hour(n: f64, burst: f64) -> Rate {
        if n <= 0.0 { Rate::OFF } else { Rate { per_sec: n / 3600.0, burst: burst.max(1.0) } }
    }

    fn unlimited(&self) -> bool {
        self.per_sec.is_infinite()
    }
}

#[derive(Clone, Debug)]
pub struct LimitConfig {
    /// Maximum request rate per user across all credentials.
    pub user: Rate,
    /// Default request rate per API key.
    pub api_key: Rate,
    /// Authentication request rate per client IP.
    pub auth_per_client: Rate,
    /// Account creation rate per client IP.
    pub signup_per_client: Rate,
    /// Outbound email rate per recipient address.
    pub mail_per_account: Rate,
    /// Failed authentication attempts allowed per account before backoff begins.
    pub account_free_failures: u32,
    /// Failed authentication attempts allowed per client IP before backoff begins.
    pub client_free_failures: u32,
    /// Initial backoff duration after exceeding free failure threshold.
    pub failure_backoff_ms: u64,
    /// Maximum backoff duration between failed authentication attempts.
    pub failure_backoff_max_ms: u64,
    /// Maximum concurrent PBKDF2 password hashing operations.
    pub hash_concurrency: usize,
    /// Timeout waiting for an available password hashing slot.
    pub hash_wait_ms: u64,
    /// Number of upstream reverse proxy hops to skip when parsing `X-Forwarded-For`.
    pub trusted_proxy_hops: usize,
}

impl Default for LimitConfig {
    fn default() -> Self {
        LimitConfig {
            user: Rate::per_sec(500.0),
            api_key: Rate::per_sec(500.0),
            auth_per_client: Rate { per_sec: 1.0, burst: 20.0 },
            signup_per_client: Rate::per_hour(20.0, 5.0),
            mail_per_account: Rate::per_hour(6.0, 3.0),
            account_free_failures: 5,
            client_free_failures: 20,
            failure_backoff_ms: 1_000,
            failure_backoff_max_ms: 15 * 60_000,
            hash_concurrency: std::thread::available_parallelism().map_or(2, |n| n.get()),
            hash_wait_ms: 5_000,
            trusted_proxy_hops: 0,
        }
    }
}

impl LimitConfig {
    /// Returns a configuration with all rate limits and backoff disabled.
    pub fn unlimited() -> LimitConfig {
        LimitConfig {
            user: Rate::OFF,
            api_key: Rate::OFF,
            auth_per_client: Rate::OFF,
            signup_per_client: Rate::OFF,
            mail_per_account: Rate::OFF,
            account_free_failures: u32::MAX,
            client_free_failures: u32::MAX,
            ..LimitConfig::default()
        }
    }
}

struct Bucket {
    tokens: f64,
    at: Instant,
    rate: Rate,
}

impl Bucket {
    fn refill(&mut self, now: Instant) {
        let dt = now.saturating_duration_since(self.at).as_secs_f64();
        self.tokens = (self.tokens + dt * self.rate.per_sec).min(self.rate.burst);
        self.at = now;
    }

    /// Returns the duration until the bucket has at least `need` tokens.
    fn wait(&self, need: f64) -> Duration {
        match self.tokens >= need {
            true => Duration::ZERO,
            false => Duration::from_secs_f64((need - self.tokens) / self.rate.per_sec),
        }
    }
}

struct Failures {
    count: u32,
    last: Instant,
    until: Instant,
}

/// Rate limiter and failure backoff tracker.
#[derive(Default)]
pub struct Limiter {
    buckets: Mutex<DetHashMap<String, Bucket>>,
    failures: Mutex<DetHashMap<String, Failures>>,
}

/// Maximum tracked keys before pruning idle state.
const MAX_KEYS: usize = 100_000;

fn limited(what: &str, wait: Duration) -> Error {
    let ms = (wait.as_millis() as u64).max(1);
    let after = if ms < 1_000 { format!("{ms} ms") } else { format!("{:.1} s", ms as f64 / 1000.0) };
    Error::RateLimited { message: format!("too many {what}; retry in {after}"), retry_after_ms: ms }
}

impl Limiter {
    /// Attempts to acquire one token from each of `keys`. If any bucket is
    /// exhausted, none are decremented and an error is returned.
    pub fn take(&self, keys: &[(&str, Rate)], what: &str) -> Result<(), Error> {
        self.take_inner(keys, what, false)
    }

    /// Acquires a token from each bucket in `keys`, allowing buckets to go negative by up to `rate.burst`.
    pub fn borrow(&self, keys: &[(&str, Rate)], what: &str) -> Result<(), Error> {
        self.take_inner(keys, what, true)
    }

    fn take_inner(&self, keys: &[(&str, Rate)], what: &str, borrow: bool) -> Result<(), Error> {
        let now = Instant::now();
        let mut g = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        if g.len() >= MAX_KEYS {
            prune_buckets(&mut g, now);
        }
        let mut wait = Duration::ZERO;
        for (k, rate) in keys.iter().filter(|(_, r)| !r.unlimited()) {
            let b = g.entry(k.to_string()).or_insert(Bucket { tokens: rate.burst, at: now, rate: *rate });
            b.rate = *rate;
            b.refill(now);
            wait = wait.max(b.wait(if borrow { 1.0 - rate.burst } else { 1.0 }));
        }
        if !wait.is_zero() {
            return Err(limited(what, wait));
        }
        for (k, _) in keys.iter().filter(|(_, r)| !r.unlimited()) {
            if let Some(b) = g.get_mut(*k) {
                b.tokens -= 1.0;
            }
        }
        Ok(())
    }

    /// Returns an error if any of `keys` is currently in backoff.
    pub fn check_backoff(&self, keys: &[&str], what: &str) -> Result<(), Error> {
        let now = Instant::now();
        let g = self.failures.lock().unwrap_or_else(|e| e.into_inner());
        let wait = keys.iter().filter_map(|k| g.get(*k)).map(|f| f.until.saturating_duration_since(now)).max();
        match wait {
            Some(w) if !w.is_zero() => Err(limited(what, w)),
            _ => Ok(()),
        }
    }

    /// Records a failed attempt for `key`, applying exponential backoff once
    /// `free` attempts are exceeded.
    pub fn failed(&self, key: &str, free: u32, cfg: &LimitConfig) {
        let now = Instant::now();
        let mut g = self.failures.lock().unwrap_or_else(|e| e.into_inner());
        if g.len() >= MAX_KEYS {
            prune_failures(&mut g, now, cfg);
        }
        let f = g.entry(key.to_string()).or_insert(Failures { count: 0, last: now, until: now });
        if now.duration_since(f.last) > forget_after(cfg) {
            f.count = 0;
        }
        f.count = f.count.saturating_add(1);
        f.last = now;
        if f.count > free {
            let doublings = (f.count - free - 1).min(30);
            let ms = cfg.failure_backoff_ms.saturating_mul(1 << doublings).min(cfg.failure_backoff_max_ms);
            f.until = now + Duration::from_millis(ms);
        }
    }

    pub fn succeeded(&self, key: &str) {
        self.failures.lock().unwrap_or_else(|e| e.into_inner()).remove(key);
    }

    /// Drops full buckets and expired failure records.
    pub fn prune(&self, cfg: &LimitConfig) {
        let now = Instant::now();
        prune_buckets(&mut self.buckets.lock().unwrap_or_else(|e| e.into_inner()), now);
        prune_failures(&mut self.failures.lock().unwrap_or_else(|e| e.into_inner()), now, cfg);
    }

    pub fn tracked(&self) -> usize {
        self.buckets.lock().unwrap_or_else(|e| e.into_inner()).len()
            + self.failures.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

fn forget_after(cfg: &LimitConfig) -> Duration {
    Duration::from_millis(4 * cfg.failure_backoff_max_ms)
}

fn prune_buckets(g: &mut DetHashMap<String, Bucket>, now: Instant) {
    g.retain(|_, b| {
        b.refill(now);
        b.tokens < b.rate.burst
    });
}

fn prune_failures(g: &mut DetHashMap<String, Failures>, now: Instant, cfg: &LimitConfig) {
    g.retain(|_, f| now.duration_since(f.last) <= forget_after(cfg));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap()
    }

    fn wait_ms(r: Result<(), Error>) -> u64 {
        match r {
            Err(Error::RateLimited { retry_after_ms, .. }) => retry_after_ms,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn buckets_admit_a_burst_then_the_rate() {
        rt().block_on(async {
            let l = Limiter::default();
            let r = Rate { per_sec: 2.0, burst: 3.0 };
            for _ in 0..3 {
                l.take(&[("k", r)], "requests").unwrap();
            }
            assert_eq!(wait_ms(l.take(&[("k", r)], "requests")), 500);
            l.take(&[("other", r)], "requests").unwrap();
            tokio::time::sleep(Duration::from_millis(500)).await;
            l.take(&[("k", r)], "requests").unwrap();
            assert!(l.take(&[("k", r)], "requests").is_err());
            tokio::time::sleep(Duration::from_secs(60)).await;
            for _ in 0..3 {
                l.take(&[("k", r)], "requests").unwrap();
            }
            assert!(l.take(&[("k", r)], "requests").is_err(), "refills up to the burst only");
            for _ in 0..1000 {
                l.take(&[("k", Rate::OFF)], "requests").unwrap();
            }
        });
    }

    #[test]
    fn takes_are_all_or_nothing() {
        rt().block_on(async {
            let l = Limiter::default();
            let user = Rate { per_sec: 1.0, burst: 5.0 };
            let key = Rate { per_sec: 1.0, burst: 1.0 };
            l.take(&[("user", user), ("key", key)], "requests").unwrap();
            assert!(l.take(&[("user", user), ("key", key)], "requests").is_err());
            for _ in 0..4 {
                l.take(&[("user", user)], "requests").unwrap();
            }
            assert!(l.take(&[("user", user)], "requests").is_err(), "the refused take used no user token");
        });
    }

    #[test]
    fn borrowing_overdraws_by_at_most_a_burst() {
        rt().block_on(async {
            let l = Limiter::default();
            let rate = Rate { per_sec: 1.0, burst: 2.0 };
            l.take(&[("user", rate)], "requests").unwrap();
            l.take(&[("user", rate)], "requests").unwrap();
            assert!(l.take(&[("user", rate)], "requests").is_err());
            l.borrow(&[("user", rate)], "requests").unwrap();
            l.borrow(&[("user", rate)], "requests").unwrap();
            assert!(l.borrow(&[("user", rate)], "requests").is_err());
            tokio::time::sleep(Duration::from_millis(2_000)).await;
            assert_eq!(wait_ms(l.take(&[("user", rate)], "requests")), 1_000);
        });
    }

    #[test]
    fn failures_back_off_exponentially() {
        rt().block_on(async {
            let cfg = LimitConfig { failure_backoff_ms: 100, failure_backoff_max_ms: 1_000, ..Default::default() };
            let l = Limiter::default();
            for _ in 0..3 {
                l.failed("a", 3, &cfg);
                l.check_backoff(&["a"], "sign-ins").unwrap();
            }
            let mut waits = Vec::new();
            for _ in 0..6 {
                l.failed("a", 3, &cfg);
                waits.push(wait_ms(l.check_backoff(&["x", "a"], "sign-ins")));
            }
            assert_eq!(waits, [100, 200, 400, 800, 1_000, 1_000]);
            tokio::time::sleep(Duration::from_millis(1_000)).await;
            l.check_backoff(&["a"], "sign-ins").unwrap();
            l.failed("a", 3, &cfg);
            assert_eq!(wait_ms(l.check_backoff(&["a"], "sign-ins")), 1_000, "still counting");
            l.succeeded("a");
            l.check_backoff(&["a"], "sign-ins").unwrap();
            for _ in 0..4 {
                l.failed("b", 3, &cfg);
            }
            tokio::time::sleep(Duration::from_millis(4_001)).await;
            l.failed("b", 3, &cfg);
            l.check_backoff(&["b"], "sign-ins").unwrap();
        });
    }

    #[test]
    fn pruning_drops_idle_state() {
        rt().block_on(async {
            let cfg = LimitConfig { failure_backoff_max_ms: 1_000, ..Default::default() };
            let l = Limiter::default();
            let r = Rate { per_sec: 1.0, burst: 2.0 };
            l.take(&[("a", r)], "requests").unwrap();
            l.take(&[("b", r)], "requests").unwrap();
            l.failed("f", 1, &cfg);
            assert_eq!(l.tracked(), 3);
            tokio::time::sleep(Duration::from_millis(500)).await;
            l.take(&[("b", r)], "requests").unwrap();
            l.prune(&cfg);
            assert_eq!(l.tracked(), 3, "nothing has refilled yet");
            tokio::time::sleep(Duration::from_millis(1_000)).await;
            l.prune(&cfg);
            assert_eq!(l.tracked(), 2, "a refilled");
            tokio::time::sleep(Duration::from_millis(4_000)).await;
            l.prune(&cfg);
            assert_eq!(l.tracked(), 0);
        });
    }
}
