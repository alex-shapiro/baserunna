//! Execution environment abstracting time, randomness, and task spawning for
//! deterministic simulation.

use crate::util::rng::Rng;
use std::future::Future;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::task::JoinSet;
use tokio::time::Instant;

enum Clock {
    Real,
    Sim { base_ms: u64, origin: Instant, skew_ms: AtomicI64 },
}

pub type Tracer = std::sync::Arc<dyn Fn(&str) + Send + Sync>;

pub struct Env {
    clock: Clock,
    /// The simulation's seeded generator; real environments use OS randomness, which needs no lock.
    rng: Option<Mutex<Rng>>,
    tasks: Mutex<JoinSet<()>>,
    tracer: Mutex<Option<Tracer>>,
}

impl Env {
    /// Creates a production environment backed by the system clock and OS CSPRNG.
    pub fn real() -> Env {
        Env { clock: Clock::Real, rng: None, tasks: Mutex::new(JoinSet::new()), tracer: Mutex::new(None) }
    }

    /// Creates a simulated environment with virtual time starting at `origin` (`base_ms`) and clock offset `skew_ms`.
    pub fn sim(seed: u64, base_ms: u64, origin: Instant, skew_ms: i64) -> Env {
        Env {
            clock: Clock::Sim { base_ms, origin, skew_ms: AtomicI64::new(skew_ms) },
            rng: Some(Mutex::new(Rng::new(seed))),
            tasks: Mutex::new(JoinSet::new()),
            tracer: Mutex::new(None),
        }
    }

    /// Installs a trace callback for engine events.
    pub fn set_tracer(&self, t: Tracer) {
        *self.tracer.lock().unwrap_or_else(|e| e.into_inner()) = Some(t);
    }

    pub fn tracing(&self) -> bool {
        self.tracer.lock().unwrap_or_else(|e| e.into_inner()).is_some()
    }

    /// Emits a trace event if a tracer is installed.
    pub fn trace(&self, f: impl FnOnce() -> String) {
        let t = self.tracer.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(t) = t {
            t(&f());
        }
    }

    pub fn is_sim(&self) -> bool {
        matches!(self.clock, Clock::Sim { .. })
    }

    /// Wall-clock milliseconds since the Unix epoch.
    pub fn now_ms(&self) -> u64 {
        match &self.clock {
            Clock::Real => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            Clock::Sim { base_ms, origin, skew_ms } => {
                let elapsed = Instant::now().saturating_duration_since(*origin).as_millis() as i64;
                (*base_ms as i64 + elapsed + skew_ms.load(Ordering::SeqCst)).max(0) as u64
            }
        }
    }

    /// Advances the simulated wall-clock skew by `delta_ms`.
    pub fn step_clock(&self, delta_ms: i64) {
        if let Clock::Sim { skew_ms, .. } = &self.clock {
            skew_ms.fetch_add(delta_ms, Ordering::SeqCst);
        }
    }

    pub fn fill_random(&self, out: &mut [u8]) {
        match &self.rng {
            Some(r) => r.lock().unwrap_or_else(|e| e.into_inner()).fill(out),
            None => aws_lc_rs::rand::fill(out).expect("OS randomness unavailable"),
        }
    }

    pub fn random_bytes(&self, n: usize) -> Vec<u8> {
        let mut v = vec![0u8; n];
        self.fill_random(&mut v);
        v
    }

    pub fn random_u64(&self) -> u64 {
        let mut b = [0u8; 8];
        self.fill_random(&mut b);
        u64::from_le_bytes(b)
    }

    pub fn random_u128(&self) -> u128 {
        let mut b = [0u8; 16];
        self.fill_random(&mut b);
        u128::from_le_bytes(b)
    }

    /// Returns a random integer in `0..n`.
    pub fn random_below(&self, n: u64) -> u64 {
        self.random_u64() % n.max(1)
    }

    /// Generates `nbytes` random bytes encoded as lowercase hex.
    pub fn random_hex(&self, nbytes: usize) -> String {
        crate::util::b64::hex(&self.random_bytes(nbytes))
    }

    /// Spawns a background task aborted on [`Env::shutdown`].
    pub fn spawn<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut t = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
        while t.try_join_next().is_some() {}
        t.spawn(fut);
    }

    /// Runs a blocking closure off the async worker threads, or inline under simulation.
    pub async fn run_blocking<T: Send + 'static>(&self, f: impl FnOnce() -> T + Send + 'static) -> T {
        match self.clock {
            Clock::Sim { .. } => f(),
            Clock::Real => match tokio::task::spawn_blocking(f).await {
                Ok(v) => v,
                Err(e) => std::panic::resume_unwind(e.into_panic()),
            },
        }
    }

    /// Aborts all tasks spawned in this environment.
    pub fn shutdown(&self) {
        let mut t = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
        t.abort_all();
    }

    pub fn task_count(&self) -> usize {
        let mut t = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
        while t.try_join_next().is_some() {}
        t.len()
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        if let Ok(t) = self.tasks.get_mut() {
            t.abort_all();
        }
    }
}

pub async fn sleep_ms(ms: u64) {
    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
}
