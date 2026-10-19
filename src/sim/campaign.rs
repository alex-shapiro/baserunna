//! Simulation campaigns: many seeds of many scenarios run in parallel, with a
//! report of failures and of how often each probe was reached.

use super::workloads::Scenario;
use super::{SimOutcome, run};
use crate::probe::{Hits, Probe};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

/// One failed run.
#[derive(Clone, Debug)]
pub struct Failure {
    pub scenario: &'static str,
    pub seed: u64,
    pub error: String,
}

/// What a campaign found.
#[derive(Default)]
pub struct Report {
    pub runs: u64,
    pub failures: Vec<Failure>,
    /// Probe hits summed over every run of each scenario.
    pub hits: BTreeMap<&'static str, Hits>,
    /// Runs that reached each probe, indexed like [`Probe::ALL`].
    pub runs_hitting: Vec<u64>,
    pub virtual_ms: u64,
}

impl Report {
    fn add(&mut self, scenario: &'static str, out: &SimOutcome) {
        self.runs += 1;
        self.virtual_ms += out.virtual_ms;
        self.hits.entry(scenario).or_default().add(&out.probes);
        self.runs_hitting.resize(Probe::ALL.len(), 0);
        for p in out.probes.hit() {
            self.runs_hitting[p as usize] += 1;
        }
        if let Err(e) = &out.result {
            self.failures.push(Failure { scenario, seed: out.seed, error: e.clone() });
        }
    }

    /// Probes no run reached.
    pub fn unreached(&self) -> Vec<Probe> {
        Probe::ALL.iter().copied().filter(|p| self.runs_hitting.get(*p as usize).copied().unwrap_or(0) == 0).collect()
    }

    /// A table of every probe: the share of runs that reached it and the
    /// scenarios that reached it most.
    pub fn coverage_table(&self) -> String {
        let mut out = String::new();
        for p in Probe::ALL {
            let runs = self.runs_hitting.get(*p as usize).copied().unwrap_or(0);
            let mut by: Vec<(&str, u64)> =
                self.hits.iter().map(|(s, h)| (*s, h.count(*p))).filter(|(_, n)| *n > 0).collect();
            by.sort_by(|a, b| b.1.cmp(&a.1));
            let top: Vec<String> = by.iter().take(3).map(|(s, n)| format!("{s}:{n}")).collect();
            out.push_str(&format!(
                "{:<36} {:>7.2}% {:>6} runs  {}\n",
                p.name(),
                100.0 * runs as f64 / self.runs.max(1) as f64,
                runs,
                if top.is_empty() { "NEVER".to_string() } else { top.join(" ") }
            ));
        }
        out
    }
}

/// Runs every `(scenario, seed)` pair on `threads` threads, seed by seed,
/// starting no run after `deadline`. `progress` sees each outcome as it
/// completes.
pub fn campaign(
    scenarios: &[Scenario],
    seeds: &[u64],
    threads: usize,
    deadline: Option<std::time::Instant>,
    progress: impl Fn(&'static str, &SimOutcome) + Sync,
) -> Report {
    let jobs: Vec<(usize, u64)> = seeds.iter().flat_map(|s| (0..scenarios.len()).map(move |i| (i, *s))).collect();
    let next = AtomicUsize::new(0);
    let report = Mutex::new(Report::default());
    std::thread::scope(|scope| {
        for _ in 0..threads.max(1) {
            scope.spawn(|| {
                loop {
                    if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
                        return;
                    }
                    let j = next.fetch_add(1, Ordering::SeqCst);
                    let Some(&(i, seed)) = jobs.get(j) else { return };
                    let s = &scenarios[i];
                    let out = run(seed, s.config_for(seed), s.run);
                    progress(s.name, &out);
                    report.lock().unwrap_or_else(|e| e.into_inner()).add(s.name, &out);
                }
            });
        }
    });
    let mut r = report.into_inner().unwrap_or_else(|e| e.into_inner());
    r.failures.sort_by(|a, b| (a.scenario, a.seed).cmp(&(b.scenario, b.seed)));
    r
}

/// Threads to use by default: every core.
pub fn default_threads() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}
