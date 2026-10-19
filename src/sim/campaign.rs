//! Simulation campaigns: runs of scenarios on every core, and a report of
//! their failures and of how often each probe was reached. A plain campaign
//! runs every scenario seed by seed; guided exploration ([`super::explore`])
//! chooses its runs as it goes.

use super::workloads::Scenario;
use super::{SimOutcome, run};
use crate::probe::{Hits, Probe};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// A run: a scenario, a seed, and the branch points of its path (see
/// [`crate::util::rng::branch`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Job {
    pub scenario: usize,
    pub seed: u64,
    pub branches: Vec<(u64, u64)>,
}

impl Job {
    /// The `sim_tool trace` arguments that repeat this run.
    pub fn args(&self, scenarios: &[Scenario]) -> String {
        let path: Vec<String> = self.branches.iter().map(|(ms, salt)| format!("{ms}:{salt:x}")).collect();
        format!("{} {} {}", scenarios[self.scenario].name, self.seed, path.join(",")).trim().to_string()
    }

    pub fn run(&self, scenarios: &[Scenario]) -> SimOutcome {
        let s = &scenarios[self.scenario];
        let mut cfg = s.config_for(self.seed);
        cfg.branches = self.branches.clone();
        run(self.seed, cfg, s.run)
    }
}

/// Parses branch points as [`Job::args`] writes them; `None` unless they
/// are well formed and in increasing order of time.
pub fn parse_branches(s: &str) -> Option<Vec<(u64, u64)>> {
    let points: Vec<(u64, u64)> = s
        .split(',')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (ms, salt) = p.split_once(':')?;
            Some((ms.parse().ok()?, u64::from_str_radix(salt, 16).ok()?))
        })
        .collect::<Option<_>>()?;
    points.windows(2).all(|w| w[0].0 < w[1].0).then_some(points)
}

/// A probe and the magnitude of its hit count in one run, or (with a
/// second probe in place of the magnitude) two rare probes reached together.
pub type Feature = (Probe, u8);

/// Probes reached in fewer runs than this share count as rare.
pub const RARE: f64 = 0.05;

fn bucket(n: u64) -> u8 {
    match n {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4..=7 => 4,
        8..=15 => 5,
        16..=127 => 6,
        _ => 7,
    }
}

/// What a campaign found.
#[derive(Default)]
pub struct Report {
    pub runs: u64,
    /// Runs with branch points.
    pub branched: u64,
    pub virtual_ms: u64,
    pub failures: Vec<(Job, String)>,
    /// Probe hits summed over every run of each scenario.
    pub hits: BTreeMap<&'static str, Hits>,
    /// Runs that reached each probe, indexed like [`Probe::ALL`].
    pub runs_hitting: Vec<u64>,
    /// Runs that showed each feature.
    pub features: BTreeMap<Feature, u64>,
}

impl Report {
    /// Counts `out`, the outcome of `job`, and returns whether it showed a
    /// feature no earlier run did.
    pub fn add(&mut self, scenarios: &[Scenario], job: &Job, out: &SimOutcome) -> bool {
        self.runs += 1;
        self.branched += !job.branches.is_empty() as u64;
        self.virtual_ms += out.virtual_ms;
        self.hits.entry(scenarios[job.scenario].name).or_default().add(&out.probes);
        self.runs_hitting.resize(Probe::ALL.len(), 0);
        for p in out.probes.hit() {
            self.runs_hitting[p as usize] += 1;
        }
        if let Err(e) = &out.result {
            self.failures.push((job.clone(), e.clone()));
        }
        let mut new = false;
        for f in self.features_of(&out.probes) {
            let n = self.features.entry(f).or_default();
            new |= *n == 0;
            *n += 1;
        }
        new
    }

    /// The features of a run with probe hits `h`.
    fn features_of(&self, h: &Hits) -> Vec<Feature> {
        let mut out: Vec<Feature> = h.hit().map(|p| (p, bucket(h.count(p)))).collect();
        // The second probe's index stands in for the magnitude, past every
        // magnitude.
        let rare: Vec<Probe> = h.hit().filter(|p| self.share(*p) < RARE).collect();
        for (i, a) in rare.iter().enumerate() {
            for b in &rare[i + 1..] {
                out.push((*a, 8 + (*b as usize).min(247) as u8));
            }
        }
        out
    }

    /// The share of runs that reached `p`.
    pub fn share(&self, p: Probe) -> f64 {
        self.runs_hitting.get(p as usize).copied().unwrap_or(0) as f64 / self.runs.max(1) as f64
    }

    /// Probes no run reached.
    pub fn unreached(&self) -> Vec<Probe> {
        Probe::ALL.iter().copied().filter(|p| self.share(*p) == 0.0).collect()
    }

    /// Probes some run reached.
    pub fn probes(&self) -> BTreeSet<Probe> {
        Probe::ALL.iter().copied().filter(|p| self.share(*p) > 0.0).collect()
    }

    /// A table of every probe: the share of runs that reached it and the
    /// scenarios that reached it most.
    pub fn coverage_table(&self) -> String {
        let mut out = String::new();
        for p in Probe::ALL {
            let runs = self.runs_hitting.get(*p as usize).copied().unwrap_or(0);
            let mut by: Vec<(&str, u64)> =
                self.hits.iter().map(|(s, h)| (*s, h.count(*p))).filter(|(_, n)| *n > 0).collect();
            by.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
            let top: Vec<String> = by.iter().take(3).map(|(s, n)| format!("{s}:{n}")).collect();
            out.push_str(&format!(
                "{:<36} {:>7.2}% {:>6} runs  {}\n",
                p.name(),
                100.0 * self.share(*p),
                runs,
                if top.is_empty() { "NEVER".to_string() } else { top.join(" ") }
            ));
        }
        out
    }
}

/// Runs `job(0)`, `job(1)`, … on `threads` threads until `job` returns
/// `None`, starting no run after `deadline`. `done` sees each run's index,
/// job and outcome as it completes.
pub fn run_jobs(
    scenarios: &[Scenario],
    threads: usize,
    deadline: Option<Instant>,
    job: impl Fn(usize) -> Option<Job> + Sync,
    done: impl Fn(usize, &Job, SimOutcome) + Sync,
) {
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..threads.max(1) {
            scope.spawn(|| {
                while deadline.is_none_or(|d| Instant::now() < d) {
                    let i = next.fetch_add(1, Ordering::SeqCst);
                    let Some(j) = job(i) else { return };
                    let out = j.run(scenarios);
                    done(i, &j, out);
                }
            });
        }
    });
}

/// Runs every scenario with each of `seeds`, seed by seed, on `threads`
/// threads, starting no run after `deadline`. `progress` sees each run.
pub fn campaign(
    scenarios: &[Scenario],
    seeds: std::ops::Range<u64>,
    threads: usize,
    deadline: Option<Instant>,
    progress: impl Fn(&Job, &SimOutcome) + Sync,
) -> Report {
    let n = scenarios.len() as u64;
    let report = Mutex::new(Report::default());
    let job = |i: usize| {
        let seed = seeds.start + i as u64 / n;
        (seed < seeds.end).then(|| Job { scenario: (i as u64 % n) as usize, seed, branches: vec![] })
    };
    run_jobs(scenarios, threads, deadline, job, |_, j, out| {
        progress(j, &out);
        report.lock().unwrap_or_else(|e| e.into_inner()).add(scenarios, j, &out);
    });
    let mut r = report.into_inner().unwrap_or_else(|e| e.into_inner());
    r.failures.sort_by_key(|(j, _)| (j.scenario, j.seed));
    r
}

/// Threads to use by default: every core.
pub fn default_threads() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_grow_with_magnitude() {
        let b: Vec<u8> = [0, 1, 2, 3, 4, 7, 8, 100, 1000].iter().map(|n| bucket(*n)).collect();
        assert_eq!(b, [0, 1, 2, 3, 4, 4, 5, 6, 7]);
    }

    #[test]
    fn branch_points_round_trip() {
        let job = Job { scenario: 0, seed: 7, branches: vec![(1_500, 0xab), (9_000, 0x1)] };
        let scenarios = super::super::workloads::scenarios();
        let args = job.args(&scenarios);
        let path = args.split(' ').nth(2).unwrap();
        assert_eq!(parse_branches(path), Some(job.branches));
        assert_eq!(parse_branches(""), Some(vec![]));
        assert_eq!(parse_branches("9000:1,1500:2"), None, "points out of order");
    }
}
