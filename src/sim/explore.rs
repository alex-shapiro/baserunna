//! Guided exploration (SPEC §13.4). A run that reaches a rare situation is
//! branched just after it reached it, so that later runs start from that
//! state instead of having to reach it again, as Antithesis restarts tests
//! from the checkpoints its assertions mark. A branched run replays its
//! parent exactly up to the branch point and then draws other random
//! numbers (see [`crate::util::rng::branch`]).
//!
//! What counts as new is a feature: a probe together with the magnitude of
//! its hit count, as coverage-guided fuzzers count edges, or two rare
//! probes reached in one run, which branching makes likelier than fresh
//! runs do. Runs that show a feature no earlier run showed join the corpus;
//! parents are chosen, and branched after their rarest probes, in
//! proportion to how rare those are.

use super::campaign::default_threads;
use super::workloads::Scenario;
use super::{SimOutcome, run};
use crate::probe::{Hits, Probe};
use crate::util::rng::Rng;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

/// A run to perform: a scenario, a seed, and the branch points of its path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Job {
    pub scenario: usize,
    pub seed: u64,
    pub branches: Vec<(u64, u64)>,
}

impl Job {
    /// The `sim_tool` arguments that repeat this run.
    pub fn args(&self, scenarios: &[Scenario]) -> String {
        let path: Vec<String> = self.branches.iter().map(|(ms, salt)| format!("{ms}:{salt:x}")).collect();
        format!("{} {} {}", scenarios[self.scenario].name, self.seed, path.join(",")).trim().to_string()
    }
}

/// A probe and the magnitude of its hit count in one run, or (with a
/// second probe in place of the magnitude) two rare probes reached together.
pub type Feature = (Probe, u8);

/// Probes reached in fewer runs than this share count as rare.
const RARE: f64 = 0.05;

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

/// The features of a run.
pub fn features(h: &Hits) -> Vec<Feature> {
    h.hit().map(|p| (p, bucket(h.count(p)))).collect()
}

struct Entry {
    job: Job,
    probes: Hits,
}

/// Pairs of rare probes, as features: the second probe's index stands in
/// for the magnitude, offset past every magnitude.
fn pairs(h: &Hits, report: &Report) -> Vec<Feature> {
    let rare: Vec<Probe> = h.hit().filter(|p| report.share(*p) < RARE).collect();
    let mut out = Vec::new();
    for (i, a) in rare.iter().enumerate() {
        for b in &rare[i + 1..] {
            out.push((*a, 8 + (*b as usize).min(247) as u8));
        }
    }
    out
}

/// What an exploration found.
#[derive(Default)]
pub struct Report {
    pub runs: u64,
    pub branched: u64,
    pub corpus: usize,
    /// Runs that showed each feature.
    pub features: BTreeMap<Feature, u64>,
    pub failures: Vec<(Job, String)>,
    /// Runs that reached each probe, indexed like [`Probe::ALL`].
    pub runs_hitting: Vec<u64>,
}

impl Report {
    /// The share of runs that reached `p`.
    pub fn share(&self, p: Probe) -> f64 {
        self.runs_hitting.get(p as usize).copied().unwrap_or(0) as f64 / self.runs.max(1) as f64
    }

    fn add(&mut self, job: &Job, out: &SimOutcome) -> bool {
        self.runs += 1;
        self.branched += !job.branches.is_empty() as u64;
        self.runs_hitting.resize(Probe::ALL.len(), 0);
        for p in out.probes.hit() {
            self.runs_hitting[p as usize] += 1;
        }
        let mut new = false;
        let mut fs = features(&out.probes);
        fs.extend(pairs(&out.probes, self));
        for f in fs {
            let n = self.features.entry(f).or_default();
            new |= *n == 0;
            *n += 1;
        }
        if let Err(e) = &out.result {
            self.failures.push((job.clone(), e.clone()));
        }
        new
    }

    /// Probes some run reached.
    pub fn probes(&self) -> BTreeSet<Probe> {
        self.features.keys().map(|(p, _)| *p).collect()
    }
}

/// How an exploration spends its runs.
pub struct Budget {
    pub runs: u64,
    /// Starts no run after this.
    pub deadline: Option<std::time::Instant>,
    pub threads: usize,
    /// Share of runs from fresh seeds rather than branched from the corpus.
    pub fresh: f64,
}

impl Budget {
    pub fn runs(runs: u64) -> Budget {
        Budget { runs, deadline: None, threads: default_threads(), fresh: 0.5 }
    }
}

/// Explores `scenarios` from `first_seed` on. The choice of runs depends
/// only on `first_seed` and the outcomes, so an exploration with a run
/// budget is deterministic. `progress` sees each outcome.
pub fn explore(
    scenarios: &[Scenario],
    first_seed: u64,
    budget: &Budget,
    progress: impl Fn(&Job, &SimOutcome) + Sync,
) -> Report {
    let mut rng = Rng::new(first_seed ^ 0xe8_9107e);
    let mut report = Report::default();
    let mut corpus: Vec<Entry> = Vec::new();
    // Fresh runs take every scenario in turn, a seed at a time, as a
    // campaign does.
    let mut fresh: u64 = 0;
    let generation = 4 * budget.threads.max(1);
    while report.runs < budget.runs && budget.deadline.is_none_or(|d| std::time::Instant::now() < d) {
        let n = generation.min((budget.runs - report.runs) as usize);
        let mut jobs = Vec::with_capacity(n);
        for _ in 0..n {
            if corpus.is_empty() || rng.chance(budget.fresh) {
                let len = scenarios.len() as u64;
                jobs.push(Job { scenario: (fresh % len) as usize, seed: first_seed + fresh / len, branches: vec![] });
                fresh += 1;
            } else {
                jobs.push(branch(&mut rng, &corpus, &report));
            }
        }
        let outs = run_all(scenarios, &jobs, budget.threads, &progress);
        for (job, out) in jobs.into_iter().zip(outs) {
            if report.add(&job, &out) {
                corpus.push(Entry { job, probes: out.probes });
            }
        }
    }
    report.corpus = corpus.len();
    report
}

/// A branch of a corpus entry: after a probe of it, chosen by rarity.
fn branch(rng: &mut Rng, corpus: &[Entry], report: &Report) -> Job {
    let rarity = |p: &Probe| 1.0 / report.runs_hitting.get(*p as usize).copied().unwrap_or(1).max(1) as f64;
    let weights: Vec<f64> = corpus.iter().map(|e| e.probes.hit().map(|p| rarity(&p)).sum()).collect();
    let e = &corpus[pick(rng, &weights)];
    let ps: Vec<Probe> = e.probes.hit().collect();
    let p = ps[pick(rng, &ps.iter().map(rarity).collect::<Vec<_>>())];
    let after = e.probes.first_ms(p).unwrap_or(0);
    let last = e.job.branches.last().map_or(0, |(ms, _)| *ms);
    let at = after.max(last) + 1 + rng.below(2_000);
    let mut job = e.job.clone();
    job.branches.push((at, rng.next_u64() | 1));
    job
}

fn pick(rng: &mut Rng, weights: &[f64]) -> usize {
    let total: f64 = weights.iter().sum();
    let mut x = rng.unit() * total;
    for (i, w) in weights.iter().enumerate() {
        if x < *w {
            return i;
        }
        x -= w;
    }
    weights.len() - 1
}

fn run_all(
    scenarios: &[Scenario],
    jobs: &[Job],
    threads: usize,
    progress: &(impl Fn(&Job, &SimOutcome) + Sync),
) -> Vec<SimOutcome> {
    let next = std::sync::atomic::AtomicUsize::new(0);
    let outs: Mutex<Vec<Option<SimOutcome>>> = Mutex::new(jobs.iter().map(|_| None).collect());
    std::thread::scope(|scope| {
        for _ in 0..threads.max(1).min(jobs.len()) {
            scope.spawn(|| {
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let Some(job) = jobs.get(i) else { return };
                    let out = run_job(scenarios, job);
                    progress(job, &out);
                    outs.lock().unwrap_or_else(|e| e.into_inner())[i] = Some(out);
                }
            });
        }
    });
    outs.into_inner().unwrap_or_else(|e| e.into_inner()).into_iter().map(|o| o.expect("every job ran")).collect()
}

/// Performs one run.
pub fn run_job(scenarios: &[Scenario], job: &Job) -> SimOutcome {
    let s = &scenarios[job.scenario];
    let mut cfg = s.config_for(job.seed);
    cfg.branches = job.branches.clone();
    run(job.seed, cfg, s.run)
}

/// Parses branch points as [`Job::args`] writes them.
pub fn parse_branches(s: &str) -> Option<Vec<(u64, u64)>> {
    s.split(',')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (ms, salt) = p.split_once(':')?;
            Some((ms.parse().ok()?, u64::from_str_radix(salt, 16).ok()?))
        })
        .collect()
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
    }
}
