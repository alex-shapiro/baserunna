//! Coverage-guided simulation exploration (SPEC §13.4).
//!
//! Runs that discover new probe features are added to a corpus and branched
//! immediately after their rarest probes. A branched run replays its parent
//! deterministically up to the branch timestamp and then diverges with a new
//! PRNG salt.

use super::SimOutcome;
use super::campaign::{Job, Report, default_threads, run_jobs};
use super::workloads::Scenario;
use crate::probe::{Hits, Probe};
use crate::util::rng::Rng;
use parking_lot::Mutex;
use std::time::Instant;

/// Number of jobs scheduled per batch before updating the corpus. Kept
/// constant so exploration is independent of the host CPU core count.
const ROUND: usize = 32;

/// Maximum virtual time offset after a probe hit at which a branch point is placed.
const BRANCH_WINDOW_MS: u64 = 2_000;

struct Entry {
    job: Job,
    probes: Hits,
    end_ms: u64,
}

impl Entry {
    /// Returns the earliest valid branch timestamp after each probe hit in this run.
    fn starts(&self) -> impl Iterator<Item = (Probe, u64)> + '_ {
        let last = self.job.branches.last().map_or(0, |(ms, _)| *ms);
        self.probes.hit().filter_map(move |p| {
            let at = self.probes.first_ms(p)?.max(last) + 1;
            (at < self.end_ms).then_some((p, at))
        })
    }
}

/// Resource limits for an exploration run.
pub struct Budget {
    pub runs: u64,
    pub deadline: Option<Instant>,
    pub threads: usize,
    /// Probability of running a fresh seed instead of branching from the corpus.
    pub fresh: f64,
}

impl Budget {
    pub fn runs(runs: u64) -> Budget {
        Budget { runs, deadline: None, threads: default_threads(), fresh: 0.5 }
    }
}

/// Aggregated report and final corpus size from a guided exploration.
pub struct Exploration {
    pub report: Report,
    pub corpus: usize,
}

/// Explores `scenarios` starting from `first_seed` within `budget`.
/// When `budget.deadline` is `None`, the sequence of jobs is deterministic.
pub fn explore(
    scenarios: &[Scenario],
    first_seed: u64,
    budget: &Budget,
    progress: impl Fn(&Job, &SimOutcome) + Sync,
) -> Exploration {
    let mut rng = Rng::new(first_seed ^ 0xe8_9107e);
    let mut report = Report::default();
    let mut corpus: Vec<Entry> = Vec::new();
    let mut fresh: u64 = 0;
    while report.runs < budget.runs && budget.deadline.is_none_or(|d| Instant::now() < d) {
        let n = ROUND.min((budget.runs - report.runs) as usize);
        let mut jobs = Vec::with_capacity(n);
        for _ in 0..n {
            let branched = match rng.chance(budget.fresh) {
                true => None,
                false => branch(&mut rng, &corpus, &report),
            };
            jobs.push(branched.unwrap_or_else(|| {
                fresh += 1;
                Job::fresh(scenarios.len(), first_seed, fresh - 1)
            }));
        }
        let outs: Mutex<Vec<Option<SimOutcome>>> = Mutex::new(jobs.iter().map(|_| None).collect());
        run_jobs(
            scenarios,
            budget.threads,
            budget.deadline,
            |i| jobs.get(i).cloned(),
            |i, j, out| {
                progress(j, &out);
                outs.lock()[i] = Some(out);
            },
        );
        let outs = outs.into_inner();
        for (job, out) in jobs.into_iter().zip(outs) {
            let Some(out) = out else { continue };
            if report.add(scenarios, &job, &out) {
                corpus.push(Entry { job, probes: out.probes, end_ms: out.virtual_ms });
            }
        }
    }
    Exploration { corpus: corpus.len(), report }
}

/// Selects a parent corpus entry and a probe within that entry weighted by
/// inverse hit frequency, and returns a new branched [`Job`].
fn branch(rng: &mut Rng, corpus: &[Entry], report: &Report) -> Option<Job> {
    let rarity = |p| 1.0 / report.runs_hitting.get(p as usize).copied().unwrap_or(0).max(1) as f64;
    let weights: Vec<f64> = corpus.iter().map(|e| e.starts().map(|(p, _)| rarity(p)).sum()).collect();
    let e = &corpus[rng.weighted(&weights)?];
    let options: Vec<(Probe, u64)> = e.starts().collect();
    let (_, at) = options[rng.weighted(&options.iter().map(|(p, _)| rarity(*p)).collect::<Vec<_>>())?];
    let mut job = e.job.clone();
    job.branches.push((at + rng.below(BRANCH_WINDOW_MS.min(e.end_ms - at)), rng.next_u64() | 1));
    Some(job)
}
