//! Guided exploration (SPEC §13.4). A run that reaches a rare situation is
//! branched just after it reached it, so that later runs start from that
//! state instead of having to reach it again, as Antithesis restarts tests
//! from the checkpoints its assertions mark. A branched run replays its
//! parent exactly up to the branch point and then draws other random
//! numbers (see [`crate::util::rng::branch`]).
//!
//! Runs that show a feature no earlier run showed ([`Report::add`]) join
//! the corpus; parents are chosen, and branched after their rarest probes,
//! in proportion to how rare those are.

use super::SimOutcome;
use super::campaign::{Job, Report, default_threads, run_jobs};
use super::workloads::Scenario;
use crate::probe::{Hits, Probe};
use crate::util::rng::Rng;
use std::sync::Mutex;
use std::time::Instant;

/// Runs chosen together, before their outcomes join the corpus, and so the
/// most threads an exploration keeps busy. Fixed, so that an exploration
/// does not depend on the machine's core count.
const ROUND: usize = 32;

/// Latest a branch point falls after the probe it follows.
const BRANCH_WINDOW_MS: u64 = 2_000;

struct Entry {
    job: Job,
    probes: Hits,
    /// How long the run lasted: a branch point after that changes nothing.
    end_ms: u64,
}

impl Entry {
    /// The earliest branch point after each probe that leaves the run time
    /// to go another way.
    fn starts(&self) -> impl Iterator<Item = (Probe, u64)> + '_ {
        let last = self.job.branches.last().map_or(0, |(ms, _)| *ms);
        self.probes.hit().filter_map(move |p| {
            let at = self.probes.first_ms(p)?.max(last) + 1;
            (at < self.end_ms).then_some((p, at))
        })
    }
}

/// How an exploration spends its runs.
pub struct Budget {
    pub runs: u64,
    /// Starts no run after this.
    pub deadline: Option<Instant>,
    pub threads: usize,
    /// Share of runs from fresh seeds rather than branched from the corpus.
    pub fresh: f64,
}

impl Budget {
    pub fn runs(runs: u64) -> Budget {
        Budget { runs, deadline: None, threads: default_threads(), fresh: 0.5 }
    }
}

/// What an exploration found: the campaign report of its runs, and the
/// size of its corpus.
pub struct Exploration {
    pub report: Report,
    pub corpus: usize,
}

/// Explores `scenarios` from `first_seed` on. The choice of runs depends
/// only on `first_seed` and the outcomes, so an exploration bounded by runs
/// rather than by a deadline is deterministic. `progress` sees each run.
pub fn explore(
    scenarios: &[Scenario],
    first_seed: u64,
    budget: &Budget,
    progress: impl Fn(&Job, &SimOutcome) + Sync,
) -> Exploration {
    let mut rng = Rng::new(first_seed ^ 0xe8_9107e);
    let mut report = Report::default();
    let mut corpus: Vec<Entry> = Vec::new();
    // Fresh runs take every scenario in turn, a seed at a time, as a
    // campaign does.
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
                outs.lock().unwrap_or_else(|e| e.into_inner())[i] = Some(out);
            },
        );
        let outs = outs.into_inner().unwrap_or_else(|e| e.into_inner());
        for (job, out) in jobs.into_iter().zip(outs) {
            // Runs the deadline kept from starting are left out.
            let Some(out) = out else { continue };
            if report.add(scenarios, &job, &out) {
                corpus.push(Entry { job, probes: out.probes, end_ms: out.virtual_ms });
            }
        }
    }
    Exploration { corpus: corpus.len(), report }
}

/// A branch of a corpus entry after a probe of it, both chosen by rarity,
/// or `None` if no entry has a probe to branch after.
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
