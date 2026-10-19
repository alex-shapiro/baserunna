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

/// Runs chosen together, before their outcomes join the corpus. Fixed, so
/// that an exploration does not depend on the machine's core count.
const ROUND: usize = 32;

struct Entry {
    job: Job,
    probes: Hits,
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
            if corpus.is_empty() || rng.chance(budget.fresh) {
                let len = scenarios.len() as u64;
                jobs.push(Job { scenario: (fresh % len) as usize, seed: first_seed + fresh / len, branches: vec![] });
                fresh += 1;
            } else {
                jobs.push(branch(&mut rng, &corpus, &report));
            }
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
                corpus.push(Entry { job, probes: out.probes });
            }
        }
    }
    Exploration { corpus: corpus.len(), report }
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
