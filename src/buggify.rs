//! Deterministic fault and rare-path injection for simulation (SPEC §13.2).
//!
//! Each call site is identified by its source location. When a site is reached
//! for the first time in a simulation run, it is enabled or disabled for the
//! rest of that run. An enabled site is triggered on each call with a
//! caller-specified probability. Buggify is disabled outside simulation.

use crate::util::rng::Rng;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::panic::Location;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Report {
    pub sites: BTreeMap<String, Site>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Site {
    pub active: bool,
    pub fired: u64,
}

struct State {
    rng: Rng,
    // Separate PRNG so task reordering does not alter site activation draws.
    reorder_rng: Rng,
    p_reorder: f64,
    p_active: f64,
    faults: bool,
    crashes: bool,
    quiet: bool,
    sites: BTreeMap<(&'static str, u32, u32), Site>,
}

thread_local! {
    static STATE: RefCell<Option<State>> = const { RefCell::new(None) };
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Config {
    /// Probability that a call site is enabled for the run.
    pub p_active: f64,
    /// Enables client-visible fault sites.
    pub faults: bool,
    /// Enables node crash points.
    pub crashes: bool,
    /// Probability that a simulated task yields before it is polled.
    pub p_reorder: f64,
}

/// Enables buggify on the current thread.
pub fn enable(seed: u64, c: Config) {
    let (rng, reorder_rng) = (Rng::new(seed), Rng::new(seed ^ 0x5eed));
    let Config { p_active, faults, crashes, p_reorder } = c;
    STATE.with(|s| {
        let sites = BTreeMap::new();
        *s.borrow_mut() = Some(State { rng, reorder_rng, p_reorder, p_active, faults, crashes, quiet: false, sites })
    });
}

/// Pauses or resumes client-visible fault sites and crash points.
pub fn set_quiet(quiet: bool) {
    STATE.with(|s| {
        if let Some(st) = s.borrow_mut().as_mut() {
            st.quiet = quiet;
        }
    });
}

/// Returns true if the current simulated task should yield before it is polled.
pub(crate) fn reorder() -> bool {
    STATE.with(|s| s.borrow_mut().as_mut().is_some_and(|st| st.p_reorder > 0.0 && st.reorder_rng.chance(st.p_reorder)))
}

/// Disables buggify on the current thread and returns the run report.
pub fn disable() -> Report {
    let st = STATE.with(|s| s.borrow_mut().take());
    let sites = st
        .map(|st| st.sites.into_iter().map(|((f, l, c), s)| (format!("{f}:{l}:{c}"), s)).collect())
        .unwrap_or_default();
    Report { sites }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Benign,
    Fault,
    Crash,
}

fn fire_at(loc: &'static Location<'static>, p: f64, kind: Kind) -> bool {
    STATE.with(|s| {
        let mut g = s.borrow_mut();
        let Some(st) = g.as_mut() else { return false };
        if (kind == Kind::Fault && (!st.faults || st.quiet)) || (kind == Kind::Crash && (!st.crashes || st.quiet)) {
            return false;
        }
        let key = (loc.file(), loc.line(), loc.column());
        let site = match st.sites.get_mut(&key) {
            Some(site) => site,
            None => {
                let active = st.rng.chance(st.p_active);
                st.sites.entry(key).or_insert(Site { active, fired: 0 })
            }
        };
        let fire = site.active && st.rng.chance(p);
        site.fired += fire as u64;
        fire
    })
}

/// Returns true if the calling site is triggered with probability `p`.
#[track_caller]
pub fn fires(p: f64) -> bool {
    fire_at(Location::caller(), p, Kind::Benign)
}

/// Returns true if the calling site is triggered with probability 0.25.
#[track_caller]
pub fn buggify() -> bool {
    fire_at(Location::caller(), 0.25, Kind::Benign)
}

/// Returns true if the calling crash point is triggered with probability `p`.
#[track_caller]
pub(crate) fn crash_fires(p: f64) -> bool {
    fire_at(Location::caller(), p, Kind::Crash)
}

/// Returns true if a client-visible fault site is triggered with probability `p`.
#[track_caller]
pub fn fault(p: f64) -> bool {
    fire_at(Location::caller(), p, Kind::Fault)
}

/// Returns a random integer in `0..n`, or `0` when buggify is disabled.
pub fn below(n: u64) -> u64 {
    STATE.with(|s| s.borrow_mut().as_mut().map_or(0, |st| st.rng.below(n.max(1))))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site() -> bool {
        fires(1.0)
    }

    #[test]
    fn sites_fire_only_when_enabled_and_active() {
        assert!(!site());
        enable(1, Config { p_active: 1.0, ..Config::default() });
        assert!(site() && site());
        let r = disable();
        assert_eq!(r.sites.values().map(|s| s.fired).sum::<u64>(), 2);
        enable(1, Config::default());
        assert!(!site());
        assert!(disable().sites.values().all(|s| !s.active));
        assert!(!site());
    }

    #[test]
    fn crash_points_register_distinct_sites() {
        let env = crate::env::Env::sim(1, 0, tokio::time::Instant::now(), 0);
        env.set_crash_hook(std::sync::Arc::new(|_| false));
        enable(1, Config { p_active: 1.0, crashes: true, ..Config::default() });
        drop(env.crash_point());
        drop(env.crash_point());
        assert_eq!(disable().sites.len(), 2);
    }

    #[test]
    fn activation_is_decided_once_per_site() {
        let mut seen = std::collections::BTreeSet::new();
        for seed in 0..50 {
            enable(seed, Config { p_active: 0.5, ..Config::default() });
            let first = site();
            assert!((0..20).all(|_| site() == first));
            seen.insert(first);
            disable();
        }
        assert_eq!(seen.len(), 2);
    }
}
