//! Buggify points (SPEC §13.2): places in service code where a simulation
//! takes rare but legitimate paths, such as a smaller batch, a cache miss,
//! an early flush, a pause before a write, or a crash between two steps,
//! that the workloads alone would seldom reach.
//!
//! A site is a source location. The first time a run reaches it, the site
//! is switched on or off for the whole run; an active site then fires on
//! each call with the probability it asks for. Outside a simulation that
//! enabled buggify on this thread, no site ever fires.

use crate::util::rng::Rng;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::panic::Location;

/// What buggify did in one run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Report {
    /// Sites reached, as `file:line`, and how often each fired.
    pub sites: BTreeMap<String, Site>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Site {
    pub active: bool,
    pub fired: u64,
}

struct State {
    rng: Rng,
    /// Draws whether tasks yield, apart from `rng` so that varying the order
    /// of tasks leaves which sites are active unchanged.
    order: Rng,
    p_reorder: f64,
    p_active: f64,
    faults: bool,
    crashes: bool,
    /// Whether faults and crashes may fire now (see [`quiesce`]).
    quiet: bool,
    sites: BTreeMap<(&'static str, u32, u32), Site>,
}

thread_local! {
    static STATE: RefCell<Option<State>> = const { RefCell::new(None) };
}

/// How buggify behaves in one run.
#[derive(Clone, Copy, Debug, Default)]
pub struct Config {
    /// The share of sites active.
    pub p_active: f64,
    /// Whether [`fault`] sites may fire.
    pub faults: bool,
    /// Whether crash points may crash nodes.
    pub crashes: bool,
    /// How often a simulated task yields before a poll.
    pub p_reorder: f64,
}

/// Enables buggify on this thread for one run.
pub fn enable(seed: u64, c: Config) {
    let (rng, order) = (Rng::new(seed), Rng::new(seed ^ 0x5eed));
    let Config { p_active, faults, crashes, p_reorder } = c;
    STATE.with(|s| {
        let sites = BTreeMap::new();
        *s.borrow_mut() = Some(State { rng, order, p_reorder, p_active, faults, crashes, quiet: false, sites })
    });
}

/// Stops fault sites and crash points from firing while `quiet`, as while a
/// simulation checks a cluster that should have recovered.
pub fn quiesce(quiet: bool) {
    STATE.with(|s| {
        if let Some(st) = s.borrow_mut().as_mut() {
            st.quiet = quiet;
        }
    });
}

/// Whether a simulated task yields before its next poll.
pub(crate) fn reorder() -> bool {
    STATE.with(|s| s.borrow_mut().as_mut().is_some_and(|st| st.p_reorder > 0.0 && st.order.chance(st.p_reorder)))
}

/// Disables buggify on this thread and reports what it did.
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

/// Whether this site fires now, with probability `p` when active.
#[track_caller]
pub fn fires(p: f64) -> bool {
    fire_at(Location::caller(), p, Kind::Benign)
}

/// Whether this site fires now: a quarter of the time when active.
#[track_caller]
pub fn buggify() -> bool {
    fire_at(Location::caller(), 0.25, Kind::Benign)
}

/// Whether a crash point fires now (see [`crate::env::Env::crash_point`]).
#[track_caller]
pub(crate) fn crash_fires(p: f64) -> bool {
    fire_at(Location::caller(), p, Kind::Crash)
}

/// Whether a site that injects a fault clients see, such as a lost
/// response or a lost transaction, fires now. Such sites fire only in runs
/// that inject faults.
#[track_caller]
pub fn fault(p: f64) -> bool {
    fire_at(Location::caller(), p, Kind::Fault)
}

/// A value below `n` from buggify's generator, or 0 outside simulation.
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
        assert!(!site(), "disabled again");
    }

    #[test]
    fn activation_is_decided_once_per_site() {
        let mut seen = std::collections::BTreeSet::new();
        for seed in 0..50 {
            enable(seed, Config { p_active: 0.5, ..Config::default() });
            let first = site();
            assert!((0..20).all(|_| site() == first), "an active site always fires at p = 1");
            seen.insert(first);
            disable();
        }
        assert_eq!(seen.len(), 2, "some runs activate the site and some do not");
    }
}
