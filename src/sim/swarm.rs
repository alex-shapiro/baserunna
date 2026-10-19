//! Swarm testing (SPEC §13.2): every seed draws its own world, with its own
//! cluster size, faults and their rates, engine and node settings, and its
//! own workload shape. A campaign then covers many configurations, each
//! with some features switched off, instead of a few hand-tuned presets.
//!
//! A knob's value depends only on the seed and the knob's name, so adding a
//! knob leaves every other knob's draw unchanged.

use super::store::StoreFaults;
use super::{ChaosFaults, DiskFaults, NetFaults, SimConfig};
use crate::engine::segment::Compression;
use crate::util::rng::Rng;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Mutex;

/// The knobs of one run: values drawn from the run's seed.
#[derive(Debug)]
pub struct Knobs {
    seed: u64,
    drawn: Mutex<BTreeMap<&'static str, String>>,
}

impl Knobs {
    pub fn new(seed: u64) -> Knobs {
        Knobs { seed, drawn: Mutex::new(BTreeMap::new()) }
    }

    fn rng(&self, name: &str) -> Rng {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in name.bytes() {
            h = (h ^ b as u64).wrapping_mul(0x0000_0100_0000_01B3);
        }
        Rng::new(self.seed ^ h)
    }

    fn note<T: Debug>(&self, name: &'static str, v: T) -> T {
        self.drawn.lock().unwrap_or_else(|e| e.into_inner()).insert(name, format!("{v:?}"));
        v
    }

    /// Uniform in `lo..=hi`.
    pub fn range(&self, name: &'static str, lo: u64, hi: u64) -> u64 {
        let v = if hi <= lo { lo } else { self.rng(name).range(lo, hi + 1) };
        self.note(name, v)
    }

    /// Log-uniform in `lo..=hi`: small values are as likely as large ones.
    pub fn log_range(&self, name: &'static str, lo: u64, hi: u64) -> u64 {
        let (l, h) = ((lo.max(1) as f64).ln(), (hi.max(lo).max(1) as f64).ln());
        let v = (l + self.rng(name).unit() * (h - l)).exp().round() as u64;
        self.note(name, v.clamp(lo, hi.max(lo)))
    }

    pub fn pick<T: Copy + Debug>(&self, name: &'static str, choices: &[T]) -> T {
        let v = choices[self.rng(name).usize(choices.len())];
        self.note(name, v)
    }

    /// True with probability `p`.
    pub fn chance(&self, name: &'static str, p: f64) -> bool {
        let v = self.rng(name).chance(p);
        self.note(name, v)
    }

    /// A fault rate around `typical`: zero (the fault is off) in a third of
    /// runs, otherwise anywhere from a tenth to four times `typical`.
    pub fn rate(&self, name: &'static str, typical: f64) -> f64 {
        let mut r = self.rng(name);
        let v = if r.chance(1.0 / 3.0) { 0.0 } else { typical * (0.1f64.ln() + r.unit() * 40f64.ln()).exp() };
        self.note(name, v)
    }

    /// The weight of one choice among several: zero (the choice is off) in a
    /// quarter of runs, otherwise 1 to 10.
    pub fn weight(&self, name: &'static str) -> u64 {
        let mut r = self.rng(name);
        let v = if r.chance(0.25) { 0 } else { 1 + r.below(10) };
        self.note(name, v)
    }

    /// The values drawn so far, by knob name.
    pub fn drawn(&self) -> Vec<(&'static str, String)> {
        self.drawn.lock().unwrap_or_else(|e| e.into_inner()).iter().map(|(k, v)| (*k, v.clone())).collect()
    }
}

/// Picks among `weights` with probability proportional to each weight, or
/// `None` if they are all zero.
pub fn weighted(r: &mut Rng, weights: &[u64]) -> Option<usize> {
    let total: u64 = weights.iter().sum();
    if total == 0 {
        return None;
    }
    let mut x = r.below(total);
    for (i, w) in weights.iter().enumerate() {
        if x < *w {
            return Some(i);
        }
        x -= w;
    }
    None
}

/// How far apart node clocks may be in a scenario's worlds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Skew {
    None,
    /// Within the lease margin.
    Margin,
    /// Up to ten times the lease margin, with jumps as large.
    Hostile,
    /// Up to an hour, with jumps as large.
    Hour,
}

/// What a scenario's workload tolerates.
#[derive(Clone, Copy, Debug)]
pub struct Profile {
    pub nodes: (usize, usize),
    /// Store and network faults and chaos actions.
    pub faults: bool,
    /// The largest clock skew the workload's checks allow for.
    pub skew: Skew,
}

impl Profile {
    pub const FAULTY: Profile = Profile { nodes: (1, 5), faults: true, skew: Skew::Hostile };
}

/// Draws a world within `p` from the run's knobs.
pub fn world(k: &Knobs, p: Profile) -> SimConfig {
    let mut c = SimConfig::basic();
    c.nodes = k.range("world.nodes", p.nodes.0 as u64, p.nodes.1 as u64) as usize;
    engine_knobs(k, &mut c);
    node_knobs(k, &mut c);
    // Some worlds are calm: no faults at all, so throughput is high.
    if p.faults && !k.chance("world.calm", 0.1) {
        let lease = c.engine.lease_ms;
        let stalls = k.rate("store.p_stall", 0.004);
        c.store = StoreFaults {
            latency_min_ms: k.range("store.latency_min_ms", 0, 5),
            latency_max_ms: k.log_range("store.latency_max_ms", 5, 120),
            p_slow: k.rate("store.p_slow", 0.02),
            p_stall: stalls,
            stall_ms: (lease, 3 * lease),
            p_fail_before: k.rate("store.p_fail_before", 0.02),
            p_fail_after: k.rate("store.p_fail_after", 0.02),
            list_lag_ms: k.pick("store.list_lag_ms", &[0, 0, 500, 5_000]),
            p_late_reply: k.rate("store.p_late_reply", 0.05),
            late_reply_ms: (50, k.log_range("store.late_reply_max_ms", 100, 2_000)),
        };
        c.net = NetFaults {
            latency_min_ms: k.range("net.latency_min_ms", 0, 2),
            latency_max_ms: k.log_range("net.latency_max_ms", 2, 40),
            p_lose_response: k.rate("net.p_lose_response", 0.01),
            p_forward_duplicate: k.rate("net.p_forward_duplicate", 0.02),
            p_forward_delay: k.rate("net.p_forward_delay", 0.03),
            forward_delay_max_ms: k.log_range("net.forward_delay_max_ms", 50, 3_000),
        };
        c.chaos = ChaosFaults {
            enabled: k.chance("chaos.enabled", 0.85),
            pause_ms: k.log_range("chaos.pause_ms", 200, 8_000),
            storm_ms: k.log_range("chaos.storm_ms", 2_000, 120_000),
            calm_ms: k.log_range("chaos.calm_ms", 1_000, 30_000),
            crash: k.weight("chaos.crash"),
            start: k.weight("chaos.start"),
            partition: k.weight("chaos.partition"),
            one_way_partition: k.weight("chaos.one_way_partition"),
            store_isolation: k.weight("chaos.store_isolation"),
            clock_step: if p.skew >= Skew::Hostile { k.weight("chaos.clock_step") } else { 0 },
            heal: 1 + k.weight("chaos.heal"),
            quiet: k.weight("chaos.quiet"),
            pause_node: k.weight("chaos.pause_node"),
            clock_step_max_ms: 0,
        };
    }
    let margin = c.engine.lease_margin_ms;
    let levels: &[Skew] = match p.skew {
        Skew::None => &[Skew::None],
        Skew::Margin => &[Skew::None, Skew::Margin],
        Skew::Hostile => &[Skew::None, Skew::Margin, Skew::Hostile],
        Skew::Hour => &[Skew::None, Skew::Margin, Skew::Hostile, Skew::Hour],
    };
    let (skew, step) = match k.pick("world.skew", levels) {
        Skew::None => (0, 0),
        Skew::Margin => (3 * margin / 4, 0),
        Skew::Hostile => (10 * margin, 10 * margin),
        Skew::Hour => (3_600_000, 3_600_000),
    };
    c.max_skew_ms = skew;
    c.chaos.clock_step_max_ms = step;
    c.buggify = k.pick("buggify.p_active", &[0.0, 0.1, 0.25, 0.5]);
    c.crash_points = c.chaos.enabled && k.chance("buggify.crash_points", 0.5);
    c.reorder = k.pick("sched.reorder", &[0.0, 0.05, 0.2, 0.5]);
    c.disk = DiskFaults {
        bytes: k.pick("disk.bytes", &[0, 4 * 1024, 64 * 1024, 1024 * 1024]),
        p_lose: if p.faults { k.rate("disk.p_lose", 0.02) } else { 0.0 },
        p_drop_write: if p.faults { k.rate("disk.p_drop_write", 0.02) } else { 0.0 },
        p_corrupt: if p.faults { k.rate("disk.p_corrupt", 0.002) } else { 0.0 },
    };
    c
}

/// Engine sizes and timings: small enough that every storage path runs in a
/// short simulation, and different in every run.
fn engine_knobs(k: &Knobs, c: &mut SimConfig) {
    let e = &mut c.engine;
    e.tail_max_bytes = k.log_range("engine.tail_max_bytes", 512, 32 * 1024) as usize;
    e.block_target = k.log_range("engine.block_target", 128, 8 * 1024) as usize;
    e.compression = k.pick("engine.compression", &[Compression::None, Compression::Zstd(1), Compression::Zstd(3)]);
    e.segment_whole_fetch_max = k.log_range("engine.segment_whole_fetch_max", 512, 64 * 1024);
    e.segment_target_bytes = k.log_range("engine.segment_target_bytes", 1024, 64 * 1024) as usize;
    e.bulk_read_bytes = k.log_range("engine.bulk_read_bytes", 256, 16 * 1024);
    e.l0_compaction_trigger = k.range("engine.l0_compaction_trigger", 1, 8) as usize;
    e.l0_compaction_max_inputs = k.range("engine.l0_compaction_max_inputs", 2, 12) as usize;
    e.level_size_ratio = k.range("engine.level_size_ratio", 2, 10);
    e.level_base_bytes = k.log_range("engine.level_base_bytes", 2 * 1024, 64 * 1024);
    e.cache_bytes = k.log_range("engine.cache_bytes", 4 * 1024, 4 * 1024 * 1024) as usize;
    e.range_delete_points_max = k.log_range("engine.range_delete_points_max", 1, 300) as usize;
    e.levels_inline_max = k.range("engine.levels_inline_max", 1, 16) as usize;
    e.stream_seal_min_bytes = k.log_range("engine.stream_seal_min_bytes", 256, 16 * 1024) as usize;
    e.stream_chunk_bytes = e.stream_seal_min_bytes * k.range("engine.stream_chunk_multiple", 1, 8) as usize;
    e.compaction_concurrency = k.range("engine.compaction_concurrency", 1, 3) as usize;
    e.commit_batch_max = k.pick("engine.commit_batch_max", &[1, 2, 4, 16, 256]);
    e.commit_retry_max = k.range("engine.commit_retry_max", 2, 10) as u32;
    e.manifest_put_hedge_ms = k.pick("engine.manifest_put_hedge_ms", &[0, 20, 100, 250, 1_000]);
    e.lease_read_threshold = k.log_range("engine.lease_read_threshold", 1, 64) as u32;
    e.lease_ms = k.pick("engine.lease_ms", &[1_000, 3_000, 5_000]);
    e.read_lease_ms = e.lease_ms * k.range("engine.read_lease_multiple", 1, 3);
    e.lease_margin_ms = e.lease_ms / k.pick("engine.lease_margin_divisor", &[4, 10, 20]);
    e.version_sample_ms = k.log_range("engine.version_sample_ms", 500, 30_000);
}

fn node_knobs(k: &Knobs, c: &mut SimConfig) {
    let n = &mut c.node;
    n.heartbeat_ms = k.log_range("node.heartbeat_ms", 1_000, 30_000);
    n.members_refresh_ms = k.log_range("node.members_refresh_ms", 200, 10_000);
    n.auth_cache_ms = k.log_range("node.auth_cache_ms", 0, 3_000);
    n.sys_staleness_ms = k.log_range("node.sys_staleness_ms", 0, 1_000);
    n.db_idle_evict_ms = k.log_range("node.db_idle_evict_ms", 500, 60_000);
    n.index_backfill_batch_rows = k.log_range("node.index_backfill_batch_rows", 1, 64) as usize;
    n.engine = c.engine.clone();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn knobs_depend_only_on_the_seed_and_their_name() {
        let a = Knobs::new(7);
        let x = a.range("x", 0, 1_000_000);
        let _ = a.range("y", 0, 10);
        let b = Knobs::new(7);
        assert_eq!(b.range("x", 0, 1_000_000), x, "drawing another knob first changes nothing");
        assert_ne!(
            (0..20).map(|s| Knobs::new(s).range("x", 0, 1_000_000)).collect::<std::collections::BTreeSet<_>>().len(),
            1
        );
        assert_eq!(a.drawn(), [("x", x.to_string()), ("y", a.range("y", 0, 10).to_string())]);
    }

    #[test]
    fn worlds_vary_and_stay_valid() {
        let mut nodes = std::collections::BTreeSet::new();
        let mut off = 0;
        for seed in 0..200 {
            let c = world(&Knobs::new(seed), Profile::FAULTY);
            c.engine.validate().unwrap();
            nodes.insert(c.nodes);
            off += (c.store.p_fail_after == 0.0) as usize;
            assert!(c.engine.lease_margin_ms < c.engine.lease_ms);
        }
        assert_eq!(nodes.len(), 5);
        assert!((30..120).contains(&off), "faults are off in about a third of worlds: {off}");
    }

    #[test]
    fn weighted_choices_skip_zero_weights() {
        let mut r = Rng::new(1);
        assert_eq!(weighted(&mut r, &[0, 0]), None);
        assert!((0..100).all(|_| weighted(&mut r, &[0, 3, 0, 1]).is_some_and(|i| i == 1 || i == 3)));
    }
}
