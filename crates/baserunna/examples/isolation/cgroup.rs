//! Readers for the node cgroup's memory and CPU counters.

use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};

/// Node cgroup directory, such as `/sys/fs/cgroup/iso/node`.
#[derive(Clone)]
pub struct NodeCgroup(pub PathBuf);

impl NodeCgroup {
    fn read(&self, file: &str) -> Option<String> {
        std::fs::read_to_string(self.0.join(file)).ok()
    }

    pub fn memory_current(&self) -> Option<u64> {
        self.read("memory.current")?.trim().parse().ok()
    }

    /// Value of `key` in a flat keyed file such as `memory.events` or `cpu.stat`.
    fn keyed(&self, file: &str, key: &str) -> Option<u64> {
        self.read(file)?.lines().find_map(|l| l.strip_prefix(key)?.strip_prefix(' ')?.trim().parse().ok())
    }

    /// OOM kills of processes in the node's own leaf cgroup `main`.
    ///
    /// Kills in sibling cgroups that the node creates for its workers do not count: a design may
    /// refuse a tenant's work by letting the kernel kill that tenant's worker.
    pub fn node_oom_kills(&self) -> Option<u64> {
        let leaf = NodeCgroup(self.0.join("main"));
        leaf.keyed("memory.events.local", "oom_kill")
    }

    pub fn cpu_usage_usec(&self) -> Option<u64> {
        self.keyed("cpu.stat", "usage_usec")
    }

    pub fn exists(&self) -> bool {
        Path::new(&self.0).join("memory.current").exists()
    }
}

/// Summary of `memory.current` samples taken every 10 ms.
#[derive(Clone, Debug, Default, Serialize)]
pub struct MemorySamples {
    pub start_bytes: u64,
    pub peak_bytes: u64,
    pub end_bytes: u64,
    pub growth_bytes: u64,
    pub samples: usize,
    /// Peak of each 1 s window since the start, in MiB.
    pub peak_mib_per_second: Vec<f64>,
}

/// Background thread that samples `memory.current` every 10 ms.
pub struct MemorySampler {
    stop: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<MemorySamples>,
}

impl MemorySampler {
    pub fn start(cg: NodeCgroup) -> MemorySampler {
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let thread = std::thread::spawn(move || {
            let started = Instant::now();
            let start = cg.memory_current().unwrap_or(0);
            let mut out = MemorySamples { start_bytes: start, peak_bytes: start, end_bytes: start, ..Default::default() };
            while !stopped.load(Relaxed) {
                if let Some(v) = cg.memory_current() {
                    out.samples += 1;
                    out.peak_bytes = out.peak_bytes.max(v);
                    out.end_bytes = v;
                    let second = started.elapsed().as_secs() as usize;
                    if out.peak_mib_per_second.len() <= second {
                        out.peak_mib_per_second.resize(second + 1, 0.0);
                    }
                    let mib = v as f64 / (1 << 20) as f64;
                    let slot = &mut out.peak_mib_per_second[second];
                    *slot = slot.max((mib * 10.0).round() / 10.0);
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            out.growth_bytes = out.peak_bytes.saturating_sub(start);
            out
        });
        MemorySampler { stop, thread }
    }

    pub fn finish(self) -> MemorySamples {
        self.stop.store(true, Relaxed);
        self.thread.join().expect("memory sampler")
    }
}
