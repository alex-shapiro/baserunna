//! Readers for Linux cgroup v2 memory and CPU counters.

use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};

/// Path to the node cgroup directory, such as `/sys/fs/cgroup/iso/node`.
#[derive(Clone)]
pub struct NodeCgroup(pub PathBuf);

impl NodeCgroup {
    fn read_file(&self, file_name: &str) -> Option<String> {
        std::fs::read_to_string(self.0.join(file_name)).ok()
    }

    pub fn memory_current(&self) -> Option<u64> {
        self.read_file("memory.current")?.trim().parse().ok()
    }

    /// Reads the integer value for `key` from a flat keyed cgroup file.
    fn read_keyed_u64(&self, file_name: &str, key: &str) -> Option<u64> {
        self.read_file(file_name)?
            .lines()
            .find_map(|line| line.strip_prefix(key)?.strip_prefix(' ')?.trim().parse().ok())
    }

    /// Counts OOM kills of processes inside the node's `main` leaf cgroup.
    ///
    /// Excludes kills in worker cgroups, because the kernel may stop a worker to enforce a tenant
    /// limit.
    pub fn node_oom_kills(&self) -> Option<u64> {
        let leaf = NodeCgroup(self.0.join("main"));
        leaf.read_keyed_u64("memory.events.local", "oom_kill")
    }

    pub fn cpu_usage_usec(&self) -> Option<u64> {
        self.read_keyed_u64("cpu.stat", "usage_usec")
    }

    /// Counts all processes and descendant cgroups under this cgroup.
    pub fn subtree_census(&self) -> SubtreeCensus {
        let mut census = SubtreeCensus::default();
        let mut dirs = vec![self.0.clone()];
        while let Some(dir) = dirs.pop() {
            if let Ok(procs) = std::fs::read_to_string(dir.join("cgroup.procs")) {
                census.processes += procs.lines().filter(|line| !line.trim().is_empty()).count();
            }
            for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                if entry.file_type().is_ok_and(|file_type| file_type.is_dir()) {
                    census.cgroups += 1;
                    dirs.push(entry.path());
                }
            }
        }
        census
    }

    pub fn exists(&self) -> bool {
        Path::new(&self.0).join("memory.current").exists()
    }
}

/// Process and descendant cgroup counts under a cgroup subtree.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct SubtreeCensus {
    pub processes: usize,
    pub cgroups: usize,
}

/// Summary of periodic `memory.current` samples.
#[derive(Clone, Debug, Default, Serialize)]
pub struct MemorySamples {
    pub start_bytes: u64,
    pub peak_bytes: u64,
    pub end_bytes: u64,
    pub growth_bytes: u64,
    pub samples: usize,
    /// Peak memory usage in MiB for each 1-second window from the start.
    pub peak_mib_per_second: Vec<f64>,
}

/// Background thread that samples `memory.current` at a fixed interval.
pub struct MemorySampler {
    stop: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<MemorySamples>,
}

impl MemorySampler {
    /// Starts sampling `memory.current` every 10 ms.
    pub fn start(cgroup: NodeCgroup) -> MemorySampler {
        MemorySampler::start_every(cgroup, Duration::from_millis(10))
    }

    pub fn start_every(cgroup: NodeCgroup, interval: Duration) -> MemorySampler {
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let thread = std::thread::spawn(move || {
            let started = Instant::now();
            let start = cgroup.memory_current().unwrap_or(0);
            let mut summary =
                MemorySamples { start_bytes: start, peak_bytes: start, end_bytes: start, ..Default::default() };
            while !stopped.load(Relaxed) {
                if let Some(current_bytes) = cgroup.memory_current() {
                    summary.samples += 1;
                    summary.peak_bytes = summary.peak_bytes.max(current_bytes);
                    summary.end_bytes = current_bytes;
                    let second = started.elapsed().as_secs() as usize;
                    if summary.peak_mib_per_second.len() <= second {
                        summary.peak_mib_per_second.resize(second + 1, 0.0);
                    }
                    let mib = current_bytes as f64 / (1 << 20) as f64;
                    let slot = &mut summary.peak_mib_per_second[second];
                    *slot = slot.max((mib * 10.0).round() / 10.0);
                }
                std::thread::sleep(interval);
            }
            summary.growth_bytes = summary.peak_bytes.saturating_sub(start);
            summary
        });
        MemorySampler { stop, thread }
    }

    pub fn finish(self) -> MemorySamples {
        self.stop.store(true, Relaxed);
        self.thread.join().expect("memory sampler")
    }
}
