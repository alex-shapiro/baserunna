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

    pub fn memory_current_bytes(&self) -> Option<u64> {
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
    /// Counts only kills in `main`. The kernel can stop workers in child cgroups to enforce
    /// tenant limits.
    pub fn node_oom_kills(&self) -> Option<u64> {
        let leaf = NodeCgroup(self.0.join("main"));
        leaf.read_keyed_u64("memory.events.local", "oom_kill")
    }

    pub fn cpu_usage_usec(&self) -> Option<u64> {
        self.read_keyed_u64("cpu.stat", "usage_usec")
    }

    /// Counts all processes and descendant cgroups under this cgroup.
    pub fn subtree_counts(&self) -> SubtreeCounts {
        let mut counts = SubtreeCounts::default();
        let mut pending_dirs = vec![self.0.clone()];
        while let Some(dir) = pending_dirs.pop() {
            if let Ok(procs_text) = std::fs::read_to_string(dir.join("cgroup.procs")) {
                counts.processes += procs_text.lines().filter(|line| !line.trim().is_empty()).count();
            }
            for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                if entry.file_type().is_ok_and(|file_type| file_type.is_dir()) {
                    counts.cgroups += 1;
                    pending_dirs.push(entry.path());
                }
            }
        }
        counts
    }

    pub fn exists(&self) -> bool {
        Path::new(&self.0).join("memory.current").exists()
    }
}

/// Process and descendant cgroup counts under a cgroup subtree.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct SubtreeCounts {
    pub processes: usize,
    pub cgroups: usize,
}

/// Summary of periodic `memory.current` samples.
#[derive(Clone, Debug, Default, Serialize)]
pub struct MemorySummary {
    pub start_bytes: u64,
    pub peak_bytes: u64,
    pub end_bytes: u64,
    pub growth_bytes: u64,
    pub sample_count: usize,
    /// Peak memory use in MiB for each 1-second window from the start.
    pub peak_mib_per_second: Vec<f64>,
}

/// Background thread that samples `memory.current` at a fixed interval.
pub struct MemorySampler {
    stop_flag: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<MemorySummary>,
}

impl MemorySampler {
    /// Starts sampling `memory.current` every 10 ms.
    pub fn start(cgroup: NodeCgroup) -> MemorySampler {
        MemorySampler::start_every(cgroup, Duration::from_millis(10))
    }

    pub fn start_every(cgroup: NodeCgroup, interval: Duration) -> MemorySampler {
        let stop_flag = Arc::new(AtomicBool::new(false));
        let thread_stop_flag = stop_flag.clone();
        let thread = std::thread::spawn(move || {
            let started_at = Instant::now();
            let start_bytes = cgroup.memory_current_bytes().unwrap_or(0);
            let mut summary = MemorySummary {
                start_bytes,
                peak_bytes: start_bytes,
                end_bytes: start_bytes,
                ..Default::default()
            };
            while !thread_stop_flag.load(Relaxed) {
                if let Some(current_bytes) = cgroup.memory_current_bytes() {
                    summary.sample_count += 1;
                    summary.peak_bytes = summary.peak_bytes.max(current_bytes);
                    summary.end_bytes = current_bytes;
                    let second = started_at.elapsed().as_secs() as usize;
                    if summary.peak_mib_per_second.len() <= second {
                        summary.peak_mib_per_second.resize(second + 1, 0.0);
                    }
                    let mib = current_bytes as f64 / (1 << 20) as f64;
                    let second_peak_mib = &mut summary.peak_mib_per_second[second];
                    *second_peak_mib = second_peak_mib.max((mib * 10.0).round() / 10.0);
                }
                std::thread::sleep(interval);
            }
            summary.growth_bytes = summary.peak_bytes.saturating_sub(start_bytes);
            summary
        });
        MemorySampler { stop_flag, thread }
    }

    pub fn finish(self) -> MemorySummary {
        self.stop_flag.store(true, Relaxed);
        self.thread.join().expect("memory sampler")
    }
}
