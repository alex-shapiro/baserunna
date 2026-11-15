//! Worker processes that run an owner's heavy work inside the owner's cgroup.
//!
//! The node starts each worker as `baserunna worker`. The worker reads one job
//! frame from stdin, runs it against the store, and writes one result frame
//! to stdout. The kernel enforces the owner's `memory.max` and `cpu.weight`, so
//! an owner that exceeds its memory loses only its own worker.
//!
//! The simulator never sets `BASERUNNA_CGROUP_ROOT`, so it runs the same jobs
//! in process without kernel limits.

use crate::engine::events::PartRef;
use crate::engine::{EngineConfig, Shared};
use crate::error::{Error, Result};
use crate::events::schema::EventSchema;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

/// Default memory limit of one owner's cgroup.
pub const OWNER_MEMORY_BYTES: u64 = 512 * 1024 * 1024;

/// Heavy tenant work that a worker process runs.
#[derive(Serialize, Deserialize)]
enum Job {
    /// Merges event parts into one new part and returns its reference.
    Merge { db: String, schema: EventSchema, inputs: Vec<(Vec<u8>, Vec<u8>)> },
}

/// The result of a [`Job`].
#[derive(Serialize, Deserialize)]
enum JobResult {
    Merged(Option<(Vec<u8>, Vec<u8>)>),
    Failed { too_large: bool, message: String },
}

/// Settings for the worker pool.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// The delegated cgroup v2 directory that holds one cgroup per owner.
    pub cgroup_root: PathBuf,
    /// The executable to start as a worker.
    pub exe: PathBuf,
    pub owner_memory_bytes: u64,
    pub owner_cpu_weight: u32,
}

/// Starts workers in per-owner cgroups and runs jobs in them.
pub struct WorkerPool {
    cfg: PoolConfig,
    db_owners: Mutex<HashMap<String, String>>,
    next_worker_id: std::sync::atomic::AtomicU64,
}

impl WorkerPool {
    /// Enables the memory and CPU controllers below `cfg.cgroup_root`.
    pub fn new(cfg: PoolConfig) -> std::io::Result<WorkerPool> {
        std::fs::write(cfg.cgroup_root.join("cgroup.subtree_control"), "+memory +cpu")?;
        Ok(WorkerPool { cfg, db_owners: Mutex::new(HashMap::new()), next_worker_id: 0.into() })
    }

    /// Records that database `db` belongs to owner `owner`.
    pub fn set_db_owner(&self, db: &str, owner: &str) {
        let mut g = self.db_owners.lock();
        if g.get(db).map(String::as_str) != Some(owner) {
            g.insert(db.to_string(), owner.to_string());
        }
    }

    fn owner_of(&self, db: &str) -> String {
        self.db_owners.lock().get(db).cloned().unwrap_or_else(|| db.to_string())
    }

    fn owner_cgroup(&self, owner: &str) -> std::io::Result<PathBuf> {
        let name: String =
            owner.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' }).collect();
        let dir = self.cfg.cgroup_root.join(format!("owner-{name}"));
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
            std::fs::write(dir.join("memory.max"), self.cfg.owner_memory_bytes.to_string())?;
            let _ = std::fs::write(dir.join("memory.swap.max"), "0");
            std::fs::write(dir.join("cpu.weight"), self.cfg.owner_cpu_weight.to_string())?;
        }
        Ok(dir)
    }

    /// Runs `merge_parts` for `db` in a worker inside the owner's cgroup.
    pub async fn merge(
        &self,
        db: &str,
        schema: &EventSchema,
        inputs: &[PartRef],
        deadline: Duration,
    ) -> Result<Option<PartRef>> {
        let inputs = inputs.iter().map(|r| (r.key().to_vec(), r.encode().to_vec())).collect();
        let job = Job::Merge { db: db.to_string(), schema: schema.clone(), inputs };
        match self.run(db, job, deadline).await? {
            JobResult::Merged(None) => Ok(None),
            JobResult::Merged(Some((k, v))) => {
                PartRef::decode(&k, &v).map(Some).map_err(|e| Error::internal(format!("worker part ref: {e}")))
            }
            JobResult::Failed { too_large: true, message } => Err(Error::TooLarge(message)),
            JobResult::Failed { message, .. } => Err(Error::internal(message)),
        }
    }

    async fn run(&self, db: &str, job: Job, deadline: Duration) -> Result<JobResult> {
        let owner = self.owner_of(db);
        let io = |e: std::io::Error| Error::internal(format!("worker: {e}"));
        let owner_dir = self.owner_cgroup(&owner).map_err(io)?;
        // Each worker has its own leaf below the owner's cgroup, so a deadline
        // kills exactly that worker while `memory.max` bounds all of the owner's workers.
        let worker_id = self.next_worker_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let leaf = owner_dir.join(format!("worker-{worker_id}"));
        std::fs::create_dir(&leaf).map_err(io)?;
        let exe = self.cfg.exe.clone();
        let limit_mib = self.cfg.owner_memory_bytes >> 20;
        let job = serde_json::to_vec(&job).map_err(|e| Error::internal(e.to_string()))?;
        let leaf2 = leaf.clone();
        let task = tokio::task::spawn_blocking(move || -> Result<JobResult> {
            let leaf = leaf2;
            let ooms_before = oom_kills(&owner_dir);
            let mut child = Command::new(exe)
                .arg("worker")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .map_err(io)?;
            // The worker reads nothing and allocates little until it receives the job,
            // so it joins the owner's cgroup before any tenant data reaches it.
            if let Err(e) = std::fs::write(leaf.join("cgroup.procs"), child.id().to_string()) {
                let _ = child.kill();
                let _ = child.wait();
                return Err(io(e));
            }
            let mut stdin = child.stdin.take().expect("piped stdin");
            let sent = write_frame(&mut stdin, &job);
            drop(stdin);
            let mut out = Vec::new();
            let read = sent.and_then(|_| child.stdout.take().expect("piped stdout").read_to_end(&mut out));
            let status = child.wait().map_err(io)?;
            if read.is_ok() && status.success() {
                let frame = read_frame(&mut out.as_slice()).map_err(io)?;
                return serde_json::from_slice(&frame).map_err(|e| Error::internal(format!("worker result: {e}")));
            }
            if oom_kills(&owner_dir) > ooms_before {
                return Err(Error::TooLarge(format!("the work exceeded the owner's memory limit of {limit_mib} MiB")));
            }
            Err(Error::internal(format!("worker exited with {status}")))
        });
        let result = match tokio::time::timeout(deadline, task).await {
            Ok(joined) => joined.map_err(|e| Error::internal(format!("worker task: {e}")))?,
            Err(_) => {
                let _ = std::fs::write(leaf.join("cgroup.kill"), "1");
                Err(Error::TooLarge(format!("the work ran longer than {} ms", deadline.as_millis())))
            }
        };
        let _ = std::fs::remove_dir(&leaf);
        result
    }
}

fn oom_kills(cgroup: &std::path::Path) -> u64 {
    let events = std::fs::read_to_string(cgroup.join("memory.events")).unwrap_or_default();
    events.lines().find_map(|l| l.strip_prefix("oom_kill ")).and_then(|n| n.trim().parse().ok()).unwrap_or(0)
}

fn write_frame(w: &mut impl Write, b: &[u8]) -> std::io::Result<()> {
    w.write_all(&(b.len() as u64).to_le_bytes())?;
    w.write_all(b)?;
    w.flush()
}

fn read_frame(r: &mut impl Read) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 8];
    r.read_exact(&mut len)?;
    let mut b = vec![0u8; u64::from_le_bytes(len) as usize];
    r.read_exact(&mut b)?;
    Ok(b)
}

/// Runs one job from stdin and writes its result to stdout (the `baserunna worker` command).
pub async fn serve(store: crate::store::DynStore, cfg: EngineConfig) -> std::io::Result<()> {
    let job = read_frame(&mut std::io::stdin().lock())?;
    let job: Job = serde_json::from_slice(&job).map_err(std::io::Error::other)?;
    let engine = crate::engine::Engine::new(store, Arc::new(crate::env::Env::real()), cfg, "worker".into());
    let result = run_job(&engine.shared, job).await;
    let result = serde_json::to_vec(&result).map_err(std::io::Error::other)?;
    write_frame(&mut std::io::stdout().lock(), &result)
}

async fn run_job(shared: &Arc<Shared>, job: Job) -> JobResult {
    let failed = |e: Error| JobResult::Failed { too_large: matches!(e, Error::TooLarge(_)), message: e.to_string() };
    match job {
        Job::Merge { db, schema, inputs } => {
            let mut refs = Vec::with_capacity(inputs.len());
            for (k, v) in &inputs {
                match PartRef::decode(k, v) {
                    Ok(r) => refs.push(r),
                    Err(e) => return failed(Error::internal(format!("part ref: {e}"))),
                }
            }
            let stats = crate::events::part::ReadStats::default();
            match crate::events::maint::merge_parts(shared, &db, &schema, &refs, &stats).await {
                Ok(out) => JobResult::Merged(out.map(|r| (r.key().to_vec(), r.encode().to_vec()))),
                Err(e) => failed(e),
            }
        }
    }
}
