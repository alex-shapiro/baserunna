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
use bytes::Bytes;
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
    /// Runs an event query at a snapshot whose encoded manifest is the job's payload, and
    /// returns the JSON response body as the result's payload.
    Query {
        db: String,
        etag: String,
        read_time: u64,
        schema: EventSchema,
        query: serde_json::Value,
        limits: (u64, usize, usize, u64),
    },
}

/// The result of a [`Job`].
#[derive(Serialize, Deserialize)]
enum JobResult {
    Merged(Option<(Vec<u8>, Vec<u8>)>),
    /// The query's JSON response body is the result's payload.
    Queried,
    /// The job's error as the JSON body of an HTTP error response.
    Failed(serde_json::Value),
}

/// Reads the `owner_id` of a database ID, or `None` if the database does not exist.
pub type OwnerResolver = Box<
    dyn Fn(String) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<String>>> + Send>> + Send + Sync,
>;

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
    owner_resolver: std::sync::OnceLock<OwnerResolver>,
    next_worker_id: std::sync::atomic::AtomicU64,
}

impl WorkerPool {
    /// Enables the memory and CPU controllers below `cfg.cgroup_root`.
    pub fn new(cfg: PoolConfig) -> std::io::Result<WorkerPool> {
        std::fs::write(cfg.cgroup_root.join("cgroup.subtree_control"), "+memory +cpu")?;
        Ok(WorkerPool {
            cfg,
            db_owners: Mutex::new(HashMap::new()),
            owner_resolver: Default::default(),
            next_worker_id: 0.into(),
        })
    }

    /// Records that database `db` belongs to owner `owner`.
    pub fn set_db_owner(&self, db: &str, owner: &str) {
        let mut g = self.db_owners.lock();
        if g.get(db).map(String::as_str) != Some(owner) {
            g.insert(db.to_string(), owner.to_string());
        }
    }

    /// Sets the function that reads a database's `owner_id` from the system database.
    pub fn set_owner_resolver(&self, f: OwnerResolver) {
        let _ = self.owner_resolver.set(f);
    }

    /// Returns the `owner_id` of database `db`, so that all databases of an owner share one cgroup.
    async fn owner_of(&self, db: &str) -> Result<String> {
        if let Some(o) = self.db_owners.lock().get(db) {
            return Ok(o.clone());
        }
        let resolve =
            self.owner_resolver.get().ok_or_else(|| Error::internal("the worker pool has no owner resolver"))?;
        let owner = resolve(db.to_string()).await?.ok_or_else(|| Error::not_found("database not found"))?;
        self.set_db_owner(db, &owner);
        Ok(owner)
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
        let owner = self.owner_of(db).await?;
        match self.run(&owner, job, Bytes::new(), deadline).await?.0 {
            JobResult::Merged(None) => Ok(None),
            JobResult::Merged(Some((k, v))) => {
                PartRef::decode(&k, &v).map(Some).map_err(|e| Error::internal(format!("worker part ref: {e}")))
            }
            JobResult::Failed(e) => Err(crate::api::error_from_json(500, &e)),
            JobResult::Queried => Err(Error::internal("the worker returned a query result for a merge")),
        }
    }

    /// Runs event query `query` on `txn`'s snapshot in a worker inside `owner`'s cgroup and
    /// returns the JSON response body.
    pub async fn query(
        &self,
        owner: &str,
        db: &str,
        txn: &mut crate::engine::Txn,
        schema: &EventSchema,
        query: serde_json::Value,
        limits: &crate::events::exec::Limits,
    ) -> Result<Bytes> {
        let version = txn.untracked_snapshot().clone();
        let job = Job::Query {
            db: db.to_string(),
            etag: version.etag.clone(),
            read_time: txn.read_time,
            schema: schema.clone(),
            query,
            limits: (limits.max_bytes_read, limits.max_memory_bytes, limits.lookup_max_bytes, limits.max_ms),
        };
        let manifest = version.m.encode();
        drop(version);
        // The worker enforces the query deadline; the extra second lets its own error arrive first.
        let deadline = Duration::from_millis(limits.max_ms) + Duration::from_secs(1);
        match self.run(owner, job, manifest, deadline).await? {
            (JobResult::Queried, body) => Ok(body),
            (JobResult::Failed(e), _) => Err(crate::api::error_from_json(500, &e)),
            _ => Err(Error::internal("the worker returned a merge result for a query")),
        }
    }

    /// Runs `job` with `payload` in a new worker in `owner`'s cgroup and returns the result and its payload.
    async fn run(&self, owner: &str, job: Job, payload: Bytes, deadline: Duration) -> Result<(JobResult, Bytes)> {
        let io = |e: std::io::Error| Error::internal(format!("worker: {e}"));
        let owner_dir = self.owner_cgroup(owner).map_err(io)?;
        // Each worker has its own leaf below the owner's cgroup, so a deadline
        // kills exactly that worker while `memory.max` bounds all of the owner's workers.
        let worker_id = self.next_worker_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let leaf = owner_dir.join(format!("worker-{worker_id}"));
        std::fs::create_dir(&leaf).map_err(io)?;
        let exe = self.cfg.exe.clone();
        let limit_mib = self.cfg.owner_memory_bytes >> 20;
        let job = serde_json::to_vec(&job).map_err(|e| Error::internal(e.to_string()))?;
        let leaf2 = leaf.clone();
        let task = tokio::task::spawn_blocking(move || -> Result<(JobResult, Bytes)> {
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
            let sent = write_frame(&mut stdin, &job).and_then(|_| write_frame(&mut stdin, &payload));
            drop(stdin);
            let mut out = Vec::new();
            let read = sent.and_then(|_| child.stdout.take().expect("piped stdout").read_to_end(&mut out));
            let status = child.wait().map_err(io)?;
            if read.is_ok() && status.success() {
                let mut r = out.as_slice();
                let header = read_frame(&mut r).map_err(io)?;
                let payload = read_frame(&mut r).map_err(io)?;
                let header =
                    serde_json::from_slice(&header).map_err(|e| Error::internal(format!("worker result: {e}")))?;
                return Ok((header, payload.into()));
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
    let (job, payload) = {
        let mut stdin = std::io::stdin().lock();
        (read_frame(&mut stdin)?, read_frame(&mut stdin)?)
    };
    let job: Job = serde_json::from_slice(&job).map_err(std::io::Error::other)?;
    let engine = crate::engine::Engine::new(store, Arc::new(crate::env::Env::real()), cfg, "worker".into());
    let (result, out) = run_job(&engine.shared, job, payload.into()).await;
    let result = serde_json::to_vec(&result).map_err(std::io::Error::other)?;
    let mut stdout = std::io::stdout().lock();
    write_frame(&mut stdout, &result)?;
    write_frame(&mut stdout, &out)
}

async fn run_job(shared: &Arc<Shared>, job: Job, payload: Bytes) -> (JobResult, Bytes) {
    let failed =
        |e: Error| (JobResult::Failed(serde_json::json!({ "error": crate::api::error_json(&e) })), Bytes::new());
    match job {
        Job::Query { db, etag, read_time, schema, query, limits } => {
            match run_query(shared, &db, etag, read_time, &schema, query, limits, payload).await {
                Ok(body) => (JobResult::Queried, body),
                Err(e) => failed(e),
            }
        }
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
                Ok(out) => (JobResult::Merged(out.map(|r| (r.key().to_vec(), r.encode().to_vec()))), Bytes::new()),
                Err(e) => failed(e),
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_query(
    shared: &Arc<Shared>,
    db: &str,
    etag: String,
    read_time: u64,
    schema: &EventSchema,
    query: serde_json::Value,
    (max_bytes_read, max_memory_bytes, lookup_max_bytes, max_ms): (u64, usize, usize, u64),
    manifest: Bytes,
) -> Result<Bytes> {
    use crate::engine::{Isolation, Snapshot, Txn, Version, manifest::Manifest};
    let m = Manifest::decode(&manifest).map_err(|e| Error::internal(format!("snapshot manifest: {e}")))?;
    drop(manifest);
    let version = m.version;
    let snapshot = Snapshot { version: Arc::new(Version::new(m, etag, shared.clone())), time_ms: read_time };
    let cfg = &shared.cfg;
    let mut txn =
        Txn::new(snapshot, Isolation::Serializable, 0, cfg.txn_max_write_bytes).with_read_limit(cfg.txn_max_read_bytes);
    let q = crate::events::query::Query::parse(query)?;
    let limits = crate::events::exec::Limits { max_bytes_read, max_memory_bytes, lookup_max_bytes, max_ms };
    let mut grant = crate::queries::QueryGrant::unlimited();
    let out = crate::events::exec::run(shared, db, &mut txn, schema, &q, &limits, &mut grant).await?;
    let body = crate::api::events::output_json(&out, version);
    serde_json::to_vec(&body).map(Bytes::from).map_err(|e| Error::internal(e.to_string()))
}
