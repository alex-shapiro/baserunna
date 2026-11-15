//! Worker processes that run an owner's heavy work inside the owner's cgroup.
//!
//! The node starts each worker as `baserunna worker`. The worker reads one job
//! frame from stdin, runs it against the store, and writes one result frame
//! to stdout. The kernel enforces the owner's `memory.max` and `cpu.weight`, so
//! an owner that exceeds its memory loses only its own worker.
//!
//! The simulator never sets `BASERUNNA_CGROUP_ROOT`, so it runs the same jobs
//! in process without kernel limits.

use crate::engine::events::{Bind, BindSlot, PartRef};
use crate::engine::{BlobRef, EngineConfig, Shared};
use crate::error::{Error, Result};
use crate::events::schema::EventSchema;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

/// Default memory limit of one owner's cgroup.
pub const OWNER_MEMORY_BYTES: u64 = 512 * 1024 * 1024;

/// Longest time a worker may take to seal unsealed rows.
const SEAL_DEADLINE: Duration = Duration::from_secs(600);

/// Longest time a worker may take to read a whole blob value.
const BLOB_READ_DEADLINE: Duration = Duration::from_secs(120);

/// Heavy tenant work that a worker process runs.
#[derive(Serialize, Deserialize)]
enum Job {
    /// Merges event parts into one new part and returns its reference.
    Merge {
        db: String,
        schema: EventSchema,
        inputs: Vec<(Vec<u8>, Vec<u8>)>,
    },
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
    /// Parses and validates the bulk insert body in the payload, writes its rows as staged
    /// parts, and returns the encoded [`Bind`] as the result's payload.
    /// Reads a whole blob value and compares it with the payload (`equals`) or parses it as a
    /// decimal integer.
    BlobValue {
        db: String,
        blob: Vec<u8>,
        equals: bool,
    },
    /// Seals unsealed rows at a snapshot whose encoded manifest is the job's payload.
    Seal {
        db: String,
        etag: String,
        read_time: u64,
        schema: EventSchema,
        state: Vec<u8>,
    },
    BulkInsert {
        db: String,
        schema: EventSchema,
        content_type: String,
        now_ms: i64,
        row_bytes_max: usize,
    },
}

/// The result of a [`Job`].
#[derive(Serialize, Deserialize)]
enum JobResult {
    Merged(Option<(Vec<u8>, Vec<u8>)>),
    BlobEquals(bool),
    BlobDecimal(Option<i64>),
    /// The last sealed row as `(version, ordinal)`, the sealed row and byte counts, and the written parts.
    Sealed(Option<((u64, u32), u64, u64, Vec<(Vec<u8>, Vec<u8>)>)>),
    /// The query's JSON response body is the result's payload.
    Queried,
    /// The body's idempotency key and, on success, the encoded [`Bind`] as the payload.
    Staged {
        body_key: Option<String>,
        error: Option<serde_json::Value>,
    },
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
            _ => Err(Error::internal("the worker returned a result of another job")),
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
            _ => Err(Error::internal("the worker returned a result of another job")),
        }
    }

    /// Returns true if blob `blob` of `db` equals `want`, read in a worker inside the owner's cgroup.
    pub async fn blob_equals(&self, db: &str, blob: &BlobRef, want: Bytes) -> Result<bool> {
        match self.blob_value(db, blob, true, want).await? {
            JobResult::BlobEquals(eq) => Ok(eq),
            _ => Err(Error::internal("the worker returned a result of another job")),
        }
    }

    /// Returns blob `blob` of `db` parsed as a decimal integer, read in a worker inside the owner's cgroup.
    pub async fn blob_decimal(&self, db: &str, blob: &BlobRef) -> Result<Option<i64>> {
        match self.blob_value(db, blob, false, Bytes::new()).await? {
            JobResult::BlobDecimal(n) => Ok(n),
            _ => Err(Error::internal("the worker returned a result of another job")),
        }
    }

    async fn blob_value(&self, db: &str, blob: &BlobRef, equals: bool, want: Bytes) -> Result<JobResult> {
        let owner = self.owner_of(db).await?;
        let mut w = crate::util::codec::Writer::new();
        blob.encode(&mut w);
        let job = Job::BlobValue { db: db.to_string(), blob: w.into_inner(), equals };
        match self.run(&owner, job, want, BLOB_READ_DEADLINE).await?.0 {
            JobResult::Failed(e) => Err(crate::api::error_from_json(500, &e)),
            r => Ok(r),
        }
    }

    /// Runs `seal_parts` for table `schema` at snapshot `snap` in a worker inside the owner's cgroup.
    pub(crate) async fn seal(
        &self,
        db: &str,
        schema: &EventSchema,
        st: &crate::engine::events::TableState,
        snap: &crate::engine::Version,
        read_time: u64,
    ) -> Result<Option<crate::events::maint::Sealed>> {
        let owner = self.owner_of(db).await?;
        let job = Job::Seal {
            db: db.to_string(),
            etag: snap.etag.clone(),
            read_time,
            schema: schema.clone(),
            state: st.encode().to_vec(),
        };
        match self.run(&owner, job, snap.m.encode(), SEAL_DEADLINE).await?.0 {
            JobResult::Sealed(None) => Ok(None),
            JobResult::Sealed(Some(((version, ordinal), rows, bytes, written))) => {
                let written = written
                    .iter()
                    .map(|(k, v)| PartRef::decode(k, v).map_err(|e| Error::internal(format!("worker part ref: {e}"))))
                    .collect::<Result<_>>()?;
                let last = crate::engine::events::Pos { version, ordinal };
                Ok(Some(crate::events::maint::Sealed { last, rows, bytes, written }))
            }
            JobResult::Failed(e) => Err(crate::api::error_from_json(500, &e)),
            _ => Err(Error::internal("the worker returned a result of another job")),
        }
    }

    /// Parses, validates, and stages bulk insert `body` in a worker inside `owner`'s cgroup.
    /// Returns the body's idempotency key and the staged rows, or the error that rejects them.
    #[allow(clippy::too_many_arguments)]
    pub async fn bulk_insert(
        &self,
        owner: &str,
        db: &str,
        schema: &EventSchema,
        content_type: &str,
        body: Bytes,
        now_ms: i64,
        row_bytes_max: usize,
        deadline: Duration,
    ) -> Result<(Option<String>, Result<Bind>)> {
        let job = Job::BulkInsert {
            db: db.to_string(),
            schema: schema.clone(),
            content_type: content_type.to_string(),
            now_ms,
            row_bytes_max,
        };
        match self.run(owner, job, body, deadline).await? {
            (JobResult::Staged { body_key, error: Some(e) }, _) => {
                Ok((body_key, Err(crate::api::error_from_json(500, &e))))
            }
            (JobResult::Staged { body_key, error: None }, bind) => {
                let bind = Bind::decode(&bind).map_err(|e| Error::internal(format!("worker bind: {e}")))?;
                Ok((body_key, Ok(bind)))
            }
            (JobResult::Failed(e), _) => Err(crate::api::error_from_json(500, &e)),
            _ => Err(Error::internal("the worker returned a result of another job")),
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
        Job::BulkInsert { db, schema, content_type, now_ms, row_bytes_max } => {
            let body_key = match crate::api::events::each_event(&content_type, &payload, &mut |_| Ok(())) {
                Ok(k) => k,
                Err(e) => return failed(e),
            };
            match stage_insert(shared, &db, &schema, &content_type, &payload, now_ms, row_bytes_max).await {
                Ok(bind) => (JobResult::Staged { body_key, error: None }, bind.encode()),
                Err(e) => (
                    JobResult::Staged { body_key, error: Some(json!({ "error": crate::api::error_json(&e) })) },
                    Bytes::new(),
                ),
            }
        }
        Job::BlobValue { db, blob, equals } => {
            let read = async {
                let blob = BlobRef::decode(&mut crate::util::codec::Reader::new(&blob))
                    .map_err(|e| Error::internal(format!("blob ref: {e}")))?;
                crate::engine::blob::read_range(shared, &db, &blob, 0..blob.size).await
            };
            match read.await {
                Ok(got) if equals => (JobResult::BlobEquals(got == payload), Bytes::new()),
                Ok(got) => (JobResult::BlobDecimal(crate::ops::decimal_of(&got)), Bytes::new()),
                Err(e) => failed(e),
            }
        }
        Job::Seal { db, etag, read_time, schema, state } => {
            let sealed = async {
                let st = crate::engine::events::TableState::decode(&state)
                    .map_err(|e| Error::internal(format!("table state: {e}")))?;
                let version = snapshot_version(shared, etag, &payload)?;
                crate::events::maint::seal_parts(shared, &db, &schema, &st, &version, read_time).await
            };
            match sealed.await {
                Ok(out) => {
                    let out = out.map(|o| {
                        let written = o.written.iter().map(|r| (r.key().to_vec(), r.encode().to_vec())).collect();
                        ((o.last.version, o.last.ordinal), o.rows, o.bytes, written)
                    });
                    (JobResult::Sealed(out), Bytes::new())
                }
                Err(e) => failed(e),
            }
        }
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
    use crate::engine::{Isolation, Snapshot, Txn};
    let snapshot = Snapshot { version: Arc::new(snapshot_version(shared, etag, &manifest)?), time_ms: read_time };
    drop(manifest);
    let version = snapshot.version.m.version;
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

async fn stage_insert(
    shared: &Arc<Shared>,
    db: &str,
    schema: &EventSchema,
    content_type: &str,
    body: &[u8],
    now_ms: i64,
    row_bytes_max: usize,
) -> Result<Bind> {
    let mut valid = crate::events::ValidRows::up_to(row_bytes_max);
    crate::api::events::each_event(content_type, body, &mut |v| valid.push(schema, &v, now_ms))?;
    if valid.is_empty() {
        return Err(Error::invalid("insert at least one event"));
    }
    let parts = crate::events::bulk::stage(shared, db, schema, std::slice::from_ref(&valid)).await?;
    Ok(Bind { table: schema.id, slots: vec![BindSlot { commit_id: 0, deadline: None }], parts })
}

/// Rebuilds the snapshot [`Version`](crate::engine::Version) whose encoded manifest the node sent.
fn snapshot_version(shared: &Arc<Shared>, etag: String, manifest: &Bytes) -> Result<crate::engine::Version> {
    let m = crate::engine::manifest::Manifest::decode(manifest)
        .map_err(|e| Error::internal(format!("snapshot manifest: {e}")))?;
    Ok(crate::engine::Version::new(m, etag, shared.clone()))
}
