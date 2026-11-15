//! Worker processes that run an owner's heavy work inside the owner's cgroup.
//!
//! The node starts one worker for each active owner as `baserunna worker`. The
//! worker reads job frames from stdin, runs them concurrently against the
//! store, and writes each result frame to stdout. The kernel enforces the owner's `memory.max` and `cpu.weight`, so
//! an owner that exceeds its memory loses only its own worker.
//!
//! The simulator never sets `BASERUNNA_CGROUP_ROOT`, so it runs the same jobs
//! in process without kernel limits.

use crate::engine::events::{Bind, BindSlot, PartRef};
use crate::engine::{BlobRef, Shared};
use crate::boundary;
use crate::error::{Error, Result};
use crate::events::schema::EventSchema;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

/// Default memory limit of one owner's cgroup.
pub const OWNER_MEMORY_BYTES: u64 = 512 * 1024 * 1024;

/// Idle time after which the node stops an owner's worker.
pub const WORKER_IDLE_STOP: Duration = Duration::from_secs(20);

/// Longest time a worker may take to seal unsealed rows.
const SEAL_DEADLINE: Duration = Duration::from_secs(600);

/// Longest time a worker may take to mark a database deleted, check it, or purge it.
const MAINTENANCE_DEADLINE: Duration = Duration::from_secs(300);

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
    /// Runs the ops of a read-only one-shot transaction request at a snapshot whose encoded
    /// manifest is the job's payload, and returns the JSON response body as the result's payload.
    ReadTxn {
        db: String,
        etag: String,
        read_time: u64,
        request: serde_json::Value,
        json_value_max: u64,
        limits: (u64, usize, usize, u64),
    },
    /// Runs table query `query` on table `table` at a snapshot whose encoded manifest is the
    /// job's payload, and returns the JSON response body as the result's payload.
    TableQuery {
        etag: String,
        read_time: u64,
        table: String,
        query: serde_json::Value,
    },
    /// Runs metadata command `cmd` on the system database; only the system worker gets it.
    Meta { cmd: crate::meta::MetaCmd },
    /// Runs one pass of the reaper; only the system worker gets it.
    Reap,
    /// Marks database `db` deleted, so the owner's workers stop serving it.
    MarkDeleted { db: String },
    /// Returns whether the manifest of database `db` is marked deleted.
    IsDeleted { db: String },
    /// Deletes every object of database `db` from the store.
    Purge { db: String },
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
    /// The job finished with no value.
    Finished,
    /// The JSON answer of a [`Job::Meta`].
    Meta(serde_json::Value),
    /// Whether the database of an [`Job::IsDeleted`] is marked deleted.
    Deleted(bool),
    Merged(Option<(Vec<u8>, Vec<u8>)>),
    BlobEquals(bool),
    BlobDecimal(Option<i64>),
    /// The last sealed row as `(version, ordinal)`, the sealed row and byte counts, and the written parts.
    Sealed(Option<((u64, u32), u64, u64, Vec<(Vec<u8>, Vec<u8>)>)>),
    /// The JSON response body of a query or a read-only transaction is the result's payload.
    Queried,
    /// The body's idempotency key and, on success, the encoded [`Bind`] as the payload.
    Staged {
        body_key: Option<String>,
        error: Option<serde_json::Value>,
    },
    /// The job's error as the JSON body of an HTTP error response.
    Failed(serde_json::Value),
}

/// A frame header that the node sends to a worker.
#[derive(Serialize, Deserialize)]
enum ToWorker {
    /// A job to run; the frame's ID names the job.
    Job(Box<Job>),
}

/// A frame header that a worker sends to the node.
#[derive(Serialize, Deserialize)]
enum FromWorker {
    /// The result of the job with the frame's ID.
    Done(JobResult),
}

/// The key of the system worker among the pool's workers. It holds the system database's
/// lease, runs its metadata commands and upkeep, and runs the reaper. No owner ID has this form.
pub const SYSTEM_WORKER: &str = "@system";

/// The environment variable that gives a worker the node's ID.
pub const WORKER_NODE_ID_VAR: &str = "BASERUNNA_WORKER_NODE_ID";

/// The engine cache of the node process, which holds only the system database.
pub const MAIN_CACHE_BYTES: usize = 16 << 20;

/// The engine cache of one worker; it counts against the owner's memory limit.
pub const WORKER_CACHE_BYTES: usize = 64 << 20;

/// The most requests of one owner that the node has in flight in the owner's worker.
pub const OWNER_IN_FLIGHT_MAX: usize = 64;

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
    /// The CPU weight of the node process.
    pub main_cpu_weight: u32,
}

/// Starts workers in per-owner cgroups and runs jobs in them.
pub struct WorkerPool {
    cfg: PoolConfig,
    db_owners: Mutex<HashMap<String, String>>,
    owner_resolver: std::sync::OnceLock<OwnerResolver>,
    /// The node's ID, under which workers hold the leases of their databases.
    node_id: std::sync::OnceLock<String>,
    /// Each owner's requests in flight: [`OWNER_IN_FLIGHT_MAX`] permits.
    owner_in_flight: Mutex<HashMap<String, Arc<tokio::sync::Semaphore>>>,
    owner_workers: Mutex<HashMap<String, Arc<OwnerWorker>>>,
    next_job_id: std::sync::atomic::AtomicU64,
    /// Makes the node of each new worker in this process, for tests; `None` starts processes.
    in_process: Option<WorkerNodeFactory>,
    /// The request streams that the pool has opened to workers.
    opened_streams: std::sync::atomic::AtomicU64,
}

/// Makes the node of a worker that runs in the node process, for tests.
pub type WorkerNodeFactory = Box<dyn Fn() -> Arc<crate::node::Node> + Send + Sync>;

impl WorkerPool {
    /// Returns a pool whose workers run in this process over in-memory pipes, without cgroups,
    /// each with a node from `make`. Tests use it to drive real request streams.
    pub fn in_process(make: WorkerNodeFactory) -> WorkerPool {
        let cfg = PoolConfig {
            cgroup_root: PathBuf::new(),
            exe: PathBuf::new(),
            owner_memory_bytes: OWNER_MEMORY_BYTES,
            owner_cpu_weight: 100,
            main_cpu_weight: 100,
        };
        WorkerPool {
            cfg,
            db_owners: Mutex::new(HashMap::new()),
            owner_resolver: Default::default(),
            node_id: Default::default(),
            owner_in_flight: Mutex::new(HashMap::new()),
            owner_workers: Mutex::new(HashMap::new()),
            next_job_id: 0.into(),
            in_process: Some(make),
            opened_streams: 0.into(),
        }
    }

    /// Returns the node ID that the worker of `owner` takes: the node's ID for an owner worker,
    /// so it takes over the leases of the last one, and a separate ID for the system worker, so
    /// the node process never sees its own ID on the system database's lease.
    fn worker_node_id(&self, owner: &str) -> String {
        let id = self.node_id.get().cloned().unwrap_or_default();
        match owner {
            SYSTEM_WORKER if !id.is_empty() => format!("{id}-system"),
            _ => id,
        }
    }

    /// Returns the number of request streams that the pool has opened to workers.
    pub fn opened_streams(&self) -> u64 {
        self.opened_streams.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Enables the memory and CPU controllers below `cfg.cgroup_root`, and gives the node
    /// process its CPU weight if the node runs in a child cgroup of `cfg.cgroup_root`.
    pub fn new(cfg: PoolConfig) -> std::io::Result<WorkerPool> {
        std::fs::write(cfg.cgroup_root.join("cgroup.subtree_control"), "+memory +cpu")?;
        if let Some(own) = own_cgroup()
            && own.parent() == Some(cfg.cgroup_root.as_path())
        {
            std::fs::write(own.join("cpu.weight"), cfg.main_cpu_weight.to_string())?;
        }
        Ok(WorkerPool {
            cfg,
            db_owners: Mutex::new(HashMap::new()),
            owner_resolver: Default::default(),
            node_id: Default::default(),
            owner_in_flight: Mutex::new(HashMap::new()),
            owner_workers: Mutex::new(HashMap::new()),
            next_job_id: 0.into(),
            in_process: None,
            opened_streams: 0.into(),
        })
    }

    /// Records that database `db` belongs to owner `owner`.
    pub fn set_db_owner(&self, db: &str, owner: &str) {
        let mut g = self.db_owners.lock();
        if g.get(db).map(String::as_str) != Some(owner) {
            g.insert(db.to_string(), owner.to_string());
        }
    }

    /// Waits until `owner` has fewer than [`OWNER_IN_FLIGHT_MAX`] requests in flight, or fails
    /// with a retryable 503 at `deadline`. The request counts until the permit drops.
    async fn admit(&self, owner: &str, deadline: tokio::time::Instant) -> Result<tokio::sync::OwnedSemaphorePermit> {
        let in_flight = self
            .owner_in_flight
            .lock()
            .entry(owner.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(OWNER_IN_FLIGHT_MAX)))
            .clone();
        match tokio::time::timeout_at(deadline, in_flight.acquire_owned()).await {
            Ok(Ok(permit)) => Ok(permit),
            _ => Err(Error::unavailable("the owner has too many requests in progress")),
        }
    }

    /// Sets the node's ID, which each worker uses as its own, so a new worker of an owner
    /// holds the leases that the owner's last worker held.
    pub fn set_node_id(&self, id: String) {
        let _ = self.node_id.set(id);
    }

    /// Sets the function that reads the owner of a database.
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
        let dir = match owner {
            SYSTEM_WORKER => self.cfg.cgroup_root.join("system"),
            _ => self.cfg.cgroup_root.join(format!("owner-{name}")),
        };
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

    /// Runs read-only one-shot transaction `request` on `txn`'s snapshot in a worker inside
    /// `owner`'s cgroup and returns the JSON response body.
    #[allow(clippy::too_many_arguments)]
    pub async fn read_txn(
        &self,
        owner: &str,
        db: &str,
        txn: &mut crate::engine::Txn,
        request: serde_json::Value,
        json_value_max: u64,
        limits: &crate::events::exec::Limits,
        deadline: Duration,
    ) -> Result<Bytes> {
        let version = txn.untracked_snapshot().clone();
        let job = Job::ReadTxn {
            db: db.to_string(),
            etag: version.etag.clone(),
            read_time: txn.read_time,
            request,
            json_value_max,
            limits: (limits.max_bytes_read, limits.max_memory_bytes, limits.lookup_max_bytes, limits.max_ms),
        };
        let manifest = version.m.encode();
        drop(version);
        match self.run(owner, job, manifest, deadline).await? {
            (JobResult::Queried, body) => Ok(body),
            (JobResult::Failed(e), _) => Err(crate::api::error_from_json(500, &e)),
            _ => Err(Error::internal("the worker returned a result of another job")),
        }
    }

    /// Runs table query `query` on `txn`'s snapshot in a worker inside `owner`'s cgroup and
    /// returns the JSON response body.
    pub async fn table_query(
        &self,
        owner: &str,
        txn: &mut crate::engine::Txn,
        table: &str,
        query: serde_json::Value,
        deadline: Duration,
    ) -> Result<Bytes> {
        let version = txn.untracked_snapshot().clone();
        let job =
            Job::TableQuery { etag: version.etag.clone(), read_time: txn.read_time, table: table.to_string(), query };
        let manifest = version.m.encode();
        drop(version);
        match self.run(owner, job, manifest, deadline).await? {
            (JobResult::Queried, body) => Ok(body),
            (JobResult::Failed(e), _) => Err(crate::api::error_from_json(500, &e)),
            _ => Err(Error::internal("the worker returned a result of another job")),
        }
    }

    /// Serves database request `req` in a worker inside the owner's cgroup. The request and
    /// response bodies stream through the boundary's credit windows, so the node holds at most
    /// [`boundary::WINDOW_CHUNKS`] chunks of each body.
    ///
    /// `caller` is the signed-in principal and its access, or `None` for a request from a peer.
    pub async fn http(
        &self,
        caller: Option<(crate::node::Principal, crate::meta::Access)>,
        rec: crate::meta::DbRecord,
        deadline: crate::node::Deadline,
        req: axum::extract::Request,
    ) -> Result<axum::response::Response> {
        let owner = rec.owner_id.clone();
        let (parts, body) = req.into_parts();
        let in_flight = match self.admit(&owner, deadline.instant()).await {
            Ok(p) => p,
            Err(e) => {
                tokio::spawn(boundary::drain_body(body));
                return Err(e);
            }
        };
        let left = deadline.instant().saturating_duration_since(tokio::time::Instant::now());
        let context = HttpContext { caller, rec };
        let head = boundary::RequestHead {
            method: parts.method.to_string(),
            uri: parts.uri.to_string(),
            headers: boundary::header_pairs(&parts.headers),
            deadline_ms: left.as_millis() as u64,
            context: serde_json::to_vec(&context).map_err(|e| Error::internal(e.to_string()))?.into(),
        };
        let id = self.next_job_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let worker = match self.owner_worker(&owner) {
            Ok(w) => w,
            Err(e) => {
                tokio::spawn(boundary::drain_body(body));
                return Err(e);
            }
        };
        *worker.last_used.lock() = std::time::Instant::now();
        let (head_done, head_wait) = tokio::sync::oneshot::channel();
        let (inbox, resp_body) = boundary::credited_body(id, worker.frames.clone());
        let credits = worker.streams.register(id, Some(inbox), Some(head_done));
        let open = boundary::Frame::new(boundary::FrameKind::Open, id, head.encode());
        self.opened_streams.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if worker.exited.load(std::sync::atomic::Ordering::Acquire) || worker.frames.send(open).is_err() {
            worker.streams.remove(id);
            tokio::spawn(boundary::drain_body(body));
            return Err(Error::unavailable("the owner's worker exited"));
        }
        tokio::spawn(boundary::send_body(id, body, credits, worker.frames.clone(), boundary::AfterEnd::Drain));
        let head = match tokio::time::timeout(left + Duration::from_secs(1), head_wait).await {
            Ok(Ok(head)) => head?,
            Ok(Err(_)) => return Err(Error::internal("the owner's worker dropped the request")),
            Err(_) => {
                worker.streams.remove(id);
                let reset = Bytes::from_static(b"the request deadline passed");
                let _ = worker.frames.send(boundary::Frame::new(boundary::FrameKind::Reset, id, reset));
                return Err(Error::TooLarge(format!("the work ran longer than {} ms", left.as_millis())));
            }
        };
        let open = StreamGuard { id, worker: worker.clone(), _in_flight: in_flight };
        let body = {
            use futures_util::StreamExt;
            axum::body::Body::from_stream(resp_body.into_data_stream().map(move |chunk| {
                let _ = &open;
                chunk
            }))
        };
        let mut resp = axum::response::Response::new(body);
        *resp.status_mut() = http::StatusCode::from_u16(head.status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
        boundary::append_headers(resp.headers_mut(), head.headers);
        Ok(resp)
    }

    /// Returns true if blob `blob` of `db` equals `want`, read in a worker inside the owner's cgroup.
    pub async fn blob_equals(&self, db: &str, blob: &BlobRef, want: Bytes) -> Result<bool> {
        match self.blob_value(db, blob, true, want).await? {
            JobResult::BlobEquals(eq) => Ok(eq),
            _ => Err(Error::internal("the worker returned a result of another job")),
        }
    }

    /// Runs metadata command `cmd` in the system worker, which holds the system database.
    pub async fn run_meta(&self, cmd: crate::meta::MetaCmd) -> Result<serde_json::Value> {
        match self.run(SYSTEM_WORKER, Job::Meta { cmd }, Bytes::new(), MAINTENANCE_DEADLINE).await?.0 {
            JobResult::Meta(j) => Ok(j),
            JobResult::Failed(e) => Err(crate::api::error_from_json(500, &e)),
            _ => Err(Error::internal("the worker returned a result of another job")),
        }
    }

    /// Runs one pass of the reaper in the system worker: purges deleted databases past their
    /// grace period and deletes expired sessions.
    pub async fn reap(&self) -> Result<()> {
        self.finish(SYSTEM_WORKER, Job::Reap).await
    }

    /// Marks database `db` of owner `owner` deleted in the owner's worker.
    pub async fn mark_deleted(&self, owner: &str, db: &str) -> Result<()> {
        self.finish(owner, Job::MarkDeleted { db: db.to_string() }).await
    }

    /// Deletes every object of database `db` of owner `owner` in the owner's worker.
    pub async fn purge(&self, owner: &str, db: &str) -> Result<()> {
        self.finish(owner, Job::Purge { db: db.to_string() }).await
    }

    /// Returns whether the manifest of database `db` of owner `owner` is marked deleted, read in
    /// the owner's worker.
    pub async fn is_deleted(&self, owner: &str, db: &str) -> Result<bool> {
        match self.run(owner, Job::IsDeleted { db: db.to_string() }, Bytes::new(), MAINTENANCE_DEADLINE).await?.0 {
            JobResult::Deleted(d) => Ok(d),
            JobResult::Failed(e) => Err(crate::api::error_from_json(500, &e)),
            _ => Err(Error::internal("the worker returned a result of another job")),
        }
    }

    async fn finish(&self, owner: &str, job: Job) -> Result<()> {
        match self.run(owner, job, Bytes::new(), MAINTENANCE_DEADLINE).await?.0 {
            JobResult::Finished => Ok(()),
            JobResult::Failed(e) => Err(crate::api::error_from_json(500, &e)),
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

    /// Runs `job` with `payload` in `owner`'s worker and returns the result and its payload.
    async fn run(&self, owner: &str, job: Job, payload: Bytes, deadline: Duration) -> Result<(JobResult, Bytes)> {
        let header: Bytes =
            serde_json::to_vec(&ToWorker::Job(Box::new(job))).map_err(|e| Error::internal(e.to_string()))?.into();
        let job_id = self.next_job_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut attempts = 0;
        let (worker, result) = loop {
            let worker = self.owner_worker(owner)?;
            let (done, result) = tokio::sync::oneshot::channel();
            worker.pending.lock().insert(job_id, done);
            *worker.last_used.lock() = std::time::Instant::now();
            let exited = worker.exited.load(std::sync::atomic::Ordering::Acquire);
            if !exited && worker.frames.send(job_frame(job_id, &header, &payload)).is_ok() {
                break (worker, result);
            }
            worker.pending.lock().remove(&job_id);
            attempts += 1;
            if attempts == 2 {
                return Err(Error::unavailable("the owner's worker exited"));
            }
        };
        match tokio::time::timeout(deadline, result).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err(Error::internal("the owner's worker dropped the job")),
            Err(_) => {
                worker.pending.lock().remove(&job_id);
                // A job cannot be stopped inside the worker, so the deadline ends all of the owner's jobs.
                let _ = std::fs::write(worker.leaf.join("cgroup.kill"), "1");
                Err(Error::TooLarge(format!("the work ran longer than {} ms", deadline.as_millis())))
            }
        }
    }

    /// Stops each worker that has no job and has been idle for at least `idle`, so an idle owner has no process.
    pub fn stop_idle_workers(&self, idle: Duration) {
        self.owner_workers.lock().retain(|_, w| {
            let pending = w.pending.lock();
            if !pending.is_empty() || !w.streams.is_empty() || w.last_used.lock().elapsed() < idle {
                return true;
            }
            w.exited.store(true, std::sync::atomic::Ordering::Release);
            let _ = std::fs::write(w.leaf.join("cgroup.kill"), "1");
            false
        });
    }

    /// Stops every worker, waits up to `wait` until the kernel reports each owner cgroup empty,
    /// and removes the owner cgroups, so a stopped node leaves no cgroups behind.
    pub async fn stop_all(&self, wait: Duration) {
        for (_, w) in self.owner_workers.lock().drain() {
            w.exited.store(true, std::sync::atomic::Ordering::Release);
        }
        let dirs: Vec<PathBuf> = std::fs::read_dir(&self.cfg.cgroup_root)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| {
                        let name = e.file_name().to_string_lossy().into_owned();
                        name.starts_with("owner-") || name == "system"
                    })
                    .map(|e| e.path())
                    .collect()
            })
            .unwrap_or_default();
        for d in &dirs {
            let _ = std::fs::write(d.join("cgroup.kill"), "1");
        }
        let until = tokio::time::Instant::now() + wait;
        for d in &dirs {
            while !cgroup_empty(d) && tokio::time::Instant::now() < until {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            if let Err(e) = std::fs::remove_dir(d) {
                crate::warn!("cannot remove cgroup {}: {e}", d.display());
            }
        }
    }

    /// Returns the number of running workers.
    pub fn running_workers(&self) -> usize {
        self.owner_workers.lock().len()
    }

    /// Returns `owner`'s running worker, or starts one in the owner's cgroup.
    fn owner_worker(&self, owner: &str) -> Result<Arc<OwnerWorker>> {
        let mut workers = self.owner_workers.lock();
        if let Some(w) = workers.get(owner)
            && !w.exited.load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(w.clone());
        }
        let worker = self.start_worker(owner).map_err(|e| Error::unavailable(format!("cannot start the owner's worker: {e}")))?;
        workers.insert(owner.to_string(), worker.clone());
        Ok(worker)
    }

    fn start_worker(&self, owner: &str) -> std::io::Result<Arc<OwnerWorker>> {
        if let Some(make) = &self.in_process {
            let (node_side, worker_side) = tokio::io::duplex(4 * boundary::CHUNK_MAX);
            let (worker_in, worker_out) = tokio::io::split(worker_side);
            tokio::spawn(serve_io(make(), worker_in, worker_out));
            let (from_worker, to_worker) = tokio::io::split(node_side);
            return Ok(self.attach(owner, PathBuf::new(), None, to_worker, from_worker));
        }
        let owner_dir = self.owner_cgroup(owner)?;
        // The worker runs in the owner's cgroup itself, one level below the node's cgroup, so a
        // teardown that waits for the node's child cgroups to empty also waits for workers.
        let leaf = owner_dir.clone();
        // Two workers of one owner would both hold the owner's leases, so a new worker starts
        // only after the kernel reports that the last one has exited.
        if std::fs::read_to_string(leaf.join("cgroup.procs")).is_ok_and(|p| !p.trim().is_empty()) {
            let _ = std::fs::write(leaf.join("cgroup.kill"), "1");
            return Err(std::io::Error::other("the owner's last worker is still exiting"));
        }
        let mut child = tokio::process::Command::new(&self.cfg.exe)
            .arg("worker")
            .env(WORKER_NODE_ID_VAR, self.worker_node_id(owner))
            // Large buffers map their own pages and the heap trims its top, so freed job
            // memory leaves the owner's cgroup instead of staying in the allocator.
            .env(
                "GLIBC_TUNABLES",
                "glibc.malloc.arena_max=2:glibc.malloc.mmap_threshold=131072:glibc.malloc.trim_threshold=131072",
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        // The worker reads nothing and allocates little until it receives a job,
        // so it joins the owner's cgroup before any tenant data reaches it.
        let pid = child.id().unwrap_or(0);
        if let Err(e) = std::fs::write(leaf.join("cgroup.procs"), pid.to_string()) {
            let _ = child.start_kill();
            return Err(e);
        }
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        Ok(self.attach(owner, leaf, Some(child), stdin, stdout))
    }

    /// Joins a started worker to the pool: `to_worker` carries frames to it and `from_worker`
    /// carries its frames back. `child` is its process, if it has one.
    fn attach(
        &self,
        owner: &str,
        leaf: PathBuf,
        child: Option<tokio::process::Child>,
        to_worker: impl tokio::io::AsyncWrite + Unpin + Send + 'static,
        mut from_worker: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    ) -> Arc<OwnerWorker> {
        let _ = owner;
        let (frames, to_send) = tokio::sync::mpsc::unbounded_channel::<boundary::Frame>();
        let worker = Arc::new(OwnerWorker {
            frames,
            pending: Mutex::new(HashMap::new()),
            streams: boundary::Streams::default(),
            leaf: leaf.clone(),
            exited: false.into(),
            last_used: Mutex::new(std::time::Instant::now()),
        });
        tokio::spawn(boundary::write_frames(to_worker, to_send));
        let reader = worker.clone();
        let limit_mib = self.cfg.owner_memory_bytes >> 20;
        let ooms_before = oom_kills(&leaf);
        tokio::spawn(async move {
            while let Ok(frame) = boundary::read_frame(&mut from_worker).await {
                if frame.kind != boundary::FrameKind::Job {
                    reader.streams.deliver(frame);
                    continue;
                }
                let Ok((job_id, header, payload)) = split_job(frame) else { break };
                let result = match serde_json::from_slice::<FromWorker>(&header) {
                    Ok(FromWorker::Done(h)) => Ok((h, payload)),
                    Err(e) => Err(Error::internal(format!("worker result: {e}"))),
                };
                *reader.last_used.lock() = std::time::Instant::now();
                if let Some(done) = reader.pending.lock().remove(&job_id) {
                    let _ = done.send(result);
                }
            }
            reader.exited.store(true, std::sync::atomic::Ordering::Release);
            let status = match child {
                Some(mut child) => format!("{:?}", child.wait().await),
                None => "the end of its pipe".to_string(),
            };
            let oom = oom_kills(&leaf) > ooms_before;
            let exit_error = |oom: bool| {
                if oom {
                    Error::TooLarge(format!("the work exceeded the owner's memory limit of {limit_mib} MiB"))
                } else {
                    Error::internal(format!("worker exited with {status}"))
                }
            };
            reader.streams.fail_all(&exit_error(oom));
            for (_, done) in reader.pending.lock().drain() {
                let _ = done.send(Err(exit_error(oom)));
            }
        });
        worker
    }
}

/// Delivers a job's result and payload to the request that waits for it.
type JobDone = tokio::sync::oneshot::Sender<Result<(JobResult, Bytes)>>;

/// The principal, database record, and access of a request that a worker serves.
#[derive(Serialize, Deserialize)]
struct HttpContext {
    /// The signed-in caller and its access, or `None` for a request from a peer node.
    caller: Option<(crate::node::Principal, crate::meta::Access)>,
    rec: crate::meta::DbRecord,
}

/// Keeps a request stream open, and its owner's in-flight permit taken, until the response
/// body ends or the client goes away. On drop it ends the stream in the worker.
struct StreamGuard {
    id: u64,
    worker: Arc<OwnerWorker>,
    _in_flight: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.worker.streams.remove(self.id);
        *self.worker.last_used.lock() = std::time::Instant::now();
        let reset = boundary::Frame::new(boundary::FrameKind::Reset, self.id, Bytes::from_static(b"the client left"));
        let _ = self.worker.frames.send(reset);
    }
}

/// One owner's worker process, which runs the owner's jobs and request streams concurrently.
struct OwnerWorker {
    /// Frames for the task that writes the worker's stdin.
    frames: boundary::Outbox,
    /// The worker's open request streams.
    streams: boundary::Streams,
    /// Jobs sent to the worker that wait for their results, by job ID.
    pending: Mutex<HashMap<u64, JobDone>>,
    /// The owner's cgroup, which holds the worker process.
    leaf: PathBuf,
    exited: std::sync::atomic::AtomicBool,
    /// When the last job of this worker started or ended.
    last_used: Mutex<std::time::Instant>,
}

/// Builds the boundary frame of job frame `job_id`: the header's length, the header, then the payload.
fn job_frame(job_id: u64, header: &[u8], payload: &[u8]) -> boundary::Frame {
    let mut data = Vec::with_capacity(8 + header.len() + payload.len());
    data.extend_from_slice(&(header.len() as u64).to_le_bytes());
    data.extend_from_slice(header);
    data.extend_from_slice(payload);
    boundary::Frame::new(boundary::FrameKind::Job, job_id, data.into())
}

/// Splits a boundary job frame into its job ID, header, and payload.
fn split_job(f: boundary::Frame) -> std::io::Result<(u64, Bytes, Bytes)> {
    let bad = || std::io::Error::new(std::io::ErrorKind::InvalidData, "a job frame is too short");
    let len = f.data.get(..8).ok_or_else(bad)?;
    let len = u64::from_le_bytes(len.try_into().map_err(|_| bad())?) as usize;
    if f.data.len() < 8 + len {
        return Err(bad());
    }
    Ok((f.id, f.data.slice(8..8 + len), f.data.slice(8 + len..)))
}

/// Returns the cgroup v2 directory of this process.
fn own_cgroup() -> Option<PathBuf> {
    let line = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let rel = line.lines().find_map(|l| l.strip_prefix("0::"))?;
    Some(PathBuf::from("/sys/fs/cgroup").join(rel.trim_start_matches('/')))
}

/// Returns true if the kernel reports that `cgroup` and its children hold no process.
fn cgroup_empty(cgroup: &std::path::Path) -> bool {
    std::fs::read_to_string(cgroup.join("cgroup.events")).map_or(true, |e| e.lines().any(|l| l == "populated 0"))
}

fn oom_kills(cgroup: &std::path::Path) -> u64 {
    let events = std::fs::read_to_string(cgroup.join("memory.events")).unwrap_or_default();
    events.lines().find_map(|l| l.strip_prefix("oom_kill ")).and_then(|n| n.trim().parse().ok()).unwrap_or(0)
}

/// Runs the jobs that the node sends on stdin and writes each result to stdout (the `baserunna worker` command).
pub async fn serve(node: Arc<crate::node::Node>) -> std::io::Result<()> {
    serve_io(node, tokio::io::stdin(), tokio::io::stdout()).await
}

/// Runs the jobs and request streams that the node sends on `input`, and writes each result
/// and response to `output`.
async fn serve_io(
    node: Arc<crate::node::Node>,
    mut input: impl tokio::io::AsyncRead + Unpin,
    output: impl tokio::io::AsyncWrite + Unpin + Send + 'static,
) -> std::io::Result<()> {
    let engine = node.engine.clone();
    // Console pages call the API in process, so the worker's node serves the full router and
    // handles each database request itself.
    node.set_router(crate::api::router(node.clone()));
    let router = crate::api::worker_routes()
        .layer(axum::extract::DefaultBodyLimit::max(node.cfg.max_json_body))
        .with_state(node.clone());
    let (results, to_write) = tokio::sync::mpsc::unbounded_channel::<boundary::Frame>();
    let writer = tokio::spawn(boundary::write_frames(output, to_write));
    let streams = Arc::new(boundary::Streams::default());
    let stream_tasks: Arc<Mutex<HashMap<u64, tokio::task::AbortHandle>>> = Default::default();
    while let Ok(frame) = boundary::read_frame(&mut input).await {
        match frame.kind {
            boundary::FrameKind::Job => {}
            boundary::FrameKind::Open => {
                // The stream exists before the next frame is read, so its first chunk finds it.
                let (inbox, body) = boundary::credited_body(frame.id, results.clone());
                let credits = streams.register(frame.id, Some(inbox), None);
                let (node, router, streams, out) = (node.clone(), router.clone(), streams.clone(), results.clone());
                let (id, tasks) = (frame.id, stream_tasks.clone());
                let task = tokio::spawn(async move {
                    serve_stream(node, router, streams, out, frame, body, credits).await;
                    tasks.lock().remove(&id);
                });
                stream_tasks.lock().insert(id, task.abort_handle());
                continue;
            }
            boundary::FrameKind::Reset => {
                // The node gave up on the stream, so its work stops at its next await.
                if let Some(task) = stream_tasks.lock().remove(&frame.id) {
                    task.abort();
                }
                streams.deliver(frame);
                continue;
            }
            _ => {
                streams.deliver(frame);
                continue;
            }
        }
        let Ok((job_id, header, payload)) = split_job(frame) else { break };
        let job = match serde_json::from_slice::<ToWorker>(&header) {
            Ok(ToWorker::Job(job)) => Ok(*job),
            Err(e) => Err(e),
        };
        let (engine, node) = (engine.clone(), node.clone());
        let results = results.clone();
        tokio::spawn(async move {
            let (result, out) = match job {
                Ok(Job::Meta { cmd }) => match node.run_meta_as::<serde_json::Value>(cmd).await {
                    Ok(j) => (JobResult::Meta(j), Bytes::new()),
                    Err(e) => (JobResult::Failed(json!({ "error": crate::api::error_json(&e) })), Bytes::new()),
                },
                Ok(Job::Reap) => match node.reap_deleted().await {
                    Ok(_) => (JobResult::Finished, Bytes::new()),
                    Err(e) => (JobResult::Failed(json!({ "error": crate::api::error_json(&e) })), Bytes::new()),
                },
                Ok(job) => run_job(&engine, job, payload).await,
                Err(e) => (
                    JobResult::Failed(json!({ "error": crate::api::error_json(&Error::internal(e.to_string())) })),
                    Bytes::new(),
                ),
            };
            let header = serde_json::to_vec(&FromWorker::Done(result)).unwrap_or_default();
            let _ = results.send(job_frame(job_id, &header, &out));
        });
    }
    drop(results);
    writer.await.map_err(std::io::Error::other)?
}

async fn run_job(engine: &Arc<crate::engine::Engine>, job: Job, payload: Bytes) -> (JobResult, Bytes) {
    let shared = &engine.shared;
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
        Job::Meta { .. } | Job::Reap => failed(Error::internal("a system job reached run_job")),
        Job::MarkDeleted { db } => {
            let db = engine.db(&db);
            match db.open(false).await {
                Ok(_) => match db.mark_deleted().await {
                    Ok(()) => (JobResult::Finished, Bytes::new()),
                    Err(e) => failed(e),
                },
                Err(Error::NotFound(_)) => (JobResult::Finished, Bytes::new()),
                Err(e) => failed(e),
            }
        }
        Job::IsDeleted { db } => {
            let db = engine.db(&db);
            let deleted = db.read_version(crate::engine::Consistency::Strong).await.is_err()
                && db.current().is_some_and(|v| v.m.is_deleted());
            (JobResult::Deleted(deleted), Bytes::new())
        }
        Job::Purge { db } => match engine.purge(&db).await {
            Ok(()) => (JobResult::Finished, Bytes::new()),
            Err(e) => failed(e),
        },
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
        Job::ReadTxn { db, etag, read_time, request, json_value_max, limits } => {
            let db = engine.db(&db);
            let snapshot = match snapshot_version(shared, etag, &payload) {
                Ok(v) => crate::engine::Snapshot { version: Arc::new(v), time_ms: read_time },
                Err(e) => return failed(e),
            };
            drop(payload);
            match run_read_txn(shared, &db, snapshot, request, json_value_max, limits).await {
                Ok(body) => (JobResult::Queried, body),
                Err(e) => failed(e),
            }
        }
        Job::TableQuery { etag, read_time, table, query } => {
            let snapshot = match snapshot_version(shared, etag, &payload) {
                Ok(v) => crate::engine::Snapshot { version: Arc::new(v), time_ms: read_time },
                Err(e) => return failed(e),
            };
            drop(payload);
            match run_table_query(shared, snapshot, &table, query).await {
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

/// Runs the ops of read-only one-shot transaction `request` at `snapshot` of `db`.
async fn run_read_txn(
    shared: &Arc<Shared>,
    db: &Arc<crate::engine::Db>,
    snapshot: crate::engine::Snapshot,
    request: serde_json::Value,
    json_value_max: u64,
    (max_bytes_read, max_memory_bytes, lookup_max_bytes, max_ms): (u64, usize, usize, u64),
) -> Result<Bytes> {
    use baserunna_api::Body;
    let req: baserunna_api::TxnRequest =
        serde_json::from_value(request).map_err(|e| Error::internal(format!("transaction request: {e}")))?;
    let ops = crate::ops::parse_ops(req.ops)?;
    if ops.iter().any(|o| o.is_write()) {
        return Err(Error::internal("a worker transaction has a write op"));
    }
    let isolation = match req.isolation {
        baserunna_api::Isolation::Serializable => crate::engine::Isolation::Serializable,
        baserunna_api::Isolation::Snapshot => crate::engine::Isolation::Snapshot,
    };
    let cfg = &shared.cfg;
    let mut txn = crate::engine::Txn::new(snapshot, isolation, 0, cfg.txn_max_write_bytes)
        .with_read_limit(cfg.txn_max_read_bytes);
    let ctx = crate::ops::OpCtx {
        db,
        encoding: req.encoding,
        access: crate::meta::Access { role: crate::meta::Role::Reader, read_only_key: true },
        json_value_max,
        query_limits: crate::events::exec::Limits { max_bytes_read, max_memory_bytes, lookup_max_bytes, max_ms },
        node: None,
        user_id: "",
    };
    let results = crate::ops::exec_ops(&ctx, &mut txn, &ops).await?;
    let version = txn.read_version();
    let body = baserunna_api::TxnResponse {
        version,
        committed: false,
        commit_id: None,
        replayed: false,
        results: Some(results),
        appends: None,
    };
    Ok(body.encode())
}

/// The fields of a [`Job::Http`].
/// Serves the request stream that `open` starts, with request body `body`, on the worker's
/// node with the database routes of the node process. It streams the response back to `out`
/// within the window of `credits`.
async fn serve_stream(
    node: Arc<crate::node::Node>,
    router: axum::Router,
    streams: Arc<boundary::Streams>,
    out: boundary::Outbox,
    open: boundary::Frame,
    body: axum::body::Body,
    credits: Arc<tokio::sync::Semaphore>,
) {
    let id = open.id;
    let resp = match boundary::RequestHead::decode(open.data) {
        Ok(head) => match route_stream(&node, router, head, body).await {
            Ok(resp) => {
                let (parts, body) = resp.into_parts();
                with_first_chunk(body).await.map(|body| axum::response::Response::from_parts(parts, body))
            }
            Err(e) => Err(e),
        },
        Err(e) => Err(e),
    };
    let resp = resp.unwrap_or_else(|e| axum::response::IntoResponse::into_response(e));
    let (parts, body) = resp.into_parts();
    let head = boundary::ResponseHead { status: parts.status.as_u16(), headers: boundary::header_pairs(&parts.headers) };
    if out.send(boundary::Frame::new(boundary::FrameKind::Head, id, head.encode())).is_ok() {
        boundary::send_body(id, body, credits, out, boundary::AfterEnd::Stop).await;
    }
    streams.remove(id);
}

/// Waits for the first chunk of response body `body`, so that a response whose body fails
/// before any byte is ready becomes an HTTP error instead of a cut 200.
async fn with_first_chunk(body: axum::body::Body) -> Result<axum::body::Body> {
    use futures_util::StreamExt;
    let mut data = body.into_data_stream();
    match data.next().await {
        None => Ok(axum::body::Body::empty()),
        Some(Err(e)) => Err(Error::internal(format!("the response body failed: {e}"))),
        Some(Ok(first)) => {
            let first = futures_util::stream::once(async move { Ok::<_, axum::Error>(first) });
            Ok(axum::body::Body::from_stream(first.chain(data)))
        }
    }
}

/// Runs one request with head `head` and body `body` through `router` on the worker's node.
async fn route_stream(
    node: &Arc<crate::node::Node>,
    router: axum::Router,
    head: boundary::RequestHead,
    body: axum::body::Body,
) -> Result<axum::response::Response> {
    use tower::ServiceExt;
    let context: HttpContext =
        serde_json::from_slice(&head.context).map_err(|e| Error::internal(format!("worker request context: {e}")))?;
    let mut req = http::Request::builder()
        .method(head.method.as_str())
        .uri(head.uri)
        .body(body)
        .map_err(|e| Error::internal(format!("worker request: {e}")))?;
    boundary::append_headers(req.headers_mut(), head.headers);
    if let Some((principal, access)) = context.caller {
        let db = node.open_db(&context.rec).await?;
        let deadline =
            crate::node::Deadline::new(tokio::time::Instant::now() + Duration::from_millis(head.deadline_ms));
        let ctx = crate::api::DbCtx { principal: principal.clone(), rec: context.rec, access, db, deadline };
        req.extensions_mut().insert(principal);
        req.extensions_mut().insert(ctx);
    }
    router.oneshot(req).await.map_err(|e| Error::internal(format!("worker router: {e}")))
}

/// Runs table query `query` on table `table` at `snapshot`.
async fn run_table_query(
    shared: &Arc<Shared>,
    snapshot: crate::engine::Snapshot,
    table: &str,
    query: serde_json::Value,
) -> Result<Bytes> {
    let version = snapshot.version.m.version;
    let cfg = &shared.cfg;
    let mut txn = crate::engine::Txn::new(snapshot, crate::engine::Isolation::Serializable, 0, cfg.txn_max_write_bytes)
        .with_read_limit(cfg.txn_max_read_bytes);
    let q: crate::table::Query =
        serde_json::from_value(query).map_err(|e| Error::invalid(format!("invalid query: {e}")))?;
    let r = crate::table::query(&mut txn, table, &q).await?;
    let body = json!({"rows": r.rows, "next_cursor": r.next_cursor, "version": version});
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
