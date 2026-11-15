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
    /// Serves a database request on the worker's node. The payload is the request body, and the
    /// result's payload is the response body.
    Http {
        method: String,
        uri: String,
        headers: Vec<(String, Vec<u8>)>,
        principal: crate::node::Principal,
        rec: crate::meta::DbRecord,
        access: crate::meta::Access,
        deadline_ms: u64,
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
    /// The JSON response body of a query or a read-only transaction is the result's payload.
    Queried,
    /// The body's idempotency key and, on success, the encoded [`Bind`] as the payload.
    Staged {
        body_key: Option<String>,
        error: Option<serde_json::Value>,
    },
    /// The response status and headers of an [`Job::Http`]; the response body is the payload.
    Http {
        status: u16,
        headers: Vec<(String, Vec<u8>)>,
    },
    /// The job's error as the JSON body of an HTTP error response.
    Failed(serde_json::Value),
}

/// A frame header that the node sends to a worker.
#[derive(Serialize, Deserialize)]
enum ToWorker {
    /// A job to run; the frame's ID names the job.
    Job(Box<Job>),
    /// The outcome of the worker's commit call with the frame's ID, or its error as a JSON error body.
    Committed(std::result::Result<crate::engine::Committed, serde_json::Value>),
    /// The outcome of the worker's bind call with the frame's ID: one result for each bind slot.
    Bound(BindOutcome),
}

/// The JSON form of a bind's outcome: one result for each slot, or the error of the whole bind.
type BindOutcome =
    std::result::Result<Vec<std::result::Result<crate::engine::Committed, serde_json::Value>>, serde_json::Value>;

/// A frame header that a worker sends to the node.
#[derive(Serialize, Deserialize)]
enum FromWorker {
    /// The result of the job with the frame's ID.
    Done(JobResult),
    /// A call to commit `request` to database `db` as the lease holder; the frame's ID names the call.
    Commit { db: String, request: Box<crate::engine::CommitRequest> },
    /// A call to bind the encoded [`Bind`] to database `db` as the lease holder; the frame's ID names the call.
    Bind { db: String, bind: Vec<u8> },
}

/// Binds a worker's staged event parts for owner `owner` to database `db` in the node process.
pub type WorkerBinder = Box<
    dyn Fn(
            String,
            String,
            Bind,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<Result<crate::engine::Committed>>>> + Send>>
        + Send
        + Sync,
>;

/// Commits a worker's write set for owner `owner` to database `db` in the node process.
pub type WorkerCommitter = Box<
    dyn Fn(
            String,
            String,
            crate::engine::CommitRequest,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::engine::Committed>> + Send>>
        + Send
        + Sync,
>;

/// Where the node runs a database request.
pub enum Placement {
    /// In the node process: the request is on the light list.
    Node(axum::extract::Request),
    /// In the owner's worker: every request that is not on the light list.
    Worker(axum::extract::Request),
}

/// The largest one-shot transaction body that the node process runs itself.
const LIGHT_TXN_BODY_MAX: u64 = 64 * 1024;

/// The most ops in a one-shot transaction that the node process runs itself.
const LIGHT_TXN_OPS_MAX: usize = 16;

/// Decides where database request `req` runs. The node process keeps only requests whose cost
/// has a fixed bound; every other request, including routes that this list does not name, runs
/// in the owner's worker.
///
/// The light list:
/// - `GET` and `HEAD` of one KV key (a blob value streams one chunk at a time);
/// - `PUT` and `DELETE` of one KV key, and blob staging (the body streams into chunks);
/// - a one-shot transaction with a body of at most 64 KiB and at most 16 `get`, `put`, or
///   `delete` ops.
pub async fn light_request(node: &crate::node::Node, req: axum::extract::Request) -> Result<Placement> {
    use http::Method;
    let path = req.uri().path();
    let Some(rest) = path.strip_prefix("/v1/dbs/") else { return Ok(Placement::Worker(req)) };
    let (_, route) = rest.split_once('/').unwrap_or((rest, ""));
    let one_key = route.strip_prefix("kv/").is_some_and(|k| !k.is_empty());
    let light = match (req.method().clone(), route) {
        (Method::GET | Method::HEAD | Method::PUT | Method::DELETE, _) if one_key => true,
        (Method::POST, "blobs") => true,
        (Method::POST, "txn") => return light_txn(node, req).await,
        _ => false,
    };
    Ok(if light { Placement::Node(req) } else { Placement::Worker(req) })
}

/// Places a one-shot transaction: the node reads its body only if the body is at most
/// [`LIGHT_TXN_BODY_MAX`] long.
async fn light_txn(node: &crate::node::Node, req: axum::extract::Request) -> Result<Placement> {
    let _ = node;
    let len = req.headers().get(http::header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()?.parse::<u64>().ok());
    if !len.is_some_and(|l| l <= LIGHT_TXN_BODY_MAX) {
        return Ok(Placement::Worker(req));
    }
    let (parts, body) = req.into_parts();
    let bytes = axum::body::to_bytes(body, LIGHT_TXN_BODY_MAX as usize)
        .await
        .map_err(|e| Error::invalid(format!("cannot read the request body: {e}")))?;
    let light = serde_json::from_slice::<baserunna_api::TxnRequest>(&bytes).is_ok_and(|t| {
        t.ops.len() <= LIGHT_TXN_OPS_MAX
            && crate::ops::parse_ops(t.ops).is_ok_and(|ops| {
                ops.iter().all(|o| {
                    matches!(o, crate::ops::Op::Get { .. } | crate::ops::Op::Put { .. } | crate::ops::Op::Delete { .. })
                })
            })
    });
    let req = axum::extract::Request::from_parts(parts, axum::body::Body::from(bytes));
    Ok(if light { Placement::Node(req) } else { Placement::Worker(req) })
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
    /// The CPU weight of the node process, which runs the light requests of all owners.
    pub main_cpu_weight: u32,
}

/// Starts workers in per-owner cgroups and runs jobs in them.
pub struct WorkerPool {
    cfg: PoolConfig,
    db_owners: Mutex<HashMap<String, String>>,
    owner_resolver: std::sync::OnceLock<OwnerResolver>,
    committer: Arc<std::sync::OnceLock<WorkerCommitter>>,
    binder: Arc<std::sync::OnceLock<WorkerBinder>>,
    owner_workers: Mutex<HashMap<String, Arc<OwnerWorker>>>,
    next_job_id: std::sync::atomic::AtomicU64,
}

impl WorkerPool {
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
            committer: Default::default(),
            binder: Default::default(),
            owner_workers: Mutex::new(HashMap::new()),
            next_job_id: 0.into(),
        })
    }

    /// Records that database `db` belongs to owner `owner`.
    pub fn set_db_owner(&self, db: &str, owner: &str) {
        let mut g = self.db_owners.lock();
        if g.get(db).map(String::as_str) != Some(owner) {
            g.insert(db.to_string(), owner.to_string());
        }
    }

    /// Sets the function that commits workers' write sets in the node process.
    pub fn set_committer(&self, f: WorkerCommitter) {
        let _ = self.committer.set(f);
    }

    /// Sets the function that binds workers' staged event parts in the node process.
    pub fn set_binder(&self, f: WorkerBinder) {
        let _ = self.binder.set(f);
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

    /// Serves database request `req` with context `ctx` on the worker of the database's owner.
    pub async fn http(&self, ctx: crate::api::DbCtx, req: axum::extract::Request) -> Result<axum::response::Response> {
        let (parts, body) = req.into_parts();
        let body = axum::body::to_bytes(body, usize::MAX)
            .await
            .map_err(|e| Error::invalid(format!("cannot read the request body: {e}")))?;
        let left = ctx.deadline.instant().saturating_duration_since(tokio::time::Instant::now());
        let headers =
            parts.headers.iter().map(|(k, v)| (k.as_str().to_string(), v.as_bytes().to_vec())).collect::<Vec<_>>();
        let job = Job::Http {
            method: parts.method.to_string(),
            uri: parts.uri.to_string(),
            headers,
            principal: ctx.principal,
            rec: ctx.rec.clone(),
            access: ctx.access,
            deadline_ms: left.as_millis() as u64,
        };
        match self.run(&ctx.rec.owner_id, job, body, left + Duration::from_secs(1)).await? {
            (JobResult::Http { status, headers }, body) => {
                let mut resp = axum::response::Response::new(axum::body::Body::from(body));
                *resp.status_mut() =
                    http::StatusCode::from_u16(status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
                for (k, v) in headers {
                    if let (Ok(k), Ok(v)) = (http::HeaderName::try_from(k), http::HeaderValue::from_bytes(&v)) {
                        resp.headers_mut().append(k, v);
                    }
                }
                Ok(resp)
            }
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
            if !exited && worker.frames.send((job_id, header.clone(), payload.clone())).is_ok() {
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
            if !pending.is_empty() || w.last_used.lock().elapsed() < idle {
                return true;
            }
            w.exited.store(true, std::sync::atomic::Ordering::Release);
            let _ = std::fs::write(w.leaf.join("cgroup.kill"), "1");
            false
        });
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
        let worker = self.start_worker(owner).map_err(|e| Error::internal(format!("worker: {e}")))?;
        workers.insert(owner.to_string(), worker.clone());
        Ok(worker)
    }

    fn start_worker(&self, owner: &str) -> std::io::Result<Arc<OwnerWorker>> {
        let owner_dir = self.owner_cgroup(owner)?;
        let leaf = owner_dir.join("worker");
        if !leaf.exists() {
            std::fs::create_dir(&leaf)?;
        }
        let mut child = tokio::process::Command::new(&self.cfg.exe)
            .arg("worker")
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
        let mut stdin = child.stdin.take().expect("piped stdin");
        let mut stdout = child.stdout.take().expect("piped stdout");
        let (frames, mut to_send) = tokio::sync::mpsc::unbounded_channel::<(u64, Bytes, Bytes)>();
        let worker = Arc::new(OwnerWorker {
            frames,
            pending: Mutex::new(HashMap::new()),
            leaf,
            exited: false.into(),
            last_used: Mutex::new(std::time::Instant::now()),
        });
        tokio::spawn(async move {
            while let Some((job_id, header, payload)) = to_send.recv().await {
                if write_job_frame(&mut stdin, job_id, &header, &payload).await.is_err() {
                    break;
                }
            }
        });
        let reader = worker.clone();
        let committer = self.committer.clone();
        let binder = self.binder.clone();
        let owner = owner.to_string();
        let limit_mib = self.cfg.owner_memory_bytes >> 20;
        let ooms_before = oom_kills(&owner_dir);
        tokio::spawn(async move {
            while let Ok((job_id, header, payload)) = read_job_frame(&mut stdout).await {
                let result = match serde_json::from_slice::<FromWorker>(&header) {
                    Ok(FromWorker::Done(h)) => Ok((h, payload)),
                    Ok(FromWorker::Commit { db, request }) => {
                        let (committer, frames, owner) = (committer.clone(), reader.frames.clone(), owner.clone());
                        tokio::spawn(async move {
                            let committed = match committer.get() {
                                Some(commit) => commit(owner, db, *request).await,
                                None => Err(Error::internal("the worker pool has no committer")),
                            };
                            let reply = ToWorker::Committed(
                                committed.map_err(|e| json!({ "error": crate::api::error_json(&e) })),
                            );
                            let header = serde_json::to_vec(&reply).unwrap_or_default();
                            let _ = frames.send((job_id, header.into(), Bytes::new()));
                        });
                        continue;
                    }
                    Ok(FromWorker::Bind { db, bind }) => {
                        let (binder, frames, owner) = (binder.clone(), reader.frames.clone(), owner.clone());
                        tokio::spawn(async move {
                            let bound = match (binder.get(), Bind::decode(&bind)) {
                                (Some(bind_parts), Ok(bind)) => bind_parts(owner, db, bind).await,
                                (None, _) => Err(Error::internal("the worker pool has no binder")),
                                (_, Err(e)) => Err(Error::internal(format!("worker bind: {e}"))),
                            };
                            let err = |e: Error| json!({ "error": crate::api::error_json(&e) });
                            let reply = ToWorker::Bound(
                                bound.map(|rs| rs.into_iter().map(|r| r.map_err(err)).collect()).map_err(err),
                            );
                            let header = serde_json::to_vec(&reply).unwrap_or_default();
                            let _ = frames.send((job_id, header.into(), Bytes::new()));
                        });
                        continue;
                    }
                    Err(e) => Err(Error::internal(format!("worker result: {e}"))),
                };
                *reader.last_used.lock() = std::time::Instant::now();
                if let Some(done) = reader.pending.lock().remove(&job_id) {
                    let _ = done.send(result);
                }
            }
            reader.exited.store(true, std::sync::atomic::Ordering::Release);
            let status = child.wait().await;
            let oom = oom_kills(&owner_dir) > ooms_before;
            for (_, done) in reader.pending.lock().drain() {
                let _ = done.send(Err(if oom {
                    Error::TooLarge(format!("the work exceeded the owner's memory limit of {limit_mib} MiB"))
                } else {
                    Error::internal(format!("worker exited with {status:?}"))
                }));
            }
        });
        Ok(worker)
    }
}

/// Delivers the node's answer to a worker's commit call.
type CommitDone = tokio::sync::oneshot::Sender<Result<crate::engine::Committed>>;

/// Delivers a job's result and payload to the request that waits for it.
type JobDone = tokio::sync::oneshot::Sender<Result<(JobResult, Bytes)>>;

/// One owner's worker process, which runs the owner's jobs concurrently.
struct OwnerWorker {
    /// Job frames for the task that writes the worker's stdin.
    frames: tokio::sync::mpsc::UnboundedSender<(u64, Bytes, Bytes)>,
    /// Jobs sent to the worker that wait for their results, by job ID.
    pending: Mutex<HashMap<u64, JobDone>>,
    /// The worker's cgroup, below the owner's cgroup.
    leaf: PathBuf,
    exited: std::sync::atomic::AtomicBool,
    /// When the last job of this worker started or ended.
    last_used: Mutex<std::time::Instant>,
}

async fn write_job_frame(
    w: &mut (impl tokio::io::AsyncWrite + Unpin),
    job_id: u64,
    header: &[u8],
    payload: &[u8],
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    w.write_all(&job_id.to_le_bytes()).await?;
    w.write_all(&(header.len() as u64).to_le_bytes()).await?;
    w.write_all(header).await?;
    w.write_all(&(payload.len() as u64).to_le_bytes()).await?;
    w.write_all(payload).await?;
    w.flush().await
}

async fn read_job_frame(r: &mut (impl tokio::io::AsyncRead + Unpin)) -> std::io::Result<(u64, Bytes, Bytes)> {
    use tokio::io::AsyncReadExt;
    let job_id = r.read_u64_le().await?;
    let mut header = vec![0u8; r.read_u64_le().await? as usize];
    r.read_exact(&mut header).await?;
    let mut payload = vec![0u8; r.read_u64_le().await? as usize];
    r.read_exact(&mut payload).await?;
    Ok((job_id, header.into(), payload.into()))
}

/// Returns the cgroup v2 directory of this process.
fn own_cgroup() -> Option<PathBuf> {
    let line = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let rel = line.lines().find_map(|l| l.strip_prefix("0::"))?;
    Some(PathBuf::from("/sys/fs/cgroup").join(rel.trim_start_matches('/')))
}

fn oom_kills(cgroup: &std::path::Path) -> u64 {
    let events = std::fs::read_to_string(cgroup.join("memory.events")).unwrap_or_default();
    events.lines().find_map(|l| l.strip_prefix("oom_kill ")).and_then(|n| n.trim().parse().ok()).unwrap_or(0)
}

/// Runs the jobs that the node sends on stdin and writes each result to stdout (the `baserunna worker` command).
pub async fn serve(node: Arc<crate::node::Node>) -> std::io::Result<()> {
    let engine = node.engine.clone();
    let router = crate::api::db_routes().with_state(node.clone());
    let (results, mut to_write) = tokio::sync::mpsc::unbounded_channel::<(u64, Bytes, Bytes)>();
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some((job_id, header, payload)) = to_write.recv().await {
            write_job_frame(&mut stdout, job_id, &header, &payload).await?;
        }
        Ok::<_, std::io::Error>(())
    });
    let commit_calls: Arc<Mutex<HashMap<u64, CommitDone>>> = Default::default();
    let next_call_id = Arc::new(std::sync::atomic::AtomicU64::new(0));
    {
        let (results, calls) = (results.clone(), commit_calls.clone());
        let remote: crate::engine::RemoteCommit = Box::new(move |db, request| {
            let call_id = next_call_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let (done, committed) = tokio::sync::oneshot::channel();
            calls.lock().insert(call_id, done);
            let header = serde_json::to_vec(&FromWorker::Commit { db, request: Box::new(request) });
            let sent =
                header.map_err(|e| Error::internal(format!("cannot encode the commit: {e}"))).and_then(
                    |h| match results.send((call_id, h.into(), Bytes::new())) {
                        Ok(()) => Ok(()),
                        Err(_) => Err(Error::internal("the worker cannot reach the node")),
                    },
                );
            Box::pin(async move {
                sent?;
                committed.await.map_err(|_| Error::internal("the node dropped the commit"))?
            })
        });
        let _ = engine.shared.remote_commit.set(remote);
    }
    let bind_calls: Arc<Mutex<HashMap<u64, tokio::sync::oneshot::Sender<BindOutcome>>>> = Default::default();
    {
        let (results, calls) = (results.clone(), bind_calls.clone());
        let next_bind_id = std::sync::atomic::AtomicU64::new(u64::MAX / 2);
        let remote: crate::engine::RemoteBind = Box::new(move |db, bind| {
            let call_id = next_bind_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let (done, bound) = tokio::sync::oneshot::channel();
            calls.lock().insert(call_id, done);
            let header = serde_json::to_vec(&FromWorker::Bind { db, bind: bind.encode().to_vec() });
            let sent = header.map_err(|e| Error::internal(format!("cannot encode the bind: {e}"))).and_then(|h| {
                results
                    .send((call_id, h.into(), Bytes::new()))
                    .map_err(|_| Error::internal("the worker cannot reach the node"))
            });
            Box::pin(async move {
                sent?;
                let from_json = |e: serde_json::Value| crate::api::error_from_json(500, &e);
                let bound = bound.await.map_err(|_| Error::internal("the node dropped the bind"))?;
                bound.map(|rs| rs.into_iter().map(|r| r.map_err(from_json)).collect()).map_err(from_json)
            })
        });
        let _ = engine.shared.remote_bind.set(remote);
    }
    let mut stdin = tokio::io::stdin();
    while let Ok((job_id, header, payload)) = read_job_frame(&mut stdin).await {
        let job = match serde_json::from_slice::<ToWorker>(&header) {
            Ok(ToWorker::Committed(r)) => {
                if let Some(done) = commit_calls.lock().remove(&job_id) {
                    let _ = done.send(r.map_err(|e| crate::api::error_from_json(500, &e)));
                }
                continue;
            }
            Ok(ToWorker::Bound(r)) => {
                if let Some(done) = bind_calls.lock().remove(&job_id) {
                    let _ = done.send(r);
                }
                continue;
            }
            Ok(ToWorker::Job(job)) => Ok(*job),
            Err(e) => Err(e),
        };
        let (engine, node, router) = (engine.clone(), node.clone(), router.clone());
        let results = results.clone();
        tokio::spawn(async move {
            let (result, out) = match job {
                Ok(Job::Http { method, uri, headers, principal, rec, access, deadline_ms }) => {
                    let ctx = HttpJob { method, uri, headers, principal, rec, access, deadline_ms };
                    match run_http(&node, router, ctx, payload).await {
                        Ok((status, headers, body)) => (JobResult::Http { status, headers }, body),
                        Err(e) => (JobResult::Failed(json!({ "error": crate::api::error_json(&e) })), Bytes::new()),
                    }
                }
                Ok(job) => run_job(&engine, job, payload).await,
                Err(e) => (
                    JobResult::Failed(json!({ "error": crate::api::error_json(&Error::internal(e.to_string())) })),
                    Bytes::new(),
                ),
            };
            let header = serde_json::to_vec(&FromWorker::Done(result)).unwrap_or_default();
            let _ = results.send((job_id, header.into(), out));
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
        Job::Http { .. } => failed(Error::internal("the worker serves requests outside run_job")),
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
struct HttpJob {
    method: String,
    uri: String,
    headers: Vec<(String, Vec<u8>)>,
    principal: crate::node::Principal,
    rec: crate::meta::DbRecord,
    access: crate::meta::Access,
    deadline_ms: u64,
}

/// Serves one database request on the worker's node with the database routes of the node process.
async fn run_http(
    node: &Arc<crate::node::Node>,
    router: axum::Router,
    job: HttpJob,
    body: Bytes,
) -> Result<(u16, Vec<(String, Vec<u8>)>, Bytes)> {
    use tower::ServiceExt;
    let db = node.open_db(&job.rec).await?;
    let deadline = crate::node::Deadline::new(tokio::time::Instant::now() + Duration::from_millis(job.deadline_ms));
    let ctx = crate::api::DbCtx { principal: job.principal.clone(), rec: job.rec, access: job.access, db, deadline };
    let mut req = http::Request::builder()
        .method(job.method.as_str())
        .uri(job.uri)
        .body(axum::body::Body::from(body))
        .map_err(|e| Error::internal(format!("worker request: {e}")))?;
    for (k, v) in job.headers {
        if let (Ok(k), Ok(v)) = (http::HeaderName::try_from(k), http::HeaderValue::from_bytes(&v)) {
            req.headers_mut().append(k, v);
        }
    }
    req.extensions_mut().insert(job.principal);
    req.extensions_mut().insert(ctx);
    let resp = router.oneshot(req).await.map_err(|e| Error::internal(format!("worker router: {e}")))?;
    let (parts, body) = resp.into_parts();
    let body = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|e| Error::internal(format!("worker response body: {e}")))?;
    let headers = parts.headers.iter().map(|(k, v)| (k.as_str().to_string(), v.as_bytes().to_vec())).collect();
    Ok((parts.status.as_u16(), headers, body))
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
