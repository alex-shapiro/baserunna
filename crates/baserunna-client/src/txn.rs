//! Interactive transactions.

use crate::api::{self, Endpoint as _, ErrorCode, Outcome};
use crate::result::decode_results;
use crate::{Db, Error, Op, OpResult, TransportError};

/// Interactive serializable transaction on a [`Db`], begun by the first [`Txn::run`] that has
/// operations.
///
/// After a request on the transaction fails, later calls return the same error. Dropping a begun
/// transaction without [`Txn::commit`] or [`Txn::abort`], such as when its task is cancelled,
/// aborts it in the background on the current Tokio runtime.
#[derive(Debug)]
pub struct Txn {
    db: Db,
    /// ID of the transaction once a request may have begun it on the server: the ID the client
    /// chose, then the ID the server returned.
    id: Option<String>,
    /// Commit ID once the transaction began.
    commit_id: Option<String>,
    writes: Vec<Op>,
    wrote: bool,
    failed: Option<Error>,
    /// Time of the first request, which bounds how long a commit status remains definitive.
    started: Option<tokio::time::Instant>,
}

/// Commit of a write transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed {
    pub version: u64,
    pub commit_id: String,
    /// First sequence number assigned to each `append` operation, in order.
    pub appends: Vec<u64>,
}

/// Error type of a transaction loop that can carry a client [`Error`].
pub trait TxnError: From<Error> {
    fn client_error(&self) -> Option<&Error>;
}

impl TxnError for Error {
    fn client_error(&self) -> Option<&Error> {
        Some(self)
    }
}

impl Txn {
    pub(crate) fn new(db: Db) -> Txn {
        Txn { db, id: None, commit_id: None, writes: Vec::new(), wrote: false, failed: None, started: None }
    }

    /// Executes `ops` and returns one result per operation.
    pub async fn run(&mut self, ops: &[Op]) -> Result<Vec<OpResult>, Error> {
        if let Some(e) = &self.failed {
            return Err(e.clone());
        }
        if ops.is_empty() {
            return Ok(Vec::new());
        }
        self.wrote |= ops.iter().any(Op::is_write);
        self.started.get_or_insert_with(tokio::time::Instant::now);
        let api_ops = ops.iter().map(Op::to_api).collect();
        // The transaction counts as failed until the request completes, so that it cannot commit
        // operations of a request that its caller cancelled.
        let cancelled = TransportError::Unknown("a request on the transaction was cancelled".into());
        self.failed = Some(Error::Transport(cancelled));
        let (path, sent) = match &self.commit_id {
            None => {
                let id = self.id.insert(self.db.client().random_id()).clone();
                let e = api::BeginTxn { db: self.db.name().into() };
                let b = api::BeginRequest {
                    encoding: api::Encoding::Base64,
                    ops: api_ops,
                    txn_id: Some(id),
                    ..api::BeginRequest::default()
                };
                let r = self.db.client().send(&e, &b).await.map(|r| {
                    (self.id, self.commit_id) = (Some(r.txn_id), Some(r.commit_id));
                    r.results
                });
                (e.path(), r)
            }
            Some(_) => {
                let id = self.id.clone().expect("a begun transaction has an ID");
                let e = api::TxnOps { db: self.db.name().into(), id };
                (e.path(), self.db.client().send(&e, &api::OpsRequest { ops: api_ops }).await.map(|r| r.results))
            }
        };
        self.failed = sent.as_ref().err().cloned();
        // A begin that was not applied opened nothing to abort.
        if self.commit_id.is_none() && self.failed.as_ref().is_some_and(|e| e.outcome() == Outcome::NotApplied) {
            self.id = None;
        }
        let results = sent?;
        decode_results(ops, &results).map_err(|reason| Error::Malformed { path, reason })
    }

    pub async fn run_one(&mut self, op: Op) -> Result<OpResult, Error> {
        Ok(self.run(std::slice::from_ref(&op)).await?.swap_remove(0))
    }

    /// Queues `ops` to execute when the transaction commits.
    pub fn write(&mut self, ops: impl IntoIterator<Item = Op>) {
        self.writes.extend(ops);
    }

    /// Executes the queued writes and commits, which validates the reads of a read-only
    /// transaction. A transaction that never began commits its writes in one request, or returns
    /// `None` if it has none.
    ///
    /// If the commit outcome is unknown, the commit is resolved through the server: the result is
    /// the commit, an [`Error::NotApplied`], or the original error if the server cannot be
    /// reached.
    pub async fn commit(mut self) -> Result<Option<Committed>, Error> {
        if let Some(e) = self.failed.clone() {
            self.abort().await;
            return Err(e);
        }
        let writes = std::mem::take(&mut self.writes);
        let Some(commit_id) = self.commit_id.clone() else {
            return self.db.write(&writes).await;
        };
        let id = self.id.clone().expect("a begun transaction has an ID");
        let e = api::CommitTxn { db: self.db.name().into(), id };
        let body = api::CommitRequest { ops: writes.iter().map(Op::to_api).collect() };
        let err = match self.db.client().send(&e, &body).await {
            Ok(c) => {
                self.id = None;
                return Ok(Some(Committed { version: c.version, commit_id: c.commit_id, appends: c.appends }));
            }
            Err(e) if e.outcome() == Outcome::NotApplied => {
                // A commit rejected before it reached the transaction, such as by a rate limit, leaves it open.
                self.failed = Some(e.clone());
                self.abort().await;
                return Err(e);
            }
            Err(e) => e,
        };
        if !self.wrote && writes.is_empty() {
            return Err(Error::NotApplied(Box::new(err)));
        }
        let started = self.started.unwrap_or_else(tokio::time::Instant::now);
        self.db.resolve(&commit_id, started, err).await.map(Some)
    }

    /// Aborts the transaction, ignoring failures because the server aborts an idle transaction
    /// after its timeout.
    pub async fn abort(mut self) {
        if let Some(id) = self.take_open() {
            abort(&self.db, id).await;
        }
    }

    /// Returns the ID of the transaction if it may be open on the server, and forgets it.
    fn take_open(&mut self) -> Option<String> {
        let id = self.id.take()?;
        (!self.failed.as_ref().is_some_and(ends_txn)).then_some(id)
    }
}

impl Drop for Txn {
    fn drop(&mut self) {
        if let Some(id) = self.take_open()
            && let Ok(rt) = tokio::runtime::Handle::try_current()
        {
            let db = self.db.clone();
            rt.spawn(async move { abort(&db, id).await });
        }
    }
}

/// Returns true if the server discarded the transaction when it returned `e`.
fn ends_txn(e: &Error) -> bool {
    matches!(e.code(), Some(ErrorCode::TxnNotFound | ErrorCode::Conflict))
}

async fn abort(db: &Db, id: String) {
    let e = api::AbortTxn { db: db.name().into(), id };
    let _ = db.client().send(&e, &api::NoBody {}).await;
}

impl Db {
    /// Returns an interactive transaction that begins with its first operations.
    pub fn begin(&self) -> Txn {
        Txn::new(self.clone())
    }

    /// Returns the retry state of a transaction loop.
    ///
    /// ```no_run
    /// # async fn example(db: baserunna_client::Db) -> Result<(), baserunna_client::Error> {
    /// use baserunna_client::Op;
    /// let mut retry = db.retry();
    /// let visits = loop {
    ///     let mut txn = db.begin();
    ///     let r = txn.run_one(Op::increment("visits", 1)).await;
    ///     if let Some(r) = retry.finish(txn, r).await? {
    ///         break r;
    ///     }
    /// };
    /// # Ok(())
    /// # }
    /// ```
    pub fn retry(&self) -> Retry {
        Retry { db: self.clone(), attempt: 1 }
    }
}

/// Retry state of a transaction loop, which restarts a transaction up to
/// [`RetryPolicy::txn_attempts`](crate::RetryPolicy::txn_attempts) times if it conflicts or
/// fails before it could apply.
#[derive(Debug)]
pub struct Retry {
    db: Db,
    attempt: u32,
}

impl Retry {
    /// Commits `txn` if `result` is `Ok`, or aborts it otherwise.
    ///
    /// Returns the committed result, `Ok(None)` after a backoff if the transaction should run
    /// again, or the error that ends the loop.
    pub async fn finish<T, E: TxnError>(&mut self, txn: Txn, result: Result<T, E>) -> Result<Option<T>, E> {
        // Only the error of a failed request on `txn` is known to have applied nothing, whatever its
        // outcome, because the transaction never commits after it.
        let failed = txn.failed.clone();
        let err = match result {
            Ok(v) => match txn.commit().await {
                Ok(_) => return Ok(Some(v)),
                Err(e) => E::from(e),
            },
            Err(e) => {
                txn.abort().await;
                e
            }
        };
        let restart = err.client_error().is_some_and(|e| restarts(e, failed.as_ref() == Some(e)));
        let client = self.db.client();
        if !restart || self.attempt >= client.retry_policy().txn_attempts {
            return Err(err);
        }
        client.backoff(self.attempt).await;
        self.attempt += 1;
        Ok(None)
    }
}

/// Returns true if a new attempt of a transaction that failed with `e` can succeed without
/// applying its writes twice.
fn restarts(e: &Error, applied_nothing: bool) -> bool {
    if e.code() == Some(ErrorCode::TxnNotFound) || e.code() == Some(ErrorCode::Aborted) {
        return true;
    }
    e.is_transient(applied_nothing)
}
