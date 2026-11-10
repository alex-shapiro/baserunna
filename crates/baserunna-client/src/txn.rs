//! Interactive transactions.

use crate::api::{self, Endpoint as _, ErrorCode, Outcome};
use crate::result::decode_results;
use crate::{Db, Error, Op, OpResult, TransportError};

/// Interactive serializable transaction on a [`Db`].
///
/// The first non-empty [`Txn::run`] begins the transaction on the server. After a request
/// fails, subsequent calls return the same error. If an open transaction is dropped before
/// [`Txn::commit`] or [`Txn::abort`], it is aborted in the background on the current Tokio
/// runtime.
#[derive(Debug)]
pub struct Txn {
    db: Db,
    open: Option<Open>,
    writes: Vec<Op>,
    wrote: bool,
    failed: Option<Error>,
    started: Option<tokio::time::Instant>,
}

#[derive(Debug)]
struct Open {
    id: String,
    commit_id: String,
}

/// Commit of a write transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed {
    pub version: u64,
    pub commit_id: String,
    /// First sequence number assigned to each [`Op::Append`] in the transaction.
    pub appends: Vec<u64>,
}

/// Error type for a [`Retry`] loop that can wrap a client [`Error`].
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
        Txn { db, open: None, writes: Vec::new(), wrote: false, failed: None, started: None }
    }

    /// Executes `ops` in the transaction.
    pub async fn run(&mut self, ops: &[Op]) -> Result<Vec<OpResult>, Error> {
        if let Some(e) = &self.failed {
            return Err(e.clone());
        }
        if ops.is_empty() {
            return Ok(Vec::new());
        }
        let queued = self.writes.len();
        self.started.get_or_insert_with(tokio::time::Instant::now);
        let api_ops: Vec<api::Op> = self.writes.drain(..).map(Op::into_api).chain(ops.iter().map(Op::to_api)).collect();
        self.wrote |= api_ops.iter().any(api::Op::is_write);
        // Mark the transaction as failed while the request is in flight so a canceled request
        // cannot be committed.
        let canceled = TransportError::Unknown("a request on the transaction was canceled".into());
        self.failed = Some(not_applied(&Error::Transport(canceled)));
        let (path, sent) = match &self.open {
            None => {
                let b =
                    api::BeginRequest { encoding: api::Encoding::Base64, ops: api_ops, ..api::BeginRequest::default() };
                let r = begin(self.db.clone(), b).await.map(|mut g| {
                    let r = g.begun.take().expect("response is present");
                    self.open = Some(Open { id: r.txn_id, commit_id: r.commit_id });
                    r.results
                });
                (api::BeginTxn { db: self.db.name().into() }.path(), r)
            }
            Some(o) => {
                let e = api::TxnOps { db: self.db.name().into(), id: o.id.clone() };
                (e.path(), self.db.client().send(&e, &api::OpsRequest { ops: api_ops }).await.map(|r| r.results))
            }
        };
        self.failed = sent.as_ref().err().map(|e| not_applied(&e.clone().without_ops(queued)));
        let Ok(results) = sent else {
            return Err(self.failed.clone().expect("a failed request is recorded"));
        };
        let results = match results.len() == queued + ops.len() {
            true => decode_results(ops, &results[queued..]),
            false => Err(format!("{} results for {} operations", results.len(), queued + ops.len())),
        };
        results.map_err(|reason| {
            let e = not_applied(&Error::Malformed { path, reason });
            self.failed = Some(e.clone());
            e
        })
    }

    pub async fn run_one(&mut self, op: Op) -> Result<OpResult, Error> {
        Ok(self.run(std::slice::from_ref(&op)).await?.swap_remove(0))
    }

    /// Queues write operations to send with the next [`Txn::run`] or [`Txn::commit`].
    pub fn write(&mut self, ops: impl IntoIterator<Item = Op>) {
        self.writes.extend(ops);
    }

    /// Flushes queued writes and commits the transaction.
    ///
    /// Returns `None` if the transaction did not modify any data. If the commit outcome is
    /// unknown, this method queries the commit status on the server.
    pub async fn commit(mut self) -> Result<Option<Committed>, Error> {
        let r = self.try_commit().await;
        if r.is_err() {
            self.end().await;
        }
        r
    }

    /// Commits like [`Txn::commit`], leaving the transaction open for the caller to abort on error.
    async fn try_commit(&mut self) -> Result<Option<Committed>, Error> {
        if let Some(e) = self.failed.clone() {
            return Err(e);
        }
        let writes = std::mem::take(&mut self.writes);
        let Some(Open { id, commit_id }) = &self.open else {
            return self.db.write(&writes).await;
        };
        let e = api::CommitTxn { db: self.db.name().into(), id: id.clone() };
        let commit_id = commit_id.clone();
        let wrote = self.wrote || !writes.is_empty();
        let body = api::CommitRequest { ops: writes.into_iter().map(Op::into_api).collect() };
        let err = match self.db.client().send(&e, &body).await {
            Ok(c) => {
                self.open = None;
                let done = Committed { version: c.version, commit_id: c.commit_id, appends: c.appends };
                return Ok(c.committed.then_some(done));
            }
            Err(e) if e.outcome() == Outcome::NotApplied => {
                // The transaction remains open on the server if the commit was not applied.
                self.failed = Some(e.clone());
                return Err(e);
            }
            Err(e) => e,
        };
        // Spawn the abort in the background in case the commit request never reached the server.
        if !wrote {
            self.end_later();
            return Err(Error::NotApplied(Box::new(err)));
        }
        let started = self.started.unwrap_or_else(tokio::time::Instant::now);
        let resolved = self.db.resolve(&commit_id, started, err, None).await;
        match resolved {
            Ok(_) => self.open = None,
            Err(_) => self.end_later(),
        }
        resolved.map(Some)
    }

    /// Aborts the transaction on the server.
    pub async fn abort(mut self) {
        self.end().await;
    }

    /// Aborts the transaction if it is still open on the server.
    async fn end(&mut self) {
        if let Some(id) = self.open_id() {
            abort(&self.db, id).await;
        }
        self.open = None;
    }

    /// Aborts the transaction in the background on the current Tokio runtime.
    fn end_later(&mut self) {
        if let Some(id) = self.open_id() {
            abort_later(self.db.clone(), id);
        }
        self.open = None;
    }

    /// Returns the ID of the transaction if it is still open on the server.
    fn open_id(&self) -> Option<String> {
        let open = self.open.as_ref()?;
        (!self.failed.as_ref().is_some_and(ends_txn)).then(|| open.id.clone())
    }
}

impl Drop for Txn {
    fn drop(&mut self) {
        self.end_later();
    }
}

/// Guard that aborts a newly begun transaction if dropped before [`Txn::run`] takes it.
struct Begun {
    db: Db,
    begun: Option<api::Begun>,
}

impl Drop for Begun {
    fn drop(&mut self) {
        if let Some(b) = self.begun.take() {
            abort_later(self.db.clone(), b.txn_id);
        }
    }
}

/// Spawns the begin request in a separate task so [`Begun`] aborts the transaction if the
/// caller is canceled.
async fn begin(db: Db, body: api::BeginRequest) -> Result<Begun, Error> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let r = db.client().send(&api::BeginTxn { db: db.name().into() }, &body).await;
        let _ = tx.send(r.map(|b| Begun { db, begun: Some(b) }));
    });
    let stopped = || Error::Transport(TransportError::Unknown("the runtime stopped the begin request".into()));
    rx.await.unwrap_or_else(|_| Err(stopped()))
}

/// Wraps an unknown error in [`Error::NotApplied`] because a failed transaction is never
/// committed.
fn not_applied(e: &Error) -> Error {
    match e.outcome() {
        Outcome::Unknown => Error::NotApplied(Box::new(e.clone())),
        Outcome::NotApplied => e.clone(),
    }
}

/// Returns true if `e` indicates that the transaction is already closed on the server.
fn ends_txn(e: &Error) -> bool {
    matches!(e.code(), Some(ErrorCode::TxnNotFound | ErrorCode::Conflict))
}

async fn abort(db: &Db, id: String) {
    let e = api::AbortTxn { db: db.name().into(), id };
    let _ = db.client().send(&e, &api::NoBody {}).await;
}

/// Spawns a background task on the current Tokio runtime to abort transaction `id`.
fn abort_later(db: Db, id: String) {
    if let Ok(rt) = tokio::runtime::Handle::try_current() {
        rt.spawn(async move { abort(&db, id).await });
    }
}

impl Db {
    /// Creates an interactive transaction on this database.
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

/// Retry state for an interactive transaction loop.
///
/// Restarts the transaction up to [`RetryPolicy::txn_attempts`](crate::RetryPolicy::txn_attempts)
/// times on conflict or transient failure.
#[derive(Debug)]
pub struct Retry {
    db: Db,
    attempt: u32,
}

impl Retry {
    /// Commits `txn` if `result` is `Ok`, or aborts it on error.
    ///
    /// Returns `Ok(Some(value))` on commit, `Ok(None)` after backing off to retry the
    /// transaction, or `Err` on a terminal error.
    pub async fn finish<T, E: TxnError>(&mut self, mut txn: Txn, result: Result<T, E>) -> Result<Option<T>, E> {
        let err = match result {
            Ok(v) => match txn.try_commit().await {
                Ok(_) => return Ok(Some(v)),
                Err(e) => E::from(e),
            },
            Err(e) => e,
        };
        let client = self.db.client();
        match err.client_error() {
            Some(e) if e.restarts_txn() && self.attempt < client.retry_policy().txn_attempts => {
                tokio::join!(txn.end(), client.backoff(self.attempt, e));
            }
            _ => {
                txn.end().await;
                return Err(err);
            }
        }
        self.attempt += 1;
        Ok(None)
    }
}
