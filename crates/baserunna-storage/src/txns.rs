//! Per-node registry for open interactive transactions (SPEC §5.12, §6.2).
//!
//! Enforces per-user transaction limits, idle and lifetime timeouts, and fair memory admission
//! across transactions and event queries.

use crate::engine::memory::{self, Held, MemoryGate};
use crate::engine::{Engine, Txn, Version};
use crate::queries::{Queries, QueryMemory};
use baserunna_api::Encoding;
use baserunna_util::error::{Error, Result};
use baserunna_util::{DetHashMap, TtlCache};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug)]
pub struct TxnLimits {
    pub per_user: usize,
    pub idle: Duration,
    pub lifetime: Duration,
}

pub struct OpenTxn {
    pub id: String,
    pub user_id: String,
    pub db_id: String,
    pub encoding: Encoding,
    created: Instant,
    last_used: Mutex<Instant>,
    memory: Arc<AtomicUsize>,
    /// Weak reference to the snapshot version.
    ///
    /// Attributes pinned version memory without locking `txn`.
    snapshot: Weak<Version>,
    /// Holds the open transaction until commit or abort sets it to `None`.
    txn: Arc<tokio::sync::Mutex<Option<Txn>>>,
}

impl OpenTxn {
    fn last_used(&self) -> Instant {
        *self.last_used.lock()
    }

    fn touch(&self) {
        *self.last_used.lock() = Instant::now();
    }
}

pub struct OpenTxns {
    engine: Arc<Engine>,
    limits: TxnLimits,
    queries: Arc<Queries>,
    open: Mutex<DetHashMap<String, Arc<OpenTxn>>>,
    aborted: TtlCache<String, &'static str>,
    /// Maps each committing transaction ID to its commit ID.
    started_commits: TtlCache<String, u64>,
}

impl OpenTxns {
    pub fn new(engine: Arc<Engine>, limits: TxnLimits, queries: Arc<Queries>) -> Arc<OpenTxns> {
        Arc::new(OpenTxns {
            engine,
            limits,
            queries,
            open: Mutex::new(DetHashMap::default()),
            aborted: TtlCache::new(limits.lifetime, 100_000),
            started_commits: TtlCache::new(limits.lifetime, 100_000),
        })
    }

    /// Admits an event query for `user_id`.
    ///
    /// Waits up to 5 s for a query slot.
    pub async fn start_query(self: &Arc<Self>, user_id: &str) -> Result<crate::queries::QueryGrant> {
        self.queries.start(user_id, self).await
    }

    pub fn len(&self) -> usize {
        self.open.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn count_for_db(&self, db_id: &str) -> usize {
        self.open.lock().values().filter(|t| t.db_id == db_id).count()
    }

    /// Enforces the node memory budget for `user_id`'s transaction on `db_id`.
    ///
    /// Unloads idle databases and evicts idle transactions or running queries with
    /// [`memory::fair_admission`] if memory exceeds the budget.
    pub fn admit(&self, user_id: &str, db_id: &str) -> Result<()> {
        if self.engine.shared.is_system(db_id) {
            return Ok(());
        }
        if self.fair_admission(user_id) {
            return Ok(());
        }
        baserunna_util::probe::hit(baserunna_util::probe::Probe::MemoryExhausted);
        Err(Error::Unavailable {
            message: "this user's transactions and queries hold their fair share of the node memory budget".into(),
            retry_after_ms: Some(1_000),
            outcome_unknown: false,
        })
    }

    /// Enforces the node memory budget for query `q`.
    ///
    /// Aborts `q` if its user already holds at least a fair share of memory. Always admits the
    /// oldest running query so at least 1 query finishes even if databases fill the budget.
    pub(crate) fn admit_query(&self, q: &QueryMemory) -> Result<()> {
        if self.fair_admission(&q.user_id) || self.queries.active_queries().last().is_some_and(|o| o.seq == q.seq) {
            return Ok(());
        }
        Err(q.fail(Error::Unavailable {
            message: "this user's queries and transactions hold their fair share of the node memory budget; retry"
                .into(),
            retry_after_ms: Some(1_000),
            outcome_unknown: false,
        }))
    }

    /// Reclaims memory so `user_id` can fit within the node memory budget.
    ///
    /// Returns `false` if memory stays over budget and `user_id` already holds at least its fair
    /// share. Otherwise evicts the holders that [`memory::fair_admission`] selects and returns
    /// `true`.
    fn fair_admission(&self, user_id: &str) -> bool {
        let shared = &self.engine.shared;
        if shared.reclaim_to_budget() {
            return true;
        }
        let used = shared.memory.used().saturating_sub(self.queries.releasing_bytes());
        if used <= shared.memory.limit() {
            return true;
        }
        let (holders, held): (Vec<_>, Vec<_>) = self.held_memory().into_iter().unzip();
        let Some(victims) = memory::fair_admission(shared.memory.limit(), used, &user_id.to_string(), &held) else {
            return false;
        };
        for i in victims {
            match &holders[i] {
                MemoryHolder::Txn(t) => {
                    if self.abort(t, "it was aborted to free node memory for other users") {
                        baserunna_util::probe::hit(baserunna_util::probe::Probe::TxnEvicted);
                    }
                }
                MemoryHolder::Query(q) => q.evict(),
            }
        }
        true
    }

    pub fn admit_begin(&self, user_id: &str, db_id: &str) -> Result<()> {
        self.check_per_user(&self.open.lock(), user_id)?;
        self.admit(user_id, db_id)
    }

    fn check_per_user(&self, open: &DetHashMap<String, Arc<OpenTxn>>, user_id: &str) -> Result<()> {
        if open.values().filter(|t| t.user_id == user_id).count() < self.limits.per_user {
            return Ok(());
        }
        Err(Error::RateLimited {
            message: format!("maximum of {} open interactive transactions per user exceeded", self.limits.per_user),
            retry_after_ms: 1_000,
        })
    }

    /// Registers `txn` as open transaction `id` for `user_id` on `db_id`.
    ///
    /// Returns a locked [`TxnGuard`] for the initial request.
    pub fn open(
        self: &Arc<Self>,
        id: String,
        user_id: &str,
        db_id: &str,
        mut txn: Txn,
        encoding: Encoding,
    ) -> Result<TxnGuard<'_>> {
        let gate = Gate { txns: Arc::downgrade(self), user_id: user_id.to_string(), db_id: db_id.to_string() };
        txn.set_memory_gate(Some(Arc::new(gate)));
        let now = Instant::now();
        let open = Arc::new(OpenTxn {
            id: id.clone(),
            user_id: user_id.to_string(),
            db_id: db_id.to_string(),
            encoding,
            created: now,
            last_used: Mutex::new(now),
            memory: txn.meter(),
            snapshot: Arc::downgrade(txn.snapshot()),
            txn: Arc::new(tokio::sync::Mutex::new(Some(txn))),
        });
        let guard = open.txn.clone().try_lock_owned().map_err(|_| Error::internal("a new transaction is in use"))?;
        let mut g = self.open.lock();
        self.check_per_user(&g, user_id)?;
        g.insert(id, open.clone());
        Ok(TxnGuard { txns: self, open, guard, finished: false })
    }

    pub async fn lock(&self, open: Arc<OpenTxn>) -> Result<TxnGuard<'_>> {
        let guard = open.txn.clone().lock_owned().await;
        if guard.is_none() {
            return Err(self.lost(&open.id));
        }
        open.touch();
        Ok(TxnGuard { txns: self, open, guard, finished: false })
    }

    pub fn get(&self, id: &str, user_id: &str, db_id: &str) -> Result<Arc<OpenTxn>> {
        if baserunna_util::buggify::fault(0.01) {
            self.remove(id);
        }
        let open = self.open.lock().get(id).cloned();
        let open = open.ok_or_else(|| self.lost(id))?;
        if open.user_id != user_id || open.db_id != db_id {
            return Err(Error::txn_not_found());
        }
        if open.created.elapsed() > self.limits.lifetime {
            self.abort(&open, "it reached the maximum transaction lifetime");
            return Err(self.lost(id));
        }
        Ok(open)
    }

    /// Removes the started-commit record for `txn_id` after its commit fails without taking effect.
    pub fn forget_started_commit(&self, txn_id: &str) {
        self.started_commits.remove(&txn_id.to_string());
    }

    fn lost(&self, id: &str) -> Error {
        baserunna_util::probe::hit(baserunna_util::probe::Probe::InteractiveTxnLost);
        if let Some(commit_id) = self.started_commits.get(&id.to_string()) {
            return Error::Unavailable {
                message: format!(
                    "the transaction's commit was already sent and may have been applied. Check its status at \
                     /commits/{commit_id:016x}"
                ),
                retry_after_ms: None,
                outcome_unknown: true,
            };
        }
        match self.aborted.get(&id.to_string()) {
            Some(reason) => Error::TxnNotFound(format!("transaction was aborted because {reason}; retry it")),
            None => Error::txn_not_found(),
        }
    }

    fn remove(&self, id: &str) {
        self.open.lock().remove(id);
    }

    fn abort(&self, open: &OpenTxn, reason: &'static str) -> bool {
        let Ok(mut g) = open.txn.try_lock() else { return false };
        self.abort_locked(open, &mut g, reason)
    }

    /// Aborts `txn` under its lock.
    ///
    /// Records `reason` before unlocking so concurrent waiters see the abort reason.
    fn abort_locked(&self, open: &OpenTxn, txn: &mut Option<Txn>, reason: &'static str) -> bool {
        let aborted = txn.take().is_some();
        if aborted {
            self.aborted.store(open.id.clone(), Some(reason), Instant::now());
        }
        self.remove(&open.id);
        aborted
    }

    pub fn expire(&self) {
        let TxnLimits { idle, lifetime, .. } = self.limits;
        let expired: Vec<_> = self
            .open
            .lock()
            .values()
            .filter_map(|t| {
                let reason = if t.created.elapsed() >= lifetime {
                    "it reached the maximum transaction lifetime"
                } else if t.last_used().elapsed() >= idle {
                    "it exceeded the idle timeout"
                } else {
                    return None;
                };
                Some((t.clone(), reason))
            })
            .collect();
        self.aborted.prune();
        self.started_commits.prune();
        for (t, reason) in expired {
            self.abort(&t, reason);
        }
    }

    /// Lists all memory holders with their current memory usage.
    ///
    /// Orders open transactions from least to most recently used, followed by running queries from
    /// newest to oldest.
    fn held_memory(&self) -> Vec<(MemoryHolder, Held<String>)> {
        let idle_since = |t: &OpenTxn| t.txn.try_lock().ok().filter(|g| g.is_some()).map(|_| t.last_used());
        let mut open: Vec<(Option<Instant>, Arc<OpenTxn>)> =
            self.open.lock().values().map(|t| (idle_since(t), t.clone())).collect();
        open.sort_by(|a, b| (a.0.is_none(), a.0, &a.1.id).cmp(&(b.0.is_none(), b.0, &b.1.id)));
        let pinned: Vec<Option<Arc<Version>>> = open
            .iter()
            .map(|(_, t)| {
                let current = self.engine.db_if_open(&t.db_id).and_then(|d| d.current());
                t.snapshot.upgrade().filter(|v| !current.is_some_and(|c| Arc::ptr_eq(&c, v)))
            })
            .collect();
        let mut readers: DetHashMap<*const Version, usize> = DetHashMap::default();
        for v in pinned.iter().flatten() {
            *readers.entry(Arc::as_ptr(v)).or_default() += 1;
        }
        open.into_iter()
            .zip(&pinned)
            .map(|((idle, t), v)| {
                let held = Held {
                    holder: t.user_id.clone(),
                    bytes: t.memory.load(Relaxed)
                        + v.as_ref().map_or(0, |v| v.memory_bytes() / readers[&Arc::as_ptr(v)]),
                    evictable: idle.is_some(),
                };
                (MemoryHolder::Txn(t), held)
            })
            .chain(self.queries.active_queries().into_iter().map(|q| {
                let held = Held { holder: q.user_id.clone(), bytes: q.charged_bytes(), evictable: true };
                (MemoryHolder::Query(q), held)
            }))
            .collect()
    }
}

/// An open transaction or running query that holds node memory.
enum MemoryHolder {
    Txn(Arc<OpenTxn>),
    Query(Arc<QueryMemory>),
}

/// Exclusive guard for an open transaction during a single request.
///
/// Dropping the guard before calling [`TxnGuard::finish`] or [`TxnGuard::take`] aborts the
/// transaction.
pub struct TxnGuard<'a> {
    txns: &'a OpenTxns,
    open: Arc<OpenTxn>,
    guard: tokio::sync::OwnedMutexGuard<Option<Txn>>,
    finished: bool,
}

impl TxnGuard<'_> {
    pub fn txn(&mut self) -> &mut Txn {
        self.guard.as_mut().expect("open transaction must be present while guard is active")
    }

    pub fn encoding(&self) -> Encoding {
        self.open.encoding
    }

    pub fn finish<T>(mut self, result: Result<T>) -> Result<T> {
        self.finished = result.is_ok();
        result
    }

    /// Takes the transaction for commit and records its commit ID in [`OpenTxns`].
    pub fn take_for_commit(self) -> Txn {
        let commit_id = self.guard.as_ref().expect("open transaction must be present while guard is active").commit_id;
        self.txns.started_commits.store(self.open.id.clone(), Some(commit_id), Instant::now());
        self.take()
    }

    pub fn take(mut self) -> Txn {
        self.finished = true;
        let mut txn = self.guard.take().expect("open transaction must be present while guard is active");
        self.txns.remove(&self.open.id);
        txn.set_memory_gate(None);
        txn
    }
}

impl Drop for TxnGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.txns.abort_locked(&self.open, &mut self.guard, "a request on it failed or was canceled");
        }
    }
}

struct Gate {
    txns: Weak<OpenTxns>,
    user_id: String,
    db_id: String,
}

impl MemoryGate for Gate {
    fn admit(&self) -> Result<()> {
        match self.txns.upgrade() {
            Some(t) => t.admit(&self.user_id, &self.db_id),
            None => Ok(()),
        }
    }

    fn step_bytes(&self) -> usize {
        self.txns.upgrade().map_or(usize::MAX, |t| (t.engine.shared.memory.limit() / 1024).max(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::tests::{engine, rt};
    use crate::engine::{Consistency, Db, EngineConfig, Isolation};

    const IDLE: Duration = Duration::from_secs(30);

    async fn open_txns() -> (Arc<OpenTxns>, Arc<Db>) {
        let (engine, _store, _env) = engine(1, EngineConfig::small_for_tests());
        let limits = TxnLimits { per_user: 10, idle: IDLE, lifetime: 10 * IDLE };
        let queries = Queries::new(1, engine.shared.memory.clone());
        let db = engine.db("a");
        db.open(true).await.unwrap();
        (OpenTxns::new(engine, limits, queries), db)
    }

    #[test]
    fn expire_aborts_transaction_after_lookup() {
        rt().block_on(async {
            let (txns, db) = open_txns().await;
            let t = db.begin(Isolation::Snapshot, Consistency::Strong).await.unwrap();
            txns.open("t".into(), "u", "a", t, Default::default()).unwrap().finish(Ok(())).unwrap();
            let held = txns.get("t", "u", "a").unwrap();
            tokio::time::sleep(IDLE).await;
            txns.expire();
            assert!(held.txn.lock().await.is_none(), "an expired transaction stayed usable");
        });
    }

    #[test]
    fn committed_transaction_is_not_reported_as_expired() {
        rt().block_on(async {
            let (txns, db) = open_txns().await;
            let t = db.begin(Isolation::Snapshot, Consistency::Strong).await.unwrap();
            drop(txns.open("t".into(), "u", "a", t, Default::default()).unwrap().take());
            tokio::time::sleep(IDLE).await;
            txns.expire();
            assert_eq!(txns.len(), 0);
            let e = txns.lost("t");
            assert!(!e.message().contains("idle"), "a retried commit would report: {e:?}");
        });
    }
}
