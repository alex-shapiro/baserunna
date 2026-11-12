//! Per-node admission of event queries: max-min fair query slots across users and query memory
//! charged to the node memory budget (SPEC §5.12, §5.13).

use crate::engine::memory::{Charge, Memory};
use crate::error::{Error, Result};
use crate::txns::OpenTxns;
use parking_lot::Mutex;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::oneshot;

/// Maximum time a new query waits for a query slot.
const SLOT_WAIT: Duration = Duration::from_secs(5);

/// Per-node registry of running event queries.
pub struct Queries {
    memory: Arc<Memory>,
    state: Mutex<SlotState>,
    running: Mutex<BTreeMap<u64, Arc<QueryMemory>>>,
}

/// Maximum number of started queries per query slot, counting queries that yielded their slot.
const STARTED_PER_SLOT: usize = 2;

struct SlotState {
    free: usize,
    held_by_user: BTreeMap<String, usize>,
    /// Waiting queries of each user, in the order that the user's queries get slots.
    waiters: BTreeMap<String, VecDeque<SlotWaiter>>,
    /// Number of queries that received a slot and have not finished.
    started: usize,
    max_started: usize,
    /// Number of the latest slot grant to each user.
    last_grant: BTreeMap<String, u64>,
    grants: u64,
    next_query_seq: u64,
}

struct SlotWaiter {
    grant: oneshot::Sender<()>,
    /// True if the query started before and yielded its slot.
    resumes: bool,
}

impl SlotState {
    fn held(&self, user_id: &str) -> usize {
        self.held_by_user.get(user_id).copied().unwrap_or(0)
    }

    /// Returns true if the first waiting query of `user_id` resumes, or if the node can start
    /// another query.
    fn can_grant(&self, user_id: &str) -> bool {
        self.waiters.get(user_id).and_then(|q| q.front()).is_some_and(|w| w.resumes || self.started < self.max_started)
    }

    /// Returns the waiting user that holds the fewest slots, and on ties the user whose latest
    /// slot grant is the oldest, among the users whose first waiting query can get a slot.
    fn next_user(&self) -> Option<String> {
        self.waiters
            .keys()
            .filter(|u| self.can_grant(u))
            .min_by_key(|u| (self.held(u), self.last_grant.get(*u).copied().unwrap_or(0)))
            .cloned()
    }

    /// Grants a slot to the first waiting query of `user_id` that still waits and can get a
    /// slot. Returns false if none does.
    fn grant_to(&mut self, user_id: &str) -> bool {
        while self.can_grant(user_id) {
            let queue = self.waiters.get_mut(user_id).expect("a user with a waiting query has a queue");
            let w = queue.pop_front().expect("a user with a waiting query has a queue entry");
            if queue.is_empty() {
                self.waiters.remove(user_id);
            }
            if w.grant.send(()).is_ok() {
                *self.held_by_user.entry(user_id.to_string()).or_default() += 1;
                self.started += !w.resumes as usize;
                self.grants += 1;
                self.last_grant.insert(user_id.to_string(), self.grants);
                return true;
            }
        }
        false
    }

    fn finish(&mut self) {
        self.started -= 1;
        self.grant_waiters();
    }

    fn grant_waiters(&mut self) {
        while self.free > 0
            && let Some(user_id) = self.next_user()
        {
            if self.grant_to(&user_id) {
                self.free -= 1;
            }
        }
    }

    fn release(&mut self, user_id: &str) {
        self.release_held(user_id);
        self.free += 1;
        self.grant_waiters();
    }

    fn release_held(&mut self, user_id: &str) {
        if let Some(n) = self.held_by_user.get_mut(user_id) {
            *n -= 1;
            if *n == 0 {
                self.held_by_user.remove(user_id);
            }
        }
    }
}

impl Queries {
    pub fn new(slots: usize, memory: Arc<Memory>) -> Arc<Queries> {
        Arc::new(Queries {
            memory,
            state: Mutex::new(SlotState {
                free: slots.max(1),
                held_by_user: BTreeMap::new(),
                waiters: BTreeMap::new(),
                started: 0,
                max_started: STARTED_PER_SLOT * slots.max(1),
                last_grant: BTreeMap::new(),
                grants: 0,
                next_query_seq: 0,
            }),
            running: Mutex::new(BTreeMap::new()),
        })
    }

    /// Admits a query of `user_id`, waiting up to 5 s for a query slot, and registers its memory
    /// for admission by `txns`.
    pub async fn start(self: &Arc<Self>, user_id: &str, txns: &Arc<OpenTxns>) -> Result<QueryGrant> {
        let mut slot =
            QuerySlot { queries: Some(self.clone()), user_id: user_id.to_string(), holds: false, started: false };
        match tokio::time::timeout(SLOT_WAIT, slot.wait(false, None)).await {
            Ok(()) => {}
            Err(_) => {
                return Err(Error::Unavailable {
                    message: "the node is busy running queries; retry".into(),
                    retry_after_ms: Some(1000),
                    outcome_unknown: false,
                });
            }
        }
        let seq = {
            let mut s = self.state.lock();
            s.next_query_seq += 1;
            s.next_query_seq
        };
        let memory = Arc::new(QueryMemory {
            seq,
            user_id: user_id.to_string(),
            bytes: AtomicUsize::new(0),
            charge: Mutex::new(Charge::new(&self.memory, 0)),
            cancelled: AtomicBool::new(false),
            evicted: tokio::sync::Notify::new(),
            failure: Mutex::new(None),
            txns: Arc::downgrade(txns),
            step_bytes: (self.memory.limit() / 1024).max(1),
            limit_bytes: self.memory.limit(),
        });
        self.running.lock().insert(seq, memory.clone());
        Ok(QueryGrant { slot, memory, queries: Some(self.clone()) })
    }

    /// Returns the number of queries that hold or wait for a query slot after their admission.
    pub fn running_count(&self) -> usize {
        self.running.lock().len()
    }

    /// Returns the queries that compete for memory, from youngest to oldest.
    pub(crate) fn running(&self) -> Vec<Arc<QueryMemory>> {
        self.running.lock().values().rev().filter(|q| !q.is_stopping()).cloned().collect()
    }

    /// Returns the bytes charged by evicted and failed queries, which are releasing their memory.
    pub(crate) fn releasing_bytes(&self) -> usize {
        self.running.lock().values().filter(|q| q.is_stopping()).map(|q| q.charged()).sum()
    }
}

/// Admission of a single event query: its query slot and its memory.
pub struct QueryGrant {
    pub slot: QuerySlot,
    pub memory: Arc<QueryMemory>,
    queries: Option<Arc<Queries>>,
}

impl QueryGrant {
    /// Returns a grant without a query slot or a memory budget, for queries outside a node.
    pub fn unlimited() -> QueryGrant {
        QueryGrant {
            slot: QuerySlot { queries: None, user_id: String::new(), holds: false, started: false },
            memory: Arc::new(QueryMemory {
                seq: 0,
                user_id: String::new(),
                bytes: AtomicUsize::new(0),
                charge: Mutex::new(Charge::default()),
                cancelled: AtomicBool::new(false),
                evicted: tokio::sync::Notify::new(),
                failure: Mutex::new(None),
                txns: Weak::new(),
                step_bytes: usize::MAX,
                limit_bytes: usize::MAX,
            }),
            queries: None,
        }
    }
}

impl QueryGrant {
    /// Gives the query's slot to a user that holds fewer slots, if one waits, and waits for a
    /// slot again. Returns an error if the node evicted the query or it failed a memory check.
    pub async fn yield_slot(&mut self) -> Result<()> {
        self.slot.yield_to_waiters(Some(&self.memory)).await;
        self.memory.check_stopped()
    }
}

impl Drop for QueryGrant {
    fn drop(&mut self) {
        if let Some(q) = &self.queries {
            q.running.lock().remove(&self.memory.seq);
        }
    }
}

/// A query slot that a query holds while it runs and yields to users that hold fewer slots.
pub struct QuerySlot {
    queries: Option<Arc<Queries>>,
    user_id: String,
    holds: bool,
    /// True once the query first received a slot.
    started: bool,
}

impl QuerySlot {
    /// Waits until this query holds a slot, or until the node evicts the query that `memory`
    /// tracks. A query that `resumes` waits ahead of the queries of its user that have not
    /// started.
    async fn wait(&mut self, resumes: bool, memory: Option<&QueryMemory>) {
        let Some(queries) = self.queries.clone() else { return };
        let (tx, rx) = oneshot::channel();
        {
            let mut s = queries.state.lock();
            let queue = s.waiters.entry(self.user_id.clone()).or_default();
            let waiter = SlotWaiter { grant: tx, resumes };
            match resumes {
                true => queue.push_front(waiter),
                false => queue.push_back(waiter),
            }
            s.grant_waiters();
        }
        let mut wait = SlotWait { queries: &queries, user_id: &self.user_id, rx: Some(rx), resumes };
        let Some(rx) = wait.rx.as_mut() else { return };
        let granted = match memory {
            None => rx.await.is_ok(),
            Some(m) => {
                let evicted = m.evicted.notified();
                tokio::pin!(evicted);
                evicted.as_mut().enable();
                if m.is_stopping() {
                    false
                } else {
                    match futures_util::future::select(rx, evicted).await {
                        futures_util::future::Either::Left((r, _)) => r.is_ok(),
                        futures_util::future::Either::Right(_) => false,
                    }
                }
            }
        };
        if granted {
            wait.rx = None;
            self.holds = true;
            self.started = true;
        }
    }

    /// Gives this query's slot to the next waiting user if that user holds no slots or at least
    /// 2 fewer slots than this query's user, and waits for a slot again.
    async fn yield_to_waiters(&mut self, memory: Option<&QueryMemory>) {
        let Some(queries) = &self.queries else { return };
        {
            let mut s = queries.state.lock();
            let mine = s.held(&self.user_id);
            let fairer = |theirs: usize| theirs == 0 || theirs + 1 < mine;
            let Some(user_id) = s.next_user().filter(|u| fairer(s.held(u))) else { return };
            self.holds = false;
            s.release_held(&self.user_id);
            if !s.grant_to(&user_id) {
                s.free += 1;
            }
        }
        self.wait(true, memory).await;
    }
}

impl Drop for QuerySlot {
    fn drop(&mut self) {
        let Some(q) = &self.queries else { return };
        let mut s = q.state.lock();
        if self.holds {
            s.release(&self.user_id);
        }
        if self.started {
            s.finish();
        }
    }
}

/// Releases a slot granted to a waiter that stopped waiting before it received the grant.
struct SlotWait<'a> {
    queries: &'a Queries,
    user_id: &'a str,
    rx: Option<oneshot::Receiver<()>>,
    resumes: bool,
}

impl Drop for SlotWait<'_> {
    fn drop(&mut self) {
        if let Some(mut rx) = self.rx.take() {
            rx.close();
            if rx.try_recv().is_ok() {
                let mut s = self.queries.state.lock();
                s.release(self.user_id);
                if !self.resumes {
                    s.finish();
                }
            }
        }
    }
}

/// Memory that a running query holds, charged to the node memory budget.
pub struct QueryMemory {
    /// Start order of the query on this node.
    pub seq: u64,
    pub user_id: String,
    bytes: AtomicUsize,
    charge: Mutex<Charge>,
    /// Set if the node evicted the query to free memory for another user.
    cancelled: AtomicBool,
    /// Wakes the query when the node evicts it.
    evicted: tokio::sync::Notify,
    /// Error of the query's own failed memory check. Set while the query releases its memory.
    failure: Mutex<Option<Error>>,
    txns: Weak<OpenTxns>,
    step_bytes: usize,
    /// Node memory budget, and the largest amount of memory a query can use.
    pub limit_bytes: usize,
}

impl QueryMemory {
    /// Changes a part of the query's memory from `old` to `new` bytes. Returns an error if the
    /// query was cancelled or the node cannot admit the growth.
    pub fn resize(&self, old: usize, new: usize) -> Result<()> {
        let total = if new >= old {
            self.bytes.fetch_add(new - old, Relaxed) + (new - old)
        } else {
            self.bytes.fetch_sub(old - new, Relaxed) - (old - new)
        };
        self.check_stopped()?;
        if total > self.limit_bytes {
            return Err(self.fail(Error::TooLarge(format!(
                "the query needs more than the node's {} byte memory budget; narrow the time range, group by \
                 fewer values, or read fewer columns",
                self.limit_bytes
            ))));
        }
        let charged = self.charged();
        if total.abs_diff(charged) < self.step_bytes {
            return Ok(());
        }
        self.charge.lock().set(total);
        match self.txns.upgrade() {
            Some(txns) if total > charged => txns.admit_query(self),
            _ => Ok(()),
        }
    }

    /// Releases `bytes` of the query's memory without an admission check.
    pub fn release(&self, bytes: usize) {
        let total = self.bytes.fetch_sub(bytes, Relaxed) - bytes;
        let mut charge = self.charge.lock();
        if charge.bytes().abs_diff(total) >= self.step_bytes {
            charge.set(total);
        }
    }

    /// Returns the bytes charged to the node memory budget.
    pub fn charged(&self) -> usize {
        self.charge.lock().bytes()
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Relaxed);
        self.evicted.notify_waiters();
    }

    /// Records `e` as the query's failure, so that its later memory changes return the same
    /// error, and returns the recorded failure.
    pub(crate) fn fail(&self, e: Error) -> Error {
        self.failure.lock().get_or_insert(e).clone()
    }

    /// Returns true if the query no longer competes for memory because it was evicted or failed.
    pub(crate) fn is_stopping(&self) -> bool {
        self.cancelled.load(Relaxed) || self.failure.lock().is_some()
    }

    /// Returns an error if the node evicted the query or the query failed a memory check.
    pub fn check_stopped(&self) -> Result<()> {
        if let Some(e) = self.failure.lock().clone() {
            return Err(e);
        }
        if self.cancelled.load(Relaxed) {
            return Err(Error::Unavailable {
                message: "the query was stopped to free node memory for other queries; retry".into(),
                retry_after_ms: Some(1000),
                outcome_unknown: false,
            });
        }
        Ok(())
    }
}

/// Bytes that one part of a query holds, reported to its [`QueryMemory`] in steps.
pub struct QueryBytes {
    query: Arc<QueryMemory>,
    bytes: usize,
    reported: usize,
}

/// Change in held bytes that triggers a report to [`QueryMemory`].
const REPORT_BYTES: usize = 64 * 1024;

impl QueryBytes {
    pub fn new(query: &Arc<QueryMemory>) -> QueryBytes {
        QueryBytes { query: query.clone(), bytes: 0, reported: 0 }
    }

    pub fn set(&mut self, bytes: usize) -> Result<()> {
        self.bytes = bytes;
        if bytes.abs_diff(self.reported) < REPORT_BYTES {
            return Ok(());
        }
        let old = std::mem::replace(&mut self.reported, bytes);
        self.query.resize(old, bytes)
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for QueryBytes {
    fn drop(&mut self) {
        self.query.release(self.reported);
    }
}

#[cfg(test)]
#[path = "queries_tests.rs"]
mod tests;
