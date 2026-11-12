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
    free_slots: usize,
    slots_by_user: BTreeMap<String, usize>,
    /// Waiting queries per user, in slot grant order.
    waiters: BTreeMap<String, VecDeque<SlotWaiter>>,
    /// Number of queries that received a slot and have not finished.
    started_queries: usize,
    max_started_queries: usize,
    /// Sequence number of the latest slot grant per user.
    last_grant_seq: BTreeMap<String, u64>,
    grant_seq: u64,
    next_query_seq: u64,
}

struct SlotWaiter {
    grant_tx: oneshot::Sender<()>,
    /// True if the query started before and yielded its slot.
    is_resuming: bool,
}

impl SlotState {
    fn held_slots(&self, user_id: &str) -> usize {
        self.slots_by_user.get(user_id).copied().unwrap_or(0)
    }

    /// Returns true if the first waiting query of `user_id` is resuming, or if the node can
    /// start another query.
    fn can_grant(&self, user_id: &str) -> bool {
        self.waiters
            .get(user_id)
            .and_then(|q| q.front())
            .is_some_and(|w| w.is_resuming || self.started_queries < self.max_started_queries)
    }

    /// Selects the next user to receive a slot from the users whose first waiting query can
    /// receive one. Prefers the user with the fewest held slots, breaking ties by the oldest
    /// slot grant.
    fn next_user(&self) -> Option<String> {
        self.waiters
            .keys()
            .filter(|u| self.can_grant(u))
            .min_by_key(|u| (self.held_slots(u), self.last_grant_seq.get(*u).copied().unwrap_or(0)))
            .cloned()
    }

    /// Grants a slot to the first waiting query of `user_id` that is still waiting and can
    /// receive one. Returns false if no query of `user_id` receives a slot.
    fn grant_to(&mut self, user_id: &str) -> bool {
        while self.can_grant(user_id) {
            let queue = self.waiters.get_mut(user_id).expect("a user with a waiting query has a queue");
            let w = queue.pop_front().expect("a user with a waiting query has a queue entry");
            if queue.is_empty() {
                self.waiters.remove(user_id);
            }
            if w.grant_tx.send(()).is_ok() {
                *self.slots_by_user.entry(user_id.to_string()).or_default() += 1;
                self.started_queries += !w.is_resuming as usize;
                self.grant_seq += 1;
                self.last_grant_seq.insert(user_id.to_string(), self.grant_seq);
                return true;
            }
        }
        false
    }

    fn finish_query(&mut self) {
        self.started_queries -= 1;
        self.grant_waiters();
    }

    fn grant_waiters(&mut self) {
        while self.free_slots > 0
            && let Some(user_id) = self.next_user()
        {
            if self.grant_to(&user_id) {
                self.free_slots -= 1;
            }
        }
    }

    fn release_slot(&mut self, user_id: &str) {
        self.decrement_user_slots(user_id);
        self.free_slots += 1;
        self.grant_waiters();
    }

    fn decrement_user_slots(&mut self, user_id: &str) {
        if let Some(n) = self.slots_by_user.get_mut(user_id) {
            *n -= 1;
            if *n == 0 {
                self.slots_by_user.remove(user_id);
            }
        }
    }
}

impl Queries {
    pub fn new(slots: usize, memory: Arc<Memory>) -> Arc<Queries> {
        let slots = slots.max(1);
        Arc::new(Queries {
            memory,
            state: Mutex::new(SlotState {
                free_slots: slots,
                slots_by_user: BTreeMap::new(),
                waiters: BTreeMap::new(),
                started_queries: 0,
                max_started_queries: STARTED_PER_SLOT * slots,
                last_grant_seq: BTreeMap::new(),
                grant_seq: 0,
                next_query_seq: 0,
            }),
            running: Mutex::new(BTreeMap::new()),
        })
    }

    /// Admits a query of `user_id`, waiting up to 5 s for a query slot, and registers its memory
    /// with `txns`.
    pub async fn start(self: &Arc<Self>, user_id: &str, txns: &Arc<OpenTxns>) -> Result<QueryGrant> {
        let mut slot = QuerySlot {
            queries: Some(self.clone()),
            user_id: user_id.to_string(),
            holds_slot: false,
            has_started: false,
        };
        match tokio::time::timeout(SLOT_WAIT, slot.wait_for_slot(false, None)).await {
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
            is_evicted: AtomicBool::new(false),
            evicted: tokio::sync::Notify::new(),
            failure: Mutex::new(None),
            txns: Arc::downgrade(txns),
            step_bytes: (self.memory.limit() / 1024).max(1),
            limit_bytes: self.memory.limit(),
        });
        self.running.lock().insert(seq, memory.clone());
        Ok(QueryGrant { slot, memory, queries: Some(self.clone()) })
    }

    /// Returns the number of admitted queries that have not finished.
    pub fn running_count(&self) -> usize {
        self.running.lock().len()
    }

    /// Returns active queries that compete for memory, ordered from newest to oldest.
    pub(crate) fn active_queries(&self) -> Vec<Arc<QueryMemory>> {
        self.running.lock().values().rev().filter(|q| !q.is_stopping()).cloned().collect()
    }

    /// Returns the bytes charged by evicted or failed queries while they release their memory.
    pub(crate) fn releasing_bytes(&self) -> usize {
        self.running.lock().values().filter(|q| q.is_stopping()).map(|q| q.charged_bytes()).sum()
    }
}

/// Admission grant for a single event query, holding its query slot and memory tracker.
pub struct QueryGrant {
    pub slot: QuerySlot,
    pub memory: Arc<QueryMemory>,
    queries: Option<Arc<Queries>>,
}

impl QueryGrant {
    /// Returns a grant without a query slot or memory limit, for queries outside a node.
    pub fn unlimited() -> QueryGrant {
        QueryGrant {
            slot: QuerySlot { queries: None, user_id: String::new(), holds_slot: false, has_started: false },
            memory: Arc::new(QueryMemory {
                seq: 0,
                user_id: String::new(),
                bytes: AtomicUsize::new(0),
                charge: Mutex::new(Charge::default()),
                is_evicted: AtomicBool::new(false),
                evicted: tokio::sync::Notify::new(),
                failure: Mutex::new(None),
                txns: Weak::new(),
                step_bytes: usize::MAX,
                limit_bytes: usize::MAX,
            }),
            queries: None,
        }
    }

    /// Yields the query's slot to a waiting user with fewer slots and waits to reacquire a slot.
    /// Returns an error if the node evicted the query or the query failed a memory check.
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

/// Query slot held while a query runs and yielded to users with fewer slots.
pub struct QuerySlot {
    queries: Option<Arc<Queries>>,
    user_id: String,
    holds_slot: bool,
    /// True after the query first receives a slot.
    has_started: bool,
}

impl QuerySlot {
    /// Waits until this query holds a slot or the node evicts `memory`. If `is_resuming` is
    /// true, the query waits ahead of its user's unstarted queries.
    async fn wait_for_slot(&mut self, is_resuming: bool, memory: Option<&QueryMemory>) {
        let Some(queries) = self.queries.clone() else { return };
        let (tx, rx) = oneshot::channel();
        {
            let mut s = queries.state.lock();
            let queue = s.waiters.entry(self.user_id.clone()).or_default();
            let waiter = SlotWaiter { grant_tx: tx, is_resuming };
            match is_resuming {
                true => queue.push_front(waiter),
                false => queue.push_back(waiter),
            }
            s.grant_waiters();
        }
        let mut wait_guard = SlotWaitGuard { queries: &queries, user_id: &self.user_id, rx: Some(rx), is_resuming };
        let Some(rx) = wait_guard.rx.as_mut() else { return };
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
            wait_guard.rx = None;
            self.holds_slot = true;
            self.has_started = true;
        }
    }

    /// Yields this query's slot if the next waiting user holds 0 slots or at least 2 fewer
    /// slots than this query's user, then waits to reacquire a slot.
    async fn yield_to_waiters(&mut self, memory: Option<&QueryMemory>) {
        let Some(queries) = &self.queries else { return };
        {
            let mut s = queries.state.lock();
            let mine = s.held_slots(&self.user_id);
            let can_yield_to = |theirs: usize| theirs == 0 || theirs + 1 < mine;
            let Some(user_id) = s.next_user().filter(|u| can_yield_to(s.held_slots(u))) else { return };
            self.holds_slot = false;
            s.decrement_user_slots(&self.user_id);
            if !s.grant_to(&user_id) {
                s.free_slots += 1;
            }
        }
        self.wait_for_slot(true, memory).await;
    }
}

impl Drop for QuerySlot {
    fn drop(&mut self) {
        let Some(q) = &self.queries else { return };
        let mut s = q.state.lock();
        if self.holds_slot {
            s.release_slot(&self.user_id);
        }
        if self.has_started {
            s.finish_query();
        }
    }
}

/// Guard that releases a granted slot if a waiter is dropped before receiving the grant.
struct SlotWaitGuard<'a> {
    queries: &'a Queries,
    user_id: &'a str,
    rx: Option<oneshot::Receiver<()>>,
    is_resuming: bool,
}

impl Drop for SlotWaitGuard<'_> {
    fn drop(&mut self) {
        if let Some(mut rx) = self.rx.take() {
            rx.close();
            if rx.try_recv().is_ok() {
                let mut s = self.queries.state.lock();
                s.release_slot(self.user_id);
                if !self.is_resuming {
                    s.finish_query();
                }
            }
        }
    }
}

/// Memory usage of a running query, charged to the node memory budget.
pub struct QueryMemory {
    /// Start sequence number of the query on this node.
    pub seq: u64,
    pub user_id: String,
    bytes: AtomicUsize,
    charge: Mutex<Charge>,
    /// True if the node evicted the query to free memory for other users.
    is_evicted: AtomicBool,
    /// Notifies the query if the node evicts it.
    evicted: tokio::sync::Notify,
    /// Error from the query's first failed memory check.
    failure: Mutex<Option<Error>>,
    txns: Weak<OpenTxns>,
    step_bytes: usize,
    /// Node memory budget, which is the maximum memory a single query can use.
    pub limit_bytes: usize,
}

impl QueryMemory {
    /// Updates a component of the query's memory from `old` to `new` bytes. Returns an error if
    /// the query was evicted or the node rejects the growth.
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
        let charged = self.charged_bytes();
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
    pub fn charged_bytes(&self) -> usize {
        self.charge.lock().bytes()
    }

    pub(crate) fn evict(&self) {
        self.is_evicted.store(true, Relaxed);
        self.evicted.notify_waiters();
    }

    /// Records `e` as the query's failure if none is recorded, and returns the recorded error.
    pub(crate) fn fail(&self, e: Error) -> Error {
        self.failure.lock().get_or_insert(e).clone()
    }

    /// Returns true if the query was evicted or failed a memory check.
    pub(crate) fn is_stopping(&self) -> bool {
        self.is_evicted.load(Relaxed) || self.failure.lock().is_some()
    }

    /// Returns an error if the query was evicted or failed a memory check.
    pub fn check_stopped(&self) -> Result<()> {
        if let Some(e) = self.failure.lock().clone() {
            return Err(e);
        }
        if self.is_evicted.load(Relaxed) {
            return Err(Error::Unavailable {
                message: "the query was stopped to free node memory for other queries; retry".into(),
                retry_after_ms: Some(1000),
                outcome_unknown: false,
            });
        }
        Ok(())
    }
}

/// Memory tracker for one part of a query, reporting changes to [`QueryMemory`] in 64 KiB steps.
pub struct QueryBytes {
    query: Arc<QueryMemory>,
    bytes: usize,
    reported_bytes: usize,
}

/// Byte delta that triggers a report to [`QueryMemory`].
const REPORT_STEP_BYTES: usize = 64 * 1024;

impl QueryBytes {
    pub fn new(query: &Arc<QueryMemory>) -> QueryBytes {
        QueryBytes { query: query.clone(), bytes: 0, reported_bytes: 0 }
    }

    pub fn set(&mut self, bytes: usize) -> Result<()> {
        self.bytes = bytes;
        if bytes.abs_diff(self.reported_bytes) < REPORT_STEP_BYTES {
            return Ok(());
        }
        let old = std::mem::replace(&mut self.reported_bytes, bytes);
        self.query.resize(old, bytes)
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for QueryBytes {
    fn drop(&mut self) {
        self.query.release(self.reported_bytes);
    }
}

#[cfg(test)]
#[path = "queries_tests.rs"]
mod tests;
