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

struct SlotState {
    free: usize,
    held_by_user: BTreeMap<String, usize>,
    waiters: VecDeque<SlotWaiter>,
    next_query_seq: u64,
}

struct SlotWaiter {
    user_id: String,
    grant: oneshot::Sender<()>,
}

impl SlotState {
    fn held(&self, user_id: &str) -> usize {
        self.held_by_user.get(user_id).copied().unwrap_or(0)
    }

    /// Returns the index of the waiter whose user holds the fewest slots, the earliest on ties.
    fn next_waiter(&self) -> Option<usize> {
        (0..self.waiters.len()).min_by_key(|&i| (self.held(&self.waiters[i].user_id), i))
    }

    fn grant_waiters(&mut self) {
        while self.free > 0
            && let Some(i) = self.next_waiter()
        {
            let w = self.waiters.remove(i).expect("waiter index is in range");
            if w.grant.send(()).is_ok() {
                self.free -= 1;
                *self.held_by_user.entry(w.user_id).or_default() += 1;
            }
        }
    }

    fn release(&mut self, user_id: &str) {
        if let Some(n) = self.held_by_user.get_mut(user_id) {
            *n -= 1;
            if *n == 0 {
                self.held_by_user.remove(user_id);
            }
        }
        self.free += 1;
        self.grant_waiters();
    }
}

impl Queries {
    pub fn new(slots: usize, memory: Arc<Memory>) -> Arc<Queries> {
        Arc::new(Queries {
            memory,
            state: Mutex::new(SlotState {
                free: slots.max(1),
                held_by_user: BTreeMap::new(),
                waiters: VecDeque::new(),
                next_query_seq: 0,
            }),
            running: Mutex::new(BTreeMap::new()),
        })
    }

    /// Admits a query of `user_id`, waiting up to 5 s for a query slot, and registers its memory
    /// for admission by `txns`.
    pub async fn start(self: &Arc<Self>, user_id: &str, txns: &Arc<OpenTxns>) -> Result<QueryGrant> {
        let mut slot = QuerySlot { queries: Some(self.clone()), user_id: user_id.to_string(), holds: false };
        match tokio::time::timeout(SLOT_WAIT, slot.wait()).await {
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

    /// Returns the queries that have not been cancelled, from youngest to oldest.
    pub(crate) fn running(&self) -> Vec<Arc<QueryMemory>> {
        self.running.lock().values().rev().filter(|q| !q.is_cancelled()).cloned().collect()
    }

    /// Returns the bytes charged by cancelled queries that have not finished.
    pub(crate) fn cancelled_bytes(&self) -> usize {
        self.running.lock().values().filter(|q| q.is_cancelled()).map(|q| q.charged()).sum()
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
            slot: QuerySlot { queries: None, user_id: String::new(), holds: false },
            memory: Arc::new(QueryMemory {
                seq: 0,
                user_id: String::new(),
                bytes: AtomicUsize::new(0),
                charge: Mutex::new(Charge::default()),
                cancelled: AtomicBool::new(false),
                txns: Weak::new(),
                step_bytes: usize::MAX,
                limit_bytes: usize::MAX,
            }),
            queries: None,
        }
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
}

impl QuerySlot {
    /// Waits until this query holds a slot.
    async fn wait(&mut self) {
        let Some(queries) = self.queries.clone() else { return };
        let (tx, rx) = oneshot::channel();
        {
            let mut s = queries.state.lock();
            s.waiters.push_back(SlotWaiter { user_id: self.user_id.clone(), grant: tx });
            s.grant_waiters();
        }
        let mut wait = SlotWait { queries: &queries, user_id: &self.user_id, rx: Some(rx) };
        if let Some(rx) = wait.rx.as_mut()
            && rx.await.is_ok()
        {
            wait.rx = None;
            self.holds = true;
        }
    }

    /// Gives this query's slot to a waiting query whose user holds fewer slots than this query's
    /// user, and waits for a slot again.
    pub async fn yield_to_waiters(&mut self) {
        let Some(queries) = &self.queries else { return };
        {
            let mut s = queries.state.lock();
            let mine = s.held(&self.user_id);
            if !s.next_waiter().is_some_and(|i| s.held(&s.waiters[i].user_id) < mine) {
                return;
            }
            self.holds = false;
            s.release(&self.user_id);
        }
        self.wait().await;
    }
}

impl Drop for QuerySlot {
    fn drop(&mut self) {
        if self.holds
            && let Some(q) = &self.queries
        {
            q.state.lock().release(&self.user_id);
        }
    }
}

/// Releases a slot granted to a waiter that stopped waiting before it received the grant.
struct SlotWait<'a> {
    queries: &'a Queries,
    user_id: &'a str,
    rx: Option<oneshot::Receiver<()>>,
}

impl Drop for SlotWait<'_> {
    fn drop(&mut self) {
        if let Some(mut rx) = self.rx.take() {
            rx.close();
            if rx.try_recv().is_ok() {
                self.queries.state.lock().release(self.user_id);
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
    cancelled: AtomicBool,
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
        self.check_cancelled()?;
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
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Relaxed)
    }

    fn check_cancelled(&self) -> Result<()> {
        if self.is_cancelled() {
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
