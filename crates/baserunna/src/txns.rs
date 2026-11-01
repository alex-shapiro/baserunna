//! Per-node registry of open interactive transactions, enforcing per-user concurrency limits,
//! idle and lifetime expiration, and max-min fair memory admission (SPEC §5.12, §6.2).

use crate::engine::memory::{self, Held, MemoryGate};
use crate::engine::{Engine, Txn, Version};
use crate::error::{Error, Result};
use crate::util::{DetHashMap, TtlCache};
use baserunna_api::Encoding;
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
    /// Weak reference to the transaction's snapshot version, used to attribute pinned version
    /// memory without locking `txn`.
    snapshot: Weak<Version>,
    /// Set to `None` on commit or abort.
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
    open: Mutex<DetHashMap<String, Arc<OpenTxn>>>,
    aborted: TtlCache<String, &'static str>,
}

impl OpenTxns {
    pub fn new(engine: Arc<Engine>, limits: TxnLimits) -> Arc<OpenTxns> {
        Arc::new(OpenTxns {
            engine,
            limits,
            open: Mutex::new(DetHashMap::default()),
            aborted: TtlCache::new(limits.lifetime, 100_000),
        })
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

    /// Enforces the node memory budget for a transaction of `user_id` on `db_id`, unloading idle
    /// databases and evicting idle transactions via [`memory::fair_admission`] if needed.
    pub fn admit(&self, user_id: &str, db_id: &str) -> Result<()> {
        let shared = &self.engine.shared;
        if shared.is_system(db_id) || shared.reclaim_to_budget() {
            return Ok(());
        }
        let (open, held): (Vec<_>, Vec<_>) = self.held_memory().into_iter().unzip();
        let user = user_id.to_string();
        let Some(victims) = memory::fair_admission(shared.memory.limit(), shared.memory.used(), &user, &held) else {
            crate::probe::hit(crate::probe::Probe::MemoryExhausted);
            return Err(Error::Unavailable {
                message: "this user's open transactions hold their fair share of the node memory budget".into(),
                retry_after_ms: Some(1_000),
                outcome_unknown: false,
            });
        };
        for i in victims {
            if self.abort(&open[i], "it was aborted to free node memory for other users") {
                crate::probe::hit(crate::probe::Probe::TxnEvicted);
            }
        }
        Ok(())
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

    /// Registers `txn` as open transaction `id` for `user_id` on `db_id` and returns a locked
    /// [`TxnGuard`] for the initial request.
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
        if crate::buggify::fault(0.01) {
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

    fn lost(&self, id: &str) -> Error {
        crate::probe::hit(crate::probe::Probe::InteractiveTxnLost);
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

    /// Records `reason` before `txn` is unlocked so that concurrent waiters observe the abort
    /// reason.
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
        for (t, reason) in expired {
            self.abort(&t, reason);
        }
    }

    /// Returns open transactions ordered from least to most recently used, paired with their
    /// memory usage (read set, write set, and an equal share of any non-current snapshot version).
    fn held_memory(&self) -> Vec<(Arc<OpenTxn>, Held<String>)> {
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
                (t, held)
            })
            .collect()
    }
}

/// Exclusive guard for an open transaction during a single request. If dropped before
/// [`TxnGuard::finish`] succeeds or [`TxnGuard::take`] is called, the transaction is aborted.
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
            self.txns.abort_locked(&self.open, &mut self.guard, "a request on it failed or was cancelled");
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
    use crate::engine::{Consistency, Isolation, Value};
    use crate::error::Error;
    use crate::sim::cost::{Resp, World, run_paused};
    use bytes::Bytes;
    use serde_json::{Value as Json, json};
    use std::time::Duration;

    async fn begin(w: &World, token: &str, db: &str) -> Resp {
        w.call(0, "POST", &format!("/v1/dbs/{db}/transactions"), token, Bytes::from("{}")).await
    }

    async fn ops(w: &World, token: &str, db: &str, id: &str, ops: Json) -> Json {
        w.json(0, "POST", &format!("/v1/dbs/{db}/transactions/{id}/ops"), token, json!({"ops": ops})).await
    }

    fn get_op() -> Json {
        json!([{"op": "get", "key": "k0"}])
    }

    fn big_puts(tag: &str) -> Json {
        (0..120).map(|i| json!({"op": "put", "key": format!("{tag}{i}"), "value": "v".repeat(20_000)})).collect()
    }

    fn growth_slack(w: &World) -> usize {
        w.node(0).engine.shared.memory.limit() / 1024 + 32 * 1024
    }

    async fn fill_memory(w: &World, token: &str, db: &str) -> (Vec<String>, Option<String>) {
        let mut open = Vec::new();
        loop {
            let r = begin(w, token, db).await;
            if r.status != 201 {
                assert_eq!((r.status, &r.json()["error"]["retryable"]), (503, &json!(true)), "{:?}", r.json());
                return (open, None);
            }
            let id = r.json()["txn_id"].as_str().unwrap().to_string();
            let r = ops(w, token, db, &id, big_puts("k")).await;
            if !r["error"].is_null() {
                assert_eq!(
                    (&r["error"]["code"], &r["error"]["retryable"]),
                    (&json!("txn_not_found"), &json!(true)),
                    "rejected ops request did not return txn_not_found: {r}"
                );
                assert!(r["error"]["message"].as_str().unwrap().contains("memory budget"), "{r}");
                return (open, Some(id));
            }
            open.push(id);
            assert!(open.len() < 20, "transactions were never rejected");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[test]
    fn interactive_transactions_are_capped_per_user() {
        run_paused(async {
            let w = World::with_config(1, 1, |c| c.txns_per_user = 3).await;
            let (alice, bob) = (w.signup(0, "alice@x.io").await, w.signup(0, "bob@x.io").await);
            w.create_db(0, &alice, "a").await;
            w.create_db(0, &bob, "b").await;
            let mut ids = Vec::new();
            for _ in 0..3 {
                let r = begin(&w, &alice, "a").await;
                assert_eq!(r.status, 201);
                ids.push(r.json()["txn_id"].as_str().unwrap().to_string());
            }
            let r = begin(&w, &alice, "a").await;
            assert_eq!((r.status, r.json()["error"]["code"].as_str()), (429, Some("rate_limited")), "{:?}", r.json());
            assert_eq!(begin(&w, &bob, "b").await.status, 201, "user quota affected another user");
            let r = w.call(0, "DELETE", &format!("/v1/dbs/a/transactions/{}", ids[0]), &alice, Bytes::new()).await;
            assert_eq!(r.status, 200);
            assert_eq!(begin(&w, &alice, "a").await.status, 201, "an aborted transaction still counted");
        });
    }

    #[test]
    fn expired_transaction_returns_abort_reason() {
        run_paused(async {
            let w = World::new(1, 1).await;
            let token = w.signup(0, "a@x.io").await;
            w.create_db(0, &token, "a").await;
            let id = begin(&w, &token, "a").await.json()["txn_id"].as_str().unwrap().to_string();
            tokio::time::sleep(Duration::from_millis(w.node(0).cfg.txn_idle_ms)).await;
            w.node(0).txns.expire();
            let r = ops(&w, &token, "a", &id, get_op()).await;
            assert_eq!(r["error"]["code"], "txn_not_found", "{r}");
            assert!(r["error"]["message"].as_str().unwrap().contains("idle timeout"), "{r}");
        });
    }

    #[test]
    fn expire_aborts_transaction_after_lookup() {
        run_paused(async {
            let w = World::new(1, 1).await;
            let token = w.signup(0, "a@x.io").await;
            let db_id = w.create_db(0, &token, "a").await;
            let node = w.node(0);
            let db = node.engine.db(&db_id);
            db.open(true).await.unwrap();
            let t = db.begin(Isolation::Snapshot, Consistency::Strong).await.unwrap();
            node.txns.open("t".into(), "a", &db_id, t, Default::default()).unwrap().finish(Ok(())).unwrap();
            let held = node.txns.get("t", "a", &db_id).unwrap();
            tokio::time::sleep(Duration::from_millis(node.cfg.txn_idle_ms)).await;
            node.txns.expire();
            assert!(held.txn.lock().await.is_none(), "an expired transaction stayed usable");
        });
    }

    #[test]
    fn committed_transaction_is_not_reported_as_expired() {
        run_paused(async {
            let w = World::new(1, 1).await;
            let token = w.signup(0, "a@x.io").await;
            let db_id = w.create_db(0, &token, "a").await;
            let node = w.node(0);
            let db = node.engine.db(&db_id);
            db.open(true).await.unwrap();
            let t = db.begin(Isolation::Snapshot, Consistency::Strong).await.unwrap();
            drop(node.txns.open("t".into(), "a", &db_id, t, Default::default()).unwrap().take());
            tokio::time::sleep(Duration::from_millis(node.cfg.txn_idle_ms)).await;
            node.txns.expire();
            assert_eq!(node.txns.len(), 0);
            let e = node.txns.lost("t");
            assert!(!e.message().contains("idle"), "a retried commit would report: {e:?}");
        });
    }

    async fn commit(w: &World, token: &str, db: &str, id: &str) -> Resp {
        w.call(0, "POST", &format!("/v1/dbs/{db}/transactions/{id}/commit"), token, Bytes::from("{}")).await
    }

    #[test]
    fn cancelled_ops_request_aborts_transaction() {
        run_paused(async {
            let w = World::new(1, 1).await;
            let token = w.signup(0, "a@x.io").await;
            w.create_db(0, &token, "a").await;
            let big = "b".repeat(w.node(0).cfg.engine.value_inline_max + 1);
            let r = w
                .json(0, "POST", "/v1/dbs/a/txn", &token, json!({"ops": [{"op": "put", "key": "blob", "value": big}]}))
                .await;
            assert!(r["error"].is_null(), "{r}");
            let id = begin(&w, &token, "a").await.json()["txn_id"].as_str().unwrap().to_string();
            w.store.set_latency_ms(1_000);
            let request = ops(
                &w,
                &token,
                "a",
                &id,
                json!([{"op": "put", "key": "x", "value": "1"}, {"op": "get", "key": "blob"}]),
            );
            let cancelled = tokio::time::timeout(Duration::from_millis(100), request).await;
            assert!(cancelled.is_err(), "the request finished before it was cancelled: {cancelled:?}");
            w.store.set_latency_ms(1);
            let r = ops(&w, &token, "a", &id, json!([{"op": "get", "key": "x"}])).await;
            assert_eq!(r["error"]["code"], "txn_not_found", "a partly applied request left its transaction open: {r}");
            assert!(r["error"]["message"].as_str().unwrap().contains("cancelled"), "{r}");
        });
    }

    #[test]
    fn transactions_are_bounded_by_memory_budget_and_can_finish() {
        run_paused(async {
            let w = World::with_config(1, 1, |c| c.engine.memory_bytes = 16 << 20).await;
            let token = w.signup(0, "a@x.io").await;
            w.create_db(0, &token, "a").await;
            let (open, rejected) = fill_memory(&w, &token, "a").await;
            let memory = &w.node(0).engine.shared.memory;
            assert!(memory.used() <= memory.limit() + growth_slack(&w), "{} of {}", memory.used(), memory.limit());
            let rejected = rejected.expect("growing past the budget did not fail");
            let r = commit(&w, &token, "a", &rejected).await;
            assert_eq!(r.json()["error"]["code"], "txn_not_found", "{:?}", r.json());
            let r = commit(&w, &token, "a", &open[0]).await;
            assert_eq!(r.status, 200, "committing a transaction under memory pressure: {:?}", r.json());
            for id in &open[1..] {
                let r = w.call(0, "DELETE", &format!("/v1/dbs/a/transactions/{id}"), &token, Bytes::new()).await;
                assert_eq!(r.status, 200, "aborting a transaction under memory pressure: {:?}", r.json());
            }
            let r = begin(&w, &token, "a").await;
            assert_eq!(r.status, 201, "released memory was not reused");
        });
    }

    #[test]
    fn concurrent_transaction_growth_is_bounded_by_memory_budget() {
        run_paused(async {
            let w = World::with_config(1, 1, |c| c.engine.memory_bytes = 16 << 20).await;
            let token = w.signup(0, "a@x.io").await;
            let db_id = w.create_db(0, &token, "a").await;
            let node = w.node(0);
            let db = node.engine.db(&db_id);
            db.open(true).await.unwrap();
            let mut open = Vec::new();
            for i in 0..20 {
                let t = db.begin(Isolation::Snapshot, Consistency::Strong).await.unwrap();
                open.push(Some(node.txns.open(format!("t{i}"), "a", &db_id, t, Default::default()).unwrap()));
            }
            let (memory, mut peak, mut rejected) = (&node.engine.shared.memory, 0, 0);
            for k in 0..120 {
                for g in open.iter_mut() {
                    let Some(t) = g.as_mut() else { continue };
                    let put = t.txn().put(Bytes::from(format!("k{k}")), Value::Inline(Bytes::from(vec![b'v'; 20_000])));
                    if let Err(e) = put {
                        assert!(matches!(e, Error::Unavailable { .. }), "{e:?}");
                        (*g, rejected) = (None, rejected + 1);
                    }
                    peak = peak.max(memory.used());
                }
            }
            assert!(
                (1..=16).contains(&rejected),
                "{rejected} of 20 transactions of 2.4 MB were rejected in a 16 MiB budget"
            );
            let bound = memory.limit() + 20 * growth_slack(&w);
            assert!(peak <= bound, "interleaved growth peaked at {peak} of {}", memory.limit());
        });
    }

    #[test]
    fn user_at_memory_budget_cannot_starve_other_users() {
        run_paused(async {
            let w = World::with_config(1, 1, |c| c.engine.memory_bytes = 16 << 20).await;
            let (alice, bob) = (w.signup(0, "alice@x.io").await, w.signup(0, "bob@x.io").await);
            w.create_db(0, &alice, "a").await;
            w.create_db(0, &bob, "b").await;
            let (alices, _) = fill_memory(&w, &alice, "a").await;
            let memory = &w.node(0).engine.shared.memory;
            let r = begin(&w, &bob, "b").await;
            assert_eq!(r.status, 201, "a user with 0 open transactions was rejected: {:?}", r.json());
            let bobs = r.json()["txn_id"].as_str().unwrap().to_string();
            let mut grown = 0;
            loop {
                let r = ops(&w, &bob, "b", &bobs, big_puts(&format!("b{grown}-"))).await;
                if !r["error"].is_null() {
                    assert_eq!(r["error"]["code"], "txn_not_found", "{r}");
                    break;
                }
                grown += 1;
                let (used, limit) = (memory.used(), memory.limit());
                assert!(used <= limit + growth_slack(&w), "bob's growth left {used} of {limit} bytes used");
                assert!(grown < 10, "bob was never rejected");
            }
            assert!(grown >= 3, "bob grew to {grown} write batches, less than half of the budget");
            let r = ops(&w, &alice, "a", &alices[0], get_op()).await;
            assert_eq!((&r["error"]["code"], &r["error"]["retryable"]), (&json!("txn_not_found"), &json!(true)), "{r}");
            assert!(r["error"]["message"].as_str().unwrap().contains("free node memory"), "{r}");
            let r = ops(&w, &alice, "a", &alices[alices.len() - 1], get_op()).await;
            assert!(r["error"].is_null(), "the most recently used transaction was evicted: {r}");
        });
    }

    #[test]
    fn transactions_pinning_stale_versions_count_against_their_user() {
        run_paused(async {
            let w = World::with_config(1, 1, |c| c.engine.memory_bytes = 16 << 20).await;
            let (alice, bob) = (w.signup(0, "alice@x.io").await, w.signup(0, "bob@x.io").await);
            w.create_db(0, &alice, "a").await;
            w.create_db(0, &bob, "b").await;
            let fill: Vec<_> =
                (0..110).map(|i| json!({"op": "put", "key": format!("f{i}"), "value": "t".repeat(2_000)})).collect();
            let r = w.json(0, "POST", "/v1/dbs/a/txn", &alice, json!({"ops": fill})).await;
            assert!(r["error"].is_null(), "{r}");
            let mut pins = 0;
            loop {
                let r = begin(&w, &alice, "a").await;
                if r.status != 201 {
                    assert_eq!(r.status, 503, "{:?}", r.json());
                    break;
                }
                pins += 1;
                assert!(pins < 100, "transactions pinning versions were never rejected");
                let put = json!({"ops": [{"op": "put", "key": format!("p{pins}"), "value": "x"}]});
                let r = w.json(0, "POST", "/v1/dbs/a/txn", &alice, put).await;
                assert!(r["error"].is_null(), "{r}");
            }
            let memory = &w.node(0).engine.shared.memory;
            let r = begin(&w, &bob, "b").await;
            assert_eq!(
                r.status,
                201,
                "bob was rejected while alice's transactions pinned {pins} versions: {:?}",
                r.json()
            );
            assert!(!memory.is_exceeded(), "admitting bob left {} of {} bytes used", memory.used(), memory.limit());
        });
    }
}
