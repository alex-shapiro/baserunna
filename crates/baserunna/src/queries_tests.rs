//! Tests for fair query slot scheduling.

use super::*;

fn queries(slots: usize) -> Arc<Queries> {
    Queries::new(slots, Arc::new(Memory::new(1 << 20)))
}

async fn slot(q: &Arc<Queries>, user: &str) -> QuerySlot {
    let mut s = QuerySlot { queries: Some(q.clone()), user_id: user.into(), holds: false };
    s.wait(false).await;
    s
}

fn held(q: &Queries, user: &str) -> usize {
    q.state.lock().held(user)
}

#[test]
fn freed_slot_goes_to_the_user_with_the_fewest_slots() {
    crate::sim::cost::run_paused(async {
        let q = queries(2);
        let a1 = slot(&q, "a").await;
        let _a2 = slot(&q, "a").await;
        let qa = q.clone();
        let a3 = tokio::spawn(async move { slot(&qa, "a").await });
        tokio::task::yield_now().await;
        let qb = q.clone();
        let b = tokio::spawn(async move { slot(&qb, "b").await });
        tokio::task::yield_now().await;
        drop(a1);
        let b = b.await.unwrap();
        assert_eq!((held(&q, "a"), held(&q, "b")), (1, 1), "the earlier waiter of a busier user got the slot");
        drop(b);
        let _a3 = a3.await.unwrap();
        assert_eq!(held(&q, "a"), 2);
    });
}

#[test]
fn busy_user_yields_a_slot_to_a_waiting_user() {
    crate::sim::cost::run_paused(async {
        let q = queries(1);
        let mut a = slot(&q, "a").await;
        a.yield_to_waiters().await;
        assert!(a.holds, "yielded without a waiter");
        let qb = q.clone();
        let b = tokio::spawn(async move {
            let mut b = slot(&qb, "b").await;
            b.yield_to_waiters().await;
            b
        });
        tokio::task::yield_now().await;
        a.yield_to_waiters().await;
        assert!(a.holds && held(&q, "a") == 1, "the slot did not return after the other user yielded");
        drop(a);
        let b = b.await.unwrap();
        assert!(b.holds && held(&q, "b") == 1);
    });
}

#[test]
fn abandoned_wait_returns_its_slot() {
    crate::sim::cost::run_paused(async {
        let q = queries(1);
        let a = slot(&q, "a").await;
        let qb = q.clone();
        let b = tokio::spawn(async move { slot(&qb, "b").await });
        tokio::task::yield_now().await;
        drop(a);
        b.abort();
        let _ = b.await;
        assert_eq!(q.state.lock().free, 1, "an abandoned grant leaked a slot");
        let _c = tokio::time::timeout(Duration::from_secs(1), slot(&q, "c")).await.expect("slot is free");
    });
}

#[test]
fn uneven_slot_split_does_not_trade_slots() {
    crate::sim::cost::run_paused(async {
        let q = queries(3);
        let mut a1 = slot(&q, "a").await;
        let _a2 = slot(&q, "a").await;
        let mut b1 = slot(&q, "b").await;
        let (qa, qb) = (q.clone(), q.clone());
        let a3 = tokio::spawn(async move { slot(&qa, "a").await });
        let b2 = tokio::spawn(async move { slot(&qb, "b").await });
        tokio::task::yield_now().await;
        a1.yield_to_waiters().await;
        b1.yield_to_waiters().await;
        assert!(a1.holds && b1.holds, "a 2/1 split traded a slot");
        assert_eq!((held(&q, "a"), held(&q, "b")), (2, 1));
        a3.abort();
        b2.abort();
    });
}

#[test]
fn yielded_query_resumes_before_new_queries_of_its_user() {
    crate::sim::cost::run_paused(async {
        let q = queries(1);
        let mut a1 = slot(&q, "a").await;
        let qa = q.clone();
        let a2 = tokio::spawn(async move { slot(&qa, "a").await });
        tokio::task::yield_now().await;
        let qb = q.clone();
        let b = tokio::spawn(async move { slot(&qb, "b").await });
        tokio::task::yield_now().await;
        let resumed = tokio::spawn(async move {
            a1.yield_to_waiters().await;
            a1
        });
        tokio::task::yield_now().await;
        drop(b.await.unwrap());
        let a1 = resumed.await.unwrap();
        assert!(a1.holds, "a new query of the user started ahead of its yielded query");
        drop(a1);
        drop(a2.await.unwrap());
    });
}

#[test]
fn query_over_the_budget_fails_and_stays_failed() {
    let memory = QueryMemory {
        seq: 1,
        user_id: "a".into(),
        bytes: AtomicUsize::new(0),
        charge: Mutex::new(Charge::default()),
        cancelled: AtomicBool::new(false),
        failure: Mutex::new(None),
        txns: Weak::new(),
        step_bytes: 1,
        limit_bytes: 1_000,
    };
    assert_eq!(memory.resize(0, 1_000), Ok(()));
    assert!(matches!(memory.resize(1_000, 1_001), Err(Error::TooLarge(_))));
    assert!(matches!(memory.resize(1_001, 10), Err(Error::TooLarge(_))), "a failed query kept running");
    assert!(memory.is_stopping());
}
