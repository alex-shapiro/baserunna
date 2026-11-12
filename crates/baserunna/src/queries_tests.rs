//! Tests for fair query slot scheduling.

use super::*;

fn queries(slots: usize) -> Arc<Queries> {
    Queries::new(slots, Arc::new(Memory::new(1 << 20)))
}

async fn slot(q: &Arc<Queries>, user: &str) -> QuerySlot {
    let mut s = QuerySlot { queries: Some(q.clone()), user_id: user.into(), holds: false };
    s.wait().await;
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
