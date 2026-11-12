//! Tests for fair query slot scheduling.

use super::*;

fn queries(slots: usize) -> Arc<Queries> {
    Queries::new(slots, Arc::new(Memory::new(1 << 20)))
}

async fn acquire_slot(q: &Arc<Queries>, user: &str) -> QuerySlot {
    let mut s = QuerySlot { queries: Some(q.clone()), user_id: user.into(), holds_slot: false, has_started: false };
    s.wait_for_slot(false, None).await;
    s
}

fn held_slots(q: &Queries, user: &str) -> usize {
    q.state.lock().held_slots(user)
}

#[test]
fn freed_slot_goes_to_the_user_with_the_fewest_slots() {
    crate::sim::cost::run_paused(async {
        let q = queries(2);
        let a1 = acquire_slot(&q, "a").await;
        let _a2 = acquire_slot(&q, "a").await;
        let qa = q.clone();
        let a3 = tokio::spawn(async move { acquire_slot(&qa, "a").await });
        tokio::task::yield_now().await;
        let qb = q.clone();
        let b = tokio::spawn(async move { acquire_slot(&qb, "b").await });
        tokio::task::yield_now().await;
        drop(a1);
        let b = b.await.unwrap();
        assert_eq!(
            (held_slots(&q, "a"), held_slots(&q, "b")),
            (1, 1),
            "the earlier waiter of a busier user got the slot"
        );
        drop(b);
        let _a3 = a3.await.unwrap();
        assert_eq!(held_slots(&q, "a"), 2);
    });
}

#[test]
fn busy_user_yields_a_slot_to_a_waiting_user() {
    crate::sim::cost::run_paused(async {
        let q = queries(1);
        let mut a = acquire_slot(&q, "a").await;
        a.yield_to_waiters(None).await;
        assert!(a.holds_slot, "yielded without a waiter");
        let qb = q.clone();
        let b = tokio::spawn(async move {
            let mut b = acquire_slot(&qb, "b").await;
            b.yield_to_waiters(None).await;
            b
        });
        tokio::task::yield_now().await;
        a.yield_to_waiters(None).await;
        assert!(a.holds_slot && held_slots(&q, "a") == 1, "the slot did not return after the other user yielded");
        drop(a);
        let b = b.await.unwrap();
        assert!(b.holds_slot && held_slots(&q, "b") == 1);
    });
}

#[test]
fn abandoned_wait_returns_its_slot() {
    crate::sim::cost::run_paused(async {
        let q = queries(1);
        let a = acquire_slot(&q, "a").await;
        let qb = q.clone();
        let b = tokio::spawn(async move { acquire_slot(&qb, "b").await });
        tokio::task::yield_now().await;
        drop(a);
        b.abort();
        let _ = b.await;
        assert_eq!(q.state.lock().free_slots, 1, "an abandoned grant leaked a slot");
        let _c = tokio::time::timeout(Duration::from_secs(1), acquire_slot(&q, "c")).await.expect("slot is free");
    });
}

#[test]
fn uneven_slot_split_does_not_trade_slots() {
    crate::sim::cost::run_paused(async {
        let q = queries(3);
        let mut a1 = acquire_slot(&q, "a").await;
        let _a2 = acquire_slot(&q, "a").await;
        let mut b1 = acquire_slot(&q, "b").await;
        let (qa, qb) = (q.clone(), q.clone());
        let a3 = tokio::spawn(async move { acquire_slot(&qa, "a").await });
        let b2 = tokio::spawn(async move { acquire_slot(&qb, "b").await });
        tokio::task::yield_now().await;
        a1.yield_to_waiters(None).await;
        b1.yield_to_waiters(None).await;
        assert!(a1.holds_slot && b1.holds_slot, "a 2/1 split traded a slot");
        assert_eq!((held_slots(&q, "a"), held_slots(&q, "b")), (2, 1));
        a3.abort();
        b2.abort();
    });
}

#[test]
fn yielded_query_resumes_before_new_queries_of_its_user() {
    crate::sim::cost::run_paused(async {
        let q = queries(1);
        let mut a1 = acquire_slot(&q, "a").await;
        let qa = q.clone();
        let a2 = tokio::spawn(async move { acquire_slot(&qa, "a").await });
        tokio::task::yield_now().await;
        let qb = q.clone();
        let b = tokio::spawn(async move { acquire_slot(&qb, "b").await });
        tokio::task::yield_now().await;
        let resumed = tokio::spawn(async move {
            a1.yield_to_waiters(None).await;
            a1
        });
        tokio::task::yield_now().await;
        drop(b.await.unwrap());
        let a1 = resumed.await.unwrap();
        assert!(a1.holds_slot, "a new query of the user started ahead of its yielded query");
        drop(a1);
        drop(a2.await.unwrap());
    });
}

fn query_memory(seq: u64, limit_bytes: usize) -> Arc<QueryMemory> {
    Arc::new(QueryMemory {
        seq,
        user_id: "a".into(),
        bytes: AtomicUsize::new(0),
        charge: Mutex::new(Charge::default()),
        is_evicted: AtomicBool::new(false),
        evicted: tokio::sync::Notify::new(),
        failure: Mutex::new(None),
        txns: Weak::new(),
        step_bytes: 1,
        limit_bytes,
    })
}

#[test]
fn query_over_the_budget_fails_and_stays_failed() {
    let memory = query_memory(1, 1_000);
    assert_eq!(memory.resize(0, 1_000), Ok(()));
    assert!(matches!(memory.resize(1_000, 1_001), Err(Error::TooLarge(_))));
    assert!(matches!(memory.resize(1_001, 10), Err(Error::TooLarge(_))), "a failed query kept running");
    assert!(memory.is_stopping());
}

#[test]
fn evicted_query_is_releasing_and_not_evicted_again() {
    let q = queries(1);
    let (old, young) = (query_memory(1, usize::MAX), query_memory(2, usize::MAX));
    let m = Arc::new(Memory::new(1 << 20));
    for (memory, bytes) in [(&old, 300), (&young, 500)] {
        *memory.charge.lock() = Charge::new(&m, bytes);
        q.running.lock().insert(memory.seq, memory.clone());
    }
    young.cancel();
    assert_eq!(q.running().iter().map(|r| r.seq).collect::<Vec<_>>(), vec![1], "an evicted query is a candidate");
    assert_eq!(q.releasing_bytes(), 500, "the evicted query's memory is not counted as releasing");
    crate::sim::cost::run_paused(async {
        let mut a = acquire_slot(&q, "a").await;
        let mut b = QuerySlot { queries: Some(q.clone()), user_id: "b".into(), holds_slot: false, has_started: false };
        let waiting = b.wait_for_slot(false, Some(&young));
        assert!(tokio::time::timeout(Duration::from_secs(1), waiting).await.is_ok(), "an evicted query kept waiting");
        let later = query_memory(3, usize::MAX);
        let evict = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            later.cancel();
        };
        let waiting = tokio::time::timeout(Duration::from_secs(1), b.wait_for_slot(false, Some(&later)));
        let (woke, ()) = tokio::join!(waiting, evict);
        assert!(woke.is_ok(), "a query evicted while waiting for a slot kept waiting");
        assert!(!b.holds_slot);
        a.yield_to_waiters(None).await;
        assert!(a.holds_slot);
    });
}

/// Runs 1 query of 50 parts per user that yields after each 1 ms part read. Returns the elapsed
/// time to first slot grant and to completion for each user.
fn slot_times(users: usize, slots: usize) -> Vec<(Duration, Duration)> {
    crate::sim::cost::run_paused(async move {
        let q = queries(slots);
        let started = tokio::time::Instant::now();
        let tasks: Vec<_> = (0..users)
            .map(|u| {
                let q = q.clone();
                tokio::spawn(async move {
                    let mut s = acquire_slot(&q, &format!("u{u}")).await;
                    let first = started.elapsed();
                    for _ in 0..50 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        s.yield_to_waiters(None).await;
                        assert!(q.state.lock().started_queries <= STARTED_PER_SLOT * slots, "too many queries started");
                    }
                    (first, started.elapsed())
                })
            })
            .collect();
        let mut out = Vec::new();
        for t in tasks {
            out.push(t.await.unwrap());
        }
        out
    })
}

#[test]
fn every_waiting_user_gets_a_slot_while_others_take_turns() {
    for (users, slots) in [(2, 1), (4, 2), (5, 3), (18, 16), (32, 16)] {
        let times = slot_times(users, slots);
        let latest = times.iter().map(|t| t.0).max().unwrap();
        assert!(
            latest <= Duration::from_millis(2 * users as u64),
            "{users} users on {slots} slots: a user first got a slot after {latest:?}: {times:?}"
        );
    }
}

#[test]
fn overload_finishes_the_started_queries_first() {
    let times = slot_times(5, 1);
    let mut done: Vec<Duration> = times.iter().map(|t| t.1).collect();
    done.sort();
    assert!(done[1] <= Duration::from_millis(110), "the 2 started queries finished after {:?}", done[1]);
    assert!(times.iter().filter(|t| t.0 >= done[0]).count() >= 3, "more than 2 queries started at once: {times:?}");
}
