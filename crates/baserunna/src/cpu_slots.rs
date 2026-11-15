//! CPU slots that guest work of all owners shares in turn by owner.
//!
//! A guest call holds a slot only while the executor polls it. Fuel makes a
//! running instance yield after a bounded slice, and each poll that returns
//! `Pending` gives the slot back. The next slot goes to the owner after the
//! last one served, so one owner's many calls wait behind each other owner.

use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// CPU slots of one node.
pub struct CpuSlots {
    state: Mutex<SlotState>,
}

#[derive(Default)]
struct SlotState {
    free: usize,
    next_ticket: u64,
    /// Owners with waiting calls, in the order they get the next slot.
    turn: VecDeque<String>,
    waiting: HashMap<String, VecDeque<(u64, Waker)>>,
    /// Tickets that received a slot and have not polled since.
    granted: std::collections::HashSet<u64>,
}

impl CpuSlots {
    pub fn new(slots: usize) -> Arc<CpuSlots> {
        Arc::new(CpuSlots { state: Mutex::new(SlotState { free: slots.max(1), ..SlotState::default() }) })
    }

    /// Runs `work` for `owner`, polling it only while it holds a slot.
    pub fn run<F: Future>(self: &Arc<Self>, owner: &str, work: F) -> SlotFuture<F> {
        SlotFuture { slots: self.clone(), owner: owner.to_string(), ticket: None, work: Box::pin(work) }
    }

    /// Takes a slot for `ticket`, or queues `waker` behind the owners already waiting.
    fn acquire(&self, owner: &str, ticket: u64, waker: &Waker) -> bool {
        let mut s = self.state.lock();
        if s.granted.remove(&ticket) {
            return true;
        }
        if s.free > 0 && s.turn.is_empty() {
            s.free -= 1;
            return true;
        }
        let queue = s.waiting.entry(owner.to_string()).or_default();
        match queue.iter_mut().find(|(t, _)| *t == ticket) {
            Some((_, w)) => w.clone_from(waker),
            None => {
                queue.push_back((ticket, waker.clone()));
                if queue.len() == 1 {
                    s.turn.push_back(owner.to_string());
                }
            }
        }
        false
    }

    /// Gives a slot back, to the next owner in turn if one waits.
    fn release(&self) {
        let mut s = self.state.lock();
        while let Some(owner) = s.turn.pop_front() {
            let Some(queue) = s.waiting.get_mut(&owner) else { continue };
            let Some((ticket, waker)) = queue.pop_front() else {
                s.waiting.remove(&owner);
                continue;
            };
            if queue.is_empty() {
                s.waiting.remove(&owner);
            } else {
                s.turn.push_back(owner);
            }
            s.granted.insert(ticket);
            drop(s);
            waker.wake();
            return;
        }
        s.free += 1;
    }

    /// Removes `ticket` from the queue, or returns its unused grant.
    fn cancel(&self, owner: &str, ticket: u64) {
        let mut s = self.state.lock();
        if s.granted.remove(&ticket) {
            drop(s);
            self.release();
            return;
        }
        if let Some(queue) = s.waiting.get_mut(owner) {
            queue.retain(|(t, _)| *t != ticket);
            if queue.is_empty() {
                s.waiting.remove(owner);
                s.turn.retain(|o| o != owner);
            }
        }
    }

    fn ticket(&self) -> u64 {
        let mut s = self.state.lock();
        s.next_ticket += 1;
        s.next_ticket
    }
}

/// Future of guest work that runs only while it holds a CPU slot.
pub struct SlotFuture<F> {
    slots: Arc<CpuSlots>,
    owner: String,
    ticket: Option<u64>,
    work: Pin<Box<F>>,
}

impl<F: Future> Future for SlotFuture<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = &mut *self;
        let ticket = *this.ticket.get_or_insert_with(|| this.slots.ticket());
        if !this.slots.acquire(&this.owner, ticket, cx.waker()) {
            return Poll::Pending;
        }
        let out = this.work.as_mut().poll(cx);
        this.slots.release();
        if out.is_ready() {
            this.ticket = None;
        }
        out
    }
}

impl<F> Drop for SlotFuture<F> {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket {
            self.slots.cancel(&self.owner, ticket);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns the ticket among `tickets` that holds a grant.
    fn granted(slots: &CpuSlots, tickets: &[(&str, u64)]) -> Vec<String> {
        let s = slots.state.lock();
        tickets.iter().filter(|(_, t)| s.granted.contains(t)).map(|(o, t)| format!("{o}{t}")).collect()
    }

    #[test]
    fn slots_go_to_owners_in_turn() {
        let slots = CpuSlots::new(1);
        let waker = Waker::noop();
        assert!(slots.acquire("a", 0, waker), "a free slot goes to the first caller");
        let waiting = [("a", 1), ("a", 2), ("a", 3), ("b", 4)];
        for (owner, ticket) in waiting {
            assert!(!slots.acquire(owner, ticket, waker), "no slot is free");
        }
        let mut order = Vec::new();
        for _ in 0..waiting.len() {
            slots.release();
            let g = granted(&slots, &waiting);
            assert_eq!(g.len(), 1, "one release grants one slot");
            let (owner, ticket) = waiting.iter().find(|(o, t)| format!("{o}{t}") == g[0]).unwrap();
            assert!(slots.acquire(owner, *ticket, waker), "the granted call takes its slot");
            order.push(g[0].clone());
        }
        assert_eq!(order, ["a1", "b4", "a2", "a3"], "owner b waits behind one call of owner a, not three");
        slots.release();
        assert_eq!(slots.state.lock().free, 1, "every slot came back");
    }

    #[test]
    fn dropped_call_returns_its_grant() {
        let slots = CpuSlots::new(1);
        let waker = Waker::noop();
        assert!(slots.acquire("a", 0, waker));
        assert!(!slots.acquire("b", 1, waker));
        slots.release();
        slots.cancel("b", 1);
        assert_eq!(slots.state.lock().free, 1);
    }
}
