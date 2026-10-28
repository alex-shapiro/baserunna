//! Per-node memory accounting and max-min fair allocation across users for interactive
//! transactions (SPEC §5.12).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

pub(crate) const ALLOC_OVERHEAD: usize = 16;

pub(crate) fn alloc_bytes(len: usize) -> usize {
    if len == 0 { 0 } else { len + ALLOC_OVERHEAD }
}

#[derive(Debug)]
pub struct Memory {
    limit: usize,
    used: AtomicUsize,
}

impl Memory {
    pub fn new(limit: usize) -> Memory {
        Memory { limit, used: AtomicUsize::new(0) }
    }

    pub fn used(&self) -> usize {
        self.used.load(Relaxed)
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn is_exceeded(&self) -> bool {
        self.used() > self.limit
    }

    pub(crate) fn add(&self, n: usize) {
        self.used.fetch_add(n, Relaxed);
    }

    pub(crate) fn sub(&self, n: usize) {
        self.used.fetch_sub(n, Relaxed);
    }
}

/// RAII guard that charges bytes to a [`Memory`] budget and releases them on drop. Cloning a
/// `Charge` adds an equal charge to the budget.
#[derive(Debug, Default)]
pub struct Charge {
    memory: Option<Arc<Memory>>,
    bytes: usize,
    meter: Option<Arc<AtomicUsize>>,
}

impl Charge {
    pub fn new(memory: &Arc<Memory>, bytes: usize) -> Charge {
        memory.add(bytes);
        Charge { memory: Some(memory.clone()), bytes, meter: None }
    }

    pub fn add(&mut self, bytes: usize) {
        self.set(self.bytes + bytes);
    }

    pub fn set(&mut self, bytes: usize) {
        if let Some(m) = &self.memory {
            m.add(bytes);
            m.sub(self.bytes);
        }
        self.bytes = bytes;
        if let Some(meter) = &self.meter {
            meter.store(bytes, Relaxed);
        }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Returns a shared atomic counter that tracks the charged byte count and is reset to 0 on
    /// drop.
    pub fn meter(&mut self) -> Arc<AtomicUsize> {
        let bytes = self.bytes;
        self.meter.get_or_insert_with(|| Arc::new(AtomicUsize::new(bytes))).clone()
    }
}

impl Clone for Charge {
    fn clone(&self) -> Charge {
        match &self.memory {
            Some(m) => Charge::new(m, self.bytes),
            None => Charge { memory: None, bytes: self.bytes, meter: None },
        }
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        if let Some(m) = &self.memory {
            m.sub(self.bytes);
        }
        if let Some(meter) = &self.meter {
            meter.store(0, Relaxed);
        }
    }
}

/// Admission check for incremental transaction memory growth.
pub trait MemoryGate: Send + Sync {
    /// Returns an error if additional memory growth is rejected.
    fn admit(&self) -> crate::error::Result<()>;

    /// Byte increment between admission checks.
    fn step_bytes(&self) -> usize;
}

/// Memory usage of an open interactive transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Held<K> {
    pub holder: K,
    pub bytes: usize,
    /// True if the transaction is idle and can be aborted to reclaim memory.
    pub evictable: bool,
}

/// Computes the max-min fair share of `available` bytes for a requester, given the byte usage of
/// all other holders in `others`.
///
/// Returns the largest allowance `L` such that `L + sum(min(d, L) for d in others) <= available`.
pub fn fair_share(mut available: usize, mut others: Vec<usize>) -> usize {
    others.sort_unstable();
    let mut claimants = others.len() + 1;
    for d in others {
        if d.saturating_mul(claimants) > available {
            break;
        }
        available -= d;
        claimants -= 1;
    }
    available / claimants
}

/// Checks whether `requester` can allocate transaction memory if total node memory `used`
/// exceeds `budget`, given the open transactions in `held` ordered by eviction priority.
///
/// Bytes not tracked in `held` are deducted from `budget`, and the remainder is divided among
/// holders by [`fair_share`]. If `requester` uses at least its fair share, returns `None`.
/// Otherwise, returns the indices in `held` of evictable transactions to abort until `used` is
/// within `budget`. In each step, the earliest evictable transaction of the holder with the
/// largest usage is selected, provided that aborting it moves that holder's usage closer to the
/// fair share.
pub fn fair_admission<K: Ord>(budget: usize, used: usize, requester: &K, held: &[Held<K>]) -> Option<Vec<usize>> {
    let mut usage: BTreeMap<&K, usize> = BTreeMap::new();
    for h in held {
        *usage.entry(&h.holder).or_default() += h.bytes;
    }
    let untracked = used.saturating_sub(usage.values().sum());
    let mine = usage.get(requester).copied().unwrap_or(0);
    let others = usage.iter().filter(|(k, _)| **k != requester).map(|(_, u)| *u).collect();
    let share = fair_share(budget.saturating_sub(untracked), others);
    if mine >= share {
        return None;
    }
    let mut candidates: BTreeMap<&K, Vec<usize>> = BTreeMap::new();
    for (i, h) in held.iter().enumerate() {
        if h.evictable && h.holder != *requester {
            candidates.entry(&h.holder).or_default().push(i);
        }
    }
    let mut excess = used.saturating_sub(budget);
    let mut evict = Vec::new();
    while excess > 0 {
        let mut victim: Option<(&K, usize)> = None;
        for (k, txns) in &candidates {
            let over = usage[k].saturating_sub(share).saturating_mul(2);
            let Some(pos) = txns.iter().position(|&i| held[i].bytes < over) else { continue };
            if victim.is_none_or(|(v, _)| usage[k] > usage[v]) {
                victim = Some((k, pos));
            }
        }
        let Some((k, pos)) = victim else { break };
        let i = candidates.get_mut(k).map(|t| t.remove(pos)).unwrap_or(pos);
        excess = excess.saturating_sub(held[i].bytes);
        if let Some(u) = usage.get_mut(k) {
            *u -= held[i].bytes;
        }
        evict.push(i);
    }
    Some(evict)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::prop;
    use crate::util::rng::Rng;

    #[test]
    fn charges_are_released_on_drop() {
        let m = Arc::new(Memory::new(100));
        let mut a = Charge::new(&m, 60);
        assert!(!m.is_exceeded());
        a.add(50);
        assert_eq!(m.used(), 110);
        assert!(m.is_exceeded());
        let b = Charge::new(&m, 5);
        a.set(10);
        assert_eq!(m.used(), 15);
        drop(a);
        drop(b);
        assert_eq!(m.used(), 0);
        let mut free = Charge::default();
        free.add(7);
        assert_eq!((free.bytes(), m.used()), (7, 0));
    }

    #[test]
    fn meters_follow_their_charge() {
        let m = Arc::new(Memory::new(100));
        let mut a = Charge::new(&m, 10);
        let meter = a.meter();
        assert_eq!(meter.load(Relaxed), 10);
        a.add(5);
        assert_eq!(meter.load(Relaxed), 15);
        let copy = a.clone();
        a.set(3);
        assert_eq!((meter.load(Relaxed), m.used()), (3, 18));
        drop(copy);
        assert_eq!(meter.load(Relaxed), 3, "a clone moved the original's meter");
        drop(a);
        assert_eq!((meter.load(Relaxed), m.used()), (0, 0));
    }

    #[test]
    fn fair_share_splits_remaining_budget() {
        assert_eq!(fair_share(300, vec![]), 300);
        assert_eq!(fair_share(300, vec![500, 500]), 100);
        assert_eq!(fair_share(300, vec![20, 500]), 140);
        assert_eq!(fair_share(300, vec![20, 30]), 250);
        assert_eq!(fair_share(0, vec![1]), 0);
    }

    #[test]
    fn fair_admission_evicts_lru_from_heaviest_holder() {
        let h = |holder: &'static str, bytes, evictable| Held { holder, bytes, evictable };
        let held = [h("hog", 50, true), h("hog", 100, false), h("small", 30, true), h("hog", 80, true)];
        assert_eq!(fair_admission(200, 300, &"new", &held), Some(vec![0, 3]));
        assert_eq!(fair_admission(200, 300, &"small", &held), Some(vec![0, 3]));
        assert_eq!(fair_admission(200, 300, &"hog", &held), None);
        assert_eq!(fair_admission(200, 240, &"new", &held), Some(vec![0]));
        assert_eq!(fair_admission(200, 200, &"new", &held), Some(vec![]));
        let fair = [h("over", 120, true), h("other", 90, true)];
        assert_eq!(fair_admission(200, 210, &"new", &fair), Some(vec![]), "evicted a holder just past its share");
    }

    #[derive(Clone, Debug)]
    struct Case {
        budget: usize,
        untracked: usize,
        requester: u8,
        held: Vec<Held<u8>>,
    }

    fn gen_case(r: &mut Rng) -> Case {
        let holders = 1 + r.below(5) as u8;
        let held = (0..r.usize(16))
            .map(|_| Held { holder: r.below(holders as u64) as u8, bytes: r.usize(300), evictable: r.chance(0.7) })
            .collect();
        Case { budget: r.usize(1_200), untracked: r.usize(400), requester: r.below(holders as u64 + 1) as u8, held }
    }

    fn shrink_case(c: &Case) -> Vec<Case> {
        let mut out: Vec<Case> = prop::shrink_vec(&c.held).into_iter().map(|held| Case { held, ..c.clone() }).collect();
        out.push(Case { untracked: 0, ..c.clone() });
        out.push(Case { budget: c.budget / 2, ..c.clone() });
        out
    }

    fn usage(held: &[Held<u8>]) -> BTreeMap<u8, usize> {
        let mut u = BTreeMap::new();
        for h in held {
            *u.entry(h.holder).or_default() += h.bytes;
        }
        u
    }

    #[test]
    fn prop_fair_share_is_max_feasible_allowance() {
        prop::check(
            "fair_share_is_max_feasible_allowance",
            3_000,
            |r| (r.usize(2_000), (0..r.usize(8)).map(|_| r.usize(600)).collect::<Vec<usize>>()),
            |(a, d)| prop::shrink_vec(d).into_iter().map(|d| (*a, d)).collect(),
            |(available, others)| {
                let fits = |l: usize| l + others.iter().map(|d| (*d).min(l)).sum::<usize>() <= *available;
                let best = (0..=*available).rev().find(|l| fits(*l)).unwrap_or(0);
                let got = fair_share(*available, others.clone());
                if got != best {
                    return Err(format!("fair share {got}, largest allowance that fits {best}"));
                }
                Ok(())
            },
        );
    }

    fn moves_closer_to_share(usage: usize, share: usize, bytes: usize) -> bool {
        usage > share && bytes < 2 * (usage - share)
    }

    #[test]
    fn prop_admission_is_max_min_fair() {
        prop::check("admission_is_max_min_fair", 5_000, gen_case, shrink_case, |c| {
            let total: usize = c.held.iter().map(|h| h.bytes).sum();
            let used = total + c.untracked;
            let mut usage = usage(&c.held);
            let mine = usage.get(&c.requester).copied().unwrap_or(0);
            let others = usage.iter().filter(|(k, _)| **k != c.requester).map(|(_, u)| *u).collect();
            let share = fair_share(c.budget.saturating_sub(c.untracked), others);
            let Some(evicted) = fair_admission(c.budget, used, &c.requester, &c.held) else {
                return match mine >= share {
                    true => Ok(()),
                    false => Err(format!("rejected a requester holding {mine} below the fair share {share}")),
                };
            };
            if mine >= share {
                return Err(format!("admitted a requester holding {mine} at or above the fair share {share}"));
            }
            let mut excess = used.saturating_sub(c.budget);
            let mut taken = std::collections::BTreeSet::new();
            let open = |taken: &std::collections::BTreeSet<usize>, usage: &BTreeMap<u8, usize>| {
                c.held
                    .iter()
                    .enumerate()
                    .filter(|(j, e)| e.evictable && e.holder != c.requester && !taken.contains(j))
                    .filter(|(_, e)| moves_closer_to_share(usage[&e.holder], share, e.bytes))
                    .map(|(j, _)| j)
                    .collect::<Vec<usize>>()
            };
            for (n, &i) in evicted.iter().enumerate() {
                let h = c.held.get(i).ok_or("evicted a transaction that does not exist")?;
                let eligible = open(&taken, &usage);
                if !eligible.contains(&i) {
                    return Err(format!("evicted ineligible {h:?} (index {i}), share {share}, usage {usage:?}"));
                }
                if excess == 0 {
                    return Err(format!("eviction {n} came after usage was back within the budget"));
                }
                if eligible.iter().any(|&j| j < i && c.held[j].holder == h.holder) {
                    return Err(format!("evicted index {i} ahead of an earlier eligible transaction"));
                }
                let heaviest = eligible.iter().map(|&j| usage[&c.held[j].holder]).max().unwrap_or(0);
                if usage[&h.holder] < heaviest {
                    return Err(format!("evicted from holder {} below the heaviest {heaviest}", h.holder));
                }
                taken.insert(i);
                *usage.get_mut(&h.holder).unwrap() -= h.bytes;
                excess = excess.saturating_sub(h.bytes);
            }
            if excess > 0 && !open(&taken, &usage).is_empty() {
                return Err(format!("stopped {excess} bytes over the budget with eligible transactions left"));
            }
            Ok(())
        });
    }

    #[test]
    fn prop_new_holder_reclaims_budget_from_heavy_holder() {
        prop::check(
            "new_holder_reclaims_budget_from_heavy_holder",
            2_000,
            |r| {
                let hog: Vec<usize> = (0..1 + r.usize(12)).map(|_| 1 + r.usize(200)).collect();
                let budget = 2 + hog.iter().sum::<usize>() / (1 + r.usize(3)) + r.usize(50);
                (budget, hog)
            },
            |(b, h)| prop::shrink_vec(h).into_iter().filter(|h| !h.is_empty()).map(|h| (*b, h)).collect(),
            |(budget, hog)| {
                let mut held: Vec<Held<u8>> =
                    hog.iter().map(|&bytes| Held { holder: 1, bytes, evictable: true }).collect();
                let used = |held: &[Held<u8>]| held.iter().map(|h| h.bytes).sum::<usize>();
                if used(&held) > *budget && fair_admission(*budget, used(&held), &1, &held).is_some() {
                    return Err("admitted the only holder while over budget".into());
                }
                let evicted =
                    fair_admission(*budget, used(&held), &0, &held).ok_or("rejected a holder with 0 bytes")?;
                let mut i = 0;
                held.retain(|_| (!evicted.contains(&i), i += 1).0);
                let (left, largest) = (used(&held), held.iter().map(|h| h.bytes).max().unwrap_or(0));
                let share = fair_share(*budget, vec![left]);
                if left > *budget && left > share + largest / 2 {
                    return Err(format!("hog kept {left} over budget {budget} and fair share {share}"));
                }
                Ok(())
            },
        );
    }
}
