//! Host table of database leases, for routing without reading tenant data.
//!
//! Owner instances report each lease they hold or observe, with fixed-size
//! fields. The table has a fixed entry count, so its memory does not grow
//! with tenant activity.

use std::collections::{BTreeMap, HashMap};
use tokio::time::Instant;

/// Most databases whose leases the table keeps.
pub const LEASE_TABLE_ENTRIES: usize = 65_536;
/// Largest database id or node id that the table accepts.
pub const LEASE_ID_MAX: usize = 64;

/// Lease of one database, as an owner instance reported it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseEntry {
    pub holder: String,
    pub valid_until: Instant,
}

/// Leases by database id, with the oldest report dropped first when full.
pub struct LeaseTable {
    capacity: usize,
    entries: HashMap<String, (LeaseEntry, u64)>,
    by_age: BTreeMap<u64, String>,
    next_report: u64,
}

impl LeaseTable {
    pub fn new(capacity: usize) -> LeaseTable {
        LeaseTable { capacity: capacity.max(1), entries: HashMap::new(), by_age: BTreeMap::new(), next_report: 0 }
    }

    /// Records that `holder` holds the lease of `db` until `valid_until`.
    ///
    /// Returns false, and records nothing, if an id is longer than [`LEASE_ID_MAX`].
    pub fn report(&mut self, db: &str, holder: &str, valid_until: Instant) -> bool {
        if db.len() > LEASE_ID_MAX || holder.len() > LEASE_ID_MAX {
            return false;
        }
        self.clear(db);
        while self.entries.len() >= self.capacity {
            let Some((_, oldest)) = self.by_age.pop_first() else { break };
            self.entries.remove(&oldest);
        }
        let report = self.next_report;
        self.next_report += 1;
        self.entries.insert(db.to_string(), (LeaseEntry { holder: holder.to_string(), valid_until }, report));
        self.by_age.insert(report, db.to_string());
        true
    }

    /// Forgets the lease of `db`, as after a failed manifest PUT.
    pub fn clear(&mut self, db: &str) {
        if let Some((_, report)) = self.entries.remove(db) {
            self.by_age.remove(&report);
        }
    }

    /// Holder of the lease of `db` if that lease is still valid at `now`.
    pub fn holder(&self, db: &str, now: Instant) -> Option<&str> {
        self.entries.get(db).filter(|(e, _)| e.valid_until > now).map(|(e, _)| e.holder.as_str())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn lease_table_keeps_a_fixed_count_and_drops_the_oldest() {
        let now = Instant::now();
        let later = now + Duration::from_secs(5);
        let mut t = LeaseTable::new(2);
        assert!(t.report("a", "n1", later));
        assert!(t.report("b", "n2", later));
        assert!(t.report("c", "n1", later));
        assert_eq!(t.len(), 2);
        assert_eq!(t.holder("a", now), None, "the oldest report goes first");
        assert_eq!(t.holder("c", now), Some("n1"));
        assert_eq!(t.holder("c", later), None, "an expired lease has no holder");
        t.clear("b");
        assert_eq!(t.holder("b", now), None);
        assert!(!t.report(&"x".repeat(LEASE_ID_MAX + 1), "n1", later), "long ids are refused");
    }
}
