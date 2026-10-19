//! Probes mark rare situations in service code that the simulator must reach
//! (SPEC §13.4). A probe counts its hits on the current thread while a
//! recording is active (every simulation run starts one) and does nothing
//! otherwise. The simulator fails a campaign that never reaches a probe, so
//! each one names a situation worth testing, not a line of code.

use std::cell::RefCell;
use tokio::time::Instant;

macro_rules! probes {
    ($($(#[doc = $doc:literal])* $name:ident,)*) => {
        /// A rare situation the simulator must reach.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum Probe {
            $($(#[doc = $doc])* $name,)*
        }

        impl Probe {
            pub const ALL: &[Probe] = &[$(Probe::$name,)*];

            pub fn name(self) -> &'static str {
                match self {
                    $(Probe::$name => stringify!($name),)*
                }
            }

            /// What the probe's situation is.
            pub fn doc(self) -> &'static str {
                match self {
                    $(Probe::$name => concat!($($doc),*).trim_ascii(),)*
                }
            }
        }
    };
}

probes! {
    // Storage engine: commits.
    /// A commit batch accepted two or more transactions.
    CommitBatched,
    /// A commit found its commit record already applied by an earlier attempt.
    CommitReplayed,
    /// A transaction reached the committer after its commit deadline.
    CommitDeadlinePassed,
    /// Commit validation found a conflict.
    CommitConflict,
    /// A transaction conflicted with an earlier transaction of its own batch.
    CommitConflictInBatch,
    /// A conditional manifest write lost to another writer (412).
    ManifestCasLost,
    /// A manifest write timed out or failed with an unknown outcome.
    ManifestWriteAmbiguous,
    /// A manifest reloaded after an ambiguous write was that write: it had landed.
    AmbiguousWriteLanded,
    /// A commit found another node's valid lease and failed with `not_owner`.
    CommitNotOwner,
    /// A commit found a foreign lease after an earlier attempt of its batch may have landed.
    CommitUnknownAfterLeaseMoved,
    /// A batch exhausted its commit retries.
    CommitRetriesExhausted,
    /// A commit took over a database whose previous lease had expired.
    LeaseTakeover,
    /// Another node's lease was found expired by the store clock.
    ExpiredLeaseByStoreClock,
    /// A lease was renewed for strong reads alone.
    ReadLeaseRenewed,
    /// A node released its lease.
    LeaseReleased,
    /// A manifest write that shortens or releases this node's lease was sent,
    /// so the node stopped relying on its longer lease.
    LeaseShortenedInFlight,
    /// A strong read waited for this node's in-flight manifest write.
    StrongReadWaitedForWrite,
    /// A version older than the installed one arrived and was ignored.
    StaleVersionIgnored,
    /// Creating a manifest raced another creator or an ambiguous create.
    ManifestCreateRaced,
    /// A commit or lease request reached a database marked deleted.
    DeletedDatabaseWrite,
    /// A manifest write was hedged after a slow store response.
    ManifestPutHedged,
    /// The hedged duplicate of a manifest write was the one that succeeded.
    HedgeWon,
    /// A hedged duplicate was rejected while the first write was still in flight.
    HedgeRejectedFirstInFlight,
    /// A transaction began while a manifest write was in flight on this node.
    OptimisticBegin,
    /// An optimistic transaction was re-executed after failing revalidation.
    OptimisticRerun,
    /// A commit was refused for the size quota.
    QuotaRefused,
    /// A commit bound a staged blob that had expired or been collected.
    StagedBlobUnbindable,
    // Storage engine: tail, segments, compaction and GC.
    /// The tail was flushed to level 0 segments.
    TailFlushed,
    /// Sealing stream chunks alone made room in the tail.
    TailSealedInPlace,
    /// Old tombstones were purged from the tail.
    TailTombstonesPurged,
    /// A range tombstone was written.
    RangeTombstoneWritten,
    /// A range tombstone was retired.
    RangeTombstoneRetired,
    /// A segment fully covered by a range tombstone was dropped.
    SegmentDropped,
    /// A segment was rewritten to purge range-deleted entries.
    SegmentRewritten,
    /// A levels object was written.
    LevelsObjectWritten,
    /// Entries were dropped or reduced to tombstones because they expired.
    EntriesExpired,
    /// A segment was rewritten or dropped because its entries expired.
    SegmentExpired,
    /// A stream chunk was sealed by a flush.
    ChunkSealedAtFlush,
    /// A stream chunk was sealed by a compaction.
    ChunkSealedInCompaction,
    /// A compaction was installed.
    CompactionInstalled,
    /// A compaction moved a segment without rewriting it.
    CompactionMoved,
    /// A compaction install found its inputs changed.
    CompactionInputsChanged,
    /// A compaction install found that an earlier, ambiguous attempt of it had landed.
    CompactionInstallReplayed,
    /// A compaction stopped because its node no longer held the lease.
    CompactionLostLease,
    /// A sweep marked unreferenced objects obsolete.
    OrphansMarked,
    /// Obsolete objects were deleted after a manifest write with an unknown outcome.
    ObsoleteDeletedAfterAmbiguousWrite,
    /// Garbage collection found a blob pinned by a long read and kept it.
    BlobPinnedAtCollection,
    /// A long blob read wrote a pin.
    BlobPinWritten,
    /// A stream retention trim was committed.
    RetentionTrimmed,
    /// A retention trim was left to another node holding the lease.
    RetentionTrimLeftToOwner,
    // Cluster and API.
    /// A duplicate forwarded request was answered with the first copy's response.
    ForwardDuplicateReplayed,
    /// A duplicate forwarded request arrived while the first copy's outcome was unknown.
    ForwardDuplicateUnknown,
    /// A forwarded request addressed to a previous process at this address was refused.
    ForwardToPreviousProcess,
    /// A forward certainly failed and the request was handled locally.
    ForwardFailedHandledLocally,
    /// A forwarded write may have been delivered, leaving its outcome unknown.
    ForwardAmbiguous,
    /// A membership entry with this node's own address was ignored.
    OwnAddressIgnored,
    /// A request met a lease holder missing from the members and refreshed them.
    MembersRefreshedForUnknownPeer,
    /// A peer was marked suspect after an undelivered forward.
    PeerSuspected,
    /// A request found its database's manifest deleted and finished the deletion.
    DeletionRepaired,
    /// A cached database name pointed at a deleted database and was resolved again.
    NameResolvedAgain,
    /// A metadata command was forwarded to the system database's lease holder.
    MetaForwarded,
    /// A transaction was retried server-side after a conflict.
    TxnRetriedOnConflict,
    /// A retried idempotent request that failed a precondition was found already applied.
    PreconditionExplainedByReplay,
    /// An interactive transaction was not found on the node asked to continue it.
    InteractiveTxnLost,
    /// A retried password reset found its new password already set and finished.
    PasswordResetFinished,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Hits {
    /// Hit counts, indexed like [`Probe::ALL`].
    pub counts: Vec<u64>,
    /// Milliseconds from the start of the recording to each probe's first hit.
    pub first_ms: Vec<Option<u64>>,
}

impl Hits {
    pub fn count(&self, p: Probe) -> u64 {
        self.counts.get(p as usize).copied().unwrap_or(0)
    }

    pub fn first_ms(&self, p: Probe) -> Option<u64> {
        self.first_ms.get(p as usize).copied().flatten()
    }

    /// The probes hit at least once.
    pub fn hit(&self) -> impl Iterator<Item = Probe> + '_ {
        Probe::ALL.iter().copied().filter(|p| self.count(*p) > 0)
    }

    pub fn add(&mut self, o: &Hits) {
        let n = Probe::ALL.len();
        self.counts.resize(n, 0);
        self.first_ms.resize(n, None);
        for (i, c) in o.counts.iter().enumerate() {
            self.counts[i] += c;
        }
        for (i, t) in o.first_ms.iter().enumerate() {
            self.first_ms[i] = match (self.first_ms[i], *t) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
        }
    }
}

struct Recording {
    origin: Instant,
    hits: Hits,
}

thread_local! {
    static RECORDING: RefCell<Option<Recording>> = const { RefCell::new(None) };
}

/// Counts a hit of `p` in the current recording.
pub fn hit(p: Probe) {
    hit_n(p, 1);
}

/// Counts `n` hits of `p` in the current recording.
pub fn hit_n(p: Probe, n: u64) {
    if n == 0 {
        return;
    }
    RECORDING.with(|r| {
        if let Some(r) = r.borrow_mut().as_mut() {
            let i = p as usize;
            r.hits.counts[i] += n;
            if r.hits.first_ms[i].is_none() {
                r.hits.first_ms[i] = Some(r.origin.elapsed().as_millis() as u64);
            }
        }
    });
}

/// Starts a new recording on this thread, discarding any current one.
pub fn record() {
    let n = Probe::ALL.len();
    let hits = Hits { counts: vec![0; n], first_ms: vec![None; n] };
    RECORDING.with(|r| *r.borrow_mut() = Some(Recording { origin: Instant::now(), hits }));
}

/// Ends this thread's recording and returns its hits.
pub fn finish() -> Hits {
    RECORDING.with(|r| r.borrow_mut().take().map(|r| r.hits).unwrap_or_default())
}

/// Hits of `p` in this thread's recording so far.
pub fn count(p: Probe) -> u64 {
    RECORDING.with(|r| r.borrow().as_ref().map_or(0, |r| r.hits.count(p)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probes_count_only_while_recording() {
        hit(Probe::CommitBatched);
        record();
        assert_eq!(count(Probe::CommitBatched), 0);
        hit(Probe::CommitBatched);
        hit_n(Probe::TailFlushed, 3);
        let h = finish();
        assert_eq!((h.count(Probe::CommitBatched), h.count(Probe::TailFlushed)), (1, 3));
        assert_eq!(h.hit().collect::<Vec<_>>(), [Probe::CommitBatched, Probe::TailFlushed]);
        assert_eq!(count(Probe::CommitBatched), 0);
        assert!(Probe::ALL.iter().all(|p| !p.doc().is_empty()), "every probe says what it marks");
    }
}
