//! Execution probes for rare states in service code (SPEC §13.4).
//!
//! When recording is active on the current thread, each probe counts how many
//! times it is hit and records the timestamp of the first hit.

use std::cell::RefCell;
use tokio::time::Instant;

macro_rules! probes {
    ($($(#[doc = $doc:literal])* $name:ident,)*) => {
        /// An instrumented state in service code.
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

            pub fn doc(self) -> &'static str {
                match self {
                    $(Probe::$name => concat!($($doc),*).trim_ascii(),)*
                }
            }
        }
    };
}

probes! {
    /// Two or more transactions were committed in a single batch.
    CommitBatched,
    /// A commit was deduplicated by an existing commit record.
    CommitReplayed,
    /// A transaction was rejected because its commit deadline had passed.
    CommitDeadlinePassed,
    /// A request was cancelled because its deadline expired.
    RequestDeadlinePassed,
    /// A commit batch flushed the tail inline because background flushes fell behind.
    TailFlushedInCommit,
    /// A background tail flush was installed.
    TailFlushedInBackground,
    /// A background tail flush was discarded because the tail changed incompatibly.
    FlushSuperseded,
    /// A retried tail flush found that its output segments were already installed.
    FlushInstallReplayed,
    /// A manifest reload observed a newer etag and resolved an earlier ambiguous write.
    AmbiguousWriteSettled,
    /// A retention trim that failed with a retryable error was retried.
    RetentionTrimRetried,
    /// A failed commit batch returned the recorded outcome for a commit that was already decided in the manifest.
    DecidedCommitAnswered,
    /// A commit status check aborted an unapplied commit ID.
    CommitAborted,
    /// A commit was rejected because its commit ID was previously aborted.
    AbortedCommitRejected,
    /// A transaction was rejected due to a read-write conflict.
    CommitConflict,
    /// A transaction conflicted with an earlier transaction in the same batch.
    CommitConflictInBatch,
    /// A conditional manifest write was rejected with HTTP 412.
    ManifestCasLost,
    /// A manifest write failed with an ambiguous outcome.
    ManifestWriteAmbiguous,
    /// A manifest reload confirmed that a previous ambiguous write was applied.
    AmbiguousWriteLanded,
    /// A commit was rejected because another node held an active lease.
    CommitNotOwner,
    /// A commit batch encountered a foreign lease after an ambiguous write attempt.
    CommitUnknownAfterLeaseMoved,
    /// A commit batch exhausted its retry limit.
    CommitRetriesExhausted,
    /// A node acquired a lease after the previous lease expired.
    LeaseTakeover,
    /// A foreign lease was treated as expired based on the object-store clock.
    ExpiredLeaseByStoreClock,
    /// A lease was renewed to serve strong reads.
    ReadLeaseRenewed,
    /// A node released its lease.
    LeaseReleased,
    /// A node reduced its local lease validity when sending a manifest write.
    LeaseShortenedInFlight,
    /// A strong read waited for an in-flight manifest write on the same node.
    StrongReadWaitedForWrite,
    /// An older manifest version was ignored because a newer version was already installed.
    StaleVersionIgnored,
    /// Initial manifest creation raced with a concurrent write.
    ManifestCreateRaced,
    /// A write or lease request targeted a deleted database.
    DeletedDatabaseWrite,
    /// A duplicate manifest write was sent after the primary request exceeded the hedge delay.
    ManifestPutHedged,
    /// A hedged manifest write completed before the primary request.
    HedgeWon,
    /// A hedged manifest write was rejected with HTTP 412 while the primary request was in flight.
    HedgeRejectedFirstInFlight,
    /// A serializable transaction started while a manifest write was in flight.
    OptimisticBegin,
    /// An optimistic transaction was re-executed after failing strong snapshot validation.
    OptimisticRerun,
    /// A commit was rejected because the database exceeded its size quota.
    QuotaRefused,
    /// A commit was rejected because a staged blob had expired or was garbage-collected.
    StagedBlobUnbindable,
    /// In-memory tail entries were flushed to level-0 segments.
    TailFlushed,
    /// Sealing stream chunks freed enough space to keep remaining entries in the tail.
    TailSealedInPlace,
    /// Expired tombstones were removed from the tail.
    TailTombstonesPurged,
    /// A range tombstone was added to the manifest.
    RangeTombstoneWritten,
    /// An expired range tombstone was removed from the manifest.
    RangeTombstoneRetired,
    /// A segment covered by a range tombstone was dropped without rewrite.
    SegmentDropped,
    /// A segment was rewritten to remove entries covered by a range tombstone.
    SegmentRewritten,
    /// Deep level metadata was written to an external levels object.
    LevelsObjectWritten,
    /// Expired entries were removed or converted to tombstones.
    EntriesExpired,
    /// A segment was rewritten or dropped because its entries expired.
    SegmentExpired,
    /// Stream records were sealed into a chunk during a tail flush.
    ChunkSealedAtFlush,
    /// Stream records were sealed into a chunk during compaction.
    ChunkSealedInCompaction,
    /// Compaction output segments were installed in the manifest.
    CompactionInstalled,
    /// A segment was moved to the next level without rewrite.
    CompactionMoved,
    /// A compaction result was discarded because its input segments were already compacted.
    CompactionInputsChanged,
    /// A retried compaction install found that its output segments were already installed.
    CompactionInstallReplayed,
    /// A compaction task stopped because the node lost the database lease.
    CompactionLostLease,
    /// A background task skipped installing its result because its lease had expired.
    BackgroundInstallUnleased,
    /// A compaction result was discarded because the task exceeded the orphan grace period.
    CompactionOutlivedOrphanGrace,
    /// A read failed with a retryable error after garbage collection deleted a segment.
    SegmentCollectedUnderRead,
    /// A manifest fetch found its levels object deleted by garbage collection and fetched the manifest again.
    LevelsCollectedUnderRead,
    /// A garbage collection sweep marked unreferenced objects as obsolete.
    OrphansMarked,
    /// Obsolete objects past their grace period were deleted after an ambiguous manifest write.
    ObsoleteDeletedAfterAmbiguousWrite,
    /// Garbage collection skipped an obsolete blob that was pinned by an active read.
    BlobPinnedAtCollection,
    /// A long-running blob read wrote a pin object.
    BlobPinWritten,
    /// A stream was trimmed to enforce retention limits.
    RetentionTrimmed,
    /// A background retention trim exited because another node held the lease.
    RetentionTrimLeftToOwner,
    /// A duplicate forwarded request was answered from the deduplication cache.
    ForwardDuplicateReplayed,
    /// A duplicate forwarded request failed because the first attempt had an ambiguous outcome.
    ForwardDuplicateUnknown,
    /// A forwarded request was rejected because it targeted a previous process at the same address.
    ForwardToPreviousProcess,
    /// A request was handled locally after forwarding failed before delivery.
    ForwardFailedHandledLocally,
    /// A forwarded write failed after possible delivery to the target node.
    ForwardAmbiguous,
    /// A cluster membership record with the local node's address was ignored during routing.
    OwnAddressIgnored,
    /// A node refreshed cluster membership after observing a lease held by an unknown peer.
    MembersRefreshedForUnknownPeer,
    /// A peer node was marked suspect after a failed forward.
    PeerSuspected,
    /// A node reloaded the manifest for waiting requests after another node announced a commit.
    NoticedVersionLoaded,
    /// An interrupted database deletion was completed during routing.
    DeletionRepaired,
    /// A cached database name was resolved again after its target database was deleted.
    NameResolvedAgain,
    /// A metadata command was forwarded to the system database lease holder.
    MetaForwarded,
    /// A one-shot transaction was retried on the server after a conflict.
    TxnRetriedOnConflict,
    /// A retried idempotent transaction that failed a precondition check was resolved as already committed.
    PreconditionExplainedByReplay,
    /// An interactive transaction handle was not found.
    InteractiveTxnLost,
    /// A retried password reset with the same password completed the reset.
    PasswordResetFinished,
    /// A simulated workload resynchronized its model from the database after an unresolved ambiguous write.
    ModelAdoptedFromDatabase,
    /// An idle database manifest was unloaded to free memory.
    DatabaseUnloaded,
    /// An interactive transaction was rejected because the node memory budget was exceeded and the
    /// user was at or above its fair share.
    MemoryExhausted,
    /// An idle interactive transaction was aborted to reclaim memory for a user below its fair
    /// share.
    TxnEvicted,
    /// A bulk bind committed staged event parts.
    PartsBound,
    /// A bulk bind masked the rows of a rejected request in a shared part.
    BindSlotMasked,
    /// A bulk bind or transaction was rejected because a staged event part had expired or was swept.
    StagedPartUnbindable,
    /// A retried bulk bind found its parts already bound by an earlier attempt that landed.
    BindInstallReplayed,
    /// A bulk bind was forwarded to the database's lease holder.
    BindForwarded,
    /// Unsealed event rows were sealed into parts.
    EventsSealed,
    /// A seal failed validation because a concurrent delete or drop removed rows it read.
    SealConflicted,
    /// Event parts were merged.
    PartsMerged,
    /// A merge failed validation because a concurrent commit changed its inputs.
    MergeConflicted,
    /// Event parts past their table's retention were dropped.
    EventPartsExpired,
    /// A delete masked rows of an event part.
    EventRowsMasked,
    /// A delete wrote a mask too large to inline as its own object.
    MaskObjectWritten,
    /// A query skipped a part by its reference alone.
    PartPrunedByRef,
    /// A time-ordered row query stopped early once remaining parts were outside the page bounds.
    RowPageStoppedEarly,
    /// A query skipped granules by the primary index or column statistics.
    GranulesPruned,
    /// A query skipped granules by a bloom filter skip index.
    GranulesPrunedByBloom,
    /// A query skipped a part because its string dictionary matched nothing.
    PartPrunedByDictionary,
    /// A query read a JSON path from a part's shared buckets.
    JsonSharedPathRead,
    /// A query ran a second aggregation stage.
    QuerySecondStage,
    /// A query mapped values through a lookup table.
    LookupApplied,
    /// A slow event part read was sent again.
    PartGetHedged,
}

impl Probe {
    /// Probes excluded from short coverage tests and required only in long campaigns.
    pub const RARE: &[Probe] = &[
        Probe::CommitRetriesExhausted,
        Probe::StaleVersionIgnored,
        Probe::QuotaRefused,
        Probe::StagedBlobUnbindable,
        Probe::TailSealedInPlace,
        Probe::SegmentDropped,
        Probe::CompactionMoved,
        Probe::CompactionLostLease,
        Probe::BackgroundInstallUnleased,
        Probe::CompactionOutlivedOrphanGrace,
        Probe::SegmentCollectedUnderRead,
        Probe::LevelsCollectedUnderRead,
        Probe::BlobPinnedAtCollection,
        Probe::RetentionTrimLeftToOwner,
        Probe::ForwardDuplicateUnknown,
        Probe::MembersRefreshedForUnknownPeer,
        Probe::DeletionRepaired,
        Probe::NameResolvedAgain,
        Probe::PreconditionExplainedByReplay,
        Probe::RetentionTrimRetried,
        Probe::CommitUnknownAfterLeaseMoved,
        Probe::DecidedCommitAnswered,
        Probe::ModelAdoptedFromDatabase,
        Probe::StagedPartUnbindable,
        Probe::BindSlotMasked,
        Probe::BindInstallReplayed,
        Probe::MaskObjectWritten,
        Probe::SealConflicted,
    ];
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Hits {
    /// Total hit counts, indexed by [`Probe`].
    pub counts: Vec<u64>,
    /// Elapsed milliseconds from the start of recording to the first hit of each probe.
    pub first_ms: Vec<Option<u64>>,
    /// Hit counts recorded at or after the branch timestamp set by [`set_branch_time`].
    pub after_branch: Vec<u64>,
}

impl Hits {
    pub fn count(&self, p: Probe) -> u64 {
        self.counts.get(p as usize).copied().unwrap_or(0)
    }

    pub fn first_ms(&self, p: Probe) -> Option<u64> {
        self.first_ms.get(p as usize).copied().flatten()
    }

    /// Returns all probes that were hit at least once.
    pub fn hit(&self) -> impl Iterator<Item = Probe> + '_ {
        Probe::ALL.iter().copied().filter(|p| self.count(*p) > 0)
    }

    /// Returns all probes that were hit at least once at or after the branch timestamp.
    pub fn hit_after_branch(&self) -> impl Iterator<Item = Probe> + '_ {
        Probe::ALL.iter().copied().filter(|p| self.after_branch.get(*p as usize).is_some_and(|n| *n > 0))
    }

    pub fn add(&mut self, o: &Hits) {
        let n = Probe::ALL.len();
        self.counts.resize(n, 0);
        self.first_ms.resize(n, None);
        self.after_branch.resize(n, 0);
        for (i, c) in o.counts.iter().enumerate() {
            self.counts[i] += c;
        }
        for (i, c) in o.after_branch.iter().enumerate() {
            self.after_branch[i] += c;
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
    branch_ms: u64,
    hits: Hits,
}

thread_local! {
    static RECORDING: RefCell<Option<Recording>> = const { RefCell::new(None) };
}

/// Records a single hit for `p`.
pub fn hit(p: Probe) {
    hit_n(p, 1);
}

/// Records `n` hits for `p`.
pub fn hit_n(p: Probe, n: u64) {
    if n == 0 {
        return;
    }
    RECORDING.with(|r| {
        if let Some(r) = r.borrow_mut().as_mut() {
            let (i, ms) = (p as usize, r.origin.elapsed().as_millis() as u64);
            r.hits.counts[i] += n;
            if ms >= r.branch_ms {
                r.hits.after_branch[i] += n;
            }
            if r.hits.first_ms[i].is_none() {
                r.hits.first_ms[i] = Some(ms);
            }
        }
    });
}

/// Starts a new probe recording on the current thread.
pub fn record() {
    let n = Probe::ALL.len();
    let hits = Hits { counts: vec![0; n], first_ms: vec![None; n], after_branch: vec![0; n] };
    RECORDING.with(|r| *r.borrow_mut() = Some(Recording { origin: Instant::now(), branch_ms: 0, hits }));
}

/// Sets the branch timestamp in milliseconds from the start of recording.
pub fn set_branch_time(ms: u64) {
    RECORDING.with(|r| {
        if let Some(r) = r.borrow_mut().as_mut() {
            r.branch_ms = ms;
        }
    });
}

/// Stops recording on the current thread and returns the collected hits.
pub fn finish() -> Hits {
    RECORDING.with(|r| r.borrow_mut().take().map(|r| r.hits).unwrap_or_default())
}

/// Returns the current hit count of `p` on the current thread.
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
        record();
        hit(Probe::CommitBatched);
        set_branch_time(u64::MAX);
        hit(Probe::TailFlushed);
        let h = finish();
        assert_eq!(h.hit_after_branch().collect::<Vec<_>>(), [Probe::CommitBatched]);
        assert!(Probe::ALL.iter().all(|p| !p.doc().is_empty()));
    }
}
