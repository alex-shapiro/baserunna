# Baserunna: A Jepsen-Style Analysis

Baserunna stores small databases on object storage and claims strict
serializability and durable commits. We evaluated these claims with
deterministic simulation, Jepsen's Elle checker, a real-cluster fault-injection
harness on MinIO, and targeted code audits. We also compared Baserunna against
the pathology classes in Jepsen's analysis of ParticleDB 1.1.4.

Every test history satisfied strict serializability across 1.6 million
real-cluster transactions and more than 135,000 simulated runs. However, the
audits and real-cluster tests exposed six bugs and documentation gaps:

- If a database's manifest object was missing, Baserunna created an empty
  manifest, and garbage collection later deleted all existing segments and
  blobs.
- If the object store returned another segment's bytes under a segment key,
  readers accepted the substitute and compaction permanently dropped the
  original segment.
- Three error paths reported an applied commit as failed, replayed, or safe to
  restart.
- `CloudStore` discarded S3 client options from `AWS_*` environment variables,
  which prevented connections to plain-HTTP endpoints.
- The documentation overstated safety guarantees in five places.

This branch fixes the missing-manifest bug, the three commit-outcome error
paths, the S3 environment-variable bug, and the documentation. Readers now
verify that each segment's byte size and entry count match its manifest entry.
Additional storage-integrity checks and cross-node tracking of started
interactive commits remain open in [`TODO.md`](TODO.md).

The deterministic simulator and the real-cluster Jepsen harness complement each
other. Within its model, the simulator is stronger than Jepsen because it
controls scheduling and checks server version numbers. However, the simulator
replaces the binary, the S3 client, and the multi-threaded runtime. The first
real-cluster run immediately caught the `CloudStore` bug that the simulator
could not reach.

## 1. Background

### 1.1 Baserunna

A Baserunna cluster runs stateless compute nodes over a shared object store.
Each database is an LSM tree stored under a key prefix. The manifest is the
only mutable object, and every commit updates the manifest with a conditional
`PUT` (`If-Match`). A single lease holder batches commits and serves
linearizable reads from memory. Peer nodes forward requests to the lease holder
and can acquire the lease after it expires. Lease validity relies on node
monotonic clocks and object-store `last_modified` timestamps. The manifest
includes an inline write log called the tail. Background tasks flush the tail
to immutable segments, compact segments, and delete obsolete objects.

The HTTP API supports one-shot and interactive transactions over KV entries,
tables, streams, and event tables, as well as binary blobs and change feeds.

### 1.2 Safety claims

Before this branch, [`SPEC.md`](SPEC.md) §1 made three safety claims:

- Committed data is never lost or reordered.
- Transactions are strictly serializable. Section 3.8 explains why this branch
  adds "by default".
- Committed data correctness relies on object storage guarantees.

`SPEC.md` §6 specifies the transaction contract. Serializable isolation and
`strong` read consistency are the defaults. Clients can request snapshot
isolation or `snapshot` read consistency. An idempotency key ensures that
retries within 10 minutes are applied at most once. A `503` response with
`outcome: "unknown"` indicates that the write may have been applied.

### 1.3 ParticleDB pathologies in Jepsen's analysis

Jepsen tested ParticleDB 1.1.4 through 1.1.18.11.rc-22 on single nodes and
reported 96 issues, most of which occurred on a healthy node with a single
client. The ParticleDB report grouped its findings into six categories:

1. **Lost committed writes and isolation anomalies in normal operation.** Tests
   observed G1a, G1b, G-single, G2, lost updates, and stale reads under
   `strict-serializable` isolation.
2. **Silent data loss after storage corruption.** After WAL bitflips,
   ParticleDB replayed a truncated prefix and continued serving (#108). If
   4 KiB WAL chunks were shuffled to simulate misdirected writes, ParticleDB
   logged `"No data found in storage — starting fresh"` and discarded the
   database (#112).
3. **Anomalies after process crashes.** `SIGKILL` faults produced internal
   errors and lost updates (#107).
4. **Contradictory or overstated documentation.** The documentation claimed
   both "Full ACID" and "snapshot isolation", gave conflicting durability
   descriptions, and cited an unpublished TLA+ proof.
5. **Untested constraints.** `NOT NULL` and `UNIQUE` constraints were not
   enforced.
6. **Incomplete fixes.** Subsequent builds arrived without changelogs and left
   reported bugs unresolved.

Other Jepsen analyses highlight additional failure modes relevant to Baserunna:

- acknowledging writes before durability (NATS, Radix);
- trusting a lease without revalidation (Bufstream, etcd);
- reporting definite failures for writes that were applied (Bufstream, Radix,
  TigerBeetle);
- serving stale caches after a restart (Bufstream);
- trusting corrupt state during recovery (NATS).

## 2. Test Design

We combined four testing methods:

1. the repository's deterministic simulator;
2. Jepsen's Elle checker on simulated histories;
3. a real-cluster test harness on MinIO with a fault-injecting nemesis;
4. manual code audits of storage corruption, error reporting, and
   documentation.

### 2.1 The deterministic simulator

The deterministic simulator in `SPEC.md` §13 runs 1 to 5 nodes on a single
thread in virtual time across 25 scenarios. It injects the following faults:

- node crashes, including crash points inside commits, compactions, and garbage
  collection sweeps;
- process pauses;
- clock offsets and clock jumps;
- asymmetric and symmetric network partitions between nodes;
- network partitions between a node and the object store;
- object-store errors before and after an operation is applied;
- object-store stalls longer than the lease duration;
- eventually consistent `LIST` results;
- duplicated and delayed forwarded requests;
- local disk-cache corruption.

Each scenario verifies reference models and invariants:

- `history` checks strict serializability, snapshot isolation, and
  read-your-writes across random multi-key transactions.
- `register` checks linearizability.
- `bank` checks balance conservation.
- Every 20 ms of virtual time, a global lease oracle verifies that at most one
  node relies on a database lease and that any node relying on a lease holds
  the latest manifest.

The `history` checker sorts committed transactions by the commit version
reported by the server, replays them sequentially, and verifies that every
read, failed precondition, and final key value matches the replay while
respecting real-time order. Because `history` relies on server-reported version
numbers to order transactions, we also added a checker that infers the
serialization order from client observations alone.

### 2.2 Elle on the simulator

We added the `elle` simulation workload in
[`elle.rs`](crates/baserunna/src/sim/workloads/elle.rs) to implement Jepsen's
list-append model:

- Each key stores a list of unique integers.
- Even keys are KV entries that interactive serializable transactions update
  with read-modify-write operations.
- Odd keys are streams that one-shot and interactive transactions append to.
- After a key receives `max_appends` appends, the generator replaces it with a
  new key to keep lists short.

The workload records invocation and completion events in execution order. If a
transaction returns an ambiguous outcome, the workload records `:info` and
allocates a new process ID for subsequent transactions from that client. After
fault injection stops and the cluster heals, the workload reads the final list
of every key, writes the history in EDN format, and checks it. We also checked
the generated EDN files with `elle-cli` 0.1.11 using `--model list-append` and
`--consistency-models strict-serializable`.

The initial Rust checker in `elle.rs` checked only prefix consistency and
append durability. Section 4.2 shows how that initial checker missed stale
reads that `elle-cli` detected. We extended `elle.rs` with three additional
checks that run in every simulation without requiring Java:

- **Internal consistency:** a read of a KV key after appends to that key in the
  same transaction ends with those appends.
- **Real-time order:** a read includes all appends from transactions that
  completed before the reading transaction began.
- **Dependency-cycle detection:** the checker derives each key's version order
  from its final list and searches the write-write, write-read, read-write, and
  real-time dependency graph for cycles.

The [`elle_check`](crates/baserunna/examples/elle_check.rs) example runs these
same checks on EDN histories produced by the real-cluster harness.

To verify that `elle-cli` rejects invalid histories in our EDN format, we fed
it synthetic histories containing a stale read, write skew, and a lost append.
`elle-cli` rejected all three.

### 2.3 Real-cluster harness on MinIO

Because the simulator replaces the S3 client, HTTP stack, multi-threaded
runtime, and OS processes, we wrote [`scripts/jepsen/run.py`](scripts/jepsen/run.py).
The harness starts MinIO and 3 or 5 `baserunna` release binaries on a single
machine and runs the list-append workload over HTTP against random nodes. Every
2 to 8 seconds, a nemesis injects one of the following faults:

- `SIGKILL` of a single node or all nodes, followed by a restart after up to
  5 s;
- `SIGSTOP` of a node for 0.5 to 20 s, or of MinIO for 0.5 to 7 s;
- a storage partition on a node via a TCP proxy that either discards bytes to
  trigger request timeouts or resets connections;
- a peer partition on a node via a TCP proxy on its advertised address.

A background supervisor restarts any node that exits unexpectedly. After the
test duration, the harness heals all proxies and processes, waits three lease
periods, reads every key, and checks the EDN history with `elle_check`. We also
checked every real-cluster history with `elle-cli`.

Changing system clocks on macOS requires root privileges, so clock-skew faults
were tested only in the simulator.

### 2.4 Code audits

We audited three areas that workload testing did not exercise:

- storage corruption handling across all persisted formats (bitflips,
  truncation, chunk swaps, substituted objects, and missing objects);
- commit-outcome classification across every error path, including S3 error
  mapping in `object_store` 0.14;
- safety claims in `README.md`, `SPEC.md`, `openapi.json`, and `DEPLOYING.md`.

Every confirmed bug has a regression test that fails without the fix, except
for the two cases noted in section 5.

### 2.5 Checker comparison on the bug zoo

The repository includes 27 bug-zoo patches that reintroduce historical bugs. We
applied seven patches (03, 04, 07, 12, 17, 22, and 27) and ran both `elle` and
`history` across 300 to 1,300 seeds per patch to compare detection rates.

## 3. Results

### 3.1 Strict serializability held across all runs

Neither `elle-cli` nor the simulator checkers found any serializability
violation, lost append, duplicate append, or dirty read.

The simulation campaigns ran as follows:

- `campaign all` on the baseline commit (`609bc87`) for 45 minutes: 27,549 runs
  (about 58 simulated days), 0 failures.
- `elle` across 2,000 seeds with faults: 0 failures in the simulator, and
  `elle-cli` accepted all 2,000 EDN histories under strict serializability.
- `campaign all` after the first three fixes (`7874096`, `901e380`, `d284785`)
  for 20 minutes: 14,574 runs (about 29 simulated days), 0 failures.
- Targeted scenario campaigns after subsequent fixes: 57,138 runs, 0 failures.
- `elle` seeds 1000–2999 after each of three checker updates: 0 failures.
- `campaign all` on the final code for 20 minutes across 4 threads: 28,029 runs
  (about 57 simulated days), 0 failures.
- Guided exploration (`explore all`) on the final code for 30 minutes: 9,388
  runs, 4,713 of them branched from rare probes, 0 failures.

Three 60-to-90-minute campaigns ran concurrently with large real-cluster tests
and were terminated by the OS (`SIGKILL`, exit code 137) under memory pressure.
They reported no failures before termination, and we exclude them from the run
totals above.

The five real-cluster runs on MinIO tested three binary revisions:

- `before fixes` (`76c411c`): includes only the initial S3 environment-variable
  fix (section 3.7).
- `fixed` (`d32f75a`): adds the lost-manifest fix (`7874096`), the segment
  metadata check (`901e380`), and the first ambiguous-rejection fix
  (`d284785`).
- `final` (`cd30b18`): adds the replay fix (`e5091e9`), the interactive-commit
  tracking fixes (`f402025`, `80a1286`), and per-commit ambiguity tracking
  (`15e5ee9`). Subsequent commits on this branch (`b0d3e88`, `722635a`,
  `9e2df7b`, `fb22d2c`) affect only unsent commits, non-UTF-8 environment
  variables, and cache eviction above 100,000 entries.

| Run | Binary | Nodes | Lease | Minutes | Faults | `ok` | `info` | Elle |
| --- | ------ | ----- | ----- | ------- | ------ | ---- | ------ | ---- |
| r1 | before fixes | 3 | 5 s | 10 | 54 | 39,803 | 116 | valid |
| r2 | before fixes | 3 | 5 s | 20 | 126 | 134,310 | 488 | valid |
| r3 | fixed | 3 | 1 s | 30 | 182 | 268,679 | 613 | valid |
| r4 | fixed | 5 | 5 s | 60 | 354 | 465,539 | 3,379 | valid |
| r5 | final | 3 | 1 s | 40 | 228 | 328,164 | 1,502 | valid |
| r6 | `07a7f79` | 5 | 1 s | 40 | 237 | 370,842 | 933 | valid |

In the table, `ok` is the number of successful transactions, including
read-only ones, and `info` is the number of transactions with an unknown
outcome. Both `elle_check` and
`elle-cli` accepted every history. Run r3 used 30 clients across 4 active keys
with a 1 s lease so that process pauses frequently exceeded the lease duration.
Appendix A lists the exact command and seed for each run.

### 3.2 Missing manifest caused total data loss (fixed)

Baserunna creates a database's manifest lazily on first open. Previously, if
`GET` on the manifest returned `NotFound`, `Db::refresh_at` created a new empty
manifest if `create` was true. `Node::open_db` passed `create = true` for
every database registered in the system database. Consequently, if a manifest
object was deleted by an operator error or misconfigured lifecycle rule,
Baserunna initialized an empty manifest, matching ParticleDB #112. After the
2-hour orphan grace period, the garbage-collection sweep marked all existing
segments and blobs as obsolete and deleted them 30 minutes later. The system
database followed the same path at startup, so a missing `sys` manifest erased
all account, organization, and database records.

Because a node always writes a database's initial manifest before writing any
other object for that database, the fix lists the database prefix before
creating an initial manifest. If any object exists under the prefix, the node
re-reads the manifest once in case a peer created the manifest concurrently.
If the manifest is still missing, `Db::refresh_at` returns a `500 internal`
error without writing or deleting anything.

Our first version of this fix omitted the second manifest read. A 20-minute
simulation campaign failed 70 runs because a peer node created the manifest
between the `GET` and the `LIST`.

Commit: `7874096`. Test: `open_rejects_missing_manifest_if_database_objects_exist`.

### 3.3 Substituted segment was read and compacted away (fixed)

Each segment block, bloom filter, index, and footer has a CRC32C checksum, but
readers did not check that the segment object matched the `SegmentMeta` entry
in the manifest. For level-0 segments and other segments up to 1 MiB, the
reader decoded any valid segment returned under the requested key. In our test,
replacing a segment object with another segment's bytes caused a scan to
return 36 of 50 keys silently. Compaction then merged the substitute segment
and deleted the original segment reference from the manifest.

`verify_segment_matches_manifest` now checks that the decoded segment's byte
size and entry count match the manifest's `SegmentMeta` entry.

Commit: `901e380`. Test: `scan_rejects_segment_that_mismatches_manifest_metadata`.

### 3.4 Applied commits were reported as definite failures (fixed)

If a manifest `PUT` times out, the committer reloads the manifest. If the
manifest ETag has not changed, the timed-out `PUT` can still be applied by the
object store, so the committer rebuilds the batch on the same base version.
Previously, if the rebuild rejected a transaction in the batch, the committer
converted only `Unavailable` errors to `outcome: "unknown"` and returned other
rejections as definite failures. On an unchanged base version, a transaction
can succeed on the first build and fail on the second build if a staged-blob
TTL or a read-key TTL expires between builds. In our test, the client received
`400 invalid_argument` for a commit that the store subsequently applied. A
client retrying without an idempotency key would then apply the transaction a
second time.

The initial fix (`d284785`) marked every rejection in a rebuilt batch as
`unknown`. Code review (section 3.10) showed that this also masked definite
conflicts for transactions that joined the batch after the timed-out `PUT`. The
committer (`15e5ee9`) now records the commit IDs included in each ambiguous
manifest `PUT` and marks a rebuild rejection as `unknown` only if the commit ID
was sent in an earlier ambiguous `PUT`.

Commits: `d284785` and `15e5ee9`. Tests:
`rejection_after_ambiguous_manifest_put_returns_unknown_outcome` and
`conflict_excluded_from_ambiguous_manifest_put_remains_definite`.

### 3.5 Applied commit after a timed-out `PUT` was reported as a replay (fixed)

If the store applied a timed-out manifest `PUT`, the committer's subsequent
manifest reload found the commit record already present and returned
`replayed: true`. For a one-shot transaction with an idempotency key, the HTTP
handler sets `results` to `null` if `replayed` is true, so the client lost the
read results of its own request. The committer now clears `replayed` if the
commit ID was sent in an ambiguous manifest `PUT` of the current batch.

Commits: `e5091e9` and `15e5ee9`. Test:
`commit_applied_by_ambiguous_manifest_put_is_not_marked_replayed`.

### 3.6 Retried interactive commit was told to restart (fixed on the committing node)

Before sending a commit, `commit_txn` removes the interactive transaction from
the open-transaction registry. Previously, if a client lost the commit response
and resent the commit request, the node returned a retryable
`404 txn_not_found`. `SPEC.md` instructs clients to handle `txn_not_found` by
restarting the transaction from `begin`, which applied the transaction twice.

`OpenTxns` now records the commit ID of each transaction in `started_commits`
before releasing the transaction lock. If a subsequent request arrives for that
transaction, the node returns `503` with `outcome: "unknown"` and the commit
ID. If the transaction is read-only or fails before the commit is sent to the
committer, the node removes the entry from `started_commits` so that subsequent
requests still receive `404 txn_not_found`. Because `started_commits` is stored
in node memory, a peer node or a restarted node still returns `404`; `TODO.md`
tracks persisting this state.

Commits: `f402025`, `80a1286`, and `b0d3e88`. Tests:
`retried_interactive_commit_returns_unknown_outcome`,
`failed_interactive_commit_returns_txn_not_found_on_retry`, and
`fuzz_txn_ops_against_model`.

### 3.7 `CloudStore` ignored S3 client options from the environment (fixed)

`CloudStore` initialized `AmazonS3Builder::from_env()` and then overwrote its
`ClientOptions` with default timeouts. This discarded all client options read
from `AWS_*` environment variables, including `AWS_ALLOW_HTTP`, and prevented
nodes from connecting to plain-HTTP S3 endpoints such as local MinIO. The store
conformance tests passed `aws_allow_http` as an explicit setting and never
exercised environment-variable loading. The first real-cluster test caught this
bug immediately at startup.

`CloudStore` now sets default `ClientOptions` on a new builder first, applies
`AWS_*` environment variables via `vars_os()`, and then applies explicit
settings.

Commits: `76c411c` and `722635a`. Test: `reads_client_options_from_environment`.

### 3.8 Documentation overclaims (fixed)

Our documentation audit found five inaccurate safety claims:

- `SPEC.md` §1 stated that transactions are strictly serializable without
  noting that snapshot isolation and `snapshot` read consistency are weaker
  opt-in modes. Conversely, `README.md` stated that reads and writes "use
  snapshot isolation".
- `SPEC.md` §6.1 stated that read-only transactions always succeed, whereas
  §5.5 specifies that interactive read-only commits validate their read sets
  and can fail with `409 conflict`.
- `SPEC.md` §6.1 stated that blind-write transactions never conflict, which
  holds under serializable isolation but not under snapshot isolation.
- `SPEC.md` §5.9 stated that `snapshot` reads are at most 5 seconds old, which
  is only the default value of `max_staleness_ms`, and did not note that
  reading across different nodes can observe older snapshots after newer ones.
- `SPEC.md` §4.6 stated that a stream consumer processes each record exactly
  once, without noting that retention trimming can delete records before a
  lagging consumer reads them.

In addition, `openapi.json` omitted the 10-minute retention window on
`idempotency_key` and did not explain how to handle `503` responses with
`outcome: "unknown"`.

Commit: `d32f75a`.

### 3.9 Open issues

The following gaps remain open and are tracked in [`TODO.md`](TODO.md):

- **Storage integrity under broken object stores.** Full blob reads do not
  verify `BlobRef.sha256`, and individual blob chunks lack CRC checksums.
  Replacing a sealed stream chunk with another chunk of equal byte size causes
  stream reads to skip records silently. Staged blob metadata objects lack
  checksums. Segments, stream chunks, and local disk-cache files do not embed
  their object IDs in their footers, so a substituted segment with the same
  byte size and entry count is still accepted. A node with an empty cache
  accepts a manifest that was rolled back to an older version. Each of these
  requires an object store that violates its contract.
- **Ambiguity across restarts and peers.** `started_commits` (section 3.6) is
  held only in the committing node's memory.
- **`retryable` flag on unknown outcomes.** `503` responses with
  `outcome: "unknown"` set `retryable: true`. `baserunna-client` inspects
  `outcome`, but a naive client checking only `retryable` could retry a
  non-idempotent write.
- **Startup preflight failure during store outages.** If the object store is
  unreachable at node startup, the conditional-write preflight check fails and
  the process exits, requiring an external process supervisor to restart it.

### 3.10 Independent reviews of the fixes

Following [`playbooks/review.md`](playbooks/review.md), we ran three rounds of
independent review on this branch.

The first review identified defects in four initial fixes:

- The initial ambiguous-rejection fix (`d284785`) marked definite conflicts as
  `unknown` for transactions that were never sent in an ambiguous `PUT`.
- The initial replay fix (`e5091e9`) compared a write-sequence counter, which
  missed multi-attempt batches and ignored event binds.
- The initial interactive-commit fix (`f402025`) recorded the commit ID after
  releasing the transaction lock, leaving a race window, and cleared the entire
  `TtlCache` if it reached capacity.
- The segment metadata check (`901e380`) compares only byte size and entry
  count, so a substituted segment with identical size and entry count is still
  accepted.

We fixed the first three defects (`15e5ee9`, `80a1286`) and recorded the
fourth in `TODO.md`.

The second review identified five additional defects:

- The dependency-cycle checker in `elle.rs` skipped reads of absent KV keys (`None`),
  which missed write-skew cycles on uninitialized keys, and lacked an
  internal-consistency check (`3514ff1`).
- `TtlCache` still cleared all entries if 100,000 live entries were present;
  it now evicts the oldest eighth (`9e2df7b`, refined in `fb22d2c`).
- Read-only interactive commits and interactive commits that failed before
  reaching the committer remained in `started_commits` (`b0d3e88`, also caught
  by `fuzz_txn_ops_against_model`).
- `CloudStore` used `std::env::vars()`, which panics if any environment
  variable is not valid UTF-8 (`722635a`).
- `scripts/jepsen/run.py` maintained a duplicate Python copy of the checker and
  ignored client errors in its exit code (`f2881f8`).

The third review caught a `TtlCache` eviction off-by-one (`fb22d2c`), missing
executable checks in `scripts/jepsen/run.py` (`278a8f2`), and inaccuracies in
an earlier draft of this report (`07a7f79`).

### 3.11 Harness defects caught during testing

We found and fixed four bugs in `scripts/jepsen/run.py` during development:

- The initial TCP proxy closed both directions of a socket on the first EOF
  instead of forwarding a TCP half-close (`SHUT_WR`), which broke pooled HTTP
  connections.
- Stream-creation retries pinned a single node for 120 s even if that node was
  stopped.
- If stream creation raised an exception before a transaction was invoked, the
  client recorded an `:info` completion without a matching `:invoke` event. We
  stripped 7 such orphan `:info` events from the r1 history before running
  `elle-cli`.
- The harness initially duplicated the checker logic in Python and exited with
  status 0 even if clients recorded unexpected errors.

## 4. Comparing the Simulator and Jepsen

### 4.1 Strengths of the deterministic simulator

- **Deterministic reproduction.** Every failure can be reproduced from its seed
  and inspected with virtual-time tracing.
- **Fine-grained fault placement.** Crash points halt execution between
  internal steps of a commit, compaction, or garbage-collection sweep, and
  `buggify` probes force rare branches.
- **Throughput and breadth.** On 4 cores, the simulator runs about 600
  fault-injected tests per minute across 25 scenarios covering authentication,
  organizations, quotas, events, and garbage collection.
- **Version-number verification.** Because `history` verifies the server's
  reported commit versions, it catches version-numbering bugs that
  observation-only checkers ignore. Under bug-zoo patch 03, which assigns the
  same version to two transactions in a batch, `history` failed 94 of 300
  seeds, whereas `elle-cli` reported 0 anomalies across 300 seeds.
- **Checker validation.** The bug zoo, checker unit tests, and mutation testing
  verify that the simulator's checkers detect injected bugs.

### 4.2 Strengths of Jepsen and real-cluster testing

- **Testing the real system.** The simulator substitutes fakes for the S3
  client, HTTP stack, and multi-threaded runtime. The first real-cluster run
  caught the `CloudStore` environment-variable bug (section 3.7) before sending
  a single transaction.
- **Independent observation-based checking.** `elle-cli` infers serialization
  orders from read and write observations alone. Under bug-zoo patch 04, which
  removes the in-flight write barrier so strong reads can miss a concurrent
  write, `elle-cli` detected `G-single-item-realtime` in 10 of 1,300 seeds,
  whereas `history` failed in 4 of 1,300 seeds. After we added the real-time
  and cycle checks to `elle.rs`, the Rust checker rejected the exact same 10
  seeds as `elle-cli`.
- **Longer histories.** A simulated run executes tens to hundreds of
  transactions, whereas real-cluster run r4 recorded 1,326,032 events. Bugs
  that require large state sizes or long histories are more likely to surface
  in extended real-cluster runs.

### 4.3 Uncovered areas

Neither testing method covered the following cases:

- clock skew on real cluster nodes;
- dynamic cluster membership changes beyond restarting fixed node slots;
- real-cluster fault injection against Tigris, the target production object
  store;
- `elle-cli` verification of snapshot isolation, row tables, event tables, or
  change feeds, which the simulator checks against reference models instead;
- monotonic clock drift across nodes exceeding the 0.5 s lease safety margin.

In addition, random simulation campaigns miss 7 of the 27 bug-zoo patches
across 2,800 seeds (`bugzoo/README.md`). In our 300-seed runs, patches 07, 22,
and 27 were not triggered by either `history` or `elle`. Reaching rare fault
combinations requires targeted tests or larger seed counts.

## 5. ParticleDB Pathology Comparison

The list below summarizes how Baserunna compares against each pathology class
from Jepsen's ParticleDB report and related analyses:

- **Lost writes and isolation anomalies in normal operation:** None observed
  across simulated and real-cluster runs.
- **Anomalies under process crashes:** None observed under single-node
  `SIGKILL`, full-cluster `SIGKILL`, or simulated intra-commit crash points.
- **Stale reads under strict serializability:** None observed on the current
  code. Both the lease oracle and `elle-cli` detect historical bugs in this
  class (patches 04 and 12).
- **Silent data loss after storage corruption:** Found and partially fixed. All
  block and footer checksum failures return errors rather than replaying
  truncated prefixes, and this branch fixes the missing-manifest bug (section
  3.2) and segment size/count verification (section 3.3). Additional
  object-binding and blob-checksum gaps remain open in `TODO.md` (section 3.9).
- **Acknowledgments before durability:** None. Nodes store all durable state in
  the object store and acknowledge commits only after the conditional manifest
  `PUT` succeeds.
- **Errors that misreport applied writes:** Found and fixed on three paths
  (sections 3.4, 3.5, and 3.6).
- **Overstated documentation:** Found and fixed in five places (section 3.8).
- **Untested features:** None found. A coverage test asserts that the simulator
  exercises every documented API endpoint and common probe. However, the
  conformance test setup for `CloudStore` differed from production startup and
  missed the environment-variable bug (section 3.7).
- **Untested fixes:** Every historical bug in `SPEC.md` §13.6 is reproduced in
  the bug zoo, and every fix in this branch has a regression test that fails
  without the fix, with two exceptions: `722635a` (non-UTF-8 environment
  variables) and the lock-release race window closed by `80a1286`.

## 6. Discussion and Recommendations

Baserunna's existing test suite—unit tests, reference-model fuzzers,
deterministic simulation, and the bug zoo—prevented the core transactional
anomalies that dominated Jepsen's ParticleDB findings. Every bug we found lay
outside the simulator's fault model:

- The missing-manifest and segment-substitution bugs required an object store
  that loses or swaps objects, which the simulated store never does.
- The commit-ambiguity bugs required a manifest `PUT` timeout followed by a
  time-dependent validation failure or a duplicate interactive commit request.
- The `CloudStore` bug required the real S3 client builder.

We recommend five follow-up actions:

1. **Inject storage-corruption faults in the simulator.** Add a fault mode that
   deletes objects, swaps objects of the same type, flips bits, and rolls back
   manifests, and assert that nodes return explicit errors rather than corrupt
   data.
2. **Run `scripts/jepsen/run.py` before releases.** Run the real-cluster
   harness for at least an hour against the target object store, and run it on
   Linux with clock-skew faults enabled.
3. **Run both `history` and `elle` in CI.** Keep both the version-based
   `history` checker and the observation-based `elle` checker.
4. **Separate outcome ambiguity from `retryable`.** Set `retryable: false` if
   `outcome` is `"unknown"` so that clients inspecting only `retryable` do not
   retry non-idempotent writes.
5. **Expand targeted tests for rare bug-zoo patches.** Add deterministic tests
   for the bug-zoo patches that random campaigns rarely trigger.

## 7. Reproduction

Run the simulator campaigns and `elle-cli`:

```sh
SIM_MINUTES=20 cargo run --release --example sim_tool -- campaign all 1000000 120000000
SIM_ELLE_DIR=out cargo run --release --example sim_tool -- campaign elle 2000 1000
cargo run --release --example elle_check -- out/*.edn
java -jar elle-cli-0.1.11-standalone.jar -m list-append -f edn -c strict-serializable out/*.edn
```

Run the real-cluster harness (requires `minio` and `mc` on `PATH`):

```sh
cargo build --release -p baserunna --bin baserunna --example elle_check
scripts/jepsen/run.py --minutes 30 --out runs/r3 --seed 3 --lease-ms 1000 --clients 30 --active-keys 4 --max-appends 16
java -Xmx8g -jar elle-cli-0.1.11-standalone.jar -m list-append -f edn -c strict-serializable runs/r3/history.edn
```

Run the bug-zoo checker comparison:

```sh
git apply bugzoo/04-in-flight-write-barrier.patch
SIM_ELLE_DIR=out cargo run --release --example sim_tool -- campaign elle 1000 6000
cargo run --release --example sim_tool -- campaign history 1000 6000
git apply -R bugzoo/04-in-flight-write-barrier.patch
```

## Appendix A. Test Runs

All real-cluster runs executed `scripts/jepsen/run.py` on an 8-core, 16 GB
macOS machine with MinIO `2025-10-15`. Both `elle_check` (`3514ff1`) and
`elle-cli` 0.1.11 accepted every history:

- **r1:** `--minutes 10 --seed 1`, binary `76c411c`. Recorded 7 client errors
  caused by the stream-creation harness bug described in section 3.11.
- **r2:** `--minutes 20 --seed 2`, binary `76c411c`. 0 client errors.
- **r3:** `--minutes 30 --seed 3 --lease-ms 1000 --clients 30 --active-keys 4
  --max-appends 16`, binary `d32f75a`. 0 client errors.
- **r4:** `--minutes 60 --seed 4 --nodes 5 --clients 20`, binary `d32f75a`. 0
  client errors. Nodes exited 83 times because startup preflight checks
  coincided with injected storage faults, and the supervisor restarted them.
- **r5:** `--minutes 40 --seed 5 --lease-ms 1000 --clients 12 --active-keys 6`,
  binary `cd30b18`. 0 client errors.
- **r6:** `--minutes 40 --seed 6 --nodes 5 --lease-ms 1000 --clients 15
  --active-keys 5`, binary `07a7f79`, which has every fix. 0 client errors,
  and the harness ran `elle_check` itself and exited with status 0.

The bug-zoo comparisons used commits `7ab104b` and `cd30b18`:

- **Patch 03** (seeds 5000–5299): `history` failed 94 seeds; `elle-cli`
  rejected 0.
- **Patch 04** (seeds 5000–5299 and 6000–6999): `elle-cli` and `elle.rs`
  rejected the same 10 seeds; `history` failed 4 seeds.
- **Patch 12** (seeds 5000–5299): `history` failed 34 seeds, and `elle.rs`
  failed 41 seeds, primarily via the lease oracle. Across seeds 6000–6999 with
  the updated `elle.rs` checks, `elle.rs` failed 147 seeds.
- **Patches 07, 22, and 27** (seeds 5000–5299): 0 failures from either checker.
- **Patch 17** (seeds 6000–6999): 0 failures from `elle.rs`.

## Appendix B. References

- Kyle Kingsbury, "ParticleDB 1.1.4", Jepsen, 2026-10-31:
  https://jepsen.io/analyses/particledb-1.1.4
- Jepsen ParticleDB test suite: https://github.com/jepsen-io/particledb
- Jepsen analyses: https://jepsen.io/analyses
- Elle transactional consistency checker: https://github.com/jepsen-io/elle and
  `elle-cli` 0.1.11: https://github.com/ligurio/elle-cli
- Jepsen consistency models: https://jepsen.io/consistency
