# Baserunna: A Jepsen-Style Analysis

Baserunna stores small databases on object storage and claims strictly
serializable transactions and no loss of committed data. We tested it with
Jepsen's methods and with Jepsen's own checker, Elle. We also compared it,
pathology by pathology, with Jepsen's analysis of ParticleDB 1.1.4.

We found no transactional anomaly. About 1.24 million transactions succeeded
on a real cluster with process kills, pauses and partitions, and more than
100,000 simulated runs passed. However, Baserunna had a ParticleDB-class
storage pathology: if a database's manifest went missing, Baserunna recreated
the database empty, and garbage collection then deleted all of its data. A
substituted segment was read as the real one and then compacted away. Three
error paths told clients that applied writes had failed, or that they could
safely restart. The documentation overclaimed in five places.

This branch fixes the lost manifest, the three error paths, and the
documentation. It detects most substituted segments, but not one with the
same size and entry count. One error path is fixed only on the node that ran
the commit. Several storage-integrity gaps remain open, and each needs a
broken object store to trigger.

The deterministic simulator is stronger than Jepsen inside its model. It
detected a version-numbering bug that Elle cannot see. It is not a
replacement for Jepsen, because it does not run the real binary, the real S3
client, or the multi-threaded runtime. The first real-cluster run found a bug
that the simulator could not reach.

## 1. Background

### 1.1 Baserunna

A Baserunna cluster is a set of stateless nodes on a shared object store.
Each database is an LSM tree under one key prefix. Its root, the manifest, is
the only mutable object, and every commit is a conditional `PUT` of the
manifest with `If-Match`. One node at a time holds a lease on a database. The
lease holder batches commits and serves strong reads from memory. Other nodes
forward requests to the lease holder, and they can take over the lease after
it expires. Leases depend on monotonic clocks and on the object store's
`last_modified` timestamps. The manifest also holds an inline write log, the
tail. Background tasks flush the tail to segments, compact segments, and
collect garbage.

The API offers one-shot and interactive transactions over KV keys, tables,
streams, and event tables, plus blobs and change feeds.

### 1.2 Safety claims

SPEC.md §1 makes three safety claims:

- Committed data is never lost or reordered.
- Transactions are strictly serializable.
- Committed data correctness relies on object storage guarantees.

Section 6 adds the details. The default isolation is serializable, and
`strong` reads are linearizable. Snapshot isolation and `snapshot`
consistency reads are options. An idempotency key makes a retry within 10
minutes apply at most once. A `503` response with `outcome: "unknown"` marks
a write that may have been applied.

### 1.3 What Jepsen found in ParticleDB

Jepsen tested ParticleDB 1.1.4 through 1.1.18.11.rc-22 on single nodes and
reported 96 issues. Almost all occurred on a healthy node with a single
client. The report says that a few engineers built ParticleDB with LLM
assistance, and it asks whether LLMs also did the testing. It describes the
following pathologies:

1. **Lost committed writes and transactional anomalies in normal
   operation.** These included G1a, G1b, G-single, G2, lost update, and stale
   reads under `strict-serializable` (#38, #66 to #70, #73, #96, #106, #109,
   #110).
2. **Silent damage from corrupt files.** After WAL bitflips, ParticleDB
   replayed a "valid prefix" and kept serving (#108). After 4 KiB chunks of a
   WAL were shuffled, it logged "No data found in storage — starting fresh",
   and all data was lost (#112).
3. **Faults that break invariants.** Process kills caused internal anomalies
   and lost update (#107).
4. **Documentation that overclaims or contradicts itself.** Examples are
   "Full ACID" next to "snapshot isolation", sync modes described as both
   weaker and "the same durability", vague isolation modes, and an
   unpublished TLA+ proof.
5. **Basic features without tests.** NOT NULL, UNIQUE and other constraints
   were not enforced. Jepsen asks "how does one add support for NOT NULL
   constraints without a test which tries to insert at least one NULL
   value?"
6. **Unexplained fixes.** Builds "materialized, sans changelogs", and LLM
   tooling "repeatedly claimed that bugs had been fixed when, in fact, they
   had not".

Other Jepsen analyses add more classes that matter for Baserunna:

- an acknowledgement before durability (NATS, Radix);
- a lease that the holder never revalidates (Bufstream, etcd);
- an error that tells a client "failed" for a write that was applied
  (Bufstream, Radix, TigerBeetle);
- stale caches after a restart (Bufstream);
- recovery that trusts corrupt state (NATS).

## 2. Test Design

We used four kinds of evidence:

1. the repository's deterministic simulator;
2. Elle on histories from the simulator;
3. a real cluster on MinIO with a Jepsen-style nemesis;
4. code audits of the paths that the other three cannot reach.

### 2.1 The deterministic simulator

The repository already has a deterministic simulator (SPEC §13, about 19,000
lines, with 24 scenarios before this branch added `elle`). It runs one to five nodes on one thread in virtual time. Its faults
include:

- crashes, including crash points inside commits, compactions and sweeps;
- process pauses;
- clock offsets and clock steps;
- one-way and two-way partitions;
- partitions between a node and the store;
- store errors before and after an operation applies;
- store stalls longer than a lease;
- eventually consistent `LIST` results;
- duplicated and delayed forwards;
- corruption of the local disk cache.

Its scenarios check reference models and invariants. The most important
for this analysis are these:

- `history` checks strict serializability, snapshot isolation, and
  read-your-writes over random multi-key transactions.
- `register` checks linearizability.
- `bank` checks conservation of the total balance.
- A global oracle checks every 20 ms that at most one node relies on a lease
  for a database, and that a node that relies on a lease has the latest
  manifest.

The `history` checker works differently from Elle. It sorts committed
transactions by the version that the server reports. It then replays them,
and it requires every read, every failed precondition, and the final state
to match the replay exactly. It also requires the order to respect real
time. If the checker accepts a history, the commit-version order is a serial
order that explains every observation, so the checker cannot miss a
violation in the transactions that it checks. It can reject a correct
history only if the server reports a wrong version, which is a bug too.
However, the checker depends on the server's versions to find the order, so
we added a checker that infers the order from observations alone.

### 2.2 Elle on the simulator

We added an `elle` workload (`src/sim/workloads/elle.rs`). It is Jepsen's
list-append workload:

- Each key holds a list of unique integers.
- Even keys are KV values that interactive serializable transactions append
  to with a read-modify-write.
- Odd keys are streams that one-shot and interactive transactions append to.
- Keys take a bounded number of appends and are then replaced, so lists stay
  short.

The workload records invoke and completion events in execution order. It
marks an ambiguous outcome as `:info` and retires the client's process.
After the cluster heals, it reads every key. It writes each history in EDN,
and we ran elle-cli 0.1.11 with `--model list-append --consistency-models
strict-serializable` on every history.

At first, the workload's own checks covered only properties that need no
dependency graph: every read is a prefix of the final value, and every
acknowledged append is present exactly once. Section 4.2 shows that this
missed stale reads that Elle found. So we added two more checks in Rust,
which run in every simulation run without Java:

- a real-time check: a read sees at least as much of a key as any
  transaction that completed before the read's transaction began;
- a cycle check: the final values give each key's version order, and the
  checker searches the write-write, write-read, read-write, and real-time
  dependencies of committed transactions for a cycle, as Elle does.

The `elle_check` example runs the same checks on an EDN history from the
real cluster.

To make sure that a "valid" result means something, we gave Elle three
broken histories in the same format: a stale read, a write skew, and a lost
append. Elle rejected all three.

### 2.3 A real cluster on MinIO

The simulator cannot reach the real S3 client, the real HTTP stack, the
multi-threaded runtime, or real process crashes. We wrote
`scripts/jepsen/run.py`, which starts MinIO and three or five `baserunna`
release binaries on one machine. It runs the `elle` workload over HTTP
against random nodes. A nemesis waits 2 to 8 seconds, injects one of these
faults, and waits for it to end:

- `SIGKILL` of one node or of every node, with a restart after up to 5 s;
- `SIGSTOP` of a node for 0.5 to 20 s, or of MinIO for 0.5 to 7 s;
- a cut between a node and storage, through a per-node TCP proxy that drops
  bytes (requests time out) or resets connections;
- a cut between a node and its peers, through a proxy on its advertised
  address.

A supervisor restarts nodes that exit, as `systemd` did in Jepsen's Datomic
tests. After the run, the harness heals the cluster, waits three lease
periods, and reads every key. We checked every history with `elle_check`
and with Elle.

The harness cannot skew clocks, because that needs root on macOS. Clock
faults were covered only in the simulator.

### 2.4 Audits

Three audits covered paths that no workload exercised:

- corruption of every persisted format (bitflips, truncation, chunk swaps,
  substituted objects, missing objects) at read time and in background tasks;
- every path by which a write's outcome reaches a client, including the S3
  error mapping in `object_store` 0.14;
- every safety claim in the README, SPEC, OpenAPI document, and client docs.

Each finding that we report as confirmed has a test that fails before the
fix.

### 2.5 Detection power

The repository has a bug zoo of 27 patches that reintroduce past bugs. We
applied seven of them (03, 04, 07, 12, 17, 22 and 27) and ran the `elle`
workload and the `history` workload on each, with 300 to 1,300 seeds, to
compare the checkers. Patch 11 no longer applies to the code.

## 3. Results

### 3.1 No transactional anomalies

We found no transactional anomaly in any configuration. Elle reported no
cycles and no non-cyclic anomalies, and the structural checks found no lost,
duplicated, or aborted-but-visible appends.

The simulator runs were as follows:

- `campaign all` on the original code (commit 609bc87), 45 minutes:
  27,549 runs, about 58 simulated days, 0 failures.
- `elle`, 2,000 seeds with faults: 0 failures in the simulator, and Elle
  found all 2,000 histories valid under strict serializability.
- `campaign all` with the first three fixes, 20 minutes: 14,574 runs, about
  29 simulated days, 0 failures. Commits 7874096, 901e380 and d284785 all
  cite this one campaign.
- Runs of the affected scenarios after later fixes: 57,138 runs, 0 failures.
- `elle` with the new checks, 6,000 more seeds: 0 failures.
- `campaign all` on the final code: @@HEADCAMPAIGN@@

Three longer campaigns (60 to 90 minutes) ended with `SIGKILL` (exit code
137). Each one ran next to a large real-cluster run, so memory pressure is
the likely cause. They printed no failure before they ended, but they did
not report their run counts, so we do not count them.

The real-cluster runs were as follows. All ran on MinIO. `elle_check` found
every history valid, and the last column gives Elle's verdict. "Before
fixes" means commit 76c411c, which has only the fix of section 3.7. "Fixed"
means commit d32f75a, which adds sections 3.2 to 3.4. "Final" means commit
cd30b18, which has every fix except b0d3e88 and the fixes of the second
review (section 3.10). Those fixes change only answers to requests that the
harness does not send.

| run | binary | nodes | lease | minutes | faults | ok | info | Elle |
| --- | ------ | ----- | ----- | ------- | ------ | -- | ---- | ---- |
| r1  | before fixes | 3 | 5 s | 10 | 54  | 39,803  | 116 | valid |
| r2  | before fixes | 3 | 5 s | 20 | 126 | 134,310 | 488 | valid |
| r3  | fixed        | 3 | 1 s | 30 | 182 | 268,679 | 613 | valid |
| r4  | fixed        | 5 | 5 s | 60 | 354 | 465,539 | 3,379 | valid |
| r5  | final        | 3 | 1 s | 40 | 228 | 328,164 | 1,502 | valid |

"ok" counts committed or successfully read transactions, and "info" counts
transactions with an unknown outcome. Run r3 used 30 clients on 4 active keys
for heavy contention, and a 1 s lease so that pauses often crossed a lease
expiry.

These results support the claim of strict serializability for the KV and
stream paths under process crashes, pauses, partitions, and store stalls.
As always with this method, they show only that these histories had no
anomaly. They cannot prove that the system has no bugs.

### 3.2 A lost manifest led to total data loss (fixed)

This was the most serious finding, and it is the same pattern as ParticleDB
#112. Baserunna creates a database's manifest lazily. If a `GET` of the
manifest returned `NotFound`, `Db::refresh_at` created a new, empty manifest.
`Node::open_db` did this for every database that still had a record in the
system database. So if a manifest was lost, through an operator error, a
bucket lifecycle rule, or a bad tool, the database came back empty with no
error. After the 2-hour orphan grace period, the sweep found every old
segment and blob unreferenced and marked them obsolete. 30 minutes later,
they were deleted.

The system database takes the same path at bootstrap. A lost `sys` manifest
would have erased every account, organization, and database record.

The fix relies on an invariant: a database's first manifest is written
before any of its other objects. Before it creates a manifest, the engine now
lists the database's prefix. If it finds any object, it reads the manifest
again, because another node can create it in the meantime. If the manifest
is still missing, the open fails with an internal error, and the engine
writes and deletes nothing.

The first version of the fix omitted the second read. A 20-minute campaign
failed 70 runs, because a concurrent node created the manifest between the
`GET` and the `LIST`. This shows why each fix needs the simulator as well as
a unit test.

Commit: 7874096. Test: `a_lost_manifest_is_not_recreated_over_existing_data`.

### 3.3 A substituted segment was read and then compacted away (fixed)

Every block, bloom filter, index, and footer of a segment has a CRC32C, but
nothing tied a segment object to the `SegmentMeta` in the manifest that
references it. For segments up to 1 MiB, which includes every level 0
segment, the reader decoded whatever valid segment the store returned under
the key. If another segment's bytes were stored there, a scan returned 36 of
50 keys, with no error. Compaction then merged the substitute and dropped the
listed segment, so the loss stayed permanent even after the original bytes
came back.

The reader now checks that the object's size and entry count equal the
manifest's entry. This is the misdirected-write fault that Jepsen used
against ParticleDB.

Commit: 901e380. Test: `a_segment_stored_under_another_segments_key_is_rejected`.

### 3.4 Applied commits were reported as definite failures (fixed)

After an ambiguous manifest `PUT` (a timeout), the committer reloads the
manifest. If the etag has not changed, the first `PUT` may still land, so the
committer rebuilds the batch on the same base. If the rebuilt batch rejected
a transaction, the committer converted only `Unavailable` errors to
`outcome: "unknown"`. Other rejections went to the client as definite
errors. On an unchanged base, a transaction can be rejected the second time
only for a reason that depends on time: a staged blob that passed its 1-hour
staging limit, or a read key that expired. In a test, the client got
`400 invalid_argument` for a commit that then landed. A client that retried
without an idempotency key would have applied the write twice.

The first fix marked every rejection in such a batch as unknown. The review
in section 3.10 showed that this also hid definite conflicts of commits that
no ambiguous write included. The committer now records which commits each
ambiguous write included, and on this path only those can get an unknown
outcome. The paths that give up on a batch still mark every commit unknown.
This is safe but less precise, and TODO.md records it.

Commits: d284785 and 15e5ee9. Tests:
`a_rejection_after_an_ambiguous_write_is_not_definite` and
`a_rejection_that_no_ambiguous_write_included_stays_definite`.

### 3.5 A request's own commit was reported as a replay (fixed)

If an ambiguous `PUT` had in fact landed, the committer found its own commit
records on the reload and answered `replayed: true`. That flag means that an
earlier request committed. For a one-shot transaction with an idempotency
key, the API then returned `results: null`, so the client lost the results of
its own reads. A recorded commit that one of the batch's ambiguous writes
included is now answered as the batch's own.

Commits: e5091e9 and 15e5ee9. Test:
`a_commit_whose_lost_reply_landed_is_not_a_replay`.

### 3.6 A retried interactive commit was told to restart (fixed on one node)

The commit handler removes an interactive transaction from the registry
when the commit starts. A resent commit, from a client that did not receive
the first response, got a retryable `404 txn_not_found`. The SPEC tells
clients that this error means "restart the transaction". A client that
followed it applied the transaction twice. Now the node remembers each
started commit, and a later request on the transaction gets `503` with
`outcome: "unknown"` and the commit ID. The node records the commit before
it releases the transaction's lock, so a request that waits for the lock
cannot slip through. A transaction without writes, or one that fails before
the commit is sent, gets the `404` as before. After a restart or on another
node, the old `404` remains. TODO.md records this gap.

The transaction fuzzer, which checks every response against an independent
model, showed that the first version also answered `503 unknown` after a
read-only commit, which cannot apply anything.

Commits: f402025, 80a1286 and b0d3e88. Tests:
`a_retried_commit_does_not_ask_to_restart`, `a_failed_commit_still_asks_to_restart`,
and `fuzz_txn_ops_against_model`.

### 3.7 Plain-HTTP S3 endpoints did not work (fixed)

`CloudStore` built its client with `AmazonS3Builder::from_env` and then
replaced the client options that it had loaded. So every client option set
through the environment, including `AWS_ALLOW_HTTP`, was ignored, and a node
could not use a plain-HTTP endpoint such as a local MinIO. The store
conformance tests passed `allow_http` as an explicit setting, so they never
tested the path that production uses. The first real-cluster run found this
before it sent a single request. The simulator could not have found it,
because it does not use `CloudStore`.

Commit: 76c411c. Test: `client_options_are_read_from_the_environment`.

### 3.8 The documentation overclaimed (fixed)

An audit of every safety claim found five contradictions or overclaims.
This is the same pattern that Jepsen criticized in ParticleDB:

- SPEC §1 said that transactions are strictly serializable, but snapshot
  isolation and snapshot reads are weaker options. The README said that
  reads and writes "use snapshot isolation".
- SPEC §6.1 said that read-only transactions always succeed, but §5.5 says
  that an interactive read-only commit can fail with `409`.
- SPEC §6.1 said that blind writes never conflict. Under snapshot isolation,
  they do.
- SPEC §5.9 said that snapshot reads are at most 5 s old. That is only the
  default of an uncapped parameter, and successive snapshot reads through
  different nodes can go back in time.
- SPEC §4.6 said that a consumer processes each record exactly once.
  Retention trims records without regard to consumers, and a lagging
  consumer silently skips them.

The OpenAPI document also omitted the 10-minute limit on idempotency, and it
did not warn that a retryable `503` can have an unknown outcome.

Commit: d32f75a.

### 3.9 Open issues

These problems remain. TODO.md lists each of them.

- **Storage integrity.** Blob chunks are never verified against their
  SHA-256, so a flipped bit reaches clients. A sealed stream chunk replaced
  by another chunk of the same stream and size makes reads skip records
  without an error. Blob meta objects have no checksum. Local cache files are
  not bound to their keys. A manifest rolled back to an older version is
  accepted by a node without a cached copy, which then collects the newer
  segments. Each problem needs a store that breaks its contract, so the SPEC
  does not promise to handle it, but each one turns a store fault into
  silent wrong data.
- **Ambiguity after a restart.** Section 3.6 applies only on the node that
  ran the commit.
- **Error flags.** A `503` with an unknown outcome is still marked
  `retryable: true`. `baserunna-client` handles it correctly, but a naive
  client could retry a non-idempotent write.
- **Start-up under a store outage.** A node that cannot reach the store at
  start-up fails its conditional-write check and exits. It needs a process
  supervisor, as Datomic's transactors did in Jepsen's analysis.

### 3.10 Review of the fixes

The review playbook asks for an independent reviewer of each branch. The
reviewer found problems in four of our fixes:

- The ambiguity fix (section 3.4) was too broad, as described above.
- The replay fix (section 3.5) decided "our write landed" by comparing a
  write counter, which missed some cases and ignored event binds.
- The interactive-commit fix (section 3.6) left a race window, and its
  cache cleared every entry when it filled.
- The segment check (section 3.3) passes a substitute with the same size
  and entry count. Its test passed only because the two segments differed
  in size.

We fixed the first three and recorded the fourth in TODO.md. A second
review found three more problems:

- The new cycle checker skipped reads of KV keys that did not exist yet,
  so it missed write skew on new keys. Elle treats such a read as an empty
  list, and now our checker does too.
- The interactive-commit cache still lost every entry when it held 100,000
  live ones. It now evicts the oldest eighth.
- A failure before the commit was sent could still be answered as "may have
  been applied".

Earlier, a campaign had caught a race in the first version of the
lost-manifest fix (section 3.2). We report these mistakes because they are the same failure
that Jepsen describes in ParticleDB: a fix that looks complete, but is not.
Each one was caught by a test or a review, not by reading the fix.

### 3.11 Harness bugs

Jepsen asks testers to check their own tools, so we report three bugs in our
harness. Each one would have hidden or invented results:

- The first TCP proxy closed both directions of a connection on the first
  end-of-file. It did not pass the half-close through as a TCP stack does,
  which broke pooled HTTP connections.
- A client created a stream by retrying the same node for 120 s while that
  node was down.
- A client recorded an `:info` completion for a transaction that it never
  invoked. Elle rejected the history as malformed instead of checking it.

## 4. Does the Simulator Match Jepsen?

The task asked whether an existing tool matches or surpasses Jepsen. Our
answer is yes inside the simulated world and no outside it, so we used both.

### 4.1 Where the simulator is stronger

- **Reproducibility.** Every failure reproduces from its seed, and a run can
  be branched at a virtual time. A Jepsen failure is often hard to
  reproduce.
- **Fault precision.** Crash points stop a node between the internal steps
  of a commit, a compaction, or a sweep. Buggify points force rare paths.
  A store can apply a write and then return an error. Signals and proxies
  cannot place faults this precisely.
- **Volume and breadth.** About 600 fault-injected runs a minute on 4 cores,
  over 25 scenarios. These include auth, organizations, quotas, and
  garbage collection, which Jepsen workloads seldom test.
- **A stronger serializability check.** The `history` checker requires the
  server's reported versions to agree with every observation. So it also
  catches wrong version numbers, which clients use in `check` and `If-Match`
  preconditions. Elle ignores versions. With bug-zoo patch 03 (two
  transactions of a batch get the same version), `history` failed 94 of 300
  seeds, and Elle found 0 anomalies in 300 histories.
- **Checks of its own checkers.** The bug zoo, checker self-tests, and
  mutation testing measure whether each test detects the bugs that it is
  for. Jepsen recommends this, and ParticleDB did not do it.

### 4.2 Where Jepsen's approach is stronger

- **The real system.** The simulator replaces the S3 client, the HTTP
  stack, and the multi-threaded runtime. Our first real-cluster run found a
  `CloudStore` bug (section 3.7) that the simulator cannot reach.
- **An independent checker.** Elle infers the order from observations, and a
  different team wrote it. With bug-zoo patch 04 (no in-flight write barrier,
  so a strong read can miss a write that another read already saw), Elle
  found G-single-item-realtime in 10 of 1,300 seeds, and `history` failed 4
  of 1,300. The checkers detect different bugs, so we recommend that the
  project keep both. After we added the real-time and cycle checks of
  section 2.2, the `elle` workload rejected exactly the 10 seeds that Elle
  rejected. On every history that we gave to both, the two checkers agreed.
  This is evidence, not proof, that the new checks match Elle for this
  workload.
- **History size.** A simulated history holds tens to hundreds of
  transactions. A real-cluster history held up to 1,326,032 events. Bugs
  that need a long history, such as ParticleDB #109 (about once per 300,000
  transactions), are more likely to appear in long histories.

### 4.3 What neither method covered

- clock skew on real nodes (the harness needs root to change clocks);
- membership changes beyond restarts of the same slots;
- MinIO is not Tigris, the only store that the SPEC says has been tested;
- Elle checks of snapshot isolation, tables, event tables, and change feeds
  (the simulator checks these against reference models instead);
- monotonic clocks that run at different rates on different nodes (the
  lease margin of 0.5 s assumes that the drift is small).

Of the 27 bug-zoo patches, the random campaign misses 7 in about 2,800
seeds, and dedicated tests cover only 2 of those 7. In our comparison, bugs
07, 22, and 27 went undetected by both checkers in 300 seeds. Rare bugs need
guided exploration, more seeds, or targeted tests.

## 5. ParticleDB Pathologies in Baserunna

Each item below is a ParticleDB pathology and its status in Baserunna.

- **Lost writes and anomalies in normal operation.** None found. The
  simulator's healthy runs, the fault runs, and Elle all agree.
- **Anomalies under process kills.** None found under `SIGKILL` of one node
  or of all nodes, or under crash points inside commits.
- **Stale reads under strict serializability.** None found on real nodes.
  The simulator's lease oracle and Elle detect the historical bugs of this
  class (patches 04 and 12).
- **Silent damage after file corruption.** Found and partly fixed. No reader
  replays a "valid prefix" of a damaged object: every checksum failure stops
  the read. But a lost manifest and a substituted segment caused silent data
  loss (sections 3.2 and 3.3, fixed), and several smaller gaps remain
  (section 3.9).
- **Acknowledgements before durability.** Not present. A commit is
  acknowledged only after the conditional `PUT` of the manifest succeeds,
  and nodes keep no durable local state.
- **Errors that hide applied writes.** Found and fixed in three places
  (sections 3.4 to 3.6). The client library classifies outcomes carefully,
  and it treats `500` responses and timeouts as unknown.
- **Documentation that overclaims.** Found and fixed (section 3.8). We found
  no claim of a formal proof.
- **Features without tests.** Not found. The coverage test asserts that
  the simulator reaches every documented API operation and every common
  probe, and reference models check the responses of most workloads. One
  test path did differ from production (section 3.7).
- **Unexplained fixes.** Not found. Each historical bug in SPEC §13.6 has a
  cause, and the bug zoo reproduces 27 of them. Each fix in this branch has a
  test that fails without it, except the lock race of 80a1286, which we did
  not reproduce.

## 6. Discussion

### 6.1 Testing practice

Baserunna does most of what Jepsen recommended to Orkes. It has example
tests for each feature, generative tests against reference models, fault
injection, and tests of its tests. This is a large difference from
ParticleDB, and it shows in the results. We found no anomaly in the
transaction core, where ParticleDB had many.

The bugs that we found share one cause: each one sits where no workload
looks.

- The storage bugs needed a store that breaks its contract, and the
  simulated store never does.
- The ambiguity bugs needed a timeout followed by a second failure for a
  different reason, or a client that resends a commit.
- The `CloudStore` bug needed the real S3 client.

Fault injection finds the bugs in the space that it searches, so the space
must include the real binary and a store that misbehaves.

### 6.2 Recommendations

1. Add a misbehaving store to the simulator. It would lose objects, replace
   one object with another, flip bits, and return older versions. The goal
   is not to survive these faults. The goal is to fail loudly, with no
   silent wrong reads and no garbage collection of live data.
2. Run `scripts/jepsen/run.py` before each release, for at least an hour,
   against the production store, and with clock faults on Linux.
3. Keep both checkers. Run Elle on the `elle` histories in CI.
4. Give every error response a single field that says whether the write was
   applied, and make `retryable` false if the outcome is unknown.
5. Use the bug zoo to measure the rare bugs that random campaigns miss, and
   add targeted tests or exploration for them.

### 6.3 Limits of this analysis

We tested one machine, for about 2.7 hours of real-cluster time,
with MinIO instead of Tigris. We did not test the console, OAuth, quotas,
event-table queries, or change feeds with Elle. As with every Jepsen
analysis, these tests can prove the presence of bugs, but not their
absence.

## 7. Reproduction

Simulator:

```sh
SIM_MINUTES=45 cargo run --release --example sim_tool -- campaign all 1000000 70000000
SIM_ELLE_DIR=out cargo run --release --example sim_tool -- campaign elle 2000 1000
java -jar elle-cli-0.1.11-standalone.jar -m list-append -f edn -c strict-serializable out/*.edn
```

Real cluster (needs `minio`, `mc`, and a release build of `baserunna`):

```sh
cargo build --release -p baserunna --bin baserunna
scripts/jepsen/run.py --minutes 30 --out runs/r3 --seed 3 --lease-ms 1000 --clients 30 --active-keys 4 --max-appends 16
java -Xmx8g -jar elle-cli-0.1.11-standalone.jar -m list-append -f edn -c strict-serializable runs/r3/history.edn
```

Bug-zoo comparison:

```sh
git apply bugzoo/04-in-flight-write-barrier.patch
cargo build --release --example sim_tool
SIM_ELLE_DIR=out cargo run --release --example sim_tool -- campaign elle 1000 6000
cargo run --release --example sim_tool -- campaign history 1000 6000
git apply -R bugzoo/04-in-flight-write-barrier.patch
```

Each fix in this branch is its own commit. Its message gives the bug, the
fix, and the test results before and after the fix.
