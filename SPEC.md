# Baserunna Specification

Baserunna is a web service that hosts small databases on object storage.
Each database supports serializable transactions, KV APIs, tables, and
streams. Service metadata is itself stored in a Baserunna database.

Status: v1 specification. Sections marked **(future)** are explicitly out of
scope for v1.

## 1. Goals and non-goals

### Goals (priority order)

1. **Safety.** Acknowledged commits are never lost or reordered; reads never
   observe uncommitted or torn state; transactions are strictly serializable.
   Correctness of committed data depends solely on object storage and does
   not depend on clocks, leases, routing, or membership.
2. **Low cost.** An idle database pays for storage only, not compute. An active
   database pays roughly one object-store PUT per group of commits. Reads on a
   warm node are served from the local cache.
3. **Operational simplicity.** Nodes are stateless and interchangeable. A node
   can be killed at any moment. Any other node can serve reads immediately or
   writes within one lease period.
4. **Testability.** The whole service runs and is tested inside a deterministic
   simulator with fault injection.
5. **Perf, within reason.** Hot reads are served from memory in microseconds.
   Each commit takes about one conditional PUT (tens of ms).

### Workload assumptions

- Databases are typically ≤ 10s of MB and rarely 100s of MB. They may also
  reference large binary assets of up to GBs each.
- Reads are about 10× more frequent than writes.
- One server may have hundreds to thousands of databases active at once, and
  far more that are dormant.
- Servers fail at arbitrary times. Uncommitted transactions may be lost.

### Non-goals (v1)

- Cross-database transactions.
- SQL. Tables have a typed, indexed row API, not a query language.
- Sub-millisecond commit latency.
- Multi-region active/active.

## 2. Storage Backend

Baserunna stores data with a purpose-built engine (`engine` module) that borrows
from SlateDB, an LSM engine for object storage. The original goal was to use SlateDB,
but that idea has been shelved for V1.

Concerns:

- **Idle DB overhead.** Each SlateDB database runs its own WAL flusher, memtable flusher, compactor, and GC tasks. Each open DB requires its own timer.
  In Baserunna we want an idle DB to hold zero tasks, and instead use a cached `Arc<Version>` that the LRU can evict.
- **Commit cost.** Each SlateDB commit is a WAL SST PUT + periodic L0 flush + manifest updates. Each Baserunna update requires one conditional PUT per group commit. Small writes are inlined in the manifest ("tail"), so the manifest doubles as the WAL.
- **Failover.** A new SlateDB writer bumps the writer epoch before serving. In Baserunna, any node can read immediately, and a new writer takes over by CAS after the current lease expires.
- **Fencing.** SlateDB stores the writer epoch in the manifest. Baserunna uses Manifest CAS (`If-Match`), and a stale writer fails at its next commit by construction.
- **Deterministic Simulation.** SlateDB's internal tokio tasks and `object_store` crate make whole-system determinism hard to guarantee. Baserunna routes every source of time, randomness, I/O, and task spawning through an injected `Env`, so the full stack runs deterministically in a test environment.
- **DB Size.** SlateDB is tuned for large datasets. Baserunna is tuned for databases ≤ 100s of MB: a few compaction levels, segments of 2 MiB, and streaming merges with bounded memory.

The engine sits behind the `ObjectStore` trait and a narrow `Database` API,
so swapping the current engine for SlateDB may be possible in the future.
The DST suite would then become its acceptance test.

## 3. Architecture

```
            ┌────────────── load balancer (any node) ───────────────┐
            ▼                         ▼                             ▼
      ┌──────────┐              ┌──────────┐                 ┌──────────┐
      │ node A   │◀── forward ─▶│ node B   │◀── forward ───▶ │ node C   │
      │ API      │              │ API      │                 │ API      │
      │ console  │              │ console  │                 │ console  │
      │ engine   │              │ engine   │                 │ engine   │
      │ caches   │              │ caches   │                 │ caches   │
      └────┬─────┘              └────┬─────┘                 └────┬─────┘
           └─────────────────────────┼────────────────────────────┘
                                     ▼
                               object storage
```

- **Node**: a process running the HTTP API, admin console, engine, and background
  workers. No durable local state.
- **Database (DB)**: an independent keyspace with its own manifest. The system DB
  (`sys`) is a DB with a fixed id.
- **Owner**: the node that currently holds a DB lease (§6). If a DB has a known owner,
  requests are forwarded to it. Otherwise, they are sent to the DB's rendezvous-hash
  home among live nodes (§7).

### 3.1 Code layout

```
src/
  lib.rs            crate root, Node wiring
  main.rs           production binary
  env.rs            Env: clock, RNG, task spawning
  util/             crc32c, varint codec, base64, hex, PRNG, bloom filter
  store/            ObjectStore trait; MemStore, FsStore, CloudStore(feature)
  engine/           segments, manifest and levels objects, versions, reads (with expiry),
                    commits (read/write sets, validation, commit records), change feeds,
                    range tombstones, leveled compaction, local tier, store clock, GC, blobs,
                    size quotas, stream records and sealed chunks
  ops.rs            the JSON transaction-op language
  filter.rs         the filter language tables and streams share
  table/            tuple encoding, schema, rows, indexes, queries
  stream/           stream catalog, record bodies, reads and filters, consumers
  cluster.rs        membership, rendezvous routing, forwarding, leases policy
  meta/             system DB schema + commands (users, sessions, keys, organizations and roles,
                    dbs, nodes) and their invariants
  auth.rs           password hashing, sessions, API keys, mailed-link tokens, Google OIDC
  mail.rs           outbound email (verification links, password resets)
  limits.rs         rate limits and sign-in backoff
  log.rs            structured (JSON) logs with request ids
  api/              axum routes (JSON API), errors, extractors
  console/          server-rendered admin console
  api/openapi.rs    OpenAPI 3.1 document
  client.rs         small async Rust client
  sim/              deterministic simulation: SimStore, transport, fake Google, workloads, checkers;
                    cost and amplification measurements
tests/              integration, property, fuzz and DST suites
examples/           runnable real-world use cases
fuzz/               optional cargo-fuzz targets, share code with tests
```

## 4. Data model

### 4.1 Keys and values

- Keys are byte strings of 1..=4096 bytes, ordered lexicographically by byte.
- Values are byte strings, stored in one of three ways:
  - inline, if at most 32 KiB, in the tail or in segments
  - out-of-band blob, if larger, with the value written as chunk objects and the key storing a `BlobRef` descriptor (§4.3)
  - tombstone, for deletes
- Each committed entry carries a `seq`, the DB version that wrote it.
  The `seq` is reported to a client as the key's `version`.
- A value may expire (§4.5).

### 4.2 Internal keyspaces

Use a prefix byte to avoid collisions between keyspaces.

| byte   | space                                                                               |
| ------ | ----------------------------------------------------------------------------------- |
| `0x00` | catalog (table schemas, DB-level metadata)                                          |
| `0x01` | user KV                                                                             |
| `0x02` | table rows: `0x02 ++ table_id(u32 BE) ++ tuple(pk)`                                 |
| `0x03` | index entries: `0x03 ++ table_id ++ index_id(u32 BE) ++ tuple(cols) [++ tuple(pk)]` |
| `0x04` | row blob columns: `0x04 ++ table_id ++ tuple(pk) ++ column_id(u32 BE)`              |
| `0x05` | stream catalog: `0x05 ++ name`                                                      |
| `0x06` | stream records and chunk references: `0x06 ++ stream_id(u64 BE) ++ seq(u64 BE)`      |
| `0x07` | stream state (the engine's own, §5.11): `0x07 ++ stream_id`                         |
| `0x08` | consumer cursors: `0x08 ++ stream_id ++ consumer`                                   |
| `0xFF` | commit records (the engine's own, §5.5): `0xFF ++ commit_id(u64 BE)`                |

### 4.3 Blobs

A blob is an immutable byte sequence of up to `blob_max` bytes (5 GiB by default).
It is stored as a set of chunk objects `dbs/{db}/blob/{blob_id}/{chunk:08}`. Each
chunk is 8 MiB; the last chunk may be shorter.

Each `blob_id` is 16 bytes: a 48-bit creation time in ms followed by 80 random
bits, hex encoded. Creation time lets commits and GC reason about staged blobs
without extra state.

The inline KV reference to a blob is a `BlobRef { id, size, sha256, content_type }`.

During a write, blobs are staged first (uploaded, unreferenced) and then bound to a key
or blob column by a transaction. A commit rejects binding a blob whose id time is older
than `blob_stage_ttl` (1 h). Both times are store time (§5.8).

Blob reads support HTTP byte ranges. A range maps onto ranged GETs of the affected chunks.

### 4.4 Tables

A table has a name (`[a-zA-Z_][a-zA-Z0-9_]{0,63}`), typed columns, a primary
key of one or more columns, and secondary indexes.

- Column types: `string`, `int` (i64), `float` (f64, NaN rejected), `bool`,
  `bytes` (base64 in JSON), `json` (any JSON value, not indexable), and `blob`
  (written as a staged `blob_id` and read back as `BlobRef` metadata, not
  indexable). Columns are `nullable` (default true) and may have a `default`.
- Primary key columns must be non-null and indexable.
- Indexes cover one or more indexable columns and may be `unique`. A unique
  index treats NULL as distinct: rows with NULL in an indexed column do not
  conflict. NULLs are indexed, so a prefix query may match `null`.
- Encoding: `tuple()` is an order-preserving, prefix-free tuple encoding (see
  `table/tuple.rs`). Its ordering is null < false < true < ints < floats <
  strings < bytes within the same position, with integers ordered
  numerically. Row values use a compact typed binary encoding keyed by
  `column_id`, so renaming or dropping columns never rewrites rows.
- Index maintenance happens inside the writing transaction. Every row write
  reads the previous row (recorded in the read set), removes stale index
  entries, and writes new ones. A unique violation aborts the transaction with
  `409 already_exists`.
- Schema changes (create/drop table, add/drop column, add/drop index) are
  transactional, except that adding an index does not block writes. The
  index is added in the `building` state: every row write maintains it
  (including unique checks), but queries refuse it with a retryable `503`.
  It is then backfilled from the table's rows in batches of separate
  serializable transactions (`index_backfill_batch_rows`, 1000 rows, and at
  most 1 MiB of entries each); the last batch marks it `ready`. Progress is
  kept in its own catalog key, not in the schema that row writes pin, so a
  batch conflicts only with writes to the rows it covers. An interrupted
  build stays `building`, and adding the same index again resumes it. A
  unique index whose rows hold duplicate values is removed again, and the
  request fails with `409 already_exists`.
- Queries: lookup by primary key; range or prefix scan by primary key;
  equality on an index prefix with an optional range on the next column;
  optional residual `filter` (§4.7) whose comparisons name a column (`col`),
  evaluated server-side; `limit`, `reverse`, and an opaque `cursor`.
- Batch updates and deletes change every row a selection (the same access
  path, `eq`, `range` and `filter` as a query) picks, at most `limit`, in
  one transaction. All the selected rows are read before any is written, so
  an update that moves rows within the index it selects through changes
  each once. The transaction's write limit bounds how many rows one request
  may change; `limit` lets a client work through more in several.

### 4.5 Times to live

A value or row written with `ttl_ms` expires that long after the time of
the version that commits it: its **version time** (§5.2), a store-clock
time. An expired value reads as deleted, and like a tombstone it hides any
older value of its key. A row expires together with its index entries and
blob columns, so a unique value is free again once its row has expired. An
update of a row, or an `increment`, keeps the expiry unless it sets one.
Reads report a committed value's expiry as `expires_at` (cluster time,
ms).

Expiry is consistent with transactions:

- Every snapshot reads at one time: the store time at which its version was
  known to be the latest (now, for a lease holder), never earlier than the
  version's own time. A transaction evaluates every expiry at its snapshot's
  time.
- Version times never decrease, and a version's time is never earlier than
  the read time of any transaction it commits, so readers agree with every
  committed transaction about what had expired.
- A serializable transaction that read a value fails to commit with a
  retryable `409 conflict` once that value has expired by the commit's time:
  its view no longer holds. Snapshot isolation does not validate reads, here
  as elsewhere.

Expiry is as precise as the nodes' measurements of the store's clock
(§5.8), whose error is at most `clock_sync_max_error_ms`: for that long
around an expiry, reads may disagree whether a value has expired. Outside
that window, a strong read sees a value until its expiry and never after; a
`snapshot` read sees values as of when its cached version was validated, at
most its staleness bound earlier. A node reads a version holding expiring
values only with a recent clock measurement, and answers `503` while it
cannot take one.

Expired values take space until the next manifest write (in the tail) or
compaction (in segments) reduces them to tombstones (§5.6).

### 4.6 Streams

A stream is a named, append-only log of records in a database: application
logs, chat conversations, sync feeds, an outbox. Appends are transactional,
so a record appended with a row update commits atomically with it.

- **Records** have a body of at most 1 MiB, either bytes or a JSON value,
  optional string headers, a `seq` and a `time`. Seqs are dense: a stream's
  first record is 0 and each append takes the next ones. A record's `time`
  is the version time (§5.2) of the commit that appended it, so times never
  decrease along a stream, and the records of one commit share one. Times
  are as precise as the nodes' measurements of the store's clock (§4.5): a
  record appended just after a read may carry a time slightly before it.
- **Names** are 1-128 characters of `[A-Za-z0-9_.:-]`. Streams are created
  explicitly or by an append with `create`. Deleting a stream deletes its
  records at once, and a stream created again under the name starts at 0.
- **Appends** are blind writes: the committer assigns seqs, so concurrent
  appends to a stream never conflict. An append with `expect_seq` commits
  only if the stream's next seq is still that number; otherwise its
  transaction fails with `412` (optimistic concurrency, as event sourcing
  needs). An append retried with its idempotency key applies once and
  reports the seqs and time the first attempt received. An append of no
  records commits nothing to the stream but still checks `expect_seq`.
- **Reads** start at a seq, a time, the last N records (`tail`) or a
  consumer's cursor, run forward or in reverse, and stop at a limit (at
  most 1,000 records and 16 MiB) or at an end seq or time. A forward read
  that reaches the end can wait for the next append (`wait_ms`), or stay
  open and deliver records as server-sent events.
- **Filters** (§4.7) select records by a JSON body field (`field`, a dotted
  path whose numeric segments index arrays), a header (`header`) or, naming
  neither, the body's text, which only `contains` tests. A filtered read
  examines at most `scan_limit` records (10,000 by default) and says where
  to continue, so its cost is bounded however few records match.
- **In transactions** reads are serializable like any other: a read
  conflicts with a concurrent append only if it reached the end of the
  stream (or an end time no record has reached), and with a trim only if
  the trim removed records it read. Reads see the transaction's own trims,
  but not its appends, whose seqs are assigned at commit.
- **Trims** remove a stream's oldest records: explicitly (`before_seq`), or
  by retention, `retention_ms` (records older than that) and `max_bytes`
  (the oldest records beyond that many body and header bytes). Retention
  keeps at least what it promises, and trims in steps: once a stream is a
  sixteenth past a limit, the lease holder trims it back to the limit,
  shortly after an append or during a sweep (§5.11). A stream may so hold
  up to a sixteenth more than its limits, and briefly more.
- **Consumers** are named cursors stored with a stream, at most at its next
  seq. A consumer that advances its cursor in the transaction that writes
  what it made of the records processes each record exactly once. Stream
  info reports the lag of its first 100 consumers; the consumers route pages
  through all of them.

### 4.7 Filters

Table queries and selections and stream reads filter with one language,
evaluated server-side. A filter is exactly one of `{"and": [f, ...]}`,
`{"or": [f, ...]}`, `{"not": f}` or a comparison: the operand's keys (a
column, a field, a header), `op`, and a `value`. Any other key, or keys of
two of these forms in one object, makes the request `400`.

- **Comparisons**: `eq`, `ne`, `lt`, `le`, `gt`, `ge` (a number, string or
  boolean), `in` (an array of at most 1000 values), `prefix` (a string),
  `contains` (a substring, or an element of an array) and `exists`, which
  alone takes no value. Values are never null.
- **Missing and null are one**: a comparison of a missing or null operand
  is false, `ne` included, so `ne` means "present and different". Only
  `exists` tells such an operand apart, and `not exists` selects it.
- **Values compare within their type**. A column compares with values of
  its type (bytes as base64, by their bytes); only string, bytes and JSON
  columns take `prefix` and `contains`, and blob columns only `exists`. JSON (JSON columns, body fields, headers as strings) compares
  numbers by value (`1` equals `1.0`), strings by their bytes and booleans
  with false first; arrays and objects only equal. A comparison across
  types is false.
- **Limits**: a filter nests at most 16 deep and has at most 256 terms.

## 5. Storage engine

### 5.1 Object layout (per DB)

```
dbs/{db_id}/manifest                 mutable; updated only with If-Match / If-None-Match
dbs/{db_id}/seg/{seg_id}             immutable sorted segment (SST), or a sealed stream chunk (§5.11)
dbs/{db_id}/seg/{id}.levels          immutable list of the segments below level 0 (§5.2)
dbs/{db_id}/blob/{blob_id}/{chunk}   immutable blob chunks
dbs/{db_id}/blob/{blob_id}/meta      the blob's descriptor (BlobRef), for binding by id
dbs/{db_id}/blob/{blob_id}/pin       rewritten by long reads (§5.8)
clock                                every node's store-clock probe (§5.8)
```

The system DB uses `db_id = "sys"`.

### 5.2 Manifest

The manifest is a binary, CRC32C-checksummed document (`engine/manifest.rs`):

```
magic "BRMF", format u16
db_id
version: u64                                  data version = seq of the last commit
write_seq: u64                                incremented by every manifest write
time_ms: u64                                  version time: store time of the latest write, never decreasing
lease: Option<{node_id, expires_at_ms, duration_ms}>
segments: [SegmentMeta]                       by level: level 0 newest first, deeper levels in key order
levels: Option<id>                            the levels object holding the segments below level 0
compaction_seq: u64                           incremented by every compaction install
tail: [Entry]                                 sorted by key, newest value per key (the inline write log)
range_deletes: [(start, end?, seq)]           range tombstones (§5.5)
tombstone_horizon: u64                        tombstones with seq <= horizon may have been purged
version_times: [(version, store_ms)]          sparse samples (≥30 s apart) for time-based policies
obsolete: [(kind, id, since_store_ms)]        segments, levels objects and blobs awaiting deletion
blob_bytes: {bound, swept, swept_at}          blob bytes for the size quota (§5.10)
chunk_bytes: {bound, swept, swept_at}         sealed stream chunk bytes, likewise (§5.11)
```

Each `SegmentMeta` records its level, size, key and seq ranges,
`deletes_applied` (the version through which range tombstones had been
applied when it was written), and how many of its entries expire and the
latest of their expiries.

Every commit rewrites the manifest, so its size is a cost on every write.
Level 0 changes with every flush and stays inline. The deeper levels change
only when compactions install, so once they hold more than
`levels_inline_max` (16) segments, their list moves to an immutable
**levels object** that each compaction replaces (writing it before its
install, like its segments). A 500 MB database's manifest then carries
about 1.5 KB of segment metadata instead of 25 KB. Small databases keep
their levels inline, so opening one still takes one GET. Readers load a
levels object once per compaction: nodes cache them like segment data, and
a manifest whose levels object is unchanged reuses the current Version's
copy.

A **Version** is an immutable, decoded manifest (with its levels) plus the
store's etag. A reader holding a Version sees a consistent snapshot (§5.4).

### 5.3 Segments

Segments are immutable and built from sorted entries:

```
[data block]* [bloom filter] [index] [footer 64 B]
block   = codec, entries (key, seq, kind, value) and their count, + crc32c   target 64 KiB
index   = per block: last_key, offset, len + crc32c
bloom   = 10 bits/key, k = 7, + crc32c
footer  = magic "BRSG", format, offsets/lengths, entry_count, crc32c
```

Each block is compressed on its own with zstd (level 3), so a point read
still fetches and decompresses one block; a block that would shrink by less
than an eighth is stored raw. Storage and transfer are the main costs, and
on row-like data compressed segments take about a fifth of the space
(§5.6).

Segments up to `segment_whole_fetch_max` (1 MiB) are fetched in one GET.
Larger ones are read footer-first, then by ranged GET per block; a forward
scan that has fetched two blocks of a segment reads ahead, fetching the rest
of the blocks it needs in requests of up to `bulk_read_bytes` (1 MiB).

Indexes, blooms and decoded blocks are cached in a node-wide LRU with a byte
budget (`cache_bytes`, default 512 MiB), keyed by `(db_id, seg_id, block)`.
Below it, an optional **local tier** on the node's disk
(`BASERUNNA_DISK_CACHE_DIR`) keeps the stored bytes of small segments, of
large segments' metadata, and of blocks, each checked by CRC32C when read.
Whatever a node fetches or writes goes there, so a restarted node, or one
that gets a database back after a failover, reads from local disk instead
of object storage. Segments are immutable, so no cache ever needs
invalidation.

### 5.4 Reads

- A point lookup checks the tail, then the level 0 segments newest to
  oldest, then at most one segment per deeper level (the one whose key
  range covers the key), skipping a segment if its bloom filter rules the
  key out. The first hit wins, because a shallower source always holds the
  newer entry. A tombstone means not found.
- A range scan does a k-way merge of the tail, each level 0 segment and each
  deeper level (a level is one sorted run, read segment after segment). The
  entry with the highest seq wins per key, and tombstones are skipped.
  Blocks load lazily, so `limit` bounds the I/O.
- Range tombstones apply on top: an entry is deleted if a range tombstone
  covers its key with a higher seq. A Version cuts its range tombstones
  into disjoint fragments, each with the newest seq covering it, so the
  check is a binary search. A point lookup of a deleted key reports a
  tombstone with the range tombstone's seq, even when nothing it deleted
  is left in storage; conflict checks rely on that (§6.1).
- An entry that has expired by the read's time reads as a tombstone of the
  same seq (§4.5).
- Snapshot isolation comes for free: a transaction keeps its `Arc<Version>`
  and its read time. Objects referenced by any version younger than
  `txn_max_lifetime` are never deleted (§5.8).

### 5.5 Commits (group commit + CAS)

Each open DB has a per-node commit queue. One committer task drains it
(spawned on demand; it exits when the queue is empty):

1. Take the batch of pending transactions. Base = the latest Version known
   to this node. The batch's version time is the latest of the base's time,
   the store's clock and the read times of its transactions (§4.5).
2. Lease check: if `base.lease` belongs to another node and is still valid
   from this node's viewpoint (§5.7), fail the batch with `not_owner` (the
   API layer forwards or returns 503).
3. For each transaction in order: if its commit record exists, in the base
   or earlier in this batch, it already committed; report that version
   (`replayed`). Otherwise validate it (§6.1) against all writes with
   `seq > txn.read_version` in base and in transactions already accepted in
   this batch. Rejected transactions get `409 conflict`. Transactions past
   their **commit deadline** (arrival at the first node + `commit_timeout_ms`,
   10 s; see §7 for forwarded requests) are
   refused without being applied. This bounds how long a "zombie" request
   can linger server-side after its client gave up. Each accepted
   transaction gets **its own sequence number** (`base.version + 1`,
   `+ 2`, …), so every committed state of a key has a unique version, even
   though the whole batch costs one manifest write. A `delete_range`
   covering at most `range_delete_points_max` (256) stored entries becomes
   a point tombstone per live key, like single deletes. A larger one
   becomes one **range tombstone** `[start, end) @ seq`, which deletes every
   older entry in the range: it drops the tail's entries there and replaces
   any range tombstone it contains. Dropping a table or an index of any
   size costs a few manifest entries. Writes of the same transaction carry
   the same seq, so they survive their own range delete. A write with a time
   to live gets its expiry from the batch's version time. Each accepted
   transaction also writes its **commit record**.
4. Entries of the tail that have expired by the version time become
   tombstones, and commit records past their retention go. If the tail then
   exceeds `tail_max_bytes` (256 KiB), write it to new level 0 segments
   (unconditional PUTs to fresh random ids; data and commit records in
   separate segments) and clear the tail.
5. Renew the lease (`node_id = self`, `lease_ms`, §5.7) and maybe sample
   `version_times`.
6. `PUT manifest If-Match: base.etag`:
   - **ok**: install the new Version and acknowledge the accepted
     transactions with `version = base.version + 1`.
   - **precondition failed**: someone else committed. Reload the manifest; if
     another node now holds a valid lease, fail with `not_owner`. Otherwise
     go back to 3. Transactions validated earlier are revalidated against the
     new base.
   - **error/timeout (ambiguous)**: reload the manifest and go back to 3,
     which finds the commit records of an attempt that landed. This is safe
     because any delayed copy of our PUT carries the same `If-Match` as the
     retry, so at most one of them applies. After `commit_retry_max`
     ambiguous attempts, fail with `503 unavailable`, `outcome: "unknown"`.
     The client resolves it via `GET /commits/{id}` or by retrying with the
     same idempotency key.
7. If compaction is warranted, schedule it.

While a manifest write is in flight, the node marks the database as
_writing_. Strong lease reads on that node wait for the write to resolve.
The new manifest may already be visible to readers on other nodes, so
serving the older cached version would let reads go backwards in time.

A crash at any point leaves either the old manifest or the new one. A segment
written in step 4 without a successful manifest PUT is an orphan, and orphan
GC removes it (§5.8).

**Commit records** make commits idempotent and their outcomes knowable. A
commit's record is an entry in the engine's own keyspace (`0xFF ++ commit
id`, §4.2) with the commit's version as its seq, written in the same
manifest write as the commit's data, so a commit applied if and only if its
record exists. Records ride the LSM like data: small and in the tail at
first, then in segments, found by bloom-filtered point lookups. Records are
kept for `commit_retention_ms` (10 minutes), measured by the age of their
version (`version_times`), and nothing ever overwrites one, so records past
it are dropped without a tombstone: from the tail at a manifest write, and
in compaction, which drops whole segments of them unread. Flushes and
compactions keep records out of data segments, and level 0 compacts data
and records separately, so neither drags the other into its merges.
Records do not count toward the size quota. Commit ids are 64 bits: among
the records of ten minutes, collisions are negligible.

### 5.6 Compaction

Compaction is leveled, with level sizes derived from the data:

- **Levels.** Level 0 holds flushed tails, newest first; their key ranges may
  overlap. Levels 1 to 6 are each one sorted run of segments with disjoint
  key ranges. The deepest level holds most of the data, and each level above
  it targets `level_size_ratio` (8) times less. Levels whose target would be
  under `level_base_bytes` (1 MiB) stay empty; the shallowest level in use is
  the _base_ level. A 10 MB database uses level 0, a base level of about
  1.3 MB, and the deepest level.
- **Picking.** When level 0 has 4 segments, its oldest (at most 12) merge
  with the base-level segments they overlap. When a deeper level exceeds its
  target, its segment that overlaps the least of the next level, relative to
  its own size, merges with the segments it overlaps there. A segment that
  overlaps nothing below moves down without being rewritten, so appends are
  written about twice.
- **Streaming.** A merge reads each input in requests of up to
  `bulk_read_bytes` and cuts its output into segments of about
  `segment_target_bytes` (2 MiB), each written as it fills. Memory stays
  bounded by one read per input and one output segment, however large the
  database. Sweeps (§5.8) stream the same way.

Only the lease holder compacts, in a background task. An install replaces
exactly its input segments, and only on a manifest with the
`compaction_seq` it was planned from: flushes may have added level 0
segments meanwhile, all newer than its inputs, but no other compaction may
have moved data between levels. Otherwise its output becomes an orphan.

A merge drops a tombstone with `seq ≤ purge_seq` if no level below the
output's has a segment covering its key. `purge_seq` is the newest sampled
version at least `txn_max_lifetime` old. The manifest then records
`tombstone_horizon = max(horizon, max dropped seq)`. A transaction with
`read_version < tombstone_horizon` fails validation conservatively with a
retryable `409 conflict`.

A merge reads at its manifest's version time: entries expired by then
become tombstones (purged as above), and commit records past their
retention go. Every merge also drops the entries its manifest's range
tombstones delete. Before any other compaction, the picker removes what
range tombstones deleted, so the space returns soon after a large delete:

- a segment whose entries a range tombstone all deletes (its key range
  inside the tombstone's, its `max_seq` below the tombstone's) is removed
  from the manifest without being read;
- level 0 segments that may still hold deleted entries merge into the base
  level;
- deeper segments that may still hold deleted entries are rewritten on
  their own level without them.

A segment may hold entries a range tombstone deletes only if it overlaps
the range, has entries older than the tombstone, and was written with a
`deletes_applied` below the tombstone's seq (flushes and compaction
outputs record the version through which their entries had range
tombstones applied). Once no segment may, and the tombstone is older than
`purge_seq`, the next manifest write retires it and raises
`tombstone_horizon` to its seq, exactly like a purged point tombstone.

Expired data is reclaimed the same way: segments of commit records past
their retention are removed unread, and a segment below level 0 at least
half of whose entries have all expired is rewritten on its level, keeping
only tombstones of them.

Leveled compaction replaced size-tiered compaction after measuring both
(`sim::amp`, `cargo run --release --example amplification`): five workloads
on a database of about 10 MB with production sizes and compression off, to
isolate the policy. Write amplification is segment bytes written per
logical byte; space amplification is stored per live bytes, after garbage
collection (settled) and at worst during the run; read amplification is
segments a point lookup may consult, at the end of the run.

| workload                | write, tiered → leveled | space settled | space worst | read      |
| ----------------------- | ----------------------- | ------------- | ----------- | --------- |
| uniform updates         | 4.1 → 7.3               | 1.57 → 1.08   | 1.64 → 1.23 | 3.0 → 2.0 |
| skewed updates          | 3.5 → 6.2               | 1.43 → 1.11   | 1.61 → 1.22 | 3.0 → 4.0 |
| appends                 | 5.3 → 2.1               | 1.03 → 1.03   | 1.07 → 1.07 | 1.0 → 1.0 |
| queue (insert + delete) | 4.1 → 7.1               | 1.70 → 1.37   | 2.18 → 1.53 | 1.6 → 1.0 |
| mixed                   | 3.5 → 5.5               | 1.27 → 1.18   | 1.83 → 1.83 | 4.7 → 1.9 |

The trade favors cost: PUTs are priced per request, not per byte, so
rewriting 2 MiB segments a few more times costs little, while stored bytes
are billed every month. With compression on, the stored bytes are about a
fifth of the above. `tests/cost.rs` budgets these numbers for small
databases, with read amplification averaged over the run (sampled after
every commit), which unlike the end of a run does not depend on where in a
compaction cycle it falls. Commit records (§5.5) add no measurable write
or space amplification.

### 5.7 Leases

- The lease lives in the manifest and is written with the same CAS as data.
  Each renewal records how long it lasts.
- Lease validity never compares wall clocks, so skew and clock jumps cannot
  make lease reads stale. Each node measures on its own monotonic clock: the
  holder relies on its lease until its duration minus a margin after it
  _sent_ the renewing write, and every other node waits until the duration
  plus the margin after it first _saw_ it. A renewal is seen only after it
  was sent, so others always wait longer than the holder relies on it; the
  margin absorbs clock rate drift and scheduling delays. `expires_at` only
  identifies a renewal and hints at routing.
- Commits renew the lease, at no cost, for `lease_ms` (5 s). Read-only
  activity renews it with a lease-only CAS, for the longer `read_lease_ms`
  (15 s), when at least `lease_read_threshold` (16) strong reads happened in
  the current or previous read lease period. Such a renewal happens about a
  second before the lease would lapse, so it covers most of a period. At
  ~$0.005 per 1000 PUTs, a renewal pays for itself versus per-read
  validation GETs once it covers about 12 reads. The margin is 0.5 s.
- First sightings outlive database handles, so evicting an idle handle
  never restarts the wait.
- Non-holders never commit while another node's lease is valid, so the
  holder's cached Version is the latest. That is what makes lease reads
  linearizable.
- If a holder dies, its DBs are write-unavailable until the last renewal's
  duration plus the margin after another node first saw it. A node that
  first looks after the crash may see the renewal just before it would have
  run out, so the wait is at most twice that: 11 s for a database whose last
  renewal was a commit, 31 s for one renewed for reads. Strong reads stay
  available everywhere through validated reads (§5.9).

### 5.8 Garbage collection

- **Store time.** Garbage collection compares timestamps that different
  nodes write with each other and with the store's own `last_modified`, so
  every one of them is in the store's clock. Each node measures its offset
  from it: it overwrites the `clock` object and reads back its
  `last_modified`, which lies between the request's start and the response
  (widened by the store's timestamp precision, a second on S3). The
  interval's midpoint is the offset and its half-width the error. The
  offset is kept against the monotonic clock, so wall-clock jumps do not
  disturb it. A node re-measures every 10 minutes, retries probes whose
  error exceeds `clock_sync_max_error_ms` (10 s), and trusts a measurement
  for an hour. Without one it collects nothing: it neither sweeps, nor
  records or deletes obsolete objects, nor binds staged blobs. Collection is
  therefore independent of node clock skew; the `blob_gc_race_hostile`
  scenario runs with node clocks an hour apart and grace periods of about a
  second.
- **Obsolete segments**: compaction moves replaced segment and levels
  object ids into `obsolete` with `since = now`. Any commit or compaction by the lease
  holder deletes entries older than `gc_grace` (30 min, which must exceed
  `txn_max_lifetime` by twice the clock error bound) and drops them from the
  list.
- **Orphans**: segments, levels objects or blobs in storage that are
  neither referenced nor listed. A periodic per-DB sweep (lease holder, at
  most hourly, only for DBs with activity) streams the database's entries
  to collect blob references and pages through the `seg/` (which holds the
  levels objects too) and `blob/` listings.
  Unreferenced objects whose `last_modified` is older than `orphan_grace`
  (2 h) are moved into `obsolete` (not deleted directly). Unreferenced blobs
  must also be older, by their id's time, than `blob_stage_ttl` plus twice
  the clock error bound, and a commit binds a staged blob only if it is
  younger than `blob_stage_ttl` and not listed obsolete. A sweep reads the
  time before the version whose references it collects, so a blob it finds
  was already too old for any commit that version does not contain.
- **Long reads**: a read that has streamed a blob for `gc_grace / 3`
  (10 min) writes a pin object next to its chunks, and rewrites it as
  often. Before deleting an obsolete blob, GC reads its pin; if the pin is
  fresh (younger than two intervals plus twice the clock error bound), the
  blob stays, an orphan again, for a later sweep. A blob is deleted at least `gc_grace`
  after it stops being referenced, so a read that pins in time keeps its
  blob however long it runs. A read by id of a blob already listed obsolete
  pins it before reading anything. A read that cannot pin (the store is
  unreachable) may fail with a retryable `503`; it never returns wrong
  bytes.
- **Deleted databases**: deletion marks the DB in the system DB. After
  `db_delete_grace` (10 min, longer than metadata cache TTLs), a reaper on
  the system database's routing target deletes every object under
  `dbs/{id}/`, a listing page at a time, then once more after another grace
  period (catching objects a lagging listing missed) before removing the
  record. Each pass is recorded against the pass count it started from, so
  concurrent reapers count it once.

### 5.9 Consistency of reads

| mode               | guarantee                                                            | mechanism                                                                                                                                                                                 |
| ------------------ | -------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `strong` (default) | linearizable                                                         | served from cache if this node holds a valid lease and has no manifest write in flight; otherwise `GET manifest If-None-Match: etag` first (a 304 costs ~1 GET), at most once per request |
| `snapshot`         | serializable snapshot, at most `max_staleness_ms` old (default 5000) | any cached Version validated within the bound                                                                                                                                             |

Transactions always read from a fixed snapshot, and serializable
transactions validate their reads at commit time. A validation that
happened after a request arrived serves all of that request's strong reads,
which keeps them linearizable at the cost of one GET.

### 5.10 Size quotas

Each database may store at most `db_max_bytes` (`BASERUNNA_DB_MAX_MB`,
10 GiB by default; the system database is exempt). Its size is what its
manifest accounts for: segment sizes, the tail, blob bytes and sealed chunk
bytes (§5.11), but not commit records (§5.5). Commits
add the size of each blob they bind to `blob_bytes.bound`; a sweep finds
the distinct blobs the database still references and records their bytes
(`swept`) with the `bound` total of the version it read (`swept_at`). Blob
bytes are `swept + bound − swept_at`: exact after a sweep, an upper bound
until the next.

- **Commits** that write anything other than deletes and trims are refused
  with `507 quota_exceeded` if their keys and values (blobs at full size)
  and appended records would take the size past the quota. The committer checks each commit
  against the size so far in its batch, so a refused commit leaves no
  trace. Commits that only delete always pass, so a full database can
  always be emptied.
- **Uploads** count while they stream and while staged, on the node
  serving them, and fail with `507` once the database's size, the uploads
  in progress and the blobs staged but not yet bound would exceed the
  quota. A staged blob counts until a commit binds it, or for
  `blob_stage_ttl`.
- Deleted data counts until compaction removes it, and overwritten blobs
  until a sweep. After a commit is refused for the quota, the next sweep
  comes after `quota_sweep_interval_ms` (1 min) instead of an hour.
  Dropping a table or deleting a large range frees its segments with the
  first compaction (§5.6).

`GET /v1/dbs/{db}` reports `size_bytes` and `quota_bytes`. Flushes and
compactions can grow a database a little past its quota, by the segment
overhead its commits did not count.

### 5.11 Streams

Stream records take the same path as every other write, and leave it once
there are enough of them to store like a log.

- **Records** are entries `0x06 ++ stream_id ++ seq` whose value holds the
  record's time, its position (the body and header bytes the stream held
  before it) and its body. The committer gives each appended record its seq
  and time, writes it to the tail, and rewrites the stream's **state**
  (`0x07 ++ stream_id`: next seq and position; first seq, position and
  time; last time; retention), all in the batch's one manifest write. An
  append costs no request of its own.
- **Small streams stay in the LSM.** Thousands of conversations taking a
  message a minute each share every flush, and compaction groups their
  records by stream. A log object per stream could not do that without a
  PUT per stream per flush, or rewriting the small objects.
- **Bulky runs are sealed.** When a flush or a compaction writes a run of
  consecutive records of one stream of at least `stream_seal_min_bytes`
  (64 KiB), it writes the run to a **sealed chunk** instead: a segment
  (§5.3) holding only that run, without a bloom filter, cut at
  `stream_chunk_bytes` (2 MiB). In the run's place goes one **chunk
  reference**, keyed by the run's last seq, with the chunk's id and size,
  its first seq and position, its bytes and time range. A chunk is never
  rewritten. A busy log's records fill the tail, so a flush seals them
  straight from it; if what is left fits in half the tail, it stays there,
  and the flush costs one PUT. A log is written to object storage once,
  and its LSM is chunk references, a few thousandths of its size. Records
  of quieter streams flush to level 0 with everything else, and are sealed
  once compaction has gathered enough of them.
- **Reads** scan the stream's keys: records come from the LSM, and a chunk
  reference expands into its chunk's records, read like any segment (block
  cache, local tier, read-ahead). Keyed by its last seq, the reference to
  the chunk holding a seq is the first entry at or after it, so a forward
  read from any seq is one scan. Only runs of consecutive seqs are sealed
  and each record is written once, so chunks never overlap each other or
  records left in the LSM. A seq of a given time or position is found by
  binary search over seqs; a chunk reference settles a probe from its range
  without reading the chunk.
- **Trims** move the state's first seq and write a range tombstone over
  `[0x06 ++ id ++ 0, 0x06 ++ id ++ first)`, which replaces the stream's
  previous one, so that the transactions that read the records trimmed
  conflict (§6.1). Compaction then drops the records and chunk references
  it covers (§5.6). A chunk holding the new first seq stays until it is
  wholly trimmed, and reads skip its trimmed records.
- **Retention.** After a commit, the lease holder looks at the state of each
  stream the commit appended to; if its oldest record is a sixteenth of
  `retention_ms` past it, or it holds a sixteenth more than `max_bytes`, a
  background task finds the trim point of the exact limits by binary search
  and commits the trim. The slack makes each trim remove a share of the
  stream: without it, every append to a stream at its limit would cost a
  second commit. The trim names the retention it was computed for, and
  does nothing if the stream's retention changed before it committed.
  Sweeps (§5.8) do the same for streams no longer appended to.
- **Garbage collection.** Chunks are stored under `seg/` but referenced
  from LSM entries, as blobs are from values: a sweep collects the chunk
  references it streams past, and unreferenced chunks become orphans
  (§5.8). Their bytes count toward the size quota like blob bytes, added
  when a chunk is sealed and recounted by every sweep (§5.10).

`sim::amp` measures streams as it measures compaction (§5.6): about 10 MB
of 200-byte records appended without compression, with sealing and
without it. Write amplification is segment and chunk bytes written per
body byte:

| workload                                   | write, sealed → LSM only | records sealed |
| ------------------------------------------ | ------------------------ | -------------- |
| one log, 50 records a commit               | 1.18 → 3.94              | 100%           |
| 20 feeds of 500 KB, appended in turn       | 2.90 → 7.30              | 88%            |
| 200 conversations of 50 KB, 25 per commit  | 7.11 → 7.11              | 1%             |

The log is written once, its records and their encoding; conversations
too small to seal stay in the LSM, where they cost what any small rows
cost. `tests/cost.rs` budgets these, and the requests of ingestion (§13.4).

---

## 6. Transactions

### 6.1 Semantics

- Default isolation is serializable, strictly serializable for committed read-write transactions.
  This uses optimistic concurrency control with backward validation:
  - the read set holds point keys (including misses) and scanned intervals.
    A scan that stopped at `limit` records only the interval it actually
    covered;
  - validation fails if any entry, including tombstones, in the current
    state with `seq > read_version` intersects the read set. A range
    tombstone newer than `read_version` that intersects the read set fails
    it too, even where it deleted nothing (conservative). So does a value
    the transaction read that has expired by the commit's version time
    (§4.5);
  - a stream read records the range of record keys it covered, through the
    end of the stream if it reached it, so an append after it fails it only
    if the read reached the end, and a trim only if it removed records the
    read returned (§5.11). Against earlier transactions of its batch, a
    transaction is checked with the seqs their appends were given.
- `snapshot` isolation validates only the write set (first-committer-wins).
  Write skew is possible, as documented. Reads that writes are derived from
  are validated anyway: a row write commits only against the table schema
  it was derived from, so a concurrent schema change (a new index, a
  dropped column or table) makes it fail with a retryable `409 conflict`
  rather than miss index maintenance.
- Read-only transactions always succeed and serialize at `read_version`.
- Blind-write transactions never conflict, appends included.
- Reads within a transaction see its own earlier writes, except appends.
- Limits: `txn_max_ops` 10,000; `txn_max_write_bytes` 16 MiB, where blobs
  count only their descriptor; 100,000 appended records; `txn_idle_timeout`
  30 s; `txn_max_lifetime` 5 min.

### 6.2 Forms

1. **One-shot** (`POST /v1/dbs/{db}/txn`): a list of ops executed atomically
   in one request. Supports `check` ops (preconditions) and an optional
   `idempotency_key`. Retrying with the same key never applies twice within
   `commit_retention_ms` (10 minutes) of the first attempt, nor when two
   attempts reach the committer together. A retry that finds the earlier
   attempt applied returns `replayed: true` with `results: null`, and the
   seqs its appends received (`appends`): the commit record keeps the
   commit's time and the first seq and count of each append.
   A retried idempotent request that fails a precondition checks the
   commit status (fenced) before answering 412, because an earlier attempt
   may have applied the write that now makes the check fail. Conflicts are
   retried server-side.
2. **Interactive** (`/v1/dbs/{db}/transactions…`): begin, ops, commit,
   abort. State lives on the owner node. If the owner changes or crashes, the
   transaction is lost (`404 txn_not_found`, retryable) and nothing was
   committed. `begin` returns the transaction's `commit_id` up front, and
   `commit` the seqs its appends received.

**Resolving ambiguity.** A `503` with `outcome: "unknown"`, or a lost
response, means a commit may or may not have applied. The client waits
until the request's commit deadline has certainly passed (about 15 s), then
calls `GET /commits/{commit_id}` (or `key:<idempotency key>`). That call
first performs a manifest write, fencing any in-flight attempt, and then
answers from the commit records (§5.5): `committed` (with its version and
`appends`) or `not_committed` (with `retention_ms`). Commit ids are 16 hex
characters. `not_committed` is definitive only if the first attempt began
less than `retention_ms` ago: an older commit's record may be gone. Past
that, the outcome is unknowable from the server, and a client that must not
apply twice (an append, an increment) resolves it from the data instead.

### 6.3 Ops

| op             | fields                                                                     | result                                                  |
| -------------- | -------------------------------------------------------------------------- | ------------------------------------------------------- |
| `get`          | key                                                                        | `{found, value? \| blob?, version?, expires_at?}`       |
| `scan`         | `prefix` or `start`/`end`, `limit` (≤1000), `reverse`, `keys_only`         | `{items, next_cursor?}`                                 |
| `put`          | key, `value` or `blob_id`, `ttl_ms?`                                       | `{}`                                                    |
| `delete`       | key                                                                        | `{}`                                                    |
| `delete_range` | `prefix` or `start`/`end`                                                  | `{}` (expanded at commit)                               |
| `check`        | key + one of `exists`, `version`, `value`                                  | fails txn with 412 if false                             |
| `increment`    | key, `by` (i64), `ttl_ms?`                                                 | `{value}`. Value is ASCII decimal; missing = 0          |
| `get_row`      | table, `pk` (array or scalar)                                              | `{found, row?, expires_at?}`                            |
| `insert_row`   | table, `row`, `ttl_ms?`                                                    | `{}`. 409 if pk exists                                  |
| `upsert_row`   | table, `row`, `ttl_ms?`                                                    | `{}`                                                    |
| `update_row`   | table, `pk`, `set`, `ttl_ms?`                                              | `{found}`. Partial update                               |
| `delete_row`   | table, `pk`                                                                | `{found}`                                               |
| `query_rows`   | table, `index?`, `eq?`, `range?`, `filter?`, `limit`, `reverse`, `cursor`  | `{rows, next_cursor?}`                                  |
| `update_rows`  | table, `where` (`index?`, `eq?`, `range?`, `filter?`), `set`, `ttl_ms?`, `limit?` | `{count}`                                        |
| `delete_rows`  | table, `where`, `limit?`                                                   | `{count}`                                               |
| `append`       | stream, `records` (`value` or `json`, `headers?`), `expect_seq?`, `create?` | `{count, first_seq?}` (`first_seq` once committed)     |
| `read_stream`  | stream, `from_seq`, `from_time`, `tail` or `consumer`, `until_seq?`, `until_time?`, `reverse`, `limit`, `max_bytes?`, `filter?`, `scan_limit?` | `{records, next_seq, done}`                  |
| `stream_info`  | stream                                                                     | `{first_seq, next_seq, bytes, first_time?, last_time?}` |
| `trim_stream`  | stream, `before_seq`                                                       | `{}`                                                    |
| `get_consumer` | stream, consumer                                                           | `{found, seq?}`                                         |
| `set_consumer` | stream, consumer, `seq`                                                    | `{}`                                                    |

`ttl_ms` (1 ms to 10 years) makes a value or row expire that long after the
write commits (§4.5); `update_row`, `update_rows` and `increment` keep an
existing expiry unless they set one.

A record's body is `value` (a string, in the request's encoding) or `json`
(any JSON value); reads return each record as `{seq, time, headers?, value
| json}`. A read's `next_seq` is where the next page starts: the next
`from_seq` of a forward read, the next `until_seq` of a reverse one. `done`
says the read reached its end seq or time, or the end of the stream.

Keys and values in JSON are UTF-8 strings by default. With
`"encoding": "base64"` on the request, every key and value in the request and
response is standard base64 instead.

### 6.4 Change feeds

`GET /v1/dbs/{db}/changes?after=…` follows the changes to keys (optionally
of a `prefix` or a `start`/`end` range); `GET /v1/dbs/{db}/tables/{t}/changes`
those to a table's rows. A page reports, for every key changed after
`after`, its state as of the page's snapshot: its value (and `expires_at`),
`deleted` (also once its value expired), or for a large range delete one
`deleted_range`. Intermediate values a key had in between are not reported.
Changes are ordered by version, and within a version range deletions come
first; a page holds at most `limit` of them and carries a `next_cursor` to
pass as the next `after`, at once while `more` is true. With `wait_ms` (up
to 20 s), a request that finds no change waits for one: on the lease holder, it
returns as soon as a commit installs, elsewhere it re-reads the manifest
every `watch_poll_ms` (1 s). Applying the pages in order to the state at
`after` yields the current state.

The engine finds the keys changed after a version by merging only the
sources holding entries that new (`Version::changes`): the tail and the
segments whose `max_seq` is past it, so a recent cursor reads little.
Deletions are kept at least `txn_max_lifetime` (§5.6), so a cursor stays
valid at least that long after the request that caught up to it. An older
one may have missed purged deletions: the request fails with `410
cursor_expired`, and the client reads the current state and follows on from
its `version`.

---

## 7. Cluster

- **Cluster time.** Timestamps that one node writes and another compares
  (heartbeats, session expiry, deadlines, deletion times) use the store's
  clock as each node measured it (§5.8), so node clock skew does not affect
  them; a node uses its own clock only until its first measurement.
- **Membership** is stored in the `nodes` table of the system DB. Each node
  rewrites `{id, addr, started_at, heartbeat_at}` every `heartbeat_ms`
  (120 s) and re-reads the table every `members_refresh_ms` (30 s). A node is
  live if `heartbeat_at > now − 3 × heartbeat_ms`. Crashes are detected by
  requests rather than heartbeats: a forward that certainly never arrived
  makes its target a suspect, which stays out of routing until the table
  shows a newer heartbeat from it, or for a backoff that starts at 10 s and
  doubles with each further failure, up to the liveness window. A node that
  shuts down gracefully deletes its row. Rows dead for more than 1 h are
  removed. An idle node costs about 30 PUTs and 120–140 validating GETs an
  hour (about $0.15 a month).
- **Routing**: for a request on DB `d`, the target is the lease holder if a
  valid lease is known, otherwise the live node maximizing
  `hash(node_id ‖ d)` (rendezvous hashing). If the target is another node, the
  request is proxied, streaming large bodies, with `x-baserunna-hops`
  incremented; a request already forwarded twice is handled where it is. If
  forwarding certainly failed (the request never left), the receiving node
  handles the request itself. It can serve reads, and it can write only if
  no other node holds a valid lease (otherwise `503 unavailable` with
  `Retry-After`).
- **Forwarded requests execute at most once.** Each forward carries a random
  `x-baserunna-forward-id`; the receiver runs the first copy and answers
  duplicates (for a minute) with its response. That memory dies with the
  process, so each forward also names its target process
  (`x-baserunna-forward-to`), and a restarted node refuses copies meant for
  its previous run with a not-applied `503`. GET and HEAD are not
  deduplicated.
- **Request ids travel with forwards.** Every request gets an id: the
  client's `x-request-id` if it is 1-64 characters of `[A-Za-z0-9._:-]`,
  otherwise a new one. Forwards (including metadata commands sent to the
  system database's lease holder) carry it in the same header, every
  response echoes it, and each node logs the requests it serves, and
  anything it logs while serving them, under it (§12).
- **Deadlines travel with forwards.** `x-baserunna-deadline-ms` carries the
  first node's commit deadline in cluster time. The receiver caps it at its
  own `commit_timeout_ms`, so a delayed forward cannot commit later than
  the client expects, and clock errors can only shorten it.
- **Internal endpoints** (`/internal/v1/…`, used to run system-DB
  transactions on its owner) always require `x-baserunna-cluster-key`. Its
  value is the configured `BASERUNNA_CLUSTER_SECRET` or, by default, a key
  generated once per cluster and stored in the system database's `config`
  table. Every node reads it from the shared store, which is the trust
  boundary. Comparison is constant-time.
- **Bootstrap**: on start a node creates the `sys` manifest
  (`If-None-Match: *`, so it is idempotent) and runs the schema-migration
  transaction (it checks the catalog first, so it is idempotent). The node
  then registers its heartbeat.

---

## 8. Service metadata (system DB)

Tables in DB `sys`, all defined through the table layer (dogfooding):

| table               | pk                  | columns                                                                            | indexes                                         |
| ------------------- | ------------------- | ---------------------------------------------------------------------------------- | ----------------------------------------------- |
| `users`             | `id`                | email, password_hash?, google_sub?, is_admin, disabled, email_verified, created_at | unique(email), unique(google_sub), (is_admin)   |
| `sessions`          | `token_hash`        | id, user_id, created_at, expires_at, client?, method                               | unique(id), (user_id), (expires_at)             |
| `api_keys`          | `id`                | user_id, name, secret_hash, db_id?, read_only, rate_limit?, created_at             | (user_id)                                       |
| `orgs`              | `id`                | name, created_at                                                                   | unique(name)                                    |
| `org_members`       | `(org_id, user_id)` | role, added_at                                                                     | (user_id)                                       |
| `databases`         | `id`                | owner_id, name, created_at                                                         | unique(owner_id, name)                          |
| `counts`            | `(kind, key)`       | count                                                                              |                                                 |
| `deleted_databases` | `id`                | owner_id, name, deleted_at, purge_passes                                           |                                                 |
| `nodes`             | `id`                | addr, started_at, heartbeat_at                                                     |                                                 |
| `config`            | `key`               | value                                                                              |                                                 |

A database's `owner_id` is a user id or an organization id (`org-…`).
`config` stores generated secrets (`cookie_key`) and the bootstrap flags.

`counts` keeps the numbers that limits and invariants need in one row
each: databases per owner (`max_dbs_per_owner`), members and owners per
organization (`max_members_per_org`, and the rule that an organization
keeps an owner), and organizations per user (`max_orgs_per_user`). A
counter at zero has no row. Commands that change what a counter counts
update it in the same transaction, so two concurrent changes conflict on
it and are retried server-side: two owners demoting each other at once
cannot both succeed. `meta::check_integrity` checks every counter against
the rows it counts, that every organization has an owner, and that no
database, membership, session or API key belongs to a missing user or
organization; the `lifecycle`, `db_churn`, `orgs` and `accounts`
workloads end with it.

A DB's manifest is created lazily on first access (`If-None-Match: *`), so
creating a DB is a single system-DB transaction that reads the owner's
counter and, for an organization, the creator's membership; the unique
`(owner_id, name)` index rejects duplicate names.

Metadata commands that act for a user (creating databases and
organizations, changing members, disabling users, creating sessions and
keys) check that user's rights inside their own transaction: whether
the user still exists and is enabled, and their role. Authorization of
metadata changes is therefore serializable with the changes themselves.

Listings of users, sessions, API keys, organizations, members and
databases are paginated (`limit`, default 100 and at most 1000, and
`cursor`): they are table queries, whose cursors only resume inside the
listing that issued them.

Auth lookups (session, API key, DB and organization name resolution,
membership roles) are snapshot reads of `sys` with `max_staleness` of
2 s, cached per node for at most 30 s. A revocation (of a session, a key,
an account, or a role) takes effect at once on the node that performed it
and within 32 s everywhere else. Only memberships are cached, never their
absence, so a member added is recognized at once everywhere. A snapshot
**miss** (unknown token, database name, or email) is retried with a strong
read before answering. User-facing listings and reads (databases,
organizations, members, sessions, API keys, users) always read strongly.
Together these ensure clients always see their own metadata writes, even
across nodes.

Name resolution is protected against staleness. If a cached name maps to
a database whose manifest is marked deleted, the name is re-resolved
strongly and the request continues against the new database. If metadata
still maps the name to the deleted database, the deletion was interrupted
halfway, and the node completes it. Creating a database whose name belongs
to such a half-deleted database also completes the deletion first. A
manifest is created lazily only after a strong check confirms the database
exists, so a stale cache can never
resurrect a purged database.

---

## 9. Authentication and authorization

- **Password**: `POST /v1/auth/signup` and `POST /v1/auth/login`. The hash is
  PBKDF2-HMAC-SHA256 with a 16-byte salt and 600,000 iterations (configurable;
  the simulator uses fewer), stored as
  `pbkdf2-sha256$<iters>$<salt_b64>$<hash_b64>`. Verification is
  constant-time. Signup policy: `open`, `first_user_only` (default; the first
  user becomes admin), or `closed`. An admin can be bootstrapped from
  configuration.
- **Google OAuth (OIDC authorization code flow)**:
  `GET /v1/auth/google/start` redirects to Google with `state`. The state is
  an HMAC-signed, 10-minute nonce that is also set as a cookie.
  `GET /v1/auth/google/callback` checks the state, exchanges the code at the
  token endpoint, and verifies the ID token. The checks are the RS256
  signature against Google's JWKS (cached), `iss`, `aud == client_id`, `exp`,
  and `email_verified`. A user is matched by `google_sub`, then by verified
  email, and is created if allowed by the signup policy. Matching by email
  links Google to the existing account only if that account has no
  password or has verified its address: otherwise whoever signed up with
  someone else's address could set a password, wait for its owner to sign
  in with Google, and share their account.
- **Sessions**: a 32-byte random token, `brs_<base64url>`. Only its SHA-256
  is stored, with a public id, the client address that signed in and how
  (`signup`, `password`, `google`, `reset`). Default TTL is 30 days. The
  console uses an `HttpOnly`, `SameSite=Lax` cookie (`Secure` when
  `public_url` is https). The API uses `Authorization: Bearer`. Users list
  their sessions and revoke one, or all but the current one.
- **Account self-service** (signed-in sessions only, not API keys):
  changing the password confirms the current one (failures count toward
  the account's sign-in backoff) and ends every other session; the new
  hash is written only if the stored one is still the one checked, so two
  concurrent changes cannot both succeed. Deleting an account confirms the
  password and removes, in one system-DB transaction, the user, their
  sessions, API keys and memberships, and their databases (whose manifests
  are then flagged deleted and later purged, as for `DELETE /v1/dbs/{db}`).
  It is refused for the last enabled administrator and for the only owner
  of an organization. Administrators disable and re-enable accounts.
- **Mailed links** (when the node has a mailer, §12): sign-up and
  `POST /v1/me/verify-email` mail a link that verifies the address;
  `POST /v1/auth/password-reset` mails a link that sets a new password,
  answering the same whether or not the account exists. Links carry a
  token signed with the cluster's `cookie_key`: its purpose, the user id
  and a fingerprint of the address (verification, 7 days) or of the
  current password hash (reset, 1 hour). A reset therefore works once,
  verifies the address, and ends every session. No token is stored. Mail
  per address is rate limited (6 an hour, burst 3).
- **API keys**: `brk_<id>_<secret>`. Only the secret's SHA-256 is stored. A
  key can be scoped to one DB and/or read-only. It is shown once at creation.
- **Rate limits** (`limits.rs`): token buckets, kept per node, so a cluster
  of N nodes admits up to N times each rate. Every authenticated request
  takes a token from its user's bucket (500/s, burst twice that) and, for
  an API key, from the key's (500/s, or the `rate_limit` the key was
  created with); both or neither. Sign-ins, sign-ups and Google sign-in
  take from their client address's bucket (1/s, burst 20), and sign-ups
  also from a second one (20 an hour, burst 5). A refused request gets
  `429 rate_limited` with `Retry-After` and was not applied.
- **Failed sign-ins** back off per account and per client address: after 5
  failures to an account (20 from an address), each further attempt waits
  1 s, doubling to 15 min, until a success (for the account) or an hour
  without failures. During the wait every attempt is refused with `429`,
  the correct password included. Unknown accounts count like known ones,
  so the responses do not reveal which exist.
- **Password hashing** (about a CPU-second at 600,000 iterations) runs on
  blocking threads, at most `hash_concurrency` (the CPU count) at once.
  A sign-in that waits more than 5 s for a slot fails with a retryable
  `503`, so floods of sign-ins cannot exhaust a node.
- **Client addresses**: the connection's peer, or behind
  `BASERUNNA_TRUSTED_PROXY_HOPS` proxies the entry that many places from
  the end of `X-Forwarded-For` (entries further left are the client's to
  forge).
- **Organizations** hold databases that several users share. Members have
  one role each:

  | role     | read data | write data and schemas | create and delete databases | manage members         | delete the organization |
  | -------- | --------- | ---------------------- | --------------------------- | ---------------------- | ----------------------- |
  | `reader` | yes       |                        |                             |                        |                         |
  | `writer` | yes       | yes                    |                             |                        |                         |
  | `admin`  | yes       | yes                    | yes                         | readers through admins |                         |
  | `owner`  | yes       | yes                    | yes                         | everyone               | yes                     |

  An organization always keeps an owner: the last one cannot leave or be
  demoted. Members are added by the email of an existing account, and
  leave by removing themselves. An organization is deleted only once it has
  no databases, so no database outlives its owner. Organization names are
  1-39 characters of `[a-z0-9_-]`, starting with a letter or digit; its
  databases are addressed as
  `org.name`. To anyone outside an organization, it and its databases do
  not exist (404).
- **Authorization**: each request on a database resolves the caller's role
  there: owner of their own databases, their membership role in an
  organization's, owner everywhere for (enabled) site administrators. A
  read-only API key caps it at reader, and a key scoped to one database
  reaches no other (it lists only that database, and no organization's).
  Deleting a database checks the role without caches.
  Console forms are protected against CSRF with a per-session token:
  HMAC(cookie_key, session token hash).

---

## 10. HTTP API

The API is JSON over HTTP. All routes are under `/v1`. The full schema is in
`openapi.json` (OpenAPI 3.1, generated by `src/api/openapi.rs`), served at `GET /v1/openapi.json`. DB-scoped
routes take `{db}`, a reference: `name` (a database of the caller's),
`org.name` (an organization's), or `id:<db_id>` (administrators).
Responses carry `x-request-id` (§7).

Errors are returned as
`{"error": {"code", "message", "retryable", "details"?}}`:

| status | code(s)                                                                    |
| ------ | -------------------------------------------------------------------------- |
| 400    | `invalid_argument`                                                         |
| 401    | `unauthenticated`                                                          |
| 403    | `permission_denied`                                                        |
| 404    | `not_found`, `txn_not_found`                                               |
| 409    | `conflict` (retryable), `already_exists`                                   |
| 412    | `precondition_failed` (includes the failing op index; also an append whose `expect_seq` no longer holds) |
| 410    | `cursor_expired` (a change feed cursor older than the deletions kept, §6.4) |
| 413    | `too_large`                                                                |
| 429    | `rate_limited` (retryable, `Retry-After`; not applied)                     |
| 507    | `quota_exceeded` (the database is at its size quota, §5.10)                |
| 503    | `unavailable` (retryable, `Retry-After`; may include `outcome: "unknown"`; also a query on an index still being built) |
| 500    | `internal`                                                                 |

Routes (see the OpenAPI document for bodies):

```
GET    /healthz                                   liveness (503 while shutting down)
GET    /v1/openapi.json
POST   /v1/auth/signup | /v1/auth/login | /v1/auth/logout
GET    /v1/auth/google/start | /v1/auth/google/callback
POST   /v1/auth/verify-email                       a mailed verification token
POST   /v1/auth/password-reset | /v1/auth/password-reset/confirm
GET    /v1/me                DELETE /v1/me         delete the account (password)
POST   /v1/me/password                             change it; ends other sessions
POST   /v1/me/verify-email                         mail a verification link
GET    /v1/me/sessions       DELETE /v1/me/sessions (all others)   DELETE /v1/me/sessions/{id}
GET    /v1/api-keys          POST /v1/api-keys     DELETE /v1/api-keys/{id}
GET    /v1/orgs              POST /v1/orgs
GET    /v1/orgs/{org}        DELETE /v1/orgs/{org}
GET    /v1/orgs/{org}/members                      POST /v1/orgs/{org}/members (email, role)
POST   /v1/orgs/{org}/members/{user}  (role)       DELETE /v1/orgs/{org}/members/{user}
GET    /v1/dbs (?org=)       POST /v1/dbs (name, org?)
GET    /v1/dbs/{db}          DELETE /v1/dbs/{db}
GET    /v1/dbs/{db}/kv                             scan (query params)
GET    /v1/dbs/{db}/changes                        change feed of keys (§6.4)
GET|PUT|DELETE /v1/dbs/{db}/kv/{*key}              raw bytes; Range, If-Match, If-None-Match
POST   /v1/dbs/{db}/blobs                          stage a blob (streamed body)
GET    /v1/dbs/{db}/blobs/{blob_id}                read a blob (Range)
POST   /v1/dbs/{db}/txn                            one-shot transaction
POST   /v1/dbs/{db}/transactions                   begin
POST   /v1/dbs/{db}/transactions/{id}/ops
POST   /v1/dbs/{db}/transactions/{id}/commit
DELETE /v1/dbs/{db}/transactions/{id}              abort
GET    /v1/dbs/{db}/commits/{commit_id}            resolve an ambiguous commit
GET    /v1/dbs/{db}/tables   POST /v1/dbs/{db}/tables
GET    /v1/dbs/{db}/tables/{t}   DELETE /v1/dbs/{db}/tables/{t}
POST   /v1/dbs/{db}/tables/{t}/columns      DELETE /v1/dbs/{db}/tables/{t}/columns/{c}
POST   /v1/dbs/{db}/tables/{t}/indexes      DELETE /v1/dbs/{db}/tables/{t}/indexes/{i}
POST   /v1/dbs/{db}/tables/{t}/rows                insert (one or many; `upsert` flag)
POST   /v1/dbs/{db}/tables/{t}/rows/get            by pk
POST   /v1/dbs/{db}/tables/{t}/rows/update        by `pk`, or many by `where`
POST   /v1/dbs/{db}/tables/{t}/rows/delete        by `pk`, or many by `where`
POST   /v1/dbs/{db}/tables/{t}/rows/query
GET    /v1/dbs/{db}/tables/{t}/changes             change feed of rows (§6.4)
GET    /v1/dbs/{db}/streams  POST /v1/dbs/{db}/streams (name, retention_ms?, max_bytes?)
GET    /v1/dbs/{db}/streams/{s}   PATCH /v1/dbs/{db}/streams/{s} (retention)   DELETE /v1/dbs/{db}/streams/{s}
POST   /v1/dbs/{db}/streams/{s}/records            append: JSON, NDJSON or lines of text
GET    /v1/dbs/{db}/streams/{s}/records            read: JSON, NDJSON or server-sent events; long poll
POST   /v1/dbs/{db}/streams/{s}/records/query      filtered read
POST   /v1/dbs/{db}/streams/{s}/trim               before_seq
GET    /v1/dbs/{db}/streams/{s}/consumers           limit, cursor
GET|PUT|DELETE /v1/dbs/{db}/streams/{s}/consumers/{c}
GET    /v1/admin/users   GET /v1/admin/nodes   POST /v1/admin/users/{id}/disable
```

Listings take `limit` and `cursor` and answer `next_cursor` (§8).

Raw KV semantics:

- `GET` returns the value with `ETag: "<version>"` and
  `x-baserunna-version`, and `x-baserunna-expires-at` if it expires. Blob
  values are streamed and support `Range: bytes=…` (206). The query
  parameter `consistency` takes `strong` or `snapshot`. Stored bytes never
  run as a page of the service's origin, where an HTML value written by
  one member of an organization would act with the session of another who
  opens it: they are served with `X-Content-Type-Options: nosniff` and
  `Content-Security-Policy: sandbox`.
- `PUT` takes the body as the value; bodies above `value_inline_max` become
  blobs automatically. `If-Match: "<v>"` makes the write conditional on the
  key's version. `If-None-Match: *` makes it create-only. `?ttl_ms=` makes
  the value expire (§4.5). `Content-Type` is kept for blob values.

Streams over HTTP:

- Appending takes `application/json` (`{records, expect_seq?, create?,
  idempotency_key?}`), `application/x-ndjson` (a JSON record per line) or
  `text/plain` (a text record per line); empty lines and a leading byte
  order mark are skipped, so `curl --data-binary @app.log -H
  'content-type: text/plain'` ingests a log file. Query parameters
  `create`, `expect_seq` and an `Idempotency-Key` header (visible ASCII)
  apply to every format. The answer is `{first_seq, next_seq, count, time,
  version, commit_id, replayed}`; a key used by a request other than an
  append is refused.
- Reading answers `application/json` (`{records, next_seq, first_seq,
  tail_seq, done, version}`), or per the `Accept` header or `format`
  parameter `application/x-ndjson` (a record per line, with `next_seq`,
  `first_seq`, `tail_seq` and `done` in `x-baserunna-*` headers) or
  `text/event-stream`. An event stream reads forward: it sends each record
  as `id: <seq>` and `data: <record>`, waits up to `wait_ms` (at most 5
  minutes) for appends, sending a comment every 15 s while idle, and ends at
  `limit` records, at the read's end seq or time, after `wait_ms`, or when
  the node shuts down; the client resumes after `Last-Event-ID`. A read that
  fails before the stream starts answers with its error status.

---

## 11. Admin console

Server-rendered HTML from the same axum app under `/console`. It uses no
JavaScript framework and no external assets. Pages:

- sign in (password and "Sign in with Google"), sign out, forgotten
  password and reset, email verification;
- databases: list, create, delete; per-DB page with a metrics panel (size
  against the quota, levels, tail, pending garbage, lease), key browser
  (prefix scan, view, delete, put, file upload, download), tables (create,
  drop; add and drop columns and indexes; rows), streams (create, delete,
  retention; newest records first, older pages, appending a record;
  consumers), and a transaction runner (JSON);
- organizations: list, create, delete; members (add by email, change
  role, remove, leave) and the organization's databases;
- account: verification status, password change, sessions (revoke one or
  all others), account deletion;
- API keys: list, create (secret shown once), revoke;
- admin only: users (disable, re-enable), cluster nodes (liveness,
  address).

Every listing is paginated. POST forms carry a CSRF token. All output is
HTML-escaped. Downloads are attachments with `nosniff` and a sandboxing
content security policy.

---

## 12. Configuration (environment)

| var                                      | default                         | meaning                                                      |
| ---------------------------------------- | ------------------------------- | ------------------------------------------------------------ |
| `BASERUNNA_LISTEN`                       | `0.0.0.0:8080`                  | bind address                                                 |
| `BASERUNNA_ADVERTISE`                    | `http://127.0.0.1:<port>`       | address peers use to forward                                 |
| `BASERUNNA_NODE_ID`                      | random                          | stable id optional                                           |
| `BASERUNNA_STORE`                        | `fs:./data`                     | `fs:<dir>`, `mem:`, `s3://bucket/prefix`, `gs://…`, `az://…` |
| `BASERUNNA_CLUSTER_SECRET`               | none (required if >1 node)      | internal endpoint auth                                       |
| `BASERUNNA_SIGNUP`                       | `first_user_only`               | `open` / `first_user_only` / `closed`                        |
| `BASERUNNA_ADMIN_EMAIL` / `_PASSWORD`    | none                            | bootstrap admin                                              |
| `BASERUNNA_GOOGLE_CLIENT_ID` / `_SECRET` | none                            | enables Google sign-in                                       |
| `BASERUNNA_PUBLIC_URL`                   | advertise                       | OAuth redirect base, cookie `Secure`                         |
| `BASERUNNA_CACHE_MB`                     | 512                             | block cache budget                                           |
| `BASERUNNA_DISK_CACHE_DIR`               | none                            | directory of the local tier (§5.3); off if unset             |
| `BASERUNNA_DISK_CACHE_MB`                | 10240                           | local tier budget                                            |
| `BASERUNNA_LEASE_MS`                     | 5000                            | lease renewed by commits; leases renewed for reads last 3×   |
| `BASERUNNA_DB_MAX_MB`                    | 10240                           | size quota of each database (§5.10); 0 for none              |
| `BASERUNNA_USER_RPS`                     | 500                             | requests per second per user, per node; 0 for no limit       |
| `BASERUNNA_API_KEY_RPS`                  | 500                             | requests per second per API key without its own `rate_limit` |
| `BASERUNNA_AUTH_RPS_PER_CLIENT`          | 1                               | sign-ins and sign-ups per second per client address          |
| `BASERUNNA_SIGNUPS_PER_HOUR_PER_CLIENT`  | 20                              | accounts created per hour per client address                 |
| `BASERUNNA_TRUSTED_PROXY_HOPS`           | 0                               | proxies appending to `X-Forwarded-For` (§9)                  |
| `BASERUNNA_HASH_CONCURRENCY`             | CPU count                       | password hashes computed at once                             |
| `BASERUNNA_PBKDF2_ITERATIONS`            | 600000                          | password hashing cost for new hashes                         |
| `BASERUNNA_DRAIN_DELAY_MS`               | 0                               | on SIGTERM/SIGINT, fail health checks this long first        |
| `BASERUNNA_SHUTDOWN_TIMEOUT_MS`          | 20000                           | then let in-flight requests finish for at most this long     |
| `BASERUNNA_LOG`                          | `info`                          | `error` / `warn` / `info` (every request) / `debug` / `off`  |
| `BASERUNNA_LOG_FORMAT`                   | `json`                          | `json` (one object per line) or `text`                       |
| `BASERUNNA_MAIL_WEBHOOK_URL`             | none                            | enables mail: each message is POSTed there as JSON           |
| `BASERUNNA_MAIL_WEBHOOK_TOKEN`           | none                            | bearer token for the mail webhook                            |
| `BASERUNNA_MAIL_FROM`                    | `Baserunna <noreply@localhost>` | sender passed to the webhook                                 |

Logs go to stderr, one JSON object per line:
`{"ts", "level", "node", "request_id", "msg", ...}`. At `info` every
request is logged once on each node it reaches, under its request id
(§7): `method`, `route` (the route template, never the path, which may
hold keys), `status`, `ms`, `hops`, and `forwarded_to` when another node
answered. The mail webhook receives `{"from", "to", "subject", "text"}`.

On SIGTERM or SIGINT a node drains (failing `/healthz`), stops accepting
connections, waits for in-flight requests, then releases its leases and
leaves the membership table, so other nodes take over its databases at
once. [`DEPLOYING.md`](DEPLOYING.md) covers TLS, load balancers, the
container image and bucket setup.

`fs:` is for single-node development only. Compare-and-swap is serialized
within one process, so the store takes an exclusive lock on `<dir>/.lock`,
and a second process (or a second store in the same process) fails to open
the directory.

---

## 13. Deterministic simulation testing (DST)

### 13.1 Determinism

- `Env` abstracts wall time, monotonic time, randomness, and task spawning.
  `SimEnv` runs on a tokio **current-thread** runtime with **paused time**.
  Time advances only when every task is idle, so a seed fully determines task
  interleavings. All randomness comes from a seeded xoshiro256** PRNG.
- Production code may not iterate `std::collections::HashMap` with the
  default random state where order affects behavior, and may not use
  `select!` or `tokio::sync::watch`, whose wake order is random. It uses
  `BTreeMap` or deterministic hashers instead. `tests/lint.rs`
  enforces these rules, plus "no direct wall-clock time, randomness or
  `tokio::spawn` outside `Env`".
- Each run produces a trace hash over every request, response, and store
  operation. Running the same seed twice must give the same hash; this is a
  test.

### 13.2 Simulated world

- **SimStore**: an in-memory object store with S3 semantics and seeded
  per-operation faults: latency, errors before apply, **ambiguous**
  failures (applied, response lost), PUTs that land after their caller
  crashed, **stalls** longer than a lease, and **listings** that lag behind
  creates and deletes.
- **SimCluster**: N nodes, each with its own `Node`, clock, task registry
  and local tier (§5.3), which survives the node's crashes like a disk.
  Faults: **crashes** (all tasks and memory lost) and restarts,
  two-way and **one-way partitions**, nodes **cut off from the store**,
  **duplicated and delayed forwards**, **clock skew and jumps**, and latency
  spikes. Clients call random nodes through the full router, which
  exercises forwarding.
- **Configurations**: `basic` (one node, no faults), `faulty` (store and
  network faults, skew within the lease margin) and `hostile` (every fault,
  with skew and jumps ten times the margin; an hour for
  `blob_gc_race_hostile`, whose grace periods are about a second). Safety
  must hold under all three; only availability may degrade.
- **SimOAuth**: a fake Google with an RSA key generated in-process and a
  JWKS, so the full OIDC verification path runs.

### 13.3 Workloads and checkers

| workload       | checks                                                                                                                                                                            |
| -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `kv_model`     | Random KV ops through every API form under faults, each response compared with a model. Ambiguous commits are resolved through `/commits/{id}`. Bulk writes and prefix deletes make range tombstones over segment data, and a final phase outlives every transaction so they retire. A small size quota refuses writes now and then: refused writes leave no trace, deletes are never refused, and the size stays near the quota. A follower of the change feed checks every change against the model and must rebuild the model's state, starting over when its cursor expires. |
| `bank`         | Concurrent transfers across nodes. The total never changes and no balance goes negative, checked during the run and at the end.                                                   |
| `register`     | Concurrent writers and strong readers on a few keys, checked for real-time linearizability.                                                                                       |
| `tables`       | Schema changes and row ops against a model, including updates and deletes by selection that must change exactly the rows the model selects. A follower of the table's change feed must rebuild the model's rows. At quiescence indexes match a recomputation from rows, unique constraints hold, and dropped tables and indexes (large enough to take range tombstones) leave nothing behind. |
| `schema_race`  | Row writes on every path (batch updates and deletes included) and at both isolation levels race schema changes. Indexes must match rows, nothing may be orphaned, and each writer's rows must match its model. |
| `ttl`          | Values and rows with times to live. Each read must see a value until its expiry window (the commit request's span plus the TTL, widened by the clock error bound) and never after, nor a value it replaced; a unique value is free exactly when its row expired. Clients take and renew a lock held by an expiring value: by version, each acquisition commits no earlier than the previous lock expired and each renewal before the lock it read did, which the locks' own expiries tell exactly. |
| `watch`        | A writer commits puts (some expiring), deletes and range deletes while a watcher follows the change feed with long polls through random nodes. Every change received must be what the writer wrote at that version, cursors expire no sooner than a transaction lifetime after they caught up, and the watcher's state must end up equal to the database's. |
| `streams`      | Appenders (some with `expect_seq`, some appending in a transaction that also increments a counter, all with idempotency keys), followers on long polls and event streams, readers by seq, time, tail, consumer, in reverse and with filters, explicit and retention trims, and a stream deleted and recreated, through random nodes under faults. Every record read must be the one appended at its seq, seqs dense and times never decreasing and within their append's window; a strong read must see every append acknowledged before it began and no trim undone; `expect_seq` lets exactly one of the appends expecting a seq through; a counter and its stream must agree; followers receive every record once, in order; retention leaves each stream within its limits; and every stream must end equal to its model. Enough data flows to seal chunks at flush and in compaction. |
| `blobs`        | Uploads of random sizes on every path, range reads, overwrites and deletes, checked by SHA-256. A final GC keeps every referenced object and removes every unreferenced one.      |
| `blob_gc_race` | Blob reads race overwrites, deletes and GC, with grace periods shorter than a download (and, hostile, node clocks an hour apart). A read may fail cleanly but never return wrong bytes. |
| `abandoned`    | Interactive transactions abandoned, aborted or committed late while leases move. No update is lost, abandoned transactions never commit, and open transactions drain.             |
| `db_churn`     | Databases created, used, deleted and recreated under a tiny cache and fast handle eviction. Each matches its model, deleted names answer 404, handles and cache stay bounded, and per-owner database counts match. |
| `auth`         | Signup, login, sessions, API keys (scoped, read-only), logout, revocation, Google OAuth (never into an unverified password account), and who can access which DB.                |
| `orgs`         | Each role does exactly what §9 allows; while roles change, every write's answer is explained by a role held within the cache lag, and owner and deletion races keep an owner and leave no orphaned database. |
| `accounts`     | Password changes, session revocation, mailed verification and reset links, and account deletion take effect everywhere within the cache lag, and leave the metadata consistent. |
| `limits`       | Per-key, per-user and per-address rate limits and sign-in backoff: refused requests get 429 with `Retry-After` and are never applied, a limit leaves other keys, users, accounts and addresses alone, and a sign-in works again once its backoff passes. |
| `lifecycle`    | Create, use, delete, and recreate DBs under crashes. Deleted data is never visible and is eventually purged, and per-owner database counts match.                                 |
| `console`      | Browser-style console flows: cookies, forms (multipart uploads included), CSRF rejection, HTML escaping of hostile input, file upload and byte-exact download, schema editing, pagination, account, organization and admin pages, password reset and verification through the mailbox. |
| `api_tour`     | The remaining endpoints (schema drops, aborts, commit status by idempotency key, metadata) with result checks.                                                                    |
| `chaos`        | Several workloads at once with crashes, partitions, and store faults.                                                                                                             |

Every node runs with rate limits that busy workloads reach now and then;
simulated clients wait out a 429 and resend, as it was not applied. Most
workloads also run under `hostile` (the `*_hostile` scenarios). Every
run ends by checking **storage invariants**: each manifest decodes, its
levels object exists and matches it, every referenced segment and live
blob exists, and every live chunk reference names a chunk that holds
exactly the records it claims, with no overlap within a stream.

### 13.4 Other test layers

- **Unit tests** in each module.
- **Property tests**, using an in-crate generator and shrinker with seeded
  reproduction. Properties: codec round trips; tuple encoding preserves order;
  segment build/read equals the input, compressed or not; an LSM with random
  writes (some expiring), point and range deletes, bulk loads, flushes,
  compactions, purges and time jumps equals a model at every read's time,
  with snapshots taken along the way still reading their state a
  transaction lifetime later, with the deeper levels inline or in levels
  objects, and with the change feed from any earlier version, applied in
  pages to that version's state, giving the current state; streams with
  random appends (some large enough to seal at once), trims, retention
  changes, deletions and recreations, among KV writes, flushes, compactions
  (which seal), sweeps and time jumps, equal a model when read from random
  seqs, times and positions in both directions, now and from snapshots
  taken earlier, with no two chunks or records of a stream overlapping; a
  stream filter evaluates like an independent model of it; a table with
  random row writes, queries and batch updates and deletes through its
  indexes equals a model; range tombstone fragments match a brute-force
  check; base64/varint/bloom laws; random organization, membership,
  database and account commands from random actors (administrators,
  every role, outsiders, disabled and deleted users) succeed or fail
  exactly as an independent model of §9's rules predicts and leave the
  model's state, with the metadata invariants checked after every step;
  database references and log records round-trip.
- **Fuzz tests**: every decoder (manifest, levels object, range tombstone,
  segment and compressed blocks, entries with expiries, tuple, row, blob
  ref, query and change feed cursors, database references, mailed-link
  tokens, multipart form bodies, stream records, chunk references, stream
  state and record bodies) gets
  arbitrary and mutated inputs and must
  return `Err`, never panic. The router gets random JSON bodies and paths and must never answer
  500 or panic. These run in `cargo test` with fixed seeds. The same
  functions are exposed as optional `cargo fuzz` targets in `fuzz/`.
- **Transaction-op fuzzing** (`sim/txn_fuzz.rs`) checks the op language's
  semantics, not just robustness. Fuzzer bytes choose one-shot and
  interactive transactions, some malformed, and an exact model predicts
  every response: results, versions, cursors, failing op indexes, unique
  violations, replays, OCC conflicts, the rows batch operations select,
  which values report an expiry, and stream appends (with `expect_seq`),
  reads, trims and consumers. Rejected requests must leave no trace. The
  model parses ops with its own copy of their types, so it cannot share a
  parsing bug with the service.
- **OpenAPI conformance**: every response in the DST is validated against
  `openapi.json` with an in-crate JSON-Schema subset checker. Every route in
  the router appears in the document, and every documented route exists.
  The checked-in `openapi.json` must equal the generated document.
- **DST coverage**: `tests/coverage.rs` runs every scenario and fails if any
  documented operation was never exercised by the simulator, or if one of
  the storage paths only some data shapes reach never ran: range
  tombstones written and retired, segments dropped and rewritten for
  them, levels objects written, commits refused for the quota, expired
  entries and segments reclaimed, chunks sealed at flush and in compaction,
  and streams trimmed for retention. Nodes
  count these (`engine::Activity`), and the simulator sums them over a run,
  crashed nodes included.
- **Request ids**: a two-node simulation captures every node's log records
  and checks that responses echo valid ids (replacing invalid ones), that a
  forwarded request is logged under its id on both nodes, and that
  metadata commands forwarded to the system database's lease holder carry
  their request's id.
- **Checker self-tests**: a checker that cannot fail proves nothing, so
  each one is fed known-bad input: non-linearizable register histories,
  broken ledgers, missing or stale index entries, orphaned rows, missing
  segments and blob chunks, foreign bytes in blob reads, reads outside
  expiry windows, overlapping lock holders, feed changes no write
  explains, write answers no role in their window explains, and stream
  reads with gaps, foreign or reordered records, a lost append or an
  undone trim.
  Disabling expiry validation, the version time's floor at read times,
  range deletions in the feed, or index maintenance in batch updates each
  fails campaigns of the `ttl`, `watch` and `tables` workloads.
- **Mutation testing** (`scripts/mutants.sh`) checks that the suites catch
  bugs injected into the engine, the table layer, the op language and the
  checkers. Over the whole configuration, before the storage-engine
  follow-up, they caught 1058 of 1083 viable mutants. Over the lines that
  follow-up changed (`--in-diff`, 693 mutants), every viable mutant is
  caught or hangs the suite, except 21 equivalent ones: off-by-one changes
  to timestamp, size and tie-break comparisons, the compression
  heuristic's thresholds, and checks that decoding repeats. The survivors
  in both runs are of these kinds, plus performance heuristics. The script
  also kills mutated tests that allocate without bound, which
  cargo-mutants' timeout cannot stop in time.
- **Cost**: object-store requests dominate cost (goal 2), so `sim::cost`
  counts them per operation on production-configured nodes (20 ms per
  request, virtual time), and `tests/cost.rs` budgets them. Current counts:

  | operation                                     | requests                        |
  | --------------------------------------------- | ------------------------------- |
  | commit (lease holder)                         | 1 PUT                           |
  | 64 concurrent commits                         | 1 PUT in total                  |
  | strong read, lease holder                     | 0                               |
  | strong read, engine on a non-holder           | 1 GET (304)                     |
  | strong read through the API, no lease         | 1 GET                           |
  | snapshot read within staleness                | 0                               |
  | read-heavy database (10 strong reads/s)       | 276 PUT + 54 GET per hour       |
  | cold open + point read, 3 segments            | 2 GET (40 ms)                   |
  | cold full scan, 128 values of 16 KiB          | 1 GET                           |
  | open + full scan after a restart (local tier) | 1 GET (the manifest)            |
  | blob upload / download                        | 0.25 PUT / 0.125 GET per MiB    |
  | steady writes, with flush/compaction/GC       | 1.001 PUT, 0.001 GET per commit |
  | idle database                                 | 0 per hour                      |
  | node overhead (heartbeats, membership)        | 31 PUT + up to 140 GET per hour |
  | stream append                                 | 1 PUT                           |
  | stream tail read, lease holder                | 0                               |
  | log ingestion, 20 KB NDJSON appends           | 1 PUT per append + 5 PUT per MiB, 0 GET |
  | cold read of a sealed log                     | 5 GET per MiB                   |

  `cost_model` prices these with S3 Standard list prices for workload
  profiles (idle, light, typical, heavy); the typical profile (100k reads
  and 10k writes a day, 20 MB) must stay under its monthly budget.

Commands:

```
cargo test                                                   # everything, a fixed set of seeds
SIM_SEEDS=500 cargo test --release --test sim                # longer DST campaigns
cargo run --release --example sim_tool -- campaign all 3000  # very long campaigns
cargo run --release --example sim_tool -- trace <scenario> <seed>
SIM_SEED=12345 cargo test --test sim sim_chaos -- --nocapture # reproduce one seed
cargo run --release --example cost_model                     # requests per operation, $/DB/month
cargo run --release --example amplification                  # compaction's write/space/read amplification
cargo bench --bench throughput                               # wall-clock throughput, thousands of DBs
scripts/mutants.sh                                           # mutation testing (several hours)
```

### 13.5 Defects found by the simulator

Bugs the DST suite found, each now covered by the regular suite:

1. _Read-your-writes across nodes._ A database created through node A was
   briefly invisible through node B, because B read a stale metadata
   snapshot. Fixed by retrying snapshot misses with a strong read (§8).
2. _Lost routing extensions._ Falling back to local handling after a failed
   forward dropped axum's routing extensions, which produced a 500.
3. _Duplicate versions in group commit._ Two writes in one batch were
   acknowledged with the same version, which broke `If-Match`. Fixed by
   giving each transaction its own sequence number (§5.5).
4. _Linearizability._ A lease holder served its cached version while its
   own manifest write was already visible elsewhere. Fixed with the
   in-flight-write barrier (§5.5). The first version of that fix used
   `watch::Sender::send`, which silently drops values when nobody is
   subscribed; the register checker caught it again.
5. _Nondeterminism._ `tokio::sync::watch` wakes waiters in random order.
   Replaced with `Notify`, and a lint now forbids it.
6. _Half-finished deletes._ An interrupted database deletion left a name
   bound to a deleted database forever. Fixed by completing the deletion
   when it is detected (§8).
7. _Zombie requests._ A request still running server-side after its client
   saw a lost response could commit later. Fixed with commit deadlines and
   the fenced status protocol (§6.2).
8. _Stale listings._ An API key created through one node was missing from a
   listing served by another (bounded-staleness read). Listings now read
   strongly.
9. _Stale names after purge._ A cached name for a deleted and already
   purged database produced 404 instead of resolving to its recreation.
   Missing manifests now trigger strong re-resolution as well.
10. _Missing is not deleted._ The first version of the half-deleted repair
    treated a never-opened database (no manifest yet, since manifests are
    created lazily) as deleted. A retried `create` then replaced a fresh
    database. Only a manifest flagged deleted counts now.
11. _Duplicated forwards._ A duplicated forwarded `commit` applied while
    the original reported `404 txn_not_found`, so the client transferred
    again; a duplicated `PUT` resurrected a deleted key. Forwards now
    execute at most once (§7).
12. _Stale lease reads under clock skew._ A node with a slow clock kept
    reading from its expired lease after a node with a fast clock took
    over and committed. Leases are now measured on monotonic clocks (§5.7).
13. _Failover livelock._ The first fix for 12 kept lease sightings in the
    database handle; idle eviction discarded them, so a dead node's lease
    looked new forever. Sightings now outlive handles.
14. _Snapshot isolation and schema changes._ A snapshot-isolated row write
    raced `add_index` with a disjoint write set, so both committed and the
    index missed the row. Schema reads are now validated at every isolation
    level (§6.1).
15. _Collected blobs mid-read._ Reading a blob that GC removed mid-read
    failed with `500`; it now fails with a retryable `503` (§5.8).
16. _Duplicated forwards across a restart._ A duplicate forwarded `DELETE`
    reached its target after a restart had erased the record of the first
    copy, and deleted the database the client had since recreated.
    Forwards are now addressed to one process (§7).

17. _Lease renewals that look alike._ Once leases could have different
    durations, a 3 s commit renewal made 3 s into a 6 s read renewal had
    the same expiry. Another node took it for the renewal it had already
    seen, took over early, and the old holder served a stale lease read (a
    database just created through one node was missing through another).
    Renewals are now identified by the manifest write that made them.
18. _Collection paused by slow clock probes._ A node whose store-clock
    probes were too slow twice in a row skipped every sweep, so garbage
    outlived a full collection cycle. Probes are now retried, and every node
    keeps its measurement fresh in the background.

19. _Sessions that outlived a password change._ Changing a password and
    ending the other sessions were two steps; a crash between them left
    the old sessions working. Sessions now carry a fingerprint of the
    password hash they were made under, so the change ends them in its own
    transaction (§9).
20. _Repairs that claimed success._ Finishing an interrupted database
    deletion answered 404 even when its metadata command failed, so the
    client believed a database gone that its organization still counted
    (and could not be deleted). A failed repair now answers a retryable
    503.
21. _Transient errors as invalid links._ A verification or reset link
    checked while the system database was unreachable was reported as
    invalid (401) instead of retryable.

22. _Lease loss after an ambiguous write._ A commit whose manifest write
    timed out, but had landed, reloaded the manifest, found another node's
    lease and failed with `not_owner`, which clients take for not applied.
    A metadata command retried on that answer created a session twice.
    Such a failure now reports an unknown outcome (§5.5).

23. _Retention left to a node that doubted its lease._ The node whose
    commit called for a retention trim checked its own lease first; after
    an ambiguous write it did not know it held one, and the trim waited
    for the next commit. Trims now commit like any write, fenced by the
    manifest, and replays of a commit schedule its trims too (§5.11).

24. _Status checks past commit retention._ An append whose response was
    lost through a partition and a crash was checked for 131 s, longer
    than commit records were kept; `not_committed` then sent the client
    to append again. The answer now carries `retention_ms`, past which it
    is not definitive (§6.2), and the workload resolves such appends from
    the records it reads.

The cost measurements (§13.4) found five more: a read-heavy database whose
lease had lapsed never acquired it again (paying a validating GET per read
forever), a strong read without a lease validated the manifest twice,
shortening the lease doubled read-only renewals (they now take the longer
read lease, §5.7), levels objects under their own prefix added a LIST
to every sweep (they now share the segments' prefix), and every flush of
a busy log wrote two more small segments, for commit records and stream
state, which compaction then merged: what sealing leaves of a flush now
stays in the tail (§5.11), and ingestion costs 5 PUTs per MiB beyond its
commits instead of 18, and no GETs.

A review alongside the simulator also found that internal endpoints were
open when no cluster secret was configured. Clusters now always generate
a key (§7), and the `auth` workload checks that internal endpoints reject
clients.

---

## 14. Examples (`examples/`)

Each example starts an in-process node (filesystem or memory store), talks to
it over real HTTP, and prints what it is doing:

1. `todo_app`: a multi-user todo service with a table, secondary indexes, and
   paginated queries.
2. `bank_ledger`: interactive serializable transactions, conflict retry,
   idempotency keys.
3. `asset_store`: a media library with large blobs, range reads, and
   metadata rows that reference blob columns.
4. `saas_tenants`: a database per tenant, API keys scoped per DB, read-only
   keys.
5. `feature_flags`: a read-heavy KV config store with snapshot reads and
   compare-and-set updates.
6. `multi_node`: three nodes on one store, forwarding, a node killed
   mid-flight, and failover without data loss.
7. `app_logs`: JSON log lines shipped as NDJSON into a stream with
   retention, the latest errors found newest first by bounded filtered
   scans, `tail -f` with a long poll, and a shipper that forwards each line
   exactly once by advancing its consumer in its transaction.
8. `chat_rooms`: a stream per room created by its first message, a
   message and a counter committed together, members waiting for new
   messages, `expect_seq` for posting only after the latest message, and
   unread counts from consumer cursors.

Tools: `sim_tool` runs simulation campaigns and prints traces.
`cost_model` prints object-store requests per operation and the cost per
database-month, and `amplification` the write, space and read
amplification of compaction and of streams. `gen_openapi` regenerates
`openapi.json`.

---

## 15. Known limitations (v1)

- Every commit rewrites the manifest, and with it the tail and level 0.
- Commits are remembered for 10 minutes: an idempotent retry or a commit
  status query must come within that (§6.2).
- TTL expiry is as precise as the nodes' store-clock measurements (§4.5),
  and expired data takes space until a manifest write or compaction reclaims
  it.
- Change feeds report each key's latest state, not every intermediate
  value, and a cursor more than a transaction lifetime behind may have to
  start over from the current state (§6.4).
- Rate limits and sign-in backoff are kept per node (§9): a cluster of N
  nodes admits up to N times each rate, and a restart forgets them.
- Size quotas are one setting for every database; there are no per-user
  or per-database overrides.
- Roles are per organization: a user cannot be granted one database of
  an organization only.
- Mail is sent only through an HTTP webhook (§12).
- Multi-node deployments need an object store with conditional writes
  (S3, GCS, Azure, R2, MinIO); `fs:` is for one process (§12).
- After a node crash, writes to the databases it leased wait for the lease
  to expire: at most 11 s for a database whose last renewal was a commit,
  31 s for one kept leased by reads alone (§5.7). Reads continue
  immediately.
- Garbage collection relies on the store's `last_modified` timestamps
  coming from one coherent clock, as they do on the major providers.
- `CloudStore` (S3/GCS/Azure through `object_store`) is exercised only by
  compile-time checks here; the simulator tests the same semantics through
  `SimStore`.
- Stream retention runs on the lease holder, so a database with no activity
  at all keeps records past their retention until it is used again
  (§5.11). Trimmed chunks and their quota bytes are reclaimed by sweeps,
  hours later, as overwritten blobs are.
- Stream filters scan records; there is no index on record contents, so a
  query finds needles at the rate it can scan (`scan_limit` records a
  request).

## 16. Future work

Point-in-time restore (keep old manifests); change feeds that keep
deletions longer, or every intermediate value; quotas and rate limits per
user or database, shared across nodes; per-database grants within an
organization; service accounts (API keys that belong to an organization);
stream consumer groups that share a stream's records among workers, with
acknowledgements and redelivery; per-block summaries of stream records
(bloom filters of field values) so that filters skip blocks; merging small
sealed chunks.
