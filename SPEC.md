# Baserunna Specification

Baserunna is a web service that stores small databases on object storage.
Each database supports serializable transactions, KV APIs, tables,
streams, and event tables for analytics. Baserunna service data is itself stored in a Baserunna database.

Status: v1 specification. Sections marked **(future)** are explicitly out of
scope for v1.

## 1. Goals and non-goals

1. **Safety**

- Commited data is never lost or reordered.
- Transactions are strictly serializable.
- Committed data correctness relies on object storage guarantees.

2. **Low cost**

- An idle, cold db avoids paying for for compute resources.
- An active database pays for one object store PUT per commit.
- Warm node reads are served from the local cache.

3. **Operational simplicity**

- Nodes are stateless and interchangeable.
- A node can be killed any time without losing committed data.
- If a node is killed, another node can serve reads immediately or writes within one lease period.

4. **Testability.**

- The service runs and is tested inside a deterministic simulator with fault injection.

5. **Perf, within reason.**

- Hot reads are served from memory.
- Each commit requires one conditional PUT.

### Workload assumptions

- Databases are typically ≤ 10s of MiB and occasionally 100s of MiB.
- Databases may also reference large binary assets of up to GiBs each.
- Event tables may hold up to ~1 TiB of append-mostly events, queried by time range.
- Reads are ~10× more frequent than writes.
- One server may host hundreds to thousands of active databases, and far more idle databases.
- Servers fail arbitrarily. Uncommitted transactions may be lost.

### Non-goals (v1)

- SQL
- Cross-database transactions
- Sub-millisecond commit latency
- Multi-region active/active

## 2. Storage Backend

Baserunna stores data with a purpose-built engine that borrows from SlateDB, an LSM engine for object storage.
The original goal was to use SlateDB, but that idea has been shelved for V1.

SlateDB concerns:

- **Idle DB overhead.** Each SlateDB database runs its own WAL flusher, memtable flusher, compactor, and GC tasks. Each open DB requires its own timer. In Baserunna we want an idle DB to hold zero tasks, and instead use a cached `Arc<Version>` that the LRU can evict.
- **Commit cost.** Each SlateDB commit requires a WAL SST PUT + periodic L0 flush + manifest updates. Each Baserunna update requires one conditional PUT per group commit. Small writes are inlined in the manifest ("tail"), so the manifest doubles as the WAL.
- **Failover.** Each new SlateDB writer bumps the writer epoch before serving. In Baserunna, any node can read immediately, and a new writer takes over by CAS after the current lease expires.
- **Fencing.** SlateDB stores the writer epoch in the manifest. Baserunna uses Manifest CAS (`If-Match`), and a stale writer fails at its next commit by construction.
- **Deterministic Simulation.** SlateDB's internal tokio tasks and `object_store` crate make whole-system determinism hard to guarantee. Baserunna routes every source of time, randomness, I/O, and task spawning through an injected `Env`, so the full stack runs deterministically in a test environment.
- **DB Size.** SlateDB is tuned for large datasets. Baserunna is tuned for small databases.

The engine sits behind the `ObjectStore` trait and a narrow `Database` API,
so swapping the current engine for SlateDB may be possible in the future if necessary.

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

- **Node**: a process running the HTTP API, admin console, engine, and background workers. No durable local state.
- **Database (DB)**: an independent keyspace with its own manifest. The system DB (`sys`) is a DB with a fixed id.
- **Owner**: the node that currently holds a DB lease (§5.7). If a DB has a known owner, requests are forwarded to it. Otherwise, they are sent to the DB's rendezvous-hash home among live nodes (§7).

### 3.1 Object storage primitives

Baserunna uses object storage as its persistence and coordination layer. It depends on five primitives provided by modern object stores:

1.  `PUT <key> <data> <mode>` provides atomic writes. Write mode is specified by an HTTP header:

    - Overwrite: Write unconditionally. Used for immutable data objects (segments, blob chunks) before they are referenced anywhere. No HTTP header.
    - Create: Create an object if it does not already exist, or fail with `PreconditionFailed` if the object already exists. HTTP header is `If-None-Match: *`.
    - Update: Update an object with a compare-and-swap (CAS), using a stored object's `etag` as comparison key. Fails with `PreconditionFailed` if the object `etag` does not match the header key. HTTP header is `If-Match: <etag>`.

2.  `GET <key> <opts>` provides strongly-consistent reads. Options are specified by HTTP headers:

    - `If-None-Match: <etag>` returns `NotModified` (HTTP 304) if a node's cached manifest is still current.
    - `Range: bytes=start-end` fetches a byte slice of an object.

3.  `DELETE <key>` provides idempotent key deletion.
4.  `LIST <key-prefix>` returns object keys, sizes, etags, and `last_modified_ms` in key order.
5.  The `last_modified_ms` timestamp returned by every `GET` and `LIST` provides a strongly consistent **store time** across nodes.

### 3.2 Conceptual data layout

Each database is stored as an LSM tree written under the key prefix `dbs/{db_id}/`. The database root is stored at `dbs/{db_id}/manifest` and is its sole mutable object. Every other database object is immutable.

#### Manifest

```
dbs/{db_id}/manifest
  ├── lease                            Current owner node + renewal duration
  ├── version (seq) & time_ms          Monotonic commit sequence number & store timestamp
  ├── tail: [Entry, ...]               Inline write log for recent small writes (≤ 256 KiB)
  ├── L0 segments ───────────────────▶ dbs/{db_id}/seg/{seg_id}        (immutable SSTs)
  └── deeper levels ─────────────────▶ dbs/{db_id}/seg/{id}.levels     (immutable level list)
                                         └──▶ dbs/{db_id}/seg/{seg_id} (L1–L6 SSTs)

Out-of-band data (referenced by descriptors inside LSM entries):
  ├── large values (> 32 KiB) ───────▶ dbs/{db_id}/blob/{blob_id}/{chunk:08} (8 MiB chunks)
  └── bulk stream runs (≥ 64 KiB) ───▶ dbs/{db_id}/seg/{seg_id}              (2 MiB sealed chunks)
```

The manifest file holds both database metadata and its write-ahead log (WAL), `tail`. The two are combined in order to halve the number (and thus cost) of `PUT` requests needed to commit a transaction. The manifest file is updated with a CAS `PUT`.

A committed transaction is assigned a monotonically increasing `seq` number. Each key written during the transaction is stamped with its `seq`. In memory, a decoded manifest is called a **Version**. The version captures the manifest's `seq`, `etag`, `time_ms` in store time, tail, and references to all active segments. Holding an `Arc<Version>` gives a reader or transaction a point-in-time snapshot of the database.

#### Segments and Levels

If the inline `tail` exceeds `tail_max_bytes` (256 KiB), the lease holder flushes the tail entries in the background to a new immutable **segment** (§5.5). Each segment is an SSTable (Sorted String Table) designed for random and sequential reads over object storage (§5.3):

- Segment entries are sorted by key, with at most one entry per key
- A segment is divided into 64 KiB data blocks, each compressed independently with zstd and checksummed with CRC32C.
- After its data blocks, the end of the segment holds a bloom filter (10 bits/key), a block index that maps from each block's highest key to its byte offset and length, and a fixed 64-byte footer.
- Because blocks are compressed independently and indexed by key, a point lookup never has to download or decompress an entire multi-megabyte segment. If a node has cached a segment's bloom filter and block index, it can rule out missing keys in memory or fetch a single 64 KiB block with a ranged `GET`.

Segments are organized into levels, from 0-6:

- **Level 0:** Each L0 segment holds a flushed tail, so L0 segments may contain overlapping key ranges. L0 segments are ordered newest-first in the manifest.
- **Levels 1–6:** Each L1-L6 segment contains a sorted run of ~2 MiB segments of strictly disjoint key ranges `[min_key, max_key]`. Each level targets 1/8th the byte size of the level below it, sized from L6 upward (§5.6).
- **Compaction:** If L0 accumulates 4 segments, or a deeper level exceeds its size target, a background compaction task on the lease-holding node merges overlapping segments from that level into the target level below. The merge keeps only the newest `seq` for each key, drops shadowed values, converts expired keys into tombstones, and purges tombstones once no deeper level contains the key. Because commits only touch the tail and L0, background compaction runs concurrently without blocking writes.

#### External `.levels` Objects

Each commit rewrites the manifest, so the manifest must stay small even as a database grows. Level 0 segment metadata is stored inline in the manifest because it changes on every flush, but once deeper levels (L1–L6) hold more than 16 segments, their metadata list is moved out to an immutable `dbs/{db_id}/seg/{id}.levels` object that only changes when a compaction completes.

#### Out-of-Band Blobs and Stream Chunks

A **blob** is a value larger than 32 KiB. Blobs are uploaded out-of-band as 8 MiB chunk objects under `dbs/{db_id}/blob/{blob_id}/`. The LSM key stores only a tiny `BlobRef` descriptor (ID, size, SHA-256, content type).

A **stream chunk** is a sequence-ordered segment of stream records. Each stream starts in the tail, but if a flush or compaction encounters ≥ 64 KiB of consecutive records for the same stream, it writes them into a dedicated 2 MiB segment (a "sealed chunk") and replaces the records in the LSM with a single compact chunk reference.

## 4. Data model

### 4.1 Keys and values

- A key is a byte string of 1..=4096 bytes, ordered lexicographically by byte.
- A value is a byte string, stored in one of three ways:
  - inline, if ≤ 32 KiB, in the tail or in a segment
  - as an out-of-band blob, if larger, with the value written as a set of chunk objects and the key storing a `BlobRef` descriptor (§4.3)
  - as a tombstone, for deletes
- Each committed entry carries a `seq` that identifies the DB version that wrote it.
  The `seq` is reported to a client as the key's `version`.
- A value may expire (§4.5).

### 4.2 Internal keyspaces

Use a prefix byte to avoid collisions between keyspaces.

| byte   | space                                                                               |
| ------ | ----------------------------------------------------------------------------------- |
| `0x00` | DB level metadata and table schemas, DB-level metadata                              |
| `0x01` | user KV                                                                             |
| `0x02` | table rows: `0x02 ++ table_id(u32 BE) ++ tuple(pk)`                                 |
| `0x03` | index entries: `0x03 ++ table_id ++ index_id(u32 BE) ++ tuple(cols) [++ tuple(pk)]` |
| `0x04` | row blob columns: `0x04 ++ table_id ++ tuple(pk) ++ column_id(u32 BE)`              |
| `0x05` | stream catalog: `0x05 ++ name`                                                      |
| `0x06` | stream records and chunk references: `0x06 ++ stream_id(u64 BE) ++ seq(u64 BE)`     |
| `0x07` | internal stream state (§5.11): `0x07 ++ stream_id`                                  |
| `0x08` | consumer cursors: `0x08 ++ stream_id ++ consumer`                                   |
| `0x09` | unsealed events: `0x09 ++ table_id ++ version(u64 BE) ++ ordinal(u32 BE)`           |
| `0x0A` | event part refs: `0x0A ++ table_id ++ partition(u32) ++ part_id`                    |
| `0x0B` | event table state (§5.13): `0x0B ++ table_id`                                       |
| `0xFF` | internal commit records (§5.5): `0xFF ++ commit_id(u64 BE)`                         |

### 4.3 Blobs

A blob is an immutable byte sequence of up to `blob_max` bytes (5 GiB by default).
It is stored as a set of chunk objects `dbs/{db}/blob/{blob_id}/{chunk:08}`. Each
chunk is 8 MiB; the last chunk may be shorter.

Each `blob_id` is 16 bytes: a 48-bit creation time in ms followed by 80 random
bits, hex encoded.

The inline KV reference to a blob is a `BlobRef { id, size, sha256, content_type }`.

During a write, blobs are first staged (uploaded, unreferenced) and then bound to a key
or blob column by a transaction. Blob staging is time-limited: a commit refuses to bind a blob
whose id time is older than an hour (`blob_stage_ttl`).

Blob reads support HTTP byte ranges. A range maps onto ranged GETs of the affected chunks.

### 4.4 Tables

A table has a name, typed columns, a primary key of one or more columns, and secondary indexes.

- Name format: `[a-zA-Z_][a-zA-Z0-9_]{0,63}`
- Column types: `string`, `int` (i64), `float` (f64, NaN rejected), `bool`,
  `bytes` (base64 in JSON), `json` (any JSON value, not indexable), and `blob`
  (written as a staged `blob_id` and read back as `BlobRef` metadata, not
  indexable). Columns are `nullable` (default true) and may have a `default`.
- Primary key columns must be non-null and indexable.
- Indexes cover one or more indexable columns and may be `unique`. A unique
  index allows multiple NULL values in an indexed column without conflict.
  NULLs are indexed, so a prefix query may match `null`.
- Encoding: `tuple()` is an order-preserving, prefix-free tuple encoding (see
  `table/tuple.rs`). Its ordering is null < false < true < ints < floats <
  strings < bytes within the same position, with integers ordered
  numerically. Row values use a compact typed binary encoding keyed by
  `column_id`, so renaming or dropping columns never rewrites rows.
- Index maintenance occurs during a write transaction. Each row write
  reads the previous row (recorded in the read set), removes stale index
  entries, and writes new ones. In case of an index uniqueness violation,
  the transaction is aborted with `409 already_exists`.
- Schema changes (create/drop table, add/drop column, add/drop index) are
  transactional, except that adding an index does not block writes. The
  index is added in the `building` state: every row write maintains it
  (including unique checks), but queries that use it fail with a retryable `503`.
  The index is backfilled from table rows in batches of separate serializable
  transactions (`index_backfill_batch_rows`, 1000 rows, and at most 1 MiB of
  entries each); the last batch marks the index `ready`. Progress is
  kept in the index catalog key. If a build is interrupted, its status remains
  `building`, and adding the same index resumes the build. A unique index whose
  rows hold duplicate values is removed again, and the request fails with `409 already_exists`.
- Queries: lookup by primary key; range or prefix scan by primary key;
  equality on an index prefix with an optional range on the next column;
  optional residual `filter` (§4.7) whose comparisons name a column (`col`),
  evaluated server-side; `limit`, `reverse`, and an opaque `cursor`.
- Batch updates and deletes change every selected row, at most `limit`, in
  one transaction. All selected rows are read before they are written, so
  an update that moves rows within an index will modify each row once.
  Each transaction is limited in the number rows it may change; `limit`
  lets a client work through more.

### 4.5 Time To Live

A value or row may be written with a TTL, implemented as **version time** (§5.2).
An expired value is treated as if it has been deleted. If a row expires, its index
entries and blob columns also expire. Updating a row does not update its TTL unless
a new TTL is explicitly set. Each read reports a committed value's expiry as `expires_at` (cluster time, ms).

Transactions evaluate TTLs against a single timestamp rather than the wall clock, so keys never expire mid-transaction:

- **Snapshot read time.** When a transaction begins, it records a `read_time` equal to the store time when its snapshot was last validated (or `now` on the lease holder), clamped to at least the snapshot's own `version_time`. A key is considered expired if `expires_at <= read_time`.
- **Monotonic version time.** Each commit assigns the new version a `version_time` of `max(parent.version_time, store_now, max(txn.read_time))`. Because `version_time` is monotonic and at least the `read_time` of every transaction in the commit, a commit never observes a key as live after a constituent transaction saw it expire.
- **Conflict on expiry.** Under serializable isolation, a transaction fails to commit with a retryable `409 conflict` if any key it read has expired by the commit's `version_time`. Snapshot isolation does not validate reads for expiry.

Nodes measure store time by probing the object store's clock (§5.8), within an error bound of `clock_sync_max_error_ms`. Within that window around `expires_at`, nodes may disagree on whether a key has expired; outside it, strong reads never see an expired key. If a database contains expiring keys and a node lacks a recent clock measurement, reads return `503`.

Expired entries remain on disk until the next manifest write (for the tail) or compaction (for segments) converts them to tombstones (§5.6).

### 4.6 Streams

A stream is a named, append-only log of records. Appends are transactional,
and a stream record may be committed alongside a row update.

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
- **Appends** are blind writes: the committer assigns sequence numbers, so
  concurrent appends to a stream never conflict. An append with `expect_seq`
  is committed only if the stream's next sequence number matches; otherwise
  its transaction fails with `412` (for optimistic concurrency). An append
  retried with its idempotency key is applied once and returns the sequence
  numbers and timestamp from the original commit. An empty append writes no
  records but still validates `expect_seq`.
- **Reads** start at a sequence number, a timestamp, the last N records (`tail`),
  or a consumer cursor, scan forward or in reverse, and stop at a limit (up to
  1,000 records or 16 MiB) or at an end sequence number or timestamp. At the
  end of a stream, a forward read can wait up to 50 s for a new append
  (`wait_ms`) or stream records as server-sent events.
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

Table queries, row selections, and stream reads share a server-side filter language. A filter object is `{"and": [f, ...]}`, `{"or": [f, ...]}`, `{"not": f}`, or a leaf comparison containing an operand (`col`, `field`, or `header`), an operator (`op`), and an argument (`value`). Requests that combine multiple filter forms in a single object or include unknown keys are rejected with `400`.

- **Operators**: `eq`, `ne`, `lt`, `le`, `gt`, `ge` (for numbers, strings, or booleans), `in` (an array of up to 1,000 non-null values), `prefix` (strings), `contains` (substring or array element), and `exists` (which does not take a `value`). Filter arguments cannot be `null`.
- **Null and missing values**: Any comparison other than `exists` against a missing or `null` operand evaluates to `false`, including `ne`. Use `{"not": {..., "op": "exists"}}` to match missing or `null` values.
- **Type semantics**: Typed columns are compared against values of the same type (`bytes` columns accept base64 strings and are compared lexicographically by raw bytes). Only `string`, `bytes`, and `json` columns support `prefix` and `contains`, and `blob` columns support only `exists`. JSON values (JSON columns, stream fields, and string headers) compare numbers by exact numeric value (`1` equals `1.0`, and `2^53 + 1` is greater than `2^53`), strings by UTF-8 bytes, and booleans with `false < true`; arrays and objects support only equality. Cross-type comparisons evaluate to `false`. Row-table `float` columns reject integer filter arguments that cannot be represented exactly as IEEE 754 `f64`, whereas event-table `float` columns compare against 64-bit integers using exact numeric ordering.
- **Limits**: Filters may have a maximum depth of 16 and at most 256 nodes.

### 4.8 Event tables

Event tables store append-mostly, timestamped records in columnar parts and execute analytical queries over time ranges with bounded memory. Because event tables reside in the same database as KV pairs, row tables, and streams, a transaction can insert events alongside row updates, and event queries can enrich events with lookups from row tables.

- **Schemas**: An event table defines up to 256 columns of type `time` (milliseconds since the Unix epoch, formatted as an integer or RFC 3339 timestamp), `string`, `int`, `float`, `bool`, or `json` (a JSON object queried by leaf path). A required non-nullable `time` column serves as the partitioning and range-filter timestamp; if its default is `"now"`, omitted timestamps are populated from the serving node's clock. The `order_by` key (1 to 8 columns) defines the sort order within storage parts and accelerates filters on leading sort columns. Events are partitioned by UTC time (`hour`, `day` by default, `week` starting Monday, or `month`) so range queries and retention drops can skip whole partitions. Optional `bloom` skip indexes accelerate equality and `in` filters on scalar columns. Columns can be added or dropped online; a newly added column with a default value applies retroactively to older events, and a newly added `time` column with default `"now"` must be nullable because older events evaluate to `null`. Column defaults are limited to 1 KiB, and `json` columns cannot have defaults, sort keys, or skip indexes.
- **Inserts**: Events are inserted either in bulk (`POST /v1/dbs/{db}/events/{t}/rows` as JSON or NDJSON) or transactionally (`insert_events` in `/txn`). Each event is limited to 1 MiB encoded and 4 MiB of flattened JSON paths and values. After applying column defaults, the total encoded size of an insert batch is bounded by `max(body_limit, 8 * request_body_bytes)` for bulk inserts or `txn_max_write_bytes` for transactions. Bulk inserts on a node are coalesced for up to 200 ms, written directly to object storage as columnar parts, and committed in a single manifest update. Bulk inserts with an `Idempotency-Key` are applied at most once; replaying the key returns the original count, and reusing a key from a different operation type is rejected with `400`. Timestamps must fall within years `0000` through `9999`. In JSON columns, `null` object fields are omitted, `-0.0` is normalized to `0`, and integers outside the signed 64-bit range are rejected with `400`.
- **Queries**: Queries scan a half-open time range `[from, to)` on the table's time column, apply an optional filter (§4.7, with `path` for JSON columns), and return either paginated rows or grouped aggregates. Query results are invariant to how events are partitioned or merged: all aggregate states are associative and commutative, and floating-point sums and averages use exact accumulator arithmetic (returning `null` if the final `f64` result overflows).
  - _Expressions_: An expression references a column (`"user"`), a JSON leaf path (`{"col": "props", "path": ["plan"]}`), a dimension lookup (`{"lookup": "plan"}`), or a time bucket (`{"bucket": {"col": "time"}, "interval": "1d", "offset": "-05:00"}`), with an optional `"as"` alias. Supported bucket intervals are seconds (`s`), minutes (`m`), hours (`h`), or days (`d`) aligned to the Unix epoch, weeks (`w`) starting Monday, and calendar months (`mo`). JSON paths address leaf values (scalars, arrays, or empty objects); paths to non-empty intermediate objects evaluate to `null`.
  - _Row queries_: Selecting `columns` (or omitting both `group_by` and `aggregates`) returns raw events ordered by the time column or by `order_by` output aliases, returning 100 rows by default (up to 10,000) along with a `cursor` for pagination.
  - _Aggregate queries_: Specifying `group_by` or `aggregates` groups matching events and evaluates named aggregate functions in alphabetical key order. Supported functions (`{"fn": ..., "of": ..., "where": ...}`) are `count`, `sum`, `min`, `max`, `avg`, `uniq` (HyperLogLog, exact up to 1,024 distinct values), `uniq_exact`, `quantiles` (DDSketch over `q` levels within 1% relative error), `window_funnel` (evaluating ordered `steps` filters within `window_ms`, supporting `strict_order`, `strict_deduplication`, and `strict_increase` per attempt), and `retention` (returning a boolean array indicating whether a group satisfied `conditions[0]` and each subsequent `conditions[i]`). An optional `then` stage re-aggregates the first stage's output columns. Results are sorted by `order_by` and truncated to `limit`; queries without a `limit` fail with `413` if group count exceeds 100,000.
  - _Lookups_: The `lookups` map (`{"plan": {"table": "users", "key": "user", "value": "plan", "default": ...}}`) joins an event expression against the primary key of a row table in the same database and returns the target column (or `default` if unmatched). Each referenced column of a lookup table is scanned once per query into a shared compact lookup map.
  - _Consistency_: Queries use `snapshot` consistency by default (reading a snapshot at most 5 s or `max_staleness_ms` old) or `strong` consistency to observe all acknowledged writes. If a cached snapshot predates the target event table or a referenced column, the query automatically retries with a strong snapshot.
  - _Resource limits_: A query is rejected with `413` if it reads more than 4 GiB of part data, exceeds 1 GiB of aggregate state / buffered rows plus active column vectors, or exceeds 1 GiB of lookup maps. Queries that run longer than 60 s are canceled with a retryable `503`.

  Example two-stage funnel query with a dimension lookup:

  ```json
  {
    "from": "2026-10-01",
    "to": "2026-11-01",
    "lookups": { "plan": { "table": "users", "key": "user", "value": "plan" } },
    "group_by": ["user", { "lookup": "plan", "as": "plan" }],
    "aggregates": {
      "steps": {
        "fn": "window_funnel",
        "window_ms": 86400000,
        "steps": [
          { "col": "event", "op": "eq", "value": "view" },
          { "col": "event", "op": "eq", "value": "buy" }
        ]
      }
    },
    "then": {
      "group_by": ["plan", "steps"],
      "aggregates": { "users": { "fn": "count" } }
    }
  }
  ```

- **Deletes**: `POST /v1/dbs/{db}/events/{t}/rows/delete` (or `delete_events` in `/txn`) transactionally removes events matching a filter within `[from, to)`. If a delete produces a mask larger than the per-partition limit, the request fails with `413` so the caller can delete narrower time ranges. Masked rows are physically purged from object storage within roughly 26 hours on active databases: background maintenance rewrites parts whose oldest deletion exceeds `event_purge_ms` (24 hours), and garbage collection removes the replaced part objects 30 minutes later (§5.8). Events deleted while still unsealed in the LSM tree become standard LSM tombstones and are reclaimed during segment compaction.
- **Retention**: If `retention_ms` is configured on an event table, queries exclude events older than `store_clock - retention_ms`, and background maintenance drops parts whose `max_time` is older than the cutoff (§5.8). If the node does not have a valid store-clock measurement, retention filtering and drops are paused.
- **Dropping tables**: Dropping an event table executes in constant time by writing range tombstones over the table's unsealed rows and part references without scanning existing parts, avoiding conflicts with concurrent inserts or merges. Unreferenced part objects are reclaimed asynchronously by the orphan sweep (§5.8).
- **Quotas**: Unsealed event bytes and referenced part bytes count toward the database size quota (§5.10), and total event part storage per database is capped at 1 TiB. Background seals, merges, deletes, and retention drops bypass quota checks so a full database can still compact and free space. Sealing unsealed events creates at least one ~330-byte part per partition, which can increase accounted size if events are sparsely distributed across many partitions.

## 5. Storage engine

### 5.1 Object layout (per DB)

```
dbs/{db_id}/manifest                 mutable; updated only with If-Match / If-None-Match
dbs/{db_id}/seg/{seg_id}             immutable sorted segment (SST), or a sealed stream chunk (§5.11)
dbs/{db_id}/seg/{id}.levels          immutable list of the segments below level 0 (§5.2)
dbs/{db_id}/blob/{blob_id}/{chunk}   immutable blob chunks
dbs/{db_id}/blob/{blob_id}/meta      the blob's descriptor (BlobRef), for binding by id
dbs/{db_id}/blob/{blob_id}/pin       rewritten by long reads (§5.8)
dbs/{db_id}/part/{part_id}/{chunk}   immutable event part chunks, or a deletion mask (§5.13)
clock                                every node's store-clock probe (§5.8)
```

### 5.2 Manifest

The manifest is a binary, CRC32C-checksummed document (`engine/manifest.rs`). The body is compressed with zstd (level 1) if compression reduces its size by at least 12.5%. To avoid recompressing the entire tail on every commit, the tail is split at content-defined key boundaries into ~32-entry pages. Each page is compressed as an independent zstd frame and cached by its SHA-256 digest so that commits only compress modified pages.

```
magic "BRMF", format u16, codec u8
db_id
version: u64                                          data version = seq of the last commit
write_seq: u64                                        incremented by every manifest write
time_ms: u64                                          version time: store time of the latest write, never decreasing
lease: Option<{node_id, expires_at_ms, duration_ms}>
segments: [SegmentMeta]                               by level: level 0 newest first, deeper levels in key order
levels: Option<id>                                    the levels object holding the segments below level 0
compaction_seq: u64                                   incremented by every compaction install
tail: [Entry]                                         sorted by key, newest value per key (the inline write log)
range_deletes: [(start, end?, seq)]                   range tombstones (§5.5)
tombstone_horizon: u64                                tombstones with seq <= horizon may have been purged
version_times: [(version, store_ms)]                  sparse samples (≥30 s apart) for time-based policies
obsolete: [(kind, id, since_store_ms)]                segments, levels objects, blobs and parts awaiting deletion
blob_bytes: {bound, swept, swept_at}                  blob bytes for the size quota (§5.10)
chunk_bytes: {bound, swept, swept_at}                 sealed stream chunk bytes, likewise (§5.11)
part_bytes: u64                                       bytes of referenced event parts and masks (§5.13)
flush_seq: u64                                        incremented by every tail flush (§5.5)
```

Each `SegmentMeta` records its level, size, key and seq ranges,
`deletes_applied` (the version through which range tombstones had been
applied when it was written), and how many of its entries expire and the
latest of their expiries.

Every commit rewrites the manifest, so its size is a cost on every write.
Level 0 changes with every flush and stays inline. The deeper levels change
only when compactions are installed, so once they hold more than
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

### 5.3 Segments (SSTables)

A segment is an immutable SSTable containing sorted, deduplicated key entries.

#### Binary Layout

```
[data block]* [bloom filter] [index] [footer 64 B]
block   = codec, entries (key, seq, kind, value) and their count, + crc32c   target 64 KiB
index   = per block: last_key, offset, len + crc32c
bloom   = 10 bits/key, k = 7, + crc32c
footer  = magic "BRSG", format, offsets/lengths, entry_count, crc32c
```

Each segment entry encodes a `key` (length-prefixed bytes), `seq` (varint), a `kind` tag byte (`0` = inline value, `1` = `BlobRef`, `2` = tombstone, with bit `0x10` set if an expiry timestamp follows), the value payload, and an optional `expires` timestamp (varint store time in ms). A block is decoded in memory with inline keys and values from a zero-copy slice into the decompressed block's `Bytes` buffer.

Entries are appended to a block in strictly increasing key order. The block is flushed whenever the uncompressed buffer reaches its target size of 64 KiB. The end of the uncompressed buffer holds a `u32` entry count. Each block is then compressed independently with zstd level 3. Compression rules:

- If the uncompressed block is ≥ 64 bytes and zstd shrinks it by more than 12.5%, the block is stored as `codec = 1 (zstd) ++ raw_len (varint) ++ compressed_bytes ++ crc32c: u32`.
- Otherwise it is stored uncompressed as `codec = 0 (raw) ++ raw_bytes ++ crc32c: u32`.

Following data segments, a bloom filter is built over all 64-bit hashes of all segment keys. The bloom filter uses 10 bits per key, `k = 7` probes, estimated ~1% false positive rate. The bloom filter is checksummed by a `crc32`. The bloom filter is omitted from stream segments, which do not index keys because they are scanned sequentially.

Following the bloom filter, the segment records `(last_key, offset: u64, len: u32)` for each data block, checksummed by a `crc32c`.

The segment footer is a fixed 64-byte tail at the end of the object, containing the `"BRSG"` magic bytes, format version, byte offsets and lengths of the bloom filter and block index, total entry count, `min_seq`, `max_seq`, and a `crc32c`. Each section (block, bloom filter, block index, and footer) is independently verified by its own CRC32C on read.

#### Fetching from Object Storage

Object storage fetching is built to minimize `GET` requests, which have ~10–50 ms latency and per-request pricing.

Small segments (≤ 1 MiB) are fetched in a single `GET`. The node immediately decodes and caches the segment index (bloom filter + block index) and all decoded data blocks in memory. All Level 0 segments (~256 KiB uncompressed) use this path.

Large segments (> 1 MiB) are loaded lazily:

1. On an index miss, the node fetches the 64-byte footer to learn `bloom_off`, then fetches the bloom filter and block index together in a ranged `GET`. Both are cached locally.
2. A point lookup checks the cached bloom filter first. If it matches, it binary-searches the block index and issues a ranged `GET` for the matching data block.

During a forward range scan, the first 2 block reads of a segment fetch individual blocks. After fetching 2 blocks of the same segment from object storage, the scan switches to bulk read-ahead and coalesces consecutive uncached blocks in the scan range (up to 1 MiB) into a single ranged `GET`. Compactions and garbage-collection sweeps read existing entries from the memory and disk caches without populating the caches on misses, so background merges do not evict the working set.

#### Local Caching

Each node caches decoded blocks, segment indexes, and `.levels` objects in a sharded in-memory CLOCK cache (up to 512 MiB). Written segments are pre-cached. Cache hits set an atomic reference bit under a shared lock, and insertions evict the first unreferenced entry under an exclusive lock.

Each node also caches up to 10 GiB on local disk. Small segments (≤ 1 MiB) are stored as a single file (`#all`). Larger segments store their metadata tail as `#meta` and individual compressed blocks as `#b0`, `#b1`, …. Each file is written atomically with a trailing CRC32C checksum.

Segments are immutable, so cached segment data is never invalidated.

### 5.4 Reads

A transaction reads segments from the manifest version that was current when the transaction began. Concurrent commits and compactions may write new manifests and new segment files without mutating existing ones. To prevent garbage collection from deleting an old segment file while a transaction is still reading it, transactions are capped at 5 min and replaced segments are not deleted from object storage until a 30 min grace period has elapsed (§5.8).

Point (single key) lookup:

1. Binary search the in-memory `tail`.
2. Check Level 0 segments from newest to oldest, filtering to segments whose `[min_key, max_key]` covers `key`.
3. Check at most one segment per deeper level (Levels 1–6), found by binary-searching the level's disjoint segment list.
4. For each candidate segment, load its segment index, check the bloom filter for likely key membership, binary-search the block index for the candidate block, load the block, and binary-search its entries.
5. Sources are searched from shallowest (newest) to deepest (oldest), so the first match wins. If a range tombstone covers a key, the lookup returns a synthetic tombstone carrying the range tombstone's `seq` to ensure transaction conflict checks (§6.1) detect the deletion.

Range scan:

1. Perform a lazy `k`-way merge across the tail and overlapping segments. Each level L1–L6 is considered a single cursor that steps through its disjoint segments in key order.
2. For each key, the cursor entry with highest `seq` wins. Older entries for that key are ignored. Any entry covered by a newer tombstone is ignored.
3. The cursor loads blocks whose key range intersects with the scan range, and stops as soon as `limit` is reached.

Any entry with `expires <= read_time` is treated as a tombstone.

### 5.5 Commits

Each open database has a per-node commit queue drained by a single committer task. The committer batches pending transactions into a single conditional `PUT` on the manifest:

1. Drain a batch of pending transactions from the queue and load `base`, the latest `Version` cached on this node.
2. Verify that `base.lease` is not held by another node (§5.7). If another node holds a valid lease, fail the batch with `not_owner` so the HTTP layer can forward or return `503`.
3. Compute the batch's `version_time` as `max(base.time_ms, store_now, max(txn.read_time))` (§4.5).
4. For each transaction in the batch:
   - Skip and return the recorded version if its commit record already exists in `base` or earlier in the batch, or reject it with `409 aborted` if its commit record is an abort record (§6.2).
   - Reject it if its 10-second request deadline has expired (§10).
   - Validate its read set (§6.1) against all writes with `seq > txn.read_version` in `base` and in earlier accepted transactions in the batch, rejecting conflicts with `409 conflict`.
   - Assign the accepted transaction the next sequence number (`base.version + 1`, `+ 2`, …), merge its writes into `tail`, and write its commit record.
5. Expire old `tail` entries. Install any completed background tail flush, or flush the tail synchronously if it exceeds `4 × tail_max_bytes` (§5.5).
6. Renew `manifest.lease` for 5 s (§5.7).
7. Write the updated manifest with `PUT manifest If-Match: base.etag`, install the new `Version` in memory, and acknowledge the accepted transactions.
8. Schedule background compaction if needed (§5.6).

While a manifest `PUT` is in flight, strong reads on the committing node wait for that `PUT` to finish, since another node may already observe the new manifest in object storage. A read waits only for the `PUT` that was in flight when the read arrived, so back-to-back commits cannot starve readers.

Single-request serializable transactions begin immediately on the node's latest cached version without waiting for an in-flight manifest `PUT`, so concurrent writes are batched into the next commit. If the transaction contains writes, commit validation checks its read set against the in-flight commit. If the transaction is read-only or fails before committing, the node waits for the in-flight `PUT` to finish, validates the read set against a strong snapshot, and re-executes the transaction on conflict. Interactive serializable transactions also begin on the latest cached version. Because their read results have already been returned to the client, a read-only commit that fails validation returns `409 conflict` instead of re-executing. Snapshot-isolation transactions wait for a strong snapshot before starting.

#### Range Deletes

A `delete_range` with size ≤ 256 is committed as individual point tombstones. A larger `delete_range` is committed as a single range tombstone `[start, end) @ seq` to `manifest.range_deletes` that drops covered entries from `tail` and removes any older range tombstones contained within `[start, end)`.

#### Tail Maintenance and Flushing

Before writing the manifest, any `tail` entries with `expires_at <= version_time` are converted into tombstones, and commit records older than 10 min are removed.

If `tail` exceeds `tail_max_bytes` (256 KiB), the lease holder flushes that version's tail entries in the background to new Level 0 segments, writing user data and commit records to separate segments. The flush is installed on the next commit (or after 1 s if the database is idle) by adding the new segments to Level 0, removing tail entries with `seq <= flushed_version`, and incrementing `flush_seq`. If background flushes fall behind and `tail` exceeds `4 × tail_max_bytes`, the committer flushes `tail` synchronously before writing the manifest.

A background flush install is rejected and its output objects are deleted if `flush_seq` changed since the flushed version, if tombstones newer than the flushed version were purged, if the flush exceeded the orphan grace period (§5.8), or if the node does not hold the lease.

#### CAS Conflicts and Retries

If a conditional `PUT` on the manifest does not succeed on the first attempt:

- On `412 Precondition Failed`, another node updated the manifest. The committer reloads the manifest. If another node now holds a valid lease, the batch fails with `not_owner`; otherwise the committer re-runs validation and applies the batch against the new `base`.
- On a timeout or network error, it is unknown whether the `PUT` succeeded. The committer reloads the manifest and retries against the new `base`. If an earlier `PUT` succeeded, step 4 finds the batch's commit records in the reloaded manifest and returns their committed versions without re-applying the writes. If the reloaded manifest's etag differs from all etags targeted by the batch's ambiguous `PUT`s and contains none of the batch's commit records, none of the ambiguous `PUT`s can succeed, so the batch outcome is settled. If retries are exhausted or the lease is lost while the outcome remains ambiguous, the committer returns `503 unavailable` with `outcome: "unknown"` for undecided transactions, while returning the recorded outcome for any transaction in the batch whose commit record is already present in the latest manifest.

#### Hedged Manifest Writes

If a manifest `PUT` does not complete within `manifest_put_hedge_ms` (250 ms), the committer sends a duplicate `PUT` with the same `If-Match` precondition. Because both requests use the same precondition, storage applies at most 1 of them. The attempt succeeds as soon as either request succeeds. If the duplicate fails with `412 Precondition Failed` while the first request is still in flight, the first request may have already been applied, so the committer treats the attempt as a timeout and reloads the manifest immediately. While the first request remains in flight after a rejected duplicate, strong reads bypass the local lease and validate the manifest from storage. On any other error, the committer waits for both requests to finish. Objects removed from `obsolete` by the planned manifest have already passed their grace period, so they are deleted even if the outcome of the `PUT` is unknown.

#### Commit Records

Commit records make retries idempotent and let clients check the outcome of ambiguous commits. If a transaction is accepted, the committer writes an entry in the system keyspace (`0xFF ++ commit_id`, §4.2) with the transaction's committed version as its `seq` in the same manifest update as its data writes. A transaction was committed if and only if its commit record exists and is not an abort record.

A commit status check runs through the committer queue. If the commit ID does not have a commit record, the committer writes an empty **abort record** in a new manifest version. Any subsequent commit attempt for that commit ID sees the abort record and fails with `409 aborted`, allowing status checks to return immediately without waiting for in-flight attempts to expire.

Commit records are stored in the LSM tree alongside regular keys, starting in `tail` and flushing to bloom-filtered segments. Checking a commit ID is a standard point lookup. Commit records are retained for 10 min and do not count toward the database size quota. Because commit records are never overwritten, expired records are dropped without writing tombstones. Flushes and Level 0 compactions keep commit records in dedicated segments separate from user data so commit-record churn does not amplify data compaction.

### 5.6 Compaction

Compaction is leveled with bottom-up level sizing and executed in the background by the lease holder.

#### Bottom-Up Level Sizing

Level 0 holds flushed tails, ordered newest-first. L0 segment key ranges may overlap. Data segments and commit-record segments (`0xFF` prefix) are flushed to separate L0 segments and compacted independently.

Levels 1–6 each hold a single sorted run of ~2 MiB segments with strictly disjoint key ranges. Target byte sizes are computed from Level 6 upward to ensure the deepest level always holds ~85–90% of the database:

- `target[6] = max(level_bytes(6), 1MiB)`
- For `level` from 5 down to 1: `target[level] = target[level + 1] / 8`
- The shallowest level whose target is at least (1 MiB) is the base level. Levels `1..base` have target `0` and remain empty.

For example, a new or < 8 MiB database compacts L0 directly into L6 (`base = 6`); a 10 MiB database uses L0 → L5 (`base = 5`, target 1.25 MiB) → L6 (10 MiB).

#### Compaction Priority and Picking

Whenever a commit or compaction finishes, the picker evaluates candidate work in priority order:

1. Drop fully deleted segments with `Drop`. Any segment whose key range `[min_key, max_key]` lies inside a range tombstone and whose `max_seq` is below the tombstone `seq` is removed from the manifest immediately without reading the segment from object storage.

2. Drop expired commit-record segments with `DropExpired`. Any commit-record segment whose `max_seq` is older than 10 min is removed from the manifest.

3. Purge partial range deletes with `Merge` or `Rewrite`. Each segment records `deletes_applied`, the manifest version through which range tombstones were already applied when the segment was written. If a newer range tombstone overlaps a segment (`deletes_applied < tombstone.seq` and `min_seq < tombstone.seq`):

   - Merge affected L0 segments into `base`.
   - Rewrite deeper L1-L6 segments in place on their current level, up to 8 segments at a time, with deleted entries stripped.

4. Reclaim expired TTL entries with `Expire`. Any L1+ segment where at least half of entries carry a TTL and all are expired is rewritten in place on its level, converting expired values into tombstones or purging them.

5. Execute a size-driven leveled compaction with `Merge` or `Move`. Each level is scored:
   - Level 0's score is `l0_count / 4`.
   - Level `l` in `1..6` has score `level_bytes(l) / target[l]` if `l ≥ base` and is non-empty.
   - The level with the highest score `>= 1.0` is selected
   - Merge the oldest L0 segments (up to 12) with all overlapping segments in `base`.
   - Merge level `l` into Level `l + 1`. Pick the segment `s` in Level `l` that minimizes write amplification. If `s` overlaps no segment in Level `l + 1`, it is moved to Level `l + 1` via a metadata-only manifest update (`Move`) without rewriting the object. so sequential append workloads write each segment at most twice (once to L0, once to `base`).

#### Streaming

Compaction never loads entire segments into memory. Instead, it streams through each input segment, reading cached blocks and fetching uncached blocks in 1 MiB ranged `GET`s without populating the cache (§5.3), and writes out new 2 MiB segments as they are filled. Memory usage remains bounded to a 1 MiB read buffer per input run and a 2 MiB output buffer, regardless of database size.

#### Deleting Tombstones

Deleting a key writes a tombstone. The tombstone must remain for two reasons:

1. To shadow older values of the same key that still live in deeper levels of the LSM tree.
2. To detect conflicts with concurrent transactions.

Compaction can permanently drop a tombstone only once both reasons no longer apply:

- No older value for the key exists below the tombstone
- No active transaction started before the delete

To guard against a stalled transaction trying to commit after 5 minutes, the manifest records `tombstone_horizon`, the highest `seq` of any tombstone that has ever been dropped. Any transaction with `read_version < tombstone_horizon` is rejected at commit time with `409 conflict`.

#### Running Compaction Without Blocking Commits

Compaction runs in the background without blocking transactions. While a compaction is running, commits continue appending to `tail` and flushing new Level 0 segments.

Because new flushes only add newer L0 segments and never modify the existing segments being compacted, a finished compaction can swap its input segments for its new output segments in the manifest. To prevent two compactions from colliding, the manifest tracks a `compaction_seq` counter that increments on every compaction install. If `compaction_seq` changed while a compaction was running, its output segments are discarded and cleaned up later as orphans (§5.8).

#### Amplification Benchmarks

Baserunna uses leveled compaction to minimize amplification. Definitions:

- **Write amplification:** segment bytes written per logical byte
- **Space amplification:** stored bytes per live byte (settled, and worst)
- **Read amplification:** segments consulted per point lookup

Object storage providers charge for inserts by number of `PUT` requests and for storage by month. Writing additional segments during compaction costs little and cuts both space amplification and point-lookup read amplification. With zstd compression enabled, stored bytes drop to ~1/5th of uncompressed size. Measured amplification statistics:

| workload                | write | space settled | space worst | read |
| ----------------------- | ----- | ------------- | ----------- | ---- |
| uniform updates         | 7.3   | 1.08          | 1.23        | 2.0  |
| skewed updates          | 6.2   | 1.11          | 1.22        | 4.0  |
| appends                 | 2.1   | 1.03          | 1.07        | 1.0  |
| queue (insert + delete) | 7.1   | 1.37          | 1.53        | 1.0  |
| mixed                   | 5.5   | 1.18          | 1.83        | 1.9  |

### 5.7 Leases

A lease designates a single node as the active writer and strong-read cache holder for a database. The lease is stored in the manifest. The manifest's `expires_at` field is used to identify distinct renewals and as a routing hint.

Each node tracks lease timing on its own monotonic clock, with a 0.5 second margin to absorb clock drift and scheduling delays. The lease holder always stops relying on its lease before any other node considers the lease expired:

- The lease holder considers its lease valid until `duration - 0.5 s` after it started sending the manifest `PUT` that renewed the lease.
- Every other node considers that lease valid until `duration + 0.5 s` after it first observed that renewal. Observed lease timestamps are retained even if an idle database handle is evicted from memory.
- A node that loads a manifest from object storage also treats its lease as expired if the store's clock (§5.8), minus clock error, exceeds `last_modified + precision + 2 × duration + 0.5 s`. Because the holder started its lease timer before the store recorded `last_modified`, stale leases left behind by crashed nodes can be taken over immediately without waiting for a local timer.
- A manifest write can shorten the holder's lease (for example, a commit after a longer read renewal) or release it. Because other nodes can observe the write as soon as it is sent, the holder immediately bounds its lease to the duration granted by the sent write regardless of whether the write succeeds.

Each commit renews the lease for 5 seconds. On a read-heavy database where at least 16 strong reads occured within the current or previous lease window, the holder issues a lease-only manifest CAS about 1 s before expiry to renew the lease for 15 seconds. At ~$0.005 per 1,000 `PUT` versus ~$0.0004 per 1,000 `GET`s, a read-lease renewal pays for itself if it saves 12 conditional `GET` requests.

Because other nodes refuse to commit while a lease is valid, the holder's cached `Version` is guaranteed to be current, allowing it to serve strong reads directly from memory.

If a lease holder crashes, database writes pause until `duration + 0.5 s` after another node first observes the dead holder's last renewal, or until the store clock shows the lease has expired, whichever comes first. If another node checks the manifest just before the lease would have expired, failover takes at most `2 * duration + 0.5 s`: up to 11 s after a commit renewal or 31 s after a read renewal. Strong reads remain available on all nodes during failover via conditional `GET` validation (§5.9).

### 5.8 Garbage Collection

Garbage collection deletes replaced segments, unreferenced orphan objects, and dropped databases from object storage.

#### Store Time

Garbage collection compares timestamps written by different nodes against object-store `last_modified` times, so all GC timestamps use the object store's clock rather than node wall clocks.

Every 10 minutes, each node overwrites a shared `clock` object and reads back its `last_modified` timestamp. Because that timestamp was assigned between the request start and response receipt on the node's monotonic clock, the midpoint of that interval gives the node's clock offset and the half-width gives the error bound. Probes with an error bound above 10 s are retried. A measurement is trusted for up to 1 hour. If a node does not have a valid measurement, it skips garbage collection and rejects writes that require store time.

#### Obsolete Segments

If compaction replaces segments or a `.levels` object, it adds their IDs to `manifest.obsolete` with the current store time. During subsequent commits or compactions, the lease holder deletes any object in `obsolete` that has been listed for at least 30 min and removes it from the manifest. Transactions time out after 5 min, so only a read whose store request stalls past the grace period can encounter a deleted segment: if the current manifest does not list the segment, the read fails with a retryable `503`; if the manifest still lists it, the segment is missing and the read fails with `500`. Similarly, if a manifest read encounters a deleted `.levels` object, the node reloads the manifest and returns `500` only if the latest manifest still references the missing object.

#### Orphans

Crashed flushes, aborted compactions, abandoned blob uploads, and overwritten rows leave unreferenced objects in storage. At most once per hour on active databases, the lease holder runs a background sweep:

1. Record the current store time and snapshot the current `Version`.
2. Stream through all LSM entries in that `Version` to collect every referenced `blob_id` and stream `chunk_id`, and list `seg/` and `blob/` in object storage.
3. Move any unreferenced segment or `.levels` object whose `last_modified` is older than 2 hours into `manifest.obsolete`.
4. Move an unreferenced blob into `manifest.obsolete` if its `last_modified` is older than 2 hours and the creation time in its `blob_id` is older than 1 hour plus twice the clock error bound. Because commits refuse to bind staged blobs older than 1 hour or blobs listed in `obsolete`, and the sweep recorded the time before reading the `Version`, no concurrent or future commit can bind a blob that the sweep marks obsolete.

Because compaction output segments are unreferenced until the compaction is installed, a sweep can mark the outputs of a compaction that runs longer than the orphan grace period as obsolete. The compaction rejects the installation if its output segments have been marked obsolete.

A `copy` operation (§6.3) shares the source key's blob without restaging it. Because the committer validates that the source key is unchanged at commit time under all isolation levels, the blob remains referenced in every manifest version between the read and the commit and cannot be swept as an orphan.

#### Event Parts

Replaced event parts and deletion masks are added to `manifest.obsolete` like LSM segments. During the orphan sweep, if the database has ever contained an event table (`FLAG_EVENT_TABLES`), the lease holder collects all referenced part and mask IDs from the LSM tree, lists `part/` in object storage, and moves unreferenced IDs whose `last_modified` exceeds the orphan grace period and whose embedded creation timestamp exceeds the stage TTL into `obsolete`. Deleting an obsolete part lists and deletes all chunk objects under `dbs/{db_id}/part/{part_id}/`.

#### Long Blob Reads

Streaming a large blob by ID can take longer than the 30 min obsolete grace period. A blob read that has run for 10 min writes a `dbs/{db_id}/blob/{blob_id}/pin` object and refreshes it every 10 min. If a blob is already listed in `obsolete` when a read by ID begins, the reader writes the pin before reading any chunks.

Before deleting an obsolete blob, GC checks its `pin` object. If the pin was updated within the last 20 min (plus twice the clock error bound), GC leaves the blob in storage as an orphan for a later sweep. If a long read cannot write its pin because object storage is unreachable, it fails with a retryable `503`.

#### Deleted Databases

Deleting a database marks its record as deleted in the system database. After a 10 min grace period (exceeding node metadata cache TTLs), the live cluster member selected by rendezvous hashing lists and deletes all objects under `dbs/{db_id}/`. It then waits another 10 min and runs a second pass to catch any objects written by lagging requests before removing the database record.

### 5.9 Read Consistency

There are two read consistency modes:

- `snapshot` consistency reads from a serializable snapshot that is at most 5 seconds old.
- `strong` consistency guarantees linearizability. It is served from cache if the serving node holds a valid lease and has no manifest write in flight; otherwise it requests `GET manifest If-None-Match: etag` first.

### 5.10 Size Quotas

Each user database is capped at 10 GiB by default (`BASERUNNA_DB_MAX_MB`; the system database is exempt). A database's accounted size is the sum of its `tail` bytes, LSM segment bytes (excluding commit-record segments), referenced blob bytes, sealed stream chunk bytes (§5.11), and event part bytes (`part_bytes` in the manifest, updated exactly on every commit because each part has a single reference).

Overwriting or deleting a key in the LSM tree does not immediately show whether a blob or stream chunk is still referenced elsewhere. To track their size without scanning the database on every commit, the manifest maintains three counters for `blob_bytes` and `chunk_bytes`:

- `bound`: cumulative bytes ever bound by commits or sealed into chunks
- `swept`: live referenced bytes counted by the most recent GC sweep (§5.8)
- `swept_at`: the value of `bound` in the manifest version that the sweep scanned

Accounted bytes are `swept + (bound - swept_at)`, which is exact after a sweep and an upper bound until the next sweep.

Quota limits are enforced on commits and uploads:

- Any transaction that writes data is rejected with `507 quota_exceeded` if its keys, values, bound blobs, and stream appends would push the database size past the quota. Transactions that only delete keys or trim streams are always allowed so a full database can be emptied.
- A node serving a blob upload counts in-flight uploads and staged blobs not yet bound to a key, rejecting the upload with `507` if they would push the database past the quota.
- Deleted LSM data counts toward the quota until compaction removes it, and overwritten blobs count until the next GC sweep. When a commit is rejected for exceeding the quota, the lease holder runs the next sweep after 1 min instead of waiting for the hourly schedule.

`GET /v1/dbs/{db}` reports `size_bytes` and `quota_bytes`. Flushes and compactions can briefly push `size_bytes` slightly above `quota_bytes` due to segment framing overhead not counted at commit time.

### 5.11 Streams

Stream records start in the LSM tree and move to dedicated 2 MiB log objects ("sealed chunks") once a stream accumulates enough consecutive data.

#### Storage Layout

Each appended record is written to `tail` under key `0x06 ++ stream_id ++ seq`. Its value stores the record's timestamp, cumulative byte position in the stream, headers, and body. In the same manifest write, the committer updates the stream's state entry (`0x07 ++ stream_id`), which tracks the stream's first and next `seq`, byte positions, timestamps, and retention settings. Appending to a stream requires no separate object-store `PUT` beyond the batch's manifest write.

Low-volume streams stay in the LSM tree. Thousands of quiet streams share a single segment `PUT` on flush, and compaction groups their records together by `stream_id`.

When a flush or compaction encounters at least 64 KiB of consecutive records for a single stream, it writes those records to one or more dedicated segment objects called sealed chunks. Each sealed chunk is up to 2 MiB and omits the bloom filter (§5.3). In the LSM tree, the run of records is replaced by a single chunk reference keyed by the run's last `seq` (`0x06 ++ stream_id ++ last_seq`), storing the chunk's segment ID, byte size, first `seq`, byte range, and timestamp range.

Sealed chunks are never rewritten by future compactions. When a busy stream fills `tail` on its own, a flush seals its records directly from `tail` into a chunk object. If the remaining unsealed `tail` entries fit in half the tail limit, they stay in `tail` and the flush costs a single `PUT`.

#### Reads, Trims, and Retention

A stream read scans the `0x06 ++ stream_id ++ ...` key range in the LSM tree. Inline record entries are returned directly, while chunk references expand into the records of the referenced chunk segment. Because each chunk reference is keyed by its last `seq`, a forward scan starting at `target_seq` lands directly on the chunk reference covering `target_seq` or the first inline record at or after it. Seeking by timestamp or byte offset binary-searches the stream's `seq` range, using each chunk reference's timestamp and byte range metadata to avoid reading chunks that do not contain the target.

Trimming a stream advances `first_seq` in the stream's state entry and writes a range tombstone over `[0x06 ++ stream_id ++ 0, 0x06 ++ stream_id ++ first_seq)`, replacing the stream's previous trim tombstone. Concurrent transactions that read trimmed records fail conflict validation against this range tombstone (§6.1), and compaction drops the covered inline records and chunk references (§5.6). A chunk that straddles `first_seq` is kept until all of its records are trimmed, with reads skipping records below `first_seq`.

Retention limits (`retention_ms` and `max_bytes`) are enforced lazily with a 1/16th margin so appends at the limit do not trigger a second commit on every write. After a commit (or during a sweep for idle streams), if a stream exceeds `retention_ms` or `max_bytes` by at least 1/16th, a background task binary-searches for the exact cutoff `seq` and commits a trim.

Sealed chunks are stored under `dbs/{db_id}/seg/{seg_id}` and garbage-collected like blobs: sweeps collect live chunk references from the LSM tree, recount `chunk_bytes.swept` (§5.10), and mark unreferenced chunk objects as orphans (§5.8).

#### Write Amplification

Simulated benchmarks (`sim::amp`) measure segment and chunk bytes written per record body byte across 10 MB of uncompressed 200-byte records, comparing chunk sealing against keeping all records in the LSM tree:

| workload                                  | write, sealed → LSM only | records sealed |
| ----------------------------------------- | ------------------------ | -------------- |
| 1 log, 50 records per commit              | 1.18 → 3.94              | 100%           |
| 20 feeds of 500 KB, appended round-robin  | 2.90 → 7.30              | 88%            |
| 200 conversations of 50 KB, 25 per commit | 7.11 → 7.11              | 1%             |

### 5.12 Memory

Each node enforces a memory budget (`BASERUNNA_DB_MEMORY_MB`, default 256 MiB) across all open user databases and active transactions. Database handles, decoded manifests (including tail entries and segment metadata), and transaction snapshots, read sets, and write sets are charged to this budget. The system database is exempt.

If memory usage exceeds the budget, the node unloads the least recently used idle databases until usage drops to 7/8 of the budget. Unloading drops the cached manifest while retaining the database handle, so active long-poll and SSE watchers do not prevent eviction, and the next request reloads the manifest with a single `GET`. A database is idle if it has 0 in-flight manifest reads, manifest writes, commits, or background tasks. Local lease state is preserved across unloads so that a lease holder resumes serving strong reads from memory after reloading the manifest.

If memory usage remains above the budget after all idle databases are unloaded, the node allocates the remaining budget across users by max-min fair sharing for interactive transactions:

- A user's transaction memory usage is the sum of its transactions' read sets, write sets, and an equal share of each non-current manifest version pinned by those transactions.
- A user's fair share is the maximum per-user allocation that fits in the remaining budget if each user below that allocation retains its current usage.
- If a user is at or above its fair share, the node rejects transaction `begin` and `ops` requests with a retryable `503`. The node rechecks the user's share each time a transaction grows by 1/1024 of the budget.
- If a user is below its fair share, the node admits the request and aborts the least recently used idle transactions of the users with the highest usage until total usage is within the budget. A transaction is aborted only if doing so moves its user's usage closer to the fair share.

Single-request transactions, interactive transaction commits, and interactive transaction aborts are always admitted so that in-flight work can finish and release memory. Each user is limited to 100 concurrent open interactive transactions per node.

### 5.13 Event storage

Event tables combine transactional LSM writes for low-volume inserts with immutable columnar objects ("parts") for bulk ingestion and analytical scans. Queries read a consistent snapshot across both unsealed LSM rows and columnar parts, while idle event tables consume zero compute.

#### Unsealed rows and sealing

Transactionally inserted events are written to the LSM tree under `0x09 ++ table_id ++ version ++ ordinal`, assigning each event a unique `(version, ordinal)` position that is preserved across all subsequent seals and merges. The committer maintains per-table counters of unsealed rows, unsealed bytes, and referenced part bytes in the table state entry (`0x0B ++ table_id`). Once a table accumulates at least 128 KiB of unsealed rows, the lease holder seals them in the background by writing columnar parts per partition, replacing the sealed LSM rows with a range tombstone, and inserting part references (`0x0A`). Sealing conflicts only with concurrent deletes covering the sealed rows. If a column with a default value was added after an unsealed row was written, the part builder materializes the default directly into the column vector without copying it into each unsealed row, and counts the expanded default size toward the 64 MiB per-part build limit.

#### Bulk inserts

Bulk insert requests bypass the LSM tree by coalescing events in memory per node and table (up to 200 ms or 16 MiB), writing staged columnar parts directly to object storage, and sending a single `/internal/v1/events/bind` request to the database lease holder. The committer installs all staged parts from the bind batch in a single manifest write and records per-request commit records. If an individual request in the batch is rejected (for example, due to an aborted idempotency key or an expired deadline), the committer masks that request's row ranges in the shared part references so the remaining requests in the batch still succeed. Table-wide errors (such as a dropped table, exceeded quota, or expired stage TTL) reject the entire bind batch.

#### Parts

Each part stores a single partition's events sorted by `(order_by, position)` across 32 MiB chunk objects (`dbs/{db_id}/part/{part_id}/{chunk}`). Within a part, rows are divided into granules of up to 8,192 rows or 10 MiB of flattened data, and each column is stored as a framed block (§5.3) per granule using type-specific encodings (delta or constant encoding for integers, dictionary or plain encoding for strings with up to 65,536 entries / 1 MiB per part dictionary, and bitmaps for nulls). Each column has an index storing block byte ranges, per-granule min/max/null statistics, and optional bloom filters covering groups of 4 granules. The part footer (`PartMeta`) stores granule row offsets, the primary index (the sort key of each granule's first row, omitting keys larger than 4 KiB), and the column directory. JSON columns are decomposed into subcolumns by leaf path: up to 1,024 frequent paths (with keys up to 256 bytes) are stored as dedicated typed columns, while remaining paths are hashed into 32 shared JSON buckets and listed in a path directory capped at 1 MiB. Each part also records per-row byte sizes so subsequent merges preserve granule size bounds.

A part reference (`0x0A ++ table_id ++ partition ++ part_id`) stores the part ID, row count, timestamp range, byte size, compaction level, min/max event positions, deletion mask (stored inline up to 4 KiB or in a separate mask object), and the timestamp of the oldest unpurged deletion.

#### Queries

Query execution prunes part references by time range, fetches part metadata and filtered column indexes, eliminates parts and granules using dictionary checks, min/max statistics, primary keys, and bloom filters, and evaluates filter columns (prewhere) before reading projected columns for matching granules.

Memory usage per query is bounded regardless of the number of columns, JSON paths, or rows scanned. The executor reads granules in runs of up to 16 whose decoded columns fit within 25% of the query memory budget, charging active column vectors alongside aggregate state. Derived values (shared JSON bucket paths, reconstructed JSON objects, dimension lookups, unsealed row values, and retroactive column defaults) are evaluated in bounded row windows that shrink proportionally with the number of expressions in the query.

Up to 32 parts are read concurrently into partial results, each capped at 1/32 of the query state budget, and merged in deterministic part order so memory accounting is independent of completion order. If a part's partial state and active columns exceed its 1/32 share, that part is re-evaluated serially against the main accumulator. Stalled part `GET` requests are hedged after 1 s. Small parts are cached as a single entry in the memory block cache, while larger parts are cached by individual metadata, index, and column blocks.

Paginated row queries ordered by the time column read only the parts needed to fill the requested page. The pagination cursor narrows the scan time range and skips parts whose `(time, position)` bounds precede the cursor. Parts are scanned in query order and execution stops once all unread parts follow the page's last row and at least 1 additional matching row is known to exist for `next_cursor`.

#### Merges and retention

After seals, binds, and GC sweeps, the lease holder runs background maintenance in priority order: dropping expired partitions past `retention_ms`, rewriting parts with a high fraction of deleted rows, merging same-tier parts within a partition (size-tiered fan-in of 8, up to 4 GiB per output part), purging parts whose oldest deletion exceeds `event_purge_ms` (24 hours), and compacting quiet completed partitions that have not received new parts for 24 hours into a single part. A merge streams input columns sequentially, writes a new part object, and swaps the part references in a transaction that conflicts only with concurrent deletes on those parts. For inputs with deletion masks, the merge scans live rows first to build output dictionaries and JSON path directories so deleted values do not remain in part metadata.

#### Drops

Dropping an event table writes range tombstones over the table's unsealed rows and part references, deletes the table state entry, and decrements the database's `part_bytes` counter by the table's recorded part bytes. Because the drop transaction does not read individual part references, its write set size is constant and it does not conflict with concurrent binds, seals, or merges; any maintenance or bind operation that is committed after the drop fails validation against the deleted table state or range tombstone.

## 6. Transactions

### 6.1 Semantics

- Default isolation is serializable, strictly serializable for committed read-write transactions.
  It uses optimistic concurrency control with backward validation:
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
  are validated anyway: a row write is committed only against the table schema
  it was derived from, so a concurrent schema change (a new index, a
  dropped column or table) makes it fail with a retryable `409 conflict`
  rather than miss index maintenance.
- Read-only transactions always succeed and are serialized at `read_version`.
- Blind-write transactions never conflict, appends included.
- Reads within a transaction see its own earlier writes, except stream appends and
  event inserts: because event positions are assigned at commit time, a
  transaction that inserts into an event table cannot query or delete from that
  table in the same transaction (`400`).
- Limits: `txn_max_ops` 10,000; `txn_max_write_bytes` 16 MiB, where blobs
  count only their descriptor; `txn_max_read_bytes` 16 MiB of keys and key
  ranges in the read set; 100,000 appended records; `txn_idle_timeout`
  30 s; `txn_max_lifetime` 5 min; 100 open interactive transactions per user
  on each node (§5.12).

### 6.2 Forms

1. **One-shot** (`POST /v1/dbs/{db}/txn`): a list of ops executed atomically
   in one request. Supports `check` precondition ops and an optional
   `idempotency_key`. Retrying with the same key never applies twice within
   `commit_retention_ms` (10 minutes) of the first attempt, nor when two
   attempts reach the committer together. A retry that finds the earlier
   attempt applied returns `replayed: true` with `results: null`, and the
   seqs its appends received (`appends`): the commit record keeps the
   commit's time and the first seq and count of each append.
   If an idempotent write request fails a precondition, the server resolves the
   commit (§5.5) before returning 412: if a prior attempt was already
   committed, the server returns the replayed result; otherwise it writes an
   abort record so that any in-flight attempt cannot be applied later.
   Read-only requests ignore idempotency keys. Conflicts are retried
   server-side.
2. **Interactive** (`/v1/dbs/{db}/transactions…`): begin, ops, commit,
   abort. `begin` and `commit` also accept `ops`, so a read-write transaction
   can be executed in two requests. If an `ops` request fails or is canceled,
   the node aborts the transaction because partial operations may have been
   applied; retryable failures return `404 txn_not_found` so that the client
   restarts the transaction. Because a serializable transaction can begin
   while a commit is in flight (§5.5), a read-only transaction must be
   committed to validate its read set. Transaction state is stored on the
   database's owner node. If the owner changes or crashes, or if the node
   aborts the transaction for timeout or memory pressure (§5.12), uncommitted
   writes are discarded and subsequent requests return a retryable
   `404 txn_not_found` with the abort reason. `begin` returns the transaction's
   `commit_id` up front, and `commit` returns the sequence numbers assigned to
   its appends.

**Resolving ambiguity.** A `503` response with `outcome: "unknown"`, or a lost
response, indicates that a commit may or may not have been applied. To resolve
the outcome, the client calls `GET /commits/{commit_id}` (or
`key:<idempotency key>`), which requires write access. If the commit record
exists, the server returns `committed` (with its version and `appends`).
Otherwise, the server writes an abort record (§5.5) and returns `not_committed`
(with `retention_ms`); any subsequent attempt with the same commit ID fails with
`409 aborted`, so retries must use a new idempotency key or transaction. Commit
IDs are 16 hex characters. A `not_committed` result is definitive only if the
first attempt began less than `retention_ms` ago, after which the commit record
may have expired.

### 6.3 Ops

Key operations:

- `get`: key → `{found, value? | blob?, version?, expires_at?}`
- `scan`: `prefix` or `start`/`end`, `limit` (≤1000), `reverse`, `keys_only` →
  `{items, next_cursor?}`
- `put`: key, `value` or `blob_id`, `ttl_ms?` → `{}`
- `delete`: key → `{}`
- `copy`: `from`, `to`, `ttl_ms?` → `{}`; 412 if `from` does not exist
- `delete_range`: `prefix` or `start`/`end` → `{}` (expanded at commit)
- `check`: key and one of `exists`, `version`, `value` → fails the
  transaction with 412 if false
- `increment`: key, `by` (i64), `ttl_ms?` → `{value}`; the value is an ASCII
  decimal integer, and a missing key is treated as 0

Row operations:

- `get_row`: table, `pk` (array or scalar) → `{found, row?, expires_at?}`
- `insert_row`: table, `row`, `ttl_ms?` → `{}`; 409 if the pk exists
- `upsert_row`: table, `row`, `ttl_ms?` → `{}`
- `update_row`: table, `pk`, `set`, `ttl_ms?` → `{found}` (partial update)
- `delete_row`: table, `pk` → `{found}`
- `query_rows`: table, `index?`, `eq?`, `range?`, `filter?`, `limit`,
  `reverse`, `cursor` → `{rows, next_cursor?}`
- `update_rows`: table, `where` (`index?`, `eq?`, `range?`, `filter?`), `set`,
  `ttl_ms?`, `limit?` → `{count}`
- `delete_rows`: table, `where`, `limit?` → `{count}`

Stream operations:

- `append`: stream, `records` (`value` or `json`, `headers?`), `expect_seq?`,
  `create?` → `{count, first_seq?}` (`first_seq` once committed)
- `read_stream`: stream, `from_seq`, `from_time`, `tail` or `consumer`,
  `until_seq?`, `until_time?`, `reverse`, `limit`, `max_bytes?`, `filter?`,
  `scan_limit?` → `{records, next_seq, done}`
- `stream_info`: stream → `{first_seq, next_seq, bytes, first_time?, last_time?}`
- `trim_stream`: stream, `before_seq` → `{}`
- `get_consumer`: stream, consumer → `{found, seq?}`
- `set_consumer`: stream, consumer, `seq` → `{}`

Event operations:

- `insert_events`: table, `rows` → `{count}`
- `query_events`: table, `from`, `to`, `where?`, `columns?` | (`group_by?`, `aggregates?`, `then?`), `lookups?`, `order_by?`, `limit?`, `cursor?` → `{columns, rows, next_cursor?, stats}`
- `delete_events`: table, `from?`, `to?`, `where` → `{deleted}`

`ttl_ms` (1 ms to 10 years) adds a TTL expiration relative to write
commit time (§4.5); `update_row`, `update_rows` and `increment` do not
update an existing TTL by default.

`copy` writes the value of `from` to `to` without duplicating blob data. If
`from` references a blob, `to` references the same blob without restaging
(§4.3), and the source read is validated at commit time under all isolation
levels so the blob cannot be garbage-collected before the commit (§5.8). `to`
does not inherit the TTL of `from`; it expires only if `ttl_ms` is specified.

A record body is a `value` string or `json` value. Reads return each record
as `{seq, time, headers?, value | json}`. A read's `next_seq` is where the
next page starts: the next `from_seq` of a forward read, the next `until_seq`
of a reverse one. `done` says the read reached its end seq or time, or the end
of the stream.

JSON keys and values are encoded as UTF-8 strings by default. With `"encoding": "base64"`
on the request, every key and value in the request and response is standard base64 instead.

### 6.4 Change feeds

Change feeds stream modified keys (`GET /v1/dbs/{db}/changes?after=…`, with an optional `prefix` or `start`/`end` range) or table rows (`GET /v1/dbs/{db}/tables/{t}/changes`) so that applying pages in order reconstructs the current state. For each key modified after the `after` cursor, a page reports the key's latest state in the page snapshot: its current value (with `expires_at` if set), `deleted` (including expired keys), or a `deleted_range` entry for a large range deletion. Intermediate values are omitted. Changes are ordered by version, with range deletions before key entries at the same version. Each page returns up to `limit` changes and a `next_cursor` to pass as the next `after` parameter; if `more` is true, additional changes are immediately available.

If a poll finds 0 changes and `wait_ms` (up to 50 s) is set, the request waits until a newer version is installed on the node or the timeout expires. A node installs a newer version either by committing locally or by receiving a commit notice from another node. After committing a new data version, a node sends a best-effort `/internal/v1/notice` request with the latest version to the database's rendezvous home node (§7), coalescing concurrent notices so at most 1 request is in flight per database. If the home node has active waiters, it reloads the manifest and wakes them. While waiting without a wakeup, a `strong` read re-checks the manifest every `watch_poll_ms` (1 s), whereas a `snapshot` read (`consistency=snapshot&max_staleness_ms=…`) re-checks the manifest only after its cached snapshot exceeds `max_staleness_ms`.

To compute a page (`Version::changes`), the engine merges only the `tail` and segments with `max_seq` at or above the cursor version. Because compaction retains tombstones for at least `txn_max_lifetime` (5 min, §5.6), a cursor remains valid for at least `txn_max_lifetime` after the snapshot of its oldest page. If tombstones needed by the cursor have been purged, the request fails with `410 cursor_expired` and the client must resynchronize. A client can resynchronize by starting from `after=0` to perform a full state transfer of all live keys. Because a reader starting from version 0 holds only keys returned by earlier pages of that transfer, it only requires tombstones newer than the oldest snapshot version read during the transfer; the cursor records that version as a floor so that purging older tombstones does not expire an in-progress transfer.

## 7. Cluster

- **Cluster time.** Timestamps that one node writes and another compares
  (heartbeats, session expiry, deadlines, deletion times) use the store's
  clock as each node measured it (§5.8), so node clock skew does not affect
  them; a node uses its own clock only until its first measurement.
- **Membership** is stored in the `nodes` table of the system DB. Each node
  rewrites `{id, addr, started_at, heartbeat_at}` every `heartbeat_ms`
  (120 s) and re-reads the table every `members_refresh_ms` (30 s). A node is
  live if `heartbeat_at > now − 3 × heartbeat_ms` and a newer process has not
  registered at its address. Each node includes itself in the live set unless it is draining. Crashes are detected by
  requests rather than heartbeats: a forward that certainly never arrived
  makes its target a suspect, which stays out of routing until the table
  shows a newer heartbeat from it, or for a backoff that starts at 10 s and
  doubles with each further failure, up to the liveness window. A node that
  shuts down gracefully deletes its row. Rows dead for more than 1 h are
  removed. An idle node costs about 30 PUTs and 120–140 validating GETs an
  hour (about $0.15 a month).
- **Routing**: for a request on DB `d`, the target is the lease holder if a
  valid lease is known, otherwise the live node maximizing
  `hash(node_id ‖ d)` (rendezvous hashing). Long-poll requests (`GET` with
  `wait_ms > 0`) are always routed to the rendezvous home node, where commit
  notices are sent (§6.4), so that waiting requests are notified even if
  another node acquires the lease. Event bulk inserts and queries are
  served by the node they reach (§5.13). If the lease holder is not in the
  cached member table, the node refreshes the member table with a strong read
  at most once per second. A member entry with the node's own
  address belongs to a previous process and is ignored during routing. If the target is another node, the
  request is proxied, streaming large bodies, with `x-baserunna-hops`
  incremented; a request already forwarded twice is handled locally. If a
  forward fails before leaving the node, or if the target rejects it with
  `x-baserunna-stale-target` because the target restarted with a new node ID,
  the entry node marks the target as suspect and handles buffered requests
  locally (or returns a retryable `503` for streamed bodies that were already
  consumed). A fallback node can always serve reads, and can commit writes if
  another node does not hold a valid lease (otherwise it returns `503 unavailable`
  with `Retry-After`).
- **Forwarded requests include the cluster key** (`x-baserunna-cluster-key`), so the
  receiving node does not deduct tokens from its rate limiters a second time.
- **Forwarded requests execute at most once.** Each forward includes a random
  `x-baserunna-forward-id`; the receiver runs the first copy and responds to
  duplicates (for 1 minute) with the cached response. Because the deduplication cache is in memory,
  each forward also specifies its target node ID
  (`x-baserunna-forward-to`), and a restarted node rejects copies addressed to
  its previous node ID with a non-applied `503`. GET and HEAD requests are not
  deduplicated.
- **Request IDs propagate across forwards.** Every request is assigned an ID: the
  client's `x-request-id` if it is 1-64 characters of `[A-Za-z0-9._:-]`,
  or a generated ID. Forwards (including metadata commands sent to the
  system database's lease holder) include it in the same header, every
  response echoes it, and each node includes it in log entries for that request (§12).
- **Deadlines are propagated across forwards.** `x-baserunna-deadline-ms` specifies the
  originating node's request deadline (§10) in cluster time for data requests and
  forwarded metadata commands. The receiving node caps the deadline at its
  own `commit_timeout_ms` so that a delayed forward cannot be committed after
  the client's deadline.
- **Internal endpoints** (`/internal/v1/…`, used to run system-DB
  transactions on its owner and to bind event parts on a database's lease
  holder, §5.13) always require `x-baserunna-cluster-key`. Its
  value is the configured `BASERUNNA_CLUSTER_SECRET` or, by default, a key
  generated once per cluster and stored in the system database's `config`
  table. Every node reads it from the shared store, which is the trust
  boundary. Comparison is constant-time.
- **Bootstrap**: on start a node creates the `sys` manifest
  (`If-None-Match: *`, so it is idempotent) and runs the schema-migration
  transaction (it checks the catalog first, so it is idempotent). The node
  then registers its heartbeat.

## 8. Service metadata (system DB)

Tables in DB `sys`, all defined through the table layer (dogfooding):

| table               | pk                  | columns                                                                            | indexes                                       |
| ------------------- | ------------------- | ---------------------------------------------------------------------------------- | --------------------------------------------- |
| `users`             | `id`                | email, password_hash?, google_sub?, is_admin, disabled, email_verified, created_at | unique(email), unique(google_sub), (is_admin) |
| `sessions`          | `token_hash`        | id, user_id, created_at, expires_at, client?, method                               | unique(id), (user_id), (expires_at)           |
| `api_keys`          | `id`                | user_id, name, secret_hash, db_id?, read_only, rate_limit?, created_at             | (user_id)                                     |
| `orgs`              | `id`                | name, created_at                                                                   | unique(name)                                  |
| `org_members`       | `(org_id, user_id)` | role, added_at                                                                     | (user_id)                                     |
| `databases`         | `id`                | owner_id, name, created_at                                                         | unique(owner_id, name)                        |
| `counts`            | `(kind, key)`       | count                                                                              |                                               |
| `deleted_databases` | `id`                | owner_id, name, deleted_at, purge_passes                                           |                                               |
| `nodes`             | `id`                | addr, started_at, heartbeat_at                                                     |                                               |
| `config`            | `key`               | value                                                                              |                                               |

A database's `owner_id` is a user id or an organization id (`org-…`).
`config` stores generated secrets (`cookie_key`) and the bootstrap flags.

`counts` keeps the numbers that limits and invariants need in one row
each: databases per owner (`max_dbs_per_owner`), members and owners per
organization (`max_members_per_org`, and the rule that an organization
keeps an owner), and organizations per user (`max_orgs_per_user`). A
counter at zero is deleted. Commands that change what a counter counts
update it in the same transaction, so two concurrent changes conflict on
it and are retried server-side: two owners demoting each other at once
cannot both succeed. `meta::check_integrity` checks every counter against
the rows it counts, that every organization has an owner, and that every
database, membership, session and API key belongs to an existing user or
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
2 s, cached per node for at most 30 s from the start of the read. A revocation (of a session, a key,
an account, or a role) takes effect at once on the node that performed it
and within 32 s everywhere else. Only memberships are cached, never their
absence, so a member added is recognized at once everywhere. A snapshot
**miss** (unknown token, database name, or email) is retried with a strong
read before answering. User-facing listings and reads (databases,
organizations, members, sessions, API keys, users) always read strongly.
Together these ensure clients always see their own metadata writes, even
across nodes.

Name resolution guards against stale cache entries. Before running a request,
the node checks the target database's manifest using the request's consistency
mode (a `GET` or `HEAD` with `consistency=snapshot` uses a snapshot within
`max_staleness_ms`; all other requests check the latest manifest). If the
manifest is marked deleted, the node re-resolves the database name with a
strong metadata read. If the name now points to a recreated database, the
request proceeds against the new database; if metadata still points to the
deleted database, the node finishes the interrupted deletion. Creating a
database with the name of a partially deleted database also finishes the prior
deletion first. Because a missing manifest is created only after a strong
metadata read confirms the database exists, a stale cache entry cannot
recreate a purged database.

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
  links Google to the existing account only if that account does not have a
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
  answering the same whether or not the account exists. Links include a
  token signed with the cluster's `cookie_key`: its purpose, the user id
  and a fingerprint of the address (verification, 7 days) or of the
  current password hash (reset, 1 hour). A reset link therefore sets one
  password (repeating it with that password finishes the reset), verifies
  the address, and ends every session without storing a token. Mail
  per address is rate limited (6 an hour, burst 3).
- **API keys**: `brk_<id>_<secret>`. Only the secret's SHA-256 is stored. A
  key can be scoped to one DB and/or read-only. It is shown once at creation.
- **Rate limits** (`limits.rs`): token buckets, kept per node, so a cluster
  of N nodes admits up to N times each rate. Every authenticated request
  takes a token, on the node the client sent it to, from its user's bucket
  (500/s, burst twice that) and, for an API key, from the key's (500/s, or
  the `rate_limit` the key was created with); both or neither. Requests that commit or abort an interactive
  transaction can overdraw a bucket by up to its burst capacity, and subsequent
  requests must wait for the bucket to refill. Sign-ins, sign-ups and Google sign-in
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

## 10. HTTP API

The API is JSON over HTTP. All routes are under `/v1`. The full schema is in
`openapi.json` (OpenAPI 3.1, generated by `src/api/openapi.rs`), served at `GET /v1/openapi.json`. DB-scoped
routes take `{db}`, a reference: `name` (a database of the caller's),
`org.name` (an organization's), or `id:<db_id>` (administrators).
Responses include the `x-request-id` header (§7).

If a request JSON body contains an integer outside of the signed and unsigned 64-bit ranges, the request is rejected with `400`.

Errors are returned as
`{"error": {"code", "message", "retryable", "details"?}}`:

| status | code(s)                                                                                                                |
| ------ | ---------------------------------------------------------------------------------------------------------------------- |
| 400    | `invalid_argument`                                                                                                     |
| 401    | `unauthenticated`                                                                                                      |
| 403    | `permission_denied`                                                                                                    |
| 404    | `not_found`, `txn_not_found`                                                                                           |
| 409    | `conflict` (retryable), `already_exists`, `aborted` (§6.2)                                                             |
| 412    | `precondition_failed` (includes the failing op index; also an append whose `expect_seq` no longer holds)               |
| 410    | `cursor_expired` (a change feed cursor older than the deletions kept, §6.4)                                            |
| 413    | `too_large`                                                                                                            |
| 429    | `rate_limited` (retryable, `Retry-After`; not applied)                                                                 |
| 507    | `quota_exceeded` (the database is at its size quota, §5.10)                                                            |
| 503    | `unavailable` (retryable, `Retry-After`; may include `outcome: "unknown"`; also a query on an index still being built) |
| 500    | `internal`                                                                                                             |

The server cancels a request with a retryable `503 unavailable` error if the
request exceeds its `commit_timeout_ms` (10 s) deadline. The response includes
`outcome: "unknown"` unless the request method is `GET` or `HEAD` or it is an
event query. The deadline is paused while the server waits for the client to
send the request body, and is extended by `wait_ms` for long-poll requests and
by an event query's own 60 s time limit (§4.8).

Routes (see the OpenAPI document for bodies):

```
GET /healthz liveness (503 while shutting down)
GET /v1/openapi.json
GET /v1/limits request limits
POST /v1/auth/signup | /v1/auth/login | /v1/auth/logout
GET /v1/auth/google/start | /v1/auth/google/callback
POST /v1/auth/verify-email a mailed verification token
POST /v1/auth/password-reset | /v1/auth/password-reset/confirm
GET /v1/me DELETE /v1/me delete the account (password)
POST /v1/me/password change it; ends other sessions
POST /v1/me/verify-email mail a verification link
GET /v1/me/sessions DELETE /v1/me/sessions (all others) DELETE /v1/me/sessions/{id}
GET /v1/api-keys POST /v1/api-keys DELETE /v1/api-keys/{id}
GET /v1/orgs POST /v1/orgs
GET /v1/orgs/{org} DELETE /v1/orgs/{org}
GET /v1/orgs/{org}/members POST /v1/orgs/{org}/members (email, role)
POST /v1/orgs/{org}/members/{user} (role) DELETE /v1/orgs/{org}/members/{user}
GET /v1/dbs (?org=) POST /v1/dbs (name, org?)
GET /v1/dbs/{db} DELETE /v1/dbs/{db}
GET /v1/dbs/{db}/kv scan (query params)
GET /v1/dbs/{db}/changes change feed of keys (§6.4)
GET|PUT|DELETE /v1/dbs/{db}/kv/{*key} raw bytes; Range, If-Match, If-None-Match
POST /v1/dbs/{db}/blobs stage a blob (streamed body)
GET /v1/dbs/{db}/blobs/{blob_id} read a blob (Range)
POST /v1/dbs/{db}/txn one-shot transaction
POST /v1/dbs/{db}/transactions begin
POST /v1/dbs/{db}/transactions/{id}/ops
POST /v1/dbs/{db}/transactions/{id}/commit
DELETE /v1/dbs/{db}/transactions/{id} abort
GET /v1/dbs/{db}/commits/{commit_id} resolve an ambiguous commit
GET /v1/dbs/{db}/tables POST /v1/dbs/{db}/tables
GET /v1/dbs/{db}/tables/{t} DELETE /v1/dbs/{db}/tables/{t}
POST /v1/dbs/{db}/tables/{t}/columns DELETE /v1/dbs/{db}/tables/{t}/columns/{c}
POST /v1/dbs/{db}/tables/{t}/indexes DELETE /v1/dbs/{db}/tables/{t}/indexes/{i}
POST /v1/dbs/{db}/tables/{t}/rows insert (one or many; `upsert` flag)
POST /v1/dbs/{db}/tables/{t}/rows/get by pk
POST /v1/dbs/{db}/tables/{t}/rows/update by `pk`, or many by `where`
POST /v1/dbs/{db}/tables/{t}/rows/delete by `pk`, or many by `where`
POST /v1/dbs/{db}/tables/{t}/rows/query
GET /v1/dbs/{db}/tables/{t}/changes change feed of rows (§6.4)
GET /v1/dbs/{db}/streams POST /v1/dbs/{db}/streams (name, retention_ms?, max_bytes?)
GET /v1/dbs/{db}/streams/{s} PATCH /v1/dbs/{db}/streams/{s} (retention) DELETE /v1/dbs/{db}/streams/{s}
POST /v1/dbs/{db}/streams/{s}/records append: JSON, NDJSON or lines of text
GET /v1/dbs/{db}/streams/{s}/records read: JSON, NDJSON or server-sent events; long poll
POST /v1/dbs/{db}/streams/{s}/records/query filtered read
POST /v1/dbs/{db}/streams/{s}/trim before_seq
GET /v1/dbs/{db}/streams/{s}/consumers limit, cursor
GET|PUT|DELETE /v1/dbs/{db}/streams/{s}/consumers/{c}
GET /v1/dbs/{db}/events POST /v1/dbs/{db}/events
GET /v1/dbs/{db}/events/{t} PATCH /v1/dbs/{db}/events/{t} (retention) DELETE /v1/dbs/{db}/events/{t}
POST /v1/dbs/{db}/events/{t}/columns DELETE /v1/dbs/{db}/events/{t}/columns/{c}
POST /v1/dbs/{db}/events/{t}/skip-indexes DELETE /v1/dbs/{db}/events/{t}/skip-indexes/{c}
POST /v1/dbs/{db}/events/{t}/rows insert: JSON or NDJSON; Idempotency-Key
POST /v1/dbs/{db}/events/{t}/rows/query
POST /v1/dbs/{db}/events/{t}/rows/delete from, to, where
GET /v1/admin/users GET /v1/admin/nodes POST /v1/admin/users/{id}/disable
GET|DELETE /v1/admin/metrics read (and on DELETE, reset) node latency metrics
```

List endpoints accept `limit` and `cursor` query parameters and return `next_cursor` (§8). Table, event table, and stream listings are ordered by name and support an optional `prefix` filter.

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
  consumers), event tables (create, drop; partition and part inspection; a
  query runner defaulting to daily event counts over the latest 30-day window;
  event insertion), and a transaction runner (JSON);
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

## 12. Configuration (environment)

| var                                      | default                         | meaning                                                      |
| ---------------------------------------- | ------------------------------- | ------------------------------------------------------------ |
| `BASERUNNA_LISTEN`                       | `0.0.0.0:8080`                  | bind address                                                 |
| `BASERUNNA_ADVERTISE`                    | `http://127.0.0.1:<port>`       | address peers use to forward                                 |
| `BASERUNNA_NODE_ID`                      | random                          | stable id optional                                           |
| `BASERUNNA_STORE`                        | `fs:./data`                     | `fs:<dir>`, `mem:`, or `s3://bucket/prefix` (Tigris)         |
| `BASERUNNA_CLUSTER_SECRET`               | none (required if >1 node)      | internal endpoint auth                                       |
| `BASERUNNA_SIGNUP`                       | `first_user_only`               | `open` / `first_user_only` / `closed`                        |
| `BASERUNNA_ADMIN_EMAIL` / `_PASSWORD`    | none                            | bootstrap admin                                              |
| `BASERUNNA_GOOGLE_CLIENT_ID` / `_SECRET` | none                            | enables Google sign-in                                       |
| `BASERUNNA_PUBLIC_URL`                   | advertise                       | OAuth redirect base, cookie `Secure`                         |
| `BASERUNNA_CACHE_MB`                     | 512                             | block cache budget                                           |
| `BASERUNNA_DB_MEMORY_MB`                 | 256                             | memory for open databases and transactions (§5.12)           |
| `BASERUNNA_DISK_CACHE_DIR`               | none                            | directory of the local tier (§5.3); off if unset             |
| `BASERUNNA_DISK_CACHE_MB`                | 10240                           | local tier budget                                            |
| `BASERUNNA_LEASE_MS`                     | 5000                            | lease renewed by commits; leases renewed for reads last 3×   |
| `BASERUNNA_PUT_HEDGE_MS`                 | 250                             | manifest `PUT` hedge delay in ms (§5.5); 0 disables hedging  |
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

Logs are written to stderr as JSON lines
(`{"ts", "level", "node", "request_id", "msg", ...}`) from a background
thread; if stderr falls behind, excess lines are dropped with a warning. At
`info` level, each node logs every request it handles under its `request_id`
(§7) with `method`, `route` (the route template rather than the raw path,
which may contain user keys), `status`, `ms`, `hops`, `forwarded_to` (when
forwarded to another node), and latency breakdowns for object-store calls
(`store`, `store_ms`), commit waits (`commits`, `commit_ms`), and in-flight
manifest waits (`read_wait_ms`). Responses include these totals in the
`Server-Timing` header (`total`, `store`, `commit`, and `wait`, in ms). After a
commit batch writes a manifest, the node logs a `msg: "commit"` event with queue
wait (`queued_ms`), batch build time (`build_ms`), store calls (`store`), and
participating `request_ids`. The mail webhook receives `{"from", "to",
"subject", "text"}`.

`GET /v1/admin/metrics` returns latency histograms for object-store
operations, commit batches, HTTP routes, and tokio scheduler lag, along with
process CPU time and host steal time on Linux. `DELETE /v1/admin/metrics`
returns and resets the histograms.

On SIGTERM or SIGINT, a node drains (failing `/healthz`), stops accepting
connections, ends active `wait_ms` long polls immediately, waits for in-flight
requests to finish, leaves the membership table, and releases its database
leases without acquiring new ones. [`DEPLOYING.md`](DEPLOYING.md) covers TLS,
load balancers, the container image, and bucket setup.

`fs:` is for single-node development only. Compare-and-swap is serialized
within one process, so the store takes an exclusive lock on `<dir>/.lock`,
and a second process (or a second store in the same process) fails to open
the directory.

## 13. Deterministic simulation testing (DST)

The simulator executes a multi-node cluster on a single thread in virtual time, injects storage, network, and process faults, and verifies client-observed behavior and cluster invariants against reference models.

### 13.1 Determinism

- `Env` abstracts wall time, monotonic time, random number generation, and task spawning. Simulations run on a single-threaded Tokio runtime with paused time, advancing the clock only when all tasks are idle. All randomness is derived from `xoshiro256**` generators seeded by the run seed.
- Production code must not iterate over `std::collections::HashMap` with the default random hasher when iteration order affects behavior, and must not use `tokio::select!` or `tokio::sync::watch`. `tests/lint.rs` enforces these rules and forbids direct wall-clock time, randomness, or `tokio::spawn` outside `Env`.
- Each simulation run computes a trace hash over all requests, responses, and store operations. Executing the same seed twice produces an identical trace hash.
- A simulation run can be branched at specified virtual timestamps (`util::rng::branch`). At each branch point, a salt is mixed into the PRNG state so that the run reproduces its parent up to the branch point and explores a different execution path afterward (§13.4).

### 13.2 Simulated world

Each simulation parameter is drawn independently from a hash of the run seed and the parameter name (`sim::swarm`). Each scenario specifies bounds on node count, fault injection, and clock skew, and each fault category is disabled in a subset of runs (with 10% of runs disabling all faults).

- **Cluster and configuration**: 1–5 nodes; randomized engine and node parameters (memtable, block, and segment sizes, compaction triggers, cache budgets, commit batch limits, hedging delays, lease durations, range-delete thresholds, inline level limits, stream chunk sizes, heartbeat and refresh intervals, cache TTLs, and index backfill batch limits).
- **Object store** (`SimStore`): variable latency and per-MiB transfer time, transient errors before application, ambiguous errors after application (where the write is applied in storage while the caller receives an error), stalls exceeding lease durations, delayed responses, and eventually consistent `LIST` results.
- **Network**: variable latency, dropped responses, duplicated and delayed forwarded requests, bidirectional and unidirectional network partitions, and per-node storage isolation.
- **Nodes**: process crashes and restarts, graceful shutdowns, clock skew and step changes, and process pauses (simulating GC pauses or VM preemption). The local disk cache persists across node restarts and injects read/write errors and data corruption.
- **Chaos**: fault storms (crashes, restarts, partitions, storage isolation, clock jumps, and pauses) alternate with recovery periods so that workloads can make progress under heavy fault injection.
- **Buggify** (`src/buggify.rs`): deterministic fault-injection points in production code trigger rare execution paths (small commit batches, forced revalidation, cache misses, early flushes and sweeps, delays before manifest writes, idle handle eviction, dropped interactive transactions, and local request handling). Each call site is enabled or disabled for the entire run. Crash points terminate a node between internal steps of commits, compactions, sweeps, deletions, and auth operations. Client-visible fault sites and crash points are disabled during the post-chaos settlement phase.
- **Task scheduling**: tasks randomly yield before being polled, varying the execution order of concurrently ready tasks.
- **SimOAuth**: an in-process OIDC provider with a generated RSA key pair and JWKS endpoint exercises the full Google OAuth verification flow.

### 13.3 Workloads and checkers

Workloads run concurrently with a shared chaos loop (`ChaosPhase`). When all active workloads finish their chaos phase, fault injection stops, the cluster heals, and each workload runs its final consistency and invariant checks. The `mix` scenario runs a random subset of at least two workloads concurrently in the same cluster. Any run that exceeds 6 hours of virtual time fails as a liveness violation.

- `history`: Concurrent clients execute random multi-key transactions (single-request and interactive, serializable and snapshot-isolated, strong and snapshot reads, with and without idempotency keys). Each write records a unique marker key. At the end of the run, committed transactions are ordered by version and replayed to verify strict serializability, snapshot isolation, and read-your-writes.
- `kv_model`: Exercises all KV endpoints under faults and compares every response against a reference model. Ambiguous commits are resolved via `/commits/{id}` or database state inspection. Verifies range tombstones, size quota enforcement, and KV change feed reconstruction.
- `bank`: Concurrent account transfers across nodes. Verifies conservation of total balance and non-negative balances throughout the run and at completion.
- `register`: Concurrent writers and strong readers on a small key set, verified for real-time linearizability across `GET`, unforwarded `GET`, and read-only and read-write transactions.
- `tables`: Verifies table schema changes, point and range row mutations, unique index contention, table change feeds, and index integrity against a reference model.
- `schema_race`: Concurrent row writes (including batch updates and deletes at both isolation levels) racing with online column and index additions and drops. Verifies row state and index consistency.
- `ttl`: Verifies KV and row TTL expiration windows and distributed lock acquisition and renewal order under clock skew and faults.
- `watch`: Concurrent puts, TTL expirations, point deletes, and range deletes while a watcher polls `/changes` with strong and stale snapshot reads, resynchronizing from a scan or by following the feed from version 0. Verifies that every change matches a committed write, cursors do not expire within the transaction lifetime, and the reconstructed state matches the final database state.
- `streams`: Concurrent stream appends (with `expect_seq`, idempotency keys, and transactional counters), long-poll and SSE followers, forward/reverse/filtered/time-based reads, consumer cursors, explicit and retention trims, and stream recreation. Verifies dense sequence numbers, monotonic timestamps, and exact state reconstruction.
- `events`: Concurrent bulk inserts (JSON and NDJSON across all nodes), transactional inserts alongside row upserts, deletes by `uid` and by filter within time ranges, column additions, and retention updates, while queriers compare randomized queries (filters, group-bys, time buckets, lookups, two-stage funnels, and paginated row scans) against a per-version reference model. After fault healing, the checker verifies that every acknowledged event is returned exactly once. Additional sub-workloads exercise concurrent table drops, recreations, and column churn, insert events near dynamic retention cutoffs to verify that retained events are never dropped early, and run accelerated garbage collection to verify that all replaced and orphaned parts are deleted.
- `blobs`: Concurrent blob uploads, range reads, overwrites, and deletes verified by SHA-256, followed by garbage collection verification.
- `blob_gc_race`: Concurrent long-running blob reads racing with overwrites, copies, deletes, and short-grace-period garbage collection under clock skew.
- `abandoned`: Interactive transactions abandoned, aborted, or committed late across lease transitions. Verifies that committed updates are preserved, abandoned transactions are not committed, and open transaction handles are drained.
- `db_churn`: Rapid database creation, mutation, deletion, and recreation with small caches and aggressive handle eviction.
- `auth`: Verifies signup, login, session management, scoped and read-only API keys, Google OAuth account linking rules, and database access control.
- `orgs`: Concurrent organization creation, membership role transitions, and deletion races. Verifies role permissions within cache staleness bounds and organization ownership invariants.
- `accounts`: Verifies password changes, session revocation, email verification and password reset links, and account deletion across nodes.
- `limits`: Verifies per-key, per-user, and per-IP rate limiting and authentication failure backoff.
- `lifecycle`: Database creation, mutation, deletion, and recreation across node crashes, verifying background storage purge and per-owner database counters.
- `console`: Exercises all web console pages, verifying CSRF protection, HTML escaping, multipart uploads, safe attachment headers, pagination, and account/org/admin flows.
- `api_tour`: Exercises schema drop endpoints, transaction aborts, commit status lookup by idempotency key, and admin metrics endpoints.
- `memory`: Exercises many databases under a tight memory budget while long-poll requests wait on unloaded databases and multiple users hold interactive transactions with large write sets. Verifies that strong reads return all settled writes, long-poll requests observe expected writes, and a heavy user cannot starve other users of transaction memory for more than 20 s.
- `mix`: Runs a random combination of at least two workloads concurrently in a shared cluster and chaos loop.

Every 20 ms of virtual time, global oracles verify that at most 1 node relies on a lease for any database and that any node relying on a lease without an in-flight manifest write holds the latest manifest version in the store. At the end of every run, storage invariant checks verify that all manifests, levels objects, segments, blobs, stream chunks, and event parts and masks are intact and consistent, that event positions are unique across unsealed rows and parts, and that `part_bytes` equals the referenced parts' sizes. After a node shuts down gracefully, the simulator verifies that none of the released databases in storage have a lease held by that node.

### 13.4 Coverage, campaigns, and exploration

- **Probes** (`src/probe.rs`): instrumentation points in production code record rare execution paths (such as replayed commits, ambiguous manifest writes that succeeded, lease takeovers, duplicate forwards, and rejected compactions). `tests/coverage.rs` runs 8 seeds of each scenario and asserts that all documented API operations and all non-rare probes are reached.
- **Campaigns** (`sim_tool campaign`): executes scenarios across seeds on all available CPU cores and reports failure reproduction commands and probe hit rates.
- **Guided exploration** (`sim_tool explore`, `sim::explore`): alternates between fresh seeds and branched runs that fork from existing runs immediately after a rare probe is hit. Runs that discover a new coverage feature (a probe hit-count bucket or a pair of rare probes in the same run) are added to the exploration corpus.
- **Bug zoo** (`bugzoo/`): 27 patches reintroduce historical concurrency and fault-handling bugs (§13.6), and `bugzoo/run.sh` measures how many simulation runs are required to detect each defect.

### 13.5 Other test layers

- **Unit tests** in each module.
- **Property tests** using the in-crate generator and shrinker (`src/util/prop.rs`): verifies codec round trips, tuple ordering, segment building and reading, LSM-tree operations and change feeds against a reference model, stream operations and filters, table and index operations, range tombstone fragmentation, max-min fair memory allocation, base64/varint/bloom filters, and metadata access-control state machines.
- **Fuzz tests**: feeds arbitrary and mutated byte inputs to all binary and text decoders and random JSON requests to the HTTP router, verifying that arbitrary inputs do not cause panics or `500` responses.
- **Transaction-op fuzzing** (`sim/txn_fuzz.rs`): generates random single-request and interactive transactions (including malformed operations) and compares every response against an independent transaction model.
- **OpenAPI conformance**: validates every simulated HTTP response against `openapi.json` and verifies that `openapi.json` matches the router and generated specification.
- **Request IDs**: verifies in a multi-node simulation that `x-request-id` headers are validated, echoed in responses, propagated across forwarded requests and metadata commands, and recorded in server logs.
- **Checker self-tests**: unit tests feed synthetic invalid histories and corrupted states to each simulation checker to verify that every checker detects violations.
- **Mutation testing** (`scripts/mutants.sh`): runs `cargo-mutants` against the storage engine, table layer, transaction operations, and simulation checkers.
- **Object-store conformance** (`store::conformance`, `tests/store_conformance.rs`): Verifies the object-store contract using 2 independent clients of the same store: conditional writes (including 16-writer races), cross-client visibility, etag uniqueness, byte ranges, pagination, store-clock coherence, and randomized model checking against `MemStore`. Runs against `MemStore`, `FsStore`, `CloudStore` backed by a fault-injecting in-process S3 server, and a live S3 store if `BASERUNNA_TEST_STORE` is set. Also verifies that each check detects synthetic contract violations, and that nodes run conditional-write preflight checks before serving.
- **Failover check** (`examples/cluster_check.rs`): Runs multiple nodes against a shared store while killing and restarting nodes at random. Concurrent clients execute counter increments with idempotency keys and bank transfers with commit IDs, verifying that all committed writes are durable, strong reads match settled writes, and total account balances are conserved.
- **Memory bound** (`tests/memory_bound.rs`): Uses a counting global allocator to verify that memory charges cover actual heap allocations for decoded manifests and that opening hundreds of databases with retained handles stays within the configured memory budget.
- **Cost model** (`sim::cost`, `tests/cost.rs`): measures object-store `GET`, `PUT`, `HEAD`, and `LIST` request counts per operation in virtual time and enforces per-operation and monthly workload cost budgets:

  | operation                                     | requests                                |
  | --------------------------------------------- | --------------------------------------- |
  | commit (lease holder)                         | 1 PUT                                   |
  | 64 concurrent commits                         | 1 PUT in total                          |
  | strong read, lease holder                     | 0                                       |
  | strong read, engine on a non-holder           | 1 GET (304)                             |
  | strong read through the API, unleased         | 1 GET                                   |
  | snapshot read within staleness                | 0                                       |
  | read-heavy database (10 strong reads/s)       | 276 PUT + 54 GET per hour               |
  | cold open + point read, 3 segments            | 2 GET (40 ms)                           |
  | cold full scan, 128 values of 16 KiB          | 1 GET                                   |
  | open + full scan after a restart (local tier) | 1 GET (the manifest)                    |
  | blob upload / download                        | 0.25 PUT / 0.125 GET per MiB            |
  | steady writes, with flush/compaction/GC       | 1.001 PUT, 0.001 GET per commit         |
  | idle database                                 | 0 per hour                              |
  | idle change-feed watcher (5 min staleness)    | 12 GET per hour                         |
  | cross-node commit wakeup via notice           | 1 GET (40 ms)                           |
  | node overhead (heartbeats, membership)        | 31 PUT + up to 140 GET per hour         |
  | stream append                                 | 1 PUT                                   |
  | stream tail read, lease holder                | 0                                       |
  | log ingestion, 20 KB NDJSON appends           | 1 PUT per append + 5 PUT per MiB, 0 GET |
  | cold read of a sealed log                     | 5 GET per MiB                           |
  | event insert in a transaction                 | 1 PUT                                   |
  | event bulk ingest, 1 KB NDJSON events         | 1.5 PUT + 1.3 GET per MiB               |
  | warm event query                              | 0                                       |
  | cold event query                              | 1.5 GET per part read                   |
  | idle event table                              | 0 per hour                              |

Commands:

```
cargo test                                                       # run unit, property, fuzz, and simulation tests
BASERUNNA_TEST_STORE=s3://bucket cargo test --release --test store_conformance   # check a live store
BASERUNNA_STORE=s3://bucket cargo run --release --example cluster_check -- 180    # run the failover check
SIM_SEEDS=500 cargo test --release --test sim                    # run 500 seeds per scenario
cargo run --release --example sim_tool -- campaign all 3000      # run a parallel campaign (SIM_MINUTES, SIM_COVERAGE=1)
cargo run --release --example sim_tool -- explore all 1          # run guided exploration (SIM_RUNS, SIM_COMPARE=1)
cargo run --release --example sim_tool -- trace <scenario> <seed> [branches]
SIM_SEED=12345 cargo test --test sim sim_mix -- --nocapture      # reproduce a single seed
bugzoo/run.sh campaign all 37                                    # run the bugzoo benchmark (§13.6)
cargo run --release --example cost_model                         # print object-store request counts and monthly costs
cargo run --release --example amplification                      # measure compaction write/space/read amplification
cargo bench --bench throughput                                   # run wall-clock throughput benchmarks
scripts/mutants.sh                                               # run mutation testing
```

### 13.6 Defects found by the simulator

1. _Read-your-writes across nodes._ A database created on node A was briefly invisible on node B because node B read a stale metadata snapshot. Fixed by retrying snapshot misses with a strong read (§8).
2. _Lost routing extensions._ Falling back to local execution after a failed forward dropped Axum routing extensions and returned `500`.
3. _Duplicate versions in group commit._ Two transactions in the same commit batch received the same version number, breaking `If-Match`. Fixed by assigning each transaction a distinct sequence number (§5.5).
4. _Linearizability during in-flight manifest writes._ A lease holder served its cached version while its own manifest write was already visible to other nodes. Fixed by adding an in-flight write barrier (§5.5).
5. _Nondeterministic wakeups._ `tokio::sync::watch` wakes tasks in random order. Replaced with `Notify` and forbidden by `tests/lint.rs`.
6. _Interrupted database deletions._ A crash mid-deletion left the database name bound to a deleted manifest. Fixed by completing interrupted deletions during name resolution (§8).
7. _Zombie requests._ A request that continued executing on the server after the client timed out could commit later. Fixed by enforcing commit deadlines and fencing commit status checks (§6.2).
8. _Stale listings._ An API key created on one node was missing from a listing served by another node using a bounded-staleness read. User-facing listings now use strong reads.
9. _Stale names after purge._ A cached name for a purged database returned `404` instead of resolving to a newly created database with the same name. Missing manifests now trigger strong name re-resolution.
10. _Uninitialized manifests treated as deleted._ The interrupted-deletion repair initially treated a newly created database without a manifest object as deleted. Only manifests with `deleted = true` now trigger repair.
11. _Duplicated forwarded requests._ A duplicated forwarded `commit` or `PUT` was executed twice. Non-idempotent forwarded requests are now deduplicated by `x-baserunna-forward-id` (§7).
12. _Stale lease reads under clock skew._ A node with a slow wall clock continued serving leased reads after another node with a fast clock took over the lease. Lease validity is now tracked using monotonic clocks (§5.7).
13. _Failover livelock after handle eviction._ Storing observed lease timestamps on the `Db` handle caused nodes to reset lease timers whenever an idle handle was evicted. Observed lease timestamps are now stored in engine-wide shared state.
14. _Snapshot isolation and schema changes._ A snapshot-isolated row write racing with `add_index` committed without updating the new index. Table schema reads are now validated at commit time under all isolation levels (§6.1).
15. _Blob garbage collection during reads._ Reading a blob chunk that was garbage-collected mid-read returned `500` instead of a retryable `503` (§5.8).
16. _Duplicated forwards across node restarts._ A duplicate forwarded `DELETE` arrived after a node restarted and deleted a recreated database. Forwarded requests now include the target node ID (`x-baserunna-forward-to`) and are rejected if the node ID has changed (§7).
17. _Identical lease expiration timestamps._ A commit renewal and a longer read-lease renewal could produce the same `expires_ms` value, causing peers to treat the newer renewal as already seen. Lease observations now key on `write_seq` instead of `expires_ms`.
18. _Garbage collection stalled by slow clock probes._ Consecutive slow store-clock probes caused a node to skip background sweeps. Clock probes are now retried and refreshed in the background.
19. _Sessions surviving password changes._ Updating a password and revoking sessions in separate transactions allowed old sessions to survive if the node crashed between them. Sessions now include a fingerprint of the password hash so that changing the password invalidates old sessions atomically (§9).
20. _Failed deletion repairs returning 404._ When repairing an interrupted database deletion failed, the node returned `404` instead of a retryable `503`, leaving the database counted against the owner's quota.
21. _Transient storage errors reported as invalid tokens._ Email verification and password reset links checked during a system database outage returned `401` instead of a retryable `503`.
22. _Lease loss after an ambiguous manifest write._ When a manifest write timed out after being applied and another node acquired the lease before the retry, the committer returned `not_owner` without marking the outcome unknown.
23. _Retention trims skipped after ambiguous writes._ After an ambiguous manifest write or replayed commit, the lease holder did not schedule background stream retention trims.
24. _Commit status checks after commit record expiration._ Checking `/commits/{id}` after `commit_retention_ms` elapsed returned `not_committed` even when the commit had been applied. The response now includes `retention_ms` so callers know when the status is no longer definitive (§6.2).
25. _Obsolete objects retained after ambiguous writes._ Obsolete objects removed from a manifest were deleted from storage only when the manifest `PUT` returned `Ok`. Obsolete objects past their grace period are now deleted even when the manifest `PUT` outcome is unknown (§5.5).
26. _Self-forwarding after a restart._ A node that restarted at the same network address treated its previous node ID as a live peer and forwarded requests to itself. Nodes now ignore membership entries with their own address (§7).
27. _Lease state lost after ambiguous writes._ When a manifest `PUT` timed out after being applied and a retry reloaded the committed manifest, the node did not record itself as the lease holder. Reloading a manifest written by an ambiguous write from the same node now restores the local lease state (§5.5).
28. _Stale authorization cached after a slow read._ Caching role lookups relative to when the read completed allowed a lookup that started before a role revocation to remain cached after the revocation committed. Cache expiration is now measured from the start of the read (§8).
29. _Unknown lease holder in routing table._ When a newly started node acquired a lease before peers refreshed their membership table, peers failed to forward requests to it. Nodes now trigger a strong membership refresh when a lease holder is missing from the cached member list (§7).
30. _Non-idempotent password reset retries._ If session creation failed after a password reset updated the password hash, retrying with the same reset link failed with `401`. Retrying a reset token with the same password now completes the reset.
31. _Lease shortened by an in-flight write._ A node holding a 15 s read lease that sent a commit renewing a 5 s write lease continued serving reads under the 15 s lease until the commit response arrived, while peers bounded the lease to 5 s as soon as the write landed. A node now bounds its local lease to the duration granted by an in-flight write as soon as the write is sent (§5.7).
32. _Compaction outputs swept as orphans._ A compaction that stalled longer than the 2-hour orphan grace period had its uncommitted output segments marked obsolete by a concurrent sweep and then installed them into the manifest, causing live segments to be deleted. Compaction installation now fails if any output segment is listed in `obsolete` (§5.8).
33. _Segments garbage-collected during slow reads._ A read that stalled longer than the 30-minute obsolete segment grace period returned `500` when its segment was deleted; it now returns a retryable `503` if the segment is no longer referenced by the current manifest (§5.8).
34. _Database deletion failures reported as not applied._ If the metadata transaction for `DELETE /v1/dbs/{db}` failed with an ambiguous error after the manifest was marked deleted, the node returned a non-applied `503`; it now marks the outcome as unknown.
35. _Metadata commands forwarded to a previous process._ Forwarded system-database commands did not filter out membership records sharing the local node's address, causing a restarted node to forward metadata commands to its own previous node ID.
36. _Missing deadlines on forwarded metadata commands._ Forwarded metadata commands did not propagate the originating request's commit deadline, allowing a stalled database creation to be committed after the client timed out and deleted the database (§7).
37. _Lease state lost on handle eviction._ Evicting an idle `Db` handle discarded the node's own lease timer, causing the lease holder to forward requests to another node while peers forwarded requests back to it. Nodes now retain their own lease state across handle eviction.
38. _Stale member records after restart._ After a node restarted at the same address with a new node ID, peers continued routing to the old node ID until its heartbeat expired. Membership resolution now keeps only the latest registration for each network address (§7).
39. _Rate limits charged on both forwarding and receiving nodes._ Forwarded requests were charged against rate limits on both the entry node and the lease holder. Forwarded requests now authenticate with the cluster key and are rate-limited only on the entry node (§7).
40. _Unique index entries lost during online index backfill._ Updating or deleting a row while a unique index was in the `Building` state unconditionally deleted the old key from the index, removing the entry written by the backfill for another row with the same value. Unique index maintenance during `Building` now deletes an entry only if its primary key matches the modified row.
41. _Deleted databases never purged._ The database reaper previously ran only on the system database lease holder, which could leave the reaper stopped during lease handoffs. The reaper now runs on the live member selected by rendezvous hashing (§5.8).
42. _Retention trims dropped on transient errors._ Failed stream retention trims were dropped until the next append or sweep. Transient trim failures are now retried with backoff.
43. _Missed compaction wakeup._ If a commit was installed while a finishing compaction task still held the compacting flag, the node did not schedule a follow-up compaction. Background tasks now recheck whether additional work is needed before clearing their active flag.
44. _Duplicate background flushes shadowing sealed stream chunks._ Installing a stale background flush after another flush of the same entries produced duplicate stream records that shadowed sealed stream chunks. Flush installs now verify that `flush_seq` is unchanged (§5.5).
45. _Referenced stream chunks deleted after ambiguous flush install._ Retrying a flush install after an ambiguous manifest write treated the already-installed flush as superseded and deleted its chunks. Superseded flushes after an ambiguous write now preserve their output objects.
46. _Precondition failure overtaken by in-flight attempt._ If a retried idempotent request failed a precondition while an earlier attempt was still in flight, the earlier attempt could still be committed after the `412` response. Precondition failures now write an abort record before returning (§6.2).
47. _Stale leased reads during hedged manifest writes._ If a hedged duplicate `PUT` failed with `412` while the primary `PUT` was still in flight, the node reloaded the prior manifest and served leased reads before the primary `PUT` completed. Nodes now disable leased reads while a hedged `PUT` remains in flight and require a manifest read started after the ambiguous write before restoring trust (§5.5).
48. _Levels object garbage-collected during stalled manifest read._ If a manifest read stalled past the GC grace period, its `.levels` object could be deleted before being fetched, causing a `500` error. The node now reloads the manifest if a `.levels` object is not found (§5.8).
49. _Forwards rejected after a peer restart._ When a node forwarded a request to a peer that had restarted at the same address with a new node ID, the peer returned `503` and the entry node returned that error to the client until its membership cache refreshed. The peer now tags the rejection with `x-baserunna-stale-target`, and the entry node marks the peer as suspect and handles buffered requests locally (§7).
50. _Event table drops starved by inserts._ Dropping an event table previously scanned all part references into the transaction read set, causing concurrent binds, seals, or merges to trigger serializable conflicts under steady insert load. Dropping an event table now writes range tombstones without reading individual part references and subtracts the table's recorded part bytes in the committer (§5.13).

Cost model tests (§13.5) also identified seven efficiency defects: lapsed leases on read-heavy databases were never re-acquired, unleased strong reads validated the manifest twice, read-only renewals used the shorter write-lease duration, `.levels` objects under a separate prefix required an extra `LIST` call per sweep, stream flushes wrote separate small segments for commit records and stream state instead of retaining unsealed entries in `tail`, snapshot-consistency API requests validated the manifest with a strong read on open, and snapshot-consistency long polls re-read the manifest every second instead of waiting for their snapshot staleness bound.

Security review alongside simulation testing identified that internal cluster endpoints were unauthenticated when `BASERUNNA_CLUSTER_SECRET` was unset; clusters now always generate and require a cluster key (§7).

## 14. Examples (`examples/`)

Each example starts an in-process node, talks to it over HTTP, and prints its activity:

1. `todo_app`: a multi-user todo service with a table, secondary indexes, and paginated queries.
2. `bank_ledger`: interactive serializable transactions, conflict retry, idempotency keys.
3. `asset_store`: a media library with large blobs, range reads, and metadata rows that reference blob columns.
4. `saas_tenants`: a database per tenant, API keys scoped per DB, read-only keys.
5. `feature_flags`: a read-heavy KV config store with snapshot reads and compare-and-set updates.
6. `multi_node`: three nodes on one store, forwarding, a node killed
   mid-flight, and failover without data loss.
7. `app_logs`: JSON log lines shipped as NDJSON into a stream with retention, the latest errors found newest first by bounded filtered scans, `tail -f` with a long poll, and a shipper that forwards each line exactly once by advancing its consumer in its transaction.
8. `chat_rooms`: a stream per room created by its first message, a message and a counter committed together, members waiting for new messages, `expect_seq` for posting only after the latest message, and unread counts from consumer cursors.

Tools:

- `sim_tool` runs simulation campaigns and prints traces.
- `cost_model` prints object-store requests per operation and the cost per database-month
- `amplification` the write, space and read amplification of compaction and of streams
- `commit_latency` measures commit and object-store request latency across manifest tail limits
- `gen_openapi` regenerates `openapi.json`

## 15. Known limitations (v1)

- The manifest is rewritten on every commit, and with it the WAL and L0.
- Commits are remembered for 10 minutes: an idempotent retry or a commit status query must come within that (§6.2).
- TTL expiry is as precise as node store-clock measurements (§4.5), and expired data takes space until a manifest write or compaction reclaims it.
- Change feeds report each key's latest state, not every intermediate value, and a cursor more than a transaction lifetime behind may have to start over from the current state (§6.4).
- Rate limits and sign-in backoff are kept per node (§9): a cluster of N nodes admits up to N times each rate, and a restart forgets them.
- Size quotas are one setting for every database; there are no per-user or per-database overrides.
- Roles are per organization: a user cannot be granted one database of an organization only.
- Mail is sent only through an HTTP webhook (§12).
- Tigris is the only tested production object store (§13.5). `fs:` stores are restricted to a single process (§12).
- After a node crash, writes to databases leased by that node wait for the lease to expire: up to 11 s after a commit renewal, or up to 31 s after a read-lease renewal (§5.7).
- Garbage collection requires object `last_modified` timestamps to come from a single coherent store clock (§13.5).
- Stream retention runs on the lease holder, so a database with no activity keeps records past their retention until it is used again (§5.11). Trimmed chunks and their quota bytes are reclaimed by sweeps, hours later, as overwritten blobs are.
- Stream filters scan records; there is no index on record contents, so a query finds needles at the rate it can scan (`scan_limit` records a request).
- Event queries execute on a single task on the receiving node, bounded by single-core decoding throughput and up to 32 concurrent part reads (§5.13).
- Event query lookups scan the entire referenced row table on each query into in-memory maps capped at 1 GiB (§4.8).
- Event merges and retention drops run only on active databases, so an idle database retains expired parts (which queries filter out) until the database is accessed again (§5.13).
- Event parts are cached only in the in-memory block cache and are not written to the local disk cache tier (§5.3).

## 16. Future work

- Point-in-time restore (keep old manifests)
- change feeds that keep deletions longer, or every intermediate value
- quotas and rate limits per user or database, shared across nodes
- per-database grants within an organization
- service accounts (API keys that belong to an organization)
- stream consumer groups that share a stream's records among workers, with acknowledgements and redelivery
- per-block summaries of stream records (bloom filters of field values) so that filters skip blocks
- merging small sealed chunks
- distributed event query execution across cluster nodes with partial aggregate merging on the entry node
- pre-aggregated event rollups and alternate sort-order projections maintained during seals and merges
