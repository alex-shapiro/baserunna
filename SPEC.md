# Baserunna Specification

Baserunna is a web service that stores small databases on object storage.
Each database supports serializable transactions, KV APIs, tables, and
streams. Baserunna service data is itself stored in a Baserunna database.

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

If the inline `tail` grows past `tail_max_bytes` (256 KiB), the committer writes its entries to a new immutable **segment** and clears the manifest tail. Each segment is an SSTable (Sorted String Table) designed for random and sequential reads over object storage (§5.3):

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
  the transaction aborts with `409 already_exists`.
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

### 5.2 Manifest

The manifest is a binary, CRC32C-checksummed document (`engine/manifest.rs`):

```
magic "BRMF", format u16
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
obsolete: [(kind, id, since_store_ms)]                segments, levels objects and blobs awaiting deletion
blob_bytes: {bound, swept, swept_at}                  blob bytes for the size quota (§5.10)
chunk_bytes: {bound, swept, swept_at}                 sealed stream chunk bytes, likewise (§5.11)
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

During a forward range scan, blocks are initially fetched one at a time. If a scan has already fetched 2 blocks of the same segment from object storage, it switches to bulk read-ahead, coalescing consecutive remaining blocks needed by the scan's key range (up to 1 MiB) into a single ranged `GET`. Compactions and garbage-collection sweeps run, bypassing caches so background merges do not evict hot working-set blocks.

#### Local Caching

Each database caches its reads and writes in an in-memory LRU, up to 512 MiB. This LRU holds decoded blocks, segment indexes, and `.levels` objects. Any segment that a node writes is pre-cached.

Each database also caches its reads and writes on the node's local disk, up to 10 GiB. Small (<1 MiB>) segments are cached as a single file, `#all`. Larger segments cache their metadata tail as `#meta` and individual compressed blocks as separate files `#b0`, `#b1`, …. Each file is written atomically and is checksummed by a trailing CRC32.

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
   - Skip and return the recorded version if its commit record already exists in `base` or earlier in the batch.
   - Reject it if its 10 second commit deadline has passed.
   - Validate its read set (§6.1) against all writes with `seq > txn.read_version` in `base` and in earlier accepted transactions in the batch, rejecting conflicts with `409 conflict`.
   - Assign the accepted transaction the next sequence number (`base.version + 1`, `+ 2`, …), merge its writes into `tail`, and write its commit record.
5. Expire old `tail` entries and flush `tail` to Level 0 segments if it exceeds 256 KiB.
6. Renew `manifest.lease` for 5 s (§5.7).
7. Write the updated manifest with `PUT manifest If-Match: base.etag`, install the new `Version` in memory, and acknowledge the accepted transactions.
8. Schedule background compaction if needed (§5.6).

While the manifest `PUT` is in flight, strong reads on the committing node wait for the write to finish. Because another node may already see the new manifest in object storage, serving the committing node's previous cached version could allow reads to move backward in time.

#### Range Deletes

A `delete_range` with size ≤ 256 is committed as individual point tombstones. A larger `delete_range` is committed as a single range tombstone `[start, end) @ seq` to `manifest.range_deletes` that drops covered entries from `tail` and removes any older range tombstones contained within `[start, end)`.

#### Tail Maintenance and Flushing

Before writing the manifest, any `tail` entries with `expires_at <= version_time` are converted into tombstones, and commit records older than 10 min are removed.

If `tail` exceeds 256 KiB, the committer writes its entries to new Level 0 segments via unconditional `PUT`s and clears `tail`. User data and commit records are written to separate segments. If the node crashes or the subsequent manifest `PUT` fails, these unreferenced segments are cleaned up later as orphans (§5.8).

#### CAS Conflicts and Retries

If a conditional `PUT` on the manifest does not succeed on the first attempt:

- On `412 Precondition Failed`, another node updated the manifest. The committer reloads the manifest. If another node now holds a valid lease, the batch fails with `not_owner`; otherwise the committer re-runs validation and applies the batch against the new `base`.
- On a timeout or network error, it is unknown whether the `PUT` landed. The committer reloads the manifest and retries against the new `base`. If the earlier `PUT` succeeded, step 4 finds the batch's commit records in the reloaded manifest and returns their committed versions without re-applying the writes. If a delayed copy of the first `PUT` is still in flight, it carries the same `If-Match` etag as the retry so at most one can succeed. After exhausting retries, the committer returns `503 unavailable` with `outcome: "unknown"`, which the client can resolve via `GET /commits/{id}` or by retrying with the same idempotency key.

#### Commit Records

Commit records make retries idempotent and let clients check the outcome of ambiguous commits. If a transaction is accepted, the committer writes an entry in the system keyspace (`0xFF ++ commit_id`, §4.2) with the transaction's committed version as its `seq` in the same manifest update as its data writes. A transaction committed if and only if its commit record exists.

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

Compaction never loads entire segments into memory. Instead, it streams through each input segment in 1 MiB ranged `GET`s, bypassing the node's cache, and writes out new 2 MiB segments as they fill. Memory usage remains bounded to one 1 MiB read buffer per input run and one 2 MiB output buffer, regardless of database size.

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
- Every other node considers that lease valid until `duration + 0.5 s` after it first observed that renewal. First-sighting timestamps are retained even if an idle database handle is evicted from memory.

Each commit renews the lease for 5 seconds. On a read-heavy database where at least 16 strong reads occured within the current or previous lease window, the holder issues a lease-only manifest CAS about 1 s before expiry to renew the lease for 15 seconds. At ~$0.005 per 1,000 `PUT` versus ~$0.0004 per 1,000 `GET`s, a read-lease renewal pays for itself if it saves 12 conditional `GET` requests.

Because other nodes refuse to commit while a lease is valid, the holder's cached `Version` is guaranteed to be current, allowing it to serve strong reads directly from memory.

If a lease holder crashes, database writes pause until `duration + 0.5 s` after another node first observes the dead holder's last renewal. If another node checks the manifest just before the lease would have expired, failover takes at most `2 * duration + 0.5 s`: up to 11 s after a commit renewal or 31 s after a read renewal. Strong reads remain available on all nodes during failover via conditional `GET` validation (§5.9).

### 5.8 Garbage Collection

Garbage collection deletes replaced segments, unreferenced orphan objects, and dropped databases from object storage.

#### Store Time

Garbage collection compares timestamps written by different nodes against object-store `last_modified` times, so all GC timestamps use the object store's clock rather than node wall clocks.

Every 10 minutes, each node overwrites a shared `clock` object and reads back its `last_modified` timestamp. Because that timestamp was assigned between the request start and response receipt on the node's monotonic clock, the midpoint of that interval gives the node's clock offset and the half-width gives the error bound. Probes with an error bound above 10 s are retried. A measurement is trusted for up to 1 hour. If a node lacks a valid measurement, it performs no garbage collection and refuses to bind staged blobs.

#### Obsolete Segments

If compaction replaces segments or a `.levels` object, it adds their IDs to `manifest.obsolete` with the current store time. During subsequent commits or compactions, the lease holder deletes any object in `obsolete` that has been listed for at least 30 min and removes it from the manifest. Because transactions time out after 5 min, no active reader can still be reading a deleted segment.

#### Orphans

Crashed flushes, aborted compactions, abandoned blob uploads, and overwritten rows leave unreferenced objects in storage. At most once per hour on active databases, the lease holder runs a background sweep:

1. Record the current store time and snapshot the current `Version`.
2. Stream through all LSM entries in that `Version` to collect every referenced `blob_id` and stream `chunk_id`, and list `seg/` and `blob/` in object storage.
3. Move any unreferenced segment or `.levels` object whose `last_modified` is older than 2 hours into `manifest.obsolete`.
4. Move an unreferenced blob into `manifest.obsolete` if its `last_modified` is older than 2 hours and the creation time in its `blob_id` is older than 1 hour plus twice the clock error bound. Because commits refuse to bind staged blobs older than 1 hour or blobs listed in `obsolete`, and the sweep recorded the time before reading the `Version`, no concurrent or future commit can bind a blob that the sweep marks obsolete.

#### Long Blob Reads

Streaming a large blob by ID can take longer than the 30 min obsolete grace period. A blob read that has run for 10 min writes a `dbs/{db_id}/blob/{blob_id}/pin` object and refreshes it every 10 min. If a blob is already listed in `obsolete` when a read by ID begins, the reader writes the pin before reading any chunks.

Before deleting an obsolete blob, GC checks its `pin` object. If the pin was updated within the last 20 min (plus twice the clock error bound), GC leaves the blob in storage as an orphan for a later sweep. If a long read cannot write its pin because object storage is unreachable, it fails with a retryable `503`.

#### Deleted Databases

Deleting a database marks its record as deleted in the system database. After a 10 min grace period (exceeding node metadata cache TTLs), a reaper on the system database's lease holder lists and deletes all objects under `dbs/{db_id}/`. It then waits another 10 min and runs a second pass to catch any objects written by lagging requests before removing the database record.

### 5.9 Read Consistency

There are two read consistency modes:

- `snapshot` consistency reads from a serializable snapshot that is at most 5 seconds old.
- `strong` consistency guarantees linearizability. It is served from cache if the serving node holds a valid lease and has no manifest write in flight; otherwise it requests `GET manifest If-None-Match: etag` first.

### 5.10 Size Quotas

Each user database is capped at 10 GiB by default (`BASERUNNA_DB_MAX_MB`; the system database is exempt). A database's accounted size is the sum of its `tail` bytes, LSM segment bytes (excluding commit-record segments), referenced blob bytes, and sealed stream chunk bytes (§5.11).

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

---

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
   in one request. Supports `check` precondition ops and an optional
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
   abort. Transaction state is stored on a database's owner node. If the
   owner changes or crashes, the transaction is lost (`404 txn_not_found`)
   with no data committed. `begin` returns the transaction's `commit_id` up
   front, and `commit` the seqs its appends received.

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

| op             | fields                                                                                                                                         | result                                                  |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------- |
| `get`          | key                                                                                                                                            | `{found, value? \| blob?, version?, expires_at?}`       |
| `scan`         | `prefix` or `start`/`end`, `limit` (≤1000), `reverse`, `keys_only`                                                                             | `{items, next_cursor?}`                                 |
| `put`          | key, `value` or `blob_id`, `ttl_ms?`                                                                                                           | `{}`                                                    |
| `delete`       | key                                                                                                                                            | `{}`                                                    |
| `delete_range` | `prefix` or `start`/`end`                                                                                                                      | `{}` (expanded at commit)                               |
| `check`        | key + one of `exists`, `version`, `value`                                                                                                      | fails txn with 412 if false                             |
| `increment`    | key, `by` (i64), `ttl_ms?`                                                                                                                     | `{value}`. Value is ASCII decimal; missing = 0          |
| `get_row`      | table, `pk` (array or scalar)                                                                                                                  | `{found, row?, expires_at?}`                            |
| `insert_row`   | table, `row`, `ttl_ms?`                                                                                                                        | `{}`. 409 if pk exists                                  |
| `upsert_row`   | table, `row`, `ttl_ms?`                                                                                                                        | `{}`                                                    |
| `update_row`   | table, `pk`, `set`, `ttl_ms?`                                                                                                                  | `{found}`. Partial update                               |
| `delete_row`   | table, `pk`                                                                                                                                    | `{found}`                                               |
| `query_rows`   | table, `index?`, `eq?`, `range?`, `filter?`, `limit`, `reverse`, `cursor`                                                                      | `{rows, next_cursor?}`                                  |
| `update_rows`  | table, `where` (`index?`, `eq?`, `range?`, `filter?`), `set`, `ttl_ms?`, `limit?`                                                              | `{count}`                                               |
| `delete_rows`  | table, `where`, `limit?`                                                                                                                       | `{count}`                                               |
| `append`       | stream, `records` (`value` or `json`, `headers?`), `expect_seq?`, `create?`                                                                    | `{count, first_seq?}` (`first_seq` once committed)      |
| `read_stream`  | stream, `from_seq`, `from_time`, `tail` or `consumer`, `until_seq?`, `until_time?`, `reverse`, `limit`, `max_bytes?`, `filter?`, `scan_limit?` | `{records, next_seq, done}`                             |
| `stream_info`  | stream                                                                                                                                         | `{first_seq, next_seq, bytes, first_time?, last_time?}` |
| `trim_stream`  | stream, `before_seq`                                                                                                                           | `{}`                                                    |
| `get_consumer` | stream, consumer                                                                                                                               | `{found, seq?}`                                         |
| `set_consumer` | stream, consumer, `seq`                                                                                                                        | `{}`                                                    |

`ttl_ms` (1 ms to 10 years) adds a TTL expiration relative to write
commit time (§4.5); `update_row`, `update_rows` and `increment` do not
upate an existing TTL by default.

A record body is a `value` string or `json` value. Reads return each record
as `{seq, time, headers?, value | json}`. A read's `next_seq` is where the
next page starts: the next `from_seq` of a forward read, the next `until_seq`
of a reverse one. `done` says the read reached its end seq or time, or the end
of the stream.

JSON keys and values are encoded as UTF-8 strings by default. With `"encoding": "base64"`
on the request, every key and value in the request and response is standard base64 instead.

### 6.4 Change feeds

`GET /v1/dbs/{db}/changes?after=…` follows the changes to keys, optionally
of a `prefix` or a `start`/`end` range; `GET /v1/dbs/{db}/tables/{t}/changes`
follows changes for a specific table. A page reports, for every key changed after
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

| status | code(s)                                                                                                                |
| ------ | ---------------------------------------------------------------------------------------------------------------------- |
| 400    | `invalid_argument`                                                                                                     |
| 401    | `unauthenticated`                                                                                                      |
| 403    | `permission_denied`                                                                                                    |
| 404    | `not_found`, `txn_not_found`                                                                                           |
| 409    | `conflict` (retryable), `already_exists`                                                                               |
| 412    | `precondition_failed` (includes the failing op index; also an append whose `expect_seq` no longer holds)               |
| 410    | `cursor_expired` (a change feed cursor older than the deletions kept, §6.4)                                            |
| 413    | `too_large`                                                                                                            |
| 429    | `rate_limited` (retryable, `Retry-After`; not applied)                                                                 |
| 507    | `quota_exceeded` (the database is at its size quota, §5.10)                                                            |
| 503    | `unavailable` (retryable, `Retry-After`; may include `outcome: "unknown"`; also a query on an index still being built) |
| 500    | `internal`                                                                                                             |

Routes (see the OpenAPI document for bodies):

```
GET /healthz liveness (503 while shutting down)
GET /v1/openapi.json
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
GET /v1/admin/users GET /v1/admin/nodes POST /v1/admin/users/{id}/disable
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

| workload       | checks                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `kv_model`     | Random KV ops through every API form under faults, each response compared with a model. Ambiguous commits are resolved through `/commits/{id}`. Bulk writes and prefix deletes make range tombstones over segment data, and a final phase outlives every transaction so they retire. A small size quota refuses writes now and then: refused writes leave no trace, deletes are never refused, and the size stays near the quota. A follower of the change feed checks every change against the model and must rebuild the model's state, starting over when its cursor expires.                                                                                                                                                                                                                                                                                                           |
| `bank`         | Concurrent transfers across nodes. The total never changes and no balance goes negative, checked during the run and at the end.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `register`     | Concurrent writers and strong readers on a few keys, checked for real-time linearizability.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `tables`       | Schema changes and row ops against a model, including updates and deletes by selection that must change exactly the rows the model selects. A follower of the table's change feed must rebuild the model's rows. At quiescence indexes match a recomputation from rows, unique constraints hold, and dropped tables and indexes (large enough to take range tombstones) leave nothing behind.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `schema_race`  | Row writes on every path (batch updates and deletes included) and at both isolation levels race schema changes. Indexes must match rows, nothing may be orphaned, and each writer's rows must match its model.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `ttl`          | Values and rows with times to live. Each read must see a value until its expiry window (the commit request's span plus the TTL, widened by the clock error bound) and never after, nor a value it replaced; a unique value is free exactly when its row expired. Clients take and renew a lock held by an expiring value: by version, each acquisition commits no earlier than the previous lock expired and each renewal before the lock it read did, which the locks' own expiries tell exactly.                                                                                                                                                                                                                                                                                                                                                                                         |
| `watch`        | A writer commits puts (some expiring), deletes and range deletes while a watcher follows the change feed with long polls through random nodes. Every change received must be what the writer wrote at that version, cursors expire no sooner than a transaction lifetime after they caught up, and the watcher's state must end up equal to the database's.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `streams`      | Appenders (some with `expect_seq`, some appending in a transaction that also increments a counter, all with idempotency keys), followers on long polls and event streams, readers by seq, time, tail, consumer, in reverse and with filters, explicit and retention trims, and a stream deleted and recreated, through random nodes under faults. Every record read must be the one appended at its seq, seqs dense and times never decreasing and within their append's window; a strong read must see every append acknowledged before it began and no trim undone; `expect_seq` lets exactly one of the appends expecting a seq through; a counter and its stream must agree; followers receive every record once, in order; retention leaves each stream within its limits; and every stream must end equal to its model. Enough data flows to seal chunks at flush and in compaction. |
| `blobs`        | Uploads of random sizes on every path, range reads, overwrites and deletes, checked by SHA-256. A final GC keeps every referenced object and removes every unreferenced one.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `blob_gc_race` | Blob reads race overwrites, deletes and GC, with grace periods shorter than a download (and, hostile, node clocks an hour apart). A read may fail cleanly but never return wrong bytes.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `abandoned`    | Interactive transactions abandoned, aborted or committed late while leases move. No update is lost, abandoned transactions never commit, and open transactions drain.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `db_churn`     | Databases created, used, deleted and recreated under a tiny cache and fast handle eviction. Each matches its model, deleted names answer 404, handles and cache stay bounded, and per-owner database counts match.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `auth`         | Signup, login, sessions, API keys (scoped, read-only), logout, revocation, Google OAuth (never into an unverified password account), and who can access which DB.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `orgs`         | Each role does exactly what §9 allows; while roles change, every write's answer is explained by a role held within the cache lag, and owner and deletion races keep an owner and leave no orphaned database.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `accounts`     | Password changes, session revocation, mailed verification and reset links, and account deletion take effect everywhere within the cache lag, and leave the metadata consistent.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `limits`       | Per-key, per-user and per-address rate limits and sign-in backoff: refused requests get 429 with `Retry-After` and are never applied, a limit leaves other keys, users, accounts and addresses alone, and a sign-in works again once its backoff passes.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `lifecycle`    | Create, use, delete, and recreate DBs under crashes. Deleted data is never visible and is eventually purged, and per-owner database counts match.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `console`      | Browser-style console flows: cookies, forms (multipart uploads included), CSRF rejection, HTML escaping of hostile input, file upload and byte-exact download, schema editing, pagination, account, organization and admin pages, password reset and verification through the mailbox.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `api_tour`     | The remaining endpoints (schema drops, aborts, commit status by idempotency key, metadata) with result checks.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `chaos`        | Several workloads at once with crashes, partitions, and store faults.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |

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

  | operation                                     | requests                                |
  | --------------------------------------------- | --------------------------------------- |
  | commit (lease holder)                         | 1 PUT                                   |
  | 64 concurrent commits                         | 1 PUT in total                          |
  | strong read, lease holder                     | 0                                       |
  | strong read, engine on a non-holder           | 1 GET (304)                             |
  | strong read through the API, no lease         | 1 GET                                   |
  | snapshot read within staleness                | 0                                       |
  | read-heavy database (10 strong reads/s)       | 276 PUT + 54 GET per hour               |
  | cold open + point read, 3 segments            | 2 GET (40 ms)                           |
  | cold full scan, 128 values of 16 KiB          | 1 GET                                   |
  | open + full scan after a restart (local tier) | 1 GET (the manifest)                    |
  | blob upload / download                        | 0.25 PUT / 0.125 GET per MiB            |
  | steady writes, with flush/compaction/GC       | 1.001 PUT, 0.001 GET per commit         |
  | idle database                                 | 0 per hour                              |
  | node overhead (heartbeats, membership)        | 31 PUT + up to 140 GET per hour         |
  | stream append                                 | 1 PUT                                   |
  | stream tail read, lease holder                | 0                                       |
  | log ingestion, 20 KB NDJSON appends           | 1 PUT per append + 5 PUT per MiB, 0 GET |
  | cold read of a sealed log                     | 5 GET per MiB                           |

  `cost_model` prices these with S3 Standard list prices for workload
  profiles (idle, light, typical, heavy); the typical profile (100k reads
  and 10k writes a day, 20 MB) must stay under its monthly budget.

Commands:

```

cargo test # everything, a fixed set of seeds
SIM_SEEDS=500 cargo test --release --test sim # longer DST campaigns
cargo run --release --example sim_tool -- campaign all 3000 # very long campaigns
cargo run --release --example sim_tool -- trace <scenario> <seed>
SIM_SEED=12345 cargo test --test sim sim_chaos -- --nocapture # reproduce one seed
cargo run --release --example cost_model # requests per operation, $/DB/month
cargo run --release --example amplification # compaction's write/space/read amplification
cargo bench --bench throughput # wall-clock throughput, thousands of DBs
scripts/mutants.sh # mutation testing (several hours)

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
    (and could not be deleted). A failed repair now answers a retryable 503.
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
- `gen_openapi` regenerates `openapi.json`

---

## 15. Known limitations (v1)

- The manifest is rewritten on every commit, and with it the WAL and L0.
- Commits are remembered for 10 minutes: an idempotent retry or a commit status query must come within that (§6.2).
- TTL expiry is as precise as node store-clock measurements (§4.5), and expired data takes space until a manifest write or compaction reclaims it.
- Change feeds report each key's latest state, not every intermediate value, and a cursor more than a transaction lifetime behind may have to start over from the current state (§6.4).
- Rate limits and sign-in backoff are kept per node (§9): a cluster of N nodes admits up to N times each rate, and a restart forgets them.
- Size quotas are one setting for every database; there are no per-user or per-database overrides.
- Roles are per organization: a user cannot be granted one database of an organization only.
- Mail is sent only through an HTTP webhook (§12).
- Multi-node deployments need an object store with conditional writes (S3, GCS, Azure, R2, MinIO); `fs:` is for one process (§12).
- After a node crash, writes to the databases it leased wait for the lease to expire: at most 11 s for a database whose last renewal was a commit, 31 s for one kept leased by reads alone (§5.7).
- Garbage collection relies on the store's `last_modified` timestamps coming from one coherent clock, as they do on the major providers.
- `CloudStore` (S3/GCS/Azure through `object_store`) is exercised only by compile-time checks; the simulator tests the same semantics through `SimStore`.
- Stream retention runs on the lease holder, so a database with no activity keeps records past their retention until it is used again (§5.11). Trimmed chunks and their quota bytes are reclaimed by sweeps, hours later, as overwritten blobs are.
- Stream filters scan records; there is no index on record contents, so a query finds needles at the rate it can scan (`scan_limit` records a request).

## 16. Future work

- Point-in-time restore (keep old manifests)
- change feeds that keep deletions longer, or every intermediate value
- quotas and rate limits per user or database, shared across nodes
- per-database grants within an organization
- service accounts (API keys that belong to an organization)
- stream consumer groups that share a stream's records among workers, with acknowledgements and redelivery
- per-block summaries of stream records (bloom filters of field values) so that filters skip blocks
- merging small sealed chunks
