# Baserunna Specification

Baserunna is a web service that hosts small databases on object storage.
Each database supports serializable transactions, KV APIs, and tables.
Service metadata is itself stored in a Baserunna database.

Status: v1 specification. Sections marked **(future)** are explicitly out of
scope for v1.

## 1. Goals and non-goals

### Goals (priority order)

1. **Safety.** Acknowledged commits are never lost or reordered; reads never
   observe uncommitted or torn state; transactions are strictly serializable.
   Correctness of committed data depends solely object storage routing and does
   not depend on clocks, leases, routing, or membership.
2. **Low cost.** An idle database pays only storage but not compute. An active
   database pays roughly one object-store PUT per group of commits. Reads on a
   warm node are served from the local cache.
3. **Operational simplicity.** Nodes are stateless and interchangeable. A node
   can be killed at any moment. Any other node can serve reads immediately or
   writes within one lease period.
4. **Testability.** The service, runs and tests fully inside a deterministic
   simulator with fault injection.
5. **Perf, within reason.** Hot reads are served from memory in microseonds.
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
from SlateDB, a LSM engine for object storage. The original goal was to use SlateDB,
but that idea has been shelved for V1.

Concerns:

- **Idle DB overhead.** Each SlateDB database runs its own WAL flusher, memtable flusher, compactor, and GC tasks. Each open DB requires its own timer.
  In Baserunna we want an idle DB to hold zero tasks, and instead use a acached `Arc<Version>` that the LRU can evict.
- **Commit cost.** Each SlateDB commit is a WAL SST PUT + periodic L0 flush + manifest updates. Each Baserunna update requires one conditional PUT per group commit. Small writes are inlined in the manifest ("tail"), so the manifest doubles as the WAL.
- **Failover.** A new SlateDB writer bumps the writer epoch before serving. In Baserunna, any node can read immediately, and a new writer takes over by CAS after the current lease expires.
- **Fencing.** SlateDB stores the writer epoch in the manifest. Baserunna uses Manifest CAS (`If-Match`), and a stale writer fails at its next commit by construction.
- **Deterministic Simulation.** SlateDB's internal tokio tasks and `object_store` crate make whole-system determinism hard to guarantee. Baserunnar routes every source of time, randomness, I/O, and task spawning through an injected `Env`, so te full stack runs deterministically in a test environment.
- **DB Size.** SlateDB is tuned for large datasets. Baserunna is tuned for databases ≤ 100s of MB, where size-tiered compaction is cheap.

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
- workers. No durable local state.
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
  engine/           segments, manifest, versions, reads, commits, compaction, GC, blobs
  txn.rs            transaction ops, read/write sets, validation
  table/            tuple encoding, schema, rows, indexes, queries
  cluster.rs        membership, rendezvous routing, forwarding, leases policy
  meta.rs           system DB schema + repository (users, sessions, keys, dbs, nodes)
  auth.rs           password hashing, sessions, API keys, Google OIDC
  api/              axum routes (JSON API), errors, extractors
  console/          server-rendered admin console
  api/openapi.rs    OpenAPI 3.1 document
  client.rs         small async Rust client
  sim/              deterministic simulation: SimStore, transport, fake Google, workloads, checkers
tests/              integration, property, fuzz and DST suites
examples/           runnable real-world use cases
fuzz/               optional cargo-fuzz targets, share code with tests
```

## 4. Data model

### 4.1 Keys and values

- Keys are byte strings of 1..=4096 bytes, ordered lexicographically by byte.
- Values are byte strings, store in one of three ways:
  - inline, if ≤ size is less than 32 KiB, in the tail or in segments
  - out-of-band blob, if larger, with the value written as chunk objects and the key storing a `BlobRef` descriptor (§4.3)
  - tombstone, for deletes
- Each committed entry carries a `seq`, the DB version that wrote it.
  The `seq` is reported to a client as the key's `version`.

### 4.2 Internal keyspaces

Use a prefix byte to avoid collisions between keyspaces.

| byte   | space                                                                               |
| ------ | ----------------------------------------------------------------------------------- |
| `0x00` | catalog (table schemas, DB-level metadata)                                          |
| `0x01` | user KV                                                                             |
| `0x02` | table rows: `0x02 ++ table_id(u32 BE) ++ tuple(pk)`                                 |
| `0x03` | index entries: `0x03 ++ table_id ++ index_id(u32 BE) ++ tuple(cols) [++ tuple(pk)]` |
| `0x04` | row blob columns: `0x04 ++ table_id ++ tuple(pk) ++ column_id(u32 BE)`              |

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
than `blob_stage_ttl` (1 h).

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
  transactional. Adding an index backfills it in the same transaction, which
  is fine at the target DB sizes; tables above `index_backfill_max_rows`
  (100k) are rejected in v1.
- Queries: lookup by primary key; range or prefix scan by primary key;
  equality on an index prefix with an optional range on the next column;
  optional residual `filter` (and/or/not, eq/ne/lt/le/gt/ge/in/prefix/is_null)
  evaluated server-side; `limit`, `reverse`, and an opaque `cursor`.

## 5. Storage engine

### 5.1 Object layout (per DB)

```
dbs/{db_id}/manifest                 mutable; updated only with If-Match / If-None-Match
dbs/{db_id}/seg/{seg_id}             immutable sorted segment (SST)
dbs/{db_id}/blob/{blob_id}/{chunk}   immutable blob chunks
```

The system DB uses `db_id = "sys"`.

### 5.2 Manifest

The manifest is a binary, CRC32C-checksummed document (`engine/manifest.rs`):

```
magic "BRMF", format u16
db_id
version: u64                                  data version = seq of the last commit
lease: Option<{node_id, expires_at_ms}>
segments: [SegmentMeta]                       newest first; non-overlapping in time, overlapping in key space
tail: [Entry]                                 sorted by key, newest value per key (the inline write log)
tombstone_horizon: u64                        tombstones with seq <= horizon may have been purged
recent_commits: [(commit_id u128, version)]   bounded ring for idempotence / ambiguity resolution
version_times: [(version, time_ms)]           sparse samples (≥30 s apart) for time-based policies
obsolete: [(kind, id, since_ms)]              segments/blobs awaiting deletion after grace
```

A **Version** is an immutable, decoded manifest plus the store's etag. A
reader holding a Version sees a consistent snapshot (§5.4).

### 5.3 Segments

Segments are immutable and built from sorted entries:

```
[data block]* [bloom filter] [index] [footer 64 B]
block   = entries (key, seq, kind, value) + crc32c       target 64 KiB
index   = per block: last_key, offset, len + crc32c
bloom   = 10 bits/key, k = 7, + crc32c
footer  = magic "BRSG", format, offsets/lengths, entry_count, crc32c
```

Segments under `segment_whole_fetch_max` (1 MiB) are fetched in one GET.
Larger ones are read footer-first, then by ranged GET per block. Blocks,
indexes, and blooms are cached in a node-wide LRU with a byte budget
(`cache_bytes`, default 512 MiB). Cache keys are `(db_id, seg_id, block)`.
Segments are immutable, so cache entries never need invalidation.

### 5.4 Reads

- A point lookup checks the tail, then segments newest to oldest (skipping a
  segment if the bloom filter or key bounds rule it out). The first hit wins.
  A tombstone means not found.
- A range scan does a k-way merge of the tail and the segment cursors. The
  newest source wins per key, and tombstones are skipped. Blocks load lazily,
  so `limit` bounds the I/O.
- Snapshot isolation comes for free: a transaction keeps its `Arc<Version>`.
  Objects referenced by any version younger than `txn_max_lifetime` are never
  deleted (§5.8).

### 5.5 Commits (group commit + CAS)

Each open DB has a per-node commit queue. One committer task drains it
(spawned on demand; it exits when the queue is empty):

1. Take the batch of pending transactions. Base = the latest Version known
   to this node.
2. Lease check: if `base.lease` belongs to another node and has not expired
   (with skew margin), fail the batch with `not_owner` (the API layer
   forwards or returns 503).
3. For each transaction in order, validate it (§6.1) against all writes with
   `seq > txn.read_version` in base and in transactions already accepted in
   this batch. Rejected transactions get `409 conflict`. Transactions past
   their **commit deadline** (request start + `commit_timeout_ms`, 10 s) are
   refused without being applied. This bounds how long a "zombie" request
   can linger server-side after its client gave up. Each accepted
   transaction gets **its own sequence number** (`base.version + 1`,
   `+ 2`, …), so every committed state of a key has a unique version, even
   though the whole batch costs one manifest write. `delete_range` expands
   to point tombstones against the merged state at this moment.
4. If the new tail exceeds `tail_max_bytes` (256 KiB), write it to a new
   segment (unconditional PUT to a fresh random id) and clear the tail.
5. Renew the lease (`node_id = self`, `expires_at = now + lease_duration`).
   Append the commit ids to `recent_commits` and maybe sample
   `version_times`.
6. `PUT manifest If-Match: base.etag`:
   - **ok**: install the new Version and acknowledge the accepted
     transactions with `version = base.version + 1`.
   - **precondition failed**: someone else committed. Reload the manifest; if
     another node now holds a valid lease, fail with `not_owner`. Otherwise
     go back to 3. Transactions validated earlier are revalidated against the
     new base.
   - **error/timeout (ambiguous)**: reload the manifest. If our commit ids
     are in `recent_commits`, it succeeded. If not, go back to 3. This is safe
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

### 5.6 Compaction

This is size-tiered compaction tuned for small DBs. Segments are ordered
newest first. Starting from the newest segment, a run is extended while the
next older segment is ≤ `2 ×` the run's accumulated size. A run of ≥ 4
segments is merged. If there are more than 12 segments, the newest segments
are merged until at most 12 remain. Merging a contiguous run preserves
recency order.

Only the lease holder compacts, in a background task. The output segment is
installed with a CAS that replaces exactly the input ids. If the inputs
changed, the output is discarded and becomes an orphan.

When the run includes the oldest segment, tombstones with
`seq ≤ purge_seq` are dropped. `purge_seq` is the newest sampled version at
least `txn_max_lifetime` old. The manifest then records
`tombstone_horizon = max(horizon, max dropped seq)`. A transaction with
`read_version < tombstone_horizon` fails validation conservatively with a
retryable `409 conflict`.

### 5.7 Leases

- The lease lives in the manifest and is written with the same CAS as data.
- The holder treats its lease as valid until
  `expires_at - clock_skew_margin`. Others treat it as valid until
  `expires_at + clock_skew_margin`. Defaults: lease 15 s, margin 1 s.
- Holders renew on every commit. Read-only activity renews with a lease-only
  CAS when at least `lease_read_threshold` (16) strong reads happened in the
  last lease period. At ~$0.005 per 1000 PUTs, a renewal pays for itself
  versus per-read validation GETs once it covers about 12 reads.
- Non-holders never commit while another node's lease is valid, so the
  holder's cached Version is the latest. That is what makes lease reads
  linearizable.
- If a holder dies, its DBs are write-unavailable for at most
  `lease_duration + margin`. Strong reads stay available everywhere through
  validated reads (§5.9).

### 5.8 Garbage collection

- **Obsolete segments**: compaction moves replaced segment ids into
  `obsolete` with `since = now`. Any commit or compaction by the lease holder
  deletes entries older than `gc_grace` (30 min, which must be at least
  `txn_max_lifetime` + margin) and drops them from the list.
- **Orphans**: segments or blobs in storage that are neither referenced nor
  listed. A periodic per-DB sweep (lease holder, at most hourly, only for DBs
  with activity) lists `seg/` and `blob/`. Unreferenced objects whose
  `last_modified` is older than `orphan_grace` (2 h) are moved into
  `obsolete` (not deleted directly). Unreferenced blobs must also be older
  than `blob_stage_ttl` so they can never be bound again.
- **Deleted databases**: deletion marks the DB in the system DB. After
  `db_delete_grace` (10 min, longer than metadata cache TTLs), a reaper on
  the system-DB lease holder deletes every object under `dbs/{id}/`, then
  repeats once more after another grace period before removing the record.

### 5.9 Consistency of reads

| mode               | guarantee                                                            | mechanism                                                                                                                                                       |
| ------------------ | -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `strong` (default) | linearizable                                                         | served from cache if this node holds a valid lease and has no manifest write in flight; otherwise `GET manifest If-None-Match: etag` first (a 304 costs ~1 GET) |
| `snapshot`         | serializable snapshot, at most `max_staleness_ms` old (default 5000) | any cached Version validated within the bound                                                                                                                   |

Transactions always read from a fixed snapshot, and serializable
transactions validate their reads at commit time.

---

## 6. Transactions

### 6.1 Semantics

- Default isolation is serializable, strictly serializable for committed read-write transactions.
  This uses optimistic concurrency control with backward validation:
  - the read set holds point keys (including misses) and scanned intervals.
    A scan that stopped at `limit` records only the interval it actually
    covered;
  - validation fails if any entry, including tombstones, in the current
    state with `seq > read_version` intersects the read set.
- `snapshot` isolation validates only the write set (first-committer-wins).
  Write skew is possible, as documented.
- Read-only transactions always succeed and serialize at `read_version`.
- Blind-write transactions never conflict.
- Reads within a transaction see its own earlier writes.
- Limits: `txn_max_ops` 10,000; `txn_max_write_bytes` 16 MiB, where blobs
  count only their descriptor; `txn_idle_timeout` 30 s;
  `txn_max_lifetime` 5 min.

### 6.2 Forms

1. **One-shot** (`POST /v1/dbs/{db}/txn`): a list of ops executed atomically
   in one request. Supports `check` ops (preconditions) and an optional
   `idempotency_key`. Retrying with the same key never applies twice while
   the key is in `recent_commits` (the last 1024 commits). A retry that finds
   the earlier attempt applied returns `replayed: true` with `results: null`.
   A retried idempotent request that fails a precondition checks the
   commit status (fenced) before answering 412, because an earlier attempt
   may have applied the write that now makes the check fail. Conflicts are
   retried server-side.
2. **Interactive** (`/v1/dbs/{db}/transactions…`): begin, ops, commit,
   abort. State lives on the owner node. If the owner changes or crashes, the
   transaction is lost (`404 txn_not_found`, retryable) and nothing was
   committed. `begin` returns the transaction's `commit_id` up front.

**Resolving ambiguity.** A `503` with `outcome: "unknown"`, or a lost
response, means a commit may or may not have applied. The client waits
until the request's commit deadline has certainly passed (about 15 s), then
calls `GET /commits/{commit_id}` (or `key:<idempotency key>`). That call
first performs a manifest write, fencing any in-flight attempt, and then
answers definitively: `committed` (with its version) or `not_committed`.

### 6.3 Ops

| op             | fields                                                                    | result                                         |
| -------------- | ------------------------------------------------------------------------- | ---------------------------------------------- |
| `get`          | key                                                                       | `{found, value? \| blob?, version?}`           |
| `scan`         | `prefix` or `start`/`end`, `limit` (≤1000), `reverse`, `keys_only`        | `{items, next_cursor?}`                        |
| `put`          | key, `value` or `blob_id`                                                 | `{}`                                           |
| `delete`       | key                                                                       | `{}`                                           |
| `delete_range` | `prefix` or `start`/`end`                                                 | `{}` (expanded at commit)                      |
| `check`        | key + one of `exists`, `version`, `value`                                 | fails txn with 412 if false                    |
| `increment`    | key, `by` (i64)                                                           | `{value}`. Value is ASCII decimal; missing = 0 |
| `get_row`      | table, `pk` (array or scalar)                                             | `{found, row?}`                                |
| `insert_row`   | table, `row`                                                              | `{}`. 409 if pk exists                         |
| `upsert_row`   | table, `row`                                                              | `{}`                                           |
| `update_row`   | table, `pk`, `set`                                                        | `{found}`. Partial update                      |
| `delete_row`   | table, `pk`                                                               | `{found}`                                      |
| `query_rows`   | table, `index?`, `eq?`, `range?`, `filter?`, `limit`, `reverse`, `cursor` | `{rows, next_cursor?}`                         |

Keys and values in JSON are UTF-8 strings by default. With
`"encoding": "base64"` on the request, every key and value in the request and
response is standard base64 instead.

---

## 7. Cluster

- **Membership** is stored in the `nodes` table of the system DB. Each node
  upserts `{id, addr, started_at, heartbeat_at}` every `heartbeat_interval`
  (10 s) and reads the table on the same cadence. A node is live if
  `heartbeat_at > now − 3 × interval`. Rows dead for more than 1 h are
  removed.
- **Routing**: for a request on DB `d`, the target is the lease holder if a
  valid lease is known, otherwise the live node maximizing
  `hash(node_id ‖ d)` (rendezvous hashing). If the target is another node, the
  request is proxied unchanged, streaming the body, with header
  `x-baserunna-hop: 1`. A request carrying that header is never forwarded
  again. If forwarding fails, the receiving node handles the request itself.
  It can serve reads, and it can write only if no other node holds a valid
  lease (otherwise `503 unavailable` with `Retry-After`).
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

| table               | pk           | columns                                                            | indexes                           |
| ------------------- | ------------ | ------------------------------------------------------------------ | --------------------------------- |
| `users`             | `id`         | email, password_hash?, google_sub?, is_admin, disabled, created_at | unique(email), unique(google_sub) |
| `sessions`          | `token_hash` | user_id, created_at, expires_at                                    | (user_id)                         |
| `api_keys`          | `id`         | user_id, name, secret_hash, db_id?, read_only, created_at          | (user_id)                         |
| `databases`         | `id`         | owner_id, name, created_at                                         | unique(owner_id, name)            |
| `deleted_databases` | `id`         | owner_id, name, deleted_at, purge_passes                           |                                   |
| `nodes`             | `id`         | addr, started_at, heartbeat_at                                     |                                   |
| `config`            | `key`        | value                                                              |                                   |

`config` stores generated secrets (`cookie_key`) and the bootstrap flags. A
DB's manifest is created lazily on first access (`If-None-Match: *`), so
creating a DB is a single system-DB transaction.

Auth lookups (session, API key, DB name resolution) are snapshot reads of
`sys` with `max_staleness` of 2 s, cached per node for at most 30 s. A
revocation takes effect at once on the node that performed it and within
30 s everywhere else. A snapshot **miss** (unknown token, database name, or
email) is retried with a strong read before answering. User-facing listings
(databases, API keys, users) always read strongly. Together these ensure
clients always see their own metadata writes, even across nodes.

Name resolution is protected against staleness. If a cached name maps to
a database whose manifest is marked deleted, the name is re-resolved
strongly and the request continues against the new database. If metadata
still maps the name to the deleted database, the deletion was interrupted
halfway, and the node completes it. Creating a database whose name belongs
to such a half-deleted database also completes the deletion first. A manifest is created lazily only after
a strong check confirms the database exists, so a stale cache can never
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
  email, and is created if allowed by the signup policy.
- **Sessions**: a 32-byte random token, `brs_<base64url>`. Only its SHA-256
  is stored. Default TTL is 30 days. The console uses an `HttpOnly`,
  `SameSite=Lax` cookie (`Secure` when `public_url` is https). The API uses
  `Authorization: Bearer`.
- **API keys**: `brk_<id>_<secret>`. Only the secret's SHA-256 is stored. A
  key can be scoped to one DB and/or read-only. It is shown once at creation.
- **Authorization**: users access their own DBs, and admins access all DBs.
  API-key scope restrictions apply on top. Console forms are protected
  against CSRF with a per-session token: HMAC(cookie_key, session token hash).

---

## 10. HTTP API

The API is JSON over HTTP. All routes are under `/v1`. The full schema is in
`openapi.json` (OpenAPI 3.1, generated by `src/api/openapi.rs`), served at `GET /v1/openapi.json`. DB-scoped
routes take `{db}`, the DB _name_ owned by the caller. Admins may use
`id:<db_id>`.

Errors are returned as
`{"error": {"code", "message", "retryable", "details"?}}`:

| status | code(s)                                                                    |
| ------ | -------------------------------------------------------------------------- |
| 400    | `invalid_argument`                                                         |
| 401    | `unauthenticated`                                                          |
| 403    | `permission_denied`                                                        |
| 404    | `not_found`, `txn_not_found`                                               |
| 409    | `conflict` (retryable), `already_exists`                                   |
| 412    | `precondition_failed` (includes the failing op index)                      |
| 413    | `too_large`                                                                |
| 503    | `unavailable` (retryable, `Retry-After`; may include `outcome: "unknown"`) |
| 500    | `internal`                                                                 |

Routes (see the OpenAPI document for bodies):

```
GET    /healthz                                   liveness
GET    /v1/openapi.json
POST   /v1/auth/signup | /v1/auth/login | /v1/auth/logout
GET    /v1/auth/google/start | /v1/auth/google/callback
GET    /v1/me
GET    /v1/api-keys          POST /v1/api-keys     DELETE /v1/api-keys/{id}
GET    /v1/dbs               POST /v1/dbs
GET    /v1/dbs/{db}          DELETE /v1/dbs/{db}
GET    /v1/dbs/{db}/kv                             scan (query params)
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
POST   /v1/dbs/{db}/tables/{t}/rows/update
POST   /v1/dbs/{db}/tables/{t}/rows/delete
POST   /v1/dbs/{db}/tables/{t}/rows/query
GET    /v1/admin/users   GET /v1/admin/nodes   POST /v1/admin/users/{id}/disable
```

Raw KV semantics:

- `GET` returns the value with `ETag: "<version>"` and
  `x-baserunna-version`. Blob values are streamed and support
  `Range: bytes=…` (206). The query parameter `consistency` takes
  `strong` or `snapshot`.
- `PUT` takes the body as the value; bodies above `value_inline_max` become
  blobs automatically. `If-Match: "<v>"` makes the write conditional on the
  key's version. `If-None-Match: *` makes it create-only.
  `Content-Type` is kept for blob values.

---

## 11. Admin console

Server-rendered HTML from the same axum app under `/console`. It uses no
JavaScript framework and no external assets. Pages:

- sign in (password and "Sign in with Google") and sign out;
- databases: list, create, delete; per-DB page with stats, key browser
  (prefix scan, view and delete values, upload), tables (schema, rows with
  pagination), and a transaction runner (JSON);
- API keys: list, create (secret shown once), revoke;
- admin only: users (disable), cluster nodes (liveness, address).

POST forms carry a CSRF token. All output is HTML-escaped.

---

## 12. Configuration (environment)

| var                                      | default                    | meaning                                                      |
| ---------------------------------------- | -------------------------- | ------------------------------------------------------------ |
| `BASERUNNA_LISTEN`                       | `0.0.0.0:8080`             | bind address                                                 |
| `BASERUNNA_ADVERTISE`                    | `http://<listen>`          | address peers use to forward                                 |
| `BASERUNNA_NODE_ID`                      | random                     | stable id optional                                           |
| `BASERUNNA_STORE`                        | `fs:./data`                | `fs:<dir>`, `mem:`, `s3://bucket/prefix`, `gs://…`, `az://…` |
| `BASERUNNA_CLUSTER_SECRET`               | none (required if >1 node) | internal endpoint auth                                       |
| `BASERUNNA_SIGNUP`                       | `first_user_only`          | `open` / `first_user_only` / `closed`                        |
| `BASERUNNA_ADMIN_EMAIL` / `_PASSWORD`    | none                       | bootstrap admin                                              |
| `BASERUNNA_GOOGLE_CLIENT_ID` / `_SECRET` | none                       | enables Google sign-in                                       |
| `BASERUNNA_PUBLIC_URL`                   | advertise                  | OAuth redirect base, cookie `Secure`                         |
| `BASERUNNA_CACHE_MB`                     | 512                        | block cache budget                                           |
| `BASERUNNA_LEASE_MS`                     | 15000                      | lease duration                                               |

`fs:` is for single-node development only. Cross-process CAS on a local
filesystem is not guaranteed.

---

## 13. Deterministic simulation testing (DST)

### 13.1 Determinism

- `Env` abstracts wall time, monotonic time, randomness, and task spawning.
  `SimEnv` runs on a tokio **current-thread** runtime with **paused time**.
  Time advances only when every task is idle, so a seed fully determines task
  interleavings. All randomness comes from a seeded xoshiro256** PRNG.
- Production code may not iterate `std::collections::HashMap` with the
  default random state where order affects behavior, and may not use
  `select!` or `tokio::sync::watch`. The watch channel shards its waiters
  with tokio's thread RNG, so wake order is random; the simulator caught
  this. It uses `BTreeMap` or deterministic hashers instead. `tests/lint.rs`
  enforces these rules, plus "no direct wall-clock time, randomness or
  `tokio::spawn` outside `Env`".
- Each run produces a trace hash over every request, response, and store
  operation. Running the same seed twice must give the same hash; this is a
  test.

### 13.2 Simulated world

- **SimStore**: an in-memory object store with S3 semantics (strong
  read-after-write, conditional PUT/GET, list). Fault injection is per
  operation and seeded: latency distributions; transient errors before
  apply; **ambiguous** failures (applied, then the response is lost);
  delayed application of in-flight PUTs after the caller died (the request
  runs on a "network" task that a node crash does not cancel).
- **SimCluster**: N nodes, each with its own `Node` instance, clock skew
  (bounded by the margin), and task registry. Faults: **crash** (abort every
  node task and in-flight request; memory lost), **restart**, network
  **partition** between nodes (forward failures), and latency spikes.
  Clients call nodes through the full axum router (`tower::ServiceExt`) and
  pick nodes at random, which exercises forwarding.
- **SimOAuth**: a fake Google with an RSA key generated in-process and a
  JWKS, so the full OIDC verification path runs.

### 13.3 Workloads and checkers

| workload    | checks                                                                                                                                                                                                                                    |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `kv_model`  | One client per DB issues random get/put/delete/scan/delete_range/check/increment ops (one-shot and interactive) under faults. Every response is compared with a `BTreeMap` model. Ambiguous commits are resolved through `/commits/{id}`. |
| `bank`      | Concurrent transfers between accounts on several nodes. Total balance is invariant and no balance goes negative, checked with snapshot reads during the run and at the end.                                                               |
| `register`  | Concurrent writers and readers on a few keys. Every strong read that starts after a write was acknowledged must observe a version ≥ that write's version (real-time linearizability). Versions observed by one client are monotonic.      |
| `tables`    | Random schema changes and row ops against a model. At quiescence, every index must exactly match a recomputation from rows, and unique constraints must hold.                                                                             |
| `blobs`     | Uploads with random sizes and chunk boundaries, range reads, overwrites, and deletes. Content SHA-256 is checked. After GC with simulated time past the graces, no referenced object is missing and every unreferenced object is gone.    |
| `auth`      | Signup, login, sessions, API keys (scoped, read-only), logout, revocation, Google OAuth, permission matrix (who can access which DB).                                                                                                     |
| `lifecycle` | Create, use, delete, and recreate DBs under crashes. Deleted data is never visible and is eventually purged.                                                                                                                              |
| `console`   | Browser-style console flows: cookies, forms, CSRF rejection, HTML escaping of hostile input.                                                                                                                                              |
| `api_tour`  | The remaining endpoints (schema drops, aborts, commit status by idempotency key, metadata) with result checks.                                                                                                                            |
| `chaos`     | All of the above mixed together with crashes, partitions, and store faults.                                                                                                                                                               |

Every DST run ends with **storage invariants** at quiescence: each manifest
decodes and every referenced segment and blob exists; the merged contents
match the model; and the trace is deterministic.

### 13.4 Other test layers

- **Unit tests** in each module.
- **Property tests**, using an in-crate generator and shrinker with seeded
  reproduction. Properties: codec round trips; tuple encoding preserves order;
  segment build/read equals the input; an LSM with random
  flush/compaction/purge equals a model; base64/varint/bloom laws.
- **Fuzz tests**: every decoder (manifest, segment, tuple, row, blob ref,
  cursors) gets arbitrary and mutated inputs and must return `Err`, never
  panic. The router gets random JSON bodies and paths and must never answer
  500 or panic. These run in `cargo test` with fixed seeds. The same
  functions are exposed as optional `cargo fuzz` targets in `fuzz/`.
- **OpenAPI conformance**: every response in the DST is validated against
  `openapi.json` with an in-crate JSON-Schema subset checker. Every route in
  the router appears in the document, and every documented route exists.
  The checked-in `openapi.json` must equal the generated document.
- **DST coverage**: `tests/coverage.rs` runs every scenario and fails if any
  documented operation was never exercised by the simulator.

Commands:

```
cargo test                                                   # everything, a fixed set of seeds
SIM_SEEDS=500 cargo test --release --test sim                # longer DST campaigns
cargo run --release --example sim_tool -- campaign all 3000  # very long campaigns
cargo run --release --example sim_tool -- trace <scenario> <seed>
SIM_SEED=12345 cargo test --test sim sim_chaos -- --nocapture # reproduce one seed
```

### 13.5 Defects found by the simulator

These bugs were found by the DST suite during development and fixed. Each
is now covered by a seed in the regular suite:

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

Tools: `sim_tool` runs simulation campaigns and prints traces.
`gen_openapi` regenerates `openapi.json`.

---

## 15. Known limitations (v1)

- Compaction and the orphan sweep materialize the merged entries of a
  database in memory. This is fine for the target sizes (10s of MB), but
  100s-of-MB databases need a streaming merge.
- An index can only be added to tables of up to 100k rows in one
  transaction (backfill is transactional).
- The `fs:` store gives compare-and-swap only within one process.
  Multi-node deployments need an object store with conditional writes
  (S3, GCS, Azure, R2, MinIO).
- After a node crash, writes to the databases it leased wait for the lease
  (15 s) to expire. Reads continue immediately.
- Blob downloads must finish within `gc_grace` (30 min) of the blob being
  dereferenced.
- Login has no rate limiting yet. Deploy behind a proxy that limits it.
- `CloudStore` (S3/GCS/Azure through `object_store`) is exercised only by
  compile-time checks here; the simulator tests the same semantics through
  `SimStore`.

## 16. Future work

Compression (zstd/lz4) of segments; local disk cache tier; online index
backfill for large tables; TTLs; change feeds; point-in-time restore (keep
old manifests); per-user quotas and rate limits; multi-user organizations.
