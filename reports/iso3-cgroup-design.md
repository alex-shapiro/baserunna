# Round 3 Design: Worker Pool Under Cgroups

Each claim has a label: **code** (file:line on `iso3/cgroup` at 8fca510),
**measured** (a named run), or **inferred** (nothing checked it yet).

Revision 2 changes the design after the coordinator's check of
revision 1. Section 6 lists each change against each rejected or
missing item.

## 1. The design

**The main process opens no database and makes no store call.** Workers
do all reads and writes of the store.

- **Owner workers.** One worker process per owner: the `owner_id` of a
  database record, or the user or org of an account route. It runs in
  the cgroup `<root>/<owner>/worker` with `memory.max` = 512 MiB (code,
  `worker.rs:676`). It holds the owner's tenant `Engine`: each `Db`, its
  lease, its committer, and all of its background jobs.
- **The system worker.** One worker process per node, in the cgroup
  `<root>/system/worker`, with its own `memory.max` (planned 256 MiB). It
  holds the `SYS_DB` lease and runs its committer, flush, compaction, GC
  sweep, the reaper scan, session expiry, heartbeat writes, and
  membership reads. It answers point reads for the main process
  (authentication, the database record, the org role) with fixed-size
  answers.
- **The anonymous worker.** One worker process per node in its own
  cgroup (planned 128 MiB). It serves the routes before sign-in: signup,
  login, verify, password reset, OpenAPI, and static console pages.
- **No light list.** `light_request`, `light_txn`, `admit_light`, and the
  per-site MiB charges go away (code, `worker.rs:194`-`:340`).
- **Why the lease moves.** The lease is a field of the manifest and
  names the node ID (code, `engine/db.rs:372`). The manifest holds the
  tail, so it is tenant data. So each worker writes its manifests under
  the node's ID.
- **One incarnation at a time.** The main process starts a new worker for
  a cgroup only after the old one's `cgroup.events` reports
  `populated 0`. The kernel enforces this, so two incarnations never hold
  the same lease.
- **How bytes move.** One multiplexed Unix socket per worker, with
  binary frames. Each request is `Open` (method, URI, headers, principal,
  database record, deadline), body `Chunk`s of at most 64 KiB, then
  `End`. The response comes back the same way. Each direction of each
  request has a credit window of 2 chunks, so the main process holds at
  most 4 × 64 KiB = 256 KiB per request. Each owner has at most 64
  requests in flight. Over that, the main process returns a retryable
  503.
- **`SYS_DB` writes from a tenant worker.** The tenant worker opens
  `SYS_DB` read-only, from store snapshots, inside its own cgroup. It
  runs the meta command logic in its own `Txn` (today in `run_meta`,
  code, `meta/mod.rs:319`, `:1155`). It then sends a commit frame: the
  `CommitRequest` with its read set and write set. This reuses the
  engine's `remote_commit` path (code, `engine/db.rs:846`). The main
  process checks the frame length against a fixed maximum (planned
  1 MiB) before it reads any byte of the content. It charges the length
  to the owner's in-flight limit, and it moves the frame to the system
  worker. The system worker runs the conflict check on the read set and
  commits. The main process never parses the frame.
- **Peers.** A commit frame for a `SYS_DB` lease on a peer goes to the
  peer's main process as `POST /internal/v1/sys/commit`, with the same
  length check, and on to the peer's system worker. `/internal/v1/meta`
  goes away. A tenant bind for a database on a peer goes to
  `POST /internal/v1/dbs/{db}/events/bind`, and the peer's main process
  routes it to the owner's worker by the path. A notice goes to
  `POST /internal/v1/dbs/{db}/notice` the same way. So the main process
  routes internal requests by path and never parses their bodies.
- **Forward dedup moves into the worker.** Today the receiving main
  process keeps each forwarded response whole for 60 s (code,
  `api/mod.rs:793`; `node.rs:367`) in up to 100,000 slots (code,
  `cluster.rs:160`). In the new design, the owner's worker keeps the
  dedup slots, inside its cgroup. The main process passes
  `x-baserunna-forward-id` through.
- **Idle workers.** The main process stops an owner worker after 20 s
  with no request (code, `worker.rs:646`). Before a planned stop, it
  sends `Stop`. The worker ends its flushes, releases its leases, and
  exits. A kill is also safe. The system and anonymous workers stay up.
- **The simulator.** The simulator runs each worker as a separate
  in-process `Node` and `Engine` with its own store handle, joined by an
  in-memory duplex with the same frames. So the simulator tests the
  protocol, the lease handoff between incarnations, and the guard. It has
  no kernel limit.

## 2. The main-process list

Each line: what it does | allowed item | code path (today, or planned).
**[changed]** marks a line that revision 2 changed or added.

1. **[changed, revision 3]** Accept TCP connections up to a fixed limit for the node and a fixed limit for each client address; refuse the rest at accept | 1 | `main.rs:278` `axum::serve` (today: no limit; planned: a counting accept loop in `main.rs` with `NODE_CONNECTIONS_MAX` and `CLIENT_CONNECTIONS_MAX`).
2. **[changed, revision 3]** Read HTTP headers with hyper's limits set in the code: HTTP/1 `max_buf_size`, header count, and header size. HTTP/2 is off (code, `Cargo.toml`: axum feature `http1` only) | 1 | planned `main.rs` `hyper_util::server::conn::auto::Builder` with explicit limits, and a test that each limit holds.
3. **[changed]** Authenticate the request: look up the token or cookie in `auth_cache`; on a miss, send one fixed-size point-read frame to the system worker | 1 | `node.rs` `authenticate_metered` (today `node.rs:1019`; planned: the miss goes to the system worker).
4. **[changed]** Look up the database record and its `owner_id`, and the org role, the same way: cache, then one point-read frame | 1 | `node.rs` `resolve_db` (today; planned: the miss goes to the system worker).
5. **[changed]** Route by the path only: a database path goes to the worker of the record's `owner_id` (for an org's database, the org's worker). This includes `/v1/dbs/{db}/...` and `/console/dbs/{db}/...` (KV, KV download and upload, blobs, transactions, tables, event queries, stream records). An org path goes to the org's worker. Other signed-in paths go to the user's worker. Paths before sign-in go to the anonymous worker. A database with its lease on a peer goes to the peer | 1 | planned `api/mod.rs` `router` with one fallback handler `proxy_to_worker`.
6. Rate-limit auth attempts and requests per client and account | 1 | `node.rs` `limiter.take`, `limit_client` (today).
7. Move request body bytes to a worker in 64 KiB chunks with a 2-chunk window | 2 | planned `worker.rs` `RequestStream` (replaces `worker.rs:487`).
8. Move response body bytes to the client the same way | 2 | planned `worker.rs` `ResponseStream` (replaces `worker.rs:503`).
9. **[changed]** Forward a request to a peer: stream the body in 64 KiB chunks; keep at most one 64 KiB copy of a body for a local retry after a stale target; stream the response back | 2 | `api/mod.rs` `db_routing` (today `api/mod.rs:497` keeps up to 1 MiB, `BUFFER_FORWARD_MAX` at `api/mod.rs:426`; planned: 64 KiB).
10. Hold each owner's in-flight semaphore of 64 requests | 2 | planned `WorkerPool::admit` (replaces `admit_light`).
11. **[changed]** Move a `SYS_DB` commit frame from a tenant worker, or from `POST /internal/v1/sys/commit`, to the system worker: check the length before any read of the content, charge the length to the owner | 2 and 3 | planned `worker.rs` frame relay (replaces `run_meta` in the main process, and `/internal/v1/meta` at `api/mod.rs:1195`).
12. **[changed]** Route `POST /internal/v1/dbs/{db}/events/bind` and `POST /internal/v1/dbs/{db}/notice` by the path to the owner's worker | 1 | planned `proxy_to_worker` (replaces `api/events.rs:451` `internal_bind` and `api/mod.rs:1196` `commit_notice`).
13. Start, stop, and kill workers; wait for `populated 0`; read `memory.events` for OOM counts | 4 | `worker.rs` `start_worker`, `stop_idle_workers`, `oom_kills` (today).
14. **[changed]** Heartbeat timer: send one fixed-size frame to the system worker, which writes the node record | 4 | `node.rs` `spawn_background` (today `node.rs:528`; planned: the write moves).
15. **[changed]** Membership timer: send one fixed-size frame to the system worker; keep the member list it returns (its size is the cluster size) | 4 | `node.rs` `refresh_members` (today `node.rs:543`; planned: the read moves).
16. Send a fixed-size version notice to a database's home node, when a worker asks | 4 | `node.rs` `notify_home` (today `node.rs:410`).
17. **[changed]** Prune the auth, name, org, and role caches and the limiter | 4 | `node.rs` `housekeeping` (today `node.rs:1581`); see the sizes below.
18. **[changed]** Reaper timer: send one fixed-size frame to the system worker | 4 | `node.rs:561` (today it runs `reap_deleted` in process, `node.rs:1595`).
19. Event-loop lag metric | 4 | `main.rs:245` (today).
20. Log writer thread | 4 | `log.rs:74` (today).
21. Health endpoint: fixed text | 4 | `api/mod.rs` `healthz` (today).
22. Drain and shutdown: stop new requests, send `Stop` to each worker, wait for each `populated 0` | 4 | `main.rs` shutdown, `node.rs` `shutdown` (today; planned: workers release their own leases).

**Status after round 7 (commit b2a4f69).** The node process opens no
tenant database. With workers, it sends each metadata command and each
reaper pass to the system worker (cgroup `<root>/system`), which holds the
`SYS_DB` lease and its upkeep. These parts of the requirement are unmet:

1. Account, org, and token routes, the database list and create, and
   the console pages outside `/console/dbs/{db}` run in the node process.
   The anonymous, user, and org workers are not built.
2. The node process bootstraps `SYS_DB` and holds its lease at start,
   before the pool exists (code, `node.rs` `Node::start`).
3. Owner workers run their metadata commands on `SYS_DB` themselves,
   under the node's ID, instead of through the system worker.
4. The node process parses `/internal/v1/meta` bodies from peers before
   it sends the command to the system worker, and parses
   `/internal/v1/notice` bodies. Notices do not reach workers.
5. The reaper's purge runs in the system worker, not in the owner's
   worker.
6. Forward dedup slots keep whole responses in the node process (code,
   `api/mod.rs` `dedup_forwarded`). Peer routing uses rendezvous only.
7. No guard and no route tests.
8. No connection limits and no explicit hyper limits.
9. The system worker has the owner memory limit, not its own limit.

**Cgroup layout (changed in round 5).** Each worker runs in its owner's
cgroup itself, `<root>/owner-<id>`, one level below the node's cgroup.
The harness teardown waits only for the node's direct child cgroups to
empty, so a deeper leaf left cgroups that blocked the next run
(measured, round 4 and the first round 5 run).

**Removed from revision 1:** `run_meta` in the main process (old item
11); `SYS_DB` flush, compaction, and sweep in the main process (old item
12); the reaper scan and session expiry in the main process (old items
20 and 21); idle eviction of `SYS_DB` in the main process (old item 19,
because the main process has no `Db` now); the store clock refresh (in
old item 18; the system worker sends the store time with each answer).

**Cache and limiter sizes (item 17).**

| Cache | Entry count | Largest entry | Code |
| --- | --- | --- | --- |
| `auth_cache` (token hash → `Principal`) | 4,096 | 1 KiB | `node.rs:330`, `:360`; `util/ttl.rs` |
| `name_cache` (owner, name → `DbRecord`) | 4,096 | 1 KiB | `node.rs:361` |
| `org_cache` (id → `Org`) | 4,096 | 1 KiB | `node.rs:362` |
| `role_cache` (org, user → `Role`) | 4,096 | 1 KiB | `node.rs:363` |
| limiter buckets and failures | 16,384 keys | planned 512 B key (not built) | `limits.rs:138` |

- **The entry count is fixed** at 4,096 per cache (code, `node.rs:330`;
  `util/ttl.rs` `store`). A miss stores no entry: `store` with `None`
  removes the key (code, `util/ttl.rs` `store`).
- **Today the entry size is fixed only in part.** An email is at most
  254 bytes (code, `auth.rs:45`). A database name is at most 63 bytes
  (code, `meta/mod.rs:194`). An org name is at most 39 bytes (code,
  `meta/org.rs:154`). `User.google_sub` and `User.password_hash` have no
  length check in our code (inferred: a search found none).
- **Built in round 4.** `TtlCache::store` refuses an entry whose JSON
  key plus value is over 1 KiB (code, `util/ttl.rs` `TTL_ENTRY_BYTES_MAX`). Such a request reads from the system worker
  each time and is not cached. The count drops to 4,096. So the four
  caches hold at most 4 × 4,096 × 1 KiB = 16 MiB (inferred bound from
  the planned limits). The limiter holds at most 16,384 × 512 B = 8 MiB.

**Background jobs the node runs today, and where each runs after the change:**

| Job | Code (today) | Runs in |
| --- | --- | --- |
| Commit batching (`committer`) | `engine/db.rs:1010` | owner worker; system worker for `SYS_DB` |
| Tail flush | `engine/db.rs:2040` | owner worker; system worker for `SYS_DB` |
| Parked flush | `engine/db.rs:2114` | owner worker; system worker for `SYS_DB` |
| LSM compaction | `engine/db.rs:2007` | owner worker; system worker for `SYS_DB` |
| GC delete of obsolete objects | `engine/db.rs:1986` | owner worker; system worker for `SYS_DB` |
| GC sweep | `engine/db.rs:2296` (`maybe_sweep`) | owner worker; system worker for `SYS_DB` |
| Stream retention trims | `engine/db.rs:2133` | owner worker |
| Reload on notice (`load_noticed`) | `engine/db.rs:973` | owner worker |
| Segment and levels uploads | `engine/version.rs:207`, `:230`, `:250` | owner worker; system worker for `SYS_DB` |
| Event seals | `events/maint.rs:73` | owner worker |
| Event part merges | `events/maint.rs:906`, `:1046` | owner worker |
| Event bulk flush | `events/bulk.rs:102`, `:131` | owner worker |
| Stream follow (SSE) | `api/streams.rs:375` | owner worker |
| Interactive transaction expiry | `node.rs:1584` `txns.expire` | owner worker |
| Idle database eviction | `engine/mod.rs:433` | owner worker; system worker for `SYS_DB` |
| Forward dedup expiry | `node.rs:1589` | owner worker |
| Reaper scan of deleted databases | `node.rs:1607` | system worker |
| Database purge | `engine/mod.rs:446` | owner worker, on a frame from the system worker |
| Session expiry | `node.rs:1616` | system worker |
| Heartbeat write | `node.rs:528` | system worker (main keeps the timer) |
| Membership read | `node.rs:543` | system worker (main keeps the timer) |
| Version notices to peers | `node.rs:427` | main |
| Cache and limiter prunes | `node.rs:553` | main |
| Idle worker stop | `main.rs:236` | main |
| Lag metric | `main.rs:245` | main |
| Mail send retries | `node.rs:1307` | the worker that served the route |

**Items I am unsure about:**

- **The system worker is shared.** One tenant that floods `SYS_DB`
  commits can slow authentication for other tenants on that node. Its
  memory has a kernel bound, so it cannot kill the node. Each owner's
  commit frames count against its 64 in-flight requests, and the
  limiter caps account actions. I have not measured this.
- **Health endpoint** (item 21) answers in the main process with fixed
  text.
- **Hyper's own buffers** on the peer client and server (item 9). I
  infer they are of fixed size per connection; I have not checked the
  hyper settings.

## 3. The guard and the two tests

**The main process has no path to tenant data, and two layers enforce
it.**

1. **No `Db` in the main process.** `Shared` gets a `ProcessRole`:
   `Main`, `System`, `Anonymous`, or `Owner { owner }`. In `Main`,
   `Engine::db(id)` fails for every `id`, `SYS_DB` included. Every `Txn`,
   `Version`, segment read, blob read, event part read, and stream read
   starts from a `Db`, so none of them can exist in the main process.
   Each of these six constructors also checks the role, so the checker
   can see the guard at each named path: `Txn::new`, `Version::new`,
   segment open, `BlobRef` read, part read, and stream read.
2. **No store in the main process.** The main process's store handle is
   `DeniedStore`. It fails every `get`, `head`, `list`, `put`, and
   `delete`, and records `Probe::MainStoreAccess`. Every read path,
   cache fill, and background job ends in the store, so this layer
   catches any path that layer 1 misses.

The coordinator allows `SYS_DB` point reads in the main process for
authentication and the database lookup. This design does not use that
permission: those reads happen in the system worker. So the guard has no
exception.

In a worker, `GuardedStore` fails a key of a database whose record names
another owner, and a tenant key in the system worker.

In tests and the simulator, a guard hit panics. In production, it returns
an internal error and counts a metric.

**Background jobs.** All storage background jobs start from a `Db`, so
layer 1 stops them in the main process. Layer 2 catches any job that
reaches the store another way. The main process's own timers (items 14,
15, 18) send frames and touch no store.

**Test 1: every route runs with the guard on.** The route table becomes
constants (`DB_ROUTES`, `ACCOUNT_ROUTES`, `ORG_ROUTES`, `CONSOLE_ROUTES`,
`INTERNAL_ROUTES`, `ANON_ROUTES`), and the worker router is built from
them. A test checks that the OpenAPI spec and the console route list
name the same set as the tables, so no route escapes them. For each
entry, a fixture sends a valid request through a real main process and
real workers (in-process duplex), with the guard in panic mode. The test
fails on any guard hit or any 5xx.

**Test 2: every route goes through the boundary.** The main router has
the auth middleware, `healthz`, and one fallback, `proxy_to_worker`. The
test runs every table entry and asserts that each request produced
exactly one `Open` frame to the expected worker: owner, org, user,
system, or anonymous. A second check cuts the worker side and sends each
table path. Each must fail with the "worker unavailable" error, not with
a handler's answer. So no table path has a handler in the main process.

## 4. Sources of extra cost

1. **The crossing.** An empty Unix-socket round trip cost 42% of KV
   throughput (measured, `BENCH_BOUNDARY` on `iso/floor` at e7085b8).
   Every request now crosses. To keep it small:
   - binary frame headers instead of `serde_json` (code, `worker.rs:614`
     encodes every header as JSON);
   - one writer task per direction that writes all queued frames in one
     `write` call, so one syscall carries many requests under load;
   - a body that fits in the `Open` frame (most KV requests) needs no
     extra frame;
   - a worker runtime with one thread per core;
   - if the cost stays above about 10%, a shared-memory ring with an
     eventfd replaces the socket (round 6).
2. **Auth cache misses** now cost a crossing to the system worker. The
   cache has a 30 s TTL (code, `node.rs:101`), so a steady client pays it
   once per 30 s.
3. **Copies.** Each body is copied once more: socket to worker. Chunks
   are `Bytes`, so the main process does not copy them again.
4. **Idle workers.** Owner workers stop after 20 s idle, so 1,000 idle
   owners have no processes. A cold owner pays a process start, a lease
   acquire, and a manifest load on its first request. The system and
   anonymous workers are a fixed cost per node.
5. **Worker memory.** Each running worker has its own runtime, caches,
   and allocator. The worker block cache counts against its bound. I set
   a smaller cache per worker (a limit, which rule 3.5 allows). This
   also helps the "top users" query that hit the 512 MiB limit in 19 of
   40 runs (measured, round 2 verdict eaa6b97).
6. **Victim latency, about +33% with no attacker** (measured, round 2).
   Suspects, to test one at a time:
   `GLIBC_TUNABLES` `mmap_threshold=131072` (code, `worker.rs:686`), the
   JSON frame headers, and the cold worker start inside phase A.
7. **Lease handoff on restart.** A new worker incarnation takes the
   lease again: about one store round trip per database (inferred).
8. **Peer forwards.** A retry buffer of 64 KiB instead of 1 MiB means a
   body between 64 KiB and 1 MiB gets a retryable 503 after a stale
   target, where today the node runs it locally (code,
   `api/mod.rs:512`).

## 5. Plan for rounds 2 to 8

1. **Round 2 (done in round 3, commits 0f57ebb and a36c16b): boundary and streaming.** Binary frames; streamed request
   and response bodies with credit windows; per-owner in-flight
   semaphore; delete the light list and the per-site charges.
2. **Round 3: the main router and the system worker.** Main router
   reduced to `proxy_to_worker`; the system worker with the `SYS_DB`
   lease, point-read frames, heartbeat, membership, reaper, and session
   expiry; the anonymous worker; path-routed internal routes; bounded
   caches and limiter; 64 KiB forward retry buffer.
3. **Round 4: the owner worker owns the database.** Tenant `Db`, lease,
   committer, flush, compaction, sweep, trims, and event maintenance in
   the worker; meta command logic in the worker's `Txn` with commit
   frames; forward dedup in the worker; purge frames. Remove the old job
   kinds (`Query`, `ReadTxn`, `TableQuery`, `BlobValue`, `Merge`,
   `Seal`, `BulkInsert`) and the bind frame.
4. **Round 5: the guard and the two route tests.** `ProcessRole`,
   `DeniedStore`, `GuardedStore`, constructor checks, route tables, both
   tests. Run `cargo test -p baserunna --lib`.
5. **Round 6: the simulator and cost.** In-process workers over a
   duplex, worker kills as simulator faults, lease handoff. Run
   `cargo test -p baserunna --test sim`. Measure the crossing.
6. **Round 7: all 8 workloads, small and attack, two runs each,** in the
   VM. Fix failures without changes to the exercised algorithms.
7. **Round 8: margin, costs, and review.** Re-run failing cells, check
   the 64 MiB margin, review the diff against rule 3.5.

**Fit.** The plan does not fit well in 8 rounds. The system worker adds
about one round. Rounds 3 and 4 move every handler and every background
job, and the simulator and cost work share round 6. I drop no work. If
a round slips, the order stays the same, and the guard (round 5) lists
by its panics what remains.

## 6. Changes in revision 2

| Coordinator item | Change |
| --- | --- |
| 1. `run_meta` in the main process | The tenant worker runs the meta logic in its own `Txn` and sends a commit frame. The main process checks its length and moves it. The system worker checks conflicts and commits. `/internal/v1/meta` becomes `/internal/v1/sys/commit`. List items 11 and 12. |
| 2. `SYS_DB` upkeep in the main process | The system worker holds the `SYS_DB` lease and runs all its upkeep. The main process opens no `Db` at all. |
| 3. Reaper and session expiry | Both run in the system worker. The main process keeps only the timer (item 18). |
| 4. Peer forwarding | Today the sender keeps up to 1 MiB per request (code, `api/mod.rs:426`, `:497`). The receiver keeps each forwarded response whole for 60 s in up to 100,000 slots (code, `api/mod.rs:793`, `cluster.rs:160`, `node.rs:367`). `forward_internal` and `forward_meta` collect whole responses (code, `node.rs:947`, `:983`). Fix: 64 KiB retry buffer; dedup in the owner worker; the internal forwards move to workers. Item 9. |
| 5. `/internal/v1/events/bind` | Path-routed to the owner's worker. Item 12. |
| 6. Console routes for a database | Routed to the worker of the record's `owner_id`, which is the org for an org's database. Item 5. |
| Item 18 condition | Sizes above. Today only the count is fixed (100,000); the planned limits fix both. |
