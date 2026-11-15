# Round 3 Design: Worker Pool Under Cgroups

Each claim has a label: **code** (file:line on `iso3/cgroup` at 8fca510),
**measured** (a named run), or **inferred** (nothing checked it yet).

## 1. The design

**The owner's worker owns the owner's databases.** The main process never
opens a tenant database. Only the worker does.

- **Unit.** One worker process per owner (`owner_id` of the database
  record). It runs in the cgroup `<root>/<owner>/worker` with
  `memory.max` = 512 MiB. This part is the same as round 2 (code,
  `worker.rs:676`).
- **No light list.** `light_request`, `light_txn`, `admit_light`, and the
  per-site MiB charges go away (code, `worker.rs:194`-`:340`). Every
  request that names a database goes to the owner's worker.
- **The worker holds the tenant `Engine`.** Today the lease lives in the
  manifest and names the node ID (code, `engine/db.rs:372`). The manifest
  holds the tail, so the manifest is tenant data. So the worker, not the
  main process, takes the lease, runs the committer, and writes the
  manifest. It writes under the node's ID. The main process keeps only
  `SYS_DB`.
- **One incarnation at a time.** The main process starts a new worker for
  an owner only after the old one's cgroup reports `populated 0` in
  `cgroup.events`. The kernel enforces this, so two workers of one owner
  never hold the same lease. A killed worker is the same case as a killed
  node, which the system already survives (inferred from the
  "stateless nodes" property in `AGENTS.md`; the simulator must prove it,
  see round 5).
- **How bytes move.** One Unix socket pair per worker carries many
  requests. Each request is a stream of frames: `Open` (method, URI,
  headers, principal, database record, deadline), body `Chunk`s, `End`.
  The response comes back the same way. Each chunk is at most 64 KiB.
  Each direction of each request has a credit window of 2 chunks. So the
  main process holds at most 4 × 64 KiB = 256 KiB per request. Each owner
  has at most 64 requests in flight (a semaphore in the main process).
  Over that, the main process returns a retryable 503.
- **How commits come back.** Tenant commits and binds do not come back.
  The worker applies them itself, because it holds the lease. Only
  `SYS_DB` writes come back: a worker that serves an account or database
  route sends a `MetaCmd` frame. The main process checks the frame length
  against a fixed maximum before it parses the frame, charges it to the
  owner's in-flight limit, and runs `run_meta`. This is allowed item 3.
- **Background jobs.** All tenant background jobs start from a tenant
  `Db` (code, `engine/db.rs:1996`-`:2023`, `:2114`, `:2133`;
  `events/bulk.rs:102`, `:131`; `events/maint.rs:1046`). The tenant `Db`
  exists only in the worker, so these jobs run only in the worker, under
  the owner's limit. The purge of a deleted database also goes to the
  owner's worker.
- **Routes without a database.** Account, org, and console routes also
  run in a worker: the worker of the signed-in principal's owner ID (the
  user, or the org for org routes). Routes before sign-in (signup, login,
  verify, password reset, OpenAPI, static console pages) run in one
  shared "anonymous" worker in its own cgroup with a 128 MiB limit.
  Workers read `SYS_DB` from snapshots and send writes as `MetaCmd`.
- **Idle workers.** The main process stops a worker after 20 s with no
  request (code, `worker.rs:646`). Before a planned stop, it sends `Stop`.
  The worker ends its flushes and releases its leases, then exits. A kill
  is also safe.
- **The simulator.** The simulator runs each worker as a separate
  in-process `Node` and `Engine` with its own store handle, joined by an
  in-memory duplex with the same frames. So the simulator tests the
  protocol, the lease handoff between incarnations, and the guard. It has
  no kernel limit.

## 2. The main-process list

Each line: what it does | allowed item | code path (today, or planned).

1. Accept TCP connections, set `TCP_NODELAY` | 1 | `main.rs` `run`, `axum::serve` (today).
2. Read HTTP headers under hyper's fixed header limits | 1 | `main.rs` `axum::serve` (today; planned: set explicit `max_headers` and header buffer size).
3. Authenticate the request: token or session cookie, through `auth_cache` | 1 | `api/mod.rs` auth middleware, `node.rs` principal lookup (today).
4. Look up the database record and its `owner_id` in `SYS_DB` | 1 | `node.rs` db record lookup, `WorkerPool::owner_of` (today, `worker.rs:355`).
5. Route: pick the owner (database owner, principal owner, or anonymous) from the path; forward to the lease-holder node if the database lives on a peer | 1 | planned `api/mod.rs` `router` reduced to one fallback handler `proxy_to_worker`.
6. Per-client and per-account rate limits on auth attempts | 1 | `node.rs` `limiter.take`, `limit_client` (today).
7. Move request body bytes to the worker in 64 KiB chunks with a 2-chunk credit window | 2 | planned `worker.rs` `RequestStream` (replaces `worker.rs:487`).
8. Move response body bytes to the client the same way | 2 | planned `worker.rs` `ResponseStream` (replaces `worker.rs:503`).
9. Proxy a request to a peer node with fixed-size buffers | 2 | `cluster.rs` forward path (today; planned: check and bound its buffers).
10. Hold each owner's in-flight semaphore (64 requests) | 2 | planned `WorkerPool::admit` (replaces `admit_light`).
11. Apply `MetaCmd` frames from workers to `SYS_DB` after a length check, charged to the owner | 3 | planned `worker.rs` frame reader calling `node.rs` `run_meta`.
12. Hold the `SYS_DB` lease and run its committer, flush, compaction, and sweep | 3 and 4 | `engine/db.rs` jobs on `SYS_DB` only (today).
13. Start, stop, and kill workers; wait for `populated 0`; read `memory.events` for OOM counts | 4 | `worker.rs` `start_worker`, `stop_idle_workers`, `oom_kills` (today).
14. Heartbeat to the cluster | 4 | `node.rs` `spawn_background`, `heartbeat` (today).
15. Refresh cluster membership | 4 | `node.rs` `refresh_members` (today).
16. Send version notices to a database's home node: one fixed-size message | 4 | `node.rs` `notify_home` (today; planned: the worker asks the main process to send it).
17. Receive `/internal/v1/notice` and pass the version number to the owner's worker | 1 and 4 | planned `proxy_to_worker`.
18. Housekeeping: prune auth, name, org, and role caches; expire forward dedup entries; prune the limiter; refresh the store clock | 4 | `node.rs` `housekeeping` (today, `node.rs:1581`), minus the tenant parts below.
19. Evict idle databases from the main `Engine`: `SYS_DB` only | 4 | `engine/mod.rs` `evict_idle` (today).
20. Reaper: list deleted databases and expired sessions in `SYS_DB`; send each purge to the owner's worker; record the purge pass | 4 | `node.rs` `reap_deleted` (today `node.rs:1595`; planned: `engine.purge` moves to the worker).
21. Expire sessions in `SYS_DB` | 4 | `node.rs` `reap_deleted` `MetaCmd::ExpireSessions` (today).
22. Event-loop lag metric | 4 | `main.rs` lag loop (today).
23. Log writer thread | 4 | `log.rs:74` (today).
24. Health endpoint | 4 | planned `proxy_to_worker` exception; returns fixed text.
25. Drain and shutdown: stop new requests, send `Stop` to each worker, wait for exits | 4 | `main.rs` shutdown, `node.rs` `shutdown` (today; planned: workers release their own leases).

**Background jobs the node runs today, and where each runs after the change:**

| Job | Code (today) | Runs in |
| --- | --- | --- |
| Commit batching (`committer`) | `engine/db.rs:1010` | worker (tenant); main (`SYS_DB`) |
| Tail flush | `engine/db.rs:2040` | worker; main for `SYS_DB` |
| Parked flush | `engine/db.rs:2114` | worker; main for `SYS_DB` |
| LSM compaction | `engine/db.rs:2007` | worker; main for `SYS_DB` |
| GC delete of obsolete objects | `engine/db.rs:1986` | worker; main for `SYS_DB` |
| GC sweep | `engine/db.rs:2296` (`maybe_sweep`) | worker; main for `SYS_DB` |
| Stream retention trims | `engine/db.rs:2133` | worker |
| Reload on notice (`load_noticed`) | `engine/db.rs:973` | worker |
| Segment and levels uploads | `engine/version.rs:207`, `:230`, `:250` | worker; main for `SYS_DB` |
| Event seals | `events/maint.rs:73` | worker (no job hop) |
| Event part merges | `events/maint.rs:906`, `:1046` | worker (no job hop) |
| Event bulk flush | `events/bulk.rs:102`, `:131` | worker |
| Stream follow (SSE) | `api/streams.rs:375` | worker |
| Interactive transaction expiry | `node.rs` `txns.expire` | worker |
| Database purge | `engine/mod.rs:446` | worker of the owner |
| Heartbeat, membership, notices | `node.rs:528`-`:561`, `:427` | main |
| Housekeeping (caches, limiter) | `node.rs:553` | main |
| Reaper scan of `SYS_DB` | `node.rs:561` | main |
| Idle worker stop | `main.rs:236` | main |
| Lag metric | `main.rs:245` | main |
| Mail send retries | `node.rs:1307` | worker that served the route |

**Items I am unsure about:**

- **`SYS_DB` as tenant data.** Item 4 reads one record, which the rules
  allow. Routes that list an owner's databases, tokens, or members scan
  `SYS_DB`. I put those in the worker. The `MetaCmd` apply (item 11) does
  conflict checks on `SYS_DB`, whose size grows with the number of
  records. I count `SYS_DB` as system data, not tenant data.
- **Lease placement.** Rule item 3 lets the main process hold leases. I
  move tenant leases into the worker, because the lease is a field of the
  tenant manifest. The main process does less than item 3 allows.
- **Health endpoint** (item 24) answers in the main process.
- **Peer forwarding** (item 9). I have not read the forward path yet.
  If it buffers whole bodies, round 2 changes it to the same chunked
  frames.

## 3. The guard and the two tests

**Two layers make a main-process read of tenant data impossible.**

1. **No tenant `Db` in the main process.** `Shared` gets a
   `ProcessRole`: `Main`, or `Worker { owner }`. In `Main`,
   `Engine::db(id)` fails for every `id` other than `SYS_DB`. Every
   `Txn`, `Version`, segment read, blob read, event part read, and stream
   read starts from a `Db`. So with no `Db`, none of these can exist.
   Each of those six constructors also checks the role and the database
   ID, so the checker can see the guard at each named path:
   `Txn::new`, `Version::new`, segment open, `BlobRef` read, part read,
   stream read.
2. **A store guard below everything.** The main process's store handle
   is a `GuardedStore`. It fails every `get`, `head`, `list`, `put`, and
   `delete` whose key is under a tenant database prefix, and it records
   `Probe::MainTenantStoreAccess`. Every read path, cache fill, and
   background job ends in the store, so this layer catches any path that
   layer 1 misses. In a worker, the same guard fails a key of a database
   whose record names another owner.

In tests and the simulator, a guard hit panics. In production, it returns
an internal error and counts a metric.

**Background jobs.** Tenant background jobs start only from a tenant
`Db`, so layer 1 stops them in the main process. Layer 2 catches any job
that reaches the store another way. The reaper's purge goes to a worker,
so the main process never lists tenant keys.

**Test 1: every route runs with the guard on.** The route table becomes
one constant, `DB_ROUTES` (and `ACCOUNT_ROUTES`, `CONSOLE_ROUTES`), and
the worker router is built from it. A test builds the router, then
checks that the OpenAPI spec and the console route list name the same
set (so no route escapes the table). For each entry, a fixture sends a
valid request through a real main process and a real worker (in-process
duplex), with the guard in panic mode. The test fails on any guard hit
or on a 5xx.

**Test 2: every route goes through the boundary.** The main router has
auth middleware and one fallback handler, `proxy_to_worker`, and no
other handler except health. The test runs every entry of the route
table and asserts that each request produced exactly one `Open` frame to
a worker (a counter per frame). A second check asserts that the main
router has no route besides health, by a request to each table path on a
main router whose worker side is cut: each must fail with the worker's
"unavailable" error, not a handler's answer.

## 4. Sources of extra cost

1. **The crossing.** An empty Unix-socket round trip cost 42% of KV
   throughput (measured, `BENCH_BOUNDARY` on `iso/floor` at e7085b8).
   Every KV request now crosses. To keep it small:
   - binary frame headers instead of `serde_json` (code, `worker.rs:614`
     encodes every header as JSON);
   - one writer task per direction that writes all queued frames in one
     `write` call, so one syscall carries many requests under load;
   - a body that fits in the `Open` frame (most KV requests) needs no
     extra frame;
   - a worker runtime with one thread per core, so one owner's 256
     clients are not serial.
   - If the cost stays above about 10%, a shared-memory ring with an
     eventfd replaces the socket (round 4).
2. **Copies.** Each body is copied once more: socket to worker. Chunks
   are `Bytes`, so the main process does not copy them again.
3. **Idle workers.** Workers stop after 20 s idle, so 1,000 idle owners
   have no processes. A cold owner pays a process start, a lease
   acquire, and a manifest load on its first request.
4. **Worker memory.** Each running worker has its own runtime, caches,
   and allocator. The worker block cache counts against its 512 MiB.
   I set a smaller cache per worker (a limit, which rule 3.5 allows; the
   report names this option in "Product decisions"). This also helps the
   "top users" query that hit the 512 MiB limit in 19 of 40 runs
   (measured, round 2 verdict eaa6b97).
5. **Victim latency, about +33% with no attacker** (measured, round 2).
   Suspects, to test one at a time in round 4:
   `GLIBC_TUNABLES` `mmap_threshold=131072` (code, `worker.rs:686`), which
   maps and unmaps each large buffer; the JSON frame headers; the cold
   worker start inside phase A.
6. **Lease handoff on restart.** A new worker incarnation must take the
   lease again. Today's lease code fences by the manifest etag, so this
   costs one store round trip per database (inferred).

## 5. Plan for rounds 2 to 8

1. **Round 2: boundary and streaming.** Binary frames; streamed request
   and response bodies with credit windows; per-owner in-flight
   semaphore; delete the light list and the per-site charges; main
   router reduced to `proxy_to_worker`.
2. **Round 3: the worker owns the database.** Tenant `Db`, lease,
   committer, flush, compaction, sweep, trims, and event maintenance move
   into the worker. Remove `Job::Query`, `ReadTxn`, `TableQuery`,
   `BlobValue`, `Merge`, `Seal`, `BulkInsert` and the commit and bind
   frames: the worker's own router runs them. Add `MetaCmd` frames and
   the purge job. Account, org, console, and anonymous routes go to
   workers.
3. **Round 4: the guard and the two route tests.** `ProcessRole`,
   `GuardedStore`, constructor checks, `DB_ROUTES` table, both tests.
   Run `cargo test -p baserunna --lib`.
4. **Round 5: the simulator.** In-process workers over a duplex,
   worker kills and restarts as simulator faults, lease handoff between
   incarnations. Run `cargo test -p baserunna --test sim`.
5. **Round 6: cost.** Measure the crossing on KV throughput, idle cost,
   the event scan, and the victim's latency alone. Fix the largest cost
   first. Shared-memory ring if the socket stays above about 10%.
6. **Round 7: all 8 workloads, small and attack, two runs each,** in
   the VM. Fix failures without changes to the exercised algorithms.
7. **Round 8: margin and review.** Re-run failing cells, check the 64 MiB
   margin over the victim's worker plus the main process, and review the
   diff against rule 3.5.

**Fit.** Rounds 2 and 3 are the largest. They move every handler and
every background job. If round 3 does not finish, the guard (round 4)
still lands, and its panics list what remains. The plan is tight; the
simulator round is the most likely to slip.
