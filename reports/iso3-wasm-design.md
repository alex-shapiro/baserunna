# Round 3 design: WebAssembly sandbox

This is the round 1 design of the WebAssembly sandbox candidate in round 3
of the tenant isolation contest. The rules are in
`reports/tenant-isolation.md`, section "Round 3 rules", on branch
`iso/base`.

Labels: **code** gives a file and line on this branch at 6cee4e4.
**inferred** is a claim that no measurement or code read supports yet.

## 1. The design

The main process keeps no storage engine for any database. Each owner gets
one wasmtime instance. That instance runs the complete per-database node:
the HTTP handlers, the engine, every cache of tenant data, and every
background job on tenant data. The main process routes, moves bytes, does
store calls for the guest, and holds leases.

### 1.1 One instance per owner, plus one system instance

- **Owner instance.** One instance per owner (user or org) holds all the
  databases of that owner. Its linear memory maximum is 512 MiB. Wasmtime
  enforces this maximum (`StoreLimits` memory size, or the memory type
  maximum). Our code adds no charge to this bound.
- **System instance.** One instance holds `SYS_DB`. It has its own memory
  maximum (planned 256 MiB). It runs auth lookups, the account, org, and
  database-list routes, metadata commands (`run_meta`), the reaper and its
  purge, session expiry, and all upkeep of `SYS_DB`.
- One instance per owner, not one per database, so that the bound is one
  runtime limit and not a sum that our code keeps. The cost is that one
  owner uses at most one core at a time (see section 4).

### 1.2 Inside the guest

- The guest is the `baserunna` crate built for `wasm32-wasip1` with the
  `guest` feature, as in round 2 (code, `crates/baserunna-guest/Cargo.toml`).
  In round 3 the guest holds the axum routers for database routes
  (`api::data`, `api::streams`, `api::events`) and the console database
  pages (`console::db`, `console::events`, `console::streams`), with a
  guest `Node` that has only the per-database parts: `Engine`, `OpenTxns`,
  `Queries`, `Maintainer`, event bulk buffers, the forward replay cache,
  and the config.
- The guest runs one single-threaded executor. Every engine task runs on
  it: request tasks, the committer, flush, compaction, sweeps, trims,
  event bulk flush and maintenance, follows, and timers.
- The executor runs only inside a call from the host,
  `guest_poll(now_ms, events)`. The guest sets its clock to `now_ms`,
  delivers the events (completions of host calls, new requests, body
  chunks, cancels), and runs tasks until no task is ready. It then returns
  whether it has timers pending.
- The executor is the one that the round 2 guest already uses: a tokio
  current-thread runtime with a paused clock and `Env::sim`, which
  `tokio::time::advance` moves to the host's time (code,
  `crates/baserunna-guest/src/lib.rs:72`, `:89`). The engine calls
  `tokio::time::sleep` directly in 23 places outside tests (code, grep
  of `engine`, `events`, `stream`, `table`, `api`), so the guest must keep
  tokio time.
- A paused tokio clock jumps to the next timer when the runtime has no
  ready task. A host call in flight must not let it jump. So `guest_poll`
  runs tasks with `yield_now` steps, which park with a zero timeout and so
  advance the clock by zero. A runtime with a deferred task parks with a
  zero timeout (code, tokio 1.54.1 `runtime/scheduler/current_thread/mod.rs:445`,
  `:457`), and the time driver caps the advance at that timeout (code,
  tokio 1.54.1 `runtime/time/mod.rs:235`). `Env::spawn` already wraps each task in `SimTask` (code,
  `env.rs:38`). A poll counter in `SimTask` tells the step loop when one
  full step polled no task, which means the guest is idle.
- Tokio does not expose the next timer deadline. So the host polls an
  instance that has timers pending every 5 ms. An idle instance with no
  timers gets no polls.
- Each owner instance has its own block cache of 16 MiB inside its
  512 MiB, in place of the node-wide `cache_bytes` (code, `main.rs:79`;
  the harness sets 64 MiB in `scripts/isolation/run.sh:86`). Section 1.8
  gives the reason for 16 MiB.

### 1.3 Host driver per instance

One host tokio task drives each instance. It loops:

1. Wait for the first of: a host call completion, a new request or body
   chunk for this owner, the 5 ms timer tick if the guest has timers.
2. Wait for a turn in the CPU scheduler (section 1.6).
3. Call `guest_poll` on a wasmtime async fiber.

### 1.4 How bytes move

- **Requests.** The host router reads headers under fixed limits,
  authenticates, looks up the database record, and picks the owner. It
  then sends the request head and the body to the owner instance in
  chunks of at most 16 KiB. The host holds at most one chunk per request.
  The guest allocates the destination in its own linear memory, so the
  instance's memory limit bounds the body. Each owner has a fixed limit
  on requests in flight (planned 256); the next request gets a 503.
- **Responses.** The guest calls `respond_head` and `respond_chunk`. Each
  chunk is at most 16 KiB, and a bounded channel of one chunk feeds the
  hyper body. A slow client makes the guest task wait. It does not make
  the host buffer grow.
- **Store calls.** The guest's `ObjectStore` is `GuestStore`. Each method
  is a host call that starts the store call and returns a ticket. Each
  owner has a fixed limit on store calls in flight (planned 16). The host
  never parses, decodes, caches, or indexes these bytes. The harness store
  is a fake S3 in another process (code, `scripts/isolation/run.sh:64`),
  so the node uses `store::cloud`.
- **Store gets.** The host reads the response body as a stream of HTTP
  frames and copies each frame into guest memory. The host holds one frame
  per call. Today `ObjectStore::get` returns the whole object as one
  `Bytes` (code, `store/mod.rs:109`), so round 5 adds a streaming get.
- **Store puts.** A put can be large: an event part can be 32 MiB (code,
  `engine/config.rs:172`, `event_chunk_bytes`). `object_store` puts take
  the whole payload (code, `store/cloud.rs:154`, `PutPayload`). A host
  copy of a 32 MiB body would not fit in the 64 MiB margin with the
  victim. So the host does not copy a put body. It gives `object_store` a
  `Bytes` that points into the guest's linear memory, where the bytes
  already count against the guest's limit.
  - This is safe only if the linear memory does not move or unmap while
    the put is in flight. The engine config sets
    `memory_may_move(false)` with a 4 GiB `memory_reservation`. Then
    wasmtime never moves a linear memory when it grows (code, wasmtime
    48.0.8 `src/config.rs:1960`, the version in `Cargo.lock`).
  - The guest keeps the buffer alive and unchanged until the ticket
    completes. The host keeps the instance's store alive until each put in
    flight ends, also after a trap.
  - If this proves unsafe, the fallback is a host buffer of fixed size per
    put (the largest object size) and at most 2 puts in flight per owner.
- **Block cache and disk cache.** The block cache lives in the guest
  (`engine::cache`), so the instance memory limit bounds it. The local disk
  tier (`engine::local`) keeps an LRU index of tenant objects in the host
  (code, `engine/local.rs:209`, `:224`). In sandbox mode the node runs with
  no local disk tier. A later round can give each instance a WASI
  directory with a fixed quota, with the index in the guest.

### 1.5 How commits and leases come back

- The commit is the manifest `PUT If-Match` (SPEC 5.5, step 7). The guest
  sends it with a separate host call, `commit_manifest`. Its fixed header
  holds the database id, the base ETag, and the lease grant.
- The host checks the body size against a fixed maximum before it reads
  the body. It charges that size to the owner's store calls in flight. It
  starts the lease timer before it sends the `PUT` (SPEC 5.7), does the
  `PUT`, and records the lease on success or clears it on error. It
  never parses the manifest.
- The host lease table maps a database id to (holder, valid until). It
  has a fixed entry count. Guests report leases of other nodes, which they
  see in manifests, through a fixed-size host call, `lease_observed`.
- `Node::route` opens the database to find its lease holder (code,
  `node.rs:738`). That is a host read of tenant data. In the new design,
  routing reads only the host lease table. On a miss, the host asks the
  owner instance for the holder, which is a fixed-size answer.
- Commit notices (`notify_home`, code `node.rs:419`) leave the guest
  through a fixed-size host call. The host sends them to the home node.
- A peer's `/internal/v1/events/bind` and `/internal/v1/meta` requests
  are request bytes for a boundary. The host sends a bind to the owner
  instance of its database, and a meta command to the system instance.
  The boundary does the work. The host applies only what the boundary
  computed, after a size check and a charge (item 3).
- A `not_owner` result after a request ran comes back as a fixed-size
  status. The host forwards the request only if the whole body fit in its
  one 16 KiB chunk, which the host still holds. Otherwise the client gets a
  retryable 503. So forwarding adds no host buffer.

### 1.6 How CPU is bounded

- **Epoch interruption, not fuel.** A host thread increments the engine
  epoch every 1 ms. Each instance has `epoch_deadline_async_yield_and_update`.
  So a guest yields its host thread at least each 1 ms, also inside a
  long loop with no await. Fuel goes away, because fuel instrumentation
  slows all guest code (round 2 used fuel, code `sandbox.rs:189`).
- **Turns by owner.** At most N instances run at once (N = cores). Waiting
  instances get turns in order by owner, as in round 2 (commit c9349c5).
  A victim with short requests waits at most one tick per running owner
  before it runs (inferred).
- **Kill on a CPU budget.** A host counter adds the ticks of each
  `guest_poll`. If one poll uses more CPU than the budget (planned 2 s,
  less than 2 × the 10 s request deadline), the host traps the instance.
  Every request in flight on that instance gets a 503. The next request
  starts a new instance, which loads its state from the store. This is
  epoch interruption, so it is a runtime limit.
- **Request deadlines in the guest.** The guest drops a request task at its
  deadline. That task's response is an error, so over-limit requests get
  HTTP errors within 2 × the request deadline.
- **Memory over the bound.** A `memory.grow` past 512 MiB fails. The Rust
  allocator in the guest then aborts, which traps the instance. The host
  answers the requests in flight with 503 and drops the instance.

### 1.7 Idle owners

- The idle cost test makes 1,000 owners touch one database each, then
  reads node memory for 30 s (code, `examples/isolation/idle_cost.rs:1`,
  `:12`, `:14`). If all 1,000 instances stay live, the node holds 1,000
  instance heaps.
- So an instance with no requests in flight, no open transactions, and no
  pending background work for 2 s quiesces and goes away. Its state is on
  the store. After that, an idle owner holds no instance memory.
- The host lease table keeps the lease of a dropped instance's databases
  until the lease ends, because the host holds leases (item 3).
- New instances map the initial heap copy-on-write (`memory_init_cow`), so
  an instance's resident memory is only the pages it touches. The size of
  a fresh instance with one open database is not measured yet. Round 2
  measures it.
- A fixed node-wide cap on live instances (planned 512) protects the node
  if many owners are active at once. When the cap is full, the host drops
  the least recently used quiesced instance. If no instance is quiesced, the
  new owner's request gets a retryable 503.

### 1.8 Memory budget at the limit

The pass condition allows 576 MiB of growth from the idle node. That
growth includes the attacker, the victim, and the host. Planned budget
(all inferred until round 7 measures it):

| Part | Budget |
| --- | --- |
| Attacker instance, linear memory at its maximum | 512 MiB |
| Victim instance: heap, 16 MiB block cache, open databases | at most 40 MiB |
| Host: request chunks (2 owners × 256 requests × 16 KiB = 8 MiB at most), store frames, fiber stacks, instance metadata | at most 20 MiB |
| Total | at most 572 MiB |

- The request chunk line is a worst case. Each request holds at most one
  16 KiB chunk in the host, and only while it moves.
- A wasm memory does not shrink. An attacker instance that reached
  512 MiB keeps it until the instance goes away. That is still inside the
  bound.

## 2. The main-process list

Every item that the main process does after the change. "Allowed" names
the item of the requirement. "Planned" paths do not exist yet.

### 2.1 Request path

| What it does | Allowed | Code path |
| --- | --- | --- |
| Accept connections, with a fixed limit on open connections | 1 | `main.rs` `run`, planned `ConnLimit` (today no limit) |
| Read HTTP headers with fixed hyper buffer and header limits | 1 | `main.rs` `run`, planned hyper builder settings |
| One hyper task per connection, under the connection limit | 1 | today `main.rs:204` `axum::serve`; planned hyper-util accept loop, because `axum::serve` sets no connection limit and no header limits |
| Request context: deadline, client address, metrics | 1 | `api/mod.rs:715` `request_context`, `:346` `tag_client` |
| Close a connection with an unread body | 2 | `api/mod.rs:1169` `close_if_body_unread` |
| Authenticate: auth cache, miss goes to the system instance | 1 | `auth.rs`, planned `SystemInstance::auth` |
| Password hashing for login, under the fixed `hash_slots` | 1 | `node.rs:308` `hash_slots` (unsure, see report) |
| Look up the database record: name cache, miss goes to the system instance | 1 | `node.rs` `name_cache`, planned `SystemInstance::db_record` |
| Org and role caches, fixed entry count and entry size | 1 | `node.rs:296`, `:298` `TtlCache` with planned caps |
| Route: host lease table, rendezvous, peer address | 1 | `node.rs:733` `route`, changed to read only the lease table |
| Forward a request to a peer; after `not_owner`, only a body of one 16 KiB chunk | 2 | `api/mod.rs:460` `db_routing`, `:676` `tag_forward` |
| Forward dedup table: ids only, fixed entry count | 1 | `cluster::ForwardDedup`; the replay bodies move to the guest (today `api/mod.rs:790` `dedup_forwarded` holds whole bodies) |
| Rate limiter per client, fixed entries | 1 | `node.rs:306` `limiter` |
| Per-owner limit on requests in flight | 2 | planned `sandbox::OwnerSlots` |
| Move request body chunks into the guest, 16 KiB | 2 | planned `sandbox::OwnerInstance::send_body` |
| Move response chunks out of the guest, 16 KiB | 2 | planned `sandbox` host call `respond_chunk` |
| Routes with no tenant data: `/healthz`, `/v1/openapi.json`, `GetLimits`, console static pages and forms | 1 | `api/mod.rs:1193` `healthz`, `:1217` `openapi`, `:1200` `limits`, `console/mod.rs` |
| Every database route `/v1/dbs/{db}/...` | 2 | one host route that sends the request to the owner instance |
| Console database pages `/console/dbs/{db}/...` | 2 | one host route that sends the request to the owner instance |
| Account, org, API key, session, and database-list routes | 2 | sent to the system instance |
| `/internal/v1/events/bind` | 2, 3 | sent to the owner instance; size check and charge first |
| `/internal/v1/meta` | 2, 3 | sent to the system instance; size check and charge first |
| `/internal/v1/notice` (fixed-size: database id and version) | 2 | sent to the owner instance if it is live; dropped if not |
| Internal auth of peer requests | 1 | `api/mod.rs:873` `internal_auth` |
| Per-route latency metrics, keyed by route template (a fixed set) | 1 | `metrics.rs:155` `ServerMetrics::routes` |
| Guest log lines: copy each line, cut at a fixed length, to the log writer | 2 | `sandbox.rs:338` `fd_write`, `log.rs:74` |
| Event query admission across users | moves: the host CPU turns replace it; each owner instance keeps its own `Queries` | `queries.rs:1` |

### 2.2 Store calls, commits, and leases

| What it does | Allowed | Code path |
| --- | --- | --- |
| Start a store call for a guest, fixed calls in flight per owner | 2 (store ruling) | planned `sandbox` host calls `store_get`, `store_put`, `store_list`, `store_delete` |
| Copy store get frames into guest memory, one frame at a time | 2 (store ruling) | planned streaming `store::cloud` get |
| Send a put body from guest memory with no copy | 2 (store ruling) | planned host call `store_put`, `Bytes` over guest memory |
| Commit: size check, charge, lease timer, manifest `PUT If-Match` | 3 | planned host call `commit_manifest` |
| Hold the lease table, fixed entry count | 3 | planned `sandbox::LeaseTable` |
| Record leases of other nodes that a guest reports | 3 | planned host call `lease_observed` |
| Send commit notices to the home node | 2 | `node.rs:419` `notify_home`, fed by a planned host call |
| Send mail that the system instance made, fixed largest size | 2 | `node.rs:1311` `send_mail` (unsure, see report) |

### 2.3 Background jobs

Every `Env::spawn` site and `spawn_blocking` site outside tests, with
where it runs after the change.

| Job | Runs in | Allowed | Code path |
| --- | --- | --- | --- |
| Heartbeat | host | 4 | `node.rs:536` `spawn_background` |
| Members refresh | host | 4 | `node.rs:551` `spawn_background` |
| Housekeeping timer | host | 4 | `node.rs:561`, `:1583` `housekeeping` |
| - `engine.evict_idle` | guest (each instance evicts its idle `Db`s); host drops idle instances | 4 for the drop | `engine/mod.rs:412`; planned `sandbox` idle drop |
| - store clock sync | host (reads the store clock, no tenant data) | 4 | `engine/mod.rs:147` `sync_clock`, `:153` `refresh_clock` |
| - `txns.expire` | guest | – | `node.rs:1586`, `txns.rs` |
| - auth, name, org, role cache prune | host | 1 | `node.rs:1587`–`:1590` |
| - `forwards.expire` | host (ids only) | 1 | `node.rs:1591` |
| - `limiter.prune` | host | 1 | `node.rs:1592` |
| Reaper scan and purge | system instance | – | `node.rs:569`, `:1597` `reap_deleted`, `engine/mod.rs:425` `purge` |
| Session expiry | system instance | – | inside `reap_deleted` |
| `SYS_DB` flush, compaction, sweep | system instance | – | same engine jobs as below, on `SYS_DB` |
| Commit notice sender | host | 2 | `node.rs:435` `notify_home` |
| Mail sender | host | 2 | `node.rs:1312` `send_mail` |
| Event-loop lag metric | host | 4 | `main.rs:171` |
| Shutdown timer | host | 4 | `main.rs:193` |
| Committer | guest | – | `engine/db.rs:1007` `enqueue`, `committer` |
| Load of noticed versions | guest | – | `engine/db.rs:970` `load_noticed` |
| Deletion of obsolete objects | guest | – | `engine/db.rs:1983` `delete_obsolete` |
| Compaction | guest | – | `engine/db.rs:2004` `after_commit` |
| Flush | guest | – | `engine/db.rs:2037` `maybe_flush` |
| Parked flush | guest | – | `engine/db.rs:2111` `park_flush` |
| Stream retention trims | guest | – | `engine/db.rs:2130` `schedule_trims` |
| GC sweep | guest | – | `engine/db.rs:2225` `maybe_sweep` |
| Levels object write to the local tier | guest, or off with no local tier | – | `engine/version.rs:207`, `:230`, `:250` |
| Disk cache read and write | off in sandbox mode | – | `engine/local.rs:209`, `:224` |
| Event bulk flush timer and flush | guest | – | `events/bulk.rs:102`, `:131` |
| Event maintenance: seals, merges, retention | guest | – | `events/maint.rs:1020` `schedule` |
| Stream follows (server-sent events) | guest; the host moves its chunks | 2 for the chunks | `api/streams.rs:375` `events` |
| Idle instance drop | host (drops a quiesced instance) | 4 | planned; replaces `sandbox.rs:602` `sweep_idle` |
| Epoch ticker | host | 4 | planned |
| Instance driver task per owner | host (moves bytes and runs `guest_poll`) | 2 | planned `sandbox::drive` |
| Log writer thread | host (writes log lines) | 4 | `log.rs:74` |

## 3. The guard and the two tests

### 3.1 The guard

- `engine::host_reads` changes from a task-local scope to a process-wide
  mode. When a node starts with the sandbox, it sets "host reads denied".
  From then on, every read path on the host fails, for every database,
  `SYS_DB` included. In the guest build, the check is a no-op, because the
  guest is the boundary.
- A process-wide mode covers background jobs with no extra work: a job on
  the host fails at its first read. The task-local scope of round 2 (code,
  `engine/host_reads.rs:10`) did not cover them.
- The check goes at the entry of each read path:
  - `Txn`: get, scan, and every read in `engine/txn.rs`;
  - `Version`: segment and levels fetch in `engine/version.rs`;
  - segments: decode in `engine/segment.rs`;
  - blobs: chunk read in `engine/blob.rs`;
  - event parts: part read and decode in `engine/events.rs`;
  - streams: record reads in `engine/stream.rs`;
  - manifests: `Db::open` and manifest decode in `engine/manifest.rs`.
- A violation is an internal error in release builds. In test builds it
  also increments a global violation counter, which the tests check.
- A second, structural layer: in sandbox mode, `Node` holds no `Engine`.
  The host gets its lease table and store client only. So a new host code
  path that wants tenant data must add an engine to the host, which a
  judge sees in the diff.

### 3.2 Test: every route with the guard on

- The host router and the guest router are built from route tables
  (template and method). The test asserts that the tables match the
  routers: a request to each template must not reach the fallback.
- The test starts a node in sandbox mode with the guard on and a memory
  store. For each route and method, it sends a valid request with setup
  data. It then advances time past each background interval, so that
  flush, compaction, sweeps, trims, event maintenance, the reaper, and
  idle drop all run.
- It asserts that the violation counter is zero.

### 3.3 Test: every route goes through the boundary

- Each instance call increments a per-request crossing counter.
- For each route in the tables, the test asserts one of these:
  - the request crossed into an owner instance or the system instance;
  - the route is on `HOST_ONLY_ROUTES`. That list holds only routes with
    no tenant data (`/healthz`, `/v1/openapi.json`, `GetLimits`, console
    static pages). Its handlers are in a module that cannot name engine
    types.
- `HOST_ROUTES` with `NotMoved` goes away. No database route is on any
  host list.

## 4. Sources of extra cost

| Cost | How I keep it small |
| --- | --- |
| Crossing per request | A small request crosses once in (head and body together) and once out (head and body together). A warm-instance call cost 7% of KV throughput for 2 crossings (measured, round 2 evidence, `BENCH_BOUNDARY`). |
| Crossings per store call | Start and completion share the next `guest_poll`. One poll delivers all completions that are ready. |
| Copies into and out of guest memory | One copy each way for HTTP bodies, in 16 KiB chunks. One copy for store gets, frame by frame. No copy for store puts. |
| One core per owner | All work of one owner runs on one instance, so a single hot owner cannot use more than one core. The KV throughput test uses 10 owners with 200 databases each on 4 vCPUs (code, `examples/isolation/kv_throughput.rs:5`, `:6`, `:11`), so this limit does not bind there. |
| Instance per active owner | Each active owner pays one guest heap. The 2 s idle drop and copy-on-write heap keep idle cost small. Measure the fresh-instance size in round 2. |
| Reload after an idle drop | A dropped owner's next request reloads its manifest from the store (one GET at 20 ms in the harness). |
| Speed of guest code | Epoch interruption instead of fuel. Build the guest with `simd128`, `opt-level=3`, LTO, one codegen unit. Cranelift with `opt_level=speed`. |
| Module compile at start | Compile ahead of time to a `.cwasm` and load it with `Module::deserialize_file`, which maps the file. Round 2 paid a fixed 97 MiB (measured, round 2 evidence; its cause is inferred). |
| Instance start | Pooling allocator with copy-on-write memory images, so a new instance maps the initial heap. |
| No local disk tier | More store GETs after the block cache misses. The harness sets no `BASERUNNA_DISK_CACHE_DIR`, so the harness runs have no disk tier today too (code, `main.rs:81`, `scripts/isolation/run.sh:86`). |
| Lost instance state after a trap or an idle drop | The next instance reloads manifests and segments from the store. |
| Simulator coverage | The simulator keeps running the engine natively, with the guard off. It does not test the boundary. |

## 5. Plan for rounds 2 to 8

The requirement comes first. Workload tuning comes last.

1. **Round 2.** Guest executor driven by `guest_poll`, `GuestStore` with
   ticket host calls, and a host driver task per owner. One owner
   instance answers a KV get and put end to end in a library test.
2. **Round 3.** Move all database routes and console database pages into
   the guest router. One host route per prefix. Request and response
   chunking with fixed buffers. `commit_manifest`, the lease table, and
   `lease_observed`. Route by the lease table. KV throughput check in the
   VM.
3. **Round 4.** System instance: auth, record lookup, account and org
   routes, `run_meta`, the reaper, session expiry, and `SYS_DB` upkeep.
   Internal routes. Remove `Engine` from the host `Node`.
4. **Round 5.** Process-wide guard on every read path, route tables, and
   the two router tests. Replace `axum::serve` with a hyper-util accept
   loop that has a connection limit and fixed header limits. Fixed caps on
   the host caches. Streaming store get and the put with no copy.
5. **Round 6.** Epoch interruption, turns by owner, the CPU budget kill,
   ahead-of-time compile, pooling allocator. Fix
   `event_inserts_in_sandbox_match_native`. Library tests pass.
6. **Round 7.** All 8 known workloads, small and attack, in the VM. Fix
   what fails without workload-specific code.
7. **Round 8.** Second run of each cell, cost measurements, final report.

This plan does not fit with margin. Rounds 2 to 4 move almost all of the
node into the guest, and each step needs the axum handlers, `OpenTxns`,
`Queries`, and the event code to build for `wasm32-wasip1`. If the plan
slips, I keep the requirement work (rounds 2 to 6) and cut workload
measurement in round 7, and I report the slip.
