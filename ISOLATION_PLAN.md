# Tenant Isolation Plan

This plan builds the tenant isolation goal in SPEC §1. The work is a series of
small PRs to `main`. Each PR is useful by itself, passes the simulator, and leaves
`main` in a working state.

A tenant is the owner of a database: the user or organization in the
`owner_id` column of the `databases` table (SPEC §8).

`TODO.md` tracks bugs. This file tracks the epic. If a PR in this plan fixes a
`TODO.md` item, the PR also removes that item.

## Background

A design contest tried to build isolation in a few fixed-length rounds.
No design qualified. The report is `reports/tenant-isolation.md` on branch
`iso/base`.

The contest taught these lessons:

- The storage engine needs few changes. In the best build, `iso3/cgroup`,
  most of the engine stayed the same. Each worker ran the same `Engine` code.
- The node shell needs large changes. Startup, routing, metadata commands,
  and the system database all change where they run.
- Correctness is the largest risk. In commit 90fd667, two processes used the
  same lease identity. The node then served stale reads after concurrent
  database creates.

## Invariants

`main` does not enforce these invariants today.

1. Each lease holder is a single process. Today it is the node (`Shared::node_id`).
2. Each tenant's use of every resource counts against that tenant's share.
   Resources are memory, CPU, caches, disk, store requests, and the work of
   the trusted process and the system database. Over a share, only that
   tenant's work becomes slower or fails.

## Crate layout

Split the `baserunna` crate into three crates:

1. `baserunna-storage`: the storage engine and the database features.
2. `baserunna-worker`: the request handlers and the background jobs of each
   tenant. All tenant work runs in this crate. It depends on
   `baserunna-storage` and `baserunna-server`.
3. `baserunna-server`: connections, authentication, routing, and byte
   movement. It also defines the frames that move requests and responses
   across the boundary. It does not depend on `baserunna-storage`.

`baserunna-server` and `baserunna-worker` each build their own binary. The
server process cannot open a database, because its binary does not link
`baserunna-storage`. A CI check on `cargo tree` keeps that dependency out. This
check replaces the store guard from milestone 4.

The split of the storage engine is easy. Today `engine/` imports only `store`,
`util`, `probe`, `error`, `buggify`, `env`, `metrics`, and `log`.

## Milestones

A milestone is a group of PRs. The PR counts are estimates.

Milestones 1 to 4 do not depend on each other. Start them in any order.
Milestone 5 needs milestones 2 and 4. Milestone 6 comes last.

### 1. Make each operation fit in a fixed amount of memory

Isolation does not need this milestone. Under a boundary, an operation without
a bound gets an error at the owner's limit, and the node continues to run. This
milestone makes those operations succeed. It also helps users today.

Each PR adds a test of the form "a request of size N uses at most k × N + C
bytes".

- [ ] Move the counting allocator into `tests/support/`. Today
      `memory_bound.rs`, `event_ingest_limits.rs`, and `event_query_limits.rs`
      each have a copy.
- [ ] Parse JSON request bodies as a stream: KV and transaction ops. TODO:
      "Stream JSON request bodies and charge bulk insert memory".
- [ ] Parse JSON request bodies as a stream: event rows. Same TODO item.
- [ ] Bound the rows that a bulk insert holds before a flush. Same TODO item.
- [ ] Limit the partitions of a bulk insert. TODO: "Limit partitions per bulk
      insert".
- [ ] Merge parts block by block. This PR is the largest, and it can become two
      PRs. TODO: "Bound memory usage during part merges".
- [ ] Limit event maintenance across the node. TODO: "Limit event maintenance
      concurrency across the node".
- [ ] Estimate string column memory from the real lengths. TODO: "Estimate
      granule run memory from actual string lengths".
- [ ] Compare blob values chunk by chunk in `check` ops (`ops.rs`).
- [ ] Bound the total bytes of results that a transaction keeps (`ops.rs`).
- [ ] Limit the blob chunks that downloads hold at the same time
      (`api/body.rs`).
- [ ] Stream bodies through the node. Bound `BUFFER_FORWARD_MAX` and the
      responses in `ForwardDedup`.
- [ ] Add connection limits. Set the header and buffer limits of hyper in the
      code (`main.rs`).
- [ ] Time out stalled request bodies. TODO: "Time out stalled request bodies".

### 2. Give each process its own lease identity

- [ ] Make the lease identity belong to a process, for example the node ID
      plus a value that is new at each process start.
- [ ] Make the simulator run more than one lease holder on each node, and kill
      them one at a time.

### 3. Give each metadata command a fixed maximum cost

A per-tenant boundary does not fit metadata commands. All of them go through
the single lease holder of the system database. Many of them also read the data
of more than one owner. For example, `create_db` for an organization reads the
role of the user in that organization. So the lease holder admits each command
at a fixed maximum cost, and it serves tenants fairly.

A limit remains: the total rate of metadata writes in the cluster has a
ceiling. This is a limit on scale, not on isolation.

- [ ] Add a length limit to each field of each metadata command. Most fields
      have a limit today. The Google `sub` value does not.
- [ ] Add a test that each command reads and writes at most a fixed number of
      rows and bytes. Commands that delete many rows already work in batches of
      `PAGE_MAX` rows.
- [ ] Add a fair queue at the lease holder of the system database, before it
      runs a command. Use a queue for each account, and a queue for each client
      address before sign-in.

### 4. Put all tenant work behind an in-process boundary

The in-process boundary is permanent. The simulator uses it, because the
simulator cannot run worker processes or kernel limits. It also lets each PR
move a part of the work while `main` stays in a working state.

- [ ] Move the storage engine and the database features into `baserunna-storage`.
- [ ] Add the frames and an in-process boundary over an in-memory channel, in
      the `baserunna` crate. A body crosses the boundary in fixed-size chunks,
      and the receiver grants credit for each chunk. So each side holds a fixed
      amount of memory for each request, whatever the size of the body.
- [ ] Make the route table data. Add two tests: each route crosses the
      boundary, and the server has no handler for any tenant route.
- [ ] Send each group of routes through the frames, one PR for each group. Both
      sides still share one `Engine`, so these PRs test the frames, not the
      separation.
  - [ ] KV
  - [ ] Transactions
  - [ ] Tables
  - [ ] Streams
  - [ ] Event tables
  - [ ] Blobs
  - [ ] Accounts and organizations
  - [ ] Console
  - [ ] Internal routes between nodes
- [ ] Give the worker side its own `Engine`, and remove tenant databases from
      the server side. The background jobs of each database move with it. A
      store guard on the server side fails each read of a tenant object, in
      tests and in the simulator.
- [ ] Run the system database behind its own boundary, with its own memory
      bound. Metadata commands and the account and organization routes run
      there. Authentication and routing get metadata records from it.
- [ ] Move connections, authentication, routing, and the frames into
      `baserunna-server`. Add the `cargo tree` check. Rename the rest of the
      `baserunna` crate to `baserunna-worker`.

### 5. Choose the boundary mechanism by measurement

At this point the only difference between the designs is the boundary.

- [ ] Measure the worker pool and the WebAssembly sandbox behind the protocol
      from milestone 4. Known numbers: an empty round trip costs 42% of
      throughput over a Unix socket, and 7% for a call into a WebAssembly
      instance (`BENCH_BOUNDARY` on branch `iso/floor`).
- [ ] Find the cause of the 81% loss of KV throughput in `iso3/cgroup`.
- [ ] Build the chosen mechanism. It must see every allocation and all CPU use,
      including those of dependencies. A counter in our code cannot. The number
      of PRs depends on the mechanism.
- [ ] Add the soft memory limit: a gauge at the admission point of each
      boundary. The gauge reads live heap bytes from a counting global
      allocator. Above a fixed share of the bound, new requests get a retryable 503. Do not use `memory.high`: in `iso3/cgroup` it stalled work for 120 s.
- [ ] Test the bound with workloads that nobody reviewed.
- [ ] Remove the memory counter (`engine/memory.rs`). This also removes the
      fair share of memory among the users of an organization.

### 6. Give each tenant a share of the caches and the store

The right method depends on the mechanism from milestone 5.

- [ ] Block cache
- [ ] Disk cache
- [ ] Store requests

## Rules for each PR

- Each PR merges to `main` by itself and passes the simulator.
- Each PR does not change the algorithm of an operation unless the PR is about
  that algorithm.
- The plan has no fixed time limit.

## Decisions for the user

- The memory bound for each tenant. The contest used 512 MiB. A higher bound
  lets larger operations succeed, but it also lets a tenant take more of the
  node. Also decide whether the bound applies to each tenant on each node, or
  to each tenant across the cluster.
- The deployment requirement. The worker pool needs Linux with a delegated
  cgroup v2 subtree.

Decided:

- Fairness among the users of an organization is not necessary for now
  (2026-11-25). All users of an organization share the bound of the
  organization. One user can fill it, and then the other users get retryable
  503s.
