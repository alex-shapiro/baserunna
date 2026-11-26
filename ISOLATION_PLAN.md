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

1. Each lease holder has its own lease identity. Today the identity belongs to
   the node (`Shared::node_id`).
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
3. `baserunna-server`: connections, authentication, and the routing of each
   request to a worker through the `Transport` trait. It does not depend on
   `baserunna-storage`.

`baserunna-server` and `baserunna-worker` each build their own binary. The
server process cannot open a database, because its binary does not link
`baserunna-storage`. A CI check on `cargo tree` keeps that dependency out. This
check replaces the store guard from milestone 3.

The split of the storage engine is easy. Today `engine/` imports only `store`,
`util`, `probe`, `error`, `buggify`, `env`, `metrics`, `log`, and `timing`.

## Milestones

A milestone is a group of PRs. The PR counts are estimates.

Milestone 0 comes first. Milestones 1 to 3 do not depend on each other. Start
them in any order. Milestone 4 needs milestone 3. Milestone 5 comes last.

### 0. Measure isolation on `main`

The isolation harness from the contest measures whether a victim tenant keeps
working while an attacker tenant runs a workload. It is on branch `iso/base`
(`crates/baserunna/examples/isolation/`, `scripts/isolation/run.sh`). A
workload passes if these conditions hold:

- The node continues to run.
- The victim has no failed requests.
- The p99 latency of the victim stays within the larger of two limits: 2 times
  its p99 alone, or its p99 alone plus 50 ms.
- Requests over a limit get HTTP errors.
- A small variant of the workload succeeds.

- [ ] Move the harness and its pass conditions to `main`. Record the result for
      each workload on `main` today.
- [ ] Run the harness at the end of each milestone, and record the results in
      this file.

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

### 2. Give each metadata command a fixed maximum cost

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

### 3. Run tenant work in workers

A worker is a node that serves the databases of a single owner. The server
sends each request to the worker of its owner through the `Transport` trait
(`cluster.rs`). Nodes already use this trait to forward requests to each
other. So the boundary protocol is HTTP. The simulator already runs nodes,
forwards requests between them, and kills them.

Each worker has its own node ID, and the ID is new each time the worker
starts. This keeps invariant 1. In this milestone, the workers run in the
process of the server. Milestone 4 chooses the mechanism that separates them.

If a worker stops, all requests in flight for that owner on that node fail.
The writes of its databases pause until their leases expire, for at most one
lease period. SPEC §1 allows the same pause after a node stops.

- [ ] Move the storage engine and the database features into `baserunna-storage`.
- [ ] Send each database request to an in-process worker for its owner. Each
      worker has its own `Engine` and node ID. The background jobs of each
      database run in its worker. The server sends the authenticated
      principal with the request, and it streams each body. The simulator runs
      each worker as a node.
- [ ] Add a store guard on the server side. It fails each read of a tenant
      object, in tests and in the simulator.
- [ ] Run the system database in its own worker. Metadata commands and the
      account and organization routes run there. Authentication and routing get
      metadata records from it.
- [ ] Move connections, authentication, and routing into `baserunna-server`.
      Add the `cargo tree` check. Rename the rest of the `baserunna` crate to
      `baserunna-worker`.

### 4. Choose the boundary mechanism by measurement

At this point the only difference between the designs is the boundary.

- [ ] Before any measurement, record the result that makes each mechanism win.
      Use absolute numbers, not percentages. Include these measurements:
  - CPU time for each request
  - the latency of a hot read
  - memory and CPU with 1,000 active owners on a node
  - the time of each event query in the harness
  - the results of the harness from milestone 0

  Known numbers: an empty round trip adds 25.6 µs of CPU for each request over
  a Unix socket, and 2.4 µs for a call into a WebAssembly instance
  (`BENCH_BOUNDARY` on branch `iso/floor`).
- [ ] Find the cause of the 81% loss of KV throughput in `iso3/cgroup`.
- [ ] Measure the worker pool and the WebAssembly sandbox with the workers from
      milestone 3. Adopt the winner by the recorded rule.
- [ ] Build the chosen mechanism. It must see every allocation and all CPU use,
      including those of dependencies. A counter in our code cannot. The number
      of PRs depends on the mechanism.
- [ ] Add the soft memory limit: a gauge at the admission point of each
      boundary. The gauge reads live heap bytes from a counting global
      allocator. Above a fixed share of the bound, new requests get a retryable 503. Do not use `memory.high`: in `iso3/cgroup` it stalled work for 120 s.
- [ ] Test the bound with workloads that nobody reviewed.
- [ ] Remove the memory counter (`engine/memory.rs`). This also removes the
      fair share of memory among the users of an organization.

### 5. Give each tenant a share of the caches and the store

The right method depends on the mechanism from milestone 4.

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
  to each tenant across the cluster. Today a single event query can use 1 GiB.
  In the contest, the worker pool at 512 MiB returned an error for a query in
  19 of 40 runs, and today's code answered all 40.
- The deployment requirement. The worker pool needs Linux with a delegated
  cgroup v2 subtree.

Decided:

- Fairness among the users of an organization is not necessary for now
  (2026-11-25). All users of an organization share the bound of the
  organization. One user can fill it, and then the other users get retryable
  503s.
