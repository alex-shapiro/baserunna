# Tenant Resource Isolation Plan

We will isolate each tenant in a separate worker process with Linux cgroup
limits. This work meets the tenant resource isolation goal in SPEC §1. Each
tenant is a database owner: the user or organization in the `owner_id` column
of the `databases` table in SPEC §8.

We will deliver this work as small PRs to `main`. `TODO.md` lists bugs. This
file lists the isolation milestones. If a PR fixes a bug in `TODO.md`, remove
that item in the same PR.

## Background

We ran a design contest to build isolation in fixed-length rounds. None of the
designs qualified. The report is in `reports/tenant-isolation.md` on branch
`iso/base`.

We learned three lessons from the contest:

- The storage engine needs few changes. In `iso3/cgroup`, each worker ran the
  existing `Engine` code.
- The node shell needs large changes. We must split startup, routing, metadata
  commands, and the system database across the server and workers.
- Correctness is the largest risk. In commit `90fd667`, two processes shared
  the same lease identity. The node then served stale reads after concurrent
  database creation.

## Invariants

The finished system enforces two invariants. `main` does not enforce them yet.

1. Each lease holder has a unique lease identity. Today, all work on a node
   shares `Shared::node_id`.
2. The node keeps each tenant within its limit for every resource:
   - Memory and CPU
   - Block and disk caches
   - Object store requests
   - Shared server and system database work

   If a node does not have enough memory, the kernel stops the tenant that uses
   the most memory first.

## Crate layout

We will split the `baserunna` crate into four crates. This split prevents the
server binary from linking the storage engine:

1. `baserunna-util`: shared support modules and the `Transport` trait.
2. `baserunna-storage`: the storage engine and database features. It depends on
   `baserunna-util`.
3. `baserunna-worker`: tenant request handlers and background jobs. All tenant
   work runs in this crate. It depends on `baserunna-storage` and
   `baserunna-util`.
4. `baserunna-server`: network connections, authentication, and request
   routing. It depends only on `baserunna-util`.

Cargo builds separate binaries for `baserunna-server` and `baserunna-worker`.
Because `baserunna-server` links only `baserunna-util`, the server process
cannot open a database. A `cargo tree` check in CI enforces this dependency
rule. That check replaces the store guard from milestone 3.

Extracting the storage engine is simple. Today `engine/` depends only on
`store` and the shared modules we will place in `baserunna-util`.

## Milestones

Each milestone is a group of PRs. After each milestone, run the isolation
harness from milestone 0. Record the results in this file.

Milestone 0 comes first. Milestones 1 to 3 are independent. Start them in any
order. Milestone 4 depends on milestone 3. Milestone 5 comes last.

### 0. Measure isolation on `main`

The isolation harness checks whether the node keeps serving a victim tenant
while an attacker tenant runs a heavy workload. The harness is on branch
`iso/base` at `crates/baserunna/examples/isolation/` and
`scripts/isolation/run.sh`.

A workload passes if it meets all five criteria:

- The node continues to run.
- All victim requests succeed.
- The victim p99 latency does not exceed the larger of `2 * baseline` or
  `baseline + 50 ms`.
- The server returns an HTTP error for requests that exceed a limit.
- A small variant of the workload succeeds.

Tasks:

- [ ] Move the isolation harness and its pass criteria to `main`. Record the
      baseline result for each workload on `main`.

### 1. Bound memory use for each operation

This milestone bounds per-operation memory so large requests succeed within a
tenant limit. These changes also protect users on `main` today. Process
isolation does not require this milestone. Under cgroup limits, an unbounded
operation fails at the tenant limit while the node continues to run.

Each PR adds a test verifying that a request of size `N` uses at most
`k * N + C` bytes.

- [ ] Move the counting allocator into `tests/support/` to remove duplicate
      copies in `memory_bound.rs`, `event_ingest_limits.rs`, and
      `event_query_limits.rs`.
- [ ] Stream-parse JSON request bodies for KV and transaction operations. Part
      of the `TODO.md` item "Stream JSON request bodies and charge bulk insert
      memory".
- [ ] Stream-parse JSON request bodies for event rows. Part of the same
      `TODO.md` item.
- [ ] Bound the rows buffered by a bulk insert before a flush. Completes the
      same `TODO.md` item.
- [ ] Limit partitions per bulk insert. Removes this item from `TODO.md`.
- [ ] Merge parts block by block. Split into two PRs if needed. Removes the
      `TODO.md` item "Bound memory usage during part merges".
- [ ] Limit event maintenance concurrency across the node. Removes this item
      from `TODO.md`.
- [ ] Estimate string column memory from actual string lengths. Removes the
      `TODO.md` item "Estimate granule run memory from actual string lengths".
- [ ] Compare blob values chunk by chunk in `check` operations in `ops.rs`.
- [ ] Bound total result bytes held by a transaction in `ops.rs`.
- [ ] Limit concurrent buffered blob chunks during downloads in `api/body.rs`.
- [ ] Stream forwarded request and response bodies through the node. Bound
      `BUFFER_FORWARD_MAX` and cached responses in `ForwardDedup`.
- [ ] Add connection, Hyper header, and Hyper buffer limits in `main.rs`.
- [ ] Time out stalled request bodies. Removes this item from `TODO.md`.

### 2. Bound metadata command cost

The system database lease holder runs all metadata commands. It limits the cost
of each command and schedules tenants fairly. Per-tenant workers cannot isolate
metadata commands. A single lease holder runs all metadata commands, and many
commands read data from multiple owners. For example, `create_db` for an
organization reads the caller's role in that organization.

The cluster still has an upper limit on total metadata write throughput. That
limit bounds scale rather than isolation.

- [ ] Enforce a length limit on every metadata command field, including the
      Google `sub` field.
- [ ] Test that every metadata command reads and writes at most a fixed number
      of rows and bytes. Multi-row deletes already run in batches of `PAGE_MAX`
      rows.
- [ ] Add a fair queue at the system database lease holder before command
      execution. Queue authenticated requests by account and unauthenticated
      requests by client IP address.

### 3. Run tenant work in workers

Each node manages a pool of workers. A worker serves the databases of a single
owner on that node. The node server forwards each database request to the
owner's worker over HTTP through the `Transport` trait in `cluster.rs`. Nodes
already use `Transport` to forward requests to peers. Peers see only nodes.
Each worker forwards requests to peers and holds leases as a node does today.

Each worker creates a lease identity by adding a new suffix to the node ID on
startup. That suffix satisfies invariant 1. Peers still derive the owning node
from the prefix. In this milestone, workers run inside the server process.
Milestone 4 moves each worker into its own OS process.

If a worker is stopped, all in-flight requests for that owner on that node
fail. Databases owned by that worker cannot accept writes until their leases
expire. That wait takes at most one lease period. SPEC §1 allows the same pause
after a node is killed.

- [ ] Move shared modules into `baserunna-util`. Move the storage engine and
      database features into `baserunna-storage`.
- [ ] Route each database request to an in-process worker for its owner. Give
      each worker its own `Engine`, lease identity, and background jobs. Pass
      the authenticated principal with each request. Stream request and
      response bodies. Run workers inside each simulated node.
- [ ] Stop idle workers after a fixed timeout. Release all held leases before
      stopping.
- [ ] Add a server-side store guard that rejects reads of tenant objects in
      tests and in the simulator.
- [ ] Run the system database in its own worker. Handle metadata commands,
      account routes, and organization routes in that worker. Read metadata
      records from it for authentication and routing.
- [ ] Move connections, authentication, and routing into `baserunna-server`.
      Add the `cargo tree` CI check. Rename the remaining `baserunna` crate to
      `baserunna-worker`.

### 4. Run each worker in its own process

Each worker runs in its own OS process inside a per-owner Linux cgroup. The
kernel enforces cgroup limits across all memory allocations and CPU time,
including third-party dependencies. In-process counters miss allocations
outside our tracking code.

- [ ] Record absolute target numbers for the worker pool before running
      benchmarks. Include these metrics:
  - CPU time per request
  - Hot read latency
  - Memory and CPU usage with 1,000 active owners on a node
  - Duration of each event query in the harness
  - Milestone 0 isolation harness results

  Baseline reference: an empty Unix socket round trip adds 25.6 µs of CPU time
  per request, measured by `BENCH_BOUNDARY` on branch `iso/floor`.
- [ ] Find the cause of the 81% loss of KV throughput in `iso3/cgroup`.
- [ ] Move each worker from milestone 3 into its own process and benchmark it.
      If any metric misses its target, stop and revise the plan.
- [ ] Enforce `memory.max` and `cpu.max` on each owner cgroup. Assign an equal
      `cpu.weight` to each worker. On hosts without cgroup v2, run workers
      without limits and log a startup warning.
- [ ] Set default limits from the harness results. Let administrators store
      per-owner overrides in the system database. Apply limit updates
      immediately to the running worker's cgroup. A single event query can use
      1 GiB today. In the contest, a 512 MiB worker limit caused 19 of 40 event
      query runs to fail, while `main` answered all 40.
- [ ] Add a soft memory limit at worker request admission. Read live heap bytes
      from a counting global allocator. Return HTTP 503 if the worker is near
      its memory limit. Do not use cgroup `memory.high`. In `iso3/cgroup`,
      `memory.high` stalled workers for 120 s.
- [ ] Place every owner cgroup inside a parent `tenants` cgroup. Set
      `memory.max` on `tenants` to total node memory minus a reserve for the
      server and the system worker. Enable `memory.oom.group` on each owner
      cgroup. If total tenant memory exceeds this limit, the kernel kills the
      worker that uses the most memory. The server and system worker stay
      outside the `tenants` cgroup, so the kernel does not kill them.
- [ ] Test the limits with unseen adversarial workloads.
- [ ] Remove the in-process memory counter in `engine/memory.rs`. Removing it
      also removes per-user memory fairness within an organization.

### 5. Limit per-tenant cache and object store usage

- [ ] Limit per-tenant block cache usage.
- [ ] Limit per-tenant disk cache usage.
- [ ] Limit per-tenant object store request rates.

## Rules for each PR

- Make each PR self-contained so we can merge it to `main` independently.
- Make sure every PR passes the simulator.
- Keep operation algorithms unchanged unless the PR targets that algorithm.
- Run this plan without a fixed deadline.

## Decisions

### 2026-11-25

- **Default limits with per-owner overrides.** Administrators can assign higher
  limits to paying customers than to free customers.
- **Process worker pools only.** We cannot compile milestone 3 workers to
  WebAssembly. In the contest, the WebAssembly sandbox ran event queries 1.4 to
  9.0 times slower.
- **Linux-only enforcement.** Enforcement requires a delegated cgroup v2
  subtree. Development on macOS is supported without enforcement.
- **Shared organization limits.** Users within an organization share the
  organization's limits. We do not enforce fairness across users inside an
  organization yet. If a user reaches the limit, other users in that
  organization receive retryable HTTP 503 errors.
- **Per-owner limits.** A user can create up to 100 organizations. Each
  organization is a separate owner. In practice, deployments either run a
  single organization or bill per organization.
- **Node-local workers.** Each node owns its workers locally. Peers see only
  nodes. Cluster membership, routing, and placement do not change.
- **Limits without guarantees.** Tenants receive resource limits rather than
  resource guarantees. Guarantees require a capacity-aware placement scheduler.
  Baserunna does not have a placement scheduler.
- **Resource isolation only.** This goal isolates compute and storage
  resources, not data access. All workers share the same object store
  credentials. We titled the goal "Tenant resource isolation" to reflect this
  scope.
