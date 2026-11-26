# Tenant Resource Isolation Plan

We will isolate each tenant in a separate worker process with Linux cgroup
limits. This work meets the goal for tenant resource isolation in SPEC §1. Each
tenant is a database owner: the user or organization in the `owner_id` column
of the `databases` table in SPEC §8.

We will merge this work as small PRs into `main`. `TODO.md` lists bugs. This
file lists the isolation milestones. If a PR fixes a bug in `TODO.md`, remove
that item in the same PR.

## Background

We ran a design contest to build isolation in fixed-length rounds. None of the
designs passed. The report is in `reports/tenant-isolation.md` on branch
`iso/base`.

We learned three lessons from the contest:

- The storage engine needs few changes. In `iso3/cgroup`, each worker ran the
  existing `Engine` code.
- The code outside the storage engine needs large changes. We must split
  startup, routing, metadata commands, and the system database across the
  server and workers.
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
   - Shared work in the server and system database

   If a node does not have enough memory, the kernel stops the tenant that uses
   the most memory first.

## Crate layout

We will split the `baserunna` crate into four crates. This split prevents the
server binary from linking the storage engine:

1. `baserunna-util`: shared support modules and the `Transport` trait.
2. `baserunna-storage`: the storage engine and database features. It depends on
   `baserunna-util`.
3. `baserunna-worker`: tenant request handlers and background jobs. This crate
   contains all tenant code. It depends on `baserunna-storage` and
   `baserunna-util`.
4. `baserunna-server`: network connections, authentication, and request
   routing. It does not depend on `baserunna-storage`.

Cargo builds separate binaries for `baserunna-server` and `baserunna-worker`.
Because `baserunna-server` does not link `baserunna-storage`, the server
process cannot open a database. A `cargo tree` check in CI enforces this rule.
That check replaces the store guard from milestone 3.

`baserunna-util` and `baserunna-storage` are now separate crates.
`baserunna-storage` contains the object store client. `baserunna-util` contains
`Role` and `Access`. The server will send an `Access` value to the worker with
each request.

## Milestones

Each milestone is a group of PRs. Milestones 1 to 3 are independent. Start
them in any order. Milestone 4 depends on milestone 3. Finish with milestone 5.

### 1. Limit memory use for each operation

This milestone limits memory for each operation so large requests succeed
within a tenant limit. These changes also protect users on `main` today.
Process isolation does not require this milestone. Under cgroup limits, an
operation without a memory limit fails at the tenant limit. The node continues
to run.

Each PR adds a test to check that a request of size `N` uses at most
`k * N + C` bytes.

- [x] Move the counting allocator into `tests/support/` and remove the extra
      copies in `memory_bound.rs`, `event_ingest_limits.rs`, and
      `event_query_limits.rs`.
- [ ] Stream-parse JSON request bodies for KV and transaction operations. This
      PR covers part of the `TODO.md` item "Stream JSON request bodies and
      charge bulk insert memory to `MemoryGate`".
- [ ] Stream-parse JSON request bodies for event rows. This PR covers another
      part of that `TODO.md` item.
- [ ] Limit the rows that a bulk insert buffers before a flush. This PR
      completes that `TODO.md` item.
- [ ] Limit partitions per bulk insert. This PR removes the matching `TODO.md`
      item.
- [ ] Merge parts block by block. Split this work into two PRs if needed. This
      change removes the `TODO.md` item "Bound memory usage during part merges".
- [ ] Limit concurrent event maintenance tasks on a node. This PR removes the
      `TODO.md` item "Limit event maintenance concurrency across the node and
      charge its memory".
- [ ] Estimate memory for string columns from actual string lengths. This PR
      removes the `TODO.md` item "Estimate granule run memory from actual
      string lengths".
- [ ] Compare blob values chunk by chunk in `check` operations in `ops.rs`.
- [ ] Limit total result bytes that a transaction holds in `ops.rs`.
- [ ] Limit how many blob chunks a download buffers at once in `api/body.rs`.
- [ ] Stream forwarded request and response bodies through the node. Limit
      `BUFFER_FORWARD_MAX` and cached responses in `ForwardDedup`.
- [ ] Add connection, Hyper header, and Hyper buffer limits in `main.rs`.
- [ ] Time out stalled request bodies. This PR removes the matching `TODO.md`
      item.

### 2. Limit metadata command cost

The lease holder for the system database runs all metadata commands. It limits
the cost of each command and schedules tenants fairly. Tenant workers cannot
isolate metadata commands because many commands read data from several owners.
For example, `create_db` for an organization reads the caller's role in that
organization.

The cluster still has a limit on metadata write throughput. That limit affects
scale, not isolation.

- [x] Set a length limit on every field in a metadata command, including the
      Google `sub` field.
- [x] Test that each metadata command reads and writes at most a fixed number
      of entries and bytes. Delete user and organization rows in batches of
      `PAGE_MAX` rows.
- [ ] Add a fair queue to the lease holder for the system database before it
      runs each command. Queue authenticated requests by account and
      unauthenticated requests by client IP address.

### 3. Run tenant work in workers

Each node runs a pool of workers. A worker serves the databases of a single
owner on that node. The node server forwards each database request to the
owner's worker over HTTP through the `Transport` trait in `cluster.rs`. Nodes
already use `Transport` to forward requests to peers. Peers see only nodes.
Each worker forwards requests to peers and holds leases as a node does today.

Each worker creates a lease identity by adding a new suffix to the node ID on
startup. That suffix meets invariant 1. Peers still find the owning node from
the prefix. In this milestone, workers run inside the server process. Milestone
4 moves each worker into its own OS process.

If a worker is stopped, all in-flight requests for that owner on that node
fail. Clients cannot write to that worker's databases until the leases expire.
That wait takes at most one lease period. SPEC §1 allows the same pause after a
node is killed.

- [x] Move shared modules into `baserunna-util`. Move the storage engine and
      database features into `baserunna-storage`.
- [ ] Route each database request to an in-process worker for its owner. Give
      each worker its own `Engine`, lease identity, and background jobs. Pass
      the authenticated principal with each request. Stream request and
      response bodies. Run workers inside each simulated node.
- [ ] Stop idle workers after a fixed timeout. Each worker releases its leases
      before it stops.
- [ ] Add a store guard in the server that rejects reads of tenant objects in
      tests and the simulator.
- [ ] Run the system database in its own worker. Handle metadata commands,
      account routes, and organization routes in that worker. Read metadata
      records from it for authentication and routing.
- [ ] Move connections, authentication, and routing into `baserunna-server`.
      Move the `Transport` trait into `baserunna-util`. Add the `cargo tree` CI
      check. Rename the remaining `baserunna` crate to `baserunna-worker`.

### 4. Run each worker in its own process

Each worker runs in its own OS process inside a per-owner Linux cgroup. The
kernel enforces cgroup limits across all memory allocations and CPU time,
including dependencies. In-process counters miss allocations outside our
tracking code.

- [ ] Merge the isolation harness from branch `iso/m0-harness`. The harness
      checks if the node serves a victim tenant while an attacker runs a heavy
      workload. A workload passes if it meets all five conditions:
  - The node continues to run.
  - All victim requests succeed.
  - Victim p99 latency is at most the larger of `2 * baseline` and
    `baseline + 50 ms`.
  - The server returns an HTTP error for requests that exceed a limit.
  - A small version of the workload succeeds.

  Under per-owner cgroups, the harness tests isolation. It checks if the kernel
  stops only the attacker. It also checks if victim requests stay fast. On
  `main`, the harness shows only node-wide effects. Milestone 1 and `TODO.md`
  already list the per-operation memory problems that cause most of those
  effects. Filter scans slow the victim by using CPU. The `cpu.max` limits in
  this milestone prevent that slowdown.
- [ ] Record absolute target numbers for the worker pool before you run
      benchmarks. Include these metrics:
  - CPU time per request
  - Hot read latency
  - Memory and CPU use with 1,000 active owners on a node
  - Duration of each event query in the harness
  - Isolation harness results

  On branch `iso/floor`, `BENCH_BOUNDARY` measured 25.6 µs of CPU time per
  request for an empty round trip over a Unix socket.
- [ ] Find the cause of the 81% loss of KV throughput in `iso3/cgroup`.
- [ ] Move each worker from milestone 3 into its own process and benchmark it.
      If any metric misses its target, stop and revise the plan.
- [ ] Enforce `memory.max` and `cpu.max` on each owner cgroup. Set an equal
      `cpu.weight` for each worker. On hosts without cgroup v2, run workers
      without limits and log a startup warning.
- [ ] Set default limits from the harness results. Let administrators store
      per-owner overrides in the system database. Apply limit updates
      immediately to the running worker's cgroup. A single event query can use
      1 GiB today. In the contest, a 512 MiB worker limit caused 19 of 40 event
      query runs to fail. Today `main` completes all 40.
- [ ] Check a soft memory limit before a worker admits a request. Read live
      heap bytes from a counting global allocator. Return HTTP 503 if the
      worker is near its memory limit. Do not use cgroup `memory.high`. In
      `iso3/cgroup`, `memory.high` stalled workers for 120 s.
- [ ] Put every owner cgroup inside a parent `tenants` cgroup. Set `memory.max`
      on `tenants` to total node memory minus a reserve for the server and the
      system worker. Enable `memory.oom.group` on each owner cgroup. If total
      tenant memory exceeds this limit, the kernel kills the worker that uses
      the most memory. The server and system worker stay outside the `tenants`
      cgroup, so the kernel does not kill them.
- [ ] Test the limits with new attack workloads.
- [ ] Remove the in-process memory counter in `engine/memory.rs`. This change
      also ends per-user memory fairness inside an organization.

### 5. Limit cache and object store use for each tenant

- [ ] Limit block cache use for each tenant.
- [ ] Limit disk cache use for each tenant.
- [ ] Limit object store request rates for each tenant.

## Rules for each PR

- Make each PR independent so we can merge it into `main` on its own.
- Every PR must pass the simulator.
- Keep operation algorithms unchanged unless the PR updates that algorithm.
- Run this plan without a fixed deadline.

## Decisions

### 2026-11-25

- **Default limits with per-owner overrides.** Administrators can set higher
  limits for paying customers than for free customers.
- **Worker processes only.** We cannot compile milestone 3 workers to
  WebAssembly. In the contest, the WebAssembly sandbox ran event queries 1.4 to
  9.0 times slower.
- **Linux-only enforcement.** Enforcement requires a delegated cgroup v2
  subtree. Developers can run Baserunna on macOS without enforcement.
- **Shared organization limits.** Users inside an organization share its
  limits. We do not enforce per-user fairness inside an organization yet. If a
  user reaches the limit, other users in that organization get retryable HTTP
  503 errors.
- **Per-owner limits.** Each organization is a separate owner. A user can
  create up to 100 organizations. Deployments either run a single organization
  or bill for each organization, so this limit is acceptable.
- **Node-local workers.** Each node runs its own workers. Peers see only nodes.
  Cluster membership, routing, and placement do not change.
- **Limits without guarantees.** Tenants get resource limits, not resource
  guarantees. Guarantees require a capacity-aware placement scheduler.
  Baserunna does not have a placement scheduler.
- **Resource isolation only.** This goal isolates compute and storage
  resources, not data access. All workers share the same object store
  credentials. We use the title "Tenant resource isolation" to show this scope.
