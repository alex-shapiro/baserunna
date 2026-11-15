# Tenant Isolation: Design Contest

## Decision

**No design is ready to adopt. The next step is one experiment: put all
tenant work inside a WebAssembly boundary and check its cost.** Two
boundary designs went into the contest: a worker pool under cgroups, and
a WebAssembly sandbox. Both kept a tenant inside its memory bound on the
paths that they covered. Both failed the two hidden workloads, because
each hidden workload used a path outside the boundary. A boundary
protects a path that no one has named only if all tenant work crosses
it. A process boundary cannot carry all work: its cheapest crossing
halves the throughput of light requests. A WebAssembly crossing costs
2.6%, so it is the only boundary that can carry all work. Its open risk
is the speed of code in the sandbox: event queries ran 2.6 times slower.
If that experiment fails its cost limits, every remaining design
changes the product. You would then decide between them.

Each claim below has a label:

- **measured:** a judge result file (`<node>-<workload>-<variant>.json`,
  summarized in `reports/tenant-isolation-results.json`) or a named
  benchmark run;
- **code:** file:line;
- **inferred:** not checked by a test or a source.

A **tenant** is the owner of a database: the user or organization in the
`owner_id` column of the `databases` table (SPEC §8). SPEC §1 asks for low
cost per database, not per user, and an organization pays for its databases.
So a member who sends requests does not get a separate share.

## Evidence

### Contest results

The rules in section 3 were committed in fa21df4, before either
prototype existed. A judge agent applied them. The verdict is in commit
09868b4. Each cell is the attack variant. Growth is node memory above
its value at the start, and the bound is 576 MiB.

| Workload                     | Today's code  | cgroup         | WebAssembly    |
| ---------------------------- | ------------- | -------------- | -------------- |
| long_strings                 | node killed   | pass, 517 MiB  | pass, 403 MiB  |
| bulk_inserts                 | node killed   | fail, 717 MiB  | node killed    |
| merges                       | node killed   | pass, 537 MiB  | pass, 468 MiB  |
| blob_checks                  | fail, 891 MiB | pass, 526 MiB  | pass, 254 MiB  |
| txn_scans (hidden)           | node killed   | node killed    | node killed    |
| table_filter_scans (hidden)  | p99 758 ms    | p99 328 ms     | p99 487 ms     |

All of these are measured. The victim's p99 limit in `table_filter_scans`
was about 57 ms. Each design passed 3 of the 4 known workloads, and
neither passed a hidden workload. Under rule 3.6 step 5, neither wins.

**Where each boundary held, the refusal was clean.** Over-limit work
ended with 413 or 503, and the node survived (measured,
`cgroup-blob_checks-attack.json`, `wasm-blob_checks-attack.json`).

**Every failure was on a path outside the boundary:**

- `txn_scans`: the scan ops of one transaction build whole pages of
  values in the node process (code, `crates/baserunna/src/ops.rs:500`).
  Neither design moved this path.
- `table_filter_scans`: table queries share the node's CPU. The only
  control is a rate limit that counts requests, not their cost (code,
  `crates/baserunna/src/limits.rs:70`). Neither design moved table
  queries.
- cgroup `bulk_inserts`: the node process holds each whole body
  outside the owner's cgroup (code, `iso/cgroup`
  `crates/baserunna/src/api/events.rs:300`).
- WebAssembly `bulk_inserts`: the JSON parse stays on the host (code,
  `iso/wasm` `crates/baserunna/src/events/bulk.rs:160`).

### Costs

| Cost                                 | Today's code | cgroup         | WebAssembly    |
| ------------------------------------ | ------------ | -------------- | -------------- |
| Throughput, median of 3              | 179,599 op/s | +0.7%          | −1.9%          |
| Event scan, median query             | 43 ms        | 61 ms (+41%)   | 113–116 ms     |
| "latest rows" query                  | 1.2 ms       | 10.5 ms        | 3.1 ms         |
| "top users" query                    | answered     | always refused | answered       |

All of these are measured. The throughput benchmark runs only KV
requests, which neither prototype moved. So it does not measure the
boundaries.

### The cost of putting all work behind a boundary

This experiment adds two boundary crossings to every request of the
throughput benchmark: one for the request and one for the response. The
far side does no work (measured, `BENCH_BOUNDARY` on `iso/floor` at
cf7262c, 3 rounds in turns):

| Crossing                        | ops/s   | Change | CPU per op |
| ------------------------------- | ------- | ------ | ---------- |
| none                            | 182,399 | –      | 18.2 µs    |
| Unix socket to a warm worker    | 91,930  | −49.6% | 36.6 µs    |
| call into a warm instance       | 177,592 | −2.6%  | 18.7 µs    |

A real worker needs only one round trip. That would cut the process cost
to about 25%, which is still 5 times the 5% limit (inferred). So a
process boundary can carry heavy work only.

## Risks

- **Code in the sandbox is slower.** Event queries took 2.6 times as
  long (measured). Each call builds a new engine with an empty block
  cache (code, `iso/wasm` `crates/baserunna-guest/src/lib.rs:434`). That
  cache may cause most of the slowdown (inferred).
- **The floor is a lower bound.** The benchmark's instance was warm and
  the call was synchronous. The prototype starts a new instance for each
  call and uses async calls with fuel (code, `iso/wasm`
  `crates/baserunna/src/sandbox.rs`). Real store calls add crossings.
- **No CPU share per owner exists yet.** Fuel stops an instance at any
  instruction, but nothing resumes instances in a fair order by owner
  (code, `iso/wasm` `crates/baserunna/src/sandbox.rs`).
- **Host copies sit outside the bound.** Examples are the snapshot
  manifest for each query and the store client's buffer for each reply
  (code, `iso/wasm` `crates/baserunna/src/sandbox.rs`).
- **The simulator does not run the sandbox yet.** `cargo test --test sim`
  passed with the sandbox off (measured by the advocate). Fuel and a
  seeded WASI clock should make the sandbox deterministic (inferred).
- **The prototype removes a guard.** The guest runs queries with an
  unlimited grant, so the node's 256 MiB query budget no longer applies
  inside the sandbox (code, `iso/wasm`
  `crates/baserunna-guest/src/lib.rs:324`).
- **Each workload ran once.** The hidden failures are far from their
  limits, so the verdict does not depend on noise (measured). The
  narrowest passes, such as cgroup `blob_checks` at 526 MiB, could
  change on a second run.

## The experiment that decides

**Full WebAssembly coverage.** All tenant request work runs inside one
persistent instance per owner. The work includes KV ops, transactions,
table queries, streams, event queries, and background jobs. The instance
keeps its engine state and block cache across calls, inside the owner's
memory bound. Epoch interruption stops instances, and a scheduler
resumes them in a fair order by owner.

The design wins if all of these hold:

- the six workloads in this report and two new blind workloads pass
  §3.3;
- throughput falls by at most 5%;
- each event query takes at most 50% longer than today. This rule reads
  the limit per query, because an overall median hides a refused query.

If it fails only on cost, the remaining designs change the product. One
example: heavy work in a boundary, plus a byte cap on each response and
a CPU budget for each light request. That choice would be yours.

I did not run this experiment. It is a full rebuild of the request path.
At the pace of this contest, it needs about ten agent rounds of
20 minutes (inferred).

## Reproduction

- Branches: `iso/base` (harness and this report), `iso/cgroup`
  (400f6a3), `iso/wasm` (e4b39fa), `iso/hidden` (the hidden workloads,
  merged into `iso/base`), and `iso/floor` (the crossing benchmark).
- One workload: in the VM, run
  `scripts/isolation/run.sh <workload> <small|attack> <out.json> <node binary>`.
  For the WebAssembly node, set `BASERUNNA_GUEST_WASM`.

## 1. Requirements

### 1.1 Tenant actions without a bound

Each item is a tenant action whose cost has no per-tenant bound today.
Sources are code (file:line) or measured (test name in `TODO.md`).

**Memory**

- A JSON request body is parsed whole into a value tree before any row limit
  applies. Event rows stay as `serde_json::Value` trees
  (`crates/baserunna/src/api/mod.rs:243`,
  `crates/baserunna-api/src/op.rs:182`). Measured: 32 to 54 times the body
  size (`txn_event_insert_parse_stays_near_body_size`).
- A bulk insert buffers and flushes rows with no memory charge
  (`events/bulk.rs:134`). Measured: 4 inserts held 2 GiB against a 768 MiB
  node budget (`concurrent_bulk_inserts_respect_node_memory_budget`).
- A part merge holds the sort key of every input row
  (`events/maint.rs:540`). Event maintenance starts one task per table with
  no node-wide limit (`events/maint.rs:1017`). Measured: 16 merges held
  1,248 MiB (`event_merges_share_a_node_wide_limit`).
- A query estimates string columns at 4 bytes per row before decoding
  (`events/exec.rs:1567`). Measured: 1,954 MiB against a 1 GiB query limit
  (`long_string_queries_respect_memory_limit`).
- A `check` op with a value reads the whole blob into memory
  (`ops.rs:575`, `ops.rs:453`). Measured: 4 checks of a 256 MiB blob added
  1,024 MiB (`full_blob_reads_respect_node_memory_budget`).

**CPU**

- The rate limit counts requests, not their cost: 500 per second per user
  and per API key (`limits.rs:70`, `node.rs:995`).
- One event query may run 60 s and read 4 GiB (`events/exec.rs:49`).
- Event seals and merges have no node-wide limit (`events/maint.rs:1017`).
  LSM compaction has a node-wide limit of 2, with no order by tenant
  (`engine/db.rs:2235`).

**Store I/O**

- The engine wraps the store in `TimedStore`, which counts requests but
  does not limit them by tenant (`engine/mod.rs:284`).
- A bulk insert writes one part per partition, with no cap
  (`events/bulk.rs:155`). Measured: 10,001 `PUT`s for one request
  (`bulk_insert_partitions_are_limited`).

**Block cache**

- Insertion evicts the next unreferenced entry, whatever its tenant
  (`engine/cache.rs:171`). Event queries fill the shared cache.

**Disk**

- The disk cache is one LRU list for all tenants, 10 GiB by default
  (`engine/local.rs:68`, `main.rs:82`).

**Connections**

- The server accepts connections with no count limit
  (`main.rs:201`).
- The request deadline pauses while a body arrives (`api/body.rs:214`).
  Measured: a stalled body stayed open for 10 minutes
  (`stalled_request_body_times_out`).
- A long poll holds its request for up to 50 s (`api/mod.rs:415`).

### 1.2 What "isolated" means, as a test

A test has two tenants. The **victim** runs normal traffic. The
**attacker** runs one heavy workload. The node runs under a hard memory
limit, as on a host whose memory matches the configuration.

- **Memory.** The node is never killed. With the attacker alone, node
  memory grows by at most the tenant bound. Work over the bound fails with
  an HTTP error status, not a dropped connection.
- **CPU.** During the attack, the victim's slowest 1% of requests take at
  most twice as long as when the victim runs alone, or at most 50 ms
  longer, whichever is larger.
- **Store I/O.** The attacker's store requests per second stay under a
  per-tenant rate. The victim's latency meets the CPU property above.
- **Block cache.** The victim's hit rate drops by at most 5 points while
  its working set fits in its reserved share.
- **Disk.** The attacker's bytes in the disk cache stay under its share.
  The victim keeps its share.
- **Connections.** While the attacker holds open connections, stalled
  bodies, or long polls, the victim can connect and meets the CPU property.

## 2. Design space

Each design names the mechanism that enforces the bound at run time.

**1. A process or VM for each tenant.** Each owner's requests run in a
process or a VM with kernel limits, as in Neon and CockroachDB SQL pods.
The mechanism is the kernel (cgroup or hypervisor). It costs a process per
active owner, a prewarmed pool to resume idle owners, and a forwarding
layer for every request. It puts low cost at risk: a server must host
thousands of active databases, and an idle database must cost no compute.

**2. A worker pool under cgroups.** Work whose cost grows with input or
stored data runs in worker processes. Each active owner has a cgroup with
memory and CPU limits, nested under the node's cgroup. The main process
keeps leases, commits, and light requests. The mechanism is the kernel: the
cgroup refuses memory and the OOM killer ends only that owner's worker. It
costs request and result transfer between processes, worker start-up,
caches that workers do not share, and a Linux-only limit. It puts
testability at risk, because the simulator cannot run kernel limits.

**3. A WebAssembly sandbox.** Heavy work runs as WebAssembly inside the
node process, one instance per request or job. The runtime caps each
instance's linear memory and counts its instructions. The mechanism is the
runtime: a memory grow request over the limit fails, and an instruction
budget or a timer interrupts the instance at any point. I/O goes through
host calls, which the host can order by owner. It costs a slowdown of
compiled code, copies across the sandbox boundary, and a large dependency.
It puts performance at risk for event queries.

**4. A language whose cost the planner bounds.** Queries and inserts must
state bounds the planner can check before it runs them: a time range, a
row limit, a group limit, and a body size. The planner computes the worst
case from part metadata and rejects a plan over the tenant's budget. The
mechanism is admission at plan time. It changes the product: some valid
queries today become invalid. It puts the event table feature at risk, and
it still depends on a correct cost model at run time.

**5. A separate service for event tables.** Event tables move to their own
nodes. KV, tables, and streams no longer share a process with analytics.
The mechanism is process separation by feature, not by tenant. It costs a
second service to deploy and operate. It puts operational simplicity at
risk, and tenants of the event service can still harm each other.

**6. Remove or cap the features with unbounded cost.** Smaller body limits,
a cap on partitions per insert, no value checks on large blobs, a cap on
merge input, and no group-by on high-cardinality strings. The mechanism is
request validation at the API edge. It changes the product. It does not
bound a path that no one has found yet.

**7. Today's design with in-process accounting.** Code charges a shared
memory gate at each site, and the node shares the budget fairly between
users. The mechanism is the charge at each site, so a site without a charge
has no bound. Its cost is low. It puts the node at risk from every path
that lacks a charge. Measured: charges missed 1.0 to 1.9 GiB of heap growth
(earlier contest, branch `resource-mgmt`).

**8. A fallible allocator for each owner.** Our own code allocates tenant
data through an owner-scoped allocator with fallible calls. Over budget,
the allocator returns an error, which becomes an HTTP error. The mechanism
is the allocator, but it covers only our own code. Dependencies, such as
`serde_json`, use the global allocator. It costs a rewrite of collections
in tenant paths.

**9. A global allocator that panics on refusal.** Every allocation is
charged to the current request. Over budget, the allocation panics, as in
ClickHouse and Databend. The mechanism is the allocator. Measured: the
panic crossed library code and aborted the process (earlier contest,
branch `contest/nightly`).

**10. A cost currency with chokepoint admission.** Each owner has a budget
of request units, charged for bytes, store requests, and measured CPU time,
as in DynamoDB and CockroachDB. Gates at the router, the task wrapper, the
cache, and the store order work by owner. The mechanism is admission at
each chokepoint. It bounds work over time, not the memory of one admitted
request.

**11. Placement of noisy owners.** The router moves an owner that exceeds
its share to a separate pool of nodes. The mechanism is placement plus a
node-level limit. It costs a second pool and a control loop. One noisy
owner can still harm others in the pool before the move.

### 2.1 Selection

Only designs 1, 2, 3, and 9 enforce a memory bound for code that no one
has checked. Design 9 aborted the process. Design 1 conflicts with low
cost. So the two designs with the best fit to SPEC §1 are:

- **Design 2, a worker pool under cgroups.**
- **Design 3, a WebAssembly sandbox.**

Neither changes the API or its semantics. Design 2 adds a deployment
requirement: Linux with a delegated cgroup v2 subtree.

## 3. Contest rules

These rules were committed before either prototype existed.

### 3.1 Setup

- Linux VM `ashdev`, 4 vCPUs, 8 GiB.
- The node is the release `baserunna` binary. It runs in cgroup
  `/sys/fs/cgroup/iso/node` with `memory.max` = 1,536 MiB. Every process
  that the node starts must stay inside this subtree. The node has write
  access to the subtree.
- The store is a fake S3 server in a separate process, outside the node's
  cgroup.
- Node settings: block cache 64 MiB, other settings at their defaults.
- The client is a separate process. It creates two owners, the victim and
  the attacker, each with its own database.
- Node memory is the cgroup's `memory.current`, sampled every 10 ms.

### 3.2 Workloads

- **Known.** The four memory workloads from `TODO.md` and the earlier
  contest: long text queries, concurrent bulk inserts, merges in 16 tables,
  and value checks on a 256 MiB blob.
- **Hidden.** Two new workloads. A separate agent writes them after both
  prototypes exist. It does not read the prototypes.

Each workload has a **small variant** that a correct node must complete.

### 3.3 A workload passes if all of these hold

1. **The node survives.** It is not killed and does not restart.
2. **The tenant bound holds.** With the attacker alone, node memory grows by
   at most 512 MiB + 64 MiB above its value at the start of the workload.
3. **The victim is not harmed.** During the attack, the victim has 0 failed
   requests. Its p99 latency is at most the larger of 2 times its p99
   alone and its p99 alone + 50 ms.
4. **The refusal is clean.** Each attacker request over the bound ends with
   an HTTP error status within 2 times the request deadline.
5. **The small variant succeeds.**

### 3.4 Costs

- **Throughput.** `cargo bench --bench throughput` at default settings, 3
  runs, median ops/s. The limit is a loss of 5%.
- **Event query cost.** The event scan benchmark (1,000,000 events in 32
  parts, 7 queries, 20 rounds), run through the node over HTTP. The limit is
  50% more median wall time per query.
- **Tests.** The library tests and the simulation tests must pass.

### 3.5 Rules for candidates

- A candidate may change where code runs and under which limits.
- A candidate may not change the algorithms or the allocations of the code
  that the workloads exercise. Example: it may run `merge_parts` in a
  sandbox. It may not make `merge_parts` stream its input.
- The judge reads the diff to check this rule.

### 3.6 How the winner is chosen

1. The candidate that passes more hidden workloads wins.
2. On a tie, the candidate that passes more known workloads wins.
3. A candidate over a cost limit loses to one inside all cost limits.
4. On a remaining tie, lower victim p99 latency wins, then the smaller
   diff.
5. If neither candidate passes a hidden workload, neither wins. The report
   then names the experiment that would decide.

Today's code (`main`) runs as a control. It is not a candidate.
