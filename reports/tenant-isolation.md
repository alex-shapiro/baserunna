# Tenant Isolation: Design Contest

## Decision

**No design qualifies, so the choice is yours. I recommend the worker
pool under cgroups.**

- **Workloads:** the worker pool passed 6 of the 8 workloads in round 2.
  The WebAssembly sandbox passed 4.
- **Hidden workloads:** the worker pool passed the hidden CPU workload.
  The sandbox did not pass either hidden workload.
- **Costs:** the worker pool's costs were small. KV throughput and idle
  cost stayed within noise of today's code, and most event queries took
  at most 15% longer. The sandbox ran event queries 1.4 to 9.0 times
  slower.

The worker pool still fails in two ways:
- A light request can download a large blob outside the worker, with a
  memory charge that is too small.
- The node holds each request and response body whole, outside the
  owner's limit.

Both failures come from work that stayed outside the boundary. Round 3
requires all tenant work inside the boundary, with a guard at run time
that proves it. Its prompt is `target/round3-prompt.md` on the machine
that ran this contest.

Each claim below has a label:

- **measured:** a judge run or a named benchmark run. The round 2 judge
  run is in commit eaa6b97. The round 1 judge run is in commit 09868b4.
- **code:** file:line.
- **inferred:** no test or source checked it.

A **tenant** is the owner of a database: the user or organization in the
`owner_id` column of the `databases` table (SPEC §8). An organization
pays for its databases, so each member who sends requests does not get
a separate share.

## Round 2 evidence

### How the contest worked

- **Candidates:**
  - The **worker pool** runs tenant work in one worker process per
    owner. Each worker runs in a cgroup with `memory.max` = 512 MiB.
  - The **WebAssembly sandbox** runs tenant work in wasmtime instances
    in the node process, with a 512 MiB bound per owner.
- **Advocates:** a separate agent built each design as its strongest
  advocate would. Each agent had 6 rounds of 20 minutes.
- **Setup:** the node runs in a cgroup with `memory.max` = 1,536 MiB.
  The fake object store adds 20 ms to each request.
- **Tenants:** a victim owner sends normal traffic. An attacker owner
  runs one workload.
- **Pass conditions:** a workload passes if all of these hold:
  - The node survives.
  - Node memory grows at most 576 MiB with the attacker alone.
  - The victim has no failures.
  - The victim's p99 stays within the larger of two limits: 2 times its
    p99 alone, or its p99 alone + 50 ms.
  - Over-limit requests get HTTP errors.
  - A small variant of the workload succeeds.
- **Hidden workloads:** an agent wrote two hidden workloads after both
  prototypes were final. That agent never saw either prototype.
- **Costs:** costs are measured and reported. They are not pass or fail
  limits.
- **Judging:** a separate judge agent applied the rules.
- **Full rules:** commits 854d4e3 and 75ff637 hold the full rules. Both
  came before any round 2 result.

### Isolation

Each cell is the attack variant, from one judge run.

| Workload                      | Today's code  | Worker pool   | WebAssembly    |
| ----------------------------- | ------------- | ------------- | -------------- |
| long_strings                  | fail, 882 MiB | pass, 325 MiB | not measured   |
| bulk_inserts                  | node killed   | fail          | fail, 829 MiB  |
| merges                        | node killed   | pass, 105 MiB | pass, 492 MiB  |
| blob_checks                   | fail, 1021    | pass, 500 MiB | pass, 265 MiB  |
| txn_scans                     | node killed   | pass, 451 MiB | pass, 508 MiB  |
| table_filter_scans            | p99 818 ms    | pass, 68 ms   | pass, 46 ms    |
| blob_downloads (hidden)       | node killed   | fail, 929 MiB | node killed    |
| stream_filter_scans (hidden)  | p99 2,905 ms  | pass, 69 ms   | p99 1,764 ms   |

All of these are measured.

- **Worker pool, `bulk_inserts`:**
  - The 4 MiB setup insert needs about 580 MiB with today's JSON parse
    (measured on today's code). So the worker refused it with a clean
    413.
  - The small variant needs about 1 GiB (inferred).
  - To pass this small variant, a candidate must change the parse, and
    the rules forbid that change. `TODO.md` already lists the cost of
    the parse as a blocker.
- **Worker pool, `blob_downloads`:**
  - Blob downloads are on the worker pool's light list, so they run in
    the node process.
  - Each download is charged 1 MiB (code, `iso2/cgroup`
    `crates/baserunna/src/worker.rs:204`), but each one holds an 8 MiB
    chunk.
  - That is a hand-written charge at one site, which is the weakness of
    today's design.
- **Worker pool, event scan:**
  - The worker pool refused the "top users" query in 19 of 40 runs,
    when its worker reached the 512 MiB owner limit.
  - Today's code answered it in 40 of 40.
- **WebAssembly:**
  - The two hidden workloads use routes that the build never moved into
    the sandbox (code, `iso2/wasm`
    `crates/baserunna/src/api/mod.rs:629`, `:651`).
  - The `long_strings` setup got a retryable 503 at the owner bound, so
    its attack never ran.
  - One library test fails.

### Costs

| Cost, against today's code      | Worker pool                  | WebAssembly     |
| ------------------------------- | ---------------------------- | --------------- |
| KV throughput, 256 clients      | −1.0%                        | −0.1%           |
| CPU per KV request              | +1.3%                        | −0.4%           |
| Idle memory, 1,000 owners       | +7%                          | +97 MiB, fixed  |
| Event query, median per query   | −2% to +15%, one query +45%  | 1.4× to 9.0×    |
| Node CPU for the event scan     | −11%                         | +88%            |
| Victim p99 with no attacker     | about +33%                   | about the same  |

All of these are measured.

- Neither build sends KV requests across its boundary. So the KV numbers
  do not measure either boundary.
- The sandbox's fixed 97 MiB comes from the compile of the guest module
  at start (inferred).
- The worker pool's extra victim latency has no known cause.

### The cost of a crossing on every request

This benchmark adds two boundary crossings to every request: one for
the request and one for the response. The far side does no work. The
benchmark ran with real HTTP, a 20 ms store delay, and 1,024 clients.
Measured with `BENCH_BOUNDARY` on branch `iso/floor` at e7085b8:

| Crossing                        | ops/s   | Change | CPU per op |
| ------------------------------- | ------- | ------ | ---------- |
| none                            | 102,669 | –      | 33.8 µs    |
| Unix socket to a warm worker    | 59,157  | −42.4% | 59.4 µs    |
| call into a warm instance       | 95,512  | −7.0%  | 36.2 µs    |

## Before adoption

**Work that closes the worker pool's gaps:**

- **Bounded buffers.** Stream request bodies to the worker, and results
  back, through bounded buffers. Today the node buffers both whole
  (code, `iso2/cgroup` `crates/baserunna/src/worker.rs:487`, `:819`).
- **No light list.** Run blob downloads, and all other light work,
  inside the worker.
- **Crossing cost.** Measure the real cost of the worker crossing on
  light requests. The benchmark above shows 42% for an empty round
  trip.
- **Victim latency.** Find the cause of the victim's extra 33% latency
  with no attacker.

**Product decisions for you:**

1. **The tenant memory bound.**
   - At 512 MiB, the worker pool refuses an event query that today's
     code answers.
   - It also refuses a 4 MiB bulk insert of empty rows.
   - A larger bound accepts both. A smaller worker cache, or a fix of
     the JSON parse, also helps.
2. **The deployment requirement.**
   - The worker pool needs Linux with a delegated cgroup v2 subtree.
   - The simulator runs with workers off, so it does not test the
     kernel bound.

## Round 1 in brief

- **Rules:** round 1 ran under the rules in commit fa21df4.
- **Verdict:** both designs passed 3 of the 4 known workloads. Neither
  passed a hidden workload. The verdict is in commit 09868b4.
- **Cause:** both hidden workloads used paths that I had kept out of
  the boundaries:
  - My prompt told the worker pool to keep "light requests" in the main
    process.
  - I cut both plans to the four known workloads to save time.
- **Measurement flaws:**
  - The store had no delay.
  - The throughput benchmark skipped the network.

  Round 2 fixed both flaws.

## Requirements

### Tenant actions without a bound in today's code

Each item below is a tenant action whose cost has no bound for each
tenant.

**Memory**

- **JSON request bodies.** A JSON request body is parsed whole into a
  value tree before any row limit applies.
  - Code: `crates/baserunna/src/api/mod.rs:243`,
    `crates/baserunna-api/src/op.rs:182`.
  - Measured: 32 to 54 times the body size.
- **Bulk inserts.** A bulk insert buffers and flushes rows with no
  memory charge (code, `events/bulk.rs:134`).
- **Part merges.** A part merge holds the sort key of every input row.
  Event maintenance has no limit across the node (code,
  `events/maint.rs:540`, `:1017`).
- **Query estimates.** A query estimates string columns at 4 bytes per
  row before it decodes them (code, `events/exec.rs:1567`).
- **Blob value checks.** A `check` op with a value reads the whole blob
  into memory (code, `ops.rs:575`, `ops.rs:453`).
- **Transaction results.** A transaction keeps every op result until it
  ends, and each scan returns up to 1,000 values (code, `ops.rs:463`,
  `ops.rs:500`).
- **Blob downloads.** Each blob download holds one 8 MiB chunk, and
  nothing limits how many run at once (code, `api/body.rs:22`,
  `engine/config.rs:137`).

**CPU**

- **Rate limit.** The rate limit counts requests, not their cost (code,
  `limits.rs:70`).
- **Event queries.** One event query may run for 60 s and read 4 GiB
  (code, `events/exec.rs:49`).
- **Table and stream queries.** A table query examines up to 10,000
  rows. A stream query examines up to 100,000 records (code,
  `table/mod.rs:1115`, `stream/mod.rs:29`).

**Store I/O, cache, disk, and connections**

- **Store requests.** The store client counts requests, but it does not
  limit them for each tenant (code, `engine/mod.rs:284`).
  - Measured: one bulk insert made 10,001 `PUT`s.
- **Block cache.** The block cache evicts entries without regard to
  their tenant (code, `engine/cache.rs:171`).
- **Disk cache.** The disk cache is one LRU list for all tenants (code,
  `engine/local.rs:68`).
- **Connections.** The server has no limit on connections (code,
  `main.rs:201`).
- **Request bodies.** The request deadline pauses while a body arrives
  (code, `api/body.rs:214`).

### Design space

| Design | Mechanism that enforces the bound | Main risk to SPEC §1 |
| --- | --- | --- |
| Process or VM per tenant | the kernel or hypervisor | low cost: thousands of active databases per server |
| Worker pool under cgroups | the kernel's cgroup limit | testability: the simulator cannot run kernel limits |
| WebAssembly sandbox | the runtime's memory limit and fuel | performance of code in the sandbox |
| Query language with bounded cost | admission at plan time | the product: some valid queries become invalid |
| Separate service for event tables | process separation by feature | operational simplicity |
| Remove or cap costly features | request validation | the product |
| Today's in-process accounting | a charge at each site | any site without a charge |
| Fallible allocator per owner | the allocator, for our own code only | dependencies that allocate |
| Global allocator that panics | the allocator | measured: the panic aborted the process |
| Cost currency with admission | admission at chokepoints | does not bound one admitted request |
| Placement of noisy owners | placement plus node limits | harm before the move |

Only four of these designs bound memory for code that no one has
checked: the first three, and the allocator that panics.

- The allocator that panics aborted the process (measured on the
  earlier branch `contest/nightly`).
- A process per tenant conflicts with low cost.

So the contest chose the worker pool and the WebAssembly sandbox.

## Reproduction

- **Harness and this report:** branch `iso/base`. The harness is
  `crates/baserunna/examples/isolation/`, and the runner is
  `scripts/isolation/run.sh`.
- **One workload:** in the VM, run
  `scripts/isolation/run.sh <workload> <small|attack> <out.json> <node binary>`.
  - For the WebAssembly node, set `BASERUNNA_GUEST_WASM`.
  - Keep the Mac awake during the run, for example with
    `caffeinate -i -s`.
- **Round 2 candidates:** `iso2/cgroup` at 8fca510, and `iso2/wasm` at
  6cee4e4.
- **Round 2 judge scripts:** commit d040b02, under
  `scripts/isolation/judge2/`.
- **Round 1 candidates:** `iso/cgroup` at 400f6a3, and `iso/wasm` at
  e4b39fa.
- **Crossing benchmark:** branch `iso/floor`.
