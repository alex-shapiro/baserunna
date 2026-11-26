# Brief: Tenant Isolation Goal and Plan

## Problem

A single tenant's requests can crash a node or starve other tenants on that
node. `TODO.md` lists six "Tenant resource exhaustion" blockers. Other tenants
receive failed requests, and operators see node crashes.

SPEC §1 lacked a tenant isolation goal. The earlier design contest lacked a
stable requirement. Its report is in `reports/tenant-isolation.md` on branch
`iso/base`. We changed the contest rules in each round, and none of the designs
qualified.

## Necessity

Without isolation, hosting many tenants on a node is unsafe. SPEC §1 expects
each server to host hundreds to thousands of active databases.

## Request

The user asked for a plan that builds isolation in small PRs instead of a
single time-boxed build. This branch contains documentation only. It adds the
isolation goal to SPEC §1 and adds `ISOLATION_PLAN.md`.

## Precedent

Multi-tenant services isolate tenants in two common ways:

- Run a process or container per tenant under kernel limits such as cgroups.
- Run tenant code in a sandboxed runtime, such as V8 isolates in Cloudflare
  Workers.

Shared components, such as front proxies and metadata services, limit
per-request cost and enforce fair admission. This plan runs a separate process
per owner under Linux cgroups.

## Cost

- The new goal turns the "Tenant resource exhaustion" items in `TODO.md` into
  violations of a SPEC §1 requirement.
- Implementing the plan will take about 30 to 40 PRs.
- Operators must run the worker pool on Linux with a delegated cgroup v2
  subtree.
- If a tenant exceeds its memory limit, all requests for that tenant on that
  node fail.

## Size

SPEC §1 changes about 10 lines. `ISOLATION_PLAN.md` is about 250 lines.

## Verdict

Build it.

## Decisions by the user

- We do not need per-user fairness inside an organization yet.
- The four crates are `baserunna-util`, `baserunna-storage`,
  `baserunna-worker`, and `baserunna-server`.
- Tenant resource isolation is goal 4 in SPEC §1, placed above testability.

## Review round 1

Reviewers: Opus 5.5 and Fable 5.2. Both recommended merging after changes. The
Gemini Neon review did not run because `agy` waited for an MCP server and never
called the model.

- **Workers as nodes reached through `Transport`.** Accepted from Fable 2 and
  Opus 4. This removes the frame protocol and nine PRs, and the simulator
  already runs nodes.
- **Acceptance test on `main` first.** Accepted from Opus 2.
- **Record mechanism selection rules before measuring.** Accepted from Opus 3
  and Fable 3.
- **Failure contract if a worker is stopped.** Accepted from Fable 4.
- **`engine/` also imports `timing`.** Accepted from Fable 5.
- **Definition of a share and who gets it.** Decided by the user after Opus 1
  and Fable 1.
- **Data access isolation.** Decided by the user after Fable's note.
- **Separate crate for frames and `cargo tree` coverage.** Rejected from Opus
  5. The frame protocol is gone, and the `cargo tree` check covers each crate,
  including the simulator.
- **No-process-boundary candidate.** Rejected from Fable 3. Counters failed
  hidden workloads in all three contest rounds because counters cannot track
  every allocation.
- **Delete the testability bullet in SPEC §1.** Rejected from Fable. The user
  requested it.
- **Delete the decisions section of this brief.** Rejected from Fable. This
  file is temporary and gives reviewers context.
- **Push `iso/base` and `iso/floor`.** Pushed at the user's request after Fable
  5.

## Review round 2

Reviewers: Opus 5.5, Fable 5.2, and Gemini Neon. All three recommended merging
after changes.

- **Placement, capacity-aware routing, worker membership, and lease takeover
  after worker death.** Rejected by the user as out of scope. We reverted the
  two round 1 changes that caused these findings. Workers belong to nodes, and
  tenants receive limits rather than guarantees.
- **Kernel OOM killer targeting the server.** Accepted from Fable and Gemini.
  We put all owner cgroups inside a parent `tenants` cgroup with
  `memory.oom.group` enabled on each owner cgroup.
- **Idle worker shutdown.** Accepted from all three reviewers.
- **Fourth crate for shared modules.** Accepted from Gemini as
  `baserunna-util`.
- **Single PR in milestone 0 and harness run after each milestone.** Accepted.
- **Block cache sized from worker limit.** Rejected from Opus. Invariant 2
  already covers caches.
- **Milestone 4 dependency on milestone 1.** Rejected from Opus and Fable. The
  existing milestone order is valid.
- **In-process workers for `fs:` stores.** Replaced by the user's decision to
  run macOS without enforcement, after Gemini's note.
- **Drop the WebAssembly candidate.** Accepted by the user after all three
  reviewers raised it.
