# Brief: Tenant Isolation Goal and Plan

## Problem

A single tenant's requests can crash a node or starve other tenants on that
node. `TODO.md` lists six "Tenant resource exhaustion" blockers. Other tenants
get failed requests, and operators see node crashes.

SPEC §1 lacked a tenant isolation goal. The earlier design contest, documented
in `reports/tenant-isolation.md` on branch `iso/base`, lacked a stable
requirement. We changed the contest rules in each round, and none of the
designs qualified.

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

Shared components, such as front proxies and metadata services, cap per-request
cost and enforce fair admission. This plan runs a separate process per owner
under Linux cgroups.

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

- Per-user fairness inside an organization is out of scope for now.
- The four crates are `baserunna-util`, `baserunna-storage`,
  `baserunna-worker`, and `baserunna-server`.
- Tenant resource isolation is goal 4 in SPEC §1, above testability.

## Review round 1

Reviewers: Opus 5.5 and Fable 5.2. Both recommended merging after changes. The
Gemini Neon review did not run because `agy` waited for an MCP server and never
called the model.

- **Workers as nodes reached through `Transport`** (Fable 2, Opus 4): accepted.
  This removes the frame protocol and nine PRs, and the simulator already runs
  nodes.
- **Acceptance test on `main` first** (Opus 2): accepted.
- **Record mechanism selection rules before measuring** (Opus 3, Fable 3):
  accepted.
- **Failure contract if a worker stops** (Fable 4): accepted.
- **`engine/` also imports `timing`** (Fable 5): accepted.
- **Definition of a share and who gets it** (Opus 1, Fable 1): decided by the
  user.
- **Data access isolation** (Fable): decided by the user.
- **Separate crate for frames and `cargo tree` coverage** (Opus 5): rejected.
  The frame protocol is gone, and the `cargo tree` check covers each crate,
  including the simulator.
- **No-process-boundary candidate** (Fable 3): rejected. Counters failed hidden
  workloads in all three contest rounds because counters cannot track every
  allocation.
- **Delete the testability bullet in SPEC §1** (Fable): rejected. The user
  requested it.
- **Delete the decisions section of this brief** (Fable): rejected. This file
  is temporary and gives reviewers context.
- **Push `iso/base` and `iso/floor`** (Fable 5): pushed at the user's request.

## Review round 2

Reviewers: Opus 5.5, Fable 5.2, and Gemini Neon. All three recommended merging
after changes.

- **Placement, capacity-aware routing, worker membership, and lease takeover
  after worker death**: rejected by the user as out of scope. We reverted the
  two round 1 changes that caused these findings. Workers belong to nodes, and
  tenants receive limits rather than guarantees.
- **Kernel OOM killer targeting the server** (Fable, Gemini): accepted. We put
  all owner cgroups inside a parent `tenants` cgroup with `memory.oom.group`
  enabled on each owner cgroup.
- **Idle worker shutdown** (all three): accepted.
- **Fourth crate for shared modules** (Gemini): accepted as `baserunna-util`.
- **Single PR in milestone 0 and harness run after each milestone**: accepted.
- **Block cache sized from worker limit** (Opus): rejected. Invariant 2 already
  covers caches.
- **Milestone 4 dependency on milestone 1** (Opus, Fable): rejected. The
  existing order holds.
- **In-process workers for `fs:` stores** (Gemini): replaced by the user's
  decision to run macOS without enforcement.
- **Drop the WebAssembly candidate** (all three): accepted by the user.
