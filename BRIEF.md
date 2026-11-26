# Brief: Tenant Isolation Goal and Plan

## Problem

The requests of one tenant can crash a node or starve the other tenants on
that node. `TODO.md` has six blockers of the type "Tenant resource
exhaustion". Other tenants see failed requests, and operators see node
crashes.

SPEC §1 did not state isolation as a goal. So the earlier design contest
(`reports/tenant-isolation.md` on branch `iso/base`) had no requirement to aim
at. Its rules changed in each round, and no design qualified.

## Necessity

If we do nothing, the service is not safe for many tenants on a node. The
workload assumptions in SPEC §1 expect hundreds to thousands of active
databases on a server.

## Request

The user asked for a plan that builds isolation carefully, in PR-sized steps,
instead of a single large build with a time limit. This branch has no code.
It adds the isolation goal to SPEC §1, and it adds `ISOLATION_PLAN.md`.

## Precedent

Multi-tenant services isolate tenants in two common ways:

- a process or container for each tenant, under kernel limits such as cgroups;
- a sandboxed runtime, such as the V8 isolates of Cloudflare Workers.

Shared components, such as a front proxy or a metadata service, use a fixed
cost for each request and fair admission. The plan keeps both mechanisms as
candidates, and it chooses between them by measurement after the common work.

## Cost

- The new goal makes the "Tenant resource exhaustion" items in `TODO.md`
  breaks of a SPEC §1 requirement.
- The plan estimates 30 to 40 PRs.
- Operators: the worker pool needs Linux with a delegated cgroup v2 subtree.
- A new failure mode: if a tenant goes over its memory share, all requests of
  that tenant in that boundary fail.

## Size

SPEC §1: about 8 lines. `ISOLATION_PLAN.md`: about 200 lines.

## Verdict

Build it.

## Decisions by the user

- Fairness among the users of an organization is not necessary for now.
- The crate names are `baserunna-util`, `baserunna-storage`,
  `baserunna-worker`, and `baserunna-server`.
- Tenant isolation is goal 4 of SPEC §1, above testability.

## Review round 1

Reviewers: Opus 5.5 and Fable 5.2. Both verdicts: merge after changes. The
Gemini Neon review did not run: `agy` waited for an MCP server and never
called the model.

- Workers are nodes, reached through `Transport` (Fable 2, Opus 4): accepted.
  It removes the frame protocol and nine PRs, and the simulator already runs
  nodes.
- Acceptance test on `main` before the other milestones (Opus 2): accepted.
- A selection rule for the mechanism, recorded before the measurement (Opus 3,
  Fable 3): accepted.
- The failure contract if a worker stops (Fable 4): accepted.
- `engine/` also imports `timing` (Fable 5): accepted.
- What a share is, and who gets one (Opus 1, Fable 1): decided by the user.
- Isolation of data access (Fable, outside the questions): decided by the user.
- Frames in their own crate, and the `cargo tree` check misses the simulator
  (Opus 5): rejected. The frames no longer exist, and the check works for each
  crate, so it also covers the simulator.
- "No process boundary" as a candidate (Fable 3): rejected. Counters failed
  the hidden workloads in all three contest rounds, and they cannot see every
  allocation.
- Delete the testability bullet in SPEC §1 (Fable): rejected. The user asked
  for it.
- Delete the decisions section of this brief (Fable): rejected. This file is
  temporary, and that section informs the reviewers.
- Push `iso/base` and `iso/floor`, which the plan cites (Fable 5): pushed, at
  the request of the user.

## Review round 2

Reviewers: Opus 5.5, Fable 5.2, and Gemini Neon. All three verdicts: merge
after changes.

- Placement, capacity-aware routing, worker membership, and lease takeover
  after a worker dies: rejected by the user as out of scope. Two changes from
  round 1 caused these findings, and both are reverted. Workers belong to
  nodes, and each tenant has limits, not guarantees.
- The kernel OOM killer can stop the server (Fable, Gemini): accepted. All
  owner cgroups go inside a `tenants` cgroup, with `memory.oom.group` on each.
- Idle workers stop (all three): accepted.
- A fourth crate for the shared modules (Gemini): accepted as
  `baserunna-util`.
- Milestone 0 holds a single PR, and the harness run is a rule for each
  milestone: accepted.
- A block cache sized from the worker's limit (Opus): rejected. Invariant 2
  already covers caches.
- Milestone 4 needs milestone 1 (Opus, Fable): rejected. The order already
  holds.
- In-process workers for `fs:` stores (Gemini): replaced by the user's
  decision that macOS runs without enforcement.
- Drop the WebAssembly candidate (all three): not decided in this round.
