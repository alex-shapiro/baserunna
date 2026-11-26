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
- The crate names are `baserunna-storage`, `baserunna-worker`, and
  `baserunna-server`.
- Tenant isolation is goal 4 of SPEC §1, above testability.
