# Brief: Isolation harness on `main` (ISOLATION_PLAN.md, milestone 0)

## Problem

The isolation plan has five milestones. After each milestone, we must measure
whether one tenant can harm another. The harness that does this measurement is
only on branch `iso/base`, a contest branch that we will not merge. Without the
harness on `main`, no milestone has a baseline or a pass check.

## Necessity

If we do nothing, each milestone PR must copy the harness from a stale branch,
and we cannot compare results with a recorded baseline. This is the first
milestone, and the other milestones depend on it.

## Request

The request is the real problem. The plan already names the harness and the
pass criteria.

## Precedent

A standard noisy-neighbor test runs a victim workload alone, then runs it with
an attacker, and compares latency percentiles (inferred from general practice;
I did not check a specific system). The harness does the same. It also runs the
node in a memory cgroup, so that a node that runs out of memory stops, as it
would in production.

## Cost

- No new user concepts, options, or failure modes. The harness is an example
  binary and scripts. The node does not change.
- The fake S3 test support gets two settings: a page size for list requests,
  and a fixed delay for each request.
- About 2,300 lines to maintain, almost all of them copied from `iso/base`.

## Size

- Copy the harness, the fake S3 server, and `run.sh` from `iso/base`.
- Change the pass criteria to the five in the plan. The contest had a sixth
  criterion: a fixed 576 MiB limit on node memory growth. The plan removes it,
  because milestone 4 sets the limits from the harness results. The harness
  still records the growth.
- Add `build.sh`, `measure.sh`, and `summarize.py` to build, run every
  workload, and print a results table.
- Estimate: 200 changed lines over the copy.

## Verdict

Build it. The plan requires it, and every later milestone depends on it.

## Design

The harness from the contest is the simplest working design. It already runs
on today's code, on a real kernel. The simulator can also check most of the
criteria if we add a tenant memory model, and milestone 0 now plans that work.
The simulator cannot measure real CPU latency, so the VM harness is still
necessary. No contest is necessary.

## Decisions during the build

- The harness keeps its own HTTP client and does not use `baserunna-client`.
  The client crate has no endpoints for event tables, table row inserts, or
  table queries. An attacker must also control the exact bytes on the wire and
  see each raw status, without retries.
- The user moved two simulator items into milestone 0: a tenant memory model
  with a counting allocator, and simulator scenarios for the workloads. The
  milestone 1 limit PRs need those checks. This branch does only the first
  milestone 0 item.
- Results go in commit messages and PR descriptions, not in the plan.

## Check of the result

- Does the result solve the problem? Yes. The harness runs on `main`, and the
  baseline is in the commit message of the baseline commit. `main` passes 1 of
  8 workloads.
- Would I choose the same design today? Mostly. The phase with the attacker
  and the victim together is too short: the victim sends only
  about 470 requests, so one slow request can fail criterion 3. A fixed attack
  length would make criterion 3 more reliable.
- What can we delete? No script uses the `--find-saturation` mode of
  `kv_throughput`. Milestone 4 needs it to set its throughput targets.
- Size: about 350 lines changed over the copy, under twice the estimate.
