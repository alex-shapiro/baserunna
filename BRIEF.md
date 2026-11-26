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
on today's code. Other designs, such as a test in the simulator, cannot show a
kernel OOM kill or real latency. So no contest is necessary.
