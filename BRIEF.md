# Brief: Share the Counting Allocator Across Memory Tests

## Problem

Three test files each had a copy of the same counting allocator:
`memory_bound.rs`, `event_ingest_limits.rs`, and `event_query_limits.rs`.
Milestone 1 of `ISOLATION_PLAN.md` adds a heap test to each of its PRs. Without
a shared module, each new test file adds a fourth copy, or a fix to one copy
misses the others. The cost falls on maintainers, not users.

## Necessity

If we do nothing, the copies continue to drift. They already differ:
`memory_bound.rs` computed peak growth inline as a signed value, and the other
two files used a `peak_growth` helper that clamps at zero. This change is
small, and the next milestone 1 PRs depend on it.

## Request

The request is the real problem. It is a precondition for the other milestone
1 PRs.

## Precedent

Cargo integration tests share code through modules under `tests/`. This
repository already uses `#[path = "support/fake_s3.rs"] mod fake_s3;` in
`store_conformance.rs`. A shared allocator module follows that pattern.

## Cost

No new concepts, options, or failure modes for users. The change deletes about
125 lines. The shared module needs `#![allow(dead_code)]` because each test
binary uses only some helpers.

## Size

Estimate: +70 lines for the module, about −190 lines from the three files.

## Verdict

Build it.

## Design

The candidates:

- Do nothing. This rejects a plan item that unblocks later PRs.
- A `#[path]` module in `tests/support/`. Each test binary gets its own
  `#[global_allocator]` and its own counters. This is the existing pattern.
- A shared library crate or a feature in `baserunna` that exports the
  allocator. This adds a crate or a feature flag for test-only code.

The `#[path]` module wins. It is the smallest change and it matches the
existing `fake_s3` module.

The module exposes `HEAP_MEASUREMENT_LOCK`, `allocated_bytes`, `reset_peak`,
`peak_growth`, and `heap_growth`. The old name `TURN` did not say what it
serializes, so the module renames it.

## Result Check

- The result solves the problem. The three files use one module, and a new
  heap test needs two lines to use it.
- I would choose the same design again.
- Nothing more can be deleted. Each helper has a caller.

## Review Decisions

- Round 1 design review: verdict "merge", no findings. No second round.
- Outside problem from the reviewer: the counting allocator counts requested
  bytes, but a cgroup limit counts RSS. Not accepted for this branch. The claim
  is not measured, and it concerns later milestone 1 PRs, not this refactor.
