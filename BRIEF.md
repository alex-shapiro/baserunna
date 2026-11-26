# Brief: Fixed cost for each metadata command

## Problem

The lease holder for the system database runs metadata commands for all
tenants. If command cost increases with stored data, a tenant can slow signup,
sign-in, and database creation for all tenants. The fair queue in the next
milestone 2 item schedules commands by count. That queue is fair only if each
command has a fixed maximum cost.

Tests do not check metadata command cost. A new `query_all` call can scan a
full table while all tests still pass.

## Necessity

Without a test, a future change can add a full-table scan and pass CI. The fair
queue then fails to isolate tenants.

## Request

The request to add a test is right, but the plan text is partly wrong. It says
that multi-row deletes already run in batches of `PAGE_MAX` rows. This applies
to sessions and API keys. Two delete commands still run in a single
transaction:

- `delete_user` deletes up to `max_dbs_per_owner` personal databases. The
  default limit is 1,000. It also deletes up to `max_orgs_per_user`
  memberships. The default limit is 100.
- `delete_org` deletes up to `max_members_per_org` memberships. The default
  limit is 1,000.

We chose to delete these rows in batches of `PAGE_MAX`. Every command then has
a fixed maximum cost. Before this change, per-tenant limits set the maximum
cost of these commands. SPEC §1 lets an administrator change per-tenant limits.
Changing those limits also changes that maximum. A tenant reaches the maximum
only after running a command for each row.

## Precedent

Production databases use per-statement cost accounting. Examples are DynamoDB
read and write capacity units and CockroachDB per-statement row counters. We
need only a test of the maximum cost. A counter on `Txn` is enough.

## Cost

- New concepts: none for users.
- Code: a small cost counter on `Txn` in the storage engine and two tests.
- Failure mode: if a node stops during account or organization deletion, the
  remaining rows stay until the caller retries. Each intermediate state follows
  the metadata rules.

## Size

About 40 lines in the engine, 250 lines of test code, and 20 lines for batched
deletes.

## Verdict

Build it. Count the entries and bytes that each transaction reads and writes.
Fill every limit through metadata commands. Then check that command cost stays
constant after adding unlimited tenant data. Correct the plan text about
batched deletes.

## Design

The test fills every per-owner limit and measures each command. Then it adds
unlimited data and measures again. Each command must read and write the same
entry counts both times. Field length rules limit the bytes in each entry.

Other approaches:

- Do nothing. This breaks the fair queue requirement.
- Enforce a row limit at run time in each system transaction. Commands above the
  limit fail for users. A test finds the problem before release without a new
  failure mode. We can add a run-time limit later.
- Keep `delete_user` and `delete_org` in a single transaction. Deletion stays
  atomic. Per-tenant limits then set the maximum cost. Very high limits can
  make account deletion fail.

## Review decisions

Round 1:

- Compare costs at two data sizes instead of fitting a per-row limit: accepted.
  The new test catches a full scan of `deleted_databases` that the old test
  missed.
- Batch database and membership deletes in `delete_user` and `delete_org`:
  accepted after the user chose batching. The largest batch reads 7,009 entries
  instead of 16,605. We removed `DeleteApiKeys` and
  `UserDeletion::TooManyCredentials`.
- Remove test constants from SPEC: accepted. SPEC now states the rule.
- Charge measured cost in the fair queue instead of command count: reported to
  the user for the next plan item.
- Tombstones slow scans and `Txn::cost` skips them: deferred to the storage
  engine and logged in `TODO.md`.
- `delete_user` deleting 1,000 sessions: rejected. A test showed that reading a
  page of `PAGE_MAX` rows returns a cursor.

Round 2:

- Interrupted `delete_org` by a site administrator leaves an organization
  without an owner: accepted. Every batch except the last keeps the actor's
  membership and at least one owner. The test checks integrity after each
  batch. It also deletes `org-big` as a non-member site administrator.
- Exhaustive match to force measuring new commands: rejected. Rust cannot list
  enum variants. A match does not catch an unmeasured command.
- Assert a maximum entry count per command: accepted. Each command reads and
  writes at most `16 * PAGE_MAX` entries.
- Interrupted account deletion removes the caller's session first: accepted.
  The node now deletes sessions last.
- Code and document updates: accepted. We moved the `delete_org` loop into
  `Node`. We updated this brief and rewrapped `SPEC.md`. We kept the byte
  counters for the test report.
- Record `Txn::cost` in production: reported to the user as out of scope.
- Run multi-batch deletion in the simulator: rejected. Integrity checks after
  each batch cover every stop point between batches.

Round 3:

- Detect unmeasured commands: accepted. A macro generates `MetaCmd::name` and
  `MetaCmd::NAMES` from a single exhaustive list. The test checks that it
  measures every entry in `NAMES`.
- Merge `META_BATCH` and `PAGE_MAX`: rejected. This internal difference is
  invisible to users.
- Reaper background work belongs to the system, not a tenant account: reported
  to the user for the fair queue item.
- Correctness: found zero bugs.
- Unlimited API keys and sessions per user: out of scope for this branch and
  logged in `TODO.md`.
