# Brief: Fixed cost for each metadata command

## Problem

The lease holder for the system database runs the metadata commands of all
tenants. If the cost of a command grows with stored data, one tenant can make
signup, sign-in, and database creation slow for all tenants. The fair queue in
the next milestone 2 item schedules commands by count. That queue is fair only
if each command has a fixed maximum cost.

Today no test checks this property. A new `query_all` call in a command can
read all rows of a table, and no test fails.

## Necessity

If we do nothing, a future change can add an unbounded scan without a signal.
The fair queue then gives a false guarantee.

## Request

The request is correct: add a test. The plan text is partly wrong. It says that
multi-row deletes already run in batches of `PAGE_MAX` rows. That is true for
sessions and API keys. It is not true for these deletes, which run in one
transaction:

- `delete_user` deletes up to `max_dbs_per_owner` (1,000) personal databases
  and up to `max_orgs_per_user` (100) memberships.
- `delete_org` deletes up to `max_members_per_org` (1,000) memberships.

The per-owner limits set the maximum cost of these commands. SPEC §1 lets an
administrator change the limits for each tenant, so an administrator also
changes this maximum. A tenant can reach the maximum only after it runs one
command for each row. Thus the cost of the large command is paid in advance by
many small commands from the same account. The user chose to delete these rows
in batches of `PAGE_MAX`, so that every command has a fixed maximum cost.

## Precedent

Production databases use per-statement cost accounting. Examples are DynamoDB
read and write capacity units and the CockroachDB per-statement row counters.
We need a test of the maximum, not billing, so a counter on `Txn` is enough.

## Cost

- New concepts: none for users.
- Code: a small cost counter on `Txn` in the storage engine and two tests.
- Failure mode: if a node stops during an account or organization deletion,
  the account or organization keeps some rows until a retry. Each state
  between batches obeys the metadata rules.

## Size

About 40 lines in the engine, about 250 lines of test code, and about 20
lines for the batched deletes.

## Verdict

Build it. Count the entries and bytes that each transaction reads and writes.
Fill every limit through metadata commands, then check that the cost of each
command does not grow when stored data grows. Correct the plan text about batched deletes.

## Design

The test fills every per-owner limit and measures each command. Then it adds
data that no limit caps and measures again. Each command must read and write
the same number of store entries both times. Field length limits bound the
bytes in each entry.

Other approaches:

- Do nothing. This fails the need of the fair queue.
- Enforce a row limit at run time in each system transaction. A command that
  exceeds the limit fails for the user. A test finds the problem before
  release, without a new failure mode. We can add the run-time limit later.
- Keep `delete_user` and `delete_org` in one transaction. Deletion stays
  atomic, but an administrator's per-tenant limit sets the maximum cost, and
  very high limits make account deletion fail.

## Review decisions

Round 1:

- Design 1, compare costs at two data sizes instead of a fitted per-row bound: accepted. The new test catches a full scan of `deleted_databases`, which the old test missed.
- Design 2, delete databases and memberships of `delete_user` and `delete_org` in batches: accepted after the user chose batches. The largest batch reads 7,009 entries instead of 16,605. `DeleteApiKeys` and `UserDeletion::TooManyCredentials` are deleted.
- Design 3, SPEC text records test constants as design: accepted. SPEC now states the rule.
- Design, outside question, the fair queue should charge measured cost instead of command count: reported to the user. It belongs to the next plan item.
- Correctness 1, tombstones make scans slower and `Txn::cost` does not count them: real but rejected for this branch. The fix belongs to the storage engine. Added to `TODO.md`.
- Correctness, `delete_user` can delete 1,000 sessions: rejected. A test showed that a page of exactly `PAGE_MAX` rows returns a cursor.

Round 2:

- Correctness 1, a site administrator's stopped `delete_org` leaves an organization with no owner: accepted. Each batch except the last keeps the actor's membership and the last owner. The test now checks integrity after each batch, and a non-member site administrator deletes `org-big`. The old filter fails it.
- Design 1a, an exhaustive match forces a measurement of each new command: rejected. Rust cannot list enum variants, so the match cannot make the test fail for an unmeasured command.
- Design 1b, assert a maximum for each command: accepted. Each command reads and writes at most `16 * PAGE_MAX` entries.
- Design 2, a stopped account deletion deletes the caller's session first: accepted. Sessions now go last.
- Design cleanups: moved the `delete_org` loop into `Node`, updated this brief, and rewrapped SPEC. Kept the byte counters because the test report prints them.
- Design, outside question, record `Txn::cost` for each command in production: reported to the user. It is outside this branch.
- Correctness, outside question, the simulator never runs a deletion of more than one batch: rejected. The integrity check after each batch covers every point where a node can stop between batches.

Round 3:

- Design 1, a new command is not measured: accepted with new evidence. A macro generates `MetaCmd::name` and `MetaCmd::NAMES` from one exhaustive list, and the test compares the measured names with `NAMES`. With the `leave` worst case removed, the test fails.
- Design, merge `META_BATCH` and `PAGE_MAX`: rejected. Users do not notice it.
- Design, outside question, the reaper's background work has no account to charge: reported to the user for the fair queue item.
- Correctness: no bugs found.
- Correctness, outside question, no per-user limit on API keys and sessions: real and outside this branch. Added to `TODO.md`.
