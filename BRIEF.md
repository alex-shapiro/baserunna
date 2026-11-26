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
many small commands from the same account. This branch keeps these deletes in
one transaction and corrects the plan text. `TRADEOFFS.md` asks whether to
delete them in batches.

## Precedent

Production databases use per-statement cost accounting. Examples are DynamoDB
read and write capacity units and the CockroachDB per-statement row counters.
We need a test of the maximum, not billing, so a counter on `Txn` is enough.

## Cost

- New concepts: none for users.
- Code: a small cost counter on `Txn` in the storage engine and one test.

## Size

About 40 lines in the engine and about 250 lines of test code.

## Verdict

Build it. Count the entries and bytes that each transaction reads and writes.
Fill every limit through metadata commands, then run each command and check its
cost against a bound. Correct the plan text about batched deletes.

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
- Batch `delete_user` and `delete_org`. This makes partial deletes visible to
  users and adds code. The per-owner limits already give a fixed maximum.

## Review decisions

Round 1:

- Design 1, compare costs at two data sizes instead of a fitted per-row bound: accepted. The new test catches a full scan of `deleted_databases`, which the old test missed.
- Design 2, delete databases and memberships of `delete_user` and `delete_org` in batches: deferred to the user in `TRADEOFFS.md`. It needs a product tradeoff.
- Design 3, SPEC text records test constants as design: accepted. SPEC now states the rule.
- Design, outside question, the fair queue should charge measured cost instead of command count: reported to the user. It belongs to the next plan item.
- Correctness 1, tombstones make scans slower and `Txn::cost` does not count them: real but rejected for this branch. The fix belongs to the storage engine. Added to `TODO.md`.
- Correctness, `delete_user` can delete 1,000 sessions: rejected. The test shows that exactly 1,000 sessions return `TooManyCredentials`.
