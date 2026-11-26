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

These limits are fixed for each deployment. A tenant can reach them only after
it runs one command for each row. Thus the cost of the large command is paid in
advance by many small commands from the same account. I keep these deletes in
one transaction and correct the plan text.

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

The test bounds each command by the number of metadata rows that it can touch.
Each row costs at most a fixed number of store entries and bytes, because the
system tables have a fixed set of indexes and limits on field lengths.

- Most commands touch a fixed number of rows.
- Batch deletes touch at most `PAGE_MAX` rows.
- `delete_user` and `delete_org` touch at most the per-owner limits.

Other approaches:

- Do nothing. This fails the need of the fair queue.
- Enforce a row limit at run time in each system transaction. A command that
  exceeds the limit fails for the user. A test finds the problem before
  release, without a new failure mode. We can add the run-time limit later.
- Batch `delete_user` and `delete_org`. This makes partial deletes visible to
  users and adds code. The per-owner limits already give a fixed maximum.
