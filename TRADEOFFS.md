# Tradeoffs

## Delete the databases and memberships of a user or organization in batches

`delete_user` deletes all personal databases and memberships in one
transaction. `delete_org` deletes all memberships in one transaction. The
per-owner limits set their cost. At the default limits, `delete_user` reads
16,605 entries (5.1 MB) and writes 760 KB. If an administrator raises one
tenant's database limit to about 45,000, the write set passes the 16 MiB
transaction limit (estimated from the measurement). Then that user cannot
delete the account.

Options:

1. Keep one transaction. Account deletion stays atomic. The administrator's
   limit sets the maximum cost, and very high limits block account deletion.
2. Delete databases and memberships in batches of `PAGE_MAX`, then delete the
   user row. Every command then has a fixed maximum cost. If a node stops
   during a deletion, the account keeps some databases and memberships until
   a retry finishes. Session and API key deletes already work this way.
   About 20 more lines.
