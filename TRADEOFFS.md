# Tradeoffs

## What a share is, and who gets one

SPEC §1 goal 4 says that each tenant's use counts against its share, but it
does not define a share. The plan has a fixed bound for each tenant, such as
512 MiB. Nothing limits the sum of the bounds on a node.

- Three active tenants at 512 MiB fill the 1,536 MiB node from the contest.
- A user can create 100 organizations (`max_orgs_per_user`), and each
  organization is a tenant. So one person can hold 101 shares.
- If the node is full, a victim that stays inside its bound can still fail.

Options:

1. Fixed bound only. This is the plan today. It is the simplest option, but it
   does not protect a victim on a full node.
2. Fair share of the node, with a fixed bound as the maximum. Each active tenant
   gets an equal part of the node, and a tenant can use idle capacity up to the
   bound. The code already has this division in `fair_share` and
   `fair_admission` (`engine/memory.rs`). The plan then keeps that logic at the
   admission point of each worker, and it removes only the charges at each call
   site. The share of each tenant becomes smaller as more tenants are active.
3. A reservation for each tenant. A node accepts a tenant only if the sum of the
   bounds fits. This gives the strongest guarantee, but idle reservations waste
   memory, and placement must know the bounds.

Recommendation: option 2.
