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

Second question: the unit that gets a share. It can be each owner (a user or
an organization), or each person, so that the organizations that a user creates
share that user's share.

Recommendation: option 2, with a share for each owner, and a lower limit on the
organizations that a user can create.

## Whether isolation also covers data access

Goal 4 isolates resources. Every worker uses the same store credentials
(`fly.toml`). So a bug in a worker can read the objects of another tenant.
Users can read "tenant isolation" as a promise about their data.

Options:

1. Resources only. Rename the goal "Tenant resource isolation", so that it
   makes no promise about data.
2. A guard in each worker that fails each read of another owner's objects. This
   protects against bugs in our code. It costs little.
3. Store credentials for each owner, limited to that owner's prefixes. This
   protects against a worker that an attacker controls. Each owner then needs
   its own store credentials, which adds operational work.

Recommendation: option 2, and rename the goal as in option 1, so that the
promise matches the guard.
