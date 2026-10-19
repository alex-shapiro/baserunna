# Bug zoo

Each patch in this directory reintroduces a historical bug found by the
deterministic simulator (see [`SPEC.md`](../SPEC.md) §13.6 for the numbered
list). The suite measures how quickly a simulation strategy detects known bugs:
apply a patch, run the simulator, and record how many seeds execute before the
first failure.

All patches modify production code only, apply cleanly to `HEAD`
(`git apply bugzoo/NN-*.patch`), and compile. `run.sh` applies each patch
sequentially, builds `sim_tool`, runs the specified `sim_tool` command, and
records the results:

```sh
bugzoo/run.sh campaign all 50
PATCHES="bugzoo/03-*.patch" OUT_DIR=zoo-out bugzoo/run.sh campaign register 300
```

Bug 05 is omitted because it was a nondeterminism bug (`tokio::sync::watch`
wakes waiters in random order) rather than a safety violation, and a source
lint now forbids `tokio::sync::watch`.

## Detection comparison

The table below shows the number of failing runs per patch for the simulator
before the SPEC §13 rewrite (39 fixed scenarios) and after it (21 swarm
scenarios). Round 1 ran ~780 seeds per patch (old: `campaign all 20 1`; new:
`campaign all 37 1`). Round 2 ran ~2,000 additional seeds for patches that had
at most 1 failure in Round 1 (old: `campaign all 52 1000`; new:
`campaign all 96 1000`, plus `explore all 1000` with `SIM_RUNS=2016`). Table
cells are formatted as `round 1` or `round 1 · round 2 campaign · round 2 exploration`.
A failure is counted only if the seed passes without the patch and fails with
the symptom introduced by the patch.

| patch | bug | old | new |
| ----- | --- | --- | --- |
| 01 | read-your-writes across nodes | 335 | 98 |
| 02 | lost routing extensions | 153 | 101 |
| 03 | duplicate versions in group commit | 71 | 43 |
| 04 | in-flight write barrier | 40 | 12 |
| 06 | half-finished deletes | 8 | 16 |
| 07 | zombie requests | 0 · 0 | 1 · 0 · 0 |
| 08 | stale listings | 17 | 9 |
| 09 | stale names after purge | 0 · 0 | 0 · 0 · 0 |
| 10 | missing is not deleted | 0 · 0 | 0 · 0 · 0 |
| 11 | duplicated forwards | 20 | 7 |
| 12 | stale lease reads under clock skew | 23 | 152 |
| 13 | failover livelock | 0 · 0 | 0 · 0 · 0 |
| 14 | snapshot isolation and schema changes | 0 | 3 |
| 15 | collected blobs mid-read | 1 · 0 | 0 · 3 · 1 |
| 16 | duplicated forwards across a restart | 0 · 0 | 0 · 0 · 0 |
| 17 | lease renewals that look alike | 0 · 2 | 1 · 0 · 0 |
| 18 | collection paused by slow clock probes | 1 | 3 |
| 19 | sessions outlive a password change | 0 | 2 |
| 20 | repairs that claimed success | 0 · 0 | 1 · 0 · 0 |
| 21 | transient errors as invalid links | 1 | 5 |
| 22 | lease loss after an ambiguous write | 0 · 0 | 0 · 0 · 6 |
| 23 | retention left to a doubting node | 0 · 0 | 0 · 0 · 9 |
| 24 | status checks past commit retention | 0 · 0 | 0 · 0 · 0 |
| 25 | garbage retained after ambiguous writes | 1 | 4 |
| 26 | self-forwarding after a restart | 0 · 0 | 0 · 0 · 0 |
| 27 | lease state lost after ambiguous writes | 0 · 1 | 0 · 0 · 2 |
| 28 | stale authorization after a slow read | 0 · 0 | 0 · 0 · 0 |

Across both rounds, the old simulator detected 14 of the 27 bugs, the new
campaign runner detected 17, and guided exploration increased total detection
to 20 (uniquely finding 22, 23, and 27). Bugs 09, 10, 13, 16, 24, 26, and 28
were not triggered by the random campaigns; dedicated deterministic tests
(`forwards_to_a_restarted_node_are_refused` and
`earlier_runs_at_this_address_are_not_routed_to`) cover 16 and 26.

## Patch descriptions

- **01** `Node::resolve_db` (`src/node.rs`) returns `NotFound` from a cached
  or snapshot lookup without retrying with a strong read
  (`resolve_db_with(.., true)`).
- **02** When `db_routing` (`src/api/mod.rs`) executes a buffered request
  locally after a forward fails prior to execution, it reconstructs the request
  from method, URI, version, and headers instead of calling
  `Request::from_parts(parts, ..)`, discarding axum extensions.
- **03** `Db::build` (`src/engine/db.rs`) assigns version `base.m.version + 1`
  to all transactions in a group-commit batch instead of `last_seq + 1`.
- **04** `Db::snapshot` skips `wait_for_in_flight_write` on strong reads.
- **06** `db_routing` skips `MetaCmd::DeleteDb` when strong re-resolution maps
  a name to the same deleted database, and `create_db` (`src/api/data.rs`)
  returns `AlreadyExists` without completing a partial database deletion.
- **07** `Db::build` does not reject transactions whose commit deadline has
  expired.
- **08** `list_keys` (`src/api/account.rs`) reads from the system database with
  bounded staleness (`sys_read`) instead of strong consistency (`sys_read_strong`).
- **09** In `db_routing`, `NotFound` from `open_db` (when the manifest has
  already been purged) is returned as HTTP 404 instead of triggering strong
  re-resolution in the system database.
- **10** `create_db` treats `NotFound` from a strong read of the database
  handle as evidence that the database was deleted, even when the manifest for
  a newly created database has not yet been written.
- **11** `dedup_forwarded` (`src/api/mod.rs`) executes forwarded writes without
  deduplicating via `ForwardDedup`.
- **12** `Db::foreign_lease` computes the remaining lease duration using the
  local wall clock (`expires_ms + lease_margin_ms - env.now_ms()`) instead of
  the monotonic instant when the lease was first observed.
- **13** `Engine::evict_idle` (`src/engine/mod.rs`) clears the observed foreign
  lease state when evicting an idle `Db` handle.
- **14** `table::get_schema` (`src/table/mod.rs`) reads the table catalog entry
  with `txn.get` instead of `txn.get_pinned`.
- **15** `blob::read_chunk` (`src/engine/blob.rs`) maps a missing blob chunk to
  HTTP 500 instead of a retryable `503 Unavailable` error.
- **16** `dedup_forwarded` does not reject forwarded requests whose
  `x-baserunna-forward-to` header specifies a different node ID.
- **17** `Shared::lease_seen` identifies a lease renewal by `lease.expires_ms`
  instead of the manifest `write_seq`.
- **18** `StoreClock::sync` (`src/engine/clock.rs`) performs only a single
  probe instead of up to 3 probes, and `Node::housekeeping` does not call
  `refresh_clock`.
- **19** `Session::is_live` (`src/meta/mod.rs`) does not check `password_fp`.
- **20** The partial-deletion repair in `db_routing` ignores errors from
  `DeleteDb` and returns HTTP 404.
- **21** `Node::open_link` maps all errors from reading the user record,
  including retryable storage errors, to HTTP 401.
- **22** In `Db::process`, a batch that encounters a foreign lease returns
  `NotOwner` (`not_applied`) even after a prior manifest `PUT` failed with an
  unknown outcome.
- **23** `Db::trim_for_retention` exits unless the local node holds the lease,
  and `Db::build` does not schedule retention trims when replaying an
  already-committed transaction.
- **24** The `not_committed` response in `src/api/data.rs` reports
  `retention_ms: u64::MAX` instead of the configured commit retention window,
  causing `Client::commit_status` to treat `not_committed` as definitive even
  after commit records have expired.
- **25** `Db::process` skips deleting expired obsolete objects (`plan.gc`) when
  the manifest `PUT` returns an unknown outcome.
- **26** `Node::live_members` and `Node::route` do not filter out peer records
  that have the local node's advertise address.
- **27** After a manifest `PUT` fails with an unknown outcome, the manifest
  reload in `Db::process` does not record the local node's lease when the
  reloaded manifest matches that write.
- **28** `Node::role_for` caches the resolved role using the timestamp when the
  read completed rather than the timestamp before the read started.
