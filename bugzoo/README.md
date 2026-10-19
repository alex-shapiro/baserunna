# Bug zoo

Each patch here puts back one bug that the deterministic simulator found
(SPEC.md §13.6, numbered as there). The zoo measures how quickly a
simulation strategy rediscovers known bugs: apply a patch, run the
strategy, and count the seeds until the first failure.

Patches touch production code only, apply to the branch's head
(`git apply bugzoo/NN-*.patch`), and compile. `run.sh` applies each patch
in turn, builds `sim_tool`, runs the `sim_tool` command it is given, and
records the results:

```sh
bugzoo/run.sh campaign all 50
PATCHES="bugzoo/03-*.patch" OUT_DIR=zoo-out bugzoo/run.sh campaign register 300
```

Bug 5 has no patch. It was nondeterminism (`tokio::sync::watch` wakes
waiters in random order). A campaign cannot observe it, and a lint now
forbids `watch` in the crate.

## Detection by the old and the new simulator

How many runs failed with each bug, for the simulator before SPEC §13's
rewrite (39 fixed scenarios) and after it (21 swarm scenarios). Round 1
gave each strategy about 780 runs per patch (old: seeds 1–20 of every
scenario, `campaign all 20 1`; new: `campaign all 37 1`). Round 2 gave the
bugs that round 1 caught at most once about 2,000 more runs each, at
other seeds (old: `campaign all 52 1000`; new: `campaign all 96 1000`, and
`explore all 1000` with `SIM_RUNS=2016`). Cells read round 1 · round 2
campaign · round 2 exploration. Runs are deterministic, so a failure
counts only at a seed that passes without the patch, and only when it
shows the patch's symptom.

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

Over both rounds, with about 2,800 campaign runs per bug, the old
simulator rediscovered 14 of the 27 bugs and the new simulator's
campaigns 17; 2,000 more runs of guided exploration raised the new
simulator's count to 20. Exploration alone found 22, 23 and 27, which no
campaign of either simulator did. Bugs 09, 10, 13, 16, 24, 26 and 28
escaped every strategy; the tests `forwards_to_a_restarted_node_are_refused`
and `earlier_runs_at_this_address_are_not_routed_to` catch 16 and 26.

## What each patch reverts

Each description says what the patch changes, so it can be redone by hand
when the patch stops applying.

- **01** `Node::resolve_db` (src/node.rs) returns the cached or snapshot
  lookup's `NotFound` as is, instead of retrying it with a strong read
  (`resolve_db_with(.., true)`).
- **02** When `db_routing` (src/api/mod.rs) handles a buffered request
  locally after a forward certainly failed, it rebuilds the request from
  method, URI, version and headers instead of `Request::from_parts(parts,
  ..)`, so axum's extensions (path parameters, matched path) are lost.
- **03** `Db::build` (src/engine/db.rs) gives every accepted transaction
  of a batch the version `base.m.version + 1` instead of `last_seq + 1`.
- **04** `Db::snapshot` no longer calls `wait_for_in_flight_write` for
  strong reads.
- **06** `db_routing` no longer runs `MetaCmd::DeleteDb` when a strong
  re-resolution maps the name to the same deleted database, and
  `create_db` (src/api/data.rs) returns `AlreadyExists` without finishing
  a half-deleted database.
- **07** `Db::build` no longer rejects a transaction whose commit deadline
  has passed.
- **08** `list_keys` (src/api/account.rs) reads with `sys_read` (bounded
  staleness) instead of `sys_read_strong`.
- **09** In `db_routing`, `NotFound` from `open_db` (the manifest is gone)
  is returned as 404 instead of triggering strong re-resolution; only a
  manifest flagged deleted still re-resolves.
- **10** `create_db`'s half-deletion check calls a database deleted when a
  strong read of its handle fails with `NotFound`, which includes a
  database whose manifest has not been created yet. The fix opened the
  database with `open_db` (creating the manifest) and required the deleted
  flag.
- **11** `dedup_forwarded` (src/api/mod.rs) passes forwarded writes on
  without `ForwardDedup`; the forward-to check stays.
- **12** `Db::foreign_lease` computes the remaining lease as `expires_ms +
  lease_margin_ms - env.now_ms()` (this node's wall clock) instead of from
  the monotonic instant of the first sighting.
- **13** `Engine::evict_idle` (src/engine/mod.rs) also drops the lease
  sightings of the handles it evicts.
- **14** `table::get_schema` (src/table/mod.rs) reads the catalog entry
  with `txn.get` instead of `txn.get_pinned`.
- **15** `blob::read_chunk` (src/engine/blob.rs) converts a missing chunk
  with the generic `StoreError` conversion (500) instead of a retryable
  `unavailable`.
- **16** `dedup_forwarded` no longer refuses forwards whose
  `x-baserunna-forward-to` names another process.
- **17** `Shared::lease_seen` identifies a renewal by `lease.expires_ms`
  instead of the manifest's `write_seq`.
- **18** `StoreClock::sync` (src/engine/clock.rs) probes once instead of
  up to three times, and `Node::housekeeping` no longer calls
  `refresh_clock`.
- **19** `Session::is_live` (src/meta/mod.rs) ignores `password_fp`.
- **20** `db_routing`'s half-deletion repair ignores a failed `DeleteDb`
  and answers 404 instead of returning the error.
- **21** `Node::open_link` maps every error from reading the user,
  retryable ones included, to 401.
- **22** In `Db::process`, a batch that finds another node's lease fails
  with plain `NotOwner` even after an ambiguous attempt, instead of an
  unknown outcome.
- **23** `Db::trim_for_retention` returns unless this node holds the
  lease, and `Db::build` no longer schedules the trims due on the streams
  of a commit it finds already applied.
- **24** The `not_committed` commit status (src/api/data.rs) reports
  `retention_ms: u64::MAX` instead of the real commit retention. This
  restores the pre-fix contract, in which `not_committed` was always
  definitive. The simulator's client (`Client::commit_status`) treats a
  status older than the reported retention as unknown, so with the patch
  its workloads trust every `not_committed`.
- **25** `Db::process` no longer deletes the batch's past-grace obsolete
  objects (`plan.gc`) when the manifest write's outcome is unknown.
- **26** `Node::live_members` and `Node::route` no longer ignore member
  entries that share this node's address.
- **27** After an ambiguous manifest write, the reload in `Db::process` no
  longer records this node's lease when the reloaded manifest is that
  write.
- **28** `Node::role_for` stores a role in the cache with the time the
  read finished instead of the time it started.
