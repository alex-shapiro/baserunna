# Bug zoo

Each patch here puts back one bug that the deterministic simulator found
(SPEC.md §13.5, numbered as there). The zoo measures how quickly a
simulation strategy rediscovers known bugs: apply a patch, run the
strategy, and count the seeds until the first failure.

Patches touch production code only, apply to the commit that added them
(`git apply bugzoo/NN-*.patch`), and compile. `run.sh` applies each patch
in turn, builds `sim_tool`, runs the `sim_tool` command it is given, and
records the results:

```sh
bugzoo/run.sh campaign all 50
PATCHES="bugzoo/03-*.patch" bugzoo/run.sh campaign register_hostile 300
```

Bug 5 has no patch. It was nondeterminism (`tokio::sync::watch` wakes
waiters in random order). A campaign cannot observe it, and a lint now
forbids `watch` in the crate.

## Detection with the current simulator

Measured with `sim_tool campaign <scenario> <N> 1` (seeds 1..N, or from
the seed given). Without a patch, the same campaigns pass at seeds 1–300
(1–150 for orgs_hostile, 1–100 for streams) except `accounts_hostile` seed 98 (`reset: 401 ... this
link has been used already`). That failure is excluded from the counts
below. "all × 20" means seeds 1–20 of each of the 39 scenarios (780
runs).

| patch | bug | scenarios tried (failing / seeds) | seeds run | failing seeds | first failing seed | notes |
| ----- | --- | --------------------------------- | --------- | ------------- | ------------------ | ----- |
| 01-read-your-writes-across-nodes | A database created through one node is briefly invisible through another | lifecycle_hostile 281/300, db_churn_hostile 300/300, orgs_hostile 138/150 | 750 | 719 | lifecycle_hostile 1 | 404 for a database that was just created |
| 02-lost-routing-extensions | Local handling after a failed forward drops axum's routing extensions (500) | chaos_hostile 76/300, kv_hostile 21/300, bank_hostile 44/300 | 900 | 141 | chaos_hostile 1 | `500` with an empty body |
| 03-duplicate-versions-in-group-commit | Two writes in one batch get the same version | register_hostile 207/300, register 222/300 | 600 | 429 | register 1 | "two writes acknowledged with version N" |
| 04-in-flight-write-barrier | Lease holder serves its cached version while its own manifest write is visible elsewhere | register_contended_hostile 298/300, register_hostile 4/300 | 600 | 302 | register_contended_hostile 1 | non-monotonic reads |
| 06-half-finished-deletes | An interrupted deletion leaves a name bound to a deleted database | db_churn_hostile 77/300, lifecycle_hostile 12/300 | 600 | 89 | db_churn_hostile 7 | "database deleted" for a recreated name; lifecycle also reports unpurged manifests |
| 07-zombie-requests | A request still running after its client gave up commits later | bank_hostile, kv_hostile, register_hostile (1–1000), streams_hostile 100, abandoned_hostile, blob_gc_race_hostile, chaos_hostile, kv_faulty_cluster, all × 20: all 0 | 3,680 | 0 | — | Not detected by campaigns: the workloads wait 30 s before checking status, longer than any commit now stays queued. Unit test `commits_past_their_deadline` fails. |
| 08-stale-listings | API key listing served from a stale snapshot misses a new key | console 238/300, auth 2/300, accounts_hostile 0/300 | 900 | 240 | console 1 | console: "paging API keys two at a time found 3 of 4" |
| 09-stale-names-after-purge | A cached name of a purged database answers 404 instead of resolving to its recreation | db_churn_hostile (1–1000), lifecycle_hostile 300, all × 20: all 0 | 2,080 | 0 | — | Not detected: the sim's name cache lives 1 s and purges happen after a 60 s grace, so a cached name never outlives a manifest. |
| 10-missing-is-not-deleted | Retried create treats a never-opened database as half-deleted and replaces it | db_churn_hostile 300, lifecycle_hostile (1–1000), all × 20: all 0 | 2,080 | 0 | — | Not detected: no workload notices that a fresh, empty database was replaced by another. |
| 11-duplicated-forwards | A duplicated forwarded request executes twice | kv_hostile 29/300, chaos_hostile 37/300, bank_hostile 0/300 | 900 | 66 | chaos_hostile 2 | model mismatches (resurrected keys, double applies) |
| 12-stale-lease-reads-under-clock-skew | Other nodes judge a lease by their own wall clock | register_hostile 11/300, kv_hostile 15/300, register_contended_hostile 16/300 | 900 | 42 | register_hostile 1 | stale reads; also 401s from the system database |
| 13-failover-livelock | Lease sightings die with evicted handles, so a dead node's lease looks new forever | register_hostile (1–1000), kv_hostile, chaos_hostile, lifecycle_hostile 300 each, all × 20: all 0 | 2,680 | 0 | — | Not detected, and no unit test fails: the store-clock lease expiry (§5.7) added later bounds the livelock. |
| 14-snapshot-isolation-schema-changes | A snapshot-isolated row write races `add_index` and the index misses the row | schema_race 4/300, schema_race_hostile 3/300, tables_hostile 0/300 | 900 | 7 | schema_race 31 | orphaned or missing index entries |
| 15-collected-blobs-mid-read | Reading a blob collected mid-read fails with 500 instead of 503 | blob_gc_race_hostile 7/300, blob_gc_race 0/300, blobs_hostile 0/300 | 900 | 7 | blob_gc_race_hostile 11 | `500 object store: object not found` |
| 16-duplicated-forwards-across-restart | A duplicate forward reaches a restarted node and deletes a recreated database | chaos_hostile 300, db_churn_hostile (1–1000), lifecycle_hostile 300, all × 20: all 0 | 2,380 | 0 | — | Not detected by campaigns. `tests/sim.rs` `forwards_to_a_restarted_node_are_refused` fails. |
| 17-lease-renewals-that-look-alike | Renewals with equal expiry are taken for one another, so a node takes over early | register_hostile (1–1000), kv_hostile 300, lifecycle_hostile 300, all × 20: all 0 | 2,380 | 0 | — | Not detected by campaigns: expiries rarely coincide to the millisecond. Unit test `lease_sightings_outlive_handles_and_prune_when_full` fails. |
| 18-collection-paused-by-slow-clock-probes | Slow clock probes leave a node unable to sweep | blobs_hostile 3/300, blobs 0/300, abandoned_hostile 0/300 | 900 | 3 | blobs_hostile 10 | "garbage blob ... survived a full GC cycle" |
| 19-sessions-outlive-password-change | Old sessions survive a crash between a password change and their revocation | accounts_hostile 12/300, accounts 4/300 | 600 | 16 | accounts_hostile 28 | "a session outlived a password reset" |
| 20-repairs-that-claimed-success | A failed half-deletion repair answers 404 instead of 503 | lifecycle_hostile (1–1000), db_churn_hostile 300, all × 20: all 0 | 2,080 | 0 | — | Not detected: the repair's metadata command rarely fails. |
| 21-transient-errors-as-invalid-links | A link checked while the system database is unreachable is reported invalid (401) | accounts_hostile 20/300, accounts 0/600 | 900 | 20 | accounts_hostile 17 | "this link is invalid or has expired" |
| 22-lease-loss-after-ambiguous-write | An ambiguous commit that finds another node's lease reports `not_owner` | accounts_hostile 1/600, lifecycle_hostile 0/300, orgs_hostile 0/150, db_churn_hostile 0/300 | 1,350 | 1 | accounts_hostile 219 | "change password: 412 the password changed concurrently" (the retried command applied twice). Weak signal. |
| 23-retention-left-to-doubting-node | Retention trims wait for the next commit after an ambiguous write | streams 1/100, streams_hostile 0/200 | 300 | 1 | streams 64 | "ret is past its retention". Weak signal. |
| 24-status-checks-past-commit-retention | `not_committed` past commit retention sends the client to append again | streams 0/300, streams_hostile 0/100, all × 20: all 0 | 1,180 | 0 | — | Not detected: an append is rarely left ambiguous for longer than commit retention (120 s in the sim). |
| 25-garbage-retained-after-ambiguous-writes | Obsolete objects are kept when the manifest write outcome is unknown | blobs 2/300, blobs_hostile 0/300, abandoned_hostile 0/300 | 900 | 2 | blobs 10 | "garbage blob ... survived a full GC cycle". Weak signal. |
| 26-self-forwarding-after-restart | A restarted node forwards to its previous run at its own address | chaos_hostile, chaos, kv_hostile, kv_single_node 300 each, all × 20: all 0 | 1,980 | 0 | — | Not detected by campaigns: the refused forwards are retryable 503s and clients retry until the old entry ages out. Unit test `earlier_runs_at_this_address_are_not_routed_to` fails. |
| 27-lease-state-lost-after-ambiguous-writes | A node whose ambiguous write landed does not know it holds the lease and skips sweeps | blobs_hostile, abandoned_hostile, blobs 300 each, all × 20: all 0 | 1,680 | 0 | — | Not detected by campaigns. Unit test `a_write_found_applied_after_an_unknown_outcome_renews_the_lease` fails. |
| 28-stale-authorization-after-slow-read | A role read that started before a membership change is cached for a full TTL after it ends | orgs_hostile (1–300), orgs 300, all × 20: all 0 | 1,380 | 0 | — | Not detected: the sim's auth cache lives 1 s, so the extra lifetime is too short for the checker to see. |

## Unit tests

`cargo test --release --lib --test sim` passes at the same commit. With a
patch applied, these tests fail:

| patch | failing tests |
| ----- | ------------- |
| 01 | 18 tests in `tests/sim.rs`, among them `sim_lifecycle`, `sim_many_db_churn`, `sim_hostile` |
| 02 | `sim_blobs_and_gc`, `sim_blob_reads_race_gc`, `sim_schema_changes_race_row_writes`, `sim_streams`, `sim_hostile` |
| 03 | `group_commit_batches_concurrent_commits`, `a_range_delete_spares_its_own_transactions_writes`, `sim_register_linearizability`, `sim_register_contended`, `sim_hostile` |
| 04 | `strong_reads_wait_for_the_write_in_flight`, `optimistic_txn_revalidation`, `optimistic_stream_revalidation`, `sim_register_contended`, `sim_hostile` |
| 07 | `commits_past_their_deadline` |
| 08 | `sim_console` |
| 11, 12 | `sim_hostile` |
| 16 | `forwards_to_a_restarted_node_are_refused` |
| 17 | `lease_sightings_outlive_handles_and_prune_when_full` |
| 18 | `slow_probes_are_not_trusted` |
| 22 | `ambiguous_commit_during_active_lease` |
| 26 | `earlier_runs_at_this_address_are_not_routed_to` |
| 27 | `a_write_found_applied_after_an_unknown_outcome_renews_the_lease` |
| 06, 09, 10, 13, 14, 15, 19, 20, 21, 23, 24, 25, 28 | none |

For 07, 17, 26 and 27, `tests/sim.rs` did not run because the unit tests
failed first.

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
  definitive; the client side of the fix lives in the streams workload,
  which patches here do not touch.
- **25** `Db::process` no longer deletes the batch's past-grace obsolete
  objects (`plan.gc`) when the manifest write's outcome is unknown.
- **26** `Node::live_members` and `Node::route` no longer ignore member
  entries that share this node's address.
- **27** After an ambiguous manifest write, the reload in `Db::process` no
  longer records this node's lease when the reloaded manifest is that
  write.
- **28** `Node::role_for` stores a role in the cache with the time the
  read finished instead of the time it started.
