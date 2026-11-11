# TODO

Planned work, grouped by triage tier. See [`SPEC.md`](SPEC.md) §15 (known
limitations) and §16 (future work) for design context.

Each item has a major tag and an optional minor topic tag: `[major · minor]`.
The major tags are:

- **blocker**: Fix these items before production use. The defect can break a
  transaction guarantee, lose or corrupt data, return wrong data without an
  error, or let a tenant crash a node or starve other tenants. A store that
  returns a corrupt object is in scope (SPEC §1).
- **nice-to-have**: The item is valid and noticeable to users or operators, but
  it has bounded impact or recovers automatically. This tier also includes
  defects that require a contract-breaking proxy or store, and features outside
  the v1 goals.
- **tangential**: The item has no demonstrated effect on users or operators. It
  is a simulator artifact, an unmeasured optimization, a refactor, or a
  cosmetic change.
- **not triaged**: The item has not yet been triaged.

Each blocker identifies a failing test marked `#[ignore]`. Remove the attribute
with the fix. Numbers in blocker items are measured unless marked "extrapolated"
or "calculated". Run the heavy tests one at a time, because a single test can
use up to 5 GB of memory. Items in the other tiers are verified by code reading
only.

## Blocker

- [ ] [blocker · Stale strong read] **Keep the cached manifest distrusted after
      an ambiguous manifest `PUT`.** Clear `own_lease` on an ambiguous `PUT` and
      on a `412`, and restore it only after a write or commit confirms the
      manifest version. A manifest `PUT` that returns an error can still be
      applied by the store later. The committer distrusts its cache on the
      error, but the next manifest read that returns the old version restores
      trust (`install`) and the node's own lease (`own_lease`). If the `PUT` is
      then applied, the lease holder serves strong reads from the old manifest
      for 2.6 s to 3.5 s. A client observes newer data and then older data in
      two cases: a strong event query on another node followed by a strong read
      on the lease holder, and a change-feed long poll on a non-lease home node
      followed by a strong `GET`. A `412` on the commit retry causes the same
      issue over a 21 ms window. Tests: `tests/stale_strong_read.rs` and
      `late_manifest_put` in `src/engine/tests.rs`.
- [ ] [blocker · Stale strong read] **Enable the late-apply fault in the swarm
      profiles.** `StoreFaults::p_late_apply` makes the simulated store return
      an error for a `PUT` and apply it later. It is 0 in every profile because
      campaigns would fail on the stale strong read defect above. Enable it with
      the fix.
- [ ] [blocker · Tenant resource exhaustion] **Charge event query memory to the
      node `MemoryGate` and fair-share `Node::query_slot`.** Concurrent event
      queries are not charged against the node's `memory_bytes` budget. Eight
      concurrent queries held 2,630 MiB against a 768 MiB budget without being
      queued or rejected. A single heavy query holds 55 MiB to 550 MiB, and
      `query_concurrency` defaults to the core count. In addition,
      `Node::query_slot` is not fair-shared: a user held all 16 slots for
      12.5 s per query while another user's 1-row query timed out with `503`
      after 5 s. Tests: `concurrent_event_queries_respect_node_memory_budget`
      and `one_user_cannot_hold_every_query_slot` in
      `tests/event_query_limits.rs`.
- [ ] [blocker · Tenant resource exhaustion] **Acquire a query slot for
      transactional `query_events` ops and extend their deadline.** Unlike the
      event query route (`/events/{t}/rows/query`), `query_events` inside `/txn`
      does not acquire a `Node::query_slot` permit: 32 concurrent requests held
      twice the heap of 16, whereas the event query route stayed flat. It also
      does not use the event query deadline: a query that returned `200` after
      12.5 s on the event query route failed with `503` and
      `outcome: "unknown"` after 10 s in `/txn`. Tests:
      `transactional_event_queries_take_query_slots` and
      `transactional_event_queries_get_the_event_query_deadline` in
      `tests/event_query_limits.rs`.
- [ ] [blocker · Tenant resource exhaustion] **Estimate granule run memory from
      actual string lengths.** Use the column index byte length to estimate run
      memory before decoding. `Run::run_rows` estimates string columns at 4
      bytes per row (the dictionary code width), even if a high-cardinality
      column is stored without a dictionary. The query charges memory only after
      it decodes a column. A query over 16 merged parts of long unique strings
      held 1,954 MiB against its 1 GiB limit. Each part read in parallel adds
      110 MiB to 152 MiB, so 32 parts need about 5 GiB (extrapolated). Test:
      `long_string_queries_respect_memory_limit` in
      `tests/event_query_limits.rs`.
- [ ] [blocker · Tenant resource exhaustion] **Stream JSON request bodies and
      charge bulk insert memory to `MemoryGate`.** Parse row fields lazily and
      reserve memory from `Content-Length`. `JsonBody` parses the full body into
      memory before the handler checks `INSERT_MAX_ROWS`. Peak heap was 32 to 54
      times the body size for valid small events (1.0 GiB to 1.7 GiB for a 32
      MiB body) and 87 times for rows with an unknown column. The bulk insert
      route avoids building a JSON tree, but its row buffer and flush hold 7 to
      44 times the body size without a memory charge: 4 concurrent inserts held
      2 GiB against a 768 MiB node budget. Tests:
      `txn_event_insert_parse_stays_near_body_size` and
      `concurrent_bulk_inserts_respect_node_memory_budget` in
      `tests/event_ingest_limits.rs`.
- [ ] [blocker · Tenant resource exhaustion] **Time out stalled request
      bodies.** Enforce a read timeout while receiving request bodies. The
      request deadline pauses while a body arrives, and the server sets no read
      timeout. A body that stopped halfway remained open after 10 minutes with
      its buffer held, both through the router and over TCP. Tests:
      `stalled_request_body_times_out` and
      `stalled_request_body_times_out_over_tcp` in
      `tests/event_ingest_limits.rs`.
- [ ] [blocker · Tenant resource exhaustion] **Limit event maintenance
      concurrency across the node and charge its memory.** Acquire a
      `compaction_slots` permit and reserve `MemoryGate` capacity during event
      seals and merges. Event maintenance starts a task per database and table
      (`events/maint.rs`) without a node-wide limit. With 16 tables, 16 merges
      ran concurrently and held 1,248 MiB, whereas a single merge held 99 MiB.
      Test: `event_merges_share_a_node_wide_limit` in
      `tests/event_ingest_limits.rs`.
- [ ] [blocker · Tenant resource exhaustion] **Bound memory usage during part
      merges.** Merge sorted runs block by block instead of loading all sort
      keys at once. `merge_parts` holds the sort key of every live input row in
      memory. Peak heap was 172 bytes per input row from 1 million to 8 million
      rows, so a partition with 100 million small rows needs about 16 GiB
      (extrapolated). Merge planning caps only the 4 GiB compressed output size,
      which such rows reach at about 1.8 billion rows (calculated). Because a
      failed merge retries after a restart and every hour, an oversized merge
      can crash-loop the node. Tests: `merge_memory_does_not_grow_with_rows` and
      `failed_merge_is_not_retried_after_restart` in
      `tests/event_ingest_limits.rs`. Delete the second test if the fix bounds
      merge memory.
- [ ] [blocker · Tenant resource exhaustion] **Limit partitions per bulk
      insert.** Cap the number of partitions touched by a bulk insert and encode
      `StagedPart::slot_rows` sparsely. The flush writes a part per partition
      sequentially without a cap or deadline check. A request spanning 10,000
      hourly partitions caused 10,001 `PUT`s: the client received `503` after
      10 s while the flush continued for 190 s without binding any rows. A 32
      MiB body can reference up to 1.6 million partitions (calculated). Test:
      `bulk_insert_partitions_are_limited` in `tests/event_ingest_limits.rs`.
- [ ] [blocker · Store fault detection] **Verify blob chunks on read.** Add
      per-block checksums to blob chunks and verify them on full and ranged
      reads. `blob::read_chunk` checks only the chunk byte length, and full blob
      reads do not verify `BlobRef.sha256`. Flipped bits, substituted chunks,
      and shifted byte ranges were served to the client as valid data. A
      truncated chunk fails only after the node sends `200`, delivering a short
      body without logging a warning. Test: `damaged_blob_chunks_are_detected`
      in `tests/store_integrity.rs`.
- [ ] [blocker · Store fault detection] **Add a checksum to blob metadata
      objects.** Protect staged blob metadata objects with a checksum and verify
      it in `load_staged`. `load_staged` checks only the blob ID, so bit flips
      in `size`, `sha256`, or the content type are committed into the LSM tree
      at bind. A smaller size truncates later reads without an error, and a
      larger size makes the key unreadable. Test:
      `damaged_staged_blob_metadata_is_detected` in `tests/store_integrity.rs`.
- [ ] [blocker · Store fault detection] **Report persistent maintenance
      failures and back off.** Increment an error counter, log at `error`
      level, and back off when background maintenance fails. With a single
      corrupt segment, compaction failed on 1,204 of 1,300 commits without
      backoff, and the sweep failed on every commit after becoming due. Stored
      bytes grew to 27 times the live data size. Compaction, sweep, flush, trim,
      and event maintenance failures log only at `debug` level and record no
      metric in `/v1/admin/metrics`. Test:
      `persistent_segment_corruption_is_reported_and_backed_off` in
      `tests/store_integrity.rs`.
- [ ] [blocker · Lost acknowledged write] **Reject binds if a target column was
      dropped after validation.** Include column IDs in `Bind` and return a
      retryable conflict if any target column was dropped. `Db::bind` checks
      that the table still exists, but not its columns. If a column is dropped
      and re-added within 190 ms after a bulk insert arrives, the insert
      returns `201` and its rows return `null` for that column. Test:
      `bulk_insert_keeps_x_values_after_a_column_drop_and_re_add` in
      `tests/event_schema_races.rs`.
- [ ] [blocker · Data recovery] **Provide a backup and restore path for the
      system database and database manifests.** Document and test a restore
      procedure, add an undelete route or a longer delete grace period, and
      align `SPEC.md` §8 with `DEPLOYING.md` on bucket versioning. After
      `DELETE /v1/dbs/{db}`, no route returns the data, and all database objects
      are purged after `db_delete_grace_ms` (10 minutes, not configurable).
      `SPEC.md` §8 instructs operators to restore a lost manifest from a backup
      or object version, whereas `DEPLOYING.md` instructs them to disable bucket
      versioning. This item has no failing test.

## Nice-to-have

- [ ] [nice-to-have · Verification] **Enable the CI triggers.**
      `.github/workflows/test.yml` and `deploy.yml` run only on
      `workflow_dispatch`, because their pull request, push, and schedule
      triggers are commented out. The test suite, simulation campaigns,
      rare-probe enforcement, and guided exploration run only if a developer
      starts them.
- [ ] [nice-to-have · Verification] **Add targeted simulation tests for all
      rare probes.** Only 5 of the 28 probes in `Probe::RARE` have a targeted
      test. `SIM_REQUIRE_PROBES` enforces the others only on the scheduled
      campaign, which does not run.
- [ ] [nice-to-have · Verification] **Verify event merge liveness and
      part-internal invariants in simulation.** Check in the `events` workload
      that part counts remain bounded and completed partitions settle into a
      single part, and verify part sort order, granule statistics, bloom
      filters, and path directories in `storage_check`.
- [ ] [nice-to-have · Verification] **Run libFuzzer targets in CI** with a
      persistent corpus, including the model-checked `http_txn` target.
- [ ] [nice-to-have · Verification] **Derive simulation settlement timeouts
      from fault parameters and separate liveness from safety failures.** Fixed
      settlement timeouts can fail if a simulated store stall exceeds the wait
      duration.
- [ ] [nice-to-have · Unexplained simulator failure] **Find the cause of
      `db_churn` seed 202681: `GET` returns `404 not_found` for an existing
      database.** During database churn, a `get` operation returned
      `404 not_found` for a database that was created and not deleted
      (`sim_tool trace db_churn 202681`). The seed passes on the current tree
      and the cause is unknown. If a delayed `DELETE` ran against the recreated
      database, this item is a blocker.
- [ ] [nice-to-have · Unexplained simulator failure] **Find the cause of
      `accounts` session failures after an account is disabled and
      re-enabled.** A valid session was rejected after its account was disabled
      and re-enabled (`sim_tool trace accounts 9506242`). The seed passes on the
      current tree and the cause is unknown. If a delayed duplicate disable ran
      after the re-enable, this item is a blocker.
- [ ] [nice-to-have · Duplicate requests] **Do not execute keyless writes
      locally after a stale-target rejection.** If a node receives
      `x-baserunna-stale-target`, it executes the request locally, assuming the
      peer never executed it. If a proxy duplicated the forwarded request, an
      earlier copy could have been executed by the peer's previous process
      (`sim_tool trace tables 7344`). The node transport sends each forward
      once, so this needs a retrying proxy between nodes. Execute locally only
      if the request is a `GET` or has an idempotency key; otherwise return
      `outcome: "unknown"`. State in `DEPLOYING.md` that proxies must not retry
      peer traffic.
- [ ] [nice-to-have · Duplicate requests] **Persist started interactive commits
      across nodes and restarts.** Derive each transaction ID from its commit ID
      so that any node can check the commit record in the manifest. A node
      tracks started interactive commits only in memory. After a node restart,
      or on a peer node, a duplicate request receives a retryable
      `404 txn_not_found` and can cause the client to re-execute the
      transaction. `baserunna-client` does not send duplicate commits, so this
      needs a proxy that retries a delivered `POST`.
- [ ] [nice-to-have · Duplicate requests] **Set `retryable: false` if `outcome`
      is `"unknown"` and the write has no idempotency key.** A `503` response
      with `outcome: "unknown"` sets `retryable: true`. The API docs and
      `baserunna-client` resolve the outcome first, but a client that inspects
      only `retryable` can apply a non-idempotent write twice.
- [ ] [nice-to-have · Duplicate requests] **Reject `Idempotency-Key` on routes
      that ignore it, and support it on `DELETE /v1/dbs/{db}/kv/{key}`.** Raw
      key-value deletes and the row routes ignore the header without an error,
      so `GET /commits/{id}` reports `not_committed` for a delete that was
      applied.
- [ ] [nice-to-have · Duplicate requests] **Display `outcome: "unknown"` in the
      web console.** The console displays only the error message, so an
      ambiguous write appears to have failed.
- [ ] [nice-to-have · Event tables] **Prevent event deletes from conflicting
      with concurrent inserts.** An event delete adds all unsealed rows of the
      table and all part references in its time range to the transaction read
      set, causing serializable conflicts with concurrent inserts and binds
      (`sim_tool trace events 2126696`). Evaluate the delete filter against
      rows and part references added after the snapshot inside the committer
      instead of tracking them in the read set.
- [ ] [nice-to-have · Event tables] **Execute large event deletes
      asynchronously.** An event delete reads all candidate parts within its
      transaction and fails with `503` if those reads exceed
      `commit_timeout_ms` (10 s). Run large deletes as background operations
      that update part masks incrementally.
- [ ] [nice-to-have · Event tables] **Recheck table retention before committing
      expired-part drops.** Read the schema key inside the `drop_expired`
      transaction so that concurrent retention updates abort the drop.
      `drop_expired` commits its planned part drops without verifying that
      `retention_ms` is unchanged. If maintenance seals rows between planning
      and committing the drop, a retention change acknowledged in that window
      (10 ms to 40 ms in the test) does not protect the expired parts: a strong
      query returned their rows after the change, and a later query did not.
      Test:
      `clearing_retention_during_a_sealing_maintenance_round_keeps_the_rows` in
      `tests/event_schema_races.rs`.
- [ ] [nice-to-have · Event tables] **Bound inline deletion masks on bound
      parts.** The committer stores masks for rejected or duplicate bind slots
      inline regardless of size. Rewrite parts in background maintenance if
      their inline mask exceeds `event_mask_inline_bytes`.
- [ ] [nice-to-have · Event tables] **Decode rows directly into column vectors
      during part builds.** Seals and bulk inserts decode each row into a
      `BTreeMap` before transposing columns, causing a 32 MiB bulk insert of
      small events to peak at roughly 11x the request body size
      (`crates/baserunna/tests/memory_bound.rs`). `build_cost` also
      underestimates small rows by about 4x.
- [ ] [nice-to-have · Event tables] **Enforce `event_max_bytes` on
      transactional event inserts.** `event_max_bytes` is checked during
      bulk-insert binds (`Db::bind`), but transactional inserts are sealed in
      the background without checking the table cap. Count unsealed event bytes
      toward `event_max_bytes` at commit time.
- [ ] [nice-to-have · Event tables] **Account for minimum part overhead during
      event inserts.** Sealing writes at least 330 bytes per partition without
      checking the database quota, so sparse inserts across many partitions can
      exceed the quota after sealing. Charge the per-partition metadata overhead
      if an insert opens a partition.
- [ ] [nice-to-have · Event tables] **Check the 4 GiB query read limit during
      in-flight part reads.** The read byte limit is only checked after each
      part finishes, allowing up to `event_read_concurrency` concurrent part
      reads to overshoot the limit.
- [ ] [nice-to-have · Event tables] **Retry snapshot event queries only on
      missing table or column errors.** `api::events::query` retries any `400`
      or `404` failure at strong consistency, which executes invalid queries
      twice.
- [ ] [nice-to-have · Event tables] **Refresh the database name cache on `404`
      during strong event queries.** Event queries resolve the database name
      using a 5 s cached snapshot even if `consistency` is `strong`, causing a
      strong query to fail once with `404` if the database was recently
      recreated on another node.
- [ ] [nice-to-have · Event tables] **Parse shared JSON buckets once per row
      during merges.** While merging a part with a partial path directory,
      `Feed::SharedPath` reparses the shared bucket JSON once per output path
      instead of once per row.
- [ ] [nice-to-have · Event tables] **Move event query block decoding and
      aggregation off Tokio worker threads.** Execute CPU-bound granule
      decoding and aggregation on a dedicated thread pool bounded by
      `query_concurrency` so large queries do not stall async workers.
- [ ] [nice-to-have · Event tables] **Cache event parts in the local disk cache
      tier.** Event parts are currently cached only in memory, so node restarts
      and working sets larger than RAM must re-fetch parts from object storage.
- [ ] [nice-to-have · Event tables] **Expose environment variables for event
      query and bulk insert limits.** Add `BASERUNNA_*` environment variables
      for `query_limits`, `query_concurrency`, `event_max_bytes`, and bulk
      insert buffer settings in `NodeConfig`.
- [ ] [nice-to-have · Event tables] **Support filtering whole JSON columns and
      reading nested JSON objects by path.** Event parts store only leaf JSON
      paths, so filtering on a root JSON column is rejected
      (`exec::Binder::filter`) and querying a non-leaf path returns `null`
      (`json::leaf`). Support `exists` on root JSON columns and reconstruct
      nested objects from their leaf paths.
- [ ] [nice-to-have · Idle databases] **Run maintenance on idle databases from
      node housekeeping.** Stream retention trims, event retention drops, event
      merges, and purges of deleted events run only after a commit or a sweep
      on an active database. An idle database keeps expired stream records,
      expired event partitions, and deleted events in storage. Trigger periodic
      flushes, compactions, retention trims, and event maintenance from node
      housekeeping. This also covers trims that are skipped after an ambiguous
      manifest write (`sim_tool trace streams 8100221`) or while the store
      clock is unknown.
- [ ] [nice-to-have · Store fault detection] **Detect sequence gaps during
      stream reads.** Verify that record sequence numbers are contiguous during
      `stream::read`. If the store returns another chunk of the same byte size
      and record count, a read returns the wrong records without an error.
      Test: `substituted_stream_chunk_of_equal_shape_is_detected` in
      `tests/store_integrity.rs`.
- [ ] [nice-to-have · Store fault detection] **Bind every stored object to
      its key.** Write the object ID into each CRC-protected footer and verify
      it during decoding. Readers verify byte size and entry count against the
      manifest, which accepts a substituted object of equal size and entry
      count. This applies to segments, stream chunks, event parts (equal row
      count), and deletion masks (equal deleted count). After a segment
      substitution, compaction commits the wrong data and the original keys
      are lost. Tests: the `substituted_*` tests in `tests/store_integrity.rs`.
- [ ] [nice-to-have · Store fault detection] **Check ranged block reads against
      the block index.** Verify that a segment block fetched by byte range
      matches its block index entry. If the store returns another block of equal
      length, the block passes its own CRC and a point read returns "not found"
      for a key that exists. Test:
      `segment_block_served_from_wrong_offset_is_detected` in
      `tests/store_integrity.rs`.
- [ ] [nice-to-have · Store dependencies] **Detect manifest rollbacks.** A
      minimum manifest version in the system database would reject a rolled-back
      manifest, though updating it on every commit would add a `PUT` per commit.
      A node with an empty manifest cache accepts any valid older manifest from
      the store, commits on top of it, and collects the newer segments as
      orphans. A contract-breaking store, an operator restore from an object
      version, or a failover to a lagging replica can cause a rollback.
- [ ] [nice-to-have · Store dependencies] **Check store clock coherence in the
      startup preflight.** Garbage collection depends on a coherent store clock
      (SPEC §15), but the preflight runs only the conditional-write checks.
- [ ] [nice-to-have · Store dependencies] **Validate additional S3-compatible
      object stores.** Run the conformance suite and failover check against AWS
      S3, Cloudflare R2, MinIO, and Google Cloud Storage. Azure Blob Storage
      needs a new store backend, because `CloudStore` accepts only `s3://`.
- [ ] [nice-to-have · Operations] **Prometheus metrics endpoint.** Expose
      `/metrics` in Prometheus text format with `/v1/admin/metrics` histograms
      and counters for store traffic, CAS conflicts, leases, caches, compactions,
      garbage collection, request forwarding, and rate-limit rejections.
- [ ] [nice-to-have · Operations] **Publish release binaries and a container
      image.** Build and publish release binaries and a container image so
      external test suites can run a local node in CI.
- [ ] [nice-to-have · Operations] **Release database leases concurrently on
      shutdown.** `Engine::release_leases` currently releases leases
      sequentially with a 2 s timeout per database, so a node with many leased
      databases can exceed the shutdown grace period.
- [ ] [nice-to-have · Operations] **Retry startup preflight checks during store
      outages.** Retry the conditional-write preflight check with exponential
      backoff before exiting. If the object store is unreachable at startup,
      the preflight check fails immediately and the process exits.
- [ ] [nice-to-have · Operations] **Await in-flight hedged manifest `PUT`s
      before returning an unknown outcome.** If a hedged duplicate `PUT` fails
      with `412` while the primary `PUT` is still in flight and retries are
      exhausted, awaiting the primary `PUT` can resolve the commit outcome
      instead of returning `outcome: "unknown"`.
- [ ] [nice-to-have · Operations] **Account for object overhead in the segment
      block cache.** `CacheVal::size` charges key and value bytes plus 48 bytes
      per entry, undercounting small entries by roughly half of their heap
      allocation.
- [ ] [nice-to-have · Operations] **Direct SMTP delivery and durable mail
      queue.** Support SMTP in addition to the HTTP mail webhook, and persist
      outbound mail so node restarts do not drop pending messages.
- [ ] [nice-to-have · Accounts] **Harden Google OAuth and test it against
      Google's production endpoints.** Only `FakeGoogle` is tested in
      simulation. Refresh the cached JWKS if an ID token contains an unknown
      `kid` instead of waiting for the 1-hour cache TTL, and add PKCE
      (`code_verifier`) and an OIDC `nonce`.
- [ ] [nice-to-have · Accounts] **Clear cached logins on all nodes if a user is
      disabled.** Disabling a user clears cached credentials only on the node
      that ran the command. Other nodes accept the user's credentials for up to
      `auth_cache_ms` (30 s).
- [ ] [nice-to-have · Accounts] **Organization access controls.** Add
      per-database role grants within an organization, organization-owned
      service accounts (API keys), and email invitations for unregistered
      users.
- [ ] [nice-to-have · Accounts] **Database rename and ownership transfer**
      between users and organizations.
- [ ] [nice-to-have · Data recovery] **Point-in-time restore, clone, and
      export.** Retain historical manifests and delay GC of referenced
      segments. Add database snapshot/clone via manifest copy-on-write and
      export/import.
- [ ] [nice-to-have · Data recovery] **Require an explicit flag for full
      deletes in `where` and `delete_range`.** An empty `where` selector
      deletes all rows of a table, and an empty `prefix` in `delete_range`
      deletes every key in the key-value keyspace. Both behaviors are
      documented. Require an explicit full-delete flag so that an accidentally
      empty selector is rejected.
- [ ] [nice-to-have · Streams] **Fix stream reads that exceed the request
      deadline.** Requests are canceled at `commit_timeout_ms`, so a cold
      filtered stream read on a slow store can fail repeatedly with
      `503 request deadline exceeded` (`sim_tool trace streams 2601188`).
- [ ] [nice-to-have · Streams] **Pin the lower bound of a reverse `read_stream`
      that uses `tail`.** On each page of a reverse read with `tail`, the
      server recomputes the lower bound from the end of the stream. If records
      are appended between pages, the oldest records of the original window are
      omitted. On the first page, have `Op::next_page` replace `tail` with
      `from_seq`.
- [ ] [nice-to-have · Streams] **Stream consumer groups and storage
      optimizations.** Add consumer groups with record acknowledgments and
      redelivery, multi-chunk read-ahead for cold sequential reads, per-block
      field summaries for filter pruning, and small-chunk compaction.
- [ ] [nice-to-have · Cluster] **Deliver commit notices across membership
      transitions.** If nodes have different cached views of live cluster
      members, a long-poll request can be routed to a rendezvous home node that
      does not receive commit notices. The waiter then falls back to polling
      the manifest every `watch_poll_ms`. Views can differ for up to
      `members_refresh_ms` (30 s) after a node joins.
- [ ] [nice-to-have · Cluster] **Route interactive transaction requests to the
      node that holds the transaction.** Encode the owning node ID in the
      transaction ID and route follow-up requests directly to that node. The
      node that begins an interactive transaction stores it in memory. Receiving
      nodes route follow-up requests using their cached membership view. If
      membership views differ across nodes, a request can be routed to a
      different node and fail with `txn_not_found`.
- [ ] [nice-to-have · Client] **Preserve the failure reason if an interactive
      commit is retried.** Stop `baserunna-client` from retrying `CommitTxn`
      after a not-applied failure, and correct the comment in `txn.rs` that says
      the transaction remains open. The commit handler removes the transaction
      from memory before committing. If the commit fails with a retryable error
      such as `503`, a client retry receives `404 txn_not_found` without the
      original failure reason.
- [ ] [nice-to-have · Client] **Add the remaining transaction operations and
      options to `baserunna_client::Op`.** Add `stream_info`, `get_consumer`,
      event-table operations, `ttl_ms` on writes, `expect_seq` on `append`, and
      value checks on `check`.
- [ ] [nice-to-have · Client] **Return operation results from `Db::write` and
      `Txn::commit`.** Both methods currently discard the per-operation results
      returned by the server, such as the new value from `increment` or the row
      count from `delete_rows`.
- [ ] [nice-to-have · Client] **Client SDKs.** Generate TypeScript and Python
      clients from `openapi.json`.
- [ ] [nice-to-have · Queries] **Query projections, ordering, and aggregates.**
      Support server-side projections, ordering by non-indexed columns, and
      aggregates (`count`, `sum`).
- [ ] [nice-to-have · Queries] **Use exact numeric comparisons for row-table
      float columns.** Compare `f64` columns against integer arguments using
      exact numeric ordering, matching event tables. Row-table filters convert
      integer filter arguments to `f64` (`table::filter_arg`) and reject
      integers that are not exactly representable as `f64`. Index `eq` and
      `range` arguments on float columns round such integers without an error
      (`key_from_json`).
- [ ] [nice-to-have · Queries] **Configurable change feed retention and SSE.**
      Change feed cursors are currently valid for `txn_max_lifetime_ms` (5
      minutes), matching tombstone retention. Per-database retention settings
      would allow consumers to resume after longer outages, and SSE streaming
      would avoid repeated long polls.

## Tangential

- [ ] [tangential · Simulator] **Fix `db_churn` timeouts if a node crash-loops
      under memory pressure.** Under a small cache budget, a restarted node
      repeatedly re-reads the same system segments before serving requests, and
      fault injection can crash the node again before the workload finishes
      (`sim_tool trace db_churn 606688`). The seed uses 2 KiB segments and a 4
      KiB disk cache; the default settings load the system database in a few
      requests.
- [ ] [tangential · Simulator] **Tolerate transient form errors in the
      `console` workload.** A password change at `/console/account/password`
      can time out during storage stalls. The workload currently treats any
      error redirect as a test failure (`sim_tool trace console 24010902`).
- [ ] [tangential · Simulator] **Run garbage collection sweeps even if the
      lease duration is short.** A post-commit sweep skips execution if the
      lease expires while reading the store clock and refreshing the manifest.
      This leaves the database unswept until the next commit
      (`sim_tool trace blobs 5000231`). The skip needs a simulator-sized lease
      or a long store stall.
- [ ] [tangential · Simulator] **Log scenario-overridden knob values in
      simulation traces.** Scenarios that override swarm configuration after
      generation currently log the pre-override values.
- [ ] [tangential · Simulator] **Improve guided exploration breadth.** Measure
      guided exploration against a random campaign with `SIM_COMPARE` first.
      Candidate improvements include deduplicating failures by normalized
      message, weighting corpus entries by novelty, and adding probe-ordering
      and buggify-site features.
- [ ] [tangential · Simulator] **Run an extended multi-thousand-seed
      `sim_events` campaign** with larger tables and node counts.
- [ ] [tangential · Test infrastructure] **Regenerate stale bug zoo patches.**
      Ten bug zoo patches no longer apply to the current tree. Update them, and
      correct the statement in `bugzoo/README.md` that all patches apply.
- [ ] [tangential · Test infrastructure] **Update the `events` fuzz target for
      the current `PartRef` fields.** `fuzz/fuzz_targets/events.rs` still
      references the removed `min_version` and `max_version` fields on
      `PartRef` and lacks `min_pos`, `max_pos`, and `masked_ms`.
- [ ] [tangential · Test infrastructure] **Run mutation testing in CI**,
      running `scripts/mutants.sh --in-diff` on pull requests and the full
      suite weekly.
- [ ] [tangential · Test infrastructure] **Track throughput benchmarks in CI**
      across commits. `tests/cost.rs` already enforces request cost budgets.
- [ ] [tangential · Transactions] **Abort open interactive transactions if a
      database is deleted.** Interactive transactions on a deleted database
      currently remain open until their 30 s idle timeout expires. They count
      against the 100-transaction per-user limit during that time.
- [ ] [tangential · Transactions] **Abort interactive transactions whose
      `begin` response is lost.** The server assigns the transaction ID in the
      `begin` response. If that response is lost after the server opens the
      transaction, the client cannot abort it. The transaction then occupies a
      per-user slot until its 30 s idle timeout expires.
- [ ] [tangential · Transactions] **Clear unsent interactive commits with a
      drop guard.** Return a guard from `take_for_commit` that removes the
      `started_commits` entry on drop unless the commit was sent. `commit_txn`
      calls `forget_started_commit` on each error path, so if a client
      disconnects while `exec_ops` is running, the entry remains in
      `started_commits` for up to 5 minutes.
- [ ] [tangential · Transactions] **Track ambiguity per commit on batch-abort
      paths.** Return definite errors for commits that were not sent in an
      ambiguous manifest `PUT`. If the committer aborts a batch through
      `fail_batch` after an ambiguous `PUT`, it marks every undecided commit in
      the batch as `outcome: "unknown"`.
- [ ] [tangential · API] **Return `200` if a retried database deletion
      completes cleanup.** A retried `DELETE /v1/dbs/{db}` that finishes an
      interrupted database deletion returns `404 not_found` even though the
      request completed the deletion.
- [ ] [tangential · API] **Add an `Unknown` fallback variant to
      `baserunna_api::Role`.** Adding `#[serde(other)]` lets `baserunna-client`
      deserialize `Me` and `Member` responses from newer servers that define
      additional roles.
- [ ] [tangential · API] **Generate `openapi.json` from the API types.** Move
      all route request and response types into `baserunna-api`, derive JSON
      Schema for them, and generate `openapi.json` from the `Endpoint`
      definitions instead of maintaining `api/openapi.rs` by hand.
- [ ] [tangential · API] **Add typed request, response, and filter definitions
      for the remaining routes in `baserunna-api`.** Several routes and
      sub-schemas still use untyped JSON on the server, including `filter`
      expressions, `query_events` results, and routes not yet used by
      `baserunna-client`.
- [ ] [tangential · Refactor] **Unify internal request forwarding retry
      loops.** Consolidate the retry and lease-refresh logic shared by
      `Node::bind_events`, `Node::forward_internal`, and `Node::meta`.
- [ ] [tangential · Optimization] **Measure local lease duration from the
      manifest `PUT` start time.** The committer currently starts its local
      lease timer at the beginning of the commit attempt rather than at the
      start of the manifest `PUT`, subtracting synchronous pre-`PUT` work (such
      as synchronous tail flushes) from the local lease duration.
- [ ] [tangential · Optimization] **Read masked merge inputs in a single
      pass.** Plan each output column during the merge pass instead of running a
      separate census pass. Before merging a part that has deletions,
      `maint::census` reads its string and JSON columns to plan output
      dictionaries and paths, and the merge then fetches the same blocks a
      second time.
- [ ] [tangential · Optimization] **Distribute event part reads across cluster
      nodes.** Route part reads to peers by rendezvous hashing to utilize
      cluster-wide memory caches and CPU, then merge partial aggregate states
      on the entry node.
- [ ] [tangential · Optimization] **Cache event query lookup maps across
      queries.** Queries currently rebuild lookup maps by scanning the target
      table on every execution. Cache lookup maps by table version and update
      them from the table change feed.
- [ ] [tangential · Optimization] **Offload event part merges from the lease
      holder.** Background part merges currently run on the database lease
      holder and contend with commit batches for CPU and network bandwidth.
- [ ] [tangential · Optimization] **Immediately delete output segments of
      rejected compactions.** Rejected compactions currently leave their output
      segments for the orphan sweep to delete after `orphan_grace_ms` (2
      hours).
- [ ] [tangential · Optimization] **Flush idle database tails to reduce
      next-commit manifest size.** Flushing a non-empty tail after a database
      becomes idle reduces the size of the next manifest `PUT` at the cost of
      an extra segment `PUT`.
- [ ] [tangential · Optimization] **Box `BlobRef` in `Value` to reduce
      in-memory tail entry size.** `Entry` currently occupies 144 bytes on the
      stack because `Value` stores `BlobRef` inline. Boxing `BlobRef` shrinks
      `Entry` to 96 bytes.
- [ ] [tangential · Optimization] **Keep the manifest tail as a sorted vector
      across a batch.** Commit batches currently move the manifest tail into a
      `BTreeMap` and back into a `Vec` on every commit. Each commit still
      encodes the full manifest, so the gain is limited.
- [ ] [tangential · Optimization] **Suppress commit notices if the home node
      has 0 waiters.** Have the home node report in its notice response whether
      any requests are waiting. The lease holder can then pause notices for a
      bounded interval while new waiters on the home node poll the manifest
      every `watch_poll_ms`. Today, a lease holder that is not the database's
      rendezvous home sends a commit notice to the home node on every commit
      batch even if 0 requests are waiting there.
- [ ] [tangential · Optimization] **Event table rollups and projections.**
      Allow event tables to declare pre-aggregated rollups or alternate sort
      orders maintained automatically during seals and merges.
- [ ] [tangential · Docs] **Replace remaining wide tables in `SPEC.md`.** The
      tables in §8 (system database), §10 (error codes), and §12
      (configuration) have rows wider than 100 characters.
- [ ] [tangential · Docs] **Correct the deploy trigger statement in
      `DEPLOYING.md`.** The guide says that every push to `main` deploys, but
      the push trigger in `deploy.yml` is commented out.
