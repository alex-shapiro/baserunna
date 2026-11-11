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
- **untriaged**: The item has not yet been triaged.

Each blocker identifies a failing test marked `#[ignore]`. Remove the attribute
with the fix. Numbers in blocker items are measured unless marked "extrapolated"
or "calculated". Run the heavy tests one at a time, because a single test can
use up to 5 GB of memory. Items in the other tiers are verified by code reading
only.

## Blocker

- [ ] [blocker · Stale strong read] **Enable the late-apply fault in the swarm
      profiles.** Enable `StoreFaults::p_late_apply` in the swarm profiles and
      list the fault in SPEC §13.2. `StoreFaults::p_late_apply` makes the
      simulated store return an error for a `PUT` and apply it later. First
      triage `db_churn` seed 2037. With `p_late_apply` at 0.01 and
      `late_apply_ms` at `(10, 2 * lease)`, `tests/sim.rs` and
      `tests/coverage.rs` pass. Campaigns over 25 scenarios had 1 failure in
      7,170 runs: `db_churn` seed 2037 timed out after a node crash loop. The
      seed passes without late apply. A second `db_churn` campaign with late
      apply had 0 failures in 10,592 runs.
- [ ] [blocker · Tenant resource exhaustion] **Charge event query memory to the
      node `MemoryGate` and fair-share `Node::query_slot`.** Reserve
      `MemoryGate` capacity for event queries and share `Node::query_slot`
      permits fairly across tenants. Eight concurrent queries held 2,630 MiB
      against a 768 MiB `memory_bytes` budget without being queued or rejected.
      A single heavy query holds 55 MiB to 550 MiB, and `query_concurrency`
      defaults to the core count. In addition, a user held all 16 query slots
      for 12.5 s per query while another user's 1-row query timed out with `503`
      after 5 s. Tests: `concurrent_event_queries_respect_node_memory_budget`
      and `one_user_cannot_hold_every_query_slot` in
      `tests/event_query_limits.rs`.
- [ ] [blocker · Tenant resource exhaustion] **Acquire a query slot for
      transactional `query_events` ops and extend their deadline.** Acquire a
      `Node::query_slot` permit and apply the event query deadline if `/txn`
      executes `query_events`. In `/txn`, 32 concurrent event queries held twice
      the heap of 16, whereas the event query route (`/events/{t}/rows/query`)
      stayed flat. A query that returned `200` after 12.5 s on the event query
      route also failed with `503` and `outcome: "unknown"` after 10 s in
      `/txn`. Tests: `transactional_event_queries_take_query_slots` and
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
      request deadline pauses while a body arrives, and the server does not set
      a read timeout. A body that stopped halfway remained open after 10 minutes
      with its buffer held, both through the router and over TCP. Tests:
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
      level, and back off if background maintenance fails. With a single
      corrupt segment, compaction failed on 1,204 of 1,300 commits without
      backoff, and the sweep failed on every commit after becoming due. Stored
      bytes grew to 27 times the live data size. Compaction, sweep, flush, trim,
      and event maintenance failures log only at `debug` level and do not record
      a metric in `/v1/admin/metrics`. Test:
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
      `DELETE /v1/dbs/{db}`, the data is inaccessible through the API, and all
      database objects are purged after `db_delete_grace_ms` (10 minutes, not
      configurable). `SPEC.md` §8 instructs operators to restore a lost manifest
      from a backup or object version, whereas `DEPLOYING.md` instructs them to
      disable bucket versioning. This item does not have a failing test.

## Nice-to-have

- [ ] [nice-to-have · Verification] **Enable the CI triggers.** Uncomment the
      pull request, push, and schedule triggers in `.github/workflows/test.yml`
      and `deploy.yml`. Today both workflows run only on `workflow_dispatch`.
- [ ] [nice-to-have · Verification] **Add targeted simulation tests for all
      rare probes.** Add targeted tests for the remaining 23 probes in
      `Probe::RARE`. `SIM_REQUIRE_PROBES` enforces them only on the scheduled
      campaign, which is disabled.
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
      database.** Investigate why a `get` operation returned `404 not_found` for
      a live database during database churn (`sim_tool trace db_churn 202681`).
      The seed passes on the current tree. If a delayed `DELETE` ran against the
      recreated database, this item is a blocker.
- [ ] [nice-to-have · Unexplained simulator failure] **Find the cause of
      `accounts` session failures after an account is disabled and
      re-enabled.** Investigate why a valid session was rejected after its
      account was disabled and re-enabled (`sim_tool trace accounts 9506242`).
      The seed passes on the current tree. If a delayed duplicate disable ran
      after the re-enable, this item is a blocker.
- [ ] [nice-to-have · Duplicate requests] **Do not execute keyless writes
      locally after a stale-target rejection.** On `x-baserunna-stale-target`,
      execute the request locally only if it is a `GET` or has an idempotency
      key, and return `outcome: "unknown"` otherwise. Also state in
      `DEPLOYING.md` that proxies must not retry peer traffic. Today the node
      executes the request locally, which can apply a write twice if a proxy
      duplicated the forwarded request (`sim_tool trace tables 7344`).
- [ ] [nice-to-have · Duplicate requests] **Persist started interactive commits
      across nodes and restarts.** Derive each transaction ID from its commit ID
      so that any node can check the commit record in the manifest. A node
      tracks started interactive commits only in memory. After a node restart,
      or on a peer node, a duplicate request receives a retryable
      `404 txn_not_found` and can cause the client to re-execute the
      transaction. `baserunna-client` does not send duplicate commits, so this
      needs a proxy that retries a delivered `POST`.
- [ ] [nice-to-have · Duplicate requests] **Set `retryable: false` if `outcome`
      is `"unknown"` and the write omits an idempotency key.** A `503` response
      with `outcome: "unknown"` sets `retryable: true`. The API docs and
      `baserunna-client` check the commit status first, but a client that
      inspects only `retryable` can apply a non-idempotent write twice.
- [ ] [nice-to-have · Duplicate requests] **Reject `Idempotency-Key` on routes
      that ignore it, and support it on `DELETE /v1/dbs/{db}/kv/{key}`.** Raw
      key-value deletes and the row routes ignore the header without an error,
      so `GET /commits/{id}` reports `not_committed` for a delete that was
      applied.
- [ ] [nice-to-have · Duplicate requests] **Display `outcome: "unknown"` in the
      web console.** The console displays only the error message, so an
      ambiguous write appears to have failed.
- [ ] [nice-to-have · Event tables] **Prevent event deletes from conflicting
      with concurrent inserts.** Evaluate the delete filter against rows and
      part references added after the snapshot inside the committer instead of
      tracking them in the read set. Today an event delete adds all unsealed
      rows and candidate part references to the read set, causing serializable
      conflicts with concurrent inserts and binds
      (`sim_tool trace events 2126696`).
- [ ] [nice-to-have · Event tables] **Execute large event deletes
      asynchronously.** Run large event deletes as background operations that
      update part masks incrementally. Today an event delete reads all candidate
      parts within its transaction and fails with `503` if those reads exceed
      `commit_timeout_ms` (10 s).
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
      parts.** Rewrite parts in background maintenance if their inline deletion
      mask exceeds `event_mask_inline_bytes`. The committer stores masks for
      rejected or duplicate bind slots inline regardless of size.
- [ ] [nice-to-have · Event tables] **Decode rows directly into column vectors
      during part builds.** Transpose columns without decoding each row into an
      intermediate `BTreeMap`, and update `build_cost` for small rows. A 32 MiB
      bulk insert of small events peaks at roughly 11x the request body size
      (`crates/baserunna/tests/memory_bound.rs`), and `build_cost`
      underestimates small rows by about 4x.
- [ ] [nice-to-have · Event tables] **Enforce `event_max_bytes` on
      transactional event inserts.** Count unsealed event bytes toward
      `event_max_bytes` at commit time. Today `event_max_bytes` is checked
      during bulk-insert binds (`Db::bind`), but transactional inserts are
      sealed in the background without checking the table cap.
- [ ] [nice-to-have · Event tables] **Account for minimum part overhead during
      event inserts.** Charge the per-partition metadata overhead if an insert
      opens a partition. Sealing writes at least 330 bytes per partition without
      checking the database quota, so sparse inserts across many partitions can
      exceed the quota after sealing.
- [ ] [nice-to-have · Event tables] **Check the 4 GiB query read limit during
      in-flight part reads.** Check the read byte limit while part reads are in
      flight instead of only after each part finishes, so concurrent part reads
      cannot overshoot the limit.
- [ ] [nice-to-have · Event tables] **Retry snapshot event queries only on
      missing table or column errors.** Restrict the strong-consistency retry in
      `api::events::query` to missing-table and missing-column errors instead of
      retrying any `400` or `404` failure.
- [ ] [nice-to-have · Event tables] **Refresh the database name cache on `404`
      during strong event queries.** Refresh the 5 s database name cache if a
      strong event query receives `404` so that a recently recreated database
      does not fail the first query.
- [ ] [nice-to-have · Event tables] **Parse shared JSON buckets once per row
      during merges.** Parse each shared JSON bucket once per row in
      `Feed::SharedPath` instead of reparsing it for every output path.
- [ ] [nice-to-have · Event tables] **Move event query block decoding and
      aggregation off Tokio worker threads.** Execute CPU-bound granule
      decoding and aggregation on a dedicated thread pool bounded by
      `query_concurrency` so large queries do not stall async workers.
- [ ] [nice-to-have · Event tables] **Cache event parts in the local disk cache
      tier.** Store fetched event parts in the local disk cache tier so node
      restarts and working sets larger than RAM do not re-fetch them from object
      storage.
- [ ] [nice-to-have · Event tables] **Expose environment variables for event
      query and bulk insert limits.** Add `BASERUNNA_*` environment variables
      for `query_limits`, `query_concurrency`, `event_max_bytes`, and bulk
      insert buffer settings in `NodeConfig`.
- [ ] [nice-to-have · Event tables] **Support filtering whole JSON columns and
      reading nested JSON objects by path.** Support `exists` on root JSON
      columns and reconstruct nested objects from their leaf paths. Event parts
      store only leaf JSON paths, so filtering on a root JSON column is rejected
      (`exec::Binder::filter`) and querying a non-leaf path returns `null`
      (`json::leaf`).
- [ ] [nice-to-have · Idle databases] **Run maintenance on idle databases from
      node housekeeping.** Trigger periodic flushes, compactions, retention
      trims, and event maintenance from node housekeeping. Today these tasks run
      only after a commit or sweep on an active database, leaving expired or
      deleted data in idle databases and after skipped trims
      (`sim_tool trace streams 8100221`).
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
      startup preflight.** Verify store clock coherence during the startup
      preflight in addition to the conditional-write checks, because garbage
      collection depends on a coherent store clock (SPEC §15).
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
      shutdown.** Release leases concurrently in `Engine::release_leases`
      instead of sequentially with a 2 s timeout per database, so nodes with
      many leased databases finish within the shutdown grace period.
- [ ] [nice-to-have · Operations] **Retry startup preflight checks during store
      outages.** Retry the conditional-write preflight check with exponential
      backoff before exiting. If the object store is unreachable at startup,
      the preflight check fails immediately and the process exits.
- [ ] [nice-to-have · Operations] **Await in-flight hedged manifest `PUT`s
      before returning an unknown outcome.** Await the primary manifest `PUT` if
      a hedged duplicate fails with `412` and retries are exhausted, so the
      committer can determine the outcome instead of returning
      `outcome: "unknown"`.
- [ ] [nice-to-have · Lease] **Take the lease only from a successful manifest
      `PUT`.** Remove the retry-loop restore that takes the lease from a
      manifest written by an ambiguous `PUT` from this node. This product
      tradeoff needs a decision. The restore has caused three stale strong
      reads, and it requires the node-wide `Shared::own_leases` map on the
      strong-read path. Without the restore, the lease stays on the database
      handle. The cost is a few manifest `GET`s after a lost response. In the
      15 s after a lost response, reads every 1.5 s send 10 `GET`s instead of
      8, and reads every 250 ms send 14 `GET`s instead of 0. A hot database
      sends one lease `PUT` with or without the restore. Recommended. If the
      restore stays, also restore the lease from a manifest `PUT` that the
      store applied after its batch ended.
- [ ] [nice-to-have · Operations] **Account for object overhead in the segment
      block cache.** Increase the per-entry overhead in `CacheVal::size` above
      48 bytes so small block-cache entries are not undercounted by half of
      their heap allocation.
- [ ] [nice-to-have · Operations] **Direct SMTP delivery and durable mail
      queue.** Support SMTP in addition to the HTTP mail webhook, and persist
      outbound mail so node restarts do not drop pending messages.
- [ ] [nice-to-have · Accounts] **Harden Google OAuth and test it against
      Google's production endpoints.** Refresh the cached JWKS on an unknown
      `kid`, add PKCE (`code_verifier`) and an OIDC `nonce`, and test against
      Google's production endpoints in addition to `FakeGoogle`.
- [ ] [nice-to-have · Accounts] **Clear cached logins on all nodes if a user is
      disabled.** Broadcast user-disable events or invalidate peer auth caches
      so other nodes do not accept disabled credentials for up to
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
      deletes in `where` and `delete_range`.** Require an explicit full-delete
      flag before an empty `where` selector deletes all table rows or an empty
      `prefix` in `delete_range` deletes all keys.
- [ ] [nice-to-have · Streams] **Fix stream reads that exceed the request
      deadline.** Extend or paginate cold filtered stream reads so slow object
      stores do not fail repeatedly at `commit_timeout_ms` with
      `503 request deadline exceeded` (`sim_tool trace streams 2601188`).
- [ ] [nice-to-have · Streams] **Pin the lower bound of a reverse `read_stream`
      that uses `tail`.** Have `Op::next_page` replace `tail` with `from_seq` on
      the first page of a reverse stream read so records appended between pages
      do not shift the window and omit the oldest records.
- [ ] [nice-to-have · Streams] **Stream consumer groups and storage
      optimizations.** Add consumer groups with record acknowledgments and
      redelivery, multi-chunk read-ahead for cold sequential reads, per-block
      field summaries for filter pruning, and small-chunk compaction.
- [ ] [nice-to-have · Cluster] **Deliver commit notices across membership
      transitions.** Route or forward commit notices across membership view
      changes during the `members_refresh_ms` (30 s) window after a node joins,
      so long-poll waiters do not fall back to `watch_poll_ms`.
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
      `Txn::commit`.** Return the per-operation results from `Db::write` and
      `Txn::commit` instead of discarding values such as the new `increment`
      value or the `delete_rows` count.
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
      Support per-database change-feed retention settings and SSE streaming so
      consumers can resume after outages longer than `txn_max_lifetime_ms` (5
      minutes) without repeated long polls.

## Tangential

- [ ] [tangential · Simulator] **Fix `db_churn` timeouts if a node crash-loops
      under memory pressure.** Avoid repeated system-segment re-reads if a node
      restarts under a tiny cache budget (`sim_tool trace db_churn 606688`,
      which uses 2 KiB segments and a 4 KiB disk cache).
- [ ] [tangential · Simulator] **Order manifest checks and strong reads that
      share a paused-clock instant.** `Db::refresh` lets a strong read reuse a
      manifest check from the same `Instant`. Under a paused clock, the read
      then returns an old version in two cases. A real clock does not produce
      equal instants:
      - In case 1, node A's `GET` returns version N, node B commits N+1, and a
        second strong read on A reuses the `GET`.
      - In case 2, A's `GET` reads N at T and its reply arrives at T + 20 ms, B
        commits N+1 at T, and a second strong read on A starts at T, waits for
        the load lock, and accepts the reply.

      The losing candidate of the late-apply fix ordered equal instants with an
      `OrderedInstant` (an `Instant` and a process-wide sequence number). A
      check answered a read only if the check started at or after the read's
      instant and finished after the read. That rule fixes case 1 with no
      change to `tests/cost.rs`, but does not fix case 2. Ordering checks by
      their start sequence fixes both cases, but increases
      `node_overhead_1_node` from 121 to 151 manifest `GET`s and
      `node_overhead_3_nodes` from 140.3 to 170.3.
- [ ] [tangential · Simulator] **Tolerate transient form errors in the
      `console` workload.** Allow transient password-change timeouts at
      `/console/account/password` during simulated storage stalls instead of
      failing the workload (`sim_tool trace console 24010902`).
- [ ] [tangential · Simulator] **Run garbage collection sweeps even if the
      lease duration is short.** Run post-commit sweeps even if a short
      simulator lease or long store stall causes the lease to expire while
      reading the store clock (`sim_tool trace blobs 5000231`).
- [ ] [tangential · Simulator] **Log scenario-overridden knob values in
      simulation traces.** Log swarm knob values after scenario overrides
      instead of logging the pre-override values.
- [ ] [tangential · Simulator] **Improve guided exploration breadth.** Measure
      guided exploration against a random campaign with `SIM_COMPARE` first.
      Candidate improvements include deduplicating failures by normalized
      message, weighting corpus entries by novelty, and adding probe-ordering
      and buggify-site features.
- [ ] [tangential · Simulator] **Run an extended multi-thousand-seed
      `sim_events` campaign** with larger tables and node counts.
- [ ] [tangential · Test infrastructure] **Regenerate stale bug zoo patches.**
      Update the 10 bug zoo patches that no longer apply to the current tree,
      and correct the statement in `bugzoo/README.md` that all patches apply.
- [ ] [tangential · Test infrastructure] **Update the `events` fuzz target for
      the current `PartRef` fields.** Replace `min_version` and `max_version`
      with `min_pos`, `max_pos`, and `masked_ms` in
      `fuzz/fuzz_targets/events.rs`.
- [ ] [tangential · Test infrastructure] **Run mutation testing in CI**,
      running `scripts/mutants.sh --in-diff` on pull requests and the full
      suite weekly.
- [ ] [tangential · Test infrastructure] **Track throughput benchmarks in CI**
      across commits. `tests/cost.rs` already enforces request cost budgets.
- [ ] [tangential · Transactions] **Abort open interactive transactions if a
      database is deleted.** Abort open interactive transactions if a database
      is deleted so they do not occupy per-user transaction slots until the 30 s
      idle timeout expires.
- [ ] [tangential · Transactions] **Abort interactive transactions whose
      `begin` response is lost.** Abort interactive transactions if the `begin`
      response fails to reach the client, freeing the per-user slot before the
      30 s idle timeout.
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
- [ ] [tangential · Change feed] **Notify waiters about a commit that the
      store applied late.** If the store applies a commit after its manifest
      `PUT` failed, the node sends no commit notice (`Shared::committed`) and
      starts no event maintenance (`Shared::events_changed`). Change-feed
      waiters on other nodes find the commit at their next poll.
- [ ] [tangential · API] **Return `200` if a retried database deletion
      completes cleanup.** Return `200` instead of `404 not_found` if a
      retried `DELETE /v1/dbs/{db}` finishes an interrupted database deletion.
- [ ] [tangential · API] **Add an `Unknown` fallback variant to
      `baserunna_api::Role`.** Add `#[serde(other)]` to `baserunna_api::Role` so
      `baserunna-client` can deserialize `Me` and `Member` responses from newer
      servers that define additional roles.
- [ ] [tangential · API] **Generate `openapi.json` from the API types.** Move
      all route request and response types into `baserunna-api`, derive JSON
      Schema for them, and generate `openapi.json` from the `Endpoint`
      definitions instead of maintaining `api/openapi.rs` by hand.
- [ ] [tangential · API] **Add typed request, response, and filter definitions
      for the remaining routes in `baserunna-api`.** Replace untyped JSON in
      `filter` expressions, `query_events` results, and the remaining server
      routes with typed definitions in `baserunna-api`.
- [ ] [tangential · Refactor] **Unify internal request forwarding retry
      loops.** Consolidate the retry and lease-refresh logic shared by
      `Node::bind_events`, `Node::forward_internal`, and `Node::meta`.
- [ ] [tangential · Optimization] **Measure local lease duration from the
      manifest `PUT` start time.** Start the local lease timer at the start of
      the manifest `PUT` instead of at the start of the commit attempt, so
      synchronous pre-`PUT` work is not subtracted from the lease duration.
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
      queries.** Cache lookup maps by table version and update them from the
      table change feed instead of scanning the target table on every query.
- [ ] [tangential · Optimization] **Offload event part merges from the lease
      holder.** Run background part merges on non-lease nodes so they do not
      contend with commit batches for CPU and network bandwidth.
- [ ] [tangential · Optimization] **Immediately delete output segments of
      rejected compactions.** Delete output segments as soon as a compaction is
      rejected instead of waiting for the orphan sweep after `orphan_grace_ms`
      (2 hours).
- [ ] [tangential · Optimization] **Flush idle database tails to reduce
      next-commit manifest size.** Flush non-empty tails if a database becomes
      idle to reduce the size of the next manifest `PUT` at the cost of an extra
      segment `PUT`.
- [ ] [tangential · Optimization] **Box `BlobRef` in `Value` to reduce
      in-memory tail entry size.** Box `BlobRef` in `Value` to shrink `Entry`
      from 144 bytes to 96 bytes on the stack.
- [ ] [tangential · Optimization] **Keep the manifest tail as a sorted vector
      across a batch.** Keep the manifest tail in a sorted `Vec` across a commit
      batch instead of converting it to a `BTreeMap` and back on every commit.
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
- [ ] [tangential · Docs] **Replace remaining wide tables in `SPEC.md`.**
      Reformat the tables in `SPEC.md` §8, §10, and §12 to fit within 100
      columns.
- [ ] [tangential · Docs] **Correct the deploy trigger statement in
      `DEPLOYING.md`.** Update `DEPLOYING.md` to reflect that the push trigger
      in `deploy.yml` is commented out.

## Untriaged

- [ ] [untriaged · Garbage collection] **Keep a very late manifest `PUT` from
      binding a swept blob.** A manifest `PUT` that the store applies more than
      1 hour late can bind a staged blob after a sweep listed the blob as an
      orphan. The `Work::Obsolete` install does not check blobs and parts
      against its base. The defect needs a store that applies a `PUT` hours
      late.
- [ ] [untriaged · Garbage collection] **Sweep objects that a late `PUT` writes
      after a database deletion.** A non-manifest `PUT` that the store applies
      after the second deletion pass leaves an orphan object. The sweep does
      not remove that object.
