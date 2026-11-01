# TODO

Planned work, ordered by priority within each section. See [`SPEC.md`](SPEC.md)
§15 (known limitations) and §16 (future work) for design context.

## Reliability and operations

- [ ] **Report `outcome: "unknown"` on stale-target rejections if a forward was
      retried.** If a node receives `x-baserunna-stale-target`, it executes the
      request locally, assuming the peer never executed it. If a proxy or
      network duplicated the forwarded request, an earlier copy could have been
      executed by the peer's previous process (`sim_tool trace tables 7344`).
      Execute locally only if the forward was sent once; otherwise return
      `outcome: "unknown"`.
- [ ] **Fix `db_churn` seed 202681: `GET` returns `404 not_found` for an
      existing database.** During database churn, a `get` operation returns
      `404 not_found` for a database that was created and not deleted
      (`sim_tool trace db_churn 202681`).
- [ ] **Fix `db_churn` timeouts if a node crash-loops under memory pressure.**
      Under a small cache budget, a restarted node repeatedly re-reads the same
      system segments before serving requests, and fault injection can crash the
      node again before the workload finishes (`sim_tool trace db_churn 606688`).
- [ ] **Fix stream reads that exceed the request deadline.** Requests are
      canceled at `commit_timeout_ms`, which can cause large stream reads to
      fail repeatedly with `503 request deadline exceeded`
      (`sim_tool trace streams 2601188`).
- [ ] **Run garbage collection sweeps even if the lease duration is short.** A
      post-commit sweep skips execution if the lease expires while reading the
      store clock and refreshing the manifest, leaving the database unswept
      until the next commit (`sim_tool trace blobs 5000231`).
- [ ] **Fix `streams` seed 202835: expired stream records are returned after
      `retention_ms`.** Stream reads return records older than `retention_ms`
      (`sim_tool trace streams 202835`).
- [ ] **Abort open interactive transactions if a database is deleted.**
      Interactive transactions on a deleted database currently remain open
      until their 30 s idle timeout expires, consuming memory and counting
      against the 100-transaction per-user limit.
- [ ] **Test Google OAuth against Google's production endpoints.** Only
      `FakeGoogle` is tested in simulation. Refresh the cached JWKS if an ID
      token contains an unknown `kid` instead of waiting for the 1-hour cache
      TTL, and add PKCE (`code_verifier`) and an OIDC `nonce`.
- [ ] **Prometheus metrics endpoint.** Expose `/metrics` in Prometheus text
      format including the histograms from `/v1/admin/metrics`, object-store
      byte counters, CAS conflicts, lease acquisitions and steals, cache hit
      rates, open database handles, compaction and GC counters, forwarded
      requests, and rate-limit rejections.
- [ ] **Publish release binaries and a container image.** Build and publish
      release binaries and a container image so external test suites can run
      a local node in CI.
- [ ] **Reschedule retention trims if the store clock is unknown.**
      `trim_for_retention` returns early if the node lacks a store-clock
      measurement, delaying retention trimming until the next append or sweep.
- [ ] **Measure local lease duration from the manifest `PUT` start time.** The
      committer currently starts its local lease timer at the beginning of the
      commit attempt rather than at the start of the manifest `PUT`, subtracting
      synchronous pre-`PUT` work (such as synchronous tail flushes) from the
      local lease duration.
- [ ] **Release database leases concurrently on shutdown.**
      `Engine::release_leases` currently releases leases sequentially with a
      2 s timeout per database.
- [ ] **Await in-flight hedged manifest `PUT`s before returning an unknown
      outcome.** If a hedged duplicate `PUT` fails with `412` while the primary
      `PUT` is still in flight and retries are exhausted, awaiting the primary
      `PUT` can resolve the commit outcome instead of returning
      `outcome: "unknown"`.
- [ ] **Prevent event deletes from conflicting with concurrent inserts.** An
      event delete adds all unsealed rows and part references in its time range
      to the transaction read set, causing serializable conflicts with
      concurrent inserts and binds (`sim_tool trace events 2126696`). Evaluate
      the delete filter against rows and part references added after the
      snapshot inside the committer instead of tracking them in the read set.
- [ ] **Execute large event deletes asynchronously.** An event delete reads all
      candidate parts within its transaction and fails with `503` if those reads
      exceed `commit_timeout_ms` (10 s). Run large deletes as background
      operations that update part masks incrementally.
- [ ] **Bound inline deletion masks on bound parts.** The committer stores masks
      for rejected or duplicate bind slots inline regardless of size. Rewrite
      parts in background maintenance if their inline mask exceeds
      `event_mask_inline_bytes`.
- [ ] **Decode rows directly into column vectors during part builds.** Seals and
      bulk inserts decode each row into a `BTreeMap` before transposing columns,
      causing a 32 MiB bulk insert of small events to peak at roughly 11x the
      request body size (`crates/baserunna/tests/memory_bound.rs`).
- [ ] **Read masked merge inputs in a single pass.** Before merging a part that
      has deletions, `maint::census` reads its string and JSON columns to plan
      output dictionaries and paths, and the merge then fetches the same blocks
      a second time. Plan each output column during the merge pass instead.
- [ ] **Estimate granule run memory from actual string lengths.**
      `Run::run_rows` estimates string columns at 4 bytes per row (the
      dictionary code width), even if a high-cardinality column is stored
      without a dictionary. Use the column index byte length so concurrent reads
      of high-cardinality string columns do not exceed their memory share and
      fall back to serial re-reads.
- [ ] **Account for object overhead in the segment block cache.**
      `CacheVal::size` charges key and value bytes plus 48 bytes per entry,
      undercounting small entries by roughly half of their heap allocation.
- [ ] **Bound memory usage during part merges.** `merge_parts` holds the sort
      key of every live input row in memory at once. Merge sorted runs block by
      block instead.
- [ ] **Enforce `event_max_bytes` on transactional event inserts.**
      `event_max_bytes` is checked during bulk-insert binds (`Db::bind`), but
      transactional inserts are sealed in the background without checking the
      table cap. Count unsealed event bytes toward `event_max_bytes` at commit
      time.
- [ ] **Purge deleted events on idle databases and in unsealed segments.**
      Unsealed events deleted before sealing remain in LSM segments until
      compaction rewrites them, and part deletion purges only run after a
      commit. Trigger periodic flushes, compactions, and event maintenance from
      node housekeeping.
- [ ] **Account for minimum part overhead during event inserts.** Sealing writes
      at least 330 bytes per partition without checking the database quota, so
      sparse inserts across many partitions can exceed the quota after sealing.
      Charge the per-partition metadata overhead if an insert opens a partition.
- [ ] **Charge event query memory to the node `MemoryGate`.** Event query state,
      column batches, and lookup maps are bounded per query (`exec::Limits`),
      but concurrent queries are not charged against the node's global
      `memory_bytes` budget and `Node::query_slot` is not fair-shared across
      users.
- [ ] **Stream `/txn` request bodies and charge bulk insert buffers to
      `MemoryGate`.** Transaction requests parse the full JSON body into memory
      before checking `INSERT_MAX_ROWS`, and bulk insert buffers are not charged
      against the node memory budget.
- [ ] **Recheck table retention before committing expired-part drops.**
      `drop_expired` commits its planned part drops without verifying that
      `retention_ms` is unchanged, so clearing retention while a drop is in
      flight still deletes the parts.
- [ ] **Check the 4 GiB query read limit during in-flight part reads.** The read
      byte limit is only checked after each part finishes, allowing up to
      `event_read_concurrency` concurrent part reads to overshoot the limit.
- [ ] **Retry snapshot event queries only on missing table or column errors.**
      `api::events::query` retries any `400` or `404` failure at strong
      consistency, which executes invalid queries twice.
- [ ] **Acquire a query slot and extend deadlines for transactional
      `query_events` ops.** Unlike the `/rows/query` endpoint, `query_events`
      inside a transaction does not acquire a `Node::query_slot` permit or
      extend the request deadline.
- [ ] **Encode `StagedPart::slot_rows` sparsely and limit partitions per bulk
      insert.** Each `StagedPart` stores a `Runs` entry for every slot in the
      bind batch, scaling bind encoding size with `slots * parts`, and a single
      bulk insert can span an unbounded number of partitions.
- [ ] **Skip referenced parts during orphan sweep installation.** If a bind
      commit is delayed past `4 * clock_sync_max_error_ms`, a concurrent sweep
      can mark its staged parts as obsolete. Filter out parts that are
      referenced in the manifest at sweep install time.
- [ ] **Reject binds if a target column was dropped after validation.**
      `Db::bind` checks that the table still exists, but if a column is dropped
      and re-added while a bulk insert is buffered, rows are bound under the old
      column ID. Include column IDs in `Bind` and return a retryable conflict if
      any column was dropped.
- [ ] **Refresh the database name cache on `404` during strong event queries.**
      Event queries resolve the database name using a 5 s cached snapshot even
      if `consistency` is `strong`, causing a strong query to fail once with
      `404` if the database was recently recreated on another node.
- [ ] **Parse shared JSON buckets once per row during merges.** While merging a
      part with a partial path directory, `Feed::SharedPath` reparses the shared
      bucket JSON once per output path instead of once per row.

## Performance

- [ ] **Move event query block decoding and aggregation off Tokio worker
      threads.** Execute CPU-bound granule decoding and aggregation on a
      dedicated thread pool bounded by `query_concurrency` so large queries do
      not stall async workers.
- [ ] **Distribute event part reads across cluster nodes.** Route part reads to
      peers by rendezvous hashing to utilize cluster-wide memory caches and CPU,
      then merge partial aggregate states on the entry node.
- [ ] **Cache event query lookup maps across queries.** Queries currently
      rebuild lookup maps by scanning the target table on every execution. Cache
      lookup maps by table version and update them from the table change feed.
- [ ] **Cache event parts in the local disk cache tier.** Event parts are
      currently cached only in memory, so node restarts and working sets larger
      than RAM must re-fetch parts from object storage.
- [ ] **Offload event part merges from the lease holder.** Background part
      merges currently run on the database lease holder and contend with commit
      batches for CPU and network bandwidth.
- [ ] **Immediately delete output segments of rejected compactions.** Rejected
      compactions currently leave their output segments for the orphan sweep to
      delete after `orphan_grace_ms` (2 hours).
- [ ] **Flush idle database tails to reduce next-commit manifest size.**
      Flushing a non-empty tail after a database becomes idle reduces the size
      of the next manifest `PUT` at the cost of an extra segment `PUT`.
- [ ] **Box `BlobRef` in `Value` to reduce in-memory tail entry size.** `Entry`
      currently occupies 144 bytes on the stack because `Value` stores
      `BlobRef` inline. Boxing `BlobRef` shrinks `Entry` to 96 bytes and
      reduces memory usage for tails with small inline values by 20–35%.
- [ ] **Keep the manifest tail as a sorted vector across a batch.** Commit
      batches currently move the manifest tail into a `BTreeMap` and back into
      a `Vec` on every commit. Merging sorted batch writes into the sorted tail
      vector makes CPU cost proportional to batch size instead of tail size.
- [ ] **Suppress commit notices if the home node has 0 waiters.** If the lease
      holder is not the database's rendezvous home, it sends a commit notice to
      the home node on every commit batch even if 0 requests are waiting there.
      Have the home node indicate in its response whether any requests are
      waiting so the lease holder can pause notices for a bounded interval,
      during which new waiters on the home node poll the manifest every
      `watch_poll_ms`.

## Features

- [ ] **Event table rollups and projections.** Allow event tables to declare
      pre-aggregated rollups or alternate sort orders maintained automatically
      during seals and merges.
- [ ] **Expose environment variables for event query and bulk insert limits.**
      Add `BASERUNNA_*` environment variables for `query_limits`,
      `query_concurrency`, and bulk insert buffer settings in `NodeConfig`.
- [ ] **Run event retention and merges on idle databases.** Event maintenance
      is currently triggered only by a commit or sweep on an active database,
      leaving expired partitions in storage if a database is idle.
- [ ] **Point-in-time restore and backups.** Retain historical manifests and
      delay GC of referenced segments. Add database snapshot/clone via
      manifest copy-on-write and export/import.
- [ ] **Database rename and ownership transfer** between users and
      organizations.
- [ ] **Organization access controls.** Add per-database role grants within an
      organization, organization-owned service accounts (API keys), and email
      invitations for unregistered users.
- [ ] **Direct SMTP delivery and durable mail queue.** Support SMTP in addition
      to the HTTP mail webhook, and persist outbound mail so node restarts do
      not drop pending messages.
- [ ] **Query projections, ordering, and aggregates.** Support server-side
      projections, ordering by non-indexed columns, and aggregates (`count`,
      `sum`).
- [ ] **Configurable change feed retention and SSE.** Change feed cursors are
      currently valid for `txn_max_lifetime_ms` (5 minutes), matching tombstone
      retention. Per-database retention settings would allow consumers to
      resume after longer outages, and SSE streaming would avoid repeated long
      polls.
- [ ] **Stream consumer groups and storage optimizations.** Add consumer
      groups with record acknowledgements and redelivery, multi-chunk
      read-ahead for cold sequential reads, per-block field summaries for
      filter pruning, small-chunk compaction, and background retention trimming
      for idle databases.
- [ ] **Generate `openapi.json` from the API types.** Move all route request and
      response types into `baserunna-api`, derive JSON Schema for them, and
      generate `openapi.json` from the `Endpoint` definitions instead of
      maintaining `api/openapi.rs` by hand.
- [ ] **Replace `baserunna::client` with `baserunna-client`.** Add event-table
      operations and schema routes to `baserunna-client`, migrate the examples,
      and delete `crates/baserunna/src/client.rs`.
- [ ] **Cover every transaction operation in `baserunna_client::Op`.** It lacks
      `update_rows`, `stream_info`, `trim_stream`, `get_consumer`,
      `set_consumer`, the event operations, `ttl_ms` on writes, `expect_seq` on
      `append`, and `check` by value, so stream consumers cannot be used from
      the client at all.
- [ ] **Return operation results from client writes.** `Db::write` and
      `Txn::commit` drop the per-operation results that the server returns, so
      a caller cannot read, for example, the value of an `increment` or the
      count of a `delete_rows` with exactly-once semantics.
- [ ] **Type the remaining JSON in `baserunna-api`.** `query_events` results,
      row and stream `filter`s, and the routes outside `Endpoint` (KV, table
      schema changes, row routes, streams, events, organizations) are still
      built or parsed as untyped JSON on the server, so the compiler cannot
      check them against `baserunna-client`.
- [ ] **Client SDKs.** Generate TypeScript and Python clients from
      `openapi.json`.
- [ ] **Validate additional S3-compatible object stores.** Run the conformance
      suite and failover check against AWS S3, Cloudflare R2, MinIO, Azure Blob
      Storage, and Google Cloud Storage.
- [ ] **Support idempotency keys for raw KV writes.** Accept an
      `Idempotency-Key` header on `PUT` and `DELETE /v1/dbs/{db}/kv/{key}` and
      return the commit ID in a response header so clients can resolve
      ambiguous writes via `GET /commits/{id}`.
- [ ] **Support filtering whole JSON columns and reading nested JSON objects by
      path.** Event parts store only leaf JSON paths, so filtering on a root
      JSON column is rejected (`exec::Binder::filter`) and querying a non-leaf
      path returns `null` (`json::leaf`). Support `exists` on root JSON columns
      and reconstruct nested objects from their leaf paths.
- [ ] **Use exact numeric comparisons for row-table float filters.** Row-table
      filters convert integer filter arguments to `f64` (`table::filter_arg`)
      and reject integers that are not exactly representable as `f64`. Compare
      `f64` columns against integer arguments using exact numeric ordering as
      event tables do.

## Testing

- [ ] **Regenerate stale bug zoo patches.** Update patches 09, 11, 16, and 18 so
      they apply to the current tree.
- [ ] **Verify event merge liveness and part-internal invariants in
      simulation.** Check in the `events` workload that part counts remain
      bounded and completed partitions settle into a single part, and verify
      part sort order, granule statistics, bloom filters, and path directories
      in `storage_check`.
- [ ] **Run an extended multi-thousand-seed `sim_events` campaign** with larger
      tables and node counts.
- [ ] **Deliver commit notices across membership transitions.** If nodes have
      different cached views of live cluster members, a long-poll request can be
      routed to a rendezvous home node that does not receive commit notices.
      Peers can hold different membership views before all peers refresh after a
      node joins. Send commit notices to the home nodes of both previous and
      current membership views.
- [ ] **Route interactive transaction requests to the node that holds the
      transaction.** An open transaction lives in the memory of the node that
      began it, but later requests on it are routed like any database request,
      to the lease holder or the rendezvous home in the receiving node's view of
      live members. When views differ, such as after a node joins or while a
      peer is suspect, a commit reaches another node and fails with
      `txn_not_found`. Encode the node in the transaction ID and forward by it.
- [ ] **Fix the `events` fuzz target**, which no longer compiles because
      `PartRef` has no `min_version` and `max_version` fields.
- [ ] **Run libFuzzer targets in CI** with a persistent corpus, including the
      model-checked `http_txn` target, and fuzz `PartMeta::decode` and
      `ColIndex::decode` on unframed byte slices instead of CRC-framed inputs.
- [ ] **Run mutation testing in CI**, running `scripts/mutants.sh --in-diff` on
      pull requests and the full suite weekly.
- [ ] **Track cost and throughput benchmarks in CI** across commits.
- [ ] **Derive simulation settlement timeouts from fault parameters and
      separate liveness from safety failures.** Fixed settlement timeouts can
      fail if a simulated store stall exceeds the wait duration.
- [ ] **Add targeted simulation tests for all rare probes.** Ensure every probe
      in `Probe::RARE` is reached in CI.
- [ ] **Log scenario-overridden knob values in simulation traces.** Scenarios
      that override swarm configuration after generation currently log the
      pre-override values.
- [ ] **Improve guided exploration breadth.** At equal run counts, guided
      exploration reaches rare probes more frequently than a random campaign
      but covers fewer distinct features (SPEC §13.4), and failures cluster in
      a small number of seeds. Deduplicating failures by normalized message,
      weighting corpus entries by novelty, and incorporating probe-ordering
      and buggify-site features would preserve depth while improving breadth.

- [ ] **Rebase stale bug-zoo patches onto `main`.** Update
      `09-stale-names-after-purge`, `11-duplicated-forwards`, and
      `16-duplicated-forwards-across-restart` so that they can be applied to
      `main`.

## API

- [ ] **Unify internal request forwarding retry loops.** Consolidate the retry
      and lease-refresh logic shared by `Node::bind_events`,
      `Node::forward_internal`, and `Node::meta`.

## Docs

- [ ] **Replace remaining wide tables in `SPEC.md`.** The tables in §8 (system
      database), §9 (organization roles), §10 (error codes), and §12
      (configuration) have rows wider than 100 characters.
