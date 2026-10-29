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
- [ ] **Fix `db_churn` timeouts of a crash-looping node.** With a small
      cache under memory pressure, a restarted node re-reads the same system
      segments thousands of times before it serves a request, and chaos
      crashes it again first, so the workload never finishes
      (`sim_tool trace db_churn 606688` on `main`, 601086 on this branch).
- [ ] **Fix stream reads that outlast the request deadline.** Since
      requests are cancelled at `commit_timeout_ms`, a checker's stream read
      can fail with `503 request deadline exceeded` until it gives up
      (`sim_tool trace streams 2601188`; also 2602361 and 2603205, on `main`
      as on this branch).
- [ ] **Sweep when a lease is short.** A sweep starts after a commit and
      skips its run if the lease has lapsed by the time its clock read and
      refresh return, so with 1 s leases and slow store calls a database may
      go unswept until its next commit; the `blobs` workload's final GC
      check then fails (`sim_tool trace blobs 5000231`, where both sweeps
      after the last two commits found the lease expired).
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
- [ ] **Keep event deletes from conflicting with concurrent inserts.** A
      delete reads every unsealed row of its table and the part refs of its
      time range, so as a serializable transaction it conflicts with any
      insert or bind into them. A delete whose reads outlast the interval
      between binds (a cold cache over many parts) retries until the cache is
      warm, or fails with `409` after `txn_retries`; under a crash storm a
      delete may never commit (`sim_tool trace events 2126696`). Evaluate
      the delete's filter on rows and refs added since its snapshot inside
      the committer instead of tracking them as reads.
- [ ] **Run event deletes past the commit deadline.** A delete reads every
      part its filter cannot rule out by time range inside its transaction,
      and the request's commit deadline (`commit_timeout_ms`, 10 s) bounds
      those reads: with 32 concurrent reads at 25 ms, about 12,000 parts.
      Larger deletes fail with a retryable `503` that cannot succeed. Run
      them as background mutations that mask part by part and report
      progress.
- [ ] **Drop event tables in constant work.** `events::drop` deletes every
      part reference in one transaction, at about 113 bytes each, so a table
      of more than about 148,000 parts (for example 17 years of hourly
      partitions) exceeds `txn_max_write_bytes` and cannot be dropped. Keep
      part bytes per table in its state, range-delete the references, and let
      the orphan sweep collect the parts.
- [ ] **Bound bind masks.** The committer stores a bind's mask inline
      whatever its size (`Mask::Inline` in `Db::build`), so a rejected or
      repeated slot whose rows interleave with others' can add about one byte
      per row of a 16 MiB buffer to a part reference. Have maintenance
      rewrite parts whose inline mask exceeds `event_mask_inline_bytes`.
- [ ] **Build parts column-wise from encoded rows.** Seals and bulk
      inserts decode each part's rows into `table::schema::Row` maps (a
      B-tree per row) before `RowsSource` transposes them, so a part of tiny
      events holds several hundred bytes per row while it is built and a
      32 MiB bulk insert of 14-byte events peaks at about 11x its body
      (`tests/memory_bound.rs`). Decode straight into column vectors.
- [ ] **Bound merge memory.** `merge_parts` holds the sort key of every live
      input row at once, so a final merge of 4 GiB of small events holds
      hundreds of MB of keys. Merge sorted runs block by block instead.
- [ ] **Cap event part bytes on transactional inserts.** `event_max_bytes`
      is checked only when binds add parts (`Db::bind`); events inserted by
      transactions become parts through seals, which never refuse. Count
      unsealed bytes toward the cap at commit.
- [ ] **Charge sealed part overhead to inserts.** A seal writes one part
      per partition, of about 330 bytes at least, and seals are not
      checked against the quota, so 6,000 events one per hourly partition
      grow from 282 KB unsealed to 1.98 MB sealed and can carry a database
      7 times past its quota. Charge the part overhead of each partition an
      insert opens when the insert commits.
- [ ] **Charge event queries to the node memory budget.** A query's
      aggregation state, kept rows and lookup maps (up to 1 GiB each,
      `exec::Limits`) are charged to the query only, so
      `query_concurrency` queries can hold many times `memory_bytes`.
      Reserve query memory through the node's `MemoryGate`, and share query
      slots fairly among users (`Node::query_slot` is first come, first
      served).
- [ ] **Stream `/txn` bodies.** A transaction's body is parsed whole into
      JSON values, so 32 MiB of tiny `insert_events` rows briefly holds
      gigabytes before `INSERT_MAX_ROWS` refuses them. Bulk inserts stream
      their bodies (`api::events::each_event`), and their buffers are not
      charged to the node memory budget either.
- [ ] **Re-check retention when dropping expired parts.** `drop_expired`
      drops the parts its plan chose without reading the table's retention
      again, so clearing retention while a drop is in flight still drops
      them.
- [ ] **Bound event query reads while parts are in flight.** The 4 GiB read
      limit is checked between absorbed parts, so up to
      `event_read_concurrency` concurrent part reads can overshoot it.
- [ ] **Retry stale event queries only for schema misses.** A snapshot query
      failing with any `400` or `404` reruns at strong consistency with a
      fresh time limit (`api::events::query`), doubling the cost of a plain
      query mistake.
- [ ] **Treat `query_events` in transactions like event queries.** The op
      takes no query slot and gets no deadline extension.
- [ ] **Store bind slot rows sparsely and cap partitions per insert.** Each
      staged part carries a `Runs` per slot of its bind (`StagedPart::
      slot_rows`), so a bind's encoding and the committer's work grow with
      slots × parts (1,501 slots × 1,502 parts encode to 2.4 MB), and one
      bulk insert may span any number of partitions, staged one `PUT` at a
      time even after every slot's deadline has passed.
- [ ] **Re-check part references when a sweep's obsolete parts install.**
      The sweep's safety for parts, as for blobs, rests on the stage-TTL
      margin alone; a bind whose manifest `PUT` lands more than
      `4 × clock_sync_max_error_ms` after its attempt began could see its
      part collected. Skip parts referenced at install time.
- [ ] **Refuse binds whose columns were dropped since validation.** A bulk
      insert validated before a column is dropped and re-added binds its
      rows under the old column id, so the value reads null or the new
      default (`Db::bind` checks the table, not its columns). Carry the
      validating schema's column ids in `Bind` and refuse, retryably, a
      bind naming a dropped one.
- [ ] **Re-resolve database names for strong event queries.** An event
      query checks that its database exists at a snapshot 5 s old whatever
      `consistency` its body asks for, so a strong query on a node that
      cached a name deleted and recreated elsewhere fails once with `404
      database deleted` before the name is looked up again.
- [ ] **Merge partial-directory parts in one pass per row.** A merge reads a
      path that an input keeps in a shared bucket by parsing that bucket's
      JSON once for every output path (`Feed::SharedPath`), so merging parts
      with partial directories costs paths × rows × bucket size.
- [ ] **Sweep staged event parts after the last event table is dropped.**
      Sweeps list `part/` only while a database has event tables or part
      bytes, so a part staged by a bulk insert that races a drop of the last
      table stays an orphan until another event table is created.

## Performance

- [ ] **Run event query decoding off the async runtime.** A query decodes
      blocks and folds aggregates on a tokio worker, so a large query delays
      other requests on that worker. Move CPU-bound granule work to a
      blocking pool or a dedicated executor, bounded by `query_concurrency`.
- [ ] **Split event queries across nodes.** One node reads every part of a
      query. Fan parts out to peers by rendezvous hash, so their caches
      stay warm, and merge partial aggregate states (they already encode and
      merge) on the entry node.
- [ ] **Cache event lookup maps.** Each query rebuilds lookup maps from the
      whole row table. Cache them per table version and patch them from the
      table's change feed.
- [ ] **Keep event parts in the local disk tier.** Parts use the memory cache
      only, so a restarted node or a working set larger than memory reads
      parts from the store again.
- [ ] **Dispatch event merges to other nodes.** Merges run on the lease
      holder and compete with its commits for CPU and bandwidth.
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

- [ ] **Event rollups and projections.** Let a table declare aggregates by
      time bucket (or alternative sort orders) that seals and merges
      maintain, so dashboards read small parts.
- [ ] **Configure event query limits by environment.** `NodeConfig`
      `query_limits`, `query_concurrency` and the bulk insert buffer have no
      `BASERUNNA_*` variables.
- [ ] **Run event retention on idle databases.** Retention and merges are
      scheduled after commits and sweeps, which run only on active
      databases; a dormant database keeps expired partitions (and pays for
      them) until it is used.
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
- [ ] **Client SDKs.** Generate TypeScript and Python clients from
      `openapi.json`, and publish the Rust `client` module as a standalone
      crate.
- [ ] **Validate additional S3-compatible object stores.** Run the conformance
      suite and failover check against AWS S3, Cloudflare R2, MinIO, Azure Blob
      Storage, and Google Cloud Storage.
- [ ] **Support idempotency keys for raw KV writes.** Accept an
      `Idempotency-Key` header on `PUT` and `DELETE /v1/dbs/{db}/kv/{key}` and
      return the commit ID in a response header so clients can resolve
      ambiguous writes via `GET /commits/{id}`.

## Testing

- [ ] **Check event maintenance liveness and storage invariants in
      simulation.** The `events` workload never bounds part counts under
      load or checks that ended partitions settle, never drops or re-adds
      columns or recreates a busy table under load, and deletes only by
      `uid` over all time. `storage_check` does not look for unreferenced
      `part/` objects or check part sortedness, statistics, blooms and
      directories against their rows. Steady-insert starvation of merges
      (`events::tests::liveness`) passed every campaign.
- [ ] **Run a long event-table campaign.** `sim_events` runs 24 seeds in CI;
      run thousands, with larger tables and more nodes, before relying on
      merges at scale.
- [ ] **Deliver commit notices across membership transitions.** If nodes have
      different cached views of live cluster members, a long-poll request can be
      routed to a rendezvous home node that does not receive commit notices.
      Peers can hold different membership views before all peers refresh after a
      node joins, or between refreshes if `members_refresh_ms` is greater than
      `2 × heartbeat_ms` in simulation swarm configurations. Derive the
      membership liveness window from both intervals, and send commit notices to
      the home nodes of both previous and current membership views.
- [ ] **Run libFuzzer targets in CI** with a persistent corpus, including the
      model-checked `http_txn` target. The `events` target feeds raw bytes
      to `PartMeta::decode` and `ColIndex::decode`, which check a frame CRC
      first; fuzz the bytes after `unframe` instead.
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

- [ ] **Share one forwarding loop.** `Node::bind_events` and
      `Node::forward_internal` repeat the retry and lease-refresh logic of
      `Node::meta` for forwarded system commands.

## Docs

- [ ] **Replace remaining wide tables in `SPEC.md`.** The tables in §8 (system
      database), §9 (organization roles), §10 (error codes), and §12
      (configuration) have rows wider than 100 characters.
