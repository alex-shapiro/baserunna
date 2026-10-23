# TODO

Planned work not included in v1, ordered by priority within each section.
See [`SPEC.md`](SPEC.md) §15 (known limitations) and §16 (future work) for
design context.

## Before production use

- [ ] **Run integration tests against production object stores.** `CloudStore`
      (S3, GCS, Azure, R2, MinIO) is currently only compile-tested. Add an
      opt-in integration test suite gated on credentials that runs the
      `ObjectStore` conformance suite: conditional `PUT` (`If-Match` and
      `If-None-Match`), list-after-write, and byte-range `GET`. Run the
      `multi_node` example against MinIO in CI. Verify that each provider's
      conditional-write semantics match `SimStore` and that `last_modified`
      timestamps are monotonic (SPEC §5.8).
- [ ] **Test Google OAuth against Google's production endpoints.** Currently
      only `FakeGoogle` is tested. Add an OIDC `nonce` and PKCE
      (`code_verifier`) to the authorization-code flow in addition to `state`.
- [ ] **Prometheus metrics endpoint.** Expose `/metrics` in Prometheus text
      format including the histograms from `/v1/admin/metrics`, object-store
      byte counters, CAS conflicts, lease acquisitions and steals, cache hit
      rates, open database handles, compaction and GC counters, forwarded
      requests, and rate-limit rejections.
- [ ] **Publish release binaries and a container image.** Build and publish
      release binaries and a container image so external test suites can run
      a local node in CI.
- [ ] **Time out store-clock probes.** A stalled probe holds the node-wide
      `StoreClock::sync` lock until the store request times out, blocking
      callers of `store_time_ms`. Because probes slower than
      `2 × clock_sync_max_error_ms` are discarded as imprecise, probes should
      time out at that bound.
- [ ] **Reschedule retention trims if the store clock is unknown.**
      `trim_for_retention` returns early if the node lacks a store-clock
      measurement, delaying retention trimming until the next append or sweep.
- [ ] **Measure local lease duration from the manifest `PUT` start time.** The
      committer currently starts its local lease timer at the beginning of the
      commit attempt rather than at the start of the manifest `PUT`, subtracting
      synchronous pre-`PUT` work (such as synchronous tail flushes) from the
      local lease duration.
- [ ] **Bound memory used by open database handles.** Each open handle retains
      its current manifest and cached compressed tail pages, and handle count
      is currently bounded only by idle eviction (`db_idle_evict_ms`). Enforce
      a byte budget across open handles with LRU eviction.
- [ ] **Release database leases concurrently on shutdown.**
      `Engine::release_leases` currently releases leases sequentially with a
      2 s timeout per database.
- [ ] **Await in-flight hedged manifest `PUT`s before returning an unknown
      outcome.** If a hedged duplicate `PUT` fails with `412` while the primary
      `PUT` is still in flight and retries are exhausted, awaiting the primary
      `PUT` can resolve the commit outcome instead of returning
      `outcome: "unknown"`.

## Performance

- [ ] **Immediately delete output segments of rejected compactions.** Rejected
      compactions currently leave their output segments for the orphan sweep to
      delete after `orphan_grace_ms` (2 hours).
- [ ] **Flush idle database tails to reduce next-commit manifest size.**
      Flushing a non-empty tail after a database becomes idle reduces the size
      of the next manifest `PUT` at the cost of an extra segment `PUT`.
- [ ] **Keep the manifest tail as a sorted vector across a batch.** Commit
      batches currently move the manifest tail into a `BTreeMap` and back into
      a `Vec` on every commit. Merging sorted batch writes into the sorted tail
      vector makes CPU cost proportional to batch size instead of tail size.
- [ ] **Send commit notices only to nodes with waiting requests.** A node
      sends its rendezvous home one internal request per manifest install,
      even when no request waits there, and requests waiting elsewhere (after
      a failed forward, or on a stale view of the lease) are never notified
      and wake only when they poll. Nodes could register their interest in a
      database with the committing node, for as long as requests wait.

## Features

- [ ] **Point-in-time restore and backups.** Retain historical manifests and
      delay GC of referenced segments. Add database snapshot/clone via
      manifest copy-on-write and export/import.
- [ ] **Database rename and ownership transfer** between users and
      organizations.
- [ ] **One long-poll limit for change feeds and stream reads.** Change
      feeds wait up to 60 s (`wait_ms`), stream record reads up to 20 s. A
      snapshot waiter's store cost does not depend on the limit, so pick one
      that fits common load balancer idle timeouts and use it for both.
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

- [ ] **Support idempotency keys for raw KV writes.** Accept an
      `Idempotency-Key` header on `PUT` and `DELETE /v1/dbs/{db}/kv/{key}` and
      return the commit ID in a response header so clients can resolve
      ambiguous writes via `GET /commits/{id}`.

## Testing

- [ ] **Fix the `auth` scenario's empty node listing.** About 1 in 24,000
      `auth` seeds fail with `Err("nodes")` (e.g. `sim_tool trace auth
      5006829`): `/v1/admin/nodes` lists only members read from the system
      database, which can be empty after a crash before the node's first
      heartbeat lands. The listing should always include the serving node.
- [ ] **Run libFuzzer targets in CI** with a persistent corpus, including the
      model-checked `http_txn` target.
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
