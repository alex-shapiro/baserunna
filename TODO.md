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
- [ ] **Suppress commit notices if the home node has 0 waiters.** If the lease
      holder is not the database's rendezvous home, it sends a commit notice to
      the home node on every commit batch even if 0 requests are waiting there.
      Have the home node indicate in its response whether any requests are
      waiting so the lease holder can pause notices for a bounded interval,
      during which new waiters on the home node poll the manifest every
      `watch_poll_ms`.

## Features

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

- [ ] **Support idempotency keys for raw KV writes.** Accept an
      `Idempotency-Key` header on `PUT` and `DELETE /v1/dbs/{db}/kv/{key}` and
      return the commit ID in a response header so clients can resolve
      ambiguous writes via `GET /commits/{id}`.

## Testing

- [ ] **Deliver commit notices across membership transitions.** If nodes have
      different cached views of live cluster members, a long-poll request can be
      routed to a rendezvous home node that does not receive commit notices.
      Peers can hold different membership views before all peers refresh after a
      node joins, or between refreshes if `members_refresh_ms` is greater than
      `2 × heartbeat_ms` in simulation swarm configurations. Derive the
      membership liveness window from both intervals, and send commit notices to
      the home nodes of both previous and current membership views.
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

- [ ] **Rebase stale bug-zoo patches onto `main`.** Update
      `09-stale-names-after-purge`, `11-duplicated-forwards`, and
      `16-duplicated-forwards-across-restart` so that they can be applied to
      `main`.

## Docs

- [ ] **Replace remaining wide tables in `SPEC.md`.** The tables in §8 (system
      database), §9 (organization roles), §10 (error codes), and §12
      (configuration) have rows wider than 100 characters.
