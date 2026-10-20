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
- [ ] **Bound store clock probes by time.** One stalled probe request holds
      the node-wide `StoreClock::sync` lock until the stall ends (up to the
      store's request timeout), blocking every caller of `store_time_ms`:
      sweeps, flushes, compactions, and commits that use TTLs or blobs. A
      probe slower than about `2 × clock_sync_max_error_ms` is discarded as
      too imprecise anyway, so it can time out at that bound and count as an
      imprecise attempt.
- [ ] **Keep retention trims scheduled while the store clock is unknown.**
      `trim_for_retention` returns without trimming when the node has no
      recent store clock measurement, and nothing retries the trim until the
      stream's next append or the next sweep, so a quiet stream can stay past
      its retention for a sweep interval.
- [ ] **Count the holder's lease from the manifest `PUT`, not the attempt.**
      The committer records its own lease as starting when the commit attempt
      began (`note_own_lease(&m, attempt_started)`), so time spent before the
      `PUT`, such as a commit that flushes the tail itself and writes segments
      first, is lost from the lease. Peers count a lease only from when they
      observe it, after the `PUT` is sent, so the send time is the safe start.
      A commit can otherwise land holding no usable lease, and the sweep or
      compaction it triggers stops at once; simulation seed `mix 60000297`
      (with the `chaos.stop` knob) fails this way, after buggify delays two
      commits' writes by most of a lease.
- [ ] **Bound the memory of open database handles.** Each open handle keeps
      its current manifest, with a tail of up to four times `tail_max_bytes`,
      and, on the node that commits to it, the compressed tail pages of its
      last manifest write; only idle eviction (`db_idle_evict_ms`) limits how
      many are open, so a node serving many busy databases has no memory
      ceiling for them, unlike the block cache. Counting them against a budget
      and evicting the least recently used handles would give one.
- [ ] **Release leases concurrently on shutdown.** `Engine::release_leases`
      releases one database at a time, a manifest write each, allowing each
      up to 2 s, so shutdown time grows with the number of open databases,
      and a process manager that kills the node first leaves the remaining
      leases to expire.
- [ ] **Wait for a hedged write's first `PUT` before reporting an unknown
      outcome.** When the duplicate of a hedged manifest write is refused
      while the first is still in flight, the attempt counts as ambiguous; if
      retries run out then, the batch reports `outcome: "unknown"` although
      the first `PUT`'s answer would settle it. `put_manifest` could hand back
      the pending request instead of spawning it, for the committer to await
      before giving up.

## Performance

- [ ] **Delete the outputs of rejected compactions at once.** A compaction
      whose install is rejected (its inputs changed, or it outlived the orphan
      grace) leaves its output segments for the hourly orphan sweep, which
      deletes them only after `orphan_grace_ms` (2 hours). Background flushes
      already delete a superseded flush's outputs as soon as the rejection is
      certain; compactions can do the same.
- [ ] **Keep quiet databases' manifests on Tigris's small-object fast path.**
      From a Fly.io machine, a Tigris `PUT` takes about 26 ms for objects of up
      to 1,000 bytes and about 70 ms just above (p50; `GET`s step similarly
      between 2 and 4 KiB), so a commit to a database with an empty tail is
      about 2.5× faster than one with any tail. For a database that commits
      rarely, flushing the tail once it goes quiet would keep its next commit
      on the fast path, at the cost of a segment `PUT` per quiet period;
      measure whether S3 and R2 behave alike before tuning for it.
      `examples/commit_latency.rs` measures both.
- [ ] **Keep the manifest tail as a sorted vector across a batch.** Each
      commit batch clones the manifest, moves the tail into a `BTreeMap`, and
      collects it back into a vector, cloning every entry's key and value
      handles. Applying the batch's writes as an overlay over the base tail
      and merging once would make the cost proportional to the batch rather
      than the tail.

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

- [ ] **Idempotency keys for raw KV writes.** `PUT` and `DELETE` on
      `/v1/dbs/{db}/kv/{key}` take no idempotency key, and a `503` with an
      unknown outcome names its commit ID only in the error message, so a
      client whose response was lost cannot resolve the write through
      `GET /commits/{id}`. Accept an `Idempotency-Key` header, as stream
      appends do, and report the commit ID in a header.

## Testing

- [ ] **Run libFuzzer targets in CI** with a persistent corpus, including the
      model-checked `http_txn` target.
- [ ] **Run mutation testing in CI**, running `scripts/mutants.sh --in-diff` on
      pull requests and the full suite weekly.
- [ ] **Track cost and throughput benchmarks in CI** across commits.
- [ ] **Derive simulation timing checks from the fault model, and classify
      failures.** Checks that wait a fixed time for the cluster to settle
      (idle handles closed, a change reaching a watcher, a retention trim)
      fail when one simulated store stall outlasts the wait, and such false
      failures hide real ones that look alike, such as a commit wrongly
      reported with an unknown outcome. Deadlines should follow from stall,
      lease and housekeeping durations, and campaign reports should tell
      safety failures from timing ones.
- [ ] **Reach every rare probe on purpose, and require probes in CI.**
      Probes in `Probe::RARE` are exempt from the coverage test, and
      campaigns fail on unreached probes only with `SIM_REQUIRE_PROBES` set.
      `PreconditionExplainedByReplay` and `RetentionTrimLeftToOwner` went
      unreached in campaigns of about 100,000 runs, leaving their code paths
      unsimulated. Each rare probe should name a workload or knob setting
      that reaches it, checked by a targeted campaign.
- [ ] **Report the knobs a scenario overrides.** Scenarios such as
      `db_churn` overwrite swarm-drawn configuration (`db_idle_evict_ms`)
      after the draw, so the knobs line of a trace shows values the run did
      not use.
- [ ] **Improve guided exploration breadth.** At equal run counts, guided
      exploration reaches rare probes more frequently than a random campaign
      but covers fewer distinct features (SPEC §13.4), and failures cluster in
      a small number of seeds. Deduplicating failures by normalized message,
      weighting corpus entries by novelty, and incorporating probe-ordering
      and buggify-site features would preserve depth while improving breadth.
