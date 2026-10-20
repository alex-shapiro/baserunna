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
- [ ] **Check local caches during bulk reads (`ReadMode::Bulk`).** Compaction
      and GC sweeps use `ReadMode::Bulk` to avoid polluting the in-memory
      block cache on misses, but `MergeIter::peek` currently bypasses the
      in-memory and local-disk caches and fetches directly from object storage.
      Checking the caches first without populating them on misses avoids
      re-fetching segments that are already cached locally.

## Performance

- [ ] **Resolve ambiguous commits without waiting for the commit deadline.**
      Clients must currently wait for the 15 s commit deadline before calling
      `GET /commits/{id}` (SPEC §6.2). Record the commit ID as aborted during
      the status check's fencing manifest write so that late commit attempts
      are rejected and the status check can return immediately.
- [ ] **Reduce CPU overhead per request.** Profiles at 340 commits/s on a
      single core show hotspots in memory allocation, `serde_json` response
      serialization, segment key comparisons, and manifest zstd compression.
- [ ] **Replace the sharded LRU block cache with a CLOCK cache.** Cache hits
      currently acquire an exclusive shard lock to update the LRU list. A
      CLOCK eviction policy allows cache hits to use a shared lock.
- [ ] **Tune the inline WAL tail limit.** Median commit latency on Tigris
      increases from 36 ms with an empty tail to 84 ms with a full 256 KiB
      tail. Lowering the tail limit reduces commit latency at the cost of
      more frequent L0 flushes.

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

## Testing

- [ ] **Run libFuzzer targets in CI** with a persistent corpus, including the
      model-checked `http_txn` target.
- [ ] **Run mutation testing in CI**, running `scripts/mutants.sh --in-diff` on
      pull requests and the full suite weekly.
- [ ] **Track cost and throughput benchmarks in CI** across commits.
- [ ] **Improve guided exploration breadth.** At equal run counts, guided
      exploration reaches rare probes more frequently than a random campaign
      but covers fewer distinct features (SPEC §13.4), and failures cluster in
      a small number of seeds. Deduplicating failures by normalized message,
      weighting corpus entries by novelty, and incorporating probe-ordering
      and buggify-site features would preserve depth while improving breadth.
