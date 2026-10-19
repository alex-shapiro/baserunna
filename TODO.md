# TODO

Work that v1 does not currently cover but should, roughly in priority order
within each section. SPEC §15 (known limitations) and §16 (future work) describe
the design context for several of these.

## Before production use

- [ ] **Run against real object stores.** `CloudStore` (S3, GCS, Azure, R2,
      MinIO) only compiles today. Add an opt-in integration test suite,
      gated on credentials, that runs the `ObjectStore` conformance checks:
      conditional PUT (`If-Match` / `If-None-Match`), list-after-write, and
      ranged GET. Run the `multi_node` example against MinIO in CI. Confirm
      that each provider's conditional-write semantics match what
      `SimStore` models, especially S3's `If-Match` support and R2/MinIO
      edge cases, and that `last_modified` comes from one coherent clock
      (garbage collection runs on it, SPEC §5.8).
- [ ] **Test Google sign-in against real Google.** Only `FakeGoogle` has
      exercised it. Also add an OIDC `nonce` and PKCE (`code_verifier`) to
      the authorization-code flow. Today only `state` is used.
- [ ] **Prometheus metrics.** Expose a `/metrics` endpoint in Prometheus
      format that includes the histograms from `/v1/admin/metrics` along
      with object-store byte counts, CAS conflicts, lease acquisitions and
      steals, cache hit rates, open DB handles, compaction and GC activity,
      forwarded requests, and rate-limit rejections.
- [ ] **Publish release binaries and a container image.** Publish prebuilt
      binaries and a container image so external test suites can run a local
      node in CI.
- [ ] **Check caches on bulk reads (`ReadMode::Bulk`).** Compaction and GC
      sweeps read segments in `ReadMode::Bulk` to avoid polluting the
      in-memory block cache on misses, but `MergeIter::peek` currently skips
      looking up already-cached blocks in memory and on local disk and goes
      straight to object storage. Checking the caches first (without
      populating them on misses) avoids re-fetching segments that the node
      already has locally, such as L0 segments it just flushed.

## Performance

- [ ] **Resolve ambiguous commits without waiting.** A client must currently
      wait for the 15 s commit deadline to pass before calling
      `GET /commits/{id}` (SPEC §6.2). Record the commit ID as aborted
      during the status check's fencing manifest write so that late commit
      attempts are rejected and the status check can return immediately.
- [ ] **Reduce CPU usage per request.** Profiles at 340 commits/s on a
      single core show hotspots in memory allocation, `serde_json` response
      serialization, segment key comparisons, and manifest zstd compression.
- [ ] **Replace the sharded LRU block cache with a CLOCK cache.** On a cache
      hit, a thread currently acquires an exclusive shard lock to update the
      LRU list. With a CLOCK eviction policy, cache hits can be served under a
      shared lock.
- [ ] **Tune the inline WAL tail limit.** Median commit latency on Tigris
      increases from 36 ms with an empty tail to 84 ms with a full 256 KiB
      tail. Lowering the tail limit reduces commit latency at the cost of
      more frequent L0 flushes.

## Features

- [ ] **Point-in-time restore and backups.** Keep old manifests and delay
      GC of segments they reference. Add DB snapshot/clone (copy-on-write
      at the manifest level is cheap) and export/import.
- [ ] **Database rename and transfer** between users and organizations.
- [ ] **Organization follow-ups.** Per-database grants within an
      organization, service accounts (API keys owned by an organization),
      and invitations by email for people without an account yet.
- [ ] **Deliver mail through SMTP** directly, not only an HTTP webhook, and
      queue it durably (today a node that stops drops its pending mail).
- [ ] **Richer queries.** OR/IN filters, ordering by non-index columns with limits,
      projections, and aggregates (count, sum) evaluated server-side.
- [ ] **Longer change feed retention.** Feed cursors stay valid for a transaction
      lifetime (5 minutes), the time tombstones are kept. A per-database
      retention would let consumers resume after longer outages, at the cost
      of tombstones kept longer. Streaming responses (SSE) would save the
      long poll's request per batch.
- [ ] **Stream follow-ups.** Consumer groups that share a stream's records
      among workers, with acknowledgements and redelivery; read-ahead
      across chunks for long cold reads; per-block summaries of record
      fields so filters skip blocks; merging small sealed chunks; waking
      idle databases whose streams are past their retention.
- [ ] **Client SDKs.** Generate TypeScript and Python clients from
      `openapi.json`. Publish the Rust `client` module as its own crate.

## Testing

- [ ] **Run libFuzzer targets in CI** for a time budget, with a corpus. Include the
      structure-aware `http_txn` target, which checks results against a model.
- [ ] **Run mutation testing in CI**, e.g. `scripts/mutants.sh --in-diff` on every PR and the
      full configuration weekly.
- [ ] **Run the cost guard and throughput benchmark in CI** and track the numbers over time.
- [ ] **Make guided exploration beat plain campaigns.** At equal runs it reaches about as many
      probe features as a swarm campaign (SPEC §13.4); its value so far is in reaching
      combinations of rare probes. Better features (orderings of rare probes, buggify sites
      fired) or energy schedules could change that.
