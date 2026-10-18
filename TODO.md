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
- [ ] **Metrics.** `GET /v1/admin/metrics` reports latency histograms of
      object-store calls, commit batches and API routes, scheduler lag and
      CPU time as JSON. Add a Prometheus-style `/metrics` endpoint with
      those and: object-store bytes; CAS conflicts; lease acquisitions and
      steals; cache hit rate; open DB handles; compaction and GC activity;
      forwarded requests; rate limit refusals. Cost is a primary goal, so
      object-store request counts matter most. `engine::Activity` already
      counts range tombstones, segment drops and rewrites, levels objects,
      quota refusals and expired entries and segments.
- [ ] **Publish release binaries and a container image.** Apps that test
      against a local node (mathtutor's test suite does) need one in CI, and
      the repo is private.
- [ ] **Check caches on bulk reads (`ReadMode::Bulk`).** Compaction and GC
      sweeps read segments in `ReadMode::Bulk` to avoid polluting the
      in-memory block cache on misses, but `MergeIter::peek` currently skips
      looking up already-cached blocks in memory and on local disk and goes
      straight to object storage. Checking the caches first (without
      populating them on misses) avoids re-fetching segments that the node
      already has locally, such as L0 segments it just flushed.

## Performance

Load testing mathtutor on fly.io (DEPLOYING.md) left these open.

- [ ] **Resolve ambiguous commits without waiting.** A client must wait
      about 15 s, past the commit deadline, before `GET /commits/{id}` can
      answer (SPEC §6.2), so a proxy 502 during a commit stalls the user for
      15 s. If the status call's fencing manifest write also recorded the
      commit id as refused, a later attempt would be rejected and the call
      could answer at once.
- [ ] **Cut CPU per request.** On one dedicated core, throughput levels off
      around 340 commits/s with the core about 80% busy. Profiles show the
      time going to memory allocation, building `serde_json` maps for
      responses, key comparisons in segment lookups, and zstd compression of
      the manifest.
- [ ] **Remove the remaining block-cache lock contention.** Cache shards
      still take an exclusive lock on every hit to update LRU order. A
      CLOCK-style cache would let hits take a shared lock.
- [ ] **Tune the inline WAL tail limit.** Every commit uploads the manifest
      with the tail inline: commits take 36 ms at the median with an empty
      tail and 84 ms with a full 256 KiB one on Tigris. A lower limit trades
      commit latency for more frequent flushes.

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

- [ ] **Run simulation campaigns continuously in CI.**
      For example, run nightly with fresh seeds, keep failing seeds as regression tests,
      and run the `sim_tool determinism` check on every PR (include a `*_hostile` scenario).
- [ ] **Run libFuzzer targets in CI** for a time budget, with a corpus. Include the
      structure-aware `http_txn` target, which checks results against a model.
- [ ] **Run mutation testing in CI**, e.g. `scripts/mutants.sh --in-diff` on every PR and the
      full configuration weekly.
- [ ] **Run the cost guard and throughput benchmark in CI** and track the numbers over time.
- [ ] **Bound status checks by commit retention in every workload.** `kv_model`, `bank`,
      `abandoned` and `schema_race` take `not_committed` as definitive however long ago the
      first attempt began; like the streams workload, they should treat answers past
      `retention_ms` as unknown and resolve them from the data.
