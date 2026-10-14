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
- [ ] **Metrics.** Add a Prometheus-style `/metrics` endpoint: object-store
      requests, bytes and errors by operation; commit latency and batch
      size; CAS conflicts; lease acquisitions and steals; cache hit rate;
      open DB handles; compaction and GC activity; forwarded requests; rate
      limit refusals. Cost is a primary goal, so object-store request counts
      matter most. `engine::Activity` already counts range tombstones,
      segment drops and rewrites, levels objects, quota refusals and
      expired entries and segments.
- [ ] **Structured logging and request IDs.** Propagate a request ID
      across forwarded hops, and log in a JSON format.
- [ ] **Account self-service.** `MetaCmd::SetPassword` exists, but no API
      or console route calls it. Add password change, account deletion,
      session listing and revocation, and an admin "re-enable user" in the
      console. Optionally add email verification and password reset, which
      need an outbound email dependency.

## Features

- [ ] **Point-in-time restore and backups.** Keep old manifests and delay
      GC of segments they reference. Add DB snapshot/clone (copy-on-write
      at the manifest level is cheap) and export/import.
- [ ] **Database rename and transfer** between users.
- [ ] **Organizations / teams.** Multiple users per DB with roles (SPEC §16).
- [ ] **Richer queries.** OR/IN filters, ordering by non-index columns with limits,
      projections, and aggregates (count, sum) evaluated server-side.
- [ ] **Longer change feed retention.** Feed cursors stay valid for a transaction
      lifetime (5 minutes), the time tombstones are kept. A per-database
      retention would let consumers resume after longer outages, at the cost
      of tombstones kept longer. Streaming responses (SSE) would save the
      long poll's request per batch.
- [ ] **Client SDKs.** Generate TypeScript and Python clients from
      `openapi.json`. Publish the Rust `client` module as its own crate.
- [ ] **Console improvements.** Blob upload and download, table schema
      editing, a DB metrics panel, and pagination on every listing.

## Testing

- [ ] **Run simulation campaigns continuously in CI.**
      For example, run nightly with fresh seeds, keep failing seeds as regression tests,
      and run the `sim_tool determinism` check on every PR (include a `*_hostile` scenario).
- [ ] **Run libFuzzer targets in CI** for a time budget, with a corpus. Include the
      structure-aware `http_txn` target, which checks results against a model.
- [ ] **Run mutation testing in CI**, e.g. `scripts/mutants.sh --in-diff` on every PR and the
      full configuration weekly.
- [ ] **Run the cost guard and throughput benchmark in CI** and track the numbers over time.
