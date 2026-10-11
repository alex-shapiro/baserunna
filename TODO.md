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
      edge cases.
- [ ] **Test Google sign-in against real Google.** Only `FakeGoogle` has
      exercised it. Also add an OIDC `nonce` and PKCE (`code_verifier`) to
      the authorization-code flow. Today only `state` is used.
- [ ] **Rate-limit login and signup.** Add per-IP and per-account
      throttling, with backoff after failed attempts. PBKDF2 at 600k
      iterations also makes login a CPU DoS vector, so put a bounded
      semaphore around password hashing.
- [ ] **Per-database size quotas.** Count bytes per DB (segments plus
      blobs) and enforce a limit at commit and blob upload. Today only
      `max_dbs_per_user`, `blob_max` and `txn_max_write_bytes` are
      enforced.
- [ ] **Per-user and per-key request rate limits** (SPEC §16).
- [ ] **Metrics.** Add a Prometheus-style `/metrics` endpoint: object-store
      requests, bytes and errors by operation; commit latency and batch
      size; CAS conflicts; lease acquisitions and steals; cache hit rate;
      open DB handles; compaction and GC activity; forwarded requests.
      Cost is a primary goal, so object-store request counts matter most.
- [ ] **Structured logging and request IDs.** Propagate a request ID
      across forwarded hops, and log in a JSON format.
- [ ] **Account self-service.** `MetaCmd::SetPassword` exists, but no API
      or console route calls it. Add password change, account deletion,
      session listing and revocation, and an admin "re-enable user" in the
      console. Optionally add email verification and password reset, which
      need an outbound email dependency.
- [ ] **Graceful shutdown on SIGTERM.** `main.rs` releases leases only on
      Ctrl-C (SIGINT). Container orchestrators send SIGTERM.
- [ ] **Deployment docs.** Cover TLS termination, load-balancer setup, a
      Dockerfile, the required bucket permissions, and recommended bucket
      lifecycle rules (for example, aborting incomplete multipart uploads).

## Storage engine

- [ ] **Streaming compaction and orphan sweep.** Both currently hold a
      DB's merged entries in memory. Use a k-way merge over segment
      iterators so 100s-of-MB databases compact in bounded memory.
- [ ] **Online index backfill.** `add_index` is a single transaction, which
      limits it to about 100k rows. Backfill in batches with a
      "building" index state, using the same approach as online schema
      changes.
- [ ] **Segment compression.** zstd or lz4 per block. This reduces storage
      and egress cost, which are the primary costs.
- [ ] **Local disk cache tier.** Spill evicted blocks and segments to local
      disk so cold starts and failovers don't re-fetch from object storage.
- [ ] **Leveled or hybrid compaction.** Size-tiered compaction is simple
      but amplifies space. Measure write and space amplification on
      realistic workloads before changing it.
- [ ] **Faster write failover.** After a crash, writers wait up to the
      15 s lease. Options: shorter leases with renewal piggybacked on
      commits, or eager release driven by membership when a node's
      heartbeat disappears.
- [ ] **Long blob downloads.** A download must finish within `gc_grace`
      (30 min) of the blob being dereferenced. Pin blobs that are being
      read, or extend the grace period while a reader is active.
- [ ] **Multi-process `fs:` store.** Its compare-and-swap only works within
      one process. Take an exclusive lock on the data directory at startup
      so a second process can't silently corrupt it.

## Features

- [ ] **TTLs** on keys and rows, expired lazily on read and by compaction.
- [ ] **Change feeds / watch.** Stream commits after a version. The
      `version_times` and `changed_since` machinery is a starting point.
- [ ] **Point-in-time restore and backups.** Keep old manifests and delay
      GC of segments they reference. Add DB snapshot/clone (copy-on-write
      at the manifest level is cheap) and export/import.
- [ ] **Database rename and transfer** between users.
- [ ] **Organizations / teams.** Multiple users per DB with roles (SPEC §16).
- [ ] **Richer queries.** OR/IN filters, ordering by non-index columns with limits,
      projections, and aggregates (count, sum) evaluated server-side.
- [ ] **Batch row operations.** Allow multi-row update and delete by query within
      one transaction, bounded by the transaction size limit.
- [ ] **Client SDKs.** Generate TypeScript and Python clients from
      `openapi.json`. Publish the Rust `client` module as its own crate.
- [ ] **Console improvements.** Blob upload and download, table schema
      editing, a DB metrics panel, and pagination on every listing.

## Testing

- [ ] **Run simulation campaigns continuously in CI.**
      For example, run nightly with fresh seeds, keep failing seeds as regression tests,
      and run the `sim_tool determinism` check on every PR.
- [ ] **Add simulator fault types**
  - object-store list results that are eventually consistent (some S3-compatible stores)
  - slow-but-successful object-store requests that are longer than the lease
  - asymmetric network partitions
  - forwarded requests that are duplicated or reordered
  - more extreme clock skew, which should fail safe
- [ ] **Add simulator workloads**:
  - concurrent schema changes racing row writes
  - interactive transactions abandoned mid-flight across failovers
  - blob range reads racing GC
  - many-DB churn to stress handle eviction and the cache.
- [ ] **Run libFuzzer targets in CI** for a time budget, with a corpus.
      Add an HTTP-level structure-aware fuzzer for transaction-op JSON.
- [ ] **Benchmarks and a cost model.**
  - Measure object-store requests per commit and per read
  - Measure cold-open latency
  - Measure throughput per node with thousands of active DBs
  - Turn benchmarks into a cost estimate per DB per month and guard against regressions.
- [ ] **Mutation testing** (for example `cargo-mutants`) on the engine and checkers, to confirm the simulator catches injected bugs.
