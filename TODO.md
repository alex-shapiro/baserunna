# TODO

Planned work, ordered by priority within each section. Blockers are
correctness or security gaps that must be closed before v1; everything else
can ship after it. See [`SPEC.md`](SPEC.md) §15 (known limitations) and §16
(future work) for design context.

## Blockers

None are known.

## Reliability and operations

- [ ] **Stop store-time callers from waiting on clock probes.**
      `StoreClock::sync` acquires its lock before checking whether the
      measurement is stale, so every `store_time_ms` caller (stream appends,
      TTL writes, blob uploads, and commits that bind staged blobs) waits for
      the probe that runs every 10 minutes. A stalled probe blocks those
      commits until its store requests time out. Check staleness before
      locking, and time probes out at `2 × clock_sync_max_error_ms`, beyond
      which they are discarded as imprecise.
- [ ] **Bound store calls by the deadlines of the requests that make them.**
      `CloudStore` times requests out after 30 s and leaves retries to
      `Retry`, which makes up to 5 attempts, so a read or a manifest load can
      take minutes while its client gave up after seconds. Pass the request's
      deadline down to store calls and stop retrying once it has passed.
- [ ] **Treat a stale-target rejection as ambiguous when forwards can be
      duplicated.** A forwarder that receives `x-baserunna-stale-target`
      handles the request locally on the assumption that it never ran, but a
      duplicated copy of the forward can have run on the target's previous
      process. The simulator's duplicating network shows the request running
      twice (`sim_tool trace tables 7344`: a unique-row insert answers
      `409 already_exists` for its own write). The production HTTP transport
      never duplicates a request, so only deployments behind retrying proxies
      are exposed. Handle a stale-target rejection locally only if the forward
      was sent once, and answer `outcome: "unknown"` otherwise.
- [ ] **Share the memory budget fairly across users.** One user's interactive
      transactions can hold a node's whole memory budget for up to their 5
      minute lifetime, so that other users' transactions on that node are
      refused meanwhile (SPEC §5.12). Reserve a share of the budget per active
      user, or refuse the user holding the most memory first.
- [ ] **End a deleted database's interactive transactions.** Transactions on a
      deleted database stay open until they idle out (30 s), counting against
      their user's 100-transaction cap and holding memory.
- [ ] **Test Google OAuth against Google's production endpoints.** Only
      `FakeGoogle` is tested. Refetch the JWKS when an ID token names an
      unknown `kid` instead of waiting for the 1-hour cache to expire. Add
      PKCE (`code_verifier`) and an OIDC `nonce` as defense in depth; `state`
      is already bound to a signed cookie.
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

## Performance

- [ ] **Immediately delete output segments of rejected compactions.** Rejected
      compactions currently leave their output segments for the orphan sweep to
      delete after `orphan_grace_ms` (2 hours).
- [ ] **Flush idle database tails to reduce next-commit manifest size.**
      Flushing a non-empty tail after a database becomes idle reduces the size
      of the next manifest `PUT` at the cost of an extra segment `PUT`.
- [ ] **Shrink in-memory tail entries.** An `Entry` takes 144 bytes before its
      key and value because `Value` stores a `BlobRef` (two strings and a
      digest) inline, so a tail of 100-byte values takes about twice its
      encoded size in memory (`tests/memory_bound.rs`). Boxing `BlobRef`
      shrinks `Entry` to 96 bytes, so the memory budget holds 20–35% more
      databases whose tails hold small values.
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
- [ ] **Support object stores beyond Tigris.** Run the object-store
      conformance suite against S3, GCS, Azure, R2, and MinIO before listing
      each as supported. GCS conditional writes use object generations rather
      than etags, so `CloudStore`'s etag handling needs GCS-specific coverage.

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
