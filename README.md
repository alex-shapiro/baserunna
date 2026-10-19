# Baserunna

> [!NOTE]
> Status: Alpha Software, Handle With Care

Baserunna is a web service that hosts databases on object storage. Its goal is to explore the bounds of cheap, reliable, scalable data storage with minimal operational burden. Features:

- Stateless compute nodes that serve thousands of active databases each
- Idle databases are evicted from compute nodes, minimizing compute overhead
- Reads and writes use snapshot isolation and serialized transactions
- KV, table, event stream, and binary blob primitives
- Self service administration

The key non-feature is _low latency_. Uncached reads and writes take 10s of ms at p50, with p99 ≥100ms. While Baserunna minimizes latency where possible, object storage is fundmentally slower than local storage. For many applications, this latency is an acceptable tradeoff for scalability and cost benefits.

Full design details are in [`SPEC.md`](SPEC.md).

## Quick start

Run a single node locally at.

```sh
cargo run --release
open http://127.0.0.1:8080/console
```

For more information on deployment, see [DEPLOYING.md](DEPLOYING.md).

## Testing

Baserunna is built to run fully inside a deterministic simulator with fault injection. To run most tests, call:

```sh
<<<<<<< HEAD
cargo test
=======
cargo test                                   # unit, property, fuzz, lint, OpenAPI, cost and simulation tests
SIM_SEEDS=500 cargo test --release --test sim                      # longer simulation campaigns
cargo run --release --example sim_tool -- campaign all 3000        # very long campaigns
cargo run --release --example sim_tool -- explore all 1            # guided exploration
cargo run --release --example sim_tool -- trace bank 101863        # print a run's full trace
SIM_SEED=4242 cargo test --test sim sim_bank -- --nocapture        # reproduce one seed
cd fuzz && cargo +nightly fuzz run http_txn -- -rss_limit_mb=1500  # coverage-guided fuzzing (optional)
cargo run --release --example cost_model                           # store requests per operation, $/DB/month
cargo run --release --example amplification                        # write/space/read amplification of compaction and streams
cargo bench --bench throughput                                     # one node, thousands of active databases
scripts/mutants.sh                                                 # mutation testing (several hours)
>>>>>>> 627278a (Rewrite SPEC §13 for the new simulator, resolve ttl writes past commit retention)
```

Run fuzz tests in the `fuzz/` directory:

```sh
cargo fuzz
```

Run benchmarks

```sh
cargo bench
```
