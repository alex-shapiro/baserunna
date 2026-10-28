# Baserunna

> [!NOTE]
> Status: Alpha Software, Handle With Care

Baserunna is a web service that hosts databases on object storage. Its goal is to explore the bounds of cheap, reliable, scalable data storage with minimal operational burden. Features:

- Stateless compute nodes that serve thousands of active databases each
- Idle databases are evicted from compute nodes, minimizing compute overhead
- Reads and writes use snapshot isolation and serialized transactions
- KV, table, event stream, and binary blob primitives
- Event tables for analytics: columnar storage, funnels, retention, and unique counts over time ranges
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
cargo test
```

Run fuzz tests in the `fuzz/` directory:

```sh
cargo fuzz
```

Run benchmarks

```sh
cargo bench
```
