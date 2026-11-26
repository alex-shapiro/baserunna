# Isolation baseline: `main` at `df5196d`

`main` passes 1 of 8 workloads. Exhausted memory causes the kernel to stop the
node in 5 attacks. High CPU load increases victim p99 latency by 14x to 51x in
2 attacks.

## Setup

- Date: 2026-11-26.
- VM: 4 vCPUs, 8 GiB of memory, Linux 6.8.0-149-generic.
- The node ran in a cgroup with a 1,536 MiB memory limit and swap disabled.
- The fake object store added 20 ms to each request.
- The harness ran each variant of each workload twice. The host Mac stayed
  awake during the run.
- Without an attacker, the median victim p99 latency was 55 ms.

## Workloads

| Workload              | Attack variant                         | Small variant            |
| --------------------- | -------------------------------------- | ------------------------ |
| `blob_checks`         | pass; node used 1,050 MiB extra memory | pass                     |
| `blob_downloads`      | out of memory; kernel stopped node     | 1 run failed criterion 3 |
| `bulk_inserts`        | out of memory; kernel stopped node     | pass                     |
| `long_strings`        | out of memory; kernel stopped node     | pass                     |
| `merges`              | out of memory; kernel stopped node     | pass                     |
| `stream_filter_scans` | victim p99: 56 ms to 2,629–2,846 ms    | pass                     |
| `table_filter_scans`  | victim p99: 52 ms to 716–818 ms        | pass                     |
| `txn_scans`           | out of memory; kernel stopped node     | pass                     |

- `blob_checks` passes only because the pass criteria do not limit tenant memory
  yet. A single tenant used 1,050 MiB of the node's 1,536 MiB limit.
- The failed small run of `blob_downloads` exceeded its 106 ms p99 limit by
  3 ms. During this run, the victim sent about 470 requests. Because the sample
  is small, the p99 is the fifth-slowest request. A single slow request can
  change the result.

## Costs

- `kv_throughput`: 72,300 operations per second across 256 clients. Each
  operation used 35 µs of node CPU time.
- `idle_cost`: 48 to 51 MiB of node memory with 1,000 idle owners.
- `event_scan`: the node rejected the "top users" query with HTTP 413 in all 40
  rounds. In 7 earlier runs of the same binary, the node completed 20 rounds in
  5 runs, 3 rounds in 1 run, and 0 rounds in 1 run. We do not know the cause
  yet.
