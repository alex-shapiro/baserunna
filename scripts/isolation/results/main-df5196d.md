# Isolation baseline: `main` at `df5196d`

`main` passes 1 of the 8 workloads. Five attacks make the node run out of
memory and stop. Two attacks raise the victim's p99 latency by a factor of 14
to 51.

## Setup

- Date: 2026-11-26.
- VM: 4 vCPUs, 8 GiB of memory, Linux 6.8.0-149-generic.
- The node ran in a cgroup with a 1,536 MiB memory limit and no swap.
- The fake object store added 20 ms to each request.
- The harness ran each variant of each workload twice. The Mac stayed awake
  for the whole run.
- With no attacker, the median victim p99 latency was 55 ms.

## Workloads

| Workload              | Attack variant                               | Small variant            |
| --------------------- | -------------------------------------------- | ------------------------ |
| `blob_checks`         | pass, but node memory grew 1,050 MiB         | pass                     |
| `blob_downloads`      | the node ran out of memory and stopped       | 1 run failed criterion 3 |
| `bulk_inserts`        | the node ran out of memory and stopped       | pass                     |
| `long_strings`        | the node ran out of memory and stopped       | pass                     |
| `merges`              | the node ran out of memory and stopped       | pass                     |
| `stream_filter_scans` | victim p99 rose from 56 ms to 2,629–2,846 ms | pass                     |
| `table_filter_scans`  | victim p99 rose from 52 ms to 716–818 ms     | pass                     |
| `txn_scans`           | the node ran out of memory and stopped       | pass                     |

- `blob_checks` passes only because no criterion limits tenant memory. One
  tenant used 1,050 MiB of the node's 1,536 MiB.
- The failed small run of `blob_downloads` exceeded its 106 ms p99 limit by
  3 ms. While an attack runs, the victim sends only about 470 requests, so the
  p99 is the fifth-slowest request. One slow request can change the result.

## Costs

- `kv_throughput`: 72,300 operations per second with 256 clients, with 35 µs
  of node CPU time for each operation.
- `idle_cost`: 48 to 51 MiB of node memory with 1,000 idle owners.
- `event_scan`: the node refused the "top users" query with HTTP 413 in all 40
  rounds. In seven earlier runs of the same node code, the query completed
  in 0, 3, 20, 20, 20, 20, and 20 of 20 rounds. The cause of this difference
  is unknown.
