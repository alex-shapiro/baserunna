#!/usr/bin/env bash
# Runs every round 2 judge measurement in order. Run inside the VM ashdev when
# no other agent or build is running. Results go to target/isolation-results/judge2/.
set -uo pipefail
cd "$(dirname "$0")"

# 1. Copy the binaries the judge runs.
./freeze.sh

# 2. The six known workloads, small and attack, on each node.
./measure.sh long_strings bulk_inserts merges blob_checks txn_scans table_filter_scans

# 3. The two hidden workloads, small and attack, on each node.
./measure.sh blob_downloads stream_filter_scans

# 4. The costs: throughput, idle cost, and the event scan, twice on each node.
./measure.sh kv_throughput idle_cost event_scan
