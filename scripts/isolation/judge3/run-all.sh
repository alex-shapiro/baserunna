#!/usr/bin/env bash
# Runs every round 3 judge measurement in order, two runs per cell. Run inside
# the VM ashdev when no other node or build runs, with the Mac held awake.
# Results go to target/isolation-results/judge3/.
set -uo pipefail
cd "$(dirname "$0")"
./freeze.sh
./measure.sh long_strings bulk_inserts merges blob_checks txn_scans table_filter_scans blob_downloads stream_filter_scans
# The hidden workloads: their names are in $HIDDEN, set once they exist.
[ -n "${HIDDEN:-}" ] && ./measure.sh ${HIDDEN}
./measure.sh kv_throughput idle_cost event_scan
