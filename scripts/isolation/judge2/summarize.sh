#!/usr/bin/env bash
# Writes reports/tenant-isolation-round2-results.json: for each round 2 judge
# result file, the §3.3 conditions, the cost readings, the setup error if any,
# and the node's run status. The raw files stay in target/isolation-results/judge2/.
set -euo pipefail
cd /Users/alex/Projects/baserunna/target/isolation-results/judge2
OUT=/Users/alex/Projects/baserunna/reports/tenant-isolation-round2-results.json

for f in *.json; do
  jq --arg file "$f" '{
    file: $file,
    workload, variant, vm_busy, store_latency_ms,
    conditions,
    error,
    event_scan, kv_throughput, idle_cost,
    run,
    attacker_memory: (.phase_b_attacker_alone.memory? // null)
  }' "$f"
done | jq -s '{results: .}' >"$OUT"

du -h "$OUT"
