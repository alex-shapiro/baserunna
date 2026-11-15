#!/usr/bin/env bash
# Runs the bulk_inserts small variant three times on today's node, to prove that
# today's code completes it and its probe with growth under the tenant bound.
# Run inside the VM ashdev after build-base.sh.
set -uo pipefail
OUTD=/Users/alex/Projects/baserunna/target/isolation-results/judge3-proof
mkdir -p "$OUTD"
for r in 1 2 3; do
  out="$OUTD/base-bulk_inserts-small-r$r.json"
  /Users/alex/Projects/baserunna/scripts/isolation/run.sh bulk_inserts small "$out" >"${out%.json}.stdout" 2>&1
  jq -c '{idle_mib: (.idle_node_bytes / 1048576), probe: .prepare.alone, b_start_mib: (.phase_b_attacker_alone.memory.start_bytes / 1048576), conditions, stat_end: .run.memory_stat_end_bytes, error, died: .run.node_died}' "$out"
done
