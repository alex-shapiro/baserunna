#!/usr/bin/env bash
# Runs workloads and costs against each frozen node. Each cell (node, workload,
# variant) runs REPS times (default 2); a cell passes only if every run passes.
# Run inside the VM ashdev when no other node or build runs.
# Usage: measure.sh <workload>...
# The modes kv_throughput, idle_cost and event_scan take no variant.
set -uo pipefail
J=$HOME/iso-judge3
OUTD=/Users/alex/Projects/baserunna/target/isolation-results/judge3
mkdir -p "$OUTD"
export EXAMPLES_DIR=$J/examples ISO_CGROUP=/sys/fs/cgroup/iso
NODES=${NODES:-"base cgroup wasm"}
REPS=${REPS:-2}

# Arguments: node, workload, variant, run number, extra client flags.
run_one() {
  local node=$1 workload=$2 variant=$3 rep=$4
  shift 4
  if [ "$node" = wasm ]; then export BASERUNNA_GUEST_WASM=$J/guest.wasm; else unset BASERUNNA_GUEST_WASM; fi
  local out="$OUTD/$node-$workload-$variant-r$rep.json"
  echo "== $(date +%T) $node $workload $variant r$rep busy=[$(pgrep -a 'cargo|rustc' | head -1)]"
  "$J/run.sh" "$workload" "$variant" "$out" "$J/$node" "$@" >"${out%.json}.stdout" 2>&1
  jq -c '.conditions // {died: .run.node_died, error}' "$out" 2>/dev/null || echo "no result file"
}

for workload in "$@"; do
  for rep in $(seq "$REPS"); do
    case $workload in
      kv_throughput) for n in $NODES; do run_one "$n" "$workload" - "$rep" --clients 256; done ;;
      idle_cost|event_scan) for n in $NODES; do run_one "$n" "$workload" - "$rep"; done ;;
      *) for v in small attack; do for n in $NODES; do run_one "$n" "$workload" "$v" "$rep"; done; done ;;
    esac
  done
done
