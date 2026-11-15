#!/usr/bin/env bash
# Runs workloads against each frozen node, one node after another for each
# workload and variant. Run inside the VM ashdev.
# Usage: measure.sh <workload>...   Modes kv_throughput, idle_cost and
# event_scan take no variant and run REPS times (default 2).
set -uo pipefail
J=$HOME/iso-judge2
OUTD=/Users/alex/Projects/baserunna/target/isolation-results/judge2
mkdir -p "$OUTD"
export EXAMPLES_DIR=$J/examples ISO_CGROUP=/sys/fs/cgroup/iso
NODES=${NODES:-"base cgroup wasm"}

# Arguments: node, workload, variant, file tag, extra client flags.
run_one() {
  local node=$1 workload=$2 variant=$3 tag=$4
  shift 4
  if [ "$node" = wasm ]; then export BASERUNNA_GUEST_WASM=$J/guest.wasm; else unset BASERUNNA_GUEST_WASM; fi
  local out="$OUTD/$node-$workload-$variant$tag.json"
  echo "== $(date +%T) $node $workload $variant$tag busy=[$(pgrep -a 'cargo|rustc' | head -1)]"
  "$J/run.sh" "$workload" "$variant" "$out" "$J/$node" "$@" >"${out%.json}.stdout" 2>&1
  jq -c '.conditions // {died: .run.node_died}' "$out" 2>/dev/null || echo "no result file"
}

for workload in "$@"; do
  case $workload in
    kv_throughput) for r in $(seq "${REPS:-2}"); do for n in $NODES; do run_one "$n" "$workload" - "-r$r" --clients 256; done; done ;;
    idle_cost|event_scan) for r in $(seq "${REPS:-2}"); do for n in $NODES; do run_one "$n" "$workload" - "-r$r"; done; done ;;
    *) for v in small attack; do for n in $NODES; do run_one "$n" "$workload" "$v" ""; done; done ;;
  esac
done
