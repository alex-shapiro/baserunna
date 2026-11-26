#!/usr/bin/env bash
# Runs isolation workloads against one node binary and writes one result file
# per run into a results directory. Run inside the Linux VM after build.sh, with
# no other node or build in the VM, and with the Mac held awake.
#
# Usage: measure.sh <results dir> [workload...]
#
# With no workloads, it runs every isolation workload and then the cost
# measurements kv_throughput, idle_cost, and event_scan. Each isolation
# workload runs its small and attack variants. Each run repeats $REPS times
# (default 2). summarize.py reads the results directory.
set -uo pipefail

if [ $# -lt 1 ]; then
  echo "usage: $0 <results dir> [workload...]" >&2
  exit 2
fi
OUTD=$(realpath -m "$1")
shift
WORKLOADS=("$@")
if [ ${#WORKLOADS[@]} -eq 0 ]; then
  WORKLOADS=(long_strings bulk_inserts merges blob_checks txn_scans table_filter_scans
    blob_downloads stream_filter_scans kv_throughput idle_cost event_scan)
fi
REPS=${REPS:-2}
RUN_SH=$(realpath "$(dirname "$0")/run.sh")
TARGET_DIR=${ISOLATION_TARGET_DIR:-$HOME/target-isolation}

# Copy the binaries, so that a build during the runs cannot change them.
# Remove the old copies first: on a shared mount, a copy over a running
# binary can kill it.
BIN=$OUTD/bin
mkdir -p "$BIN"
rm -f "$BIN/baserunna" "$BIN/isolation" "$BIN/fake_s3_server"
cp "$TARGET_DIR/release/baserunna" "$BIN/baserunna"
cp "$TARGET_DIR/release/examples/isolation" "$TARGET_DIR/release/examples/fake_s3_server" "$BIN/"
export EXAMPLES_DIR=$BIN

# Arguments: workload, variant, run number, extra client flags.
run_one() {
  local workload=$1 variant=$2 rep=$3
  shift 3
  local out="$OUTD/$workload-$variant-r$rep.json"
  echo "== $(date +%T) $workload $variant r$rep"
  "$RUN_SH" "$workload" "$variant" "$out" "$BIN/baserunna" "$@" >"${out%.json}.stdout" 2>&1
  jq -c '.conditions // {died: .run.node_died, error}' "$out" 2>/dev/null || echo "no result file"
}

for workload in "${WORKLOADS[@]}"; do
  for rep in $(seq "$REPS"); do
    case $workload in
      kv_throughput) run_one "$workload" - "$rep" --clients 256 ;;
      idle_cost | event_scan) run_one "$workload" - "$rep" ;;
      *) for variant in small attack; do run_one "$workload" "$variant" "$rep"; done ;;
    esac
  done
done
