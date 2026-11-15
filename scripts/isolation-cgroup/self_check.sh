#!/usr/bin/env bash
# Runs the named isolation workloads against this worktree's node and prints each summary.
# Usage, inside the ashdev VM: self_check.sh VARIANT WORKLOAD...
set -u
VARIANT=$1
shift
NODE=$HOME/target-iso-cgroup/release/baserunna
OUT=$HOME/iso2-out
mkdir -p "$OUT"
for W in "$@"; do
    ISO_CGROUP=/sys/fs/cgroup/iso2-cgroup timeout 280 \
        /Users/alex/Projects/baserunna/scripts/isolation/run.sh "$W" "$VARIANT" "$OUT/$W-$VARIANT.json" "$NODE" 2>&1 | tail -1
done
