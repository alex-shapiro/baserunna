#!/bin/bash
# Usage: one.sh WORKLOAD VARIANT
cd $HOME/iso-base-src
OUT=$HOME/iso-cgroup-runs/$1_$2.json
ISO_CGROUP=/sys/fs/cgroup/iso-cgroup-dev EXAMPLES_DIR=$HOME/target-iso-base-fake/release/examples timeout 285 bash scripts/isolation/run.sh $1 $2 $OUT $HOME/target-iso-cgroup/release/baserunna >/dev/null 2>&1
echo "exit $?"
jq -c "{workload, variant, total_s, conditions, run: (.run | {node_died, cgroup_oom_kills, subtree_oom_kills, cgroup_memory_peak_bytes})}" $OUT
ls /sys/fs/cgroup | grep -c iso-cgroup-dev
