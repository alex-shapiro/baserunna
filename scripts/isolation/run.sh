#!/usr/bin/env bash
# Runs one tenant isolation workload against a node in a memory-limited cgroup
# (reports/tenant-isolation.md §3.1) and writes the result JSON.
#
# Usage: run.sh <workload> <small|attack|-> <out.json> [node binary]
#
# Run inside the Linux VM. The node binary defaults to $NODE_BIN, then to
# $HOME/target-iso-base/release/baserunna. The harness binaries (isolation and
# fake_s3_server) come from $EXAMPLES_DIR, which defaults to
# $HOME/target-iso-base/release/examples. Ports are free ports unless $S3_PORT
# and $NODE_PORT are set. $ISO_CGROUP replaces /sys/fs/cgroup/iso.
set -euo pipefail

if [ $# -lt 3 ]; then
  echo "usage: $0 <workload> <small|attack|-> <out.json> [node binary]" >&2
  exit 2
fi
WORKLOAD=$1
VARIANT=$2
OUT=$(realpath -m "$3")
NODE_BIN=$(realpath "${4:-${NODE_BIN:-$HOME/target-iso-base/release/baserunna}}")
EXAMPLES_DIR=${EXAMPLES_DIR:-$HOME/target-iso-base/release/examples}
free_port() { python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }
S3_PORT=${S3_PORT:-$(free_port)}
NODE_PORT=${NODE_PORT:-$(free_port)}
CG=${ISO_CGROUP:-/sys/fs/cgroup/iso}
NODE_CG=$CG/node
WORK=$(mktemp -d)
NODE_PID=
S3_PID=

teardown() {
  set +e
  if [ -n "$NODE_PID" ] && kill -0 "$NODE_PID" 2>/dev/null; then
    kill -TERM "$NODE_PID"
    for _ in $(seq 50); do kill -0 "$NODE_PID" 2>/dev/null || break; sleep 0.1; done
  fi
  if [ -d "$NODE_CG" ]; then
    echo 1 | sudo tee "$NODE_CG/cgroup.kill" >/dev/null
    for _ in $(seq 50); do [ -z "$(cat "$NODE_CG"/cgroup.procs "$NODE_CG"/*/cgroup.procs 2>/dev/null)" ] && break; sleep 0.1; done
    sudo find "$CG" -depth -type d -exec rmdir {} \; 2>/dev/null
  fi
  [ -n "$S3_PID" ] && kill "$S3_PID" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap teardown EXIT
trap "exit 143" TERM INT

if [ -d "$CG" ]; then
  echo "$CG exists; another run is active or did not tear down" >&2
  trap - EXIT
  exit 1
fi

# Other agents' nodes or compilers in the VM add noise. Record them; do not stop them.
BUSY_PROCS=$( (pgrep -a baserunna; pgrep -a cargo) || true)

# The store runs outside the node cgroup.
"$EXAMPLES_DIR/fake_s3_server" "127.0.0.1:$S3_PORT" >"$WORK/s3.env" &
S3_PID=$!
for _ in $(seq 100); do grep -q BASERUNNA_STORE "$WORK/s3.env" 2>/dev/null && break; sleep 0.05; done

# Node cgroup: 1536 MiB, no swap, delegated to this user so the node can create child cgroups.
echo "+memory +cpu +pids" | sudo tee /sys/fs/cgroup/cgroup.subtree_control >/dev/null
sudo mkdir -p "$NODE_CG"
echo "+memory +cpu +pids" | sudo tee "$CG/cgroup.subtree_control" >/dev/null
echo 1536M | sudo tee "$NODE_CG/memory.max" >/dev/null
echo 0 | sudo tee "$NODE_CG/memory.swap.max" >/dev/null
sudo chown -R "$(id -u):$(id -g)" "$NODE_CG"
mkdir "$NODE_CG/main"
echo "+memory +cpu +pids" >"$NODE_CG/cgroup.subtree_control"

# Start the node paused, move it into the leaf cgroup, then let it run.
set -a
# shellcheck disable=SC1091
. "$WORK/s3.env"
set +a
export BASERUNNA_LISTEN=127.0.0.1:$NODE_PORT BASERUNNA_SIGNUP=open BASERUNNA_CGROUP_ROOT=$NODE_CG BASERUNNA_CACHE_MB=64
GO=$WORK/go
sh -c 'while [ ! -e "$0" ]; do sleep 0.01; done; exec "$1"' "$GO" "$NODE_BIN" >"$WORK/node.log" 2>&1 &
NODE_PID=$!
echo "$NODE_PID" | sudo tee "$NODE_CG/main/cgroup.procs" >/dev/null
touch "$GO"

CLIENT_ARGS=(--node "http://127.0.0.1:$NODE_PORT" --cgroup "$NODE_CG" --workload "$WORKLOAD" --out "$OUT" --node-pid "$NODE_PID")
[ "$VARIANT" != "-" ] && CLIENT_ARGS+=(--variant "$VARIANT")
mkdir -p "$(dirname "$OUT")"
set +e
"$EXAMPLES_DIR/isolation" "${CLIENT_ARGS[@]}"
CLIENT_STATUS=$?
set -e

NODE_EXIT=null
if ! kill -0 "$NODE_PID" 2>/dev/null; then
  wait "$NODE_PID" 2>/dev/null && NODE_EXIT=0 || NODE_EXIT=$?
fi
OOM_KILLS=$(awk '$1 == "oom_kill" {print $2}' "$NODE_CG/memory.events")
PEAK=$(cat "$NODE_CG/memory.peak" 2>/dev/null || echo null)
cp "$WORK/node.log" "${OUT%.json}.node.log"
if [ -f "$OUT" ]; then
  jq --argjson exit "$NODE_EXIT" --argjson oom "${OOM_KILLS:-0}" --argjson peak "$PEAK" \
    --arg bin "$NODE_BIN" --argjson client "$CLIENT_STATUS" --arg busy "$BUSY_PROCS" \
    '.vm_busy = ($busy != "") | .vm_busy_processes = ($busy | split("\n") | map(select(. != "")))
     | .run = {node_binary: $bin, node_exit_status: $exit, cgroup_oom_kills: $oom, cgroup_memory_peak_bytes: $peak,
             client_exit_status: $client, node_died: ($exit != null or $oom > 0)}
     | if .run.node_died and .conditions then .conditions["1_node_survives"] = false | .conditions.pass = false else . end' \
    "$OUT" >"$WORK/out.json" && mv "$WORK/out.json" "$OUT"
fi
echo "result: $OUT"
jq -c '{workload, variant, conditions, run, error}' "$OUT" 2>/dev/null || true
