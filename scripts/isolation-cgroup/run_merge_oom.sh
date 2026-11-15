#!/bin/bash
# Runs one event merge in a worker under a per-owner cgroup and prints the OOM count.
# Usage: run_merge_oom.sh OWNER_MIB ROWS (in the ashdev VM, with the fake_s3_server example from iso/base built)
set -u
CG=/sys/fs/cgroup/iso-cgroup-dev
BIN=$HOME/target-iso-cgroup/release/baserunna
DIR=$(mktemp -d)
$HOME/target-iso-base-fake/release/examples/fake_s3_server 127.0.0.1:19000 > $DIR/s3.env 2>&1 &
S3=$!
for i in $(seq 50); do grep -q BASERUNNA_STORE $DIR/s3.env && break; sleep 0.1; done
set -a; . $DIR/s3.env; set +a
sudo mkdir -p $CG/node/main
echo "+memory +cpu" | sudo tee $CG/cgroup.subtree_control >/dev/null
sudo chown -R $(id -u):$(id -g) $CG/node
( P=$BASHPID; echo $P | sudo tee $CG/node/main/cgroup.procs >/dev/null
  exec env BASERUNNA_CGROUP_ROOT=$CG/node BASERUNNA_OWNER_MEMORY_MB=$1 \
    BASERUNNA_LISTEN=127.0.0.1:18080 BASERUNNA_SIGNUP=open BASERUNNA_LOG=debug $BIN ) > $DIR/node.log 2>&1 &
NODE=$!
for i in $(seq 50); do curl -sf 127.0.0.1:18080/healthz >/dev/null && break; sleep 0.2; done
timeout 200 python3 $(dirname $0)/merge_oom.py http://127.0.0.1:18080 $2
echo "node alive: $(kill -0 $NODE 2>/dev/null && echo yes || echo no)"
echo "node main cgroup peak $(( $(cat $CG/node/main/memory.peak) >> 20 )) MiB"
for d in $CG/node/owner-*; do echo "$d: $(grep oom_kill $d/memory.events | head -1); peak $(( $(cat $d/memory.peak) >> 20 )) MiB"; done
grep -iE "merge|worker|\"error\"" $DIR/node.log | grep -v '^\s*$' | head -8
kill $NODE $S3; wait $NODE $S3 2>/dev/null
for d in $CG/node/owner-*/worker-* $CG/node/owner-* $CG/node/main $CG/node; do sudo rmdir $d 2>/dev/null; done
sudo rmdir $CG; rm -rf $DIR
