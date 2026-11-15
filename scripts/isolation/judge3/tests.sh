#!/usr/bin/env bash
# Runs the library tests and the simulator tests of both round 3 candidates on
# the host, at their final commits. Run under the shared compile lock.
# Output goes to target/isolation-results/judge3/tests-<design>.txt.
set -uo pipefail
OUTD=/Users/alex/Projects/baserunna/target/isolation-results/judge3
W=/Users/alex/Projects/baserunna-wt
mkdir -p "$OUTD"

# The WebAssembly host tests load the guest module from the worktree. macOS
# cannot build it, so copy the module that the VM built.
mkdir -p "$W/iso3-wasm/target/wasm32-wasip1/release"
limactl shell ashdev -- cat /home/alex.linux/iso-judge3/guest.wasm >"$W/iso3-wasm/target/wasm32-wasip1/release/baserunna_guest.wasm"

for design in cgroup wasm; do
  cd "$W/iso3-$design"
  out="$OUTD/tests-$design.txt"
  git log --oneline -1 >"$out"
  echo "== library tests" >>"$out"
  cargo test -q -p baserunna --lib 2>&1 | grep -E '^test result|FAILED|panicked|^failures' >>"$out"
  echo "== simulator tests" >>"$out"
  cargo test -q -p baserunna --test sim 2>&1 | grep -E '^test result|FAILED|panicked|^failures' >>"$out"
done
cat "$OUTD"/tests-*.txt
