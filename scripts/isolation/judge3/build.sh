#!/usr/bin/env bash
# Builds every binary the round 3 judge runs, from each branch's final commit.
# Run inside the VM ashdev, under the host's shared compile lock.
set -euo pipefail
W=/Users/alex/Projects/baserunna-wt

# Today's code plus the harness and the hidden workloads (branch iso3/hidden).
cd "$W/iso3-hidden" && git log --oneline -1
CARGO_TARGET_DIR=$HOME/target-iso-base cargo build -q --release --bin baserunna --example isolation --example fake_s3_server

# The worker pool under cgroups (branch iso3/cgroup).
cd "$W/iso3-cgroup" && git log --oneline -1
CARGO_TARGET_DIR=$HOME/target-iso-cgroup cargo build -q --release --bin baserunna

# The WebAssembly sandbox (branch iso3/wasm): the guest module, then the node.
cd "$W/iso3-wasm" && git log --oneline -1
export CARGO_TARGET_DIR=$HOME/target-iso-wasm
export AR_wasm32_wasip1=$(rustc --print sysroot)/lib/rustlib/aarch64-unknown-linux-gnu/bin/llvm-ar
export CFLAGS_wasm32_wasip1="--sysroot=/nonexistent"
cargo build -q -p baserunna-guest --target wasm32-wasip1 --release
cargo build -q --release -p baserunna --bin baserunna
