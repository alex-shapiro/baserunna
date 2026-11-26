#!/usr/bin/env bash
# Builds the node and the isolation harness from this checkout into
# $ISOLATION_TARGET_DIR, which defaults to $HOME/target-isolation. Run inside
# the Linux VM.
set -euo pipefail
cd "$(dirname "$0")/../.."
git log --oneline -1
export CARGO_TARGET_DIR=${ISOLATION_TARGET_DIR:-$HOME/target-isolation}
cargo build -q --release --bin baserunna --example isolation --example fake_s3_server
