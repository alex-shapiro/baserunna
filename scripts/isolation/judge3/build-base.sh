#!/usr/bin/env bash
# Builds today's node and the isolation harness from the main checkout (branch
# iso/base) into $HOME/target-iso-base. Run inside the VM ashdev, under the
# host's shared compile lock.
set -euo pipefail
cd /Users/alex/Projects/baserunna
git log --oneline -1
CARGO_TARGET_DIR=$HOME/target-iso-base cargo build -q --release --bin baserunna --example isolation --example fake_s3_server
