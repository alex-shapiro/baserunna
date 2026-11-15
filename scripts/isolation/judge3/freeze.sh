#!/usr/bin/env bash
# Copies the binaries the judge runs into $HOME/iso-judge3, so later builds
# cannot change them. Run inside the VM ashdev after build.sh.
set -euo pipefail
J=$HOME/iso-judge3
mkdir -p "$J/examples"
rm -f "$J/base" "$J/cgroup" "$J/wasm" "$J/guest.wasm" "$J/run.sh" "$J/examples/"*
cp "$HOME/target-iso-base/release/baserunna" "$J/base"
cp "$HOME/target-iso-cgroup/release/baserunna" "$J/cgroup"
cp "$HOME/target-iso-wasm/release/baserunna" "$J/wasm"
cp "$HOME/target-iso-wasm/wasm32-wasip1/release/baserunna_guest.wasm" "$J/guest.wasm"
cp "$HOME/target-iso-base/release/examples/isolation" "$HOME/target-iso-base/release/examples/fake_s3_server" "$J/examples/"
cp /Users/alex/Projects/baserunna-wt/iso3-hidden/scripts/isolation/run.sh "$J/run.sh"
ls -la "$J" "$J/examples"
