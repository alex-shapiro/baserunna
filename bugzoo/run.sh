#!/usr/bin/env bash
# Evaluates a sim_tool command against each patch in bugzoo/.
#
#   bugzoo/run.sh campaign all 50
#   bugzoo/run.sh campaign register 300 1
#   PATCHES="bugzoo/03-*.patch bugzoo/04-*.patch" bugzoo/run.sh campaign register 100
#
# For each patch, this script applies the patch, builds sim_tool in release mode,
# runs `sim_tool <args>`, records the run count and first failure to $RESULTS
# (default: bugzoo/results.tsv), and reverts the patch. If $OUT_DIR is set, full
# command output is written to $OUT_DIR/<patch>.out.
# The working tree outside bugzoo/ must be clean.
#
# Use an incremental release profile in a separate target directory for faster builds:
#
#   CARGO_TARGET_DIR=target/zoo CARGO_PROFILE_RELEASE_INCREMENTAL=true \
#   CARGO_PROFILE_RELEASE_CODEGEN_UNITS=256 bugzoo/run.sh campaign all 50
set -uo pipefail

if [ $# -eq 0 ]; then
  echo "usage: $0 <sim_tool args...>   e.g. $0 campaign all 50" >&2
  exit 2
fi
cd "$(git rev-parse --show-toplevel)" || exit 1
if ! git diff --quiet HEAD -- . ':!bugzoo'; then
  echo "the working tree has uncommitted changes; commit or stash them first" >&2
  exit 1
fi

results=${RESULTS:-bugzoo/results.tsv}
target=${CARGO_TARGET_DIR:-target}
bin="$target/release/examples/sim_tool"
patches=${PATCHES:-$(ls bugzoo/[0-9]*.patch)}
current=""
revert() { [ -n "$current" ] && git apply -R "$current" && current=""; }
trap 'revert; exit 130' INT TERM

[ -s "$results" ] || printf 'patch\tcommand\tseeds\tfailed\tpassed\tfirst_failure\n' > "$results"
for p in $patches; do
  name=$(basename "$p" .patch)
  if ! git apply "$p"; then
    printf '%s\t%s\t-\t-\t-\tpatch does not apply\n' "$name" "$*" | tee -a "$results"
    continue
  fi
  current=$p
  if ! cargo build --release --example sim_tool -q 2> "bugzoo/.build-$name.log"; then
    printf '%s\t%s\t-\t-\t-\tbuild failed (bugzoo/.build-%s.log)\n' "$name" "$*" "$name" | tee -a "$results"
    revert
    continue
  fi
  rm -f "bugzoo/.build-$name.log"
  revert
  out=$("$bin" "$@" 2>&1)
  if [ -n "${OUT_DIR:-}" ]; then
    mkdir -p "$OUT_DIR" && printf '%s\n' "$out" > "$OUT_DIR/$name.out"
  fi
  seeds=$(sed -n -e 's/^\([0-9][0-9]*\) runs in .*/\1/p' -e 's/^explored \([0-9][0-9]*\) runs.*/\1/p' <<< "$out" | awk '{s += $1} END {print s + 0}')
  if [ "$seeds" -eq 0 ]; then
    echo "sim_tool $* ran nothing: $(tail -n 1 <<< "$out")" >&2
    exit 1
  fi
  failed=$(grep -c '^FAIL ' <<< "$out")
  first=$(grep -m1 '^FAIL ' <<< "$out" | cut -c1-200)
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$*" "$seeds" "$failed" "$((seeds - failed))" "${first:--}" | tee -a "$results"
done
