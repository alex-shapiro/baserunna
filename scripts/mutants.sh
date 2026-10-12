#!/bin/sh
# Run `cargo mutants` (configured in .cargo/mutants.toml), killing any
# mutated test binary above MUTANTS_MAX_RSS_MB (default 2048). Some mutants
# allocate without bound and can exhaust the machine before cargo-mutants'
# timeout; a killed test fails, so its mutant counts as caught.
#
#   scripts/mutants.sh                        # the whole configuration, 2 jobs
#   scripts/mutants.sh -f src/engine/txn.rs   # extra arguments go to cargo mutants
set -u
max_kb=$(( ${MUTANTS_MAX_RSS_MB:-2048} * 1024 ))
cargo mutants -j "${MUTANTS_JOBS:-2}" "$@" &
run=$!
while kill -0 "$run" 2>/dev/null; do
    # Match the executable path only, never arguments.
    ps -axo pid=,rss=,comm= | while read -r pid rss cmd; do
        case "$cmd" in
            */target/mutants/deps/*)
                if [ "$rss" -gt "$max_kb" ]; then
                    echo "mutants.sh: killing $pid ($((rss / 1024)) MiB): $cmd" >&2
                    kill -9 "$pid"
                fi
                ;;
        esac
    done
    sleep 1
done
wait "$run"
