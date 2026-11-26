"""Runs isolation workloads against one node binary and writes one result file
per run into a results directory. Run inside the Linux VM after build.py, with
no other node or build in the VM, and with the Mac held awake.

Usage: python3 -I measure.py <results dir> [workload...]

With no workloads, it runs every isolation workload and then the cost
measurements kv_throughput, idle_cost, and event_scan. Each isolation workload
runs its small and attack variants. Each run repeats $REPS times (default 2).
summarize.py reads the results directory.
"""

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ISOLATION_WORKLOADS = [
    "long_strings",
    "bulk_inserts",
    "merges",
    "blob_checks",
    "txn_scans",
    "table_filter_scans",
    "blob_downloads",
    "stream_filter_scans",
]
COST_WORKLOADS = ["kv_throughput", "idle_cost", "event_scan"]
COST_CLIENT_FLAGS = {"kv_throughput": ["--clients", "256"]}
RUN_PY = Path(__file__).resolve().with_name("run.py")


def copy_binaries(target_dir, bin_dir):
    """Copies the binaries, so that a build during the runs cannot change them. Removes the old
    copies first: on a shared mount, a copy over a running binary can kill it."""
    bin_dir.mkdir(parents=True, exist_ok=True)
    sources = [
        target_dir / "release/baserunna",
        target_dir / "release/examples/isolation",
        target_dir / "release/examples/fake_s3_server",
    ]
    for source in sources:
        (bin_dir / source.name).unlink(missing_ok=True)
        shutil.copy2(source, bin_dir / source.name)


def run_one(out_dir, bin_dir, workload, variant, rep, client_flags):
    out = out_dir / f"{workload}-{variant}-r{rep}.json"
    print(f"== {time.strftime('%H:%M:%S')} {workload} {variant} r{rep}", flush=True)
    command = [
        sys.executable,
        "-I",
        str(RUN_PY),
        workload,
        variant,
        str(out),
        str(bin_dir / "baserunna"),
    ]
    env = dict(os.environ, EXAMPLES_DIR=str(bin_dir))
    with open(out.with_suffix(".stdout"), "w") as log:
        subprocess.run(
            command + client_flags, env=env, stdout=log, stderr=subprocess.STDOUT
        )
    try:
        result = json.loads(out.read_text())
    except (OSError, ValueError):
        print("no result file", flush=True)
        return
    run = result.get("run") or {}
    print(
        json.dumps(
            result.get("conditions")
            or {"died": run.get("node_died"), "error": result.get("error")}
        ),
        flush=True,
    )


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    out_dir = Path(sys.argv[1]).resolve()
    workloads = sys.argv[2:] or ISOLATION_WORKLOADS + COST_WORKLOADS
    reps = int(os.environ.get("REPS", "2"))
    target_dir = Path(
        os.environ.get("ISOLATION_TARGET_DIR", Path.home() / "target-isolation")
    )
    bin_dir = out_dir / "bin"
    copy_binaries(target_dir, bin_dir)
    for workload in workloads:
        for rep in range(1, reps + 1):
            if workload in COST_WORKLOADS:
                run_one(
                    out_dir,
                    bin_dir,
                    workload,
                    "-",
                    rep,
                    COST_CLIENT_FLAGS.get(workload, []),
                )
            else:
                for variant in ("small", "attack"):
                    run_one(out_dir, bin_dir, workload, variant, rep, [])


if __name__ == "__main__":
    main()
