"""Run isolation and cost workloads against a node binary and save JSON results.

Usage: python3 -I measure.py <results dir> [workload...]

Run this script inside the Linux VM after build.py.
Stop other nodes and builds in the VM. Keep the host Mac awake.
If you omit workloads, this script runs all isolation workloads and all cost benchmarks.
This script runs both the small and attack variants of each isolation workload.
Set $REPS to change the repeat count per workload. The default is 2.
Use summarize.py to format the saved results directory.
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
RUN_SCRIPT = Path(__file__).resolve().with_name("run.py")


def copy_binaries(target_dir, bin_dir):
    """Copy binaries into `bin_dir` so concurrent builds do not overwrite them.

    Delete old copies first because overwriting a running binary on a shared mount can crash it.
    """
    bin_dir.mkdir(parents=True, exist_ok=True)
    sources = [
        target_dir / "release/baserunna",
        target_dir / "release/examples/isolation",
        target_dir / "release/examples/fake_s3_server",
    ]
    for source in sources:
        (bin_dir / source.name).unlink(missing_ok=True)
        shutil.copy2(source, bin_dir / source.name)


def run_workload(results_dir, bin_dir, workload, variant, iteration, client_flags):
    result_path = results_dir / f"{workload}-{variant}-r{iteration}.json"
    print(
        f"== {time.strftime('%H:%M:%S')} {workload} {variant} r{iteration}",
        flush=True,
    )
    command = [
        sys.executable,
        "-I",
        str(RUN_SCRIPT),
        workload,
        variant,
        str(result_path),
        str(bin_dir / "baserunna"),
    ]
    env = dict(os.environ, EXAMPLES_DIR=str(bin_dir))
    with open(result_path.with_suffix(".stdout"), "w") as log:
        subprocess.run(
            command + client_flags, env=env, stdout=log, stderr=subprocess.STDOUT
        )
    try:
        result = json.loads(result_path.read_text())
    except (OSError, ValueError):
        print("missing result file", flush=True)
        return
    run_info = result.get("run") or {}
    print(
        json.dumps(
            result.get("conditions")
            or {"stopped": run_info.get("node_stopped"), "error": result.get("error")}
        ),
        flush=True,
    )


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    results_dir = Path(sys.argv[1]).resolve()
    workloads = sys.argv[2:] or ISOLATION_WORKLOADS + COST_WORKLOADS
    repeats = int(os.environ.get("REPS", "2"))
    target_dir = Path(
        os.environ.get("ISOLATION_TARGET_DIR", Path.home() / "target-isolation")
    )
    bin_dir = results_dir / "bin"
    copy_binaries(target_dir, bin_dir)
    for workload in workloads:
        for iteration in range(1, repeats + 1):
            if workload in COST_WORKLOADS:
                run_workload(
                    results_dir,
                    bin_dir,
                    workload,
                    "-",
                    iteration,
                    COST_CLIENT_FLAGS.get(workload, []),
                )
            else:
                for variant in ("small", "attack"):
                    run_workload(results_dir, bin_dir, workload, variant, iteration, [])


if __name__ == "__main__":
    main()
