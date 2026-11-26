"""Print a Markdown summary of isolation results written by measure.py.

Usage: python3 -I summarize.py <results dir>

A workload passes only if every run of its small and attack variants passes.
This script prints the failed criterion numbers for each failed run.
This script also prints key metrics for each cost benchmark.
"""

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

COST_WORKLOADS = ("kv_throughput", "idle_cost", "event_scan")
CRITERIA = (
    "1_node_running",
    "2_victim_requests_succeed",
    "3_victim_p99_within_limit",
    "4_rejections_are_http_errors",
    "5_small_variant_succeeds",
)


def failed_criteria(result):
    """Return the numbers of criteria that failed or did not run."""
    conditions = result.get("conditions") or {}
    failed = []
    for name in CRITERIA:
        value = conditions.get(name)
        if isinstance(value, dict):
            value = value.get("pass")
        if value is False or (value is None and name != "5_small_variant_succeeds"):
            failed.append(name.split("_", 1)[0])
    return failed


def run_cell(result):
    """Format a table cell with the run verdict and key metrics."""
    if (result.get("conditions") or {}).get("pass"):
        verdict = "pass"
    else:
        verdict = "fail " + ",".join(failed_criteria(result))
    latency = (result.get("conditions") or {}).get("3_victim_p99_within_limit") or {}
    details = []
    if latency:
        details.append(
            f"p99 {latency['p99_alone_ms']:.0f}→{latency['p99_attacked_ms']:.0f} ms"
        )
    growth = result.get("phase_b_growth_from_idle_mib")
    if growth is not None:
        details.append(f"{growth:.0f} MiB")
    if result.get("error"):
        details.append("error")
    return verdict + (f" ({', '.join(details)})" if details else "")


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    results_dir = Path(sys.argv[1])
    isolation_runs = defaultdict(list)
    cost_runs = defaultdict(list)
    for path in sorted(results_dir.glob("*.json")):
        result = json.loads(path.read_text())
        workload = result.get("workload")
        if workload in COST_WORKLOADS:
            cost_runs[workload].append(result)
        else:
            isolation_runs[(workload, result.get("variant"))].append(result)

    workloads = sorted({workload for workload, _ in isolation_runs})
    print("| Workload | Result | Small runs | Attack runs |")
    print("| --- | --- | --- | --- |")
    for workload in workloads:
        small, attack = (
            isolation_runs[(workload, "small")],
            isolation_runs[(workload, "attack")],
        )
        passed = (
            bool(small)
            and bool(attack)
            and all(
                (run_result.get("conditions") or {}).get("pass")
                for run_result in small + attack
            )
        )
        cells = [
            "<br>".join(run_cell(run_result) for run_result in group) or "not run"
            for group in (small, attack)
        ]
        print(
            f"| {workload} | {'pass' if passed else 'fail'} | {cells[0]} | {cells[1]} |"
        )

    victim_alone_p99_ms = [
        run_result["phase_a_victim_alone"]["p99_ms"]
        for group in isolation_runs.values()
        for run_result in group
        if run_result.get("phase_a_victim_alone")
    ]
    if victim_alone_p99_ms:
        print(
            f"\nVictim p99 without an attacker: median {statistics.median(victim_alone_p99_ms):.1f} ms over {len(victim_alone_p99_ms)} runs."
        )

    for result in cost_runs["kv_throughput"]:
        kv_result = (result.get("kv_throughput") or {}).get("result") or {}
        print(
            f"\nkv_throughput: {kv_result.get('ops_per_s', 0):,.0f} ops/s, {kv_result.get('node_cpu_usec_per_op', 0):.1f} µs node CPU per op, "
            f"p99 {kv_result.get('p99_ms', 0):.1f} ms, {kv_result.get('errors')} errors"
        )
    for result in cost_runs["idle_cost"]:
        idle_summary = (result.get("idle_cost") or {}).get("summary") or {}
        print(
            f"\nidle_cost: {idle_summary.get('memory_idle_end_mib', 0):.1f} MiB idle after setup, "
            f"{idle_summary.get('idle_cpu_usec_per_s', 0):.0f} µs CPU per s"
        )
    for result in cost_runs["event_scan"]:
        event_scan = result.get("event_scan") or {}
        queries = ", ".join(
            f"{query['query']} {query['median_ms']:.0f} ms"
            for query in event_scan.get("per_query", [])
        )
        print(
            f"\nevent_scan: {event_scan.get('failed_queries')} failed queries; median per query: {queries}"
        )


if __name__ == "__main__":
    main()
