"""Prints a Markdown summary of the isolation results that measure.py wrote.

A workload passes only if every run of its small and attack variants passes.
For each failed run, the table names the pass criteria that failed. The cost
measurements (kv_throughput, idle_cost, event_scan) have no pass criteria, so
the summary prints their main numbers.

Usage: python3 -I summarize.py <results dir>
"""

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

COST_WORKLOADS = ("kv_throughput", "idle_cost", "event_scan")
CRITERIA = (
    "1_node_survives",
    "2_victim_requests_succeed",
    "3_victim_p99_within_limit",
    "4_refusals_are_http_errors",
    "5_small_variant_succeeds",
)


def failed_criteria(result):
    """Returns the numbers of the criteria that failed or did not run."""
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
    """Returns one table cell for one run: pass, or the failed criteria and the key numbers."""
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
    results = Path(sys.argv[1])
    runs = defaultdict(list)
    costs = defaultdict(list)
    for path in sorted(results.glob("*.json")):
        result = json.loads(path.read_text())
        workload = result.get("workload")
        if workload in COST_WORKLOADS:
            costs[workload].append(result)
        else:
            runs[(workload, result.get("variant"))].append(result)

    workloads = sorted({w for w, _ in runs})
    print("| Workload | Result | Small runs | Attack runs |")
    print("| --- | --- | --- | --- |")
    for workload in workloads:
        small, attack = runs[(workload, "small")], runs[(workload, "attack")]
        passed = (
            bool(small)
            and bool(attack)
            and all((r.get("conditions") or {}).get("pass") for r in small + attack)
        )
        cells = [
            "<br>".join(run_cell(r) for r in group) or "not run"
            for group in (small, attack)
        ]
        print(
            f"| {workload} | {'pass' if passed else 'fail'} | {cells[0]} | {cells[1]} |"
        )

    victim_alone = [
        r["phase_a_victim_alone"]["p99_ms"]
        for group in runs.values()
        for r in group
        if r.get("phase_a_victim_alone")
    ]
    if victim_alone:
        print(
            f"\nVictim p99 with no attacker: median {statistics.median(victim_alone):.1f} ms over {len(victim_alone)} runs."
        )

    for result in costs["kv_throughput"]:
        kv = (result.get("kv_throughput") or {}).get("result") or {}
        print(
            f"\nkv_throughput: {kv.get('ops_per_s', 0):,.0f} ops/s, {kv.get('node_cpu_usec_per_op', 0):.1f} µs node CPU per op, "
            f"p99 {kv.get('p99_ms', 0):.1f} ms, {kv.get('errors')} errors"
        )
    for result in costs["idle_cost"]:
        idle = (result.get("idle_cost") or {}).get("summary") or {}
        print(
            f"\nidle_cost: {idle.get('memory_idle_end_mib', 0):.1f} MiB idle after setup, "
            f"{idle.get('idle_cpu_usec_per_s', 0):.0f} µs CPU per s"
        )
    for result in costs["event_scan"]:
        scan = result.get("event_scan") or {}
        queries = ", ".join(
            f"{q['query']} {q['median_ms']:.0f} ms" for q in scan.get("per_query", [])
        )
        print(
            f"\nevent_scan: {scan.get('failed_queries')} failed queries; median per query: {queries}"
        )


if __name__ == "__main__":
    main()
