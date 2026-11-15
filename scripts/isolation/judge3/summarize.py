"""Summarizes the round 3 judge results in target/isolation-results/judge3/.

Prints one JSON document with:
- cells: for each node, workload, and variant, every run's conditions, and
  the cell result. A cell passes only if it has at least 2 runs and every
  run passes.
- victim_alone: for each node, the victim's p99 in phase A (no attacker) of
  every workload run, and the median of those values. This is a cost.
- costs: the kv_throughput, idle_cost, and event_scan readings of each run.

Usage: python3 -I summarize.py [results dir]
"""

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

RESULTS = Path(sys.argv[1] if len(sys.argv) > 1 else "/Users/alex/Projects/baserunna/target/isolation-results/judge3")
MIN_RUNS_PER_CELL = 2


def run_summary(path, d):
    run = d.get("run") or {}
    return {
        "file": path.name,
        "pass": bool((d.get("conditions") or {}).get("pass")),
        "conditions": d.get("conditions"),
        "error": d.get("error"),
        "node_died": run.get("node_died"),
        "vm_busy": d.get("vm_busy"),
        "idle_node_mib": (d.get("idle_node_bytes") or 0) / 2**20,
        "memory_stat_end_bytes": run.get("memory_stat_end_bytes"),
    }


def main():
    cells = defaultdict(list)
    victim_alone = defaultdict(list)
    costs = defaultdict(list)
    for path in sorted(RESULTS.glob("*.json")):
        node = path.name.split("-", 1)[0]
        d = json.loads(path.read_text())
        workload, variant = d.get("workload"), d.get("variant")
        if workload in ("kv_throughput", "idle_cost", "event_scan"):
            costs[f"{node} {workload}"].append({"file": path.name, workload: d.get(workload), "error": d.get("error")})
            continue
        cells[(node, workload, variant)].append(run_summary(path, d))
        p99 = (d.get("phase_a_victim_alone") or {}).get("p99_ms")
        if p99 is not None:
            victim_alone[node].append(p99)

    out_cells = []
    for (node, workload, variant), runs in sorted(cells.items()):
        out_cells.append({
            "node": node,
            "workload": workload,
            "variant": variant,
            "runs": len(runs),
            "pass": len(runs) >= MIN_RUNS_PER_CELL and all(r["pass"] for r in runs),
            "run_results": runs,
        })
    out_victim = {
        node: {"median_p99_ms": statistics.median(v), "runs": len(v), "p99_ms": v}
        for node, v in sorted(victim_alone.items())
    }
    print(json.dumps({"cells": out_cells, "victim_alone": out_victim, "costs": costs}, indent=1))


if __name__ == "__main__":
    main()
