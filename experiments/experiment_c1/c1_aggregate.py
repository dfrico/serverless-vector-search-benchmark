"""
Experiment C1, Phase 3 — aggregate the sweep cells into three axis tables.

Reads results/measure_*.json (one per cell) and results/phase1_baseline.json,
and writes results/axis_tables.json + results/axis_tables.md. The baseline cell
(measure_base.json) is the shared anchor point that appears in all three axes:
n_probe=10, num_workers=16, blocks_per_task=16.

Anchors for the cost verdict (from Experiment B, findings/20):
  - Blocks baseline: $0.000477 / query at 100% recall (wall-time cost model).
  - S3 Vectors:      $0.0000025 / query at 97.3% recall.
The 190x gap is 0.000477 / 0.0000025 ~= 191x.
"""
from __future__ import annotations

import glob
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
import c1_common as K  # noqa: E402

EXPB_BLOCKS_COST = 0.000477   # $/query, 100% recall (Experiment B wall model)
S3V_COST = 0.0000025          # $/query, 97.3% recall
S3V_RECALL = 97.3


def load_cells():
    cells = {}
    for p in glob.glob(os.path.join(K.RESULTS_DIR, "measure_*.json")):
        if p.endswith(".raw.json"):   # per-query sidecars (2026-10-16), not cells
            continue
        with open(p) as f:
            m = json.load(f)
        cells[m["label"]] = m
    return cells


def row(m):
    L = m["latency_s"]
    return {
        "label": m["label"],
        "n_probe": m["params"]["n_probe"],
        "num_workers": m["params"]["num_workers"],
        "blocks_per_task": m["params"]["blocks_per_task"],
        "recall": m["recall_at_10_pct"],
        "p50": L["p50"], "p95": L["p95"], "mean": L["mean"], "min": L["min"],
        "billed": m["cost_billed_per_query_usd"],
        "wall": m["cost_wall_per_query_usd"],
        "throughput": m["throughput_ops_s"],
        "avg_map_tasks": m.get("avg_n_map_tasks"),
        "ingest_cost": m.get("ingest_cost_usd"),
        "ingest_index_s": m.get("ingest_index_s"),
    }


def main():
    cells = load_cells()
    if "base" not in cells:
        print("MISSING measure_base.json — run phase 1 first.")
        return 1
    base = row(cells["base"])

    # Axis A: n_probe {1,3,5,7,10=base}
    axisA = []
    for lbl in ["a_np1", "a_np3", "a_np5", "a_np7"]:
        if lbl in cells:
            axisA.append(row(cells[lbl]))
    axisA.append(base)
    axisA.sort(key=lambda r: r["n_probe"])

    # Axis B: num_workers {4,8,16=base,32}
    axisB = []
    for lbl in ["b_nw4", "b_nw8", "b_nw32"]:
        if lbl in cells:
            axisB.append(row(cells[lbl]))
    axisB.append(base)
    axisB.sort(key=lambda r: r["num_workers"])

    # Axis C: blocks_per_task {1,2,4,8,16=base}
    axisC = []
    for lbl in ["c_bpt1", "c_bpt2", "c_bpt4", "c_bpt8"]:
        if lbl in cells:
            axisC.append(row(cells[lbl]))
    axisC.append(base)
    axisC.sort(key=lambda r: r["blocks_per_task"])

    # Best cost/query at full recall (>=99.99%) across all cells.
    full = [row(m) for m in cells.values() if m["recall_at_10_pct"] >= 99.99]
    best_full = min(full, key=lambda r: r["billed"]) if full else None
    # Absolute cheapest regardless of recall.
    allrows = [row(m) for m in cells.values()]
    best_any = min(allrows, key=lambda r: r["billed"]) if allrows else None

    tables = {
        "baseline": base,
        "axisA_n_probe": axisA,
        "axisB_num_workers": axisB,
        "axisC_blocks_per_task": axisC,
        "anchors": {
            "expB_blocks_cost_per_query": EXPB_BLOCKS_COST,
            "s3v_cost_per_query": S3V_COST,
            "s3v_recall_pct": S3V_RECALL,
            "gap_x": round(EXPB_BLOCKS_COST / S3V_COST, 1),
        },
        "best_at_full_recall": best_full,
        "best_any_recall": best_any,
    }
    with open(os.path.join(K.RESULTS_DIR, "axis_tables.json"), "w") as f:
        json.dump(tables, f, indent=2, sort_keys=True)

    def fmt(rows, cols, headers):
        lines = ["| " + " | ".join(headers) + " |",
                 "|" + "---|" * len(headers)]
        for r in rows:
            lines.append("| " + " | ".join(str(r[c]) for c in cols) + " |")
        return "\n".join(lines)

    md = []
    md.append("# Experiment C1 — Blocks tuning at 1M (deep_1m, top_k=10)\n")
    md.append(f"Baseline anchor: n_probe=10, num_workers=16, blocks_per_task=16 -> "
              f"recall {base['recall']}%, p50 {base['p50']}s, billed "
              f"${base['billed']}/query, wall ${base['wall']}/query.\n")
    md.append("## Axis A — n_probe (rebuild per value; recall + latency)\n")
    md.append(fmt(axisA, ["n_probe", "recall", "p50", "p95", "mean", "billed", "wall", "throughput"],
                  ["n_probe", "recall %", "p50 s", "p95 s", "mean s", "billed $/q", "wall $/q", "ops/s"]))
    md.append("\n## Axis B — num_workers (ingest-only; cost signal + query flatness)\n")
    md.append(fmt(axisB, ["num_workers", "ingest_cost", "ingest_index_s", "recall", "p50", "billed"],
                  ["num_workers", "ingest $", "ingest index_s", "recall %", "p50 s", "billed $/q"]))
    md.append("\n## Axis C — blocks_per_task (config rewrite; latency/cost trade)\n")
    md.append(fmt(axisC, ["blocks_per_task", "avg_map_tasks", "recall", "p50", "p95", "billed", "wall", "throughput"],
                  ["blocks_per_task", "map tasks", "recall %", "p50 s", "p95 s", "billed $/q", "wall $/q", "ops/s"]))
    md.append("\n## Headline\n")
    if best_full:
        md.append(f"- Best Blocks cost/query at full recall (>=99.99%): "
                  f"${best_full['billed']} billed (${best_full['wall']} wall) at "
                  f"n_probe={best_full['n_probe']}, num_workers={best_full['num_workers']}, "
                  f"blocks_per_task={best_full['blocks_per_task']}.")
    if best_any:
        md.append(f"- Absolute cheapest cell: ${best_any['billed']} billed at "
                  f"recall {best_any['recall']}% "
                  f"(n_probe={best_any['n_probe']}, bpt={best_any['blocks_per_task']}).")
    md.append(f"- Experiment B Blocks baseline: ${EXPB_BLOCKS_COST}/query (100% recall).")
    md.append(f"- S3 Vectors: ${S3V_COST}/query ({S3V_RECALL}% recall). "
              f"Gap = {round(EXPB_BLOCKS_COST / S3V_COST, 1)}x.")
    if best_full:
        md.append(f"- Gap after tuning (best full-recall billed vs S3V): "
                  f"{round(best_full['billed'] / S3V_COST, 1)}x.")
    with open(os.path.join(K.RESULTS_DIR, "axis_tables.md"), "w") as f:
        f.write("\n".join(md) + "\n")

    print("\n".join(md))
    print("\nwrote results/axis_tables.json and results/axis_tables.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
