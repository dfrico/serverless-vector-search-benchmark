"""
Experiment C3, Phase 4 — aggregate the 10M sweep cells into the comparison table.

Same structure as Experiment B's eb_aggregate, with ONE deliberate change: Blocks
cost uses the BILLED model (finding 22 / methodology-decisions), not the wall
model eb_aggregate used. Billed = sum of per-task map+reduce durations x 3.008GB
x x86 rate + one request per activation. This is the honest Lambda charge under
fan-out (20 map tasks/query at 10M), and it is the number the 1M-vs-10M
comparison must use to stay consistent with findings 22 and 24.

  - Blocks   (x86, 3008MB): c1_common.query_costs(adapter_timings).billed_cost.
  - LanceDB  (arm64, 2048MB): server_ms/1000 x 2.048GB x arm rate + 1 request.
  - S3 Vec   : one QueryVectors request at $2.50/M (query bytes negligible).

Discovers whatever sweep_<adapter>_c<conc> cells exist in results/. Verifies the
cross-adapter fairness anchor (identical corpus sha256 + metric l2) across every
cell. Writes results/comparison_table_10m.json and .md.

Run via c3_aws.sh (any venv; pure local files + stdlib, plus c1_common import).
"""
from __future__ import annotations

import glob
import json
import os
import re
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
import c3_common as K  # noqa: E402
import eb_common as C  # noqa: E402
from harness.aggregate import RunAggregator  # noqa: E402
import c1_common  # noqa: E402

ADAPTER_ORDER = {"s3vectors": 0, "lancedb": 1, "blocks": 2}
LANCEDB_MEM_GB = K.LANCEDB_MEM_GB


def blocks_billed(t):
    """Billed Blocks cost for one query from its adapter_timings (finding 22)."""
    (wall_cost, billed_cost, n_map, n_reduce, billed_map_s, billed_reduce_s,
     map_exec, red_exec) = c1_common.query_costs(t)
    # if per-task durations are missing, billed_map_s collapses to 0 and we would
    # under-count; flag that by returning wall as a conservative fallback.
    if billed_map_s == 0.0 and map_exec > 0.0:
        return wall_cost, True  # fell back to wall
    return billed_cost, False


def cell_cost(adapter, records):
    req = C.LAMBDA_REQ_PER_M / 1e6
    total = 0.0
    fell_back = False
    for r in records:
        if not r.get("ok"):
            continue
        t = r.get("adapter_timings", {}) or {}
        if adapter == "blocks":
            c, fb = blocks_billed(t)
            total += c
            fell_back = fell_back or fb
        elif adapter == "lancedb":
            total += (t.get("server_ms", 0.0) / 1000.0) * LANCEDB_MEM_GB * C.LAMBDA_ARM_GBS + req
        elif adapter == "s3vectors":
            total += C.S3V_QUERY_PER_M / 1e6
    return total, fell_back


def discover_cells():
    cells = []
    for path in glob.glob(os.path.join(K.RESULTS_DIR, "sweep_*_c*.jsonl")):
        m = re.match(r"sweep_(blocks|s3vectors|lancedb)_c(\d+)\.jsonl$",
                     os.path.basename(path))
        if m:
            cells.append((m.group(1), int(m.group(2)), path))
    cells.sort(key=lambda x: (ADAPTER_ORDER.get(x[0], 9), x[1]))
    return cells


def main():
    cells = []
    sha_seen, metric_seen = set(), set()
    any_fallback = False

    for adapter, conc, jsonl in discover_cells():
        summ = RunAggregator(jsonl).aggregate()
        m = summ.manifest
        fp = m.get("dataset_fingerprint", {})
        sha = fp.get("content_sha256")
        metric = m.get("capabilities", {}).get("metric")
        if sha:
            sha_seen.add(sha)
        if metric:
            metric_seen.add(metric)

        qop = next((o for o in summ.per_op if o.op == "query"), None)
        lat = qop.latency if qop else None
        with open(jsonl) as f:
            records = [json.loads(x) for x in f if x.strip()]
        cost_total, fb = cell_cost(adapter, records)
        any_fallback = any_fallback or fb
        n_ok = summ.ok_total
        cost_per_q = cost_total / n_ok if n_ok else 0.0

        cells.append({
            "adapter": adapter,
            "configured_concurrency": m.get("config", {}).get("max_in_flight"),
            "effective_concurrency": m.get("effective_concurrency"),
            "n_ok": n_ok,
            "n_fail": summ.fail_total,
            "realised_throughput_ops_s": round(summ.realised_throughput, 4),
            "latency_s": ({
                "min": round(lat.min, 4), "p50": round(lat.p50, 4),
                "p95": round(lat.p95, 4), "p99": round(lat.p99, 4),
                "max": round(lat.max, 4), "mean": round(lat.mean, 4),
            } if lat else None),
            "cost_model": "billed" if adapter == "blocks" and not fb else
                          ("wall_fallback" if adapter == "blocks" else "billed"),
            "cost_total_usd": round(cost_total, 6),
            "cost_per_query_usd": round(cost_per_q, 8),
            "cost_per_1000q_usd": round(cost_per_q * 1000, 5),
            "corpus_sha256": sha,
            "metric": metric,
        })

    anchor_ok = (sha_seen == {K.CORPUS_SHA256})
    metric_ok = (metric_seen == {"l2"})

    table = {
        "dataset": K.DATASET,
        "top_k": K.TOP_K,
        "n_cells": len(cells),
        "cost_model_note": "Blocks=billed (finding 22); LanceDB/S3V billed==wall.",
        "fairness": {
            "corpus_sha256_expected": K.CORPUS_SHA256,
            "corpus_sha256_across_cells": sorted(sha_seen),
            "corpus_sha256_identical": anchor_ok,
            "metric_across_cells": sorted(metric_seen),
            "metric_identical_l2": metric_ok,
        },
        "cells": cells,
    }
    out_json = os.path.join(K.RESULTS_DIR, "comparison_table_10m.json")
    with open(out_json, "w") as f:
        json.dump(table, f, indent=2, sort_keys=True)

    lines = []
    lines.append("# Experiment C3 — three-way comparison at 10M (deep_10m, top_k=10)")
    lines.append("")
    lines.append(f"Fairness: corpus sha256 identical across all {len(cells)} cells: "
                 f"{'yes' if anchor_ok else 'NO'} ({K.CORPUS_SHA256[:16]}...). "
                 f"metric = l2 across all cells: {'yes' if metric_ok else 'NO'}.")
    lines.append("")
    lines.append("Latency is harness-side submit->ack (OpResult.latency_s), seconds. "
                 "Throughput is realised ok-ops/second (closed-loop). Cost is the "
                 "BILLED AWS estimate (finding 22): Blocks sums per-task Lambda GB-s "
                 "over its 20 map tasks/query; LanceDB is server_ms x 2.048GB; S3 "
                 "Vectors is one QueryVectors request.")
    if any_fallback:
        lines.append("")
        lines.append("WARNING: at least one Blocks cell lacked per-task durations "
                     "in its records, so its cost fell back to the wall model "
                     "(over-counts fan-out). Marked cost_model=wall_fallback.")
    lines.append("")
    lines.append("| adapter | conc (cfg/eff) | n | throughput ops/s | p50 s | p95 s "
                 "| p99 s | mean s | cost/query $ | cost/1k q $ |")
    lines.append("|" + "---|" * 10)
    for c in cells:
        L = c["latency_s"] or {}
        lines.append(
            f"| {c['adapter']} | {c['configured_concurrency']}/{c['effective_concurrency']} "
            f"| {c['n_ok']} | {c['realised_throughput_ops_s']:.2f} "
            f"| {L.get('p50', 0):.3f} | {L.get('p95', 0):.3f} | {L.get('p99', 0):.3f} "
            f"| {L.get('mean', 0):.3f} | {c['cost_per_query_usd']:.7f} "
            f"| {c['cost_per_1000q_usd']:.4f} |")
    lines.append("")
    out_md = os.path.join(K.RESULTS_DIR, "comparison_table_10m.md")
    with open(out_md, "w") as f:
        f.write("\n".join(lines) + "\n")

    print("\n".join(lines))
    print(f"\nwrote {os.path.relpath(out_json, K.WORKSPACE)} + .md")
    print(f"fairness: sha_identical={anchor_ok} metric_l2={metric_ok}")
    return 0 if (anchor_ok and metric_ok) else 1


if __name__ == "__main__":
    raise SystemExit(main())
