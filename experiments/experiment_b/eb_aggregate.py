"""
Experiment B, Phase 4 — aggregate the 9 sweep cells into the comparison table.

For each (adapter, concurrency) cell we reuse harness/aggregate.py for the
latency percentiles (from OpResult.latency_s, the harness-side ground truth) and
realised throughput, then add a per-cell AWS cost estimate computed from the raw
adapter_timings:

  - Blocks (x86, 3008MB): billed Lambda GB-s = (map_execution + reduce_execution)
    seconds x 3.008GB per query, plus one request per map + reduce activation.
  - LanceDB (arm64, 2048MB): billed GB-s = server_ms/1000 x 2.048GB per query,
    plus one request per query.
  - S3 Vectors: one QueryVectors request per query at $2.50/M (query data bytes
    are negligible at 96-dim). Ingest PUT cost is a separate, one-off charge.

Cross-adapter fairness is verified from the 9 manifests: identical corpus
content_sha256 and identical metric across every cell (finding-17 anchor).

Writes results/comparison_table.json and results/comparison_table.md.
Run via eb_aws.sh (any venv with the repo on path).
"""
from __future__ import annotations

import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))   # experiments/experiment_b -> repo root
# _HERE (for the sibling eb_common) + repo root (for the `harness` package).
for _p in (_HERE, WORKSPACE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import eb_common as C  # noqa: E402
from harness.aggregate import RunAggregator  # noqa: E402

ADAPTERS = ["blocks", "s3vectors", "lancedb"]
BLOCKS_MEM_GB = 3.008
LANCEDB_MEM_GB = 2.048


def cell_cost(adapter, records):
    """Total AWS cost estimate over the ok records in one cell (USD)."""
    req = C.LAMBDA_REQ_PER_M / 1e6
    total = 0.0
    for r in records:
        if not r.get("ok"):
            continue
        t = r.get("adapter_timings", {})
        if adapter == "blocks":
            gbs = (t.get("0_map_execution_blocks", 0.0)
                   + t.get("0_reduce_execution_blocks", 0.0)) * BLOCKS_MEM_GB
            n_act = len(t.get("0_map_invocation_blocks", []) or []) \
                + len(t.get("0_reduce_invocation_blocks", []) or [])
            total += gbs * C.LAMBDA_X86_GBS + n_act * req
        elif adapter == "lancedb":
            gbs = (t.get("server_ms", 0.0) / 1000.0) * LANCEDB_MEM_GB
            total += gbs * C.LAMBDA_ARM_GBS + req
        elif adapter == "s3vectors":
            total += C.S3V_QUERY_PER_M / 1e6  # one QueryVectors request
    return total


def main():
    cells = []
    sha_seen, metric_seen = {}, {}

    for adapter in ADAPTERS:
        for conc in C.CONCURRENCY_LEVELS:
            stem = f"sweep_{adapter}_c{conc}"
            jsonl = os.path.join(C.RESULTS_DIR, f"{stem}.jsonl")
            manifest = os.path.join(C.RESULTS_DIR, f"{stem}.manifest.json")
            if not os.path.exists(jsonl):
                print(f"MISSING {jsonl}")
                continue
            summ = RunAggregator(jsonl).aggregate()
            m = summ.manifest
            fp = m.get("dataset_fingerprint", {})
            sha = fp.get("content_sha256")
            metric = m.get("capabilities", {}).get("metric")
            sha_seen[stem] = sha
            metric_seen[stem] = metric

            qop = next((o for o in summ.per_op if o.op == "query"), None)
            lat = qop.latency if qop else None
            with open(jsonl) as f:
                records = [json.loads(x) for x in f if x.strip()]
            cost_total = cell_cost(adapter, records)
            n_ok = summ.ok_total
            cost_per_q = cost_total / n_ok if n_ok else 0.0

            cells.append({
                "adapter": adapter,
                "adapter_name": m.get("adapter"),
                "configured_concurrency": m.get("config", {}).get("max_in_flight"),
                "effective_concurrency": m.get("effective_concurrency"),
                "max_safe_concurrency": m.get("capabilities", {}).get("max_safe_concurrency"),
                "n_ok": n_ok,
                "n_fail": summ.fail_total,
                "realised_throughput_ops_s": round(summ.realised_throughput, 4),
                "nominal_ops_s": summ.nominal_ops_per_second,
                "latency_s": ({
                    "min": round(lat.min, 4), "p50": round(lat.p50, 4),
                    "p95": round(lat.p95, 4), "p99": round(lat.p99, 4),
                    "max": round(lat.max, 4), "mean": round(lat.mean, 4),
                } if lat else None),
                "cost_total_usd": round(cost_total, 6),
                "cost_per_query_usd": round(cost_per_q, 8),
                "cost_per_1000q_usd": round(cost_per_q * 1000, 5),
                "corpus_sha256": sha,
                "metric": metric,
            })

    # --- cross-adapter fairness checks ---
    shas = set(sha_seen.values())
    metrics = set(metric_seen.values())
    anchor_ok = (shas == {C.CORPUS_SHA256})
    metric_ok = (metrics == {"l2"})

    table = {
        "dataset": C.DATASET,
        "top_k": C.TOP_K,
        "n_cells": len(cells),
        "fairness": {
            "corpus_sha256_expected": C.CORPUS_SHA256,
            "corpus_sha256_across_cells": sorted(shas),
            "corpus_sha256_identical": anchor_ok,
            "metric_across_cells": sorted(m for m in metrics if m),
            "metric_identical_l2": metric_ok,
        },
        "cells": cells,
    }
    out_json = os.path.join(C.RESULTS_DIR, "comparison_table.json")
    with open(out_json, "w") as f:
        json.dump(table, f, indent=2, sort_keys=True)
    print(f"wrote {os.path.relpath(out_json, WORKSPACE)}")

    # --- markdown ---
    lines = []
    lines.append("# Experiment B — three-way comparison at 1M (deep_1m, top_k=10)")
    lines.append("")
    lines.append(f"Fairness: corpus sha256 identical across all {len(cells)} cells: "
                 f"{'yes' if anchor_ok else 'NO'} ({C.CORPUS_SHA256[:16]}...). "
                 f"metric = l2 across all cells: {'yes' if metric_ok else 'NO'}.")
    lines.append("")
    lines.append("Latency is harness-side submit->ack (OpResult.latency_s), seconds. "
                 "Throughput is realised ok-ops/second (closed-loop; nominal was 100 ops/s). "
                 "Cost is an AWS estimate: Lambda GB-s + request fees (Blocks/LanceDB) or "
                 "QueryVectors request fees (S3 Vectors).")
    lines.append("")
    header = ("| adapter | conc (cfg/eff) | n | throughput ops/s | p50 s | p95 s | "
              "p99 s | mean s | cost/query $ | cost/1k q $ |")
    sep = "|" + "---|" * 11
    lines.append(header)
    lines.append(sep)
    for c in cells:
        L = c["latency_s"] or {}
        lines.append(
            f"| {c['adapter']} | {c['configured_concurrency']}/{c['effective_concurrency']} "
            f"| {c['n_ok']} | {c['realised_throughput_ops_s']:.2f} "
            f"| {L.get('p50', 0):.3f} | {L.get('p95', 0):.3f} | {L.get('p99', 0):.3f} "
            f"| {L.get('mean', 0):.3f} | {c['cost_per_query_usd']:.7f} "
            f"| {c['cost_per_1000q_usd']:.4f} |")
    lines.append("")
    out_md = os.path.join(C.RESULTS_DIR, "comparison_table.md")
    with open(out_md, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"wrote {os.path.relpath(out_md, WORKSPACE)}")

    print("\n".join(lines))
    print(f"\nfairness: sha_identical={anchor_ok} metric_l2={metric_ok}")
    if not (anchor_ok and metric_ok):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
