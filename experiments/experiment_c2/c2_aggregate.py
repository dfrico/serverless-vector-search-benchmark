"""
Experiment C2, Phase 3 — aggregate the cold-start samples into a table.

Reads results/cold_<adapter>.jsonl for the three adapters and results/
phase1_<adapter>.json for the recall gate. Produces, per (adapter, config):
n, wall-latency min / median / p95 / max, median billed cost, and the
adapter-specific cold signal:
  - Blocks : median map-phase spin-up latency (0_map_invocation_blocks max per
             query). A cold map worker spends its container init here, so this
             is the Blocks cold proxy (there is no per-task cold flag).
  - LanceDB: count of cold=True vs cold=False (the handler reports it per query).
  - S3 Vec : none (managed service, no container).

Writes results/cold_start_table.md and results/cold_start_table.json. The
headline compares idle_cold vs warm median wall latency per adapter (the
cold-start penalty a first-query-after-idle user would feel). The three
interpretation questions from the brief are answered in findings/24.

Run via c2_aws.sh (any venv; pure stdlib + local files, no AWS calls).
"""
from __future__ import annotations

import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
RESULTS_DIR = os.path.join(_HERE, "results")

ADAPTERS = ["blocks", "s3vectors", "lancedb"]
CONFIGS = ["abs_cold", "idle_cold", "warm"]


def percentiles(values):
    if not values:
        return {"min": None, "median": None, "p95": None, "max": None}
    xs = sorted(values)
    n = len(xs)

    def pct(p):
        if n == 1:
            return xs[0]
        rank = p / 100.0 * (n - 1)
        lo = int(rank)
        hi = min(lo + 1, n - 1)
        return xs[lo] + (xs[hi] - xs[lo]) * (rank - lo)

    return {"min": xs[0], "median": pct(50), "p95": pct(95), "max": xs[-1]}


def median(values):
    return percentiles(values)["median"] if values else None


def load_samples(adapter):
    path = os.path.join(RESULTS_DIR, f"cold_{adapter}.jsonl")
    if not os.path.exists(path):
        return []
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def cold_signal(adapter, recs):
    """Adapter-specific cold shape for one (adapter, config) group."""
    if adapter == "blocks":
        vals = [r["map_invocation_max_s"] for r in recs
                if r.get("map_invocation_max_s") is not None]
        return {"kind": "map_invocation_max_s_median",
                "value": round(median(vals), 4) if vals else None}
    if adapter == "lancedb":
        n_cold = sum(1 for r in recs if r.get("cold") is True)
        n_warm = sum(1 for r in recs if r.get("cold") is False)
        return {"kind": "cold_flag", "cold_true": n_cold, "cold_false": n_warm}
    return {"kind": "none"}


def main():
    rows = []
    per_adapter_medians = {}
    for adapter in ADAPTERS:
        samples = load_samples(adapter)
        per_adapter_medians[adapter] = {}
        for config in CONFIGS:
            recs = [r for r in samples if r.get("config") == config]
            walls = [r["wall_clock_s"] for r in recs]
            billed = [r["billed_cost_usd"] for r in recs]
            p = percentiles(walls)
            per_adapter_medians[adapter][config] = p["median"]
            rows.append({
                "adapter": adapter,
                "config": config,
                "n": len(recs),
                "wall_min_s": round(p["min"], 4) if p["min"] is not None else None,
                "wall_median_s": round(p["median"], 4) if p["median"] is not None else None,
                "wall_p95_s": round(p["p95"], 4) if p["p95"] is not None else None,
                "wall_max_s": round(p["max"], 4) if p["max"] is not None else None,
                "billed_median_usd": round(median(billed), 8) if billed else None,
                "cold_signal": cold_signal(adapter, recs),
            })

    # recall gate summary
    recall = {}
    for adapter in ADAPTERS:
        rp = os.path.join(RESULTS_DIR, f"phase1_{adapter}.json")
        if os.path.exists(rp):
            with open(rp) as f:
                d = json.load(f)
            recall[adapter] = {
                "recall_at_10_pct": d.get("recall_at_10_pct"),
                "threshold_pct": d.get("recall_threshold_pct"),
                "passed": d.get("passed"),
                "ingest_cost_usd": d.get("ingest_cost_usd"),
            }

    # headline: idle_cold vs warm median wall, and the cold penalty ratio.
    headline = {}
    for adapter in ADAPTERS:
        m = per_adapter_medians[adapter]
        idle = m.get("idle_cold")
        warm = m.get("warm")
        absc = m.get("abs_cold")
        penalty = (idle / warm) if (idle and warm) else None
        headline[adapter] = {
            "abs_cold_median_s": round(absc, 4) if absc is not None else None,
            "idle_cold_median_s": round(idle, 4) if idle is not None else None,
            "warm_median_s": round(warm, 4) if warm is not None else None,
            "idle_over_warm_ratio": round(penalty, 3) if penalty else None,
        }

    table = {"rows": rows, "recall_gate": recall, "headline": headline}
    with open(os.path.join(RESULTS_DIR, "cold_start_table.json"), "w") as f:
        json.dump(table, f, indent=2, sort_keys=True)

    # --- markdown ---
    L = []
    L.append("# Experiment C2 — cold-start table (deep_1m, 1M, top_k=10)")
    L.append("")
    L.append("Wall latency is the client-side round trip, in seconds — the "
             "primary cold-start metric. Billed cost is the honest per-query "
             "Lambda charge (finding 22 model); for a cold query the container "
             "init is mostly outside the billed handler time, so billed moves "
             "far less than wall.")
    L.append("")
    L.append("## Recall gate (Phase 1)")
    L.append("")
    L.append("| adapter | recall@10 | bar | pass | ingest cost $ |")
    L.append("|---|---|---|---|---|")
    for adapter in ADAPTERS:
        r = recall.get(adapter)
        if r:
            L.append(f"| {adapter} | {r['recall_at_10_pct']}% | "
                     f"{r['threshold_pct']}% | {'yes' if r['passed'] else 'NO'} | "
                     f"{r['ingest_cost_usd']} |")
        else:
            L.append(f"| {adapter} | (missing) | | | |")
    L.append("")
    L.append("## Cold-start table")
    L.append("")
    L.append("| adapter | config | n | wall min s | wall median s | wall p95 s "
             "| wall max s | billed median $ | cold signal |")
    L.append("|" + "---|" * 9)
    for row in rows:
        cs = row["cold_signal"]
        if cs["kind"] == "cold_flag":
            csig = f"cold True/False = {cs['cold_true']}/{cs['cold_false']}"
        elif cs["kind"] == "map_invocation_max_s_median":
            csig = f"map spin-up median {cs['value']}s"
        else:
            csig = "none (managed)"
        L.append(f"| {row['adapter']} | {row['config']} | {row['n']} | "
                 f"{row['wall_min_s']} | {row['wall_median_s']} | "
                 f"{row['wall_p95_s']} | {row['wall_max_s']} | "
                 f"{row['billed_median_usd']} | {csig} |")
    L.append("")
    L.append("## Headline: idle-cold vs warm median wall latency")
    L.append("")
    L.append("| adapter | abs_cold median s | idle_cold median s | warm median s "
             "| idle/warm ratio |")
    L.append("|---|---|---|---|---|")
    for adapter in ADAPTERS:
        h = headline[adapter]
        L.append(f"| {adapter} | {h['abs_cold_median_s']} | "
                 f"{h['idle_cold_median_s']} | {h['warm_median_s']} | "
                 f"{h['idle_over_warm_ratio']} |")
    L.append("")
    with open(os.path.join(RESULTS_DIR, "cold_start_table.md"), "w") as f:
        f.write("\n".join(L) + "\n")

    print("\n".join(L))
    print("\nwrote results/cold_start_table.md + .json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
