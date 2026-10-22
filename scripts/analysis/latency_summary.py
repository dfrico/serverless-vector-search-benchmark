"""Latency per cell with bootstrap 95% CIs, for the reported run and (where it exists) August.

Reads result files only. Sweep cells (B, C3) use the harness latency (submit -> ack); C1 cells use
wall_ack_utc - wall_submit_utc of the measured queries; C2 states use wall_clock_s per sample.
Throughput = ok queries / (last ack - first submit), as the aggregators compute it.

    harness/.venv/bin/python scripts/analysis/latency_summary.py --tag 2026-10-ec2
Writes scripts/analysis/results_<tag>/latency_summary.{json,md}.
"""
from __future__ import annotations

import argparse
import collections
import glob
import json
import os
import random
import re

HERE = os.path.dirname(os.path.abspath(__file__))
EXP = os.path.join(os.path.abspath(os.path.join(HERE, "..", "..")), "experiments")
rng = random.Random(42)


def pct(xs, q):
    xs = sorted(xs)
    k = (len(xs) - 1) * q
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def boot_ci(xs, q, b=5000):
    n = len(xs)
    vals = [pct([xs[rng.randrange(n)] for _ in range(n)], q) for _ in range(b)]
    return [pct(vals, 0.025), pct(vals, 0.975)]


def summarise(lats, span=None):
    d = dict(n=len(lats), p50=pct(lats, .5), p50_ci=boot_ci(lats, .5),
             p95=pct(lats, .95), p95_ci=boot_ci(lats, .95), p99=pct(lats, .99), max=max(lats))
    if span:
        d["ops_s"] = len(lats) / span
    return d


def sweep_cells(results_dir, exp):
    out = {}
    for f in sorted(glob.glob(os.path.join(results_dir, "sweep_*_c*.jsonl"))):
        m = re.match(r"sweep_(\w+?)_c(\d+)\.jsonl", os.path.basename(f))
        recs = [json.loads(l) for l in open(f) if l.strip()]
        recs = [r for r in recs if r.get("op") == "query" and r.get("ok")]
        span = max(r["ack_ts"] for r in recs) - min(r["submit_ts"] for r in recs)
        out[f"{exp}/{m[1]}_c{m[2]}"] = summarise([r["latency_s"] for r in recs], span)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="2026-10-ec2")
    args = ap.parse_args()
    res = {}
    for run, sub in (("oct", f"results_{args.tag}"), ("aug", "results")):
        r = {}
        for exp in ("experiment_b", "experiment_c3"):
            r.update(sweep_cells(os.path.join(EXP, exp, sub), exp[11:]))
        if run == "oct":
            for f in sorted(glob.glob(os.path.join(EXP, "experiment_c1", sub, "measure_*.raw.json"))):
                recs = [x for x in json.load(open(f))["records"] if x["phase"] == "measured"]
                lats = [x["wall_ack_utc"] - x["wall_submit_utc"] for x in recs]
                span = max(x["wall_ack_utc"] for x in recs) - min(x["wall_submit_utc"] for x in recs)
                r[f"c1/{os.path.basename(f)[8:-9]}"] = summarise(lats, span)
            for f in sorted(glob.glob(os.path.join(EXP, "experiment_c2", sub, "cold_*.jsonl"))):
                by = collections.defaultdict(list)
                for line in open(f):
                    x = json.loads(line)
                    by[f"c2/{x['adapter']}_{x['config']}"].append(x["wall_clock_s"])
                for k, v in by.items():
                    r[k] = summarise(v)
        res[run] = r
    od = os.path.join(HERE, f"results_{args.tag}")
    os.makedirs(od, exist_ok=True)
    json.dump(res, open(os.path.join(od, "latency_summary.json"), "w"), indent=1)
    with open(os.path.join(od, "latency_summary.md"), "w") as f:
        f.write(f"# Latency per cell ({args.tag}; August for comparison)\n\nBootstrap 95% CI, 5000 "
                "resamples, seed 42. Seconds.\n\n| cell | run | n | p50 [CI] | p95 [CI] | p99 | ops/s |\n"
                "|---|---|---|---|---|---|---|\n")
        for k in res["oct"]:
            for run in ("oct", "aug"):
                d = res[run].get(k)
                if not d:
                    continue
                f.write(f"| {k} | {run} | {d['n']} | {d['p50']:.3g} [{d['p50_ci'][0]:.3g}, "
                        f"{d['p50_ci'][1]:.3g}] | {d['p95']:.3g} [{d['p95_ci'][0]:.3g}, "
                        f"{d['p95_ci'][1]:.3g}] | {d['p99']:.3g} | "
                        f"{d.get('ops_s', float('nan')):.3g} |\n")
    print(open(os.path.join(od, "latency_summary.md")).read())


if __name__ == "__main__":
    main()
