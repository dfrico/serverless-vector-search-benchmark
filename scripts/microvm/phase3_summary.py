"""Summarise F39 Phase 3 (E1 warm latency, E2 lifecycle, E3 trace) from the result files of one tag.

E1: sweep cells of experiment B (1M) and C3 (10M) for lancedb_microvm: n, p50/p95 with bootstrap 95% CIs (the
latency_summary.py method), throughput, median server time and endpoint overhead (wall - server), errors, chipset.
E2: per configuration (launch at delay d / the queries after it / auto-resume / after it): wall_clock_s summary, plus
RUNNING time and time to auto-suspend. E3 trace: queries by state before the query, resumed count, latency split.
Lifecycle: per MicroVM, control-plane running span (startedAt -> terminatedAt) and suspend/resume hook counts, so
Cost Explorer for the day can be set beside the sum (`scripts/microvm/ce_readout.py <day>`).

    harness/.venv/bin/python scripts/microvm/phase3_summary.py --tag 2026-10-mvm
Writes scripts/microvm/results/phase3_summary_<tag>.{json,md}.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import glob
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "scripts", "analysis"))
from latency_summary import pct, summarise  # noqa: E402

EXP = os.path.join(ROOT, "experiments")


def ts(s):
    return dt.datetime.fromisoformat(str(s)).timestamp()


def e1(tag):
    out = {}
    for exp, size in (("experiment_b", "1m"), ("experiment_c3", "10m")):
        for f in sorted(glob.glob(os.path.join(EXP, exp, f"results_{tag}", "sweep_lancedb_microvm_c*.jsonl"))):
            c = int(os.path.basename(f).split("_c")[-1].split(".")[0])
            recs = [json.loads(l) for l in open(f) if l.strip()]
            q = [r for r in recs if r.get("op") == "query"]
            ok = [r for r in q if r.get("ok")]
            span = max(r["ack_ts"] for r in ok) - min(r["submit_ts"] for r in ok)
            d = summarise([r["latency_s"] for r in ok], span)
            srv = [r["adapter_timings"]["server_ms"] / 1000 for r in ok]
            ovh = [r["adapter_timings"]["wall_clock_s"] - r["adapter_timings"]["server_ms"] / 1000 for r in ok]
            errs = collections.Counter((r.get("error") or "")[:40] for r in q if not r.get("ok"))
            parts = collections.Counter(p for r in ok for p in (r["adapter_timings"].get("cpu_part") or []))
            d.update(server_p50=pct(srv, .5), overhead_p50=pct(ovh, .5), errors=dict(errs), cpu_part=dict(parts),
                     n_first_after_launch=sum(1 for r in ok if r["adapter_timings"].get("first_after_launch")))
            out[f"{size}_c{c}"] = d
    return out


def e2(tag):
    out = {}
    for f in sorted(glob.glob(os.path.join(EXP, "experiment_c2", f"results_{tag}", "e2_lancedb_microvm_*.jsonl"))):
        ds = os.path.basename(f)[len("e2_lancedb_microvm_"):-6]
        by = collections.defaultdict(list)
        extra = collections.defaultdict(list)
        for line in open(f):
            r = json.loads(line)
            by[r["config"]].append(r["wall_clock_s"])
            for k in ("run_to_running_s", "idle_to_suspended_s"):
                if r.get(k) is not None:
                    extra[f"{r['config']}.{k}"].append(r[k])
            if r.get("first"):
                extra[f"{r['config']}.resumed"].append(bool(r.get("resumed")))
        out[ds] = {k: summarise(v) if len(v) > 1 else {"n": 1, "value": v[0]} for k, v in by.items()}
        for k, v in extra.items():
            out[ds][k] = (sum(v), len(v)) if isinstance(v[0], bool) else {"n": len(v), "p50": pct(v, .5),
                                                                            "min": min(v), "max": max(v)}
    return out


def e3(tag):
    f = os.path.join(EXP, "experiment_c2", f"results_{tag}", "e3_trace_lancedb_microvm_deep_1m.jsonl")
    if not os.path.exists(f):
        return None
    recs = [json.loads(l) for l in open(f)]
    by = collections.defaultdict(list)
    for r in recs:
        by[r["state_before"]].append(r["wall_clock_s"])
    return {"n": len(recs), "resumed": sum(1 for r in recs if r.get("resumed")),
            "by_state": {k: summarise(v) if len(v) > 1 else {"n": 1, "value": v[0]} for k, v in by.items()}}


def lifecycles(tag):
    rows = []
    paths = glob.glob(os.path.join(EXP, "experiment_*", f"results_{tag}", "*.lifecycle.json"))
    for p in sorted(paths):
        r = json.load(open(p))
        rows.append(dict(src=os.path.relpath(p, ROOT), microvm_id=r.get("microvm_id"),
                         span_s=ts(r["terminated_at"]) - ts(r["started_at"]),
                         n_suspend=len(r.get("suspend_hooks") or []), n_resume=len(r.get("resume_hooks") or [])))
    for p in sorted(glob.glob(os.path.join(EXP, "experiment_c2", f"results_{tag}", "e2_*.manifest.json"))):
        m = json.load(open(p))
        for i, x in enumerate(m.get("launches", []) + ([m["resume"]] if m.get("resume") else [])):
            lc = x.get("lifecycle") or {}
            rows.append(dict(src=f"{os.path.relpath(p, ROOT)}#{i}", microvm_id=lc.get("microvm_id"),
                             lifecycle=lc))
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="2026-10-mvm")
    a = ap.parse_args()
    res = {"tag": a.tag, "e1": e1(a.tag), "e2": e2(a.tag), "e3_trace": e3(a.tag), "lifecycles": lifecycles(a.tag)}
    stem = os.path.join(HERE, "results", f"phase3_summary_{a.tag}")
    json.dump(res, open(f"{stem}.json", "w"), indent=1, default=str)
    lines = ["| cell | n | p50 s [95% CI] | p95 s [95% CI] | q/s | server p50 s | overhead p50 s | errors | chipset |",
             "|---|---|---|---|---|---|---|---|---|"]
    for k, d in res["e1"].items():
        lines.append(f"| {k} | {d['n']} | {d['p50']:.3f} [{d['p50_ci'][0]:.3f}, {d['p50_ci'][1]:.3f}] | "
                     f"{d['p95']:.3f} [{d['p95_ci'][0]:.3f}, {d['p95_ci'][1]:.3f}] | {d['ops_s']:.2f} | "
                     f"{d['server_p50']:.3f} | {d['overhead_p50']:.3f} | {d['errors'] or '-'} | {d['cpu_part']} |")
    open(f"{stem}.md", "w").write("\n".join(lines) + "\n")
    print("\n".join(lines))
    print(json.dumps({"e2": res["e2"], "e3_trace": res["e3_trace"]}, indent=1, default=str)[:6000])
    print("->", stem + ".{json,md}")


if __name__ == "__main__":
    main()
