"""E3 check: rebuild one UTC day's MicroVM usage from our records and set it beside Cost Explorer (F39).

Inventory: every MicroVM log stream of the experiment images that day (one stream per MicroVM). Lifetime: the
control-plane startedAt/terminatedAt our adapter saved in the result files (any JSON under the MicroVM result
folders holding microvm_id + started_at + terminated_at); where no record exists, the stream's first and last
event. Suspended intervals: the server's "hook suspend" -> next "hook resume" log lines (or terminate).

Model, per MicroVM (finding 29): memory GB-s = baseline GB x running s; baseline vCPU-s = baseline GB / 2 x
running s. Cost Explorer's vCPU-s above that baseline is the billed burst, set beside the sum of the guests' busy CPU
where the teardown stats exist. That sum is an upper bound for the running time: the guest counters are restored from
the snapshot, so they include the image build's CPU (review of 24 Oct; the earlier per-MicroVM clipped excess,
guest_excess_s_where_measured, is kept only for the record and is not comparable with the billed burst). Snapshot GB written and read are compared with the per-suspend and
per-resume sizes measured at 1M on 20 Oct (finding 29); the 10M launch read is derived from the remainder.
Reads Cost Explorer only through scripts/microvm/results/ce_<day>.json (ce_readout.py), so this makes no paid calls.

    AWS_PROFILE=thesis harness/.venv/bin/python scripts/microvm/e3_reconcile.py 2026-10-22
Writes scripts/microvm/results/e3_reconcile_<day>.json.
"""
import datetime as dt
import glob
import json
import os
import sys

import boto3

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUT = os.path.join(ROOT, "scripts", "microvm", "results")
IMAGES = {"thesis-lancedb-mvm-deep1m": 2.0, "thesis-lancedb-mvm-deep10m": 8.0}  # baseline GB
# finding 29, 20 Oct readout at 1M: GB written per suspend, GB read per resume, GB read per launch (19 Oct)
W_SUSPEND_1M = 7.8760147095 / 5            # 20 Oct: five suspends
R_LAUNCH_1M = 2.96                          # 19 Oct: one launch
R_RESUME_1M = (10.6811904895 - R_LAUNCH_1M) / 5  # 20 Oct: one launch + five resumes


def ts(s):
    return dt.datetime.fromisoformat(str(s)).timestamp()


def our_records():
    """microvm_id -> (started, terminated, stats) from every result JSON that holds them."""
    recs = {}

    def walk(o):
        if isinstance(o, dict):
            if o.get("microvm_id") and o.get("started_at") and o.get("terminated_at"):
                recs[o["microvm_id"]] = (ts(o["started_at"]), ts(o["terminated_at"]), o.get("stats_at_teardown"))
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)

    pats = ["experiments/*/results_2026-10-mvm/**/*.json", "scripts/microvm/results/*.json"]
    for p in pats:
        for f in glob.glob(os.path.join(ROOT, p), recursive=True):
            try:
                walk(json.load(open(f)))
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
    return recs


def main(day):
    d0 = dt.datetime.fromisoformat(day).replace(tzinfo=dt.timezone.utc).timestamp()
    d1 = d0 + 86400
    logs = boto3.client("logs", region_name="us-east-1")
    recs = our_records()
    vms = []
    for image, gb in IMAGES.items():
        group = f"/aws/lambda-microvms/{image}"
        pref = day.replace("-", "/")
        for page in logs.get_paginator("describe_log_streams").paginate(logGroupName=group, logStreamNamePrefix=pref):
            for s in page["logStreams"]:
                mid = s["logStreamName"].split("]", 1)[1]
                hooks = []
                for p in logs.get_paginator("filter_log_events").paginate(
                        logGroupName=group, logStreamNames=[s["logStreamName"]], filterPattern='"hook"'):
                    hooks += [(e["timestamp"] / 1000, e["message"].split()[1]) for e in p["events"]
                              if e["message"].startswith("hook ")]
                hooks.sort()
                if mid in recs:
                    start, end, stats = recs[mid]
                    src = "result record"
                else:
                    start, end, stats = s["firstEventTimestamp"] / 1000, s["lastEventTimestamp"] / 1000, None
                    src = "log stream bounds"
                suspended, t_susp = 0.0, None
                for t, h in hooks:
                    if h == "suspend":
                        t_susp = t
                    elif h == "resume" and t_susp is not None:
                        suspended += t - t_susp
                        t_susp = None
                if t_susp is not None:
                    suspended += end - t_susp
                running = (min(end, d1) - max(start, d0)) - suspended
                n = {h: sum(1 for _, x in hooks if x == h) for h in ("run", "suspend", "resume")}
                vms.append({"microvm_id": mid, "image": image, "baseline_gb": gb, "source": src,
                            "started": start, "terminated": end, "lifetime_s": round(end - start, 1),
                            "suspended_s": round(suspended, 1), "running_s": round(running, 1), **n,
                            "guest_busy_cpu_s": (stats or {}).get("host_busy_cpu_s"),
                            "guest_uptime_s": (stats or {}).get("uptime_s")})
    ce = {r["usage_type"]: r["quantity"] for r in json.load(open(os.path.join(OUT, f"ce_{day}.json")))["rows"]
          if r["usage_type"].startswith("Lambda-MicroVM")}
    mem = sum(v["baseline_gb"] * v["running_s"] for v in vms)
    vcpu_base = sum(v["baseline_gb"] / 2 * v["running_s"] for v in vms)
    ce_mem, ce_vcpu = ce["Lambda-MicroVM-Memory-GB-Second-ARM"], ce["Lambda-MicroVM-vCPU-Second-ARM"]
    by = {im: [v for v in vms if v["image"] == im] for im in IMAGES}
    cnt = {im: {k: sum(v[k] for v in by[im]) for k in ("run", "suspend", "resume")} | {"microvms": len(by[im])}
           for im in IMAGES}
    guest = [v for v in vms if v["guest_busy_cpu_s"] is not None]
    guest_excess = sum(max(0.0, v["guest_busy_cpu_s"] - v["baseline_gb"] / 2 * v["guest_uptime_s"]) for v in guest)
    w1 = r1 = cnt["thesis-lancedb-mvm-deep1m"]
    write_1m = w1["suspend"] * W_SUSPEND_1M
    read_1m = r1["run"] * R_LAUNCH_1M + r1["resume"] * R_RESUME_1M
    c10 = cnt["thesis-lancedb-mvm-deep10m"]
    rec = {"day": day, "microvms": vms, "counts": cnt,
           "model": {"memory_gb_s": round(mem, 1), "vcpu_baseline_s": round(vcpu_base, 1),
                     "snapshot_write_gb_1m": round(write_1m, 2), "snapshot_read_gb_1m": round(read_1m, 2)},
           "cost_explorer": ce,
           "compare": {"memory_ratio_ce_model": round(ce_mem / mem, 4),
                       "vcpu_burst_billed_s": round(ce_vcpu - vcpu_base, 1),
                       "guest_excess_s_where_measured": round(guest_excess, 1),
                       "guest_measured_microvms": len(guest),
                       "guest_busy_cpu_s_sum": round(sum(v["guest_busy_cpu_s"] for v in guest), 1),
                       "guest_baseline_vcpu_s_running": round(sum(v["baseline_gb"] / 2 * v["running_s"] for v in guest), 1),
                       "vcpu_burst_share_of_baseline": round((ce_vcpu - vcpu_base) / vcpu_base, 4),
                       "snapshot_write_ce_minus_1m_model_gb": round(ce["Lambda-MicroVM-Snapshot-Write-GB"] - write_1m, 2),
                       "snapshot_read_remainder_per_10m_launch_gb":
                           round((ce["Lambda-MicroVM-Snapshot-Read-GB"] - read_1m) / max(1, c10["run"]), 2)},
           "sources": {"lifetime": {s: sum(1 for v in vms if v["source"] == s) for s in ("result record", "log stream bounds")}}}
    path = os.path.join(OUT, f"e3_reconcile_{day}.json")
    json.dump(rec, open(path, "w"), indent=1)
    print(json.dumps({k: rec[k] for k in ("counts", "model", "compare", "sources")}, indent=1), "\n->", path)


if __name__ == "__main__":
    main(sys.argv[1])
