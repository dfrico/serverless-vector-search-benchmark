"""Experiment E3 validation trace (F39): low-rate Poisson traffic on one LanceDB MicroVM with auto-suspend.

One MicroVM (1M image, 2 GB) with an idle policy of --idle-suspend s receives queries at Poisson arrivals of
--rate per hour for --duration s (seeded). Each query records wall/server time, the resumed flag and the
MicroVM state just before it. At teardown the adapter's transition record (run/resume/suspend hook times,
control-plane startedAt/terminatedAt) is saved, so the cost model can count running seconds, suspends and
resumes. The model's cost for the trace is then checked against Cost Explorer for the same UTC day (which
also holds the day's E1/E2 MicroVMs; their lifecycle records are summed the same way).
Expected cost: 2 h of a 2 GB MicroVM at most ~$0.25, plus ~1.6 GB snapshot written per suspend (~$0.006).

    RESULTS_TAG=2026-10-mvm AWS_DEFAULT_REGION=us-east-1 harness/.venv/bin/python \
        experiments/experiment_c2/e3_trace_microvm.py
Writes experiments/experiment_c2/results_<tag>/e3_trace_lancedb_microvm_<dataset>.{jsonl,manifest.json,lifecycle.json}.
"""
import argparse
import json
import os
import random
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
sys.path.insert(0, ROOT)
from harness.adapters.lancedb_microvm import build_for_dataset  # noqa: E402
import boto3  # noqa: E402

ACCT = boto3.client("sts").get_caller_identity()["Account"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="deep_1m", choices=["deep_1m"])
    ap.add_argument("--rate", type=float, default=60.0, help="queries per hour (Poisson)")
    ap.add_argument("--duration", type=float, default=7200.0)
    ap.add_argument("--idle-suspend", type=int, default=60)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--max-relaunch", type=int, default=10)
    a = ap.parse_args()
    tag = os.environ.get("RESULTS_TAG")
    if not tag or tag == "orig":
        sys.exit("set RESULTS_TAG (not orig)")
    out_dir = os.path.join(_HERE, f"results_{tag}")
    os.makedirs(out_dir, exist_ok=True)
    stem = os.path.join(out_dir, f"e3_trace_lancedb_microvm_{a.dataset}")
    csv = os.path.join(ROOT, "datasets", a.dataset, f"vectors_{a.dataset}.csv")
    qs = [[float(x) for x in l.split()] for l in open(os.path.join(ROOT, "datasets", a.dataset,
                                                                     f"queries_{a.dataset}_1000q.csv"))]
    rng = random.Random(a.seed)
    arrivals, t = [], 0.0
    while True:
        t += rng.expovariate(a.rate / 3600.0)
        if t >= a.duration:
            break
        arrivals.append(t)
    manifest = {"started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "dataset": a.dataset,
                "protocol": vars(a), "n_arrivals": len(arrivals)}
    def make(settle):
        return build_for_dataset(a.dataset, csv, ACCT, settle_s=settle, idle_suspend_s=a.idle_suspend,
                                 suspended_ttl_s=3600, max_lifetime_s=int(a.duration) + 3600)

    manifest["failures"], manifest["transitions"] = [], []
    c = make(60.0)
    jl = open(f"{stem}.jsonl", "w")
    try:
        c.setup(a.dataset, dimensions=96)
        t0 = time.time()
        manifest["trace_start_utc"] = t0
        for i, at in enumerate(arrivals):
            time.sleep(max(0.0, t0 + at - time.time()))
            state = c._state()
            t_sub = time.time()
            relaunched = False
            try:
                _, tm = c.query(qs[i % len(qs)])
            except RuntimeError as e:
                # 22 Oct: AWS terminated the MicroVM when its suspend hook timed out, and the next query got
                # HTTP 502. Record it, launch a new MicroVM (no settle: the user is waiting) and re-send.
                g = c._mvm.get_microvm(microvmIdentifier=c.microvm_id)
                manifest["failures"].append({"i": i, "error": str(e)[:300], "microvm_id": c.microvm_id,
                                             "state": g.get("state"), "state_reason": g.get("stateReason"),
                                             "utc": time.time()})
                print(f"q{i} FAILED: {e} ({g.get('state')}: {g.get('stateReason')})", flush=True)
                if len(manifest["failures"]) > a.max_relaunch:
                    raise
                c.teardown()
                manifest["transitions"].append(c.transition_record)
                c = make(0.0)
                c.setup(a.dataset, dimensions=96)
                _, tm = c.query(qs[i % len(qs)])
                relaunched = True
            rec = {"adapter": "lancedb-microvm", "config": "e3_trace", "i": i, "arrival_s": at,
                   "state_before": state, "wall_submit_utc": t_sub, "wall_ack_utc": time.time(),
                   "user_wait_s": time.time() - t_sub, "relaunched": relaunched,
                   "microvm_id": c.microvm_id, **tm}
            jl.write(json.dumps(rec, default=str) + "\n")
            jl.flush()
            print(f"q{i} t={at:.0f}s state={state} wait={rec['user_wait_s']:.3f}s "
                  f"resumed={tm.get('resumed')}", flush=True)
        manifest["trace_end_utc"] = time.time()
        manifest["backend_version"] = c.backend_version()
        manifest["lifecycle"] = c.lifecycle_record
    finally:
        c.teardown()
        jl.close()
        manifest["transitions"].append(c.transition_record)
        json.dump(c.transition_record, open(f"{stem}.lifecycle.json", "w"), indent=1, default=str)
        manifest["ended_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        json.dump(manifest, open(f"{stem}.manifest.json", "w"), indent=1, default=str)
    print("wrote", f"{stem}.jsonl", flush=True)


if __name__ == "__main__":
    main()
