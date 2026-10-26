"""Experiment E2 (F39): what replaces the cold start for LanceDB on Lambda MicroVMs.

Two parts, distinct query vectors per sample (the C2 rule):
  launch : for each delay d in --delays, --per-delay launches. Each launch waits d s after RUNNING, sends
           the first query, then 4 more, and terminates. A launch reads the image snapshot; AWS keeps
           prefetching after RUNNING (finding 29), so the first-query cost is reported per delay.
  resume : one MicroVM with an idle policy of --idle-suspend s. --cycles times: wait until AWS suspends it
           for idleness (time recorded), stay suspended --suspended s, send a query (auto-resume), then 4
           more. This is the state a user meets after a quiet period (C2's idle-cold).
Each record carries wall_clock_s, server_ms, cpu_part, the resumed flag, submit/ack UTC and the MicroVM id.
Expected cost (finding 29 §Next): at 1M about $0.15; at 10M (8 GB) about $0.9. Above the $0.50 rule, so the
10M run needs --i-have-approval.

    RESULTS_TAG=2026-10-mvm AWS_DEFAULT_REGION=us-east-1 harness/.venv/bin/python \
        experiments/experiment_c2/e2_microvm.py --dataset deep_1m
Writes experiments/experiment_c2/results_<tag>/e2_lancedb_microvm_<dataset>.{jsonl,manifest.json}.
"""
import argparse
import json
import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
sys.path.insert(0, ROOT)
from harness.adapters.lancedb_microvm import build_for_dataset  # noqa: E402
import boto3

ACCT = boto3.client("sts").get_caller_identity()["Account"]


def launch(make, manifest, label):
    """Launch one MicroVM; on a launch that AWS terminates before RUNNING, record it and retry once after 60 s."""
    for attempt in (1, 2):
        c = make()
        try:
            c.setup(manifest["dataset"], dimensions=96)
            return c
        except RuntimeError as e:
            reason = None
            try:
                reason = c._mvm.get_microvm(microvmIdentifier=c.microvm_id).get("stateReason")
            except Exception:  # noqa: BLE001
                pass
            manifest["failed_launches"].append({"label": label, "attempt": attempt, "microvm_id": c.microvm_id,
                                                "error": str(e)[:300], "state_reason": reason,
                                                "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
            print(f"launch {label} attempt {attempt} failed: {e} ({reason})", flush=True)
            c.teardown()
            if attempt == 2:
                raise
            time.sleep(60)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="deep_1m", choices=["deep_1m", "deep_10m"])
    ap.add_argument("--delays", default="0,30,60")
    ap.add_argument("--per-delay", type=int, default=3)
    ap.add_argument("--cycles", type=int, default=10)
    ap.add_argument("--idle-suspend", type=int, default=60)
    ap.add_argument("--suspended", type=int, default=30)
    ap.add_argument("--parts", default="launch,resume")
    ap.add_argument("--gap", type=float, default=30.0, help="seconds between a terminate and the next launch")
    ap.add_argument("--i-have-approval", action="store_true", help="required for deep_10m (cost > $0.50)")
    ap.add_argument("--resume-settle", type=float, default=60.0,
                    help="seconds before the resume part's first query; 0 at 10M, whose first query waits minutes on the "
                         "lazy snapshot restore (finding 31). With a 60 s settle the 22 Oct MicroVM was terminated on a "
                         "suspend-hook timeout, a defect of the 21 Oct image build (finding 32), and the query got HTTP 502")
    ap.add_argument("--image", default=None, help="image name override, e.g. thesis-lancedb-mvm-deep10m-fresh (finding 32)")
    ap.add_argument("--out-suffix", default="", help="e.g. _resume, so a rerun of one part keeps the other's files")
    a = ap.parse_args()
    if a.dataset == "deep_10m" and not a.i_have_approval:
        sys.exit("deep_10m E2 costs about $0.9 (> $0.50): ask the user, then pass --i-have-approval")
    tag = os.environ.get("RESULTS_TAG")
    if not tag or tag == "orig":
        sys.exit("set RESULTS_TAG (not orig)")
    out_dir = os.path.join(_HERE, f"results_{tag}")
    os.makedirs(out_dir, exist_ok=True)
    stem = os.path.join(out_dir, f"e2_lancedb_microvm_{a.dataset}{a.out_suffix}")
    csv = os.path.join(ROOT, "datasets", a.dataset, f"vectors_{a.dataset}.csv")
    qs = [[float(x) for x in l.split()] for l in open(os.path.join(ROOT, "datasets", a.dataset,
                                                                     f"queries_{a.dataset}_1000q.csv"))]
    qi = iter(range(len(qs)))  # distinct vectors across the whole run
    manifest = {"started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "dataset": a.dataset,
                "protocol": vars(a), "launches": [], "resume": None, "failed_launches": [], "failed_cycles": [],
                "launch_transitions": []}
    jl = open(f"{stem}.jsonl", "a")

    def query_and_log(c, config, cycle, extra=None):
        i = next(qi)
        t_sub = time.time()
        _, tm = c.query(qs[i])
        rec = {"adapter": "lancedb-microvm", "config": config, "cycle": cycle, "query_index": i,
               "wall_submit_utc": t_sub, "wall_ack_utc": time.time(), "microvm_id": c.microvm_id, **tm,
               **(extra or {})}
        jl.write(json.dumps(rec, default=str) + "\n")
        jl.flush()
        return rec

    try:
        run_parts(a, manifest, csv, query_and_log)
    finally:
        jl.close()
        manifest["ended_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        json.dump(manifest, open(f"{stem}.manifest.json", "w"), indent=1, default=str)
        print("wrote", f"{stem}.jsonl", flush=True)


def run_parts(a, manifest, csv, query_and_log):
    if "launch" in a.parts:
        for d in [float(x) for x in a.delays.split(",")]:
            for k in range(a.per_delay):
                time.sleep(a.gap)  # let the previous MicroVM's memory leave the quota (22 Oct: a launch 1 s after
                #                    a terminate ended TERMINATED with "Internal service error.")
                c = launch(lambda: build_for_dataset(a.dataset, csv, ACCT, settle_s=0.0, image_name=a.image,
                                                     max_lifetime_s=1800),
                           manifest, f"launch_d{int(d)}_k{k}")
                try:
                    time.sleep(d)
                    first = query_and_log(c, f"launch_d{int(d)}", k, {"first": True,
                                          "run_to_running_s": c.lifecycle_record.get("run_to_running_s")})
                    rest = [query_and_log(c, f"launch_d{int(d)}_next", k) for _ in range(4)]
                    manifest["launches"].append({"delay_s": d, "k": k, "backend_version": c.backend_version(),
                                                 "lifecycle": c.lifecycle_record})
                    print(f"launch d={d:.0f} k={k}: first {first['wall_clock_s']:.3f}s "
                          f"next median {sorted(r['wall_clock_s'] for r in rest)[2]:.3f}s {first['cpu_part']}", flush=True)
                finally:
                    c.teardown()
                    manifest["launch_transitions"].append(c.transition_record)

    if "resume" in a.parts:
        time.sleep(a.gap)
        c = launch(lambda: build_for_dataset(a.dataset, csv, ACCT, settle_s=a.resume_settle, image_name=a.image,
                                             idle_suspend_s=a.idle_suspend,
                                             suspended_ttl_s=1800, max_lifetime_s=4 * 3600), manifest, "resume")
        try:
            for attempt in range(3):  # the first query waits for the table load; retry if it lands on a suspend
                try:
                    query_and_log(c, "resume_warm_before", -1, {"attempt": attempt})
                    break
                except RuntimeError as e:
                    print(f"resume warm-up attempt {attempt} failed: {e}", flush=True)
                    if attempt == 2:
                        raise
                    time.sleep(30)
            for k in range(a.cycles):
                t_idle = time.time()
                while c._state() != "SUSPENDED":
                    if time.time() - t_idle > a.idle_suspend + 600:
                        raise RuntimeError("MicroVM did not suspend for idleness")
                    time.sleep(2)
                idle_to_suspended = time.time() - t_idle
                time.sleep(a.suspended)
                try:
                    first = query_and_log(c, "resume", k, {"first": True, "idle_to_suspended_s": idle_to_suspended})
                    rest = [query_and_log(c, "resume_next", k) for _ in range(4)]
                except RuntimeError as e:  # e.g. AWS terminated the MicroVM on a suspend-hook timeout (22 Oct)
                    g = c._mvm.get_microvm(microvmIdentifier=c.microvm_id)
                    manifest["failed_cycles"].append({"cycle": k, "error": str(e)[:300], "state": g.get("state"),
                                                      "state_reason": g.get("stateReason")})
                    print(f"resume k={k} FAILED: {e} ({g.get('state')}: {g.get('stateReason')})", flush=True)
                    break
                print(f"resume k={k}: idle->suspended {idle_to_suspended:.0f}s first {first['wall_clock_s']:.3f}s "
                      f"resumed={first['resumed']} next median {sorted(r['wall_clock_s'] for r in rest)[2]:.3f}s",
                      flush=True)
            manifest["resume"] = {"backend_version": c.backend_version(), "lifecycle": c.lifecycle_record}
        finally:
            c.teardown()
            manifest["resume_transitions"] = c.transition_record


if __name__ == "__main__":
    main()
