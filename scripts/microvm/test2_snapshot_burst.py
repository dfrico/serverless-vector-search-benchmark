"""F39 Phase 1 test 2: snapshot GB per launch/resume/suspend and whether burst CPU is billed.

Run on a UTC day with NO other MicroVM activity (no builds, no other MicroVMs), so that day's Cost Explorer
usage for Lambda MicroVMs belongs to this run alone (finding 29). One MicroVM from the 1M image:
  1. launch (1 snapshot read);
  2. idle window of IDLE_S with no requests: baseline only;
  3. query window of QUERY_S at concurrency 1: guest busy CPU before/after, to predict burst vCPU-s;
  4. CYCLES explicit suspends, each followed after SUSPENDED_S by a query that auto-resumes the MicroVM
     (CYCLES snapshot writes and CYCLES reads), plus 4 more queries per cycle (early E2 data);
  5. terminate.
Every state change is timestamped. On the next day, scripts/microvm/ce_readout.py compares the billed usage
with what this file predicts.

    AWS_DEFAULT_REGION=us-east-1 harness/.venv/bin/python scripts/microvm/test2_snapshot_burst.py
Writes scripts/microvm/results/test2_<utc>.json.
"""
import json
import os
import sys
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
from harness.adapters.lancedb_microvm import LanceDBMicroVMClient  # noqa: E402
import boto3

ACCT = boto3.client("sts").get_caller_identity()["Account"]
IDLE_S, QUERY_S, CYCLES, SUSPENDED_S = 600, 600, 5, 30
OUT = os.path.join(ROOT, "scripts", "microvm", "results")


def now():
    return time.time()


def main():
    res = {"started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "protocol":
           {"idle_s": IDLE_S, "query_s": QUERY_S, "cycles": CYCLES, "suspended_s": SUSPENDED_S}, "events": []}

    def ev(name, **kw):
        res["events"].append({"t": now(), "event": name, **kw})
        print(time.strftime("%H:%M:%S"), name, json.dumps(kw, default=str)[:200], flush=True)

    qs = [[float(x) for x in l.split()] for l in open(os.path.join(ROOT, "datasets", "deep_1m", "queries_deep_1m_1000q.csv"))]
    c = LanceDBMicroVMClient(
        "thesis-lancedb-mvm-deep1m", account_id=ACCT, execution_role_arn=f"arn:aws:iam::{ACCT}:role/thesis-microvm-exec",
        source_csv_path=os.path.join(ROOT, "datasets", "deep_1m", "vectors_deep_1m.csv"),
        idle_suspend_s=3600, suspended_ttl_s=1800, max_lifetime_s=2 * 3600, settle_s=0)
    try:
        ev("launch_requested")
        c.setup("deep_1m", dimensions=96)
        res["backend_version"], res["lifecycle"] = c.backend_version(), c.lifecycle_record
        ev("running", **c.lifecycle_record)

        s0 = c.stats()["proc"]
        ev("idle_start", busy_cpu_s=s0["host_busy_cpu_s"])
        time.sleep(IDLE_S)
        s1 = c.stats()["proc"]
        ev("idle_end", busy_cpu_s=s1["host_busy_cpu_s"])
        res["idle"] = {"wall_s": s1["t"] - s0["t"], "busy_cpu_s": s1["host_busy_cpu_s"] - s0["host_busy_cpu_s"]}

        n, t_end, i = 0, now() + QUERY_S, 0
        lat = []
        while now() < t_end:
            _, tm = c.query(qs[i % len(qs)])
            lat.append(tm["server_ms"])
            n, i = n + 1, i + 1
        s2 = c.stats()["proc"]
        ev("query_end", n=n, busy_cpu_s=s2["host_busy_cpu_s"])
        wall = s2["t"] - s1["t"]
        busy = s2["host_busy_cpu_s"] - s1["host_busy_cpu_s"]
        res["query_window"] = {"wall_s": wall, "n": n, "busy_cpu_s": busy, "avg_busy_vcpu": busy / wall,
                               "burst_vcpu_s_predicted": max(0.0, busy - wall * 1.0),
                               "server_ms_median": sorted(lat)[len(lat) // 2], "cpu_part": tm.get("cpu_part")}

        cycles = []
        for k in range(CYCLES):
            t_s = now()
            sus = c.suspend()
            ev("suspended", cycle=k + 1, suspend_s=sus)
            time.sleep(SUSPENDED_S)
            t_q = now()
            _, first = c.query(qs[(k * 7) % len(qs)])
            ev("resumed_by_query", cycle=k + 1, first_wall_s=first["wall_clock_s"], resumed=first["resumed"])
            nxt = [c.query(qs[(k * 7 + j) % len(qs)])[1]["wall_clock_s"] for j in range(1, 5)]
            cycles.append({"suspend_request_t": t_s, "suspend_s": sus, "query_t": t_q, "first": first,
                           "next4_wall_s": nxt})
        res["cycles"] = cycles
    except Exception as e:  # noqa: BLE001
        res["error"] = f"{type(e).__name__}: {e}"
        ev("ERROR", error=res["error"])
    finally:
        mid = c.microvm_id
        res["reconnects"] = c.reconnects
        c.teardown()
        ev("terminated", microvm_id=mid)
        if mid and c._mvm is not None:
            g = c._mvm.get_microvm(microvmIdentifier=mid)
            res["started_at"], res["terminated_at"], res["final_state"] = str(g.get("startedAt")), str(g.get("terminatedAt")), g["state"]
        # What Cost Explorer should show for the day if only this run used MicroVMs.
        susp = sum(cy["query_t"] - cy["suspend_request_t"] - cy["suspend_s"] for cy in res.get("cycles", []))
        first_t = next((e["t"] for e in res["events"] if e["event"] == "running"), None)
        last_t = res["events"][-1]["t"]
        res["prediction"] = {
            "launches": 1, "suspends": len(res.get("cycles", [])), "resumes": len(res.get("cycles", [])),
            "running_s_approx": (last_t - first_t - susp) if first_t else None,
            "baseline_vcpu": 1, "baseline_gb": 2,
            "burst_vcpu_s_predicted": res.get("query_window", {}).get("burst_vcpu_s_predicted"),
        }
        os.makedirs(OUT, exist_ok=True)
        p = os.path.join(OUT, f"test2_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
        json.dump(res, open(p, "w"), indent=1, default=str)
        print("wrote", p, flush=True)


if __name__ == "__main__":
    main()
