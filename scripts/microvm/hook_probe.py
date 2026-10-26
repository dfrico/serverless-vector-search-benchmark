"""Why does the suspend hook time out at 10M? (finding 32, follow-up to finding 31's 10M resume runs.)

Uses trace images (server v5 with TRACE=1, `mvm-v5.zip`): the server logs, on its own clock, when each hook
request reached the handler and whether the reply could be written, and a heartbeat that shows whether the
guest process stopped running (gap > 1 s) plus memory, reclaim, fault and CPU-steal counters.

    build <dataset>       build thesis-lancedb-mvm-<ds>-trace (same settings as the experiment image + TRACE=1)
    run <dataset> [--cycles N] [--ballast-mb MB] [--dropcaches] [--tag T] [--image NAME]
        One MicroVM with the E2 idle policy (60 s, auto-resume): one query (the startup restore), optional
        ballast, then up to N cycles of [5 queries, optional drop_caches, no traffic until AWS suspends it,
        20 s suspended, one query that auto-resumes it]. Stops at the first termination. Always terminates the
        MicroVM, then saves the cycle record, get_microvm's stateReason and the whole log stream.

Writes scripts/microvm/results/hook_probe_<ds>_<tag>_<utc>.json.
"""
from __future__ import annotations

import argparse
import json
import os
import time

import boto3
import requests

ACCT = boto3.client("sts").get_caller_identity()["Account"]
BUCKET = f"thesis-microvm-{ACCT}"
NET = "arn:aws:lambda:us-east-1:aws:network-connector:aws-network-connector:"
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUT = os.path.join(ROOT, "scripts", "microvm", "results")
MEM = {"deep_1m": 2048, "deep_10m": 8192}
c = boto3.client("lambda-microvms", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")


IMAGE = None  # --image: run this image instead of the trace image (control runs on the experiment image)


def name_of(ds):
    return IMAGE or f"thesis-lancedb-mvm-{ds.replace('_', '')}-trace"


def arn_of(ds):
    return f"arn:aws:lambda:us-east-1:{ACCT}:microvm-image:{name_of(ds)}"


def build(ds, trace=True, v4=False):
    """trace=False builds <name>-fresh: the same artifact without TRACE (server behaves as v4), built today.
    v4=True builds <name>-v4today from mvm-v4.zip, the 21 Oct image's exact artifact and settings (26 Oct control:
    separates the build date from the server artifact)."""
    global IMAGE
    if v4:
        IMAGE, trace = f"thesis-lancedb-mvm-{ds.replace('_', '')}-v4today", False
    elif not trace:
        IMAGE = f"thesis-lancedb-mvm-{ds.replace('_', '')}-fresh"
    t0 = time.time()
    c.create_microvm_image(
        name=name_of(ds), baseImageArn="arn:aws:lambda:us-east-1:aws:microvm-image:al2023-1",
        buildRoleArn=f"arn:aws:iam::{ACCT}:role/thesis-microvm-build",
        codeArtifact={"uri": f"s3://{BUCKET}/artifacts/{'mvm-v4.zip' if v4 else 'mvm-v5.zip'}"},
        cpuConfigurations=[{"architecture": "ARM_64"}], resources=[{"minimumMemoryInMiB": MEM[ds]}],
        environmentVariables={"TABLE_URI": f"s3://{BUCKET}/lancedb/{ds}.lance", "AWS_DEFAULT_REGION": "us-east-1",
                              "VALIDATE_MODE": "full", **({"TRACE": "1"} if trace else {})},
        hooks={"port": 8080, "microvmHooks": {"run": "ENABLED", "resume": "ENABLED", "suspend": "ENABLED"},
               "microvmImageHooks": {"ready": "ENABLED", "readyTimeoutInSeconds": 3600,
                                     "validate": "ENABLED", "validateTimeoutInSeconds": 3600}},
        description=f"F39 hook-timeout probe {ds} ({'v4 artifact, control build' if v4 else 'TRACE=1' if trace else 'v4 behaviour, fresh build'})")
    while (s := c.get_microvm_image(imageIdentifier=arn_of(ds))["state"]) == "CREATING":
        time.sleep(15)
    print(json.dumps({"image": name_of(ds), "state": s, "build_s": round(time.time() - t0, 1)}), flush=True)


def query_vec(ds, i):
    with open(os.path.join(ROOT, "datasets", ds, f"queries_{ds}_1000q.csv")) as f:
        for j, line in enumerate(f):
            if j == i:
                return [float(x) for x in line.split()]


def run(a):
    ds = a.dataset
    rec = {"image": name_of(ds), "dataset": ds, "tag": a.tag, "ballast_mb": a.ballast_mb, "dropcaches": a.dropcaches,
           "idle_s": 60, "cycles": [], "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    mid, s, url = None, requests.Session(), None

    def state():
        return c.get_microvm(microvmIdentifier=mid)

    def post(path, body=None, timeout=600):
        t = time.time()
        r = s.post(url + path, json=body, timeout=timeout)
        return {"t": t, "wall_s": round(time.time() - t, 3), "status": r.status_code,
                "body": r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text[:300]}

    def q(i):
        out = post("/query", {"queries": [query_vec(ds, i)], "top_k": 10})
        if isinstance(out["body"], dict):
            out["body"] = {k: out["body"].get(k) for k in ("server_ms", "runs", "resumes", "cpu_part")}
        return out

    try:
        r = c.run_microvm(imageIdentifier=arn_of(ds), executionRoleArn=f"arn:aws:iam::{ACCT}:role/thesis-microvm-exec",
                          ingressNetworkConnectors=[NET + "ALL_INGRESS"], egressNetworkConnectors=[NET + "INTERNET_EGRESS"],
                          idlePolicy={"autoResumeEnabled": True, "maxIdleDurationSeconds": 60,
                                      "suspendedDurationSeconds": 900},
                          maximumDurationInSeconds=3600)
        mid = rec["microvm_id"] = r["microvmId"]
        url = f"https://{r['endpoint']}"
        while (st0 := state())["state"] != "RUNNING":
            if st0["state"] in ("TERMINATING", "TERMINATED"):
                raise RuntimeError(f"{st0['state']} before RUNNING: {st0.get('stateReason')}")
            time.sleep(0.5)
        rec["running_at"] = time.time()
        s.headers["X-aws-proxy-auth"] = c.create_microvm_auth_token(
            microvmIdentifier=mid, expirationInMinutes=60, allowedPorts=[{"port": 8080}])["authToken"]["X-aws-proxy-auth"]
        rec["startup_query"] = q(0)
        print("startup", json.dumps(rec["startup_query"]), flush=True)
        if a.ballast_mb:
            rec["ballast"] = post(f"/debug/alloc?mb={a.ballast_mb}")
            print("ballast", json.dumps(rec["ballast"])[:400], flush=True)
        for k in range(a.cycles):
            cy = {"cycle": k, "queries": [q(1 + 6 * k + j) for j in range(5)]}
            if a.dropcaches:
                cy["dropcaches"] = post("/debug/dropcaches")
            cy["idle_from"] = time.time()
            while (st := state())["state"] == "RUNNING" and time.time() - cy["idle_from"] < 400:
                time.sleep(1)
            cy["left_running_s"] = round(time.time() - cy["idle_from"], 1)
            cy["state"] = st["state"]
            while cy["state"] == "SUSPENDING":
                time.sleep(1)
                cy["state"] = state()["state"]
            cy["settled_s"] = round(time.time() - cy["idle_from"], 1)
            print(f"cycle {k}: {cy['state']} after {cy['left_running_s']} s (settled {cy['settled_s']} s)", flush=True)
            if cy["state"] != "SUSPENDED":
                cy["state_reason"] = state().get("stateReason")
                rec["cycles"].append(cy)
                break
            time.sleep(20)
            cy["resume_query"] = q(1 + 6 * k + 5)
            print(f"  resume query {json.dumps(cy['resume_query'])}", flush=True)
            rec["cycles"].append(cy)
            if cy["resume_query"]["status"] != 200:
                break
    except Exception as e:  # keep what was recorded; the MicroVM is terminated below either way
        rec["error"] = f"{type(e).__name__}: {e}"
        print("ERROR", rec["error"], flush=True)
    finally:
        if mid:
            st = state()
            rec["final_get"] = {k: str(st.get(k)) for k in ("state", "stateReason", "startedAt", "terminatedAt")}
            if st["state"] not in ("TERMINATED", "TERMINATING"):
                c.terminate_microvm(microvmIdentifier=mid)
                t2 = time.time()
                while state()["state"] != "TERMINATED" and time.time() - t2 < 180:
                    time.sleep(2)
            rec["final_state"] = state()["state"]
            time.sleep(30)  # let the last log lines arrive
            group = f"/aws/lambda-microvms/{name_of(ds)}"
            rec["log"] = []
            for page in logs.get_paginator("filter_log_events").paginate(
                    logGroupName=group, logStreamNamePrefix=time.strftime("%Y/%m/%d", time.gmtime(rec["running_at"] if "running_at" in rec else time.time()))):
                rec["log"] += [(e["timestamp"] / 1000, e["message"].rstrip()) for e in page["events"]
                               if e["logStreamName"].endswith(mid)]
    path = os.path.join(OUT, f"hook_probe_{ds}_{a.tag}_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
    json.dump(rec, open(path, "w"), indent=1, default=str)
    print("->", path, flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["build", "run"])
    ap.add_argument("dataset", choices=list(MEM))
    ap.add_argument("--cycles", type=int, default=6)
    ap.add_argument("--ballast-mb", type=int, default=0)
    ap.add_argument("--dropcaches", action="store_true")
    ap.add_argument("--tag", default="plain")
    ap.add_argument("--no-trace", action="store_true", help="build: <name>-fresh without TRACE")
    ap.add_argument("--v4", action="store_true", help="build: <name>-v4today from mvm-v4.zip (control)")
    ap.add_argument("--image", default=None, help="e.g. thesis-lancedb-mvm-deep10m (no trace lines, no /debug)")
    a = ap.parse_args()
    global IMAGE
    IMAGE = a.image
    build(a.dataset, trace=not a.no_trace, v4=a.v4) if a.cmd == "build" else run(a)


if __name__ == "__main__":
    main()
