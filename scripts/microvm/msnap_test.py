"""F39 Phase 1 test 3: does an M-snap image load the table at build time and keep it in the snapshot?

Builds a MicroVM image whose server copies TABLE_URI to local disk and warms it before /ready,
reads the build-time load report (copy time, objects, bytes, which IAM identity the build had),
then runs one MicroVM and checks that the table is still in memory: page faults and latency of the
first query vs the next ones, and recall@10 on the first N queries against the ground truth.
Always terminates the MicroVM.

    harness/.venv/bin/python scripts/microvm/msnap_test.py --dataset deep_1m --memory 2048 [--skip-build]
Writes scripts/microvm/results/msnap_<dataset>_<utc>.json.
"""
import argparse
import datetime as dt
import json
import os
import statistics
import time

import boto3
import requests

ACCT = boto3.client("sts").get_caller_identity()["Account"]
BUCKET = f"thesis-microvm-{ACCT}"
NET = "arn:aws:lambda:us-east-1:aws:network-connector:aws-network-connector:"
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUT = os.path.join(ROOT, "scripts", "microvm", "results")
c = boto3.client("lambda-microvms", region_name="us-east-1")


def image_arn(name):
    return f"arn:aws:lambda:us-east-1:{ACCT}:microvm-image:{name}"


def build(name, ds, mem, artifact, validate=False, extra=None):
    t0 = time.time()
    c.create_microvm_image(
        name=name, baseImageArn="arn:aws:lambda:us-east-1:aws:microvm-image:al2023-1",
        buildRoleArn=f"arn:aws:iam::{ACCT}:role/thesis-microvm-build",
        codeArtifact={"uri": f"s3://{BUCKET}/artifacts/{artifact}"},
        cpuConfigurations=[{"architecture": "ARM_64"}], resources=[{"minimumMemoryInMiB": mem}],
        environmentVariables={"TABLE_URI": f"s3://{BUCKET}/lancedb/{ds}.lance", "AWS_DEFAULT_REGION": "us-east-1",
                              **(extra or {})},
        hooks={"port": 8080, "microvmHooks": {"run": "ENABLED", "resume": "ENABLED", "suspend": "ENABLED"},
               "microvmImageHooks": {"ready": "ENABLED", "readyTimeoutInSeconds": 3600,
                                     **({"validate": "ENABLED", "validateTimeoutInSeconds": 3600} if validate else {})}},
        description=f"F39 M-snap {ds}")
    while (s := c.get_microvm_image(imageIdentifier=image_arn(name))["state"]) == "CREATING":
        time.sleep(15)
    return s, time.time() - t0


def build_log_load(name):
    """The server prints 'load {...}' when the build-time load ends; return the last such report."""
    logs = boto3.client("logs", region_name="us-east-1")
    ev = logs.filter_log_events(logGroupName=f"/aws/lambda-microvms/{name}", filterPattern='?"load {" ?validate')["events"]
    return [e["message"] for e in ev]


def load_queries(ds, n):
    q = [[float(x) for x in l.split()] for _, l in zip(range(n), open(os.path.join(ROOT, "datasets", ds, f"queries_{ds}_1000q.csv")))]
    gt = [[int(x) for x in l.split(",")[:10]] for _, l in zip(range(n), open(os.path.join(ROOT, "datasets", ds, f"true_neighbours_{ds}_1000q.csv")))]
    return q, gt


def run_once(name, queries, gt, delay):
    """Launch one MicroVM, wait `delay` s after RUNNING, then query; always terminate."""
    out, mid = {"first_query_delay_s": delay}, None
    try:
        t0 = time.time()
        r = c.run_microvm(imageIdentifier=image_arn(name), executionRoleArn=f"arn:aws:iam::{ACCT}:role/thesis-microvm-exec",
                          ingressNetworkConnectors=[NET + "ALL_INGRESS"], egressNetworkConnectors=[NET + "INTERNET_EGRESS"],
                          idlePolicy={"autoResumeEnabled": True, "maxIdleDurationSeconds": 600, "suspendedDurationSeconds": 600},
                          maximumDurationInSeconds=1800)
        mid = out["microvm_id"] = r["microvmId"]
        while c.get_microvm(microvmIdentifier=mid)["state"] != "RUNNING":
            time.sleep(0.5)
        out["to_running_s"] = time.time() - t0
        tok = c.create_microvm_auth_token(microvmIdentifier=mid, expirationInMinutes=60,
                                          allowedPorts=[{"port": 8080}])["authToken"]["X-aws-proxy-auth"]
        s = requests.Session()
        s.headers["X-aws-proxy-auth"] = tok
        url = f"https://{r['endpoint']}"
        out["stats_at_start"] = s.get(url + "/stats", timeout=60).json()
        time.sleep(delay)
        lat, hits = [], 0
        for i, (q, g) in enumerate(zip(queries, gt)):
            t1 = time.perf_counter()
            o = s.post(url + "/query", json={"queries": [q], "top_k": 10}, timeout=300).json()
            lat.append({"wall_s": time.perf_counter() - t1, "server_ms": o.get("server_ms")})
            hits += len({h[0] for h in o["results"][0]} & set(g))
            if i == 0:
                out["stats_after_first_query"] = s.get(url + "/stats", timeout=60).json()["proc"]
        out["first_query"] = lat[0]
        rest = lat[1:]
        out["next_queries"] = {"n": len(rest), "wall_median_s": statistics.median(x["wall_s"] for x in rest),
                               "server_median_ms": statistics.median(x["server_ms"] for x in rest)}
        out["recall_at_10_pct"] = 100.0 * hits / (10 * len(queries))
        out["stats_at_end"] = s.get(url + "/stats", timeout=60).json()["proc"]
        print(json.dumps({k: out[k] for k in ("first_query_delay_s", "to_running_s", "first_query", "next_queries",
                                              "recall_at_10_pct")}), flush=True)
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
        print("ERROR", out["error"], flush=True)
    finally:
        if mid:
            c.terminate_microvm(microvmIdentifier=mid)
            t2 = time.time()
            while c.get_microvm(microvmIdentifier=mid)["state"] != "TERMINATED" and time.time() - t2 < 120:
                time.sleep(1)
            out["final_state"] = c.get_microvm(microvmIdentifier=mid)["state"]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="deep_1m")
    ap.add_argument("--memory", type=int, default=2048)
    ap.add_argument("--name", default=None)
    ap.add_argument("--artifact", default="msnap-v1.zip")
    ap.add_argument("--skip-build", action="store_true")
    ap.add_argument("--recall-n", type=int, default=100)
    ap.add_argument("--validate", action="store_true", help="enable the /validate build hook (prefetch)")
    ap.add_argument("--env", action="append", default=[], help="extra image env KEY=VALUE")
    ap.add_argument("--delays", default="0", help="one launch per value: seconds to wait after RUNNING before the first query")
    a = ap.parse_args()
    name = a.name or f"thesis-lancedb-msnap-{a.dataset.replace('_', '')}"
    extra = dict(e.split("=", 1) for e in a.env)
    res = {"started_utc": dt.datetime.utcnow().isoformat() + "Z", "image": name, "dataset": a.dataset,
           "memory_MiB": a.memory, "extra_env": extra}
    if not a.skip_build:
        res["build_state"], res["build_s"] = build(name, a.dataset, a.memory, a.artifact, a.validate, extra)
        print("build", res["build_state"], f"{res['build_s']:.0f}s", flush=True)
    res["build_log_load"] = build_log_load(name)
    if res.get("build_state", "CREATED") == "CREATED":
        queries, gt = load_queries(a.dataset, a.recall_n)
        res["launches"] = [run_once(name, queries, gt, float(d)) for d in a.delays.split(",")]
    finish(res)


def finish(res):
    os.makedirs(OUT, exist_ok=True)
    p = os.path.join(OUT, f"msnap_{res['dataset']}_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
    json.dump(res, open(p, "w"), indent=1, default=str)
    print("wrote", p, flush=True)


if __name__ == "__main__":
    main()
