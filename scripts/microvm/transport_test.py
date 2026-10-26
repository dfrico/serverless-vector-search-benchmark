"""Where does E1's ~0.05 s of client overhead come from? (finding 33, follow-up to finding 31's E1 transport item.)

Hypothesis: server v4/v5 writes each HTTP response as two writes (headers, then body) with Nagle on. On a busy
keep-alive connection the endpoint delays its ACK of the first write, and the body waits for that ACK (about 40 ms on
Linux, on a 10 ms timer grid). A new or long-idle connection ACKs at once (quick-ACK mode), which would explain the
cheap first request of each E1 cell and the 10-20 ms of the spaced E3 trace queries.

    build <dataset>   package server v6 as mvm-v6.zip and build thesis-lancedb-mvm-<ds>-v6 (same settings as the
                      experiment image, build_image.py; v6 writes one send by default, X-Write-Mode selects v4's)
    run <dataset> [--image NAME] [--n 100]
        One MicroVM through the adapter (60 s settle, as E1), then blocks of back-to-back requests on one keep-alive
        session per arm, in the order ABCD then DCBA so drift averages out:
          query_<mode>  POST /query with X-Write-Mode = mode   (on the v4 image the header is ignored: v4 behaviour)
          ping_<mode>   GET /ping (no search work)
        then query_split spaced 2 s apart (idle connection), and query_split on a new session per request.
        Records wall time, server_ms and the request's position on the guest-side connection.

Writes scripts/microvm/results/transport/transport_<ds>_<image>_<utc>.json.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
import zipfile

import boto3
import requests
from requests.adapters import HTTPAdapter

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
ACCT = boto3.client("sts").get_caller_identity()["Account"]
BUCKET = f"thesis-microvm-{ACCT}"
OUT = os.path.join(ROOT, "scripts", "microvm", "results", "transport")
MEM = {"deep_1m": 2048, "deep_10m": 8192}
ARTIFACT = "mvm-v6.zip"


def build(ds):
    src = os.path.join(ROOT, "harness", "deploy", "microvm")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for f in ("Dockerfile", "server.py"):
            z.write(os.path.join(src, f), f)
    boto3.client("s3").put_object(Bucket=BUCKET, Key=f"artifacts/{ARTIFACT}", Body=buf.getvalue())
    c = boto3.client("lambda-microvms", region_name="us-east-1")
    name = f"thesis-lancedb-mvm-{ds.replace('_', '')}-v6"
    arn = f"arn:aws:lambda:us-east-1:{ACCT}:microvm-image:{name}"
    t0 = time.time()
    c.create_microvm_image(
        name=name, baseImageArn="arn:aws:lambda:us-east-1:aws:microvm-image:al2023-1",
        buildRoleArn=f"arn:aws:iam::{ACCT}:role/thesis-microvm-build",
        codeArtifact={"uri": f"s3://{BUCKET}/artifacts/{ARTIFACT}"},
        cpuConfigurations=[{"architecture": "ARM_64"}], resources=[{"minimumMemoryInMiB": MEM[ds]}],
        environmentVariables={"TABLE_URI": f"s3://{BUCKET}/lancedb/{ds}.lance", "AWS_DEFAULT_REGION": "us-east-1",
                              "VALIDATE_MODE": "full"},
        hooks={"port": 8080, "microvmHooks": {"run": "ENABLED", "resume": "ENABLED", "suspend": "ENABLED"},
               "microvmImageHooks": {"ready": "ENABLED", "readyTimeoutInSeconds": 3600,
                                     "validate": "ENABLED", "validateTimeoutInSeconds": 3600}},
        description=f"F39 experiment image {ds}, server v6 (one-write responses; finding 33)")
    while (s := c.get_microvm_image(imageIdentifier=arn)["state"]) == "CREATING":
        time.sleep(15)
    rec = {"image": name, "artifact": ARTIFACT, "state": s, "build_s": round(time.time() - t0, 1),
           "built_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    os.makedirs(OUT, exist_ok=True)
    json.dump(rec, open(os.path.join(OUT, f"build_{name}.json"), "w"), indent=1)
    print(json.dumps(rec), flush=True)


def queries(ds, n):
    with open(os.path.join(ROOT, "datasets", ds, f"queries_{ds}_1000q.csv")) as f:
        return [[float(x) for x in line.split()] for _, line in zip(range(n), f)]


def run(a):
    from harness.adapters.lancedb_microvm import build_for_dataset
    ds = a.dataset
    csv = os.path.join(ROOT, "datasets", ds, f"vectors_{ds}.csv")
    client = build_for_dataset(ds, csv, ACCT, settle_s=60.0, image_name=a.image)
    image = a.image or f"thesis-lancedb-mvm-{ds.replace('_', '')}"
    rec = {"dataset": ds, "image": image, "n_per_block": a.n, "records": [],
           "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    Q = queries(ds, 1000)
    qi = [0]
    try:
        client.setup(ds, dimensions=96)
        rec["microvm_id"], rec["lifecycle"] = client.microvm_id, client.lifecycle_record
        url = f"https://{client.endpoint}"
        token = client._session.headers["X-aws-proxy-auth"]

        def session():
            s = requests.Session()
            s.mount("https://", HTTPAdapter(pool_connections=1, pool_maxsize=1))
            s.headers["X-aws-proxy-auth"] = token
            return s

        def one(s, arm, block, kind, mode):
            h = {"X-Write-Mode": mode}
            t0 = time.perf_counter()
            if kind == "query":
                r = s.post(url + "/query", json={"queries": [Q[qi[0] % 1000]], "top_k": 10}, headers=h, timeout=60)
                qi[0] += 1
            else:
                r = s.get(url + "/ping", headers=h, timeout=60)
            wall = time.perf_counter() - t0
            j = r.json() if r.status_code == 200 else {}
            rec["records"].append({"arm": arm, "block": block, "status": r.status_code, "wall_s": wall,
                                   "server_ms": j.get("server_ms"), "write_mode": j.get("write_mode"),
                                   "req_on_conn": j.get("req_on_conn"), "conn_age_s": j.get("conn_age_s"),
                                   "cpu_part": j.get("cpu_part"), "t": time.time()})

        arms = [("query", "split"), ("query", "single"), ("query", "split_nodelay"), ("ping", "split"),
                ("ping", "single")]
        sessions = {f"{k}_{m}": session() for k, m in arms}
        for block, order in enumerate([arms, arms[::-1]]):
            for kind, mode in order:
                arm = f"{kind}_{mode}"
                for _ in range(a.n):
                    one(sessions[arm], arm, block, kind, mode)
                print(f"block {block} {arm}: done", flush=True)
        s = session()
        for _ in range(20):
            time.sleep(2.0)
            one(s, "query_split_spaced2s", 2, "query", "split")
        for _ in range(30):
            with session() as s:
                one(s, "query_split_newconn", 3, "query", "split")
        print("spaced and new-connection arms: done", flush=True)
        rec["stats"] = client.stats()
    except Exception as e:  # keep what was recorded; teardown terminates the MicroVM either way
        rec["error"] = f"{type(e).__name__}: {e}"
        print("ERROR", rec["error"], flush=True)
    finally:
        client.teardown()
    rec["summary"] = summarize(rec["records"])
    print(json.dumps(rec["summary"], indent=1), flush=True)
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, f"transport_{ds}_{image}_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
    json.dump(rec, open(path, "w"), indent=1, default=str)
    print("->", path, flush=True)


def summarize(records, skip=3):
    """Per arm: median wall and median overhead (wall - server time, queries only), skipping the first `skip`
    requests of each keep-alive block (they follow an idle gap), plus the share of walls within 1 ms of a 10-ms step."""
    import statistics as st
    out = {}
    for arm in dict.fromkeys(r["arm"] for r in records):
        rs = [r for r in records if r["arm"] == arm and r["status"] == 200]
        if arm.startswith(("query_split_spaced", "query_split_newconn")):
            steady = rs
        else:
            steady = [r for b in (0, 1) for r in [x for x in rs if x["block"] == b][skip:]]
        walls = [r["wall_s"] for r in steady]
        ov = [r["wall_s"] - r["server_ms"] / 1e3 for r in steady if r["server_ms"] is not None]
        grid = [abs(w * 1e3 - 10 * round(w * 100)) <= 1 for w in walls]
        out[arm] = {"n": len(steady), "errors": sum(1 for r in records if r["arm"] == arm and r["status"] != 200),
                    "wall_p50_s": st.median(walls) if walls else None,
                    "overhead_p50_ms": st.median(ov) * 1e3 if ov else None,
                    "overhead_p10_p90_ms": [sorted(ov)[len(ov) // 10] * 1e3, sorted(ov)[9 * len(ov) // 10] * 1e3] if ov else None,
                    "on_10ms_grid_share": sum(grid) / len(grid) if grid else None,
                    "req_on_conn_max": max((r["req_on_conn"] or 0) for r in rs) if rs else None,
                    "first_in_block_overhead_ms": [round((r["wall_s"] - (r["server_ms"] or 0) / 1e3) * 1e3, 1)
                                                   for b in (0, 1) for r in [x for x in rs if x["block"] == b][:skip]]}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["build", "run"])
    ap.add_argument("dataset", choices=list(MEM))
    ap.add_argument("--image", default=None, help="default: the E1 experiment image (server v4)")
    ap.add_argument("--n", type=int, default=100, help="requests per arm per block")
    a = ap.parse_args()
    build(a.dataset) if a.cmd == "build" else run(a)


if __name__ == "__main__":
    main()
