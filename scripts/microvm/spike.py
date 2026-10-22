"""F39 Phase 0 spike: lifecycle and access checks for LanceDB on Lambda MicroVMs.

Runs one MicroVM from the spike image, measures time to RUNNING and ping latency, checks
the execution-role credentials, S3 throughput and opening the 10M LanceDB table (read
only), then three idle -> auto-suspend -> auto-resume cycles and one explicit
suspend/resume. Always terminates the MicroVM. Latencies are from wherever this runs
(the laptop for the spike), so they are not thesis numbers.

    AWS_PROFILE=thesis harness/.venv/bin/python scripts/microvm/spike.py
Writes scripts/microvm/results/spike_<utc>.json.
"""
import datetime as dt
import json
import os
import statistics
import time

import boto3
import requests

ACCT = boto3.client("sts").get_caller_identity()["Account"]
IMAGE = f"arn:aws:lambda:us-east-1:{ACCT}:microvm-image:thesis-lancedb-spike"
EXEC_ROLE = f"arn:aws:iam::{ACCT}:role/thesis-microvm-exec"
NET = "arn:aws:lambda:us-east-1:aws:network-connector:aws-network-connector:"
LANCE_BUCKET = f"thesis-lancedb-{ACCT}"
IDLE_S = 60
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

c = boto3.client("lambda-microvms", region_name="us-east-1")
res = {"started_utc": dt.datetime.utcnow().isoformat() + "Z", "image": IMAGE, "events": []}


def log(msg, **kw):
    kw["t"] = time.time()
    res["events"].append({"msg": msg, **kw})
    print(f"{time.strftime('%H:%M:%S')} {msg} {json.dumps(kw, default=str)[:300]}", flush=True)


def state(mid):
    return c.get_microvm(microvmIdentifier=mid)["state"]


def wait_state(mid, want, timeout=600, poll=0.5):
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = state(mid)
        if s == want:
            return time.time() - t0
        time.sleep(poll)
    raise TimeoutError(f"{mid} not {want} after {timeout}s (last {s})")


def get(url, hdr, path, timeout=900):
    t0 = time.perf_counter()
    r = requests.get(url + path, headers=hdr, timeout=timeout)
    return time.perf_counter() - t0, r.status_code, (r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text[:300])


def main():
    mid = None
    try:
        t0 = time.time()
        r = c.run_microvm(imageIdentifier=IMAGE, executionRoleArn=EXEC_ROLE,
                          ingressNetworkConnectors=[NET + "ALL_INGRESS"],
                          egressNetworkConnectors=[NET + "INTERNET_EGRESS"],
                          idlePolicy={"autoResumeEnabled": True, "maxIdleDurationSeconds": IDLE_S,
                                      "suspendedDurationSeconds": 1800},
                          maximumDurationInSeconds=3600)
        mid, ep = r["microvmId"], r["endpoint"]
        res.update(microvm_id=mid, run_api_s=time.time() - t0)
        log("run_microvm", state=r["state"], api_s=res["run_api_s"])
        wait_state(mid, "RUNNING")
        res["to_running_s"] = time.time() - t0
        log("RUNNING", since_run_s=res["to_running_s"])

        t1 = time.time()
        tok = c.create_microvm_auth_token(microvmIdentifier=mid, expirationInMinutes=60,
                                          allowedPorts=[{"allPorts": {}}])["authToken"]
        res["token_api_s"] = time.time() - t1
        res["token_header_keys"] = sorted(tok)
        hdr = {"X-aws-proxy-auth": tok["X-aws-proxy-auth"]}
        url = f"https://{ep}"

        first = get(url, hdr, "/ping")
        res["first_ping"] = {"s": first[0], "code": first[1], "body": first[2]}
        res["first_request_since_run_s"] = time.time() - t0
        log("first ping", s=first[0], code=first[1], body=first[2])
        pings = [get(url, hdr, "/ping")[0] for _ in range(20)]
        res["warm_ping_s"] = {"n": 20, "median": statistics.median(pings), "min": min(pings), "max": max(pings)}
        log("20 warm pings", **res["warm_ping_s"])

        for name, path in [("creds", "/creds"),
                           ("s3_10mb", f"/s3?bucket={LANCE_BUCKET}&key=__KEY__&mb=10"),
                           ("lance_10m", f"/lance?uri=s3://{LANCE_BUCKET}/lancedb&table=deep_10m")]:
            if "__KEY__" in path:
                key = pick_data_key()
                path = path.replace("__KEY__", key)
                res["s3_key"] = key
            s, code, body = get(url, hdr, path)
            res[name] = {"s": s, "code": code, "body": body}
            log(name, s=s, code=code, body=body)

        cycles = []
        for i in range(3):
            time.sleep(IDLE_S + 20)
            t_wait = time.time()
            while state(mid) != "SUSPENDED" and time.time() - t_wait < 300:
                time.sleep(2)
            st = state(mid)
            s, code, body = get(url, hdr, "/ping")
            after = [get(url, hdr, "/ping")[0] for _ in range(5)]
            cycles.append({"state_before": st, "extra_wait_s": time.time() - t_wait - s - sum(after),
                           "first_s": s, "code": code, "next5_median_s": statistics.median(after),
                           "resume_hooks": len(body.get("resume_hooks", [])) if isinstance(body, dict) else None,
                           "pid": body.get("pid") if isinstance(body, dict) else None})
            log(f"auto-resume cycle {i + 1}", **cycles[-1])
        res["auto_resume_cycles"] = cycles

        t2 = time.time()
        c.suspend_microvm(microvmIdentifier=mid)
        wait_state(mid, "SUSPENDED")
        res["explicit_suspend_s"] = time.time() - t2
        t3 = time.time()
        c.resume_microvm(microvmIdentifier=mid)
        wait_state(mid, "RUNNING")
        res["explicit_resume_to_running_s"] = time.time() - t3
        s, code, body = get(url, hdr, "/ping")
        res["explicit_resume_first_ping"] = {"s": s, "code": code, "since_resume_api_s": time.time() - t3}
        log("explicit suspend/resume", suspend_s=res["explicit_suspend_s"],
            resume_s=res["explicit_resume_to_running_s"], first_ping=s)
        res["final_ping"] = body
    except Exception as e:
        res["error"] = f"{type(e).__name__}: {e}"
        log("ERROR", error=res["error"])
    finally:
        if mid:
            t4 = time.time()
            c.terminate_microvm(microvmIdentifier=mid)
            try:
                res["terminate_s"] = wait_state(mid, "TERMINATED", timeout=300)
            except Exception as e:
                res["terminate_error"] = str(e)
            g = c.get_microvm(microvmIdentifier=mid)
            res["final_state"] = g["state"]
            res["started_at"], res["terminated_at"] = str(g.get("startedAt")), str(g.get("terminatedAt"))
            log("terminated", state=g["state"], s=time.time() - t4)
        res["ended_utc"] = dt.datetime.utcnow().isoformat() + "Z"
        os.makedirs(OUT, exist_ok=True)
        p = os.path.join(OUT, f"spike_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
        json.dump(res, open(p, "w"), indent=1, default=str)
        print("wrote", p)


def pick_data_key():
    s3 = boto3.client("s3", region_name="us-east-1")
    r = s3.list_objects_v2(Bucket=LANCE_BUCKET, Prefix="lancedb/deep_10m.lance/data/", MaxKeys=5)
    objs = sorted(r.get("Contents", []), key=lambda o: -o["Size"])
    return objs[0]["Key"]


if __name__ == "__main__":
    main()
