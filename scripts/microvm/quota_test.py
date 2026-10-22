"""F39 Phase 1 test 1: can a MicroVM with 8 GB baseline memory run under the account's 8 GB quota?

If the quota counts baseline memory, RunMicrovm succeeds and the MicroVM reaches RUNNING. If it counts
peak memory (4x baseline = 32 GB), RunMicrovm should fail with ServiceQuotaExceededException.
Always terminates the MicroVM.

    harness/.venv/bin/python scripts/microvm/quota_test.py [image-name]
Writes scripts/microvm/results/quota_test_<utc>.json.
"""
import datetime as dt
import json
import os
import sys
import time

import boto3
import requests

ACCT = boto3.client("sts").get_caller_identity()["Account"]
NAME = sys.argv[1] if len(sys.argv) > 1 else "thesis-microvm-quota8"
IMAGE = f"arn:aws:lambda:us-east-1:{ACCT}:microvm-image:{NAME}"
NET = "arn:aws:lambda:us-east-1:aws:network-connector:aws-network-connector:"
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

c = boto3.client("lambda-microvms", region_name="us-east-1")
res = {"started_utc": dt.datetime.utcnow().isoformat() + "Z", "image": IMAGE}
mid = None
try:
    t0 = time.time()
    try:
        r = c.run_microvm(imageIdentifier=IMAGE, ingressNetworkConnectors=[NET + "ALL_INGRESS"],
                          idlePolicy={"autoResumeEnabled": False, "maxIdleDurationSeconds": 300,
                                      "suspendedDurationSeconds": 0},
                          maximumDurationInSeconds=600)
    except c.exceptions.ServiceQuotaExceededException as e:
        res.update(outcome="QUOTA_EXCEEDED", error=str(e))
        raise SystemExit
    mid = res["microvm_id"] = r["microvmId"]
    while (s := c.get_microvm(microvmIdentifier=mid)["state"]) not in ("RUNNING", "TERMINATED") \
            and time.time() - t0 < 300:
        time.sleep(0.5)
    res.update(state=s, to_state_s=time.time() - t0)
    if s == "RUNNING":
        tok = c.create_microvm_auth_token(microvmIdentifier=mid, expirationInMinutes=5,
                                          allowedPorts=[{"port": 8080}])["authToken"]["X-aws-proxy-auth"]
        p = requests.get(f"https://{r['endpoint']}/ping", headers={"X-aws-proxy-auth": tok}, timeout=60)
        res.update(ping_code=p.status_code, outcome="RUNNING_OK" if p.ok else "RUNNING_NO_PING")
    else:
        res.update(outcome=f"NOT_RUNNING:{s}",
                   state_reason=c.get_microvm(microvmIdentifier=mid).get("stateReason"))
except SystemExit:
    pass
except Exception as e:
    res.update(outcome="ERROR", error=f"{type(e).__name__}: {e}")
finally:
    if mid:
        c.terminate_microvm(microvmIdentifier=mid)
        t1 = time.time()
        while c.get_microvm(microvmIdentifier=mid)["state"] != "TERMINATED" and time.time() - t1 < 120:
            time.sleep(1)
        res["final_state"] = c.get_microvm(microvmIdentifier=mid)["state"]
    os.makedirs(OUT, exist_ok=True)
    p = os.path.join(OUT, f"quota_test_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
    json.dump(res, open(p, "w"), indent=1, default=str)
    print(json.dumps(res, default=str))
