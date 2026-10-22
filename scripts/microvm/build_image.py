"""Build the experiment MicroVM image for one dataset (F39): server v4 (`mvm-v4.zip`), M-snap, full `/validate`.

Same settings as `thesis-lancedb-mvm-deep1m` v1.0 (finding 30), with the baseline memory per dataset: 2 GB at 1M,
8 GB at 10M (finding 29, test 1). The table must already be in the MicroVM bucket (`copy_table.py deep_10m`).
Records the build's state, duration and the server's build-time load/validate log lines.

    AWS_PROFILE=thesis harness/.venv/bin/python scripts/microvm/build_image.py deep_10m
Writes scripts/microvm/results/build_<dataset>_<utc>.json.
"""
from __future__ import annotations

import json
import os
import sys
import time

import boto3

ACCT = boto3.client("sts").get_caller_identity()["Account"]
BUCKET = f"thesis-microvm-{ACCT}"
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUT = os.path.join(ROOT, "scripts", "microvm", "results")
ARTIFACT = "mvm-v4.zip"
MEM = {"deep_1m": 2048, "deep_10m": 8192}
c = boto3.client("lambda-microvms", region_name="us-east-1")


def main(ds):
    name = f"thesis-lancedb-mvm-{ds.replace('_', '')}"
    arn = f"arn:aws:lambda:us-east-1:{ACCT}:microvm-image:{name}"
    rec = {"image": name, "dataset": ds, "artifact": ARTIFACT, "baseline_mib": MEM[ds], "validate_mode": "full",
           "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
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
        description=f"F39 experiment image {ds}")
    while (s := c.get_microvm_image(imageIdentifier=arn)["state"]) == "CREATING":
        time.sleep(15)
    rec.update(state=s, build_s=round(time.time() - t0, 1))
    try:
        logs = boto3.client("logs", region_name="us-east-1")
        ev = logs.filter_log_events(logGroupName=f"/aws/lambda-microvms/{name}",
                                    filterPattern='?"load {" ?validate')["events"]
        rec["build_log"] = [e["message"] for e in ev]
    except Exception as e:  # the log group name is not documented; keep the build record either way
        rec["build_log_error"] = repr(e)
    path = os.path.join(OUT, f"build_{ds}_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
    json.dump(rec, open(path, "w"), indent=1)
    print(json.dumps({k: v for k, v in rec.items() if k != "build_log"}), "\n->", path)


if __name__ == "__main__":
    main(sys.argv[1])
