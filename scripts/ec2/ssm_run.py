#!/usr/bin/env python3
"""
Run a bash script on the EC2 benchmark client through SSM (no SSH, no quoting
layers) and print its output.

    harness/.venv/bin/python scripts/ec2/ssm_run.py script.sh [--wait 600]
    harness/.venv/bin/python scripts/ec2/ssm_run.py -c 'tail -5 ~/bootstrap.log'

The script runs as ec2-user in a login shell. Long jobs should start a tmux
session and return; poll them with a later short command.
"""
from __future__ import annotations

import argparse
import base64
import sys
import time

import boto3

INSTANCE_NAME = "thesis-bench-client"


def instance_id(ec2, name: str = INSTANCE_NAME) -> str:
    r = ec2.describe_instances(Filters=[{"Name": "tag:Name", "Values": [name]},
                                        {"Name": "instance-state-name", "Values": ["running"]}])
    ids = [i["InstanceId"] for res in r["Reservations"] for i in res["Instances"]]
    if len(ids) != 1:
        sys.exit(f"expected one running {name}, found {ids}")
    return ids[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("script", nargs="?", help="local bash script to run remotely")
    ap.add_argument("-c", dest="inline", help="inline bash instead of a file")
    ap.add_argument("--wait", type=int, default=300, help="seconds to wait for completion")
    ap.add_argument("--name", default=INSTANCE_NAME,
                    help="Name tag of the target instance (thesis-bench-client-2 = MicroVM client)")
    a = ap.parse_args()
    body = a.inline if a.inline is not None else open(a.script).read()
    b64 = base64.b64encode(body.encode()).decode()
    cmd = f"echo {b64} | base64 -d > /tmp/ssm_job.sh && sudo -u ec2-user -i bash /tmp/ssm_job.sh"

    s = boto3.Session(profile_name="thesis", region_name="us-east-1")
    ssm, ec2 = s.client("ssm"), s.client("ec2")
    iid = instance_id(ec2, a.name)
    cid = ssm.send_command(InstanceIds=[iid], DocumentName="AWS-RunShellScript",
                           Parameters={"commands": [cmd], "executionTimeout": [str(a.wait + 60)]},
                           TimeoutSeconds=60)["Command"]["CommandId"]
    t0 = time.time()
    while True:
        time.sleep(3)
        try:
            inv = ssm.get_command_invocation(CommandId=cid, InstanceId=iid)
        except ssm.exceptions.InvocationDoesNotExist:
            continue
        if inv["Status"] not in ("Pending", "InProgress", "Delayed") or time.time() - t0 > a.wait:
            break
    print(inv.get("StandardOutputContent", ""), end="")
    err = inv.get("StandardErrorContent", "")
    if err.strip():
        print("--- stderr ---\n" + err, file=sys.stderr, end="")
    print(f"[ssm] {iid} status={inv['Status']} after {time.time() - t0:.0f}s", file=sys.stderr)
    sys.exit(0 if inv["Status"] == "Success" else 1)


if __name__ == "__main__":
    main()
