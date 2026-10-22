"""Copy the retained 10M LanceDB table into the MicroVM bucket for the 10M image (F39), server side.

The 10M table is the one LanceDB-on-Lambda queries in C3 (ingested from vectors_deep_10m.csv in 2,000-vector
batches, 5,000 data files). Copying it, instead of writing a new one with make_table.py, keeps the two LanceDB
variants on the very same table files. Run only when no baseline chain is measuring: the source bucket is
access-logged and the copy's reads would land in a cost window. Writes the same record shape as
make_table.py, with the copy's object count and bytes checked against the source.

    AWS_DEFAULT_REGION=us-east-1 harness/.venv/bin/python scripts/microvm/copy_table.py deep_10m
"""
import concurrent.futures as cf
import json
import os
import sys
import time

import boto3

ACCT = boto3.client("sts").get_caller_identity()["Account"]
SRC, DST = f"thesis-lancedb-{ACCT}", f"thesis-microvm-{ACCT}"
SHA = {"deep_10m": "72e0840f1f4605ac5d8715422f3d77154b310414e908ce6768b1bcebda0aa849"}
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def listing(s3, bucket, prefix):
    out = {}
    for p in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        out.update({o["Key"][len(prefix):]: o["Size"] for o in p.get("Contents", [])})
    return out


def main(ds):
    ec2 = boto3.client("ec2", region_name="us-east-1")
    st = ec2.describe_instances(Filters=[{"Name": "tag:Name", "Values": ["thesis-bench-client"]}])
    states = [i["State"]["Name"] for r in st["Reservations"] for i in r["Instances"]]
    if "running" in states and "--force" not in sys.argv:
        sys.exit("thesis-bench-client is running (a baseline chain may be measuring); rerun after it stops")
    s3 = boto3.client("s3", region_name="us-east-1")
    src_prefix, dst_prefix = f"lancedb/{ds}.lance/", f"lancedb/{ds}.lance/"
    src = listing(s3, SRC, src_prefix)
    if listing(s3, DST, dst_prefix):
        sys.exit(f"s3://{DST}/{dst_prefix} is not empty; refusing to overwrite")
    t0 = time.time()

    def cp(rel):
        s3.copy({"Bucket": SRC, "Key": src_prefix + rel}, DST, dst_prefix + rel)

    with cf.ThreadPoolExecutor(64) as ex:
        list(ex.map(cp, list(src)))
    dst = listing(s3, DST, dst_prefix)
    ok = dst == src
    rec = {"dataset": ds, "uri": f"s3://{DST}/lancedb/{ds}.lance", "source": f"s3://{SRC}/{src_prefix}",
           "method": "server-side copy of the table LanceDB-on-Lambda queries in C3", "content_sha256": SHA[ds],
           "content_sha256_note": "hash of the CSV the source table was ingested from (C3 anchor), not of the copy",
           "objects": len(dst), "bytes": sum(dst.values()), "data_files": sum("data/" in k for k in dst),
           "copy_matches_source": ok, "seconds": time.time() - t0}
    json.dump(rec, open(os.path.join(ROOT, "scripts", "microvm", "results", f"make_table_{ds}.json"), "w"), indent=1)
    print(json.dumps(rec))
    if not ok:
        sys.exit("copy does not match the source listing")


if __name__ == "__main__":
    main(sys.argv[1])
