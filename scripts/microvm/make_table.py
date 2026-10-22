"""Write a Deep1B LanceDB table into the MicroVM bucket for the MicroVM images (F39 Phase 1).

Same rows, schema and fragment layout as the LanceDB-on-Lambda ingest: one `add` per 2,000 vectors,
rows built as the Lambda handler builds them ({"id": int, "vector": [float, ...]}), so the table
differs from the Lambda one only in where it lives. Checks the corpus hash first. The MicroVM bucket
keeps MicroVM reads out of the access-logged LanceDB bucket.

    scripts/fixes/.venv/bin/python scripts/microvm/make_table.py deep_1m
Writes s3://thesis-microvm-<acct>/lancedb/<dataset>.lance and scripts/microvm/results/make_table_<dataset>.json.
"""
import hashlib
import json
import os
import sys
import time

import lancedb

ACCT = boto3.client("sts").get_caller_identity()["Account"]
EXPECTED = {"deep_1m": "aa56f86460774bcaeec4675120c5d07de8000c58a008097abca2e6ccd75d21df",
            "deep_10m": "72e0840f1f4605ac5d8715422f3d77154b310414e908ce6768b1bcebda0aa849"}
BATCH = 2000
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def main(ds, report_only=False):
    path = os.path.join(ROOT, "datasets", ds, f"vectors_{ds}.csv")
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 24), b""):
            h.update(chunk)
    sha = h.hexdigest()
    if sha != EXPECTED[ds]:
        sys.exit(f"corpus sha256 {sha} != expected {EXPECTED[ds]}")
    uri = f"s3://thesis-microvm-{ACCT}/lancedb"
    db = lancedb.connect(uri)
    t0, n, tbl, rows = time.time(), 0, None, []
    if report_only:
        return report(ds, uri, sha, db.open_table(ds), None)
    if ds in db.table_names():
        sys.exit(f"{ds} already exists in {uri}; refusing to append")

    def flush():
        nonlocal tbl, rows
        if tbl is None:
            tbl = db.create_table(ds, data=rows)
        else:
            tbl.add(rows)
        rows = []

    with open(path) as f:
        for line in f:
            i, vec = line.rstrip("\n").split(",", 1)
            rows.append({"id": int(i), "vector": [float(x) for x in vec.split()]})
            n += 1
            if len(rows) == BATCH:
                flush()
                if n % 200_000 == 0:
                    print(f"{n:,} rows {time.time() - t0:.0f}s", flush=True)
    if rows:
        flush()
    report(ds, uri, sha, tbl, time.time() - t0)


def report(ds, uri, sha, tbl, seconds):
    import boto3
    pages = boto3.client("s3").get_paginator("list_objects_v2").paginate(
        Bucket=f"thesis-microvm-{ACCT}", Prefix=f"lancedb/{ds}.lance/")
    objs = [o for p in pages for o in p.get("Contents", [])]
    out = {"dataset": ds, "uri": f"{uri}/{ds}.lance", "content_sha256": sha, "rows": tbl.count_rows(),
           "data_files": sum("/data/" in o["Key"] for o in objs), "objects": len(objs),
           "bytes": sum(o["Size"] for o in objs), "schema": str(tbl.schema), "batch": BATCH,
           "seconds": seconds, "lancedb": lancedb.__version__}
    os.makedirs(os.path.join(ROOT, "scripts", "microvm", "results"), exist_ok=True)
    json.dump(out, open(os.path.join(ROOT, "scripts", "microvm", "results", f"make_table_{ds}.json"), "w"), indent=1)
    print(json.dumps(out))


if __name__ == "__main__":
    main(sys.argv[1], report_only="--report-only" in sys.argv)
