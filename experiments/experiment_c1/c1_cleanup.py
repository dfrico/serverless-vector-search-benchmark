"""
Experiment C1, Phase 3 — cleanup. Drop the deep_1m data from the Blocks bucket,
keep the deploy (Lithops runtime + bucket), and verify the end state.

Uses BlocksClientAWS.teardown(drop_data=True) — the finding-21 override that
deletes on real AWS: vectors_deep_1m.csv, true_neighbours_deep_1m.csv, the
indexes/deep_1m/ prefix, queries_deep_1m_* staging, and the shared reduce/
prefix. It does NOT delete the Lambda runtime or the bucket, so the deploy
stays ready for C2/C3.

Run via ec_blocks.sh.
"""
from __future__ import annotations

import functools
import json
import os
import sys

print = functools.partial(print, flush=True)  # noqa: A001

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
import c1_common as K  # noqa: E402
import eb_common as C  # noqa: E402


def snapshot(s3, bucket, ds):
    def count(prefix, exact=False):
        n = 0
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if exact and obj["Key"] != prefix:
                    continue
                n += 1
        return n
    return {
        "vectors_csv": count(f"vectors_{ds}.csv", exact=True),
        "true_neighbours_csv": count(f"true_neighbours_{ds}.csv", exact=True),
        "indexes": count(f"indexes/{ds}/"),
        "queries_staging": count(f"queries_{ds}_"),
        "reduce_prefix": count("reduce/"),
    }


def main():
    import boto3
    acct = K.account_id()
    bucket = f"thesis-blocks-{acct}"
    s3 = boto3.client("s3", region_name="us-east-1")

    before = snapshot(s3, bucket, C.DATASET)
    print(f"[c1/cleanup] BEFORE {C.DATASET}: {before}")

    client = K.build_blocks(acct)
    client.setup(C.DATASET, dimensions=C.DIMENSIONS, top_k_default=C.TOP_K,
                 create_if_missing=False)
    client.teardown(drop_data=True)

    after = snapshot(s3, bucket, C.DATASET)
    print(f"[c1/cleanup] AFTER  {C.DATASET}: {after}")

    # bucket + runtime kept: just confirm the bucket still exists.
    s3.head_bucket(Bucket=bucket)
    print(f"[c1/cleanup] bucket {bucket} still present (deploy kept).")

    clean = all(v == 0 for v in after.values())
    out = {"bucket": bucket, "dataset": C.DATASET, "before": before,
           "after": after, "clean": clean, "deploy_kept": True}
    with open(os.path.join(K.RESULTS_DIR, "cleanup.json"), "w") as f:
        json.dump(out, f, indent=2, sort_keys=True)
    print("\n=== C1 CLEANUP", "CLEAN" if clean else "RESIDUE", "===")
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0 if clean else 1


if __name__ == "__main__":
    raise SystemExit(main())
