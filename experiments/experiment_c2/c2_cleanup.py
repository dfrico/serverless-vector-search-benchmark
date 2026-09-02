"""
Experiment C2 — teardown for ONE adapter.

Drops this run's 1M data and restores the pre-experiment Lambda env for the two
container adapters (removes the LAST_COLD_MARKER var this experiment added). Keeps
the deploys (Lambda functions, ECR images, buckets) for the later C3 10M work.

  - blocks   : delete the corpus CSV + index objects directly with boto3
               (BlocksClient.teardown(drop_data=True) is a MinIO-only no-op on
               real AWS — see finding 20), then strip LAST_COLD_MARKER from the
               Lithops worker Lambdas.
  - s3vectors: teardown(drop_data=True) deletes the index (no env to restore).
  - lancedb  : teardown(drop_data=True) drops the table, then strip
               LAST_COLD_MARKER from the Lambda.

Run:
  ./c2_blocks.sh c2_cleanup.py blocks
  ./c2_aws.sh    c2_cleanup.py s3vectors
  ./c2_aws.sh    c2_cleanup.py lancedb
"""
from __future__ import annotations

import argparse
import functools
import os
import sys

print = functools.partial(print, flush=True)  # noqa: A001

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import c2_common as K  # noqa: E402
import eb_common as C  # noqa: E402


def _delete_blocks_data(acct):
    """Delete the 1M corpus + index objects from the Blocks bucket (boto3)."""
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = f"thesis-blocks-{acct}"
    prefixes = [f"vectors_{C.DATASET}.csv", f"indexes/{C.DATASET}/"]
    deleted = 0
    for prefix in prefixes:
        token = None
        while True:
            kw = {"Bucket": bucket, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            objs = [{"Key": o["Key"]} for o in resp.get("Contents", [])]
            if objs:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
                deleted += len(objs)
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break
    return bucket, deleted


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("adapter", choices=["blocks", "s3vectors", "lancedb"])
    args = ap.parse_args()
    adapter = args.adapter
    acct = K.acct()
    print(f"[c2/cleanup/{adapter}] account={acct}")

    if adapter == "blocks":
        bucket, deleted = _delete_blocks_data(acct)
        print(f"[c2/cleanup/blocks] deleted {deleted} objects from {bucket}")
        targets = K.discover_blocks_worker_lambdas()
        K.restore_env(targets)
        print(f"[c2/cleanup/blocks] restored env on {targets}")
    elif adapter == "s3vectors":
        client = K.build_and_setup("s3vectors", create_if_missing=False)
        client.teardown(drop_data=True)
        print("[c2/cleanup/s3vectors] index dropped via teardown(drop_data=True)")
    elif adapter == "lancedb":
        client = K.build_and_setup("lancedb", create_if_missing=False)
        client.teardown(drop_data=True)
        print("[c2/cleanup/lancedb] table dropped via teardown(drop_data=True)")
        K.restore_env(["thesis-lancedb-lambda"])
        print("[c2/cleanup/lancedb] restored env on thesis-lancedb-lambda")

    print(f"\n=== C2 CLEANUP {adapter.upper()} DONE ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
