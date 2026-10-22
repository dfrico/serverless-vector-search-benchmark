"""
Self-test for BlocksClientAWS.teardown on real AWS (finding 21).

The parent BlocksClient.teardown is a no-op on real AWS (its delete path is
guarded by AWS_ENDPOINT_URL_S3, a MinIO-only variable). The subclass overrides
teardown to delete via boto3. This test proves the override works end to end:

  1. setup() against the real Blocks bucket with a THROWAWAY dataset name,
  2. ingest 10 tiny vectors and build a 1-block index (real S3 + aws_lambda),
  3. put marker objects under queries_{ds}_ and reduce/ so those prefixes are
     exercised too,
  4. count the dataset's S3 objects (boto3) — expect > 0,
  5. teardown(drop_data=True),
  6. count again — expect 0.

Spend: one tiny Lambda index build plus a few S3 PUT/DELETE calls — pennies. No
queries are run. A finally block force-deletes the throwaway objects even if the
test fails, so nothing lingers. It never touches deep_1m or deep_100k.

Run:  AWS_PROFILE=thesis harness/.venv/bin/python harness/tests/verify_teardown.py
(no MinIO env).
"""
import json
import os
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(os.path.dirname(_HERE))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import boto3  # noqa: E402
from harness.adapters.blocks_aws import BlocksClientAWS  # noqa: E402

BUCKET = f"thesis-blocks-{boto3.client('sts').get_caller_identity()['Account']}"
REGION = "us-east-1"
DATASET = "zz_teardown_selftest"   # clearly throwaway; never deep_1m / deep_100k
PROTECTED = {"deep_1m", "deep_100k"}
DIM = 8
N_VECTORS = 10


def _count(s3, prefix, exact=False):
    """Count objects under a prefix (or an exact key)."""
    n = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if exact and obj["Key"] != prefix:
                continue
            n += 1
    return n


def _snapshot(s3, ds):
    return {
        "vectors_csv": _count(s3, f"vectors_{ds}.csv", exact=True),
        "indexes": _count(s3, f"indexes/{ds}/"),
        "queries_staging": _count(s3, f"queries_{ds}_"),
        "reduce_marker": _count(s3, f"reduce/res_{ds}.json", exact=True),
    }


def main():
    # Safety: never run against a real corpus name.
    assert DATASET not in PROTECTED, f"refusing: {DATASET} is protected"

    import boto3
    s3 = boto3.client("s3", region_name=REGION)

    # Tiny local CSV: id,<space-separated floats>, Blocks' native row shape.
    tmp = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False)
    for i in range(N_VECTORS):
        vec = " ".join(str(round(0.1 * ((i + j) % 7), 4)) for j in range(DIM))
        tmp.write(f"{i},{vec}\n")
    tmp.close()
    csv_path = tmp.name
    print(f"[verify] wrote throwaway corpus {csv_path} "
          f"({N_VECTORS} vectors x {DIM} dims)")

    client = BlocksClientAWS(bucket=BUCKET, vectors_csv_path=csv_path, region=REGION)
    result = {"dataset": DATASET, "bucket": BUCKET}
    try:
        # 1-block, nlist=1 index trains cleanly on 10 vectors.
        client.setup(DATASET, dimensions=DIM, top_k_default=10, num_blocks=1,
                     partitions_per_block=1, n_probe=1, blocks_per_task=1,
                     create_if_missing=True)
        print(f"[verify] setup OK — num_blocks={client.num_blocks} "
              f"nlist={client.partitions_per_block}")

        _, ing = client.bulk_insert_from_path(csv_path, build_index=True)
        print(f"[verify] ingest OK — upload_s="
              f"{ing.get('bulk_insert_upload_s'):.2f} "
              f"index_s={ing.get('bulk_insert_index_s'):.2f}")

        # Exercise the queries_ and reduce/ prefixes without running a real query.
        s3.put_object(Bucket=BUCKET, Key=f"queries_{DATASET}_1.csv", Body=b"marker")
        s3.put_object(Bucket=BUCKET, Key=f"reduce/res_{DATASET}.json", Body=b"{}")

        before = _snapshot(s3, DATASET)
        print(f"[verify] BEFORE teardown: {before}")

        client.teardown(drop_data=True)

        after = _snapshot(s3, DATASET)
        print(f"[verify] AFTER  teardown: {after}")

        passed = (
            before["vectors_csv"] == 1
            and before["indexes"] >= 1
            and before["queries_staging"] == 1
            and before["reduce_marker"] == 1
            and all(v == 0 for v in after.values())
        )
        result.update({"before": before, "after": after, "passed": bool(passed)})
    finally:
        # Defensive sweep: force-delete any throwaway objects so nothing lingers,
        # even if the test failed before teardown ran.
        residue = []
        for prefix in (f"vectors_{DATASET}.csv", f"indexes/{DATASET}/",
                       f"queries_{DATASET}_", f"reduce/res_{DATASET}.json"):
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
                residue += [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if residue:
            for i in range(0, len(residue), 1000):
                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": residue[i:i + 1000]})
            print(f"[verify] defensive sweep removed {len(residue)} leftover objects")
        os.remove(csv_path)

    out_path = os.path.join(_HERE, "results", "verify_teardown_result.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)

    print("\n=== VERIFY TEARDOWN", "PASS" if result.get("passed") else "FAIL", "===")
    print(json.dumps(result, indent=2))
    return 0 if result.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
