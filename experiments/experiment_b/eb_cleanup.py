"""
Experiment B — data-plane cleanup after the sweep (keeps all deploys).

Deletes the 1M deep_1m data from each adapter's data plane so no ongoing storage
cost remains, but keeps the reusable infra (Blocks Lambda functions + ECR runtime;
LanceDB Lambda + ECR + role + bucket; S3 Vectors bucket).

  - Blocks: delete vectors_deep_1m.csv and indexes/deep_1m/* directly via boto3.
    (BlocksClient.teardown(drop_data=True) only deletes when AWS_ENDPOINT_URL_S3 is
    set — a MinIO-only path — so on real AWS it is a no-op; we delete by hand.)
  - S3 Vectors: teardown(drop_data=True) -> delete the 'deep-1m' index.
  - LanceDB: teardown(drop_data=True) -> Lambda "drop" op deletes the table.

Then it prints the end-state: what data remains per plane, and confirms each
deploy is still present. Run via eb_aws.sh (spike-localhost venv + thesis profile).
"""
from __future__ import annotations

import functools
import os
import sys

print = functools.partial(print, flush=True)  # noqa: A001

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))   # experiments/experiment_b -> repo root
# _HERE (for the sibling eb_common) + repo root (for the `harness` package).
for _p in (_HERE, WORKSPACE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import boto3  # noqa: E402
import eb_common as C  # noqa: E402


def _bucket_size(s3, bucket, prefix=""):
    p = s3.get_paginator("list_objects_v2")
    n, tot = 0, 0
    for pg in p.paginate(Bucket=bucket, Prefix=prefix):
        for o in pg.get("Contents", []):
            n += 1
            tot += o["Size"]
    return n, tot


def main():
    acct = C.account_id()
    s3 = boto3.client("s3", region_name="us-east-1")

    # --- 1. Blocks: delete corpus + index blocks (keep bucket + functions + ECR) ---
    bkt = f"thesis-blocks-{acct}"
    to_delete = []
    for pref in ("vectors_deep_1m.csv", "indexes/deep_1m/"):
        p = s3.get_paginator("list_objects_v2")
        for pg in p.paginate(Bucket=bkt, Prefix=pref):
            for o in pg.get("Contents", []):
                to_delete.append({"Key": o["Key"]})
    if to_delete:
        for i in range(0, len(to_delete), 1000):
            s3.delete_objects(Bucket=bkt, Delete={"Objects": to_delete[i:i + 1000]})
    print(f"[cleanup/blocks] deleted {len(to_delete)} objects (corpus + index)")

    # --- 2. S3 Vectors: delete the index ---
    try:
        s3v = C.build_client("s3vectors", acct)
        s3v.setup(C.DATASET, dimensions=C.DIMENSIONS, top_k_default=C.TOP_K,
                  create_if_missing=True)
        s3v.teardown(drop_data=True)
        print("[cleanup/s3vectors] index 'deep-1m' deleted")
    except Exception as e:  # noqa: BLE001
        print(f"[cleanup/s3vectors] warning: {type(e).__name__}: {e}")

    # --- 3. LanceDB: drop the table ---
    try:
        ldb = C.build_client("lancedb", acct)
        ldb.setup(C.DATASET, dimensions=C.DIMENSIONS, top_k_default=C.TOP_K,
                  create_if_missing=True)
        ldb.teardown(drop_data=True)
        print("[cleanup/lancedb] table dropped")
    except Exception as e:  # noqa: BLE001
        print(f"[cleanup/lancedb] warning: {type(e).__name__}: {e}")

    # --- 4. verify end-state ---
    print("\n=== END-STATE ===")
    n, tot = _bucket_size(s3, bkt)
    n_corpus, _ = _bucket_size(s3, bkt, "vectors_deep_1m.csv")
    n_idx, _ = _bucket_size(s3, bkt, "indexes/deep_1m/")
    print(f"Blocks bucket {bkt}: {n} objects, {tot/1e6:.1f} MB "
          f"(deep_1m corpus={n_corpus}, deep_1m index={n_idx})")

    lb = f"thesis-lancedb-{acct}"
    n2, tot2 = _bucket_size(s3, lb)
    print(f"LanceDB bucket {lb}: {n2} objects, {tot2/1e6:.1f} MB")

    lam = boto3.client("lambda", region_name="us-east-1")
    for fn in ("thesis-lancedb-lambda",):
        try:
            c = lam.get_function(FunctionName=fn)["Configuration"]
            print(f"Lambda {fn}: PRESENT ({c['State']}, {c['MemorySize']}MB)")
        except Exception as e:  # noqa: BLE001
            print(f"Lambda {fn}: MISSING ({type(e).__name__})")

    # Blocks worker functions (names created by the Blocks deploy start with lithops)
    fns = []
    p = lam.get_paginator("list_functions")
    for pg in p.paginate():
        for f in pg.get("Functions", []):
            nm = f["FunctionName"]
            if "lithops" in nm.lower() or "blocks" in nm.lower():
                fns.append(nm)
    print(f"Blocks/Lithops Lambda functions present: {len(fns)} -> {fns}")

    ecr = boto3.client("ecr", region_name="us-east-1")
    repos = []
    try:
        for r in ecr.describe_repositories().get("repositories", []):
            repos.append(r["repositoryName"])
    except Exception as e:  # noqa: BLE001
        print(f"ecr list warning: {e}")
    print(f"ECR repositories: {repos}")

    # S3 Vectors: list remaining indexes in the vector bucket
    try:
        s3vc = boto3.client("s3vectors", region_name="us-east-1")
        vb = f"thesis-s3v-{acct}"
        idxs = s3vc.list_indexes(vectorBucketName=vb).get("indexes", [])
        print(f"S3 Vectors bucket {vb}: {len(idxs)} indexes -> "
              f"{[i.get('indexName') for i in idxs]}")
    except Exception as e:  # noqa: BLE001
        print(f"S3 Vectors list_indexes: {type(e).__name__}: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
