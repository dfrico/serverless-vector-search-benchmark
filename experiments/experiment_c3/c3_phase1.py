"""
Experiment C3, Phase 1 — infra sizing + by-name preflight (no ingest, $0).

Two jobs, both cheap:
  1. Blocks 10M sizing. Confirm num_blocks == num_workers == 160 gives ~62,500
     vectors per partition (the same partition size as the proven 1M run), and
     that a map task loading blocks_per_task=8 partitions fits inside the 3008 MB
     Lambda. Record ALL Blocks params to the manifest (finding 22: setup args are
     NOT auto-captured, so a run is not reproducible unless we write them down).
  2. Infra liveness, BY NAME. Finding 24 saw list-* APIs return empty transiently,
     so we check each resource by name (get/describe/head), and only fall back to
     a retried list for the Lithops workers whose names carry a deploy hash.

Budget guard: verify the monthly budget is active and print current spend, so we
start C3 with a known baseline. Phase 1 spends $0 (control-plane reads only).

Run via c3_aws.sh (boto3 only; no lithops/faiss import needed):
    ./c3_aws.sh c3_phase1.py
Writes results/phase1_infra.json.
"""
from __future__ import annotations

import functools
import json
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError

print = functools.partial(print, flush=True)  # noqa: A001

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
import c3_common as K  # noqa: E402

REGION = "us-east-1"
N_VECTORS = 10_000_000
FLOAT_BYTES = 4
LAMBDA_MEM_MB = 3008
# rough .ann footprint multiplier over raw float32 vectors: IVF keeps the vectors
# in inverted lists plus a small centroid table and an id map. ~2.2x is a safe
# over-estimate for the loaded-in-RAM index; the runtime + faiss add ~200 MB.
ANN_OVERHEAD = 2.2
RUNTIME_MB = 250


def sizing():
    p = K.BLOCKS_PARAMS
    per_block = N_VECTORS / p["num_blocks"]
    per_task_vecs = per_block * p["blocks_per_task"]
    map_tasks_per_query = p["num_blocks"] // p["blocks_per_task"]
    raw_mb = per_task_vecs * K.DIMENSIONS * FLOAT_BYTES / 1e6
    est_task_mb = raw_mb * ANN_OVERHEAD + RUNTIME_MB
    fits = est_task_mb < LAMBDA_MEM_MB
    return {
        "num_blocks": p["num_blocks"],
        "num_workers": p["num_workers"],
        "num_workers_equals_num_blocks": p["num_workers"] == p["num_blocks"],
        "partitions_per_block_nlist": p["partitions_per_block"],
        "n_probe": p["n_probe"],
        "blocks_per_task": p["blocks_per_task"],
        "vectors_per_partition": int(per_block),
        "vectors_per_partition_1m_equiv": int(N_VECTORS / 10 / 16),  # 1M / 16 blocks
        "map_tasks_per_query": int(map_tasks_per_query),
        "vectors_loaded_per_map_task": int(per_task_vecs),
        "raw_vectors_mb_per_task": round(raw_mb, 1),
        "est_task_rss_mb": round(est_task_mb, 1),
        "lambda_mem_mb": LAMBDA_MEM_MB,
        "fits_in_lambda": bool(fits),
    }


def check_budget(session, acct):
    b = session.client("budgets")
    try:
        resp = b.describe_budgets(AccountId=acct)
    except ClientError as e:
        return {"ok": False, "error": str(e)}
    out = []
    for bud in resp.get("Budgets", []):
        spend = (bud.get("CalculatedSpend") or {}).get("ActualSpend") or {}
        out.append({
            "name": bud.get("BudgetName"),
            "limit_usd": (bud.get("BudgetLimit") or {}).get("Amount"),
            "actual_spend_usd": spend.get("Amount"),
            "health": (bud.get("HealthStatus") or {}).get("Status"),
        })
    return {"ok": True, "budgets": out}


def check_lambda_by_name(lam, name):
    try:
        c = lam.get_function(FunctionName=name)["Configuration"]
        return {"present": True, "state": c.get("State"),
                "mem_mb": c.get("MemorySize"), "timeout_s": c.get("Timeout"),
                "arch": (c.get("Architectures") or ["?"])[0]}
    except ClientError as e:
        return {"present": False, "error": e.response["Error"]["Code"]}


def check_ecr_by_name(ecr, name):
    try:
        r = ecr.describe_repositories(repositoryNames=[name])["repositories"][0]
        imgs = ecr.describe_images(repositoryName=name).get("imageDetails", [])
        tags = sorted({t for im in imgs for t in (im.get("imageTags") or [])})
        return {"present": True, "uri": r["repositoryUri"], "n_images": len(imgs),
                "tags": tags}
    except ClientError as e:
        return {"present": False, "error": e.response["Error"]["Code"]}


def discover_lithops_workers(lam, retries=3, wait=5):
    """List worker Lambdas by name filter, with a retry guard: finding 24 saw
    list-* return empty transiently, so an empty first page is retried."""
    for attempt in range(1, retries + 1):
        names = []
        paginator = lam.get_paginator("list_functions")
        for page in paginator.paginate():
            for fn in page["Functions"]:
                n = fn["FunctionName"]
                if "lithops-worker" in n or "lithops_worker" in n:
                    names.append(n)
        if names:
            return {"present": True, "names": sorted(names), "attempts": attempt}
        if attempt < retries:
            print(f"[c3/p1] lithops worker list empty (attempt {attempt}); retry in {wait}s")
            time.sleep(wait)
    return {"present": False, "names": [], "attempts": retries}


def check_s3v(session, bucket):
    try:
        c = session.client("s3vectors", region_name=REGION)
    except Exception as e:  # noqa: BLE001 — service may be unavailable in some regions
        return {"present": False, "error": f"no s3vectors client: {e}"}
    try:
        c.get_vector_bucket(vectorBucketName=bucket)
        return {"present": True, "bucket": bucket}
    except ClientError as e:
        return {"present": False, "bucket": bucket,
                "error": e.response["Error"]["Code"]}


def check_bucket(s3, bucket):
    try:
        s3.head_bucket(Bucket=bucket)
        return {"present": True}
    except ClientError as e:
        return {"present": False, "error": e.response["Error"]["Code"]}


def check_dynamo(session, table):
    try:
        d = session.client("dynamodb").describe_table(TableName=table)["Table"]
        return {"present": True, "status": d["TableStatus"]}
    except ClientError as e:
        return {"present": False, "error": e.response["Error"]["Code"]}


def main():
    os.makedirs(K.RESULTS_DIR, exist_ok=True)
    session = boto3.Session(region_name=REGION)
    ident = session.client("sts").get_caller_identity()
    acct = ident["Account"]
    print(f"[c3/p1] account={acct} arn={ident['Arn']}")
    assert acct == os.environ.get("AWS_ACCOUNT_ID", acct), f"wrong account {acct} (AWS_ACCOUNT_ID)"
    print("[c3/p1] spend estimate for THIS phase: $0.00 (control-plane reads only)")

    lam = session.client("lambda")
    ecr = session.client("ecr")
    s3 = session.client("s3")

    sz = sizing()
    print(f"[c3/p1] Blocks sizing: {sz['vectors_per_partition']} vecs/partition "
          f"(1M-equiv {sz['vectors_per_partition_1m_equiv']}), "
          f"{sz['map_tasks_per_query']} map tasks/query, "
          f"est task RSS {sz['est_task_rss_mb']}MB / {LAMBDA_MEM_MB}MB "
          f"fits={sz['fits_in_lambda']}")
    if not sz["fits_in_lambda"]:
        print("STOP: Blocks map task would not fit in the 3008MB Lambda.")
    if not sz["num_workers_equals_num_blocks"]:
        print("STOP: num_workers != num_blocks — Blocks would silently under-recall.")

    infra = {
        "blocks_bucket": check_bucket(s3, f"thesis-blocks-{acct}"),
        "blocks_dynamodb_BlocksDB_default": check_dynamo(session, "BlocksDB-default"),
        "blocks_ecr_runtime": check_ecr_by_name(ecr, "lithops_v363_rb26/blocks-db-runtime"),
        "blocks_lithops_workers": discover_lithops_workers(lam),
        "lancedb_lambda": check_lambda_by_name(lam, "thesis-lancedb-lambda"),
        "lancedb_ecr": check_ecr_by_name(ecr, "thesis-lancedb-lambda"),
        "lancedb_bucket": check_bucket(s3, f"thesis-lancedb-{acct}"),
        "s3vectors_bucket": check_s3v(session, f"thesis-s3v-{acct}"),
    }
    for k, v in infra.items():
        present = v.get("present")
        print(f"[c3/p1] {k}: {'OK' if present else 'MISSING'} {v}")

    budget = check_budget(session, acct)
    if budget.get("ok"):
        for b in budget["budgets"]:
            print(f"[c3/p1] budget {b['name']}: spend ${b['actual_spend_usd']} / "
                  f"${b['limit_usd']} health={b['health']}")

    # Blocks needs bucket + dynamo + runtime image + at least the two worker lambdas.
    blocks_ok = (infra["blocks_bucket"]["present"]
                 and infra["blocks_dynamodb_BlocksDB_default"]["present"]
                 and infra["blocks_ecr_runtime"]["present"]
                 and infra["blocks_lithops_workers"]["present"])
    lancedb_ok = (infra["lancedb_lambda"]["present"]
                  and infra["lancedb_ecr"]["present"]
                  and infra["lancedb_bucket"]["present"])
    s3v_ok = infra["s3vectors_bucket"]["present"]
    all_ok = (blocks_ok and lancedb_ok and s3v_ok
              and sz["fits_in_lambda"] and sz["num_workers_equals_num_blocks"])

    manifest = {
        "phase": "c3_phase1",
        "account": acct,
        "region": REGION,
        "dataset": K.DATASET,
        "corpus_sha256_anchor": K.read_corpus_sha256(),
        "blocks_params": K.BLOCKS_PARAMS,
        "blocks_sizing": sz,
        "concurrency_cap": K.CONCURRENCY_CAP,
        "recall_stop": K.RECALL_STOP,
        "infra": infra,
        "infra_ok": {"blocks": blocks_ok, "lancedb": lancedb_ok, "s3vectors": s3v_ok},
        "budget": budget,
        "all_ok": bool(all_ok),
    }
    out = os.path.join(K.RESULTS_DIR, "phase1_infra.json")
    with open(out + ".tmp", "w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    os.replace(out + ".tmp", out)
    print(f"[c3/p1] wrote {out}")
    print(f"\n=== C3 PHASE 1 {'PASS' if all_ok else 'ATTENTION NEEDED'} ===")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
