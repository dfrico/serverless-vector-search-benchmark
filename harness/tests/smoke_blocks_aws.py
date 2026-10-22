"""
Bridge task, Phase 4 — AWS end-to-end smoke for Blocks.

Proves Blocks runs on real AWS Lambda + S3 for the first time:
  1. ingest the bundled 100k corpus (S3 upload + Lithops/aws_lambda index build),
  2. run 10 queries (top_k=10) through the same aws_lambda data plane,
  3. assert recall@10 == 100% against the bundled ground truth.

100k, NOT 1M — this is a bring-up smoke, not a benchmark. If recall < 100%,
the script STOPS with a non-zero exit and does NOT proceed.

Run:  AWS_PROFILE=thesis harness/.venv/bin/python harness/tests/smoke_blocks_aws.py
(no MinIO env).
"""
import csv
import json
import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))
if WORKSPACE not in sys.path:
    sys.path.insert(0, WORKSPACE)

from harness.adapters.blocks_aws import BlocksClientAWS  # noqa: E402
import boto3

DATA_DIR = os.path.join(WORKSPACE, "spike-localhost")
QUERIES_CSV = os.path.join(DATA_DIR, "queries_deep_100k.csv")
GROUND_TRUTH_CSV = os.path.join(DATA_DIR, "true_neighbours_deep_100k.csv")
VECTORS_CSV = os.path.join(DATA_DIR, "vectors_deep_100k.csv")

BUCKET = f"thesis-blocks-{boto3.client('sts').get_caller_identity()['Account']}"
DATASET = "deep_100k"  # index blocks live under indexes/deep_100k/ — deleted at cleanup
N_QUERIES = 10
TOP_K = 10


def load_queries(path, n):
    out = []
    with open(path) as f:
        for row in csv.reader(f):
            if not row:
                continue
            out.append([float(x) for x in row[0].split(" ") if x])
            if len(out) >= n:
                break
    return out


def load_ground_truth(path, n):
    out = []
    with open(path) as f:
        for row in csv.reader(f):
            out.append([int(x) for x in row if x != ""])
            if len(out) >= n:
                break
    return out


def recall_at_k(results, ground_truth):
    hit = tot = 0
    for r, g in zip(results, ground_truth):
        got = {vid for vid, _ in r}
        gset = set(g)
        hit += len(got & gset)
        tot += len(gset)
    return 100.0 * hit / tot if tot else 0.0


def main():
    t_start = time.time()
    queries = load_queries(QUERIES_CSV, N_QUERIES)
    ground_truth = load_ground_truth(GROUND_TRUTH_CSV, N_QUERIES)
    print(f"loaded {len(queries)} queries, {len(ground_truth)} ground-truth rows")

    client = BlocksClientAWS(bucket=BUCKET, vectors_csv_path=VECTORS_CSV)
    client.setup(DATASET, dimensions=96, top_k_default=TOP_K, create_if_missing=True)
    print(f"[smoke] setup OK — bucket={BUCKET} dataset={DATASET} "
          f"num_blocks={client.num_blocks} nlist={client.partitions_per_block} "
          f"n_probe={client.n_probe} num_workers={client._DEFAULT_NUM_WORKERS}")

    # --- ingest: S3 upload + aws_lambda index build ---
    t_ing = time.time()
    _, ing_timings = client.bulk_insert_from_path(VECTORS_CSV, build_index=True)
    ingest_s = time.time() - t_ing
    print(f"[smoke] ingest done in {ingest_s:.1f}s — "
          f"upload_s={ing_timings.get('bulk_insert_upload_s'):.1f} "
          f"index_s={ing_timings.get('bulk_insert_index_s'):.1f}")

    idx = client._client.list_indexes(DATASET)
    print(f"[smoke] list_indexes('{DATASET}') = {idx}")

    # --- query: 10 queries, top_k=10 ---
    t_q = time.time()
    results, q_timings = client.query_batch(queries, top_k=TOP_K)
    query_s = time.time() - t_q
    print(f"[smoke] {len(results)} query results in {query_s:.1f}s")

    non_empty = sum(1 for r in results if r)
    recall = recall_at_k(results, ground_truth)
    print(f"[smoke] non_empty={non_empty}/{len(results)}  recall@{TOP_K} = {recall:.2f}%")

    passed = (
        len(results) == len(queries)
        and non_empty == len(queries)
        and abs(recall - 100.0) < 1e-9
    )

    out = {
        "passed": passed,
        "recall_at_10": recall,
        "n_queries": len(queries),
        "non_empty": non_empty,
        "ingest_s": ingest_s,
        "query_s": query_s,
        "wall_clock_s": time.time() - t_start,
        "index_blocks": idx,
        "backend": "aws_lambda",
        "bucket": BUCKET,
        "dataset": DATASET,
    }
    with open(os.path.join(_HERE, "results", "aws_smoke_result.json"), "w") as f:
        json.dump(out, f, indent=2, default=str)

    print("\n=== AWS SMOKE", "PASS" if passed else "FAIL", "===")
    if not passed:
        print("recall < 100% or empty/short results — STOPPING per Phase 4 rule.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
