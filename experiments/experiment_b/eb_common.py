"""
Experiment B — shared helpers for the three-way 1M comparison.

Loaders, recall, a streaming CSV reader, cost models, and the fairness anchors.
Imported by run_recall.py (Phase 2) and run_sweep.py (Phase 3).
"""
from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))   # experiments/experiment_b -> repo root
# Repo root (for the consolidated `harness` package) + the external Blocks repo.
# The concrete adapters are imported lazily in build_client.
for _p in (WORKSPACE, os.path.join(WORKSPACE, "references", "blocks-db-wip")):
    if _p not in sys.path:
        sys.path.insert(0, _p)
DATA_DIR = os.path.join(WORKSPACE, "datasets", "deep_1m")
VECTORS_CSV = os.path.join(DATA_DIR, "vectors_deep_1m.csv")
QUERIES_CSV = os.path.join(DATA_DIR, "queries_deep_1m_1000q.csv")
TRUTH_CSV = os.path.join(DATA_DIR, "true_neighbours_deep_1m_1000q.csv")
# RESULTS_TAG selects the output folder: unset or "orig" -> results/ (the August
# evidence the 2 Sep thesis cites); any other tag -> results_<tag>/ (2026-10-16).
_TAG = os.environ.get("RESULTS_TAG", "orig")
RESULTS_DIR = os.path.join(_HERE, "results" if _TAG in ("", "orig") else f"results_{_TAG}")

# Fairness anchor (finding 17). Every adapter's manifest must report this hash,
# because all three ingest the same source_csv_path bytes.
CORPUS_SHA256 = "aa56f86460774bcaeec4675120c5d07de8000c58a008097abca2e6ccd75d21df"
DIMENSIONS = 96
TOP_K = 10
DATASET = "deep_1m"

# Recall preconditions (correctness gate, NOT a thesis metric). Stop below these.
RECALL_STOP = {"blocks": 95.0, "s3vectors": 70.0, "lancedb": 99.0}
# Configured concurrency levels; the generator caps each at max_safe_concurrency.
CONCURRENCY_LEVELS = [1, 4, 10]

# --- pricing (us-east-1), reused from the S3 Vectors / LanceDB adapter cost banners ---
LAMBDA_X86_GBS = 0.0000166667      # Blocks runtime image is x86_64
LAMBDA_ARM_GBS = 0.0000133334      # LanceDB Lambda is arm64
LAMBDA_REQ_PER_M = 0.20            # $/1M invocations
S3V_PUT_PER_GB = 0.20              # min 128KB/request
S3V_PUT_MIN_KB = 128
S3V_QUERY_PER_M = 2.50             # $/1M QueryVectors requests


def account_id():
    import boto3
    return boto3.client("sts").get_caller_identity()["Account"]


def build_client(adapter, acct, *, put_batch=500, insert_batch=2000):
    """Construct an adapter client. Lazy imports keep each venv self-contained
    (all adapters run from the consolidated harness/.venv)."""
    if adapter == "blocks":
        from harness.adapters.blocks_aws import BlocksClientAWS
        return BlocksClientAWS(bucket=f"thesis-blocks-{acct}",
                               vectors_csv_path=VECTORS_CSV, region="us-east-1")
    if adapter == "s3vectors":
        from harness.adapters.s3vectors import S3VectorsClient
        return S3VectorsClient(f"thesis-s3v-{acct}", region="us-east-1",
                               source_csv_path=VECTORS_CSV, put_batch_size=put_batch)
    if adapter == "lancedb":
        from harness.adapters.lancedb_lambda import LanceDBLambdaClient
        return LanceDBLambdaClient(
            "thesis-lancedb-lambda", region="us-east-1",
            data_bucket=f"thesis-lancedb-{acct}", source_csv_path=VECTORS_CSV,
            image_uri=f"{acct}.dkr.ecr.us-east-1.amazonaws.com/thesis-lancedb-lambda:latest",
            insert_batch_size=insert_batch)
    if adapter == "lancedb_microvm":  # F39: table baked into the MicroVM image, no ingest
        from harness.adapters.lancedb_microvm import build_for_dataset
        # MVM_IMAGE: run another image of this dataset (E1 re-run on server v6, finding 33)
        return build_for_dataset("deep_1m", VECTORS_CSV, acct, image_name=os.environ.get("MVM_IMAGE") or None)
    raise SystemExit(f"unknown adapter {adapter}")


def load_queries(path=QUERIES_CSV):
    """[[float]*96] — one space-separated query per line, no id column."""
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                out.append([float(x) for x in line.split(" ") if x])
    return out


def load_truth(path=TRUTH_CSV):
    """[[int]*10] — exact top-10 neighbour ids per query (comma-separated)."""
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                out.append([int(x) for x in line.split(",") if x != ""])
    return out


def iter_vectors_csv(path=VECTORS_CSV):
    """Stream (int id, [float]*96) per row '<id>,<96 space floats>' — no full load."""
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            id_str, rest = line.split(",", 1)
            yield int(id_str), [float(x) for x in rest.split(" ") if x]


def recall_at_k(results, truth, k=TOP_K):
    """mean over queries of |returned top-k ids ∩ truth top-k ids| / k, as %.

    results[i] = list of (id, dist) from the adapter; truth[i] = list of ids.
    Returns (recall_pct, per_query_hit_counts).
    """
    per_query = []
    for r, g in zip(results, truth):
        got = {int(vid) for vid, _ in r[:k]}
        gset = set(int(x) for x in g[:k])
        per_query.append(len(got & gset))
    denom = k * len(per_query)
    recall = 100.0 * sum(per_query) / denom if denom else 0.0
    return recall, per_query
