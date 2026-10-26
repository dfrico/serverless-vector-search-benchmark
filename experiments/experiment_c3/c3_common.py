"""
Experiment C3 — shared helpers for the 10M-scale three-way comparison.

C3 is the 10M ramp of Experiment B (finding 20) / C2 (finding 24). It asks: does
the cost and latency ranking (S3 Vectors << LanceDB < Blocks) hold at 10M, and do
any adapters change shape at scale?

This module mirrors c2_common but points at the deep_10m corpus and pins the
Blocks 10M sizing. It is import-safe in the consolidated harness/.venv: every
adapter import is lazy.

Cost model: billed, not wall (finding 22 / methodology-decisions). Report the sum
of per-task Lambda GB-seconds + request fees, not wall x memory x rate.

Blocks 10M sizing (finding 22 sweet spot, scaled 10x):
  num_blocks = num_workers = 160   -> ~62,500 vectors/partition, same partition
                                       size as the 1M run (num_blocks=16). Keeping
                                       partition size fixed keeps per-task memory
                                       and search time in the proven envelope.
  partitions_per_block = 10, n_probe = 7, blocks_per_task = 8  -> 20 map tasks per
                                       query (160 / 8). n_probe is baked at build
                                       time, so pin_params must run before build.
  IMPORTANT (finding 22 foot-gun): num_workers MUST equal num_blocks. If
  num_workers < num_blocks, some blocks are never searched and recall collapses
  silently. Setup args are NOT auto-captured, so record all of them in the manifest.
"""
from __future__ import annotations

import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))   # experiments/experiment_c3 -> repo root
# RESULTS_TAG selects the output folder: unset or "orig" -> results/ (the August
# evidence the 2 Sep thesis cites); any other tag -> results_<tag>/ (2026-10-16).
_TAG = os.environ.get("RESULTS_TAG", "orig")
RESULTS_DIR = os.path.join(_HERE, "results" if _TAG in ("", "orig") else f"results_{_TAG}")

# Put the repo root (for the `harness` package), the sibling experiment_b/
# experiment_c1 dirs (for eb_common / c1_common), and the external Blocks repo on
# path so everything imports cleanly in either venv.
for _p in (WORKSPACE,
           os.path.join(WORKSPACE, "experiments", "experiment_b"),
           os.path.join(WORKSPACE, "experiments", "experiment_c1"),
           os.path.join(WORKSPACE, "references", "blocks-db-wip")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import eb_common as C  # noqa: E402

# --- deep_10m corpus paths (override eb_common's deep_1m paths) ---
# C3_DATASET: re-ingest the same corpus under another name (2026-10-18 LanceDB 10M ingest
# re-measure into deep_10m_reingest) without touching the measured deep_10m table.
DATASET = os.environ.get("C3_DATASET", "deep_10m")
DATA_DIR = os.path.join(WORKSPACE, "datasets", "deep_10m")
VECTORS_CSV = os.path.join(DATA_DIR, "vectors_deep_10m.csv")
QUERIES_CSV = os.path.join(DATA_DIR, "queries_deep_10m_1000q.csv")
TRUTH_CSV = os.path.join(DATA_DIR, "true_neighbours_deep_10m_1000q.csv")

DIMENSIONS = C.DIMENSIONS   # 96
TOP_K = C.TOP_K             # 10

# Fairness anchor for the 10M corpus. Filled in from Phase 0's fingerprint after
# datasets/deep_10m/vectors_deep_10m.csv is built (see set_corpus_anchor below).
CORPUS_SHA256 = "72e0840f1f4605ac5d8715422f3d77154b310414e908ce6768b1bcebda0aa849"  # deep_10m Phase 0 anchor

# Recall preconditions for C3 (from the brief). Stop the experiment below these.
RECALL_STOP = {"blocks": 95.0, "s3vectors": 70.0, "lancedb": 99.0}

# Blocks 10M sizing (see module docstring). num_workers == num_blocks (mandatory).
BLOCKS_PARAMS = dict(n_probe=7, num_blocks=160, partitions_per_block=10,
                     blocks_per_task=8, num_workers=160)

# Lambda memory for the billed cost model.
LANCEDB_MEM_GB = 2.048
BLOCKS_MEM_GB = 3.008

# Concurrency caps per adapter (from Experiment B capabilities). Blocks is serial.
# lancedb_microvm: concurrent-connection limit of the 8 GB (4 vCPU) MicroVM, a documented quota (F39).
CONCURRENCY_CAP = {"blocks": 1, "s3vectors": 4, "lancedb": 10, "lancedb_microvm": 32}


def acct():
    return C.account_id()


# --- corpus loaders bound to the deep_10m paths ---
def load_queries():
    return C.load_queries(QUERIES_CSV)


def load_truth():
    return C.load_truth(TRUTH_CSV)


def iter_vectors_csv():
    return C.iter_vectors_csv(VECTORS_CSV)


def recall_at_k(results, truth, k=TOP_K):
    return C.recall_at_k(results, truth, k)


def read_corpus_sha256():
    """Read the deep_10m corpus content_sha256 from Phase 0's sidecar, if present.
    Phase 0 writes datasets/deep_10m/corpus_sha256.txt so the anchor is not
    hand-copied. Returns None if the sidecar is missing."""
    p = os.path.join(DATA_DIR, "corpus_sha256.txt")
    if os.path.exists(p):
        with open(p) as f:
            return f.read().strip()
    return None


def build_client(adapter, a=None):
    """Construct an adapter client bound to the deep_10m corpus CSV.

    We do not reuse eb_common.build_client because it hardcodes the deep_1m CSV;
    the source_csv_path anchor must point at the 10M corpus so each manifest
    records the 10M content_sha256.
    """
    a = a or acct()
    if adapter == "blocks":
        # Retry-on-throttle for the Lambda invoker so a 160-way build (and the
        # 20-task-per-query fan-out) drains within this account's concurrency cap
        # of 10 instead of dying on a 429. See c3_lithops_patch for the why.
        import c3_lithops_patch
        c3_lithops_patch.apply()
        from harness.adapters.blocks_aws import BlocksClientAWS
        return BlocksClientAWS(bucket=f"thesis-blocks-{a}",
                               vectors_csv_path=VECTORS_CSV, region="us-east-1")
    if adapter == "s3vectors":
        from harness.adapters.s3vectors import S3VectorsClient
        return S3VectorsClient(f"thesis-s3v-{a}", region="us-east-1",
                               source_csv_path=VECTORS_CSV, put_batch_size=500)
    if adapter == "lancedb":
        from harness.adapters.lancedb_lambda import LanceDBLambdaClient
        return LanceDBLambdaClient(
            "thesis-lancedb-lambda", region="us-east-1",
            data_bucket=f"thesis-lancedb-{a}", source_csv_path=VECTORS_CSV,
            image_uri=f"{a}.dkr.ecr.us-east-1.amazonaws.com/thesis-lancedb-lambda:latest",
            insert_batch_size=2000)
    if adapter == "lancedb_microvm":  # F39: table baked into the MicroVM image, no ingest
        from harness.adapters.lancedb_microvm import build_for_dataset
        # MVM_IMAGE: run another image of this dataset (E1 re-run on server v6, finding 33)
        return build_for_dataset("deep_10m", VECTORS_CSV, a, image_name=os.environ.get("MVM_IMAGE") or None)
    raise SystemExit(f"unknown adapter {adapter}")


def build_and_setup(adapter, *, create_if_missing):
    """Construct + setup one adapter against the deep_10m corpus. For Blocks, pin
    the 10M params after setup (setup would otherwise hydrate old config.json)."""
    client = build_client(adapter)
    setup_kwargs = dict(dimensions=DIMENSIONS, top_k_default=TOP_K,
                        create_if_missing=create_if_missing)
    if adapter == "blocks":
        setup_kwargs["num_blocks"] = BLOCKS_PARAMS["num_blocks"]
        client.setup(DATASET, **setup_kwargs)
        import c1_common
        c1_common.pin_params(client, **BLOCKS_PARAMS)
    else:
        client.setup(DATASET, **setup_kwargs)
    return client


def jsonl_append(path, obj):
    with open(path, "a") as f:
        f.write(json.dumps(obj, default=str) + "\n")
