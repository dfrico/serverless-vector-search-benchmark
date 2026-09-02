#!/usr/bin/env python3
"""
Build exact top-10 ground truth for the 1M Deep1B sample (experiment A, phase 1).

This is local only: no AWS, no adapter code touched. It produces the two files the
Blocks recall probe needs at 1M:

  1. datasets/deep_1m/true_neighbours_deep_1m_1000q.csv
     1000 rows, each 10 comma-separated ints — the exact top-10 neighbour IDs for
     the first 1000 query vectors, in the CSV's re-numbered ID space (ids 0..999999).
     Same shape as the bundled true_neighbours_deep_100k.csv, which the day-2 recall
     check consumed (harness-day2/smoke_test.py:load_ground_truth).

  2. datasets/deep_1m/queries_deep_1m_1000q.csv
     1000 rows, each 96 space-separated floats, NO id column — the Blocks/serverless
     query format (same as queries_deep_100k.csv).

Why our own ground truth
------------------------
The public groundtruth.public.10K.ibin references the ORIGINAL 1B corpus row
indices (ids up to ~1e9), not this 1M sample's re-numbered ids. So it cannot be
used directly. We compute fresh, self-consistent ground truth with an exact flat
L2 search over the 1M sample — the path serverless-vdb/get_true_results.py uses,
and path (b) recommended in findings/17_dataset_sampling.md.

Faithfulness
------------
We parse the SAME CSV bytes Blocks will ingest (datasets/deep_1m/vectors_deep_1m.csv),
not the .fbin, so the ground truth matches exactly what Blocks indexes. IDs in the
CSV are sequential from 0 in row order, so CSV row i has id i; we assert this.

The query vectors are read from query.public.10K.fbin (the first 1000), then written
back out as text so the recall run reads the identical query bytes the ground truth
was built on.

Metric
------
faiss.IndexFlatL2 ranks by squared L2. Blocks and LanceDB also use squared L2;
S3 Vectors uses true euclidean. Ranking is identical either way (finding 13), and
recall compares ID sets, so the metric choice does not bias the ground truth.

Usage
-----
    harness-day1/.venv/bin/python scripts/build_ground_truth.py
    harness-day1/.venv/bin/python scripts/build_ground_truth.py --verify
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone

import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(_HERE)
# Reuse the dataset fingerprint contract (finding 13) so hashes match the manifest.
sys.path.insert(0, os.path.join(WORKSPACE, "harness-day6"))
from dataset_hash import fingerprint_csv  # noqa: E402

VECTORS_CSV = os.path.join(WORKSPACE, "datasets", "deep_1m", "vectors_deep_1m.csv")
QUERIES_FBIN = os.path.join(WORKSPACE, "datasets", "deep_1m", "query.public.10K.fbin")
OUT_GT = os.path.join(WORKSPACE, "datasets", "deep_1m",
                      "true_neighbours_deep_1m_1000q.csv")
OUT_QUERIES = os.path.join(WORKSPACE, "datasets", "deep_1m",
                           "queries_deep_1m_1000q.csv")

EXPECT_DIMS = 96
N_VECTORS = 1_000_000
N_QUERIES = 1_000            # first 1000 queries, matching the 100k precedent
TOP_K = 10


def read_fbin_header(path: str) -> tuple[int, int]:
    """Read the 8-byte .fbin header -> (n_points, n_dims). Little-endian int32."""
    with open(path, "rb") as f:
        hdr = np.frombuffer(f.read(8), dtype="<i4")
    return int(hdr[0]), int(hdr[1])


def open_fbin(path: str, n_points: int, n_dims: int) -> np.memmap:
    return np.memmap(path, dtype="<f4", mode="r", offset=8,
                     shape=(n_points, n_dims))


def load_vectors_csv(path: str) -> np.ndarray:
    """
    Parse <id>,<96 space floats> rows into a (N_VECTORS, 96) float32 array.
    Asserts ids are sequential from 0 (row i has id i), so the array index IS
    the id — which is what the ground-truth neighbour ids must be.
    """
    if not os.path.exists(path):
        raise SystemExit(f"vectors CSV not found: {path}")
    print(f"[load] parsing {path} ...")
    vecs = np.empty((N_VECTORS, EXPECT_DIMS), dtype=np.float32)
    t0 = time.perf_counter()
    with open(path) as f:
        for i, line in enumerate(f):
            rid_str, rest = line.split(",", 1)
            if int(rid_str) != i:
                raise SystemExit(
                    f"row {i} has non-sequential id {rid_str}; the ground-truth "
                    f"id space assumes row order == id. STOP.")
            row = np.fromstring(rest, dtype=np.float32, sep=" ")
            if row.shape[0] != EXPECT_DIMS:
                raise SystemExit(
                    f"row {i} has {row.shape[0]} floats, expected {EXPECT_DIMS}. STOP.")
            vecs[i] = row
            if (i + 1) % 100_000 == 0:
                print(f"[load]   {i + 1:,}/{N_VECTORS:,} rows", end="\r", flush=True)
    n_read = i + 1
    if n_read != N_VECTORS:
        raise SystemExit(f"read {n_read} rows, expected {N_VECTORS}. STOP.")
    print(f"\n[load] {n_read:,} vectors in {time.perf_counter() - t0:.1f}s")
    return vecs


def load_queries(path: str, n: int) -> np.ndarray:
    """Read the first n query vectors from the .fbin as a (n, 96) float32 array."""
    npts, ndims = read_fbin_header(path)
    print(f"[queries] {path} header = ({npts}, {ndims})")
    if ndims != EXPECT_DIMS:
        raise SystemExit(f"query dims {ndims} != {EXPECT_DIMS}. STOP.")
    if npts < n:
        raise SystemExit(f"query file has {npts} < {n} vectors. STOP.")
    arr = open_fbin(path, npts, ndims)
    q = np.ascontiguousarray(arr[:n], dtype=np.float32)
    if not np.isfinite(q).all():
        raise SystemExit("query vectors contain non-finite values. STOP.")
    print(f"[queries] loaded first {n} of {npts} query vectors, shape {q.shape}")
    return q


def compute_ground_truth(vecs: np.ndarray, queries: np.ndarray):
    """Exact top-TOP_K via faiss.IndexFlatL2. Returns (neighbour_ids, faiss_version)."""
    import faiss
    print(f"[faiss] version {faiss.__version__}")
    t0 = time.perf_counter()
    index = faiss.IndexFlatL2(EXPECT_DIMS)
    index.add(vecs)
    print(f"[faiss] added {index.ntotal:,} vectors in {time.perf_counter() - t0:.1f}s")
    t1 = time.perf_counter()
    _dist, ids = index.search(queries, TOP_K)
    print(f"[faiss] searched {queries.shape[0]} queries (k={TOP_K}) in "
          f"{time.perf_counter() - t1:.1f}s")
    return ids, faiss.__version__


def write_gt_csv(ids: np.ndarray, path: str) -> None:
    """1000 rows, each TOP_K comma-separated ints (neighbour ids in the CSV id space)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="\n") as f:
        for row in ids:
            f.write(",".join(str(int(x)) for x in row))
            f.write("\n")
    print(f"[write] ground truth -> {path}")


def write_queries_csv(queries: np.ndarray, path: str) -> None:
    """1000 rows, each 96 space-separated floats, NO id (Blocks query format)."""
    strs = queries.astype(str)                    # shortest round-trip float32 repr
    with open(path, "w", newline="\n") as f:
        for j in range(strs.shape[0]):
            f.write(" ".join(strs[j]))
            f.write("\n")
    print(f"[write] queries -> {path}")


def verify_outputs(gt_path: str, q_path: str) -> bool:
    """1000 rows each; every gt id in [0, N_VECTORS); no dup within a query's top-10."""
    ok = True
    # ground truth
    gt_rows = []
    with open(gt_path) as f:
        for line in f:
            gt_rows.append([int(x) for x in line.strip().split(",") if x != ""])
    print(f"[check] ground-truth rows: {len(gt_rows)} (expected {N_QUERIES})")
    ok = ok and len(gt_rows) == N_QUERIES
    bad_range = bad_len = bad_dup = 0
    for r in gt_rows:
        if len(r) != TOP_K:
            bad_len += 1
        if any(not (0 <= v < N_VECTORS) for v in r):
            bad_range += 1
        if len(set(r)) != len(r):
            bad_dup += 1
    print(f"[check] rows != {TOP_K} ids: {bad_len}; ids out of [0,{N_VECTORS}): "
          f"{bad_range}; rows with duplicate ids: {bad_dup}")
    ok = ok and bad_len == 0 and bad_range == 0 and bad_dup == 0
    # queries
    with open(q_path) as f:
        q_lines = [ln for ln in f]
    print(f"[check] query rows: {len(q_lines)} (expected {N_QUERIES})")
    ok = ok and len(q_lines) == N_QUERIES
    n_floats = len(q_lines[0].strip().split(" ")) if q_lines else 0
    print(f"[check] floats in query row 0: {n_floats} (expected {EXPECT_DIMS})")
    ok = ok and n_floats == EXPECT_DIMS
    return ok


def do_build(args) -> int:
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    t_all = time.perf_counter()
    vecs = load_vectors_csv(args.vectors)
    queries = load_queries(args.queries_fbin, args.n_queries)
    ids, faiss_version = compute_ground_truth(vecs, queries)
    write_gt_csv(ids, args.out_gt)
    write_queries_csv(queries, args.out_queries)
    ok = verify_outputs(args.out_gt, args.out_queries)
    fp_gt = fingerprint_csv(args.out_gt)
    fp_q = fingerprint_csv(args.out_queries)
    wall = time.perf_counter() - t_all
    print("\n=== phase 1 record ===")
    print(f"script: {os.path.relpath(os.path.abspath(__file__), WORKSPACE)}")
    print(f"faiss_version: {faiss_version}")
    print(f"started_utc: {started}")
    print(f"wall_clock_s: {wall:.1f}")
    print(f"n_vectors: {vecs.shape[0]}  n_queries: {queries.shape[0]}  top_k: {TOP_K}")
    print(f"ground_truth: {os.path.relpath(args.out_gt, WORKSPACE)}")
    print(f"  content_sha256: {fp_gt['content_sha256']}")
    print(f"  line_count: {fp_gt['line_count']}  byte_size: {fp_gt['byte_size']}")
    print(f"queries: {os.path.relpath(args.out_queries, WORKSPACE)}")
    print(f"  content_sha256: {fp_q['content_sha256']}")
    print(f"  line_count: {fp_q['line_count']}  byte_size: {fp_q['byte_size']}")
    print("\nPASS" if ok else "\nFAIL")
    return 0 if ok else 1


def do_verify(args) -> int:
    if not (os.path.exists(args.out_gt) and os.path.exists(args.out_queries)):
        raise SystemExit("output files missing; run without --verify first")
    ok = verify_outputs(args.out_gt, args.out_queries)
    fp_gt = fingerprint_csv(args.out_gt)
    fp_q = fingerprint_csv(args.out_queries)
    print(f"[verify] ground truth content_sha256: {fp_gt['content_sha256']}")
    print(f"[verify] queries      content_sha256: {fp_q['content_sha256']}")
    print("\nVERIFY PASS" if ok else "\nVERIFY FAIL")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--vectors", default=VECTORS_CSV)
    ap.add_argument("--queries-fbin", default=QUERIES_FBIN, dest="queries_fbin")
    ap.add_argument("--out-gt", default=OUT_GT, dest="out_gt")
    ap.add_argument("--out-queries", default=OUT_QUERIES, dest="out_queries")
    ap.add_argument("--n-queries", type=int, default=N_QUERIES, dest="n_queries")
    ap.add_argument("--verify", action="store_true")
    args = ap.parse_args()
    return do_verify(args) if args.verify else do_build(args)


if __name__ == "__main__":
    raise SystemExit(main())
