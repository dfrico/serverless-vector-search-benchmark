#!/usr/bin/env python3
"""
Experiment C3, Phase 0 — build the deep_10m dataset (local, no AWS spend).

This is the 10M-scale ramp of the three-way comparison. Phase 0 makes the corpus,
the queries, and fresh ground truth, all local. It reuses the finding-17 .fbin
reader and the finding-13 CSV fingerprint contract, so the corpus anchor
(content_sha256) is computed the SAME way each adapter's run manifest computes it.

What it makes (all under datasets/deep_10m/):
  1. vectors_deep_10m.csv     — ALL 10,000,000 base vectors, in .fbin row order,
                                sequential IDs from 0. Same row shape as
                                vectors_deep_1m.csv: `<id>,<96 space floats>`.
  2. queries_deep_10m_1000q.csv — the 1000 query vectors, IDENTICAL to the 1M
                                queries file (we copy its bytes). This keeps the
                                query set fixed across the 1M and 10M experiments.
  3. true_neighbours_deep_10m_1000q.csv — exact top-10 neighbour IDs per query,
                                from a FAISS IndexFlatL2 over ALL 10M vectors.
                                The public groundtruth is against the full 1B
                                corpus, so it is unusable here (finding 17).
  4. README.md                — provenance, sizes, content_sha256, gt-gen time.

Why ALL 10M, not a sample: the 1M experiment sampled 1M of 10M. The 10M ramp uses
the whole base set, so the corpus is a strict superset and the scale ratio is
exactly 10x. IDs are the .fbin row index (0..9,999,999); because we write rows in
order, the FAISS positional index equals the CSV id, so ground-truth ids line up.

.fbin format (big-ann-benchmarks): 8-byte header (int32 n_points, int32 n_dims),
then contiguous float32, row-major, no per-vector prefix. base.10M reads
(10_000_000, 96).

Usage:
    python c3_phase0.py            # build everything + verify + README
    python c3_phase0.py --verify   # re-check the produced files, no rebuild
    python c3_phase0.py --skip-vectors   # (dev) skip the CSV, do gt/queries only
"""
from __future__ import annotations

import argparse
import functools
import os
import shutil
import sys
import time
from datetime import datetime, timezone

import numpy as np

print = functools.partial(print, flush=True)  # noqa: A001

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))   # experiments/experiment_c3 -> repo root
# finding-13 fingerprint contract — same hash the run manifests will record.
if WORKSPACE not in sys.path:                          # repo root for the `harness` package
    sys.path.insert(0, WORKSPACE)
from harness.dataset_hash import fingerprint_csv  # noqa: E402

SOURCE_FBIN = os.path.join(WORKSPACE, "references", "deep1b", "base.10M.fbin")
# queries are identical to the 1M experiment; copy those exact bytes.
SRC_QUERIES = os.path.join(WORKSPACE, "datasets", "deep_1m",
                           "queries_deep_1m_1000q.csv")

OUT_DIR = os.path.join(WORKSPACE, "datasets", "deep_10m")
OUT_VECTORS = os.path.join(OUT_DIR, "vectors_deep_10m.csv")
OUT_QUERIES = os.path.join(OUT_DIR, "queries_deep_10m_1000q.csv")
OUT_TRUTH = os.path.join(OUT_DIR, "true_neighbours_deep_10m_1000q.csv")
OUT_README = os.path.join(OUT_DIR, "README.md")
OUT_SHA_SIDECAR = os.path.join(OUT_DIR, "corpus_sha256.txt")

SOURCE_URL = ("https://storage.yandexcloud.net/yandex-research/"
              "ann-datasets/DEEP/base.10M.fbin")
EXPECT_POINTS = 10_000_000
EXPECT_DIMS = 96
TOP_K = 10
N_QUERIES = 1000
WRITE_CHUNK = 100_000     # CSV rows per write batch (bounds peak memory)
ADD_CHUNK = 1_000_000     # vectors per FAISS add (bounds peak memory)


def read_fbin_header(path: str) -> tuple[int, int]:
    with open(path, "rb") as f:
        hdr = np.frombuffer(f.read(8), dtype="<i4")
    return int(hdr[0]), int(hdr[1])


def open_fbin(path: str, n_points: int, n_dims: int) -> np.memmap:
    """Memmap the float32 body after the 8-byte header. No copy to RAM."""
    return np.memmap(path, dtype="<f4", mode="r", offset=8,
                     shape=(n_points, n_dims))


def verify_source(path: str) -> tuple[int, int]:
    """Header must read (10M, 96); file size must match; first 2 rows finite."""
    if not os.path.exists(path):
        raise SystemExit(
            f"source .fbin not found: {path}\n"
            f"download it first:\n  curl -L '{SOURCE_URL}' -o '{path}'")
    n, d = read_fbin_header(path)
    print(f"[verify] header: n_points={n} n_dims={d}")
    if (n, d) != (EXPECT_POINTS, EXPECT_DIMS):
        raise SystemExit(f"header {(n, d)} != {(EXPECT_POINTS, EXPECT_DIMS)}. STOP.")
    expect_bytes = 8 + n * d * 4
    actual_bytes = os.path.getsize(path)
    print(f"[verify] file size: {actual_bytes} bytes (expected {expect_bytes})")
    if actual_bytes != expect_bytes:
        raise SystemExit(f"size {actual_bytes} != {expect_bytes} — truncated. STOP.")
    arr = open_fbin(path, n, d)
    first2 = np.asarray(arr[:2])
    print(f"[verify] first 2 vectors: shape={first2.shape} "
          f"finite={bool(np.isfinite(first2).all())}")
    print(f"[verify]   vec0[:4]={first2[0, :4].tolist()}")
    print(f"[verify]   vec1[:4]={first2[1, :4].tolist()}")
    if first2.shape != (2, EXPECT_DIMS) or not np.isfinite(first2).all():
        raise SystemExit("first 2 vectors malformed. STOP.")
    return n, d


def write_vectors_csv(arr: np.memmap, n: int, out_path: str) -> None:
    """Write ALL n rows in .fbin order: `<row_index>,<96 shortest-repr floats>`.
    Row index is the sequential id (== FAISS positional index later)."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    tmp = out_path + ".tmp"
    t0 = time.time()
    written = 0
    with open(tmp, "w", newline="\n") as f:
        for start in range(0, n, WRITE_CHUNK):
            stop = min(start + WRITE_CHUNK, n)
            sub = np.asarray(arr[start:stop])       # contiguous slice -> RAM
            strs = sub.astype(str)                  # shortest float32 repr
            lines = [f"{start + j}," + " ".join(strs[j])
                     for j in range(strs.shape[0])]
            f.write("\n".join(lines))
            f.write("\n")
            written += strs.shape[0]
            print(f"[write] {written}/{n} rows ({time.time() - t0:.0f}s)", end="\r")
    os.replace(tmp, out_path)
    print(f"\n[write] done: {written} rows -> {out_path} in {time.time() - t0:.0f}s")


def copy_queries() -> int:
    """Copy the 1M query file verbatim, so the query set is identical at 10M."""
    if not os.path.exists(SRC_QUERIES):
        raise SystemExit(f"source queries not found: {SRC_QUERIES}")
    shutil.copyfile(SRC_QUERIES, OUT_QUERIES)
    with open(OUT_QUERIES) as f:
        nq = sum(1 for line in f if line.strip())
    print(f"[queries] copied {SRC_QUERIES} -> {OUT_QUERIES} ({nq} queries)")
    return nq


def load_query_vectors(path: str) -> np.ndarray:
    """Read the space-separated query CSV -> float32 (n, 96)."""
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append([float(x) for x in line.split(" ") if x])
    q = np.asarray(rows, dtype="<f4")
    assert q.shape[1] == EXPECT_DIMS, f"query dim {q.shape[1]} != {EXPECT_DIMS}"
    return q


def build_ground_truth(arr: np.memmap, n: int, queries: np.ndarray,
                       out_path: str) -> float:
    """Exact top-10 L2 neighbours over ALL n vectors, via FAISS IndexFlatL2.
    Returns wall seconds. IDs written are the flat index == .fbin row == CSV id."""
    import faiss
    print(f"[gt] FAISS IndexFlatL2(dim={EXPECT_DIMS}) over {n:,} vectors")
    t0 = time.time()
    index = faiss.IndexFlatL2(EXPECT_DIMS)
    for start in range(0, n, ADD_CHUNK):
        stop = min(start + ADD_CHUNK, n)
        # FAISS needs a contiguous C-order float32 copy; memmap slice -> ascontig.
        index.add(np.ascontiguousarray(arr[start:stop], dtype="<f4"))
        print(f"[gt] added {stop:,}/{n:,} ({time.time() - t0:.0f}s)", end="\r")
    print(f"\n[gt] index built: ntotal={index.ntotal} in {time.time() - t0:.0f}s")
    q = np.ascontiguousarray(queries, dtype="<f4")
    ts = time.time()
    _dist, ids = index.search(q, TOP_K)
    print(f"[gt] searched {q.shape[0]} queries top-{TOP_K} in {time.time() - ts:.1f}s")
    tmp = out_path + ".tmp"
    with open(tmp, "w", newline="\n") as f:
        for row in ids:
            f.write(",".join(str(int(x)) for x in row))
            f.write("\n")
    os.replace(tmp, out_path)
    print(f"[gt] wrote {out_path} ({ids.shape[0]} rows x {ids.shape[1]} ids)")
    return time.time() - t0


def count_lines(path: str) -> int:
    with open(path, "rb") as f:
        return sum(chunk.count(b"\n") for chunk in iter(lambda: f.read(1 << 20), b""))


def verify_outputs(n_expected: int) -> bool:
    """Line counts, first-2 vector format, query/truth shape. Loud on any miss."""
    ok = True
    vlines = count_lines(OUT_VECTORS)
    print(f"[check] vectors line count: {vlines} (expected {n_expected})")
    ok = ok and vlines == n_expected
    # first 2 vector rows: int id + 96 floats
    with open(OUT_VECTORS) as f:
        for i in range(2):
            row = f.readline().rstrip("\n")
            rid, rest = row.split(",", 1)
            floats = rest.split(" ")
            good = int(rid) == i and len(floats) == EXPECT_DIMS
            try:
                _ = [float(x) for x in floats]
            except ValueError:
                good = False
            print(f"[check] vec row {i}: id={rid} nfloats={len(floats)} ok={good} "
                  f"first3={floats[:3]}")
            ok = ok and good
    qlines = count_lines(OUT_QUERIES)
    print(f"[check] queries line count: {qlines} (expected {N_QUERIES})")
    ok = ok and qlines == N_QUERIES
    tlines = count_lines(OUT_TRUTH)
    print(f"[check] truth line count: {tlines} (expected {N_QUERIES})")
    ok = ok and tlines == N_QUERIES
    # truth row: 10 comma-separated ids in [0, n)
    with open(OUT_TRUTH) as f:
        r0 = f.readline().strip().split(",")
    good = len(r0) == TOP_K and all(0 <= int(x) < n_expected for x in r0)
    print(f"[check] truth row 0: nids={len(r0)} ok={good} ids={r0}")
    ok = ok and good
    return ok


def write_readme(fp: dict, gt_seconds: float, n_points: int, nq: int) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    q_size = os.path.getsize(OUT_QUERIES)
    t_size = os.path.getsize(OUT_TRUTH)
    body = f"""# deep_10m — full 10,000,000-vector Deep1B base set

Corpus for Experiment C3, the 10M-scale ramp of the three-way vector-search
comparison (Blocks, S3 Vectors, LanceDB). Built local, no AWS spend.

## Provenance

- Source URL: {SOURCE_URL}
- Source file: references/deep1b/base.10M.fbin (DEEP 10M base, .fbin, 96-dim, L2)
- Corpus: ALL {n_points:,} base vectors, in .fbin row order (NOT a sample).
  Sequential CSV IDs from 0, equal to the .fbin row index.
- Queries: {nq} vectors, identical bytes to the 1M experiment
  (datasets/deep_1m/queries_deep_1m_1000q.csv), copied so the query set is
  fixed across the 1M and 10M runs.
- Ground truth: exact top-{TOP_K} L2 neighbours over ALL {n_points:,} vectors,
  computed with FAISS IndexFlatL2. The public groundtruth references the full
  1B corpus row indices, so it cannot be used here (see findings/17).

## Produced files

- {os.path.relpath(OUT_VECTORS, WORKSPACE)}
  - Row count: {fp['line_count']:,}
  - Byte size: {fp['byte_size']:,}
  - content_sha256: `{fp['content_sha256']}`
  - Format: `<id>,<96 space-separated shortest-repr float32>` per row.
- {os.path.relpath(OUT_QUERIES, WORKSPACE)}
  - Row count: {nq}, byte size {q_size:,}. Space-separated floats, no id column.
- {os.path.relpath(OUT_TRUTH, WORKSPACE)}
  - Row count: {nq}, byte size {t_size:,}. {TOP_K} comma-separated neighbour ids
    per query, in this corpus's id space.

- Ground-truth build wall time: {gt_seconds:.0f}s (FAISS IndexFlatL2 over 10M).
- Generated (UTC): {ts}

## Regenerate

    curl -L '{SOURCE_URL}' -o references/deep1b/base.10M.fbin
    python experiments/experiment_c3/c3_phase0.py
"""
    with open(OUT_README, "w") as f:
        f.write(body)
    print(f"[readme] wrote {OUT_README}")


def do_build(args) -> int:
    n, d = verify_source(SOURCE_FBIN)
    arr = open_fbin(SOURCE_FBIN, n, d)
    if not args.skip_vectors:
        write_vectors_csv(arr, n, OUT_VECTORS)
    else:
        print("[write] --skip-vectors: leaving vectors CSV as-is")
    nq = copy_queries()
    queries = load_query_vectors(OUT_QUERIES)
    print(f"[gt] loaded {queries.shape[0]} query vectors, dim {queries.shape[1]}")
    gt_seconds = build_ground_truth(arr, n, queries, OUT_TRUTH)
    print("[hash] fingerprinting vectors CSV (reads the whole file) ...")
    fp = fingerprint_csv(OUT_VECTORS)
    print(f"[hash] content_sha256={fp['content_sha256']}")
    print(f"[hash] line_count={fp['line_count']} byte_size={fp['byte_size']}")
    # sidecar so c3_common reads the anchor rather than a hand-copied constant.
    with open(OUT_SHA_SIDECAR, "w") as f:
        f.write(fp["content_sha256"] + "\n")
    print(f"[hash] wrote anchor sidecar {OUT_SHA_SIDECAR}")
    write_readme(fp, gt_seconds, n, nq)
    ok = verify_outputs(n)
    print("\nPASS" if ok else "\nFAIL")
    return 0 if ok else 1


def do_verify(args) -> int:
    for p in (OUT_VECTORS, OUT_QUERIES, OUT_TRUTH):
        if not os.path.exists(p):
            raise SystemExit(f"missing: {p} (run without --verify first)")
    print("[verify] fingerprinting vectors CSV ...")
    fp = fingerprint_csv(OUT_VECTORS)
    print(f"[verify] content_sha256={fp['content_sha256']}")
    print(f"[verify] line_count={fp['line_count']} byte_size={fp['byte_size']}")
    ok = verify_outputs(EXPECT_POINTS)
    print("\nVERIFY PASS" if ok else "\nVERIFY FAIL")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--verify", action="store_true",
                    help="re-check produced files without rebuilding")
    ap.add_argument("--skip-vectors", action="store_true",
                    help="(dev) do not rewrite the vectors CSV")
    args = ap.parse_args()
    return do_verify(args) if args.verify else do_build(args)


if __name__ == "__main__":
    raise SystemExit(main())
