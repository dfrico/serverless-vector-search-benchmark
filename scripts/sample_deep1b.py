#!/usr/bin/env python3
"""
Sample 1,000,000 vectors from the Deep1B (DEEP) 10M base set and write them as a
CSV in the Blocks adapter's expected format.

This is prep for the first real experiment (Blocks recall at 1M). It is local
only: no AWS, no adapter code touched.

Source
------
The documented Skoltech page (sites.skoltech.ru/compvision/noimi) is dead — it
now 302-redirects to the university homepage. The live, canonical mirror is the
big-ann-benchmarks Yandex Cloud bucket:

    https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/base.10M.fbin

This is the same DEEP/Deep1B distribution the Blocks paper uses: 96-dim, L2, so
no adapter change is needed. We use the 10M base subset (3.84 GB) as a proper
superset and sample 1,000,000 vectors from it.

.fbin binary format (big-ann-benchmarks convention)
---------------------------------------------------
    bytes 0-3   int32  n_points   (little-endian)
    bytes 4-7   int32  n_dims     (little-endian)
    bytes 8..   float32 n_points * n_dims, row-major, contiguous, NO per-vector
                prefix.
So the .fbin has ONE header for the whole file, unlike .fvecs which repeats an
int32 dimension prefix before every vector. For base.10M the header must read
(10_000_000, 96).

Output format (Blocks / serverless-vdb dataset shape, finding 03)
-----------------------------------------------------------------
One row per vector:  <id>,<96 space-separated floats>\\n
IDs are SEQUENTIAL integers from 0 in write order, NOT the original .fbin row
index. Sequential IDs keep the id space dense and match how the bundled 100k
dataset is numbered. Floats are written with numpy's shortest round-trip repr
(same style as the bundled 100k file; verified to round-trip the float32 bits
exactly).

Determinism / idempotency
-------------------------
Sampling uses numpy's default_rng(seed) (seed 42 by default; see below). It draws
1,000,000 DISTINCT indices from [0, n_points) uniformly WITHOUT replacement, then
SORTS them so the memmap is read front-to-back (fast) and the output is stable.
Same seed -> same index set -> same sorted order -> byte-identical CSV. So a
re-run reproduces the exact same file (and the same content SHA-256).

Seed convention
----------------
No context/methodology-decisions.md existed when this was written, so we adopt
seed = 42 as the project-wide sampling seed and record it here and in the
dataset README + finding 17. Any later sampling should reuse 42 unless a
documented reason says otherwise.

Usage
-----
    # sample + write CSV + write README (needs the .fbin downloaded first)
    python scripts/sample_deep1b.py

    # re-read the produced CSV and re-check it WITHOUT re-sampling
    python scripts/sample_deep1b.py --verify
"""
from __future__ import annotations

import argparse
import csv
import os
import random
import sys
from datetime import datetime, timezone

import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(_HERE)
# harness-day6 holds the dataset fingerprint contract (finding 13). Reuse it so
# the hash this script records is computed the SAME way the run manifest will be.
# The fingerprint contract (finding 13) now lives in harness/dataset_hash.py.
sys.path.insert(0, WORKSPACE)
from harness.dataset_hash import fingerprint_csv  # noqa: E402

SOURCE_URL = ("https://storage.yandexcloud.net/yandex-research/"
              "ann-datasets/DEEP/base.10M.fbin")
DEFAULT_SOURCE = os.path.join(WORKSPACE, "references", "deep1b", "base.10M.fbin")
DEFAULT_OUT = os.path.join(WORKSPACE, "datasets", "deep_1m", "vectors_deep_1m.csv")
DEFAULT_README = os.path.join(WORKSPACE, "datasets", "deep_1m", "README.md")

EXPECT_POINTS = 10_000_000
EXPECT_DIMS = 96
N_SAMPLE = 1_000_000
SEED = 42
WRITE_CHUNK = 100_000          # rows per write batch (bounds peak memory)


def read_fbin_header(path: str) -> tuple[int, int]:
    """Read the 8-byte .fbin header -> (n_points, n_dims). Little-endian int32."""
    with open(path, "rb") as f:
        hdr = np.frombuffer(f.read(8), dtype="<i4")
    return int(hdr[0]), int(hdr[1])


def open_fbin(path: str, n_points: int, n_dims: int) -> np.memmap:
    """Memmap the float32 body (after the 8-byte header). No data copied to RAM."""
    return np.memmap(path, dtype="<f4", mode="r", offset=8,
                     shape=(n_points, n_dims))


def verify_source(path: str) -> tuple[int, int]:
    """
    Verify the .fbin before trusting it (step 2 of the brief, adapted to .fbin):
      - header must read (EXPECT_POINTS, EXPECT_DIMS);
      - file size must equal 8 + n_points * n_dims * 4;
      - the first 2 vectors must be finite, EXPECT_DIMS wide.
    Raises SystemExit on any mismatch (loud, per the brief).
    """
    if not os.path.exists(path):
        raise SystemExit(
            f"source .fbin not found: {path}\n"
            f"download it first:\n  curl -L '{SOURCE_URL}' -o '{path}'")
    n, d = read_fbin_header(path)
    print(f"[verify] header: n_points={n} n_dims={d}")
    if (n, d) != (EXPECT_POINTS, EXPECT_DIMS):
        raise SystemExit(
            f"header {(n, d)} != expected {(EXPECT_POINTS, EXPECT_DIMS)} — the "
            f"file is corrupt or DEEP changed its format. STOP.")
    expect_bytes = 8 + n * d * 4
    actual_bytes = os.path.getsize(path)
    print(f"[verify] file size: {actual_bytes} bytes (expected {expect_bytes})")
    if actual_bytes != expect_bytes:
        raise SystemExit(
            f"file size {actual_bytes} != expected {expect_bytes} — truncated or "
            f"corrupt download. STOP.")
    arr = open_fbin(path, n, d)
    first2 = np.asarray(arr[:2])
    print(f"[verify] first 2 vectors: shape={first2.shape} "
          f"finite={bool(np.isfinite(first2).all())}")
    print(f"[verify]   vec0[:4]={first2[0, :4].tolist()}")
    print(f"[verify]   vec1[:4]={first2[1, :4].tolist()}")
    if first2.shape != (2, EXPECT_DIMS) or not np.isfinite(first2).all():
        raise SystemExit("first 2 vectors are malformed. STOP.")
    return n, d


def sample_indices(n_points: int, n_sample: int, seed: int) -> np.ndarray:
    """Uniform, without replacement, then sorted for sequential reads."""
    rng = np.random.default_rng(seed)
    idx = rng.choice(n_points, size=n_sample, replace=False)
    idx.sort()
    # sanity: distinct + in range
    assert idx.shape[0] == n_sample
    assert np.unique(idx).shape[0] == n_sample, "sampled indices not distinct"
    assert idx[0] >= 0 and idx[-1] < n_points
    return idx


def write_csv(arr: np.memmap, idx: np.ndarray, out_path: str) -> None:
    """
    Write <seq_id>,<96 space floats> for each sampled row, in sorted-index order.
    seq_id is the WRITE position (0..n_sample-1), not the original .fbin index.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    n = idx.shape[0]
    written = 0
    with open(out_path, "w", newline="\n") as f:
        for start in range(0, n, WRITE_CHUNK):
            chunk_idx = idx[start:start + WRITE_CHUNK]
            sub = np.asarray(arr[chunk_idx])          # gather -> RAM (small)
            strs = sub.astype(str)                    # shortest float32 repr
            lines = []
            for j in range(strs.shape[0]):
                seq_id = start + j
                lines.append(f"{seq_id}," + " ".join(strs[j]))
            f.write("\n".join(lines))
            f.write("\n")
            written += strs.shape[0]
            print(f"[write] {written}/{n} rows", end="\r", flush=True)
    print(f"\n[write] done: {written} rows -> {out_path}")


def write_readme(readme_path: str, out_path: str, fp: dict, seed: int,
                 n_points: int) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    script_rel = os.path.relpath(os.path.abspath(__file__), WORKSPACE)
    out_rel = os.path.relpath(out_path, WORKSPACE)
    body = f"""# deep_1m — 1,000,000-vector Deep1B sample

Prep dataset for the first real experiment (Blocks recall at 1M).

## Provenance

- Source URL: {SOURCE_URL}
- Source file: references/deep1b/base.10M.fbin (DEEP 10M base, .fbin, 96-dim, L2)
- Sampling: {N_SAMPLE:,} of {n_points:,} vectors, uniform, without replacement,
  then sorted by original index. Sequential CSV IDs from 0 (NOT the original
  .fbin row index).
- Sampling seed: {seed}
- Sampling script: {script_rel}  (re-run to regenerate; `--verify` to re-check)

## Produced file — {out_rel}

- Row count: {fp['line_count']:,}
- Byte size: {fp['byte_size']:,}
- content_sha256: `{fp['content_sha256']}`
- Generated (UTC): {ts}

## Companion files (for the recall experiment)

- query.public.10K.fbin — 10,000 query vectors, 96-dim float32 (.fbin header
  (10000, 96)).
- groundtruth.public.10K.ibin — top-100 neighbour IDs per query, int32 (.ibin
  header (10000, 100)).

WARNING — the public ground truth references the ORIGINAL 10M/1B corpus row
indices, not this 1M subset's re-numbered IDs. It cannot be used directly for
recall on this sample. The recall experiment must either (a) preserve original
row indices as CSV IDs, or (b) compute fresh ground truth over this 1M sample.
See findings/17_dataset_sampling.md.

## Regenerate

    curl -L '{SOURCE_URL}' -o references/deep1b/base.10M.fbin
    python {script_rel}
"""
    with open(readme_path, "w") as f:
        f.write(body)
    print(f"[readme] wrote {readme_path}")


def spot_check(out_path: str, n_rows_expected: int, seed: int = SEED) -> bool:
    """Pick 3 random rows; each must have an int id in [0, n) and 96 floats."""
    # count lines cheaply
    with open(out_path, "rb") as f:
        total = sum(chunk.count(b"\n") for chunk in iter(lambda: f.read(1 << 20), b""))
    print(f"[check] line count: {total} (expected {n_rows_expected})")
    ok = total == n_rows_expected
    rng = random.Random(seed)
    targets = sorted(rng.sample(range(total), 3))
    picked = {}
    with open(out_path) as f:
        for i, line in enumerate(f):
            if i in targets:
                picked[i] = line.rstrip("\n")
                if len(picked) == len(targets):
                    break
    for i in targets:
        row = picked[i]
        rid, rest = row.split(",", 1)
        floats = rest.split(" ")
        rid_i = int(rid)
        good = (0 <= rid_i < n_rows_expected) and len(floats) == EXPECT_DIMS
        # confirm the floats parse
        try:
            _ = [float(x) for x in floats]
        except ValueError:
            good = False
        print(f"[check] row {i}: id={rid_i} nfloats={len(floats)} ok={good} "
              f"first3={floats[:3]}")
        ok = ok and good
    return ok


def do_sample(args) -> int:
    n, d = verify_source(args.source)
    arr = open_fbin(args.source, n, d)
    print(f"[sample] drawing {args.n_sample:,} of {n:,} (seed {args.seed})...")
    idx = sample_indices(n, args.n_sample, args.seed)
    print(f"[sample] index range [{idx[0]}, {idx[-1]}], distinct={args.n_sample:,}")
    write_csv(arr, idx, args.out)
    fp = fingerprint_csv(args.out)
    print(f"[hash] content_sha256={fp['content_sha256']}")
    print(f"[hash] line_count={fp['line_count']} byte_size={fp['byte_size']}")
    write_readme(args.readme, args.out, fp, args.seed, n)
    ok = spot_check(args.out, args.n_sample, args.seed)
    fp2 = fingerprint_csv(args.out)
    match = fp2["content_sha256"] == fp["content_sha256"]
    print(f"[verify] re-hash matches README: {match}")
    print("\nPASS" if (ok and match) else "\nFAIL")
    return 0 if (ok and match) else 1


def do_verify(args) -> int:
    """Re-read the CSV and re-compute the hash WITHOUT re-sampling."""
    if not os.path.exists(args.out):
        raise SystemExit(f"CSV not found: {args.out} (run without --verify first)")
    fp = fingerprint_csv(args.out)
    print(f"[verify] content_sha256={fp['content_sha256']}")
    print(f"[verify] line_count={fp['line_count']} byte_size={fp['byte_size']}")
    ok = spot_check(args.out, args.n_sample, args.seed)
    print("\nVERIFY PASS" if ok else "\nVERIFY FAIL")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", default=DEFAULT_SOURCE, help="path to base.10M.fbin")
    ap.add_argument("--out", default=DEFAULT_OUT, help="output CSV path")
    ap.add_argument("--readme", default=DEFAULT_README, help="dataset README path")
    ap.add_argument("--n-sample", type=int, default=N_SAMPLE, dest="n_sample")
    ap.add_argument("--seed", type=int, default=SEED)
    ap.add_argument("--verify", action="store_true",
                    help="re-read + re-hash the CSV without re-sampling")
    args = ap.parse_args()
    return do_verify(args) if args.verify else do_sample(args)


if __name__ == "__main__":
    raise SystemExit(main())
