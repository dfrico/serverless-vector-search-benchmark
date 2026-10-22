"""
Experiment C3 addendum (2026-10-16) — S3 Vectors 10M ingest rate, logged.

Finding 25 reported a ramping ingest rate (~366 vec/s averaged over the first
4.5 h, ~1045 vec/s at steady state), but no log of those numbers survived (see
context/baseline-audit.md). This script repeats the 10M ingest exactly as
c3_phase2.py did (same adapter, same CSV order, PutVectors batches of 500, one
sequential client) into a SEPARATE index, `deep-10m-rate`, so the measured
`deep-10m` index is never touched. Every PutVectors call is logged with its end
time, running count and request latency.

Outputs (results_<tag>/):
  s3v_ingest_rate.progress.jsonl  one line per PutVectors call (written as it goes)
  s3v_ingest_rate.json            summary: total time, rates per 30-min window,
                                  first-4.5 h average, provenance
Run via c3_aws.sh. ~5.6 h wall, ~$0.77 of PutVectors. Use --delete to drop the
`deep-10m-rate` index afterwards (it otherwise costs storage).
    RESULTS_TAG=2026-10 caffeinate -i ./c3_aws.sh c3_s3v_ingest_rate.py --delete
"""
from __future__ import annotations

import argparse
import functools
import json
import os
import sys
import time

print = functools.partial(print, flush=True)  # noqa: A001

_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (_HERE, os.path.join(os.path.dirname(_HERE), "experiment_b"),
           os.path.join(os.path.dirname(_HERE), "experiment_c1"),
           os.path.dirname(os.path.dirname(_HERE))):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import c3_common as K  # noqa: E402
import eb_common as C  # noqa: E402

DATASET = "deep_10m_rate"   # -> S3 Vectors index "deep-10m-rate"
WINDOW_S = 1800


def summarise(points, t0):
    """Vectors and rate per 30-min window, and the average over the first 4.5 h
    (None if the run was shorter), from the (t, n_done) points."""
    out, last_n, k = [], 0, 0
    t_last = points[-1][0]
    while t0 + k * WINDOW_S < t_last:
        a, b = t0 + k * WINDOW_S, min(t0 + (k + 1) * WINDOW_S, t_last)
        n_b = max((n for t, n in points if t <= b), default=0)
        out.append({"from_h": round((a - t0) / 3600, 2), "to_h": round((b - t0) / 3600, 2),
                    "vectors": n_b - last_n, "rate_vec_s": round((n_b - last_n) / (b - a), 1)})
        last_n, k = n_b, k + 1
    first45 = None
    if t_last - t0 >= 4.5 * 3600:
        n45 = max(n for t, n in points if t - t0 <= 4.5 * 3600)
        first45 = round(n45 / (4.5 * 3600), 1)
    return out, first45


def put_cost_usd(n_req, batch):
    """Same PutVectors model as c3_phase2.ingest_cost (128 KB minimum per request)."""
    billable = max(batch * C.DIMENSIONS * 4, C.S3V_PUT_MIN_KB * 1024)
    return n_req * billable / 1e9 * C.S3V_PUT_PER_GB


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--put-batch", type=int, default=500, dest="put_batch")
    ap.add_argument("--delete", action="store_true",
                    help="drop the deep-10m-rate index after the run")
    args = ap.parse_args()

    from harness.adapters.s3vectors import S3VectorsClient
    from harness.records import run_provenance

    os.makedirs(K.RESULTS_DIR, exist_ok=True)
    prog_path = os.path.join(K.RESULTS_DIR, "s3v_ingest_rate.progress.jsonl")
    out_path = os.path.join(K.RESULTS_DIR, "s3v_ingest_rate.json")
    client = S3VectorsClient(f"thesis-s3v-{K.acct()}", region="us-east-1",
                             source_csv_path=K.VECTORS_CSV,
                             put_batch_size=args.put_batch)
    client.setup(DATASET, dimensions=K.DIMENSIONS, top_k_default=K.TOP_K,
                 create_if_missing=True)
    prov = run_provenance(client, DATASET, patch_path=out_path[:-5] + ".code.patch")

    points = []
    pf = open(prog_path, "w")

    def on_put(t_end, n_done, req_s):
        points.append((t_end, n_done))
        pf.write(json.dumps({"t": t_end, "n": n_done, "req_s": round(req_s, 4)}) + "\n")
        if len(points) % 200 == 0:
            pf.flush()
            el = t_end - t0
            print(f"[s3v-rate] {n_done:>10,} vectors  {el / 3600:5.2f} h  "
                  f"avg {n_done / el:6.1f} vec/s  last req {req_s:.2f}s")

    client.progress_callback = on_put
    t0 = time.time()
    print(f"[s3v-rate] start {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(t0))} "
          f"index={client.index_name} batch={args.put_batch}")
    _none, timings = client.bulk_insert(K.iter_vectors_csv(), build_index=True)
    total = time.time() - t0
    pf.close()

    windows, first45 = summarise(points, t0)
    summary = {
        "dataset": DATASET, "index": client.index_name,
        "corpus_sha256_expected": K.read_corpus_sha256(),
        "started_utc": t0, "ingest_seconds": round(total, 1),
        "n_vectors": timings.get("n_vectors"), "n_put_requests": timings.get("n_put_requests"),
        "put_batch_size": args.put_batch,
        "avg_rate_vec_s": round(timings.get("n_vectors", 0) / total, 1),
        "first_4_5h_avg_rate_vec_s": first45,
        "rate_per_30min": windows,
        "est_put_cost_usd": round(put_cost_usd(timings.get("n_put_requests", 0), args.put_batch), 4),
        "provenance": prov,
    }
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"[s3v-rate] done: {summary['n_vectors']:,} vectors in {total / 3600:.2f} h, "
          f"avg {summary['avg_rate_vec_s']} vec/s, first 4.5 h {first45} vec/s")
    print(f"[s3v-rate] wrote {out_path}")

    if args.delete:
        client.teardown(drop_data=True)
        print(f"[s3v-rate] deleted index {client.index_name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
