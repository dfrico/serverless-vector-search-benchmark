"""
Experiment B, Phase 2 — recall precondition for ONE adapter at 1M.

This is a correctness GATE, not a thesis metric (supplement non-goals). It proves
the adapter returns the right neighbours before we measure its latency/cost.

For the chosen adapter:
  1. setup() against the 1M corpus with source_csv_path -> datasets/deep_1m
     (so the manifest records the shared content_sha256 anchor).
  2. ingest the 1M corpus (bulk_insert_from_path for Blocks; streaming
     bulk_insert iterable for S3 Vectors and LanceDB).
  3. run the 1000 queries top_k=10, compute recall@10 vs the fresh ground truth.
  4. STOP (non-zero exit) if recall is below the adapter's threshold.

Writes results/recall_<adapter>.json with ingest wall-clock, recall, per-query
hits, and the first-query returned ids (for the cross-adapter spot-check). Prints
are flushed so a background run's log shows live progress.

Venv/env: all adapters run from the consolidated harness/.venv. Blocks uses the
MinIO env; S3 Vectors + LanceDB use AWS_PROFILE=thesis with no MinIO env.
"""
from __future__ import annotations

import argparse
import functools
import json
import os
import sys
import time
from datetime import datetime, timezone

print = functools.partial(print, flush=True)  # noqa: A001 — live logs for bg runs

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))   # experiments/experiment_b -> repo root
# _HERE (for the sibling eb_common) + repo root (for the `harness` package).
for _p in (_HERE, WORKSPACE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import eb_common as C  # noqa: E402


def build_client(adapter, acct, args):
    if adapter == "blocks":
        from harness.adapters.blocks_aws import BlocksClientAWS
        return BlocksClientAWS(bucket=f"thesis-blocks-{acct}",
                               vectors_csv_path=C.VECTORS_CSV, region="us-east-1")
    if adapter == "s3vectors":
        from harness.adapters.s3vectors import S3VectorsClient
        return S3VectorsClient(f"thesis-s3v-{acct}", region="us-east-1",
                               source_csv_path=C.VECTORS_CSV,
                               put_batch_size=args.put_batch)
    if adapter == "lancedb":
        from harness.adapters.lancedb_lambda import LanceDBLambdaClient
        return LanceDBLambdaClient(
            "thesis-lancedb-lambda", region="us-east-1",
            data_bucket=f"thesis-lancedb-{acct}", source_csv_path=C.VECTORS_CSV,
            image_uri=f"{acct}.dkr.ecr.us-east-1.amazonaws.com/thesis-lancedb-lambda:latest",
            insert_batch_size=args.insert_batch)
    raise SystemExit(f"unknown adapter {adapter}")


def do_ingest(adapter, client):
    if adapter == "blocks":
        return client.bulk_insert_from_path(C.VECTORS_CSV, build_index=True)
    return client.bulk_insert(C.iter_vectors_csv(C.VECTORS_CSV), build_index=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("adapter", choices=["blocks", "s3vectors", "lancedb"])
    ap.add_argument("--put-batch", type=int, default=500, dest="put_batch")
    ap.add_argument("--insert-batch", type=int, default=2000, dest="insert_batch")
    ap.add_argument("--num-blocks", type=int, default=16, dest="num_blocks")
    ap.add_argument("--ingest-only", action="store_true", dest="ingest_only",
                    help="stop after ingest (LanceDB: recall runs in recall_query_lancedb.py)")
    args = ap.parse_args()
    adapter = args.adapter

    os.makedirs(C.RESULTS_DIR, exist_ok=True)
    out_path = os.path.join(C.RESULTS_DIR, f"recall_{adapter}.json")
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    t_start = time.time()
    acct = C.account_id()
    print(f"[recall/{adapter}] start={started} account={acct}")

    queries = C.load_queries()
    truth = C.load_truth()
    assert len(queries) == len(truth) == 1000, "expected 1000 queries + truth rows"
    print(f"[recall/{adapter}] loaded {len(queries)} queries + truth rows")

    client = build_client(adapter, acct, args)
    setup_kwargs = dict(dimensions=C.DIMENSIONS, top_k_default=C.TOP_K,
                        create_if_missing=True)
    if adapter == "blocks":
        setup_kwargs["num_blocks"] = args.num_blocks
    client.setup(C.DATASET, **setup_kwargs)
    fp = client.dataset_fingerprint(C.DATASET)
    caps = client.capabilities()
    print(f"[recall/{adapter}] setup OK name={client.name} "
          f"metric={caps.metric} max_conc={caps.max_safe_concurrency}")
    print(f"[recall/{adapter}] fingerprint sha256={fp.get('content_sha256')}")
    anchor_ok = fp.get("content_sha256") == C.CORPUS_SHA256

    # --- ingest ---
    t_ing = time.time()
    _none, ing_timings = do_ingest(adapter, client)
    ingest_s = time.time() - t_ing
    print(f"[recall/{adapter}] ingest done in {ingest_s:.1f}s "
          f"timings_keys={sorted(k for k in ing_timings)}")
    # Ingest record (2026-10-16): written before the recall step, so the ingest
    # time survives even if the recall step fails or runs in another script.
    from harness.records import write_sidecar
    write_sidecar(out_path, "ingest", {"ingest_seconds": ingest_s,
                                       "ingest_timings": ing_timings,
                                       "started_utc": started})
    if args.ingest_only:
        print(f"[recall/{adapter}] --ingest-only: stopping before recall")
        return 0

    # --- recall: all 1000 queries ---
    t_q = time.time()
    results, q_timings = client.query_batch(queries, top_k=C.TOP_K)
    query_s = time.time() - t_q
    recall, per_query = C.recall_at_k(results, truth, C.TOP_K)
    n_perfect = sum(1 for h in per_query if h == C.TOP_K)
    n_zero = sum(1 for h in per_query if h == 0)
    non_empty = sum(1 for r in results if r)
    print(f"[recall/{adapter}] recall@10={recall:.2f}% perfect={n_perfect}/1000 "
          f"zero={n_zero}/1000 non_empty={non_empty}/1000 query_s={query_s:.1f}")

    threshold = C.RECALL_STOP[adapter]
    passed = recall >= threshold and non_empty == len(queries) and anchor_ok

    summary = {
        "adapter": client.name,
        "adapter_key": adapter,
        "started_utc": started,
        "account": acct,
        "dataset": C.DATASET,
        "corpus_sha256": fp.get("content_sha256"),
        "corpus_sha256_anchor_ok": anchor_ok,
        "metric": caps.metric,
        "max_safe_concurrency": caps.max_safe_concurrency,
        "recall_at_10_pct": round(recall, 4),
        "recall_threshold_pct": threshold,
        "passed": bool(passed),
        "per_query_hits": {
            "perfect_10of10": n_perfect, "zero_hits": n_zero,
            "mean": round(sum(per_query) / len(per_query), 4),
            "min": min(per_query), "max": max(per_query),
        },
        "non_empty": non_empty,
        "ingest_seconds": round(ingest_s, 3),
        "ingest_timings": {k: v for k, v in ing_timings.items()
                           if isinstance(v, (int, float, str, bool))},
        "recall_query_seconds": round(query_s, 3),
        "wall_clock_seconds": round(time.time() - t_start, 3),
        "query0_top10_returned": [int(i) for i, _ in results[0][:C.TOP_K]],
        "query0_top10_truth": [int(x) for x in truth[0][:C.TOP_K]],
    }
    tmp = out_path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(summary, f, indent=2, sort_keys=True)
    os.replace(tmp, out_path)
    print(f"[recall/{adapter}] wrote {os.path.relpath(out_path, WORKSPACE)}")
    # Raw evidence (2026-10-16): per-query ids/dists, full timings, provenance.
    from harness.records import per_query_records, run_provenance, write_sidecar
    side = write_sidecar(out_path, "per_query", {
        "per_query": per_query_records(results, truth, C.TOP_K),
        "ingest_timings": ing_timings, "query_timings": q_timings,
        "provenance": run_provenance(client, C.DATASET,
                                     patch_path=out_path[:-5] + ".code.patch"),
    })
    print(f"[recall/{adapter}] wrote {os.path.relpath(side, WORKSPACE)}")

    print("\n=== RECALL", adapter.upper(), "PASS" if passed else "FAIL/STOP", "===")
    print(json.dumps(summary, indent=2, sort_keys=True))
    if not passed:
        reason = []
        if recall < threshold:
            reason.append(f"recall {recall:.2f}% < {threshold}%")
        if non_empty != len(queries):
            reason.append(f"non_empty {non_empty}/{len(queries)}")
        if not anchor_ok:
            reason.append("corpus sha256 anchor mismatch")
        print("STOP:", "; ".join(reason))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
