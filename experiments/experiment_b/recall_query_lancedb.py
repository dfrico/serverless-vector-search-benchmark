"""
Experiment B, Phase 2 — LanceDB recall QUERY re-run (ingest already done).

The one-shot run_recall.py lancedb ingested all 1M vectors (1360s, confirmed in
results/recall_lancedb.log), then FAILED on the recall step: query_batch() packs
all 1000 queries into ONE Lambda invocation, and the client's boto3 lambda client
is built with the default 60s read timeout (lancedb_lambda_client.py line 326, no
botocore Config). The single big invocation ran past 60s client-side and raised
ReadTimeoutError. This is a client-timeout issue, NOT a recall/correctness fault,
and the table is fully populated — so we DO NOT re-ingest.

A timing probe showed LanceDB exact flat KNN is ~4-5s per query at 1M (reading
the 384MB table from S3 per invocation), so 1000 queries serially would take
~83 min. Instead we raise the Lambda timeout to 900s (done out of band), then run
10 chunks of 100 queries CONCURRENTLY (the adapter's max_safe_concurrency is 10):
10 Lambda containers run in parallel, so all 1000 queries finish in roughly one
chunk's server time (~9 min). Results are re-assembled in query order. It writes
results/recall_lancedb.json in the run_recall.py schema.

Run via eb_aws.sh (spike-localhost venv + thesis profile).
"""
from __future__ import annotations

import functools
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

print = functools.partial(print, flush=True)  # noqa: A001

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))   # experiments/experiment_b -> repo root
# _HERE (for the sibling eb_common) + repo root (for the `harness` package).
for _p in (_HERE, WORKSPACE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import eb_common as C  # noqa: E402

CHUNK = 100                 # queries per Lambda invocation (~520s server @ ~5.2s/query)
WORKERS = 10                # concurrent invocations (== max_safe_concurrency)
READ_TIMEOUT_S = 890        # < Lambda's raised 900s ceiling, so a slow call is not cut
PRIOR_INGEST_SECONDS = 1360.4   # from results/recall_lancedb.log (the successful ingest)



def _ingest_seconds():
    """Ingest time from run_recall.py --ingest-only's sidecar (2026-10-16); the
    hard-coded August value applies only to the original results/ folder."""
    side = os.path.join(C.RESULTS_DIR, "recall_lancedb.ingest.json")
    if os.path.exists(side):
        with open(side) as f:
            return round(json.load(f)["ingest_seconds"], 3)
    return PRIOR_INGEST_SECONDS


def main():
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    t_start = time.time()
    acct = C.account_id()
    print(f"[recall-q/lancedb] start={started} account={acct} chunk={CHUNK}")

    queries = C.load_queries()
    truth = C.load_truth()
    assert len(queries) == len(truth) == 1000

    client = C.build_client("lancedb", acct)
    client.setup(C.DATASET, dimensions=C.DIMENSIONS, top_k_default=C.TOP_K,
                 create_if_missing=True)  # reconnect only — table already has 1M rows

    # Raise the boto3 read timeout on the adapter's lambda client so a legitimate
    # multi-second invocation is not cut at the default 60s.
    import boto3
    from botocore.config import Config
    client._client = boto3.client(
        "lambda", region_name="us-east-1",
        config=Config(read_timeout=READ_TIMEOUT_S, connect_timeout=15,
                      retries={"max_attempts": 2}))

    fp = client.dataset_fingerprint(C.DATASET)
    caps = client.capabilities()
    anchor_ok = fp.get("content_sha256") == C.CORPUS_SHA256
    print(f"[recall-q/lancedb] setup OK metric={caps.metric} "
          f"max_conc={caps.max_safe_concurrency} sha256={fp.get('content_sha256')} "
          f"anchor_ok={anchor_ok}")

    # --- recall queries: fan out chunks concurrently (boto3 clients are
    #     thread-safe for calls); re-assemble in query order by chunk index. ---
    t_q = time.time()
    starts = list(range(0, len(queries), CHUNK))

    def run_chunk(start):
        part, _t = client.query_batch(queries[start:start + CHUNK], top_k=C.TOP_K)
        print(f"[recall-q/lancedb] chunk @{start} done (+{len(part)}, "
              f"{time.time() - t_q:.1f}s elapsed)")
        return start, part

    parts = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for start, part in ex.map(run_chunk, starts):
            parts[start] = part
    results = []
    for start in starts:
        results.extend(parts[start])
    query_s = time.time() - t_q

    recall, per_query = C.recall_at_k(results, truth, C.TOP_K)
    n_perfect = sum(1 for h in per_query if h == C.TOP_K)
    n_zero = sum(1 for h in per_query if h == 0)
    non_empty = sum(1 for r in results if r)
    print(f"[recall-q/lancedb] recall@10={recall:.2f}% perfect={n_perfect}/1000 "
          f"zero={n_zero}/1000 non_empty={non_empty}/1000 query_s={query_s:.1f}")

    threshold = C.RECALL_STOP["lancedb"]
    passed = recall >= threshold and non_empty == len(queries) and anchor_ok

    summary = {
        "adapter": client.name,
        "adapter_key": "lancedb",
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
        "ingest_seconds": _ingest_seconds(),
        "ingest_note": "ingest done in the first run (see recall_lancedb.log); "
                       "this run only re-ran the recall queries after the boto3 "
                       "read-timeout fix — the table was NOT re-ingested.",
        "recall_query_seconds": round(query_s, 3),
        "recall_query_chunk": CHUNK,
        "recall_query_workers": WORKERS,
        "wall_clock_seconds": round(time.time() - t_start, 3),
        "query0_top10_returned": [int(i) for i, _ in results[0][:C.TOP_K]],
        "query0_top10_truth": [int(x) for x in truth[0][:C.TOP_K]],
    }
    out_path = os.path.join(C.RESULTS_DIR, "recall_lancedb.json")
    tmp = out_path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(summary, f, indent=2, sort_keys=True)
    os.replace(tmp, out_path)
    print(f"[recall-q/lancedb] wrote {os.path.relpath(out_path, WORKSPACE)}")
    # Raw evidence (2026-10-16): per-query ids/dists and provenance.
    from harness.records import per_query_records, run_provenance, write_sidecar
    write_sidecar(out_path, "per_query", {
        "per_query": per_query_records(results, truth, C.TOP_K),
        "provenance": run_provenance(client, C.DATASET,
                                     patch_path=out_path[:-5] + ".code.patch"),
    })

    print("\n=== RECALL LANCEDB", "PASS" if passed else "FAIL/STOP", "===")
    print(json.dumps(summary, indent=2, sort_keys=True))
    if not passed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
