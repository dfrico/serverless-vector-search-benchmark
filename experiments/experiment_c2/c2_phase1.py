"""
Experiment C2, Phase 1 — reingest 1M + recall gate for ONE adapter.

This is the correctness precondition for the cold-start experiment. It rebuilds
each adapter's 1M index from the shared deep_1m corpus, then verifies recall@10
on the first 100 ground-truth queries. If an adapter misses its recall bar, the
experiment stops before any Phase 2 spend.

Recall bars (from the brief): Blocks >= 99%, S3 Vectors >= 90%, LanceDB >= 99%.

Blocks uses the C1 tuning sweet spot (blocks_per_task=8, num_workers=16,
num_blocks=16, partitions_per_block=10, n_probe=7). n_probe is baked at build
time, so pin_params must run before build_index.

Run one adapter per process so the three ingests run concurrently:
  ./c2_blocks.sh c2_phase1.py blocks
  ./c2_aws.sh    c2_phase1.py s3vectors
  ./c2_aws.sh    c2_phase1.py lancedb

Writes results/phase1_<adapter>.json (recall + ingest cost). Prints flushed for
live background logs.
"""
from __future__ import annotations

import argparse
import functools
import json
import os
import sys
import time
from datetime import datetime, timezone

print = functools.partial(print, flush=True)  # noqa: A001

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import c2_common as K  # noqa: E402
import eb_common as C  # noqa: E402

RECALL_N = 100  # recall gate query count (brief: 100 queries per adapter)


def ingest(adapter, client):
    """Reingest the 1M corpus for one adapter. Returns (ingest_timings,
    ingest_cost_usd)."""
    if adapter == "blocks":
        import c1_common
        # upload corpus once + build the index at the pinned params (n_probe=7).
        timings, cost = c1_common.build_index(
            client, num_workers=K.BLOCKS_PARAMS["num_workers"], upload=True)
        return timings, cost
    _none, timings = client.bulk_insert(C.iter_vectors_csv(C.VECTORS_CSV),
                                         build_index=True)
    cost = ingest_cost(adapter, timings)
    return timings, cost


def ingest_cost(adapter, t):
    """Ingest cost estimate for the streamed adapters (Blocks handled by
    c1_common.ingest_cost)."""
    req = C.LAMBDA_REQ_PER_M / 1e6
    if adapter == "lancedb":
        server_ms = float(t.get("server_ms_total", 0.0))
        n_inv = int(t.get("n_invocations", 0))
        return (server_ms / 1000.0) * K.LANCEDB_MEM_GB * C.LAMBDA_ARM_GBS + n_inv * req
    if adapter == "s3vectors":
        # PutVectors is billed on data uploaded, min 128KB/request (eb_common).
        n_req = int(t.get("n_put_requests", 0))
        batch = int(t.get("put_batch_size", 0)) or 1
        bytes_per_req = batch * C.DIMENSIONS * 4  # float32 payload, keys negligible
        billable = max(bytes_per_req, C.S3V_PUT_MIN_KB * 1024)
        gb = (n_req * billable) / 1e9
        return gb * C.S3V_PUT_PER_GB
    return 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("adapter", choices=["blocks", "s3vectors", "lancedb"])
    args = ap.parse_args()
    adapter = args.adapter

    os.makedirs(K.RESULTS_DIR, exist_ok=True)
    out_path = os.path.join(K.RESULTS_DIR, f"phase1_{adapter}.json")
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    t_start = time.time()
    acct = K.acct()
    print(f"[c2/phase1/{adapter}] start={started} account={acct}")
    print(f"[c2/phase1/{adapter}] pre-phase spend estimate: reingest 1M "
          "(Blocks ~$0.01, S3V ~$0.08 PUT, LanceDB ~$0.06) + 100q recall "
          "(<$0.001). Phase 1 total across 3 adapters ~$0.30. No single op > $3.")

    queries = C.load_queries()
    truth = C.load_truth()
    assert len(queries) == len(truth) == 1000, "expected 1000 queries + truth"
    gate_q = queries[:RECALL_N]
    gate_t = truth[:RECALL_N]

    client = K.build_and_setup(adapter, create_if_missing=True)
    # LanceDB ingest fires many short batch invokes; on a shared account Lambda
    # concurrency limit they can throttle (TooManyRequestsException). The client
    # now defaults to adaptive retries + a 120s read timeout (set in
    # harness/adapters/lancedb_lambda.py), so no
    # per-experiment boto3 monkeypatch is needed for the ingest path.

    fp = client.dataset_fingerprint(C.DATASET)
    anchor_ok = fp.get("content_sha256") == C.CORPUS_SHA256
    print(f"[c2/phase1/{adapter}] setup OK sha256={fp.get('content_sha256')} "
          f"anchor_ok={anchor_ok}")

    t_ing = time.time()
    ing, ing_cost = ingest(adapter, client)
    ingest_s = time.time() - t_ing
    print(f"[c2/phase1/{adapter}] ingest done in {ingest_s:.1f}s "
          f"ingest_cost=${ing_cost:.5f}")
    if ing_cost > 3.0:
        print(f"STOP: ingest cost ${ing_cost:.4f} exceeds the $3 ask-first line.")
        return 3

    # The recall gate is ONE long invoke (~100 x 5s). Reconfigure the client for
    # a 900s read timeout with no auto-retry, so a slow-but-fine invoke is not
    # aborted early nor duplicated by a retry.
    if adapter == "lancedb":
        client.configure_client(read_timeout_s=900, max_retries=0)

    # --- recall gate: first 100 queries via one query_batch ---
    t_q = time.time()
    results, q_timings = client.query_batch(gate_q, top_k=C.TOP_K)
    query_s = time.time() - t_q
    recall, per_query = C.recall_at_k(results, gate_t, C.TOP_K)
    n_perfect = sum(1 for h in per_query if h == C.TOP_K)
    non_empty = sum(1 for r in results if r)
    threshold = K.RECALL_STOP[adapter]
    print(f"[c2/phase1/{adapter}] recall@10={recall:.2f}% (bar {threshold}%) "
          f"perfect={n_perfect}/{RECALL_N} non_empty={non_empty}/{RECALL_N} "
          f"recall_query_s={query_s:.1f}")

    passed = recall >= threshold and non_empty == RECALL_N and anchor_ok
    summary = {
        "phase": "c2_phase1",
        "adapter": adapter,
        "started_utc": started,
        "account": acct,
        "dataset": C.DATASET,
        "corpus_sha256": fp.get("content_sha256"),
        "corpus_sha256_anchor_ok": anchor_ok,
        "recall_n": RECALL_N,
        "recall_at_10_pct": round(recall, 4),
        "recall_threshold_pct": threshold,
        "perfect_10of10": n_perfect,
        "non_empty": non_empty,
        "ingest_seconds": round(ingest_s, 3),
        "ingest_cost_usd": round(ing_cost, 6),
        "ingest_timings": {k: v for k, v in ing.items()
                           if isinstance(v, (int, float, str, bool))},
        "recall_query_seconds": round(query_s, 3),
        "wall_clock_seconds": round(time.time() - t_start, 3),
        "params": K.BLOCKS_PARAMS if adapter == "blocks" else None,
        "passed": bool(passed),
    }
    tmp = out_path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(summary, f, indent=2, sort_keys=True)
    os.replace(tmp, out_path)
    print(f"[c2/phase1/{adapter}] wrote results/phase1_{adapter}.json")
    # Raw evidence (2026-10-16): per-query ids/dists, full timings, provenance.
    from harness.records import per_query_records, run_provenance, write_sidecar
    write_sidecar(out_path, "per_query", {
        "per_query": per_query_records(results, gate_t, C.TOP_K),
        "ingest": ing, "query_timings": q_timings,
        "provenance": run_provenance(client, C.DATASET,
                                     patch_path=out_path[:-5] + ".code.patch"),
    })

    if not passed:
        reason = []
        if recall < threshold:
            reason.append(f"recall {recall:.2f}% < {threshold}%")
        if non_empty != RECALL_N:
            reason.append(f"non_empty {non_empty}/{RECALL_N}")
        if not anchor_ok:
            reason.append("corpus sha256 anchor mismatch")
        print(f"\n=== C2 PHASE 1 {adapter.upper()} STOP ===", "; ".join(reason))
        return 1
    print(f"\n=== C2 PHASE 1 {adapter.upper()} PASS ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
