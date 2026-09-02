"""
Experiment C1, Phase 1 — baseline re-ingest + recall gate.

Uploads the deep_1m corpus once, builds the baseline index (num_blocks=16,
partitions_per_block=10, n_probe=10, blocks_per_task=16, num_workers=16), and
verifies recall@10 = 100% over the 1000 ground-truth queries (one query_batch
job, the same path Experiment B used). Then runs the baseline 50-query serial
latency/cost measurement, which is the shared anchor point for all three axes.

Writes results/phase1_baseline.json (recall + ingest cost) and
results/measure_base.json (the baseline sweep cell). Run via ec_blocks.sh.
"""
from __future__ import annotations

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

import c1_common as K  # noqa: E402
import eb_common as C  # noqa: E402


def main():
    os.makedirs(K.RESULTS_DIR, exist_ok=True)
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    t_start = time.time()
    acct = K.account_id()
    print(f"[c1/phase1] start={started} account={acct}")
    print("[c1/phase1] pre-phase spend estimate: 1 upload+build (~$0.01-0.02) + "
          "1000q recall job (~$0.001) + 50q baseline measure (~$0.025). "
          "Well under the $0.20/ingest guard.")

    queries = C.load_queries()
    truth = C.load_truth()
    assert len(queries) == len(truth) == 1000, "expected 1000 queries + truth"
    print(f"[c1/phase1] loaded {len(queries)} queries + truth")

    client = K.build_blocks(acct)
    client.setup(C.DATASET, dimensions=C.DIMENSIONS, top_k_default=C.TOP_K,
                 num_blocks=K.BASE_NUM_BLOCKS,
                 partitions_per_block=K.BASE_PARTITIONS_PER_BLOCK,
                 n_probe=K.BASE_N_PROBE, blocks_per_task=K.BASE_BLOCKS_PER_TASK,
                 create_if_missing=True)
    K.pin_params(client, n_probe=K.BASE_N_PROBE, num_workers=K.BASE_NUM_WORKERS)
    fp = client.dataset_fingerprint(C.DATASET)
    anchor_ok = fp.get("content_sha256") == C.CORPUS_SHA256
    print(f"[c1/phase1] setup OK sha256={fp.get('content_sha256')} anchor_ok={anchor_ok}")

    # --- build baseline index (upload corpus once) ---
    t_ing = time.time()
    ing, ing_cost = K.build_index(client, num_workers=K.BASE_NUM_WORKERS, upload=True)
    ingest_s = time.time() - t_ing
    print(f"[c1/phase1] ingest done in {ingest_s:.1f}s upload_s={ing.get('upload_s', 0):.1f} "
          f"index_s={ing.get('index_s', 0):.1f} ingest_cost=${ing_cost:.4f}")
    print(f"[c1/phase1] ingest index_timers={json.dumps(ing['index_timers'], default=str)[:400]}")
    if ing_cost > 0.20:
        print(f"STOP: baseline ingest cost ${ing_cost:.4f} exceeds $0.20 guard.")
        return 3

    # --- recall gate: all 1000 queries via one query_batch job ---
    t_q = time.time()
    results, q_timings = client.query_batch(queries, top_k=C.TOP_K)
    query_s = time.time() - t_q
    recall, per_query = C.recall_at_k(results, truth, C.TOP_K)
    n_perfect = sum(1 for h in per_query if h == C.TOP_K)
    non_empty = sum(1 for r in results if r)
    print(f"[c1/phase1] recall@10={recall:.2f}% perfect={n_perfect}/1000 "
          f"non_empty={non_empty}/1000 recall_query_s={query_s:.1f}")

    passed = recall >= 99.999 and non_empty == 1000 and anchor_ok
    baseline = {
        "phase": "c1_phase1_baseline",
        "started_utc": started,
        "account": acct,
        "dataset": C.DATASET,
        "corpus_sha256": fp.get("content_sha256"),
        "corpus_sha256_anchor_ok": anchor_ok,
        "params": {"num_blocks": K.BASE_NUM_BLOCKS,
                   "partitions_per_block": K.BASE_PARTITIONS_PER_BLOCK,
                   "n_probe": K.BASE_N_PROBE,
                   "blocks_per_task": K.BASE_BLOCKS_PER_TASK,
                   "num_workers": K.BASE_NUM_WORKERS},
        "recall_at_10_pct": round(recall, 4),
        "perfect_10of10": n_perfect,
        "non_empty": non_empty,
        "ingest_seconds": round(ingest_s, 3),
        "ingest_cost_usd": round(ing_cost, 6),
        "ingest_upload_s": round(ing.get("upload_s", 0.0), 3),
        "ingest_index_s": round(ing.get("index_s", 0.0), 3),
        "recall_query_seconds": round(query_s, 3),
        "passed": bool(passed),
    }
    with open(os.path.join(K.RESULTS_DIR, "phase1_baseline.json"), "w") as f:
        json.dump(baseline, f, indent=2, sort_keys=True)
    print("[c1/phase1] wrote results/phase1_baseline.json")

    if not passed:
        print(f"STOP: baseline recall gate failed (recall={recall:.2f}%, "
              f"non_empty={non_empty}, anchor_ok={anchor_ok}).")
        return 1

    # --- baseline 50-query serial measure (shared anchor for all 3 axes) ---
    print("[c1/phase1] running baseline 50q serial measure ...")
    m = K.measure(client, queries, truth, n=50, warmup=3, top_k=C.TOP_K)
    m.update({"label": "base", "axis": "baseline",
              "params": baseline["params"], "started_utc": started})
    with open(os.path.join(K.RESULTS_DIR, "measure_base.json"), "w") as f:
        json.dump(m, f, indent=2, sort_keys=True)
    print(f"[c1/phase1] baseline measure: recall={m['recall_at_10_pct']}% "
          f"p50={m['latency_s']['p50']}s billed=${m['cost_billed_per_query_usd']} "
          f"wall=${m['cost_wall_per_query_usd']} thr={m['throughput_ops_s']} ops/s")
    print(f"[c1/phase1] total wall {time.time() - t_start:.1f}s")
    print("\n=== C1 PHASE 1 PASS ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
