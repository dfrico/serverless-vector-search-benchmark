"""
Experiment A (local) — Blocks recall at 1M.

Verify Blocks returns correct neighbours at the real 1M corpus size. This is a
one-time system-correctness check (supplement non-goals: recall is NOT a thesis
metric), so we run it once and report PASS / TUNED / DEGRADED / FAIL.

Why local, not AWS
------------------
Recall is a property of the index (num_blocks / partitions_per_block / n_probe),
the data, and FAISS — not of WHERE the map/reduce workers run. Lithops-localhost
and Lithops-on-Lambda run the SAME Blocks indexing + search code, so they return
the SAME neighbour ids for a fixed build. Running locally answers the recall
question at zero AWS cost. The AWS bring-up is deferred to experiment B, where
latency and cost (which DO depend on the substrate) are the point. (User decision,
2026-08-22.)

Recall vs the workload generator
--------------------------------
The day-3 generator's OpResult does NOT persist neighbour ids (workload.py:232
discards the hits), so recall cannot be read from its JSONL — that path is a
timing instrument. Recall is computed here by calling query_batch(all queries)
directly and comparing ids to ground truth, exactly as
harness/tests/smoke_blocks_local.py does. query_batch sends every query in ONE Lithops map/reduce job (blocks_client
query_batch -> VectorDBClient.query_batch), so 1000 queries cost a few jobs, not
1000. We additionally run a small generator burst of single queries at
max_in_flight=1 to record an INDICATIVE per-query wall-clock at 1M (each single
query is its own full job — issuing all 1000 one-at-a-time would be needlessly
slow, so a sample is enough for the "indicative median" the brief asks for).

Data (finding 17, phase 1)
--------------------------
  corpus : datasets/deep_1m/vectors_deep_1m.csv            (1,000,000 x 96, ids 0..)
  queries: datasets/deep_1m/queries_deep_1m_1000q.csv      (first 1000, 96 floats)
  truth  : datasets/deep_1m/true_neighbours_deep_1m_1000q.csv (1000 x 10 exact ids)

Ground-truth ids and Blocks-returned ids share the CSV's re-numbered id space
(Blocks preserves column-0 ids: capabilities().id_stability == "preserved"), so
they compare directly.

Usage (harness/.venv + MinIO env)
    python recall_1m.py --num-blocks 16
    python recall_1m.py --num-blocks 64 --nlist 64 --n-probe 16   # a tuning attempt
    python recall_1m.py --num-blocks 16 --skip-ingest             # reuse a built index
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))   # experiments/experiment_a -> repo root
if WORKSPACE not in sys.path:                          # repo root for the `harness` package
    sys.path.insert(0, WORKSPACE)

DATA_DIR = os.path.join(WORKSPACE, "datasets", "deep_1m")
VECTORS_CSV = os.path.join(DATA_DIR, "vectors_deep_1m.csv")
QUERIES_CSV = os.path.join(DATA_DIR, "queries_deep_1m_1000q.csv")
TRUTH_CSV = os.path.join(DATA_DIR, "true_neighbours_deep_1m_1000q.csv")
# RESULTS_TAG selects the output folder: unset or "orig" -> results/ (the August
# evidence the 2 Sep thesis cites); any other tag -> results_<tag>/ (2026-10-16).
_TAG = os.environ.get("RESULTS_TAG", "orig")
RESULTS_DIR = os.path.join(_HERE, "results" if _TAG in ("", "orig") else f"results_{_TAG}")

DIMENSIONS = 96
TOP_K = 10


def load_queries(path):
    """[[float,...]] — one 96-float query per line, no id column."""
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                out.append([float(x) for x in line.split(" ") if x])
    return out


def load_truth(path):
    """[[int,...]] — one row of exact top-10 neighbour ids per query."""
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                out.append([int(x) for x in line.split(",") if x != ""])
    return out


def recall_at_k(results, truth, k):
    """
    mean over queries of |returned top-k ids  ∩  truth top-k ids| / k.
    `results[i]` is a list of (id, dist) from the adapter; `truth[i]` a list of ids.
    Also returns per-query hit counts so we can see the distribution.
    """
    per_query = []
    for r, g in zip(results, truth):
        got = {int(vid) for vid, _ in r[:k]}
        gset = set(int(x) for x in g[:k])
        per_query.append(len(got & gset))
    denom = k * len(per_query)
    recall = 100.0 * sum(per_query) / denom if denom else 0.0
    return recall, per_query


def run_timing_sample(client, query_pool, dataset, n_sample, timestamp):
    """
    Indicative per-query latency at 1M: a burst of `n_sample` single queries at
    max_in_flight=1 (Blocks' concurrency-safe cap), persisted by the day-5 sink.
    Returns (median_wall_clock_s, sink_paths).
    """
    from harness.workload import WorkloadGenerator, WorkloadConfig
    from harness.sink import JSONLSink
    config = WorkloadConfig(pattern="burst", burst_size=n_sample, cycles=1,
                            idle_s=0.0, max_in_flight=1, top_k=TOP_K, seed=None)
    gen = WorkloadGenerator(client, query_pool)
    sink = JSONLSink(client, config, dataset_name=dataset, output_dir=RESULTS_DIR,
                     generator=gen, timestamp=timestamp)
    stem = f"recall1m_{timestamp}_timing"
    sink.jsonl_path = os.path.join(RESULTS_DIR, f"{stem}.jsonl")
    sink.manifest_path = os.path.join(RESULTS_DIR, f"{stem}.manifest.json")

    # Let the sink consume the run (single source of truth), then read per-query
    # latencies back from the JSONL it wrote.
    paths = asyncio.run(sink.consume(gen.run(config)))
    latencies = []
    with open(sink.jsonl_path) as f:
        for line in f:
            rec = json.loads(line)
            if rec.get("op") == "query" and rec.get("ok"):
                latencies.append(rec["latency_s"])
    median = statistics.median(latencies) if latencies else float("nan")
    return median, paths, latencies


def classify(recall):
    """Recall decision tree from the brief."""
    if recall >= 95.0:
        return "PASS"
    if recall >= 50.0:
        return "DEGRADED"       # tune; STOP after 3 attempts -> report DEGRADED
    return "FAIL"               # same failure as 1k -> STOP and report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--num-blocks", type=int, default=16, dest="num_blocks")
    ap.add_argument("--nlist", type=int, default=None,
                    help="partitions_per_block (IVF nlist); default = adapter default (10)")
    ap.add_argument("--n-probe", type=int, default=None, dest="n_probe",
                    help="IVF n_probe; default = adapter default (10)")
    ap.add_argument("--timing-sample", type=int, default=25, dest="timing_sample",
                    help="single queries for the indicative latency sample")
    ap.add_argument("--skip-ingest", action="store_true",
                    help="reuse an already-built index for this dataset")
    ap.add_argument("--dataset", default=None,
                    help="dataset name; default derives from the params")
    ap.add_argument("--keep-data", action="store_true",
                    help="do NOT drop the index/dataset from MinIO at the end")
    args = ap.parse_args()

    from harness.adapters.blocks import BlocksClient

    ts = int(time.time())
    params_tag = f"nb{args.num_blocks}"
    if args.nlist:
        params_tag += f"_nl{args.nlist}"
    if args.n_probe:
        params_tag += f"_np{args.n_probe}"
    dataset = args.dataset or f"deep1m_{params_tag}"

    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[recall1m] dataset={dataset} num_blocks={args.num_blocks} "
          f"nlist={args.nlist} n_probe={args.n_probe} start={started}")

    queries = load_queries(QUERIES_CSV)
    truth = load_truth(TRUTH_CSV)
    print(f"[recall1m] loaded {len(queries)} queries, {len(truth)} truth rows")
    assert len(queries) == len(truth) == 1000, "expected 1000 queries + 1000 truth rows"

    client = BlocksClient(bucket="lithops-data", vectors_csv_path=VECTORS_CSV)
    client.setup(dataset, dimensions=DIMENSIONS, top_k_default=TOP_K,
                 num_blocks=args.num_blocks,
                 partitions_per_block=args.nlist, n_probe=args.n_probe,
                 create_if_missing=True)
    print(f"[recall1m] setup OK — num_blocks={client.num_blocks} "
          f"nlist={client.partitions_per_block} n_probe={client.n_probe} "
          f"blocks_per_task={client.blocks_per_task} num_workers={client._DEFAULT_NUM_WORKERS}")

    ingest_s = None
    ingest_timings = None
    if not args.skip_ingest:
        t0 = time.time()
        _none, ingest_timings = client.bulk_insert_from_path(VECTORS_CSV, build_index=True)
        ingest_s = time.time() - t0
        print(f"[recall1m] ingest+index build: {ingest_s:.1f}s "
              f"(upload {ingest_timings.get('bulk_insert_upload_s', 0):.1f}s, "
              f"index {ingest_timings.get('bulk_insert_index_s', 0):.1f}s)")
    else:
        print("[recall1m] --skip-ingest: reusing existing index")

    # --- recall: all 1000 queries in one batched query job ---
    t1 = time.time()
    results, q_timings = client.query_batch(queries, top_k=TOP_K)
    batch_s = time.time() - t1
    recall, per_query = recall_at_k(results, truth, TOP_K)
    n_perfect = sum(1 for h in per_query if h == TOP_K)
    n_zero = sum(1 for h in per_query if h == 0)
    print(f"[recall1m] recall@{TOP_K} = {recall:.2f}%  "
          f"(batched {len(queries)} queries in {batch_s:.1f}s; "
          f"perfect={n_perfect}/1000, zero-hit={n_zero}/1000)")

    # --- indicative per-query latency sample via the generator + sink ---
    median_q_s, timing_paths, sample_latencies = run_timing_sample(
        client, queries, dataset, args.timing_sample, ts)
    print(f"[recall1m] indicative per-query wall-clock (n={len(sample_latencies)}): "
          f"median {median_q_s:.2f}s")

    verdict = classify(recall)

    summary = {
        "experiment": "A_blocks_recall_1m_local",
        "timestamp": ts,
        "started_utc": started,
        "dataset": dataset,
        "params": {
            "num_blocks": client.num_blocks,
            "partitions_per_block": client.partitions_per_block,
            "n_probe": client.n_probe,
            "blocks_per_task": client.blocks_per_task,
            "num_workers": client._DEFAULT_NUM_WORKERS,
        },
        "corpus_fingerprint": client.dataset_fingerprint(dataset),
        "recall_at_10_pct": round(recall, 4),
        "per_query_hits": {
            "perfect_10of10": n_perfect,
            "zero_hits": n_zero,
            "mean_hits": round(sum(per_query) / len(per_query), 4),
            "min_hits": min(per_query),
            "max_hits": max(per_query),
        },
        "verdict": verdict,
        "ingest_seconds": ingest_s,
        "ingest_timings_keys": sorted(ingest_timings.keys()) if ingest_timings else None,
        "recall_batch_seconds": round(batch_s, 2),
        "indicative_query_wall_clock_s": {
            "median": round(median_q_s, 4),
            "n_sample": len(sample_latencies),
            "min": round(min(sample_latencies), 4) if sample_latencies else None,
            "max": round(max(sample_latencies), 4) if sample_latencies else None,
        },
        "query0_top10_returned": [int(i) for i, _ in results[0][:TOP_K]],
        "query0_top10_truth": [int(x) for x in truth[0][:TOP_K]],
        "timing_jsonl": os.path.relpath(timing_paths.jsonl, WORKSPACE),
        "timing_manifest": os.path.relpath(timing_paths.manifest, WORKSPACE),
    }
    out_path = os.path.join(RESULTS_DIR, f"recall1m_{ts}_{params_tag}_summary.json")
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2, sort_keys=True)
    print(f"[recall1m] summary -> {os.path.relpath(out_path, WORKSPACE)}")
    print(f"[recall1m] VERDICT = {verdict} (recall {recall:.2f}%)")

    if not args.keep_data:
        client.teardown(drop_data=True)
        print("[recall1m] teardown(drop_data=True) done")
    else:
        client.teardown(drop_data=False)
        print("[recall1m] kept data in MinIO (--keep-data)")

    print("\n" + json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
