"""
Experiment C1, Phase 2 — one sweep cell per invocation.

Axes:
  A (n_probe):       rebuild the index at n_probe=VALUE (num_workers=16, bpt=16),
                     then 50-query serial measure. n_probe is baked at build time.
  B (num_workers):   rebuild the index at num_workers=VALUE (n_probe=10, bpt=16),
                     record ingest cost, then 50-query serial measure to confirm
                     the query metrics are flat. num_workers is ingest-only.
  C (blocks_per_task): rewrite config.json's query_batch_size=VALUE (no rebuild),
                     then 50-query serial measure. blocks_per_task is query-time.

Rebuilds never re-upload the corpus (identical bytes across every cell; Phase 1
uploaded it once). Each rebuild is guarded: if its estimated ingest cost exceeds
$0.20 the run STOPS (exit 3) before any queries, per the brief.

Writes results/measure_<label>.json. Run via ec_blocks.sh:
  ec_blocks.sh c1_run.py --axis A --value 7 --label a_np7
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

import c1_common as K  # noqa: E402
import eb_common as C  # noqa: E402

INGEST_GUARD_USD = 0.20


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--axis", required=True, choices=["A", "B", "C"])
    ap.add_argument("--value", required=True, type=int)
    ap.add_argument("--label", required=True)
    ap.add_argument("--n", type=int, default=50)
    ap.add_argument("--warmup", type=int, default=3)
    args = ap.parse_args()

    os.makedirs(K.RESULTS_DIR, exist_ok=True)
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    t_start = time.time()
    acct = K.account_id()
    print(f"[c1/{args.label}] axis={args.axis} value={args.value} start={started}")

    queries = C.load_queries()
    truth = C.load_truth()

    client = K.build_blocks(acct)
    # setup hydrates from the resident config; pin_params then forces our values.
    client.setup(C.DATASET, dimensions=C.DIMENSIONS, top_k_default=C.TOP_K,
                 create_if_missing=False)

    # resolve the swept parameter for this axis
    n_probe = K.BASE_N_PROBE
    num_workers = K.BASE_NUM_WORKERS
    bpt = K.BASE_BLOCKS_PER_TASK
    if args.axis == "A":
        n_probe = args.value
    elif args.axis == "B":
        num_workers = args.value
    elif args.axis == "C":
        bpt = args.value

    ingest_cost = None
    ingest = {}
    if args.axis in ("A", "B"):
        K.pin_params(client, n_probe=n_probe, blocks_per_task=K.BASE_BLOCKS_PER_TASK,
                     num_workers=num_workers)
        print(f"[c1/{args.label}] rebuild n_probe={n_probe} num_workers={num_workers} "
              f"bpt={K.BASE_BLOCKS_PER_TASK} (no re-upload) ...")
        ingest, ingest_cost = K.build_index(client, num_workers=num_workers, upload=False)
        print(f"[c1/{args.label}] rebuild index_s={ingest.get('index_s', 0):.1f} "
              f"ingest_cost=${ingest_cost:.4f}")
        if ingest_cost > INGEST_GUARD_USD:
            print(f"STOP: ingest cost ${ingest_cost:.4f} exceeds ${INGEST_GUARD_USD} guard.")
            return 3
    else:  # axis C — config rewrite only
        K.pin_params(client, n_probe=K.BASE_N_PROBE, blocks_per_task=bpt,
                     num_workers=K.BASE_NUM_WORKERS)
        cfg = K.set_blocks_per_task(client, bpt)
        print(f"[c1/{args.label}] set query_batch_size={cfg['query_batch_size']} "
              f"(config rewrite, no rebuild)")

    # --- 50-query serial measure ---
    print(f"[c1/{args.label}] measuring {args.n} queries (warmup {args.warmup}) ...")
    m = K.measure(client, queries, truth, n=args.n, warmup=args.warmup, top_k=C.TOP_K,
                  raw_path=os.path.join(K.RESULTS_DIR, f"measure_{args.label}.raw.json"))
    m.update({
        "label": args.label, "axis": args.axis, "swept_value": args.value,
        "started_utc": started,
        "params": {"num_blocks": K.BASE_NUM_BLOCKS,
                   "partitions_per_block": K.BASE_PARTITIONS_PER_BLOCK,
                   "n_probe": n_probe, "blocks_per_task": bpt,
                   "num_workers": num_workers},
    })
    if ingest_cost is not None:
        m["ingest_cost_usd"] = round(ingest_cost, 6)
        m["ingest_index_s"] = round(ingest.get("index_s", 0.0), 3)
        durs = ingest.get("index_timers", {}).get("indexing_function_blocks") or []
        m["ingest_worker_durations_s"] = [round(float(d), 3) for d in durs
                                          if isinstance(d, (int, float))]

    out = os.path.join(K.RESULTS_DIR, f"measure_{args.label}.json")
    with open(out, "w") as f:
        json.dump(m, f, indent=2, sort_keys=True)
    print(f"[c1/{args.label}] recall={m['recall_at_10_pct']}% "
          f"p50={m['latency_s']['p50']}s p95={m['latency_s']['p95']}s "
          f"billed=${m['cost_billed_per_query_usd']} wall=${m['cost_wall_per_query_usd']} "
          f"avg_map_tasks={m['avg_n_map_tasks']} thr={m['throughput_ops_s']} ops/s")
    print(f"[c1/{args.label}] wrote {os.path.relpath(out, K.WORKSPACE)} "
          f"in {time.time() - t_start:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
