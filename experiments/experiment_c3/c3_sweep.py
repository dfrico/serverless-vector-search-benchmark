"""
Experiment C3, Phase 3 — latency/throughput sweep for ONE (adapter, concurrency)
cell at 10M. Same shape as Experiment B Phase 3 (finding 20): the day-3 constant
workload generator drives an adapter that Phase 2 already ingested and verified.
It does NOT ingest; setup() reconnects to the existing 10M index.

Differences from Experiment B, and why:
  - Concurrency is capped per adapter by capability (Blocks 1, S3V 4, LanceDB 10),
    so we only run the levels that are meaningful. A level above the cap collapses
    to the cap and would duplicate a cell we already have.
  - 5 warmup queries per cell are discarded (pays the container cold start once).
  - Resilience: the measured run is wrapped in the C2 driver's transient-fault
    guard (experiments/experiment_c2/c2_driver.py). A transient environment fault
    (e.g. a local clock skew after laptop sleep expiring an AWS signature) must
    not lose the whole sweep. On such a fault we retry the cell ONCE from a fresh
    client (a fresh cell file, so no partial double-count); a second failure exits
    non-zero. We do NOT retry individual queries — a cold query retried warm would
    be mislabeled.

Cell size: the generator is duration-bounded (closed-loop), so n varies with
adapter speed — exactly as Experiment B. --duration is chosen per cell to target
~500 ok queries on the fast adapter; slow adapters (Blocks ~20s/query serial)
yield far fewer in the same wall budget, and we record the actual n. Cost/query
is computed at aggregate time with the billed model (finding 22).

Run via c3_blocks.sh (Blocks) or c3_aws.sh (S3V, LanceDB):
    ./c3_blocks.sh c3_sweep.py blocks 1 --duration 600
    ./c3_aws.sh    c3_sweep.py s3vectors 4 --duration 30
Writes results/sweep_<adapter>_c<conc>.jsonl + .manifest.json.
"""
from __future__ import annotations

import argparse
import asyncio
import functools
import os
import sys
import time

print = functools.partial(print, flush=True)  # noqa: A001

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
import c3_common as K  # noqa: E402
import eb_common as C  # noqa: E402
from harness.workload import WorkloadConfig, WorkloadGenerator  # noqa: E402
from harness.sink import JSONLSink  # noqa: E402


def build_setup(adapter):
    """Reconnect to the Phase-2 10M index (create_if_missing hydrates Blocks'
    stored config; for Blocks we re-pin the 10M params to be safe)."""
    return K.build_and_setup(adapter, create_if_missing=True)


def run_cell(adapter, conc, args, attempt):
    """One measured cell. Returns the sink result. Raises on failure so the
    caller's guard can retry."""
    queries = K.load_queries()
    assert len(queries) == 1000, "expected 1000 query rows"

    client = build_setup(adapter)
    fp = client.dataset_fingerprint(K.DATASET)
    caps = client.capabilities()
    anchor_ok = fp.get("content_sha256") == K.CORPUS_SHA256
    print(f"[c3/sweep/{adapter} c={conc}] attempt={attempt} setup OK "
          f"metric={caps.metric} max_safe_conc={caps.max_safe_concurrency} "
          f"sha256={fp.get('content_sha256')} anchor_ok={anchor_ok}")
    if not anchor_ok:
        raise RuntimeError("corpus sha256 anchor mismatch — refusing to sweep stale data")

    if adapter == "lancedb":
        # single queries fit well inside 120s; keep adaptive retries for transient
        # throttles under concurrency 10.
        client.configure_client(read_timeout_s=120, max_retries=8, retry_mode="adaptive")

    # warmup (discarded): pays the container cold start once.
    if args.warmup > 0:
        t = time.time()
        client.query_batch(queries[:args.warmup], top_k=C.TOP_K)
        print(f"[c3/sweep/{adapter} c={conc}] warmup {args.warmup} discarded "
              f"in {time.time() - t:.1f}s")

    config = WorkloadConfig(pattern="constant", duration_s=args.duration,
                            ops_per_second=args.ops, max_in_flight=conc,
                            top_k=C.TOP_K, seed=args.seed)
    gen = WorkloadGenerator(client, queries)
    eff = gen.effective_concurrency(config)
    print(f"[c3/sweep/{adapter} c={conc}] configured={conc} effective={eff} "
          f"duration={args.duration}s ops={args.ops}")

    sink = JSONLSink(client, config, dataset_name=K.DATASET,
                     output_dir=K.RESULTS_DIR, generator=gen, timestamp=args.ts)
    stem = f"sweep_{adapter}_c{conc}"
    sink.jsonl_path = os.path.join(K.RESULTS_DIR, f"{stem}.jsonl")
    sink.manifest_path = os.path.join(K.RESULTS_DIR, f"{stem}.manifest.json")

    t0 = time.time()
    paths = asyncio.run(sink.consume(gen.run(config)))
    wall = time.time() - t0
    print(f"[c3/sweep/{adapter} c={conc}] DONE records={paths.n_records} "
          f"wall={wall:.1f}s realised={paths.n_records / wall:.2f} ops/s")
    return paths


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("adapter", choices=["blocks", "s3vectors", "lancedb"])
    ap.add_argument("concurrency", type=int)
    ap.add_argument("--duration", type=float, default=45.0)
    ap.add_argument("--ops", type=float, default=100.0)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--warmup", type=int, default=5)
    ap.add_argument("--ts", type=int, default=None)
    args = ap.parse_args()
    adapter, conc = args.adapter, args.concurrency

    os.makedirs(K.RESULTS_DIR, exist_ok=True)
    cap = K.CONCURRENCY_CAP[adapter]
    if conc > cap:
        print(f"[c3/sweep/{adapter}] NOTE: requested c={conc} > adapter cap {cap}; "
              f"the generator will collapse effective concurrency to {cap}.")

    # C2 transient-fault guard: retry the cell ONCE on a transient environment
    # fault, from a fresh client + fresh cell file. A second failure is fatal.
    for attempt in (1, 2):
        try:
            run_cell(adapter, conc, args, attempt)
            print(f"\n=== C3 SWEEP {adapter.upper()} c={conc} DONE ===")
            return 0
        except Exception as e:  # noqa: BLE001 — keep the sweep alive across a transient fault
            print(f"[c3/sweep/{adapter} c={conc}] attempt {attempt} ABORTED "
                  f"(transient?): {e!r}")
            if attempt == 2:
                print(f"\n=== C3 SWEEP {adapter.upper()} c={conc} FAILED (2 attempts) ===")
                return 1
            print(f"[c3/sweep/{adapter} c={conc}] retrying once from a fresh client ...")
            time.sleep(10)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
