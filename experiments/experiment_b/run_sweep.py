"""
Experiment B, Phase 3 — latency/throughput sweep for ONE (adapter, concurrency).

Drives the day-3 workload generator (constant pattern) against an adapter that
Phase 2 already ingested and verified. It does NOT ingest: setup() reconnects to
the existing 1M index (create_if_missing hydrates Blocks' params from the stored
config.json; S3 Vectors + LanceDB re-open their existing index/table). Each op is
one top_k=10 query; latency is the harness-side monotonic submit->ack in
OpResult.latency_s (finding 10 ground truth).

Fairness knobs held constant across all 9 runs:
  - same 1000-query pool, same seed (queries are sampled from the pool by the
    generator's seeded RNG, so a fixed seed => identical query sequence per run);
  - constant arrival pattern, same nominal ops_per_second (closed-loop: on a slow
    backend the realised rate is far lower — that IS the measurement);
  - top_k=10; metric fixed by the data (l2).

Per-adapter handling the brief fixes:
  - Blocks: run a warmup batch of N queries first and DISCARD it (pays the Lithops
    worker cold start once), then measure. Capped at max_safe_concurrency=1.
  - S3 Vectors: no batch query API — each generator query is its own boto3 call.
  - LanceDB: setup() already pings; the FIRST measured query is cold. We flag it
    (it is simply the first JSONL record) and do NOT warm further.

Writes results/sweep_<adapter>_c<configured>.jsonl + .manifest.json (the day-5
sink naming convention aggregate.py expects). Prints flushed for live bg logs.

Venv/env: Blocks -> eb_blocks.sh; S3 Vectors + LanceDB -> eb_aws.sh.
"""
from __future__ import annotations

import argparse
import asyncio
import functools
import os
import sys
import time

print = functools.partial(print, flush=True)  # noqa: A001 — live logs for bg runs

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))   # experiments/experiment_b -> repo root
# _HERE (for the sibling eb_common) + repo root (for the `harness` package).
for _p in (_HERE, WORKSPACE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import eb_common as C  # noqa: E402
from harness.workload import WorkloadConfig, WorkloadGenerator  # noqa: E402
from harness.sink import JSONLSink  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("adapter", choices=["blocks", "s3vectors", "lancedb"])
    ap.add_argument("concurrency", type=int, help="configured max_in_flight")
    ap.add_argument("--duration", type=float, default=45.0, dest="duration")
    ap.add_argument("--ops", type=float, default=100.0, dest="ops",
                    help="nominal ops/second (closed-loop upper bound)")
    ap.add_argument("--seed", type=int, default=42, dest="seed")
    ap.add_argument("--warmup", type=int, default=-1, dest="warmup",
                    help="warmup queries to discard; -1 = auto (5 for blocks, else 0)")
    ap.add_argument("--ts", type=int, default=None, dest="ts",
                    help="manifest timestamp (epoch s); default now")
    args = ap.parse_args()
    adapter, conc = args.adapter, args.concurrency

    warmup = args.warmup
    if warmup < 0:
        warmup = 5 if adapter == "blocks" else 0

    os.makedirs(C.RESULTS_DIR, exist_ok=True)
    acct = C.account_id()
    print(f"[sweep/{adapter} c={conc}] account={acct} duration={args.duration}s "
          f"ops={args.ops} seed={args.seed} warmup={warmup}")

    queries = C.load_queries()
    assert len(queries) == 1000, "expected 1000 query rows"

    # --- reconnect to the Phase-2 index (NO ingest) ---
    client = C.build_client(adapter, acct)
    setup_kwargs = dict(dimensions=C.DIMENSIONS, top_k_default=C.TOP_K,
                        create_if_missing=True)
    if adapter == "blocks":
        setup_kwargs["num_blocks"] = 16  # hydrated from stored config on reconnect
    client.setup(C.DATASET, **setup_kwargs)
    fp = client.dataset_fingerprint(C.DATASET)
    caps = client.capabilities()
    anchor_ok = fp.get("content_sha256") == C.CORPUS_SHA256
    print(f"[sweep/{adapter} c={conc}] setup OK metric={caps.metric} "
          f"max_safe_concurrency={caps.max_safe_concurrency} "
          f"sha256={fp.get('content_sha256')} anchor_ok={anchor_ok}")
    if not anchor_ok:
        print("STOP: corpus sha256 anchor mismatch — refusing to sweep stale data")
        return 1

    # --- warmup (Blocks only): discard timings ---
    if warmup > 0:
        t = time.time()
        client.query_batch(queries[:warmup], top_k=C.TOP_K)
        print(f"[sweep/{adapter} c={conc}] warmup {warmup} queries discarded "
              f"in {time.time() - t:.1f}s")

    # --- measured run ---
    config = WorkloadConfig(pattern="constant", duration_s=args.duration,
                            ops_per_second=args.ops, max_in_flight=conc,
                            top_k=C.TOP_K, seed=args.seed)
    gen = WorkloadGenerator(client, queries)
    eff = gen.effective_concurrency(config)
    print(f"[sweep/{adapter} c={conc}] configured_concurrency={conc} "
          f"effective_concurrency={eff}")

    sink = JSONLSink(client, config, dataset_name=C.DATASET,
                     output_dir=C.RESULTS_DIR, generator=gen, timestamp=args.ts)
    # deterministic per-cell names (aggregate.py derives the manifest by convention)
    stem = f"sweep_{adapter}_c{conc}"
    sink.jsonl_path = os.path.join(C.RESULTS_DIR, f"{stem}.jsonl")
    sink.manifest_path = os.path.join(C.RESULTS_DIR, f"{stem}.manifest.json")

    t0 = time.time()
    paths = asyncio.run(sink.consume(gen.run(config)))
    wall = time.time() - t0
    print(f"[sweep/{adapter} c={conc}] DONE records={paths.n_records} "
          f"wall={wall:.1f}s realised_rate={paths.n_records / wall:.2f} ops/s")
    print(f"[sweep/{adapter} c={conc}] wrote {os.path.relpath(paths.jsonl, WORKSPACE)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
