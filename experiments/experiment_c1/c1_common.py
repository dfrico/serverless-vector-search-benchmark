"""
Experiment C1 — Blocks parameter tuning at 1M. Shared helpers.

Goal: sweep Blocks-specific parameters against the same deep_1m corpus used in
Experiment B, to answer whether Blocks' ~190x cost gap vs S3 Vectors is
architectural (a per-query serverless-job floor) or tuning-recoverable.

We reuse Experiment B's loaders, recall, corpus anchor, and pricing from
eb_common. We add Blocks-specific build/measure/cost helpers here.

Parameter mechanics (proven by reading the reference repo; see findings/22):
  - n_probe        is baked into each .ann at BUILD time (index.nprobe is written
                   into the FAISS index and read back by the map worker). Changing
                   it needs a re-ingest (index rebuild).
  - num_workers    sets the ingest fan-out (obj_chunk_number), but it also fixes
                   the total partition count. indexator.py has no "distribute"
                   module for blocks, so each of the num_workers ingest workers
                   writes n_blocks_per_function = max(1, num_index // num_workers)
                   partitions (centroid_{id*npf ...}). Total = num_workers x npf.
                   The query only ever reads partitions 0..num_index-1.
                     * num_workers <= num_index (and divides it): total = num_index,
                       coverage is complete, recall and query metrics are unchanged;
                       only ingest time rises as workers drop (more serial work each).
                     * num_workers  > num_index: total = num_workers > num_index, so
                       partitions num_index..num_workers-1 are written but NEVER read.
                       Those vectors become unqueryable and recall collapses (we saw
                       51.2% at num_workers=32, num_index=16). Ingest is also slowest
                       here (most cold starts + connection-pool contention).
                   So the sweet spot is num_workers == num_index; it is both correct
                   and the fastest ingest. Changing num_workers needs a rebuild.
  - blocks_per_task (query_batch_size) is a QUERY-time parameter read fresh from
                   config.json on every search. Changing it is a config rewrite,
                   no rebuild.

Two cost models (both computed per query from the orchestrator timers):
  - "wall"   = (map_execution + reduce_execution) x MEM_GB x rate + requests.
               This is the model Experiment B used; its baseline anchor is
               $0.000477/query at 100% recall. map_execution is the WALL time of
               the map phase, so parallel fan-out does not raise it.
  - "billed" = (sum of per-task map durations + sum of per-task reduce durations)
               x MEM_GB x rate + one request per task. This is the honest Lambda
               charge: fan-out multiplies it. At the baseline (1 map task) the two
               models coincide; under blocks_per_task fan-out they diverge, which
               is the Axis C finding.
"""
from __future__ import annotations

import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))   # experiments/experiment_c1 -> repo root
RESULTS_DIR = os.path.join(_HERE, "results")

# Reuse Experiment B's shared helpers (loaders, recall, pricing, corpus anchor)
# and the AWS Blocks adapter. Put the repo root (for the `harness` package), the
# sibling experiment_b dir (for eb_common), and the external Blocks repo on path.
for _p in (WORKSPACE,
           os.path.join(WORKSPACE, "experiments", "experiment_b"),
           os.path.join(WORKSPACE, "references", "blocks-db-wip")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import eb_common as C  # noqa: E402

# Blocks Lambda memory used for the cost model. Experiment B billed both the map
# and reduce phase at 3.008 GB (BLOCKS_MEM_GB) so the $0.000477 anchor is defined
# at that memory. We keep the SAME number so C1 costs are directly comparable.
# (The reduce phase's configured memory is 2048 MB; using 3.008 for both matches
# Experiment B and slightly over-counts reduce. Noted in the deliverable.)
BLOCKS_MEM_GB = 3.008
INDEX_MEM_GB = 3.008  # ingest map workers run at the capped 3008 MB

# Baseline configuration (Experiment B parameters).
BASE_NUM_BLOCKS = 16
BASE_PARTITIONS_PER_BLOCK = 10   # FAISS IVF nlist
BASE_N_PROBE = 10
BASE_BLOCKS_PER_TASK = 16
BASE_NUM_WORKERS = 16


def account_id():
    return C.account_id()


def build_blocks(acct):
    """Construct the AWS Blocks adapter bound to the deep_1m corpus."""
    from harness.adapters.blocks_aws import BlocksClientAWS
    return BlocksClientAWS(bucket=f"thesis-blocks-{acct}",
                           vectors_csv_path=C.VECTORS_CSV, region="us-east-1")


def pin_params(client, *, n_probe, num_blocks=BASE_NUM_BLOCKS,
               partitions_per_block=BASE_PARTITIONS_PER_BLOCK,
               blocks_per_task=BASE_BLOCKS_PER_TASK, num_workers=BASE_NUM_WORKERS):
    """Force the adapter's build parameters AFTER setup().

    setup() hydrates params from an existing config.json, which would override
    the value we want to sweep. Setting the instance attributes here makes
    _build_config() use exactly these values. num_workers shadows the class
    default and drives only the ingest fan-out.
    """
    client.num_blocks = num_blocks
    client.partitions_per_block = partitions_per_block
    client.n_probe = n_probe
    client.blocks_per_task = blocks_per_task
    client.dimensions = C.DIMENSIONS
    client.top_k_default = C.TOP_K
    client._DEFAULT_NUM_WORKERS = num_workers


def build_index(client, *, num_workers, upload=False):
    """Build (or rebuild) the Blocks index for the pinned parameters.

    upload=True first uploads the corpus CSV (upload_dataset also clears old
    .ann files). upload=False reuses the resident corpus and only clears stale
    .ann files before rebuilding — the corpus bytes are identical across every
    C1 configuration, so we upload once in Phase 1 and rebuild in place after.

    Returns (timings, ingest_cost_usd).
    """
    from vectordb.serverless_vectordb import ServerlessVectorDB
    from vectordb.utils.index_ops import save_index_config, delete_indexes
    from vectordb.utils.dataset_ops import upload_dataset

    ds = client.dataset_name
    cfg = client._build_config()
    timings = {}

    t0 = time.time()
    if upload:
        upload_dataset(client.bucket, ds, client.vectors_csv_path)  # also clears .ann
        timings["upload_s"] = time.time() - t0
    else:
        delete_indexes(client.bucket, ds)  # clear stale .ann before rebuild
        timings["clear_s"] = time.time() - t0

    sv = ServerlessVectorDB(**cfg)
    t1 = time.time()
    idx_timers = sv.indexing(f"vectors_{ds}.csv", num_workers=num_workers)
    timings["index_s"] = time.time() - t1
    save_index_config(client.bucket, ds, cfg)
    timings["index_timers"] = idx_timers

    cost = ingest_cost(idx_timers)
    timings["ingest_cost_usd"] = cost
    return timings, cost


def ingest_cost(idx_timers):
    """Honest Lambda charge for one index build, in USD.

    initialize_database returns indexing_function_blocks = the list of per-worker
    return values, and generate_index_blocks returns its own elapsed seconds, so
    this list is the per-worker billed durations. Cost = sum(durations) x 3.008GB
    x x86 rate + one request per worker.
    """
    durs = idx_timers.get("indexing_function_blocks") or []
    durs = [float(d) for d in durs if isinstance(d, (int, float))]
    gbs = sum(durs) * INDEX_MEM_GB
    n_act = len(durs)
    return gbs * C.LAMBDA_X86_GBS + n_act * (C.LAMBDA_REQ_PER_M / 1e6)


def set_blocks_per_task(client, bpt):
    """Rewrite config.json's query_batch_size in place (no rebuild)."""
    from vectordb.utils.index_ops import load_index_config, save_index_config
    ds = client.dataset_name
    cfg = load_index_config(client.bucket, ds, "blocks", None)
    cfg["query_batch_size"] = bpt
    save_index_config(client.bucket, ds, cfg)
    client.blocks_per_task = bpt
    return cfg


def query_costs(timings):
    """Return (wall_cost, billed_cost, n_map, n_reduce, billed_map_s,
    billed_reduce_s, map_execution_s, reduce_execution_s) for one query."""
    req = C.LAMBDA_REQ_PER_M / 1e6
    map_exec = float(timings.get("0_map_execution_blocks", 0.0) or 0.0)
    red_exec = float(timings.get("0_reduce_execution_blocks", 0.0) or 0.0)
    n_map = len(timings.get("0_map_invocation_blocks", []) or [])
    n_reduce = len(timings.get("0_reduce_invocation_blocks", []) or [])

    # per-task durations: map_times[t][0] is that task's total elapsed seconds;
    # reduce_times[t] is a float.
    map_blocks = timings.get("0_map_blocks", []) or []
    billed_map_s = 0.0
    for t in map_blocks:
        if isinstance(t, (list, tuple)) and t:
            billed_map_s += float(t[0])
        elif isinstance(t, (int, float)):
            billed_map_s += float(t)
    reduce_blocks = timings.get("0_reduce_blocks", []) or []
    billed_reduce_s = sum(float(x) for x in reduce_blocks
                          if isinstance(x, (int, float)))

    wall_cost = (map_exec + red_exec) * BLOCKS_MEM_GB * C.LAMBDA_X86_GBS \
        + (n_map + n_reduce) * req
    billed_cost = (billed_map_s + billed_reduce_s) * BLOCKS_MEM_GB * C.LAMBDA_X86_GBS \
        + (n_map + n_reduce) * req
    return (wall_cost, billed_cost, n_map, n_reduce, billed_map_s,
            billed_reduce_s, map_exec, red_exec)


def percentiles(values):
    """min/p50/p95/p99/max/mean with linear interpolation, on a copy."""
    if not values:
        return {k: 0.0 for k in ("min", "p50", "p95", "p99", "max", "mean")}
    xs = sorted(values)
    n = len(xs)

    def pct(p):
        if n == 1:
            return xs[0]
        rank = p / 100.0 * (n - 1)
        lo = int(rank)
        hi = min(lo + 1, n - 1)
        frac = rank - lo
        return xs[lo] + (xs[hi] - xs[lo]) * frac

    return {
        "min": xs[0], "p50": pct(50), "p95": pct(95), "p99": pct(99),
        "max": xs[-1], "mean": sum(xs) / n,
    }


def measure(client, queries, truth, *, n=50, warmup=3, top_k=None):
    """Run warmup (discarded) + n serial single-vector queries, max_in_flight=1.

    Returns a dict with recall@10, latency percentiles, throughput, and both
    cost models (mean per query), plus per-query raw latency/cost lists.
    """
    top_k = top_k if top_k is not None else C.TOP_K
    total = warmup + n
    assert len(queries) >= total, f"need >= {total} queries, have {len(queries)}"

    # warmup (discarded) — steady-state runtime, not cold start.
    for i in range(warmup):
        client.query(queries[i], top_k=top_k)

    lats, wall_costs, billed_costs = [], [], []
    n_maps, n_reduces, billed_map_ss, map_exec_ss = [], [], [], []
    hits_ids = []
    for i in range(warmup, total):
        hits, timings = client.query(queries[i], top_k=top_k)
        lats.append(float(timings.get("wall_clock_s", 0.0)))
        (wc, bc, nm, nr, bms, brs, mex, rex) = query_costs(timings)
        wall_costs.append(wc)
        billed_costs.append(bc)
        n_maps.append(nm)
        n_reduces.append(nr)
        billed_map_ss.append(bms)
        map_exec_ss.append(mex)
        hits_ids.append(hits)

    measured_truth = truth[warmup:total]
    recall, per_query = C.recall_at_k(hits_ids, measured_truth, top_k)
    n_perfect = sum(1 for h in per_query if h == top_k)
    total_time = sum(lats)
    throughput = n / total_time if total_time else 0.0

    return {
        "n": n, "warmup": warmup, "top_k": top_k,
        "recall_at_10_pct": round(recall, 4),
        "perfect_10of10": n_perfect,
        "latency_s": {k: round(v, 4) for k, v in percentiles(lats).items()},
        "throughput_ops_s": round(throughput, 4),
        "cost_wall_per_query_usd": round(sum(wall_costs) / n, 8),
        "cost_billed_per_query_usd": round(sum(billed_costs) / n, 8),
        "avg_n_map_tasks": round(sum(n_maps) / n, 2),
        "avg_n_reduce_tasks": round(sum(n_reduces) / n, 2),
        "avg_billed_map_s": round(sum(billed_map_ss) / n, 4),
        "avg_map_execution_s": round(sum(map_exec_ss) / n, 4),
        "raw_latencies_s": [round(x, 4) for x in lats],
        "raw_cost_billed_usd": [round(x, 8) for x in billed_costs],
    }
