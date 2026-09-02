"""
Day-3 smoke test: drive BlocksClient through the three workload patterns.

Runs against the day-1 `deep_100k` index (queries) + a separate ephemeral
`day3_inserts_*` dataset (mixed inserts), so the queried index is never mutated.

IMPORTANT empirical context (findings/10):
  - A single Blocks query on localhost is ~3-5s (each spins up a full Lithops
    map/reduce job). The nominal `ops_per_second` is an UPPER BOUND — the
    closed-loop generator throttles to achievable throughput. Count assertions
    use achievable throughput with generous slack, NOT the nominal rate.
  - Blocks' query path is CONCURRENCY-UNSAFE on the localhost backend: map
    workers download index blocks to a fixed `/tmp/index_0.ann` and delete it
    after reading, so two concurrent queries collide ("could not open
    /tmp/index_0.ann"). This is a localhost artifact (AWS Lambda gives each
    worker its own /tmp); the repos are read-only so it can't be patched here.
    => Blocks declares capabilities().max_safe_concurrency=1, and the generator
    now caps effective concurrency at that automatically (finding 11). So we
    CONFIGURE max_in_flight=2 here to exercise the cap: the generator lowers it
    to 1 for Blocks (logging a "concurrency capped" line) while other backends
    with a higher max_safe_concurrency would run at the configured value.

Run with the boto3->MinIO env vars exported (see harness/README.md).
"""
import asyncio
import csv
import os
import sys
import time

WORKSPACE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if WORKSPACE not in sys.path:
    sys.path.insert(0, WORKSPACE)

from harness.adapters.blocks import BlocksClient          # noqa: E402
from harness.workload import WorkloadGenerator, WorkloadConfig, OpResult  # noqa: E402

QUERIES_CSV = os.path.join(WORKSPACE, "spike-localhost", "queries_deep_100k.csv")
VECTORS_CSV = os.path.join(WORKSPACE, "spike-localhost", "vectors_deep_100k.csv")
BUCKET = "lithops-data"
DATASET = "deep_100k"


def load_query_pool(path, n):
    out = []
    with open(path) as f:
        for i, row in enumerate(csv.reader(f)):
            if i >= n or not row:
                break
            out.append([float(x) for x in row[0].split(" ") if x])
    return out


def load_insert_pool(path, n, start_id):
    """Held-out (id, vector) pairs with brand-new IDs (>= start_id) so they don't
    collide with the 0..99999 IDs already in the deep_100k index."""
    out = []
    with open(path) as f:
        for i, row in enumerate(csv.reader(f)):
            if i >= n or not row:
                break
            out.append((start_id + i, [float(x) for x in row[0].split(" ") if x]))
    return out


async def collect(gen, config):
    """Drain the async generator into a list of OpResult."""
    results = []
    async for r in gen.run(config):
        results.append(r)
    return results


def summarize(results):
    oks = sum(1 for r in results if r.ok)
    lats = [r.latency_s for r in results if r.ok]
    return {
        "n": len(results),
        "ok": oks,
        "fail": len(results) - oks,
        "lat_min": min(lats) if lats else None,
        "lat_max": max(lats) if lats else None,
        "by_op": {op: sum(1 for r in results if r.op == op) for op in {r.op for r in results}},
    }


def check_common(results, label, min_expected):
    """Shared assertions: count floor, all-ok, latency>0, timings non-empty."""
    checks = {}
    checks[f"{label}:count>={min_expected}"] = len(results) >= min_expected
    checks[f"{label}:all_ok"] = all(r.ok for r in results) and len(results) > 0
    checks[f"{label}:latency>0"] = all(r.latency_s > 0 for r in results) and len(results) > 0
    checks[f"{label}:timings_non_empty"] = all(bool(r.adapter_timings) for r in results) and len(results) > 0
    return checks


def main():
    query_pool = load_query_pool(QUERIES_CSV, 200)
    print(f"loaded {len(query_pool)} query vectors")

    qclient = BlocksClient(bucket=BUCKET)
    qclient.setup(DATASET, dimensions=96, top_k_default=10, create_if_missing=False)

    all_checks = {}
    summaries = {}

    # Configure max_in_flight=2 to EXERCISE the automatic concurrency cap: Blocks
    # declares max_safe_concurrency=1 (concurrency-unsafe /tmp; see module
    # docstring + findings/10/11), so the generator caps effective concurrency to
    # 1 and logs a "concurrency capped" line. Other backends with a higher
    # max_safe_concurrency would run at the configured 2.
    MIF = 2

    # --- Pattern 1: constant arrival, queries only -----------------------
    # 5 ops/s x 5s nominal = 25; achievable with ~3-5s serial queries is ~1-2, so
    # floor at 1 (very conservative).
    print(f"\n=== Pattern 1: constant (5 ops/s, 5s, queries, max_in_flight={MIF}) ===")
    gen = WorkloadGenerator(qclient, query_pool)
    cfg = WorkloadConfig(pattern="constant", ops_per_second=5, duration_s=5, max_in_flight=MIF, top_k=10)
    t0 = time.monotonic()
    res = asyncio.run(collect(gen, cfg))
    summaries["constant"] = {**summarize(res), "wall_s": round(time.monotonic() - t0, 1)}
    print("  ", summaries["constant"])
    all_checks.update(check_common(res, "constant", min_expected=1))

    # --- Pattern 2: burst, queries only ----------------------------------
    # 3 cycles x 10-op bursts = 30 issued; closed-loop => all 30 complete even at
    # max_in_flight=1 (the burst just drains serially within/after each cycle).
    print(f"\n=== Pattern 2: burst (10-op bursts, 2s idle, 3 cycles, max_in_flight={MIF}) ===")
    gen = WorkloadGenerator(qclient, query_pool)
    cfg = WorkloadConfig(pattern="burst", burst_size=10, idle_s=2, cycles=3, max_in_flight=MIF, top_k=10)
    t0 = time.monotonic()
    res = asyncio.run(collect(gen, cfg))
    summaries["burst"] = {**summarize(res), "wall_s": round(time.monotonic() - t0, 1)}
    print("  ", summaries["burst"])
    all_checks.update(check_common(res, "burst", min_expected=10))
    # burst issues exactly cycles*burst_size; closed-loop means all complete.
    all_checks["burst:issued_all_30"] = len(res) == 30

    # --- Pattern 3: mixed insert/query -----------------------------------
    # NB: window extended 10s -> 70s. At max_in_flight=1 with ~5.3s serial queries
    # a 10s window issues only ~2 ops; the deterministic 0.1 interleave emits its
    # first insert at the 10th op, so the window must be long enough to issue >=10
    # ops for an insert to actually run (inserts themselves are ~0.01s, so ~10
    # queries x 5.3s ~= 55s, plus margin). A short window makes the ratio
    # assertion vacuous (0 inserts trivially "within tol"); we additionally assert
    # >=1 insert actually ran. Direct consequence of the query-latency finding.
    print(f"\n=== Pattern 3: mixed (70s, insert_ratio=0.1, max_in_flight={MIF}) ===")
    insert_pool = load_insert_pool(QUERIES_CSV, 200, start_id=1_000_000)
    insert_ds = f"day3_inserts_{int(time.time())}"
    iclient = BlocksClient(bucket=BUCKET)
    iclient.setup(insert_ds, dimensions=96, create_if_missing=True)
    gen = WorkloadGenerator(qclient, query_pool, insert_client=iclient, insert_pool=insert_pool)
    cfg = WorkloadConfig(pattern="mixed", duration_s=70, insert_ratio=0.1,
                         insert_batch_size=100, insert_build_index=False, max_in_flight=MIF, top_k=10)
    t0 = time.monotonic()
    res = asyncio.run(collect(gen, cfg))
    summaries["mixed"] = {**summarize(res), "wall_s": round(time.monotonic() - t0, 1)}
    print("  ", summaries["mixed"])
    all_checks.update(check_common(res, "mixed", min_expected=2))
    n_ins = sum(1 for r in res if r.op == "bulk_insert")
    actual_ratio = n_ins / len(res) if res else 0.0
    # Require an insert to ACTUALLY run, else the ratio check is vacuous.
    all_checks["mixed:insert_ran"] = n_ins >= 1
    # within +/-20% of target (0.1) -> [0.08, 0.12]; allow a small absolute floor
    # because at tiny N the discrete ratio is coarse (e.g. 1/10=0.10, 1/9=0.11).
    ratio_ok = abs(actual_ratio - 0.1) <= 0.02 + (1.0 / max(len(res), 1))
    all_checks["mixed:ratio_within_tol"] = ratio_ok
    print(f"   insert ratio: {n_ins}/{len(res)} = {actual_ratio:.3f} (target 0.10) "
          f"-> ran>=1:{n_ins >= 1} within_tol:{ratio_ok}")

    # cleanup ephemeral insert dataset
    iclient.teardown(drop_data=True)
    qclient.teardown()

    passed = all(all_checks.values())
    print("\n=== DAY-3 SMOKE", "PASS" if passed else "FAIL", "===")
    for k, v in all_checks.items():
        print(f"  {'OK ' if v else 'XX '} {k}: {v}")
    print("\nsummaries:")
    for pat, s in summaries.items():
        print(f"  {pat}: {s}")

    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
