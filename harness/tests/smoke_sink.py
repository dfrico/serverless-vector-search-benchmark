"""
Day-5 smoke test: end-to-end sink + aggregator + timings parser.

Drives the day-3 workload generator + BlocksClient against the deep_100k index
through the constant pattern (5s, queries only, max_in_flight=2 -> capped to 1
by capabilities().max_safe_concurrency), attaches the JSONLSink, then runs the
RunAggregator over the produced file.

Asserts (see day-5 brief):
  - sink wrote both .jsonl and .manifest.json
  - every JSONL line parses and has all OpResult fields
  - manifest carries git SHA, timestamp, config, capabilities
  - aggregator produces per-op counts + latency p50/p95/p99 + realised throughput
  - Blocks timings parser produced a normalized phase dict for the query op
  - configured-vs-realised gap reported and realised < nominal (closed-loop)

Run with the boto3->MinIO env vars exported (see harness/README.md), and
with the venv bin on PATH (Lithops localhost shells out to bare `python`).
"""
import asyncio
import csv
import dataclasses
import json
import os
import sys

WORKSPACE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if WORKSPACE not in sys.path:
    sys.path.insert(0, WORKSPACE)

from harness.adapters.blocks import BlocksClient                    # noqa: E402
from harness.workload import WorkloadGenerator, WorkloadConfig, OpResult  # noqa: E402
from harness.sink import JSONLSink                                  # noqa: E402
from harness.aggregate import RunAggregator                         # noqa: E402

QUERIES_CSV = os.path.join(WORKSPACE, "spike-localhost", "queries_deep_100k.csv")
VECTORS_CSV = os.path.join(WORKSPACE, "spike-localhost", "vectors_deep_100k.csv")
BUCKET = "lithops-data"
DATASET = "deep_100k"
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

OP_RESULT_FIELDS = {f.name for f in dataclasses.fields(OpResult)}


def load_query_pool(path, n):
    out = []
    with open(path) as f:
        for i, row in enumerate(csv.reader(f)):
            if i >= n or not row:
                break
            out.append([float(x) for x in row[0].split(" ") if x])
    return out


async def run_with_sink(gen, config, sink):
    """Attach the sink directly to the generator's async iterable."""
    return await sink.consume(gen.run(config))


def main():
    query_pool = load_query_pool(QUERIES_CSV, 200)
    print(f"loaded {len(query_pool)} query vectors")

    # vectors_csv_path set so the manifest's dataset_fingerprint hashes the real
    # corpus (day 6) rather than recording a null hash.
    qclient = BlocksClient(bucket=BUCKET, vectors_csv_path=VECTORS_CSV)
    qclient.setup(DATASET, dimensions=96, top_k_default=10, create_if_missing=False)

    gen = WorkloadGenerator(qclient, query_pool)
    # max_in_flight=2 -> capped to 1 by Blocks max_safe_concurrency (see finding 11).
    cfg = WorkloadConfig(pattern="constant", ops_per_second=5, duration_s=5,
                         max_in_flight=2, top_k=10)
    # Sink reads effective concurrency straight from the generator (day 6, change 5).
    eff = gen.effective_concurrency(cfg)

    sink = JSONLSink(qclient, cfg, dataset_name=DATASET, output_dir=OUT_DIR, generator=gen)
    print(f"\n=== running constant (5 ops/s, 5s, queries, max_in_flight=2 -> eff={eff}) ===")
    paths = asyncio.run(run_with_sink(gen, cfg, sink))
    qclient.teardown()
    print(f"sink wrote {paths.n_records} records -> {os.path.basename(paths.jsonl)}")

    checks = {}

    # (a) both files exist
    checks["files_written"] = os.path.exists(paths.jsonl) and os.path.exists(paths.manifest)

    # (b) every JSONL line parses + has all OpResult fields
    lines = []
    with open(paths.jsonl) as f:
        for line in f:
            line = line.strip()
            if line:
                lines.append(json.loads(line))
    checks["jsonl_nonempty"] = len(lines) >= 1
    checks["jsonl_all_fields"] = all(set(d.keys()) == OP_RESULT_FIELDS for d in lines)
    checks["jsonl_adapter_stamped"] = all(d.get("adapter") == "blocks" for d in lines)

    # (c) manifest has the reproducibility keys
    with open(paths.manifest) as f:
        manifest = json.load(f)
    checks["manifest_git_sha"] = bool(manifest.get("git_sha"))
    checks["manifest_timestamp"] = "timestamp" in manifest
    checks["manifest_config"] = manifest.get("config", {}).get("pattern") == "constant"
    checks["manifest_capabilities"] = (
        manifest.get("capabilities", {}).get("max_safe_concurrency") == 1
        and manifest.get("capabilities", {}).get("metric") == "l2"
    )
    checks["manifest_eff_concurrency"] = manifest.get("effective_concurrency") == 1
    # day-6 reproducibility fields now present in the manifest
    checks["manifest_dataset_fingerprint"] = bool(
        manifest.get("dataset_fingerprint", {}).get("content_sha256")
    )
    checks["manifest_backend_version"] = (
        manifest.get("backend_version", {}).get("commit_sha") not in (None, "", "unknown")
    )
    checks["manifest_env"] = bool(manifest.get("env", {}).get("lithops"))
    checks["manifest_seed_key"] = "seed" in manifest  # None here (no seed), but the key must exist

    # (d) aggregator
    summary = RunAggregator(paths.jsonl, paths.manifest).aggregate()
    q_stats = next((o for o in summary.per_op if o.op == "query"), None)
    checks["agg_has_query_op"] = q_stats is not None
    checks["agg_counts"] = q_stats is not None and q_stats.ok >= 1
    checks["agg_percentiles"] = (
        q_stats is not None and q_stats.latency is not None
        and all(v is not None for v in (q_stats.latency.p50, q_stats.latency.p95, q_stats.latency.p99))
    )
    checks["agg_throughput"] = summary.realised_throughput > 0

    # (e) Blocks timings parser produced a normalized phase dict for query
    checks["parser_phase_breakdown"] = (
        q_stats is not None and q_stats.phases_parsed
        and q_stats.phase_breakdown is not None
        and "total_querying_s" in q_stats.phase_breakdown
        and "worker_search_s" in q_stats.phase_breakdown
    )

    # (f) configured-vs-realised gap: realised < nominal on Blocks-localhost
    checks["gap_reported"] = summary.nominal_ops_per_second is not None
    checks["realised_below_nominal"] = (
        summary.nominal_ops_per_second is not None
        and summary.realised_throughput < summary.nominal_ops_per_second
    )

    passed = all(checks.values())
    print("\n=== DAY-5 SMOKE", "PASS" if passed else "FAIL", "===")
    for k, v in checks.items():
        print(f"  {'OK ' if v else 'XX '} {k}: {v}")

    print("\n----- aggregator text summary -----")
    print(summary.format())

    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
