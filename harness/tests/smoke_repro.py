"""
Day-6 smoke test: reproducibility plumbing.

Two parts:
  A. Seed reproducibility (fast, backend-free). A recording fake client captures
     the query vectors the generator samples. Driven through the burst pattern
     (fires a fixed op count), two runs with the SAME seed must produce identical
     query-op sequences; a DIFFERENT seed must produce a different one. Uses a
     fake client because seeded sampling is backend-independent and this lets us
     compare many ops without paying ~5s/query.
  B. Manifest completeness (one real BlocksClient run). A short constant run with
     the JSONLSink attached, then assert every day-6 manifest field is present
     AND populated (real sha256, a known backend commit_sha, env versions, the
     seed value round-tripped) — not None, not "unknown".

Run with the boto3->MinIO env vars exported and the venv bin on PATH (part B
drives real Blocks queries against deep_100k).
"""
import asyncio
import csv
import json
import os
import sys

WORKSPACE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if WORKSPACE not in sys.path:
    sys.path.insert(0, WORKSPACE)

from harness.client import BenchmarkVectorClient, ClientCapabilities   # noqa: E402
from harness.adapters.blocks import BlocksClient                       # noqa: E402
from harness.workload import WorkloadGenerator, WorkloadConfig         # noqa: E402
from harness.sink import JSONLSink                                     # noqa: E402

QUERIES_CSV = os.path.join(WORKSPACE, "spike-localhost", "queries_deep_100k.csv")
VECTORS_CSV = os.path.join(WORKSPACE, "spike-localhost", "vectors_deep_100k.csv")
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
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


class RecordingFakeClient(BenchmarkVectorClient):
    """Instant, backend-free client that records the query vectors it receives.
    Full interface so the generator drives it exactly like a real adapter."""

    def __init__(self):
        self.seen_queries = []

    @property
    def name(self):
        return "fake"

    def backend_version(self):
        return {"repo": "fake", "commit_sha": "n/a", "commit_date": "n/a"}

    def dataset_fingerprint(self, dataset_name):
        return {"content_sha256": None, "line_count": None, "byte_size": None, "source_path": None}

    def capabilities(self):
        # max_safe_concurrency high so nothing is capped; queries only.
        return ClientCapabilities(supports_pending=False, metric="l2",
                                  id_stability="preserved", ingest_modes=set(),
                                  max_safe_concurrency=64)

    def setup(self, *a, **k):
        pass

    def teardown(self, *a, **k):
        pass

    def bulk_insert(self, *a, **k):
        return (None, {"op": "bulk_insert", "wall_clock_s": 0.0})

    def query(self, vector, *, top_k=None):
        self.seen_queries.append(list(vector))
        return ([(0, 0.0)], {"op": "query", "wall_clock_s": 0.0})

    def query_batch(self, vectors, *, top_k=None):
        return ([[(0, 0.0)] for _ in vectors], {"op": "query_batch", "wall_clock_s": 0.0})

    def delete(self, *a, **k):
        return {"op": "delete", "wall_clock_s": 0.0}


async def drain(gen, config):
    out = []
    async for r in gen.run(config):
        out.append(r)
    return out


def seeded_query_sequence(query_pool, seed):
    """Run a fixed-size burst with `seed` and return the recorded query vectors."""
    fake = RecordingFakeClient()
    gen = WorkloadGenerator(fake, query_pool)
    cfg = WorkloadConfig(pattern="burst", burst_size=8, idle_s=0.0, cycles=2,
                         max_in_flight=4, top_k=10, seed=seed)
    asyncio.run(drain(gen, cfg))
    return fake.seen_queries


def main():
    query_pool = load_query_pool(QUERIES_CSV, 200)
    print(f"loaded {len(query_pool)} query vectors")
    checks = {}

    # ---- Part A: seed reproducibility (fake client, fast) ---------------
    print("\n=== Part A: seed reproducibility ===")
    seq_a1 = seeded_query_sequence(query_pool, seed=1234)
    seq_a2 = seeded_query_sequence(query_pool, seed=1234)
    seq_b = seeded_query_sequence(query_pool, seed=9999)
    print(f"  seed=1234 run1: {len(seq_a1)} queries, run2: {len(seq_a2)}; seed=9999: {len(seq_b)}")
    checks["same_seed_identical"] = seq_a1 == seq_a2 and len(seq_a1) > 0
    checks["diff_seed_differs"] = seq_a1 != seq_b
    # sanity: the sequence is actually sampled (not all identical), i.e. RNG is live
    checks["sequence_nontrivial"] = len({tuple(v) for v in seq_a1}) > 1

    # ---- Part B: manifest completeness (real BlocksClient) --------------
    print("\n=== Part B: manifest completeness (real Blocks run) ===")
    qclient = BlocksClient(bucket=BUCKET, vectors_csv_path=VECTORS_CSV)
    qclient.setup(DATASET, dimensions=96, top_k_default=10, create_if_missing=False)
    gen = WorkloadGenerator(qclient, query_pool)
    cfg = WorkloadConfig(pattern="constant", ops_per_second=5, duration_s=1,
                         max_in_flight=2, top_k=10, seed=7)
    sink = JSONLSink(qclient, cfg, dataset_name=DATASET, output_dir=OUT_DIR, generator=gen)
    paths = asyncio.run(sink.consume(gen.run(cfg)))
    qclient.teardown()
    print(f"  wrote {paths.n_records} records + manifest {os.path.basename(paths.manifest)}")

    with open(paths.manifest) as f:
        m = json.load(f)

    checks["manifest_schema_v2"] = m.get("schema") == "harness.run_manifest.v2"
    # dataset fingerprint: real hash + shape
    df = m.get("dataset_fingerprint", {})
    checks["dataset_fingerprint_hash"] = bool(df.get("content_sha256")) and df.get("line_count") == 100000
    # backend version: a real commit sha (this env has the pinned Blocks clone)
    bv = m.get("backend_version", {})
    checks["backend_version_sha"] = bv.get("commit_sha") not in (None, "", "unknown") and bv.get("repo") == "blocks-db-wip"
    # seed round-trips
    checks["seed_recorded"] = m.get("seed") == 7
    # env fingerprint: the behaviour-defining packages present (not absent)
    env = m.get("env", {})
    checks["env_populated"] = all(
        env.get(k) not in (None, "", "absent") for k in ("lithops", "faiss_cpu", "numpy", "boto3", "orjson")
    )
    # effective concurrency from the generator (capped 2 -> 1 on Blocks)
    checks["eff_concurrency"] = m.get("effective_concurrency") == 1

    passed = all(checks.values())
    print("\n=== DAY-6 SMOKE", "PASS" if passed else "FAIL", "===")
    for k, v in checks.items():
        print(f"  {'OK ' if v else 'XX '} {k}: {v}")

    print("\n----- sample manifest (day-6 fields) -----")
    for key in ("schema", "backend_version", "dataset_fingerprint", "seed", "env",
                "effective_concurrency"):
        print(f"  {key}: {json.dumps(m.get(key))}")

    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
