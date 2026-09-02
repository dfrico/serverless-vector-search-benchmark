"""
Day-9 three-way smoke — SINGLE-ADAPTER runner.

Runs exactly one adapter end-to-end through the abstract BenchmarkVectorClient
interface, driven by the day-3 workload generator and persisted by the day-5
JSONL sink. smoke_three_way.py launches this THREE times (once per adapter), each
in its own subprocess with the right credentials env, so the three runs never
share a boto3 default session or a set of AWS credentials:

  blocks         -> MinIO env  (AWS_ENDPOINT_URL_S3, minioadmin)
  s3vectors      -> thesis profile (real AWS)
  lancedb-lambda -> thesis profile (real AWS)

All three run from the one consolidated venv (harness/.venv).

Per adapter this does, in order:
  1. construct the adapter with source_csv_path = the SHARED 1k sample CSV, so all
     three report the SAME dataset_fingerprint.content_sha256 (the cross-adapter
     anchor, finding 13).
  2. setup() (create index/verify function as the adapter's capabilities dictate).
  3. ingest the 1,000 vectors via the adapter's declared ingest mode
     (path for Blocks, iterable for the other two).
  4. run the day-3 generator: burst pattern, burst_size=5, cycles=1,
     max_in_flight=1 -> EXACTLY 5 sequential queries (see the note below on why
     burst, not constant). The day-5 sink writes one JSONL + one manifest.
  5. one direct query() of query_pool[0] -> record its top-10 IDs for the
     cross-adapter neighbour-overlap check (deterministic, independent of the
     generator run).
  6. teardown(drop_data=True): drop the per-run data (index/table/dataset). Durable
     LanceDB infra is NOT touched here (build_and_deploy.py destroy does that).

WHY burst, not "constant" (documented deviation): the brief asked for the constant
pattern with 5 queries. The generator's constant driver is TIME-bounded (it issues
queries for duration_s at max_in_flight=1), so the query COUNT it produces depends
on per-query latency. Across these three backends latency differs by 1-2 orders of
magnitude (Blocks runs a whole Lithops map/reduce job per query, seconds; S3
Vectors / LanceDB are ~100 ms), so no single constant config yields an EQUAL, fixed
count of 5 for all three. The burst driver with burst_size=5, cycles=1 issues
EXACTLY 5 queries per adapter regardless of latency, with a clean generator
shutdown, and at max_in_flight=1 the queries are serial anyway (the concurrency
slot gates arrivals, so there is no rate-pacing difference to observe). This is the
faithful realisation of "5 sequential queries, identical across adapters".
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))   # harness/tests -> repo root
if WORKSPACE not in sys.path:                          # repo root for the `harness` package
    sys.path.insert(0, WORKSPACE)

VECTORS_CSV = os.path.join(WORKSPACE, "spike-localhost", "vectors_deep_100k.csv")
QUERIES_CSV = os.path.join(WORKSPACE, "spike-localhost", "queries_deep_100k.csv")
SAMPLE_CSV = os.path.join(_HERE, "tmp_1k_sample.csv")   # regenerated from VECTORS_CSV on run
RESULTS_DIR = os.path.join(_HERE, "results")

DIMENSIONS = 96
N_SAMPLE = 1000
TOP_K = 10
N_QUERY_POOL = 20          # queries loaded into the pool (only the first 5 are issued)

# S3 Vectors index is eventually consistent (finding 14): a fresh write needs a
# short settle before it is queryable. Blocks / LanceDB commit synchronously.
S3VECTORS_SETTLE_S = 3.0


def write_sample(src, dst, n):
    """Copy the first n `id,<space floats>` rows of src to dst verbatim."""
    with open(src) as fin, open(dst, "w", newline="") as fout:
        w = csv.writer(fout)
        for i, row in enumerate(csv.reader(fin)):
            if i >= n:
                break
            w.writerow(row)
    return dst


def load_sample_vectors(path):
    """[(int_id, [floats])] from an `id,<space floats>` CSV."""
    out = []
    with open(path) as f:
        for row in csv.reader(f):
            if not row:
                continue
            out.append((int(row[0]), [float(x) for x in row[1].split(" ") if x]))
    return out


def load_queries(path, n):
    out = []
    with open(path) as f:
        for i, row in enumerate(csv.reader(f)):
            if i >= n:
                break
            if not row:
                continue
            out.append([float(x) for x in row[0].split(" ") if x])
    return out


def build_client(adapter, ts):
    """Construct + setup + ingest the requested adapter. Returns (client, dataset)."""
    if adapter == "blocks":
        from harness.adapters.blocks import BlocksClient
        dataset = f"smoke9_blocks_{ts}"
        client = BlocksClient(bucket="lithops-data", vectors_csv_path=SAMPLE_CSV)
        # 1,000 vectors is a tiny corpus. Use a SINGLE IVF block searched
        # exhaustively (n_probe == nlist) so Blocks returns the exact top-k over
        # the whole corpus. An earlier num_blocks=2 config returned only one
        # block's vectors and missed the true nearest neighbour (see findings/16):
        # Blocks' multi-block partitioning is tuned for large corpora (day-2 got
        # 100% recall at 100k with num_blocks=16), not a 1k smoke sample.
        client.setup(dataset, dimensions=DIMENSIONS, top_k_default=TOP_K,
                     num_blocks=1, partitions_per_block=16, n_probe=16,
                     create_if_missing=True)
        client.bulk_insert_from_path(SAMPLE_CSV, build_index=True)
        return client, dataset

    if adapter == "s3vectors":
        import boto3
        from harness.adapters.s3vectors import S3VectorsClient
        account = boto3.client("sts", region_name="us-east-1").get_caller_identity()["Account"]
        bucket = f"thesis-s3v-{account}"
        dataset = f"smoke9_s3v_{ts}"
        client = S3VectorsClient(bucket, region="us-east-1", source_csv_path=SAMPLE_CSV)
        client.setup(dataset, dimensions=DIMENSIONS, top_k_default=TOP_K, create_if_missing=True)
        client.bulk_insert(iter(load_sample_vectors(SAMPLE_CSV)), build_index=True)
        time.sleep(S3VECTORS_SETTLE_S)   # let the write become queryable
        return client, dataset

    if adapter == "lancedb-lambda":
        import boto3
        from harness.adapters.lancedb_lambda import LanceDBLambdaClient
        account = boto3.client("sts", region_name="us-east-1").get_caller_identity()["Account"]
        data_bucket = f"thesis-lancedb-{account}"
        image_uri = f"{account}.dkr.ecr.us-east-1.amazonaws.com/thesis-lancedb-lambda:latest"
        dataset = f"smoke9_lance_{ts}"
        client = LanceDBLambdaClient(
            "thesis-lancedb-lambda", region="us-east-1", data_bucket=data_bucket,
            source_csv_path=SAMPLE_CSV, image_uri=image_uri,
        )
        client.setup(dataset, dimensions=DIMENSIONS, top_k_default=TOP_K, create_if_missing=True)
        client.bulk_insert(iter(load_sample_vectors(SAMPLE_CSV)), build_index=True)
        return client, dataset

    raise SystemExit(f"unknown adapter {adapter!r}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("adapter", choices=["blocks", "s3vectors", "lancedb-lambda"])
    ap.add_argument("timestamp", type=int, help="shared run id so all three files share a stem prefix")
    args = ap.parse_args()

    from harness.workload import WorkloadGenerator, WorkloadConfig
    from harness.sink import JSONLSink

    # Regenerate the shared 1k sample + results dir under harness/tests (the old
    # pre-consolidation staged copies did not move with the consolidation).
    os.makedirs(RESULTS_DIR, exist_ok=True)
    write_sample(VECTORS_CSV, SAMPLE_CSV, N_SAMPLE)

    query_pool = load_queries(QUERIES_CSV, N_QUERY_POOL)
    print(f"[{args.adapter}] loaded {len(query_pool)} query vectors")

    client, dataset = build_client(args.adapter, args.timestamp)
    print(f"[{args.adapter}] setup + ingest done -> dataset {dataset}")

    # burst -> EXACTLY 5 sequential queries at max_in_flight=1 (see module docstring).
    config = WorkloadConfig(pattern="burst", burst_size=5, cycles=1, idle_s=0.0,
                            max_in_flight=1, top_k=TOP_K, seed=None)
    gen = WorkloadGenerator(client, query_pool)
    sink = JSONLSink(client, config, dataset_name=dataset, output_dir=RESULTS_DIR,
                     generator=gen, timestamp=args.timestamp)
    # The sink names files by pattern; the day-9 brief wants them named by adapter.
    # Override the two paths (consume() reads these attributes) so each adapter's
    # run lands on a distinct, adapter-named file even though the pattern is shared.
    stem = f"run_{args.timestamp}_{args.adapter}"
    sink.jsonl_path = os.path.join(RESULTS_DIR, f"{stem}.jsonl")
    sink.manifest_path = os.path.join(RESULTS_DIR, f"{stem}.manifest.json")

    paths = asyncio.run(sink.consume(gen.run(config)))
    print(f"[{args.adapter}] sink wrote {paths.n_records} records -> {os.path.basename(paths.jsonl)}")

    # Deterministic neighbour-overlap probe: top-10 IDs of query_pool[0], independent
    # of the generator, so all three adapters answer the SAME query vector.
    hits, q_t = client.query(query_pool[0], top_k=TOP_K)
    top10 = [int(i) for i, _ in hits]
    overlap = {
        "adapter": client.name,
        "dataset": dataset,
        "content_sha256": client.dataset_fingerprint(dataset).get("content_sha256"),
        "query0_top10_ids": top10,
        "query0_hits": [[int(i), float(d)] for i, d in hits],
        "query0_timings_keys": sorted(q_t.keys()),
    }
    with open(os.path.join(RESULTS_DIR, f"overlap_{args.timestamp}_{args.adapter}.json"), "w") as f:
        json.dump(overlap, f, indent=2)
    print(f"[{args.adapter}] query0 top-10 IDs: {top10}")

    client.teardown(drop_data=True)
    print(f"[{args.adapter}] teardown(drop_data=True) done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
