"""
Day-7 smoke test for S3VectorsClient (harness/adapters/s3vectors.py).

The S3 Vectors equivalent of the day-2 Blocks smoke, run at a DELIBERATELY tiny
scale (1,000 vectors, 100 queries) to keep real-AWS spend near zero
(~$0.0005 estimated — see the cost banner printed before the first AWS call and
findings/14_s3vectors_adapter.md).

Flow:
  0. Sample 1,000 rows from spike-localhost/vectors_deep_100k.csv -> tmp_1k_sample.csv.
  1. Print the estimated cost banner (no AWS call yet).
  2. Create a throwaway bucket + index (smoke_s3v_<ts>).
  3. bulk_insert the 1,000 vectors (build_index=True).
  4. query_batch the first 100 queries from queries_deep_100k.csv.
  5. Assert: 100 non-empty results, valid shapes, distances ascending, IDs in
     [0, 1000), timings carry {op, wall_clock_s}; capabilities()/backend_version()
     shapes; dataset_fingerprint() matches the fingerprint of tmp_1k_sample.csv.
  6. teardown(drop_data=True); verify the index (vector store) is gone.

Prerequisites (real AWS):
  - Valid credentials for a region where S3 Vectors is available (default
    us-east-1). Export AWS_PROFILE / run `aws sso login` first.
  - No AWS_ENDPOINT_URL / AWS_ENDPOINT_URL_S3VECTORS set (those are for MinIO).

Run:  harness/.venv/bin/python harness/tests/smoke_s3vectors.py
"""
import csv
import json
import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))
if WORKSPACE not in sys.path:
    sys.path.insert(0, WORKSPACE)

from harness.adapters.s3vectors import S3VectorsClient  # noqa: E402
from harness.dataset_hash import fingerprint_csv        # noqa: E402
from botocore.exceptions import ClientError             # noqa: E402

VECTORS_CSV = os.path.join(WORKSPACE, "spike-localhost", "vectors_deep_100k.csv")
QUERIES_CSV = os.path.join(WORKSPACE, "spike-localhost", "queries_deep_100k.csv")
SAMPLE_CSV = os.path.join(_HERE, "tmp_1k_sample.csv")
RESULT_JSON = os.path.join(_HERE, "smoke_result.json")

REGION = "us-east-1"
DIMENSIONS = 96
N_SAMPLE = 1000
N_QUERIES = 100
TOP_K = 10


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
            vid = int(row[0])
            vec = [float(x) for x in row[1].split(" ") if x]
            out.append((vid, vec))
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


def cost_banner():
    """Estimate and print the smoke run's AWS spend BEFORE the first AWS call.
    Pricing (us-east-1, from https://aws.amazon.com/s3/pricing/, S3 Vectors):
      PUT $0.20/GB, min 128KB/request; query request $2.50/million;
      query data processed $0.004/TB (first 100k vectors); returned <512KB free.
    """
    put_requests = (N_SAMPLE + 99) // 100          # 100/batch
    put_cost = put_requests * (128 * 1024) / 1e9 * 0.20   # min-charge dominates
    query_req_cost = N_QUERIES / 1_000_000 * 2.50
    total = put_cost + query_req_cost
    print("=" * 66)
    print("S3 VECTORS SMOKE — ESTIMATED AWS SPEND (before any AWS call)")
    print(f"  region                : {REGION}")
    print(f"  PUT: {put_requests} requests x 128KB min  -> ${put_cost:.6f}")
    print(f"  QUERY: {N_QUERIES} requests x $2.50/M       -> ${query_req_cost:.6f}")
    print(f"  query data processed/returned            -> ~$0 (tiny/free tier)")
    print(f"  storage (minutes of 0.0004 GB)           -> ~$0")
    print(f"  ESTIMATED TOTAL                          -> ${total:.6f}")
    print("=" * 66)
    return total


def main():
    checks = {}

    # ---- 0. sample + fingerprint expectation (no AWS) -------------------
    write_sample(VECTORS_CSV, SAMPLE_CSV, N_SAMPLE)
    sample_vecs = load_sample_vectors(SAMPLE_CSV)
    queries = load_queries(QUERIES_CSV, N_QUERIES)
    expected_fp = fingerprint_csv(SAMPLE_CSV)
    print(f"sampled {len(sample_vecs)} vectors -> {SAMPLE_CSV}")
    print(f"loaded {len(queries)} queries; sample sha256={expected_fp['content_sha256'][:16]}...")

    # ---- 1. capabilities + backend_version (no AWS) ---------------------
    ts = int(time.time())
    bucket = f"smoke-s3v-{ts}"
    dataset = f"smoke_s3v_{ts}"
    client = S3VectorsClient(bucket, region=REGION, source_csv_path=SAMPLE_CSV)

    caps = client.capabilities()
    checks["capabilities"] = (
        caps.supports_pending is False
        and caps.metric == "l2"
        and caps.id_stability == "preserved"
        and caps.ingest_modes == {"iterable"}
        and caps.max_safe_concurrency == 4
    )
    print(f"capabilities: pending={caps.supports_pending} metric={caps.metric!r} "
          f"ids={caps.id_stability!r} modes={caps.ingest_modes} "
          f"max_conc={caps.max_safe_concurrency} -> {checks['capabilities']}")

    bv = client.backend_version()
    checks["backend_version"] = (
        bv.get("service") == "s3vectors"
        and bool(bv.get("boto3")) and bv.get("boto3") != "unknown"
        and bool(bv.get("botocore")) and bv.get("botocore") != "unknown"
        and bool(bv.get("region"))
        and bool(bv.get("api_version"))
    )
    print(f"backend_version: {bv} -> {checks['backend_version']}")

    # dataset_fingerprint matches the sample file's fingerprint
    fp = client.dataset_fingerprint(dataset)
    checks["dataset_fingerprint"] = (
        fp.get("content_sha256") == expected_fp["content_sha256"]
        and fp.get("line_count") == N_SAMPLE
        and fp.get("byte_size") == expected_fp["byte_size"]
    )
    print(f"dataset_fingerprint: sha256 matches sample={checks['dataset_fingerprint']}")

    # ---- cost banner + gate ---------------------------------------------
    est = cost_banner()
    if est > 1.0:
        print(f"ABORT: estimated spend ${est:.4f} exceeds $1 gate.")
        return 2

    # ---- 2..6. real AWS run ---------------------------------------------
    try:
        client.setup(dataset, dimensions=DIMENSIONS, top_k_default=TOP_K, create_if_missing=True)
        print(f"setup OK — bucket={bucket} index={dataset} arn={client.index_arn}")

        _, ins_t = client.bulk_insert(iter(sample_vecs), build_index=True)
        checks["bulk_insert_timings"] = (
            ins_t.get("op") == "bulk_insert"
            and isinstance(ins_t.get("wall_clock_s"), float)
            and ins_t.get("n_vectors") == N_SAMPLE
        )
        print(f"bulk_insert: {ins_t}")

        # S3 Vectors is eventually consistent on fresh writes; give it a moment.
        time.sleep(2.0)

        results, q_t = client.query_batch(queries, top_k=TOP_K)
        print(f"query_batch: {len(results)} results in {q_t.get('wall_clock_s'):.2f}s")

        checks["result_count"] = len(results) == N_QUERIES
        checks["all_non_empty"] = all(len(r) > 0 for r in results)
        checks["shape_valid"] = all(
            isinstance(r, list) and len(r) <= TOP_K
            and all(isinstance(h, tuple) and len(h) == 2 for h in r)
            for r in results
        )
        checks["ids_in_range"] = all(
            0 <= vid < N_SAMPLE for r in results for vid, _ in r
        )
        checks["distances_ascending"] = all(
            all(r[i][1] <= r[i + 1][1] for i in range(len(r) - 1)) for r in results
        )
        checks["query_timings"] = (
            q_t.get("op") == "query_batch" and isinstance(q_t.get("wall_clock_s"), float)
        )
        # single query() envelope too
        _, sq_t = client.query(queries[0], top_k=TOP_K)
        checks["single_query_timings"] = (
            sq_t.get("op") == "query" and isinstance(sq_t.get("wall_clock_s"), float)
        )
        print(f"  non_empty={sum(1 for r in results if r)}/{len(results)}  "
              f"ids_in_range={checks['ids_in_range']}  ascending={checks['distances_ascending']}")

    finally:
        # Always attempt cleanup so a mid-run failure does not leave billable data.
        client.teardown(drop_data=True)
        print("teardown(drop_data=True) done")

    # ---- verify the vector store is gone --------------------------------
    # Use the sanitized index name the adapter actually created (S3 Vectors index
    # names disallow the underscores in the logical dataset name).
    idx_name = client.index_name
    verify = __import__("boto3").client("s3vectors", region_name=REGION)
    try:
        verify.get_index(vectorBucketName=client.vector_bucket_name, indexName=idx_name)
        checks["index_deleted"] = False
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        checks["index_deleted"] = code in ("NotFoundException", "ResourceNotFoundException")
    print(f"index_deleted after teardown -> {checks['index_deleted']}")

    passed = all(checks.values())
    print("\n=== DAY-7 SMOKE", "PASS" if passed else "FAIL", "===")
    for k, v in checks.items():
        print(f"  {'OK ' if v else 'XX '} {k}: {v}")

    out = {"passed": passed, "checks": checks,
           "n_vectors": N_SAMPLE, "n_queries": N_QUERIES,
           "estimated_cost_usd": est}
    with open(RESULT_JSON, "w") as f:
        json.dump(out, f, indent=2)

    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
