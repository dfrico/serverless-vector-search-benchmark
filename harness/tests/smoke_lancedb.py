"""
Day-8 smoke test for LanceDBLambdaClient (harness/adapters/lancedb_lambda.py).

The LanceDB-on-Lambda equivalent of the day-2/day-7 smokes, at a DELIBERATELY tiny
scale (1,000 vectors, 100 queries) to keep real-AWS spend near zero (see the cost
banner printed before the first AWS call and findings/15_lancedb_lambda_adapter.md).

Prerequisites (real AWS):
  - build_and_deploy.py deploy has run: the Lambda `thesis-lancedb-lambda`, its
    ECR image, IAM role, and the S3 data bucket all exist.
  - Valid credentials (AWS_PROFILE=thesis). No AWS_ENDPOINT_URL* set (MinIO guard).

Flow:
  0. Sample 1,000 rows from spike-localhost/vectors_deep_100k.csv -> tmp_1k_sample.csv.
  1. capabilities()/backend_version()/dataset_fingerprint() (fingerprint needs no AWS;
     backend_version's lancedb version is learned from a ping inside setup()).
  2. Print the estimated cost banner; abort if > $1.
  3. setup() against a throwaway table smoke_lance_<ts> (create-on-first-insert).
  4. bulk_insert the 1,000 vectors; query_batch the first 100 + one single query().
  5. Assert: 100 non-empty results, valid shapes, distances ascending, IDs in
     [0, 1000), timings carry {op, wall_clock_s} (+ cold/server_ms).
  6. Empirically decide whether LanceDB's L2 _distance is SQUARED or TRUE, by
     recomputing the distance to query[0]'s own top hit (for findings/15).
  7. teardown(drop_data=True); verify the table is gone (a query now fails).

Run:  AWS_PROFILE=thesis AWS_REGION=us-east-1 harness/.venv/bin/python harness/tests/smoke_lancedb.py
"""
import csv
import json
import math
import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))
if WORKSPACE not in sys.path:
    sys.path.insert(0, WORKSPACE)

import boto3                                              # noqa: E402
from harness.adapters.lancedb_lambda import LanceDBLambdaClient  # noqa: E402
from harness.dataset_hash import fingerprint_csv          # noqa: E402

VECTORS_CSV = os.path.join(WORKSPACE, "spike-localhost", "vectors_deep_100k.csv")
QUERIES_CSV = os.path.join(WORKSPACE, "spike-localhost", "queries_deep_100k.csv")
SAMPLE_CSV = os.path.join(_HERE, "tmp_1k_sample.csv")
RESULT_JSON = os.path.join(_HERE, "smoke_result.json")

REGION = os.environ.get("AWS_REGION", "us-east-1")
FUNCTION_NAME = "thesis-lancedb-lambda"
ECR_REPO = "thesis-lancedb-lambda"
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


def cost_banner(n_invokes_est):
    """
    Estimate + print the smoke's AWS spend BEFORE the first AWS call.
    Lambda arm64 (us-east-1): $0.0000133334 per GB-second + $0.20 / 1M requests.
    S3 requests are pennies-per-million; ECR image storage ~$0.05/GB-month (the
    image lives for the session only). All dominated by Lambda GB-seconds.
    """
    mem_gb = 2.0
    avg_s = 5.0                      # generous per-invoke wall time incl. cold start
    gb_seconds = mem_gb * avg_s * n_invokes_est
    compute = gb_seconds * 0.0000133334
    requests = n_invokes_est / 1_000_000 * 0.20
    total = compute + requests
    print("=" * 66)
    print("LANCEDB-ON-LAMBDA SMOKE — ESTIMATED AWS SPEND (before any AWS call)")
    print(f"  region                : {REGION}")
    print(f"  Lambda: ~{n_invokes_est} invokes x {mem_gb}GB x ~{avg_s}s "
          f"= {gb_seconds:.0f} GB-s -> ${compute:.6f}")
    print(f"  Lambda requests: {n_invokes_est} x $0.20/M            -> ${requests:.6f}")
    print(f"  S3 GET/PUT + ECR storage (session)         -> ~$0 (pennies)")
    print(f"  ESTIMATED TOTAL                            -> ${total:.6f}")
    print("=" * 66)
    return total


def classify_l2(returned_dist, query_vec, hit_vec):
    """Is LanceDB's reported _distance the SQUARED or the TRUE L2? Decide by
    recomputing both from the raw vectors and matching (relative tolerance)."""
    sq = sum((a - b) ** 2 for a, b in zip(query_vec, hit_vec))
    true = math.sqrt(sq)

    def close(a, b):
        return abs(a - b) <= 1e-3 * max(1.0, abs(b))

    if close(returned_dist, sq):
        return "squared", sq, true
    if close(returned_dist, true):
        return "true", sq, true
    return "unknown", sq, true


def main():
    checks = {}
    account = boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]
    data_bucket = f"thesis-lancedb-{account}"
    image_uri = f"{account}.dkr.ecr.{REGION}.amazonaws.com/{ECR_REPO}:latest"

    # ---- 0. sample + fingerprint expectation (no AWS) -------------------
    write_sample(VECTORS_CSV, SAMPLE_CSV, N_SAMPLE)
    sample_vecs = load_sample_vectors(SAMPLE_CSV)
    sample_by_id = {vid: vec for vid, vec in sample_vecs}
    queries = load_queries(QUERIES_CSV, N_QUERIES)
    expected_fp = fingerprint_csv(SAMPLE_CSV)
    print(f"sampled {len(sample_vecs)} vectors -> {SAMPLE_CSV}")
    print(f"loaded {len(queries)} queries; sample sha256={expected_fp['content_sha256'][:16]}...")

    ts = int(time.time())
    table = f"smoke_lance_{ts}"
    client = LanceDBLambdaClient(
        FUNCTION_NAME, region=REGION, data_bucket=data_bucket,
        source_csv_path=SAMPLE_CSV, image_uri=image_uri,
    )

    # ---- 1. capabilities (no AWS) + fingerprint -------------------------
    caps = client.capabilities()
    checks["capabilities"] = (
        caps.supports_pending is False
        and caps.metric == "l2"
        and caps.id_stability == "preserved"
        and caps.ingest_modes == {"iterable"}
        and caps.max_safe_concurrency == 10
    )
    print(f"capabilities: pending={caps.supports_pending} metric={caps.metric!r} "
          f"ids={caps.id_stability!r} modes={caps.ingest_modes} "
          f"max_conc={caps.max_safe_concurrency} -> {checks['capabilities']}")

    fp = client.dataset_fingerprint(table)
    checks["dataset_fingerprint"] = (
        fp.get("content_sha256") == expected_fp["content_sha256"]
        and fp.get("line_count") == N_SAMPLE
        and fp.get("byte_size") == expected_fp["byte_size"]
    )
    print(f"dataset_fingerprint: sha256 matches sample={checks['dataset_fingerprint']}")

    # ---- 2. cost banner + gate ------------------------------------------
    n_invokes_est = ((N_SAMPLE + 499) // 500) + 1 + 1 + 1 + 1  # insert + qbatch + query + drop + ping
    est = cost_banner(n_invokes_est)
    if est > 1.0:
        print(f"ABORT: estimated spend ${est:.4f} exceeds $1 gate.")
        return 2

    # ---- 3..7. real AWS run ---------------------------------------------
    l2_form = None
    try:
        client.setup(table, dimensions=DIMENSIONS, top_k_default=TOP_K, create_if_missing=True)
        print(f"setup OK — fn={FUNCTION_NAME} table={table} arn={client.function_arn}")

        bv = client.backend_version()
        checks["backend_version"] = (
            bv.get("service") == "lambda"
            and bool(bv.get("region"))
            and bool(bv.get("lancedb")) and bv.get("lancedb") != "unknown"
            and bool(bv.get("runtime"))
            and bool(bv.get("image_uri"))
        )
        print(f"backend_version: {bv} -> {checks['backend_version']}")

        _, ins_t = client.bulk_insert(iter(sample_vecs), build_index=True)
        checks["bulk_insert_timings"] = (
            ins_t.get("op") == "bulk_insert"
            and isinstance(ins_t.get("wall_clock_s"), float)
            and ins_t.get("n_vectors") == N_SAMPLE
        )
        print(f"bulk_insert: {ins_t}")

        results, q_t = client.query_batch(queries, top_k=TOP_K)
        print(f"query_batch: {len(results)} results in {q_t.get('wall_clock_s'):.2f}s "
              f"(cold={q_t.get('cold')} server_ms={q_t.get('server_ms'):.1f})")

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
            and "server_ms" in q_t and "cold" in q_t
        )
        _, sq_t = client.query(queries[0], top_k=TOP_K)
        checks["single_query_timings"] = (
            sq_t.get("op") == "query" and isinstance(sq_t.get("wall_clock_s"), float)
        )
        print(f"  non_empty={sum(1 for r in results if r)}/{len(results)}  "
              f"ids_in_range={checks['ids_in_range']}  ascending={checks['distances_ascending']}")

        # ---- 6. empirically classify the L2 distance form ---------------
        top_id, top_dist = results[0][0]
        if top_id in sample_by_id:
            l2_form, sq_val, true_val = classify_l2(top_dist, queries[0], sample_by_id[top_id])
            checks["l2_form_known"] = l2_form in ("squared", "true")
            print(f"  L2 form: reported={top_dist:.4f} squared={sq_val:.4f} "
                  f"true={true_val:.4f} -> {l2_form}")
        else:
            checks["l2_form_known"] = False
            print(f"  L2 form: top hit id {top_id} not in sample; cannot classify")

    finally:
        client.teardown(drop_data=True)
        print("teardown(drop_data=True) done")

    # ---- 7. verify the table is gone ------------------------------------
    verify = boto3.client("lambda", region_name=REGION)
    ev = {"op": "query", "db_uri": f"s3://{data_bucket}/lancedb", "table": table,
          "queries": [queries[0]], "top_k": TOP_K}
    resp = verify.invoke(FunctionName=FUNCTION_NAME, InvocationType="RequestResponse",
                         Payload=json.dumps(ev).encode())
    out = json.loads(resp["Payload"].read())
    # A dropped table -> open_table raises inside the handler -> {"ok": false}.
    checks["table_dropped"] = (out.get("ok") is False)
    print(f"table_dropped after teardown -> {checks['table_dropped']} ({out.get('error', out)[:80] if isinstance(out.get('error'), str) else out.get('ok')})")

    passed = all(checks.values())
    print("\n=== DAY-8 SMOKE", "PASS" if passed else "FAIL", "===")
    for k, v in checks.items():
        print(f"  {'OK ' if v else 'XX '} {k}: {v}")

    out_doc = {"passed": passed, "checks": checks,
               "n_vectors": N_SAMPLE, "n_queries": N_QUERIES,
               "l2_form": l2_form, "estimated_cost_usd": est}
    with open(RESULT_JSON, "w") as f:
        json.dump(out_doc, f, indent=2)

    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
