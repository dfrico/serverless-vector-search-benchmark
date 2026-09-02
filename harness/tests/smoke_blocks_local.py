"""
Smoke test for BlocksClient (harness/adapters/blocks.py).

Runs the bundled 100k queries through BlocksClient.query_batch on Lithops
localhost + MinIO and asserts:
  (a) result shapes match day-1's run_blocks_query.py output,
  (b) recall@10 against the bundled ground truth (first 1000 queries) is 100%,
  (c) the timings dict is non-empty.

Assumes the day-1 index already exists in MinIO (indexes/deep_100k/blocks/).
Run with the boto3->MinIO env vars exported (see harness/README.md):
  AWS_ENDPOINT_URL_S3, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
"""
import csv
import json
import os
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from harness.adapters.blocks import BlocksClient
# importing the Blocks adapter puts blocks-db-wip on sys.path, so this resolves.
from vectordb.utils.index_ops import list_indexes

WORKSPACE = _ROOT
_HERE = os.path.dirname(os.path.abspath(__file__))
QUERIES_CSV = os.path.join(WORKSPACE, "spike-localhost", "queries_deep_100k.csv")
GROUND_TRUTH_CSV = os.path.join(WORKSPACE, "spike-localhost", "true_neighbours_deep_100k.csv")
# Reference first-query result, moved into the package with the consolidation.
DAY1_RESULT = os.path.join(_HERE, "query_result.json")
RESULTS_DIR = os.path.join(_HERE, "results")

BUCKET = "lithops-data"
DATASET = "deep_100k"
VECTORS_CSV = os.path.join(WORKSPACE, "spike-localhost", "vectors_deep_100k.csv")


def load_queries(path):
    out = []
    with open(path) as f:
        for row in csv.reader(f):
            if not row:
                continue
            out.append([float(x) for x in row[0].split(" ") if x])
    return out


def load_ground_truth(path):
    out = []
    with open(path) as f:
        for row in csv.reader(f):
            out.append([int(x) for x in row if x != ""])
    return out


def recall_at_k(results, ground_truth):
    hit = tot = 0
    for r, g in zip(results, ground_truth):
        got = {vid for vid, _ in r}
        gset = set(g)
        hit += len(got & gset)
        tot += len(gset)
    return 100.0 * hit / tot if tot else 0.0


def main():
    queries = load_queries(QUERIES_CSV)
    ground_truth = load_ground_truth(GROUND_TRUTH_CSV)
    print(f"loaded {len(queries)} queries, {len(ground_truth)} ground-truth rows")

    client = BlocksClient(bucket=BUCKET, vectors_csv_path=VECTORS_CSV)

    checks = {}

    # (d) capabilities — local Blocks config: no pending, l2, ids preserved, path ingest.
    caps = client.capabilities()
    caps_ok = (
        caps.supports_pending is False
        and caps.metric == "l2"
        and caps.id_stability == "preserved"
        and caps.ingest_modes == {"path"}
    )
    checks["capabilities"] = caps_ok
    print(f"(d) capabilities: supports_pending={caps.supports_pending} metric={caps.metric!r} "
          f"id_stability={caps.id_stability!r} ingest_modes={caps.ingest_modes} -> {caps_ok}")

    # Reuse the existing day-1 index — do not rebuild.
    client.setup(DATASET, dimensions=96, top_k_default=10, create_if_missing=False)
    print(f"setup OK — num_blocks={client.num_blocks} nlist={client.partitions_per_block} "
          f"n_probe={client.n_probe} blocks_per_task={client.blocks_per_task}")

    results, timings = client.query_batch(queries, top_k=10)
    print(f"query_batch returned {len(results)} results")

    # (a) shape
    shape_ok = (
        len(results) == len(queries)
        and all(isinstance(r, list) for r in results)
        and all(len(r) <= 10 for r in results)
        and all(
            (isinstance(hit, (tuple, list)) and len(hit) == 2)
            for r in results for hit in r
        )
    )
    checks["shape_matches"] = shape_ok
    non_empty = sum(1 for r in results if r)
    checks["all_non_empty"] = non_empty == len(results)
    print(f"(a) shape_ok={shape_ok}, non_empty={non_empty}/{len(results)}")

    # cross-check first query against day-1 output if available
    if os.path.exists(DAY1_RESULT):
        with open(DAY1_RESULT) as f:
            day1 = json.load(f)
        day1_ids = [int(i) for i, _ in day1["first_top10"]]
        adapter_ids = [int(i) for i, _ in results[0]]
        checks["matches_day1_first_query"] = day1_ids == adapter_ids
        print(f"    day1 first-query IDs == adapter first-query IDs: {day1_ids == adapter_ids}")

    # (b) recall@10 over first 1000 (ground truth coverage)
    recall = recall_at_k(results[:len(ground_truth)], ground_truth)
    checks["recall_100"] = abs(recall - 100.0) < 1e-9
    print(f"(b) recall@10 = {recall:.2f}%")

    # (c) timings non-empty
    checks["timings_non_empty"] = bool(timings)
    print(f"(c) timings non-empty={bool(timings)}, keys={list(timings.keys())}")

    client.teardown()

    # ------------------------------------------------------------------
    # Mutation phase — full interface contract on a THROWAWAY dataset, so
    # reruns are isolated and the shared day-1 `deep_100k` index is never
    # touched. Built from the same bundled corpus, so query answers match.
    # ------------------------------------------------------------------
    throwaway = f"smoke_throwaway_{int(time.time())}"
    print(f"\n--- mutation phase on throwaway dataset '{throwaway}' ---")
    mclient = BlocksClient(bucket=BUCKET, vectors_csv_path=VECTORS_CSV)
    mclient.setup(throwaway, dimensions=96, top_k_default=10, create_if_missing=True)

    # (e) bulk_insert_from_path returns (None, non-empty timings)
    ret = mclient.bulk_insert_from_path(VECTORS_CSV, build_index=True)
    bi_ok = (
        isinstance(ret, tuple) and len(ret) == 2
        and ret[0] is None and isinstance(ret[1], dict) and bool(ret[1])
    )
    checks["bulk_insert_from_path"] = bi_ok
    print(f"(e) bulk_insert_from_path -> ret[0]={ret[0]!r}, timings_non_empty={bool(ret[1])}, "
          f"keys={sorted(ret[1].keys())} -> {bi_ok}")

    # (f) index now exists for the throwaway dataset (via underlying VectorDBClient)
    idx_after_build = mclient._client.list_indexes(throwaway)
    checks["index_exists_after_build"] = len(idx_after_build) > 0
    print(f"(f) list_indexes('{throwaway}') = {idx_after_build} -> {len(idx_after_build) > 0}")

    # (g) query throwaway: same corpus -> 10,000 non-empty results
    m_results, _ = mclient.query_batch(queries, top_k=10)
    m_non_empty = sum(1 for r in m_results if r)
    checks["throwaway_query_shape"] = (len(m_results) == len(queries) and m_non_empty == len(queries))
    print(f"(g) throwaway query: {len(m_results)} results, non_empty={m_non_empty}/{len(queries)}")

    # (h) delete top-3 ids of query 0, reindex; they must vanish from query 0's top-10
    del_ids = [int(i) for i, _ in m_results[0][:3]]
    mclient.delete(del_ids, reindex=True)
    after_del, _ = mclient.query_batch([queries[0]], top_k=10)
    after_ids = [int(i) for i, _ in after_del[0]]
    checks["delete_removes_ids"] = all(d not in after_ids for d in del_ids)
    print(f"(h) deleted {del_ids}; query0 top-10 now {after_ids} -> "
          f"deleted gone={all(d not in after_ids for d in del_ids)}")

    # (i) teardown(drop_data=True) leaves no index marker (finding 08 bug fix).
    #     teardown nulls the adapter's _client, so check via the Blocks util directly.
    mclient.teardown(drop_data=True)
    idx_after_drop = list_indexes(BUCKET, throwaway)
    checks["clean_after_drop"] = (idx_after_drop == [])
    print(f"(i) list_indexes('{throwaway}') after drop = {idx_after_drop} -> {idx_after_drop == []}")

    passed = all(checks.values())
    print("\n=== SMOKE TEST", "PASS" if passed else "FAIL", "===")
    for k, v in checks.items():
        print(f"  {'OK ' if v else 'XX '} {k}: {v}")

    out = {"passed": passed, "checks": checks, "recall": recall,
           "num_results": len(results), "timings_keys": list(timings.keys())}
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(os.path.join(RESULTS_DIR, "smoke_result.json"), "w") as f:
        json.dump(out, f, indent=2)

    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
