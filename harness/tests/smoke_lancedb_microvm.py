"""Phase 2 gate for the LanceDB-on-MicroVM adapter (fix list F39, findings 29/30).

Checks the BenchmarkVectorClient contract and the gate agreed on 18 Oct, against a live MicroVM:
  identity and backend_version fields; ingest raises (table baked into the image); fingerprint
  equals the shared corpus hash; timings envelope; a suspend followed by an auto-resume is flagged
  `resumed`; attach mode never terminates a MicroVM it did not launch; teardown terminates and is
  idempotent; recall@10 on N queries against the ground truth (bar 99%, as LanceDB-on-Lambda), and
  per-query top-10 ids compared with the LanceDB-on-Lambda B run (same engine, same data).
Run from the EC2 client:
    AWS_DEFAULT_REGION=us-east-1 harness/.venv/bin/python -m harness.tests.smoke_lancedb_microvm --dataset deep_1m
Writes scripts/microvm/results/gate_<dataset>_<utc>.json.
"""
import argparse
import json
import os
import statistics
import time

from harness.adapters.lancedb_microvm import LanceDBMicroVMClient
import boto3

ACCT = boto3.client("sts").get_caller_identity()["Account"]
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
EXPECTED_SHA = {"deep_1m": "aa56f86460774bcaeec4675120c5d07de8000c58a008097abca2e6ccd75d21df",
                "deep_10m": "72e0840f1f4605ac5d8715422f3d77154b310414e908ce6768b1bcebda0aa849"}
LAMBDA_PER_QUERY = {"deep_1m": "experiments/experiment_b/results_2026-10-ec2/recall_lancedb.per_query.json",
                    # C3 10M LanceDB gate (50 queries, the same first 50 query vectors)
                    "deep_10m": "experiments/experiment_c3/results_2026-10-ec2/phase2_lancedb.per_query.json"}


def check(res, name, ok, detail=None):
    res["checks"].append({"check": name, "ok": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + (f"  {detail}" if detail is not None else ""), flush=True)


def make(ds, **kw):
    rec = os.path.join(ROOT, "scripts", "microvm", "results", f"make_table_{ds}.json")
    return LanceDBMicroVMClient(
        f"thesis-lancedb-mvm-{ds.replace('_', '')}", account_id=ACCT,
        execution_role_arn=f"arn:aws:iam::{ACCT}:role/thesis-microvm-exec",
        source_csv_path=os.path.join(ROOT, "datasets", ds, f"vectors_{ds}.csv"),
        table_record=json.load(open(rec)) if os.path.exists(rec) else None, **kw)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="deep_1m")
    ap.add_argument("--recall-n", type=int, default=1000)
    ap.add_argument("--settle", type=float, default=30.0)
    ap.add_argument("--idle-suspend", type=int, default=60)
    a = ap.parse_args()
    ds = a.dataset
    res = {"dataset": ds, "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "checks": []}
    c = make(ds, settle_s=a.settle, idle_suspend_s=a.idle_suspend)
    other = None
    try:
        check(res, "name", c.name == "lancedb-microvm", c.name)
        cap = c.capabilities()
        check(res, "capabilities before setup", cap.ingest_modes == set() and cap.metric == "l2"
              and cap.id_stability == "preserved" and not cap.supports_pending, cap)
        for op, fn in (("bulk_insert", lambda: c.bulk_insert([])), ("delete", lambda: c.delete([1]))):
            try:
                fn()
                check(res, f"{op} raises NotImplementedError", False)
            except NotImplementedError:
                check(res, f"{op} raises NotImplementedError", True)
        fp = c.dataset_fingerprint(ds)
        check(res, "fingerprint = shared corpus hash", fp.get("content_sha256") == EXPECTED_SHA[ds],
              fp.get("content_sha256"))

        t0 = time.time()
        c.setup(ds, dimensions=96)
        res["setup_s"] = time.time() - t0
        bv = c.backend_version()
        res["backend_version"], res["ingest_record"], res["lifecycle"] = bv, c.ingest_record, c.lifecycle_record
        check(res, "backend_version fields", bv["lancedb"] == "0.33.0" and bv["image_version"] != "unknown"
              and isinstance(bv["baseline_memory_mib"], int) and bv["cpu_part"] not in ("unknown", None), bv)
        check(res, "max_safe_concurrency from size", c.capabilities().max_safe_concurrency
              == {2048: 8, 4096: 16, 8192: 32}.get(bv["baseline_memory_mib"]), c.capabilities().max_safe_concurrency)
        check(res, "ingest_record (no pretend ingest)", c.ingest_record.get("table") is not None
              and str(c.ingest_record.get("image_env_table_uri", "")).endswith(f"/{ds}.lance"), c.ingest_record)

        qs = [[float(x) for x in l.split()] for _, l in
              zip(range(a.recall_n), open(os.path.join(ROOT, "datasets", ds, f"queries_{ds}_1000q.csv")))]
        gt = [[int(x) for x in l.split(",")[:10]] for _, l in
              zip(range(a.recall_n), open(os.path.join(ROOT, "datasets", ds, f"true_neighbours_{ds}_1000q.csv")))]
        hits0, tm0 = c.query(qs[0])
        check(res, "timings envelope", {"op", "wall_clock_s", "server_ms", "cpu_part", "resumed",
                                        "first_after_launch"} <= set(tm0) and tm0["op"] == "query", tm0)
        check(res, "hits ascending, k=10", len(hits0) == 10 and all(hits0[i][1] <= hits0[i + 1][1] for i in range(9)))

        found, walls, servers, ids_mvm = 0, [], [], []
        for q, g in zip(qs, gt):
            h, tm = c.query(q)
            found += len({i for i, _ in h} & set(g))
            walls.append(tm["wall_clock_s"])
            servers.append(tm["server_ms"])
            ids_mvm.append([i for i, _ in h])
        recall = 100.0 * found / (10 * len(qs))
        res["recall"] = {"n": len(qs), "recall_at_10_pct": recall, "wall_median_s": statistics.median(walls),
                         "server_median_ms": statistics.median(servers)}
        check(res, "recall@10 >= 99% (gate)", recall >= 99.0, f"{recall:.2f}% on {len(qs)} queries")
        lp = LAMBDA_PER_QUERY.get(ds)
        if lp and os.path.exists(os.path.join(ROOT, lp)):
            lam = {r["query_index"]: r["ids"] for r in json.load(open(os.path.join(ROOT, lp)))["per_query"]}
            same = sum(ids_mvm[i] == lam[i] for i in range(len(ids_mvm)) if i in lam)
            sets = sum(set(ids_mvm[i]) == set(lam[i]) for i in range(len(ids_mvm)) if i in lam)
            n = sum(1 for i in range(len(ids_mvm)) if i in lam)
            res["vs_lambda"] = {"n": n, "identical_ranked": same, "identical_sets": sets}
            check(res, "same top-10 sets as LanceDB-on-Lambda", sets == n, res["vs_lambda"])

        batch, tb = c.query_batch(qs[:10])
        check(res, "query_batch = query", [[i for i, _ in h] for h in batch] == ids_mvm[:10], tb)

        # attach mode: a second client attaches by id, queries, and must not terminate on teardown
        other = make(ds, attach_microvm_id=c.microvm_id)
        other.setup(ds, dimensions=96)
        _, to = other.query(qs[1])
        other.teardown()
        check(res, "attach: query ok and teardown leaves it running", c._state() == "RUNNING", to)
        other = None

        # suspend -> auto-resume on the next request, flagged `resumed`
        res["suspend_s"] = c.suspend()
        t1 = time.time()
        _, tr = c.query(qs[2])
        res["first_after_resume"] = {"client_s": time.time() - t1, **tr}
        _, tn = c.query(qs[3])
        check(res, "resumed flag on first query after resume only", tr["resumed"] and not tn["resumed"], [tr, tn])
    except Exception as e:  # noqa: BLE001
        res["error"] = f"{type(e).__name__}: {e}"
        print("ERROR", res["error"], flush=True)
    finally:
        if other is not None:
            other.teardown()
        mid = c.microvm_id
        c.teardown()
        c.teardown()  # idempotent
        if mid:
            st = c._mvm.get_microvm(microvmIdentifier=mid)["state"]
            check(res, "teardown terminated the owned MicroVM (and is idempotent)", st == "TERMINATED", st)
        res["passed"] = all(x["ok"] for x in res["checks"]) and "error" not in res
        out = os.path.join(ROOT, "scripts", "microvm", "results",
                           f"gate_{ds}_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
        json.dump(res, open(out, "w"), indent=1, default=str)
        print("GATE", "PASSED" if res["passed"] else "FAILED", "->", out, flush=True)


if __name__ == "__main__":
    main()
