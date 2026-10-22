"""Usage (from the repo root, inside a launcher env):
    RESULTS_TAG=x experiments/experiment_c3/c3_aws.sh scripts/ec2/smoke10m.py s3vectors 3

Read-only smoke check against the live 10M indexes: N queries per adapter,
recall@10 vs ground truth, client latency. No ingest, no teardown."""
import os, sys, time, json

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
C3 = os.path.join(ROOT, "experiments", "experiment_c3")
for p in (ROOT, C3, os.path.join(ROOT, "experiments", "experiment_c1"),
          os.path.join(ROOT, "experiments", "experiment_b")):
    if p not in sys.path:
        sys.path.insert(0, p)
os.chdir(C3)
import c3_common as c3  # noqa: E402

adapter = sys.argv[1]
n = int(sys.argv[2]) if len(sys.argv) > 2 else 3
queries = c3.load_queries()[:n]
truth = c3.load_truth()[:n]

t0 = time.time()
client = c3.build_and_setup(adapter, create_if_missing=False)
print(f"[{adapter}] setup {time.time()-t0:.1f}s backend_version={client.backend_version()}")
fp = client.dataset_fingerprint(c3.DATASET)
results, lat = [], []
for i, q in enumerate(queries):
    t = time.time()
    r = client.query(q, top_k=10)
    lat.append(time.time() - t)
    hits, timings = r
    results.append(hits)
    print(f"[{adapter}] q{i} {lat[-1]:.2f}s top3={[h[0] for h in hits[:3]]} truth3={list(truth[i])[:3]}")
rec = c3.recall_at_k(results, truth)
print(json.dumps({"adapter": adapter, "n": n, "recall_pct": rec[0],
                  "lat_s": [round(x, 3) for x in lat],
                  "content_sha256": fp.get("content_sha256")}))
