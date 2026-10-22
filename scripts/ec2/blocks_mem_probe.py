"""Memory probe for the Blocks 10M query path (2026-10-17).

The C3 Blocks c=1 sweep cell grew ~60 MB per query and ran the 8 GB EC2 client
out of memory after ~120 queries. This runs N queries the same way the sweep does
(c3_common.build_and_setup + adapter.query) and prints RSS after each, plus the
thread count. Used to find the leak and to check the fix in
harness/adapters/blocks.py (_release_blocks_db).

Run via the C3 Blocks launcher:
    RESULTS_TAG=probe ./c3_blocks.sh ../../scripts/ec2/blocks_mem_probe.py 12
"""
import gc, os, sys, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
C3 = os.path.join(HERE, "..", "..", "experiments", "experiment_c3")
sys.path.insert(0, os.path.abspath(C3))
import c3_common as K  # noqa: E402

def rss_mb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1]) / 1024
    return -1

n = int(sys.argv[1]) if len(sys.argv) > 1 else 12
client = K.build_and_setup("blocks", create_if_missing=True)
client = client[-1] if isinstance(client, tuple) else client
queries = K.load_queries()
print(f"setup done rss={rss_mb():.0f} MB threads={threading.active_count()}", flush=True)
for i in range(n):
    t = time.time()
    hits, _ = client.query(queries[i % len(queries)], top_k=10)
    print(f"q{i:02d} {time.time()-t:5.1f}s rss={rss_mb():7.0f} MB threads={threading.active_count()} hits={len(hits)}", flush=True)
gc.collect()
print(f"after gc.collect rss={rss_mb():.0f} MB threads={threading.active_count()}")
