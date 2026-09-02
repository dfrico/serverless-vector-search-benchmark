"""Quick LanceDB timing probe: measure cold single query, warm single query,
and a 10-query batch, to size the recall chunk. No ingest — reconnect only."""
from __future__ import annotations
import functools, os, sys, time
print = functools.partial(print, flush=True)  # noqa

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))   # experiments/experiment_b -> repo root
for _p in (_HERE, WORKSPACE):   # _HERE for eb_common, root for the `harness` package
    if _p not in sys.path:
        sys.path.insert(0, _p)
import eb_common as C  # noqa

acct = C.account_id()
queries = C.load_queries()
client = C.build_client("lancedb", acct)
client.setup(C.DATASET, dimensions=C.DIMENSIONS, top_k_default=C.TOP_K, create_if_missing=True)
import boto3
from botocore.config import Config
client._client = boto3.client("lambda", region_name="us-east-1",
                              config=Config(read_timeout=890, connect_timeout=15, retries={"max_attempts": 1}))

for label, n in [("single-A", 1), ("single-B", 1), ("batch-10", 10)]:
    t = time.time()
    res, tm = client.query_batch(queries[:n], top_k=C.TOP_K)
    print(f"{label}: n={n} wall={time.time()-t:.2f}s server_ms={tm.get('server_ms')} "
          f"cold={tm.get('cold')} got={len(res)} ids0={[i for i,_ in res[0][:3]]}")
