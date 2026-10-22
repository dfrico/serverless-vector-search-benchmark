"""Drop one re-ingest LanceDB table (2026-10-18). Refuses the measured deep_10m table.

    ./c3_aws.sh c3_drop_lancedb.py deep_10m_reingest
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
import c3_common as K  # noqa: E402

name = sys.argv[1]
if name in ("deep_10m", "deep_1m") or "reingest" not in name:
    raise SystemExit(f"refusing to drop {name!r}: only *reingest* tables")
client = K.build_client("lancedb")
client.setup(name, dimensions=K.DIMENSIONS, top_k_default=K.TOP_K, create_if_missing=False)
client.teardown(drop_data=True)
print(f"dropped LanceDB table {name}")
