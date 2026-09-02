"""
AWS Lambda handler wrapping LanceDB — the naive-serverless baseline for the
thesis harness (day 8). One Lambda function, invoked per operation; the LanceDB
dataset lives in S3 (s3:// URI), so every invocation reads/writes object storage.
This is the honest "naive serverless" architecture (finding 15): no warm index
server, no EFS — just Lambda + S3.

Event shape (JSON):
  {
    "op":     "insert" | "query" | "delete" | "drop" | "ping",
    "db_uri": "s3://<bucket>/<prefix>",   # LanceDB connection URI
    "table":  "<table name>",
    # op-specific:
    "dimensions": <int>,                  # insert (used only when creating)
    "ids":     [<int>, ...],              # insert (paired with vectors) / delete
    "vectors": [[<float>, ...], ...],     # insert
    "queries": [[<float>, ...], ...],     # query
    "top_k":   <int>,                     # query
    "metric":  "l2" | "cosine" | "dot",  # query (default "l2")
  }

Response shape (JSON):
  {
    "ok": true,
    "op": "<op>",
    "cold": <bool>,          # True iff this was the container's first invocation
    "server_ms": <float>,    # wall time spent inside the handler
    ... op-specific: "results" | "n_inserted" | "n_deleted_requested" | "dropped"
  }

results (query): [[[id, distance], ...], ...] — one ranked list per query vector,
ascending by distance, ids are the client-supplied integer ids.
"""
import time

import lancedb

# Cold-start detection: this module is imported once per container. The first
# handler call in a fresh container sees _WARM is False (cold); later calls in the
# same warm container see True. This is the standard Lambda cold/warm signal.
_WARM = False

# Cache the LanceDB connection per URI across warm invocations (cheap win; the
# table itself is re-opened per op so writes from other invocations are visible).
_CONN_CACHE = {}


def _connect(db_uri):
    conn = _CONN_CACHE.get(db_uri)
    if conn is None:
        conn = lancedb.connect(db_uri)
        _CONN_CACHE[db_uri] = conn
    return conn


def _do_insert(db, table, event):
    ids = event["ids"]
    vectors = event["vectors"]
    rows = [{"id": int(i), "vector": [float(x) for x in v]} for i, v in zip(ids, vectors)]
    if table in db.table_names():
        tbl = db.open_table(table)
        tbl.add(rows)
    else:
        # Create-on-first-insert: the table lives in Lambda/S3 storage, so the
        # client's setup() never creates it — the first insert does.
        db.create_table(table, data=rows)
    return {"n_inserted": len(rows)}


def _do_query(db, table, event):
    top_k = int(event.get("top_k", 10))
    metric = event.get("metric", "l2")
    queries = event["queries"]
    tbl = db.open_table(table)
    results = []
    for q in queries:
        rows = (
            tbl.search([float(x) for x in q])
            .metric(metric)
            .limit(top_k)
            .select(["id"])
            .to_list()
        )
        # LanceDB returns ascending _distance already; keep it explicit.
        hits = [[int(r["id"]), float(r["_distance"])] for r in rows]
        results.append(hits)
    return {"results": results}


def _do_delete(db, table, event):
    ids = [int(i) for i in event.get("ids", [])]
    if ids:
        tbl = db.open_table(table)
        # LanceDB delete takes a SQL predicate; "id IN (..)" removes by id.
        tbl.delete(f"id IN ({','.join(str(i) for i in ids)})")
    return {"n_deleted_requested": len(ids)}


def _do_drop(db, table, event):
    dropped = False
    if table in db.table_names():
        db.drop_table(table)
        dropped = True
    return {"dropped": dropped}


_OPS = {
    "insert": _do_insert,
    "query": _do_query,
    "delete": _do_delete,
    "drop": _do_drop,
}


def handler(event, context):
    global _WARM
    cold = not _WARM
    _WARM = True

    t0 = time.time()
    op = event.get("op")
    resp = {"ok": True, "op": op, "cold": cold}

    if op == "ping":
        resp["server_ms"] = (time.time() - t0) * 1000.0
        resp["lancedb"] = getattr(lancedb, "__version__", "unknown")
        return resp

    db_uri = event["db_uri"]
    table = event["table"]
    fn = _OPS.get(op)
    if fn is None:
        return {"ok": False, "op": op, "cold": cold, "error": f"unknown op {op!r}"}

    try:
        db = _connect(db_uri)
        resp.update(fn(db, table, event))
    except Exception as e:  # noqa: BLE001 — surface the error to the caller as JSON
        return {"ok": False, "op": op, "cold": cold,
                "error": f"{type(e).__name__}: {e}"}

    resp["server_ms"] = (time.time() - t0) * 1000.0
    return resp
