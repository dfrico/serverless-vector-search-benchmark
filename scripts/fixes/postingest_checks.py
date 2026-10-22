"""Post-ingest integrity checks on the deployed deep_10m data (Pedro's critical #1).

For each system this checks what the content_sha256 cannot:
  1. COUNT     - number of vectors stored == line count of the source CSV
  2. VALUES    - sampled stored vectors == source vectors (where the API returns data)
  3. SELF-QUERY- querying with a sampled source vector returns that same id at rank 1
                 with distance ~0 (catches missing rows, wrong ids, bad conversion)

Read-only on AWS except the queries themselves. Estimated cost < $0.30
(Blocks self-queries dominate: 20 Lambda tasks per query).

Run via run_checks.sh (sets AWS_PROFILE=thesis). Output:
  scripts/fixes/results/postingest_<system>.json
"""
from __future__ import annotations
import argparse, json, os, random, struct, sys, time
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
for p in (ROOT, os.path.join(ROOT, "experiments", "experiment_c3"),
          os.path.join(ROOT, "experiments", "experiment_c1"),
          os.path.join(ROOT, "experiments", "experiment_b"),
          os.path.join(ROOT, "experiments", "experiment_c2"),
          os.path.join(ROOT, "references", "blocks-db-wip")):
    if p not in sys.path:
        sys.path.insert(0, p)

import boto3  # noqa: E402
import c3_common as C3  # noqa: E402

OUT = os.path.join(HERE, "results")
os.makedirs(OUT, exist_ok=True)
EXPECTED_N = 10_000_000
EPS_SQ = 1e-6   # squared-L2 tolerance for "same vector"


def sample_source_vectors(n_sample, seed=42, csv=C3.VECTORS_CSV):
    """One streaming pass over the 10M CSV; keep n_sample random rows. Also counts lines."""
    rng = random.Random(seed)
    wanted = set(rng.sample(range(EXPECTED_N), n_sample))
    got, n_lines = {}, 0
    t0 = time.time()
    with open(csv) as f:
        for line in f:
            if not line.strip():
                continue
            if n_lines in wanted:
                id_str, rest = line.rstrip("\n").split(",", 1)
                got[int(id_str)] = [float(x) for x in rest.split(" ") if x]
            n_lines += 1
    print(f"[sample] read {n_lines:,} lines in {time.time()-t0:.0f}s, kept {len(got)}")
    return got, n_lines


def sqdist(a, b):
    return sum((x - y) ** 2 for x, y in zip(a, b))


def self_query(client, samples, workers):
    """Query each sampled vector; record rank-1 id, distance, and whether id is in top-10."""
    def one(item):
        vid, vec = item
        try:
            hits, _ = client.query(vec, top_k=10)
            ids = [int(h[0]) for h in hits]
            d0 = float(hits[0][1]) if hits else None
            return dict(id=vid, top1=ids[0] if ids else None, top1_dist=d0,
                        in_top10=vid in ids, ok=True)
        except Exception as e:  # noqa: BLE001
            return dict(id=vid, ok=False, error=f"{type(e).__name__}: {e}")
    with ThreadPoolExecutor(max_workers=workers) as ex:
        rows = list(ex.map(one, samples.items()))
    ok = [r for r in rows if r["ok"]]
    return dict(n=len(rows), n_ok=len(ok),
                top1_exact=sum(r["top1"] == r["id"] for r in ok),
                top1_zero_dist_other_id=sum(r["top1"] != r["id"] and r["top1_dist"] is not None
                                            and r["top1_dist"] < 1e-6 for r in ok),
                in_top10=sum(r["in_top10"] for r in ok),
                failures=[r for r in rows if not r["ok"]][:5],
                mismatches=[r for r in ok if r["top1"] != r["id"]][:20])


# ---------------------------------------------------------------- S3 Vectors
def check_s3vectors(samples, n_lines, n_query):
    a = C3.acct()
    s3v = boto3.client("s3vectors", region_name="us-east-1")
    from harness.adapters.s3vectors import S3VectorsClient
    bucket, index = f"thesis-s3v-{a}", S3VectorsClient._sanitize_name(C3.DATASET)
    # COUNT: ids are dense 0..N-1 and ingest was in order -> binary-search the largest existing key,
    # then confirm the key just above it does not exist.
    def exists(k):
        r = s3v.get_vectors(vectorBucketName=bucket, indexName=index, keys=[str(k)])
        return len(r.get("vectors", [])) == 1
    lo, hi = 0, 2 * EXPECTED_N
    while lo < hi:
        mid = (lo + hi + 1) // 2
        lo, hi = (mid, hi) if exists(mid) else (lo, mid - 1)
    count = dict(max_key=lo, implied_count=lo + 1, key_above_exists=exists(lo + 1),
                 note="assumes dense ids + in-order ingest; spot-checked by VALUES below")
    # VALUES: fetch sampled vectors with data, compare to source
    ids = list(samples)
    found, max_sq, missing = 0, 0.0, []
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        r = s3v.get_vectors(vectorBucketName=bucket, indexName=index,
                            keys=[str(k) for k in chunk], returnData=True)
        got = {int(v["key"]): v["data"]["float32"] for v in r.get("vectors", [])}
        for k in chunk:
            if k not in got:
                missing.append(k); continue
            found += 1
            max_sq = max(max_sq, sqdist(got[k], samples[k]))
    values = dict(n=len(ids), found=found, missing=missing[:20], max_sq_diff=max_sq,
                  all_equal=(not missing and max_sq < EPS_SQ))
    client = C3.build_and_setup("s3vectors", create_if_missing=False)
    sq = self_query(client, dict(list(samples.items())[:n_query]), workers=4)
    return dict(system="s3vectors", index=index, csv_lines=n_lines, count=count,
                values=values, self_query=sq)


# ---------------------------------------------------------------- LanceDB
def check_lancedb(samples, n_lines, n_query):
    res = dict(system="lancedb", csv_lines=n_lines)
    a = C3.acct()
    uri = f"s3://thesis-lancedb-{a}/lancedb"
    try:  # direct read from the laptop (needs `pip install lancedb` in this venv)
        import lancedb
        db = lancedb.connect(uri)
        tbl = db.open_table(C3.DATASET)
        res["count"] = dict(count_rows=tbl.count_rows())
        res["schema"] = str(tbl.schema)  # float32 vs float64 vector column matters for table size
        ids = list(samples)[:200]
        rows = tbl.search().where(f"id IN ({','.join(map(str, ids))})").limit(len(ids)).to_list()
        vcol = next(k for k in rows[0] if k in ("vector", "vec", "embedding")) if rows else None
        got = {int(r["id"]): list(r[vcol]) for r in rows}
        miss = [k for k in ids if k not in got]
        max_sq = max((sqdist(got[k], samples[k]) for k in got), default=None)
        res["values"] = dict(n=len(ids), found=len(got), missing=miss[:20], max_sq_diff=max_sq,
                             all_equal=(not miss and max_sq is not None and max_sq < EPS_SQ))
    except Exception as e:  # noqa: BLE001
        res["direct_read_error"] = f"{type(e).__name__}: {e} (count/values skipped; self-query still runs)"
    client = C3.build_and_setup("lancedb", create_if_missing=False)
    res["self_query"] = self_query(client, dict(list(samples.items())[:n_query]), workers=10)
    return res


# ---------------------------------------------------------------- Blocks
def check_blocks(samples, n_lines, n_query):
    a = C3.acct()
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = f"thesis-blocks-{a}"
    prefix = f"indexes/{C3.DATASET}/"
    keys, token = [], None
    while True:
        kw = dict(Bucket=bucket, Prefix=prefix)
        if token: kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in r.get("Contents", []) if o["Key"].endswith(".ann")]
        if not r.get("IsTruncated"): break
        token = r["NextContinuationToken"]
    # COUNT: FAISS index header = fourcc(4) d(int32) ntotal(int64); read only the first 16 bytes.
    per_block, fourccs = {}, set()
    for k in keys:
        b = s3.get_object(Bucket=bucket, Key=k, Range="bytes=0-15")["Body"].read()
        fourcc = b[:4].decode("latin-1"); d, ntotal = struct.unpack("<iq", b[4:16])
        fourccs.add(fourcc); per_block[k.rsplit("/", 1)[-1]] = dict(d=d, ntotal=ntotal)
    total = sum(v["ntotal"] for v in per_block.values())
    count = dict(n_block_files=len(keys), fourccs=sorted(fourccs), sum_ntotal=total,
                 dims={v["d"] for v in per_block.values()} == {96},
                 note="if the fourcc is not a known FAISS index code, the header parse is invalid - "
                      "inspect one file instead")
    client = C3.build_and_setup("blocks", create_if_missing=False)
    sq = self_query(client, dict(list(samples.items())[:n_query]), workers=1)
    return dict(system="blocks", csv_lines=n_lines, count=count, self_query=sq)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("systems", nargs="+", choices=["s3vectors", "lancedb", "blocks"])
    ap.add_argument("--sample", type=int, default=1000, help="source rows sampled (values check)")
    ap.add_argument("--q-s3v", type=int, default=200)
    ap.add_argument("--q-lancedb", type=int, default=50, help="~51 s per query, 10 in parallel")
    ap.add_argument("--q-blocks", type=int, default=30, help="~13 s per query, serial")
    ap.add_argument("--dataset", choices=["deep_10m", "deep_1m"], default="deep_10m",
                    help="deep_1m (2026-10-16): check the 1M indexes a re-run just built")
    args = ap.parse_args()
    global EXPECTED_N, OUT
    if args.dataset == "deep_1m":
        # Point the C3 helpers at the 1M corpus and the C2 Blocks parameters
        # (16 blocks); results go to results_<RESULTS_TAG or 1m>/ so the 10M
        # files from 25-26 Sep are kept.
        import c2_common as C2
        C3.DATASET = "deep_1m"
        C3.DATA_DIR = os.path.join(ROOT, "datasets", "deep_1m")
        C3.VECTORS_CSV = os.path.join(C3.DATA_DIR, "vectors_deep_1m.csv")
        C3.BLOCKS_PARAMS = dict(C2.BLOCKS_PARAMS)
        EXPECTED_N = 1_000_000
        OUT = os.path.join(HERE, "results", f"deep_1m_{os.environ.get('RESULTS_TAG', 'adhoc')}")
        os.makedirs(OUT, exist_ok=True)
    samples, n_lines = sample_source_vectors(args.sample, csv=C3.VECTORS_CSV)
    for s in args.systems:
        t0 = time.time()
        fn = dict(s3vectors=check_s3vectors, lancedb=check_lancedb, blocks=check_blocks)[s]
        nq = dict(s3vectors=args.q_s3v, lancedb=args.q_lancedb, blocks=args.q_blocks)[s]
        res = fn(samples, n_lines, nq)
        res["elapsed_s"] = round(time.time() - t0, 1)
        res["timestamp_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        path = os.path.join(OUT, f"postingest_{s}.json")
        json.dump(res, open(path, "w"), indent=1, default=str)
        print(json.dumps({k: v for k, v in res.items() if k != "self_query"}, indent=1, default=str))
        print("self_query:", {k: v for k, v in res["self_query"].items() if k not in ("mismatches", "failures")})
        print(f"-> {path}")


if __name__ == "__main__":
    main()
