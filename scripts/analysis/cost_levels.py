"""Cost Levels 1 and 2 per measured cell, for one results tag (fix list F2, F41; data items D2-D5).

Level 1 (Lambda charge): CloudWatch REPORT lines (@billedDuration, which includes INIT for our
container-image functions) whose timestamp falls inside the cell's query windows, x memory x the
us-east-1 list rate, + $0.20 per million requests. No free tier.
Level 2 (marginal cost per query): Level 1 + S3 requests counted from the S3 server access logs in
the same windows (Blocks, LanceDB and Lithops buckets) + DynamoDB reads on BlocksDB-default
(CloudWatch ConsumedReadCapacityUnits) + S3 Vectors request and data-processed fees.

Windows come from the per-query wall_submit_utc / wall_ack_utc that every October record carries.
A sweep cell is one window [first submit, last ack] (cells ran one at a time); a C2 cold-start state
is one window per sample. Warm-up queries are outside the windows.
C2 samples of one cycle run back to back (one sample's ack is the next one's submit), so padded windows
would overlap and count the boundary events twice. Every event is therefore assigned to exactly one
window (exclusive_intervals): a window keeps PAD_AFTER after its ack for REPORT lines (a REPORT is
logged at or before the ack; no invocation of ours ends within 0.5 s of its query's submit), and
0.5 s for S3 log records (1 s resolution, compared at the middle of their second). Totals are exact;
only the split between two adjacent C2 states can move by the requests of that boundary second.

Usage (laptop, harness/.venv, read-only on AWS; Logs Insights costs $0.005/GB scanned):
    AWS_PROFILE=thesis harness/.venv/bin/python scripts/analysis/cost_levels.py \
        --tag 2026-10-ec2 --s3logs ~/Developer/mst-datasets/s3logs
Writes scripts/analysis/results_<tag>/cost_levels.{json,md}.
"""
from __future__ import annotations

import argparse
import bisect
import collections
import datetime as dt
import glob
import json
import os
import re
import statistics as st
import time

import boto3

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
EXP = os.path.join(ROOT, "experiments")

# --- prices (us-east-1 list, to re-check at edit time: fix list D11) ---
X86 = 0.0000166667      # $/GB-s
ARM = 0.0000133334
LREQ = 0.20e-6          # $/Lambda request
S3_GET = 0.0004e-3      # GET, HEAD and other requests
S3_PUT = 0.005e-3       # PUT, COPY, POST, LIST
DDB_RRU = 0.125e-6      # on-demand read request unit
S3V_REQ = 2.5e-6
S3V_TIERS = [(100_000, 0.004), (10_000_000, 0.002), (float("inf"), 0.0004)]  # $/TB by vector band
S3V_VEC_BYTES = 96 * 4  # vector data; the key is added per index size in s3v_fee (no filterable metadata)
S3V_TB = 2 ** 40        # AWS's pricing example (5.17 KB, $1.93) only works with binary KB and TB


def s3v_key_bytes(nvec):
    """Average length of the str(int) keys 0..nvec-1, which S3 Vectors counts in data processed."""
    return sum((min(nvec, 10 ** d) - (10 ** (d - 1) if d > 1 else 0)) * d for d in range(1, len(str(nvec)) + 1)) / nvec

GROUPS = {  # log group -> (memory GB, $/GB-s)
    "/aws/lambda/lithops-worker-rb26-363-11170a3116": (3.008, X86),   # Blocks map
    "/aws/lambda/lithops-worker-rb26-363-600f82b870": (2.048, X86),   # Blocks reduce
    "/aws/lambda/thesis-lancedb-lambda": (2.048, ARM),
}
ADAPTER_GROUPS = {
    "blocks": list(GROUPS)[:2],
    "lancedb": [list(GROUPS)[2]],
    "s3vectors": [],
}
ACCT = boto3.client("sts").get_caller_identity()["Account"]
BUCKETS = {
    "blocks": [f"thesis-blocks-{ACCT}", "lithops-us-east-1-rb26"],
    "lancedb": [f"thesis-lancedb-{ACCT}"],
    "s3vectors": [],
}
# s around each window for REPORT lines (ms timestamps). Measured 18 Oct: a LanceDB REPORT is logged
# within 0.01 s of the client's ack, a Blocks reduce REPORT 1.2-2.3 s before it; 0.5 s keeps the
# harness's own post-run invocations (a 2 ms ping 0.9 s after the last C2 sample) out of the cell.
PAD_BEFORE, PAD_AFTER = 0.5, 0.5
S3_PAD_AFTER = 0.5                 # s after each window for S3 log records (1 s resolution)

logs = boto3.client("logs", region_name="us-east-1")
cw = boto3.client("cloudwatch", region_name="us-east-1")


# ----------------------------------------------------------------------------- cells
def _jsonl(path):
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def _win(recs):
    sub = [r["wall_submit_utc"] for r in recs]
    ack = [r["wall_ack_utc"] for r in recs]
    return min(sub), max(ack)


def _lat(recs, key):
    return [r[key] for r in recs if r.get(key) is not None]


def cells(tag):
    out = []
    rd = lambda e: os.path.join(EXP, e, f"results_{tag}")
    for exp, size in (("experiment_b", "1m"), ("experiment_c3", "10m")):
        for f in sorted(glob.glob(os.path.join(rd(exp), "sweep_*_c*.jsonl"))):
            m = re.match(r"sweep_(\w+?)_c(\d+)\.jsonl", os.path.basename(f))
            recs = [r for r in _jsonl(f) if r.get("op") == "query" and r.get("ok")]
            out.append(dict(exp=exp[11:], cell=f"{m[1]}_c{m[2]}", adapter=m[1], size=size,
                            n=len(recs), windows=[_win(recs)],
                            p50=st.median(_lat(recs, "latency_s"))))
    for f in sorted(glob.glob(os.path.join(rd("experiment_c1"), "measure_*.raw.json"))):
        recs = [r for r in json.load(open(f))["records"] if r["phase"] == "measured"]
        lat = [r["wall_ack_utc"] - r["wall_submit_utc"] for r in recs]
        out.append(dict(exp="c1", cell=os.path.basename(f)[8:-9], adapter="blocks", size="1m",
                        n=len(recs), windows=[_win(recs)], p50=st.median(lat)))
    for f in sorted(glob.glob(os.path.join(rd("experiment_c2"), "cold_*.jsonl"))):
        by = collections.defaultdict(list)
        for r in _jsonl(f):
            by[r["config"]].append(r)
        for cfg, recs in by.items():
            out.append(dict(exp="c2", cell=f"{recs[0]['adapter']}_{cfg}", adapter=recs[0]["adapter"],
                            size="1m", n=len(recs),
                            windows=[(r["wall_submit_utc"], r["wall_ack_utc"]) for r in recs],
                            p50=st.median(_lat(recs, "wall_clock_s"))))
    return out


# ----------------------------------------------------------------------------- level 1
Q = ('filter @type = "REPORT" | stats count(*) as n, sum(@billedDuration) as billed_ms, '
     'count(@initDuration) as n_init, sum(@initDuration) as init_ms by @log')


def insights(groups, t0, t1):
    qid = logs.start_query(logGroupNames=groups, startTime=int(t0), endTime=int(t1) + 1,
                           queryString=Q, limit=100)["queryId"]
    while True:
        r = logs.get_query_results(queryId=qid)
        if r["status"] in ("Complete", "Failed", "Cancelled", "Timeout"):
            break
        time.sleep(1)
    if r["status"] != "Complete":
        raise RuntimeError(f"insights {r['status']}")
    rows = {}
    for row in r["results"]:
        d = {x["field"]: x["value"] for x in row}
        grp = "/aws/lambda/" + d["@log"].split(":/aws/lambda/")[-1]
        rows[grp] = {k: float(d.get(k) or 0) for k in ("n", "billed_ms", "n_init", "init_ms")}
    return rows, r["statistics"].get("bytesScanned", 0)


def report_lines(groups, t0, t1):
    """REPORT events in [t0, t1] with exact timestamps (filter_log_events, no Insights rounding)."""
    out = collections.defaultdict(lambda: dict(n=0, billed_ms=0.0, n_init=0, init_ms=0.0))
    rx = re.compile(r"Billed Duration: (\d+) ms.*?(?:Init Duration: ([\d.]+) ms)?\s*$")
    for g in groups:
        kw = dict(logGroupName=g, startTime=int(t0 * 1000), endTime=int(t1 * 1000),
                  filterPattern='"REPORT RequestId"')
        while True:
            r = logs.filter_log_events(**kw)
            for e in r["events"]:
                m = rx.search(e["message"].strip())
                if not m:
                    continue
                o = out[g]
                o["n"] += 1
                o["billed_ms"] += float(m[1])
                if m[2]:
                    o["n_init"] += 1
                    o["init_ms"] += float(m[2])
            if "nextToken" not in r:
                break
            kw["nextToken"] = r["nextToken"]
    return dict(out)


def exclusive_intervals(cs, after, key):
    """Give each cell `key`: its windows as non-overlapping [lo, hi) intervals, padded by
    PAD_BEFORE / `after`, with an overlap between consecutive windows (any cells, same adapter) given
    to the earlier window."""
    by = collections.defaultdict(list)
    for c in cs:
        for w in c["windows"]:
            by[c["adapter"]].append((w[0], w[1], c))
    for wins in by.values():
        wins.sort(key=lambda x: x[0])
        prev_hi = float("-inf")
        for a, b, c in wins:
            lo = max(a - PAD_BEFORE, prev_hi)
            hi = max(b + after, lo)
            c.setdefault(key, []).append((lo, hi))
            prev_hi = hi


def level1(cell):
    groups = ADAPTER_GROUPS[cell["adapter"]]
    tot = collections.defaultdict(lambda: dict(n=0, billed_ms=0.0, n_init=0, init_ms=0.0))
    for a, b in cell["iv_report"]:
        for g, v in report_lines(groups, a, b - 0.001).items():
            for k in v:
                tot[g][k] += v[k]
    usd = gbs = n_inv = n_init = 0.0
    per_group = {}
    for g, v in tot.items():
        mem, rate = GROUPS[g]
        g_gbs = v["billed_ms"] / 1000 * mem
        per_group[g.rsplit("/", 1)[-1]] = dict(v, gb_s=g_gbs)
        gbs += g_gbs
        usd += g_gbs * rate + v["n"] * LREQ
        n_inv += v["n"]
        n_init += v["n_init"]
    n = cell["n"]
    return dict(l1_usd_q=usd / n if n else 0.0, gb_s_q=gbs / n if n else 0.0,
                inv_q=n_inv / n if n else 0.0, n_init=int(n_init), per_group=per_group)


# ----------------------------------------------------------------------------- level 2
LOG_RX = re.compile(r'^\S+ (\S+) \[([^\]]+)\] \S+ \S+ \S+ (\S+) (\S+) ')
# Benchmark tooling traffic, not system requests: code bundles shipped to the EC2 clients and
# result pushes share the Blocks bucket and can fall inside a cell window (2026-10-18).
TOOLING_PREFIXES = ("client-bundle/", "client-results/")


def op_class(op):
    if op.startswith("REST.DELETE") or op.startswith("BATCH.DELETE") or not op.startswith("REST."):
        return None                     # free, or not a billed client request
    if op == "REST.GET.BUCKET" or op.startswith(("REST.PUT", "REST.POST", "REST.COPY")):
        return "put_list"
    return "get"


def load_s3logs(root, t_min, t_max):
    """{bucket: {class: sorted [epoch seconds]}} for records in [t_min, t_max]."""
    ev = collections.defaultdict(lambda: collections.defaultdict(list))
    newest = 0.0                    # newest record in any parsed file, inside the windows or not
    day0 = dt.datetime.fromtimestamp(t_min - 86400, dt.timezone.utc).strftime("%Y-%m-%d")
    n_files = 0
    for path in glob.iglob(os.path.join(root, "**", "*"), recursive=True):
        base = os.path.basename(path)
        if os.path.isdir(path) or base[:10] < day0:
            continue
        n_files += 1
        with open(path, errors="replace") as f:
            for line in f:
                m = LOG_RX.match(line)
                if not m:
                    continue
                cls = op_class(m[3])
                if cls is None or m[4].startswith(TOOLING_PREFIXES):
                    continue
                t = dt.datetime.strptime(m[2], "%d/%b/%Y:%H:%M:%S %z").timestamp()
                newest = max(newest, t)
                if t_min <= t <= t_max:
                    ev[m[1]][cls].append(t)
    for b in ev.values():
        for c in b.values():
            c.sort()
    return ev, n_files, newest


def count_half(ts, a, b):
    """Records whose mid-second T + 0.5 lies in [a, b)."""
    return bisect.bisect_left(ts, b - 0.5) - bisect.bisect_left(ts, a - 0.5)


def s3_requests(cell, ev):
    get = put = 0
    by_bucket = {}
    for bucket in BUCKETS[cell["adapter"]]:
        g = p = 0
        for a, b in cell["iv_s3"]:
            # log times are truncated to the second: a record at second T counts at T + 0.5
            g += count_half(ev.get(bucket, {}).get("get", []), a, b)
            p += count_half(ev.get(bucket, {}).get("put_list", []), a, b)
        by_bucket[bucket.replace(ACCT, "ACCT")] = dict(get=g, put_list=p)
        get += g
        put += p
    n = cell["n"]
    return dict(s3_get_q=get / n, s3_put_list_q=put / n,
                s3_usd_q=(get * S3_GET + put * S3_PUT) / n, s3_by_bucket=by_bucket)


def ddb_reads(cell):
    if cell["adapter"] != "blocks":
        return dict(ddb_rru_q=0.0, ddb_usd_q=0.0)
    a = min(w[0] for w in cell["windows"])
    b = max(w[1] for w in cell["windows"])
    r = cw.get_metric_statistics(
        Namespace="AWS/DynamoDB", MetricName="ConsumedReadCapacityUnits",
        Dimensions=[{"Name": "TableName", "Value": "BlocksDB-default"}],
        StartTime=dt.datetime.fromtimestamp(a - 60, dt.timezone.utc),
        EndTime=dt.datetime.fromtimestamp(b + 60, dt.timezone.utc), Period=60, Statistics=["Sum"])
    rru = sum(p["Sum"] for p in r["Datapoints"])
    return dict(ddb_rru_q=rru / cell["n"], ddb_usd_q=rru * DDB_RRU / cell["n"])


def s3v_fee(size):
    nvec = 1_000_000 if size == "1m" else 10_000_000
    cost, prev = 0.0, 0
    for upper, rate in S3V_TIERS:
        band = max(0, min(nvec, upper) - prev)
        cost += band * (S3V_VEC_BYTES + s3v_key_bytes(nvec)) / S3V_TB * rate
        prev = upper
        if nvec <= upper:
            break
    return dict(s3v_request_usd_q=S3V_REQ, s3v_data_usd_q=cost)


# ----------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="2026-10-ec2")
    ap.add_argument("--s3logs", default=os.path.expanduser("~/Developer/mst-datasets/s3logs"))
    ap.add_argument("--only", default="", help="comma list of exp names (b,c1,c2,c3)")
    args = ap.parse_args()

    cs = cells(args.tag)
    if args.only:
        cs = [c for c in cs if c["exp"] in args.only.split(",")]
    t_min = min(w[0] for c in cs for w in c["windows"]) - 60
    t_max = max(w[1] for c in cs for w in c["windows"]) + 60
    exclusive_intervals(cs, PAD_AFTER, "iv_report")
    exclusive_intervals(cs, S3_PAD_AFTER, "iv_s3")
    t0 = time.time()
    # Completeness: a record logged 10 min after a window, in any bucket, means that window's records have
    # been delivered (S3 log delivery is best effort and usually within an hour).
    ev, n_files, latest_log = load_s3logs(args.s3logs, t_min, t_max)
    print(f"s3 logs: {n_files} files parsed in {time.time() - t0:.0f}s; newest record "
          f"{dt.datetime.fromtimestamp(latest_log, dt.timezone.utc).isoformat()}")

    rows = []
    for c in cs:
        r = dict(exp=c["exp"], cell=c["cell"], adapter=c["adapter"], size=c["size"], n=c["n"],
                 p50_s=round(c["p50"], 4),
                 window_utc=[dt.datetime.fromtimestamp(c["windows"][0][0], dt.timezone.utc).isoformat(),
                             dt.datetime.fromtimestamp(c["windows"][-1][1], dt.timezone.utc).isoformat()])
        r.update(level1(c))
        r.update(s3_requests(c, ev))
        r["s3_logs_complete"] = latest_log > c["windows"][-1][1] + 600
        r.update(ddb_reads(c))
        if c["adapter"] == "s3vectors":
            r.update(s3v_fee(c["size"]))
        else:
            r.update(s3v_request_usd_q=0.0, s3v_data_usd_q=0.0)
        r["l2_usd_q"] = (r["l1_usd_q"] + r["s3_usd_q"] + r["ddb_usd_q"]
                         + r["s3v_request_usd_q"] + r["s3v_data_usd_q"])
        rows.append(r)
        print(f"{r['exp']:3} {r['cell']:24} n={r['n']:5} L1=${r['l1_usd_q']:.7f} "
              f"GB-s/q={r['gb_s_q']:.2f} inv/q={r['inv_q']:.1f} init={r['n_init']} "
              f"S3 get/q={r['s3_get_q']:.0f} put+list/q={r['s3_put_list_q']:.0f} "
              f"ddb/q={r['ddb_rru_q']:.2f} L2=${r['l2_usd_q']:.7f}")

    od = os.path.join(HERE, f"results_{args.tag}")
    os.makedirs(od, exist_ok=True)
    meta = dict(tag=args.tag, generated_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
                prices=dict(x86_gbs=X86, arm_gbs=ARM, lambda_req=LREQ, s3_get=S3_GET, s3_put=S3_PUT,
                            ddb_rru=DDB_RRU, s3v_req=S3V_REQ, s3v_tiers=S3V_TIERS[:2] + [[None, 0.0004]],
                            s3v_vec_bytes=S3V_VEC_BYTES),
                pad_s=[PAD_BEFORE, PAD_AFTER], s3_pad_after_s=S3_PAD_AFTER, windows="exclusive", s3_log_files=n_files)
    suffix = f"_{args.only.replace(',', '_')}" if args.only else ""
    with open(os.path.join(od, f"cost_levels{suffix}.json"), "w") as f:
        json.dump(dict(meta=meta, cells=rows), f, indent=1)
    with open(os.path.join(od, f"cost_levels{suffix}.md"), "w") as f:
        f.write(f"# Cost levels per cell ({args.tag})\n\nLevel 1 = CloudWatch billed duration x memory x "
                "rate + request fee. Level 2 = Level 1 + S3 requests (access logs) + DynamoDB reads + "
                "S3 Vectors request and data fees. Per query.\n\n"
                "| exp | cell | n | p50 s | inv/q | GB-s/q | L1 $/q | S3 GET/q | S3 PUT+LIST/q | S3 $/q | "
                "DDB RRU/q | S3V $/q | L2 $/q | logs ok |\n|" + "---|" * 14 + "\n")
        for r in rows:
            f.write(f"| {r['exp']} | {r['cell']} | {r['n']} | {r['p50_s']:.3g} | {r['inv_q']:.1f} | "
                    f"{r['gb_s_q']:.2f} | {r['l1_usd_q']:.7f} | {r['s3_get_q']:.0f} | "
                    f"{r['s3_put_list_q']:.0f} | {r['s3_usd_q']:.7f} | {r['ddb_rru_q']:.2f} | "
                    f"{r['s3v_request_usd_q'] + r['s3v_data_usd_q']:.7f} | {r['l2_usd_q']:.7f} | "
                    f"{'yes' if r['s3_logs_complete'] else 'NO'} |\n")
    print(f"wrote {od}/cost_levels{suffix}.(json|md)")


if __name__ == "__main__":
    main()
