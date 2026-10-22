"""Snapshot Lambda concurrency/throttle metrics for the August experiments (read-only)."""
import json, sys, datetime as dt
import boto3
s = boto3.Session(profile_name="thesis", region_name="us-east-1")
cw = s.client("cloudwatch")
start = dt.datetime(2026, 8, 20, tzinfo=dt.timezone.utc)
end = dt.datetime(2026, 9, 2, tzinfo=dt.timezone.utc)
fns = ["lithops-worker-rb26-363-11170a3116", "lithops-worker-rb26-363-600f82b870",
       "thesis-lancedb-lambda", "blocksdb-autoindexer-default"]
queries, meta = [], {}
def add(name, metric, stat, dims, period):
    qid = f"q{len(queries)}"
    meta[qid] = {"name": name, "metric": metric, "stat": stat, "period": period}
    queries.append({"Id": qid, "MetricStat": {"Metric": {"Namespace": "AWS/Lambda",
        "MetricName": metric, "Dimensions": dims}, "Period": period, "Stat": stat},
        "ReturnData": True})
for period in (300, 3600):
    add("ACCOUNT", "ConcurrentExecutions", "Maximum", [], period)
    add("ACCOUNT", "Throttles", "Sum", [], period)
    add("ACCOUNT", "Invocations", "Sum", [], period)
    for f in fns:
        d = [{"Name": "FunctionName", "Value": f}]
        for m, st in (("ConcurrentExecutions", "Maximum"), ("Throttles", "Sum"),
                      ("Invocations", "Sum"), ("Duration", "Maximum")):
            add(f, m, st, d, period)
out = {"captured_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
       "window": [start.isoformat(), end.isoformat()], "series": []}
for i in range(0, len(queries), 100):
    tok = None
    while True:
        kw = dict(MetricDataQueries=queries[i:i+100], StartTime=start, EndTime=end, ScanBy="TimestampAscending")
        if tok: kw["NextToken"] = tok
        r = cw.get_metric_data(**kw)
        for res in r["MetricDataResults"]:
            m = meta[res["Id"]]
            ser = next((x for x in out["series"] if x["id"] == res["Id"]), None)
            if ser is None:
                ser = dict(m, id=res["Id"], points=[]); out["series"].append(ser)
            ser["points"] += [[t.isoformat(), v] for t, v in zip(res["Timestamps"], res["Values"])]
        tok = r.get("NextToken")
        if not tok: break
json.dump(out, open(sys.argv[1], "w"), indent=1)
for ser in out["series"]:
    if ser["period"] == 300 and ser["points"]:
        vals = [v for _, v in ser["points"]]
        print(f'{ser["name"][:38]:38} {ser["metric"]:22} {ser["stat"]:7} n={len(vals):4} max={max(vals):g} sum={sum(vals):g}')
