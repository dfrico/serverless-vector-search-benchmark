"""Read one UTC day's Lambda/MicroVM usage from Cost Explorer and set it beside test 2's prediction.

Usage-type names for MicroVMs are not documented; the script lists every usage type of the day, grouped by
service, and marks the ones that look like MicroVM compute or snapshot usage. It then derives, for the
test-2 day: snapshot GB written per suspend, GB read per launch/resume, billed vCPU-s and GB-s against the
running time x baseline, and the excess (burst) against the guest's measured busy CPU above 1 vCPU.
Cost Explorer API calls cost $0.01 each; this makes two.

    AWS_PROFILE=thesis harness/.venv/bin/python scripts/microvm/ce_readout.py 2026-10-19 scripts/microvm/results/test2_<utc>.json
Writes scripts/microvm/results/ce_<day>.json.
"""
import datetime as dt
import json
import os
import sys

import boto3

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
KEYS = ("microvm", "micro-vm", "snapshot", "vcpu", "burst")


def main(day, test2_path=None):
    end = (dt.date.fromisoformat(day) + dt.timedelta(days=1)).isoformat()
    ce = boto3.client("ce", region_name="us-east-1")
    r = ce.get_cost_and_usage(TimePeriod={"Start": day, "End": end}, Granularity="DAILY",
                              Metrics=["UsageQuantity", "UnblendedCost"],
                              GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"},
                                       {"Type": "DIMENSION", "Key": "USAGE_TYPE"}])
    rows = []
    for g in r["ResultsByTime"][0]["Groups"]:
        svc, ut = g["Keys"]
        m = g["Metrics"]
        rows.append({"service": svc, "usage_type": ut, "quantity": float(m["UsageQuantity"]["Amount"]),
                     "unit": m["UsageQuantity"].get("Unit"), "cost": float(m["UnblendedCost"]["Amount"]),
                     "microvm_like": any(k in ut.lower() for k in KEYS) or "microvm" in svc.lower()})
    out = {"day": day, "rows": rows, "estimated": r["ResultsByTime"][0].get("Estimated")}
    print(f"{day} (estimated={out['estimated']})")
    for x in sorted(rows, key=lambda x: (not x["microvm_like"], x["service"], x["usage_type"])):
        print(f"  {'*' if x['microvm_like'] else ' '} {x['service'][:28]:28} {x['usage_type'][:44]:44} "
              f"{x['quantity']:>14.4f} {x['unit'] or '':16} ${x['cost']:.4f}")
    if test2_path:
        t2 = json.load(open(test2_path))
        p = t2.get("prediction", {})
        out["test2_prediction"] = p
        out["test2_query_window"] = t2.get("query_window")
        print("\ntest 2 prediction:", json.dumps(p))
        print("Divide the snapshot write GB by", p.get("suspends"), "and the read GB by",
              (p.get("launches") or 0) + (p.get("resumes") or 0), "(launches + resumes);")
        print("compare billed vCPU-s with running_s x 1 and the excess with burst_vcpu_s_predicted.")
    path = os.path.join(ROOT, "scripts", "microvm", "results", f"ce_{day}.json")
    json.dump(out, open(path, "w"), indent=1)
    print("wrote", path)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)
