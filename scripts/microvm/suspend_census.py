"""Every 10M MicroVM that met an idle suspend, with its image build and the outcome (findings 31-32).

For each MicroVM: the image (and its build date), the chip it ran on (from our result records), the idle suspends
that worked (a server "hook suspend" line followed by a "hook resume" line), and whether AWS terminated it with the
stateReason "Suspend lifecycle hook timed out" (one failed suspend) or "Internal service error." (25 Oct, E2 on the
rebuilt image: after a resume whose hook the server had answered). A failed suspend may or may not leave a hook line
in the log (22 Oct: none; 24 Oct: logged with the reply failing), so failures are counted from the stateReason.
Reads CloudWatch Logs and GetMicrovm only (no paid calls beyond the API requests).

    AWS_PROFILE=thesis harness/.venv/bin/python scripts/microvm/suspend_census.py
Writes scripts/microvm/results/suspend_census_10m.json.
"""
import datetime as dt
import json
import os

import boto3

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUT = os.path.join(ROOT, "scripts", "microvm", "results", "suspend_census_10m.json")
E2D = "experiments/experiment_c2/results_2026-10-mvm"
HP = "scripts/microvm/results/hook_probe"
# (microvm id, source record holding its chip, run label)
RUNS = [
    ("microvm-8d6faeef-97bb-3d95-9e1f-eccb4fba5266", f"{E2D}/e2_lancedb_microvm_deep_10m.manifest.json", "E2 resume 22 Oct"),
    ("microvm-8824e2ea-8bd8-37e8-836f-0fad2ec0e51c", f"{E2D}/e2_lancedb_microvm_deep_10m_resume.jsonl", "E2 resume 24 Oct #1"),
    ("microvm-ad7be41a-872a-3021-9139-5d02c008de3a", f"{E2D}/e2_lancedb_microvm_deep_10m_resume2.jsonl", "E2 resume 24 Oct #2"),
]
for f in sorted(os.listdir(os.path.join(ROOT, HP))):
    if f.startswith("hook_probe_deep_10m_") and f.endswith(".json"):
        d = json.load(open(os.path.join(ROOT, HP, f)))
        if d.get("microvm_id") and d.get("startup_query"):
            RUNS.append((d["microvm_id"], f"{HP}/{f}", f"hook probe {d['tag']}"))
for f in sorted(os.listdir(os.path.join(ROOT, E2D))):
    if f.startswith("e2_lancedb_microvm_deep_10m_resume_fresh") and f.endswith(".jsonl"):
        ids = {json.loads(l)["microvm_id"] for l in open(os.path.join(ROOT, E2D, f))}
        RUNS += [(i, f"{E2D}/{f}", "E2 resume, fresh image") for i in sorted(ids)]
E2V6 = "experiments/experiment_c2/results_2026-10-mvm6"  # 26 Oct E2 re-run on the v6 image
if os.path.exists(os.path.join(ROOT, E2V6, "e2_lancedb_microvm_deep_10m_resume.jsonl")):
    ids = {json.loads(l)["microvm_id"] for l in open(os.path.join(ROOT, E2V6, "e2_lancedb_microvm_deep_10m_resume.jsonl"))}
    RUNS += [(i, f"{E2V6}/e2_lancedb_microvm_deep_10m_resume.jsonl", "E2 resume, v6 image") for i in sorted(ids)]


def chip(mid, path):
    txt = open(os.path.join(ROOT, path)).read()
    if path.endswith(".jsonl"):
        txt = "\n".join(l for l in txt.splitlines() if mid in l)
    parts = sorted({p for p in ("0xd40", "0xd4f") if p in txt})
    return {"0xd40": "Graviton 3", "0xd4f": "Graviton 4"}.get(parts[0]) if len(parts) == 1 else None


def utc(t):
    """ISO time in UTC (boto3 returns local-zone datetimes; the 22 Oct row is a string from the manifest)."""
    if t in (None, "None"):
        return None
    t = dt.datetime.fromisoformat(t) if isinstance(t, str) else t
    return t.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def main():
    mv = boto3.client("lambda-microvms", region_name="us-east-1")
    logs = boto3.client("logs", region_name="us-east-1")
    acct = boto3.client("sts").get_caller_identity()["Account"]
    built = {}
    rows = []
    # Rows from the previous census are kept for MicroVMs that have since aged out of the API (26 Oct re-run).
    prev = {r["microvm_id"]: r for r in json.load(open(OUT))["rows"]} if os.path.exists(OUT) else {}
    built.update({r["image"]: r["image_built"] for r in prev.values()})  # images deleted since (26 Oct)
    for mid, src, label in RUNS:
        try:
            g = mv.get_microvm(microvmIdentifier=mid)
            inferred = False
        except mv.exceptions.ResourceNotFoundException:
            if mid in prev and not prev[mid]["inferred"]:
                rows.append(prev[mid])
                print(f"{label:26} kept from the previous census (aged out of the API)")
                continue
            # 22 Oct: aged out of the API by 25 Oct. The manifest has its lifetime (15:34:51-15:37:06, 135 s = run +
            # 60 s idle + the hook timeout) and the log has no suspend hook, as in the confirmed timeouts; the reason
            # was not recorded, so this failure is inferred and counted separately.
            t = json.load(open(os.path.join(ROOT, src)))["resume_transitions"]
            g = {"imageArn": "x:thesis-lancedb-mvm-deep10m", "startedAt": dt.datetime.fromisoformat(t["started_at"]),
                 "terminatedAt": t["terminated_at"], "stateReason": "Suspend lifecycle hook timed out (inferred)"}
            inferred = True
        image = g["imageArn"].rsplit(":", 1)[1]
        if image not in built:
            built[image] = str(mv.get_microvm_image(
                imageIdentifier=f"arn:aws:lambda:us-east-1:{acct}:microvm-image:{image}")["createdAt"]
                .astimezone(dt.timezone.utc))[:10]
        group = f"/aws/lambda-microvms/{image}"
        hooks = []
        for page in logs.get_paginator("filter_log_events").paginate(
                logGroupName=group, logStreamNamePrefix=g["startedAt"].astimezone(dt.timezone.utc).strftime("%Y/%m/%d"), filterPattern='"hook"'):
            hooks += [(e["timestamp"], e["message"].split()[1]) for e in page["events"]
                      if e["logStreamName"].endswith(mid) and e["message"].startswith("hook ")]
        hooks.sort()
        ok = sum(1 for a, b in zip(hooks, hooks[1:]) if a[1] == "suspend" and b[1] == "resume")
        reason = g.get("stateReason") or ""
        failed = int(reason.startswith("Suspend lifecycle hook timed out"))
        internal = int(reason.startswith("Internal service error"))  # 25 Oct: terminated right after a resume
        rows.append({"microvm_id": mid, "run": label, "image": image, "image_built": built[image],
                     "chip": chip(mid, src), "suspends_ok": ok, "suspend_failed": failed, "inferred": inferred,
                     "internal_error_termination": internal,
                     "state_reason": reason,
                     "started": utc(g["startedAt"]), "terminated": utc(g.get("terminatedAt")), "record": src})
        print(f"{label:26} {image:34} built {built[image]} {rows[-1]['chip']}: ok {ok}, failed {failed}")
    by = {}
    for r in rows:
        b = by.setdefault(r["image_built"], {"microvms": 0, "ok": 0, "failed": 0, "failed_inferred": 0,
                                                "internal_error": 0, "images": set()})
        b["microvms"] += 1
        b["failed_inferred"] += r["suspend_failed"] * r["inferred"]
        b["internal_error"] += r["internal_error_termination"]
        b["ok"] += r["suspends_ok"]
        b["failed"] += r["suspend_failed"]
        b["images"].add(r["image"])
    summary = {k: {**v, "images": sorted(v["images"]), "attempts": v["ok"] + v["failed"]} for k, v in by.items()}
    json.dump({"rows": rows, "by_build_date": summary}, open(OUT, "w"), indent=1)
    print(json.dumps(summary, indent=1), "\n->", OUT)


if __name__ == "__main__":
    main()
