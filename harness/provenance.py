"""
Code provenance for run manifests (added 2026-10-16).

The earlier manifests recorded only `git rev-parse HEAD`, which pointed at the
commit *before* an experiment's own code was committed, so the code that ran was
not pinned. `code_provenance()` also records whether the measured code differs
from HEAD and a hash of that difference; `write_patch()` saves the difference next
to the results so the exact code can be rebuilt as HEAD + patch.

Scope: only the code that produces numbers (harness/, experiments/, scripts/),
relative to the thesis folder. The git root is the whole degree folder, whose
unrelated changes must not make every run look dirty.
"""
from __future__ import annotations

import hashlib
import os
import subprocess
from typing import Dict, List, Optional

_HERE = os.path.dirname(os.path.abspath(__file__))
THESIS_ROOT = os.path.dirname(_HERE)
CODE_PATHS = ("harness", "experiments", "scripts")
BLOCKS_REPO = os.path.join(THESIS_ROOT, "references", "blocks-db-wip")
_CODE_EXT = (".py", ".sh", ".txt", ".json", ".toml", ".cfg", ".yaml", ".yml")


def _git(args: List[str], cwd: str) -> Optional[str]:
    try:
        out = subprocess.run(["git", *args], cwd=cwd, capture_output=True,
                             text=True, timeout=20)
        return out.stdout if out.returncode == 0 else None
    except Exception:  # noqa: BLE001
        return None


def _untracked_code() -> List[str]:
    out = _git(["ls-files", "--others", "--exclude-standard", "--", *CODE_PATHS],
               THESIS_ROOT) or ""
    return sorted(p for p in out.splitlines()
                  if p.endswith(_CODE_EXT) and "/results" not in p)


def _diff_text() -> str:
    """Tracked changes vs HEAD plus the full text of untracked code files."""
    parts = [_git(["diff", "HEAD", "--binary", "--", *CODE_PATHS], THESIS_ROOT) or ""]
    for rel in _untracked_code():
        try:
            with open(os.path.join(THESIS_ROOT, rel), encoding="utf-8", errors="replace") as f:
                parts.append(f"\n=== untracked: {rel} ===\n{f.read()}")
        except OSError:
            pass
    return "".join(parts)


BUNDLE_FILE = os.path.join(THESIS_ROOT, "CODE_BUNDLE.json")


def code_provenance() -> Dict[str, object]:
    """Commit, dirty flag and diff hash for the harness code, plus the Blocks commit.

    On a client without the thesis git repo (the EC2 benchmark client, 2026-10-16)
    the code arrives as a bundle; CODE_BUNDLE.json, written on the laptop when the
    bundle was made, carries the same fields, and the bundle includes the patch."""
    head = _git(["rev-parse", "HEAD"], THESIS_ROOT)
    if head is None and os.path.exists(BUNDLE_FILE):
        import json
        with open(BUNDLE_FILE) as f:
            prov = json.load(f)
        prov["source"] = "bundle"
        prov["blocks_commit"] = (_git(["rev-parse", "HEAD"], BLOCKS_REPO) or "absent").strip()
        prov["blocks_dirty"] = bool((_git(["status", "--porcelain", "--untracked-files=no"],
                                          BLOCKS_REPO) or "").strip())
        return prov
    head = (head or "unknown").strip()
    diff = _diff_text()
    tracked = _git(["diff", "--relative", "--name-only", "HEAD", "--", *CODE_PATHS],
                   THESIS_ROOT) or ""
    changed = sorted({p for p in tracked.splitlines()
                      if p.endswith(_CODE_EXT) and "/results" not in p}
                     | set(_untracked_code()))
    blocks = (_git(["rev-parse", "HEAD"], BLOCKS_REPO) or "absent").strip()
    blocks_dirty = bool((_git(["status", "--porcelain", "--untracked-files=no"], BLOCKS_REPO) or "").strip())
    return {
        "git_sha": head,
        "dirty": bool(diff.strip()),
        "diff_sha256": hashlib.sha256(diff.encode()).hexdigest() if diff.strip() else None,
        "changed_files": changed,
        "blocks_commit": blocks,
        "blocks_dirty": blocks_dirty,
    }


def write_patch(dest_path: str) -> Optional[str]:
    """Write the uncommitted code diff to `dest_path` if the tree is dirty.
    Returns the path written, or None when the code matches HEAD. On a bundle
    client the patch shipped with the bundle is copied instead."""
    if _git(["rev-parse", "HEAD"], THESIS_ROOT) is None and os.path.exists(BUNDLE_FILE):
        src = os.path.join(THESIS_ROOT, "CODE_BUNDLE.patch")
        if not os.path.exists(src):
            return None
        import shutil
        os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
        shutil.copyfile(src, dest_path)
        return dest_path
    diff = _diff_text()
    if not diff.strip():
        return None
    os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
    with open(dest_path, "w") as f:
        f.write(diff)
    return dest_path
