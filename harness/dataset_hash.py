"""
Dataset fingerprinting for reproducibility (harness day 6).

A benchmark run's results are only interpretable against the exact corpus bytes
they were produced from. The datasets are git-ignored and reassembled per machine
(`cat dataseta* > vectors_deep_100k.csv`), so "same filename" does NOT guarantee
"same bytes". This module computes a stable content fingerprint the sink stamps
into the run manifest.

SHA-256 of the raw file bytes is the fingerprint: order-sensitive and
content-exact, which is what we want (a reordered or re-sampled corpus is a
different dataset for benchmarking purposes). Hashing is streamed in chunks so a
110 MB CSV doesn't have to be held in memory.
"""
from __future__ import annotations

import hashlib
import os
from typing import Optional

_CHUNK = 1 << 20  # 1 MiB streaming read


def fingerprint_csv(path: str) -> dict:
    """
    Content fingerprint of a dataset CSV.

    Returns:
        {
          "content_sha256": <hex str>,
          "line_count": <int>,       # newline count (rows, incl. any header)
          "byte_size": <int>,
          "source_path": <abs path str>,
        }

    Raises:
        FileNotFoundError: if `path` does not exist (a missing dataset must be
            loud — a run against no corpus is meaningless, not "unknown").
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"dataset CSV not found: {path}")

    h = hashlib.sha256()
    line_count = 0
    byte_size = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(_CHUNK)
            if not chunk:
                break
            h.update(chunk)
            line_count += chunk.count(b"\n")
            byte_size += len(chunk)

    return {
        "content_sha256": h.hexdigest(),
        "line_count": line_count,
        "byte_size": byte_size,
        "source_path": os.path.abspath(path),
    }


def unknown_fingerprint(source_path: Optional[str] = None) -> dict:
    """
    Fingerprint placeholder when the corpus path is not known to the adapter
    (e.g. querying a dataset already resident in the object store with no local
    CSV). The manifest still records the shape; the hash is null, not fabricated.
    """
    return {
        "content_sha256": None,
        "line_count": None,
        "byte_size": None,
        "source_path": os.path.abspath(source_path) if source_path else None,
    }
