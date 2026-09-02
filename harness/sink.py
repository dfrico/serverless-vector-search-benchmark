"""
JSONL sink for the serverless vector-DB benchmark harness (day 5).

Consumes the async iterable of `OpResult` records the workload generator yields
and persists them as newline-delimited JSON, one record per line, alongside a
run manifest capturing everything needed to interpret and reproduce the run.

Backend-neutral: the sink imports NO concrete adapter. It reads `client.name`
and `client.capabilities()` through the abstract `BenchmarkVectorClient`
interface, so it works for any adapter (Blocks, S3 Vectors, LanceDB). Adapter-
specific timing interpretation is NOT done here — the raw `adapter_timings` dict
is persisted opaquely and parsed later (see timings_parsers.py + aggregate.py).

Filename convention (in `output_dir`, default harness/results/):
  run_{timestamp}_{pattern}.jsonl           — one OpResult per line
  run_{timestamp}_{pattern}.manifest.json   — the run manifest

`timestamp` is an integer epoch second, injectable for deterministic tests.
"""
from __future__ import annotations

import dataclasses
import importlib.metadata
import json
import os
import platform
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import AsyncIterable, Optional

# Interface + record types, imported from the consolidated harness package.
_HERE = os.path.dirname(os.path.abspath(__file__))

from harness.client import BenchmarkVectorClient, ClientCapabilities  # noqa: E402
from harness.workload import OpResult, WorkloadConfig                 # noqa: E402

DEFAULT_RESULTS_DIR = os.path.join(_HERE, "results")


@dataclass(frozen=True)
class SinkPaths:
    """Absolute paths the sink wrote, returned by `consume`."""
    jsonl: str
    manifest: str
    n_records: int


def _git_sha(cwd: str) -> str:
    """HEAD SHA of the repo containing `cwd`, or 'unknown' if not a git repo."""
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=cwd, capture_output=True, text=True, timeout=5,
        )
        if out.returncode == 0:
            return out.stdout.strip()
    except Exception:  # noqa: BLE001 — any git/subprocess failure -> unknown
        pass
    return "unknown"


def _capabilities_to_dict(caps: ClientCapabilities) -> dict:
    """asdict, but coerce the ingest_modes set to a sorted list (JSON has no set)."""
    d = dataclasses.asdict(caps)
    d["ingest_modes"] = sorted(caps.ingest_modes)
    return d


# Packages whose versions pin the harness's numerical/orchestration behaviour.
_ENV_PACKAGES = ("lithops", "faiss-cpu", "numpy", "boto3", "orjson")


def _env_fingerprint() -> dict:
    """
    Installed versions of the behaviour-defining packages, keyed by a normalized
    name (hyphen -> underscore, e.g. faiss-cpu -> faiss_cpu). A package that isn't
    installed is "absent" rather than raising, so a partial env still produces a
    manifest.
    """
    out = {}
    for pkg in _ENV_PACKAGES:
        key = pkg.replace("-", "_")
        try:
            out[key] = importlib.metadata.version(pkg)
        except importlib.metadata.PackageNotFoundError:
            out[key] = "absent"
    return out


def _effective_concurrency_default(client: BenchmarkVectorClient, config: WorkloadConfig) -> int:
    """
    Best-effort effective concurrency for the manifest when the caller doesn't
    pass one explicitly: min(configured, query-client max_safe_concurrency).

    This is EXACT for the query-only patterns (constant/burst). For the mixed
    pattern with a distinct insert client the generator additionally mins in the
    insert client's cap, so callers of mixed runs SHOULD pass the generator-
    computed value explicitly (see JSONLSink `effective_concurrency` arg).
    """
    return min(config.max_in_flight, client.capabilities().max_safe_concurrency)


class JSONLSink:
    """
    Persist a workload run to JSONL + a manifest.

    Args:
        client: the query client (any BenchmarkVectorClient). Read for `name`,
            `capabilities()`, `backend_version()`, `dataset_fingerprint()`; never
            mutated.
        config: the WorkloadConfig driving the run (persisted verbatim).
        dataset_name: logical dataset the run targeted (manifest field).
        output_dir: directory for the two output files (created if missing).
        generator: the WorkloadGenerator driving the run. If given, the sink reads
            the TRUE effective concurrency from `generator.effective_concurrency(config)`
            (exact for every pattern, including mixed) — the preferred source.
        effective_concurrency: explicit override, used only if `generator` is None.
            If both are None, falls back to min(config.max_in_flight, query cap)
            (exact for query-only patterns; see helper docstring).
        timestamp: epoch seconds for the filename + manifest; None -> time.time().
            Injectable so tests get deterministic filenames.
    """

    def __init__(
        self,
        client: BenchmarkVectorClient,
        config: WorkloadConfig,
        *,
        dataset_name: str,
        output_dir: str = DEFAULT_RESULTS_DIR,
        generator: Optional[object] = None,
        effective_concurrency: Optional[int] = None,
        timestamp: Optional[int] = None,
    ):
        self.client = client
        self.config = config
        self.dataset_name = dataset_name
        self.output_dir = output_dir
        # Effective concurrency, in priority order: generator's public method
        # (exact for all patterns) -> explicit override -> query-only default.
        if generator is not None:
            self.effective_concurrency = generator.effective_concurrency(config)
        elif effective_concurrency is not None:
            self.effective_concurrency = effective_concurrency
        else:
            self.effective_concurrency = _effective_concurrency_default(client, config)
        self.timestamp = int(timestamp if timestamp is not None else time.time())

        stem = f"run_{self.timestamp}_{config.pattern}"
        self.jsonl_path = os.path.join(output_dir, f"{stem}.jsonl")
        self.manifest_path = os.path.join(output_dir, f"{stem}.manifest.json")

    def build_manifest(self) -> dict:
        """Assemble the run manifest dict (also written to disk by `consume`)."""
        return {
            "schema": "harness.run_manifest.v2",
            "git_sha": _git_sha(_HERE),
            "timestamp": self.timestamp,
            "timestamp_iso": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(self.timestamp)) + "Z",
            "adapter": self.client.name,
            "backend_version": self.client.backend_version(),
            "dataset_name": self.dataset_name,
            "dataset_fingerprint": self.client.dataset_fingerprint(self.dataset_name),
            "config": dataclasses.asdict(self.config),
            "seed": self.config.seed,
            "effective_concurrency": self.effective_concurrency,
            "capabilities": _capabilities_to_dict(self.client.capabilities()),
            "env": _env_fingerprint(),
            "hostname": socket.gethostname(),
            "python_version": platform.python_version(),
        }

    async def consume(self, results: AsyncIterable[OpResult]) -> SinkPaths:
        """
        Write the manifest, then stream each OpResult to one JSONL line as it
        arrives (results are flushed incrementally so a killed run keeps the
        records it already produced). Returns the paths + record count.
        """
        os.makedirs(self.output_dir, exist_ok=True)

        with open(self.manifest_path, "w") as mf:
            json.dump(self.build_manifest(), mf, indent=2, sort_keys=True)

        n = 0
        with open(self.jsonl_path, "w") as jf:
            async for r in results:
                # default=str: any stray non-JSON-native timing value degrades to
                # its string form rather than crashing the whole run.
                jf.write(json.dumps(dataclasses.asdict(r), default=str) + "\n")
                jf.flush()
                n += 1

        return SinkPaths(jsonl=self.jsonl_path, manifest=self.manifest_path, n_records=n)
