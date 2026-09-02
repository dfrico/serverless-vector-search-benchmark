"""
Runtime patch: make the Lithops AWS Lambda invoker retry on throttling (429).

Why this exists (C3, 10M ramp). The account Lambda concurrency limit is 10, but a
10M Blocks build fans out num_workers=160 tasks and each query fans out 20 map
tasks. Lithops invokes Lambda asynchronously (X-Amz-Invocation-Type: Event), so
AWS queues accepted invokes and drains them within the concurrency limit. But
Lithops' invoke() RAISES on any non-202 status, including 429
CallerRateLimitExceeded. A single throttled invoke in a 160-way burst kills a task
and the whole job hangs waiting for a result that never comes (seen on 2026-08-23).

The fix is minimal and faithful to Lithops' own control flow: on 429 (or a 5xx),
re-sign and re-send the invoke with capped exponential backoff + full jitter until
AWS returns 202. Because the invokes are async, an accepted (202) invoke is then
run by AWS at up to 10 concurrent; the rest wait in the async queue. The build and
the per-query fan-out therefore complete in waves inside the cap of 10 — slower
wall time, identical result. This keeps the finding-22 Blocks sweet spot intact
(num_blocks=160, num_workers=160, blocks_per_task=8), so recall and the cost model
are unchanged; only wall latency grows. That growth is itself a 10M scaling
observation, recorded in the finding.

apply() is idempotent and guarded: if Lithops is not importable (the S3 Vectors /
LanceDB venv), it is a no-op. Call it before any Blocks build or query.
"""
from __future__ import annotations

import functools
import json
import random
import time


def apply(*, max_attempts: int = 80, base: float = 0.4, cap: float = 8.0,
          verbose: bool = True) -> bool:
    """Monkeypatch AWSLambdaBackend.invoke to retry on 429/5xx. Returns True if
    the patch is in place (or already was), False if Lithops is absent."""
    try:
        from lithops.serverless.backends.aws_lambda import aws_lambda as _m
    except Exception:
        return False

    Backend = _m.AWSLambdaBackend
    if getattr(Backend, "_c3_throttle_patched", False):
        return True

    AWSRequest = _m.AWSRequest
    SigV4Auth = _m.SigV4Auth

    _throttle_count = {"n": 0}

    def invoke(self, runtime_name, runtime_memory, payload):
        function_name = self._format_function_name(runtime_name, runtime_memory)
        url = f"https://{self.host}/2015-03-31/functions/{function_name}/invocations"
        body = json.dumps(payload, default=str)
        for attempt in range(max_attempts):
            # re-sign every attempt: a long backoff could outlive a SigV4 signature.
            headers = {"Host": self.host, "X-Amz-Invocation-Type": "Event",
                       "User-Agent": self.user_agent}
            request = AWSRequest(method="POST", url=url, data=body, headers=headers)
            SigV4Auth(self.credentials, "lambda", self.region).add_auth(request)
            try:
                r = self.session.send(request.prepare())
            except Exception:
                time.sleep(min(cap, base * (2 ** min(attempt, 6))) * (0.5 + random.random()))
                continue

            if r.status_code == 202:
                return r.headers["x-amzn-RequestId"]
            if r.status_code == 429 or r.status_code >= 500:
                _throttle_count["n"] += 1
                if verbose and _throttle_count["n"] % 50 == 0:
                    print(f"[c3-lithops-patch] throttled invokes so far: "
                          f"{_throttle_count['n']} (retrying with backoff)", flush=True)
                time.sleep(min(cap, base * (2 ** min(attempt, 6))) * (0.5 + random.random()))
                continue
            if r.status_code == 401:
                raise Exception("Unauthorized - Invalid API Key")
            if r.status_code == 404:
                raise Exception(f"Lithops Runtime: {runtime_name} not deployed")
            raise Exception(f"Error {r.status_code}: {r.text}")
        raise Exception(f"Lambda invoke still throttled (429) after {max_attempts} "
                        "attempts — concurrency cap too low for this fan-out")

    functools.update_wrapper(invoke, _m.AWSLambdaBackend.invoke)
    Backend.invoke = invoke
    Backend._c3_throttle_patched = True
    if verbose:
        print("[c3-lithops-patch] AWS Lambda invoker now retries on 429/5xx "
              "(async invokes drain within the concurrency cap).", flush=True)
    return True
