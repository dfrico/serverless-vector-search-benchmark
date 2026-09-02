# LanceDB-on-Lambda container image — the naive-serverless baseline (day 8).
#
# Base: the official AWS Lambda Python 3.11 runtime image. It ships the Lambda
# Runtime Interface Client, so the function needs only our handler + LanceDB.
# Built for arm64/Graviton (see build_and_deploy.py --arch); the base image is
# multi-arch, so `docker build --platform linux/arm64` selects the arm64 variant.
FROM public.ecr.aws/lambda/python:3.11

# LanceDB (vector DB). Pinned so the image is reproducible and backend_version
# records an exact version. 0.33.0 is the newest release with a linux-aarch64
# (Graviton/arm64) cp311 wheel. numpy and pyarrow are pinned to versions that
# publish arm64/cp311 wheels too — the Lambda base image has NO C compiler, so an
# unpinned numpy (which resolves to a too-new source-only release) fails to build.
# lancedb bundles the Rust object-store, so s3:// URIs work with no extra deps —
# the Lambda execution role's credentials are picked up from the environment.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir "numpy==1.26.4" "pyarrow==17.0.0" "lancedb==0.33.0"

# The handler dispatches {op: insert|query|delete|drop|ping}. LAMBDA_TASK_ROOT is
# the base image's working dir and is already on sys.path.
COPY lambda_handler.py ${LAMBDA_TASK_ROOT}/

# module.function the Lambda runtime invokes per request.
CMD ["lambda_handler.handler"]
