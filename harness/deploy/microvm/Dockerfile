# LanceDB on Lambda MicroVMs (fix list F39). Same container base and pins as the
# LanceDB-on-Lambda image (harness/deploy/Dockerfile), so only where the table lives
# changes. MicroVMs are arm64 only; the Lambda base image is multi-arch.
FROM public.ecr.aws/lambda/python:3.11

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir "numpy==1.26.4" "pyarrow==17.0.0" "lancedb==0.33.0" boto3

COPY server.py /app/server.py
WORKDIR /app
EXPOSE 8080

# The Lambda base image's entrypoint is the runtime interface client; replace it.
ENTRYPOINT ["python3", "-u", "/app/server.py"]
