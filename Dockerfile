FROM python:3.13-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# ── build stage ───────────────────────────────────────────────────────
FROM base AS builder

ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.0.1-dev

COPY pyproject.toml ./
COPY kafka_mongo_pipeline/ ./kafka_mongo_pipeline/
RUN pip install --upgrade pip && pip install .

# ── runtime stage ─────────────────────────────────────────────────────
FROM base AS runtime

COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY kafka_mongo_pipeline/ ./kafka_mongo_pipeline/

RUN adduser --disabled-password --gecos "" appuser
USER appuser

CMD ["python", "-m", "kafka_mongo_pipeline.main"]
