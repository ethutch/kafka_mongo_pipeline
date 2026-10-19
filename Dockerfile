FROM python:3.13-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Build stage
FROM base AS builder

ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.0.1-dev

# 1. Define the build argument with a default value of 'false'
ARG INSTALL_DEV=false

COPY pyproject.toml README.md ./
COPY src/ ./src/
COPY tests/ ./tests/

RUN pip install --upgrade pip && \
    if [ "$INSTALL_DEV" = "true" ]; then \
        pip install ".[dev]"; \
    else \
        pip install .; \
    fi

# Test stage
FROM builder AS test

ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    KAFKA_TOPICS=customer-events \
    KAFKA_AUTO_OFFSET_RESET=earliest \
    KAFKA_SECURITY_PROTOCOL=PLAINTEXT \
    KAFKA_SESSION_TIMEOUT_MS=45000 \
    KAFKA_MAX_POLL_INTERVAL_MS=300000 \
    MONGODB_URI=mongodb://localhost:27017 \
    MONGODB_DATABASE=pytest  \
    MONGODB_SERVER_SELECTION_TIMEOUT_MS=1000 \
    MONGODB_CONNECT_TIMEOUT_MS=1000 \
    DEAD_LETTER_TOPIC=northwind-elt-events-dlq \
    SUPPORTED_CUSTOMER_EVENTS=4,9 \
    SUPPORTED_ORDER_EVENTS=7

RUN pytest -vv --tb=long

# Runtime stage
FROM base AS runtime

COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

RUN adduser --disabled-password --gecos "" appuser

USER appuser

CMD ["python", "-m", "kafka_mongo_pipeline.main"]