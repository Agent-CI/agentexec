# agentexec-worker base image
#
# This image provides the agentexec framework for running background workers.
# Users extend this image with their own task definitions.
#
# Usage:
#   FROM ghcr.io/agent-ci/agentexec-worker:latest
#   COPY ./src /app/src
#   ENV AGENTEXEC_WORKER_MODULE=src.worker
#
# Required environment variables at runtime:
#   DATABASE_URL - Database connection URL (e.g., postgresql://...)
#   REDIS_URL - Redis connection URL (e.g., redis://...)
#   AGENTEXEC_WORKER_MODULE - Python module path with pool definition
#
# Optional environment variables:
#   OPENAI_API_KEY - Required if using OpenAI agents
#   AGENTEXEC_NUM_WORKERS - Number of worker processes (default: 4)
#   AGENTEXEC_QUEUE_NAME - Redis queue name (default: agentexec_tasks)

FROM python:3.11-slim AS base

# Prevent Python from writing pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # Ensure Python can find modules in /app
    PYTHONPATH=/app

WORKDIR /app

# Install system dependencies for database drivers
RUN apt-get update && apt-get install -y --no-install-recommends \
    # PostgreSQL client library (for psycopg2)
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Build stage for compiling dependencies
FROM base AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    # Build tools
    gcc \
    # PostgreSQL dev headers (for psycopg2)
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install pip dependencies
RUN pip install --no-cache-dir --upgrade pip

# Install agentexec with common database drivers
# Users can extend with additional drivers as needed
RUN pip install --no-cache-dir \
    agentexec \
    psycopg2-binary \
    # For async PostgreSQL (optional, commonly used)
    asyncpg

# Final stage
FROM base AS runtime

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy entrypoint script
COPY entrypoint.py /app/entrypoint.py

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash worker
USER worker

# Health check - verify Python and agentexec are available
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import agentexec" || exit 1

# Default command runs the worker entrypoint
ENTRYPOINT ["python", "/app/entrypoint.py"]
