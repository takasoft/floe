# syntax=docker/dockerfile:1

# ---- builder: install Floe + the postgres driver into an isolated venv ----
FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /build
# Copy only what the build backend (hatchling) needs to resolve + build the wheel.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[postgres]"

# ---- runtime: slim image with just the venv + example pipeline ----
FROM python:3.12-slim AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY --from=builder /opt/venv /opt/venv

# Run as an unprivileged user. With an S3 warehouse + Postgres catalog the
# container writes nothing to the local filesystem, so no writable mounts needed.
RUN useradd --create-home --uid 1000 floe
WORKDIR /app
COPY examples ./examples
USER floe

# Liveness probe: the CLI must import and run. Cheap and dependency-free.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["floe", "version"]

ENTRYPOINT ["floe"]
CMD ["--help"]
