# --- Build stage: install the package into a virtualenv we can copy ---
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir .

# --- Runtime stage: minimal image, non-root user, healthcheck ---
FROM python:3.12-slim AS runtime

LABEL org.opencontainers.image.title="EuroStream" \
      org.opencontainers.image.description="GDPR-compliant real-time customer & order analytics platform" \
      org.opencontainers.image.source="https://github.com/your-org/eurostream" \
      org.opencontainers.image.licenses="MIT"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

# Non-root user; /app/data is the only writable path (warehouse, bus log,
# audit trail). Everything else in the container is read-only.
RUN groupadd --system eurostream \
    && useradd --system --gid eurostream --home /app eurostream \
    && mkdir -p /app/data \
    && chown -R eurostream:eurostream /app

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv

USER eurostream

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["uvicorn", "eurostream.api:app", "--host", "0.0.0.0", "--port", "8000"]
