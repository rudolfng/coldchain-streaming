# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    COLDCHAIN_ROOT=/app \
    HOME=/app

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY scripts ./scripts
RUN uv sync --frozen --no-dev

# Deja en cache el JAR del servicio de expansión de KafkaIO para que levantar el pipeline
# no dependa de una descarga de Maven adicional después de construir la imagen.
RUN /app/.venv/bin/python -c "from apache_beam.io.kafka import default_io_expansion_service; service=default_io_expansion_service(); address=service.__enter__(); print(address); service.__exit__(None, None, None)"

# UID 1000 = primer usuario de casi cualquier Linux: así puede escribir en el bind mount
# ./data sin chmod (Docker Desktop en Windows/Mac no aplica estos permisos).
RUN useradd --create-home --uid 1000 coldchain \
    && mkdir -p /app/data/cache /app/data/processed /app/data/out \
    && chown -R coldchain:coldchain /app
USER coldchain

CMD ["/app/.venv/bin/python", "-m", "coldchain_streaming.pipeline"]
