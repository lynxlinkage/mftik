FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    ALEMBIC_CONFIG=/app/packages/db/alembic.ini

COPY pyproject.toml uv.lock ./
COPY packages ./packages
COPY apps ./apps
COPY scripts ./scripts

RUN uv sync --package mft-db --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

CMD ["mft-db-migrate"]
