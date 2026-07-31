FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

COPY pyproject.toml uv.lock ./
COPY packages ./packages
COPY services ./services
COPY strategies ./strategies

RUN uv sync --package mft-strategy --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "-m", "mft_strategy"]
