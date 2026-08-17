# One image for every Python process in the monorepo.
#
# api, td, md, sts, sym, paper and the migrator differ only in which console
# script they run, and they all share `packages/common` + `packages/db`. Six
# near-identical Dockerfiles meant six builds of the same dependency tree, so
# this installs the whole workspace once and lets each container pick its
# entrypoint via `command:`:
#
#   mftik-api | td | md | sts | sym | paper | mftik-db-migrate
#
# The venv is on PATH, so the commands above are the literal `command:` values.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# What links the GHCR package back to this repo. Without it the package is
# owned by the org and connected to nothing, and the Actions token of the repo
# that built it cannot push a second version.
LABEL org.opencontainers.image.source="https://github.com/lynxlinkage/mftik"

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    ALEMBIC_CONFIG=/app/packages/db/alembic.ini

COPY pyproject.toml uv.lock ./
COPY packages ./packages
COPY apps ./apps
# scripts/seed_paper_apis.py is run as a one-shot container on deploy.
COPY scripts ./scripts

RUN uv sync --all-packages --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

CMD ["mftik-api"]
