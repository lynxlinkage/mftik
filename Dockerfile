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

# The git tag, PEP 440-normalized (v0.9.5 → 0.9.5). `uv sync --frozen`
# stamps workspace members from the lock (0.0.0) and will not re-run
# hatchling, so the release wheel is built separately and installed over
# that. Handshake and `mftik --version` then read the installed metadata.
# Not MFTIK_VERSION — compose already uses that name for the image tag.
ARG MFTIK_DIST_VERSION=0.0.0
ENV MFTIK_DIST_VERSION=$MFTIK_DIST_VERSION

RUN uv build --package mftik --out-dir /tmp/dist
RUN uv sync --all-packages --frozen --no-dev
ENV PATH="/app/.venv/bin:$PATH"
RUN uv pip install --offline --no-deps --reinstall /tmp/dist/mftik-*.whl
RUN python -c "from importlib.metadata import version; import os; \
    v = version('mftik'); e = os.environ['MFTIK_DIST_VERSION']; \
    assert v == e, (v, e)"

EXPOSE 8000

CMD ["mftik-api"]
