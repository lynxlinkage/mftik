
set dotenv-load := false

default:
    @just --list

# Install Python workspace + frontend deps
sync:
    uv sync --all-packages
    cd frontend && npm install

# Run all Python tests (sqlite only — fast, and what most changes need)
test:
    uv run --all-packages pytest packages apps -q

# Run them again on Postgres too, which is what CI does and what production is.
# sqlite ignores VARCHAR length and has no decimal type, so it cannot show you
# a column too small for what a venue sends. Needs `just up postgres` running;
# the database is created on first use and its tables are dropped every run.
test-pg *args="packages apps":
    #!/usr/bin/env bash
    set -euo pipefail
    url="${TEST_POSTGRES_URL:-postgresql+asyncpg://mftik:mftik@localhost:5432/mftik_test}"
    docker compose exec -T postgres \
      createdb -U "${POSTGRES_USER:-mftik}" mftik_test 2>/dev/null \
      && echo "created mftik_test" || true
    TEST_POSTGRES_URL="$url" uv run --all-packages pytest {{args}} -q

# Lint Python. Same invocation CI runs, so a green run here means a green one
# there — conftest.py included, since it sits at the root and neither path
# would otherwise reach it.
lint:
    uv run --all-packages ruff check packages apps conftest.py

# Sign a real history read with a stored credential and print what came back.
# Read-only: every call is a GET on a history endpoint and nothing is written.
backfill-check *args:
    uv run --all-packages python scripts/backfill_check.py {{args}}

# Apply DB migrations
migrate revision="head":
    uv run --all-packages mftik-db-migrate {{revision}}

# Seed dev user + two paper APIs (idempotent)
seed:
    uv run --all-packages python scripts/seed_paper_apis.py

# Ask a running MD for market data: just fetch quote Gate_Spot_BTCUSDT
fetch *args:
    REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}" \
      uv run --all-packages python scripts/fetch_md.py {{args}}

# Fail if the migrations would not build the models. Runs against a scratch
# database so it never touches the dev one, and drops it again afterwards.
check-migrations:
    #!/usr/bin/env bash
    set -euo pipefail
    user="${POSTGRES_USER:-mftik}"
    docker compose exec -T postgres dropdb -U "$user" --if-exists mftik_migration_check
    docker compose exec -T postgres createdb -U "$user" mftik_migration_check
    export DATABASE_URL_SYNC="postgresql+psycopg://mftik:mftik@localhost:5432/mftik_migration_check"
    uv run --all-packages alembic -c packages/db/alembic.ini upgrade head
    uv run --all-packages alembic -c packages/db/alembic.ini check
    docker compose exec -T postgres dropdb -U "$user" mftik_migration_check

# Autogenerate a migration (message required)
makemigration message:
    uv run --all-packages alembic -c packages/db/alembic.ini revision --autogenerate -m "{{message}}"

# Export FastAPI OpenAPI → contracts/openapi.json
openapi:
    uv run --all-packages python -c "import json; from mftik_api.main import app; print(json.dumps(app.openapi(), indent=2))" > contracts/openapi.json

# Fail if OpenAPI contract is stale
check-contracts:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp="$(mktemp)"
    uv run --all-packages python -c "import json; from mftik_api.main import app; print(json.dumps(app.openapi(), indent=2))" > "$tmp"
    if ! diff -u contracts/openapi.json "$tmp"; then
      echo "contracts/openapi.json is stale — run: just openapi" >&2
      rm -f "$tmp"
      exit 1
    fi
    rm -f "$tmp"
    echo "contracts/openapi.json is up to date"

# Frontend typecheck
frontend-check:
    cd frontend && npm run check

# Docker compose
up *args:
    # Build the two images explicitly first. Every Python service shares the
    # `mftik:dev` tag, so letting `up --build` build them all would have seven
    # concurrent builds racing to write the same tag ("image already exists").
    docker compose build migrate frontend
    docker compose up {{args}}

down:
    docker compose down
