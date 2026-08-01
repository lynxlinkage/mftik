
set dotenv-load := false

default:
    @just --list

# Install Python workspace + frontend deps
sync:
    uv sync --all-packages
    cd frontend && npm install

# Run all Python tests
test:
    uv run --all-packages pytest packages apps -q

# Lint Python
lint:
    uv run --all-packages ruff check packages apps

# Apply DB migrations
migrate revision="head":
    uv run --all-packages mft-db-migrate {{revision}}

# Seed dev user + two paper APIs (idempotent)
seed:
    uv run --all-packages python scripts/seed_paper_apis.py

# Autogenerate a migration (message required)
makemigration message:
    uv run --all-packages alembic -c packages/db/alembic.ini revision --autogenerate -m "{{message}}"

# Export FastAPI OpenAPI → contracts/openapi.json
openapi:
    uv run --all-packages python -c "import json; from mft_api.main import app; print(json.dumps(app.openapi(), indent=2))" > contracts/openapi.json

# Fail if OpenAPI contract is stale
check-contracts:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp="$(mktemp)"
    uv run --all-packages python -c "import json; from mft_api.main import app; print(json.dumps(app.openapi(), indent=2))" > "$tmp"
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
    docker compose up --build {{args}}

down:
    docker compose down
