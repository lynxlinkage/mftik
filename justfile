
set dotenv-load := false

default:
    @just --list

# Install Python workspace + frontend deps
sync:
    uv sync --all-packages
    cd frontend && npm install

# Unit + component, in parallel (§9.1, F30). Integration and e2e are
# `just test-int`. Postgres is not in this set. Needs the broker up:
# `just up nats`. There is no fake to fall back on. On CI the recipe
# fails when this step's wall time exceeds 120s — that clock does not
# include `uv sync` or service startup. CI is `tier_budget.on_ci`
# (empty, 0, false, no, off are not CI). A unit or component call over
# its cap warns on CI and still fails locally.
test:
    #!/usr/bin/env bash
    set -euo pipefail
    start=$(date +%s.%N)
    set +e
    uv run --all-packages pytest packages apps -q -n auto -m "not integration and not e2e"
    code=$?
    set -e
    end=$(date +%s.%N)
    elapsed=$(python3 -c "print(${end} - ${start})")
    # check_wall_budget asks on_ci(); off CI it exits 0 without gating.
    wall=0
    uv run --all-packages python scripts/check_wall_budget.py "$elapsed" || wall=$?
    if [ "$code" -ne 0 ]; then
      exit "$code"
    fi
    exit "$wall"

# Integration and e2e (§9.1). When TEST_POSTGRES_URL is set, the Postgres
# dialect of the database tests is part of this set. Serial on purpose:
# those tests share one database and truncate it between cases.
test-int:
    uv run --all-packages pytest packages apps -q -m "integration or e2e"

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

# Time this node's hot paths on asyncio vs uvloop — evidence for docs/archive/EventLoop.md.
# Wants a broker nobody else is using: it publishes thousands of messages and
# writes a tape. `--probe` reports behaviour differences instead.
loop-bench *args:
    uv run --all-packages python scripts/loop_bench.py {{args}}

# Apply DB migrations
migrate revision="head":
    uv run --all-packages mftik-db-migrate {{revision}}

# Seed dev user + two paper APIs (idempotent)
seed:
    uv run --all-packages python scripts/seed_paper_apis.py

# Ask a running MD for market data: just fetch quote Gate_Spot_BTCUSDT
#
# Reads the broker out of the environment, and the defaults already point at
# the compose stack's published ports — so this needs no variables set unless
# the node is somewhere else.
fetch *args:
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

# Frontend UI contract. Install browsers once: cd frontend && npx playwright install chromium
frontend-e2e:
    cd frontend && npx playwright test

# Docker compose
up *args:
    # Build the two images explicitly first. Every Python service shares the
    # `mftik:dev` tag, so letting `up --build` build them all would have seven
    # concurrent builds racing to write the same tag ("image already exists").
    docker compose build migrate frontend
    docker compose up {{args}}

down:
    docker compose down

# Strategon. Every recipe wants STRATEGON_API_KEY in the environment; the
# token is minted on the control plane's API tokens page and is not in .env.
#
# Phase and members of every set.
s7n-status *args:
    python3 scripts/s7n.py status {{args}}

# Preflight the plane sets against the control plane and print what a tag
# would apply — secrets show as their `secret.*` tokens, so this is safe to
# paste. Nothing is written.
s7n-plan version="v0.0.0":
    python3 scripts/s7n.py apply deployment/sets/planes.json --version {{version}} --dry-run

# Roll the plane sets to a tag that is already in the catalog: a rollback, or
# a spec change under the running version. `--tar` is only for a new tag,
# which is the release workflow's job.
s7n-planes version:
    python3 scripts/s7n.py apply deployment/sets/planes.json --version {{version}}

# The secret catalog. `put` reads the value from stdin so it is never in argv:
#   ssh cp 'grep ^DATABASE_URL= /opt/mftik/deploy/.env | cut -d= -f2-' | just s7n-secret-put mftik-database-url
s7n-secrets:
    python3 scripts/s7n.py secrets list

s7n-secret-put name:
    python3 scripts/s7n.py secrets put {{name}}

# Point git at the tracked hooks in scripts/git-hooks. `.git/hooks` is not
# cloned, so every checkout has to opt in once; run this after `just sync`.
install-hooks:
    git config core.hooksPath scripts/git-hooks
    @echo "hooks installed — pre-commit runs check-contracts (skip once: git commit --no-verify)"
