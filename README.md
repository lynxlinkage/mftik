# MFT — Mid-Frequency Algo Trading Platform

Async, broker-centric trading platform. Domains talk through Redis (Streams + Pub/Sub).

## Layout

| Path | Role |
|------|------|
| `apps/api` | FastAPI gateway |
| `apps/td` | Trading domain (remote paper client) |
| `apps/paper` | Paper exchange engine (Redis RPC + streams) |
| `apps/md` | Market-data domain |
| `apps/sts` | Strategy domain |
| `packages/common` | Shared library (`mft`) — protocol, broker, runtime, exchange |
| `packages/db` | Schema + Alembic migrations (`mft_db`) |
| `contracts/` | OpenAPI contract (Python ↔ JS) |
| `frontend/` | SvelteKit UI (outside uv workspace) |
| `Dockerfile` | One image for every Python process — they differ only by `command:` |
| `deploy/` | Production stack for mft.lynkora.com (see [docs/CICD.md](docs/CICD.md)) |

## Quick start

```bash
cp .env.example .env
just sync   # uv sync --all-packages + frontend npm install
just up     # build the shared image, then docker compose up
```

- API health: http://localhost:8000/health
- Control UI: http://localhost:5173 (Home / STS / TD / MD / Audit)

Use `just up` rather than `docker compose up --build`: every Python service
shares one image tag, so building them all at once races.

## Deployment

Merging to `main` builds, tags and ships to https://mft.lynkora.com.
See [docs/CICD.md](docs/CICD.md).

## Common tasks

```bash
just sync              # install Python workspace + frontend
just test              # pytest
just lint              # ruff
just migrate           # alembic upgrade head
just seed              # dev user + two paper APIs (idempotent)
just openapi           # regenerate contracts/openapi.json
just check-contracts   # fail if OpenAPI is stale
just frontend-check
```

Apps do not import each other. Shared code lives in `packages/common` (`import mft`) and `packages/db` (`import mft_db`).
