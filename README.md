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

## Quick start

```bash
cp .env.example .env
just sync   # uv sync --all-packages + frontend npm install
just up     # docker compose up --build
```

- API health: http://localhost:8000/health
- Control UI: http://localhost:5173 (Home / STS / TD / MD / Audit)

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
