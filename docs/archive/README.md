# Archived: original repository README

This was the layout-and-commands README that lived at the repository root
before the public rewrite. Kept so the map of paths and `just` recipes is
not lost. The current README is at the repository root.

---

# MFTIK — Mid-Frequency Algo Trading Platform

Async, broker-centric trading platform. Domains talk through Redis (Streams + Pub/Sub).

## Layout

| Path | Role |
|------|------|
| `apps/api` | FastAPI gateway |
| `apps/td` | Trading domain (remote paper client) |
| `apps/paper` | Paper exchange engine (Redis RPC + streams) |
| `apps/md` | Market-data domain |
| `apps/sts` | Strategy domain |
| `packages/common` | Shared library (`mftik`) — protocol, broker, runtime, exchange, strategy |
| `packages/db` | Schema + Alembic migrations (`mftik_db`) |
| `contracts/` | OpenAPI contract (Python ↔ JS) |
| `frontend/` | SvelteKit UI (outside uv workspace) |
| `Dockerfile` | One image for every Python process — they differ only by `command:` |
| `deploy/` | Production stack — Traefik-routed, runs published images |

## Quick start

```bash
cp .env.example .env
just sync   # uv sync --all-packages + frontend npm install
just install-hooks   # pre-commit: fail early on a stale OpenAPI contract
just up     # build the shared image, then docker compose up
```

- API health: http://localhost:8000/health
- Control UI: http://localhost:5173 (Home / STS / TD / MD / Audit)

Use `just up` rather than `docker compose up --build`: every Python service
shares one image tag, so building them all at once races.

## Deployment

Tagging `v*` builds and publishes images to GHCR; nothing deploys itself.
A host runs them with a production compose file that pulls those images
and reads its domain, image tag and secrets from a `.env` beside it. That
file and the host-side runbook — addresses, secrets, volumes — are kept
outside this repository.

## Common tasks

```bash
just sync              # install Python workspace + frontend
just test              # pytest
just lint              # ruff
just migrate           # alembic upgrade head
just seed              # dev user + two paper APIs (idempotent)
just openapi           # regenerate contracts/openapi.json
just check-contracts   # fail if OpenAPI is stale
just install-hooks     # run that check from a pre-commit hook
just frontend-check
```

Apps do not import each other. Shared code lives in `packages/common` (`import mftik`) and `packages/db` (`import mftik_db`).
