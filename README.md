# MFT — Mid-Frequency Algo Trading Platform

Async, broker-centric trading platform. Domains communicate only through Redis (point-to-point Streams + Pub/Sub).

## Architecture

| Component | Role |
|-----------|------|
| **Redis** | Message broker (P2P + pub/sub) |
| **adapter** | Exchange market/private API connectivity |
| **market_data** | Normalize public MD and publish to broker |
| **trading** | Wire adapters into trading-domain messages |
| **strategy** | Algo layer (`Strategy` base class + event handlers) |
| **api** | FastAPI REST + `WS /ws/{session_id}` live logs |
| **frontend** | SvelteKit UI with live logging session |

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

- API health: http://localhost:8000/health
- Live logs UI: http://localhost:5173
- Redis: `localhost:6379`

## Workspace

Python packages managed with [uv](https://github.com/astral-sh/uv). Shared libs live under `packages/`; runnable services under `services/`. User strategies go in `strategies/`.
