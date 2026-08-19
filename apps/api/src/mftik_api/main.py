from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from mftik.broker import Broker

from mftik_api.auth import AuthMiddleware, auth_router
from mftik_api.backfill_cron import run_backfill_cron
from mftik_api.log_persist import run_log_persist
from mftik_api.routes import (
    apis_router,
    audits_router,
    board_router,
    environment_router,
    health_router,
    logs_router,
    md_router,
    registry_router,
    stats_router,
    sts_router,
    sym_router,
    td_router,
)
from mftik_api.ws import (
    board_bridge,
    md_log_bridge,
    sts_log_bridge,
    sts_status_bridge,
    td_log_bridge,
)


def allowed_origins() -> list[str]:
    """Origins allowed to make credentialed cross-origin requests. None, by default.

    This used to be ``["*"]`` with ``allow_credentials=True``, which is not a
    permissive setting so much as an invalid one: browsers refuse to send
    cookies to a wildcard origin, so it never granted anything. It was
    harmless only because nothing here has ever been cross-origin — the Vite
    proxy makes local same-origin, and production serves the document, /api
    and /ws from one hostname.

    Empty is therefore the correct default and not a restriction: same-origin
    requests do not consult CORS at all. ``MFTIK_ALLOWED_ORIGINS`` exists for
    the deployment that genuinely splits the UI off onto another host, and
    that deployment has to name it — a session cookie is at stake, and the
    wildcard cannot be what names it.
    """
    raw = os.getenv("MFTIK_ALLOWED_ORIGINS", "")
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("mftik_api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    broker = Broker()
    await broker.connect()
    app.state.broker = broker
    logger.info("API broker connected")

    persist_stop = asyncio.Event()
    persist_task = asyncio.create_task(run_log_persist(persist_stop))
    backfill_stop = asyncio.Event()
    backfill_task = asyncio.create_task(run_backfill_cron(backfill_stop))
    try:
        yield
    finally:
        persist_stop.set()
        backfill_stop.set()
        for task in (persist_task, backfill_task):
            try:
                await asyncio.wait_for(task, timeout=10)
            except (TimeoutError, asyncio.CancelledError):
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        await broker.close()
        app.state.broker = None
        logger.info("API broker closed")


app = FastAPI(title="MFTIK API", version="0.1.0", lifespan=lifespan)
# Added before CORS on purpose. `add_middleware` prepends, so the last call is
# the outermost layer — CORS has to wrap the gate, not sit behind it, or a
# preflight (which carries no credentials, by design) would be refused as
# unauthenticated and the real request would never be sent.
app.add_middleware(AuthMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(auth_router)
app.include_router(health_router)
app.include_router(stats_router)
app.include_router(apis_router)
app.include_router(registry_router)
app.include_router(environment_router)
app.include_router(sts_router)
app.include_router(td_router)
app.include_router(md_router)
app.include_router(sym_router)
app.include_router(audits_router)
app.include_router(logs_router)
app.include_router(board_router)


@app.websocket("/ws/board")
async def ws_board(websocket: WebSocket) -> None:
    """Live executions, attributed to the session that placed them."""
    await board_bridge(websocket)


@app.websocket("/ws/status/sts")
async def ws_sts_status(websocket: WebSocket) -> None:
    """Every STS session's state changes, for the strategies list."""
    await sts_status_bridge(websocket)


@app.websocket("/ws/sts/{session_id}")
async def ws_sts_session(websocket: WebSocket, session_id: str) -> None:
    await sts_log_bridge(websocket, session_id)


@app.websocket("/ws/td/{api_id}")
async def ws_td_session(websocket: WebSocket, api_id: int) -> None:
    await td_log_bridge(websocket, api_id)


@app.websocket("/ws/md/{venue}")
async def ws_md_venue(websocket: WebSocket, venue: str) -> None:
    await md_log_bridge(websocket, venue)


def run() -> None:
    import uvicorn

    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    uvicorn.run("mftik_api.main:app", host=host, port=port, log_level="info")


if __name__ == "__main__":
    run()
