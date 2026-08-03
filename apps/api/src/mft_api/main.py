from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from mft.broker import Broker

from mft_api.routes import (
    apis_router,
    audits_router,
    health_router,
    md_router,
    stats_router,
    sts_router,
    sym_router,
    td_router,
)
from mft_api.ws import md_log_bridge, sts_log_bridge, td_log_bridge

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("mft_api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    broker = Broker()
    await broker.connect()
    app.state.broker = broker
    logger.info("API broker connected")
    try:
        yield
    finally:
        await broker.close()
        app.state.broker = None
        logger.info("API broker closed")


app = FastAPI(title="MFT API", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(health_router)
app.include_router(stats_router)
app.include_router(apis_router)
app.include_router(sts_router)
app.include_router(td_router)
app.include_router(md_router)
app.include_router(sym_router)
app.include_router(audits_router)


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
    uvicorn.run("mft_api.main:app", host=host, port=port, log_level="info")


if __name__ == "__main__":
    run()
