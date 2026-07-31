from __future__ import annotations

import logging
import os

from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware

from mft_api.routes.health import router as health_router
from mft_api.ws import session_log_bridge

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

app = FastAPI(title="MFT API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(health_router)


@app.websocket("/ws/{session_id}")
async def ws_session(websocket: WebSocket, session_id: str) -> None:
    await session_log_bridge(websocket, session_id)


def run() -> None:
    import uvicorn

    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    uvicorn.run("mft_api.main:app", host=host, port=port, log_level="info")


if __name__ == "__main__":
    run()
