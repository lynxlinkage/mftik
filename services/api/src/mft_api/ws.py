from __future__ import annotations

import asyncio
import logging

from fastapi import WebSocket, WebSocketDisconnect

from mft_broker import BrokerClient
from mft_protocol import MessageEnvelope, Topics

logger = logging.getLogger(__name__)


async def session_log_bridge(websocket: WebSocket, session_id: str) -> None:
    """Bridge Redis log.session.{id} pub/sub to a WebSocket client."""
    await websocket.accept()
    channel = Topics.log_session(session_id)
    stop = asyncio.Event()

    welcome = MessageEnvelope(
        type="log",
        source="api",
        session_id=session_id,
        payload={"level": "info", "message": f"Session {session_id} connected"},
    )
    await websocket.send_text(welcome.to_json())

    async with BrokerClient() as broker:
        async def forward() -> None:
            async for envelope in broker.listen([channel], stop=stop):
                try:
                    await websocket.send_text(envelope.to_json())
                except Exception:
                    stop.set()
                    break

        task = asyncio.create_task(forward())
        # Allow subscribe to settle, then announce on the broker channel
        await asyncio.sleep(0.05)
        await broker.publish(channel, welcome)

        try:
            while True:
                data = await websocket.receive_text()
                await broker.publish(
                    channel,
                    MessageEnvelope(
                        type="log",
                        source="api.client",
                        session_id=session_id,
                        payload={"level": "debug", "message": data},
                    ),
                )
        except WebSocketDisconnect:
            logger.info("WebSocket disconnected session=%s", session_id)
        finally:
            stop.set()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
