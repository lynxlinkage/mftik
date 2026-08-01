from __future__ import annotations

import asyncio
import logging

from fastapi import WebSocket, WebSocketDisconnect
from mft.broker import Broker
from mft.protocol import Log, LogEnvelope, Topics, UntypedEnvelope

logger = logging.getLogger(__name__)


async def _log_bridge(
    websocket: WebSocket,
    *,
    channel: str,
    stream_id: str,
    source: str,
) -> None:
    """Bridge a Redis log channel to a WebSocket client.

    Replays the Redis log buffer first (so deploy-time lines are not lost),
    then forwards live pub/sub. Envelope ids are deduped across the seam.
    """
    await websocket.accept()
    stop = asyncio.Event()
    seen: set[str] = set()
    live: asyncio.Queue[UntypedEnvelope | None] = asyncio.Queue()

    async def send_raw(raw: str) -> bool:
        try:
            env = UntypedEnvelope.from_json(raw)
            if env.id in seen:
                return True
            seen.add(env.id)
        except Exception:
            pass
        try:
            await websocket.send_text(raw)
            return True
        except Exception:
            stop.set()
            return False

    async with Broker() as broker:

        async def pump_pubsub() -> None:
            try:
                async for envelope in broker.subscribe(channel, stop=stop):
                    await live.put(envelope)
            finally:
                await live.put(None)

        sub_task = asyncio.create_task(pump_pubsub())
        await asyncio.sleep(0.05)

        # Historical lines first (Noop on_start / on_ready / recon, etc.).
        for raw in await broker.fetch_log_buffer(channel):
            if not await send_raw(raw):
                break

        # Anything published during the buffer read (deduped by id).
        while True:
            try:
                env = live.get_nowait()
            except asyncio.QueueEmpty:
                break
            if env is None:
                break
            if not await send_raw(env.to_json()):
                break

        welcome = LogEnvelope.wrap(
            Log(
                level="info",
                message=f"{source} log stream {stream_id} connected",
            ),
            type="log",
            source="api",
            session_id=stream_id,
        )
        await send_raw(welcome.to_json())

        async def pump_live() -> None:
            while not stop.is_set():
                env = await live.get()
                if env is None:
                    break
                if not await send_raw(env.to_json()):
                    break

        live_task = asyncio.create_task(pump_live())
        try:
            while True:
                data = await websocket.receive_text()
                await broker.publish_log(
                    channel,
                    LogEnvelope.wrap(
                        Log(level="debug", message=data),
                        type="log",
                        source="api.client",
                        session_id=stream_id,
                    ),
                )
        except WebSocketDisconnect:
            logger.info(
                "WebSocket disconnected source=%s id=%s", source, stream_id
            )
        finally:
            stop.set()
            for t in (sub_task, live_task):
                t.cancel()
            await asyncio.gather(sub_task, live_task, return_exceptions=True)


async def sts_log_bridge(websocket: WebSocket, session_id: str) -> None:
    """Bridge ``log.sts.{session_id}`` → ``/ws/sts/{session_id}``."""
    await _log_bridge(
        websocket,
        channel=Topics.log_sts(session_id),
        stream_id=session_id,
        source="sts",
    )


async def td_log_bridge(websocket: WebSocket, api_id: int) -> None:
    """Bridge ``log.td.{api_id}`` → ``/ws/td/{api_id}``."""
    await _log_bridge(
        websocket,
        channel=Topics.log_td(api_id),
        stream_id=str(api_id),
        source="td",
    )


async def md_log_bridge(websocket: WebSocket, venue: str) -> None:
    """Bridge ``log.md.{venue}`` → ``/ws/md/{venue}``."""
    await _log_bridge(
        websocket,
        channel=Topics.log_md(venue),
        stream_id=venue,
        source="md",
    )


# Backward-compatible name used by older imports.
session_log_bridge = sts_log_bridge
