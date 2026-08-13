from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect
from mft.broker import Broker
from mft.protocol import TD_FILL, Log, LogEnvelope, Topics, UntypedEnvelope
from mft_db.repositories import OrderRepository
from mft_db.session import session_scope

from mft_api.decimals import wire_decimal

logger = logging.getLogger(__name__)

#: Every trading account's private fan-out. Fills name an ``api_id`` and no
#: session, which is what :func:`board_bridge` exists to bridge.
TD_GLOBAL_PATTERN = "td.*.global"


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


async def sts_status_bridge(websocket: WebSocket) -> None:
    """Bridge ``status.sts`` → ``/ws/status/sts``. Read-only, on purpose.

    Unlike the log bridges this never publishes what a client sends. A log
    channel echoing a browser back to itself is a debugging convenience; on
    the status channel it would let any connected page forge session states
    for every other viewer, and nothing here authenticates the caller.

    Replays the buffer before going live so a page that loads just after a
    session ended still learns about it, and dedupes by envelope id across
    the seam. Consumers apply the newest event per ``session_id``: replayed
    history and live events can overlap, and each event is a full snapshot.
    """
    await websocket.accept()
    stop = asyncio.Event()
    seen: set[str] = set()
    channel = Topics.status_sts()

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
        live: asyncio.Queue[UntypedEnvelope | None] = asyncio.Queue()

        async def pump_pubsub() -> None:
            try:
                async for envelope in broker.subscribe(channel, stop=stop):
                    await live.put(envelope)
            finally:
                await live.put(None)

        sub_task = asyncio.create_task(pump_pubsub())
        await asyncio.sleep(0.05)

        for raw in await broker.fetch_log_buffer(channel):
            if not await send_raw(raw):
                break

        async def pump_live() -> None:
            while not stop.is_set():
                env = await live.get()
                if env is None:
                    break
                if not await send_raw(env.to_json()):
                    break

        live_task = asyncio.create_task(pump_live())
        try:
            # Receive only to notice the client going away — whatever it sends
            # is discarded rather than published.
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            logger.info("STS status stream disconnected")
        except Exception:
            logger.exception("STS status stream failed")
        finally:
            stop.set()
            for t in (sub_task, live_task):
                t.cancel()
            await asyncio.gather(sub_task, live_task, return_exceptions=True)


async def session_of(api_id: int, client_order_id: str | None) -> str | None:
    """Which strategy session placed an order, or None if none of ours did.

    The rule the board's live stream turns on, kept out of the socket
    plumbing because it is the part that can be wrong in a way nobody sees: a
    misattributed fill does not raise, it climbs the wrong row.

    Read from ``orders``, where the submit path recorded it. The slot packed
    into the id would answer faster and would even be sound for a live stream —
    it is unambiguous among sessions alive at once — but the recorded answer
    stays right if this is ever asked about something older.

    None covers three different things on purpose: no id on the fill, no order
    on file, and an order that was placed outside this platform. All three mean
    "not a session's", and the stream drops them rather than pick one.
    """
    if not client_order_id:
        return None
    try:
        async with session_scope() as db:
            order = await OrderRepository(db).get_by_key(api_id, client_order_id)
    except Exception:
        logger.debug(
            "board attribution failed cid=%s", client_order_id, exc_info=True
        )
        return None
    return order.session_id if order is not None else None


async def board_bridge(websocket: WebSocket) -> None:
    """Bridge live executions to ``/ws/board``, attributed to a session.

    The board's counts come from the database, which is minutes behind a
    fill by design — the writer batches, and the settlement line is deliberately
    further back still. This is the seam that makes a *live* run readable
    without either of those being made to hurry.

    Fills arrive on ``td.{api_id}.global``, which names an account and knows
    nothing of strategy sessions. The attribution is looked up in ``orders``,
    where the submit path recorded it: the order row exists well before any
    fill on it can, so the lookup cannot lose a race with its own event. The
    slot packed into the ``client_order_id`` would answer faster and would even
    be sound here — it is unambiguous among sessions alive at once, which is
    all this stream shows — but the recorded answer is the one that stays right
    if this ever renders anything older.

    Read-only, like the status bridge and for the same reason: nothing here
    authenticates the caller, and a channel that echoed a browser would let any
    connected page invent executions for every other viewer.

    One database lookup per fill per connected viewer. Fine for the handful of
    people who watch a board, and the wrong shape if that ever stops being
    true — the fix then is one subscriber fanning out, not a cache here.
    """
    await websocket.accept()
    stop = asyncio.Event()

    async def send(payload: dict[str, Any]) -> bool:
        try:
            await websocket.send_text(json.dumps(payload))
            return True
        except Exception:
            stop.set()
            return False

    async with Broker() as broker:
        live: asyncio.Queue[tuple[str, UntypedEnvelope] | None] = asyncio.Queue()

        async def pump_pubsub() -> None:
            try:
                async for topic, envelope in broker.psubscribe(
                    TD_GLOBAL_PATTERN, stop=stop
                ):
                    await live.put((topic, envelope))
            finally:
                await live.put(None)

        sub_task = asyncio.create_task(pump_pubsub())

        async def pump_live() -> None:
            while not stop.is_set():
                item = await live.get()
                if item is None:
                    break
                topic, envelope = item
                if envelope.type != TD_FILL:
                    continue
                api_id = _api_id_of(topic)
                payload = envelope.payload if isinstance(envelope.payload, dict) else {}
                session_id = await session_of(
                    api_id, payload.get("client_order_id")
                )
                if session_id is None:
                    # An execution on an account this platform holds but no
                    # session of ours placed — real, and not this stream's to
                    # file under somebody.
                    continue
                if not await send(
                    {
                        "type": "fill",
                        "session_id": session_id,
                        "api_id": api_id,
                        "universal_ticker": payload.get("universal_ticker"),
                        "side": payload.get("side"),
                        # Same padding removal the REST rows get: a live row
                        # sitting beside stored ones must not be the only one
                        # showing eighteen decimal places.
                        "qty": wire_decimal(payload.get("qty")),
                        "price": wire_decimal(payload.get("price")),
                        "ts": payload.get("ts") or envelope.ts,
                    }
                ):
                    break

        live_task = asyncio.create_task(pump_live())
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            logger.info("board stream disconnected")
        except Exception:
            logger.exception("board stream failed")
        finally:
            stop.set()
            for t in (sub_task, live_task):
                t.cancel()
            await asyncio.gather(sub_task, live_task, return_exceptions=True)


def _api_id_of(topic: str) -> int:
    """``td.{api_id}.global`` → the account. Zero when it is not one."""
    parts = topic.split(".")
    if len(parts) == 3 and parts[0] == "td" and parts[2] == "global":
        try:
            return int(parts[1])
        except ValueError:
            return 0
    return 0


# Backward-compatible name used by older imports.
session_log_bridge = sts_log_bridge
