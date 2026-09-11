"""Long-lived broker → Postgres session-log persist worker."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from mftik.broker import Broker
from mftik.protocol import Topics, UntypedEnvelope
from mftik_db.repositories import SessionLogRepository
from mftik_db.session import session_scope

logger = logging.getLogger("mftik_api.log_persist")

LOG_PATTERN = Topics.log_pattern()


def _batch_size() -> int:
    return max(1, int(os.getenv("LOG_PERSIST_BATCH_SIZE", "100")))


def _flush_interval() -> float:
    return max(0.1, float(os.getenv("LOG_PERSIST_FLUSH_INTERVAL", "2.0")))


def parse_log_topic(topic: str) -> tuple[str, str] | None:
    """Parse ``log.{domain}.{stream_id}`` → ``(domain, stream_id)``."""
    parts = topic.split(".", 2)
    if len(parts) != 3 or parts[0] != "log":
        return None
    domain, stream_id = parts[1], parts[2]
    if domain not in ("sts", "td", "md") or not stream_id:
        return None
    return domain, stream_id


def envelope_to_row(
    topic: str,
    envelope: UntypedEnvelope,
) -> dict[str, Any] | None:
    parsed = parse_log_topic(topic)
    if parsed is None:
        return None
    domain, stream_id = parsed
    payload = envelope.payload if isinstance(envelope.payload, dict) else {}
    message = payload.get("message")
    if message is None:
        return None
    level = str(payload.get("level") or "info")
    return {
        "envelope_id": envelope.id,
        "domain": domain,
        "stream_id": stream_id,
        "source": envelope.source or "unknown",
        "level": level[:16],
        "message": str(message),
        "ts": float(envelope.ts),
    }


async def flush_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    async with session_scope() as db:
        repo = SessionLogRepository(db)
        await repo.bulk_insert_ignore(rows)
    logger.debug("flushed %d session log row(s)", len(rows))


class _Buffer:
    """The persist worker's unflushed rows, visible to a late socket."""

    __slots__ = ("lock", "rows")

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.rows: list[dict[str, Any]] = []


#: Set while :func:`run_log_persist` is running. ``None`` means no worker.
_buffer: _Buffer | None = None


async def flush_pending() -> None:
    """Write the persist worker's unflushed rows, if it is running.

    A late ``/ws/{domain}/{id}`` calls this before reading
    ``session_logs``. Without it, a line still sitting in the batch
    (up to ``LOG_PERSIST_FLUSH_INTERVAL``) is in neither Postgres nor
    the live subscription. No-op when the worker is down — that window
    is already lost.
    """
    buf = _buffer
    if buf is None:
        return
    await _flush(buf)


async def _flush(buf: _Buffer) -> None:
    async with buf.lock:
        if not buf.rows:
            return
        to_write = list(buf.rows)
        buf.rows.clear()
    try:
        await flush_rows(to_write)
    except Exception:
        logger.exception("failed to flush %d log row(s)", len(to_write))


async def run_log_persist(stop: asyncio.Event) -> None:
    """Subscribe to ``log.*`` and batch-insert into ``session_logs`` until stop.

    Flushes when the buffer reaches ``LOG_PERSIST_BATCH_SIZE`` or every
    ``LOG_PERSIST_FLUSH_INTERVAL`` seconds (whichever comes first), and
    when a late socket asks via :func:`flush_pending`.
    """
    global _buffer
    batch_size = _batch_size()
    flush_interval = _flush_interval()
    buf = _Buffer()
    _buffer = buf

    broker = Broker()
    await broker.connect()
    logger.info(
        "log persist worker started (batch=%d interval=%.1fs)",
        batch_size,
        flush_interval,
    )

    async def ticker() -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=flush_interval)
            except TimeoutError:
                pass
            await _flush(buf)

    ticker_task = asyncio.create_task(ticker())
    try:
        async for topic, envelope in broker.psubscribe(LOG_PATTERN, stop=stop):
            row = envelope_to_row(topic, envelope)
            if row is None:
                continue
            async with buf.lock:
                buf.rows.append(row)
                full = len(buf.rows) >= batch_size
            if full:
                await _flush(buf)
    finally:
        stop.set()
        ticker_task.cancel()
        try:
            await ticker_task
        except asyncio.CancelledError:
            pass
        await _flush(buf)
        _buffer = None
        await broker.close()
        logger.info("log persist worker stopped")
