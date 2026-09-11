"""Publish structured lines to domain log channels (API WebSocket bridges)."""

from __future__ import annotations

from typing import Any, Protocol


class _LogPublisher(Protocol):
    async def publish(self, topic: str, envelope: Any) -> None: ...


async def publish_sts_log(
    broker: _LogPublisher,
    session_id: str,
    message: str,
    *,
    source: str,
    level: str = "info",
    type: str | None = None,
    **extra: Any,
) -> None:
    """Fan out a log line for ``/ws/sts/{session_id}``.

    Live subscribers see it now. A late socket replays ``session_logs``.

    ``type`` is the qualified registry key and the only way
    :attr:`Log.type` is set. A ``type`` key in ``extra`` is dropped so a
    strategy cannot reroute the line by ``self.log(..., type=...)``.
    """
    from mftik.protocol.messages import Log, LogEnvelope
    from mftik.protocol.topics import Topics

    extra.pop("type", None)
    topic = Topics.log_sts(session_id)
    envelope = LogEnvelope.wrap(
        Log(level=level, message=message, type=type, **extra),
        type="log",
        source=source,
        session_id=session_id,
    )
    await broker.publish(topic, envelope)


async def publish_td_log(
    broker: _LogPublisher,
    api_id: int,
    message: str,
    *,
    source: str,
    level: str = "info",
    **extra: Any,
) -> None:
    """Fan out a log line for ``/ws/td/{api_id}``."""
    from mftik.protocol.messages import Log, LogEnvelope
    from mftik.protocol.topics import Topics

    topic = Topics.log_td(api_id)
    envelope = LogEnvelope.wrap(
        Log(level=level, message=message, **extra),
        type="log",
        source=source,
        session_id=str(api_id),
    )
    await broker.publish(topic, envelope)


async def publish_md_log(
    broker: _LogPublisher,
    venue: str,
    message: str,
    *,
    source: str,
    level: str = "info",
    instance: str | None = None,
    **extra: Any,
) -> None:
    """Fan out a log line for ``/ws/md/{venue}``.

    ``instance`` names the MD that wrote it, and is null when nothing can:
    an unpinned attach is one where the deploy did not choose, so the API has
    no name to put here. See :class:`Log`.
    """
    from mftik.protocol.messages import Log, LogEnvelope
    from mftik.protocol.topics import Topics

    topic = Topics.log_md(venue)
    envelope = LogEnvelope.wrap(
        Log(level=level, message=message, instance=instance, **extra),
        type="log",
        source=source,
        session_id=venue,
    )
    await broker.publish(topic, envelope)


# Backward-compatible alias (STS session logs).
async def publish_session_log(
    broker: _LogPublisher,
    session_id: str,
    message: str,
    *,
    source: str,
    level: str = "info",
    type: str | None = None,
    **extra: Any,
) -> None:
    await publish_sts_log(
        broker,
        session_id,
        message,
        source=source,
        level=level,
        type=type,
        **extra,
    )
