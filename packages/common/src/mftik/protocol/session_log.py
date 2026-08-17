"""Publish structured lines to domain log channels (API WebSocket bridges)."""

from __future__ import annotations

from typing import Any, Protocol


class _LogPublisher(Protocol):
    async def publish(self, topic: str, envelope: Any) -> int: ...

    async def publish_log(self, topic: str, envelope: Any, **kwargs: Any) -> int: ...


async def publish_sts_log(
    broker: _LogPublisher,
    session_id: str,
    message: str,
    *,
    source: str,
    level: str = "info",
    **extra: Any,
) -> None:
    """Fan out a log line for ``/ws/sts/{session_id}`` (buffered + live)."""
    from mftik.protocol.messages import Log, LogEnvelope
    from mftik.protocol.topics import Topics

    topic = Topics.log_sts(session_id)
    envelope = LogEnvelope.wrap(
        Log(level=level, message=message, **extra),
        type="log",
        source=source,
        session_id=session_id,
    )
    publish_log = getattr(broker, "publish_log", None)
    if publish_log is not None:
        await publish_log(topic, envelope)
    else:
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
    """Fan out a log line for ``/ws/td/{api_id}`` (buffered + live)."""
    from mftik.protocol.messages import Log, LogEnvelope
    from mftik.protocol.topics import Topics

    topic = Topics.log_td(api_id)
    envelope = LogEnvelope.wrap(
        Log(level=level, message=message, **extra),
        type="log",
        source=source,
        session_id=str(api_id),
    )
    publish_log = getattr(broker, "publish_log", None)
    if publish_log is not None:
        await publish_log(topic, envelope)
    else:
        await broker.publish(topic, envelope)


async def publish_md_log(
    broker: _LogPublisher,
    venue: str,
    message: str,
    *,
    source: str,
    level: str = "info",
    **extra: Any,
) -> None:
    """Fan out a log line for ``/ws/md/{venue}`` (buffered + live)."""
    from mftik.protocol.messages import Log, LogEnvelope
    from mftik.protocol.topics import Topics

    topic = Topics.log_md(venue)
    envelope = LogEnvelope.wrap(
        Log(level=level, message=message, **extra),
        type="log",
        source=source,
        session_id=venue,
    )
    publish_log = getattr(broker, "publish_log", None)
    if publish_log is not None:
        await publish_log(topic, envelope)
    else:
        await broker.publish(topic, envelope)


# Backward-compatible alias (STS session logs).
async def publish_session_log(
    broker: _LogPublisher,
    session_id: str,
    message: str,
    *,
    source: str,
    level: str = "info",
    **extra: Any,
) -> None:
    await publish_sts_log(
        broker,
        session_id,
        message,
        source=source,
        level=level,
        **extra,
    )
