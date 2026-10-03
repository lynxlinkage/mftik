"""TD health-check RPC handler."""

from __future__ import annotations

from mftik.protocol import (
    TD_HEALTH,
    Envelope,
    HealthStatus,
    HealthStatusEnvelope,
    UntypedEnvelope,
)


async def handle_health(message: UntypedEnvelope) -> Envelope[HealthStatus]:
    """``td.health`` on the control subject: one message in, one reply out."""
    return HealthStatusEnvelope.wrap(
        HealthStatus(status="ok", service="td"),
        type=TD_HEALTH,
        source="td",
        session_id=message.session_id,
    )
