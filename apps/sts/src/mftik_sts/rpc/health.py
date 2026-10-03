"""STS health-check RPC handler."""

from __future__ import annotations

from mftik.protocol import (
    STS_HEALTH,
    Envelope,
    HealthStatus,
    UntypedEnvelope,
)


async def handle_health(message: UntypedEnvelope) -> Envelope[HealthStatus]:
    """``sts.health`` on the control subject: one message in, one reply out."""
    return Envelope[HealthStatus].wrap(
        HealthStatus(status="ok", service="sts"),
        type=STS_HEALTH,
        source="sts",
        session_id=message.session_id,
    )
