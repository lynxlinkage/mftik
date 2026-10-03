"""MD health-check RPC handler."""

from __future__ import annotations

from mftik.protocol import (
    MD_HEALTH,
    Envelope,
    HealthStatus,
    HealthStatusEnvelope,
    UntypedEnvelope,
)


async def handle_health(message: UntypedEnvelope) -> Envelope[HealthStatus]:
    """``md.health`` on the control subject: one message in, one reply out."""
    return HealthStatusEnvelope.wrap(
        HealthStatus(status="ok", service="md"),
        type=MD_HEALTH,
        source="md",
        session_id=message.session_id,
    )
