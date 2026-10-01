"""TD health-check RPC handler."""

from __future__ import annotations

from mftik.broker import IncomingRequest
from mftik.protocol import (
    TD_HEALTH,
    HealthStatus,
    HealthStatusEnvelope,
)


async def handle_health(req: IncomingRequest) -> None:
    """Reply to ``td.health`` with a simple ok status."""
    await req.reply(
        HealthStatusEnvelope.wrap(
            HealthStatus(status="ok", service="td"),
            type=TD_HEALTH,
            source="td",
            session_id=req.envelope.session_id,
        )
    )
