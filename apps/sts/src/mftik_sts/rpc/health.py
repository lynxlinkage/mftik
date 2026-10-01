"""STS health-check RPC handler."""

from __future__ import annotations

from mftik.broker import IncomingRequest
from mftik.protocol import STS_HEALTH, HealthStatus, HealthStatusEnvelope


async def handle_health(
    req: IncomingRequest,
    *,
    instance: str | None = None,
) -> None:
    del instance
    await req.reply(
        HealthStatusEnvelope.wrap(
            HealthStatus(status="ok", service="sts"),
            type=STS_HEALTH,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )
