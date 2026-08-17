"""STS health-check RPC handler."""

from __future__ import annotations

from typing import TYPE_CHECKING

from mftik.broker import IncomingRequest
from mftik.protocol import STS_HEALTH, HealthStatus, HealthStatusEnvelope

if TYPE_CHECKING:
    from mftik_sts.session import SessionManager


async def handle_health(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    del sessions
    await req.reply(
        HealthStatusEnvelope.wrap(
            HealthStatus(status="ok", service="sts"),
            type=STS_HEALTH,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )
