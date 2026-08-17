"""MD health-check RPC handler."""

from __future__ import annotations

from typing import TYPE_CHECKING

from mftik.broker import IncomingRequest
from mftik.protocol import MD_HEALTH, HealthStatus, HealthStatusEnvelope

if TYPE_CHECKING:
    from mftik_md.session import SessionManager


async def handle_health(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    del sessions
    await req.reply(
        HealthStatusEnvelope.wrap(
            HealthStatus(status="ok", service="md"),
            type=MD_HEALTH,
            source="md",
            session_id=req.envelope.session_id,
        )
    )
