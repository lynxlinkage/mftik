"""MD health-check RPC handler."""

from __future__ import annotations

from typing import TYPE_CHECKING

from mftik.broker import IncomingRequest
from mftik.protocol import MD_HEALTH, HealthStatus, HealthStatusEnvelope

if TYPE_CHECKING:
    from mftik_md.tape_store import TapeStore


async def handle_health(
    req: IncomingRequest,
    *,
    store: TapeStore | None = None,
) -> None:
    del store
    await req.reply(
        HealthStatusEnvelope.wrap(
            HealthStatus(status="ok", service="md"),
            type=MD_HEALTH,
            source="md",
            session_id=req.envelope.session_id,
        )
    )
