"""TD health-check RPC handler."""

from __future__ import annotations

from typing import TYPE_CHECKING

from mft.broker import IncomingRequest
from mft.protocol import (
    TD_HEALTH,
    HealthStatus,
    HealthStatusEnvelope,
)

if TYPE_CHECKING:
    from mft_td.session import SessionManager


async def handle_health(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    """Reply to ``td.health`` with a simple ok status."""
    del sessions  # unused for health
    await req.reply(
        HealthStatusEnvelope.wrap(
            HealthStatus(status="ok", service="td"),
            type=TD_HEALTH,
            source="td",
            session_id=req.envelope.session_id,
        )
    )
