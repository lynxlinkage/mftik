"""Bybit's slot. DCP is not used (F37).

Bybit's disconnect-cancel is offered to institutional accounts and is
not the countdown this layer refreshes. This slot must not call it.
The class exists so a Bybit ``api_id`` still resolves to a slot.
"""

from __future__ import annotations

from mftik_td.account.deadman.base import DeadMansSwitch


class BybitDeadMan(DeadMansSwitch):
    """Bybit's slot. Not DCP."""

    venue = "Bybit"
