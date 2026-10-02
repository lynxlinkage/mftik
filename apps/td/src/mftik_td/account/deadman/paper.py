"""Paper has no venue and no countdown.

The slot exists so every registered venue resolves to one. F37's list
does not include it. Nothing here places an order.
"""

from __future__ import annotations

from mftik_td.account.deadman.base import DeadMansSwitch


class PaperDeadMan(DeadMansSwitch):
    """Paper's slot. There is no countdown to arm."""

    venue = "Paper"
