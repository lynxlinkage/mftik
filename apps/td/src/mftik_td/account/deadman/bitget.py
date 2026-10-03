"""Bitget unified-account countdown (F37).

B6-07 arms this. The plan names Bitget UTA, which is the account this
adapter already talks to. This module does not call Bitget.
"""

from __future__ import annotations

from mftik_td.account.deadman.base import DeadMansSwitch


class BitgetDeadMan(DeadMansSwitch):
    """Bitget's slot. UTA countdown, not a per-symbol one."""

    venue = "Bitget"
