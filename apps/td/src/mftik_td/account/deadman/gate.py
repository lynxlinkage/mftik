"""Gate spot's countdown slot.

F37 names "Gate" among the countdown venues and does not say whether
that is this spot venue, ``GateFutures``, or both. This slot does not
choose. B6-07 decides which registry name it arms. This module does
not call Gate.
"""

from __future__ import annotations

from mftik_td.account.deadman.base import DeadMansSwitch


class GateDeadMan(DeadMansSwitch):
    """Gate spot's slot. Whether it is armed is B6-07's."""

    venue = "Gate"
