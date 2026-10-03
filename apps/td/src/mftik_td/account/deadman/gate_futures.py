"""Gate USD-M futures' countdown slot.

F37 names "Gate" and the registry splits spot (``Gate``) from futures
(``GateFutures``). This slot does not choose which of the two is armed.
B6-07 decides. This module does not call Gate.
"""

from __future__ import annotations

from mftik_td.account.deadman.base import DeadMansSwitch


class GateFuturesDeadMan(DeadMansSwitch):
    """Gate futures' slot. Whether it is armed is B6-07's."""

    venue = "GateFutures"
