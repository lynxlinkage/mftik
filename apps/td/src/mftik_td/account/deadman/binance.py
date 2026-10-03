"""Binance spot.

F37's countdown list is UM and CM, per symbol. Spot is not on it.
This slot exists so the registry name resolves. B6-07 does not arm it.
"""

from __future__ import annotations

from mftik_td.account.deadman.base import DeadMansSwitch


class BinanceDeadMan(DeadMansSwitch):
    """Binance spot's slot. Not a countdown venue in F37."""

    venue = "Binance"
