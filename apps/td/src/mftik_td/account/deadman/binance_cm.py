"""Binance COIN-M countdown cancel, per symbol (F37).

B6-07 arms this. Same shape as USD-M: one countdown per symbol, refreshed
while the trading layer is up and orders on that symbol are resting.
This module does not call Binance.
"""

from __future__ import annotations

from mftik_td.account.deadman.base import DeadMansSwitch


class BinanceCmDeadMan(DeadMansSwitch):
    """Binance COIN-M's slot. The countdown is per symbol."""

    venue = "BinanceCM"
