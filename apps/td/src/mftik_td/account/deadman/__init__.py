"""One :class:`DeadMansSwitch` per registered venue.

:func:`deadman_for` is the lookup. Adding a venue to
:mod:`mftik.exchange.venues` without a slot here is a failed test, not
a silent miss at the first account of that venue.
"""

from __future__ import annotations

from mftik.exchange.venues import require

from mftik_td.account.deadman.base import DeadMansSwitch
from mftik_td.account.deadman.binance import BinanceDeadMan
from mftik_td.account.deadman.binance_cm import BinanceCmDeadMan
from mftik_td.account.deadman.binance_um import BinanceUmDeadMan
from mftik_td.account.deadman.bitget import BitgetDeadMan
from mftik_td.account.deadman.bybit import BybitDeadMan
from mftik_td.account.deadman.deribit import DeribitDeadMan
from mftik_td.account.deadman.gate import GateDeadMan
from mftik_td.account.deadman.gate_futures import GateFuturesDeadMan
from mftik_td.account.deadman.okx import OkxDeadMan
from mftik_td.account.deadman.paper import PaperDeadMan

#: Registry name → slot class. Keys are the canonical spellings in
#: :data:`mftik.exchange.venues.VENUES`.
SLOTS: dict[str, type[DeadMansSwitch]] = {
    slot.venue: slot
    for slot in (
        PaperDeadMan,
        GateDeadMan,
        GateFuturesDeadMan,
        BinanceDeadMan,
        BinanceUmDeadMan,
        BinanceCmDeadMan,
        BybitDeadMan,
        OkxDeadMan,
        BitgetDeadMan,
        DeribitDeadMan,
    )
}


def deadman_for(venue: str) -> DeadMansSwitch:
    """The countdown slot for ``venue``.

    The name is normalized the same way the venue registry is, so a row
    that says ``binanceum`` still finds :class:`BinanceUmDeadMan`. An
    unknown venue is :class:`~mftik.exchange.venues.UnknownVenueError`,
    not a default slot.
    """
    canonical = require(venue).name
    return SLOTS[canonical]()


__all__ = [
    "SLOTS",
    "BinanceCmDeadMan",
    "BinanceDeadMan",
    "BinanceUmDeadMan",
    "BitgetDeadMan",
    "BybitDeadMan",
    "DeadMansSwitch",
    "DeribitDeadMan",
    "GateDeadMan",
    "GateFuturesDeadMan",
    "OkxDeadMan",
    "PaperDeadMan",
    "deadman_for",
]
