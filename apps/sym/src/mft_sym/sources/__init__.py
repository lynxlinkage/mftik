"""Venue instrument sources. Venues are hardcoded here by design.

The symbol plane is the golden record, so its inputs are deliberately static:
adding a venue is a code change that goes through review, not a runtime
configuration that can drift.
"""

from mft.broker import Broker
from mft.exchange.tickers import Category

from mft_sym.sources.base import (
    Instrument,
    InstrumentSource,
    tick_from_precision,
)
from mft_sym.sources.binance import BinanceSpotInstrumentSource
from mft_sym.sources.bybit import BybitInstrumentSource
from mft_sym.sources.gate import GateSpotInstrumentSource
from mft_sym.sources.paper import PaperInstrumentSource


def default_sources(broker: Broker) -> list[InstrumentSource]:
    """Every venue the plane tracks.

    ``broker`` is only needed by venues reached over IPC rather than HTTP.

    Bybit appears twice, which is what a unified-account venue looks like here:
    one credential, but two listings that are fetched and delisted
    independently — see :mod:`mft_sym.sources.bybit`.
    """
    return [
        PaperInstrumentSource(broker),
        GateSpotInstrumentSource(),
        BinanceSpotInstrumentSource(),
        BybitInstrumentSource(category=Category.SPOT),
        BybitInstrumentSource(category=Category.PERP),
    ]


__all__ = [
    "BinanceSpotInstrumentSource",
    "BybitInstrumentSource",
    "GateSpotInstrumentSource",
    "Instrument",
    "InstrumentSource",
    "PaperInstrumentSource",
    "default_sources",
    "tick_from_precision",
]
