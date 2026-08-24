"""Venue instrument sources. Venues are hardcoded here by design.

The symbol plane is the golden record, so its inputs are deliberately static:
adding a venue is a code change that goes through review, not a runtime
configuration that can drift.
"""

from mftik.broker import Broker
from mftik.exchange.tickers import Category

from mftik_sym.sources.base import (
    Instrument,
    InstrumentSource,
    tick_from_precision,
)
from mftik_sym.sources.binance import BinanceSpotInstrumentSource
from mftik_sym.sources.binance_future import BinanceFutureInstrumentSource
from mftik_sym.sources.bybit import BybitInstrumentSource
from mftik_sym.sources.gate import GateSpotInstrumentSource
from mftik_sym.sources.gate_future import GateFuturesInstrumentSource
from mftik_sym.sources.okx import OkxInstrumentSource
from mftik_sym.sources.paper import PaperInstrumentSource


def default_sources(broker: Broker) -> list[InstrumentSource]:
    """Every venue the plane tracks.

    ``broker`` is only needed by venues reached over IPC rather than HTTP.

    Bybit and OKX appear twice, which is what a unified-account venue looks
    like here: one credential, but two listings that are fetched and delisted
    independently — see :mod:`mftik_sym.sources.bybit` and
    :mod:`mftik_sym.sources.okx`.

    Binance appears twice too, and for the opposite reason: ``Binance`` and
    ``BinanceFuture`` are two venues with two credentials and two listing
    endpoints, so their sources share nothing but a brand. ``Gate`` /
    ``GateFutures`` is the same split.
    """
    return [
        PaperInstrumentSource(broker),
        GateSpotInstrumentSource(),
        GateFuturesInstrumentSource(),
        BinanceSpotInstrumentSource(),
        BinanceFutureInstrumentSource(),
        BybitInstrumentSource(category=Category.SPOT),
        BybitInstrumentSource(category=Category.PERP),
        OkxInstrumentSource(category=Category.SPOT),
        OkxInstrumentSource(category=Category.PERP),
    ]


__all__ = [
    "BinanceFutureInstrumentSource",
    "BinanceSpotInstrumentSource",
    "BybitInstrumentSource",
    "GateFuturesInstrumentSource",
    "GateSpotInstrumentSource",
    "Instrument",
    "InstrumentSource",
    "OkxInstrumentSource",
    "PaperInstrumentSource",
    "default_sources",
    "tick_from_precision",
]
