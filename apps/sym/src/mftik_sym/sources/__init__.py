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
from mftik_sym.sources.binance_delivery import BinanceDeliveryInstrumentSource
from mftik_sym.sources.binance_future import BinanceFutureInstrumentSource
from mftik_sym.sources.bitget import BitgetInstrumentSource
from mftik_sym.sources.bybit import BybitInstrumentSource
from mftik_sym.sources.deribit import DeribitInstrumentSource
from mftik_sym.sources.gate import GateSpotInstrumentSource
from mftik_sym.sources.gate_future import GateFuturesInstrumentSource
from mftik_sym.sources.okx import OkxInstrumentSource
from mftik_sym.sources.paper import PaperInstrumentSource


def default_sources(broker: Broker) -> list[InstrumentSource]:
    """Every venue the plane tracks.

    ``broker`` is only needed by venues reached over IPC rather than HTTP.

    Bybit, OKX, Bitget and Deribit appear twice, which is what a
    unified-account venue looks like here: one credential, but two
    listings that are fetched and delisted independently — see
    :mod:`mftik_sym.sources.bybit`, :mod:`mftik_sym.sources.okx`,
    :mod:`mftik_sym.sources.bitget` and :mod:`mftik_sym.sources.deribit`.
    Bitget's Perp source is itself a union of two wire categories; a
    second Perp source would deactivate the first. Deribit's Perp source
    is ``kind=future`` filtered to linear perpetuals.

    Binance appears as three venues — ``Binance``, ``BinanceUM`` and
    ``BinanceCM`` — because those are three credentials and three
    listing endpoints. ``BinanceUM`` and ``BinanceCM`` then
    appear twice more, the way Bybit does: one ``exchangeInfo`` mixes
    perpetuals and dated futures, and a refresh must delist each book
    independently.
    ``Gate`` / ``GateFutures`` is the classic split with no dated book yet.
    """
    return [
        PaperInstrumentSource(broker),
        GateSpotInstrumentSource(),
        GateFuturesInstrumentSource(),
        BinanceSpotInstrumentSource(),
        BinanceFutureInstrumentSource(category=Category.PERP),
        BinanceFutureInstrumentSource(category=Category.FUTURE),
        BinanceDeliveryInstrumentSource(category=Category.INVERSE),
        BinanceDeliveryInstrumentSource(category=Category.FUTURE),
        BybitInstrumentSource(category=Category.SPOT),
        BybitInstrumentSource(category=Category.PERP),
        OkxInstrumentSource(category=Category.SPOT),
        OkxInstrumentSource(category=Category.PERP),
        BitgetInstrumentSource(category=Category.SPOT),
        BitgetInstrumentSource(category=Category.PERP),
        DeribitInstrumentSource(category=Category.SPOT),
        DeribitInstrumentSource(category=Category.PERP),
    ]


__all__ = [
    "BinanceDeliveryInstrumentSource",
    "BinanceFutureInstrumentSource",
    "BinanceSpotInstrumentSource",
    "BitgetInstrumentSource",
    "BybitInstrumentSource",
    "DeribitInstrumentSource",
    "GateFuturesInstrumentSource",
    "GateSpotInstrumentSource",
    "Instrument",
    "InstrumentSource",
    "OkxInstrumentSource",
    "PaperInstrumentSource",
    "default_sources",
    "tick_from_precision",
]
