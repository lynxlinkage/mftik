"""Venue connector factories for MD — the only place venue names appear."""

from __future__ import annotations

import logging
from typing import Protocol

from mftik.broker import Broker
from mftik.exchange import PaperExchange, venues
from mftik.exchange.binance.future.public import BinanceFuturePublicClient
from mftik.exchange.binance.spot.public import BinanceSpotPublicClient
from mftik.exchange.bybit.public import BybitPublicClient
from mftik.exchange.errors import ExchangeError
from mftik.exchange.gate.spot.public import GateSpotPublicClient
from mftik.exchange.paper.remote_public import PaperRemotePublicClient
from mftik.symbols import SymbolClient

from mftik_md.session.venue import MarketDataConnector

logger = logging.getLogger(__name__)


class ConnectorFactory(Protocol):
    """Builds the market-data connector for a venue name.

    The only place in MD that knows venue names. Every venue difference —
    which transports it needs, which feeds it has — is settled by picking a
    connector here, so nothing downstream has to branch on the venue again.
    """

    async def create(self, venue: str) -> MarketDataConnector:
        """Build (but do not connect) the connector for ``venue``."""


class PaperPublicFactory:
    """Paper venue public client factory.

    * ``exchange`` set — in-process :class:`PaperExchange` (unit tests).
    * ``exchange`` omitted — :class:`PaperRemotePublicClient` (docker / prod).
    """

    venue = venues.PAPER.name

    def __init__(
        self,
        broker: Broker,
        exchange: PaperExchange | None = None,
    ) -> None:
        self._broker = broker
        self._exchange = exchange

    @property
    def remote(self) -> bool:
        return self._exchange is None

    async def create(self, venue: str) -> MarketDataConnector:
        if venue != self.venue:
            raise ValueError(f"unsupported md venue: {venue!r}")
        if self._exchange is not None:
            logger.info("MD paper public client mode=local")
            return self._exchange.public()
        logger.info("MD paper public client mode=remote")
        return PaperRemotePublicClient(self._broker)


class VenuePublicFactory:
    """Dispatches on the feed's venue to build the right public client.

    Mirrors TD's ``VenueSessionFactory``: ``Paper`` keeps the existing
    behaviour, and the real venues build real market-data clients. A venue that
    is in the registry but has no public client fails here rather than
    attaching a session that would never produce a tick.

    Venues do not all publish the same feeds — Gate and Binance serve all five
    topics, ``BinanceFuture`` and Bybit add liquidations, paper serves a subset
    — so a subscribe to a topic the venue has no stream for fails at
    ``ensure_feed``, which is where that difference belongs.

    One client per venue, including Bybit: its category-per-socket shape is
    the connector's business, not this factory's. ``BybitPublicClient`` opens a
    socket per category on first use, so a session streaming only spot never
    holds one to the perp book.

    Neither real client is given credentials. Market data is open at both
    venues, and a feed that needed a trading account would make MD wait on one.
    """

    def __init__(
        self,
        broker: Broker,
        *,
        paper: PaperPublicFactory | None = None,
        symbols: SymbolClient | None = None,
    ) -> None:
        self._broker = broker
        self._paper = paper or PaperPublicFactory(broker)
        # One client per MD process: its cache keeps symbol resolution off the
        # wire when feeds start.
        self._symbols = symbols or SymbolClient(broker)

    @property
    def paper(self) -> PaperPublicFactory:
        return self._paper

    async def create(self, venue: str) -> MarketDataConnector:
        resolved = venues.require(venue)
        if resolved is venues.PAPER:
            return await self._paper.create(resolved.name)
        if resolved is venues.GATE:
            logger.info("MD building Gate public client")
            return GateSpotPublicClient(symbols=self._symbols)
        if resolved is venues.BINANCE:
            logger.info("MD building Binance public client")
            return BinanceSpotPublicClient(symbols=self._symbols)
        if resolved is venues.BINANCE_FUTURE:
            logger.info("MD building BinanceFuture public client")
            return BinanceFuturePublicClient(symbols=self._symbols)
        if resolved is venues.BYBIT:
            logger.info("MD building Bybit public client")
            return BybitPublicClient(symbols=self._symbols)
        raise ExchangeError(
            f"venue {resolved.name!r} is registered but MD has no public "
            f"client for it"
        )
