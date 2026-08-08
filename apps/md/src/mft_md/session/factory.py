"""Venue connector factories for MD — the only place venue names appear."""

from __future__ import annotations

import logging
from typing import Protocol

from mft.broker import Broker
from mft.exchange import PaperExchange, venues
from mft.exchange.errors import ExchangeError
from mft.exchange.gate.spot.public import GateSpotPublicClient
from mft.exchange.paper.remote_public import PaperRemotePublicClient
from mft.symbols import SymbolClient

from mft_md.session.venue import MarketDataConnector

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
    behaviour, ``Gate`` builds a real Gate market-data client. A venue
    that is in the registry but has no public client fails here rather than
    attaching a session that would never produce a tick.

    Venues do not all publish the same feeds — Gate serves all five topics,
    paper serves a subset — so a subscribe to a topic the venue has no stream
    for fails at ``ensure_feed``, which is where that difference belongs.
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
        raise ExchangeError(
            f"venue {resolved.name!r} is registered but MD has no public "
            f"client for it"
        )
