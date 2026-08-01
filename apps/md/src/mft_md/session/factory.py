"""Venue public-client factories for MD."""

from __future__ import annotations

import logging
from typing import Protocol

from mft.broker import Broker
from mft.exchange import PaperExchange
from mft.exchange.base import PublicClient
from mft.exchange.paper.remote_public import PaperRemotePublicClient

logger = logging.getLogger(__name__)


class PublicClientFactory(Protocol):
    """Creates a :class:`PublicClient` for a venue name."""

    async def create(self, venue: str) -> PublicClient:
        """Build (but do not connect) a public client for ``venue``."""


class PaperPublicFactory:
    """Paper venue public client factory.

    * ``exchange`` set — in-process :class:`PaperExchange` (unit tests).
    * ``exchange`` omitted — :class:`PaperRemotePublicClient` (docker / prod).
    """

    venue = "paper"

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

    async def create(self, venue: str) -> PublicClient:
        if venue != self.venue:
            raise ValueError(f"unsupported md venue: {venue!r}")
        if self._exchange is not None:
            logger.info("MD paper public client mode=local")
            return self._exchange.public()
        logger.info("MD paper public client mode=remote")
        return PaperRemotePublicClient(self._broker)
