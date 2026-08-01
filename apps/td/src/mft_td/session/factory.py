"""Session factories — build :class:`Session` for a given API id."""

from __future__ import annotations

import logging
from typing import Protocol

from mft.broker import Broker
from mft.exchange import PaperExchange

from mft_td.session.session import Session

logger = logging.getLogger(__name__)


class SessionFactory(Protocol):
    """Creates a trading :class:`Session` for an API credential id."""

    async def create(self, api_id: int) -> Session:
        """Build (but do not start) a session for ``api_id``."""


class PaperSessionFactory:
    """Session factory backed by :class:`PaperExchange`.

    Each ``api_id`` maps to a paper ``api_key`` / ``api_secret`` pair. Distinct
    keys are isolated accounts on the shared paper venue.
    """

    venue = "paper"

    def __init__(
        self,
        broker: Broker,
        exchange: PaperExchange,
        *,
        key_prefix: str = "paper",
    ) -> None:
        self._broker = broker
        self._exchange = exchange
        self._key_prefix = key_prefix
        # api_id → (api_key, api_secret, passphrase)
        self._credentials: dict[int, tuple[str, str, str | None]] = {}

    def bind_api(
        self,
        api_id: int,
        api_key: str,
        api_secret: str,
        *,
        passphrase: str | None = None,
    ) -> None:
        """Associate a DB api row with paper credentials and register the account."""
        self._exchange.register_api(
            api_key, api_secret, passphrase=passphrase
        )
        self._credentials[api_id] = (api_key, api_secret, passphrase)
        logger.info(
            "PaperSessionFactory bound api_id=%s api_key=%s", api_id, api_key
        )

    def credentials_for(self, api_id: int) -> tuple[str, str, str | None]:
        """Return (api_key, api_secret, passphrase), synthesizing if unbound."""
        existing = self._credentials.get(api_id)
        if existing is not None:
            return existing
        api_key = f"{self._key_prefix}-key-{api_id}"
        api_secret = f"{self._key_prefix}-secret-{api_id}"
        self.bind_api(api_id, api_key, api_secret)
        return api_key, api_secret, None

    async def create(self, api_id: int) -> Session:
        api_key, api_secret, passphrase = self.credentials_for(api_id)
        private = self._exchange.private(
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            auto_register=False,
        )
        return Session(api_id=api_id, broker=self._broker, private=private)
