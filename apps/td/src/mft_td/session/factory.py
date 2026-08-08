"""Session factories — build :class:`Session` for a given API id."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from mft.broker import Broker
from mft.exchange import PaperExchange, venues
from mft.exchange.errors import ExchangeError
from mft.exchange.gate.spot.private import GateSpotPrivateClient
from mft.exchange.paper.remote import PaperRemotePrivateClient
from mft.symbols import SymbolClient

from mft_td.session.session import Session

logger = logging.getLogger(__name__)

#: Loads the ``apis`` row for an api_id (``mft_td.db.get_api`` in production).
LoadApi = Callable[[int], Awaitable[Any]]


class SessionFactory(Protocol):
    """Creates a trading :class:`Session` for a given API credential id."""

    async def create(self, api_id: int) -> Session:
        """Build (but do not start) a session for ``api_id``."""


class PaperSessionFactory:
    """Session factory for the paper venue.

    * ``exchange`` set — in-process :class:`PaperExchange` (unit tests).
    * ``exchange`` omitted — :class:`PaperRemotePrivateClient` against the
      paper-engine Redis service (docker / production TD).
    """

    venue = venues.PAPER.name

    def __init__(
        self,
        broker: Broker,
        exchange: PaperExchange | None = None,
        *,
        key_prefix: str = "paper",
        symbols: SymbolClient | None = None,
    ) -> None:
        self._broker = broker
        self._exchange = exchange
        self._key_prefix = key_prefix
        # Optional: without a symbol plane the session simply cannot pre-lock,
        # which keeps paper usable in tests that run no `sym` service.
        self._symbols = symbols
        # api_id → (api_key, api_secret, passphrase)
        self._credentials: dict[int, tuple[str, str, str | None]] = {}

    @property
    def remote(self) -> bool:
        return self._exchange is None

    def bind_api(
        self,
        api_id: int,
        api_key: str,
        api_secret: str,
        *,
        passphrase: str | None = None,
        balances: dict | None = None,
    ) -> None:
        """Associate a DB api row with paper credentials."""
        if self._exchange is not None:
            self._exchange.register_api(
                api_key,
                api_secret,
                passphrase=passphrase,
                balances=balances,
            )
        self._credentials[api_id] = (api_key, api_secret, passphrase)
        logger.info(
            "PaperSessionFactory bound api_id=%s api_key=%s mode=%s",
            api_id,
            api_key,
            "remote" if self.remote else "local",
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
        if self._exchange is not None:
            private = self._exchange.private(
                api_key=api_key,
                api_secret=api_secret,
                passphrase=passphrase,
                auto_register=False,
            )
        else:
            private = PaperRemotePrivateClient(
                self._broker,
                api_key=api_key,
                api_secret=api_secret,
                passphrase=passphrase,
            )
        return Session(
            api_id=api_id,
            broker=self._broker,
            private=private,
            symbols=self._symbols,
        )


class VenueSessionFactory:
    """Dispatches on ``apis.venue`` to build the right private client.

    ``paper`` keeps the existing behaviour (in-process or paper-engine remote);
    ``Gate`` builds a real Gate client from the credential row. Unknown or
    unregistered venues fail here rather than producing a session that silently
    trades somewhere else.
    """

    def __init__(
        self,
        broker: Broker,
        *,
        load_api: LoadApi,
        paper: PaperSessionFactory | None = None,
        symbols: SymbolClient | None = None,
    ) -> None:
        self._broker = broker
        self._load_api = load_api
        # One client per TD process: its cache is what keeps symbol resolution
        # off the wire on the order path.
        self._symbols = symbols or SymbolClient(broker)
        self._paper = paper or PaperSessionFactory(broker, symbols=self._symbols)

    @property
    def paper(self) -> PaperSessionFactory:
        return self._paper

    async def create(self, api_id: int) -> Session:
        row = await self._load_api(api_id)
        if row is None:
            raise ExchangeError(f"no api credential for api_id={api_id}")

        venue = venues.require(row.venue)
        if venue is venues.PAPER:
            self._paper.bind_api(
                api_id,
                row.api_key,
                row.api_secret,
                passphrase=row.passphrase,
            )
            return await self._paper.create(api_id)

        if venue is venues.GATE:
            private = GateSpotPrivateClient(
                api_key=row.api_key,
                api_secret=row.api_secret,
                symbols=self._symbols,
            )
            logger.info(
                "TD building Gate session api_id=%s key=%s…",
                api_id,
                row.api_key[:6],
            )
            return Session(
                api_id=api_id,
                broker=self._broker,
                private=private,
                symbols=self._symbols,
            )

        raise ExchangeError(
            f"venue {venue.name!r} is registered but TD has no client for it"
        )
