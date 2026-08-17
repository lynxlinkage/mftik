"""Session factories — build :class:`Session` for a given API id."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from mftik.broker import Broker
from mftik.exchange import PaperExchange, venues
from mftik.exchange.binance.future.private import BinanceFuturePrivateClient
from mftik.exchange.binance.spot.private import BinanceSpotPrivateClient
from mftik.exchange.bybit.private import BybitPrivateClient
from mftik.exchange.errors import ExchangeError
from mftik.exchange.gate.spot.private import GateSpotPrivateClient
from mftik.exchange.paper.remote import PaperRemotePrivateClient
from mftik.symbols import SymbolClient

from mftik_td.session.session import Session

logger = logging.getLogger(__name__)

#: Loads the ``apis`` row for an api_id (``mftik_td.db.get_api`` in production).
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
            return self._session(api_id, private)

        if venue is venues.BYBIT:
            # Bybit is a unified account: this one connector holds the order
            # socket, the account stream and REST for the whole credential.
            #
            # ``category`` says which book *orders* go to, and defaults to
            # spot because ``OrderSubmit`` still carries a bare symbol — see
            # ``Session._instrument``. It does not narrow what the session
            # reports: the account stream is unscoped, so perp fills and perp
            # positions reach recon and the OMS either way, which is what
            # makes them true of the account rather than of one book.
            private = BybitPrivateClient(
                api_key=row.api_key,
                api_secret=row.api_secret,
                symbols=self._symbols,
            )
            logger.info(
                "TD building Bybit session api_id=%s key=%s…",
                api_id,
                row.api_key[:6],
            )
            return self._session(api_id, private)

        if venue is venues.BINANCE:
            # ``api_secret`` is the Ed25519 private key, not a shared secret.
            # It is parsed here, at construction, so a malformed credential
            # fails the attach rather than the first order.
            private = BinanceSpotPrivateClient(
                api_key=row.api_key,
                api_secret=row.api_secret,
                symbols=self._symbols,
            )
            logger.info(
                "TD building Binance session api_id=%s key=%s…",
                api_id,
                row.api_key[:6],
            )
            return self._session(api_id, private)

        if venue is venues.BINANCE_FUTURE:
            # The same Ed25519 credential shape as spot, and a different key:
            # Binance's USDⓈ-M plane is a separate account with separate API
            # keys, so a spot credential stored against this venue would fail
            # its logon rather than trade the wrong book.
            #
            # This connector also reports positions, which no spot session
            # does — TD picks them up by name, so nothing here has to say so.
            private = BinanceFuturePrivateClient(
                api_key=row.api_key,
                api_secret=row.api_secret,
                symbols=self._symbols,
            )
            logger.info(
                "TD building BinanceFuture session api_id=%s key=%s…",
                api_id,
                row.api_key[:6],
            )
            return self._session(api_id, private)

        raise ExchangeError(
            f"venue {venue.name!r} is registered but TD has no client for it"
        )

    def _session(self, api_id: int, private: Any) -> Session:
        return Session(
            api_id=api_id,
            broker=self._broker,
            private=private,
            symbols=self._symbols,
        )
