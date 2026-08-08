"""Connecting and disconnecting. Nothing about what a venue can do.

There is deliberately no ``PublicClient`` or ``PrivateClient`` here, and adding
either back would be a mistake worth arguing about first. Venues differ in what
they serve and how: which reads come over a socket and which need REST, which
feeds exist, whether an order can be found by the id we gave it. A shared
interface made those differences invisible at the type level and then leaked
them anyway — as optional methods that raise, and as consumers reaching around
the interface to ask whether an implementation was real.

That last part is the tell. TD had to write
``type(client).fetch_positions is not PrivateClient.fetch_positions`` to find
out whether a venue actually supported positions, because the interface
promised a method every venue had. A contract a caller must work around is not
one it can rely on.

So ``mft.exchange.<venue>`` offers a *connector per venue* whose methods happen
to resemble each other, not an implementation of a shared contract. A venue
that cannot do something has no method for it, which ``hasattr`` answers
honestly. What a consumer needs is the consumer's business to state: MD
declares the shape it drives feeds through, TD the shape it trades through, and
each composes a venue behind it.

What survives here is the part that really is universal — a client connects and
closes, and knows which of those it is. Order shapes that were methods on the
old interface are builders now, in :mod:`mft.exchange.models`: filling in
``type`` is the same job everywhere, so it belongs to nobody's implementation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from mft.exchange.errors import ExchangeNotConnectedError


class BaseClient(ABC):
    """Shared lifecycle for public and private venue clients."""

    name: str = "base"

    def __init__(self) -> None:
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected

    def _ensure_connected(self) -> None:
        if not self._connected:
            raise ExchangeNotConnectedError(
                f"{self.name} client is not connected; call connect() first"
            )

    @abstractmethod
    async def connect(self) -> None:
        """Establish connectivity to the venue."""

    @abstractmethod
    async def close(self) -> None:
        """Tear down connectivity and open streams."""

    async def __aenter__(self) -> BaseClient:
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()
