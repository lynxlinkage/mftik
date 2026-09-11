"""What a transport owes the broker.

:class:`~mftik.broker.client.Broker` is the vocabulary a plane may speak.
The seam sits at *serialized envelopes*, not at store primitives. Strings,
not models, cross it. The families below are named for what a caller
wants: fan-out, request-reply, and the plumbing a fenced session link
sits on.

What stays above this line is envelope encoding,
:class:`~mftik.broker.request.IncomingRequest` and
:class:`~mftik.broker.link.LeasedSessionLink`.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence
from urllib.parse import urlsplit, urlunsplit


def redacted_url(url: str) -> str:
    """``url`` with its password replaced, for logging.

    Every store this speaks to takes its credential inline in the URL, and
    every service logs :meth:`BrokerTransport.describe` on every boot — so a
    password left in one lands in ``docker logs`` for the whole fleet and in
    anything those logs are shipped to.
    """
    try:
        parts = urlsplit(url)
        if not parts.password:
            return url
        host = parts.hostname or ""
        if parts.port is not None:
            host = f"{host}:{parts.port}"
        user = parts.username or ""
        return urlunsplit(
            (
                parts.scheme,
                f"{user}:***@{host}",
                parts.path,
                parts.query,
                parts.fragment,
            )
        )
    except ValueError:
        return "<unparseable url>"


class BrokerTransport(ABC):
    """One bus, answering the broker's vocabulary.

    Implementations live beside this file and are built through
    :func:`mftik.broker.transport.build`. One exists.

    Every method here is called by :class:`~mftik.broker.client.Broker` and by
    nothing else.
    """

    @abstractmethod
    async def connect(self) -> None:
        """Open the connection, and fail here if the store is unreachable."""

    @abstractmethod
    async def close(self) -> None:
        """Release the connection. Safe to call twice, and on a failed connect."""

    @abstractmethod
    def describe(self) -> str:
        """One line naming where this is connected, for the startup log.

        Credentials must already be redacted.
        """

    @abstractmethod
    async def publish(self, topic: str, raw: str) -> None:
        """Hand one message to every current subscriber of ``topic``.

        Best effort: a message published while nobody is subscribed is gone.
        """

    @abstractmethod
    def subscribe(
        self,
        topics: Sequence[str],
        *,
        stop: asyncio.Event | None,
        ready: asyncio.Event | None = None,
    ) -> AsyncIterator[tuple[str, str]]:
        """Yield ``(topic, raw)`` from ``topics`` until ``stop`` is set.

        The iterator does not start handing messages until pending SUBs
        have been written to this process's server. ``ready`` is set at
        that moment, before the first yield.
        """

    @abstractmethod
    def psubscribe(
        self, patterns: Sequence[str], *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str]]:
        """Yield ``(topic, raw)`` for topics matching ``patterns``.

        Patterns use one wildcard per segment — ``log.*.*``, never ``log.*``.
        """

    @abstractmethod
    def reply_inbox(self, request_id: str) -> str | None:
        """Where a reply to ``request_id`` should be addressed, if in-band.

        NATS carries a reply subject beside the message and answers ``None``.
        """

    @abstractmethod
    async def request(
        self,
        subject: str,
        raw: str,
        *,
        request_id: str,
        inbox: str | None,
        timeout: float,
    ) -> str:
        """Send ``raw`` and return the one reply, or time out."""

    @abstractmethod
    async def probe(
        self,
        subject: str,
        raw: str,
        *,
        request_id: str,
        inbox: str | None,
        timeout: float,
    ) -> str:
        """Ask whether anybody serves ``subject``, leaving nothing behind."""

    @abstractmethod
    def serve(
        self, subject: str, *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str | None]]:
        """Yield ``(raw, reply_inbox)`` for work on ``subject`` until ``stop``."""

    @abstractmethod
    async def send_reply(self, inbox: str, raw: str) -> None:
        """Answer a request on the ``inbox`` :meth:`serve` handed over."""
