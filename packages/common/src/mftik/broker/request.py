"""Request-reply request handle."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mftik.protocol import Envelope, UntypedEnvelope

if TYPE_CHECKING:
    from mftik.broker.client import Broker


class IncomingRequest:
    """A request waiting for a reply on a request-reply subject."""

    __slots__ = ("envelope", "_broker", "_replied")

    def __init__(self, broker: Broker, envelope: UntypedEnvelope) -> None:
        self.envelope = envelope
        self._broker = broker
        self._replied = False

    @property
    def replied(self) -> bool:
        return self._replied

    async def reply(self, envelope: Envelope[Any]) -> None:
        """Send a reply envelope to the requester's reply inbox, if there is one.

        A missing ``reply_to`` is not an error. A handler that always replies
        is safe when the requester is already gone.

        :attr:`replied` distinguishes the two afterwards: a handler that wants
        to skip building an answer nobody will read can check ``reply_to``
        itself first.
        """
        reply_to = self.envelope.reply_to
        if not reply_to:
            return
        await self._broker._send_reply(reply_to, envelope)
        self._replied = True
