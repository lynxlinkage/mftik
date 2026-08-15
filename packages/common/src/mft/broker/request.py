"""Request-reply request handle."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mft.protocol import Envelope, UntypedEnvelope

if TYPE_CHECKING:
    from mft.broker.client import Broker


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

        A missing ``reply_to`` is not an error. It is what
        :meth:`~mft.broker.client.Broker.post` produces — the same queue and
        the same handlers as :meth:`~mft.broker.client.Broker.request`, minus
        anybody waiting — so a handler that always replies is exactly what
        makes a subject postable. Raising here would have meant every such
        handler needed a guard, and forgetting one would surface only on the
        posted path, after the work was already done.

        :attr:`replied` distinguishes the two afterwards: a handler that wants
        to skip building an answer nobody will read can check ``reply_to``
        itself first.
        """
        reply_to = self.envelope.reply_to
        if not reply_to:
            return
        await self._broker._send_reply(reply_to, envelope)
        self._replied = True
