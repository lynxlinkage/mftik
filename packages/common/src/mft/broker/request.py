"""Request-reply request handle."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mft.broker.errors import ReplyError
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
        """Send a reply envelope to the requester's reply inbox."""
        reply_to = self.envelope.reply_to
        if not reply_to:
            raise ReplyError(
                f"request {self.envelope.id} has no reply_to; cannot reply"
            )
        await self._broker._send_reply(reply_to, envelope)
        self._replied = True
