"""Generic message envelope — the wire wrapper for all broker messages.

Every envelope carries ``pv`` (:data:`~mftik.protocol.version.PROTOCOL_VERSION`).
That integer is the only state this package is the authority for (§3.3):
a code constant, compared by every receiver before the payload is parsed.
A different ``pv`` is refused with ``protocol_mismatch`` and is not
interpreted (F26). :func:`mftik.protocol.version.reject_if_pv_mismatch`
is that check. It is not applied inside :meth:`Envelope.from_json` —
parsing the payload of a message whose version you have not accepted is
the thing F26 forbids, so the receiver calls the check on the raw frame
first.

Usage::

    env = Envelope[Heartbeat].wrap(
        Heartbeat(status="ok"),
        type="heartbeat",
        source="md",
    )
    raw = env.to_json()
    restored = Envelope[Heartbeat].from_json(raw)
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Generic, Self, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from mftik.protocol.version import PROTOCOL_VERSION

PayloadT = TypeVar("PayloadT")


class Envelope(BaseModel, Generic[PayloadT]):
    """Typed wire envelope: metadata + payload of type ``PayloadT``.

    ``pv`` defaults to the version this process speaks, so a sender that
    goes through :meth:`wrap` stamps the current protocol without naming
    it. A receiver does not trust that default: a frame that omitted
    ``pv`` would gain it on the way through ``model_validate``, which is
    why the mismatch check reads the raw JSON.
    """

    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    type: str
    source: str
    #: Protocol version (F26). See :data:`mftik.protocol.version.PROTOCOL_VERSION`.
    pv: int = PROTOCOL_VERSION
    session_id: str | None = None
    reply_to: str | None = None
    ts: float = Field(default_factory=time.time)
    #: MD per-atom sequence (F25); set only by connection workers on ``md.a.*``.
    #: Optional, and ``pv`` is unchanged: a frame that omits ``seq`` is still
    #: this version. The field defaults to ``None`` for every other message.
    seq: int | None = None
    payload: PayloadT

    @classmethod
    def wrap(
        cls,
        payload: PayloadT,
        *,
        type: str,
        source: str,
        session_id: str | None = None,
        reply_to: str | None = None,
        seq: int | None = None,
    ) -> Self:
        """Build an envelope around a payload (infers ``Envelope[PayloadT]``).

        ``seq`` is the MD per-atom sequence. Callers other than a connection
        worker leave it unset.
        """
        return cls(
            type=type,
            source=source,
            session_id=session_id,
            reply_to=reply_to,
            seq=seq,
            payload=payload,
        )

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, data: str | bytes) -> Self:
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return cls.model_validate_json(data)


# Untyped wire form used when the payload schema is not known yet.
# Classic assignment (not `type X = ...`) so classmethods stay available.
UntypedEnvelope = Envelope[dict[str, Any]]
