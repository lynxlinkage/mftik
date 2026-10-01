"""Protocol version. The one piece of state this package is authority for.

§3.3 gives ``pv`` a single writer: the code constant below. It lives on
every envelope, and every receiver compares it before looking at the
payload. A difference is ``protocol_mismatch`` (F26). There is no
negotiation and no schema comparison — the number either matches or the
message is refused.

Stopping a worker does not use this. A Supervisor signals the process,
so a peer that speaks another version can still be stopped (§4.6).

IF-01 defines the constant, the envelope field, and the refusal code.
:func:`reject_if_pv_mismatch` is the check a receiver will call. It
raises ``NotImplementedError("IF-01")`` until B4-01 wires it; the
contract tests describe what it returns then.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mftik.protocol.messages import RpcError

#: Protocol v2, the first versioned envelope (F26). An envelope with no
#: ``pv`` is the previous, unversioned protocol and is not this version.
#: Bump this integer when the wire format changes. Do not keep reading a
#: message from the previous number.
PROTOCOL_VERSION = 2

#: ``RpcError.code`` a receiver sets when ``pv`` is not
#: :data:`PROTOCOL_VERSION`. The plane still answers with its own error
#: type (``sts.error``, ``td.error``, ``md.error``); this is the code
#: inside that payload, not a new envelope type (B0-03 §5.3).
PROTOCOL_MISMATCH = "protocol_mismatch"


def reject_if_pv_mismatch(raw: str | bytes) -> RpcError | None:
    """Refuse a message whose ``pv`` is not :data:`PROTOCOL_VERSION`.

    Look at ``pv`` and nothing else (F26: do not try to parse the
    payload). A JSON object whose ``pv`` is missing, a different
    integer, or not an integer is a mismatch. A JSON object whose
    ``pv`` is exactly :data:`PROTOCOL_VERSION` is not, even when the
    payload would not validate — validation is the caller's next step,
    and only then.

    A body that is not a JSON object is not a version mismatch. That is
    a malformed frame, and this function raises ``ValueError`` rather
    than reporting ``protocol_mismatch`` for it.

    The plane that received the message wraps the returned
    :class:`~mftik.protocol.messages.RpcError` in its own ``*.error``
    envelope. This function does not choose that type.

    Raises:
        NotImplementedError: always, with ``IF-01``. B4-01 is the
            implementation.
    """
    del raw
    raise NotImplementedError("IF-01")
