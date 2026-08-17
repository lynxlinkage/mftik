"""Normalized reject codes for order requests that did not go through.

Every venue has its own vocabulary for saying no. Gate answers
``BALANCE_NOT_ENOUGH``, the paper engine raises ``InsufficientBalanceError``,
the next venue will spell it a third way — and TD's own refusals are not in
any of those vocabularies at all. A strategy that wants to tell "I cannot
afford this" from "the venue does not like my price" should not have to learn
all of them.

The code answers the only question a strategy really asks of a refusal:
*whose problem is this, and can trying again possibly help?* Three bands:

``100``–``199`` — TD refused it, and nothing was sent
    No venue event will ever follow, and the condition is a standing one: the
    session is not attached, the funds are not there. A strategy that retries
    one of these on a timer retries it forever.

``200``–``299`` — the venue refused it, in a way we recognise
    The same meaning across venues; the venue's own words are still in
    ``reason``. Whether a retry helps depends on the code — a crossed
    post-only wants a new price, a rate limit wants a pause.

anything else — a venue reject with no mapping yet
    The wire carries the venue's native code verbatim, an ``int`` or a label
    ``str``, so the detail survives until the table catches up. Reading one is
    fine; just expect it to turn into a ``2xx`` once it is common enough to
    map, so treat it as a venue reject you do not understand rather than as a
    stable contract.

:func:`is_td_internal` and :func:`is_venue` are the band tests; use them
rather than comparing against the numbers by hand.

Letting native codes through as themselves has one caveat: a venue whose own
numbering falls inside ``100``–``299`` would be indistinguishable from a code
this platform assigned. What keeps that from happening is mapping that
venue's codes rather than leaving them to fall through — the table in
``mftik_td.errors`` is where that is done, and it is cheap to extend.

Codes cross the wire as plain ints, so compare with ``==``, never ``is``::

    if reject.error_code == RejectCode.VENUE_POST_ONLY_WOULD_CROSS:
        ...
"""

from __future__ import annotations

from enum import IntEnum

#: First code in each band, and one past the end of the normalized range.
TD_BAND = 100
VENUE_BAND = 200
BAND_END = 300


class RejectCode(IntEnum):
    """Why an order request failed, in terms every venue shares."""

    #: No refusal — the request was taken. The default on every payload.
    NONE = 0

    # --- 1xx: refused inside TD, never sent to the venue -------------------

    #: TD broke on its own account: an unclassified internal failure.
    TD_INTERNAL = 100
    #: The payload was not a well-formed order request.
    TD_INVALID_REQUEST = 101
    #: The request named an api_id this TD does not serve.
    TD_WRONG_API_ID = 102
    #: The STS session holds no attach on this api_id.
    TD_SESSION_NOT_ATTACHED = 103
    #: TD's pre-lock found the free balance short. TD's ledger said no; the
    #: venue was never asked.
    TD_INSUFFICIENT_BALANCE = 104
    #: TD could not persist the order before sending, so it did not send.
    TD_STATE_WRITE_FAILED = 105
    #: Cancel refused locally: the order is PENDING_NEW (no venue id to cancel
    #: against) or already finished.
    TD_NOT_CANCELABLE = 106
    #: The venue client is not connected, so the call never left the process.
    TD_VENUE_NOT_CONNECTED = 107
    #: STS got no ack: no TD is serving the account, or it is wedged.
    TD_NO_ACK = 108
    #: An ack came back that STS could not read.
    TD_UNREADABLE_ACK = 109
    #: The request type is not one TD's order plane answers.
    TD_UNSUPPORTED_REQUEST = 110
    #: The send itself failed — a dead socket, a timeout. Outcome at the venue
    #: is unknown: TD marks the order UNKNOWN and resolves it; strategies must
    #: not treat this as a determined reject or a successful cancel.
    TD_SEND_FAILED = 111
    #: The order named an instrument this session's venue does not trade —
    #: another venue's ticker, or a market this one has no book for. Refused
    #: before the pre-lock, because nothing about it can be made to work.
    TD_WRONG_INSTRUMENT = 112
    #: Leverage for this instrument could not be read — spot ticker, a venue
    #: without a leverage API, or the venue answered without a usable figure.
    TD_LEVERAGE_UNAVAILABLE = 113
    #: ``reduce_only`` was asked for on a spot order. Spot has no position to
    #: reduce, so there is nothing the flag could mean and no venue would
    #: honour it. Refused rather than dropped: a caller sets it to be sure an
    #: order cannot open exposure, and silently ignoring it would leave that
    #: caller believing in a guarantee it does not have.
    TD_REDUCE_ONLY_UNSUPPORTED = 114

    # --- 2xx: the venue said no, in a way we recognise ---------------------

    #: The venue refused it and gave nothing machine-readable to go on.
    VENUE_REJECTED = 200
    #: Free balance at the venue could not cover the order.
    VENUE_INSUFFICIENT_BALANCE = 201
    #: A post-only order would have crossed and taken liquidity, so the venue
    #: refused it rather than fill it. Reprice and send again.
    VENUE_POST_ONLY_WOULD_CROSS = 202
    #: The calling IP is not on the credential's whitelist.
    VENUE_IP_NOT_WHITELISTED = 203
    #: The credential did not authenticate: bad key, bad signature, stale
    #: timestamp.
    VENUE_AUTH_FAILED = 204
    #: Authenticated, but this key may not do this — a read-only key placing
    #: an order, a disabled account.
    VENUE_PERMISSION_DENIED = 205
    #: Too many requests. Back off and retry.
    VENUE_RATE_LIMITED = 206
    #: The venue has no such order. On a cancel this usually means it is
    #: already gone.
    VENUE_ORDER_NOT_FOUND = 207
    #: The order exists but is finished, so there was nothing left to act on.
    VENUE_ORDER_ALREADY_CLOSED = 208
    #: The client_order_id has been used before on this account.
    VENUE_DUPLICATE_CLIENT_ORDER_ID = 209
    #: The venue rejected a field: unknown symbol, price off tick, size off
    #: step, a missing parameter.
    VENUE_INVALID_PARAM = 210
    #: Below a minimum the venue enforces — min qty or min notional.
    VENUE_BELOW_MINIMUM = 211
    #: The instrument is not tradeable right now: delisted, halted, unknown.
    VENUE_SYMBOL_NOT_TRADABLE = 212
    #: A limit on the account or position stopped it — risk limit, leverage,
    #: trading restricted.
    VENUE_RISK_LIMIT = 213
    #: The venue broke on its own account: internal error, overloaded.
    VENUE_INTERNAL_ERROR = 214


def is_normalized(code: int | str) -> bool:
    """Whether ``code`` is one this platform assigned rather than a venue's."""
    return isinstance(code, int) and TD_BAND <= code < BAND_END


def is_td_internal(code: int | str) -> bool:
    """Whether TD refused it — meaning nothing reached the venue.

    The one test worth making on a refusal: nothing will follow it, and the
    same request will be refused the same way until something changes.
    """
    return isinstance(code, int) and TD_BAND <= code < VENUE_BAND


def is_venue(code: int | str) -> bool:
    """Whether the venue refused it, mapped or not.

    True for the ``2xx`` band and for every native code, since a venue is the
    only thing that produces one.
    """
    if isinstance(code, str):
        return bool(code)
    return code != RejectCode.NONE and not is_td_internal(code)


def describe(code: int | str) -> str:
    """``code`` as something readable in a log line.

    Normalized codes render as ``201 VENUE_INSUFFICIENT_BALANCE``; a native
    code renders as itself, since its name is whatever the venue calls it.
    """
    if isinstance(code, int):
        try:
            return f"{code} {RejectCode(code).name}"
        except ValueError:
            return str(code)
    return code


__all__ = [
    "BAND_END",
    "TD_BAND",
    "VENUE_BAND",
    "RejectCode",
    "describe",
    "is_normalized",
    "is_td_internal",
    "is_venue",
]
