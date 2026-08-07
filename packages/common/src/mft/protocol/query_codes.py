"""Normalized codes for market-data queries that came back without data.

The sibling of :mod:`mft.protocol.reject_codes`, deliberately kept apart from
it. That module's bands are documented in terms of orders — what reached the
venue, what a retry would do to an order — and a query shares none of that:
nothing is at stake if a query is refused, and the recovery is to ask
differently rather than to stop asking. Folding queries into ``RejectCode``
would have meant widening ``BAND_END`` and blunting ``is_td_internal`` until
neither statement was quite true of either caller.

Two bands, same shape as its sibling so the reading transfers:

``100``–``199`` — MD refused it, and the venue was never asked
    Either nothing is there to serve the query, or the request could not be
    answered as written. Nothing will follow, and the condition holds until
    something changes — a client connects, or the caller asks for a different
    interval.

``200``–``299`` — the venue answered, and the answer was no
    Recognised across venues; the venue's own words stay in ``reason``.

anything else — a venue error with no mapping yet
    The venue's native code, ``int`` or label ``str``, verbatim. Fine to read;
    expect it to become a ``2xx`` once it is common enough to map.

Whose fault it was and whether to try again are different questions, and the
bands only answer the first. :func:`is_retryable` answers the second, and it
cuts across both bands — a rate limit is the venue's ``2xx`` but worth
retrying, while an unsupported interval is MD's ``1xx`` and never will be.
Use it rather than inferring a retry policy from the band.

Codes cross the wire as plain ints, so compare with ``==``, never ``is``::

    if result.error_code == QueryCode.VENUE_RATE_LIMITED:
        ...
"""

from __future__ import annotations

from enum import IntEnum

#: First code in each band, and one past the end of the normalized range.
MD_BAND = 100
VENUE_BAND = 200
BAND_END = 300


class QueryCode(IntEnum):
    """Why a market-data query returned nothing."""

    #: No failure — the query was answered. The default on every payload.
    NONE = 0

    # --- 1xx: refused inside MD, the venue was never asked -----------------

    #: MD broke on its own account: an unclassified internal failure.
    MD_INTERNAL = 100
    #: The payload was not a well-formed query.
    MD_INVALID_REQUEST = 101
    #: The request type is not one MD's query plane answers.
    MD_UNSUPPORTED_REQUEST = 102
    #: No connected venue client to ask, so nothing was sent.
    MD_VENUE_NOT_CONNECTED = 103
    #: The venue serves no history for this instrument class at all — the
    #: paper engine invents prices tick by tick and keeps no past. Distinct
    #: from an empty answer on purpose: "cannot ask" is not "asked, got none".
    MD_VENUE_NO_HISTORY = 104
    #: The venue does not serve candles at this interval. Refused against the
    #: adapter's own table, before any round trip.
    MD_INTERVAL_NOT_SUPPORTED = 105
    #: Too many queries already in flight.
    MD_TOO_MANY_IN_FLIGHT = 106
    #: The caller got no ack: nothing is serving queries, or it is wedged.
    MD_NO_ACK = 107
    #: An ack came back that the caller could not read.
    MD_UNREADABLE_ACK = 108
    #: The call to the venue never completed — a dropped socket, a timeout.
    #: The venue may well have served it; MD just never saw the answer.
    MD_VENUE_CALL_FAILED = 109

    # --- 2xx: the venue answered, and the answer was no --------------------

    #: The venue refused it and gave nothing machine-readable to go on.
    VENUE_REJECTED = 200
    #: Too many requests. Back off and ask again.
    VENUE_RATE_LIMITED = 201
    #: The venue rejected a field: an interval it does not know, a limit past
    #: its ceiling, a malformed parameter.
    VENUE_INVALID_PARAM = 202
    #: The venue has no such instrument.
    VENUE_SYMBOL_NOT_FOUND = 203
    #: The venue broke on its own account: internal error, overloaded.
    VENUE_INTERNAL_ERROR = 204


#: Codes worth trying again unchanged. Everything absent here is a standing
#: condition: the same query will fail the same way until the caller changes
#: it or the platform's state does.
_RETRYABLE = frozenset(
    {
        QueryCode.MD_INTERNAL,
        QueryCode.MD_VENUE_CALL_FAILED,
        QueryCode.MD_NO_ACK,
        QueryCode.MD_TOO_MANY_IN_FLIGHT,
        QueryCode.VENUE_RATE_LIMITED,
        QueryCode.VENUE_INTERNAL_ERROR,
    }
)


def is_normalized(code: int | str) -> bool:
    """Whether ``code`` is one this platform assigned rather than a venue's."""
    return isinstance(code, int) and MD_BAND <= code < BAND_END


def is_md_internal(code: int | str) -> bool:
    """Whether MD refused it — meaning the venue was never asked."""
    return isinstance(code, int) and MD_BAND <= code < VENUE_BAND


def is_venue(code: int | str) -> bool:
    """Whether the venue produced it, mapped or not.

    True for the ``2xx`` band and for every native code, since a venue is the
    only thing that produces one.
    """
    if isinstance(code, str):
        return bool(code)
    return code != QueryCode.NONE and not is_md_internal(code)


def is_retryable(code: int | str) -> bool:
    """Whether the same query is worth sending again unchanged.

    Conservative about codes it does not recognise: an unmapped native code
    could be anything, and a caller that retries one on a timer would retry it
    forever. Those read as not retryable until the table catches up.
    """
    return isinstance(code, int) and code in _RETRYABLE


def describe(code: int | str) -> str:
    """``code`` as something readable in a log line.

    Normalized codes render as ``201 VENUE_RATE_LIMITED``; a native code
    renders as itself, since its name is whatever the venue calls it.
    """
    if isinstance(code, int):
        try:
            return f"{code} {QueryCode(code).name}"
        except ValueError:
            return str(code)
    return code


__all__ = [
    "BAND_END",
    "MD_BAND",
    "VENUE_BAND",
    "QueryCode",
    "describe",
    "is_md_internal",
    "is_normalized",
    "is_retryable",
    "is_venue",
]
