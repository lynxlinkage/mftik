"""Venue errors → :class:`RejectCode`, in one table per venue.

TD is where an order request meets a venue, so TD is where the venue's answer
gets translated. :func:`normalize` takes the exception an adapter raised and
returns the ``error_code`` that goes on the wire beside it. The venue's own
words ride along unchanged as the reject's ``reason``.

The mapping lives here, centrally, rather than inside each adapter: the codes
are a cross-venue contract, and keeping every venue's spellings side by side
is what makes it obvious when two venues mean the same thing under different
names. Adding a venue is one entry in :data:`VENUES`.

Nothing here fails closed. A label we have never seen is passed through as the
venue's own code — see :mod:`mft.protocol.reject_codes` — so an incomplete
table costs precision, never information. That is deliberate: mapping a label
to the wrong code would have a strategy act on a reason that is not the real
one, which is worse than handing it something it does not recognise.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from mft.exchange.errors import (
    ExchangeNotConnectedError,
    InstrumentNotFoundError,
    InsufficientBalanceError,
)
from mft.exchange.paper.private import PaperAuthError
from mft.protocol.reject_codes import RejectCode


@dataclass(frozen=True)
class VenueErrors:
    """How one venue spells the errors we have codes for."""

    #: Machine-readable label → code. Gate's ``data.errs.label`` and REST
    #: ``label`` both land here.
    labels: dict[str, RejectCode] = field(default_factory=dict)
    #: Numeric venue code → code, for venues that answer with integers.
    codes: dict[int, RejectCode] = field(default_factory=dict)
    #: Lowercased message fragment → code, tried in order. A last resort for
    #: venues that raise no code at all; only worth having where the messages
    #: are ours to keep stable.
    messages: tuple[tuple[str, RejectCode], ...] = ()


#: Gate spot. Labels are Gate's own, from ``data.errs.label`` on a trading
#: call and the ``label`` field on a REST error body.
GATE = VenueErrors(
    labels={
        # funds
        "BALANCE_NOT_ENOUGH": RejectCode.VENUE_INSUFFICIENT_BALANCE,
        "MARGIN_BALANCE_NOT_ENOUGH": RejectCode.VENUE_INSUFFICIENT_BALANCE,
        "INSUFFICIENT_AVAILABLE": RejectCode.VENUE_INSUFFICIENT_BALANCE,
        # post-only that would have taken liquidity
        "POC_FILL_IMMEDIATELY": RejectCode.VENUE_POST_ONLY_WOULD_CROSS,
        "ORDER_POC_IMMEDIATE": RejectCode.VENUE_POST_ONLY_WOULD_CROSS,
        # credentials and permissions
        "IP_FORBIDDEN": RejectCode.VENUE_IP_NOT_WHITELISTED,
        "INVALID_KEY": RejectCode.VENUE_AUTH_FAILED,
        "INVALID_SIGNATURE": RejectCode.VENUE_AUTH_FAILED,
        "INVALID_CREDENTIALS": RejectCode.VENUE_AUTH_FAILED,
        "MISSING_REQUIRED_HEADER": RejectCode.VENUE_AUTH_FAILED,
        "REQUEST_EXPIRED": RejectCode.VENUE_AUTH_FAILED,
        "READ_ONLY": RejectCode.VENUE_PERMISSION_DENIED,
        "FORBIDDEN": RejectCode.VENUE_PERMISSION_DENIED,
        "ACCOUNT_LOCKED": RejectCode.VENUE_PERMISSION_DENIED,
        "TRADE_RESTRICTED": RejectCode.VENUE_PERMISSION_DENIED,
        # pacing
        "TOO_MANY_REQUESTS": RejectCode.VENUE_RATE_LIMITED,
        "REQUEST_TOO_FREQUENT": RejectCode.VENUE_RATE_LIMITED,
        "TOO_FAST": RejectCode.VENUE_RATE_LIMITED,
        # the order itself
        "ORDER_NOT_FOUND": RejectCode.VENUE_ORDER_NOT_FOUND,
        "ORDER_NOT_OWNED": RejectCode.VENUE_ORDER_NOT_FOUND,
        "USER_NOT_FOUND": RejectCode.VENUE_ORDER_NOT_FOUND,
        "ORDER_CLOSED": RejectCode.VENUE_ORDER_ALREADY_CLOSED,
        "ORDER_CANCELLED": RejectCode.VENUE_ORDER_ALREADY_CLOSED,
        "ORDER_FINISHED": RejectCode.VENUE_ORDER_ALREADY_CLOSED,
        "REPEATED_CREATION": RejectCode.VENUE_DUPLICATE_CLIENT_ORDER_ID,
        "DUPLICATE_REQUEST": RejectCode.VENUE_DUPLICATE_CLIENT_ORDER_ID,
        # request fields
        "INVALID_PARAM_VALUE": RejectCode.VENUE_INVALID_PARAM,
        "INVALID_ARGUMENT": RejectCode.VENUE_INVALID_PARAM,
        "INVALID_REQUEST_BODY": RejectCode.VENUE_INVALID_PARAM,
        "MISSING_REQUIRED_PARAM": RejectCode.VENUE_INVALID_PARAM,
        "BAD_REQUEST": RejectCode.VENUE_INVALID_PARAM,
        "INVALID_CURRENCY": RejectCode.VENUE_INVALID_PARAM,
        "PRICE_TOO_DEVIATED": RejectCode.VENUE_INVALID_PARAM,
        "AMOUNT_TOO_LITTLE": RejectCode.VENUE_BELOW_MINIMUM,
        "QUANTITY_NOT_ENOUGH": RejectCode.VENUE_BELOW_MINIMUM,
        "SIZE_TOO_SMALL": RejectCode.VENUE_BELOW_MINIMUM,
        # instrument
        "INVALID_CURRENCY_PAIR": RejectCode.VENUE_SYMBOL_NOT_TRADABLE,
        "CONTRACT_NOT_FOUND": RejectCode.VENUE_SYMBOL_NOT_TRADABLE,
        "CONTRACT_IN_DELISTING": RejectCode.VENUE_SYMBOL_NOT_TRADABLE,
        "ORDER_BOOK_NOT_FOUND": RejectCode.VENUE_SYMBOL_NOT_TRADABLE,
        # account limits
        "RISK_LIMIT_EXCEEDED": RejectCode.VENUE_RISK_LIMIT,
        "LEVERAGE_TOO_HIGH": RejectCode.VENUE_RISK_LIMIT,
        "LEVERAGE_TOO_LOW": RejectCode.VENUE_RISK_LIMIT,
        "AUTO_BORROW_TOO_MUCH": RejectCode.VENUE_RISK_LIMIT,
        # theirs, not ours
        "INTERNAL": RejectCode.VENUE_INTERNAL_ERROR,
        "SERVER_ERROR": RejectCode.VENUE_INTERNAL_ERROR,
        "TOO_BUSY": RejectCode.VENUE_INTERNAL_ERROR,
    },
    # Gate's WebSocket ``error.code``. Only the three documented values are
    # mapped — guessing at the rest would be worse than passing them through.
    codes={
        1: RejectCode.VENUE_INVALID_PARAM,  # invalid request body
        2: RejectCode.VENUE_INVALID_PARAM,  # invalid argument
        3: RejectCode.VENUE_INTERNAL_ERROR,  # server side error
    },
)

#: The paper engine. Its errors carry no label, only a message — but the
#: messages are ours, raised in ``mft.exchange.paper.engine``, so matching on
#: them is a maintenance question rather than a guess about a third party.
PAPER = VenueErrors(
    messages=(
        ("duplicate client_order_id", RejectCode.VENUE_DUPLICATE_CLIENT_ORDER_ID),
        ("unknown client_order_id", RejectCode.VENUE_ORDER_NOT_FOUND),
        ("unknown order_id", RejectCode.VENUE_ORDER_NOT_FOUND),
        ("is not open", RejectCode.VENUE_ORDER_ALREADY_CLOSED),
        ("insufficient liquidity", RejectCode.VENUE_INSUFFICIENT_BALANCE),
        ("qty must be positive", RejectCode.VENUE_INVALID_PARAM),
        ("require price", RejectCode.VENUE_INVALID_PARAM),
        ("api_key and api_secret are required", RejectCode.VENUE_AUTH_FAILED),
    ),
)

#: Keyed by ``venues`` canonical name, which is also what a private client
#: reports as its ``name``.
VENUES: dict[str, VenueErrors] = {
    "Gate": GATE,
    "Paper": PAPER,
}

#: Typed errors, for the adapters that raise a class instead of a label.
#: Checked most-specific first, so a subclass never loses to its base.
BY_TYPE: tuple[tuple[type[BaseException], RejectCode], ...] = (
    # Never reached the venue: the client was not connected to send it.
    (ExchangeNotConnectedError, RejectCode.TD_VENUE_NOT_CONNECTED),
    (InsufficientBalanceError, RejectCode.VENUE_INSUFFICIENT_BALANCE),
    (InstrumentNotFoundError, RejectCode.VENUE_SYMBOL_NOT_TRADABLE),
    # Paper's only typed refusal; every other paper error is a bare
    # ``OrderError`` and falls to :attr:`VenueErrors.messages`.
    (PaperAuthError, RejectCode.VENUE_AUTH_FAILED),
)


def normalize(exc: BaseException, *, venue: str) -> int | str:
    """The reject code for an exception an adapter raised.

    Resolution runs most-specific first: the venue's own label, then its
    numeric code, then the typed exception, then — for venues that give us
    nothing else — a message match.

    An unmapped venue code comes back as itself rather than as some catch-all,
    which is what makes the table safe to extend later: today's native code is
    tomorrow's ``2xx``, and nothing in between silently changes meaning. The
    venue's own words are not returned here at all; the caller already has
    them in ``str(exc)`` and puts them on the reject as ``reason``.
    """
    table = VENUES.get(venue, VenueErrors())

    label = getattr(exc, "label", "")
    if isinstance(label, str) and label:
        mapped = table.labels.get(label.upper())
        # Unmapped: hand back the venue's own label, not a catch-all.
        return mapped if mapped is not None else label

    code = getattr(exc, "code", None)
    if isinstance(code, int) and not isinstance(code, bool):
        mapped_code = table.codes.get(code)
        return mapped_code if mapped_code is not None else code

    for exc_type, typed in BY_TYPE:
        if isinstance(exc, exc_type):
            return typed

    lowered = str(exc).lower()
    for fragment, matched in table.messages:
        if fragment in lowered:
            return matched

    # The venue said no and gave us nothing to key on.
    return RejectCode.VENUE_REJECTED


__all__ = [
    "BY_TYPE",
    "GATE",
    "PAPER",
    "VENUES",
    "VenueErrors",
    "normalize",
]
