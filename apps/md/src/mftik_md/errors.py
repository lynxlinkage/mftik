"""Venue errors → :class:`QueryCode`, in one table per venue.

The read-side counterpart of :mod:`mftik_td.errors`, and the same shape: MD is
where a market-data query meets a venue, so MD is where the venue's answer gets
translated. :func:`normalize` takes the exception an adapter raised and returns
the ``error_code`` that goes on the wire beside it; the venue's own words ride
along unchanged as the result's ``reason``.

Smaller than TD's table, because the surface is smaller — a public read has no
funds, no credentials and no order to be wrong about. What is left is the
instrument, the parameters, and the venue's willingness to answer right now.

Two things differ from TD's, both because a read is not an order:

* The typed table maps our own refusals too. :class:`~mftik_md.fetch.NoReaderError`
  is not a venue error at all — it means the venue does not serve the read —
  and a caller has to be able to tell that from an empty answer.
* The fallback is :attr:`QueryCode.MD_VENUE_CALL_FAILED`, not a venue reject.
  A venue that refuses a read says so with a label; an exception carrying no
  label at all is far more likely a socket or a timeout than a refusal. Being
  wrong in this direction costs a retry, where TD's direction — assuming a
  refusal — is what keeps an order from being sent twice.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from mftik.exchange.errors import (
    ExchangeNotConnectedError,
    InstrumentNotFoundError,
)
from mftik.exchange.intervals import InvalidIntervalError
from mftik.protocol.query_codes import QueryCode
from mftik.symbols import SymbolNotFoundError

from mftik_md.fetch.readers import NoReaderError


@dataclass(frozen=True)
class VenueErrors:
    """How one venue spells the read errors we have codes for."""

    #: Machine-readable label → code. Gate's REST ``label`` lands here.
    labels: dict[str, QueryCode] = field(default_factory=dict)
    #: Numeric venue code → code, for venues that answer with integers.
    codes: dict[int, QueryCode] = field(default_factory=dict)
    #: Lowercased message fragment → code, tried in order. A last resort for
    #: venues that raise no code at all.
    messages: tuple[tuple[str, QueryCode], ...] = ()


#: Gate spot. Labels come from the ``label`` field on a REST error body; the
#: ones here are what ``/spot/candlesticks`` and its neighbours actually
#: answer with.
GATE = VenueErrors(
    labels={
        # instrument
        "INVALID_CURRENCY_PAIR": QueryCode.VENUE_SYMBOL_NOT_FOUND,
        "CONTRACT_NOT_FOUND": QueryCode.VENUE_SYMBOL_NOT_FOUND,
        "CONTRACT_IN_DELISTING": QueryCode.VENUE_SYMBOL_NOT_FOUND,
        "ORDER_BOOK_NOT_FOUND": QueryCode.VENUE_SYMBOL_NOT_FOUND,
        # request fields — an interval Gate does not know, a limit past its
        # ceiling, a malformed parameter
        "INVALID_PARAM_VALUE": QueryCode.VENUE_INVALID_PARAM,
        "INVALID_ARGUMENT": QueryCode.VENUE_INVALID_PARAM,
        "MISSING_REQUIRED_PARAM": QueryCode.VENUE_INVALID_PARAM,
        "BAD_REQUEST": QueryCode.VENUE_INVALID_PARAM,
        "INVALID_CURRENCY": QueryCode.VENUE_INVALID_PARAM,
        # pacing
        "TOO_MANY_REQUESTS": QueryCode.VENUE_RATE_LIMITED,
        "REQUEST_TOO_FREQUENT": QueryCode.VENUE_RATE_LIMITED,
        "TOO_FAST": QueryCode.VENUE_RATE_LIMITED,
        # theirs, not ours
        "INTERNAL": QueryCode.VENUE_INTERNAL_ERROR,
        "SERVER_ERROR": QueryCode.VENUE_INTERNAL_ERROR,
        "TOO_BUSY": QueryCode.VENUE_INTERNAL_ERROR,
        # the body did not parse as JSON at all
        "BAD_RESPONSE": QueryCode.VENUE_INTERNAL_ERROR,
    },
)

#: Binance spot. Numeric codes only — Binance publishes no label, and the
#: numbers are its documented contract. They are all negative, so an unmapped
#: one passes through as itself without any risk of being read as a code this
#: platform assigned.
#:
#: Much shorter than TD's Binance table, and for the same reason this whole
#: module is shorter than TD's: a public read has no funds, no credentials and
#: no order to be wrong about. The ``-2xxx`` order codes cannot reach a read at
#: all, and the ones that could — a bad symbol, an interval Binance does not
#: serve — are the whole surface.
BINANCE = VenueErrors(
    codes={
        # instrument
        -1121: QueryCode.VENUE_SYMBOL_NOT_FOUND,  # invalid symbol
        -2016: QueryCode.VENUE_SYMBOL_NOT_FOUND,  # no trading window
        # request fields — an interval Binance does not know, a limit past its
        # ceiling, a malformed parameter
        -1013: QueryCode.VENUE_INVALID_PARAM,
        -1100: QueryCode.VENUE_INVALID_PARAM,
        -1101: QueryCode.VENUE_INVALID_PARAM,
        -1102: QueryCode.VENUE_INVALID_PARAM,
        -1103: QueryCode.VENUE_INVALID_PARAM,
        -1104: QueryCode.VENUE_INVALID_PARAM,
        -1105: QueryCode.VENUE_INVALID_PARAM,
        -1120: QueryCode.VENUE_INVALID_PARAM,  # bad interval
        -1128: QueryCode.VENUE_INVALID_PARAM,
        -1130: QueryCode.VENUE_INVALID_PARAM,
        # pacing
        -1003: QueryCode.VENUE_RATE_LIMITED,
        # theirs, not ours
        -1000: QueryCode.VENUE_INTERNAL_ERROR,
        -1001: QueryCode.VENUE_INTERNAL_ERROR,  # disconnected
        -1006: QueryCode.VENUE_INTERNAL_ERROR,  # unexpected response
        -1007: QueryCode.VENUE_INTERNAL_ERROR,  # backend timeout
        -1008: QueryCode.VENUE_INTERNAL_ERROR,  # server busy
        -1016: QueryCode.VENUE_INTERNAL_ERROR,  # service shutting down
    },
)

#: Binance USDⓈ-M futures. The same numbering as spot — one company, one error
#: space — and the same short table, for the same reason: a public read has no
#: funds, no credentials and no order to be wrong about. ``-1120`` for an
#: interval Binance does not serve is the one that actually fires here, since
#: futures serves one window fewer than spot.
BINANCE_FUTURE = BINANCE

OKX = VenueErrors(
    codes={
        51000: QueryCode.VENUE_INVALID_PARAM,
        50011: QueryCode.VENUE_RATE_LIMITED,
        50001: QueryCode.VENUE_INTERNAL_ERROR,
        50013: QueryCode.VENUE_INTERNAL_ERROR,
    },
)

#: Bybit v5. Numeric codes only, five and six digit, so an unmapped one passes
#: through as itself with no risk of being read as a code this platform
#: assigned.
#:
#: Short for the same reason Binance's is: a public read has no funds, no
#: credentials and no order to be wrong about. The ``1100xx``/``1701xx`` order
#: families cannot reach a read at all, and what could — a symbol Bybit does
#: not list, an interval it does not serve — is the whole surface.
BYBIT = VenueErrors(
    codes={
        # request fields — an unknown interval, a limit past the ceiling, a
        # category that does not go with the symbol
        10001: QueryCode.VENUE_INVALID_PARAM,
        # instrument
        170210: QueryCode.VENUE_SYMBOL_NOT_FOUND,  # not open for trading
        # pacing
        10006: QueryCode.VENUE_RATE_LIMITED,  # too many visits
        10018: QueryCode.VENUE_RATE_LIMITED,  # exceeded IP rate limit
        # theirs, not ours
        10000: QueryCode.VENUE_INTERNAL_ERROR,  # server timeout
        10016: QueryCode.VENUE_INTERNAL_ERROR,  # server error
    },
)

#: The paper engine, with nothing to map: it has no reader at all, so the
#: factory refuses before any of this is reached. Listed so the venue is known
#: rather than falling through as unrecognised.
PAPER = VenueErrors()

#: Keyed by ``venues`` canonical name, which is also what a public client
#: reports as its ``name``.
VENUES: dict[str, VenueErrors] = {
    "Binance": BINANCE,
    "BinanceFuture": BINANCE_FUTURE,
    "Bybit": BYBIT,
    "Okx": OKX,
    "Gate": GATE,
    "GateFutures": GATE,
    "Paper": PAPER,
}

#: Typed errors, for the refusals that never reach a venue at all. Checked
#: most-specific first, so a subclass never loses to its base.
BY_TYPE: tuple[tuple[type[BaseException], QueryCode], ...] = (
    # This venue does not serve the read at all, which is not the same as
    # serving it and having nothing to return.
    (NoReaderError, QueryCode.MD_VENUE_UNSUPPORTED_READ),
    # Refused against the adapter's own interval table, before any round trip.
    (InvalidIntervalError, QueryCode.MD_INTERVAL_NOT_SUPPORTED),
    # The venue client exists but is not connected, so nothing was sent.
    (ExchangeNotConnectedError, QueryCode.MD_VENUE_NOT_CONNECTED),
    # The symbol plane could not resolve it, so the venue was never asked.
    (SymbolNotFoundError, QueryCode.VENUE_SYMBOL_NOT_FOUND),
    (InstrumentNotFoundError, QueryCode.VENUE_SYMBOL_NOT_FOUND),
)


def normalize(exc: BaseException, *, venue: str) -> int | str:
    """The query code for an exception a public adapter raised.

    Resolution runs most-specific first: the typed refusals, then the venue's
    own label, then its numeric code, then a message match.

    Typed comes first here, unlike TD, because the types in :data:`BY_TYPE` are
    ours rather than the venue's — an ``InvalidIntervalError`` is a decision
    this platform made and must not be re-read as whatever label happened to be
    attached to it.

    An unmapped venue label comes back as itself rather than as some catch-all,
    which is what makes the table safe to extend: today's native label is
    tomorrow's ``2xx``, and nothing in between silently changes meaning.
    """
    for exc_type, typed in BY_TYPE:
        if isinstance(exc, exc_type):
            return typed

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

    lowered = str(exc).lower()
    for fragment, matched in table.messages:
        if fragment in lowered:
            return matched

    # Nothing to key on. For a read that reads as "the call did not get
    # through" rather than "the venue refused" — see the module docstring.
    return QueryCode.MD_VENUE_CALL_FAILED


__all__ = [
    "BINANCE",
    "BY_TYPE",
    "GATE",
    "PAPER",
    "BYBIT",
    "VENUES",
    "VenueErrors",
    "normalize",
]
