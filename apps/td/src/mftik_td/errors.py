"""Venue errors → :class:`RejectCode`, in one table per venue.

TD is where an order request meets a venue, so TD is where the venue's answer
gets translated. :func:`normalize` takes the exception an adapter raised and
returns the ``error_code`` that goes on the wire beside it. The venue's own
words ride along unchanged as the reject's ``reason``.

A venue can also say no *without* raising: it accepts the request, then kills
the order and gives its reason on the private order stream. That refusal
reaches TD as an order update rather than an exception, so it has a second
entry point, :func:`normalize_reason`, reading the same tables.

The mapping lives here, centrally, rather than inside each adapter: the codes
are a cross-venue contract, and keeping every venue's spellings side by side
is what makes it obvious when two venues mean the same thing under different
names. Adding a venue is one entry in :data:`VENUES`.

Nothing here fails closed. A label we have never seen is passed through as the
venue's own code — see :mod:`mftik.protocol.reject_codes` — so an incomplete
table costs precision, never information. That is deliberate: mapping a label
to the wrong code would have a strategy act on a reason that is not the real
one, which is worse than handing it something it does not recognise.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from mftik.exchange.bitget.protocol import (
    BitgetAccountModeError,
    BitgetAuthError,
)
from mftik.exchange.errors import (
    ExchangeNotConnectedError,
    InstrumentNotFoundError,
    InsufficientBalanceError,
)
from mftik.exchange.paper.private import PaperAuthError
from mftik.protocol.reject_codes import RejectCode


@dataclass(frozen=True)
class VenueErrors:
    """How one venue spells the errors we have codes for."""

    #: Machine-readable label → code. Gate's ``data.errs.label`` and REST
    #: ``label`` both land here, as does the ``rejectReason`` a venue puts on
    #: an order it refused on the stream.
    #:
    #: Matched case-insensitively — ``__post_init__`` upper-cases the keys — so
    #: a table can be written in the venue's own spelling and still be found.
    #: Worth those four lines: Bybit's ``EC_PostOnlyWillTakeLiquidity`` can be
    #: read against its docs, ``EC_POSTONLYWILLTAKELIQUIDITY`` cannot.
    labels: dict[str, RejectCode] = field(default_factory=dict)
    #: Numeric venue code → code, for venues that answer with integers.
    codes: dict[int, RejectCode] = field(default_factory=dict)
    #: Numeric venue code → message fragments that say which refusal it really
    #: was, tried in order before :attr:`codes` and falling through to it.
    #:
    #: For venues whose codes are coarser than their meanings. Binance answers
    #: ``-2010`` to every rejected new order — out of funds, duplicate id,
    #: post-only that would have crossed, symbol halted — and only the message
    #: tells them apart. Mapping ``-2010`` to any one of those would tell a
    #: strategy something specific and false, which is the one failure mode
    #: this module exists to avoid; leaving it unmapped would throw away
    #: information the venue did give us.
    refine: dict[int, tuple[tuple[str, RejectCode], ...]] = field(
        default_factory=dict
    )
    #: Lowercased message fragment → code, tried in order. A last resort for
    #: venues that raise no code at all; only worth having where the messages
    #: are ours to keep stable.
    messages: tuple[tuple[str, RejectCode], ...] = ()
    #: Labels and numeric codes a venue uses to say an immediate-or-nothing
    #: order could not be filled — **not** refusals, and so not codes at all.
    #:
    #: A FOK that filled nothing did what it was told: fill everything now or
    #: nothing. Most venues report that as a plain cancel on the order stream,
    #: but Gate and Binance's margined books answer the *call* with an error
    #: instead, which is the only reason TD ever sees one. Everything named
    #: here is routed to a cancelled order rather than a reject, so a strategy
    #: hears the same thing on every venue — see
    #: :func:`is_unfilled_immediate`.
    #:
    #: Labels are matched case-insensitively, like :attr:`labels`.
    unfilled: frozenset[int | str] = frozenset()

    def __post_init__(self) -> None:
        upper = {key.upper(): value for key, value in self.labels.items()}
        if upper != self.labels:
            object.__setattr__(self, "labels", upper)
        shouted = frozenset(
            entry.upper() if isinstance(entry, str) else entry
            for entry in self.unfilled
        )
        if shouted != self.unfilled:
            object.__setattr__(self, "unfilled", shouted)


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
    # "Order cannot be filled completely" — the instruction working, not a
    # refusal. Gate is one of the two venues that answers this at the call.
    unfilled=frozenset({"FOK_NOT_FILL"}),
    # Gate's WebSocket ``error.code``. Only the three documented values are
    # mapped — guessing at the rest would be worse than passing them through.
    codes={
        1: RejectCode.VENUE_INVALID_PARAM,  # invalid request body
        2: RejectCode.VENUE_INVALID_PARAM,  # invalid argument
        3: RejectCode.VENUE_INTERNAL_ERROR,  # server side error
    },
)

#: Binance spot. Numeric codes only — Binance publishes no label, and the
#: numbers are its documented contract.
#:
#: They are all negative, which is what makes it safe for an unmapped one to
#: pass through as itself: :mod:`mftik.protocol.reject_codes` warns that a venue
#: numbering inside ``100``–``299`` would be indistinguishable from a code this
#: platform assigned, and Binance's cannot collide.
#:
#: The ``-2010`` and ``-2011`` families are refined by message, because one
#: code covers many refusals; see :attr:`VenueErrors.refine`.
BINANCE = VenueErrors(
    codes={
        # pacing
        -1003: RejectCode.VENUE_RATE_LIMITED,  # too many requests / weight
        -1015: RejectCode.VENUE_RATE_LIMITED,  # too many new orders
        # theirs, not ours
        -1000: RejectCode.VENUE_INTERNAL_ERROR,
        -1001: RejectCode.VENUE_INTERNAL_ERROR,  # disconnected
        -1008: RejectCode.VENUE_INTERNAL_ERROR,  # server busy
        -1016: RejectCode.VENUE_INTERNAL_ERROR,  # service shutting down
        # Execution status genuinely unknown on both of these: the order may
        # have landed. Read as the venue's problem so recon settles it, rather
        # than as a refusal a strategy would take as final.
        -1006: RejectCode.VENUE_INTERNAL_ERROR,  # unexpected response
        -1007: RejectCode.VENUE_INTERNAL_ERROR,  # backend timeout
        # credentials and permissions
        -1002: RejectCode.VENUE_AUTH_FAILED,  # unauthorized
        -1021: RejectCode.VENUE_AUTH_FAILED,  # timestamp outside recvWindow
        -1022: RejectCode.VENUE_AUTH_FAILED,  # bad signature
        -2014: RejectCode.VENUE_AUTH_FAILED,  # malformed api key
        # "Invalid API-key, IP, or permissions for action" — Binance will not
        # say which, so neither can we.
        -2015: RejectCode.VENUE_AUTH_FAILED,
        # request fields
        -1013: RejectCode.VENUE_INVALID_PARAM,  # rejected before the engine
        -1020: RejectCode.VENUE_INVALID_PARAM,  # unsupported operation
        -1100: RejectCode.VENUE_INVALID_PARAM,  # illegal characters
        -1101: RejectCode.VENUE_INVALID_PARAM,
        -1102: RejectCode.VENUE_INVALID_PARAM,  # mandatory param missing
        -1103: RejectCode.VENUE_INVALID_PARAM,
        -1104: RejectCode.VENUE_INVALID_PARAM,
        -1105: RejectCode.VENUE_INVALID_PARAM,
        -1106: RejectCode.VENUE_INVALID_PARAM,
        -1108: RejectCode.VENUE_INVALID_PARAM,
        -1111: RejectCode.VENUE_INVALID_PARAM,  # too much precision
        -1114: RejectCode.VENUE_INVALID_PARAM,  # TIF sent where not allowed
        -1115: RejectCode.VENUE_INVALID_PARAM,  # invalid TIF
        -1116: RejectCode.VENUE_INVALID_PARAM,  # invalid order type
        -1117: RejectCode.VENUE_INVALID_PARAM,  # invalid side
        -1118: RejectCode.VENUE_INVALID_PARAM,  # empty newClientOrderId
        -1119: RejectCode.VENUE_INVALID_PARAM,  # empty origClientOrderId
        -1128: RejectCode.VENUE_INVALID_PARAM,  # bad optional param combo
        -1130: RejectCode.VENUE_INVALID_PARAM,
        -1131: RejectCode.VENUE_INVALID_PARAM,  # bad recvWindow
        # instrument
        -1121: RejectCode.VENUE_SYMBOL_NOT_TRADABLE,  # invalid symbol
        -2016: RejectCode.VENUE_SYMBOL_NOT_TRADABLE,  # no trading window
        # the order itself
        -2011: RejectCode.VENUE_ORDER_NOT_FOUND,  # cancel rejected
        -2013: RejectCode.VENUE_ORDER_NOT_FOUND,  # no such order
        -2026: RejectCode.VENUE_ORDER_ALREADY_CLOSED,  # archived
        # A new order Binance refused, without a message we recognise.
        -2010: RejectCode.VENUE_REJECTED,
    },
    refine={
        # Every rejected new order is -2010; the message is the only thing that
        # says why. Fragments are lowercased and matched in order, so the more
        # specific ones come first.
        -2010: (
            ("insufficient balance", RejectCode.VENUE_INSUFFICIENT_BALANCE),
            # LIMIT_MAKER — our POST_ONLY — that would have taken liquidity.
            ("immediately match and take", RejectCode.VENUE_POST_ONLY_WOULD_CROSS),
            ("duplicate order", RejectCode.VENUE_DUPLICATE_CLIENT_ORDER_ID),
            ("market is closed", RejectCode.VENUE_SYMBOL_NOT_TRADABLE),
            ("too many open orders", RejectCode.VENUE_RISK_LIMIT),
            ("may not place or cancel orders", RejectCode.VENUE_PERMISSION_DENIED),
            ("disabled on this account", RejectCode.VENUE_PERMISSION_DENIED),
            # "Price * QTY is zero or less" is the notional floor by another
            # name.
            ("zero or less", RejectCode.VENUE_BELOW_MINIMUM),
            ("min_notional", RejectCode.VENUE_BELOW_MINIMUM),
            ("filter failure", RejectCode.VENUE_INVALID_PARAM),
            ("not supported for this symbol", RejectCode.VENUE_INVALID_PARAM),
            ("would trigger immediately", RejectCode.VENUE_INVALID_PARAM),
        ),
        # A cancel Binance refused for a reason other than the order being
        # gone: ``cancelRestrictions`` said the order had moved on.
        -2011: (
            ("cancel restrictions", RejectCode.VENUE_ORDER_ALREADY_CLOSED),
        ),
        # Filter failures arrive here too on some paths.
        -1013: (
            ("min_notional", RejectCode.VENUE_BELOW_MINIMUM),
            ("lot_size", RejectCode.VENUE_BELOW_MINIMUM),
        ),
    },
)

#: Binance USDⓈ-M futures. The same numbering as spot for everything both
#: markets can refuse — one company, one error space — plus the ``-2xxx`` and
#: ``-4xxx`` families that only a margined book has: insufficient margin, a
#: reduce-only order with nothing to reduce, a position past what the current
#: leverage allows.
#:
#: Built on top of :data:`BINANCE` rather than beside it, so a code added there
#: is a code this venue knows too. The one place they genuinely disagree is
#: post-only: spot expresses it as ``LIMIT_MAKER`` and refuses it inside the
#: ``-2010`` message, while futures has a code of its own (``-5022``) for a
#: ``GTX`` order that would have taken.
BINANCE_UM = VenueErrors(
    codes={
        **BINANCE.codes,
        # post-only (GTX) that would have crossed — futures says it outright
        -5022: RejectCode.VENUE_POST_ONLY_WOULD_CROSS,
        # funds and margin
        -2018: RejectCode.VENUE_INSUFFICIENT_BALANCE,  # balance insufficient
        -2019: RejectCode.VENUE_INSUFFICIENT_BALANCE,  # margin insufficient
        # counterparty's best price fails the PERCENT_PRICE filter
        -4131: RejectCode.VENUE_INVALID_PARAM,
        # the order itself
        #
        # "Unable to fill." Not the FOK path, whatever this comment used to
        # say — that is ``-5021``, in :attr:`~VenueErrors.unfilled` below.
        # Kept as a refusal because it has never been seen on the wire here
        # and its wording covers more than one thing; routing an unobserved
        # code to a cancelled order would turn a real refusal into a
        # non-event on nothing but a guess.
        -2020: RejectCode.VENUE_REJECTED,
        -2021: RejectCode.VENUE_INVALID_PARAM,  # would trigger immediately
        -2022: RejectCode.VENUE_INVALID_PARAM,  # reduce-only rejected
        -4061: RejectCode.VENUE_INVALID_PARAM,  # positionSide does not match mode
        -4164: RejectCode.VENUE_BELOW_MINIMUM,  # notional under the floor
        # limits on the account
        -2023: RejectCode.VENUE_RISK_LIMIT,  # user is in liquidation mode
        -2027: RejectCode.VENUE_RISK_LIMIT,  # position past max at this leverage
        -2028: RejectCode.VENUE_RISK_LIMIT,  # leverage below what margin allows
        # permissions
        -4400: RejectCode.VENUE_PERMISSION_DENIED,  # compliance restricted
        -4401: RejectCode.VENUE_PERMISSION_DENIED,  # compliance restricted
    },
    # "Due to the order could not be filled immediately, the FOK order has
    # been rejected. The order will not be recorded in the order history."
    # Binance calls it a rejection; it is the instruction working, and the
    # spot book reports the same event as an ``EXPIRED`` order instead.
    unfilled=frozenset({-5021}),
    refine={
        **BINANCE.refine,
        -2010: (
            *BINANCE.refine[-2010],
            ("margin is insufficient", RejectCode.VENUE_INSUFFICIENT_BALANCE),
            ("reduceonly", RejectCode.VENUE_INVALID_PARAM),
        ),
    },
)

#: OKX. Numeric codes for what it refuses at the call — the ``sCode`` on an
#: order reply and the ``code`` on a REST or socket error, which are the same
#: numbering.
#:
#: **A crossed post-only is not among them.** OKX accepts the order, kills it,
#: and reports ``state=canceled`` with ``cancelSource=31``; no exception is
#: ever raised, so :func:`normalize` cannot be asked. The adapter reads that
#: integer (``mftik.exchange.okx.models.CANCEL_REFUSALS``) and puts OKX's own
#: words for it on the order, which is what :func:`normalize_reason` matches.
#: The words come through :attr:`~VenueErrors.messages` rather than
#: :attr:`~VenueErrors.labels` because OKX publishes no label on this path —
#: the fragment is the adapter's, kept stable on purpose, the same arrangement
#: :data:`PAPER` uses.
OKX = VenueErrors(
    messages=(
        (
            "post-only order will take liquidity",
            RejectCode.VENUE_POST_ONLY_WOULD_CROSS,
        ),
    ),
    codes={
        50101: RejectCode.VENUE_AUTH_FAILED,
        50102: RejectCode.VENUE_AUTH_FAILED,
        50111: RejectCode.VENUE_AUTH_FAILED,
        50113: RejectCode.VENUE_AUTH_FAILED,
        50119: RejectCode.VENUE_AUTH_FAILED,
        50114: RejectCode.VENUE_PERMISSION_DENIED,
        50110: RejectCode.VENUE_IP_NOT_WHITELISTED,
        50011: RejectCode.VENUE_RATE_LIMITED,
        51000: RejectCode.VENUE_INVALID_PARAM,
        51008: RejectCode.VENUE_INSUFFICIENT_BALANCE,
        51119: RejectCode.VENUE_INSUFFICIENT_BALANCE,
        51131: RejectCode.VENUE_INSUFFICIENT_BALANCE,
        51020: RejectCode.VENUE_BELOW_MINIMUM,
        51400: RejectCode.VENUE_ORDER_NOT_FOUND,
        51603: RejectCode.VENUE_ORDER_NOT_FOUND,
        51024: RejectCode.VENUE_RISK_LIMIT,
        50001: RejectCode.VENUE_INTERNAL_ERROR,
        50013: RejectCode.VENUE_INTERNAL_ERROR,
    },
)

#: Bybit v5. Numeric codes for what it refuses at the call, ``EC_`` labels for
#: what it refuses on the ``order`` topic — the numbers are its documented
#: contract, the same ones over REST and over the sockets.
#:
#: The numbers are all five and six digit, so an unmapped one passes through as
#: itself without any risk of colliding with the ``100``–``299`` band this
#: platform assigns — see :mod:`mftik.protocol.reject_codes`.
#:
#: **The two books number the same fact differently**, which is why several
#: meanings appear twice: ``110001`` is "no such order" on the contract books
#: and ``170213`` is the same answer on spot.
#:
#: Deliberately partial. Bybit documents several hundred codes, most of which
#: belong to endpoints this adapter never calls — leverage, margin mode,
#: transfers, sub-accounts. What is here is what an order can be refused with.
#:
#: **The labels are on a different path from the codes.** Bybit answers some
#: refusals by killing the order after it has accepted the request: the row
#: arrives on the ``order`` topic with ``orderStatus=Rejected`` and its
#: ``rejectReason``, never as an exception, so :func:`normalize` never sees it
#: and only :func:`normalize_reason` reads these. A crossed post-only is the
#: one that matters in practice — it is the ordinary cost of quoting passively,
#: and without ``EC_PostOnlyWillTakeLiquidity`` here it is indistinguishable
#: from a refusal for any other reason.
#:
#: Two ``EC_`` values are deliberately absent. ``EC_BySelfMatch`` and
#: ``EC_PerCancelRequest`` end an order as ``Cancelled``, not ``Rejected``, so
#: they arrive as an order update and never reach a reject at all — mapping
#: them would add codes that can never fire. (Self-trade prevention is a cancel
#: on every venue here: Binance's ``EXPIRED_IN_MATCH`` and Gate's ``stp``
#: finish-reason are both normalized to ``CANCELED`` in their adapters.)
BYBIT = VenueErrors(
    labels={
        # post-only that would have taken liquidity — "your post only order
        # would have executed as a taker, and so was rejected"
        "EC_PostOnlyWillTakeLiquidity": RejectCode.VENUE_POST_ONLY_WOULD_CROSS,
        # the order itself
        "EC_DuplicatedClOrdID": RejectCode.VENUE_DUPLICATE_CLIENT_ORDER_ID,
        "EC_OrigClOrdIDDoesNotExist": RejectCode.VENUE_ORDER_NOT_FOUND,
        "EC_TooLateToCancel": RejectCode.VENUE_ORDER_ALREADY_CLOSED,
        # request fields
        "EC_MissingClOrdID": RejectCode.VENUE_INVALID_PARAM,
        "EC_MissingOrigClOrdID": RejectCode.VENUE_INVALID_PARAM,
        "EC_UnknownMessageType": RejectCode.VENUE_INVALID_PARAM,
        "EC_UnknownOrderType": RejectCode.VENUE_INVALID_PARAM,
        "EC_UnknownSide": RejectCode.VENUE_INVALID_PARAM,
        "EC_UnknownTimeInForce": RejectCode.VENUE_INVALID_PARAM,
        "EC_LimitOrderInvalidPrice": RejectCode.VENUE_INVALID_PARAM,
        "EC_MarketOrderPriceIsNotZero": RejectCode.VENUE_INVALID_PARAM,
        "EC_MarketOrderCannotBePostOnly": RejectCode.VENUE_INVALID_PARAM,
        # instrument
        "EC_InvalidSymbolStatus": RejectCode.VENUE_SYMBOL_NOT_TRADABLE,
        "EC_InCallAuctionStatus": RejectCode.VENUE_SYMBOL_NOT_TRADABLE,
    },
    codes={
        # credentials and permissions
        10003: RejectCode.VENUE_AUTH_FAILED,  # invalid api key
        10004: RejectCode.VENUE_AUTH_FAILED,  # error sign
        10002: RejectCode.VENUE_AUTH_FAILED,  # request outside recv_window
        33004: RejectCode.VENUE_AUTH_FAILED,  # api key expired
        10005: RejectCode.VENUE_PERMISSION_DENIED,  # permission denied
        10010: RejectCode.VENUE_IP_NOT_WHITELISTED,  # unmatched IP
        # pacing
        10006: RejectCode.VENUE_RATE_LIMITED,  # too many visits
        10018: RejectCode.VENUE_RATE_LIMITED,  # exceeded IP rate limit
        # request fields
        10001: RejectCode.VENUE_INVALID_PARAM,  # parameter error
        110003: RejectCode.VENUE_INVALID_PARAM,  # price out of range
        110017: RejectCode.VENUE_INVALID_PARAM,  # reduce-only not satisfied
        110025: RejectCode.VENUE_INVALID_PARAM,  # position idx / mode mismatch
        170193: RejectCode.VENUE_INVALID_PARAM,  # buy price above the cap
        170194: RejectCode.VENUE_INVALID_PARAM,  # sell price below the floor
        # funds
        110004: RejectCode.VENUE_INSUFFICIENT_BALANCE,  # wallet balance short
        110007: RejectCode.VENUE_INSUFFICIENT_BALANCE,  # ab not enough
        110012: RejectCode.VENUE_INSUFFICIENT_BALANCE,  # available short
        110045: RejectCode.VENUE_INSUFFICIENT_BALANCE,  # wallet balance short
        170131: RejectCode.VENUE_INSUFFICIENT_BALANCE,  # spot balance short
        # minimums
        170140: RejectCode.VENUE_BELOW_MINIMUM,  # order value under the floor
        170136: RejectCode.VENUE_BELOW_MINIMUM,  # order qty under the floor
        # limits on the account
        110009: RejectCode.VENUE_RISK_LIMIT,  # too many stop orders
        110020: RejectCode.VENUE_RISK_LIMIT,  # too many active orders
        # the order itself
        110001: RejectCode.VENUE_ORDER_NOT_FOUND,  # order does not exist
        170213: RejectCode.VENUE_ORDER_NOT_FOUND,  # order does not exist, spot
        # instrument
        170210: RejectCode.VENUE_SYMBOL_NOT_TRADABLE,  # not open for trading
        # theirs, not ours
        10016: RejectCode.VENUE_INTERNAL_ERROR,  # server error
        10000: RejectCode.VENUE_INTERNAL_ERROR,  # server timeout
    },
)

#: Bitget UTA v3. Numeric codes only — v2 codes are not a fallback (V11).
#: Unmapped codes pass through as themselves.
#:
#: **40085** is a UTA key on Classic v2 (or the reverse). Auth codes from
#: the adapter's ``AUTH_CODES`` set land here. A crossed post-only is
#: accepted then killed; the adapter puts words on the order and
#: :func:`normalize_reason` matches them, the OKX ``CANCEL_REFUSALS``
#: rule. Local refusals (passphrase, accountMode, missing posSide, spot
#: market-buy base qty) are ours and live in :attr:`messages`.
BITGET = VenueErrors(
    messages=(
        (
            "the post-only order will take liquidity",
            RejectCode.VENUE_POST_ONLY_WOULD_CROSS,
        ),
        ("passphrase is required", RejectCode.VENUE_AUTH_FAILED),
        ("accountmode=", RejectCode.VENUE_PERMISSION_DENIED),
        ("has no holdmode", RejectCode.VENUE_PERMISSION_DENIED),
        ("hedge_mode requires posside", RejectCode.VENUE_INVALID_PARAM),
        (
            "spot market buy sizes qty in quote",
            RejectCode.VENUE_INVALID_PARAM,
        ),
    ),
    codes={
        # credentials — Classic key on v3 / UTA key on v2
        40006: RejectCode.VENUE_AUTH_FAILED,
        40009: RejectCode.VENUE_AUTH_FAILED,
        40014: RejectCode.VENUE_AUTH_FAILED,
        40018: RejectCode.VENUE_AUTH_FAILED,
        40085: RejectCode.VENUE_AUTH_FAILED,
        # pacing
        40034: RejectCode.VENUE_RATE_LIMITED,
        429: RejectCode.VENUE_RATE_LIMITED,
        # request / order
        40015: RejectCode.VENUE_INVALID_PARAM,
        40016: RejectCode.VENUE_INVALID_PARAM,
        40017: RejectCode.VENUE_INVALID_PARAM,
        43001: RejectCode.VENUE_ORDER_NOT_FOUND,
        43012: RejectCode.VENUE_ORDER_NOT_FOUND,
        22001: RejectCode.VENUE_ORDER_NOT_FOUND,
        43011: RejectCode.VENUE_INSUFFICIENT_BALANCE,
        40762: RejectCode.VENUE_BELOW_MINIMUM,
        45110: RejectCode.VENUE_SYMBOL_NOT_TRADABLE,
    },
)

#: The paper engine. Its errors carry no label, only a message — but the
#: messages are ours, raised in ``mftik.exchange.paper.engine``, so matching on
#: them is a maintenance question rather than a guess about a third party.
PAPER = VenueErrors(
    messages=(
        # ``post-only order would cross at X (best opposite Y)`` — the engine
        # refuses before the order exists, the same promise a real venue makes.
        ("post-only order would cross", RejectCode.VENUE_POST_ONLY_WOULD_CROSS),
        ("duplicate client_order_id", RejectCode.VENUE_DUPLICATE_CLIENT_ORDER_ID),
        ("unknown client_order_id", RejectCode.VENUE_ORDER_NOT_FOUND),
        ("unknown order_id", RejectCode.VENUE_ORDER_NOT_FOUND),
        ("is not open", RejectCode.VENUE_ORDER_ALREADY_CLOSED),
        # ``insufficient liquidity`` is deliberately absent. The engine used
        # to raise it for a market order that outran the book, and mapping a
        # thin book onto an empty account was wrong twice over — the balance
        # was never the problem, and the order had already traded. That order
        # now ends CANCELED like the IOC and FOK beside it, so nothing
        # produces the message and nothing should claim to translate it.
        ("qty must be positive", RejectCode.VENUE_INVALID_PARAM),
        ("require price", RejectCode.VENUE_INVALID_PARAM),
        ("api_key and api_secret are required", RejectCode.VENUE_AUTH_FAILED),
    ),
)

#: Keyed by ``venues`` canonical name, which is also what a private client
#: reports as its ``name``.
VENUES: dict[str, VenueErrors] = {
    "Binance": BINANCE,
    "BinanceUM": BINANCE_UM,
    "BinanceCM": BINANCE_UM,
    "Bybit": BYBIT,
    "Okx": OKX,
    "Bitget": BITGET,
    "Gate": GATE,
    "GateFutures": GATE,
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
    # Before its base class: a UTA account that cannot trade is an account
    # problem, not a key problem, and this tuple is read most-specific
    # first. Without the subclass entry the ``accountmode=`` /
    # ``has no holdmode`` fragments below are unreachable, because a typed
    # match is resolved before any message fragment is.
    (BitgetAccountModeError, RejectCode.VENUE_PERMISSION_DENIED),
    (BitgetAuthError, RejectCode.VENUE_AUTH_FAILED),
)


def normalize(exc: BaseException, *, venue: str) -> int | str:
    """The reject code for an exception an adapter raised.

    Resolution runs most-specific first: the venue's own label, then its
    numeric code — refined by message where one code covers several refusals —
    then the typed exception, then, for venues that give us nothing else, a
    message match.

    An unmapped venue code comes back as itself rather than as some catch-all,
    which is what makes the table safe to extend later: today's native code is
    tomorrow's ``2xx``, and nothing in between silently changes meaning. The
    venue's own words are not returned here at all; the caller already has
    them in ``str(exc)`` and puts them on the reject as ``reason``.

    **The chain is searched, not just the exception handed in.** Every adapter
    re-raises a venue rejection as an :class:`~mftik.exchange.errors.OrderError`
    so TD publishes an order reject rather than treating it as a transport
    failure — and a plain ``OrderError`` carries neither a label nor a code.
    Read literally, that made this whole table unreachable from the order
    path: every venue rejection normalized to ``VENUE_REJECTED``. What the
    adapters do preserve is ``raise ... from exc``, so the original is one
    ``__cause__`` away, and that is where the label and the code are found.
    """
    table = VENUES.get(venue, VenueErrors())

    for candidate in _chain(exc):
        found = _venue_code(candidate, table)
        if found is not None:
            return found

    for exc_type, typed in BY_TYPE:
        if isinstance(exc, exc_type):
            return typed

    lowered = str(exc).lower()
    for fragment, matched in table.messages:
        if fragment in lowered:
            return matched

    # The venue said no and gave us nothing to key on.
    return RejectCode.VENUE_REJECTED


def is_unfilled_immediate(exc: BaseException, *, venue: str) -> bool:
    """Whether ``exc`` is a venue saying an immediate order filled nothing.

    Not a refusal, and so deliberately not a code: a fill-or-kill that took
    nothing carried out its instruction exactly, the way an IOC that took
    nothing does. The difference is only which channel the venue answers on —
    Bybit, OKX and Binance spot report it as a cancelled order on the private
    stream, while Gate and Binance's margined books raise at the call. TD asks
    this question so the two arrive at a strategy the same way, as one
    cancelled order with nothing filled.

    Read against :attr:`VenueErrors.unfilled` and nothing else. A venue whose
    spelling is not in that table falls through to :func:`normalize` and
    reaches the strategy as a reject, which is what this platform did for
    every venue before — an incomplete table costs consistency, never
    information.

    Searches ``__cause__`` for the same reason :func:`normalize` does: every
    adapter re-raises a venue rejection as a bare ``OrderError``, and the
    label or code is one link down the chain.
    """
    table = VENUES.get(venue, VenueErrors())
    if not table.unfilled:
        return False

    for candidate in _chain(exc):
        label = getattr(candidate, "label", "")
        if isinstance(label, str) and label:
            return label.upper() in table.unfilled
        code = getattr(candidate, "code", None)
        if isinstance(code, int) and not isinstance(code, bool):
            return code in table.unfilled
    return False


def normalize_reason(reason: str, *, venue: str) -> int | str:
    """The reject code for a refusal the venue put on an *order*, not a call.

    :func:`normalize` is the entry point for a venue that answers ``no`` by
    raising. Some do not: Bybit accepts the request, kills the order, and says
    why on the ``order`` topic as ``rejectReason``. That row reaches TD as an
    order update with ``OrderStatus.REJECTED`` and never passes through an
    exception, so ``normalize`` cannot be asked — which is why this exists
    rather than the caller hard-coding ``VENUE_REJECTED``.

    ``reason`` is the venue's own string, carried this far by
    :attr:`~mftik.exchange.models.Order.reject_reason`. It is matched against
    the venue's labels first and its message fragments second, the same two
    steps and the same order ``normalize`` ends with.

    The fallthrough is the same bargain too: an unmapped reason comes back as
    itself, so the detail survives until the table catches up, and only a
    refusal that came with *no* reason at all becomes ``VENUE_REJECTED`` —
    which is exactly what that code says.
    """
    reason = reason.strip()
    if not reason:
        # The venue refused it and told us nothing. Honest, and all anyone
        # knows.
        return RejectCode.VENUE_REJECTED

    table = VENUES.get(venue, VenueErrors())

    mapped = table.labels.get(reason.upper())
    if mapped is not None:
        return mapped

    lowered = reason.lower()
    for fragment, matched in table.messages:
        if fragment in lowered:
            return matched

    # Unmapped: hand back the venue's own word, not a catch-all.
    return reason


def _chain(exc: BaseException) -> list[BaseException]:
    """``exc`` and everything it was raised from, outermost first.

    Guarded against a cycle: ``raise X from Y`` where Y already points back at
    X is legal Python and would otherwise loop here.
    """
    out: list[BaseException] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        out.append(current)
        current = current.__cause__
    return out


def _venue_code(exc: BaseException, table: VenueErrors) -> int | str | None:
    """The venue's own answer on one exception, or None if it carries none."""
    label = getattr(exc, "label", "")
    if isinstance(label, str) and label:
        mapped = table.labels.get(label.upper())
        # Unmapped: hand back the venue's own label, not a catch-all.
        return mapped if mapped is not None else label

    code = getattr(exc, "code", None)
    if isinstance(code, int) and not isinstance(code, bool):
        lowered = str(exc).lower()
        for fragment, refined in table.refine.get(code, ()):
            if fragment in lowered:
                return refined
        mapped_code = table.codes.get(code)
        return mapped_code if mapped_code is not None else code

    return None


__all__ = [
    "BINANCE",
    "BITGET",
    "BY_TYPE",
    "GATE",
    "OKX",
    "PAPER",
    "BYBIT",
    "VENUES",
    "VenueErrors",
    "is_unfilled_immediate",
    "normalize",
    "normalize_reason",
]
