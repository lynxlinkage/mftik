"""Atoms — one exchange subscription, named in the exchange's own words.

An **atom** is the smallest thing MD can ask a venue for: one entry in one
``SUBSCRIBE`` on one connection. Its identity is ``(venue, endpoint,
channel)``, and ``channel`` is **verbatim the venue's own parameter** —
``btcusdt@bookTicker``, ``ticker.BTC-27DEC26-100000-C.100ms``,
``tickers:BTC-USDT-SWAP``. Nothing in it is platform vocabulary.

That is the opposite of how MD used to ask. A platform feed key
(``bestquote.Deribit_Perp_BTCUSD``) was handed to a per-venue connector, which
resolved it to a ``stream_*`` method, which decided what to subscribe to — so
the unit MD could count, place and reconcile was a *product topic*, and a topic
is not what a venue acknowledges. Two topics sharing one venue channel
(Binance ``ticker`` and ``bestquote`` both read ``@bookTicker``) had to be
refcounted behind the connector's back, and one topic needing two channels had
to be assembled inside the connector, on one connection, because that is where
the two pumps could see each other.

Atoms invert it. A feed is one or more atoms (F19), each atom is on exactly one
connection, and that connection is free to be a different process from its
sibling's (F17). What used to be hidden inside a venue client becomes four pure
functions this module gives a shape to, one implementation per venue in
``mftik.exchange.<venue>.atoms``:

``atoms_for(topic, ticker, opts)``
    Which atoms a platform feed is made of, and how their events compose.
``decode(atom, frame)``
    One venue message → the platform models it carries. One frame may carry
    several — Deribit's ``ticker.*`` row is a quote *and* the greeks *and* the
    open interest — which is the other direction of F19's many-to-many.
``capacity(endpoint)``
    What one connection to that endpoint can hold.
``join_policy(atom)``
    What a late joiner on an already-live atom sees.

**State authority (§3.3): this layer holds none.** These are pure functions of
their arguments. Everything that persists across a frame belongs to the MD
connection worker: the ``observed`` set (acked by the venue, keyed by
connection epoch), the folded order book, the per-atom ``seq``, the tape
append. The connection worker is the authority for market content (F21), and
this module is only the vocabulary it speaks.

**Invariants.**

* **A1 — the channel is the venue's string, verbatim.** An ``atom_id``
  contains no platform topic and no
  :class:`~mftik.exchange.tickers.UniversalTicker`. A venue that spells its
  subscription as a structure (OKX's ``arg``) normalizes it to one string here,
  and that string is what goes on the wire.
* **A2 — many-to-many, both ways (F19).** One feed may need several atoms; one
  atom's frame may produce several platform events. Neither direction is a
  special case for the caller.
* **A3 — every function here is pure.** No connection, no database, no clock.
  Symbol resolution happens before :func:`atoms_for` is called: it takes an
  already-resolved ticker, and the venue's own spelling of the instrument
  reaches the channel through that.
* **A4 — what comes out of :func:`decode` is a platform model (F21).** STS
  never sees a venue's raw frame and never folds a book.
* **A5 — ``join_policy`` is about late joiners, not about joining feeds.** It
  is MdVenueSubscriptions I5: a consumer attaching to an atom that another
  consumer already made live either gets a replay or is documented silent.
  Composing a feed out of several atoms is :attr:`AtomPlan.projector`, and it
  happens on the STS ingress rather than in MD.
* **A6 — the atom's subject is not here.** ``md.a.{venue}.{hash}`` and the
  stable hash of an ``atom_id`` belong to the protocol (IF-01). This module
  owns the identity that gets hashed and nothing about the wire it travels on.

Null data until B7: every venue's implementation raises
``NotImplementedError("IF-08")``, and the contract tests describing what it
will do are ``xfail(strict=True)``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from mftik.exchange.errors import ExchangeError
from mftik.exchange.models import InstrumentScoped
from mftik.exchange.tickers import UniversalTicker

#: What separates the three parts of an ``atom_id``. Venue names are CamelCase
#: and endpoint names are single lowercase words, so neither can contain it; a
#: channel can (OKX's ``tickers:BTC-USDT-SWAP``), which is why only the first
#: two occurrences are split on.
SEPARATOR = ":"

# --- platform feed topics --------------------------------------------------
#
# The platform's own vocabulary: what a strategy declares, and what
# ``atoms_for`` translates. One fixed list for every venue, so a venue that
# does not serve a topic refuses it by name rather than serving something
# adjacent. These moved here from the deleted ``VenueSession._open`` (RM-05),
# next to the function that consumes them now.

TOPIC_ORDERBOOK = "orderbook"
TOPIC_TICKER = "ticker"
TOPIC_TRADE = "trade"
#: The same tape with the venue's own coalescing applied — one print per
#: aggressing order per price rather than one per match.
TOPIC_AGG_TRADE = "aggtrade"
TOPIC_BEST_QUOTE = "bestquote"
#: Public forced-liquidation prints.
TOPIC_LIQUIDATION = "liquidation"
#: Predicted funding rate for the upcoming settlement.
TOPIC_FUNDING_RATE = "funding_rate"
#: Current open interest, one side.
TOPIC_OPEN_INTEREST = "open_interest"
#: Live option greeks, IVs and mark.
TOPIC_GREEKS = "greeks"
#: Klines need an interval and a feed key is only ``topic.ticker``, so the
#: interval rides in the topic: ``kline_1m.Paper_Spot_BTCUSDT``.
KLINE_PREFIX = "kline_"


class AtomError(ExchangeError):
    """Base for everything this layer refuses."""


class UnsupportedTopicError(AtomError):
    """This venue serves no such feed, or does not serve it on that market.

    Raised by :func:`atoms_for` rather than answered with an empty plan: a
    subscription that resolves to no atoms is indistinguishable from one the
    venue acknowledged and never pushes, which is the failure atoms exist to
    make impossible.
    """


class UnknownEndpointError(AtomError):
    """No capacity is stated for that endpoint, so nothing can be placed on it.

    Raised rather than defaulted, for the reason
    :class:`~mftik.exchange.binance.future.streams.UnknownStreamError` is: a
    guessed limit is a placement decision nobody checked.
    """


class InvalidAtomError(AtomError):
    """The string is not a well-formed ``atom_id``."""


class JoinPolicy(StrEnum):
    """What a consumer attaching to an already-live atom sees (I5).

    A policy per atom rather than per venue: on one Bybit connection a folded
    book replays and a ticker-shared field does not, and that difference is the
    channel's, not the venue's.
    """

    #: Every push is the whole state, so the next one is enough. Binance
    #: ``@bookTicker``, OKX ``bbo-tbt`` / ``books5``, Deribit's depth-capped
    #: ``book.*``.
    NEXT_PUSH = "next_push"
    #: The connection worker holds folded state and replays it into the new
    #: consumer before the next push — an in-process replay of a message it
    #: already applied, never a REST round trip.
    SNAPSHOT_REPLAY = "snapshot_replay"
    #: Silent until the venue's next push happens to carry the field this
    #: consumer reads. The ticker-shared topics (Bybit and Gate
    #: ``funding_rate`` / ``open_interest``, Deribit's greeks) are this, and it
    #: is documented rather than filled in from REST.
    SILENT = "silent"


class Projector(StrEnum):
    """How a feed's events are produced from its atoms' events (F19).

    Named here and applied on the STS ingress, because the atoms of one feed
    may be on different connections and therefore in different processes (F17);
    MD publishes atomic events and composes nothing. The functions are
    platform-generic — no venue code, negligible arithmetic — so running them
    on the ingress does not violate I4.
    """

    #: One atom, and its decoded events are the feed's events unchanged.
    PASSTHROUGH = "passthrough"
    #: ``join(BestQuote, TickerStats) -> Ticker``: emit on every stats print,
    #: carrying the latest quote, and emit nothing until a quote has arrived.
    #: Binance UM and CM ``ticker`` are the only users in the tree.
    QUOTE_STATS = "quote_stats"


@dataclass(frozen=True, order=True)
class Atom:
    """One venue subscription: ``(venue, endpoint, channel)``.

    Frozen and ordered so it can key a dict, sit in a desired set and sort
    without a key function — the same reasons
    :class:`~mftik.exchange.tickers.UniversalTicker` is.

    ``endpoint`` is the venue's own split of its public feeds, and it is part of
    the identity because it decides which connection the atom can go on:
    Binance futures answers book streams on ``/public`` and everything else on
    ``/market``, and a subscribe on the wrong one is acknowledged and then never
    pushes. A venue with one public host has one endpoint name.

    Nothing here identifies the *instrument* in platform terms. The channel
    carries the venue's spelling, and turning that back into a
    :class:`~mftik.exchange.tickers.UniversalTicker` is not something a pure
    function can do — see A3.
    """

    venue: str
    endpoint: str
    channel: str

    def __post_init__(self) -> None:
        for name, value in (("venue", self.venue), ("endpoint", self.endpoint)):
            if not value or SEPARATOR in value:
                raise InvalidAtomError(
                    f"invalid {name} {value!r} in an atom; it must be non-empty "
                    f"and contain no {SEPARATOR!r}"
                )
        if not self.channel:
            raise InvalidAtomError("an atom's channel cannot be empty")

    def __str__(self) -> str:
        return self.atom_id

    @property
    def atom_id(self) -> str:
        """The canonical rendering: ``BinanceUM:public:btcusdt@bookTicker``.

        The string the protocol hashes into an ``md.a.*`` subject (IF-01), and
        the key the tape and its coverage are recorded under (F20). Stable
        across a controller restart because it is a pure function of the three
        parts.
        """
        return SEPARATOR.join((self.venue, self.endpoint, self.channel))

    @classmethod
    def parse(cls, atom_id: str) -> Atom:
        """Read an ``atom_id`` back. Only the first two separators split.

        A channel may contain :data:`SEPARATOR` — OKX's normalized ``arg`` is
        ``tickers:BTC-USDT-SWAP`` — and the venue and the endpoint may not,
        which is what makes the round trip unambiguous.
        """
        parts = (atom_id or "").split(SEPARATOR, 2)
        if len(parts) != 3:
            raise InvalidAtomError(
                f"invalid atom id {atom_id!r}; expected "
                f"venue{SEPARATOR}endpoint{SEPARATOR}channel, e.g. "
                f"BinanceUM{SEPARATOR}public{SEPARATOR}btcusdt@bookTicker"
            )
        return cls(venue=parts[0], endpoint=parts[1], channel=parts[2])


@dataclass(frozen=True)
class AtomOptions:
    """What a feed needs that its topic and its ticker do not say.

    Deliberately small, and platform-level. A venue's own knobs — Binance's
    update speed, Deribit's book grouping — are the adapter's choice and are
    not surfaced here: a strategy that could pick them would be declaring a
    venue channel, which is the thing atoms took away from it.
    """

    #: Canonical kline interval (``1m``), for the ``kline_*`` topics. Empty for
    #: every other topic, and a kline topic without one is a refusal.
    interval: str = ""
    #: Book levels wanted, or 0 for the adapter's default. A venue serves a
    #: fixed set (Binance 5/10/20, Deribit 1/10/20) and refuses the rest,
    #: because a level count it does not serve is acknowledged and then silent.
    depth: int = 0


@dataclass(frozen=True)
class AtomPlan:
    """Which atoms one platform feed is made of, and how they compose.

    What :func:`atoms_for` answers, and what MD records when an intent is
    registered: the controller keeps ``{feed: [atom_id]}`` and the ingress
    keeps the projector. A feed whose atoms land on different connections is
    the normal case (F17); the plan does not say where they go, only what they
    are.

    A feed is ``down`` when **any** of its atoms is down, and ``live`` only once
    all of them are back (F19). That rule belongs to the ingress state machine
    rather than here, but it is why the plan lists the atoms instead of
    flattening them.
    """

    #: The platform topic this plan resolves — ``ticker``, ``kline_1m``.
    topic: str
    #: The instrument in platform terms. The atoms carry the venue's.
    ticker: UniversalTicker
    #: Every atom the feed needs, in the order the adapter named them. One is
    #: the common case; two is Binance UM/CM ``ticker``.
    atoms: tuple[Atom, ...]
    #: How the atoms' events become the feed's.
    projector: Projector = Projector.PASSTHROUGH

    @property
    def atom_ids(self) -> tuple[str, ...]:
        """The plan's atoms as canonical strings, for ``{feed: [atom_id]}``."""
        return tuple(atom.atom_id for atom in self.atoms)


@dataclass(frozen=True)
class Capacity:
    """What one connection to one endpoint can hold, and how fast to fill it.

    Read by the MD controller's placement (F22): atoms go onto an existing
    connection until :attr:`max_atoms` is reached, and a new connection worker
    is started only then. Measured per venue rather than guessed — a limit above
    what the venue enforces is a connection the venue drops, and one below it is
    a process per handful of atoms.
    """

    #: Most atoms one connection may hold.
    max_atoms: int
    #: Messages per second the venue will push down one connection before it
    #: throttles or disconnects. Placement treats it as a second ceiling, so a
    #: connection can be full on rate while under :attr:`max_atoms`.
    max_messages_per_second: float
    #: Most atoms in one ``SUBSCRIBE`` frame. The reconciler batches its diff
    #: to this.
    subscribe_batch: int
    #: Frames per second the reconciler's token bucket allows.
    subscribe_per_second: float


#: A venue's own message for one atom, JSON-decoded and nothing more.
#:
#: The subscription envelope is already off: the connection worker's transport
#: routes a frame to the atom it belongs to, so what reaches :func:`decode` is
#: the payload body — Deribit's ``params.data``, Binance's combined-stream
#: ``data``, Bybit's ``data``. Decoding happens once per frame (§6.3).
Frame = Mapping[str, Any]


class AtomAdapter(Protocol):
    """The four pure functions each ``mftik.exchange.<venue>.atoms`` provides.

    A protocol a *module* satisfies, not a class: there is no shared venue
    interface in this tree and there is not one here either (see
    :mod:`mftik.exchange.base`). What this states is the shape MD drives, so
    that a venue missing a function fails where it is looked up rather than at
    the first frame.
    """

    def atoms_for(
        self, topic: str, ticker: UniversalTicker, opts: AtomOptions
    ) -> AtomPlan:
        """The atoms one platform feed needs on this venue.

        Raises :class:`UnsupportedTopicError` when the venue serves no such
        feed, or does not serve it on that instrument's market.
        """

    def decode(self, atom: Atom, frame: Frame) -> list[InstrumentScoped]:
        """The platform events one frame on ``atom`` carries.

        Several events from one frame is normal: a Deribit ``ticker.*`` row
        carries a quote, the greeks and the open interest at once. An empty list
        is a frame that carried nothing this atom's consumers asked for — a
        heartbeat, or a delta naming none of the fields they read.
        """

    def capacity(self, endpoint: str) -> Capacity:
        """Limits for one connection to ``endpoint``.

        Raises :class:`UnknownEndpointError` for an endpoint this venue does not
        split its feeds over.
        """

    def join_policy(self, atom: Atom) -> JoinPolicy:
        """What a consumer joining this atom while it is live sees."""


def kline_interval(topic: str) -> str:
    """The interval inside a ``kline_*`` topic, or ``""`` for other topics.

    ``kline_1m`` → ``1m``. Not validated: which windows a venue serves is the
    adapter's answer, and whether a window is spelled canonically is
    :func:`~mftik.exchange.intervals.normalize_interval`'s.
    """
    if not topic.startswith(KLINE_PREFIX):
        return ""
    return topic[len(KLINE_PREFIX) :]


__all__ = [
    "KLINE_PREFIX",
    "SEPARATOR",
    "TOPIC_AGG_TRADE",
    "TOPIC_BEST_QUOTE",
    "TOPIC_FUNDING_RATE",
    "TOPIC_GREEKS",
    "TOPIC_LIQUIDATION",
    "TOPIC_OPEN_INTEREST",
    "TOPIC_ORDERBOOK",
    "TOPIC_TICKER",
    "TOPIC_TRADE",
    "Atom",
    "AtomAdapter",
    "AtomError",
    "AtomOptions",
    "AtomPlan",
    "Capacity",
    "Frame",
    "InvalidAtomError",
    "JoinPolicy",
    "Projector",
    "UnknownEndpointError",
    "UnsupportedTopicError",
    "kline_interval",
]
