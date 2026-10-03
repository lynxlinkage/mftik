"""Paper's atoms.

See :mod:`mftik.exchange.atoms` for what an atom is, what each function
promises, and the invariants A1 to A6 every venue's module keeps.

**A simulated venue still has atoms**, and it gets them for the same reason
every other venue does: MD places, counts and reconciles atoms, so a venue
without them would need a second code path through the controller, the
reconciler and the tape — on the one venue every test and every example
strategy runs against.

**One endpoint**, ``public``, and one channel per ``(topic, symbol)``. There is
no wire to be verbatim about: the paper engine is in-process, so its channel
names are its own and A1 holds trivially. A channel is ``{topic}.{symbol}``
(``orderbook.BTCUSDT``). What matters is that the shape is the real one — a
frame in, platform models out, a capacity that can be reached — because a
paper venue that could not fill a connection would hide every placement bug
until the first real one.

**Every push is complete**, so the late-joiner policy is
:attr:`~mftik.exchange.atoms.JoinPolicy.NEXT_PUSH` and there is nothing to
fold.

B4-06 implements this for the one topic the paper remote public client
actually streams, the order book. Measured capacity, and any topic whose
remote stream is still unwired, stay with B7-02g (#234).
"""

from __future__ import annotations

from mftik.exchange.atoms import (
    TOPIC_ORDERBOOK,
    Atom,
    AtomOptions,
    AtomPlan,
    Capacity,
    Frame,
    InvalidAtomError,
    JoinPolicy,
    Projector,
    UnknownEndpointError,
    UnsupportedTopicError,
)
from mftik.exchange.models import InstrumentScoped, OrderBook
from mftik.exchange.tickers import Category, UniversalTicker

#: The venue every paper instrument is on. The same spelling as
#: ``PaperPublicClient.name`` and ``PAPER_VENUE``.
VENUE = "Paper"

#: The one in-process feed every paper channel is served on.
PUBLIC = "public"

#: Topics whose remote public stream exists today. ``stream_ticker`` and
#: ``stream_trades`` on the remote client are not wired, so they are not
#: atoms yet (B7-02g).
_SERVED_TOPICS = frozenset({TOPIC_ORDERBOOK})

# Not measured. The paper engine is in-process and states no connection
# ceiling. These placeholders exist so placement can read a finite
# Capacity; B7-02g replaces them if a limit is ever measured.
_UNMEASURED = Capacity(
    max_atoms=1024,
    max_messages_per_second=1_000_000,
    subscribe_batch=1024,
    subscribe_per_second=1_000_000,
)


def parse_channel(channel: str) -> tuple[str, str]:
    """``orderbook.BTCUSDT`` → ``("orderbook", "BTCUSDT")``.

    One dot, because a paper symbol does not contain one (a decimal in a
    strike is written ``D``). The topic is the platform topic: paper has no
    separate wire spelling.
    """
    topic, separator, symbol = channel.partition(".")
    if not separator or not topic or not symbol or "." in symbol:
        raise InvalidAtomError(
            f"invalid paper channel {channel!r}; expected topic.symbol, "
            "e.g. orderbook.BTCUSDT"
        )
    return topic, symbol


def _require_paper(atom: Atom) -> tuple[str, str]:
    if atom.venue != VENUE or atom.endpoint != PUBLIC:
        raise InvalidAtomError(f"not a paper public atom: {atom.atom_id}")
    topic, symbol = parse_channel(atom.channel)
    if topic not in _SERVED_TOPICS:
        raise UnsupportedTopicError(
            f"paper does not serve {topic!r}; the remote public client "
            "streams orderbook"
        )
    return topic, symbol


def atoms_for(topic: str, ticker: UniversalTicker, opts: AtomOptions) -> AtomPlan:
    """Which paper channels one platform feed needs.

    One atom, channel ``{topic}.{symbol}``, projector passthrough. ``opts``
    is ignored: a paper book push is the whole book, so a depth does not
    change the channel.
    """
    del opts
    if ticker.venue != VENUE or ticker.category is not Category.SPOT:
        raise UnsupportedTopicError(f"paper atoms are {VENUE} spot, not {ticker}")
    if topic not in _SERVED_TOPICS:
        raise UnsupportedTopicError(
            f"paper does not serve {topic!r}; the remote public client "
            "streams orderbook"
        )
    atom = Atom(VENUE, PUBLIC, f"{topic}.{ticker.symbol}")
    return AtomPlan(
        topic=topic,
        ticker=ticker,
        atoms=(atom,),
        projector=Projector.PASSTHROUGH,
    )


def decode(atom: Atom, frame: Frame) -> list[InstrumentScoped]:
    """One paper engine payload → the platform events it carries.

    The payload is a complete order book (the model the paper engine
    already publishes). A book for a different instrument is not this
    atom's event: the list is empty rather than a book published on the
    wrong subject.
    """
    _topic, symbol = _require_paper(atom)
    book = OrderBook.model_validate(dict(frame))
    if book.venue != VENUE or book.symbol != symbol:
        return []
    return [book]


def capacity(endpoint: str) -> Capacity:
    """Per-connection limits for ``public``.

    The numbers are not measured. See :data:`_UNMEASURED`.
    """
    if endpoint != PUBLIC:
        raise UnknownEndpointError(
            f"paper has no endpoint {endpoint!r}; the only one is {PUBLIC!r}"
        )
    return _UNMEASURED


def join_policy(atom: Atom) -> JoinPolicy:
    """Late-joiner semantics for one paper channel.

    Every push is the whole book, so the next one is enough and there is
    nothing to fold.
    """
    _require_paper(atom)
    return JoinPolicy.NEXT_PUSH


__all__ = [
    "PUBLIC",
    "VENUE",
    "atoms_for",
    "capacity",
    "decode",
    "join_policy",
    "parse_channel",
]
