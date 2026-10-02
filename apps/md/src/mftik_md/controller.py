"""MD controller: which atoms are wanted, and which connection holds them.

Demand comes from three places (§6.2): a session intent, a standing
subscription, and a selector. The desired set is their union. Each atom
remembers every owner that asked for it, and placement pins that atom to
one connection until nobody wants it any more (F22).

**State authority (§3.3).** Two rows belong to this layer, and both are
held by the MD controller:

* The desired atoms of each connection, and the ``generation`` they were
  pushed with. Memory only. It can be recomputed from intents, standing
  subscriptions and selector state. The connection worker reads it.
  Until a restarted controller has recomputed and pushed a new
  generation, the worker keeps the set it already has (P5). This module
  has no operation that clears a worker because the controller restarted.
* The selector's universe, epoch and recentring state. Those are derived
  by :mod:`mftik_md.selector` and persisted so a restart continues
  instead of re-centring. The session hears the result on
  ``md.universe.{session_id}``. The table is IF-14's; this module does
  not touch it.

``controller_epoch`` is the other half of a generation. It increases in
the database once per controller start (§6.2). §8.4 does not name the
column, and IF-14 owns the schema, so this module takes the integer it
was given and does not store one. A worker accepts a generation only
when it is strictly greater than the one it holds (F18); that
comparison is the connection worker's (IF-10). Minting the pair is
:meth:`MdOrchestrator.generation`.

**Invariants.**

* **C1 — owners, not refcounts.** ``desired_atoms`` is the union of the
  demands it is given. An owner named twice still owns each atom once
  (the same rule as intent put, P-1). Standing subscriptions are owners
  too; they are config, not sessions, so a procman report never releases
  them. Dropping a terminal owner is the caller's: §8.2 rule 3, applied
  by :func:`gc_owners`, or an explicit ``md.intent.delete``.
* **C2 — placement does not move an atom (F22).** An atom already on a
  connection stays there until it leaves the desired set. A smaller
  capacity does not evict it. A hole on an older connection does not
  pull it back. Two connections are never merged.
* **C3 — a new atom prefers an existing connection.** Same
  ``(venue, endpoint)`` only, the one with room under ``max_atoms`` and
  the lowest ``n``. A new connection is opened only when none has room.
  Its ``n`` is the lowest non-negative integer that connection's
  ``(venue, endpoint)`` does not already use. ``place`` sees the live
  connections, so an ``n`` reappears only once that worker is gone from
  the input — the same delete-before-create the supervisor already
  applies to a restart (F22).
* **C4 — an empty connection is not a connection.** It is absent from
  the placement, and its worker ends. It is not a legal
  :class:`ConnView` either.
* **C5 — one publisher (P4).** An atom is on at most one connection.
  The venue and the endpoint travel with the atom (A1), so it can only
  sit on a connection of that same pair.
* **C6 — generation is ``(controller_epoch, seq)``.** Compared
  lexicographically, so epoch 2 beats any seq of epoch 1. ``seq`` is
  the caller's; this object does not keep a counter. Every push carries
  the whole set for that connection, not a diff (P2, F18). The object
  the worker is handed is :class:`ConnDesired`: which connection, plus
  the connection module's :class:`~mftik_md.conn.Desired`.
* **C7 — expiry is the listed settlement, not a re-derivation.** An
  instrument whose listed expiry has arrived leaves the desired set,
  and each owner who held it is told with ``md.feed.end``
  (``state=expired``, ``code=expired``), one notice per
  ``(owner, topic)``. This replaces ``_expiry_tasks``. An atom has no
  platform ticker in it (A3), so the binding that resolved the feed is
  what names the topic.
* **C8 — fail-static (P5).** :func:`gc_owners` releases nothing when the
  report itself is missing (F32), and nothing on a single missed
  report. A session that is in the report is kept, including one STS
  included because it is ``restarting`` (R4) — this layer does not
  special-case that word; it trusts the set.

**Not settled here.** The contract tests do not decide these, and the
stubs do not guess:

* ``Capacity.max_messages_per_second`` is a second ceiling (§6.1), but
  nothing in ``place``'s inputs says how many messages one atom costs.
  The tests cover ``max_atoms`` and stickiness only.
* A gap where the whole procman report stops releases nothing (C8).
  Whether that gap clears the absence streak — so a miss before a
  controller roll and a miss on the first report after it would *not*
  be the two consecutive samples — is not specified.
* Where the database keeps ``controller_epoch``.

Null data until B8: :func:`desired_atoms`, :func:`place`, :func:`expire`
and :func:`gc_owners` raise ``NotImplementedError("IF-09")``. The types
around them are real, the way an atom's identity is real while its
venue adapter is not.

A connection's identity, its generation and the set pushed to one
worker are the connection module's: :class:`mftik_md.conn.ConnId`,
:class:`mftik_md.conn.Generation` and :class:`mftik_md.conn.Desired`.
This module imports those three. :class:`ConnDesired` is which
connection a :class:`~mftik_md.conn.Desired` belongs to — a placement
names many connections, and the worker already knows which process it
is. :class:`ConnView` is the input :func:`place` reads.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from mftik.exchange.atoms import Atom, Capacity
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import IntentOwner

from mftik_md.conn import ConnId, Desired, Generation

#: What :func:`gc_owners` and its neighbours raise with. The ticket number
#: is the whole of the common-acceptance rule; the rest names the batch
#: that replaces the stub.
_TICKET = "IF-09"


class ControllerError(Exception):
    """A value this layer will not treat as controller state.

    A placement that puts one atom on two connections, a negative epoch,
    a report generation that went backwards. Distinct from
    :class:`~mftik.exchange.atoms.UnknownEndpointError`, which is the
    venue adapter saying it has no capacity to give, and from
    :class:`NotImplementedError`, which is this ticket's null data.
    """


def _counter(name: str, value: int) -> int:
    """A generation part: a non-negative int, and not ``True``."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int, got {type(value).__name__}")
    if value < 0:
        raise ControllerError(f"{name} must be >= 0, got {value}")
    return value


def _aware(name: str, value: datetime) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ControllerError(f"{name} must be a timezone-aware datetime")


def _check_atoms(conn_id: ConnId, atoms: frozenset[Atom]) -> None:
    if not isinstance(conn_id, ConnId):
        raise TypeError(f"connection id must be a ConnId, got {conn_id!r}")
    if not isinstance(atoms, frozenset):
        raise TypeError(f"connection {conn_id} atoms must be a frozenset")
    if not atoms:
        raise ControllerError(
            f"connection {conn_id} has no atoms; an empty connection's "
            "worker has ended and is not a placement input (C4)"
        )
    for atom in atoms:
        if not isinstance(atom, Atom):
            raise TypeError(
                f"connection {conn_id} holds {atom!r}, which is not an Atom"
            )
        if (atom.venue, atom.endpoint) != (conn_id.venue, conn_id.endpoint):
            raise ControllerError(
                f"atom {atom.atom_id} cannot sit on {conn_id}; the venue "
                "and the endpoint are part of the atom (C5)"
            )


@dataclass(frozen=True)
class ConnView:
    """A live connection and the atoms already on it. Input to :func:`place`.

    Sticky placement is impossible to ask for without this: the function
    has to be able to see where an atom already is so it can leave it
    there (C2).
    """

    id: ConnId
    atoms: frozenset[Atom]

    def __post_init__(self) -> None:
        _check_atoms(self.id, self.atoms)


@dataclass(frozen=True)
class ConnAssignment:
    """One connection in a placement result. Never empty (C4)."""

    id: ConnId
    atoms: frozenset[Atom]

    def __post_init__(self) -> None:
        _check_atoms(self.id, self.atoms)


@dataclass(frozen=True)
class Placement:
    """Every live connection after :func:`place`, and the atoms on each.

    The full picture, not a diff: the orchestrator compares it with the
    previous one to see which workers to start and which have ended.
    Connections are stored in :class:`ConnId` order so two placements of
    the same decision compare equal.
    """

    conns: tuple[ConnAssignment, ...] = ()

    def __post_init__(self) -> None:
        seen_ids: set[ConnId] = set()
        seen_atoms: set[Atom] = set()
        for conn in self.conns:
            if not isinstance(conn, ConnAssignment):
                raise TypeError(
                    f"a placement holds {conn!r}, which is not a ConnAssignment"
                )
            if conn.id in seen_ids:
                raise ControllerError(
                    f"connection {conn.id} appears twice in one placement"
                )
            seen_ids.add(conn.id)
            overlap = seen_atoms & set(conn.atoms)
            if overlap:
                atom = next(iter(overlap))
                raise ControllerError(
                    f"atom {atom.atom_id} is on two connections (C5)"
                )
            seen_atoms.update(conn.atoms)
        ordered = tuple(sorted(self.conns, key=lambda conn: conn.id))
        object.__setattr__(self, "conns", ordered)

    def on(self, conn_id: ConnId) -> frozenset[Atom]:
        """The atoms on ``conn_id``, or an empty set when it is not placed."""
        for conn in self.conns:
            if conn.id == conn_id:
                return conn.atoms
        return frozenset()

    def conn_of(self, atom: Atom) -> ConnId | None:
        """The one connection ``atom`` was placed on, or None."""
        for conn in self.conns:
            if atom in conn.atoms:
                return conn.id
        return None

    def views(self) -> tuple[ConnView, ...]:
        """This result, shaped as the next call's ``conns`` argument."""
        return tuple(ConnView(conn.id, conn.atoms) for conn in self.conns)


@dataclass(frozen=True)
class ConnDesired:
    """Which connection a :class:`~mftik_md.conn.Desired` is for (F18, C6).

    ``desired`` is the connection module's object: the whole atom set,
    not a diff, and the generation the worker compares with the one it
    holds. ``id`` is only here because the controller names many
    connections in one result. The worker already knows its own id, so
    it is handed ``desired`` and not a second shape. Nothing in this
    ticket builds or sends one.

    An empty atom set is not a push. That worker has ended (C4). An atom
    whose venue or endpoint is not ``id``'s cannot sit on it (C5).
    """

    id: ConnId
    desired: Desired

    def __post_init__(self) -> None:
        if not isinstance(self.desired, Desired):
            raise TypeError("desired must be the connection module's Desired")
        _check_atoms(self.id, self.desired.atoms)


@dataclass(frozen=True, order=True)
class StandingOwner:
    """A standing subscription. Config, not a session (§6.2).

    It has no ``procman.report`` row and no terminal phase.
    :func:`gc_owners` does not see it. It leaves the desired set when
    the config stops naming it, which is the caller dropping the demand.
    """

    name: str

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise ControllerError("a standing subscription needs a name")


#: Who can own an atom. A session owner is the protocol's pair (P-2);
#: a standing owner is the config's name. One set holds both.
Owner = IntentOwner | StandingOwner


@dataclass(frozen=True)
class Demand:
    """The atoms one owner wants, already resolved.

    Resolution (a feed key to atoms) happens through the venue's
    ``atoms_for`` when the intent is registered (IF-08, §6.1). By the
    time demand reaches here the atoms exist, and the same owner naming
    the same atom twice is still one owner (C1). An empty atom set is
    legal: an owner that has not resolved anything yet simply does not
    appear in the union.
    """

    owner: Owner
    atoms: frozenset[Atom]

    def __post_init__(self) -> None:
        if not isinstance(self.owner, (IntentOwner, StandingOwner)):
            raise TypeError(
                "a demand's owner is an IntentOwner or a StandingOwner"
            )
        if not isinstance(self.atoms, frozenset):
            raise TypeError("a demand's atoms must be a frozenset")
        for atom in self.atoms:
            if not isinstance(atom, Atom):
                raise TypeError(f"demand holds {atom!r}, which is not an Atom")


@dataclass(frozen=True)
class FeedBinding:
    """One platform feed an owner resolved onto atoms.

    The reverse of ``{feed: [atom_id]}`` (§6.1). Expiry needs it because
    ``md.feed.end`` is per ``(owner, topic)`` and an atom's channel is
    the venue's string, not a platform topic (A1, A3). ``expiry`` is the
    listed settlement, or None for an instrument that does not settle
    (a spot pair, a perpetual). Several topics may share one atom
    (Deribit's ``ticker.*``); several atoms may serve one topic
    (Binance UM ``ticker``).
    """

    owner: Owner
    ticker: UniversalTicker
    topic: str
    atoms: frozenset[Atom]
    expiry: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.owner, (IntentOwner, StandingOwner)):
            raise TypeError(
                "a binding's owner is an IntentOwner or a StandingOwner"
            )
        if not isinstance(self.ticker, UniversalTicker):
            raise TypeError("a binding's ticker must be a UniversalTicker")
        if not isinstance(self.topic, str) or not self.topic:
            raise ControllerError("a binding needs a topic")
        if not isinstance(self.atoms, frozenset):
            raise TypeError("a binding's atoms must be a frozenset")
        for atom in self.atoms:
            if not isinstance(atom, Atom):
                raise TypeError(f"binding holds {atom!r}, which is not an Atom")
        if self.expiry is not None:
            _aware("expiry", self.expiry)


@dataclass(frozen=True)
class ExpiryNotice:
    """One ``md.feed.end`` to send because an instrument settled (C7).

    ``state`` and ``code`` are the wire values ``FeedEnd`` already uses
    for listed settlement. ``expiry`` is that settlement time. One notice
    per ``(owner, ticker, topic)``, not per atom: two topics on one
    shared atom are two notices, and two atoms of one topic are one.
    """

    owner: Owner
    ticker: UniversalTicker
    topic: str
    expiry: datetime
    state: str = "expired"
    code: str = "expired"

    def __post_init__(self) -> None:
        if self.state != "expired" or self.code != "expired":
            raise ControllerError(
                "an expiry notice is state=expired, code=expired; "
                f"got state={self.state!r} code={self.code!r}"
            )
        _aware("expiry", self.expiry)


@dataclass(frozen=True)
class Expiry:
    """What :func:`expire` concludes. ``desired`` is the set that remains."""

    desired: Mapping[Atom, frozenset[Owner]]
    notices: tuple[ExpiryNotice, ...]


@dataclass(frozen=True)
class OwnerGc:
    """What one procman report did to the session owners we hold (C8).

    ``release`` is who to drop now. ``absent`` is who was missing from
    this report and is not yet released, handed back as
    ``previous_absent`` next time. ``generation`` is the report
    generation this result consumed, so a replay is recognisable.

    ``release`` is an output. Feeding a previous result's ``release``
    back in is not how the next call learns anything; ``absent`` and
    ``generation`` are.
    """

    release: frozenset[IntentOwner]
    absent: frozenset[IntentOwner]
    generation: int | None


def desired_atoms(
    demands: Sequence[Demand],
) -> Mapping[Atom, frozenset[Owner]]:
    """Union of ``demands``. Each atom maps to the owners that named it.

    B8-01. No refcount (C1): the same owner in two demands is one owner.
    An owner who is not in ``demands`` is not in the result, which is
    how a released session disappears. Standing owners and session
    owners share the set.
    """
    raise NotImplementedError(f"{_TICKET}: desired atoms are B8-01")


def place(
    desired: Collection[Atom],
    conns: Collection[ConnView],
    capacity: Mapping[tuple[str, str], Capacity],
) -> Placement:
    """Pin ``desired`` onto connections. Sticky, and it does not merge (F22).

    ``capacity`` is keyed by ``(venue, endpoint)`` — per endpoint, not
    per connection. Only :attr:`~mftik.exchange.atoms.Capacity.max_atoms`
    is a placement input today. The message-rate ceiling is on the same
    object and is not applied here; nothing says what one atom costs.

    An endpoint with no entry raises
    :class:`~mftik.exchange.atoms.UnknownEndpointError` rather than
    opening an unbounded connection. ``max_atoms < 1`` raises
    :class:`ControllerError`: a capacity of zero would open a connection
    per atom and never fill one. An atom already on a connection stays
    even when that connection is over ``max_atoms`` (C2).

    B8-02.
    """
    raise NotImplementedError(f"{_TICKET}: place is B8-02")


def expire(
    desired: Mapping[Atom, frozenset[Owner]],
    bindings: Sequence[FeedBinding],
    now: datetime,
) -> Expiry:
    """Drop instruments whose listed expiry has arrived, and notify (C7).

    An instrument is expired when a binding gives it an ``expiry`` and
    ``expiry <= now``. A binding with no expiry never expires. An atom
    with no binding is kept: without a listed settlement there is nothing
    to enforce, and dropping it would be a guess. A notice goes to an
    owner who still owns an atom of that binding in ``desired``. The
    moment of settlement is already expired; a microsecond before it is
    not.

    Disagreement — one atom bound to two tickers, or two expiries — is
    a caller's bug. The contract tests do not define it.

    B8-04.
    """
    _aware("now", now)
    raise NotImplementedError(f"{_TICKET}: expiry is B8-04")


def gc_owners(
    held: Collection[IntentOwner],
    previous_absent: Collection[IntentOwner],
    previous_generation: int | None,
    *,
    report: Collection[IntentOwner] | None,
    report_generation: int | None,
) -> OwnerGc:
    """Release session owners absent from two consecutive reports (§8.2).

    One call is one instance's ``procman.report.sts.{instance}``. The
    caller turns that report into :class:`~mftik.protocol.IntentOwner`
    values; this function does not parse worker ids (IF-03 owns the
    spelling ``sts/session/<id>``). ``held`` is the session owners we
    currently have demands for. Standing owners are not passed in.

    * A report whose generation is not newer than ``previous_generation``
      is a replay. It is not a second sample: ``release`` is empty and
      the streak is unchanged.
    * The first report an owner is missing from does not release them.
      They come back in ``absent``.
    * The next newer report they are also missing from releases them.
    * An owner who is in the report is not released, and is not absent,
      even if the previous report missed them.
    * ``report is None`` means the publication stopped. Release nothing
      (F32, C8). Whether ``absent`` is cleared by that gap is not
      specified; the contract test only asserts that nobody is released.

    A generation that goes backwards, or a report that arrives without
    one, is refused. A stopped report has no generation.

    B8-01.
    """
    if report is None:
        if report_generation is not None:
            raise ControllerError(
                "a stopped report has no generation; "
                "pass report_generation=None"
            )
    elif report_generation is None:
        raise ControllerError("a report needs its generation")
    else:
        _counter("report_generation", report_generation)
    if previous_generation is not None:
        _counter("previous_generation", previous_generation)
        if (
            report_generation is not None
            and report_generation < previous_generation
        ):
            raise ControllerError(
                "report generation went backwards; publications only increase"
            )
    raise NotImplementedError(f"{_TICKET}: owner GC is B8-01")


class MdOrchestrator:
    """The MD controller's façade over the pure functions above.

    Constructing one records the epoch this process was started with and
    nothing else. It does not read the database, publish a generation,
    or keep the sequence counter (C6). Every method that would decide
    something raises ``NotImplementedError("IF-09")``; :meth:`generation`
    only builds the pair.
    """

    def __init__(self, controller_epoch: int) -> None:
        self.controller_epoch = _counter("controller_epoch", controller_epoch)

    def generation(self, seq: int) -> Generation:
        """``(controller_epoch, seq)``. Nothing is published."""
        return Generation(self.controller_epoch, seq)

    def desired(
        self, demands: Sequence[Demand]
    ) -> Mapping[Atom, frozenset[Owner]]:
        """:func:`desired_atoms`."""
        return desired_atoms(demands)

    def place(
        self,
        desired: Collection[Atom],
        conns: Collection[ConnView],
        capacity: Mapping[tuple[str, str], Capacity],
    ) -> Placement:
        """:func:`place`. The epoch is not an input; placement ignores it."""
        return place(desired, conns, capacity)

    def expire(
        self,
        desired: Mapping[Atom, frozenset[Owner]],
        bindings: Sequence[FeedBinding],
        now: datetime,
    ) -> Expiry:
        """:func:`expire`."""
        return expire(desired, bindings, now)

    def gc_owners(
        self,
        held: Collection[IntentOwner],
        previous_absent: Collection[IntentOwner],
        previous_generation: int | None,
        *,
        report: Collection[IntentOwner] | None,
        report_generation: int | None,
    ) -> OwnerGc:
        """:func:`gc_owners`."""
        return gc_owners(
            held,
            previous_absent,
            previous_generation,
            report=report,
            report_generation=report_generation,
        )


__all__ = [
    "ConnAssignment",
    "ConnDesired",
    "ConnView",
    "ControllerError",
    "Demand",
    "Expiry",
    "ExpiryNotice",
    "FeedBinding",
    "MdOrchestrator",
    "Owner",
    "OwnerGc",
    "Placement",
    "StandingOwner",
    "desired_atoms",
    "expire",
    "gc_owners",
    "place",
]
