"""MD connection worker — one websocket, one process (F17).

A connection worker owns **one** public socket and everything that can only
be known by watching it. The controller pushes a complete desired set; this
process is the only one that sees the venue's acks, the frames, and the
holes in a book. Market data never goes back through the controller (P1).

**State authority (§3.3).** This layer is the sole writer of:

* **Observed** — the atoms the venue has actually acked on this socket.
  Keyed by the connection epoch, held in memory. Readers are this worker
  and the state broadcast. A reconnect zeroes it; the next diff backfills.
* **Market content, including the folded book (F21).** Memory, published
  on ``md.a.*`` as platform models. A reconnect rebuilds a book from the
  venue's snapshot. STS never sees a raw frame and never folds.
* **Feed liveness.** Broadcast on ``md.w.*``. Ten seconds of silence is
  the *listener's* rule for calling a feed ``down`` (§5.6); it only
  notifies, and it is not a timer this process runs.
* **Per-atom ``seq`` (F25).** Stamped on each publication. Contiguous
  inside one incarnation; a new incarnation starts again from the same
  origin. See C6 for the boundary this ticket does not settle.
* **Tape and coverage** for an atom this worker holds (F20). Redis, one
  region, keyed by ``atom_id``. A hole left by a reconnect or an in-place
  restart is measured into coverage, not filled in.

It does **not** own the desired set or its generation — the MD controller
does, and can rebuild both from intents, standing subscriptions and
selector state. Until a strictly newer generation arrives this worker
keeps the last one (P5) and does not invent a replacement. It does not
own placement either: an atom is never moved onto another connection
(F22).

**Invariants.**

* **C1 — one worker, one socket (F17).** Identity is
  :class:`ConnId` ``(venue, endpoint, n)``. The process count is the
  connection count.
* **C2 — :func:`reconcile` is pure.** No clock, no socket, no token
  bucket. It names the diff. The worker batches that diff to the venue's
  ``subscribe_batch`` and paces it with the token bucket when it sends
  (B8-03); neither input fits this signature, so neither happens here.
* **C3 — observed is keyed by the connection epoch.** An ack whose epoch
  is not the one :class:`Observed` is on is discarded, late or early.
  The current epoch's atoms are left as they were.
* **C4 — a reconnect zeroes observed and advances the epoch by one.**
  The next :func:`reconcile` subscribes the whole desired set again. Acks
  from the socket that died no longer apply (C3).
* **C5 — a book gap resyncs that one atom.** One
  :attr:`ActionKind.RESYNC`: unsubscribe then subscribe that atom, on
  this same socket. No other atom is named, and the socket stays up.
  There is no gap message on the wire (F23).
* **C6 — ``seq`` is per atom and contiguous inside one incarnation.**
  Each publication of an atom on one :class:`SeqClock` is exactly one
  greater than the previous one for that atom; another atom has its own
  counter. A new incarnation starts over at :data:`SEQ_ORIGIN`. §3.3
  and this ticket's acceptance put that boundary on the incarnation.
  F25 and §5.3 also say the count starts over when ``on_md_update``
  hears ``live``, which would reset it on a reconnect inside the same
  process. The contract test locks the incarnation reading only. It
  does not assert what a reconnect does to the clock.
* **C7 — a generation is accepted only when it is strictly greater.**
  :class:`Generation` is ``(controller_epoch, seq)`` in that order, the
  MD controller's push counter (F18). It is not a session generation and
  not an env generation (F39). An equal or older push is ignored even
  when its atom set differs; the controller has to bump the counter for
  a change to land. While none arrives, the last desired stands (P5).
* **C8 — a publication is a platform model (F21).** One frame is decoded
  once. ``owner = (worker_id, incarnation)`` rides along for diagnosis
  only (F22); subscribers do not dedupe on it.
* **C9 — tape is trade-class atoms only (F20).** ``trade``, ``aggtrade``,
  ``liquidation``. Book and quote atoms are not recorded: the next push
  is the whole state. The key is ``atom_id``. Only the worker that holds
  the atom appends. This module does not read a topic out of a channel
  string (that string is the venue's, verbatim); :func:`taped` is the
  vocabulary for a caller that already has the platform topic.
* **C10 — the state broadcast is unidirectional (F14).** This worker
  publishes it, not the controller, so a controller rolling does not
  interrupt it (P1). Immediately on change, and every
  :data:`BROADCAST_INTERVAL_S` while steady. An atom's transition is a
  separate event on the same subject. Nobody acks it.
* **C11 — an in-place restart is delete-before-create (F24, P4).**
  :func:`restart_in_place` is the operator entry. The new incarnation
  starts only after the old pid is gone. Atoms are not migrated. The
  tape records the hole.
* **C12 — at most one publisher per atom (F22).** This worker publishes
  only atoms it holds, and it does not dedupe anyone else.

Null data until the B tickets. :func:`reconcile` returns no actions.
Everything that would touch a socket, a clock, Redis or a process raises
``NotImplementedError("IF-10")``. The contract tests that name the
behaviour are ``xfail(strict=True)``.

§3.3 puts ``seq`` on the envelope. The shared
:class:`~mftik.protocol.envelope.Envelope` has no such field, and IF-05
owns ``event.seq`` on the session side. This module carries ``seq`` and
``owner`` on :class:`Publication` and does not change the envelope.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from mftik.exchange.atoms import (
    TOPIC_AGG_TRADE,
    TOPIC_LIQUIDATION,
    TOPIC_TRADE,
    Atom,
)
from mftik.protocol import (
    MD_ATOM_STATE,
    MD_WORKER_STATE,
    MdAtomState,
    MdWorkerState,
    Topics,
)

#: How often a quiet worker repeats its state (F14, §5.6). A change goes
#: out immediately and does not wait for this. Five missed repeats is the
#: listener's ten-second silence; that threshold is not enforced here.
BROADCAST_INTERVAL_S = 2.0

#: Platform topics whose history a venue will not hand back (F20). One
#: atom is recorded once even when two topics share its channel — Binance
#: UM ``trade`` and ``aggtrade`` are both ``@aggTrade``.
TAPED_TOPICS: frozenset[str] = frozenset(
    {TOPIC_TRADE, TOPIC_AGG_TRADE, TOPIC_LIQUIDATION}
)

#: The first ``seq`` of an atom in an incarnation (C6). A gap is a missing
#: positive integer after this, which is what an ``all`` feed's consumer
#: detects (F25).
SEQ_ORIGIN = 1


class ConnError(Exception):
    """This layer refused a value that cannot be a connection's state."""


def _count(value: object, name: str) -> None:
    """Reject anything that is not a non-negative int.

    ``bool`` is an ``int``, and ``True`` is not a generation or an epoch.
    """
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConnError(f"invalid {name} {value!r}")


@dataclass(frozen=True, order=True)
class ConnId:
    """One connection: ``(venue, endpoint, n)`` (F17).

    ``n`` numbers sockets on the same endpoint after placement has filled
    the previous one. It is not an atom, and it is not reused for a
    different venue. The worker id is one NATS token — slashes, no dots —
    because it sits inside ``md.w.{instance}.{worker_id}``. ``:`` is the
    atom id's separator, so a venue or an endpoint cannot contain one
    either: an atom of that pair would not parse.
    """

    venue: str
    endpoint: str
    n: int

    def __post_init__(self) -> None:
        for name, value in (("venue", self.venue), ("endpoint", self.endpoint)):
            if (
                not isinstance(value, str)
                or not value
                or any(mark in value for mark in "/.:")
            ):
                raise ConnError(
                    f"invalid {name} {value!r}; a connection id's {name} is a "
                    "non-empty token without '/', '.' or ':'"
                )
        _count(self.n, "connection index")

    @property
    def worker_id(self) -> str:
        """``md/conn/Deribit/public/0``. One subject token (C1)."""
        return f"md/conn/{self.venue}/{self.endpoint}/{self.n}"


def broadcast_subject(instance: str, conn: ConnId) -> str:
    """``md.w.{instance}.{worker_id}`` for this connection (§5.6)."""
    return Topics.md_worker(instance, conn.worker_id)


@dataclass(frozen=True, order=True)
class Generation:
    """``(controller_epoch, seq)`` — the controller's push counter (F18).

    Ordered lexicographically, which is what "strictly newer" means:
    ``(1, 9) < (2, 0)``. ``controller_epoch`` increments in the DB each
    time the MD controller process starts, so a stale controller's pushes
    lose to the new one's. ``seq`` counts pushes inside that epoch.

    Not a session generation, and not an env generation (F39). Those are
    not this layer's fields.
    """

    controller_epoch: int
    seq: int

    def __post_init__(self) -> None:
        _count(self.controller_epoch, "generation controller_epoch")
        _count(self.seq, "generation seq")


@dataclass(frozen=True)
class Desired:
    """One connection's whole wanted set, at one generation (F18).

    The complete list, not a delta. Replacing it is
    :func:`accept_generation`'s decision; this object does not know what
    the worker currently holds.
    """

    generation: Generation
    atoms: frozenset[Atom]

    def __post_init__(self) -> None:
        if not isinstance(self.atoms, frozenset):
            raise ConnError("desired atoms are a frozenset, not a list of deltas")


class AtomPhase(StrEnum):
    """What the venue has done with one atom (§6.3).

    ``pending``: subscribe sent, no ack yet. ``subscribed``: the venue
    acked it. A refusal is :attr:`AtomView.error` with the phase left as
    last known, which is what :class:`~mftik.protocol.v2.MdAtomState` says.
    """

    PENDING = "pending"
    SUBSCRIBED = "subscribed"


@dataclass(frozen=True)
class AtomView:
    """One atom as the venue has it, on the current connection epoch.

    ``gap`` is local. A book hole is not a protocol field (F23) and is not
    copied onto :class:`~mftik.protocol.v2.MdAtomState`. It asks
    :func:`reconcile` for a single-atom resync (C5). Only a subscription
    the venue already acked can have a hole in it.
    """

    atom: Atom
    phase: AtomPhase
    first_msg_at: float | None = None
    last_msg_at: float | None = None
    error: str | None = None
    gap: bool = False

    def __post_init__(self) -> None:
        if self.gap and self.phase is not AtomPhase.SUBSCRIBED:
            raise ConnError("a gap is a hole in a subscription the venue already acked")
        if self.error is not None and self.error == "":
            raise ConnError("an empty error is no error; pass None")


@dataclass(frozen=True)
class Observed:
    """Atoms the venue has acked on this connection epoch (C3).

    The epoch is the key. Reconnect builds a new :class:`Observed` with
    the next epoch and no atoms (C4); it does not clear this one in
    place, because an ack captured against the old value must still be
    comparable to it.
    """

    epoch: int
    atoms: tuple[AtomView, ...] = ()

    def __post_init__(self) -> None:
        _count(self.epoch, "connection epoch")
        ids = [view.atom for view in self.atoms]
        if len(ids) != len(set(ids)):
            raise ConnError("observed names an atom twice")

    def get(self, atom: Atom) -> AtomView | None:
        """The view for ``atom``, or ``None`` when this epoch has not acked it."""
        for view in self.atoms:
            if view.atom == atom:
                return view
        return None


@dataclass(frozen=True)
class Ack:
    """One venue reply for one atom, stamped with the socket's epoch.

    A batched reply is folded one ack at a time, so each atom carries the
    epoch of the socket that sent the subscribe. ``phase`` is what the
    reply says. ``error`` set means the venue refused it; the phase is
    then the last one the worker knew, not a new one the refusal invented.
    """

    epoch: int
    atom: Atom
    phase: AtomPhase
    error: str | None = None

    def __post_init__(self) -> None:
        _count(self.epoch, "ack epoch")


class ActionKind(StrEnum):
    """What the worker should send. Not what it has sent."""

    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    #: Unsubscribe then subscribe this one atom, on the same socket (C5).
    RESYNC = "resync"


@dataclass(frozen=True)
class Action:
    """A diff entry. A resync names exactly one atom (C5).

    Subscribe and unsubscribe may name several; the worker, not
    :func:`reconcile`, slices them to the venue's batch size.
    """

    kind: ActionKind
    atoms: tuple[Atom, ...]

    def __post_init__(self) -> None:
        if not self.atoms:
            raise ConnError("an action names at least one atom")
        if len(self.atoms) != len(set(self.atoms)):
            raise ConnError("an action names an atom twice")
        if self.kind is ActionKind.RESYNC and len(self.atoms) != 1:
            raise ConnError("a book resync names exactly one atom")


@dataclass(frozen=True)
class Owner:
    """``(worker_id, incarnation)`` on a publication. Diagnostic only (C8)."""

    worker_id: str
    incarnation: int

    def __post_init__(self) -> None:
        if not self.worker_id or "." in self.worker_id:
            raise ConnError(f"invalid owner worker id {self.worker_id!r}")
        _count(self.incarnation, "incarnation")


@dataclass(frozen=True)
class Publication:
    """What this worker attaches to one ``md.a.*`` message (C6, C8).

    ``event`` is a platform model, never the venue's frame. ``seq`` is
    that atom's next number in this incarnation. ``owner`` is not a
    fencing token a subscriber must check.

    This is the in-process stamp, not a second envelope. The shared
    envelope does not carry these fields yet.
    """

    atom: Atom
    event: object
    seq: int
    owner: Owner
    recv_ts: float


def reconcile(desired: Desired, observed: Observed) -> tuple[Action, ...]:
    """Diff one connection's desired set against what the venue has acked.

    Pure (C2). Null until B8-03: no actions, whatever the two arguments
    are. The signature stays ``(desired, observed)`` — the token bucket,
    the batch size and the clock are the sender's, not inputs here.
    """
    return ()


def fold_ack(observed: Observed, ack: Ack) -> Observed:
    """Apply one venue ack, or drop it when the epoch does not match (C3).

    B8-03. An ack from any other epoch leaves ``observed`` unchanged,
    including atoms the current epoch has already acked.
    """
    raise NotImplementedError("IF-10")


def reset_observed(observed: Observed) -> Observed:
    """A new socket has acked nothing (C4).

    B8-03. The returned epoch is ``observed.epoch + 1`` and its atom list
    is empty. The following :func:`reconcile` against the held desired
    subscribes that whole set.
    """
    raise NotImplementedError("IF-10")


def accept_generation(held: Generation | None, offered: Generation) -> bool:
    """Whether ``offered`` replaces the desired this worker holds (C7).

    B8-03. ``None`` is "nothing held yet", and the first push is accepted.
    After that only a strictly greater generation is. Equal is not: a
    repeat of the same push must not be treated as a new desired.
    """
    raise NotImplementedError("IF-10")


class Reconciler:
    """The pure functions for one connection. No socket, no clock, no memory.

    The worker holds the :class:`Observed` and the last :class:`Desired`.
    This class does not: every function takes what it needs and returns a
    new value. :meth:`reconcile` is :func:`reconcile`.
    """

    reconcile = staticmethod(reconcile)
    fold_ack = staticmethod(fold_ack)
    reset = staticmethod(reset_observed)
    accept_generation = staticmethod(accept_generation)


class SeqClock:
    """Per-atom sequence numbers for one incarnation (C6, F25).

    One clock per incarnation. The first value for each atom is
    :data:`SEQ_ORIGIN`. Whether a reconnect inside that incarnation
    replaces the clock is the ``live`` reading of F25; this class does
    not reset itself.

    B4-06 is the first publisher that has to advance this. Until then
    :meth:`next` raises.
    """

    def __init__(self, incarnation: int) -> None:
        _count(incarnation, "incarnation")
        self.incarnation = incarnation

    def next(self, atom_id: str) -> int:
        """The next ``seq`` for ``atom_id`` in this incarnation."""
        if not atom_id:
            raise ConnError("seq is per atom, so it needs an atom id")
        raise NotImplementedError("IF-10")


class BookFold:
    """The folded book for one atom (F21). Authority: this worker.

    A reconnect drops it and rebuilds from the venue snapshot. A hole
    marks the atom :attr:`AtomView.gap` and resyncs that atom only (C5);
    it does not drop the socket. Late joiners are replayed from
    :meth:`snapshot`, which is an in-process copy of state already
    applied, not a REST fetch.

    B7 folds. Until then both methods raise.
    """

    def __init__(self, atom: Atom) -> None:
        self.atom = atom

    def apply(self, event: object) -> None:
        """Fold one decoded book event. ``event`` is a platform model."""
        raise NotImplementedError("IF-10")

    def snapshot(self) -> object:
        """The book a late joiner is replayed, or a raise while this is null."""
        raise NotImplementedError("IF-10")


def taped(topic: str) -> bool:
    """Whether a platform topic is recorded (C9, F20).

    A caller that has the topic — the side that ran ``atoms_for`` — uses
    this. The channel string is not a topic, and this function does not
    parse one out of it.
    """
    return topic in TAPED_TOPICS


class TapeAppend:
    """Append and coverage for atoms this worker holds (C9, F20).

    The Redis write is B7-04, which also moves the key to ``atom_id``.
    Until then both methods raise. A failure of the eventual write must
    not escape into the fan-out; that rule belongs to the implementation,
    and the signature here returns ``None`` so there is nothing to raise
    on the success path.
    """

    def append(self, atom: Atom, event: object, *, recv_ts: float) -> None:
        """Append one trade-class print under ``atom.atom_id``."""
        raise NotImplementedError("IF-10")

    def note_gap(self, atom: Atom, *, reason: str) -> None:
        """Record a hole. ``reason`` is ``reconnect`` or ``restart``.

        Coverage only: nothing is backfilled from the venue, because the
        topics that are taped are the ones the venue will not replay.
        """
        raise NotImplementedError("IF-10")


class StateBroadcast:
    """Unidirectional ``md.w.*`` payloads (C10, F14).

    Building a payload is real: the fields are the v2 models IF-01
    defined. Sending one is B8-06, so :meth:`publish` raises. The
    scheduler — immediate on change, then every
    :data:`BROADCAST_INTERVAL_S` — is that ticket's as well. This class
    holds no timer.

    :attr:`WORKER_TYPE` and :attr:`ATOM_TYPE` are the envelope type
    strings (``md.worker.state``, ``md.atom.state``).
    """

    WORKER_TYPE = MD_WORKER_STATE
    ATOM_TYPE = MD_ATOM_STATE

    def __init__(self, *, instance: str, conn: ConnId, incarnation: int) -> None:
        self.instance = instance
        self.conn = conn
        self.incarnation = incarnation

    @property
    def subject(self) -> str:
        return broadcast_subject(self.instance, self.conn)

    def worker_state(self, *, state: str, version: int) -> MdWorkerState:
        """The connection's own state. ``state`` is not a closed vocabulary.

        ``version`` increases on every publication, including the steady
        one. The envelope type string is :attr:`WORKER_TYPE`.
        """
        return MdWorkerState(
            instance=self.instance,
            worker_id=self.conn.worker_id,
            incarnation=self.incarnation,
            state=state,
            version=version,
        )

    def atom_state(self, view: AtomView, *, version: int) -> MdAtomState:
        """One atom's observed phase. A separate event from the worker state.

        ``gap`` is not copied: there is no gap field on the wire (F23).
        The envelope type string is :attr:`ATOM_TYPE`.
        """
        return MdAtomState(
            atom_id=view.atom.atom_id,
            state=view.phase.value,
            version=version,
            incarnation=self.incarnation,
            first_msg_at=view.first_msg_at,
            last_msg_at=view.last_msg_at,
            error=view.error,
        )

    def publish(self, payload: MdWorkerState | MdAtomState) -> None:
        """Send ``payload`` on :attr:`subject`. B8-06."""
        raise NotImplementedError("IF-10")


class ConnWorker:
    """One connection process (C1).

    Constructing it does not open a socket and does not spawn a process.
    :meth:`run` is that loop, and it is null. The last desired is null
    too: :meth:`desired` returns ``None`` until B8-03 stores what
    :func:`accept_generation` let through.

    Identity — the worker id, the broadcast subject, the diagnostic
    owner — is real, because it is a function of the constructor
    arguments and C1 fixes it.
    """

    def __init__(self, conn: ConnId, *, instance: str, incarnation: int) -> None:
        if not instance or "." in instance:
            raise ConnError(
                f"invalid instance {instance!r}; it is one subject token, without a '.'"
            )
        _count(incarnation, "incarnation")
        self.conn = conn
        self.instance = instance
        self.incarnation = incarnation

    @property
    def worker_id(self) -> str:
        return self.conn.worker_id

    @property
    def subject(self) -> str:
        """The ``md.w.*`` subject this worker broadcasts on."""
        return broadcast_subject(self.instance, self.conn)

    @property
    def owner(self) -> Owner:
        """Diagnostic owner stamped on publications (C8)."""
        return Owner(self.worker_id, self.incarnation)

    def desired(self) -> Desired | None:
        """The last accepted desired, or ``None`` while this is null data."""
        return None

    def accept_desired(self, desired: Desired) -> None:
        """Replace the held desired when the generation is strictly newer (C7).

        A rejected generation leaves the held one in place (P5). B8-03.
        """
        raise NotImplementedError("IF-10")

    def run(self) -> None:
        """The socket loop: read, decode once, publish, reconcile. B4-06."""
        raise NotImplementedError("IF-10")

    def publish(self, atom: Atom, event: object, *, recv_ts: float) -> Publication:
        """Stamp ``event`` with this incarnation's next ``seq`` and publish it.

        B4-06. ``event`` is the platform model ``decode`` returned.
        """
        raise NotImplementedError("IF-10")

    def book(self, atom: Atom) -> BookFold:
        """The fold for ``atom``. Empty of state; applying still raises."""
        return BookFold(atom)


def restart_in_place(conn: ConnId) -> None:
    """Operator entry: replace this connection's process (C11, F24).

    Called from outside the worker. The process named by ``conn`` is the
    thing being replaced, so it cannot run its own restart. The new
    incarnation starts only after the old pid is gone (delete-before-create,
    P4). Atoms stay on this connection (F22). The tape records the hole.

    B8-06. The CLI that names it is ``mftik md restart <conn>`` (IF-15).
    """
    raise NotImplementedError("IF-10")


__all__ = [
    "BROADCAST_INTERVAL_S",
    "SEQ_ORIGIN",
    "TAPED_TOPICS",
    "Ack",
    "Action",
    "ActionKind",
    "AtomPhase",
    "AtomView",
    "BookFold",
    "ConnError",
    "ConnId",
    "ConnWorker",
    "Desired",
    "Generation",
    "Observed",
    "Owner",
    "Publication",
    "Reconciler",
    "SeqClock",
    "StateBroadcast",
    "TapeAppend",
    "accept_generation",
    "broadcast_subject",
    "fold_ack",
    "reconcile",
    "reset_observed",
    "restart_in_place",
    "taped",
]
