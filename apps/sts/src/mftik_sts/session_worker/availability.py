"""Follow MD and TD availability broadcasts (F14, §5.6).

The session ingress is not the authority for feed or account state. The
MD connection worker is, on ``md.w.{instance}.{worker_id}``, and the TD
account worker is, on ``td.account.state.{api_id}``. This object is the
follower: it turns those broadcasts into ``on_md_update`` /
``on_td_update`` / ``on_resync`` and into the answers ``md.state`` and
``td.state`` give. It never fails the session (principle 1). A strategy
that cannot ride out a gap calls ``fail()`` itself.

**Silence.** The 10s timer (§5.6, F14, :data:`SILENCE_S`) for a worker
or an account arms only after the first broadcast from that source. Until
then ``state`` is ``None`` — unknown, not down — and order entry is not
refused here. TD's own ``TD_VENUE_NOT_CONNECTED`` remains the gate.
Once armed, :data:`SILENCE_S` without a broadcast marks that worker's
feeds ``down`` and that account ``unavailable``, reason
``broadcast_silent``. The next broadcast clears it. While the ingress
itself is disconnected the timer does not run: the gap is the reconnect
path, and a false ``unavailable`` would block a cancel.

**Atoms.** Which feeds a worker carries is
learned from ``MdAtomState`` on its own subject. A feed is ``down`` when
any of its atoms is down (F19) and ``live`` only when every one of them
is up. A silent worker takes down every feed whose atoms it last
carried. An atom is up when its state is ``subscribed`` and it has no
``error``. ``MdWorkerState.state`` is not a closed vocabulary; ``live``
is the up word (the word the broadcast interface already builds) and
any other word takes the worker's feeds down, using that word as the
reason.

**Gaps.** A version that skips, or a new
incarnation, is a missed transition. Both payloads are full snapshots,
and there is no query RPC on either plane, so the newest broadcast is
the state. The gap is logged. Nothing here sends a request to the worker.

**Resync (F13).** ``on_resync`` has two
causes and no others:

* ``account_reset`` — ``TdAccountReset``, or a new incarnation on
  ``td.account.state``. The strategy sees ``unavailable`` (if it has not
  already), then ``on_resync``, then ``ready``. A new incarnation whose
  own state is still ``unavailable`` only notifies ``unavailable`` and
  waits for the reset or a later ``ready``: announcing ``ready`` while
  the worker says it is not would let the strategy trade on a book that
  has not been rebuilt.
* ``reconnect`` — the ingress's own NATS connection. Every declared feed
  goes ``down`` with reason ``ingress_reconnect``, then ``live`` after
  the connection is back, and each account gets ``on_resync``. Account
  availability is not changed: the send connection is not this one.

The view is a settled ``oms.view`` (``settled=True``). The read waits up
to :data:`~mftik.strategy.oms.SETTLED_VIEW_TIMEOUT_S` for UNKNOWN orders
to converge. It runs on the ingress loop, off the strategy thread, and
:func:`schedule_effects` starts it as a task so the notices beside it
are not held for that wait. A ``td.error`` reply — the trading layer is
closed, or the payload is invalid — is logged and the read returns
``None``. That ``on_resync`` is skipped. An error payload is never
parsed as an empty book. The session stays up.

Same-incarnation recovery from silence restores the broadcast's state
and does not call ``on_resync``. F13 names only the two causes above.
§5.6's host-loss row also says the account receives ``on_resync``; this
code does not, on a same-incarnation recovery from silence. That
disagreement is left open.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field

from mftik.exchange.oms import OmsView
from mftik.protocol import (
    MD_ATOM_STATE,
    MD_WORKER_STATE,
    TD_ACCOUNT_RESET,
    TD_ACCOUNT_STATE,
    TD_ERROR,
    TD_OMS_VIEW,
    Envelope,
    MdAtomState,
    MdWorkerState,
    TdAccountReset,
    TdAccountState,
    TdOmsViewRequest,
    Topics,
    UntypedEnvelope,
)
from mftik.strategy.oms import SETTLED_VIEW_TIMEOUT_S

logger = logging.getLogger(__name__)

#: Five missed 2s broadcasts (§5.6). Armed only after the first one.
SILENCE_S = 10.0

#: Worker connection word that means the socket is up. Anything else is down.
WORKER_UP = "live"

#: Atom phase that means the venue has acked the subscription.
ATOM_UP = "subscribed"

REASON_SILENCE = "broadcast_silent"
REASON_INGRESS = "ingress_reconnect"
REASON_RESET = "account_reset"

_SOURCE = "sts.session_worker"


@dataclass(frozen=True)
class MdUpdate:
    """One ``on_md_update``. ``token`` orders it against a later commit."""

    feed: str
    state: str
    reason: str
    token: int


@dataclass(frozen=True)
class TdUpdate:
    """One ``on_td_update``.

    ``deferred`` is a ``ready`` that must not become visible until the
    strategy thread commits it, which is after ``on_resync`` has run.
    """

    api_id: int
    state: str
    reason: str
    token: int
    deferred: bool = False


@dataclass(frozen=True)
class Resync:
    """One ``on_resync`` to perform after ``oms.view`` returns.

    ``then_td`` is offered after the resync event, still deferred, so
    the strategy corrects its book before it is told it may trade.
    """

    api_id: int
    cause: str
    then_td: TdUpdate | None = None


Effect = MdUpdate | TdUpdate | Resync


def notice_text(effect: MdUpdate | TdUpdate) -> str:
    """The one warning line per transition. Alert matches the session log of it."""
    if isinstance(effect, MdUpdate):
        return (
            f"md availability feed={effect.feed} "
            f"state={effect.state} reason={effect.reason}"
        )
    return (
        f"td availability api_id={effect.api_id} "
        f"state={effect.state} reason={effect.reason}"
    )


@dataclass
class _Worker:
    last_mono: float | None = None
    armed: bool = False
    silent: bool = False
    version: int | None = None
    incarnation: int | None = None
    connection: str | None = None
    atoms: set[str] = field(default_factory=set)
    atom_version: dict[str, int] = field(default_factory=dict)


@dataclass
class _Atom:
    up: bool
    reason: str
    worker: str


@dataclass
class _Account:
    last_mono: float | None = None
    armed: bool = False
    silent: bool = False
    version: int | None = None
    incarnation: int | None = None
    reported: str | None = None
    pending_reset: bool = False
    #: Incarnation we have already scheduled ``on_resync`` for.
    reset_for: int | None = None


class Availability:
    """The session's view of feed and account availability.

    Bookkeeping is updated when a broadcast arrives. The strategy-facing
    state of a deferred ``ready`` waits for :meth:`commit_td`, which the
    strategy thread calls immediately before ``on_td_update``.
    """

    def __init__(
        self,
        *,
        feeds: Mapping[str, Collection[str]],
        accounts: Collection[int],
    ) -> None:
        self._feeds: dict[str, frozenset[str]] = {
            feed: frozenset(atoms) for feed, atoms in feeds.items()
        }
        self._accounts = set(accounts)
        self._workers: dict[str, _Worker] = {}
        self._atoms: dict[str, _Atom] = {}
        self._meta: dict[int, _Account] = {
            api_id: _Account() for api_id in self._accounts
        }
        self._md: dict[str, str] = {}
        self._td: dict[int, str] = {}
        self._md_token: dict[str, int] = {}
        self._td_token: dict[int, int] = {}
        self._seq = 0
        self._ingress_down = False
        self._lock = threading.Lock()

    def md_state(self, feed: str) -> str | None:
        """``live`` / ``down``, or ``None`` if the feed is unknown or not held."""
        with self._lock:
            if feed not in self._feeds:
                return None
            return self._md.get(feed)

    def td_state(self, api_id: int) -> str | None:
        """``ready`` / ``degraded`` / ``unavailable``, or ``None`` if unknown.

        ``None`` is not a refusal. The account has not broadcast yet.
        """
        with self._lock:
            if api_id not in self._accounts:
                return None
            return self._td.get(api_id)

    def md_ready_counts(self) -> tuple[int, int]:
        """``(not down, declared)`` for the status snapshot after ``on_ready``.

        Unknown still counts as ready: silence is not armed, and a feed
        that has joined is not reported down until a broadcast says so.
        """
        with self._lock:
            total = len(self._feeds)
            down = sum(1 for feed in self._feeds if self._md.get(feed) == "down")
            return total - down, total

    def td_ready_counts(self) -> tuple[int, int]:
        """``(ready or unknown, declared)``. ``degraded`` is not ready."""
        with self._lock:
            total = len(self._accounts)
            ready = sum(
                1
                for api_id in self._accounts
                if self._td.get(api_id) in (None, "ready")
            )
            return ready, total

    def tick(self, now: float) -> list[Effect]:
        """Mark sources that have been silent for :data:`SILENCE_S`.

        ``now`` is monotonic. A source that has never broadcast is left
        unknown. Nothing happens while the ingress is disconnected.
        """
        with self._lock:
            if self._ingress_down:
                return []
            effects: list[Effect] = []
            dirty = False
            for worker in self._workers.values():
                if not _silence_due(worker.armed, worker.silent, worker.last_mono, now):
                    continue
                worker.silent = True
                dirty = True
            if dirty:
                effects.extend(self._refresh_feeds())
            for api_id in sorted(self._accounts):
                acct = self._meta[api_id]
                if not _silence_due(acct.armed, acct.silent, acct.last_mono, now):
                    continue
                acct.silent = True
                if self._td.get(api_id) != "unavailable":
                    effects.append(
                        self._td_now(api_id, "unavailable", REASON_SILENCE)
                    )
            return effects

    def apply_md(self, subject: str, raw: str, now: float) -> list[Effect]:
        """One message on ``md.w.*``. Unknown types are ignored (P1)."""
        env = _parse(raw)
        if env is None:
            return []
        with self._lock:
            if env.type == MD_WORKER_STATE:
                try:
                    payload = MdWorkerState.model_validate(env.payload)
                except Exception:
                    logger.warning(
                        "md worker state unreadable subject=%s", subject, exc_info=True
                    )
                    return []
                return self._worker_state(subject, payload, now)
            if env.type == MD_ATOM_STATE:
                try:
                    payload = MdAtomState.model_validate(env.payload)
                except Exception:
                    logger.warning(
                        "md atom state unreadable subject=%s", subject, exc_info=True
                    )
                    return []
                return self._atom_state(subject, payload, now)
            return []

    def apply_td_state(self, raw: str, now: float) -> list[Effect]:
        """One message on ``td.account.state.{api_id}``."""
        env = _parse(raw)
        if env is None or env.type != TD_ACCOUNT_STATE:
            return []
        try:
            payload = TdAccountState.model_validate(env.payload)
        except Exception:
            logger.warning("td account state unreadable", exc_info=True)
            return []
        with self._lock:
            return self._account_state(payload, now)

    def apply_td_global(self, raw: str, now: float) -> list[Effect] | None:
        """``TdAccountReset`` on ``td.{api_id}.global``, or ``None`` if it isn't one.

        ``None`` means the caller should deliver the message as a normal
        account event. A reset is consumed here even when this session
        does not hold the account.
        """
        env = _parse(raw)
        if env is None or env.type != TD_ACCOUNT_RESET:
            return None
        try:
            payload = TdAccountReset.model_validate(env.payload)
        except Exception:
            logger.warning("td account reset unreadable", exc_info=True)
            return []
        with self._lock:
            return self._account_reset(payload, now)

    def ingress_disconnected(self) -> list[Effect]:
        """Every declared feed ``down``, reason ``ingress_reconnect``."""
        with self._lock:
            if self._ingress_down:
                return []
            self._ingress_down = True
            notices: list[Effect] = []
            for feed in self._feeds:
                notices.append(self._md_now(feed, "down", REASON_INGRESS))
            return notices

    def ingress_reconnected(self, now: float) -> list[Effect]:
        """Feeds ``live``, then one ``on_resync(reconnect)`` per account.

        ``now`` restarts silence clocks so the gap we could not hear is
        not also a false ``broadcast_silent``. A feed whose worker is
        already down is corrected after the ``live``.
        """
        with self._lock:
            if not self._ingress_down:
                return []
            self._ingress_down = False
            for worker in self._workers.values():
                if worker.armed and worker.last_mono is not None:
                    worker.last_mono = now
            for acct in self._meta.values():
                if acct.armed and acct.last_mono is not None:
                    acct.last_mono = now
            effects: list[Effect] = []
            for feed in self._feeds:
                effects.append(self._md_now(feed, "live", REASON_INGRESS))
            effects.extend(self._refresh_feeds())
            for api_id in sorted(self._accounts):
                effects.append(Resync(api_id, "reconnect", None))
            return effects

    def commit_md(self, feed: str, state: str, token: int) -> bool:
        """Strategy thread, immediately before ``on_md_update``.

        False means a newer notice already owns the feed, so the caller
        does not deliver this one. A stale token does not rewind the feed.
        """
        with self._lock:
            if token < self._md_token.get(feed, 0):
                return False
            self._md_token[feed] = token
            self._md[feed] = state
            return True

    def commit_td(self, api_id: int, state: str, token: int) -> bool:
        """Strategy thread, immediately before ``on_td_update``.

        This is what makes a deferred ``ready`` visible, and only after
        ``on_resync`` has been pulled ahead of it. False means a newer
        notice already owns the account.
        """
        with self._lock:
            if token < self._td_token.get(api_id, 0):
                return False
            self._td_token[api_id] = token
            self._td[api_id] = state
            return True

    def _next_token(self) -> int:
        self._seq += 1
        return self._seq

    def _md_now(self, feed: str, state: str, reason: str) -> MdUpdate:
        token = self._next_token()
        self._md[feed] = state
        self._md_token[feed] = token
        notice = MdUpdate(feed, state, reason, token)
        logger.warning("%s", notice_text(notice))
        return notice

    def _td_now(self, api_id: int, state: str, reason: str) -> TdUpdate:
        token = self._next_token()
        self._td[api_id] = state
        self._td_token[api_id] = token
        notice = TdUpdate(api_id, state, reason, token, deferred=False)
        logger.warning("%s", notice_text(notice))
        return notice

    def _td_later(self, api_id: int, state: str, reason: str) -> TdUpdate:
        """A notice whose state waits for :meth:`commit_td`."""
        token = self._next_token()
        notice = TdUpdate(api_id, state, reason, token, deferred=True)
        logger.warning("%s", notice_text(notice))
        return notice

    def _worker_state(
        self, subject: str, payload: MdWorkerState, now: float
    ) -> list[Effect]:
        worker = self._workers.setdefault(subject, _Worker())
        if worker.version is not None and payload.version < worker.version:
            return []
        if worker.version is not None and payload.version > worker.version + 1:
            logger.warning(
                "md broadcast gap subject=%s version %s -> %s",
                subject,
                worker.version,
                payload.version,
            )
        if (
            worker.incarnation is not None
            and payload.incarnation != worker.incarnation
        ):
            logger.warning(
                "md broadcast incarnation subject=%s %s -> %s",
                subject,
                worker.incarnation,
                payload.incarnation,
            )
        worker.version = payload.version
        worker.incarnation = payload.incarnation
        worker.connection = payload.state
        _hear(worker, now)
        return self._refresh_feeds()

    def _atom_state(
        self, subject: str, payload: MdAtomState, now: float
    ) -> list[Effect]:
        worker = self._workers.setdefault(subject, _Worker())
        last = worker.atom_version.get(payload.atom_id)
        if last is not None and payload.version < last:
            return []
        if last is not None and payload.version > last + 1:
            logger.warning(
                "md broadcast gap subject=%s atom=%s version %s -> %s",
                subject,
                payload.atom_id,
                last,
                payload.version,
            )
        if (
            worker.incarnation is not None
            and payload.incarnation != worker.incarnation
        ):
            logger.warning(
                "md broadcast incarnation subject=%s %s -> %s",
                subject,
                worker.incarnation,
                payload.incarnation,
            )
        previous = self._atoms.get(payload.atom_id)
        if previous is not None and previous.worker != subject:
            old = self._workers.get(previous.worker)
            if old is not None:
                old.atoms.discard(payload.atom_id)
        worker.atom_version[payload.atom_id] = payload.version
        worker.incarnation = payload.incarnation
        worker.atoms.add(payload.atom_id)
        _hear(worker, now)
        up = payload.state == ATOM_UP and not payload.error
        reason = payload.error or payload.state
        self._atoms[payload.atom_id] = _Atom(up=up, reason=reason, worker=subject)
        return self._refresh_feeds()

    def _refresh_feeds(self) -> list[MdUpdate]:
        if self._ingress_down:
            return []
        notices: list[MdUpdate] = []
        for feed, atoms in self._feeds.items():
            state, reason = self._observe(atoms)
            if state is None or self._md.get(feed) == state:
                continue
            notices.append(self._md_now(feed, state, reason))
        return notices

    def _observe(self, atoms: frozenset[str]) -> tuple[str | None, str]:
        if not atoms:
            return None, ""
        down_reason: str | None = None
        seen_up = 0
        for atom_id in sorted(atoms):
            observed = self._atoms.get(atom_id)
            if observed is None:
                continue
            worker = self._workers.get(observed.worker)
            if worker is not None and worker.silent:
                down_reason = down_reason or REASON_SILENCE
                continue
            if (
                worker is not None
                and worker.connection is not None
                and worker.connection != WORKER_UP
            ):
                down_reason = down_reason or worker.connection
                continue
            if not observed.up:
                down_reason = down_reason or observed.reason or "down"
                continue
            seen_up += 1
        if down_reason is not None:
            return "down", down_reason
        if seen_up == len(atoms):
            return "live", WORKER_UP
        return None, ""

    def _account_state(self, payload: TdAccountState, now: float) -> list[Effect]:
        if payload.api_id not in self._accounts:
            return []
        acct = self._meta[payload.api_id]
        if acct.version is not None and payload.version < acct.version:
            return []
        if acct.version is not None and payload.version > acct.version + 1:
            logger.warning(
                "td broadcast gap api_id=%s version %s -> %s",
                payload.api_id,
                acct.version,
                payload.version,
            )
        incarnation_changed = (
            acct.incarnation is not None and payload.incarnation != acct.incarnation
        )
        if incarnation_changed:
            logger.warning(
                "td broadcast incarnation api_id=%s %s -> %s",
                payload.api_id,
                acct.incarnation,
                payload.incarnation,
            )
        acct.version = payload.version
        acct.incarnation = payload.incarnation
        acct.reported = payload.state
        _hear(acct, now)
        reason = payload.reason or payload.state
        if incarnation_changed:
            return self._begin_reset(
                payload.api_id, payload.incarnation, payload.state, reason
            )
        if acct.pending_reset and payload.state == "ready":
            return self._finish_reset(payload.api_id, payload.incarnation, reason)
        if self._td.get(payload.api_id) == payload.state:
            return []
        return [self._td_now(payload.api_id, payload.state, reason)]

    def _account_reset(self, payload: TdAccountReset, now: float) -> list[Effect]:
        del now  # a reset is not a state-subject broadcast, so it does not arm silence
        if payload.api_id not in self._accounts:
            return []
        acct = self._meta[payload.api_id]
        if acct.incarnation is not None and payload.incarnation != acct.incarnation:
            logger.warning(
                "td broadcast incarnation api_id=%s %s -> %s",
                payload.api_id,
                acct.incarnation,
                payload.incarnation,
            )
        acct.incarnation = payload.incarnation
        return self._finish_reset(payload.api_id, payload.incarnation, REASON_RESET)

    def _begin_reset(
        self, api_id: int, incarnation: int, reported: str, reason: str
    ) -> list[Effect]:
        """A new incarnation. ``unavailable`` now; resync only if it is already up."""
        acct = self._meta[api_id]
        effects: list[Effect] = []
        if self._td.get(api_id) != "unavailable":
            effects.append(self._td_now(api_id, "unavailable", reason))
        if reported == "unavailable":
            acct.pending_reset = True
            return effects
        return effects + self._finish_reset(api_id, incarnation, reason)

    def _finish_reset(self, api_id: int, incarnation: int, reason: str) -> list[Effect]:
        acct = self._meta[api_id]
        acct.pending_reset = False
        if acct.reset_for == incarnation:
            return []
        acct.reset_for = incarnation
        effects: list[Effect] = []
        if self._td.get(api_id) != "unavailable":
            effects.append(self._td_now(api_id, "unavailable", reason))
        then = self._td_later(api_id, "ready", reason or "ready")
        effects.append(Resync(api_id, "account_reset", then))
        return effects


def _hear(slot: _Worker | _Account, now: float) -> None:
    slot.armed = True
    slot.silent = False
    slot.last_mono = now


def _silence_due(armed: bool, silent: bool, last: float | None, now: float) -> bool:
    if not armed or silent or last is None:
        return False
    return now - last >= SILENCE_S


def _parse(raw: str) -> UntypedEnvelope | None:
    try:
        return UntypedEnvelope.from_json(raw)
    except Exception:
        logger.warning("availability broadcast unreadable", exc_info=True)
        return None


def schedule_effects(
    effects: Sequence[Effect],
    *,
    offer: Callable[[MdUpdate | TdUpdate], None],
    deliver: Callable[[Resync], Awaitable[None]],
    log: Callable[[MdUpdate | TdUpdate], Awaitable[None]],
) -> None:
    """Offer notices now. Start each settled view read as a task.

    ``deliver`` waits up to :data:`SETTLED_VIEW_TIMEOUT_S` on the loop
    that calls this, which is the ingress, not the strategy thread.
    Awaiting the read here would hold the other notices in this batch.
    """
    loop = asyncio.get_running_loop()
    for effect in effects:
        if isinstance(effect, Resync):
            loop.create_task(deliver(effect))
            continue
        offer(effect)
        loop.create_task(log(effect))


async def read_oms_view(
    broker: object,
    *,
    api_id: int,
    session_id: str,
) -> OmsView | None:
    """Settled ``oms.view`` for one ``on_resync``.

    The request is ``settled=True`` and the timeout is
    :data:`SETTLED_VIEW_TIMEOUT_S`. ``None`` means the read failed or TD
    answered ``td.error`` (the trading layer is closed, or the payload
    was refused). The caller logs and skips that ``on_resync``. It does
    not fail the session, and it does not hand the strategy an empty
    book. The broker is the ingress's, so this await is not on the
    strategy thread.
    """
    request = getattr(broker, "request", None)
    if request is None:
        logger.warning("oms.view for on_resync has no broker api_id=%s", api_id)
        return None
    envelope = Envelope.wrap(
        TdOmsViewRequest(api_id=api_id, settled=True),
        type=TD_OMS_VIEW,
        source=_SOURCE,
        session_id=session_id,
    )
    try:
        reply = await request(
            Topics.td_account(api_id),
            envelope,
            timeout=SETTLED_VIEW_TIMEOUT_S,
        )
    except Exception:
        logger.warning(
            "oms.view for on_resync failed api_id=%s", api_id, exc_info=True
        )
        return None
    if getattr(reply, "type", None) == TD_ERROR:
        payload = getattr(reply, "payload", None) or {}
        if not isinstance(payload, dict):
            payload = {}
        code = str(payload.get("code", "td.error"))
        message = str(payload.get("message", "td refused the read"))
        logger.warning(
            "oms.view for on_resync refused api_id=%s code=%s message=%s",
            api_id,
            code,
            message,
        )
        return None
    try:
        return OmsView.model_validate(reply.payload)
    except Exception:
        logger.warning(
            "oms.view for on_resync unreadable api_id=%s", api_id, exc_info=True
        )
        return None


__all__ = [
    "ATOM_UP",
    "REASON_INGRESS",
    "REASON_RESET",
    "REASON_SILENCE",
    "SILENCE_S",
    "WORKER_UP",
    "Availability",
    "Effect",
    "MdUpdate",
    "Resync",
    "TdUpdate",
    "notice_text",
    "read_oms_view",
    "schedule_effects",
]
