"""Waiting for an account's book to settle before it is read.

Extracted from the ``sts.recon`` handler TD used to serve. That request was an
OMS snapshot and nothing else, so the part of it that outlives the session
mechanism is the waiting: an UNKNOWN order means the book does not yet say
what the account holds, and answering anyway is how a strategy ends up acting
on a leg it cannot see.

:meth:`mftik_td.account.handlers.OmsHandler.view` calls
:func:`view_when_settled` for ``settled=True`` (B6-08, F13). The chase is
the session's own single-flight
:meth:`~mftik_td.account.session.Session.kick_resolve_all_unknown`.
This function does not call
:meth:`~mftik_td.account.session.Session.reconcile`.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from weakref import WeakKeyDictionary

from mftik.exchange.oms import OmsView

from mftik_td.session.session import Session

logger = logging.getLogger(__name__)

#: How long a settled read waits for the book to come clean before it answers
#: with it as-is. Comfortably past the forced venue recon behind it, so this
#: only fires when that could not settle the book either.
SETTLED_WAIT_TIMEOUT_S = 30.0

_Waiters = list[asyncio.Event]
_Hook = Callable[[], Awaitable[None]]

#: Waiters parked on one session, and the hook installed for them.
#:
#: One hook fans out to every waiter, so two settled reads share the
#: session's single chase instead of wrapping ``_on_book_settled`` over
#: each other. The maps are weak: a session that goes away drops them.
_waiters: WeakKeyDictionary[Session, _Waiters] = WeakKeyDictionary()
_hooks: WeakKeyDictionary[Session, _Hook] = WeakKeyDictionary()
_outers: WeakKeyDictionary[Session, _Hook | None] = WeakKeyDictionary()


def _register(session: Session, settled: asyncio.Event) -> None:
    """Park ``settled`` and install the fan-out hook if this is the first."""
    bucket = _waiters.get(session)
    if bucket is None:
        bucket = []
        _waiters[session] = bucket
        outer = session._on_book_settled
        _outers[session] = outer

        async def _on_settled() -> None:
            current = _waiters.get(session)
            if current:
                for event in list(current):
                    event.set()
            saved = _outers.get(session)
            if saved is not None:
                await saved()

        session._on_book_settled = _on_settled
        _hooks[session] = _on_settled
    bucket.append(settled)


def _unregister(session: Session, settled: asyncio.Event) -> None:
    """Drop ``settled``. The last waiter puts the session's hook back."""
    bucket = _waiters.get(session)
    if bucket is None:
        return
    try:
        bucket.remove(settled)
    except ValueError:
        return
    if bucket:
        return
    _waiters.pop(session, None)
    hook = _hooks.pop(session, None)
    outer = _outers.pop(session, None)
    if hook is not None and session._on_book_settled is hook:
        session._on_book_settled = outer


async def view_when_settled(
    session: Session, *, timeout: float = SETTLED_WAIT_TIMEOUT_S
) -> OmsView:
    """``session``'s OMS snapshot, once nothing in the book is UNKNOWN.

    A clean book is answered straight from memory: TD does not start a venue
    pass on a reader's behalf. An UNKNOWN order is chased instead — the kick
    is single-flight in the session, so concurrent readers share one pass —
    and the answer waits for TD's own resolve or reconnect recon to settle it.

    ``timeout`` bounds that wait. Resolve and the forced recon behind it retry
    indefinitely, which is right, but a venue that can never answer (revoked
    key, delisted instrument) would otherwise park the reader forever with
    nothing but log lines to show for it. Late and honest beats silent, so an
    expired wait answers with the book as it stands, UNKNOWN and all.

    :meth:`~mftik_td.account.session.Session.book_ready_for_snapshot` is
    ``started`` and no UNKNOWN. A book with no UNKNOWN is answered even
    when the session has not been started: there is nothing to chase.
    The account handler does not call this unless the trading layer is up.
    """
    if session.book_ready_for_snapshot():
        return session.view_for_sts()

    settled = asyncio.Event()
    # No await between the check above and this install, so the chase
    # loop cannot settle the book in the gap and leave us waiting.
    _register(session, settled)
    try:
        if not session.has_unknown():
            return session.view_for_sts()
        # ``None`` means the session is already destroyed. Waiting out
        # the timeout would only delay the handler's refusal.
        if session.kick_resolve_all_unknown() is None:
            return session.view_for_sts()
        try:
            await asyncio.wait_for(settled.wait(), timeout=timeout)
        except TimeoutError:
            logger.warning(
                "TD settled wait expired api_id=%s after %ss — answering "
                "with the book as it stands, UNKNOWN still open",
                session.api_id,
                timeout,
            )
    finally:
        _unregister(session, settled)
    return session.view_for_sts()
