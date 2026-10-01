"""Waiting for an account's book to settle before it is read.

Extracted from the ``sts.recon`` handler TD used to serve. That request was an
OMS snapshot and nothing else, so the part of it that outlives the session
mechanism is the waiting: an UNKNOWN order means the book does not yet say
what the account holds, and answering anyway is how a strategy ends up acting
on a leg it cannot see. IF-11 puts this behind ``view(settled=True)``; nothing
calls it until then.
"""

from __future__ import annotations

import asyncio
import logging

from mftik.exchange.oms import OmsView

from mftik_td.session.session import Session

logger = logging.getLogger(__name__)

#: How long a settled read waits for the book to come clean before it answers
#: with it as-is. Comfortably past the forced venue recon behind it, so this
#: only fires when that could not settle the book either.
SETTLED_WAIT_TIMEOUT_S = 30.0


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
    """
    if session.book_ready_for_snapshot():
        return session.view_for_sts()

    settled = asyncio.Event()
    outer = session._on_book_settled

    async def _on_settled() -> None:
        settled.set()
        if outer is not None:
            await outer()

    session._on_book_settled = _on_settled
    try:
        if session.has_unknown():
            session.kick_resolve_all_unknown()
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
        if session._on_book_settled is _on_settled:
            session._on_book_settled = outer
    return session.view_for_sts()
