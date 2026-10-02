"""STS session RPC placeholders that the controller does not replace.

Start, end and list are :mod:`mftik_sts.controller`. The functions below
for those types stay in this module and are not registered: a caller that
still imports them gets ``NotImplementedError("IF-04")`` rather than a
second implementation. Fail and force-stop stay registered on
``sts.{instance}`` and still raise. ``sts.ctl.{session_id}`` is not
registered; that subject stays the worker's (B4-03).
"""

from __future__ import annotations

from mftik.broker import IncomingRequest

#: The ticket that defines what goes here. Raised rather than replied: a
#: 501-shaped answer is an interface decision, and IF-04 is where it is made.
_IF = "IF-04"


async def handle_session_create(
    req: IncomingRequest,
    *,
    instance: str | None = None,
) -> None:
    """``sts.session.start`` — the handler is IF-04. The wire type is IF-01."""
    del req, instance
    raise NotImplementedError(_IF)


async def handle_session_list(
    req: IncomingRequest,
    *,
    instance: str | None = None,
) -> None:
    """``sts.session.list`` — the list is a DB read from B4-02 on."""
    del req, instance
    raise NotImplementedError(_IF)


async def handle_session_stop(
    req: IncomingRequest,
    *,
    instance: str | None = None,
) -> None:
    """``sts.session.end`` — the handler is IF-04. The wire type is IF-01."""
    del req, instance
    raise NotImplementedError(_IF)


async def handle_session_force_stop(
    req: IncomingRequest,
    *,
    instance: str | None = None,
) -> None:
    """``sts.session.force_stop`` — the Supervisor's job from B3-02 on."""
    del req, instance
    raise NotImplementedError(_IF)


async def handle_session_fail(
    req: IncomingRequest,
    *,
    instance: str | None = None,
) -> None:
    """``sts.session.fail`` — served on the session's own subject in IF-04."""
    del req, instance
    raise NotImplementedError(_IF)
