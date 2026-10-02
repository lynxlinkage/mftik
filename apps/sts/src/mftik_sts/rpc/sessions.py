"""STS session RPC — still the router's placeholders.

The interface is :mod:`mftik_sts.controller` (IF-04). These functions stay
registered and still raise ``NotImplementedError("IF-04")``. The running
process does not call the controller. B4-02 is what wires it in.

RM-04 deleted the session manager these handlers called: the per-session
subprocess, its process table, and the DB wiring that made a row out of it.
Nothing in this plane can start or end a session now, so each handler raises
rather than answering with something a caller could mistake for a session.

They stay registered on purpose. An unknown type is a plane that was never
told about the message; this is a plane that knows the message and cannot
serve it yet, and the two should not read the same in a log.
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
