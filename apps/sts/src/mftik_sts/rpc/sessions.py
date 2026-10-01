"""STS session start / end / list RPC — placeholders until IF-04.

RM-04 deleted the session manager these handlers called: the per-session
subprocess, its process table, and the DB wiring that made a row out of it.
Nothing in this plane can start or end a session now, so each handler raises
rather than answering with something a caller could mistake for a session.

They stay registered in the router on purpose. An unknown type is a plane
that was never told about the message; this is a plane that knows the
message and cannot serve it yet, and the two should not read the same in a
log. IF-04 (#182) defines the interface that replaces them.
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
    """``sts.session.create`` — becomes ``sts.session.start`` in IF-04."""
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
    """``sts.session.stop`` — becomes ``sts.session.end`` in IF-04."""
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
