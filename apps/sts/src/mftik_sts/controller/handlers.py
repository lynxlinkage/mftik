"""``sts.session.start`` / ``end`` / ``list`` as handlers (IF-02).

Not registered on the running process. The router still answers those
types with ``NotImplementedError("IF-04")``. B4-02 is what connects these
callables to a subject.

A handler's whole input is the decoded envelope and its whole output is
the reply (H1). Each one raises until B4-02.
"""

from __future__ import annotations

from mftik.broker.handler import Handler, Reply
from mftik.protocol import Topics, UntypedEnvelope

from mftik_sts.controller._ticket import unimplemented
from mftik_sts.controller.orchestrator import StsOrchestrator


def start_handler(orchestrator: StsOrchestrator) -> Handler:
    """``sts.session.start`` for this controller (§5.1, F12, §8.1).

    The reply is an accept: :class:`~mftik.protocol.messages.StsCreateSessionResult`
    with ``status="starting"`` and the request's ``session_id``. ``on_start``
    has not run. Later progress is ``sts.session.status``, not a second
    field on the reply.

    Served on ``Topics.sts(instance)`` when B4-02 registers it. The subject
    is not bound here.
    """

    async def handle(message: UntypedEnvelope) -> Reply | None:
        # Held for B4-02. The stub does not read the orchestrator.
        _ = (orchestrator, message)
        unimplemented()

    return handle


def end_handler(orchestrator: StsOrchestrator) -> Handler:
    """``sts.session.end`` for this controller (§5.1).

    The controller does not run ``on_stop``. The session worker does, on
    ``sts.ctl.{session_id}`` (IF-05, §8.1). This callable is the
    instance-level entry the ticket names. It takes
    :class:`~mftik.protocol.v2.StsSessionEndRequest` and, once implemented,
    replies with :class:`~mftik.protocol.v2.StsSessionEndResult`: a terminal
    status, not an accept.

    It is not bound to a subject. §5.1 says the controller serves end on
    ``sts.{instance}``, together with start and list. The wire constant and
    :class:`~mftik.protocol.v2.StsSessionEndRequest` say the API sends that
    payload to the worker on ``sts.ctl.{session_id}``. Both sentences are
    in the tree. This function does not choose between them.
    """

    async def handle(message: UntypedEnvelope) -> Reply | None:
        _ = (orchestrator, message)
        unimplemented()

    return handle


def list_handler(orchestrator: StsOrchestrator) -> Handler:
    """``sts.session.list`` on ``sts.{instance}`` (§5.1).

    The reply is :class:`~mftik.protocol.messages.ListSessionsResult`, the
    sessions this instance's supervisor holds. Not the TD list. The request
    type is :class:`~mftik.protocol.messages.ListSessionsRequest`.
    """

    async def handle(message: UntypedEnvelope) -> Reply | None:
        _ = (orchestrator, message)
        unimplemented()

    return handle


def control_subject(instance: str) -> str:
    """``sts.{instance}``, the subject start and list are served on.

    End is not implied. See :func:`end_handler`.
    """
    return Topics.sts(instance)
