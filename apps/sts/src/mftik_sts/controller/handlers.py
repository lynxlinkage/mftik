"""``sts.session.start`` / ``end`` / ``list`` as handlers (IF-02).

A handler's whole input is the decoded envelope and its whole output is
the reply (H1). B4-02 serves all three on ``sts.{instance}`` (#298).
``sts.ctl.{session_id}`` stays the worker's subject; nothing here
registers on it.

The reply does not spawn. The router schedules converge after a start
accept, so the accept returns before the process exists (F12).

Registry and env (IF-16, §5.7) are the same shape: a signature here, and
``NotImplementedError("IF-16")`` until B5-10. The running process still
dispatches :mod:`mftik_sts.rpc.registry` and :mod:`mftik_sts.rpc.env`.
``api.registry.catchup`` is not one of these handlers. The API serves
that subject; :func:`catch_up_registry` is the controller's client call.
"""

from __future__ import annotations

from mftik.broker import Broker
from mftik.broker.handler import Handler, Reply
from mftik.protocol import (
    STS_ERROR,
    STS_SESSION_END,
    STS_SESSION_LIST,
    STS_SESSION_START,
    Envelope,
    ListSessionsRequest,
    RpcError,
    RpcErrorEnvelope,
    StsCreateSessionRequest,
    StsSessionEndRequest,
    Topics,
    UntypedEnvelope,
)
from mftik.protocol.messages import ApiRegistryCatchupResult
from pydantic import ValidationError

from mftik_sts.controller.orchestrator import CallError, StsOrchestrator
from mftik_sts.hostdisk._ticket import unimplemented as unimplemented_disk


def _invalid(message: UntypedEnvelope, exc: Exception) -> Reply:
    return RpcErrorEnvelope.wrap(
        RpcError(code="invalid_request", message=str(exc)),
        type=STS_ERROR,
        source="sts",
        session_id=message.session_id,
    )


def _rejected(message: UntypedEnvelope, error: CallError) -> Reply:
    return RpcErrorEnvelope.wrap(
        RpcError(code=error.code, message=error.message),
        type=STS_ERROR,
        source="sts",
        session_id=message.session_id,
    )


def start_handler(orchestrator: StsOrchestrator) -> Handler:
    """``sts.session.start`` for this controller (§5.1, F12, §8.1).

    The reply is an accept: :class:`~mftik.protocol.messages.StsCreateSessionResult`
    with ``status="starting"`` and the request's ``session_id``. ``on_start``
    has not run. Later progress is ``sts.session.status``, not a second
    field on the reply. This function does not spawn.

    Served on ``Topics.sts(instance)``. The subject is bound by the process,
    not by this function.
    """

    async def handle(message: UntypedEnvelope) -> Reply | None:
        try:
            request = StsCreateSessionRequest.model_validate(message.payload)
        except ValidationError as exc:
            return _invalid(message, exc)
        outcome = await orchestrator.accept(request)
        if isinstance(outcome, CallError):
            return _rejected(message, outcome)
        return Envelope.wrap(
            outcome,
            type=STS_SESSION_START,
            source="sts",
            session_id=message.session_id,
        )

    return handle


def end_handler(orchestrator: StsOrchestrator) -> Handler:
    """``sts.session.end`` on ``sts.{instance}`` (§5.1, #298).

    The controller does not run ``on_stop``. It stops the worker through
    the Supervisor (``SIGTERM``, bounded by ``stop_grace_s``); the worker
    runs ``on_stop`` on that signal (B4-03). The reply is the terminal
    status. An unknown session is an error reply, not an exception.
    ``sts.ctl.{session_id}`` is not registered here.
    """

    async def handle(message: UntypedEnvelope) -> Reply | None:
        try:
            request = StsSessionEndRequest.model_validate(message.payload)
        except ValidationError as exc:
            return _invalid(message, exc)
        outcome = await orchestrator.end_session(request)
        if isinstance(outcome, CallError):
            return _rejected(message, outcome)
        return Envelope.wrap(
            outcome,
            type=STS_SESSION_END,
            source="sts",
            session_id=message.session_id,
        )

    return handle


def list_handler(orchestrator: StsOrchestrator) -> Handler:
    """``sts.session.list`` on ``sts.{instance}`` (§5.1).

    The reply is :class:`~mftik.protocol.messages.ListSessionsResult`, the
    sessions this controller holds. Not the TD list. The request type is
    :class:`~mftik.protocol.messages.ListSessionsRequest`. ``status`` is
    matched against the column word (``live``, ``done``, ``failed``).
    """

    async def handle(message: UntypedEnvelope) -> Reply | None:
        try:
            request = ListSessionsRequest.model_validate(message.payload)
        except ValidationError as exc:
            return _invalid(message, exc)
        return Envelope.wrap(
            orchestrator.list_sessions(request),
            type=STS_SESSION_LIST,
            source="sts",
            session_id=message.session_id,
        )

    return handle


def control_subject(instance: str) -> str:
    """``sts.{instance}``, where start, end and list are served (#298).

    ``sts.ctl.{session_id}`` stays the worker's. Registry sync, registry
    reload, and env sync are the same subject once B5-10 registers them
    (§5.7). They are not registered here.
    """
    return Topics.sts(instance)


def registry_sync_handler(orchestrator: StsOrchestrator) -> Handler:
    """``sts.registry.sync`` (§5.7, F39).

    Writes the digest-addressed replica and reports ``loaded`` /
    ``skipped`` from :func:`mftik_sts.hostdisk.probe`. It does not import
    the tree and does not touch a running worker. The request is
    :class:`~mftik.protocol.messages.StsRegistrySyncRequest` and the reply
    is :class:`~mftik.protocol.messages.StsRegistrySyncResult`.

    Not bound to a subject. The process that is running still answers
    this type from :mod:`mftik_sts.rpc.registry`.
    """

    async def handle(message: UntypedEnvelope) -> Reply | None:
        _ = (orchestrator, message)
        unimplemented_disk()

    return handle


def registry_reload_handler(orchestrator: StsOrchestrator) -> Handler:
    """``sts.registry.reload`` (§5.7, F39).

    Rescans the name → digest index. It does not import a tree. The
    request is :class:`~mftik.protocol.messages.StsRegistryReloadRequest`
    and the reply is
    :class:`~mftik.protocol.messages.StsRegistryReloadResult`.

    Not bound to a subject. B5-10 connects it.
    """

    async def handle(message: UntypedEnvelope) -> Reply | None:
        _ = (orchestrator, message)
        unimplemented_disk()

    return handle


def env_sync_handler(orchestrator: StsOrchestrator) -> Handler:
    """``sts.env.sync`` (§5.7, F39).

    Writes the extras replica (``env/gen-{N}``) and does not retarget a
    running worker's ``sys.path``. The request is
    :class:`~mftik.protocol.messages.StsEnvSyncRequest` and the reply is
    :class:`~mftik.protocol.messages.StsEnvSyncResult`.

    Not bound to a subject. The process that is running still answers
    this type from :mod:`mftik_sts.rpc.env`.
    """

    async def handle(message: UntypedEnvelope) -> Reply | None:
        _ = (orchestrator, message)
        unimplemented_disk()

    return handle


async def catch_up_registry(
    broker: Broker, instance: str
) -> ApiRegistryCatchupResult:
    """Ask the API to reconcile this disk (``api.registry.catchup``).

    The wire type is STS → API. The API serves the subject. This
    controller does not register a handler for it. The client that runs
    today is :func:`mftik_sts.registry_catchup.catch_up_until_matched`;
    this function does not call it and does not replace it. B5-10 is
    what moves that client. §5.7 also says the controller serves the
    subject, which this signature does not do.
    """
    _ = (broker, instance)
    unimplemented_disk()
