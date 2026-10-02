"""``sts.session.start`` / ``end`` / ``list`` as handlers (IF-02).

Not registered on the running process. The router still answers those
types with ``NotImplementedError("IF-04")``. B4-02 is what connects these
callables to a subject.

A handler's whole input is the decoded envelope and its whole output is
the reply (H1). Each one raises until B4-02.

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
    Topics,
    UntypedEnvelope,
)
from mftik.protocol.messages import ApiRegistryCatchupResult

from mftik_sts.controller._ticket import unimplemented
from mftik_sts.controller.orchestrator import StsOrchestrator
from mftik_sts.hostdisk._ticket import unimplemented as unimplemented_disk


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

    End is not implied. See :func:`end_handler`. Registry sync, registry
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
