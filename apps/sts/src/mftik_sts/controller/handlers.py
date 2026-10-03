"""``sts.session.start`` / ``end`` / ``list`` as handlers (IF-02).

A handler's whole input is the decoded envelope and its whole output is
the reply (H1). B4-02 serves all three on ``sts.{instance}`` (#298).
``sts.ctl.{session_id}`` stays the worker's subject; nothing here
registers on it.

``start_handler`` records the session and does not spawn, so the
contract tests stay free of a process. The router then awaits spawn
before it sends that reply: ``CapacityExceeded`` is the start refusal,
and until spawn is entered the session is on ``procman.report`` via
``extra_workers`` (B4-07). ``on_start`` has not run (F12).

Registry and env (IF-16, §5.7) write the digest-addressed replica.
The process that is running still has the old
:mod:`mftik_sts.rpc.registry` and :mod:`mftik_sts.rpc.env` handlers on
:func:`mftik_sts.rpc.dispatch`, which import trees. ``control_handler``
answers these three types from the functions here and does not fall
through to that import. ``api.registry.catchup`` is not one of these
handlers. The API serves that subject; :func:`catch_up_registry` is
the controller's client call.
"""

from __future__ import annotations

import asyncio
import logging

from mftik.broker import Broker
from mftik.broker.handler import Handler, Reply
from mftik.envapply import (
    ApplyFailed,
    EnvironmentDisruptive,
    EnvironmentInvalid,
)
from mftik.environment import EnvironmentLocked
from mftik.protocol import (
    API_REGISTRY_CATCHUP,
    STS_ENV_SYNC,
    STS_ERROR,
    STS_REGISTRY_RELOAD,
    STS_REGISTRY_SYNC,
    STS_SESSION_END,
    STS_SESSION_LIST,
    STS_SESSION_START,
    ApiRegistryCatchupRequest,
    ApiRegistryCatchupRequestEnvelope,
    Envelope,
    ListSessionsRequest,
    RpcError,
    RpcErrorEnvelope,
    StsCreateSessionRequest,
    StsEnvSyncRequest,
    StsRegistryReloadRequest,
    StsRegistrySyncRequest,
    StsSessionEndRequest,
    Topics,
    UntypedEnvelope,
)
from mftik.protocol.messages import ApiRegistryCatchupResult
from pydantic import ValidationError

from mftik_sts.controller.orchestrator import CallError, StsOrchestrator
from mftik_sts.hostdisk.sync import (
    apply_env_sync,
    apply_registry_sync,
    reload_index,
)

logger = logging.getLogger(__name__)


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
    field on the reply. This function does not spawn. The router awaits
    :meth:`~mftik_sts.controller.StsOrchestrator.finish_start` before
    sending the reply, so a capacity refusal replaces it.

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
    reload, and env sync are the same subject (§5.7). The router binds
    them; this function only names it.
    """
    return Topics.sts(instance)


def registry_sync_handler(orchestrator: StsOrchestrator) -> Handler:
    """``sts.registry.sync`` (§5.7, F39).

    Writes the digest-addressed replica and reports ``loaded`` /
    ``skipped`` from :func:`mftik_sts.hostdisk.probe`. It does not import
    the tree and does not touch a running worker. The request is
    :class:`~mftik.protocol.messages.StsRegistrySyncRequest` and the reply
    is :class:`~mftik.protocol.messages.StsRegistrySyncResult`.

    Disk work and the import probe run off the event loop. A tree that
    cannot be written is ``skipped``, not an error reply.
    """

    async def handle(message: UntypedEnvelope) -> Reply | None:
        try:
            request = StsRegistrySyncRequest.model_validate(message.payload)
        except ValidationError as exc:
            return _invalid(message, exc)
        try:
            digests, _generations = await orchestrator.code_pins()
            result = await asyncio.to_thread(
                apply_registry_sync, request, keep_digests=digests
            )
        except Exception as exc:
            logger.exception("registry sync failed")
            return _failed(message, "sync_failed", exc)
        return Envelope.wrap(
            result,
            type=STS_REGISTRY_SYNC,
            source="sts",
            session_id=message.session_id,
        )

    return handle


def registry_reload_handler(orchestrator: StsOrchestrator) -> Handler:
    """``sts.registry.reload`` (§5.7, F39).

    Rescans the name → digest index. It does not import a tree. The
    request is :class:`~mftik.protocol.messages.StsRegistryReloadRequest`
    and the reply is
    :class:`~mftik.protocol.messages.StsRegistryReloadResult`.

    It does not start the import probe.
    """

    async def handle(message: UntypedEnvelope) -> Reply | None:
        try:
            StsRegistryReloadRequest.model_validate(message.payload)
        except ValidationError as exc:
            return _invalid(message, exc)
        _ = orchestrator
        try:
            result = await asyncio.to_thread(reload_index)
        except Exception as exc:
            logger.exception("registry reload failed")
            return _failed(message, "reload_failed", exc)
        return Envelope.wrap(
            result,
            type=STS_REGISTRY_RELOAD,
            source="sts",
            session_id=message.session_id,
        )

    return handle


def env_sync_handler(orchestrator: StsOrchestrator) -> Handler:
    """``sts.env.sync`` (§5.7, F39).

    Writes the extras replica (``env/gen-{N}``) and does not retarget a
    running worker's ``sys.path``. The request is
    :class:`~mftik.protocol.messages.StsEnvSyncRequest` and the reply is
    :class:`~mftik.protocol.messages.StsEnvSyncResult`.

    The pin file is rewritten from this instance's non-terminal pins
    before the apply, so ``commit`` cannot drop a generation a session
    still imports. Running workers are not retargeted.
    """

    async def handle(message: UntypedEnvelope) -> Reply | None:
        try:
            request = StsEnvSyncRequest.model_validate(message.payload)
        except ValidationError as exc:
            return _invalid(message, exc)
        try:
            _digests, generations = await orchestrator.code_pins()
            result = await asyncio.to_thread(
                apply_env_sync, request, keep_generations=generations
            )
        except (
            ApplyFailed,
            EnvironmentDisruptive,
            EnvironmentInvalid,
            EnvironmentLocked,
        ) as exc:
            logger.warning("env sync refused: %s", exc)
            return _failed(message, "env_sync_failed", exc)
        except Exception as exc:
            logger.exception("env sync failed")
            return _failed(message, "env_sync_failed", exc)
        return Envelope.wrap(
            result,
            type=STS_ENV_SYNC,
            source="sts",
            session_id=message.session_id,
        )

    return handle


def _failed(message: UntypedEnvelope, code: str, exc: Exception) -> Reply:
    return RpcErrorEnvelope.wrap(
        RpcError(code=code, message=str(exc)),
        type=STS_ERROR,
        source="sts",
        session_id=message.session_id,
    )


async def catch_up_registry(
    broker: Broker, instance: str
) -> ApiRegistryCatchupResult:
    """Ask the API to reconcile this disk (``api.registry.catchup``).

    The wire type is STS → API. The API serves the subject. This
    controller does not register a handler for it.
    :func:`mftik_sts.registry_catchup.catch_up_until_matched` retries
    this call. §5.7 also says the controller serves the subject, which
    this function does not do: the API already answers it.
    """
    reply = await broker.request(
        API_REGISTRY_CATCHUP,
        ApiRegistryCatchupRequestEnvelope.wrap(
            ApiRegistryCatchupRequest(instance=instance),
            type=API_REGISTRY_CATCHUP,
            source="sts",
        ),
        timeout=60,
    )
    return ApiRegistryCatchupResult.model_validate(reply.payload)
