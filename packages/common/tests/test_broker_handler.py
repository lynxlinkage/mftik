"""The handler layer: the split between a message's answer and its wire.

IF-02. Three things are tested here, in the order they matter:

* **The handler on its own, with no bus at all.** This is the whole point of
  F31 — ``await handler(envelope)`` is a function call, so what a plane *does*
  with a message is testable without a subject, a requester or a reply inbox.
* **``serve``, over real NATS.** The transport half is tested once, here, so
  that no plane has to test it again (§9.2: no broker fake, connection and
  behaviour tested separately).
* **The conversion IF-02 did.** ``serve_health`` answers the same subject with
  the same envelope it did before, and its stale-probe rule is now a handler
  that returns ``None``.

The ``xfail(strict=True)`` block at the end is the contract the B tickets owe
(IF 共同驗收 4): every plane's request-reply goes through this layer, and no
handler is handed the transport. ``strict`` is what makes those markers
impossible to leave behind — the conversion that makes one pass has to delete
it.
"""

from __future__ import annotations

import ast
import asyncio
import importlib
from pathlib import Path
from typing import Any

import pytest
from broker_harness import a_broker, inject_raw_request
from mftik.broker import Broker, RequestTimeoutError
from mftik.broker.handler import Reply, serve
from mftik.health import health_handler, serve_health
from mftik.protocol import (
    Envelope,
    HealthCheck,
    HealthCheckEnvelope,
    HealthStatus,
    RpcError,
    Topics,
    UntypedEnvelope,
    probe_is_stale,
)
from mftik.registry.protocol import MFTIK_VERSION

SUBJECT = "demo.handler"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-handler") as client:
        yield client


def a_probe(*, age: float = 0.0) -> UntypedEnvelope:
    """A health probe, optionally old enough to be dropped."""
    probe = HealthCheckEnvelope.wrap(HealthCheck(), type="md.health", source="test")
    envelope = UntypedEnvelope.model_validate_json(probe.to_json())
    if not age:
        return envelope
    return envelope.model_copy(update={"ts": envelope.ts - age})


def a_request(n: int) -> Envelope[dict[str, Any]]:
    return Envelope[dict].wrap({"n": n}, type="demo", source="test")


def an_answer(n: int) -> Reply:
    return Envelope[dict].wrap({"n": n}, type="demo.reply", source="server")


# --- the handler, called directly ------------------------------------------


async def test_a_handler_is_a_call_from_one_message_to_its_answer() -> None:
    """No broker, no subject, no inbox — this is F31's whole shape (H1).

    Written against the one converted RPC rather than a toy, because the
    assertion worth making is that a real plane's answer is reachable this way.
    """
    handle = health_handler(domain="md", instance="md-jp-1")

    reply = await handle(a_probe())

    assert reply is not None
    assert reply.type == "md.health"
    assert reply.source == "md"
    status = HealthStatus.model_validate(reply.payload)
    assert status.status == "ok"
    assert status.instance == "md-jp-1"
    assert status.domain == "md"
    assert status.version == MFTIK_VERSION


async def test_a_handler_answers_nothing_rather_than_answering_nobody() -> None:
    """``None`` is an answer, not a failure (H2).

    A probe this old has a caller that stopped waiting; its reply inbox is
    gone. The rule used to be a ``continue`` inside a serve loop, which meant
    the only way to test it was to send a message and prove nothing came back.
    """
    probe = a_probe(age=60.0)
    assert probe_is_stale(probe)

    assert await health_handler(domain="md", instance="md-jp-1")(probe) is None


async def test_what_a_plane_says_about_itself_is_read_at_reply_time() -> None:
    """``describe`` runs per reply, so it cannot describe a dropped venue."""
    venues = ["Paper"]
    handle = health_handler(
        domain="md", instance="md-jp-1", describe=lambda: {"venues": list(venues)}
    )

    first = await handle(a_probe())
    venues.append("Deribit")
    second = await handle(a_probe())

    assert first is not None and second is not None
    assert HealthStatus.model_validate(first.payload).venues == ["Paper"]
    assert HealthStatus.model_validate(second.payload).venues == [
        "Paper",
        "Deribit",
    ]


# --- serve, over the bus ---------------------------------------------------
#
# B2-05: these open a private socket to test ``serve``. That is the transport
# half IF-02 tests once; the direct handler calls above stay unit. No later
# ticket rewrites them into a direct call.


@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
async def test_serve_sends_back_what_the_handler_returned(broker: Broker) -> None:
    seen: list[UntypedEnvelope] = []
    stop = asyncio.Event()

    async def handle(message: UntypedEnvelope) -> Reply | None:
        seen.append(message)
        return an_answer(message.payload["n"] + 1)

    task = asyncio.create_task(serve(broker, SUBJECT, handle, stop=stop))
    await asyncio.sleep(0.05)
    try:
        reply = await broker.request(SUBJECT, a_request(1), timeout=2)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)

    assert reply.type == "demo.reply"
    assert reply.payload == {"n": 2}
    # Decoded, and with the reply address the transport put on it — the handler
    # is given it but has no use for it (H1).
    assert [message.payload for message in seen] == [{"n": 1}]
    assert seen[0].type == "demo"


@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
async def test_serve_answers_one_message_at_a_time(broker: Broker) -> None:
    """H3. The next message waits until this one has been answered.

    A handler that must not hold the subject starts its own task and returns.
    This layer does not add concurrency on top of that.
    """
    stop = asyncio.Event()
    started = asyncio.Event()
    release = asyncio.Event()
    order: list[str] = []

    async def handle(message: UntypedEnvelope) -> Reply | None:
        n = message.payload["n"]
        order.append(f"start {n}")
        if n == 1:
            started.set()
            await release.wait()
        order.append(f"end {n}")
        return an_answer(n)

    task = asyncio.create_task(serve(broker, SUBJECT, handle, stop=stop))
    await asyncio.sleep(0.05)
    try:
        first = asyncio.create_task(
            broker.request(SUBJECT, a_request(1), timeout=2)
        )
        await asyncio.wait_for(started.wait(), timeout=2)
        second = asyncio.create_task(
            broker.request(SUBJECT, a_request(2), timeout=2)
        )
        # Long enough for the second request to reach the subscription. If
        # ``serve`` pulled it concurrently, ``order`` would already name it.
        await asyncio.sleep(0.05)
        assert order == ["start 1"]
        release.set()
        assert (await first).payload == {"n": 1}
        assert (await second).payload == {"n": 2}
    finally:
        release.set()
        stop.set()
        await asyncio.wait_for(task, timeout=5)

    assert order == ["start 1", "end 1", "start 2", "end 2"]


@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
async def test_a_handler_returning_none_sends_nothing(broker: Broker) -> None:
    """The requester times out, which is what "no answer" looks like (H2)."""
    stop = asyncio.Event()
    served = asyncio.Event()

    async def handle(message: UntypedEnvelope) -> Reply | None:
        served.set()
        return None

    task = asyncio.create_task(serve(broker, SUBJECT, handle, stop=stop))
    await asyncio.sleep(0.05)
    try:
        with pytest.raises(RequestTimeoutError):
            await broker.request(SUBJECT, a_request(1), timeout=0.3)
        assert served.is_set()
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)


@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
async def test_a_handler_that_raises_costs_one_reply_not_the_subject(
    broker: Broker,
) -> None:
    """H5. The subject is the plane's control plane; one bad message is one
    bad message.

    A handler that wants the requester to hear about its own failure returns an
    :class:`RpcError` envelope — ``serve`` does not invent one, which is why
    the first request below times out rather than coming back rejected.
    """
    stop = asyncio.Event()

    async def handle(message: UntypedEnvelope) -> Reply | None:
        if message.payload["n"] == 1:
            raise RuntimeError("the handler's problem")
        return an_answer(message.payload["n"])

    task = asyncio.create_task(serve(broker, SUBJECT, handle, stop=stop))
    await asyncio.sleep(0.05)
    try:
        with pytest.raises(RequestTimeoutError):
            await broker.request(SUBJECT, a_request(1), timeout=0.3)
        reply = await broker.request(SUBJECT, a_request(2), timeout=2)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)

    assert reply.payload == {"n": 2}


@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
async def test_a_handler_may_reject_by_returning_a_reject(broker: Broker) -> None:
    """The other half of H5: a refusal is a reply like any other."""
    stop = asyncio.Event()

    async def handle(message: UntypedEnvelope) -> Reply | None:
        return Envelope[RpcError].wrap(
            RpcError(code="unknown_type", message=f"unknown type: {message.type}"),
            type="demo.error",
            source="server",
        )

    task = asyncio.create_task(serve(broker, SUBJECT, handle, stop=stop))
    await asyncio.sleep(0.05)
    try:
        reply = await broker.request(SUBJECT, a_request(1), timeout=2)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)

    assert RpcError.model_validate(reply.payload).code == "unknown_type"


@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
async def test_an_unreadable_request_never_reaches_the_handler(
    broker: Broker,
) -> None:
    """H6 from below: a handler is only ever handed an envelope.

    ``Broker.serve`` drops what will not parse. Asserted through ``serve``
    because this is the door every plane will come through.
    """
    stop = asyncio.Event()
    seen: list[UntypedEnvelope] = []

    async def handle(message: UntypedEnvelope) -> Reply | None:
        seen.append(message)
        return an_answer(message.payload["n"])

    task = asyncio.create_task(serve(broker, SUBJECT, handle, stop=stop))
    await asyncio.sleep(0.2)
    try:
        await inject_raw_request(broker, SUBJECT, "{not an envelope")
        reply = await broker.request(SUBJECT, a_request(7), timeout=2)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)

    assert reply.payload == {"n": 7}
    assert [message.payload for message in seen] == [{"n": 7}]


@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
async def test_serve_rebuilds_itself_after_an_unexpected_failure(
    broker: Broker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """H4. August 2026: STS ran three live sessions for seven hours with
    nothing able to list, pause or stop one of them, because a serve loop's
    ``try`` covered the dispatch and not the iteration.

    Each plane still writes that loop out for itself, and only STS's copy has a
    test like this one. Once they all come through ``serve``, this is where it
    is tested.
    """
    stop = asyncio.Event()
    real_serve = broker.serve
    failures: list[str] = []

    def flaky(*args: Any, **kwargs: Any) -> Any:
        if not failures:
            failures.append("boom")
            raise RuntimeError("something Broker.serve does not handle")
        return real_serve(*args, **kwargs)

    async def handle(message: UntypedEnvelope) -> Reply | None:
        return an_answer(message.payload["n"])

    monkeypatch.setattr(broker, "serve", flaky)

    task = asyncio.create_task(
        serve(broker, SUBJECT, handle, stop=stop, restart_delay=0.0)
    )
    await asyncio.sleep(0.05)
    try:
        reply = await broker.request(SUBJECT, a_request(3), timeout=5)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)

    assert failures == ["boom"]
    assert reply.payload == {"n": 3}


@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
async def test_stop_ends_the_loop(broker: Broker) -> None:
    """The one thing that may end it, and it must actually end it (H4)."""
    stop = asyncio.Event()
    seen: list[UntypedEnvelope] = []

    async def handle(message: UntypedEnvelope) -> Reply | None:
        seen.append(message)
        return None

    task = asyncio.create_task(serve(broker, SUBJECT, handle, stop=stop))
    await asyncio.sleep(0.1)
    stop.set()

    await asyncio.wait_for(task, timeout=5)
    assert seen == []


# --- the conversion: the wire did not move ---------------------------------


@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
async def test_serve_health_still_answers_its_own_subject(broker: Broker) -> None:
    """``health.{domain}.{instance}``, the reply every dashboard row reads.

    The subject, the reply type and the payload are what the API's instance
    list and ``refuse_if_serving`` already depend on, so the conversion is only
    honest if this is unchanged.
    """
    stop = asyncio.Event()
    task = asyncio.create_task(
        serve_health(
            broker,
            domain="md",
            instance="md-jp-1",
            stop=stop,
            describe=lambda: {"venues": ["Paper"]},
        )
    )
    await asyncio.sleep(0.05)
    try:
        reply = await broker.request(
            Topics.health("md", "md-jp-1"),
            HealthCheckEnvelope.wrap(
                HealthCheck(), type="md.health", source="test"
            ),
            timeout=2,
        )
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)

    assert reply.type == "md.health"
    assert reply.source == "md"
    status = HealthStatus.model_validate(reply.payload)
    assert status.status == "ok"
    assert status.service == "md"
    assert status.instance == "md-jp-1"
    assert status.domain == "md"
    assert status.version == MFTIK_VERSION
    assert status.venues == ["Paper"]


# --- the contract the B tickets owe ----------------------------------------

#: The tree, found from this file rather than from the working directory, so the
#: scan reads the same under ``pytest packages`` and under ``pytest`` at the
#: root. Same reasoning as ``test_broker_is_the_only_transport``.
ROOT = Path(__file__).resolve().parents[3]

#: The layer itself: ``serve`` is the one place allowed to iterate
#: ``Broker.serve`` and to hold an :class:`~mftik.broker.IncomingRequest`,
#: because being that place is what it is for.
LAYER = ROOT / "packages" / "common" / "src" / "mftik" / "broker"

#: Where the scan looks: the three planes being rewritten, plus the shared
#: library. ``apps/sym`` and ``apps/paper`` are outside the refactor (§5 to §7
#: are STS, MD and TD), so their loops are left where they are.
#:
#: The API is outside too. ``api.registry.catchup`` is served there today, and
#: §5.7 moves it onto the STS controller (F40); that handler's signature is
#: IF-16, not this ticket. Registry, extras, artifacts and event-log reads
#: already live under ``apps/sts`` and stay in the scan: F40 assigns them to
#: that controller, so this contract does not leave their owner open and does
#: not define their signatures.
TREES = (
    ROOT / "packages" / "common" / "src",
    ROOT / "apps" / "sts" / "src",
    ROOT / "apps" / "md" / "src",
    ROOT / "apps" / "td" / "src",
)

#: A floor, so a glob that finds nothing passes for the wrong reason.
MIN_FILES_SCANNED = 100


def _sources() -> list[Path]:
    return [
        path
        for tree in TREES
        for path in sorted(tree.rglob("*.py"))
        if LAYER not in path.parents
    ]


def _mixes_transport_with_logic(path: Path) -> list[str]:
    """Where ``path`` still writes its own serve loop, or takes a request handle.

    Two spellings of one thing. ``x.serve(...)`` is a module running the
    transport itself; importing ``IncomingRequest`` is a module being handed
    the wire to reply on. A converted module does neither: it builds handlers
    and calls :func:`serve` by name, the way ``mftik.health`` does.
    """
    tree = ast.parse(path.read_text(), filename=str(path))
    found: list[str] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "serve"
        ):
            found.append(
                f"{path.relative_to(ROOT)}:{node.lineno} runs its own serve loop"
            )
        elif isinstance(node, ast.ImportFrom) and any(
            alias.name == "IncomingRequest" for alias in node.names
        ):
            found.append(
                f"{path.relative_to(ROOT)}:{node.lineno} imports IncomingRequest"
            )
    return found


def test_the_scan_reaches_the_tree() -> None:
    """A guard that checked nothing would pass every time."""
    files = _sources()

    assert len(files) >= MIN_FILES_SCANNED, (
        f"only {len(files)} source files found under {ROOT} — "
        "the scan is looking in the wrong place"
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "B4-02 / B4-05 / B4-07 convert the three planes' request-reply onto "
        "this layer, and B6-05 / B7-05 take the backfill and fetch loops with "
        "them. F40 keeps registry, extras, artifacts and event-log reads on "
        "the STS controller: IF-16 defines the registry and env signatures, "
        "and B5-11 keeps the artifact and event-log handlers there. Today "
        "each one hand-rolls the loop or replies through the request handle"
    ),
)
def test_the_three_planes_answer_through_this_layer() -> None:
    """What IF-02 exists to end: transport written out beside the logic (F31).

    One copy of the serve loop per plane — plus MD's fetch worker and TD's
    backfill worker — and handlers that reply through a transport handle
    instead of returning an answer. Both are why 250 tests go through NATS to
    assert something that is a function of one message (§9.3), and both go away
    as each plane is converted.

    F40 keeps registry, extras, ``sts.artifact.*`` and event-log reads on the
    STS controller, so those modules are in this list too: they still take
    :class:`~mftik.broker.IncomingRequest`. IF-16 owns the registry and env
    signatures; B5-11 keeps the artifact and event-log handlers on the
    controller. This scan does not define either.

    Listed by file and line so the failure reads as the worklist it is.
    """
    mixed = [
        finding
        for path in _sources()
        for finding in _mixes_transport_with_logic(path)
    ]

    assert mixed == [], (
        "these still mix the wire with the answer:\n  "
        + "\n  ".join(mixed)
        + "\n\nA converted module builds a Handler — one decoded message in, "
        "one reply envelope out — and hands it to mftik.broker.handler.serve."
    )


@pytest.mark.parametrize(
    "plane",
    [
        "sts",
        "md",
        pytest.param(
            "td",
            marks=pytest.mark.xfail(
                strict=True,
                reason="B4-05 rewrites the TD router onto this layer",
            ),
        ),
    ],
)
async def test_a_plane_s_health_type_is_answered_by_a_handler(plane: str) -> None:
    """The per-plane form of H1, on the smallest RPC each plane has.

    ``{plane}.health`` arrives on the plane's own control subject and is routed
    by ``Envelope.type``, which is the other half of the conversion: the router
    becomes a handler that returns its answer, so every type it dispatches can
    be tested by calling it.

    IF-02 deliberately converted ``serve_health`` and nothing else, so these
    three are the contract rather than the change.
    """
    module = importlib.import_module(f"mftik_{plane}.rpc.health")
    message = UntypedEnvelope.model_validate_json(
        HealthCheckEnvelope.wrap(
            HealthCheck(), type=f"{plane}.health", source="test"
        ).to_json()
    )

    reply = await module.handle_health(message)

    assert reply is not None
    assert HealthStatus.model_validate(reply.payload).status == "ok"
