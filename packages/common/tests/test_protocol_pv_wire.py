"""Senders stamp ``pv`` on the bytes that leave the process.

B4-01. :class:`~mftik.protocol.Envelope` defaults ``pv`` to
:data:`~mftik.protocol.PROTOCOL_VERSION`, and ``Broker.publish``,
``Broker.request`` and the reply path send ``envelope.to_json()``.
This reads those bytes back off the shared NATS connection. Parsing
them with ``Envelope.from_json`` would fill in a missing ``pv`` from
the field default, so the assertion uses ``json.loads`` on the payload
the server delivered.

The receiver's refusal is not on this path. Where that call sits is
issue #282.
"""

from __future__ import annotations

import asyncio
import contextlib
import json

import pytest
from broker_harness import session_loop, subjects_under
from mftik.broker import Broker, IncomingRequest
from mftik.protocol import PROTOCOL_VERSION, Envelope
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg

pytestmark = [session_loop, pytest.mark.component]


def _pv(raw: bytes) -> object:
    """``pv`` as it was on the wire, with no envelope default applied."""
    body = json.loads(raw)
    if not isinstance(body, dict) or "pv" not in body:
        raise AssertionError(f"wire frame has no pv: {raw!r}")
    return body["pv"]


async def _wait_until_subscribed(
    connection: NatsClient, key_prefix: str, subject: str
) -> None:
    """Yield until ``subject`` is on this connection, then flush the SUB."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 2.0
    while loop.time() < deadline:
        if subject in subjects_under(connection, key_prefix):
            await connection.flush(timeout=2)
            return
        await asyncio.sleep(0)
    raise TimeoutError(subject)


async def _capture_publish(
    broker: Broker,
    connection: NatsClient,
    topic: str,
    envelope: Envelope[dict],
) -> bytes:
    subject = f"{broker.config.key_prefix}.ps.{topic}"
    captured: asyncio.Queue[bytes] = asyncio.Queue()

    async def on_msg(msg: Msg) -> None:
        captured.put_nowait(msg.data)

    sub = await connection.subscribe(subject, cb=on_msg)
    try:
        await connection.flush(timeout=2)
        await broker.publish(topic, envelope)
        return await asyncio.wait_for(captured.get(), timeout=2)
    finally:
        with contextlib.suppress(Exception):
            await sub.unsubscribe()


async def _capture_request_and_reply(
    broker: Broker,
    connection: NatsClient,
    subject: str,
    request: Envelope[dict],
    reply: Envelope[dict],
) -> tuple[bytes, bytes]:
    """Raw request bytes, then the raw bytes ``IncomingRequest.reply`` sends."""
    rpc = f"{broker.config.key_prefix}.rpc.{subject}"
    stop = asyncio.Event()
    incoming: asyncio.Queue[IncomingRequest] = asyncio.Queue()

    async def server() -> None:
        async for req in broker.serve(subject, stop=stop):
            await incoming.put(req)
            return

    server_task = asyncio.create_task(server())
    request_task: asyncio.Task[object] | None = None
    watcher = None
    reply_sub = None
    try:
        await _wait_until_subscribed(connection, broker.config.key_prefix, rpc)
        request_bytes: asyncio.Queue[bytes] = asyncio.Queue()

        async def on_request(msg: Msg) -> None:
            request_bytes.put_nowait(msg.data)

        watcher = await connection.subscribe(rpc, cb=on_request)
        await connection.flush(timeout=2)
        request_task = asyncio.create_task(
            broker.request(subject, request, timeout=2)
        )
        raw_request = await asyncio.wait_for(request_bytes.get(), timeout=2)
        req = await asyncio.wait_for(incoming.get(), timeout=2)
        reply_to = req.envelope.reply_to
        if not reply_to:
            raise AssertionError("request had no reply inbox")

        reply_bytes: asyncio.Queue[bytes] = asyncio.Queue()

        async def on_reply(msg: Msg) -> None:
            reply_bytes.put_nowait(msg.data)

        # The client's own inbox subscription also matches. A second
        # subscriber receives a copy, which is the raw reply.
        reply_sub = await connection.subscribe(reply_to, cb=on_reply)
        await connection.flush(timeout=2)
        await req.reply(reply)
        raw_reply = await asyncio.wait_for(reply_bytes.get(), timeout=2)
        await asyncio.wait_for(request_task, timeout=2)
        return raw_request, raw_reply
    finally:
        stop.set()
        server_task.cancel()
        if request_task is not None and not request_task.done():
            request_task.cancel()
        pending = [server_task]
        if request_task is not None:
            pending.append(request_task)
        await asyncio.gather(*pending, return_exceptions=True)
        for sub in (watcher, reply_sub):
            if sub is None:
                continue
            with contextlib.suppress(Exception):
                await sub.unsubscribe()


async def test_publish_request_and_reply_carry_pv_on_the_wire(
    broker: Broker,
    nats_connection: NatsClient,
) -> None:
    """A publish, a request and a reply each leave with the current ``pv``.

    Compared against ``to_json()`` as well as the parsed field, so a
    frame that was rewritten on the way out and happened to contain
    some other ``pv`` does not pass.
    """
    published = Envelope[dict].wrap({"n": 1}, type="demo", source="test")
    pub_raw = await _capture_publish(
        broker, nats_connection, "topic.pv", published
    )
    assert pub_raw.decode() == published.to_json()
    assert _pv(pub_raw) == PROTOCOL_VERSION

    request = Envelope[dict].wrap({"n": 2}, type="demo.request", source="test")
    reply = Envelope[dict].wrap({"n": 3}, type="demo.reply", source="server")
    request_raw, reply_raw = await _capture_request_and_reply(
        broker, nats_connection, "demo.pv", request, reply
    )
    assert request_raw.decode() == request.to_json()
    assert _pv(request_raw) == PROTOCOL_VERSION
    assert reply_raw.decode() == reply.to_json()
    assert _pv(reply_raw) == PROTOCOL_VERSION
