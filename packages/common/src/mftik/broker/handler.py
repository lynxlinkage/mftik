"""Handlers — a message in, a reply out, with the transport taken out of it.

A **handler** is the half of an RPC that is not the wire. It is handed one
already-decoded message and it answers with the envelope to send back, or with
``None`` when there is nothing to send. It never sees the subject it arrived on,
the reply inbox it would be written to, or the broker. :func:`serve` is the
other half: the loop that reads a subject, hands each message over, and sends
back whatever came out.

Today the two are written together: the same twenty lines of
``while not stop.is_set(): async for req in broker.serve(...)`` around a
dispatch that replies through the request handle it was given, once per plane
and again in MD's fetch worker and TD's backfill worker. Two costs come out of
that, and the second is the expensive one:

* The loop is copied, so a fix to it is a fix in several places. That is how STS
  spent seven hours in August 2026 running sessions nothing could list or stop:
  one copy had its ``try`` around the dispatch and not around the iteration, so
  the coroutine returned and the process stayed up (``test_rpc_loop_survives``).
* The logic can only be reached through NATS. "What does this plane do with
  this message" is a function of one message, but asserting it needs a subject,
  a requester and a reply inbox — which is why 250 of the surviving tests go
  through the bus to test something that never needed it (§9.3). F31 is the
  answer, and it is a shape rather than a fake: the behaviour test calls the
  handler directly, and the transport is tested once, here.

**State authority (§3.3): this layer holds none.** :func:`serve` owns one
subscription for as long as it runs and nothing else; a handler's state is the
handler's, injected when it is built. Nothing here is the authority for
anything, which is why there is no class to instantiate.

**Invariants.**

* **H1 — a handler never touches the transport.** Its whole input is the
  decoded envelope and its whole output is the reply. No broker, no subject, no
  inbox, no ``await`` on the wire. A handler that needs to publish holds the
  publisher it was built with; that is a side effect, not a reply.
* **H2 — ``None`` is "nothing to send", not a failure.** A probe too old to be
  worth answering (:func:`~mftik.protocol.probe_is_stale`), a message whose
  requester has gone, a fan-out message that was never a question. Each of
  those is a handler returning ``None``, and :func:`serve` sends nothing.
* **H3 — one message at a time, in arrival order.** :func:`serve` awaits the
  handler before reading the next message, which is what every plane's loop
  does today. A handler that must not hold up its subject starts a task and
  returns; it does not get concurrency from this layer.
* **H4 — only ``stop`` or cancellation ends the loop.** Every other failure is
  logged and the loop is rebuilt, because a process whose control subject went
  silent keeps trading with nothing able to stop it.
* **H5 — a handler's exception costs one reply, not the subject.** It is logged
  and the loop goes on. :func:`serve` does not invent an error reply: whether a
  fault is answered or left to time out is the handler's to decide, and a
  handler that wants to answer returns an :class:`~mftik.protocol.RpcError`
  envelope like any other reply.
* **H6 — the payload is not interpreted here.** A handler is handed
  :data:`~mftik.protocol.UntypedEnvelope` and validates what it reads. Typed
  payloads, and the ``pv`` version gate, belong to the protocol (IF-01).

This is the layer §3.4 names ``mftik.broker.handler``. Converting each plane's
RPC onto it is the B tickets' work; ``serve_health`` (:mod:`mftik.health`) is
the one conversion IF-02 did, as the worked example. F40 keeps registry,
extras, artifacts and event-log reads on the STS controller, so those RPCs
come through here with the rest. IF-16 owns the registry and env handler
signatures; this module does not define them.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Protocol

from mftik.protocol import Envelope, UntypedEnvelope

if TYPE_CHECKING:
    from mftik.broker.client import Broker

logger = logging.getLogger(__name__)

#: How long :func:`serve` waits before rebuilding itself after an exception it
#: did not expect.
#:
#: :meth:`~mftik.broker.Broker.serve` already survives what it can name, so this
#: only paces what is left. It is a pace rather than a backoff on purpose: the
#: failures that reach it are a connection that went away and came back, and a
#: subject nobody is answering costs more than a reconnect attempt does.
RESTART_DELAY_SECONDS = 1.0

#: What a handler answers with. Untyped in the signature because every reply has
#: a different payload model, and the handler is the one that knows which.
Reply = Envelope[Any]


class Handler(Protocol):
    """One decoded message in, one reply or nothing out.

    A protocol rather than a base class, so a plain ``async def`` is a handler
    and so is a callable object holding the state its answers come from — which
    is what every converted RPC will be, since the state is what makes the
    answer interesting.

    Not :meth:`mftik.broker.Broker.serve_handler`. That callable is handed an
    :class:`~mftik.broker.IncomingRequest` and writes the reply itself, which
    is the mixing this layer takes apart. It stays for now: the API's registry
    catch-up still calls it. §5.7 moves ``api.registry.catchup`` onto the STS
    controller (F40); that handler's signature is IF-16, not this module.
    """

    async def __call__(self, message: UntypedEnvelope) -> Reply | None:
        """Answer ``message``, or return ``None`` to send nothing (H2)."""


async def serve(
    broker: Broker,
    subject: str,
    handler: Handler,
    *,
    stop: asyncio.Event | None = None,
    restart_delay: float = RESTART_DELAY_SECONDS,
) -> None:
    """Run ``handler`` on every request to ``subject`` until ``stop``.

    The transport half of this module: decode, hand over, reply, and keep the
    subscription alive. ``stop`` is the only thing that ends it short of
    cancellation (H4) — an exception from the handler costs that one reply (H5)
    and an exception from the iteration costs ``restart_delay`` and a new
    subscription.

    Left without a ``stop``, this runs until the task is cancelled. That is
    what a worker whose shutdown is a signal wants; a plane that stops by
    setting an event should pass it, so the loop ends where the event does
    rather than where the cancellation lands.
    """
    while stop is None or not stop.is_set():
        try:
            async for request in broker.serve(subject, stop=stop):
                reply = await _answer(handler, subject, request.envelope)
                if reply is None:
                    continue
                try:
                    await request.reply(reply)
                except Exception:
                    # The answer is built and the requester may still be
                    # waiting for it, but one undeliverable reply is not a
                    # reason to stop answering the rest.
                    logger.exception(
                        "reply failed subject=%s type=%s id=%s",
                        subject,
                        request.envelope.type,
                        request.envelope.id,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            # Reaching here means something ``Broker.serve`` does not already
            # handle, and the answer is still to serve: this coroutine
            # returning is how a plane ends up alive with its control subject
            # silent and no line anywhere saying so.
            logger.exception(
                "serve loop failed subject=%s — restarting", subject
            )
            if stop is None:
                await asyncio.sleep(restart_delay)
                continue
            try:
                await asyncio.wait_for(stop.wait(), timeout=restart_delay)
            except TimeoutError:
                continue


async def _answer(
    handler: Handler, subject: str, message: UntypedEnvelope
) -> Reply | None:
    """``handler(message)``, with its failures kept off the loop (H5)."""
    try:
        return await handler(message)
    except Exception:
        logger.exception(
            "handler failed subject=%s type=%s id=%s",
            subject,
            message.type,
            message.id,
        )
        return None


__all__ = [
    "RESTART_DELAY_SECONDS",
    "Handler",
    "Reply",
    "serve",
]
