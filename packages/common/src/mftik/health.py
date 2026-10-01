"""Serving one instance's liveness subject.

Every instanced plane answers the same question in the same way, so this lives
here rather than three times over. What differs is only what a plane can say
about itself — which venues an MD reaches, which accounts a TD holds — and that
arrives as a callable.

The subject is :meth:`Topics.health`, not the plane's work subject, and
:func:`health_handler` is why that matters: a probe old enough that its caller
has stopped waiting is dropped rather than answered. Replying to one is not
merely useless, it is a reply to an address nobody is reading any more — so an
instance coming back from an outage would manufacture exactly the litter the
broker's own bounds exist to prevent.

This is the one RPC IF-02 converted to :mod:`mftik.broker.handler`, as the
worked example. The serve loop that used to be written out here is
:func:`mftik.broker.handler.serve` now, and what is left is the answer itself:
one message in, one envelope out, callable from a test with no bus in sight
(F31, §9.2). The wire is unchanged — same subject, same reply type, same
payload, same probes dropped.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from mftik.broker import Broker
from mftik.broker.errors import RequestTimeoutError
from mftik.broker.handler import Handler, serve
from mftik.protocol import (
    Envelope,
    HealthCheck,
    HealthStatus,
    Topics,
    UntypedEnvelope,
    probe_is_stale,
)
from mftik.registry.protocol import MFTIK_VERSION

logger = logging.getLogger(__name__)

#: What a plane adds to its own reply — venues for MD, accounts for TD.
Describe = Callable[[], dict[str, object]]


def health_handler(
    *,
    domain: str,
    instance: str,
    describe: Describe | None = None,
) -> Handler:
    """The answer half: a probe in, this instance's status out.

    ``describe`` is read at reply time rather than once here, so what a plane
    says about itself cannot describe a venue it has since dropped.
    """
    reply_type = f"{domain}.health"

    async def handle(message: UntypedEnvelope) -> Envelope[HealthStatus] | None:
        if probe_is_stale(message):
            # Its caller stopped waiting long ago and is no longer reading the
            # reply inbox. Answering would write to an address nobody reads.
            logger.debug(
                "dropping stale health probe instance=%s id=%s",
                instance,
                message.id,
            )
            return None
        extra = describe() if describe is not None else {}
        return Envelope[HealthStatus].wrap(
            HealthStatus(
                status="ok",
                service=domain,
                instance=instance,
                domain=domain,
                version=MFTIK_VERSION,
                **extra,  # type: ignore[arg-type]
            ),
            type=reply_type,
            source=domain,
        )

    return handle


async def serve_health(
    broker: Broker,
    *,
    domain: str,
    instance: str,
    stop: asyncio.Event,
    describe: Describe | None = None,
) -> None:
    """Answer liveness on ``health.{domain}.{instance}`` until ``stop``.

    Runs alongside the plane's own RPC loop rather than inside it. The two have
    different failure meanings: an RPC loop that stops leaves sessions nobody
    can list or halt, while this one stopping costs a dashboard row. Sharing a
    task would let either take the other down.
    """
    subject = Topics.health(domain, instance)
    logger.info(
        "%s health listening instance=%s subject=%s",
        domain.upper(),
        instance,
        subject,
    )
    await serve(
        broker,
        subject,
        health_handler(domain=domain, instance=instance, describe=describe),
        stop=stop,
    )


class InstanceAlreadyServing(RuntimeError):
    """A process is already answering this instance's control subject."""

    def __init__(self, subject: str, source: str) -> None:
        self.subject = subject
        self.source = source
        super().__init__(
            f"{subject} is already served by {source} — a second process "
            f"with this MFTIK_INSTANCE is not a supported topology"
        )


async def refuse_if_serving(
    broker: Broker, *, domain: str, instance: str, timeout: float = 1.0
) -> None:
    """Exit-path probe: refuse to boot if this instance name is already up.

    ``probe`` is not a lock. Two processes that pass in the same window can
    still both start; that remaining race is accepted. ``--scale`` and an
    overlapped restart should die here and name who answered.
    """
    named = {"td": Topics.td, "md": Topics.md, "sts": Topics.sts}
    if domain not in named:
        raise ValueError(f"{domain} is not an instanced plane")
    subject = named[domain](instance)
    try:
        reply = await broker.probe(
            subject,
            Envelope[HealthCheck].wrap(
                HealthCheck(),
                type=f"{domain}.health",
                source="boot",
            ),
            timeout=timeout,
        )
    except RequestTimeoutError:
        return
    source = reply.source
    if isinstance(reply.payload, dict):
        source = str(reply.payload.get("instance") or source)
    raise InstanceAlreadyServing(subject, source)
