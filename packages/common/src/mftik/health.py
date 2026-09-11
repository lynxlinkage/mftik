"""Serving one instance's liveness subject.

Every instanced plane answers the same question in the same way, so the loop
lives here rather than three times over. What differs is only what a plane can
say about itself — which venues an MD reaches, which accounts a TD holds — and
that arrives as a callable.

The subject is :meth:`Topics.health`, not the plane's work subject, and the
loop below is why that matters: a probe old enough that its caller has stopped
waiting is dropped rather than answered. Replying to one is not merely useless,
it is a reply to an address nobody is reading any more — so an instance coming
back from an outage would manufacture exactly the litter the broker's own
bounds exist to prevent.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from mftik.broker import Broker
from mftik.broker.errors import RequestTimeoutError
from mftik.protocol import (
    Envelope,
    HealthCheck,
    HealthStatus,
    Topics,
    probe_is_stale,
)
from mftik.registry.protocol import MFTIK_VERSION

logger = logging.getLogger(__name__)

#: How long the loop waits before rebuilding itself after an exception it did
#: not expect. Same reasoning as each plane's RPC loop: ``Broker.serve``
#: survives what it knows how to survive, so this only paces the rest.
RESTART_DELAY_SECONDS = 1.0

#: What a plane adds to its own reply — venues for MD, accounts for TD.
Describe = Callable[[], dict[str, object]]


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
    reply_type = f"{domain}.health"
    logger.info(
        "%s health listening instance=%s subject=%s",
        domain.upper(),
        instance,
        subject,
    )
    while not stop.is_set():
        try:
            async for req in broker.serve(subject, stop=stop):
                if probe_is_stale(req.envelope):
                    # Its caller stopped waiting long ago and deleted the reply
                    # key. Answering would write to a key nobody reads.
                    logger.debug(
                        "dropping stale health probe instance=%s id=%s",
                        instance,
                        req.envelope.id,
                    )
                    continue
                extra = describe() if describe is not None else {}
                try:
                    await req.reply(
                        Envelope[HealthStatus].wrap(
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
                    )
                except Exception:
                    logger.exception(
                        "health reply failed instance=%s id=%s",
                        instance,
                        req.envelope.id,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "health serve loop failed instance=%s — restarting", instance
            )
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=RESTART_DELAY_SECONDS
                )
            except TimeoutError:
                continue


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
