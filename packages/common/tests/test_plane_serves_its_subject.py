"""Every instanced plane serves the subject it was handed, not a constant.

Issue #72. STS took ``subject=`` and served ``Topics.STS`` regardless, so a
named instance answered nothing on ``sts.{instance}`` while its health probe —
which has its own subject — kept saying *connected*. A deploy that pinned STS
timed out against a process the dashboard called healthy.

The three loops were changed by one patch applied per file, and it matched two
of them: STS's ``run_rpc`` had no docstring, so the multi-line pattern missed
the two lines that mattered and hit only the signature. That is the failure
mode this file is aimed at — not one plane's bug, but a change believed to be
uniform that was not. So it runs against all three, and a fourth plane joining
them will fail here until it is wired the same way.

Tested through ``broker.serve`` rather than by reading the source: what has to
be true is which Redis list the loop pops, and only running it says that.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import a_broker
from mftik.protocol import (
    MD_HEALTH,
    STS_HEALTH,
    TD_HEALTH,
    HealthCheck,
    HealthCheckEnvelope,
    HealthStatus,
    Topics,
)

#: The planes with a named subject, and how to ask each one whether it is
#: listening. Health is the probe that needs no session manager — every other
#: request type would fail on the ``None`` passed below for reasons that have
#: nothing to do with which subject was served.
PLANES = [
    pytest.param("td", TD_HEALTH, Topics.td, id="td"),
    pytest.param("md", MD_HEALTH, Topics.md, id="md"),
    pytest.param("sts", STS_HEALTH, Topics.sts, id="sts"),
]


def _run_rpc(plane: str):
    if plane == "td":
        from mftik_td import app

        return app.run_rpc
    if plane == "md":
        from mftik_md import app

        return app.run_rpc
    from mftik_sts import app

    return app.run_rpc


@pytest.mark.asyncio
@pytest.mark.parametrize(("plane", "health_type", "named"), PLANES)
async def test_a_plane_answers_on_the_subject_it_was_given(
    plane: str, health_type: str, named
) -> None:
    """The named subject, which is the one a pinned deploy addresses."""
    subject = named(f"{plane}-jp-1")
    async with a_broker(f"subj-{plane}") as broker:
        stop = asyncio.Event()
        task = asyncio.create_task(
            _run_rpc(plane)(broker, None, stop, subject=subject)  # type: ignore[arg-type]
        )
        await asyncio.sleep(0.05)
        try:
            reply = await broker.request(
                subject,
                HealthCheckEnvelope.wrap(
                    HealthCheck(), type=health_type, source="test"
                ),
                timeout=2.0,
            )
        finally:
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        assert HealthStatus.model_validate(reply.payload).status == "ok"


@pytest.mark.asyncio
@pytest.mark.parametrize(("plane", "health_type", "named"), PLANES)
async def test_a_plane_does_not_answer_on_a_subject_it_was_not_given(
    plane: str, health_type: str, named
) -> None:
    """The half that actually failed.

    Serving a constant looks identical to serving the argument until something
    asks on a *different* subject — which is why the bug survived a green
    suite and reached a running stack.
    """
    async with a_broker(f"subj-{plane}-neg") as broker:
        stop = asyncio.Event()
        task = asyncio.create_task(
            _run_rpc(plane)(
                broker,
                None,  # type: ignore[arg-type]
                stop,
                subject=named(f"{plane}-jp-1"),
            )
        )
        await asyncio.sleep(0.05)
        try:
            with pytest.raises(Exception):
                await broker.request(
                    named(f"{plane}-somewhere-else"),
                    HealthCheckEnvelope.wrap(
                        HealthCheck(), type=health_type, source="test"
                    ),
                    timeout=0.2,
                )
        finally:
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
