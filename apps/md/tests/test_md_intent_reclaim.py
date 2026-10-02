"""One real subject: a put, then two STS reports that omit the owner.

The report period is five seconds. These reports are published, not
waited for.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.intent_gc import watch_sts_reports
from mftik.protocol import (
    MD_INTENT_PUT,
    PROCMAN_REPORT,
    IntentOwner,
    MdIntentPutResult,
    ProcmanReport,
    ProcmanReportEnvelope,
    ProcmanWorker,
    Topics,
    UntypedEnvelope,
)
from mftik_md.app import run_rpc
from mftik_md.intents import MdIntentBook

pytestmark = pytest.mark.integration


def _put_envelope() -> UntypedEnvelope:
    return UntypedEnvelope.wrap(
        {
            "session_id": "abc",
            "owner": {"sts_instance": "sts", "session_id": "abc"},
            "feeds": ["book"],
        },
        type=MD_INTENT_PUT,
        source="api",
        session_id="abc",
    )


def _report(generation: int) -> ProcmanReportEnvelope:
    return ProcmanReportEnvelope.wrap(
        ProcmanReport(
            generation=generation,
            workers=[
                ProcmanWorker(
                    id="sts/session/other",
                    code_ref="v1",
                    rss_bytes=None,
                    phase="running",
                    ready=True,
                    incarnation=1,
                )
            ],
        ),
        type=PROCMAN_REPORT,
        source="sts",
    )


async def _until(predicate, timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("timed out")


@pytest.mark.real_sleep(
    reason="NATS subscription has no ready event; the report itself is published"
)
async def test_two_reports_that_omit_the_owner_release_the_intent() -> None:
    owner = IntentOwner(sts_instance="sts", session_id="abc")
    book = MdIntentBook()
    stop = asyncio.Event()
    async with a_broker() as broker:
        broker: Broker
        rpc = asyncio.create_task(
            run_rpc(broker, None, stop, subject=Topics.md("md"), intents=book)
        )
        watch = asyncio.create_task(
            watch_sts_reports(
                broker,
                held=book.owners,
                release=book.release_owners,
                stop=stop,
                states=book.gc_states,
            )
        )
        try:
            await asyncio.sleep(0.05)
            reply = await broker.request(
                Topics.md("md"), _put_envelope(), timeout=2
            )
            assert MdIntentPutResult.model_validate(reply.payload).atoms == {}
            assert reply.session_id == "abc"
            assert owner in book.owners()
            await broker.publish(Topics.procman_report("sts", "sts"), _report(1))
            await _until(lambda: book.gc_states.get("sts") is not None)
            assert owner in book.owners()
            await broker.publish(Topics.procman_report("sts", "sts"), _report(2))
            await _until(lambda: owner not in book.owners())
        finally:
            stop.set()
            rpc.cancel()
            watch.cancel()
            await asyncio.gather(rpc, watch, return_exceptions=True)
