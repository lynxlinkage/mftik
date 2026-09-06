"""PI-1 and PI-3 — a session's feeds may be split, and each goes where it says.

Two questions this file keeps apart. *Which MD serves a feed* is the one the
document answers, and the one PI-3 is about. *Whether that MD is there at all*
is PI-2, and it has to be settled before any plane is asked to do anything —
because failing at attach time hands the operator a lease timeout where they
should have had a sentence naming what is wrong.
"""

from __future__ import annotations

import pytest
from broker_harness import a_broker
from db_harness import a_database, an_instance, an_owner
from mftik.broker import Broker
from mftik.broker.errors import RequestTimeoutError
from mftik.protocol import (
    ANY_INSTANCE,
    MD_SESSION_ATTACH,
    MD_SESSION_DETACH,
    STS_SESSION_CREATE,
    STS_SESSION_FAIL,
    MdAttachResult,
    MdAttachResultEnvelope,
    StsCreateSessionResult,
    StsCreateSessionResultEnvelope,
    StsSessionControlResult,
    StsSessionControlResultEnvelope,
    Topics,
)
from mftik_api import orchestrate
from mftik_api.broker_rpc import DomainRpcError
from mftik_api.orchestrate import deploy_strategy

JP1 = "md-jp-1"
JP2 = "md-jp-2"
FEED_A = "bestquote.Paper_Spot_BTCUSDT"
FEED_B = "aggtrade.Paper_Spot_ETHUSDT"


class Recording:
    """Answers STS and MD, and remembers which subject each went to."""

    def __init__(self, *, up: set[str], fail_after: int | None = None) -> None:
        self._up = up
        self._fail_after = fail_after
        self.attaches: list[tuple[str, list[str]]] = []
        self.detaches: list[str] = []
        self.failed: str | None = None
        self.created_on: str | None = None
        self.create = None

    async def probe(self, subject, envelope, *, timeout=None):
        instance = subject.rsplit(".", 1)[-1]
        if instance not in self._up:
            raise RequestTimeoutError(subject, envelope.id, timeout or 0)
        return envelope

    async def post(self, subject, envelope):
        if envelope.type == MD_SESSION_DETACH:
            self.detaches.append(subject)

    async def publish_log(self, topic, envelope):
        return 1

    async def publish(self, topic, envelope):
        return 1

    async def request(self, subject, envelope, *, timeout=None):
        if envelope.type == STS_SESSION_CREATE:
            self.created_on = subject
            self.create = envelope.payload
            return StsCreateSessionResultEnvelope.wrap(
                StsCreateSessionResult(
                    session_id=envelope.payload.session_id,
                    strategy="tiny",
                    status="live",
                ),
                type=STS_SESSION_CREATE,
                source="sts",
            )
        if envelope.type == MD_SESSION_ATTACH:
            feeds = list(envelope.payload.subscriptions)
            self.attaches.append((subject, feeds))
            if (
                self._fail_after is not None
                and len(self.attaches) > self._fail_after
            ):
                raise DomainRpcError("attach_failed", "venue would not open")
            return MdAttachResultEnvelope.wrap(
                MdAttachResult(
                    session_id=envelope.payload.session_id,
                    subscriptions=feeds,
                    refcounts={f: 1 for f in feeds},
                ),
                type=MD_SESSION_ATTACH,
                source="md",
            )
        if envelope.type == STS_SESSION_FAIL:
            self.failed = envelope.payload.reason
            return StsSessionControlResultEnvelope.wrap(
                StsSessionControlResult(
                    session_id=envelope.payload.session_id, status="failed"
                ),
                type=STS_SESSION_FAIL,
                source="sts",
            )
        raise AssertionError(f"unexpected rpc: {envelope.type}")


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        async with database.maker() as session:
            await an_owner(session)
            await an_instance(session, JP1, "md")
            await an_instance(session, JP2, "md")
            await an_instance(session, "sts-tw", "sts")
            await session.commit()
        monkeypatch.setattr(orchestrate, "session_scope", database.scope)
        yield database.scope


async def test_each_instance_is_attached_to_its_own_feeds(db) -> None:
    """PI-3. Two attaches, each carrying only what its instance was given."""
    broker = Recording(up={JP1, JP2})

    await deploy_strategy(
        broker,
        strategy_id="tiny",
        md={JP1: [FEED_A], JP2: [FEED_B]},
        created_by=1,
    )

    assert broker.attaches == [
        (Topics.md(JP1), [FEED_A]),
        (Topics.md(JP2), [FEED_B]),
    ]


async def test_an_unpinned_deploy_still_uses_the_shared_pool(db) -> None:
    """PI-5. A document that names nothing behaves exactly as it did."""
    broker = Recording(up=set())

    await deploy_strategy(
        broker,
        strategy_id="tiny",
        md={ANY_INSTANCE: [FEED_A, FEED_B]},
        created_by=1,
    )

    assert broker.attaches == [(Topics.MD, [FEED_A, FEED_B])]


async def test_a_plain_list_is_read_as_unpinned(db) -> None:
    """The shape every document written before instances existed uses."""
    broker = Recording(up=set())

    await deploy_strategy(
        broker, strategy_id="tiny", md=[FEED_A], created_by=1
    )

    assert broker.attaches == [(Topics.MD, [FEED_A])]


async def test_a_name_nothing_declared_is_refused_before_any_attach(
    db,
) -> None:
    """PI-2, first half: a typo in the document."""
    broker = Recording(up={JP1})

    with pytest.raises(DomainRpcError) as refused:
        await deploy_strategy(
            broker,
            strategy_id="tiny",
            md={"md-jp-9": [FEED_A]},
            created_by=1,
        )

    assert refused.value.code == "unknown_instance"
    assert "md-jp-9" in refused.value.message
    assert broker.attaches == [], "nothing was asked to do anything"


async def test_a_declared_name_that_is_silent_is_refused_differently(
    db,
) -> None:
    """PI-2, second half: a machine to go and look at.

    The two refusals must not be one message. An undeclared name is fixed in
    the document; a declared name that does not answer is fixed by deploying
    something. Collapsing them tells the operator neither.
    """
    broker = Recording(up=set())

    with pytest.raises(DomainRpcError) as refused:
        await deploy_strategy(
            broker, strategy_id="tiny", md={JP1: [FEED_A]}, created_by=1
        )

    assert refused.value.code == "instance_down"
    assert JP1 in refused.value.message
    assert broker.attaches == []


async def test_a_disabled_instance_is_refused_but_not_evicted(db) -> None:
    """`enabled=false` drains: new deploys refuse it, live ones carry on."""
    from mftik_db.repositories import InstanceRepository

    async with db() as session:
        repo = InstanceRepository(session)
        row = await repo.get_by_name(JP1)
        await repo.update(row, enabled=False)
        await session.commit()

    broker = Recording(up={JP1})
    with pytest.raises(DomainRpcError) as refused:
        await deploy_strategy(
            broker, strategy_id="tiny", md={JP1: [FEED_A]}, created_by=1
        )

    assert refused.value.code == "instance_disabled"


async def test_a_failure_partway_unwinds_the_attaches_that_landed(db) -> None:
    """The rollback the fan-out made necessary.

    With one attach there was nothing to unwind. With three, a failure on the
    second leaves the first pumping feeds for a session that is about to be
    failed — until a reaper noticed, two scans and up to a minute later.
    """
    broker = Recording(up={JP1, JP2}, fail_after=1)

    with pytest.raises(DomainRpcError):
        await deploy_strategy(
            broker,
            strategy_id="tiny",
            md={JP1: [FEED_A], JP2: [FEED_B]},
            created_by=1,
        )

    assert broker.detaches == [Topics.md(JP1)], (
        "the attach that landed was rolled back"
    )
    assert broker.failed is not None, "and the session was failed"


async def test_a_deploy_may_name_the_sts_that_runs_it(db) -> None:
    """And the row records what was *asked for*, not where it landed."""
    broker = Recording(up={"sts-tw"})

    await deploy_strategy(
        broker, strategy_id="tiny", md={}, created_by=1, instance="sts-tw"
    )

    assert broker.created_on == Topics.sts("sts-tw")
    assert broker.create.instance == "sts-tw"


async def test_an_unpinned_deploy_goes_to_the_sts_pool(db) -> None:
    broker = Recording(up=set())

    await deploy_strategy(broker, strategy_id="tiny", md={}, created_by=1)

    assert broker.created_on == Topics.STS
    assert broker.create.instance is None, (
        "null means the deploy did not care, and anyone may rebuild it"
    )


async def test_an_sts_that_does_not_answer_is_refused_before_creating(
    db,
) -> None:
    broker = Recording(up=set())

    with pytest.raises(DomainRpcError) as refused:
        await deploy_strategy(
            broker,
            strategy_id="tiny",
            md={},
            created_by=1,
            instance="sts-tw",
        )

    assert refused.value.code == "instance_down"
    assert broker.created_on is None, "nothing was created"


async def test_naming_an_md_instance_for_sts_is_refused(db) -> None:
    """The domain is checked, not just the name.

    ``md-jp-1`` is a declared instance and answering; it is simply not an STS.
    Sending a session create to it would time out with nothing to say.
    """
    broker = Recording(up={JP1})

    with pytest.raises(DomainRpcError) as refused:
        await deploy_strategy(
            broker, strategy_id="tiny", md={}, created_by=1, instance=JP1
        )

    assert refused.value.code == "unknown_instance"
