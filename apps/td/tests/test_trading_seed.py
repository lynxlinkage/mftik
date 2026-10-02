"""A TD restart must not turn a live trading layer off (P5).

The account worker outlives the controller. ``TdIntentBook`` does not.
Until unreleased ``td_intents`` have been read, reconcile publishes
nothing, so a reattached worker never sees ``active=false`` from the
empty book.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from db_harness import a_database, an_instance, an_owner
from mftik.procman import ObservedWorker, RestartIntensity, Supervisor
from mftik.protocol import IntentOwner, TdIntentPut
from mftik_db.models.api import Api
from mftik_db.models.intent import TdIntent
from mftik_db.models.session import StsSessionRow
from mftik_td import app
from mftik_td.account import AccountWorker
from mftik_td.controller import (
    AccountView,
    BoundAccount,
    TdIntentBook,
    TdOrchestrator,
    intent_book,
)
from mftik_td.db import install_intent_seed, read_held_intents, seed_intent_book
from mftik_td.oms import Ledger, Oms
from mftik_td.supervise import apply_reconcile

INSTANCE = "td"


class _Session:
    def __init__(self) -> None:
        self.oms = Oms()
        self.ledger = Ledger()
        self.private = object()
        self.destroyed = False

    async def start(self) -> None:
        return None

    async def destroy(self) -> None:
        self.destroyed = True


class _Broker:
    """The account subject, in process. Records every desired bit."""

    def __init__(self, worker: AccountWorker) -> None:
        self.worker = worker
        self.sent: list[bool] = []

    async def request(self, subject: str, envelope, timeout: float):
        del subject, timeout
        self.sent.append(envelope.payload.active)
        return await self.worker.trading.handle(envelope)


class _NoProcess:
    async def spawn(self, spec) -> None:
        raise AssertionError(f"spawn {spec}")

    async def stop(self, worker_id: str) -> None:
        raise AssertionError(f"stop {worker_id}")

    async def release_slot(self, worker_id: str) -> None:
        raise AssertionError(f"release {worker_id}")


def _orch(tmp_path: Path) -> TdOrchestrator:
    return TdOrchestrator(
        Supervisor(tmp_path, plane="td", instance=INSTANCE),
        intensity=RestartIntensity(max_restarts=2, window_s=30, min_backoff_s=0.5),
        code_ref="v1",
    )


def _view(api_id: int) -> AccountView:
    return AccountView(
        api_id=api_id,
        observed=ObservedWorker.RUNNING,
        pid_gone=False,
        incarnation=1,
    )


def _put(session_id: str, *api_ids: int) -> TdIntentPut:
    return TdIntentPut(
        session_id=session_id,
        owner=IntentOwner(sts_instance="sts", session_id=session_id),
        api_ids=list(api_ids),
    )


@pytest.fixture
async def scope():
    async with a_database() as database:
        yield database.scope


async def _rows(
    scope,
    *,
    hold: bool,
    sts_instance: str | None = "sts",
) -> int:
    """One Bybit account on this TD, plus a released row and another instance.

    Returns the home account's ``api_id``. ``hold`` false releases the
    home row. ``sts_instance`` is written on the live session; ``None``
    is a row the seed cannot name.
    """
    async with scope() as session:
        await an_owner(session)
        home = await an_instance(session, name=INSTANCE, domain="td")
        away = await an_instance(session, name="td-jp", domain="td")
        mine = Api(
            owner_id=1,
            venue="Bybit",
            api_key="seed-home",
            api_secret="secret",
            instance_id=home.id,
        )
        other = Api(
            owner_id=1,
            venue="Bybit",
            api_key="seed-away",
            api_secret="secret",
            instance_id=away.id,
        )
        session.add_all((mine, other))
        await session.flush()
        session.add(
            StsSessionRow(
                session_id="sess-live",
                created_by=1,
                instance=sts_instance,
                td={},
                md_ids={},
                st_paras={},
                st_facts={},
            )
        )
        session.add(
            StsSessionRow(
                session_id="sess-old",
                created_by=1,
                instance="sts",
                td={},
                md_ids={},
                st_paras={},
                st_facts={},
            )
        )
        released = None if hold else datetime.now(UTC)
        session.add(
            TdIntent(session_id="sess-live", api_id=mine.id, released_at=released)
        )
        session.add(
            TdIntent(
                session_id="sess-old",
                api_id=mine.id,
                released_at=datetime.now(UTC),
            )
        )
        session.add(TdIntent(session_id="sess-live", api_id=other.id))
        return mine.id


async def _live_worker(api_id: int) -> tuple[AccountWorker, _Session]:
    """A reattached worker whose trading layer is already on."""
    session = _Session()
    worker = AccountWorker(api_id, venue="Bybit", session=session)  # type: ignore[arg-type]
    await worker.trading.activate()
    assert worker.trading.active is True
    return worker, session


async def _apply(
    orch,
    _worker,
    book,
    api_id: int,
    *,
    publish: bool,
    broker: _Broker,
) -> None:
    account = BoundAccount(api_id=api_id, venue="Bybit", instance=INSTANCE)
    actions = orch.reconcile(
        (account,), book.rows(), (_view(api_id),), publish=publish
    )
    await apply_reconcile(
        _NoProcess(),
        actions,
        (account,),
        code_ref="v1",
        cancel_on_disconnect={},
        broker=broker,
    )


def test_a_seed_keeps_a_put_that_arrived_while_the_read_was_in_flight() -> None:
    book = TdIntentBook()
    book.put(_put("live", 7))
    install_intent_seed(book, (_put("live", 9), _put("from-db", 8)))
    rows = {row.session_id: list(row.api_ids) for row in book.rows()}
    assert rows == {"live": [7], "from-db": [8]}


async def test_a_failed_seed_does_not_clear_the_book() -> None:
    book = TdIntentBook()
    book.put(_put("live", 7))

    async def boom(instance: str) -> tuple[TdIntentPut, ...]:
        del instance
        raise RuntimeError("db down")

    assert await seed_intent_book(book, instance=INSTANCE, read=boom) is False
    assert [row.session_id for row in book.rows()] == ["live"]


async def test_the_publish_gate_stays_shut_until_seeding_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    book = intent_book()
    book.clear()
    calls = 0

    async def seed(target, *, instance: str, read=None) -> bool:
        nonlocal calls
        del read
        calls += 1
        assert target is book
        assert instance == app.INSTANCE
        return calls >= 2

    monkeypatch.setattr(app.td_db, "seed_intent_book", seed)
    try:
        assert await app._held_set_ready(False) is False
        assert await app._held_set_ready(False) is True
        assert await app._held_set_ready(True) is True
        assert calls == 2
    finally:
        book.clear()


async def test_reconcile_once_forwards_the_publish_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[bool] = []

    class _Orch:
        code_ref = "v1"

        def reconcile(self, accounts, intents, views, *, publish: bool = True):
            del accounts, intents, views
            seen.append(publish)
            return ()

    async def load(instance: str):
        assert instance == app.INSTANCE
        return (), {}

    async def views(*args, **kwargs):
        del args, kwargs
        return ()

    async def apply(*args, **kwargs):
        del args, kwargs
        return None

    monkeypatch.setattr(app, "load_accounts", load)
    monkeypatch.setattr(app, "account_views", views)
    monkeypatch.setattr(app, "apply_reconcile", apply)
    await app._reconcile_once(None, _Orch(), (), None, publish=False)  # type: ignore[arg-type]
    await app._reconcile_once(None, _Orch(), (), None, publish=True)  # type: ignore[arg-type]
    assert seen == [False, True]


@pytest.mark.component
async def test_a_restart_with_a_held_intent_never_pushes_false(
    tmp_path: Path, scope
) -> None:
    """The reattached layer stays on. The empty book is not published."""
    api_id = await _rows(scope, hold=True)
    worker, session = await _live_worker(api_id)
    broker = _Broker(worker)
    book = TdIntentBook()
    orch = _orch(tmp_path)
    reads = 0

    async def read(instance: str):
        nonlocal reads
        reads += 1
        return await read_held_intents(instance, scope=scope)

    seeded = False
    for _ in range(2):
        if not seeded:
            seeded = await seed_intent_book(book, instance=INSTANCE, read=read)
        await _apply(orch, worker, book, api_id, publish=seeded, broker=broker)

    assert seeded is True
    assert reads == 1
    assert broker.sent == [True, True]
    assert worker.trading.active is True
    assert session.destroyed is False
    assert [row.session_id for row in book.rows()] == ["sess-live"]
    assert list(book.rows()[0].api_ids) == [api_id]
    assert book.rows()[0].owner.sts_instance == "sts"


@pytest.mark.component
async def test_a_restart_with_nothing_unreleased_pushes_false_after_the_seed(
    tmp_path: Path, scope
) -> None:
    """False is the seeded answer, not the answer of an empty memory book."""
    api_id = await _rows(scope, hold=False)
    worker, session = await _live_worker(api_id)
    broker = _Broker(worker)
    book = TdIntentBook()
    orch = _orch(tmp_path)

    await _apply(orch, worker, book, api_id, publish=False, broker=broker)
    assert broker.sent == []
    assert worker.trading.active is True

    seeded = await seed_intent_book(
        book,
        instance=INSTANCE,
        read=lambda instance: read_held_intents(instance, scope=scope),
    )
    assert seeded is True
    assert book.rows() == ()
    await _apply(orch, worker, book, api_id, publish=seeded, broker=broker)

    assert broker.sent == [False]
    assert worker.trading.active is False
    assert session.destroyed is True


@pytest.mark.component
async def test_a_failed_seed_read_pushes_nothing(tmp_path: Path) -> None:
    worker, session = await _live_worker(7)
    broker = _Broker(worker)
    book = TdIntentBook()
    orch = _orch(tmp_path)

    async def boom(instance: str) -> tuple[TdIntentPut, ...]:
        del instance
        raise RuntimeError("seed read failed")

    seeded = await seed_intent_book(book, instance=INSTANCE, read=boom)
    assert seeded is False
    await _apply(orch, worker, book, 7, publish=seeded, broker=broker)

    assert broker.sent == []
    assert worker.trading.active is True
    assert session.destroyed is False
    assert book.rows() == ()


@pytest.mark.component
async def test_a_seed_that_cannot_name_the_sts_instance_pushes_nothing(
    tmp_path: Path, scope
) -> None:
    """Dropping the row would look like no intent and push false."""
    api_id = await _rows(scope, hold=True, sts_instance=None)
    worker, session = await _live_worker(api_id)
    broker = _Broker(worker)
    book = TdIntentBook()

    seeded = await seed_intent_book(
        book,
        instance=INSTANCE,
        read=lambda instance: read_held_intents(instance, scope=scope),
    )
    assert seeded is False
    assert book.rows() == ()
    await _apply(_orch(tmp_path), worker, book, api_id, publish=seeded, broker=broker)
    assert broker.sent == []
    assert worker.trading.active is True
    assert session.destroyed is False
