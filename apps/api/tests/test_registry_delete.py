"""DELETE /registry/v1/strategies/{name}, and what add/delete say about STS.

Writing files is only half of either operation. STS imports the registry into
a running process, so an add nobody told it about is not deployable and a
delete nobody told it about is still deployable — and in both cases the HTTP
call succeeded. What these check is that the answer says which.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from fastapi import HTTPException
from mftik.protocol import (
    STS_REGISTRY_RELOAD,
    StsRegistryReloadResult,
    StsRegistryReloadResultEnvelope,
)
from mftik.registry import RegistryStore, qualify
from mftik_api.broker_rpc import DomainRpcError
from mftik_api.routes import registry as registry_routes
from mftik_api.routes.registry import add_strategy, delete_strategy
from mftik_api.schemas import RegistryAddBody

_TINY = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""


class HealthyBroker:
    """An STS that loads everything the store holds."""

    def __init__(self, store: RegistryStore) -> None:
        self.store = store
        self.calls = 0

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        assert envelope.type == STS_REGISTRY_RELOAD
        self.calls += 1
        return StsRegistryReloadResultEnvelope.wrap(
            StsRegistryReloadResult(
                loaded=[qualify(r.origin, r.type) for r in self.store.list_all()]
            ),
            type=STS_REGISTRY_RELOAD,
            source="sts",
        )


class SkippingBroker:
    """An STS that reloads and refuses the tree — a bad import, say."""

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        return StsRegistryReloadResultEnvelope.wrap(
            StsRegistryReloadResult(loaded=[]),
            type=STS_REGISTRY_RELOAD,
            source="sts",
        )


class StuckBroker:
    """An STS that already had the key and does not let go of it."""

    def __init__(self, keys: list[str]) -> None:
        self.keys = keys

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        return StsRegistryReloadResultEnvelope.wrap(
            StsRegistryReloadResult(loaded=self.keys),
            type=STS_REGISTRY_RELOAD,
            source="sts",
        )


class DeadBroker:
    """No STS answered."""

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        raise DomainRpcError(code="timeout", message="no reply from sts")


@dataclass
class _Row:
    type: str
    sts_session: str


def _live_sessions(monkeypatch: pytest.MonkeyPatch, rows: list[_Row]) -> None:
    class FakeRepo:
        def __init__(self, _db: object) -> None:
            pass

        async def list_live_for_origin(self, origin: str) -> list[_Row]:
            return [r for r in rows if r.type.startswith(f"{origin}::")]

    @asynccontextmanager
    async def scope() -> Any:
        yield object()

    monkeypatch.setattr(registry_routes, "session_scope", scope)
    monkeypatch.setattr(registry_routes, "StrategyRepository", FakeRepo)


async def _add(store: RegistryStore, origin: str = "private") -> None:
    await add_strategy(
        RegistryAddBody(files={"strategy.py": _TINY}, origin=origin),
        store=store,
        broker=HealthyBroker(store),
    )


# --- what add reports ------------------------------------------------------


async def test_add_reports_that_sts_picked_it_up(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    broker = HealthyBroker(store)

    out = await add_strategy(
        RegistryAddBody(files={"strategy.py": _TINY}), store=store, broker=broker
    )

    assert out.loaded is True
    assert out.load_error is None
    assert broker.calls == 1


async def test_add_says_so_when_sts_did_not_answer(tmp_path: Path) -> None:
    """The files are on disk, so this is a 200 that admits a problem.

    A 5xx would invite a retry of a write that already happened, and the
    second one would 409 on a conflict the caller did not create.
    """
    store = RegistryStore(tmp_path)

    out = await add_strategy(
        RegistryAddBody(files={"strategy.py": _TINY}), store=store, broker=DeadBroker()
    )

    assert out.name == "tiny"
    assert out.loaded is False
    assert "no reply from sts" in out.load_error
    assert "restarts" in out.load_error
    # The add itself stands.
    assert [r.name for r in store.list_private()] == ["tiny"]


async def test_add_says_so_when_sts_reloaded_and_skipped_it(
    tmp_path: Path,
) -> None:
    """Stored, reloaded, and still not deployable — the confusing case."""
    store = RegistryStore(tmp_path)

    out = await add_strategy(
        RegistryAddBody(files={"strategy.py": _TINY}),
        store=store,
        broker=SkippingBroker(),
    )

    assert out.loaded is False
    assert "private::Tiny" in out.load_error
    assert "STS log" in out.load_error


# --- delete ----------------------------------------------------------------


async def test_delete_removes_the_tree_and_reports_it_unloaded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _live_sessions(monkeypatch, [])
    store = RegistryStore(tmp_path)
    await _add(store)

    out = await delete_strategy(
        "tiny", store=store, broker=HealthyBroker(store), origin="private"
    )

    assert out.name == "tiny"
    assert out.type == "Tiny"
    assert out.unloaded is True
    assert out.unload_error is None
    assert store.list_private() == []


async def test_delete_picks_the_origin_it_was_given(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _live_sessions(monkeypatch, [])
    store = RegistryStore(tmp_path)
    await _add(store, origin="private")
    await _add(store, origin="public")

    await delete_strategy(
        "tiny", store=store, broker=HealthyBroker(store), origin="public"
    )

    assert store.list_public() == []
    assert [r.name for r in store.list_private()] == ["tiny"]


async def test_deleting_what_is_not_there_is_404(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _live_sessions(monkeypatch, [])
    store = RegistryStore(tmp_path)

    with pytest.raises(HTTPException) as caught:
        await delete_strategy(
            "tiny", store=store, broker=HealthyBroker(store), origin="private"
        )

    assert caught.value.status_code == 404


async def test_a_pulled_copy_is_not_deletable_here(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _live_sessions(monkeypatch, [])
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY}, origin="node1")

    with pytest.raises(HTTPException) as caught:
        await delete_strategy(
            "tiny", store=store, broker=HealthyBroker(store), origin="node1"
        )

    assert caught.value.status_code == 400
    assert "remotes" in str(caught.value.detail)
    assert [r.name for r in store.list_pulled()] == ["tiny"]


async def test_delete_refuses_while_a_session_is_running_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The session would survive it, which is the reason to refuse.

    Deleting a strategy is a decision that it should not be running. Finding
    out afterwards that it still is, from a process holding a class whose
    source is gone, is the wrong order to learn it in.
    """
    _live_sessions(monkeypatch, [_Row(type="private::Tiny", sts_session="sts-7")])
    store = RegistryStore(tmp_path)
    await _add(store)

    with pytest.raises(HTTPException) as caught:
        await delete_strategy(
            "tiny", store=store, broker=HealthyBroker(store), origin="private"
        )

    assert caught.value.status_code == 409
    assert "sts-7" in str(caught.value.detail)
    assert [r.name for r in store.list_private()] == ["tiny"]


async def test_another_strategys_session_does_not_block_the_delete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The guard is per strategy, not per origin."""
    _live_sessions(monkeypatch, [_Row(type="private::Other", sts_session="sts-9")])
    store = RegistryStore(tmp_path)
    await _add(store)

    out = await delete_strategy(
        "tiny", store=store, broker=HealthyBroker(store), origin="private"
    )

    assert out.name == "tiny"


async def test_delete_says_so_when_sts_still_answers_to_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _live_sessions(monkeypatch, [])
    store = RegistryStore(tmp_path)
    await _add(store)

    out = await delete_strategy(
        "tiny",
        store=store,
        broker=StuckBroker(["private::Tiny"]),
        origin="private",
    )

    assert out.unloaded is False
    assert "still answers" in out.unload_error
    # Deleted regardless — the files are this API's to remove, and it did.
    assert store.list_private() == []


async def test_delete_says_so_when_sts_did_not_answer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _live_sessions(monkeypatch, [])
    store = RegistryStore(tmp_path)
    await _add(store)

    out = await delete_strategy(
        "tiny", store=store, broker=DeadBroker(), origin="private"
    )

    assert out.unloaded is False
    assert "no reply from sts" in out.unload_error
    assert store.list_private() == []
