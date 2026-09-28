"""A rebuild attach that MD refuses is not retried."""

from __future__ import annotations

import pytest
from mftik.protocol import MD_ERROR, RpcError, RpcErrorEnvelope
from mftik_sts.session import manager as manager_mod
from mftik_sts.session.manager import AttachRefused, SessionManager


class _Broker:
    def __init__(self, code: str) -> None:
        self.code = code
        self.calls = 0

    async def request(self, subject: str, envelope: object, timeout: float) -> object:
        self.calls += 1
        return RpcErrorEnvelope.wrap(
            RpcError(code=self.code, message="nope"),
            type=MD_ERROR,
            source="md",
        )


@pytest.mark.asyncio
async def test_a_refused_attach_is_not_retried() -> None:
    broker = _Broker("VENUE_SYMBOL_NOT_FOUND")
    sessions = SessionManager(broker)  # type: ignore[arg-type]
    with pytest.raises(AttachRefused, match="VENUE_SYMBOL_NOT_FOUND"):
        await sessions._attach_with_retry(  # noqa: SLF001
            what="md",
            subject="md.attach",
            envelope=object(),
            error_type=MD_ERROR,
        )
    assert broker.calls == 1


@pytest.mark.asyncio
async def test_a_generic_refusal_is_not_retried() -> None:
    broker = _Broker("nope")
    sessions = SessionManager(broker)  # type: ignore[arg-type]
    with pytest.raises(AttachRefused, match="nope"):
        await sessions._attach_with_retry(  # noqa: SLF001
            what="td",
            subject="td.attach",
            envelope=object(),
            error_type=MD_ERROR,
        )
    assert broker.calls == 1


@pytest.mark.asyncio
async def test_an_unavailable_attach_is_retried_until_the_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(manager_mod, "_ATTACH_BACKOFF_S", 0.01)
    monkeypatch.setattr(manager_mod, "_ATTACH_BUDGET_S", 0.05)
    broker = _Broker("unavailable")
    sessions = SessionManager(broker)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="rebuild could not attach"):
        await sessions._attach_with_retry(  # noqa: SLF001
            what="md",
            subject="md.attach",
            envelope=object(),
            error_type=MD_ERROR,
        )
    assert broker.calls > 1
