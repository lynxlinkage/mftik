"""The strategy-side ledger — a read of TD memory over ``td.account``.

Every assertion here goes through the same path a strategy does: serve the
book the way TD does, then read it back through ``StrategyLedger``.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest
from broker_harness import a_broker
from mftik.broker import Broker, RequestTimeoutError
from mftik.exchange.models import Balance
from mftik.exchange.oms import LedgerView
from mftik.protocol import (
    TD_LEDGER_VIEW,
    Envelope,
    TdLedgerViewRequest,
    Topics,
)
from mftik.strategy.ledger import StrategyLedger

API_ID = 7


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


def _ledger(broker: Broker, *api_ids: int) -> StrategyLedger:
    ledger = StrategyLedger()

    def td_sole() -> int:
        ids = list(api_ids)
        if len(ids) != 1:
            raise RuntimeError(f"needs exactly one td account, got {ids}")
        return ids[0]

    session = SimpleNamespace(
        broker=broker,
        td_api_ids=list(api_ids),
        td_sole=td_sole,
        session_id="s-ledger",
        strategy=SimpleNamespace(name="quiet"),
    )
    ledger.bind(SimpleNamespace(session=session))
    return ledger


class _Book:
    def __init__(self, broker: Broker) -> None:
        self._broker = broker
        self._books: dict[int, dict[str, Balance]] = {}
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    def write(self, api_id: int, asset: str, free: str, prelock: str = "0") -> None:
        book = self._books.setdefault(api_id, {})
        book[asset] = Balance(
            asset=asset,
            free=Decimal(free),
            locked=Decimal("0"),
            prelock=Decimal(prelock),
        )

    def clear(self, api_id: int) -> None:
        self._books.pop(api_id, None)

    async def start(self, *api_ids: int) -> None:
        for api_id in api_ids:
            self._tasks.append(
                asyncio.create_task(self._serve(api_id), name=f"book-{api_id}")
            )
            await asyncio.sleep(0)

    async def close(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _serve(self, api_id: int) -> None:
        async for req in self._broker.serve(
            Topics.td_account(api_id), stop=self._stop
        ):
            payload = TdLedgerViewRequest.model_validate(req.envelope.payload or {})
            balances = dict(self._books.get(api_id, {}))
            if payload.asset is not None:
                bal = balances.get(payload.asset)
                balances = {} if bal is None else {payload.asset: bal}
            await req.reply(
                Envelope[LedgerView].wrap(
                    LedgerView(api_id=api_id, balances=balances),
                    type=TD_LEDGER_VIEW,
                    source="td",
                )
            )


@pytest.mark.asyncio
async def test_nothing_written_yet_reads_as_zero(broker: Broker) -> None:
    book = _Book(broker)
    await book.start(API_ID)
    try:
        ledger = _ledger(broker, API_ID)
        assert await ledger.available("USDT") == Decimal("0")
        assert await ledger.balances() == {}
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_available_subtracts_the_prelock(broker: Broker) -> None:
    book = _Book(broker)
    book.write(API_ID, "USDT", "1000", "400")
    await book.start(API_ID)
    try:
        ledger = _ledger(broker, API_ID)
        assert await ledger.free("USDT") == Decimal("1000")
        assert await ledger.prelock("USDT") == Decimal("400")
        assert await ledger.available("USDT") == Decimal("600")
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_a_single_account_needs_no_api_id(broker: Broker) -> None:
    book = _Book(broker)
    book.write(API_ID, "USDT", "1000")
    await book.start(API_ID)
    try:
        ledger = _ledger(broker, API_ID)
        assert await ledger.available("USDT") == Decimal("1000")
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_multiple_accounts_must_be_named(broker: Broker) -> None:
    book = _Book(broker)
    book.write(7, "USDT", "1000")
    book.write(8, "USDT", "50")
    await book.start(7, 8)
    try:
        ledger = _ledger(broker, 7, 8)
        assert await ledger.available("USDT") == Decimal("0")
        assert await ledger.available("USDT", 7) == Decimal("1000")
        assert await ledger.available("USDT", 8) == Decimal("50")
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_an_unknown_asset_reads_as_zero(broker: Broker) -> None:
    book = _Book(broker)
    book.write(API_ID, "USDT", "1000")
    await book.start(API_ID)
    try:
        ledger = _ledger(broker, API_ID)
        assert await ledger.available("DOGE") == Decimal("0")
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_a_later_write_is_what_the_strategy_sees(broker: Broker) -> None:
    book = _Book(broker)
    book.write(API_ID, "USDT", "1000")
    await book.start(API_ID)
    try:
        ledger = _ledger(broker, API_ID)
        assert await ledger.available("USDT") == Decimal("1000")
        book.write(API_ID, "USDT", "900", "100")
        assert await ledger.available("USDT") == Decimal("800")
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_td_down_fails_closed(broker: Broker) -> None:
    ledger = _ledger(broker, API_ID)
    with pytest.raises(RequestTimeoutError):
        await ledger.available("USDT")
