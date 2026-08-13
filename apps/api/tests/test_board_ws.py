"""``/ws/board`` — getting a live execution onto the right row.

Fills arrive on a channel that names an *account*. The board lists
*sessions*. That mapping is the part worth testing, and it is worth testing
because getting it wrong is invisible: a misattributed fill does not raise, it
climbs somebody else's count and looks like ordinary trading.

The socket plumbing around it is the same shape as ``sts_status_bridge`` and is
not re-tested here; what is specific to this bridge is the rule.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from decimal import Decimal

import pytest
from mft_api import ws as ws_module
from mft_db.models import Base
from mft_db.models.history import Attribution, Source
from mft_db.repositories import OrderRepository
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

API_ID = 7
CID = "281474976710656001"
TICKER = "Binance_Spot_BTCUSDT"


@pytest.fixture
async def db(monkeypatch):
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)

    @asynccontextmanager
    async def scope():
        async with maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    monkeypatch.setattr(ws_module, "session_scope", scope)
    yield scope
    await engine.dispose()


async def seed_order(
    scope, *, session_id: str | None, key: str = CID, api_id: int = API_ID
) -> None:
    async with scope() as session:
        await OrderRepository(session).bulk_upsert(
            [
                {
                    "api_id": api_id,
                    "order_key": key,
                    "client_order_id": key,
                    "venue_order_id": "500",
                    "session_id": session_id,
                    "strategy": None,
                    "cid_slot": None,
                    "attribution": (
                        Attribution.DIRECT if session_id else Attribution.EXTERNAL
                    ),
                    "universal_ticker": TICKER,
                    "side": "buy",
                    "order_type": "limit",
                    "status": "new",
                    "qty": Decimal("1"),
                    "price": Decimal("100"),
                    "filled_qty": Decimal("0"),
                    "avg_price": None,
                    "submitted_at": 1000.0,
                    "ts": 1000.0,
                    "source": Source.STREAM,
                }
            ]
        )


# --- attribution -----------------------------------------------------------


async def test_a_fill_reaches_the_session_that_placed_the_order(db) -> None:
    await seed_order(db, session_id="sess-1")

    assert await ws_module.session_of(API_ID, CID) == "sess-1"


async def test_an_order_we_never_placed_belongs_to_no_session(db) -> None:
    """Real trading on an account we hold, and none of it a session's."""
    await seed_order(db, session_id=None)

    assert await ws_module.session_of(API_ID, CID) is None


async def test_an_order_not_on_file_belongs_to_no_session(db) -> None:
    assert await ws_module.session_of(API_ID, "never-seen") is None


async def test_a_fill_with_no_client_order_id_is_not_guessed_at(db) -> None:
    await seed_order(db, session_id="sess-1")

    assert await ws_module.session_of(API_ID, None) is None
    assert await ws_module.session_of(API_ID, "") is None


async def test_one_accounts_order_does_not_answer_for_another(db) -> None:
    """Client order ids are only unique within an account."""
    await seed_order(db, session_id="sess-1", api_id=API_ID)

    assert await ws_module.session_of(99, CID) is None


async def test_an_unreachable_database_drops_the_event(db, monkeypatch) -> None:
    """A live counter is a convenience; it must not take the socket down."""

    @asynccontextmanager
    async def broken():
        raise RuntimeError("database is down")
        yield  # pragma: no cover

    monkeypatch.setattr(ws_module, "session_scope", broken)

    assert await ws_module.session_of(API_ID, CID) is None


# --- routing ---------------------------------------------------------------


def test_the_account_is_read_off_the_channel_name() -> None:
    assert ws_module._api_id_of("td.42.global") == 42
    assert ws_module._api_id_of("td.notanumber.global") == 0
    assert ws_module._api_id_of("log.td.42") == 0
    assert ws_module._api_id_of("td.42") == 0
