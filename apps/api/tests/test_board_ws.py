"""``/ws/board`` — getting a live execution onto the right row.

Fills arrive on a channel that names an *account*. The board lists
*sessions*. That mapping is the part worth testing, and it is worth testing
because getting it wrong is invisible: a misattributed fill does not raise, it
climbs somebody else's count and looks like ordinary trading.

The socket plumbing around it is the same shape as ``sts_status_bridge`` and is
not re-tested here; what is specific to this bridge is the rule.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from db_harness import a_database, an_owner
from mftik.protocol import STS_SESSION_STATUS
from mftik_api import log_persist
from mftik_api import ws as ws_module
from mftik_db.models.history import Attribution, Source
from mftik_db.models.session import SessionStatus, StsSessionRow
from mftik_db.repositories import OrderRepository, SessionLogRepository

API_ID = 7
CID = "281474976710656001"
TICKER = "Binance_Spot_BTCUSDT"


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        monkeypatch.setattr(ws_module, "session_scope", database.scope)
        yield database.scope


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


# --- late replay -----------------------------------------------------------


async def test_session_log_replay_reads_postgres(db) -> None:
    async with db() as session:
        await SessionLogRepository(session).bulk_insert_ignore(
            [
                {
                    "envelope_id": "env-old",
                    "domain": "sts",
                    "stream_id": "s1",
                    "source": "sts",
                    "level": "info",
                    "message": "first",
                    "ts": 1.0,
                },
                {
                    "envelope_id": "env-new",
                    "domain": "sts",
                    "stream_id": "s1",
                    "source": "sts",
                    "level": "info",
                    "message": "second",
                    "ts": 2.0,
                },
            ]
        )

    lines = await ws_module.session_log_replay("sts", "s1")
    messages = [json.loads(line)["payload"]["message"] for line in lines]
    assert messages == ["first", "second"]
    assert json.loads(lines[0])["id"] == "env-old"


async def test_session_log_replay_flushes_the_persist_batch(
    db, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A line still in the worker buffer must not miss the late socket."""
    monkeypatch.setattr(log_persist, "session_scope", db)
    buf = log_persist._Buffer()
    buf.rows.append(
        {
            "envelope_id": "env-gap",
            "domain": "sts",
            "stream_id": "s-gap",
            "source": "sts",
            "level": "info",
            "message": "on_start",
            "ts": 1.0,
        }
    )
    log_persist._buffer = buf
    try:
        lines = await ws_module.session_log_replay("sts", "s-gap")
    finally:
        log_persist._buffer = None
    assert [json.loads(line)["payload"]["message"] for line in lines] == [
        "on_start"
    ]


async def test_status_replay_is_the_session_list_not_session_logs(db) -> None:
    """A late board must not invent status from log lines."""
    async with db() as session:
        await an_owner(session)
        session.add(
            StsSessionRow(
                session_id="s-live",
                created_by=1,
                created_at=datetime(2026, 9, 1, tzinfo=UTC),
                status=SessionStatus.LIVE.value,
                strategy="idle",
                type="private::Tiny",
            )
        )
        await SessionLogRepository(session).bulk_insert_ignore(
            [
                {
                    "envelope_id": "not-status",
                    "domain": "sts",
                    "stream_id": "s-live",
                    "source": "sts",
                    "level": "info",
                    "message": "session started",
                    "ts": 1.0,
                }
            ]
        )

    lines = await ws_module.status_replay()
    assert len(lines) == 1
    env = json.loads(lines[0])
    assert env["type"] == STS_SESSION_STATUS
    assert env["payload"]["session_id"] == "s-live"
    assert env["payload"]["status"] == "live"
    assert env["payload"]["type"] == "private::Tiny"
    assert all("session started" not in line for line in lines)
