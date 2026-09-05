"""Migration 0030 — glued dated symbols become PAIR-YYMMDD."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from db_harness import OWNER_ID, a_database, an_owner
from mftik_db.models import (
    BackfillCursorRow,
    StsSessionRow,
    SymbolTicker,
)

_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "mftik_db"
    / "migrations"
    / "versions"
    / "0030_dated_symbol_hyphen.py"
)


def _migration():
    spec = importlib.util.spec_from_file_location("m0030_up", _PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_rewrite_text_hyphenates_dated_futures_only() -> None:
    rewrite_text = _migration().rewrite_text
    assert (
        rewrite_text("BinanceUM_Future_BTCUSDT250926")
        == "BinanceUM_Future_BTCUSDT-250926"
    )
    assert (
        rewrite_text("aggtrade.BinanceCM_Future_BTCUSD260925")
        == "aggtrade.BinanceCM_Future_BTCUSD-260925"
    )
    assert rewrite_text("BinanceUM_Perp_BTCUSDT") == "BinanceUM_Perp_BTCUSDT"
    assert rewrite_text("BinanceUM_Future_BTCUSDT-250926") == (
        "BinanceUM_Future_BTCUSDT-250926"
    )
    assert (
        rewrite_text("BinanceUM_Option_BTCUSDT-260905-100000-C")
        == "BinanceUM_Option_BTCUSDT-260905-100000-C"
    )


def test_rewrite_text_round_trips() -> None:
    rewrite_text = _migration().rewrite_text
    old = "aggtrade.BinanceUM_Future_BTCUSDT250926"
    new = "aggtrade.BinanceUM_Future_BTCUSDT-250926"
    assert rewrite_text(old) == new
    assert rewrite_text(new, reverse=True) == old


def test_rewrite_json_walks_keys_and_values() -> None:
    rewrite_json = _migration().rewrite_json
    assert rewrite_json(
        {
            "md": ["ticker.BinanceUM_Future_BTCUSDT250926"],
            "BinanceCM_Future_BTCUSD260925": {"qty": 1},
        }
    ) == {
        "md": ["ticker.BinanceUM_Future_BTCUSDT-250926"],
        "BinanceCM_Future_BTCUSD-260925": {"qty": 1},
    }


def _upgrade(conn: sa.Connection) -> None:
    ops = Operations(MigrationContext.configure(conn))
    with Operations.context(ops.migration_context):
        _migration().upgrade()


@pytest.fixture
async def upgraded(database_url):
    async with a_database(database_url) as database:
        async with database.maker() as session:
            await an_owner(session)
            session.add(
                SymbolTicker(
                    universal_ticker="BinanceUM_Perp_BTCUSDT",
                    base="BTC",
                    quote="USDT",
                    exch_ticker="BTCUSDT",
                )
            )
            session.add(
                SymbolTicker(
                    universal_ticker="BinanceUM_Future_BTCUSDT250926",
                    base="BTC",
                    quote="USDT",
                    exch_ticker="BTCUSDT_250926",
                )
            )
            session.add(
                SymbolTicker(
                    universal_ticker="BinanceCM_Future_BTCUSD260925",
                    base="BTC",
                    quote="USD",
                    exch_ticker="BTCUSD_260925",
                )
            )
            session.add(
                StsSessionRow(
                    session_id="s-1",
                    created_by=OWNER_ID,
                    yaml_text="md:\n  - aggtrade.BinanceUM_Future_BTCUSDT250926\n",
                    md_ids=["aggtrade.BinanceUM_Future_BTCUSDT250926"],
                    st_paras={"ticker": "BinanceCM_Future_BTCUSD260925"},
                )
            )
            session.add(
                BackfillCursorRow(
                    api_id=1,
                    stream="trades",
                    scope="BinanceUM_Future_BTCUSDT250926",
                    confirmed_through_ts=1.0,
                )
            )
            await session.commit()
        async with database.engine.begin() as conn:
            await conn.run_sync(_upgrade)
        yield database


async def test_dated_tickers_and_session_documents_move(upgraded) -> None:
    async with upgraded.maker() as session:
        tickers = sorted(
            (await session.execute(sa.select(SymbolTicker.universal_ticker)))
            .scalars()
            .all()
        )
        assert tickers == [
            "BinanceCM_Future_BTCUSD-260925",
            "BinanceUM_Future_BTCUSDT-250926",
            "BinanceUM_Perp_BTCUSDT",
        ]
        row = (await session.execute(sa.select(StsSessionRow))).scalar_one()
        assert row.yaml_text == (
            "md:\n  - aggtrade.BinanceUM_Future_BTCUSDT-250926\n"
        )
        assert row.md_ids == ["aggtrade.BinanceUM_Future_BTCUSDT-250926"]
        assert row.st_paras == {"ticker": "BinanceCM_Future_BTCUSD-260925"}
        scope = (
            await session.execute(sa.select(BackfillCursorRow.scope))
        ).scalar_one()
        assert scope == "BinanceUM_Future_BTCUSDT-250926"
