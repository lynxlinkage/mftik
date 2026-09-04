"""Migration 0029 — stored BinanceFuture / BinanceDelivery spellings move."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from db_harness import OWNER_ID, a_database, an_owner
from mftik_db.models import (
    AlertSource,
    Api,
    MdSessionRow,
    StsSessionRow,
    SymbolTicker,
)

_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "mftik_db"
    / "migrations"
    / "versions"
    / "0029_binance_um_cm_rename.py"
)


def _migration():
    spec = importlib.util.spec_from_file_location("m0029_up", _PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_rewrite_text_moves_both_spellings() -> None:
    rewrite_text = _migration().rewrite_text
    assert rewrite_text("BinanceFuture_Perp_BTCUSDT") == "BinanceUM_Perp_BTCUSDT"
    assert (
        rewrite_text("aggtrade.BinanceDelivery_Inverse_BTCUSD")
        == "aggtrade.BinanceCM_Inverse_BTCUSD"
    )


def test_rewrite_json_walks_keys_and_values() -> None:
    rewrite_json = _migration().rewrite_json
    assert rewrite_json(
        {
            "md": ["ticker.BinanceFuture_Perp_BTCUSDT"],
            "BinanceDelivery_Future_BTCUSD260925": {"qty": 1},
        }
    ) == {
        "md": ["ticker.BinanceUM_Perp_BTCUSDT"],
        "BinanceCM_Future_BTCUSD260925": {"qty": 1},
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
                Api(
                    owner_id=OWNER_ID,
                    venue="BinanceFuture",
                    api_key="k-um",
                    api_secret="s",
                )
            )
            session.add(
                Api(
                    owner_id=OWNER_ID,
                    venue="BinanceDelivery",
                    api_key="k-cm",
                    api_secret="s",
                )
            )
            session.add(
                SymbolTicker(
                    universal_ticker="BinanceFuture_Perp_BTCUSDT",
                    base="BTC",
                    quote="USDT",
                    exch_ticker="BTCUSDT",
                )
            )
            session.add(
                SymbolTicker(
                    universal_ticker="BinanceDelivery_Inverse_BTCUSD",
                    base="BTC",
                    quote="USD",
                    exch_ticker="BTCUSD_PERP",
                )
            )
            session.add(
                StsSessionRow(
                    session_id="s-1",
                    created_by=OWNER_ID,
                    yaml_text="md:\n  - aggtrade.BinanceFuture_Perp_BTCUSDT\n",
                    md_ids=["aggtrade.BinanceFuture_Perp_BTCUSDT"],
                    st_paras={"ticker": "BinanceDelivery_Inverse_BTCUSD"},
                )
            )
            session.add(
                MdSessionRow(
                    venue="BinanceFuture",
                    session_id="s-1",
                    created_by=OWNER_ID,
                )
            )
            session.add(
                AlertSource(
                    created_by=OWNER_ID,
                    domain="md",
                    selector="BinanceDelivery",
                )
            )
            await session.commit()
        async with database.engine.begin() as conn:
            await conn.run_sync(_upgrade)
        yield database


async def test_venue_columns_take_the_new_registry_names(upgraded) -> None:
    async with upgraded.maker() as session:
        venues = sorted(
            (await session.execute(sa.select(Api.venue))).scalars().all()
        )
        assert venues == ["BinanceCM", "BinanceUM"]
        md_venue = (
            await session.execute(sa.select(MdSessionRow.venue))
        ).scalar_one()
        assert md_venue == "BinanceUM"
        selector = (
            await session.execute(sa.select(AlertSource.selector))
        ).scalar_one()
        assert selector == "BinanceCM"


async def test_tickers_and_session_documents_move(upgraded) -> None:
    async with upgraded.maker() as session:
        tickers = sorted(
            (await session.execute(sa.select(SymbolTicker.universal_ticker)))
            .scalars()
            .all()
        )
        assert tickers == [
            "BinanceCM_Inverse_BTCUSD",
            "BinanceUM_Perp_BTCUSDT",
        ]
        row = (
            await session.execute(sa.select(StsSessionRow))
        ).scalar_one()
        assert row.yaml_text == "md:\n  - aggtrade.BinanceUM_Perp_BTCUSDT\n"
        assert row.md_ids == ["aggtrade.BinanceUM_Perp_BTCUSDT"]
        assert row.st_paras == {"ticker": "BinanceCM_Inverse_BTCUSD"}
