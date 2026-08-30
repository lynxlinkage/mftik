"""Migration 0027 helpers — td list → mapping, and CrossArb st_paras."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "mftik_db"
    / "migrations"
    / "versions"
    / "0027_sts_td_mapping.py"
)


def _mod():
    spec = importlib.util.spec_from_file_location("m0027", _PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_td_mapping_from_ids_names_accounts_by_id() -> None:
    mapping = _mod().td_mapping_from_ids([3, 7])
    assert list(mapping) == ["account-3", "account-7"]
    assert mapping["account-3"] == {"api_id": 3}


def test_td_mapping_from_ids_accepts_json_text() -> None:
    mapping = _mod().td_mapping_from_ids("[11, 22]")
    assert list(mapping) == ["account-11", "account-22"]


def test_cross_arb_st_paras_take_the_first_two_td_keys() -> None:
    out = _mod().backfill_cross_arb_st_paras(
        {"qty": "0.001"}, ["account-11", "account-22"]
    )
    assert out == {
        "qty": "0.001",
        "quote_account": "account-11",
        "hedge_account": "account-22",
    }


def test_cross_arb_st_paras_leave_existing_names_alone() -> None:
    paras = {
        "quote_account": "binance quoter",
        "hedge_account": "gate hedger",
    }
    assert (
        _mod().backfill_cross_arb_st_paras(
            paras, ["account-11", "account-22"]
        )
        is None
    )


def test_cross_arb_st_paras_need_two_keys() -> None:
    assert _mod().backfill_cross_arb_st_paras({}, ["account-11"]) is None
    assert _mod().backfill_cross_arb_st_paras({}, []) is None
