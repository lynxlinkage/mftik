"""The account-worker interface IF-11 defines, and that it only returns null.

The shape is real: two layers, the handlers, the broadcast subjects, one
dead-man's-switch slot per venue. The behaviour is not. Starting the
pool, switching the trading layer, answering an order and publishing a
state raise ``NotImplementedError("IF-11")``.

What B6 has to make true is in ``test_account_contract.py``, as xfail.
"""

from __future__ import annotations

import ast
from decimal import Decimal
from pathlib import Path

import pytest
from mftik.exchange.models import OrderType, Side
from mftik.exchange.venues import UnknownVenueError, names
from mftik.procman.spec import validate_worker_id
from mftik.protocol import (
    STS_ORDER_CANCEL,
    STS_ORDER_SUBMIT,
    TD_LEDGER_VIEW,
    TD_OMS_ORDER,
    TD_OMS_VIEW,
    TD_ORDER_CANCEL_SESSION,
    Envelope,
    OrderCancel,
    OrderSubmit,
    TdCancelSessionRequest,
    TdLedgerViewRequest,
    TdOmsOrderRequest,
    TdOmsViewRequest,
    Topics,
)
from mftik_td.account import (
    INTERVAL_S,
    SLOTS,
    TICKET,
    WAIT_TIMEOUT_S,
    AccountWorker,
    deadman_for,
)
from mftik_td.session.settled import SETTLED_WAIT_TIMEOUT_S

API = 42
SESSION = "abc123"


def _worker(**overrides: object) -> AccountWorker:
    kwargs: dict[str, object] = {"venue": "Paper"}
    kwargs.update(overrides)
    return AccountWorker(API, **kwargs)  # type: ignore[arg-type]


def test_the_module_imports_and_names_its_ticket() -> None:
    assert TICKET == "IF-11"


def test_an_account_worker_is_one_api_id_and_does_nothing_yet() -> None:
    worker = _worker()
    assert worker.api_id == API
    assert worker.venue == "Paper"
    assert worker.incarnation == 0
    assert worker.cancel_on_disconnect is False
    assert worker.worker_id == f"td/account/{API}"
    validate_worker_id(worker.worker_id)
    assert worker.trading.resident is worker.resident
    assert worker.trading.active is False
    assert worker.trading.leverage == {}
    assert worker.resident.started is False
    assert worker.resident.pool is None
    assert worker.resident.keepalive is None
    assert worker.broadcast.current() is None
    assert worker.broadcast.incarnation == 0
    assert worker.deadman.venue == "Paper"


def test_the_worker_id_follows_the_diagram() -> None:
    assert AccountWorker(7, venue="Deribit").worker_id == "td/account/7"


@pytest.mark.parametrize("api_id", [0, -1, True, "42"])
def test_api_id_has_to_be_a_positive_int(api_id: object) -> None:
    with pytest.raises(ValueError):
        AccountWorker(api_id, venue="Paper")  # type: ignore[arg-type]


def test_incarnation_cannot_be_negative() -> None:
    with pytest.raises(ValueError):
        AccountWorker(API, venue="Paper", incarnation=-1)
    assert AccountWorker(API, venue="Paper", incarnation=0).incarnation == 0


def test_cancel_on_disconnect_defaults_off_and_is_a_bool() -> None:
    assert AccountWorker(API, venue="Paper").cancel_on_disconnect is False
    worker = AccountWorker(API, venue="Okx", cancel_on_disconnect=True)
    assert worker.cancel_on_disconnect is True
    with pytest.raises(TypeError):
        AccountWorker(API, venue="Paper", cancel_on_disconnect="yes")  # type: ignore[arg-type]


def test_an_unknown_venue_is_refused() -> None:
    with pytest.raises(UnknownVenueError):
        AccountWorker(API, venue="NotAVenue")


def test_a_venue_name_is_normalized_to_the_registry() -> None:
    worker = AccountWorker(API, venue="binanceum")
    assert worker.venue == "BinanceUM"
    assert worker.deadman.venue == "BinanceUM"


def test_every_registered_venue_has_one_dead_man_slot() -> None:
    assert set(SLOTS) == set(names())
    for venue in names():
        slot = deadman_for(venue)
        assert type(slot) is SLOTS[venue]
        assert slot.venue == venue


def test_subjects_stay_where_topics_already_put_them() -> None:
    """Views are request-reply on ``td.account``, not on the fan-out."""
    worker = _worker()
    assert worker.orders.subject(API) == Topics.td_order(API) == f"td.order.{API}"
    assert worker.oms.subject(API) == Topics.td_account(API) == f"td.account.{API}"
    assert worker.ledger.subject(API) == Topics.td_account(API)
    assert worker.orders.subject(API) != Topics.td_oms(API)
    assert worker.ledger.subject(API) != Topics.td_ledger(API)
    assert worker.broadcast.subject == Topics.td_account_state(API)
    assert worker.broadcast.reset_subject == Topics.td_global(API)
    assert INTERVAL_S == 2.0


def test_the_wait_budget_is_the_one_the_settled_read_already_uses() -> None:
    assert WAIT_TIMEOUT_S == SETTLED_WAIT_TIMEOUT_S == 30.0


def test_an_oms_view_request_without_settled_stays_unsettled() -> None:
    """Older callers omit the field. That is the memory read (V1)."""
    request = TdOmsViewRequest.model_validate({"api_id": API})
    assert request.settled is False
    assert TdOmsViewRequest(api_id=API, settled=True).settled is True


def test_order_handler_types_are_submit_cancel_and_cancel_session() -> None:
    worker = _worker()
    assert worker.orders.TYPES == frozenset(
        {STS_ORDER_SUBMIT, STS_ORDER_CANCEL, TD_ORDER_CANCEL_SESSION}
    )
    assert worker.oms.TYPES == frozenset({TD_OMS_VIEW, TD_OMS_ORDER})
    assert worker.ledger.TYPES == frozenset({TD_LEDGER_VIEW})


# walks the TD sources; over the 50 ms unit call cap
@pytest.mark.component
def test_the_td_process_does_not_import_the_account_worker() -> None:
    """B6 wires it. The process that is running today must not."""
    root = Path(__file__).resolve().parents[1] / "src" / "mftik_td"
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        if "account" in path.relative_to(root).parts:
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.ImportFrom) and node.module:
                modules.append(node.module)
            elif isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            for module in modules:
                if module == "mftik_td.account" or module.startswith(
                    "mftik_td.account."
                ):
                    offenders.append(f"{path.relative_to(root)}:{node.lineno}")
    assert offenders == []


def _submit() -> OrderSubmit:
    return OrderSubmit(
        session_id=SESSION,
        api_id=API,
        universal_ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        qty=Decimal("0.01"),
        price=Decimal("60000"),
        client_order_id="1",
    )


async def test_actions_raise_the_ticket_number() -> None:
    worker = _worker()
    slot = worker.deadman
    message = Envelope[dict[str, object]].wrap(
        {}, type=TD_ORDER_CANCEL_SESSION, source="test"
    )
    calls = [
        worker.resident.start(),
        worker.resident.close(),
        worker.resident.keepalive_once(),
        worker.resident.handle_backfill(message),
        worker.trading.activate(),
        worker.trading.deactivate(),
        worker.orders(message),
        worker.orders.submit(_submit()),
        worker.orders.cancel(
            OrderCancel(session_id=SESSION, api_id=API, client_order_id="1")
        ),
        worker.orders.cancel_session(TdCancelSessionRequest(session_id=SESSION)),
        worker.oms(message),
        worker.oms.view(TdOmsViewRequest(api_id=API)),
        worker.oms.view(TdOmsViewRequest(api_id=API, settled=True)),
        worker.oms.order(TdOmsOrderRequest(api_id=API, client_order_id="1")),
        worker.ledger(message),
        worker.ledger.view(TdLedgerViewRequest(api_id=API)),
        worker.broadcast.publish("ready"),
        worker.broadcast.publish_steady(),
        worker.broadcast.publish_reset(),
        slot.refresh(symbols=frozenset()),
        slot.extend(),
        slot.stop(),
    ]
    for call in calls:
        with pytest.raises(NotImplementedError, match=TICKET):
            await call
    with pytest.raises(NotImplementedError, match=TICKET):
        slot.supported()


async def test_a_bad_argument_is_refused_before_the_stub() -> None:
    worker = _worker()
    with pytest.raises(TypeError):
        await worker.orders.submit(object())  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        await worker.orders.cancel_session(
            TdCancelSessionRequest(session_id=SESSION), timeout=-1
        )
    with pytest.raises(ValueError):
        await worker.oms.view(TdOmsViewRequest(api_id=API), timeout=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        await worker.broadcast.publish("down")  # type: ignore[arg-type]
