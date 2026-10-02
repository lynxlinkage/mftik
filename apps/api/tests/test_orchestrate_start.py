"""§8.1 start / end. A failed start is not rolled back (F12, IF-13)."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from db_harness import a_database, an_instance, an_owner
from fastapi import HTTPException
from mftik.broker import Broker
from mftik.broker.config import BrokerConfig
from mftik.broker.errors import NoRespondersError
from mftik.protocol import (
    MD_INTENT_DELETE,
    MD_INTENT_PUT,
    PROTOCOL_VERSION,
    STS_SESSION_END,
    STS_SESSION_START,
    TD_INTENT_DELETE,
    TD_INTENT_PUT,
    Envelope,
    IntentOwner,
    MdIntentPut,
    Topics,
    parse_strategy_yml,
)
from mftik_api import orchestrate
from mftik_api.broker_rpc import DomainRpcError
from mftik_api.orchestrate import end, end_subject, start
from mftik_api.routes import sts as sts_routes
from mftik_api.schemas import StrategyDeployBody
from mftik_db.models.account import Account
from mftik_db.models.api import Api
from mftik_db.models.intent import MdIntent, TdIntent
from mftik_db.models.session import SessionStatus, StsSessionRow
from mftik_db.repositories import IntentRepository, StsSessionRepository
from sqlalchemy import select

YAML = """\
td:
  paper:
md:
  md-jp:
    - orderbook.Paper_Spot_BTCUSDT
sts: {}
"""

YAML_ON_FAILURE = """\
restart: on_failure
td:
  paper:
md:
  md-jp:
    - orderbook.Paper_Spot_BTCUSDT
sts: {}
"""


class _Store:
    def list_all(self) -> list[object]:
        return []


class ScriptedTransport:
    """Answers accept RPCs without opening a broker connection."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, str, float]] = []
        self.fail_on: set[str] = set()
        self.replies_pv: int | None = PROTOCOL_VERSION
        self.probes = 0

    def reply_inbox(self, request_id: str) -> None:
        del request_id
        return None

    async def request(
        self,
        subject: str,
        raw: str,
        *,
        request_id: str,
        inbox: str | None,
        timeout: float,
    ) -> str:
        del inbox
        self.sent.append((subject, raw, timeout))
        body = json.loads(raw)
        if body["type"] in self.fail_on:
            raise NoRespondersError(subject, request_id, timeout)
        return _reply(body, pv=self.replies_pv)

    async def probe(
        self,
        subject: str,
        raw: str,
        *,
        request_id: str,
        inbox: str | None,
        timeout: float,
    ) -> str:
        del subject, raw, request_id, inbox, timeout
        self.probes += 1
        return Envelope.wrap(
            {"status": "ok"}, type="health", source="plane"
        ).to_json()


def _reply(body: dict, *, pv: int | None) -> str:
    kind = body["type"]
    session_id = body["payload"]["session_id"]
    if kind == STS_SESSION_START:
        payload: dict = {"session_id": session_id, "status": "starting"}
    elif kind == STS_SESSION_END:
        payload = {"session_id": session_id, "status": "done"}
    elif kind == MD_INTENT_PUT:
        payload = {"session_id": session_id, "atoms": {}}
    else:
        payload = {"session_id": session_id}
    envelope: dict = {
        "id": "reply",
        "type": kind,
        "source": "plane",
        "session_id": session_id,
        "ts": 0,
        "payload": payload,
    }
    if pv is not None:
        envelope["pv"] = pv
    return json.dumps(envelope)


def _broker(transport: ScriptedTransport) -> Broker:
    return Broker(
        BrokerConfig(nats_url="nats://unused", key_prefix="mft"),
        transport=transport,  # type: ignore[arg-type]
    )


@pytest.fixture
async def world(monkeypatch, database_url):
    async with a_database(database_url) as database:
        async with database.maker() as session:
            await an_owner(session)
            await an_instance(session, "sts-jp", "sts", region="jp")
            await an_instance(session, "md-jp", "md", region="jp")
            td = await an_instance(session, "td-jp", "td", region="jp")
            session.add(
                Api(
                    id=1,
                    owner_id=1,
                    venue="Paper",
                    api_key="paper-key",
                    api_secret="s",
                    type="HMAC",
                    instance_id=td.id,
                )
            )
            session.add(Account(name="paper", api_id=1, created_by=1))
            await session.commit()
        monkeypatch.setattr(orchestrate, "session_scope", database.scope)

        async def _no_audit(**_kwargs: object) -> None:
            return None

        monkeypatch.setattr(sts_routes, "record_audit", _no_audit)
        yield database


def test_end_subject_is_the_session_control_subject() -> None:
    assert end_subject("abc") == Topics.sts_control("abc")


def test_deploy_route_accepts_with_202() -> None:
    matches = [
        route
        for route in sts_routes.router.routes
        if getattr(route, "path", None) == "/sts/deploy/{strategy_type}"
    ]
    assert len(matches) == 1
    assert matches[0].status_code == 202


async def test_stop_route_stays_unimplemented() -> None:
    with pytest.raises(HTTPException) as exc:
        await sts_routes.stop_session(
            "abc",
            SimpleNamespace(),  # type: ignore[arg-type]
        )
    assert exc.value.status_code == 501


def test_deploy_maps_domain_errors() -> None:
    assert sts_routes._deploy_status(DomainRpcError("unknown_api", "x")) == 404
    assert (
        sts_routes._deploy_status(DomainRpcError("unknown_strategy", "x"))
        == 404
    )
    assert sts_routes._deploy_status(DomainRpcError("not_found", "x")) == 404
    assert sts_routes._deploy_status(DomainRpcError("timeout", "x")) == 504
    assert (
        sts_routes._deploy_status(
            DomainRpcError("incompatible_environment", "x")
        )
        == 409
    )
    assert (
        sts_routes._deploy_status(DomainRpcError("strategy_refused", "x"))
        == 400
    )
    assert (
        sts_routes._deploy_status(DomainRpcError("protocol_mismatch", "x"))
        == 502
    )


async def test_bad_yaml_is_400() -> None:
    with pytest.raises(HTTPException) as exc:
        await sts_routes.deploy(
            "NoopStrategy",
            StrategyDeployBody(yaml=": :"),
            SimpleNamespace(),  # type: ignore[arg-type]
            _Store(),  # type: ignore[arg-type]
        )
    assert exc.value.status_code == 400


async def test_an_unknown_strategy_type_is_404() -> None:
    with pytest.raises(HTTPException) as exc:
        await sts_routes.deploy(
            "NotAStrategy",
            StrategyDeployBody(yaml="sts: {}"),
            SimpleNamespace(),  # type: ignore[arg-type]
            _Store(),  # type: ignore[arg-type]
        )
    assert exc.value.status_code == 404
    assert "unknown strategy type" in str(exc.value.detail)


async def test_a_quote_account_missing_from_td_is_400() -> None:
    yaml = "td:\n  paper:\nsts:\n  quote_account: hedge\n"
    with pytest.raises(HTTPException) as exc:
        await sts_routes.deploy(
            "NoopStrategy",
            StrategyDeployBody(yaml=yaml),
            SimpleNamespace(),  # type: ignore[arg-type]
            _Store(),  # type: ignore[arg-type]
        )
    assert exc.value.status_code == 400
    assert "quote_account" in str(exc.value.detail)


async def _accepted(
    world,
    transport: ScriptedTransport,
    *,
    yaml: str = YAML,
    instance: str | None = "sts-jp",
):
    spec = parse_strategy_yml(yaml)
    return await start(
        spec,
        broker=_broker(transport),
        strategy_type="NoopStrategy",
        yaml_text=yaml,
        created_by=1,
        instance=instance,
    )


async def test_start_writes_the_spec_and_intents_then_asks_the_planes(
    world,
) -> None:
    transport = ScriptedTransport()
    result = await _accepted(world, transport)

    assert result.status == "starting"
    assert result.progress is None
    assert result.td == []
    assert result.md == []
    assert result.type == "NoopStrategy"
    assert transport.probes >= 2

    async with world.scope() as db:
        row = await StsSessionRepository(db).get_by_session_id(result.session_id)
        assert row is not None
        assert row.yaml_text == YAML
        assert row.restart == "never"
        assert row.generation == 1
        assert row.status == SessionStatus.LIVE.value
        assert row.instance == "sts-jp"
        assert row.type == "NoopStrategy"
        assert row.td["paper"]["api_id"] == 1
        md = await db.get(MdIntent, (result.session_id, "md-jp"))
        td = await db.get(TdIntent, (result.session_id, 1))
        assert md is not None and md.released_at is None
        assert td is not None and td.released_at is None

    kinds = [json.loads(raw)["type"] for _subject, raw, _timeout in transport.sent]
    subjects = [subject for subject, _raw, _timeout in transport.sent]
    assert kinds == [TD_INTENT_PUT, MD_INTENT_PUT, STS_SESSION_START]
    assert subjects == [
        Topics.td("td-jp"),
        Topics.md("md-jp"),
        Topics.sts("sts-jp"),
    ]
    assert all(
        timeout == orchestrate._ACCEPT_TIMEOUT_S
        for _subject, _raw, timeout in transport.sent
    )
    start_payload = json.loads(transport.sent[2][1])["payload"]
    assert start_payload["restart"] == "never"
    assert start_payload["instance"] == "sts-jp"
    assert "strategy_digest" not in start_payload
    assert "env_generation" not in start_payload
    owner = json.loads(transport.sent[0][1])["payload"]["owner"]
    assert owner == {
        "sts_instance": "sts-jp",
        "session_id": result.session_id,
    }


async def test_on_failure_is_stored_and_sent(world) -> None:
    transport = ScriptedTransport()
    result = await _accepted(world, transport, yaml=YAML_ON_FAILURE)

    async with world.scope() as db:
        row = await StsSessionRepository(db).get_by_session_id(result.session_id)
        assert row is not None
        assert row.restart == "on_failure"
    start_payload = json.loads(transport.sent[2][1])["payload"]
    assert start_payload["restart"] == "on_failure"


async def test_an_unnamed_deploy_stores_a_null_instance_and_addresses_the_derived_sts(
    world,
) -> None:
    transport = ScriptedTransport()
    result = await _accepted(world, transport, instance=None)

    async with world.scope() as db:
        row = await StsSessionRepository(db).get_by_session_id(result.session_id)
        assert row is not None
        assert row.instance is None
    assert transport.sent[2][0] == Topics.sts("sts-jp")
    start_payload = json.loads(transport.sent[2][1])["payload"]
    assert start_payload["instance"] is None
    owner = json.loads(transport.sent[0][1])["payload"]["owner"]
    assert owner["sts_instance"] == "sts-jp"


async def test_a_failed_start_is_not_rolled_back(world) -> None:
    transport = ScriptedTransport()
    transport.fail_on.add(STS_SESSION_START)
    with pytest.raises(DomainRpcError) as exc:
        await _accepted(world, transport)
    assert exc.value.code == "timeout"
    assert exc.value.no_responders is True

    kinds = [json.loads(raw)["type"] for _subject, raw, _timeout in transport.sent]
    assert STS_SESSION_END not in kinds
    assert MD_INTENT_DELETE not in kinds
    assert TD_INTENT_DELETE not in kinds
    assert kinds == [TD_INTENT_PUT, MD_INTENT_PUT, STS_SESSION_START]

    async with world.scope() as db:
        rows = (await db.execute(select(StsSessionRow))).scalars().all()
        assert len(rows) == 1
        assert rows[0].status == SessionStatus.LIVE.value
        md = (await db.execute(select(MdIntent))).scalars().all()
        td = (await db.execute(select(TdIntent))).scalars().all()
        assert len(md) == 1 and md[0].released_at is None
        assert len(td) == 1 and td[0].released_at is None


async def test_a_reply_with_the_wrong_pv_is_not_an_accept_and_is_not_rolled_back(
    world,
) -> None:
    transport = ScriptedTransport()
    transport.replies_pv = 1
    with pytest.raises(DomainRpcError) as exc:
        await _accepted(world, transport)
    assert exc.value.code == "protocol_mismatch"

    kinds = [json.loads(raw)["type"] for _subject, raw, _timeout in transport.sent]
    assert kinds == [TD_INTENT_PUT]
    async with world.scope() as db:
        rows = (await db.execute(select(StsSessionRow))).scalars().all()
        assert len(rows) == 1
        td = (await db.execute(select(TdIntent))).scalars().one()
        md = (await db.execute(select(MdIntent))).scalars().one()
        assert td.released_at is None
        assert md.released_at is None


async def test_an_unknown_account_is_404_and_writes_nothing(world) -> None:
    yaml = (
        "td:\n  missing:\nmd:\n  md-jp:\n"
        "    - orderbook.Paper_Spot_BTCUSDT\nsts: {}\n"
    )
    with pytest.raises(HTTPException) as exc:
        await sts_routes.deploy(
            "NoopStrategy",
            StrategyDeployBody(yaml=yaml, instance="sts-jp"),
            _broker(ScriptedTransport()),  # type: ignore[arg-type]
            _Store(),  # type: ignore[arg-type]
        )
    assert exc.value.status_code == 404

    async with world.scope() as db:
        rows = (await db.execute(select(StsSessionRow))).scalars().all()
        assert rows == []


async def test_deploy_returns_202_fields(world) -> None:
    transport = ScriptedTransport()
    result = await sts_routes.deploy(
        "NoopStrategy",
        StrategyDeployBody(yaml=YAML, instance="sts-jp", timeout=99),
        _broker(transport),  # type: ignore[arg-type]
        _Store(),  # type: ignore[arg-type]
    )
    assert result.session_id
    assert result.status == "starting"
    assert result.progress is None
    assert result.td == []
    assert result.md == []
    assert all(
        timeout == orchestrate._ACCEPT_TIMEOUT_S
        for _subject, _raw, timeout in transport.sent
    )


async def test_end_releases_after_the_session_accepts(world) -> None:
    transport = ScriptedTransport()
    broker = _broker(transport)
    result = await _accepted(world, transport)
    await end(result.session_id, "operator stop", broker=broker)

    kinds = [json.loads(raw)["type"] for _subject, raw, _timeout in transport.sent]
    assert kinds[-3:] == [STS_SESSION_END, MD_INTENT_DELETE, TD_INTENT_DELETE]
    assert transport.sent[-3][0] == end_subject(result.session_id)
    assert transport.sent[-2][0] == Topics.md("md-jp")
    assert transport.sent[-1][0] == Topics.td("td-jp")
    assert json.loads(transport.sent[-2][1])["payload"]["reason"] == "operator stop"

    async with world.scope() as db:
        row = await StsSessionRepository(db).get_by_session_id(result.session_id)
        assert row is not None
        md = await db.get(MdIntent, (result.session_id, "md-jp"))
        td = await db.get(TdIntent, (result.session_id, 1))
        assert md is not None and md.released_at is not None
        assert td is not None and td.released_at is not None


async def test_a_failed_end_does_not_release(world) -> None:
    transport = ScriptedTransport()
    result = await _accepted(world, transport)
    transport.fail_on.add(STS_SESSION_END)
    with pytest.raises(DomainRpcError) as exc:
        await end(result.session_id, "operator stop", broker=_broker(transport))
    assert exc.value.code == "timeout"

    kinds = [json.loads(raw)["type"] for _subject, raw, _timeout in transport.sent]
    assert MD_INTENT_DELETE not in kinds
    assert TD_INTENT_DELETE not in kinds
    async with world.scope() as db:
        md = await db.get(MdIntent, (result.session_id, "md-jp"))
        td = await db.get(TdIntent, (result.session_id, 1))
        assert md is not None and md.released_at is None
        assert td is not None and td.released_at is None


async def test_end_with_no_named_owner_releases_and_does_not_invent_a_subject(
    world,
) -> None:
    transport = ScriptedTransport()
    async with world.scope() as db:
        await StsSessionRepository(db).create_live(
            session_id="orphan",
            created_by=1,
            type="NoopStrategy",
            instance=None,
            td={},
        )
        await IntentRepository(db).put(
            MdIntentPut(
                session_id="orphan",
                owner=IntentOwner(sts_instance="sts-jp", session_id="orphan"),
                feeds={"md-jp": ["book"]},
            )
        )

    await end("orphan", "gone", broker=_broker(transport))

    kinds = [json.loads(raw)["type"] for _subject, raw, _timeout in transport.sent]
    assert kinds == [STS_SESSION_END]
    async with world.scope() as db:
        md = await db.get(MdIntent, ("orphan", "md-jp"))
        assert md is not None and md.released_at is not None
