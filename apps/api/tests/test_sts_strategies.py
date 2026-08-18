"""GET /sts/strategies — the list is every STS session, not just successful deploys.

Attach failures persist a session and never record ``type`` / ``yaml_text``.
The list used to be driven by a sidecar table written only after deploy
succeeded, so those rows vanished. Pinning the endpoint is what keeps a
later ``WHERE type IS NOT NULL`` from bringing that back silently.
"""

from __future__ import annotations

import pytest
from db_harness import a_database, an_owner
from mftik.protocol import (
    STS_SESSION_LIST,
    ListSessionsResult,
    ListSessionsResultEnvelope,
)
from mftik_api.routes import sts as sts_routes
from mftik_db.repositories import StsSessionRepository


class QuietBroker:
    """Pause-state probe: no live sessions, so the list is the database alone."""

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        return ListSessionsResultEnvelope.wrap(
            ListSessionsResult(sessions=[]),
            type=STS_SESSION_LIST,
            source="sts",
        )


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        async with database.scope() as session:
            await an_owner(session)
        monkeypatch.setattr(sts_routes, "session_scope", database.scope)
        yield database.scope


async def test_an_attach_failure_still_appears_on_the_list(db) -> None:
    async with db() as session:
        repo = StsSessionRepository(session)
        await repo.create_live(
            session_id="s-ok",
            created_by=1,
            strategy="tiny",
            type="private::Tiny",
            yaml_text="sts: {}\n",
        )
        await repo.create_live(session_id="s-orphan", created_by=1, strategy="tiny")
        await repo.mark_failed(
            "s-orphan", "attach failed — rolled back during deploy"
        )

    result = await sts_routes.list_strategies(QuietBroker())
    by_id = {row.session_id: row for row in result.strategies}

    assert set(by_id) == {"s-ok", "s-orphan"}
    assert by_id["s-ok"].type == "private::Tiny"
    assert by_id["s-orphan"].type is None
    assert by_id["s-orphan"].status == "failed"
