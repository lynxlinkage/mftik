"""Single-STS anycast for tests that do not stand up the instances table."""

from __future__ import annotations

from mftik.protocol import Topics
from mftik_api.sts_fanout import StsTarget


def patch_authoritative_anycast(monkeypatch) -> None:  # noqa: ANN001
    async def _one() -> list[StsTarget]:
        return [StsTarget(name="sts", subject=Topics.STS, authoritative=True)]

    monkeypatch.setattr("mftik_api.sts_fanout.list_targets", _one)
