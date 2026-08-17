"""Unit tests for log topic parsing and batch flush helpers."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from mftik.protocol import Envelope
from mftik_api import log_persist


def test_parse_log_topic_ok() -> None:
    assert log_persist.parse_log_topic("log.sts.abc") == ("sts", "abc")
    assert log_persist.parse_log_topic("log.td.42") == ("td", "42")
    assert log_persist.parse_log_topic("log.md.Gate") == ("md", "Gate")


def test_parse_log_topic_rejects_junk() -> None:
    assert log_persist.parse_log_topic("log.sts") is None
    assert log_persist.parse_log_topic("log.xx.y") is None
    assert log_persist.parse_log_topic("sys.heartbeat") is None


def test_envelope_to_row() -> None:
    env = Envelope[dict].wrap(
        {"level": "warn", "message": "boom"},
        type="log",
        source="td",
        session_id="9",
    )
    row = log_persist.envelope_to_row("log.td.9", env)
    assert row is not None
    assert row["envelope_id"] == env.id
    assert row["domain"] == "td"
    assert row["stream_id"] == "9"
    assert row["level"] == "warn"
    assert row["message"] == "boom"
    assert row["source"] == "td"
    assert row["ts"] == env.ts


def test_envelope_to_row_skips_missing_message() -> None:
    env = Envelope[dict].wrap(
        {"level": "info"},
        type="log",
        source="sts",
    )
    assert log_persist.envelope_to_row("log.sts.x", env) is None


async def test_run_log_persist_flushes_on_batch_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOG_PERSIST_BATCH_SIZE", "2")
    monkeypatch.setenv("LOG_PERSIST_FLUSH_INTERVAL", "30")

    flushed: list[list[dict[str, Any]]] = []

    async def fake_flush(rows: list[dict[str, Any]]) -> None:
        flushed.append(list(rows))

    monkeypatch.setattr(log_persist, "flush_rows", fake_flush)

    class FakeBroker:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def psubscribe(self, _pattern: str, *, stop: asyncio.Event):
            for i in range(3):
                if stop.is_set():
                    return
                env = Envelope[dict].wrap(
                    {"level": "info", "message": f"m{i}"},
                    type="log",
                    source="sts",
                    session_id="s1",
                )
                yield "log.sts.s1", env
            stop.set()

    monkeypatch.setattr(log_persist, "Broker", FakeBroker)

    stop = asyncio.Event()
    await log_persist.run_log_persist(stop)

    assert len(flushed) >= 1
    assert sum(len(batch) for batch in flushed) == 3
    assert any(len(batch) == 2 for batch in flushed)


async def test_run_log_persist_flushes_on_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOG_PERSIST_BATCH_SIZE", "100")
    monkeypatch.setenv("LOG_PERSIST_FLUSH_INTERVAL", "0.05")

    flushed: list[list[dict[str, Any]]] = []
    flush_event = asyncio.Event()

    async def fake_flush(rows: list[dict[str, Any]]) -> None:
        flushed.append(list(rows))
        flush_event.set()

    monkeypatch.setattr(log_persist, "flush_rows", fake_flush)

    class FakeBroker:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def psubscribe(self, _pattern: str, *, stop: asyncio.Event):
            env = Envelope[dict].wrap(
                {"level": "info", "message": "slow"},
                type="log",
                source="md",
                session_id="gate",
            )
            yield "log.md.gate", env
            await flush_event.wait()
            stop.set()
            # Keep the generator open until stop so the ticker can fire.
            while not stop.is_set():
                await asyncio.sleep(0.01)
                return

    monkeypatch.setattr(log_persist, "Broker", FakeBroker)

    stop = asyncio.Event()
    await asyncio.wait_for(log_persist.run_log_persist(stop), timeout=2)

    assert len(flushed) == 1
    assert flushed[0][0]["message"] == "slow"
