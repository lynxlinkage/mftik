#!/usr/bin/env python3
"""B4-09 compose acceptance.

Opt in with ``MFTIK_E2E_COMPOSE=1`` (see ``apps/sts/tests/test_b4_09_e2e.py``).
CI has no compose stack. This process drives ``docker-compose.yml`` plus
``docker-compose.b4-09.yml``: a start over budget, the paper path, then a
SIGTERM roll of each controller inside its running container.

Stdout is the measurements. It does not print hosts, addresses, passwords,
or environment values.
"""

from __future__ import annotations

import asyncio
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_FILES = ("-f", "docker-compose.yml", "-f", "docker-compose.b4-09.yml")
_GC_WINDOW_S = 10.0
_MARKER_NAMES = ("start", "ready", "books", "fill", "trade", "traded")
_SECRET_MARKERS = (
    "PASSWORD",
    "SECRET",
    "DATABASE_URL",
    "TOKEN",
    "API_KEY",
)

_OVER = """\
from mftik.strategy import Strategy


class OverBudget(Strategy):
    async def on_start(self) -> None:
        return None
"""

_OVER_YAML = """\
td:
  "paper trader": {}
sts: {}
"""

_PATH = r'''
import asyncio
import time
from decimal import Decimal
from pathlib import Path

from mftik.exchange.models import OrderType, Side
from mftik.strategy import Strategy
from mftik.strategy.timer import now_ms


class B4Run(Strategy):
    def __init__(self) -> None:
        super().__init__()
        self._t0 = 0.0
        self._ack = 0.0
        self._traded = False

    async def on_start(self) -> None:
        Path(self.paras["start"]).write_text("start\n", encoding="utf-8")

    async def on_ready(self, ready: object) -> None:
        del ready
        self._t0 = time.perf_counter()
        api_id = self.oms.api_ids()[0]
        ok = False
        while time.perf_counter() - self._t0 < 8:
            t_submit = time.perf_counter()
            ok = await self.oms.submit_order(
                api_id,
                ticker="Paper_Spot_BTCUSDT",
                side=Side.BUY,
                type=OrderType.MARKET,
                qty=Decimal("0.01"),
            )
            if ok:
                self._ack = time.perf_counter() - t_submit
                break
            await asyncio.sleep(0.05)
        Path(self.paras["ready"]).write_text(
            f"ok={ok} submit_s={time.perf_counter() - self._t0:.4f} "
            f"ack_s={self._ack:.4f} code={self.oms.last_reject_code}\n",
            encoding="utf-8",
        )
        self.timer.token().register(
            now_ms() + 50, 100, self._poke, label="b409-poke"
        )

    async def on_fill(self, api_id: int, fill: object) -> None:
        Path(self.paras["fill"]).write_text(
            f"api_id={api_id} qty={getattr(fill, 'qty', '')} "
            f"since_ready_s={time.perf_counter() - self._t0:.4f}\n",
            encoding="utf-8",
        )

    async def _poke(self) -> None:
        flag = Path(self.paras["trade"])
        if not flag.is_file() or self._traded:
            return
        self._traded = True
        api_id = self.oms.api_ids()[0]
        t0 = time.perf_counter()
        ok = await self.oms.submit_order(
            api_id,
            ticker="Paper_Spot_BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            qty=Decimal("0.01"),
            price=Decimal("1"),
        )
        cid = self.oms.last_client_order_id
        cancelled = False
        if ok and cid:
            cancelled = await self.oms.cancel_order(api_id, cid)
        Path(self.paras["traded"]).write_text(
            f"ok={ok} cancel={cancelled} cid={cid} "
            f"s={time.perf_counter() - t0:.4f} "
            f"code={self.oms.last_reject_code}\n",
            encoding="utf-8",
        )

    async def on_order_book(self, book: object) -> None:
        del book
        path = Path(self.paras["books"])
        n = int(path.read_text() or "0") if path.is_file() else 0
        path.write_text(str(n + 1), encoding="utf-8")
'''

_PATH_YAML = """\
td:
  "paper trader": {}
md:
  md:
    - orderbook.Paper_Spot_BTCUSDT
sts:
  start: "/b4-09/start"
  ready: "/b4-09/ready"
  books: "/b4-09/books"
  fill: "/b4-09/fill"
  trade: "/b4-09/trade"
  traded: "/b4-09/traded"
"""

_PID_PY = r"""
import os
import pathlib

needle = os.environ["B4_NEEDLE"].encode()
found = []
for entry in pathlib.Path("/proc").iterdir():
    if not entry.name.isdigit():
        continue
    try:
        command = (entry / "cmdline").read_bytes()
    except OSError:
        continue
    if needle in command:
        found.append(entry.name)
print(" ".join(found))
"""


class AcceptanceError(SystemExit):
    """The compose flow did not meet B4-09. The message is the evidence."""


def _redact(text: str) -> str:
    kept: list[str] = []
    for line in text.splitlines():
        upper = line.upper()
        if any(mark in upper for mark in _SECRET_MARKERS):
            continue
        kept.append(line)
    return "\n".join(kept[-80:])


def _api_url() -> str:
    return os.environ.get("B4_09_API_URL", "http://127.0.0.1:8000").rstrip("/")


def _nats_url() -> str:
    return os.environ.get("B4_09_NATS_URL", "nats://127.0.0.1:4222")


def _compose_env(markers: Path, budget: str | None) -> dict[str, str]:
    env = os.environ.copy()
    env["B4_09_MARKERS"] = str(markers)
    if budget is None:
        env.pop("PROCMAN_MEMORY_BUDGET_MB", None)
        env.pop("PROCMAN_MAX_WORKERS", None)
    else:
        env["PROCMAN_MEMORY_BUDGET_MB"] = budget
        env.pop("PROCMAN_MAX_WORKERS", None)
    return env


def _compose(
    args: list[str], *, markers: Path, budget: str | None, timeout: float = 600
) -> subprocess.CompletedProcess[str]:
    if shutil.which("docker") is None:
        raise AcceptanceError("docker is not installed; compose acceptance skipped")
    cmd = ["docker", "compose", *_FILES, *args]
    completed = subprocess.run(
        cmd,
        cwd=ROOT,
        env=_compose_env(markers, budget),
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if completed.returncode != 0:
        detail = _redact(completed.stdout + "\n" + completed.stderr)
        raise AcceptanceError(f"compose failed ({completed.returncode})\n{detail}")
    return completed


def _compose_out(
    service: str, *args: str, markers: Path, timeout: float = 30
) -> str:
    completed = _compose(
        ["exec", "-T", service, *args],
        markers=markers,
        budget=None,
        timeout=timeout,
    )
    return completed.stdout


def _mftik(
    args: list[str], config: Path, *, timeout: float
) -> subprocess.CompletedProcess[str]:
    binary = Path(sys.executable).parent / "mftik"
    if not binary.is_file():
        binary = Path(shutil.which("mftik") or "")
    env = os.environ.copy()
    env["MFTIK_CONFIG"] = str(config)
    env["MFTIK_AUTH_ENABLED"] = "0"
    return subprocess.run(
        [str(binary), *args],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def _http(method: str, path: str, body: dict | None = None) -> tuple[int, str]:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        _api_url() + path,
        data=data,
        method=method,
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def _wait_http_ok(path: str, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            status, _body = _http("GET", path)
        except OSError:
            status = 0
        if status == 200:
            return
        time.sleep(0.5)
    raise AcceptanceError(f"{path} did not answer 200")


def _psql(sql: str, markers: Path) -> str:
    return _compose_out(
        "postgres",
        "sh",
        "-c",
        "psql -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\" -tAc "
        + shlex.quote(sql),
        markers=markers,
    ).strip()


def _pids(service: str, needle: str, markers: Path) -> list[int]:
    out = _compose_out(
        service,
        "sh",
        "-c",
        "B4_NEEDLE=" + shlex.quote(needle) + " python -c " + shlex.quote(_PID_PY),
        markers=markers,
    )
    return [int(item) for item in out.split() if item.isdigit()]


def _controller_pid(service: str, markers: Path) -> str:
    return _compose_out(
        service,
        "sh",
        "-c",
        "cat /tmp/b4-09-controller.pid",
        markers=markers,
    ).strip()


def _bus_prefix(markers: Path) -> str:
    raw = _compose_out(
        "api",
        "sh",
        "-c",
        'printf %s "${BROKER_KEY_PREFIX:-}"',
        markers=markers,
    ).strip()
    return raw or "mft"


def _instance(service: str, markers: Path) -> str:
    raw = _compose_out(
        service,
        "sh",
        "-c",
        'printf %s "${MFTIK_INSTANCE:-}"',
        markers=markers,
    ).strip()
    return raw or service


def _write_tree(root: Path, source: str, yaml: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "strategy.py").write_text(source, encoding="utf-8")
    (root / "strategy.yml").write_text(yaml, encoding="utf-8")


def _config(path: Path) -> None:
    path.write_text(
        'default = "local"\n\n[profiles.local]\nurl = '
        + json.dumps(_api_url())
        + "\n",
        encoding="utf-8",
    )


def _clear_markers(markers: Path) -> None:
    markers.mkdir(parents=True, exist_ok=True)
    for name in _MARKER_NAMES:
        path = markers / name
        if path.exists():
            path.unlink()


def _books(markers: Path) -> int:
    path = markers / "books"
    if not path.is_file():
        return 0
    return int(path.read_text(encoding="utf-8") or "0")


def _require_capacity(markers: Path, tree: Path, config: Path) -> None:
    _compose(
        ["up", "-d", "--force-recreate", "--no-deps", "sts"],
        markers=markers,
        budget="1",
    )
    _wait_plane("sts", markers, seconds=40)
    pushed = _mftik(["push", str(tree)], config, timeout=60)
    if pushed.returncode != 0:
        raise AcceptanceError(
            "push failed\n" + _redact(pushed.stdout + pushed.stderr)
        )
    status, body = _http(
        "POST",
        "/sts/deploy/private%3A%3AOverBudget",
        {"yaml": _OVER_YAML},
    )
    if status != 503 or "memory_budget_mb" not in body:
        raise AcceptanceError(f"api refusal status={status}")
    cli = _mftik(
        ["run", "--no-push", "--no-wait", str(tree)],
        config,
        timeout=60,
    )
    output = cli.stdout + cli.stderr
    if cli.returncode == 0 or "memory_budget_mb" not in output:
        raise AcceptanceError(
            "mftik run did not refuse the budget\n" + _redact(output)
        )
    reason = _psql(
        "select reason from sts_sessions where type = 'private::OverBudget' "
        "order by created_at desc limit 1",
        markers,
    )
    if "capacity_exceeded" not in reason:
        raise AcceptanceError("failed row has no capacity_exceeded")
    workers = _pids("sts", "mftik_sts.session_worker", markers)
    if workers:
        raise AcceptanceError(f"over-budget start spawned workers {workers}")
    print("b4-09 capacity refused api=503 cli_exit=" + str(cli.returncode))


def _wait_plane(plane: str, markers: Path, *, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    last = "down"
    while time.monotonic() < deadline:
        last = asyncio.run(_probe(plane, markers))
        if last == "ok":
            return
        time.sleep(0.5)
    raise AcceptanceError(f"{plane} health {last}")


async def _probe(plane: str, markers: Path) -> str:
    from mftik.broker import Broker, BrokerConfig
    from mftik.broker.errors import RequestTimeoutError
    from mftik.protocol import Envelope, HealthCheck, Topics

    broker = Broker(
        BrokerConfig(nats_url=_nats_url(), key_prefix=_bus_prefix(markers))
    )
    await broker.connect()
    try:
        await broker.request(
            Topics.health(plane, _instance(plane, markers)),
            Envelope[HealthCheck].wrap(
                HealthCheck(),
                type=f"{plane}.health",
                source="b4-09",
            ),
            timeout=1.5,
        )
    except RequestTimeoutError:
        return "timeout"
    except Exception as exc:
        return type(exc).__name__
    finally:
        await broker.close()
    return "ok"


async def _oms(api_id: int, markers: Path) -> dict[str, object]:
    from mftik.broker import Broker, BrokerConfig
    from mftik.protocol import TD_OMS_VIEW, Envelope, TdOmsViewRequest, Topics

    broker = Broker(
        BrokerConfig(nats_url=_nats_url(), key_prefix=_bus_prefix(markers))
    )
    await broker.connect()
    try:
        reply = await broker.request(
            Topics.td_account(api_id),
            Envelope[TdOmsViewRequest].wrap(
                TdOmsViewRequest(api_id=api_id),
                type=TD_OMS_VIEW,
                source="b4-09",
            ),
            timeout=2,
        )
    finally:
        await broker.close()
    payload = reply.payload if isinstance(reply.payload, dict) else {}
    orders = payload.get("orders") or {}
    return dict(orders)


def _report_ids(envelope: dict[str, object]) -> list[str]:
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        return []
    workers = payload.get("workers") or []
    if not isinstance(workers, list):
        return []
    return [str(item["id"]) for item in workers if isinstance(item, dict)]


def _report_generation(envelope: dict[str, object]) -> int | None:
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        return None
    generation = payload.get("generation")
    return generation if isinstance(generation, int) else None


def _wanted(service: str, ids: list[str]) -> bool:
    if service == "sts":
        return any(item.startswith("sts/session/") for item in ids)
    if service == "td":
        return any(item.startswith("td/account/") for item in ids)
    return "md/fetch" in ids


async def _roll_report(
    subject: str,
    *,
    markers: Path,
    service: str,
    old_pid: str,
    base_gen: int | None,
) -> tuple[dict[str, object] | None, float]:
    """SIGTERM the controller and wait for the new process's report."""
    import nats
    import nats.errors

    client = await nats.connect(_nats_url())
    try:
        sub = await client.subscribe(subject)
        await asyncio.sleep(0)
        started = time.monotonic()
        await asyncio.to_thread(
            _compose,
            [
                "exec",
                "-T",
                service,
                "sh",
                "-c",
                'kill -TERM "$(cat /tmp/b4-09-controller.pid)"',
            ],
            markers=markers,
            budget=None,
        )
        fresh = False
        while time.monotonic() - started < _GC_WINDOW_S:
            if not fresh:
                new = await asyncio.to_thread(
                    _controller_pid, service, markers
                )
                fresh = bool(new and new != old_pid)
            left = _GC_WINDOW_S - (time.monotonic() - started)
            try:
                msg = await sub.next_msg(timeout=min(0.5, max(0.1, left)))
            except nats.errors.TimeoutError:
                continue
            body = json.loads(msg.data.decode())
            if not isinstance(body, dict):
                continue
            generation = _report_generation(body)
            old = (
                base_gen is not None
                and generation is not None
                and generation >= base_gen
                and generation != 1
            )
            if old or not fresh or not _wanted(service, _report_ids(body)):
                continue
            return body, time.monotonic() - started
        return None, time.monotonic() - started
    finally:
        await client.drain()


def _roll(
    service: str,
    needle: str,
    markers: Path,
    *,
    session_id: str,
) -> None:
    instance = _instance(service, markers)
    prefix = _bus_prefix(markers)
    subject = f"{prefix}.ps.procman.report.{service}.{instance}"
    before = _pids(service, needle, markers)
    if service == "sts" and len(before) != 1:
        raise AcceptanceError(f"{service} workers before roll {before}")
    if not before:
        raise AcceptanceError(f"{service} had no {needle} worker")
    old = _controller_pid(service, markers)
    baseline = asyncio.run(_one_report(subject, 6))
    base_gen = _report_generation(baseline) if baseline else None
    report, gap = asyncio.run(
        _roll_report(
            subject,
            markers=markers,
            service=service,
            old_pid=old,
            base_gen=base_gen,
        )
    )
    if report is None:
        raise AcceptanceError(
            f"{service} procman.report missed the {_GC_WINDOW_S:.0f}s window"
        )
    after = _pids(service, needle, markers)
    if sorted(after) != sorted(before):
        raise AcceptanceError(f"{service} worker pids {before} -> {after}")
    status, body = _http("GET", f"/sts/sessions/{session_id}")
    phase = ""
    if status == 200:
        parsed = json.loads(body)
        if isinstance(parsed, dict) and isinstance(parsed.get("phase"), str):
            phase = parsed["phase"]
    if phase != "running":
        raise AcceptanceError(
            f"session phase after {service} roll is {phase!r}"
        )
    print(
        f"b4-09 roll plane={service} action=adopt "
        f"pids={','.join(str(pid) for pid in sorted(after))} "
        f"gap_s={gap:.2f} generation={_report_generation(report)} "
        f"workers={len(_report_ids(report))}"
    )


async def _one_report(
    subject: str, seconds: float
) -> dict[str, object] | None:
    import nats
    import nats.errors

    client = await nats.connect(_nats_url())
    try:
        sub = await client.subscribe(subject)
        try:
            msg = await sub.next_msg(timeout=seconds)
        except nats.errors.TimeoutError:
            return None
        body = json.loads(msg.data.decode())
        return body if isinstance(body, dict) else None
    finally:
        await client.drain()


def _paper_path(markers: Path, tree: Path, config: Path) -> str:
    _clear_markers(markers)
    _compose(
        ["up", "-d", "--force-recreate", "--no-deps", "sts"],
        markers=markers,
        budget=None,
    )
    for plane in ("sts", "td", "md"):
        _wait_plane(plane, markers, seconds=60)
    started = time.perf_counter()
    cli = _mftik(
        ["run", "--wait", "--no-follow", str(tree)],
        config,
        timeout=180,
    )
    if cli.returncode != 0:
        raise AcceptanceError(
            "mftik run failed\n" + _redact(cli.stdout + cli.stderr)
        )
    session_id = ""
    for token in cli.stdout.split():
        if token.startswith("session="):
            session_id = token.split("=", 1)[1]
    if not session_id:
        raise AcceptanceError("mftik run did not print a session id")
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and not (markers / "fill").is_file():
        time.sleep(0.1)
    if not (markers / "fill").is_file() or not (markers / "start").is_file():
        ready = (markers / "ready").read_text(encoding="utf-8") if (
            markers / "ready"
        ).is_file() else ""
        raise AcceptanceError(f"markers missing ready={ready!r}")
    ready = (markers / "ready").read_text(encoding="utf-8")
    if not ready.startswith("ok=True"):
        raise AcceptanceError(f"submit refused {ready.strip()}")
    fill = (markers / "fill").read_text(encoding="utf-8").strip()
    wall = time.perf_counter() - started
    print(f"b4-09 path wall_s={wall:.2f} {ready.strip()} {fill}")
    print("b4-09 fill_source=paper-engine")
    return session_id


def _stop(session_id: str, markers: Path) -> None:
    status, body = _http("POST", f"/sts/sessions/{session_id}/stop")
    if status != 200:
        raise AcceptanceError(f"stop status={status} (#345 item 6 if 503)")
    td = _psql(
        "select count(*) from td_intents where session_id = "
        + _sql_literal(session_id)
        + " and released_at is not null",
        markers,
    )
    md = _psql(
        "select count(*) from md_intents where session_id = "
        + _sql_literal(session_id)
        + " and released_at is not null",
        markers,
    )
    if td in {"", "0"} or md in {"", "0"}:
        raise AcceptanceError(f"intents still held td={td} md={md}")
    print(f"b4-09 end released td={td} md={md}")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _trader_api_id(markers: Path) -> int:
    raw = _psql(
        "select apis.id from accounts "
        "join apis on apis.id = accounts.api_id "
        "where accounts.name = 'paper trader'",
        markers,
    )
    if not raw.isdigit():
        raise AcceptanceError("paper trader account is missing")
    return int(raw)


def main() -> None:
    markers = Path(os.environ.get("B4_09_MARKERS", "/tmp/b4-09-markers"))
    markers.mkdir(parents=True, exist_ok=True)
    os.environ["B4_09_MARKERS"] = str(markers)
    with tempfile.TemporaryDirectory(prefix="b409-e2e-") as tmp:
        root = Path(tmp)
        config = root / "mftik.toml"
        _config(config)
        over = root / "over"
        path = root / "path"
        _write_tree(over, _OVER, _OVER_YAML)
        _write_tree(path, _PATH, _PATH_YAML)
        _compose(
            [
                "up",
                "-d",
                "postgres",
                "redis",
                "nats",
                "migrate",
                "seed",
                "api",
                "paper",
                "sym",
                "td",
                "md",
                "sts",
            ],
            markers=markers,
            budget=None,
        )
        _wait_http_ok("/health", 180)
        _require_capacity(markers, over, config)
        session_id = _paper_path(markers, path, config)
        api_id = _trader_api_id(markers)
        orders = asyncio.run(_oms(api_id, markers))
        books = _books(markers)
        conn = _pids("md", "mftik_md.conn_worker", markers)
        fetch = _pids("md", "mftik_md.fetch", markers)
        print(f"md_conn_workers={len(conn)} md_books={books}")
        if books == 0:
            print(
                "md books stayed 0; the MD process does not spawn conn (B8)"
            )
        if len(fetch) != 1:
            raise AcceptanceError(f"fetch workers {fetch}")
        _roll("sts", "mftik_sts.session_worker", markers, session_id=session_id)
        if asyncio.run(_oms(api_id, markers)) != orders:
            raise AcceptanceError("oms changed across the sts roll")
        if books:
            grown = _books(markers)
            if grown <= books:
                raise AcceptanceError(f"books stuck at {books} after sts")
            books = grown
        _roll("td", "mftik_td.account", markers, session_id=session_id)
        if asyncio.run(_oms(api_id, markers)) != orders:
            raise AcceptanceError("oms changed across the td roll")
        fetch_after_td = _pids("md", "mftik_md.fetch", markers)
        _roll("md", "mftik_md.fetch", markers, session_id=session_id)
        if _pids("md", "mftik_md.fetch", markers) != fetch_after_td:
            raise AcceptanceError("fetch pid changed across the md roll")
        if books:
            if _books(markers) <= books:
                raise AcceptanceError(f"books stuck at {books} after md")
        elif _pids("md", "mftik_md.conn_worker", markers) != conn:
            raise AcceptanceError("conn worker set changed across the md roll")
        (markers / "trade").write_text("1", encoding="utf-8")
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline and not (markers / "traded").is_file():
            time.sleep(0.1)
        if not (markers / "traded").is_file():
            raise AcceptanceError("place/cancel did not run")
        traded = (markers / "traded").read_text(encoding="utf-8")
        if not traded.startswith("ok=True") or "cancel=True" not in traded:
            raise AcceptanceError(traded.strip())
        print(f"b4-09 order {traded.strip()}")
        held = _psql(
            "select count(*) from td_intents where session_id = "
            + _sql_literal(session_id)
            + " and released_at is null",
            markers,
        )
        if held in {"", "0"}:
            raise AcceptanceError("td intent released while the worker was alive")
        _stop(session_id, markers)
    print("b4-09 e2e ok")


if __name__ == "__main__":
    try:
        main()
    except AcceptanceError:
        raise
    except Exception as exc:
        raise AcceptanceError(f"{type(exc).__name__}: {exc}") from exc
