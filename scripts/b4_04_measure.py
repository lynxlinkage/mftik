"""B4-04 measurements: cross-connection no-responders, ack hop, GIL.

The long run prints the numbers §5.3 records. Integration tests call the
same functions with shorter bounds. Nothing here changes product code:
``sys.setswitchinterval`` is restored before the process returns.

    uv run --all-packages python scripts/b4_04_measure.py
    uv run --all-packages python scripts/b4_04_measure.py --gil-s 30 --hops 3000

Needs a NATS server on ``NATS_URL`` (default ``nats://127.0.0.1:4222``).
The server CI and compose start is ``nats:2.11-alpine`` with ``-m 8222``
and no config file. Point the script at that server, not a shared one
that other suites are publishing on: subjects are prefixed, but the GIL
run wants a quiet box.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import sys
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import nats
from mftik.broker import Broker, BrokerConfig
from mftik.broker.errors import RequestTimeoutError
from mftik.clock import SystemClock
from mftik.protocol import (
    TD_ORDER_ACK,
    Envelope,
    OrderAck,
    OrderSubmit,
    Topics,
    UntypedEnvelope,
)

# ``HMSG <subject> <sid> 16 16`` plus ``NATS/1.0 503\\r\\n\\r\\n`` is the
# status nats-server 2.11 writes. 16 is the header byte count and the
# total byte count: the body is empty.
_STATUS_503 = b"NATS/1.0 503\r\n\r\n"
_HDR_LEN = b" 16 16\r\n"

_STRATEGY = """\
import time
from decimal import Decimal
from pathlib import Path

from mftik.exchange.models import OrderType, Side
from mftik.strategy import Strategy


class B404NoTd(Strategy):
    async def on_ready(self, ready) -> None:
        out = Path(self.paras["out"])
        started = time.perf_counter()
        error = ""
        try:
            accepted = await self.oms.submit_order(
                424242,
                ticker="Paper_Spot_BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.01"),
                price=Decimal("1"),
            )
        except Exception as exc:
            accepted = None
            error = f"{type(exc).__name__}: {exc}"
        elapsed = time.perf_counter() - started
        out.write_text(
            f"accepted={accepted}\\n"
            f"elapsed_s={elapsed:.6f}\\n"
            f"code={self.oms.last_reject_code}\\n"
            f"reason={self.oms.last_reject_reason}\\n"
            f"error={error}\\n",
            encoding="utf-8",
        )
        self.exit()
"""


def nats_endpoint() -> tuple[str, int]:
    """Host and port from ``NATS_URL``. No credentials in the result."""
    raw = os.getenv("NATS_URL", "nats://127.0.0.1:4222")
    rest = raw.split("://", 1)[-1]
    if "@" in rest:
        rest = rest.split("@", 1)[1]
    host, _, port = rest.partition(":")
    return host or "127.0.0.1", int(port or "4222")


def broker_config(prefix: str | None = None) -> BrokerConfig:
    """A throwaway prefix so this run does not share subjects."""
    host, port = nats_endpoint()
    return BrokerConfig(
        nats_url=f"nats://{host}:{port}",
        key_prefix=prefix or f"b404-{uuid.uuid4().hex[:10]}",
    )


def summarize(samples: Sequence[float]) -> dict[str, float | int]:
    """min / p50 / p99 / max. Linear rank, so n=1 is that sample."""
    if not samples:
        return {"n": 0, "min": 0.0, "p50": 0.0, "p99": 0.0, "max": 0.0}
    ordered = sorted(samples)

    def pct(p: float) -> float:
        if len(ordered) == 1:
            return ordered[0]
        rank = (len(ordered) - 1) * p
        lo = int(rank)
        hi = min(lo + 1, len(ordered) - 1)
        weight = rank - lo
        return ordered[lo] * (1.0 - weight) + ordered[hi] * weight

    return {
        "n": len(ordered),
        "min": ordered[0],
        "p50": pct(0.50),
        "p99": pct(0.99),
        "max": ordered[-1],
    }


class _SpinGate:
    """Wake the responder once the strategy hook is on the CPU.

    Armed before the publish, pulsed at the first line of the spin.
    The responder does not reply while the gate is armed but not yet
    pulsed, so the ack cannot be queued before the hook starts.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._armed = False
        self._pulsed = False

    def arm(self) -> None:
        with self._lock:
            self._armed = True
            self._pulsed = False

    def pulse(self) -> None:
        with self._lock:
            self._pulsed = True

    def disarm(self) -> None:
        with self._lock:
            self._armed = False
            self._pulsed = False

    def armed(self) -> bool:
        with self._lock:
            return self._armed

    def pulsed(self) -> bool:
        with self._lock:
            return self._pulsed


def _cpu_spin(seconds: float) -> int:
    """Pure-Python work for ``seconds``. Holds the GIL between switches."""
    count = 0
    acc = 0
    end = time.perf_counter() + seconds
    while time.perf_counter() < end:
        acc = (acc + 1) * 3 + count
        count += 1
    return count


def _ms(stats: Mapping[str, float | int]) -> str:
    if not stats or stats.get("n") == 0:
        return "n=0"
    return (
        f"n={stats['n']} min={stats['min'] * 1e3:.3f}ms "
        f"p50={stats['p50'] * 1e3:.3f}ms p99={stats['p99'] * 1e3:.3f}ms "
        f"max={stats['max'] * 1e3:.3f}ms"
    )


def machine_facts() -> dict[str, Any]:
    """CPU, Python, and the libraries this run actually imported."""
    model = "unknown"
    family = ""
    cpu_model_id = ""
    mhz = ""
    try:
        text = Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="replace")
    except OSError:
        text = ""
    for line in text.splitlines():
        key, _, value = line.partition(":")
        key = key.strip()
        value = value.strip()
        if key == "model name" and model == "unknown":
            model = value
        elif key == "cpu family" and not family:
            family = value
        elif key == "model" and not cpu_model_id:
            cpu_model_id = value
        elif key == "cpu MHz" and not mhz:
            mhz = value
    import platform

    uvloop_version = None
    try:
        import uvloop

        uvloop_version = getattr(uvloop, "__version__", None)
    except ImportError:
        uvloop = None  # noqa: F841
    return {
        "cpu_model": model,
        "cpu_family": family,
        "cpu_model_id": cpu_model_id,
        "cpu_mhz": mhz,
        "cores": os.cpu_count(),
        "python": platform.python_version(),
        "nats_py": _dist_version("nats-py"),
        "uvloop": uvloop_version,
        "switch_interval_s": sys.getswitchinterval(),
    }


def _dist_version(name: str) -> str | None:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version(name)
    except PackageNotFoundError:
        return None


def server_version() -> str | None:
    """``/varz`` version when the monitor port is up, else the binary."""
    host, _port = nats_endpoint()
    try:
        with socket.create_connection((host, 8222), timeout=0.5) as sock:
            sock.sendall(
                b"GET /varz HTTP/1.0\r\nHost: localhost\r\n\r\n"
            )
            blob = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                blob += chunk
        body = blob.split(b"\r\n\r\n", 1)[-1]
        payload = json.loads(body.decode())
        version = payload.get("version")
        if isinstance(version, str):
            return version
    except (OSError, json.JSONDecodeError, UnicodeError):
        pass
    return None


def _recv_line(sock: socket.socket, buf: bytearray) -> bytes:
    while b"\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError(f"eof with {bytes(buf)!r}")
        buf.extend(chunk)
    line, _, rest = bytes(buf).partition(b"\r\n")
    buf[:] = rest
    return line


def _raw_connect(
    flags: dict[str, Any],
) -> tuple[socket.socket, bytearray]:
    host, port = nats_endpoint()
    sock = socket.create_connection((host, port), timeout=2)
    sock.settimeout(1.0)
    buf = bytearray()
    info = _recv_line(sock, buf)
    if not info.startswith(b"INFO "):
        sock.close()
        raise RuntimeError(f"expected INFO, got {info!r}")
    payload = {
        "verbose": False,
        "pedantic": False,
        "lang": "b404",
        "version": "0",
        "protocol": 1,
        **flags,
    }
    sock.sendall(f"CONNECT {json.dumps(payload)}\r\nPING\r\n".encode())
    line = _recv_line(sock, buf)
    if line.startswith(b"-ERR"):
        sock.close()
        raise RuntimeError(line.decode())
    while line != b"PONG":
        line = _recv_line(sock, buf)
        if line.startswith(b"-ERR"):
            sock.close()
            raise RuntimeError(line.decode())
    return sock, buf


def _read_window(sock: socket.socket, seconds: float) -> bytes:
    """Read until ``seconds`` of silence, or a 503 plus its PONG."""
    sock.settimeout(seconds)
    chunks = bytearray()
    try:
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            chunks.extend(chunk)
            if _STATUS_503 in chunks and b"PONG\r\n" in chunks:
                break
    except TimeoutError:
        pass
    return bytes(chunks)


def _publish_nobody(sock: socket.socket, reply: str) -> None:
    sock.sendall(
        f"PUB nobody.b404 {reply} 5\r\nhello\r\nPING\r\n".encode()
    )


def probe_no_responders() -> dict[str, Any]:
    """Which connection's CONNECT flags make the server write a 503.

    The 503 is not a message routed to ``reply``. nats-server writes it
    only onto a subscription owned by the publishing connection. See
    ``client.subForReply`` in nats-server 2.11.
    """
    host, port = nats_endpoint()
    info_sock = socket.create_connection((host, port), timeout=2)
    info_buf = bytearray()
    info_line = _recv_line(info_sock, info_buf)
    info_sock.close()
    info = json.loads(info_line[5:])
    both = {"headers": True, "no_responders": True}
    headers_only = {"headers": True, "no_responders": False}

    def run(
        pub_flags: dict[str, Any],
        *,
        pub_sub: bool,
        other_sub: bool,
        other_flags: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        publisher, pbuf = _raw_connect(pub_flags)
        other, obuf = _raw_connect(other_flags or both)
        try:
            if pub_sub:
                publisher.sendall(b"SUB inbox.b404.* 1\r\nPING\r\n")
                if _recv_line(publisher, pbuf) != b"PONG":
                    raise RuntimeError("publisher SUB was not acked")
            if other_sub:
                other.sendall(b"SUB inbox.b404.* 7\r\nPING\r\n")
                if _recv_line(other, obuf) != b"PONG":
                    raise RuntimeError("other SUB was not acked")
            _publish_nobody(publisher, "inbox.b404.abc")
            return {
                "publisher": _read_window(publisher, 0.15),
                "other": _read_window(other, 0.15),
            }
        finally:
            publisher.close()
            other.close()

    nr_without_headers = ""
    try:
        bad, _buf = _raw_connect(
            {"headers": False, "no_responders": True}
        )
        bad.close()
    except RuntimeError as exc:
        nr_without_headers = str(exc)

    return {
        "server_info_version": info.get("version"),
        "server_info_headers": info.get("headers"),
        "publisher_subscribed": run(both, pub_sub=True, other_sub=False),
        "other_only": run(both, pub_sub=False, other_sub=True),
        "both_subscribed": run(both, pub_sub=True, other_sub=True),
        "publisher_headers_without_no_responders": run(
            headers_only, pub_sub=True, other_sub=True
        ),
        "other_without_headers": run(
            both,
            pub_sub=False,
            other_sub=True,
            other_flags={"headers": False, "no_responders": False},
        ),
        "no_responders_without_headers": nr_without_headers,
    }


def _has_503(blob: bytes) -> bool:
    """True when this read is a headers-only 503, not a bare PONG."""
    return _STATUS_503 in blob and _HDR_LEN in blob and b"HMSG " in blob


async def probe_product_connections() -> dict[str, Any]:
    """The flags ``NatsTransport.connect`` actually negotiates.

    Same kwargs as ``NatsTransport.connect``: nats-py 2.15 sets
    ``no_responders`` from the server INFO ``headers`` bit, so both
    product connections advertise it. The 503 still stays on the
    connection that published.
    """
    config = broker_config()
    ingress = Broker(config)
    send = Broker(config)
    await ingress.connect()
    await send.connect()
    token = uuid.uuid4().hex
    inbox = f"_INBOX.{token}.req"
    got: list[tuple[str, str]] = []
    stop = asyncio.Event()
    ready = asyncio.Event()

    async def collect() -> None:
        async for subject, raw in ingress.iter_core(
            f"_INBOX.{token}.*", stop=stop, ready=ready
        ):
            got.append((subject, raw))

    task = asyncio.create_task(collect())
    same: list[Any] = []
    try:
        await ready.wait()
        envelope = Envelope[dict].wrap(
            {"n": 1}, type="b404.probe", source="b404"
        )
        await send.publish_with_reply(
            "nobody.b404.cross", envelope, reply=inbox
        )
        await asyncio.sleep(0.2)
        nc = send.transport.nc  # type: ignore[attr-defined]
        own = nc.new_inbox()

        async def on_msg(msg: Any) -> None:
            same.append(msg)

        sub = await nc.subscribe(own, cb=on_msg)
        await nc.flush()
        started = time.perf_counter()
        await send.publish_with_reply(
            "nobody.b404.same", envelope, reply=own
        )
        deadline = time.perf_counter() + 0.5
        while not same and time.perf_counter() < deadline:
            await asyncio.sleep(0.01)
        elapsed = time.perf_counter() - started
        await sub.unsubscribe()
        header = dict(same[0].header) if same and same[0].header else None
        data = bytes(same[0].data) if same else None
        return {
            "cross_received": list(got),
            "same_header": header,
            "same_data": data,
            "same_elapsed_s": elapsed,
        }
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await ingress.close()
        await send.close()


def _drop_loaded_strategy(key: str, root: Path) -> None:
    """Undo ``load_local_registry`` for this tree.

    The worker imports the strategy as ``_mftik_reg_*``. A later test
    in the same process treats any such module as a controller leak.
    """
    import sys

    from mftik_sts.impl import _REGISTRY

    _REGISTRY.pop(key, None)
    root_s = str(root.resolve())
    for name in list(sys.modules):
        if not name.startswith("_mftik_reg_"):
            continue
        module = sys.modules.get(name)
        if module is None:
            continue
        bits: list[str] = []
        file = getattr(module, "__file__", None)
        if isinstance(file, str):
            bits.append(file)
        for entry in getattr(module, "__path__", []) or []:
            bits.append(str(entry))
        if any(bit.startswith(root_s) for bit in bits):
            sys.modules.pop(name, None)


async def session_no_responders(root: Path) -> dict[str, Any]:
    """One real ``amain`` submit against an api with no TD worker.

    The strategy runs ``submit_order`` from ``on_ready``. The inbox
    handler is the worker's, not a copy.
    """
    import uvloop
    from mftik.protocol import StsCreateSessionRequest
    from mftik.registry.qualify import qualify
    from mftik.registry.store import RegistryStore

    data = root / "data"
    out = root / "out.txt"
    added = RegistryStore(data).add({"strategy.py": _STRATEGY})
    key = qualify("private", added.type)
    names = (
        "MFTIK_DATA",
        "BROKER_KEY_PREFIX",
        "STS_EVENTLOG_DIR",
        "MFTIK_STATUS_FD",
    )
    saved = {name: os.environ.get(name) for name in names}
    os.environ["MFTIK_DATA"] = str(data)
    os.environ["BROKER_KEY_PREFIX"] = f"b404{uuid.uuid4().hex[:8]}"
    os.environ.pop("STS_EVENTLOG_DIR", None)
    os.environ.pop("MFTIK_STATUS_FD", None)
    policy = asyncio.get_event_loop_policy()
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    try:
        from mftik_sts.session_worker.process import amain

        request = StsCreateSessionRequest(
            session_id="b40401",
            created_by=1,
            strategy=key,
            type=key,
            st_paras={"out": str(out)},
        )
        started = time.perf_counter()
        code = await amain(request)
        wall = time.perf_counter() - started
    finally:
        asyncio.set_event_loop_policy(policy)
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        _drop_loaded_strategy(key, data)
    if not out.is_file():
        return {"exit_code": code, "missing_result": True, "wall_s": wall}
    fields = {}
    for line in out.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            fields[k] = v
    accepted_raw = fields.get("accepted")
    return {
        "exit_code": code,
        "wall_s": wall,
        "accepted": accepted_raw == "True",
        "accepted_raw": accepted_raw,
        "elapsed_s": float(fields.get("elapsed_s", "nan")),
        "code": (
            int(fields["code"])
            if fields.get("code", "").isdigit()
            else fields.get("code")
        ),
        "reason": fields.get("reason", ""),
        "error": fields.get("error", ""),
    }


class _Harness:
    """Ingress loop pieces the session worker uses for one reply.

    ``PendingTable.complete`` and ``Broker.iter_core`` are the product
    objects. The loop body is the inbox filter: ``_pv_ok`` then
    ``complete``. ``CrossThreadBroker.request`` is the same register /
    ``publish_with_reply`` / await sequence; the future is created here
    so the done callback can time ``call_soon_threadsafe``.
    """

    def __init__(self, config: BrokerConfig) -> None:
        self.config = config
        self.clock = SystemClock()
        self.token = uuid.uuid4().hex
        self.pending: Any = None
        self.broker: Broker | None = None
        self.stop = asyncio.Event()
        self.ready = asyncio.Event()
        self.md_ready = asyncio.Event()
        self.tasks: list[asyncio.Task[Any]] = []
        self.stamps: dict[str, float] = {}
        self.sent_at: dict[str, float] = {}
        self.hops: list[float] = []
        self.lags: list[float] = []
        self.hook_holds: list[float] = []
        self.read_offsets: list[float] = []
        self.timeouts = 0
        self.md_seen = 0
        self._lock = threading.Lock()

    def inbox_for(self, request_id: str) -> str:
        return f"_INBOX.{self.token}.{request_id}"

    def note_sent(self, request_id: str) -> None:
        with self._lock:
            self.sent_at[request_id] = time.perf_counter()

    async def start(self, *, md_topic: str | None = None) -> None:
        from mftik_sts.session_worker.pending import PendingTable
        from mftik_sts.session_worker.process import _pv_ok

        self.pending = PendingTable()
        self._pv_ok = _pv_ok
        self.broker = Broker(self.config)
        await self.broker.connect()
        self.tasks.append(asyncio.create_task(self._inbox(), name="b404-inbox"))
        self.tasks.append(asyncio.create_task(self._expire(), name="b404-expire"))
        if md_topic is not None:
            self.tasks.append(
                asyncio.create_task(self._md(md_topic), name="b404-md")
            )
        await self.ready.wait()

    async def _inbox(self) -> None:
        assert self.broker is not None
        async for subject, raw in self.broker.iter_core(
            f"_INBOX.{self.token}.*", stop=self.stop, ready=self.ready
        ):
            if not self._pv_ok(raw):
                continue
            request_id = subject.rsplit(".", 1)[-1]
            arrived = time.perf_counter()
            with self._lock:
                self.stamps[request_id] = arrived
                sent = self.sent_at.get(request_id)
                if sent is not None:
                    self.lags.append(arrived - sent)
            self.pending.complete(request_id, raw, now=self.clock.monotonic())

    async def _expire(self) -> None:
        while not self.stop.is_set():
            self.pending.expire(self.clock.monotonic())
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=0.02)
            except TimeoutError:
                continue

    async def _md(self, topic: str) -> None:
        assert self.broker is not None
        async for _topic, raw in self.broker.iter_raw(
            [topic], stop=self.stop, ready=self.md_ready
        ):
            self._pv_ok(raw)
            self.md_seen += 1

    def reset_samples(self) -> None:
        """Drop warmup. Call only when nothing is in flight."""
        with self._lock:
            self.hops.clear()
            self.lags.clear()
            self.hook_holds.clear()
            self.read_offsets.clear()
            self.stamps.clear()
            self.sent_at.clear()
        self.timeouts = 0

    async def close(self) -> None:
        self.stop.set()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.pending is not None:
            self.pending.cancel_all(
                RequestTimeoutError("shutdown", "shutdown", 0.0)
            )
        if self.broker is not None:
            await self.broker.close()

    async def request(
        self,
        send: Broker,
        subject: str,
        envelope: Envelope[Any],
        *,
        timeout: float,
        spin_s: float = 0.0,
        spin_marks: dict[str, float] | None = None,
        gate: _SpinGate | None = None,
    ) -> tuple[UntypedEnvelope | None, float, BaseException | None]:
        """One ``CrossThreadBroker.request``, plus an optional CPU hold.

        The hold starts after ``publish_with_reply`` returns, which is
        the shape of a hook that publishes and then computes. Call this
        from the strategy loop. ``complete`` runs on the ingress loop.
        """
        assert self.pending is not None
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        request_id = envelope.id
        published = asyncio.Event()

        def _done(fut: asyncio.Future[str]) -> None:
            del fut
            now = time.perf_counter()
            with self._lock:
                arrived = self.stamps.get(request_id)
            if arrived is None:
                return
            self.hops.append(now - arrived)
            if spin_s > 0 and spin_marks is not None:
                start = spin_marks.get("start")
                end = spin_marks.get("end")
                # Negative infinity marks "the callback ran before the spin".
                self.read_offsets.append(
                    float("-inf") if start is None else arrived - start
                )
                self.hook_holds.append(
                    float("-inf") if end is None else now - end
                )

        future.add_done_callback(_done)
        self.pending.register(
            request_id,
            loop=loop,
            future=future,
            deadline=self.clock.monotonic() + timeout,
            subject=subject,
            timeout=timeout,
        )
        started = time.perf_counter()
        if gate is not None and spin_s > 0:
            gate.arm()

        async def _wait() -> str:
            try:
                await send.publish_with_reply(
                    subject, envelope, reply=self.inbox_for(request_id)
                )
            except Exception as exc:
                self.pending.cancel(request_id, exc)
                published.set()
                raise
            if spin_s > 0:
                # No await between the flush and this spin. The gate
                # was armed before the publish, and pulsed here, so the
                # reply is still in the responder. Yielding first would
                # let a queued ack complete the future before the hook.
                started_spin = time.perf_counter()
                if gate is not None:
                    gate.pulse()
                count = _cpu_spin(spin_s)
                if spin_marks is not None:
                    spin_marks["start"] = started_spin
                    spin_marks["end"] = time.perf_counter()
                    spin_marks["iterations"] = float(count)
            published.set()
            return await future

        task = asyncio.create_task(_wait())
        try:
            await published.wait()
            error: BaseException | None = None
            raw: str | None = None
            try:
                raw = await task
            except BaseException as exc:
                error = exc
                self.timeouts += 1
            wall = time.perf_counter() - started
            parsed = UntypedEnvelope.from_json(raw) if raw is not None else None
            return parsed, wall, error
        finally:
            if gate is not None and spin_s > 0:
                gate.disarm()


async def _serve_acks(
    config: BrokerConfig,
    subject: str,
    stop: threading.Event,
    note_sent: Callable[[str], None],
    *,
    delay_s: float = 0.0,
    gate: _SpinGate | None = None,
) -> None:
    """Reply to ``subject`` on this thread's loop.

    ``stop`` is a ``threading.Event`` because the caller lives on the
    ingress loop and an ``asyncio.Event`` is bound to the loop that
    waits on it.
    """
    broker = Broker(config)
    await broker.connect()
    local = asyncio.Event()

    async def _bridge() -> None:
        while not stop.is_set():
            await asyncio.sleep(0.02)
        local.set()

    bridge = asyncio.create_task(_bridge())
    try:
        async for req in broker.serve(subject, stop=local):
            if gate is not None and gate.armed():
                deadline = time.perf_counter() + 1.0
                while not gate.pulsed() and gate.armed():
                    if time.perf_counter() >= deadline:
                        break
                    await asyncio.sleep(0.0005)
            if delay_s > 0:
                await asyncio.sleep(delay_s)
            body = req.envelope.payload
            payload = body if isinstance(body, dict) else {}
            ack = OrderAck(
                api_id=int(payload.get("api_id", 1)),
                client_order_id=str(payload.get("client_order_id", "cid")),
                accepted=True,
            )
            note_sent(req.envelope.id)
            await req.reply(
                Envelope[OrderAck].wrap(ack, type=TD_ORDER_ACK, source="b404")
            )
    finally:
        local.set()
        bridge.cancel()
        await asyncio.gather(bridge, return_exceptions=True)
        await broker.close()


def _start_loop(name: str, coro: Any) -> tuple[threading.Thread, asyncio.Future[Any]]:
    """Run ``coro`` on a uvloop thread. The future lives on the caller loop."""
    import uvloop

    caller = asyncio.get_running_loop()
    done: asyncio.Future[Any] = caller.create_future()

    def _thread() -> None:
        loop = uvloop.new_event_loop()
        try:
            result = loop.run_until_complete(coro)
            caller.call_soon_threadsafe(done.set_result, result)
        except BaseException as exc:
            caller.call_soon_threadsafe(done.set_exception, exc)
        finally:
            loop.close()

    thread = threading.Thread(target=_thread, name=name, daemon=True)
    thread.start()
    return thread, done


def _publish_md(
    config: BrokerConfig,
    topic: str,
    stop: threading.Event,
    target_per_s: float,
) -> int:
    """Own one uvloop and publish until ``stop``. Called from a thread."""
    import uvloop

    loop = uvloop.new_event_loop()
    try:
        return loop.run_until_complete(
            _publish_md_async(config, topic, stop, target_per_s)
        )
    finally:
        loop.close()


async def _publish_md_async(
    config: BrokerConfig,
    topic: str,
    stop: threading.Event,
    target_per_s: float,
) -> int:
    broker = Broker(config)
    await broker.connect()
    envelope = Envelope[dict].wrap(
        {"bid": "1", "ask": "2"}, type="md.best_quote", source="b404"
    )
    interval = 1.0 / target_per_s if target_per_s > 0 else 0.0
    count = 0
    nxt = time.perf_counter()
    try:
        while not stop.is_set():
            await broker.publish(topic, envelope)
            count += 1
            if interval <= 0:
                continue
            nxt += interval
            pause = nxt - time.perf_counter()
            if pause > 0:
                await asyncio.sleep(pause)
            elif pause < -interval:
                nxt = time.perf_counter()
        await broker.flush()
    finally:
        await broker.close()
    return count


def _order(subject_api: int = 1) -> Envelope[OrderSubmit]:
    return Envelope[OrderSubmit].wrap(
        OrderSubmit(
            session_id="b40402",
            api_id=subject_api,
            universal_ticker="Paper_Spot_BTCUSDT",
            side="buy",
            type="limit",
            qty="0.01",
            price="1",
            client_order_id=uuid.uuid4().hex[:16],
        ),
        type="sts.order.submit",
        source="b404",
        session_id="b40402",
    )


async def measure_hops(
    *,
    n_idle: int,
    n_md: int,
    n_spin: int,
    spin_s: float,
    md_target_per_s: float,
    warmup: int = 20,
    timeout_s: float = 2.0,
) -> dict[str, Any]:
    """Hop from ingress ``iter_core`` to the strategy future's callback.

    Three conditions, sequential, each on its own connections. ``lag`` is
    the responder's ``reply`` call to that yield. ``hop`` is the yield to
    the strategy done-callback. ``wall`` includes the optional spin.
    """

    async def _run(
        n: int,
        *,
        spin: float,
        md: bool,
        gate: _SpinGate | None = None,
    ) -> dict[str, Any]:
        config = broker_config()
        subject = f"b404.hop.{uuid.uuid4().hex[:8]}"
        md_topic = f"b404.md.{uuid.uuid4().hex[:8]}"
        harness = _Harness(config)
        stop_md = threading.Event()
        serve_stop = threading.Event()
        md_thread: threading.Thread | None = None
        md_sent: list[int] = [0]
        await harness.start(md_topic=md_topic if md else None)
        _thread, serve_done = _start_loop(
            "b404-td",
            _serve_acks(
                config,
                subject,
                serve_stop,
                harness.note_sent,
                gate=gate,
            ),
        )
        del _thread
        if md:
            await harness.md_ready.wait()

            def _md() -> None:
                md_sent[0] = _publish_md(
                    config, md_topic, stop_md, md_target_per_s
                )

            md_thread = threading.Thread(target=_md, name="b404-md-pub", daemon=True)
            md_thread.start()

        async def _drive() -> dict[str, Any]:
            send = Broker(config)
            await send.connect()
            walls: list[float] = []
            errors: list[str] = []
            seen_before = 0
            timed = 0.0
            try:
                # The serve subscription has to be on the server before
                # the timed samples. A short retry covers that.
                for _ in range(40):
                    _parsed, _wall, error = await harness.request(
                        send, subject, _order(), timeout=0.25, spin_s=0.0
                    )
                    if error is None:
                        break
                    await asyncio.sleep(0.02)
                else:
                    raise RuntimeError("ack responder did not answer")
                harness.reset_samples()
                for _ in range(warmup):
                    await harness.request(
                        send, subject, _order(), timeout=timeout_s, spin_s=0.0
                    )
                harness.reset_samples()
                seen_before = harness.md_seen
                started_timed = time.perf_counter()
                for _ in range(n):
                    marks: dict[str, float] = {}
                    _parsed, wall, error = await harness.request(
                        send,
                        subject,
                        _order(),
                        timeout=timeout_s,
                        spin_s=spin,
                        spin_marks=marks if spin > 0 else None,
                        gate=gate,
                    )
                    walls.append(wall)
                    if error is not None:
                        errors.append(f"{type(error).__name__}: {error}")
                timed = time.perf_counter() - started_timed
            finally:
                await send.close()
            return {
                "walls": walls,
                "errors": errors,
                "seen_before": seen_before,
                "timed_s": timed,
            }

        started = time.perf_counter()
        _strat, strat_done = _start_loop("b404-strategy", _drive())
        del _strat
        try:
            driven = await strat_done
        finally:
            stop_md.set()
            serve_stop.set()
            if md_thread is not None:
                md_thread.join(timeout=2)
            try:
                await asyncio.wait_for(serve_done, timeout=2)
            except (TimeoutError, Exception):
                pass
            await harness.close()
        elapsed = time.perf_counter() - started
        timed = float(driven["timed_s"])
        md_seen = harness.md_seen - int(driven["seen_before"])
        return {
            "hop": summarize(harness.hops),
            "lag": summarize(harness.lags),
            "hook_hold": summarize(harness.hook_holds),
            "read_offset": summarize(harness.read_offsets),
            "wall": summarize(driven["walls"]),
            "timeouts": harness.timeouts,
            "errors": list(driven["errors"])[:5],
            "md_seen": md_seen,
            "md_sent": md_sent[0],
            "md_per_s": (md_seen / timed) if timed else 0.0,
            "elapsed_s": elapsed,
            "reply_gated": gate is not None,
        }

    idle = await _run(n_idle, spin=0.0, md=False)
    busy = await _run(n_md, spin=0.0, md=True)
    # The responder waits for the spin to start, then replies at once.
    # A fixed sleep races the flush: the ack is often queued before the
    # hook, and the hop collapses to the idle number.
    hooked = await _run(
        n_spin, spin=spin_s, md=False, gate=_SpinGate() if spin_s > 0 else None
    )
    return {
        "idle": idle,
        "md": busy,
        "cpu_hook": hooked,
        "spin_s": spin_s,
    }


class _RecordingClock(SystemClock):
    """``heartbeat_loop``'s clock. Records how late each sleep returned."""

    def __init__(self) -> None:
        self.entered: list[float] = []
        self.overrun: list[float] = []

    async def sleep(self, seconds: float) -> None:
        started = time.perf_counter()
        self.entered.append(started)
        await super().sleep(seconds)
        self.overrun.append(time.perf_counter() - started - seconds)


async def measure_gil(
    *,
    seconds: float,
    intervals: Sequence[float],
    ping_every_s: float = 0.05,
    ack_delay_s: float = 0.3,
    timeout_s: float = 2.0,
) -> dict[str, Any]:
    """Heartbeat jitter and inbox delay while a hook holds the GIL.

    The hook is a pure-Python spin on the strategy thread. Ingress runs
    the real ``heartbeat_loop`` at ``HEARTBEAT_PERIOD_S`` and reads a
    ping plus one delayed ack. The ack is completed on the ingress
    clock; the strategy future runs when the spin returns.
    """
    from mftik.procman.heartbeat import heartbeat_loop
    from mftik_sts.session_worker.limits import HEARTBEAT_PERIOD_S

    rows = []
    previous = sys.getswitchinterval()
    try:
        for interval in intervals:
            sys.setswitchinterval(interval)
            rows.append(
                await _one_interval(
                    seconds=seconds,
                    ping_every_s=ping_every_s,
                    ack_delay_s=ack_delay_s,
                    timeout_s=timeout_s,
                    heartbeat_period_s=HEARTBEAT_PERIOD_S,
                    heartbeat_loop=heartbeat_loop,
                    interval=interval,
                )
            )
    finally:
        sys.setswitchinterval(previous)
    solo = _solo_throughput(intervals, seconds=min(0.8, max(0.2, seconds / 4)))
    return {"intervals": rows, "solo_iterations_per_s": solo}


async def _one_interval(
    *,
    seconds: float,
    ping_every_s: float,
    ack_delay_s: float,
    timeout_s: float,
    heartbeat_period_s: float,
    heartbeat_loop: Any,
    interval: float,
) -> dict[str, Any]:
    config = broker_config()
    subject = f"b404.gil.{uuid.uuid4().hex[:8]}"
    ping_subject = f"b404.ping.{uuid.uuid4().hex[:8]}"
    harness = _Harness(config)
    await harness.start()
    clock = _RecordingClock()
    hb_stop = asyncio.Event()
    hb = asyncio.create_task(
        heartbeat_loop(
            clock,
            ready=lambda: True,
            period_s=heartbeat_period_s,
            stop=hb_stop,
            fd=None,
        ),
        name="b404-hb",
    )
    delays: list[float] = []
    ping_stop = asyncio.Event()
    ping_ready = asyncio.Event()

    async def _pings() -> None:
        assert harness.broker is not None
        async for _subject, raw in harness.broker.iter_core(
            ping_subject, stop=ping_stop, ready=ping_ready
        ):
            try:
                sent = float(raw)
            except ValueError:
                continue
            delays.append(time.perf_counter() - sent)

    ping_task = asyncio.create_task(_pings(), name="b404-ping")
    await ping_ready.wait()
    serve_stop = threading.Event()
    _thread, serve_done = _start_loop(
        "b404-gil-td",
        _serve_acks(
            config,
            subject,
            serve_stop,
            harness.note_sent,
            delay_s=ack_delay_s,
        ),
    )
    del _thread
    ping_thread_stop = threading.Event()

    def _ping_pub() -> None:
        import uvloop

        loop = uvloop.new_event_loop()

        async def _run() -> None:
            nc = await nats.connect(
                config.nats_url,
                max_reconnect_attempts=-1,
                pending_size=8 * 1024 * 1024,
                allow_reconnect=False,
            )
            try:
                while not ping_thread_stop.is_set():
                    stamp = f"{time.perf_counter():.9f}"
                    await nc.publish(ping_subject, stamp.encode())
                    await nc.flush()
                    await asyncio.sleep(ping_every_s)
            finally:
                await nc.close()

        try:
            loop.run_until_complete(_run())
        finally:
            loop.close()

    publisher = threading.Thread(target=_ping_pub, name="b404-ping-pub", daemon=True)
    publisher.start()
    # Let one ping land so the subscription is hot, then the hook.
    await asyncio.sleep(ping_every_s + 0.05)

    async def _hook() -> dict[str, Any]:
        """Connect on this loop, then hold it after the publish flushes.

        The reply is delayed so it arrives during the spin. Ingress can
        read it only when this thread releases the GIL.
        """
        send = Broker(config)
        await send.connect()
        marks: dict[str, float] = {}
        try:
            warmed = False
            for _ in range(40):
                _parsed, _wall, error = await harness.request(
                    send,
                    subject,
                    _order(),
                    timeout=max(timeout_s, ack_delay_s + 0.5),
                    spin_s=0.0,
                )
                if error is None:
                    warmed = True
                    break
                await asyncio.sleep(0.02)
            if not warmed:
                raise RuntimeError("ack responder did not answer")
            harness.reset_samples()
            _parsed, wall, error = await harness.request(
                send,
                subject,
                _order(),
                timeout=timeout_s,
                spin_s=seconds,
                spin_marks=marks,
            )
        finally:
            await send.close()
        return {
            "wall": wall,
            "error": error,
            "ok": error is None,
            "iterations": marks.get("iterations", 0.0),
            "start": marks.get("start"),
            "end": marks.get("end"),
        }

    _strat, strat_done = _start_loop("b404-spin", _hook())
    del _strat
    try:
        outcome = await strat_done
    finally:
        ping_thread_stop.set()
        publisher.join(timeout=2)
        ping_stop.set()
        hb_stop.set()
        serve_stop.set()
        ping_task.cancel()
        hb.cancel()
        await asyncio.gather(ping_task, hb, return_exceptions=True)
        try:
            await asyncio.wait_for(serve_done, timeout=2)
        except (TimeoutError, Exception):
            pass
        await harness.close()
    intervals = [
        clock.entered[i] - clock.entered[i - 1]
        for i in range(1, len(clock.entered))
    ]
    with harness._lock:
        ack_at = next(iter(harness.stamps.values()), None)
    spin_start = outcome.get("start")
    spin_end = outcome.get("end")
    read_during_spin = False
    if ack_at is not None and spin_start is not None and spin_end is not None:
        read_during_spin = spin_start < ack_at < spin_end
    from mftik.strategy.oms import ORDER_ACK_TIMEOUT_S
    from mftik_sts.controller.defaults import SESSION_HB_TIMEOUT_S

    return {
        "switch_interval_s": interval,
        "spin_s": seconds,
        "iterations": outcome.get("iterations"),
        "iterations_per_s": (
            outcome["iterations"] / seconds if outcome.get("iterations") else 0
        ),
        "ack_ok": bool(outcome.get("ok")),
        "ack_error": (
            None
            if outcome.get("error") is None
            else f"{type(outcome['error']).__name__}: {outcome['error']}"
        ),
        "ack_wall_s": outcome.get("wall"),
        "ack_read_during_spin": read_during_spin,
        "ack_read_offset_s": (
            None if ack_at is None or spin_start is None else ack_at - spin_start
        ),
        "heartbeat_interval": summarize(intervals),
        "heartbeat_overrun": summarize(clock.overrun),
        "heartbeat_samples": len(clock.overrun),
        "inbox_delay": summarize(delays),
        "hb_timeout_s": SESSION_HB_TIMEOUT_S,
        "ack_timeout_s": ORDER_ACK_TIMEOUT_S,
        "hb_at_risk": any(gap >= SESSION_HB_TIMEOUT_S for gap in intervals)
        or any(late >= SESSION_HB_TIMEOUT_S for late in clock.overrun),
        "ack_at_risk": not bool(outcome.get("ok")),
    }


def _solo_throughput(intervals: Sequence[float], *, seconds: float) -> dict[str, float]:
    """Iterations per second of :func:`_cpu_spin` with no other thread."""
    previous = sys.getswitchinterval()
    out: dict[str, float] = {}
    try:
        for interval in intervals:
            sys.setswitchinterval(interval)
            count = _cpu_spin(seconds)
            out[f"{interval:.4f}"] = count / seconds
    finally:
        sys.setswitchinterval(previous)
    return out


async def measure_paper(*, n: int, timeout_s: float = 2.0) -> dict[str, Any]:
    """``submit_order`` wall time against the real paper account worker."""
    from decimal import Decimal

    from mftik.exchange import PaperExchange
    from mftik.exchange.models import OrderType, Side
    from mftik.procman import CloseMode, Supervisor, WorkerPhase, log_path
    from mftik.strategy import Strategy
    from mftik.strategy.eventlog import EventLog
    from mftik_db.models import Base
    from mftik_db.models.api import Api
    from mftik_db.models.instance import Instance
    from mftik_db.models.user import User
    from mftik_paper.rpc import dispatch
    from mftik_td.controller.defaults import (
        ACCOUNT_HB_TIMEOUT_S,
        ACCOUNT_START_TIMEOUT_S,
        ACCOUNT_STOP_GRACE_S,
    )
    from mftik_td.controller.types import BoundAccount, account_worker_id
    from mftik_td.controller.worker import account_worker_spec
    from mftik_td.supervise import account_worker_argv
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    root = Path("/tmp") / f"b404-paper-{uuid.uuid4().hex[:8]}"
    root.mkdir(parents=True)
    db_path = root / "td.db"
    url = f"sqlite+aiosqlite:///{db_path}"
    sync = f"sqlite:///{db_path}"
    engine = create_async_engine(url)
    api_id = 0
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as session:
            session.add(User(id=1, email="b404@test.invalid"))
            await session.flush()
            instance = Instance(name="td", domain="td", enabled=True)
            session.add(instance)
            await session.flush()
            row = Api(
                owner_id=1,
                venue="Paper",
                api_key="b404-paper-key",
                api_secret="b404-paper-secret",
                instance_id=instance.id,
                cancel_on_disconnect=False,
            )
            session.add(row)
            await session.commit()
            api_id = int(row.id)
    finally:
        await engine.dispose()

    exchange = PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=60,
    )
    exchange.register_api(
        "b404-paper-key",
        "b404-paper-secret",
        balances={"USDT": Decimal("1000000")},
    )
    await exchange.start()
    config = broker_config()
    paper_stop = asyncio.Event()
    paper_broker = Broker(config)
    await paper_broker.connect()

    async def _paper() -> None:
        async for req in paper_broker.serve(Topics.PAPER, stop=paper_stop):
            await dispatch(req, exchange=exchange)

    paper_task = asyncio.create_task(_paper(), name="b404-paper-rpc")
    work = root / "work"
    supervisor = Supervisor(work, plane="td", instance="td", budget=None)
    harness = _Harness(config)
    walls: list[float] = []
    errors: list[str] = []
    try:
        await supervisor.start()
        env = dict(os.environ)
        env.pop("MFTIK_STATUS_FD", None)
        env["DATABASE_URL"] = url
        env["DATABASE_URL_SYNC"] = sync
        env["NATS_URL"] = config.nats_url
        env["BROKER_KEY_PREFIX"] = config.key_prefix
        env["BROKER_REQUEST_TIMEOUT"] = "0.5"
        spec = account_worker_spec(
            BoundAccount(api_id=api_id, venue="Paper", instance="td"),
            incarnation=1,
            argv=account_worker_argv(api_id, 1, False),
            code_ref="b404",
            start_timeout_s=ACCOUNT_START_TIMEOUT_S,
            hb_timeout_s=ACCOUNT_HB_TIMEOUT_S,
            stop_grace_s=ACCOUNT_STOP_GRACE_S,
            env=env,
        )
        await supervisor.spawn(spec)
        deadline = time.perf_counter() + 6.5
        ready = False
        phase = None
        while time.perf_counter() < deadline:
            status = await supervisor.status(account_worker_id(api_id))
            if status is not None:
                phase = status.phase
                if status.ready:
                    ready = True
                    break
                if status.phase in (
                    WorkerPhase.FAILED,
                    WorkerPhase.CRASHED,
                    WorkerPhase.FATAL,
                ):
                    break
            await asyncio.sleep(0.05)
        if not ready:
            logs = _worker_logs(work, api_id, log_path, account_worker_id)
            raise RuntimeError(f"paper worker not ready phase={phase}\n{logs}")
        await harness.start()

        async def _submit() -> None:
            local = Broker(config)
            await local.connect()
            try:
                strategy = Strategy()
                session = _PaperSession(
                    local, EventLog("b40402", directory=None)
                )
                strategy.bind(session)
                session.strategy = strategy
                # Warm one order, then time ``n`` submits. This thread
                # owns the send connection. ``harness`` completes the
                # replies on the ingress loop, which is CrossThreadBroker.
                from mftik_sts.session_worker.process import CrossThreadBroker

                session.broker = CrossThreadBroker(
                    local, harness.pending, harness.inbox_for, harness.clock
                )
                for i in range(n + 1):
                    started = time.perf_counter()
                    try:
                        accepted = await strategy.oms.submit_order(
                            api_id,
                            ticker="Paper_Spot_BTCUSDT",
                            side=Side.BUY,
                            type=OrderType.LIMIT,
                            qty=Decimal("0.01"),
                            price=Decimal("1"),
                        )
                    except Exception as exc:
                        if i > 0:
                            errors.append(f"{type(exc).__name__}: {exc}")
                        continue
                    if i == 0:
                        continue
                    walls.append(time.perf_counter() - started)
                    if not accepted:
                        errors.append(
                            f"nack code={strategy.oms.last_reject_code} "
                            f"reason={strategy.oms.last_reject_reason}"
                        )
            finally:
                await local.close()

        _thread, done = _start_loop("b404-paper-strategy", _submit())
        del _thread
        await done
    finally:
        paper_stop.set()
        paper_task.cancel()
        await asyncio.gather(paper_task, return_exceptions=True)
        await paper_broker.close()
        await harness.close()
        try:
            await supervisor.close(CloseMode.STOP)
        except Exception:
            pass
        await exchange.stop()
    return {
        "n": n,
        "api_id": api_id,
        "wall": summarize(walls),
        "errors": errors[:5],
        "timeout_s": timeout_s,
    }


def _worker_logs(work: Path, api_id: int, log_path: Any, worker_id: Any) -> str:
    chunks = []
    for stream in ("stderr", "stdout"):
        path = log_path(work, worker_id(api_id), stream)
        if path.is_file():
            chunks.append(f"--- {stream} ---\n{path.read_text()[-2000:]}")
    return "\n".join(chunks)


class _PaperSession:
    """The attributes ``StrategyOms.submit_order`` reads. Not a worker."""

    def __init__(self, broker: Any, event_log: Any) -> None:
        self.session_id = "b40402"
        self.type = "b404"
        self.broker = broker
        self.event_log = event_log
        self.order_phase = "running"
        self.strategy: Any = None
        self.symbols = None
        self.td: dict[str, Any] = {}
        self.md: dict[str, Any] = {}
        self.td_api_ids: list[int] = []

    def request_exit(
        self, reason: str = "strategy_exit", *, failed: bool = False
    ) -> None:
        del reason, failed

    def td_account(self, name: str) -> Any:
        raise KeyError(name)

    def td_sole(self) -> int:
        raise RuntimeError("no sole account")


def _report(payload: Mapping[str, Any]) -> str:
    head = {
        "machine": payload.get("machine"),
        "server": payload.get("server"),
    }
    lines = ["B4-04", json.dumps(head, ensure_ascii=False)]
    if "no_responders" in payload:
        nr = payload["no_responders"]
        lines.append(
            "no-responders "
            f"server={nr.get('server_info_version')} "
            f"headers={nr.get('server_info_headers')} "
            f"publisher_503={_has_503(nr['publisher_subscribed']['publisher'])} "
            f"other_only_len={len(nr['other_only']['other'])} "
            f"both_other_len={len(nr['both_subscribed']['other'])} "
            "without_flag_503="
            f"{_has_503(nr['publisher_headers_without_no_responders']['publisher'])} "
            f"nr_without_headers={nr.get('no_responders_without_headers')!r}"
        )
    if "product" in payload:
        product = payload["product"]
        lines.append(
            "product "
            f"cross={product['cross_received']!r} "
            f"same_header={product['same_header']!r} "
            f"same_data={product['same_data']!r} "
            f"same_elapsed_s={product['same_elapsed_s']:.4f}"
        )
    if "session" in payload:
        lines.append("session " + json.dumps(payload["session"], default=str))
    if "hops" in payload:
        for name, row in payload["hops"].items():
            if not isinstance(row, dict) or "hop" not in row:
                continue
            lines.append(
                f"hop {name} hop[{_ms(row['hop'])}] lag[{_ms(row['lag'])}] "
                f"hold[{_ms(row['hook_hold'])}] "
                f"read_off[{_ms(row['read_offset'])}] "
                f"wall[{_ms(row['wall'])}] timeouts={row['timeouts']} "
                f"md_per_s={row['md_per_s']:.0f} md_seen={row['md_seen']}"
            )
        lines.append(f"spin_s={payload['hops'].get('spin_s')}")
    if "gil" in payload:
        for row in payload["gil"]["intervals"]:
            lines.append(
                "gil "
                f"interval={row['switch_interval_s']} "
                f"iter/s={row['iterations_per_s']:.0f} "
                f"ack_ok={row['ack_ok']} "
                f"read_during_spin={row['ack_read_during_spin']} "
                f"read_offset_s={row['ack_read_offset_s']} "
                f"hb_interval[{_ms(row['heartbeat_interval'])}] "
                f"hb_overrun[{_ms(row['heartbeat_overrun'])}] "
                f"inbox[{_ms(row['inbox_delay'])}] "
                f"hb_at_risk={row['hb_at_risk']} ack_at_risk={row['ack_at_risk']}"
            )
        lines.append(
            "solo " + json.dumps(payload["gil"]["solo_iterations_per_s"])
        )
    if "paper" in payload:
        paper = payload["paper"]
        lines.append(
            f"paper n={paper['n']} wall[{_ms(paper['wall'])}] errors={paper['errors']}"
        )
    return "\n".join(lines)


async def run(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "machine": machine_facts(),
        "server": server_version(),
    }
    payload["no_responders"] = probe_no_responders()
    payload["product"] = await probe_product_connections()
    payload["session"] = await session_no_responders(Path(args.session_dir))
    payload["hops"] = await measure_hops(
        n_idle=args.hops,
        n_md=args.hops,
        n_spin=args.spin_n,
        spin_s=args.spin_s,
        md_target_per_s=args.md_per_s,
        warmup=args.warmup,
    )
    payload["gil"] = await measure_gil(
        seconds=args.gil_s,
        intervals=tuple(args.intervals),
        ping_every_s=args.ping_every,
        ack_delay_s=args.ack_delay,
    )
    if not args.skip_paper:
        payload["paper"] = await measure_paper(n=args.paper_n)
    return payload


def _parse(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hops", type=int, default=3000)
    parser.add_argument("--spin-n", type=int, default=400)
    parser.add_argument("--spin-s", type=float, default=0.1)
    parser.add_argument("--md-per-s", type=float, default=8000)
    parser.add_argument("--warmup", type=int, default=30)
    parser.add_argument("--gil-s", type=float, default=30.0)
    parser.add_argument(
        "--intervals",
        type=float,
        nargs="+",
        default=(0.005, 0.001, 0.0005),
    )
    parser.add_argument("--ping-every", type=float, default=0.05)
    parser.add_argument("--ack-delay", type=float, default=0.3)
    parser.add_argument("--paper-n", type=int, default=100)
    parser.add_argument("--skip-paper", action="store_true")
    parser.add_argument(
        "--session-dir",
        default="",
        help="directory for the no-responders worker tree",
    )
    parser.add_argument("--json-out", default="")
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="short run used to check the script, not the numbers in §5.3",
    )
    args = parser.parse_args(argv)
    if args.smoke:
        args.hops = 40
        args.spin_n = 8
        args.spin_s = 0.1
        args.md_per_s = 2000
        args.warmup = 5
        args.gil_s = 1.2
        args.paper_n = 10
        args.ping_every = 0.05
    if not args.session_dir:
        args.session_dir = f"/tmp/b404-session-{uuid.uuid4().hex[:8]}"
    return args


def main(argv: Sequence[str] | None = None) -> int:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    args = _parse(argv)
    payload = asyncio.run(run(args))
    text = _report(payload)
    print(text)
    if args.json_out:
        Path(args.json_out).write_text(
            json.dumps(payload, default=_json_default, indent=2),
            encoding="utf-8",
        )
    return 0


def _json_default(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    raise TypeError(type(value).__name__)


if __name__ == "__main__":
    raise SystemExit(main())
