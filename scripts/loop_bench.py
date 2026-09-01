"""Measure what this node's hot paths cost on asyncio vs uvloop.

Evidence for `docs/EventLoop.md`. A loop swap is the kind of change that is
argued about with numbers from someone else's benchmark, so this one runs
*this* codebase: the real `Broker`, the real pydantic envelopes, against a real
Redis. Nothing here re-implements a hot path in order to time it.

Loop choice is a process-wide decision, so the parent process only orchestrates
— every case runs in a child that installs one loop and prints one JSON line.
Cases are interleaved across loops rather than run in blocks, so a noisy
neighbour on the box biases both loops instead of whichever went second.

    just loop-bench                       # every case, 5 reps, both loops
    just loop-bench --case rpc --reps 20
    just loop-bench --probe               # behaviour, not throughput

Needs a Redis nobody else is using — it publishes thousands of messages and
writes a tape stream under its own key prefix. Point it somewhere scratch:

    REDIS_URL=redis://localhost:6379/9 just loop-bench

**tcp_echo is the control, and reading it first is the point.** It is the
workload uvloop exists to win: a raw transport with no library above it. If it
does not show a gain, the harness is broken and no other number here means
anything. Every case above it in the table has more Python between the socket
and the payload, and the gain shrinks as that grows — which is the finding,
not a flaw in the measurement.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import resource
import statistics
import subprocess
import sys
import time

from mftik.broker.client import Broker
from mftik.broker.config import BrokerConfig
from mftik.protocol import Topics, UntypedEnvelope

#: Key prefix for everything this script writes, so it cannot be mistaken for
#: a node's own state and can be dropped wholesale.
KEY_PREFIX = "loopbench"


def broker_config() -> BrokerConfig:
    return BrokerConfig(
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        key_prefix=KEY_PREFIX,
    )


def cpu_seconds() -> float:
    """CPU burned by this process, user + system.

    Reported alongside wall time because they answer different questions. Wall
    time says whether a strategy sees its print sooner; CPU says how much of
    the box the plane needs to keep up, which is what decides how many
    sessions fit before anything has to scale out.
    """
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


def quote_envelope(seq: int) -> UntypedEnvelope:
    """An envelope shaped like the best-quote pushes MD fans out."""
    return UntypedEnvelope.wrap(
        {
            "universal_ticker": "binance.spot.BTC/USDT",
            "bid": "63501.10",
            "bid_qty": "0.412",
            "ask": "63501.20",
            "ask_qty": "1.883",
            "event_ms": 1_724_000_000_000 + seq,
            "seq": seq,
        },
        type="best_quote",
        source="md",
    )


# --- cases -----------------------------------------------------------------


async def case_fanout(messages: int = 4000, sessions: int = 8) -> dict:
    """MD's hot path: `Dispatcher.publish` as it is written today.

    One awaited PUBLISH per subscribed session, then the tape append — so
    `sessions + 1` serialised round trips per print. The loop cannot make a
    round trip that does not happen, which is why `pipelined_fanout` exists
    beside this one.
    """
    broker = Broker(broker_config())
    await broker.connect()
    topics = [Topics.md_session(f"loopbench-{i}") for i in range(sessions)]
    feed = "binance.spot.BTC/USDT.best_quote"

    cpu0, t0 = cpu_seconds(), time.perf_counter()
    for seq in range(messages):
        envelope = quote_envelope(seq)
        for topic in topics:
            await broker.publish(topic, envelope)
        await broker.tape_append(
            feed,
            {"payload": json.dumps(envelope.payload)},
            maxlen=10_000,
            ttl_seconds=60,
        )
    wall, cpu = time.perf_counter() - t0, cpu_seconds() - cpu0
    await broker.close()

    round_trips = messages * (sessions + 1)
    return {
        "messages": messages,
        "sessions": sessions,
        "round_trips": round_trips,
        "wall_s": wall,
        "msgs_per_s": messages / wall,
        "cpu_s": cpu,
        "cpu_us_per_round_trip": cpu / round_trips * 1e6,
    }


async def case_pipelined_fanout(messages: int = 4000, sessions: int = 8) -> dict:
    """The same fan-out with the round trips batched instead of awaited.

    Not a loop comparison. It is the *other* lever on the same path, here so
    the two can be read against each other — because a change that helps more
    than a loop swap is the thing to know before choosing a loop swap.
    """
    broker = Broker(broker_config())
    await broker.connect()
    topics = [Topics.md_session(f"loopbench-{i}") for i in range(sessions)]
    tape_key = broker.tape_key("binance.spot.BTC/USDT.best_quote")

    cpu0, t0 = cpu_seconds(), time.perf_counter()
    for seq in range(messages):
        envelope = quote_envelope(seq)
        raw = envelope.to_json()
        pipe = broker.redis.pipeline(transaction=False)
        for topic in topics:
            pipe.publish(topic, raw)
        pipe.xadd(
            tape_key,
            {"payload": json.dumps(envelope.payload)},
            maxlen=10_000,
            approximate=True,
        )
        pipe.expire(tape_key, 60)
        await pipe.execute()
    wall, cpu = time.perf_counter() - t0, cpu_seconds() - cpu0
    await broker.close()

    return {
        "messages": messages,
        "sessions": sessions,
        "round_trips": messages,
        "wall_s": wall,
        "msgs_per_s": messages / wall,
        "cpu_s": cpu,
    }


async def case_rpc(requests: int = 1500) -> dict:
    """`Broker.request` against `Broker.serve` — RPUSH out, BLPOP back.

    Every control-plane action in the node is one of these, and so is every
    order a strategy places. Latency is the number that matters here, not
    throughput, which is why percentiles are reported.
    """
    server, client = Broker(broker_config()), Broker(broker_config())
    await server.connect()
    await client.connect()
    subject = "loopbench.echo"
    stop = asyncio.Event()

    async def serve() -> None:
        async for request in server.serve(subject, stop=stop):
            await request.reply(
                UntypedEnvelope.wrap(
                    {"ok": True}, type="echo_result", source="loopbench"
                )
            )

    task = asyncio.create_task(serve(), name="loopbench-serve")
    # One warm request so the pool, the serve loop and the BLPOP path are all
    # live before anything is timed.
    await client.request(
        subject, UntypedEnvelope.wrap({}, type="echo", source="loopbench")
    )

    latencies: list[float] = []
    cpu0, t0 = cpu_seconds(), time.perf_counter()
    for seq in range(requests):
        started = time.perf_counter()
        await client.request(
            subject,
            UntypedEnvelope.wrap({"seq": seq}, type="echo", source="loopbench"),
        )
        latencies.append((time.perf_counter() - started) * 1e6)
    wall, cpu = time.perf_counter() - t0, cpu_seconds() - cpu0

    stop.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await server.close()
    await client.close()

    latencies.sort()
    return {
        "requests": requests,
        "wall_s": wall,
        "rps": requests / wall,
        "cpu_s": cpu,
        "mean_us": statistics.fmean(latencies),
        "p50_us": latencies[len(latencies) // 2],
        "p99_us": latencies[int(len(latencies) * 0.99)],
    }


async def case_subscribe(messages: int = 6000) -> dict:
    """`Broker.subscribe` — the receive side every STS feed pump sits on.

    `pubsub.get_message` plus one `UntypedEnvelope.from_json` per message. The
    fan-out case pays only for publishing; this pays for parsing, which is
    where a strategy's own latency starts.
    """
    publisher, subscriber = Broker(broker_config()), Broker(broker_config())
    await publisher.connect()
    await subscriber.connect()
    topic = Topics.md_session("loopbench-sub")
    stop, done = asyncio.Event(), asyncio.Event()
    received = 0

    async def consume() -> None:
        nonlocal received
        async for _envelope in subscriber.subscribe(topic, stop=stop):
            received += 1
            if received >= messages:
                done.set()
                return

    task = asyncio.create_task(consume(), name="loopbench-consume")
    # Pub/Sub drops what nobody is listening to, so the SUBSCRIBE has to have
    # landed on the server before the first publish — not merely been sent.
    while (await publisher.redis.pubsub_numsub(topic))[0][1] == 0:
        await asyncio.sleep(0.01)

    cpu0, t0 = cpu_seconds(), time.perf_counter()
    for seq in range(messages):
        await publisher.publish(topic, quote_envelope(seq))
    await asyncio.wait_for(done.wait(), timeout=120)
    wall, cpu = time.perf_counter() - t0, cpu_seconds() - cpu0

    stop.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await publisher.close()
    await subscriber.close()

    return {
        "messages": messages,
        "received": received,
        "wall_s": wall,
        "msgs_per_s": messages / wall,
        "cpu_s": cpu,
        "cpu_us_per_msg": cpu / messages * 1e6,
    }


async def case_ws_ingest(frames: int = 30_000) -> dict:
    """A venue read loop: `BinanceSocket._pump` plus `_decode`.

    Frames are served by a local `websockets` server rather than a venue, so
    what is measured is the read and the decode and not the internet.
    """
    from websockets.asyncio.client import connect
    from websockets.asyncio.server import serve

    # A Binance combined-stream bookTicker push, verbatim in shape.
    frame = json.dumps(
        {
            "stream": "btcusdt@bookTicker",
            "data": {
                "u": 400900217,
                "s": "BTCUSDT",
                "b": "63501.10000000",
                "B": "0.41200000",
                "a": "63501.20000000",
                "A": "1.88300000",
            },
        }
    )

    async def handler(websocket) -> None:
        for _ in range(frames):
            await websocket.send(frame)

    server = await serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    seen = 0
    # Summed so the decode cannot be optimised away as unread work.
    checksum = 0.0
    cpu0, t0 = cpu_seconds(), time.perf_counter()
    async with connect(f"ws://127.0.0.1:{port}", ping_interval=None) as conn:
        async for raw in conn:
            message = json.loads(raw)
            checksum += float(message["data"]["b"])
            seen += 1
            if seen >= frames:
                break
    wall, cpu = time.perf_counter() - t0, cpu_seconds() - cpu0

    server.close()
    await server.wait_closed()
    return {
        "frames": seen,
        "wall_s": wall,
        "frames_per_s": seen / wall,
        "cpu_s": cpu,
        "cpu_us_per_frame": cpu / seen * 1e6,
        "checksum": round(checksum, 2),
    }


async def case_tcp_echo(round_trips: int = 30_000) -> dict:
    """The control: a bare stream round trip, no library above the transport.

    Read this one first. It is where uvloop's advantage is undiluted, so it is
    what says whether the harness can see an advantage at all.
    """
    payload = b"x" * 256
    # Held so teardown can cancel the handler outright: letting it end on its
    # own failed read leaves `wait_closed` waiting on it.
    handlers: list[asyncio.Task] = []

    async def echo(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        current = asyncio.current_task()
        if current is not None:
            handlers.append(current)
        with contextlib.suppress(
            asyncio.IncompleteReadError, ConnectionResetError
        ):
            while True:
                writer.write(await reader.readexactly(len(payload)))
                await writer.drain()

    server = await asyncio.start_server(echo, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection("127.0.0.1", port)

    cpu0, t0 = cpu_seconds(), time.perf_counter()
    for _ in range(round_trips):
        writer.write(payload)
        await writer.drain()
        await reader.readexactly(len(payload))
    wall, cpu = time.perf_counter() - t0, cpu_seconds() - cpu0

    writer.close()
    with contextlib.suppress(ConnectionResetError):
        await writer.wait_closed()
    for handler in handlers:
        handler.cancel()
    await asyncio.gather(*handlers, return_exceptions=True)
    server.close()
    await server.wait_closed()

    return {
        "round_trips": round_trips,
        "wall_s": wall,
        "rtt_per_s": round_trips / wall,
        "cpu_s": cpu,
        "cpu_us_per_round_trip": cpu / round_trips * 1e6,
    }


CASES = {
    "tcp_echo": case_tcp_echo,
    "ws_ingest": case_ws_ingest,
    "subscribe": case_subscribe,
    "rpc": case_rpc,
    "fanout": case_fanout,
    "pipelined_fanout": case_pipelined_fanout,
}

#: Which numbers are worth printing per case, in the order they read best.
REPORT = {
    "tcp_echo": ["rtt_per_s", "cpu_us_per_round_trip"],
    "ws_ingest": ["frames_per_s", "cpu_us_per_frame"],
    "subscribe": ["msgs_per_s", "cpu_us_per_msg"],
    "rpc": ["rps", "p50_us", "p99_us", "cpu_s"],
    "fanout": ["msgs_per_s", "cpu_us_per_round_trip"],
    "pipelined_fanout": ["msgs_per_s", "cpu_s"],
}

#: Metrics where a smaller number is the better one, so the ratio column can
#: say which loop won rather than leaving it to be worked out per row.
LOWER_IS_BETTER = {
    "cpu_s",
    "cpu_us_per_frame",
    "cpu_us_per_msg",
    "cpu_us_per_round_trip",
    "mean_us",
    "p50_us",
    "p99_us",
    "wall_s",
}

CASES_NEEDING_REDIS = {"fanout", "pipelined_fanout", "rpc", "subscribe"}


# --- behaviour probe -------------------------------------------------------


async def probe() -> dict:
    """Report the loop behaviours this tree depends on.

    Compatibility is not a throughput question, and the differences that would
    actually bite are not in a table of messages per second. These are the ones
    with a call site in the tree.
    """
    from concurrent.futures import ThreadPoolExecutor

    loop = asyncio.get_running_loop()
    found: dict[str, object] = {
        "loop_class": f"{type(loop).__module__}.{type(loop).__name__}"
    }

    # `loop.time()` granularity. TD reads this clock for lease grace, the
    # unknown-order recon backoff and deferred cancels; the venue sockets stamp
    # `last_frame_at` with it. libuv's clock is milliseconds, CPython's is not.
    steps = []
    for _ in range(2000):
        before = loop.time()
        await asyncio.sleep(0)
        after = loop.time()
        if after > before:
            steps.append(after - before)
    found["loop_time_steps_per_2000_yields"] = len(steps)
    found["loop_time_min_step_us"] = round(min(steps) * 1e6, 3) if steps else None

    # Short sleeps. Every poll interval in the tree is built on one.
    for requested in (0.0, 0.0001, 0.001, 0.01):
        samples = []
        for _ in range(200):
            started = time.perf_counter()
            await asyncio.sleep(requested)
            samples.append((time.perf_counter() - started) * 1e6)
        found[f"sleep_{requested}_median_us"] = round(
            statistics.median(samples), 1
        )

    # SIGTERM handling — how every service in the node shuts down.
    import signal

    fired = asyncio.Event()
    try:
        loop.add_signal_handler(signal.SIGUSR1, fired.set)
        signal.raise_signal(signal.SIGUSR1)
        try:
            await asyncio.wait_for(fired.wait(), timeout=2)
            found["add_signal_handler"] = "delivered"
        except TimeoutError:
            found["add_signal_handler"] = "registered, not delivered"
        loop.remove_signal_handler(signal.SIGUSR1)
    except NotImplementedError:
        found["add_signal_handler"] = "NotImplementedError"

    # Off-loop work: eventlog writes use `to_thread`, alert matching uses a
    # dedicated pool.
    found["to_thread"] = await asyncio.to_thread(lambda: "ok")
    with ThreadPoolExecutor(max_workers=1) as pool:
        found["run_in_executor_custom_pool"] = await loop.run_in_executor(
            pool, lambda: "ok"
        )

    # Reconnect code catches OSError. A loop that raised something else would
    # slip straight past every handler in the exchange package.
    try:
        await asyncio.open_connection("127.0.0.1", 1)
    except OSError as exc:
        found["refused_connect_raises"] = type(exc).__name__
        found["refused_connect_is_oserror"] = True
    except BaseException as exc:  # noqa: BLE001 — the type is the answer
        found["refused_connect_raises"] = type(exc).__name__
        found["refused_connect_is_oserror"] = False

    # Named tasks and cancellation: every shutdown path in the node.
    async def sleeper() -> None:
        await asyncio.sleep(10)

    task = asyncio.create_task(sleeper(), name="loopbench-named")
    await asyncio.sleep(0)
    found["task_get_name"] = task.get_name()
    task.cancel()
    try:
        await task
        found["cancel_raises"] = "nothing"
    except asyncio.CancelledError:
        found["cancel_raises"] = "CancelledError"

    seen: list[str] = []
    loop.set_exception_handler(
        lambda _loop, context: seen.append(str(context.get("message", "")))
    )

    def boom() -> None:
        raise RuntimeError("loopbench")

    loop.call_soon(boom)
    await asyncio.sleep(0.05)
    found["custom_exception_handler_called"] = bool(seen)
    loop.set_exception_handler(None)
    return found


# --- orchestration ---------------------------------------------------------


def run_child(loop_name: str, what: str) -> dict:
    """Run one case (or the probe) in a child process on ``loop_name``."""
    completed = subprocess.run(
        [sys.executable, __file__, "--child", loop_name, what],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise SystemExit(
            f"{loop_name}/{what} failed:\n{completed.stderr or completed.stdout}"
        )
    return json.loads(completed.stdout.strip().splitlines()[-1])


def report(results: dict[str, dict[str, list[dict]]]) -> None:
    """Print medians per loop, with the ratio the right way round."""
    for case, by_loop in results.items():
        reps = len(by_loop["asyncio"])
        print(f"\n=== {case} — median of {reps} rep(s) ===")
        print(f"{'metric':<26}{'asyncio':>14}{'uvloop':>14}{'verdict':>22}")
        for metric in REPORT[case]:
            base = statistics.median(row[metric] for row in by_loop["asyncio"])
            other = statistics.median(row[metric] for row in by_loop["uvloop"])
            if metric in LOWER_IS_BETTER:
                factor = base / other if other else float("inf")
            else:
                factor = other / base if base else float("inf")
            verdict = (
                f"uvloop {factor:.2f}x better"
                if factor >= 1
                else f"uvloop {1 / factor:.2f}x worse"
            )
            print(f"{metric:<26}{base:>14.2f}{other:>14.2f}{verdict:>22}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--child",
        nargs=2,
        metavar=("LOOP", "WHAT"),
        help="internal: run one case on one loop and print one JSON line",
    )
    parser.add_argument(
        "--case",
        action="append",
        choices=sorted(CASES),
        help="case to run; repeatable. Default: all of them.",
    )
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument(
        "--probe",
        action="store_true",
        help="report behavioural differences instead of throughput",
    )
    parser.add_argument(
        "--json",
        metavar="PATH",
        help="also write every raw measurement here, one JSON object per line",
    )
    args = parser.parse_args()

    if args.child:
        loop_name, what = args.child
        if loop_name == "uvloop":
            import uvloop

            runner = uvloop.run
        else:
            runner = asyncio.run
        payload = runner(probe() if what == "probe" else CASES[what]())
        payload["loop"] = loop_name
        payload["case"] = what
        print(json.dumps(payload))
        return

    if args.probe:
        for loop_name in ("asyncio", "uvloop"):
            print(f"\n=== {loop_name} ===")
            print(json.dumps(run_child(loop_name, "probe"), indent=2))
        return

    cases = args.case or list(CASES)
    if any(case in CASES_NEEDING_REDIS for case in cases):
        print(
            f"redis: {broker_config().redis_url} (key prefix {KEY_PREFIX!r})",
            file=sys.stderr,
        )

    results: dict[str, dict[str, list[dict]]] = {
        case: {"asyncio": [], "uvloop": []} for case in cases
    }
    raw: list[dict] = []
    for rep in range(1, args.reps + 1):
        for case in cases:
            for loop_name in ("asyncio", "uvloop"):
                row = run_child(loop_name, case)
                results[case][loop_name].append(row)
                raw.append(row | {"rep": rep})
                print(
                    f"rep {rep}/{args.reps} {case} {loop_name}", file=sys.stderr
                )
    report(results)
    if args.json:
        with open(args.json, "w") as handle:
            for row in raw:
                handle.write(json.dumps(row) + "\n")
        print(f"\nraw measurements: {args.json}")


if __name__ == "__main__":
    main()
