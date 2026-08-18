"""Async Redis broker — pub/sub and request-reply IPC."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import redis.asyncio as redis
from pydantic import BaseModel
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError

from mftik.broker.config import BrokerConfig
from mftik.broker.errors import BrokerNotConnectedError, RequestTimeoutError
from mftik.broker.request import IncomingRequest
from mftik.broker.stream import BidirectionalStream
from mftik.protocol import (
    Envelope,
    Heartbeat,
    HeartbeatEnvelope,
    Topics,
    UntypedEnvelope,
)

logger = logging.getLogger(__name__)


def redacted_url(url: str) -> str:
    """``url`` with its password replaced, for logging.

    A Redis URL carries the credential inline and every service logs this
    line on every connect, so the password lands in ``docker logs`` for the
    whole fleet and in anything those logs are shipped to.

    Parsed rather than pattern-matched: a password may contain ``@`` and
    ``:``, so splitting on either finds the wrong one and prints the rest.
    Anything that will not parse returns a placeholder — falling back to the
    original would leak exactly the string this exists to hide.
    """
    try:
        parts = urlsplit(url)
        if not parts.password:
            return url
        host = parts.hostname or ""
        # ``.port`` raises on a non-numeric port, and it raises here rather
        # than in ``urlsplit`` — which is why the whole reconstruction is
        # inside the try and not just the parse.
        if parts.port is not None:
            host = f"{host}:{parts.port}"
        user = parts.username or ""
        return urlunsplit(
            (
                parts.scheme,
                f"{user}:***@{host}",
                parts.path,
                parts.query,
                parts.fragment,
            )
        )
    except ValueError:
        return "<unparseable url>"

Handler = Callable[[IncomingRequest], Awaitable[None]]


def _to_json(value: BaseModel | dict[str, Any]) -> str:
    if isinstance(value, BaseModel):
        return value.model_dump_json()
    return json.dumps(value, default=str)


#: How many measured gaps one feed's coverage will carry before the tape stops
#: being described as one series at all. A feed that was interrupted this often
#: inside its retention window is not a recording with holes in it, and the
#: field is a hash value, not a stream — it does not get to grow forever.
TAPE_MAX_GAPS = 32


def encode_tape_gaps(gaps: Sequence[tuple[int, int]]) -> str:
    """Render measured gaps as ``start-end`` pairs, oldest first.

    A flat string rather than JSON: these are pairs of integers written on
    every feed restart and read on every warm-up, and the coverage hash is
    read as ``dict[str, str]`` by everything that touches it.
    """
    return ",".join(f"{start}-{end}" for start, end in gaps)


def decode_tape_gaps(raw: str | None) -> list[tuple[int, int]]:
    """Parse :func:`encode_tape_gaps`. Unreadable entries are skipped.

    Never raises. Coverage describes a warm-up that may never happen, while
    the caller is a feed coming up to serve strategies that trade now — a
    field that will not parse costs a gap record, not a recording.
    """
    if not raw:
        return []
    gaps: list[tuple[int, int]] = []
    for chunk in raw.split(","):
        head, _, tail = chunk.partition("-")
        try:
            gaps.append((int(head), int(tail)))
        except ValueError:
            logger.warning("tape coverage has an unreadable gap: %r", chunk)
    return gaps


def _int_or_none(raw: str | None) -> int | None:
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def build_redis(config: BrokerConfig) -> redis.Redis:
    """Build the Redis client every service talks through.

    Module-level rather than inline in :meth:`Broker.connect` because what it
    encodes is a policy about failure, and a policy that can only be observed
    by connecting to a real server is one nothing checks.
    """
    return redis.from_url(
        config.redis_url,
        decode_responses=True,
        # Pooled connections are handed out newest-first, so one that sinks to
        # the bottom of the pool can idle past the server's ``timeout`` and be
        # closed there. Nothing notices until it is borrowed again, and then
        # the command fails on a socket that was already gone — which is how a
        # domain gets a burst of ConnectionErrors on a Redis that is perfectly
        # healthy. Checking a connection's health on checkout is what finds one
        # of those.
        health_check_interval=config.health_check_interval,
        socket_keepalive=True,
        # Finding it is not the same as surviving it. With no retry, redis-py
        # raises the health check's own ConnectionError at whichever caller
        # happened to borrow the connection — and in STS that caller is a feed
        # pump whose only reading of an exception is that the session can no
        # longer run. The retry is what turns "this connection is dead" into
        # "drop it and use a live one", which is what the health check was for.
        #
        # ConnectionError alone, deliberately. A retry re-sends the command, so
        # one that failed while reading its reply is delivered twice — and
        # ``request`` carries new orders. That duplicate is refused at the venue
        # on its client_order_id (mftik_td.errors.VENUE_DUPLICATE_CLIENT_ORDER_ID
        # is already the code for it), so it costs a spurious reject rather than
        # a doubled position, which is a trade worth making to get the reconnect.
        # TimeoutError is not: no ``socket_timeout`` is set, so it has no way to
        # arise here, and listing it would widen the re-send window for nothing.
        retry=Retry(
            ExponentialBackoff(cap=0.5, base=0.05),
            config.command_retries,
            supported_errors=(RedisConnectionError,),
        ),
        retry_on_error=[RedisConnectionError],
    )


class Broker:
    """Async Redis IPC client.

    Three primitives:

    1. **Pub/Sub** — fan-out broadcast via Redis Pub/Sub
       (``publish`` / ``subscribe``).
    2. **Request-reply** — 1:1 RPC via Redis lists
       (``request`` / ``serve``).
    3. **Bidirectional stream** — duplex channel = pub + sub
       (``bistream``).
    """

    def __init__(
        self,
        config: BrokerConfig | None = None,
        *,
        redis_client: redis.Redis | None = None,
    ) -> None:
        self.config = config or BrokerConfig.from_env()
        self._redis = redis_client
        self._owns_redis = redis_client is None

    # --- lifecycle ---------------------------------------------------------

    @property
    def redis(self) -> redis.Redis:
        if self._redis is None:
            raise BrokerNotConnectedError(
                "Broker is not connected; call connect() first"
            )
        return self._redis

    async def connect(self) -> None:
        if self._redis is None:
            self._redis = build_redis(self.config)
            self._owns_redis = True
        await self._redis.ping()
        logger.info("Connected to Redis at %s", redacted_url(self.config.redis_url))

    async def close(self) -> None:
        if self._redis is not None and self._owns_redis:
            await self._redis.aclose()
            self._redis = None

    async def __aenter__(self) -> Broker:
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    # --- key helpers -------------------------------------------------------

    def _rpc_queue(self, subject: str) -> str:
        return f"{self.config.key_prefix}:rpc:{subject}"

    def _rpc_reply(self, request_id: str) -> str:
        return f"{self.config.key_prefix}:rpc:reply:{request_id}"

    def _log_buffer_key(self, topic: str) -> str:
        return f"{self.config.key_prefix}:logbuf:{topic}"

    def state_key(self, name: str) -> str:
        """Redis key backing a shared state hash (e.g. ``td.ledger.7``)."""
        return f"{self.config.key_prefix}:state:{name}"

    # --- shared state (hashes) ---------------------------------------------
    #
    # Pub/Sub tells a reader that something changed; these hold what it
    # changed *to*. A late subscriber, a restarted process and a strategy that
    # missed a message all read the same current answer here, which is what
    # makes "the writer's state and the reader's state agree" true by
    # construction rather than by both sides keeping their own copy in sync.

    async def state_put(
        self, name: str, field: str, value: BaseModel | dict[str, Any]
    ) -> None:
        """Write one field of a state hash."""
        await self.redis.hset(  # type: ignore[misc]
            self.state_key(name), field, _to_json(value)
        )

    async def state_put_many(
        self, name: str, values: Mapping[str, BaseModel | dict[str, Any]]
    ) -> None:
        """Write several fields in one round trip."""
        if not values:
            return
        await self.redis.hset(  # type: ignore[misc]
            self.state_key(name),
            mapping={k: _to_json(v) for k, v in values.items()},
        )

    async def state_replace(
        self, name: str, values: Mapping[str, BaseModel | dict[str, Any]]
    ) -> None:
        """Make the hash exactly ``values`` — the recon path.

        Delete and rewrite run in one transaction so a reader never observes
        the empty gap between them.
        """
        key = self.state_key(name)
        pipe = self.redis.pipeline(transaction=True)
        pipe.delete(key)
        if values:
            pipe.hset(key, mapping={k: _to_json(v) for k, v in values.items()})
        await pipe.execute()

    async def state_get(self, name: str, field: str) -> dict[str, Any] | None:
        raw = await self.redis.hget(self.state_key(name), field)  # type: ignore[misc]
        return None if raw is None else json.loads(raw)

    async def state_all(self, name: str) -> dict[str, dict[str, Any]]:
        rows = await self.redis.hgetall(self.state_key(name))  # type: ignore[misc]
        return {field: json.loads(raw) for field, raw in rows.items()}

    async def state_drop(self, name: str, *fields: str) -> int:
        if not fields:
            return 0
        return int(
            await self.redis.hdel(self.state_key(name), *fields)  # type: ignore[misc]
        )

    async def state_clear(self, *names: str) -> None:
        """Delete whole state hashes — call when their owner goes away.

        State that outlives its writer is worse than no state: a reader cannot
        tell a stale answer from a current one.
        """
        if names:
            await self.redis.delete(*(self.state_key(n) for n in names))

    # --- recorded tape (streams) -------------------------------------------
    #
    # A feed's own history, kept so a strategy that starts later can warm up on
    # what it missed. Streams rather than lists because the retention policy is
    # a *duration* — a stream id is a millisecond timestamp, so "keep two
    # hours" is ``XTRIM MINID`` and "read from T" is ``XRANGE``, neither of
    # which a list can express: ``LTRIM`` counts entries, and the same count is
    # eight hours of a quiet instrument or twenty minutes of a busy one.
    #
    # Two bounds, and they mean different things. ``maxlen`` on append is the
    # memory fuse — approximate, so Redis trims whole nodes and the write stays
    # cheap. The MINID trim is the intent. Whichever binds first is what the
    # reader gets, and :meth:`tape_coverage` is how it finds out which.

    def tape_key(self, feed: str) -> str:
        """Redis stream holding recorded tape for ``feed``."""
        return f"{self.config.key_prefix}:tape:{feed}"

    def tape_coverage_key(self, feed: str) -> str:
        """Redis hash describing what :meth:`tape_key` currently covers."""
        return f"{self.config.key_prefix}:tape:coverage:{feed}"

    async def tape_append(
        self,
        feed: str,
        fields: Mapping[str, str],
        *,
        maxlen: int,
        ttl_seconds: int,
    ) -> None:
        """Append one record, capping the stream at ``maxlen`` entries.

        The id is Redis' own clock, not the venue's timestamp. Event time is a
        field on the record instead, because ``XADD`` refuses an id that does
        not exceed the last one and a venue tape is not strictly monotonic —
        one late print out of a million would otherwise end the recording.

        ``ttl_seconds`` is renewed on every append, so a feed that stops being
        recorded expires on its own. Without it a tape would outlive the last
        strategy that ever wanted it: the MINID trim only runs against feeds
        that are still pumping, and a stream nobody writes to is never capped
        by ``maxlen`` either. Every instrument ever subscribed would keep its
        last two hours for as long as Redis lived.
        """
        pipe = self.redis.pipeline()
        pipe.xadd(
            self.tape_key(feed),
            dict(fields),
            maxlen=maxlen,
            approximate=True,
        )
        pipe.expire(self.tape_key(feed), ttl_seconds)
        pipe.expire(self.tape_coverage_key(feed), ttl_seconds)
        await pipe.execute()

    async def tape_tail(
        self, feed: str, *, count: int
    ) -> list[tuple[str, dict[str, str]]]:
        """Read the newest ``count`` records, oldest → newest.

        The newest rather than the oldest: warming up means catching up to now,
        and a stream capped by two independent bounds holds an unknown number
        of records, so "the first N" is not a window anyone asked for.
        """
        if count <= 0:
            return []
        rows = await self.redis.xrevrange(
            self.tape_key(feed), max="+", min="-", count=count
        )
        return [(str(rid), dict(fields)) for rid, fields in reversed(rows)]

    async def tape_trim_before(self, feed: str, *, min_id_ms: int) -> int:
        """Drop records older than ``min_id_ms``. Returns how many went."""
        return int(
            await self.redis.xtrim(self.tape_key(feed), minid=min_id_ms)
        )

    async def tape_mark_recording(
        self, feed: str, *, since_ms: int, ttl_seconds: int
    ) -> None:
        """Record that this feed started recording at ``since_ms``.

        Called when a feed begins pumping. Whether that breaks continuity
        depends on what the previous recording left behind:

        * A ``stopped_ms`` stamp means the last recorder shut down cleanly and
          said when. The interruption is then *measured* — ``since_ms`` minus
          that stamp — so the records before it are not on the far side of an
          unknown hole. Continuity is kept and the gap is appended to the
          ``gaps`` coverage field, for the reader to judge.
        * No stamp means the last recorder vanished — SIGKILL, OOM, the machine
          going away — and nobody wrote down when. The hole is unmeasurable, so
          continuity restarts here and the earlier records fall behind the mark.

        The distinction is the whole point. A deploy interrupts a feed for a few
        seconds, and resetting continuity for it discards two hours of tape that
        is sitting intact in the stream — the warm-up window, thrown away to
        describe a hole shorter than one bar. What cannot be measured is still
        treated as fatal to continuity; what can is handed over as a fact.

        Carries its own TTL because a feed can be subscribed and then print
        nothing at all — a dead instrument, a venue outage — and the appends
        that would otherwise renew it never come.

        Read-modify-write, and safe today because a feed has exactly one
        recorder: MD refcounts subscribers within one process, so ``started``
        fires once per feed per process and no second writer exists to race.
        A second MD writing the same feed — the blue/green handover — changes
        that, and is the reason it would need a fencing token here.
        """
        prior = await self.tape_coverage(feed)
        stopped_ms = _int_or_none(prior.get("stopped_ms"))
        prior_since = _int_or_none(prior.get("continuous_since_ms"))
        gaps = decode_tape_gaps(prior.get("gaps"))

        measured = (
            stopped_ms is not None
            and prior_since is not None
            # A stop stamped after the start it precedes is a clock that moved,
            # not a gap. Unmeasurable, so it is treated as one.
            and stopped_ms <= since_ms
        )
        if measured:
            assert prior_since is not None and stopped_ms is not None
            gaps = [*gaps, (stopped_ms, since_ms)]
            continuous_since = prior_since
            # A tape this punctured is not one series in any useful sense, and
            # the field would grow without bound. Collapsing to a fresh mark is
            # the same answer the unmeasurable case gets, for the same reason.
            if len(gaps) > TAPE_MAX_GAPS:
                gaps = []
                continuous_since = since_ms
        else:
            gaps = []
            continuous_since = since_ms

        pipe = self.redis.pipeline()
        pipe.hset(
            self.tape_coverage_key(feed),
            mapping={
                "continuous_since_ms": str(continuous_since),
                "recording": "1",
                "stopped_ms": "",
                "gaps": encode_tape_gaps(gaps),
            },
        )
        pipe.expire(self.tape_coverage_key(feed), ttl_seconds)
        await pipe.execute()

    async def tape_mark_stopped(self, feed: str, *, at_ms: int) -> None:
        """Record that this feed stopped recording at ``at_ms``.

        The stream is left alone. A reader that wants the last two hours before
        a feed went quiet can still have them — it just has to know they end,
        and that is exactly what this says.

        It is also the near edge of any gap that follows. Only a recorder that
        got to run its shutdown leaves this behind, which is what makes a
        planned interruption measurable and an unplanned one not — see
        :meth:`tape_mark_recording`.
        """
        await self.redis.hset(  # type: ignore[misc]
            self.tape_coverage_key(feed),
            mapping={"recording": "0", "stopped_ms": str(at_ms)},
        )

    async def tape_coverage(self, feed: str) -> dict[str, str]:
        """What :meth:`tape_key` covers, or ``{}`` if it was never recorded."""
        return dict(await self.redis.hgetall(self.tape_coverage_key(feed)))  # type: ignore[misc]

    # --- Pub/Sub -----------------------------------------------------------

    async def publish(self, topic: str, envelope: Envelope[Any]) -> int:
        """Publish an envelope to a pub/sub topic (fan-out)."""
        return int(await self.redis.publish(topic, envelope.to_json()))

    async def publish_log(
        self,
        topic: str,
        envelope: Envelope[Any],
        *,
        maxlen: int | None = None,
        ttl_seconds: int = 86_400,
    ) -> int:
        """Publish a log line and append it to a Redis list for late subscribers.

        Redis Pub/Sub alone drops messages when nobody is listening (e.g. UI
        opens ``/ws/sts/...`` after deploy). The buffer is replayed on connect.
        ``maxlen`` defaults to :attr:`BrokerConfig.log_buffer_maxlen` (100).
        """
        keep = (
            self.config.log_buffer_maxlen if maxlen is None else max(1, maxlen)
        )
        raw = envelope.to_json()
        key = self._log_buffer_key(topic)
        pipe = self.redis.pipeline()
        pipe.rpush(key, raw)
        pipe.ltrim(key, -keep, -1)
        pipe.expire(key, ttl_seconds)
        pipe.publish(topic, raw)
        results = await pipe.execute()
        return int(results[-1])

    async def fetch_log_buffer(self, topic: str) -> list[str]:
        """Return buffered log JSON lines for ``topic`` (oldest → newest)."""
        rows = await self.redis.lrange(self._log_buffer_key(topic), 0, -1)
        return list(rows)

    async def subscribe(
        self,
        topics: str | Sequence[str],
        *,
        stop: asyncio.Event | None = None,
    ) -> AsyncIterator[UntypedEnvelope]:
        """Yield envelopes from one or more pub/sub topics until ``stop``.

        Uses Redis Pub/Sub. Messages published while not subscribed are lost
        unless they were also written via :meth:`publish_log`.
        """
        channel_list = (topics,) if isinstance(topics, str) else tuple(topics)
        if not channel_list:
            raise ValueError("subscribe requires at least one topic")

        pubsub = self.redis.pubsub()
        await pubsub.subscribe(*channel_list)
        try:
            while stop is None or not stop.is_set():
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if message is None:
                    await asyncio.sleep(0.01)
                    continue
                data = message.get("data")
                if data is None:
                    continue
                yield UntypedEnvelope.from_json(data)
        finally:
            await pubsub.unsubscribe(*channel_list)
            await pubsub.aclose()

    async def psubscribe(
        self,
        patterns: str | Sequence[str],
        *,
        stop: asyncio.Event | None = None,
    ) -> AsyncIterator[tuple[str, UntypedEnvelope]]:
        """Yield ``(channel, envelope)`` from pattern subscriptions until ``stop``.

        Uses Redis ``PSUBSCRIBE``. Messages published while not subscribed are
        lost unless they were also written via :meth:`publish_log`.
        """
        pattern_list = (patterns,) if isinstance(patterns, str) else tuple(patterns)
        if not pattern_list:
            raise ValueError("psubscribe requires at least one pattern")

        pubsub = self.redis.pubsub()
        await pubsub.psubscribe(*pattern_list)
        try:
            while stop is None or not stop.is_set():
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if message is None:
                    await asyncio.sleep(0.01)
                    continue
                if message.get("type") != "pmessage":
                    continue
                data = message.get("data")
                channel = message.get("channel")
                if data is None or channel is None:
                    continue
                yield str(channel), UntypedEnvelope.from_json(data)
        finally:
            await pubsub.punsubscribe(*pattern_list)
            await pubsub.aclose()

    def bistream(
        self,
        *,
        tx: str,
        rx: str,
    ) -> BidirectionalStream:
        """Open a bidirectional stream (publish on ``tx``, subscribe on ``rx``)."""
        return BidirectionalStream(self, tx=tx, rx=rx)

    def bistream_pair(
        self,
        name: str,
    ) -> tuple[BidirectionalStream, BidirectionalStream]:
        """Open both ends of a named bistream: ``(up, down)``.

        ``up`` publishes ``bistream.{name}.up`` and receives ``.down``;
        ``down`` is the complement.
        """
        up_topic, down_topic = BidirectionalStream.topics(name)
        up = self.bistream(tx=up_topic, rx=down_topic)
        down = self.bistream(tx=down_topic, rx=up_topic)
        return up, down

    # --- Request-reply -----------------------------------------------------

    async def request(
        self,
        subject: str,
        envelope: Envelope[Any],
        *,
        timeout: float | None = None,
    ) -> UntypedEnvelope:
        """Send a request and wait for a single reply.

        The envelope's ``id`` is used as the correlation id. A temporary
        reply list key is written into ``reply_to`` before enqueueing.
        """
        wait = self.config.request_timeout if timeout is None else timeout
        reply_key = self._rpc_reply(envelope.id)
        outbound = (
            envelope
            if envelope.reply_to == reply_key
            else envelope.model_copy(update={"reply_to": reply_key})
        )

        queue = self._rpc_queue(subject)
        await self.redis.rpush(queue, outbound.to_json())

        # BLPOP timeout is whole seconds; poll until the deadline for accuracy.
        deadline = time.monotonic() + wait
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RequestTimeoutError(subject, outbound.id, wait)
                result = await self.redis.blpop(reply_key, timeout=1)
                if result is None:
                    continue
                _key, data = result
                return UntypedEnvelope.from_json(data)
        finally:
            await self.redis.delete(reply_key)

    async def post(self, subject: str, envelope: Envelope[Any]) -> None:
        """Enqueue on a request-reply subject without waiting for a reply.

        The same queue :meth:`request` uses and the same competing consumers
        take from it; what is missing is the ``reply_to``, so the handler
        answers nobody and this returns as soon as Redis has the message.

        For work whose *result* the sender has no use for and whose duration it
        must not inherit — a backfill run is minutes of venue round trips, and
        the shutdown path that asks for one is measured in seconds. A request
        left in the list because nothing is serving the subject yet is not lost:
        the next consumer to come up takes it, which is the recovery a pub/sub
        message could not offer.
        """
        await self.redis.rpush(self._rpc_queue(subject), envelope.to_json())

    async def serve(
        self,
        subject: str,
        *,
        stop: asyncio.Event | None = None,
    ) -> AsyncIterator[IncomingRequest]:
        """Yield incoming requests on a request-reply subject.

        Call ``await req.reply(envelope)`` to respond. Competing consumers
        on the same subject share work via Redis list ``BLPOP``.
        """
        queue = self._rpc_queue(subject)
        while stop is None or not stop.is_set():
            result = await self.redis.blpop(queue, timeout=1)
            if result is None:
                continue
            _key, data = result
            envelope = UntypedEnvelope.from_json(data)
            yield IncomingRequest(self, envelope)

    async def serve_handler(
        self,
        subject: str,
        handler: Handler,
        *,
        stop: asyncio.Event | None = None,
    ) -> None:
        """Run ``handler`` for each incoming request until ``stop``."""
        async for req in self.serve(subject, stop=stop):
            await handler(req)

    async def _send_reply(self, reply_to: str, envelope: Envelope[Any]) -> None:
        await self.redis.rpush(reply_to, envelope.to_json())
        await self.redis.expire(reply_to, self.config.reply_ttl_seconds)

    # --- convenience -------------------------------------------------------

    async def heartbeat_loop(
        self,
        source: str,
        *,
        interval: float = 5.0,
        stop: asyncio.Event | None = None,
        on_tick: Callable[[], None] | None = None,
    ) -> None:
        """Publish periodic heartbeats on the heartbeat pub/sub topic."""
        while stop is None or not stop.is_set():
            envelope = HeartbeatEnvelope.wrap(
                Heartbeat(),
                type="heartbeat",
                source=source,
            )
            await self.publish(Topics.HEARTBEAT, envelope)
            if on_tick is not None:
                on_tick()
            try:
                if stop is not None:
                    await asyncio.wait_for(stop.wait(), timeout=interval)
                else:
                    await asyncio.sleep(interval)
            except TimeoutError:
                continue


# Back-compat alias used during the rename.
BrokerClient = Broker
