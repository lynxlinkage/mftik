"""The Redis transport — one flat keyspace, lists for work, hashes for state.

This is the implementation every plane ran on before there was a second one,
moved behind :class:`~mftik.broker.transport.base.BrokerTransport` unchanged.
Key strings are byte-for-byte what they were: a lease name is still the whole
tail under the prefix, so ``{prefix}:sts:alive:{session}`` is what MD and STS
were renewing before any of this existed. A node rolling back to this transport
finds its own state where it left it.

What it cannot do is close two races, and both are documented where they are
lost rather than papered over. :meth:`RedisTransport.lease_hold` and
:meth:`RedisTransport.lease_release` are a read then a write, because Redis
without scripting has no compare-and-set, and the suite's Redis has no
scripting. The NATS transport closes them.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit

import redis.asyncio as redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from mftik.broker.config import BrokerConfig
from mftik.broker.errors import BrokerNotConnectedError, RequestTimeoutError
from mftik.broker.transport.base import LEASE_ANONYMOUS, BrokerTransport

logger = logging.getLogger(__name__)

#: How many probes one health subject's queue keeps. Only the recent ones can
#: still have a caller waiting, so this is a fuse rather than a buffer: it is
#: what stops a dashboard polling a down instance from growing a list without
#: end. Comfortably above any plausible number of concurrent dashboards.
PROBE_QUEUE_MAXLEN = 16

#: How long a probe queue outlives its last write. Refreshed on every probe, so
#: it is not what bounds a queue being actively written to — :data:`PROBE_QUEUE_MAXLEN`
#: is. What this buys is that the key of an instance nobody probes any more goes
#: away on its own rather than sitting in Redis for the life of the deployment.
PROBE_QUEUE_TTL_SECONDS = 300

#: How long :meth:`RedisTransport.serve` waits before polling again after a
#: poll that failed. Matched to the poll's own BLPOP timeout: long enough that
#: a Redis outage does not fill the log a hundred times a second, short enough
#: that nobody notices the gap in a control plane once Redis is back.
_SERVE_POLL_RETRY_S = 1.0


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


def _ms(seconds: float) -> int:
    """Whole milliseconds, and never zero.

    Milliseconds rather than the seconds Redis' ``EX`` takes, so a caller may
    ask for a fraction. Zero is rejected by Redis outright, and a rounding
    error is a poor way to find that out, so it becomes the shortest lease
    expressible instead.

    NATS cannot do this — its per-message TTL is whole seconds with a one
    second floor — so a caller that depends on a sub-second lease is depending
    on this transport. The leases in production are thirty seconds.
    """
    return max(1, int(seconds * 1000))


def _record_ms(record_id: str) -> int:
    """Milliseconds out of a ``<ms>-<seq>`` stream id.

    Zero for an id that will not parse, which reads as "older than any
    continuity mark" and costs the caller that one record. Redis' own ids
    always parse; what this covers is a tape written by something else.
    """
    head, _, _tail = record_id.partition("-")
    try:
        return int(head)
    except ValueError:
        return 0


def build_redis(config: BrokerConfig) -> redis.Redis:
    """Build the Redis client every service talks through.

    Module-level rather than inline in :meth:`RedisTransport.connect` because
    what it encodes is a policy about failure, and a policy that can only be
    observed by connecting to a real server is one nothing checks.
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
        # TimeoutError is still not listed, but not for the reason this comment
        # used to give. It said a TimeoutError cannot arise without a
        # ``socket_timeout``; it can. redis-py gives a *blocking* command a read
        # deadline of its own — BLPOP's timeout plus a margin — and a socket
        # that stalls for a second past it raises ``redis.exceptions.TimeoutError``
        # from inside ``read_response``. On 2026-08-18 that is what ended STS's
        # RPC serve loop, and with it every pause, stop and health check for
        # seven hours, while its sessions went on trading.
        #
        # It stays out because retrying a blocking pop is worse than failing
        # one: the re-sent BLPOP takes the *next* element, so an element the
        # server had already handed to the reply that got lost is dropped, and
        # dropped silently. A retry cannot tell those apart from here. The two
        # loops that issue blocking pops handle it where the semantics are
        # known instead — see :meth:`RedisTransport.serve` and
        # :meth:`RedisTransport.request`.
        retry=Retry(
            ExponentialBackoff(cap=0.5, base=0.05),
            config.command_retries,
            supported_errors=(RedisConnectionError,),
        ),
        retry_on_error=[RedisConnectionError],
    )


class RedisTransport(BrokerTransport):
    """Pub/Sub for fan-out, lists for work, hashes for state, streams for tape."""

    def __init__(
        self,
        config: BrokerConfig,
        *,
        client: redis.Redis | None = None,
    ) -> None:
        self.config = config
        self._redis = client
        self._owns_client = client is None

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
            self._owns_client = True
        await self._redis.ping()

    async def close(self) -> None:
        if self._redis is not None and self._owns_client:
            await self._redis.aclose()
            self._redis = None

    def describe(self) -> str:
        return f"Redis at {redacted_url(self.config.redis_url)}"

    # --- keys --------------------------------------------------------------

    def _key(self, name: str) -> str:
        return f"{self.config.key_prefix}:{name}"

    def _rpc_queue(self, subject: str) -> str:
        return f"{self.config.key_prefix}:rpc:{subject}"

    def _rpc_reply(self, request_id: str) -> str:
        return f"{self.config.key_prefix}:rpc:reply:{request_id}"

    def _log_buffer_key(self, topic: str) -> str:
        return f"{self.config.key_prefix}:logbuf:{topic}"

    def state_key(self, name: str) -> str:
        """Redis hash backing a shared state (e.g. ``td.ledger.7``)."""
        return f"{self.config.key_prefix}:state:{name}"

    def lease_key(self, name: str) -> str:
        """Redis key backing the lease ``name`` (e.g. ``sts:alive:s-1``).

        A lease name is the whole key tail, not a segment under a ``lease:``
        namespace of its own. The names in use predate the broker — MD and STS
        have been renewing ``{prefix}:{domain}:alive:{session}`` in production
        since before there was an abstraction to put them behind — and a
        rolling upgrade that moved them would have both halves of the fleet
        reading a different key for "is anybody running this session", which is
        the one question that must not have two answers.
        """
        return self._key(name)

    def tape_key(self, feed: str) -> str:
        """Redis stream holding recorded tape for ``feed``."""
        return f"{self.config.key_prefix}:tape:{feed}"

    def tape_coverage_key(self, feed: str) -> str:
        """Redis hash describing what :meth:`tape_key` currently covers."""
        return f"{self.config.key_prefix}:tape:coverage:{feed}"

    # --- fan-out -----------------------------------------------------------

    async def publish(self, topic: str, raw: str) -> None:
        await self.redis.publish(topic, raw)

    async def subscribe(
        self, topics: Sequence[str], *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str]]:
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(*topics)
        try:
            while stop is None or not stop.is_set():
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if message is None:
                    await asyncio.sleep(0.01)
                    continue
                data = message.get("data")
                channel = message.get("channel")
                if data is None or channel is None:
                    continue
                yield str(channel), data
        finally:
            await pubsub.unsubscribe(*topics)
            await pubsub.aclose()

    async def psubscribe(
        self, patterns: Sequence[str], *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str]]:
        pubsub = self.redis.pubsub()
        await pubsub.psubscribe(*patterns)
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
                yield str(channel), data
        finally:
            await pubsub.punsubscribe(*patterns)
            await pubsub.aclose()

    # --- fan-out with a tail -----------------------------------------------

    async def publish_log(
        self, topic: str, raw: str, *, maxlen: int, ttl_seconds: int
    ) -> None:
        key = self._log_buffer_key(topic)
        pipe = self.redis.pipeline()
        pipe.rpush(key, raw)
        pipe.ltrim(key, -maxlen, -1)
        pipe.expire(key, ttl_seconds)
        pipe.publish(topic, raw)
        await pipe.execute()

    async def fetch_log_buffer(self, topic: str) -> list[str]:
        return list(await self.redis.lrange(self._log_buffer_key(topic), 0, -1))

    # --- request-reply -----------------------------------------------------

    def reply_inbox(self, request_id: str) -> str | None:
        """A list key, written into the envelope before it is enqueued.

        In-band because it has to be: the serving process is a different
        process and sees nothing of this request but the JSON.
        """
        return self._rpc_reply(request_id)

    async def request(
        self,
        subject: str,
        raw: str,
        *,
        request_id: str,
        inbox: str | None,
        timeout: float,
    ) -> str:
        assert inbox is not None
        await self.redis.rpush(self._rpc_queue(subject), raw)
        return await self._await_reply(subject, request_id, inbox, timeout)

    async def probe(
        self,
        subject: str,
        raw: str,
        *,
        request_id: str,
        inbox: str | None,
        timeout: float,
    ) -> str:
        assert inbox is not None
        queue = self._rpc_queue(subject)
        pipe = self.redis.pipeline(transaction=True)
        pipe.rpush(queue, raw)
        # Newest first: if an instance does come back, the probes worth
        # answering are the recent ones, and the stale ones it still finds are
        # dropped on arrival by their ``ts``.
        pipe.ltrim(queue, -PROBE_QUEUE_MAXLEN, -1)
        pipe.expire(queue, PROBE_QUEUE_TTL_SECONDS)
        await pipe.execute()
        return await self._await_reply(subject, request_id, inbox, timeout)

    async def _await_reply(
        self, subject: str, request_id: str, inbox: str, wait: float
    ) -> str:
        """Block on ``inbox`` until it answers or ``wait`` runs out."""
        # A pop that returns nothing is not a verdict — only the deadline is.
        # So this polls, and ``serve_poll_seconds`` is how often it looks up.
        deadline = time.monotonic() + wait
        poll = self.config.serve_poll_seconds
        try:
            while True:
                if deadline - time.monotonic() <= 0:
                    raise RequestTimeoutError(subject, request_id, wait)
                try:
                    result = await self.redis.blpop(inbox, timeout=poll)
                except (RedisTimeoutError, RedisConnectionError):
                    # Same read deadline as ``serve``'s poll, and the same
                    # answer: a poll that failed is not a reply and not a
                    # verdict either. The deadline above is what decides this
                    # call, so go back and keep asking until it passes.
                    logger.warning(
                        "broker reply poll failed subject=%s id=%s — polling again",
                        subject,
                        request_id,
                        exc_info=True,
                    )
                    continue
                if result is None:
                    continue
                _key, data = result
                return data
        finally:
            await self.redis.delete(inbox)

    async def post(self, subject: str, raw: str) -> None:
        await self.redis.rpush(self._rpc_queue(subject), raw)

    async def serve(
        self, subject: str, *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str | None]]:
        queue = self._rpc_queue(subject)
        while stop is None or not stop.is_set():
            try:
                result = await self.redis.blpop(
                    queue, timeout=self.config.serve_poll_seconds
                )
            except (RedisTimeoutError, RedisConnectionError):
                # A blocking pop carries its own read deadline, so a stalled
                # socket raises here even with no ``socket_timeout`` set, and
                # a Redis that is really down raises here once the retry in
                # :func:`build_redis` has given up. Both are reasons to poll
                # again, not to stop serving.
                logger.warning(
                    "broker serve poll failed subject=%s — polling again",
                    subject,
                    exc_info=True,
                )
                await asyncio.sleep(_SERVE_POLL_RETRY_S)
                continue
            if result is None:
                continue
            _key, data = result
            # ``None``: the reply address rode in on the JSON, so the broker
            # already has it and there is nothing to hand over.
            yield data, None

    async def send_reply(self, inbox: str, raw: str) -> None:
        await self.redis.rpush(inbox, raw)
        await self.redis.expire(inbox, self.config.reply_ttl_seconds)

    # --- shared state ------------------------------------------------------

    async def state_put_many(self, name: str, values: Mapping[str, str]) -> None:
        if not values:
            return
        await self.redis.hset(  # type: ignore[misc]
            self.state_key(name), mapping=dict(values)
        )

    async def state_replace(self, name: str, values: Mapping[str, str]) -> None:
        # Delete and rewrite in one transaction, so a reader never observes the
        # empty gap between them.
        key = self.state_key(name)
        pipe = self.redis.pipeline(transaction=True)
        pipe.delete(key)
        if values:
            pipe.hset(key, mapping=dict(values))
        await pipe.execute()

    async def state_get(self, name: str, field: str) -> str | None:
        return await self.redis.hget(self.state_key(name), field)  # type: ignore[misc]

    async def state_all(self, name: str) -> dict[str, str]:
        rows = await self.redis.hgetall(self.state_key(name))  # type: ignore[misc]
        return dict(rows)

    async def state_drop(self, name: str, fields: Sequence[str]) -> int:
        if not fields:
            return 0
        return int(
            await self.redis.hdel(self.state_key(name), *fields)  # type: ignore[misc]
        )

    async def state_clear(self, names: Sequence[str]) -> None:
        if names:
            await self.redis.delete(*(self.state_key(n) for n in names))

    # --- leases ------------------------------------------------------------

    async def lease_put(
        self, name: str, *, ttl: float, owner: str = LEASE_ANONYMOUS
    ) -> None:
        await self.redis.set(self.lease_key(name), owner, px=_ms(ttl))

    async def lease_take(
        self, name: str, *, ttl: float, owner: str = LEASE_ANONYMOUS
    ) -> bool:
        return bool(
            await self.redis.set(self.lease_key(name), owner, px=_ms(ttl), nx=True)
        )

    async def lease_owner(self, name: str) -> str | None:
        return await self.redis.get(self.lease_key(name))

    async def lease_hold(self, name: str, *, owner: str, ttl: float) -> bool:
        """Read then ``PEXPIRE``, and lose the race in the safe direction.

        Deliberately not a re-write of the value. If the lease lapsed between
        the two and a rival took it, this extends the *rival's* lease by one
        period — the rival keeps the resource and this caller finds out on its
        next pass. Re-writing the value would have taken it from them, which is
        the failure a lease exists to prevent.
        """
        key = self.lease_key(name)
        if await self.redis.get(key) != owner:
            return False
        return bool(await self.redis.pexpire(key, _ms(ttl)))

    async def lease_release(self, name: str, *, owner: str) -> bool:
        key = self.lease_key(name)
        if await self.redis.get(key) != owner:
            return False
        await self.redis.delete(key)
        return True

    async def lease_drop(self, name: str) -> None:
        await self.redis.delete(self.lease_key(name))

    # --- counters ----------------------------------------------------------

    async def counter_next(self, name: str) -> int:
        return int(await self.redis.incr(self._key(name)))

    # --- recorded tape -----------------------------------------------------
    #
    # Streams rather than lists because the retention policy is a *duration* —
    # a stream id is a millisecond timestamp, so "keep two hours" is
    # ``XTRIM MINID`` and "read from T" is ``XRANGE``, neither of which a list
    # can express: ``LTRIM`` counts entries, and the same count is eight hours
    # of a quiet instrument or twenty minutes of a busy one.

    async def tape_append(
        self,
        feed: str,
        fields: Mapping[str, str],
        *,
        maxlen: int,
        ttl_seconds: int,
        recorded_ms: int | None = None,
    ) -> None:
        # ``maxlen`` is exact, not ``~``. Approximate trimming is the cheaper
        # spelling and it is what this used to say, but Redis then trims whole
        # macro nodes — so a stream shorter than one node is never trimmed at
        # all, and the fuse the interface promises does not hold until the feed
        # is a hundred records past it. At steady state exact trimming removes
        # one entry per append, which is what this path can afford.
        #
        # ``*`` leaves the stamp to Redis, which is what production wants: an
        # explicit id must exceed every id already in the stream, so a client
        # clock that stepped backwards over NTP would start failing appends.
        # ``<ms>-*`` keeps the sequence Redis' too, so a caller naming the same
        # millisecond twice is fine.
        pipe = self.redis.pipeline()
        pipe.xadd(
            self.tape_key(feed),
            dict(fields),
            id="*" if recorded_ms is None else f"{recorded_ms}-*",
            maxlen=maxlen,
            approximate=False,
        )
        pipe.expire(self.tape_key(feed), ttl_seconds)
        pipe.expire(self.tape_coverage_key(feed), ttl_seconds)
        await pipe.execute()

    async def tape_tail(
        self, feed: str, *, count: int
    ) -> list[tuple[int, dict[str, str]]]:
        if count <= 0:
            return []
        rows = await self.redis.xrevrange(
            self.tape_key(feed), max="+", min="-", count=count
        )
        return [(_record_ms(str(rid)), dict(fields)) for rid, fields in reversed(rows)]

    async def tape_trim_before(self, feed: str, *, min_id_ms: int) -> int:
        # ``approximate=False``, against redis-py's default. ``XTRIM MINID ~``
        # stops at the first macro node it cannot drop whole, so a two hour
        # window on a quiet feed drops nothing and reports nothing — and the
        # count is what the caller logs the sweep by. This runs on a timer per
        # feed, not per print, so the exact form costs nothing that matters.
        return int(
            await self.redis.xtrim(
                self.tape_key(feed), minid=min_id_ms, approximate=False
            )
        )

    async def tape_coverage(self, feed: str) -> dict[str, str]:
        return dict(await self.redis.hgetall(self.tape_coverage_key(feed)))  # type: ignore[misc]

    async def tape_coverage_put(
        self, feed: str, values: Mapping[str, str], *, ttl_seconds: int
    ) -> None:
        pipe = self.redis.pipeline()
        pipe.hset(self.tape_coverage_key(feed), mapping=dict(values))
        pipe.expire(self.tape_coverage_key(feed), ttl_seconds)
        await pipe.execute()
